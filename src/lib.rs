//! Local S3 core. Synchronous and SQLite-backed; callers supply Unix milliseconds.
pub mod checksum;
mod multipart;
pub mod server;

pub use checksum::{Checksum, ChecksumAlgorithm, ChecksumInput};
pub use multipart::{CompletedPart, PartInfo, UploadInfo};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DEFAULT_REGION: &str = "us-east-1";
/// Upper bound for keys/prefixes/uploads/parts returned by one list call.
pub const MAX_LIST: usize = 1000;
pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_USER_METADATA_BYTES: usize = 2048;
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const MAX_PART_NUMBER: u32 = 10_000;
const SCHEMA_VERSION: i64 = 1;

#[derive(Debug)]
pub enum Error {
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidBucketName,
    KeyTooLong,
    MetadataTooLarge,
    InvalidArgument(String),
    InvalidRequest(String),
    InvalidRange,
    InvalidPart(String),
    InvalidPartOrder,
    EntityTooSmall,
    BadDigest(String),
    PreconditionFailed,
    NotModified,
    NotImplemented(String),
    Storage(rusqlite::Error),
}

impl Error {
    /// S3 error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::KeyTooLong => "KeyTooLongError",
            Self::MetadataTooLarge => "MetadataTooLarge",
            Self::InvalidArgument(_) => "InvalidArgument",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::InvalidRange => "InvalidRange",
            Self::InvalidPart(_) => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::EntityTooSmall => "EntityTooSmall",
            Self::BadDigest(_) => "BadDigest",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotModified => "NotModified",
            Self::NotImplemented(_) => "NotImplemented",
            Self::Storage(_) => "InternalError",
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::Storage(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::NoSuchBucket => "The specified bucket does not exist",
            Self::NoSuchKey => "The specified key does not exist.",
            Self::NoSuchUpload => "The specified upload does not exist.",
            Self::BucketAlreadyOwnedByYou => {
                "Your previous request to create the named bucket succeeded and you already own it."
            }
            Self::BucketNotEmpty => "The bucket you tried to delete is not empty",
            Self::InvalidBucketName => "The specified bucket is not valid.",
            Self::KeyTooLong => "Your key is too long",
            Self::MetadataTooLarge => {
                "Your metadata headers exceed the maximum allowed metadata size"
            }
            Self::InvalidRange => "The requested range is not satisfiable",
            Self::InvalidPartOrder => "The list of parts was not in ascending order.",
            Self::EntityTooSmall => "Your proposed upload is smaller than the minimum allowed size",
            Self::PreconditionFailed => {
                "At least one of the pre-conditions you specified did not hold"
            }
            Self::NotModified => "Not Modified",
            Self::InvalidArgument(s)
            | Self::InvalidRequest(s)
            | Self::InvalidPart(s)
            | Self::BadDigest(s)
            | Self::NotImplemented(s) => s,
            Self::Storage(e) => return write!(f, "{e}"),
        };
        f.write_str(s)
    }
}

impl std::error::Error for Error {}

/// System and user metadata stored with an object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<String>,
    /// `x-amz-meta-*` with lowercase names (without the prefix).
    #[serde(default)]
    pub user: BTreeMap<String, String>,
}

impl Metadata {
    fn validate(&self) -> Result<(), Error> {
        let size: usize = self.user.iter().map(|(k, v)| k.len() + v.len()).sum();
        if size > MAX_USER_METADATA_BYTES {
            return Err(Error::MetadataTooLarge);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Bucket {
    pub name: String,
    pub region: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    /// Unquoted ETag: hex MD5, or `<hex>-<parts>` for multipart objects.
    pub etag: String,
    pub last_modified_ms: i64,
    pub metadata: Metadata,
    pub checksum: Option<Checksum>,
}

/// Conditions for writes (`If-Match` / `If-None-Match: *`).
#[derive(Debug, Clone, Default)]
pub struct WriteCondition {
    pub if_match: Option<String>,
    pub if_none_match_any: bool,
}

/// Conditions for reads. Times are Unix milliseconds (second precision is used).
#[derive(Debug, Clone, Default)]
pub struct ReadCondition {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since_ms: Option<i64>,
    pub if_unmodified_since_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=start-` or `bytes=start-end` (end inclusive).
    From { start: u64, end: Option<u64> },
    /// `bytes=-n`: the last n bytes.
    Suffix(u64),
}

impl ByteRange {
    /// Parses a single `bytes=` range; `None` for anything else.
    pub fn parse(s: &str) -> Option<Self> {
        let spec = s.trim().strip_prefix("bytes=")?.trim();
        let (a, b) = spec.split_once('-')?;
        let num = |s: &str| {
            (!s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()))
                .then(|| s.parse::<u64>().ok())
                .flatten()
        };
        if a.is_empty() {
            return num(b).map(Self::Suffix);
        }
        let start = num(a)?;
        let end = if b.is_empty() { None } else { Some(num(b)?) };
        if end.is_some_and(|e| e < start) {
            return None;
        }
        Some(Self::From { start, end })
    }
    /// Half-open `[start, end)` within an object of `size` bytes.
    pub fn resolve(self, size: u64) -> Result<(u64, u64), Error> {
        match self {
            Self::From { start, end } => {
                if start >= size {
                    return Err(Error::InvalidRange);
                }
                Ok((start, end.map_or(size, |e| e.saturating_add(1).min(size))))
            }
            Self::Suffix(n) => {
                if n == 0 || size == 0 {
                    return Err(Error::InvalidRange);
                }
                Ok((size - n.min(size), size))
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    pub metadata: Metadata,
    pub checksum: ChecksumInput,
    pub content_md5: Option<[u8; 16]>,
    pub condition: WriteCondition,
}

#[derive(Debug, Clone)]
pub struct GetOutput {
    pub info: ObjectInfo,
    pub data: Vec<u8>,
    /// Half-open byte range when a Range was applied.
    pub range: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Default)]
pub struct ListRequest {
    pub prefix: String,
    pub delimiter: String,
    /// Exclusive start position (StartAfter / Marker / continuation token).
    pub after: String,
    pub max_keys: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ListResult {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Last key or common prefix returned, set when truncated.
    pub next_marker: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketStats {
    pub name: String,
    pub region: String,
    pub created_at_ms: i64,
    pub objects: u64,
    pub bytes: u64,
    pub multipart_uploads: u64,
}

pub struct Ls2 {
    conn: Connection,
}

impl Ls2 {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        Self::init(Connection::open(path)?)
    }
    pub fn in_memory() -> Result<Self, Error> {
        Self::init(Connection::open_in_memory()?)
    }
    fn init(conn: Connection) -> Result<Self, Error> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                region TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS objects (
                id INTEGER PRIMARY KEY,
                bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
                key BLOB NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                last_modified_ms INTEGER NOT NULL,
                metadata TEXT NOT NULL,
                checksum TEXT,
                UNIQUE (bucket, key));
            CREATE TABLE IF NOT EXISTS chunks (
                object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                size INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (object_id, seq));
            CREATE TABLE IF NOT EXISTS uploads (
                id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
                key BLOB NOT NULL,
                initiated_at_ms INTEGER NOT NULL,
                metadata TEXT NOT NULL,
                checksum_algorithm TEXT);
            CREATE INDEX IF NOT EXISTS uploads_by_key ON uploads (bucket, key, initiated_at_ms, id);
            CREATE TABLE IF NOT EXISTS parts (
                upload_id TEXT NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
                part_number INTEGER NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                last_modified_ms INTEGER NOT NULL,
                checksum TEXT,
                data BLOB NOT NULL,
                PRIMARY KEY (upload_id, part_number));",
        )?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > SCHEMA_VERSION {
            return Err(Error::InvalidRequest(format!(
                "Database schema version {version} is newer than this ls2 ({SCHEMA_VERSION})"
            )));
        }
        conn.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION}"))?;
        Ok(Self { conn })
    }

    fn write_tx(&mut self) -> Result<rusqlite::Transaction<'_>, Error> {
        Ok(self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?)
    }

    // ---- buckets ----

    /// Creates a bucket. Re-creating an owned bucket in us-east-1 succeeds, as in S3.
    pub fn create_bucket(
        &mut self,
        name: &str,
        region: Option<&str>,
        now_ms: i64,
    ) -> Result<(), Error> {
        if !valid_bucket_name(name) {
            return Err(Error::InvalidBucketName);
        }
        let region = region.filter(|r| !r.is_empty()).unwrap_or(DEFAULT_REGION);
        if !(1..=32).contains(&region.len())
            || !region
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        {
            return Err(Error::InvalidArgument("Invalid LocationConstraint".into()));
        }
        let tx = self.write_tx()?;
        let inserted = tx.execute(
            "INSERT INTO buckets(name, region, created_at_ms) VALUES (?1, ?2, ?3)
             ON CONFLICT(name) DO NOTHING",
            params![name, region, now_ms],
        )?;
        if inserted == 0 {
            let existing: String =
                tx.query_row("SELECT region FROM buckets WHERE name=?1", [name], |r| {
                    r.get(0)
                })?;
            if !(existing == DEFAULT_REGION && region == DEFAULT_REGION) {
                return Err(Error::BucketAlreadyOwnedByYou);
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn bucket(&self, name: &str) -> Result<Bucket, Error> {
        load_bucket(&self.conn, name)?.ok_or(Error::NoSuchBucket)
    }

    /// Buckets in name order after `after` (exclusive). Returns `(buckets, truncated)`.
    pub fn list_buckets(
        &self,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<(Vec<Bucket>, bool), Error> {
        let mut stmt = self.conn.prepare(
            "SELECT name, region, created_at_ms FROM buckets
             WHERE name > ?1 AND substr(name, 1, length(?2)) = ?2 ORDER BY name LIMIT ?3",
        )?;
        let mut buckets = stmt
            .query_map(params![after, prefix, limit as i64 + 1], |r| {
                Ok(Bucket {
                    name: r.get(0)?,
                    region: r.get(1)?,
                    created_at_ms: r.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = buckets.len() > limit;
        buckets.truncate(limit);
        Ok((buckets, truncated))
    }

    /// Deletes an empty bucket; in-progress multipart uploads are discarded with it.
    pub fn delete_bucket(&mut self, name: &str) -> Result<(), Error> {
        let tx = self.write_tx()?;
        require_bucket(&tx, name)?;
        let has_objects: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket=?1)",
            [name],
            |r| r.get(0),
        )?;
        if has_objects {
            return Err(Error::BucketNotEmpty);
        }
        tx.execute("DELETE FROM buckets WHERE name=?1", [name])?;
        tx.commit()?;
        Ok(())
    }

    pub fn stats(&self) -> Result<Vec<BucketStats>, Error> {
        let mut stmt = self.conn.prepare(
            "SELECT b.name, b.region, b.created_at_ms,
                (SELECT count(*) FROM objects o WHERE o.bucket = b.name),
                (SELECT coalesce(sum(size), 0) FROM objects o WHERE o.bucket = b.name),
                (SELECT count(*) FROM uploads u WHERE u.bucket = b.name)
             FROM buckets b ORDER BY b.name",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(BucketStats {
                name: r.get(0)?,
                region: r.get(1)?,
                created_at_ms: r.get(2)?,
                objects: r.get(3)?,
                bytes: r.get(4)?,
                multipart_uploads: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    // ---- objects ----

    /// Stores an object atomically, replacing any existing one.
    /// Without a client checksum, a CRC64NVME checksum is computed as S3 does.
    pub fn put_object(
        &mut self,
        bucket: &str,
        key: &str,
        data: &[u8],
        opts: PutOptions,
        now_ms: i64,
    ) -> Result<ObjectInfo, Error> {
        validate_key(key)?;
        opts.metadata.validate()?;
        let md5 = checksum::verify_md5(data, opts.content_md5)?;
        let checksum = opts
            .checksum
            .resolve(data, Some(ChecksumAlgorithm::Crc64Nvme))?;
        let info = ObjectInfo {
            bucket: bucket.into(),
            key: key.into(),
            size: data.len() as u64,
            etag: checksum::hex(&md5),
            last_modified_ms: now_ms,
            metadata: opts.metadata,
            checksum,
        };
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        check_write_condition(&tx, bucket, key, &opts.condition)?;
        let id = replace_object(&tx, &info)?;
        if !data.is_empty() {
            tx.execute(
                "INSERT INTO chunks(object_id, seq, offset, size, data) VALUES (?1, 0, 0, ?2, ?3)",
                params![id, data.len() as i64, data],
            )?;
        }
        tx.commit()?;
        Ok(info)
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, Error> {
        require_bucket(&self.conn, bucket)?;
        Ok(load_object(&self.conn, bucket, key)?
            .ok_or(Error::NoSuchKey)?
            .1)
    }

    /// Evaluates read conditions and returns metadata only.
    pub fn head_object_if(
        &self,
        bucket: &str,
        key: &str,
        cond: &ReadCondition,
    ) -> Result<ObjectInfo, Error> {
        let info = self.head_object(bucket, key)?;
        check_read_condition(&info, cond)?;
        Ok(info)
    }

    pub fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
        cond: &ReadCondition,
    ) -> Result<GetOutput, Error> {
        let tx = self.conn.unchecked_transaction()?;
        require_bucket(&tx, bucket)?;
        let (id, info) = load_object(&tx, bucket, key)?.ok_or(Error::NoSuchKey)?;
        check_read_condition(&info, cond)?;
        let resolved = range.map(|r| r.resolve(info.size)).transpose()?;
        let (start, end) = resolved.unwrap_or((0, info.size));
        let data = read_range(&tx, id, start, end)?;
        Ok(GetOutput {
            info,
            data,
            range: resolved,
        })
    }

    /// Deleting a missing key succeeds, as in S3.
    pub fn delete_object(&mut self, bucket: &str, key: &str) -> Result<(), Error> {
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        delete_key(&tx, bucket, key)?;
        tx.commit()?;
        Ok(())
    }

    /// Deletes keys in one transaction; per-key results in input order.
    pub fn delete_objects(
        &mut self,
        bucket: &str,
        keys: &[String],
    ) -> Result<Vec<Result<(), Error>>, Error> {
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(match validate_key(key) {
                Ok(()) => {
                    delete_key(&tx, bucket, key)?;
                    Ok(())
                }
                Err(e) => Err(e),
            });
        }
        tx.commit()?;
        Ok(results)
    }

    /// Server-side copy. `metadata: None` copies the source metadata (COPY directive).
    /// The copy keeps the source ETag and checksum. `source` holds the
    /// `x-amz-copy-source-if-*` conditions; any failure is PreconditionFailed.
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object(
        &mut self,
        src_bucket: &str,
        src_key: &str,
        source: &ReadCondition,
        dst_bucket: &str,
        dst_key: &str,
        metadata: Option<Metadata>,
        now_ms: i64,
    ) -> Result<ObjectInfo, Error> {
        validate_key(dst_key)?;
        if let Some(m) = &metadata {
            m.validate()?;
        }
        let tx = self.write_tx()?;
        require_bucket(&tx, src_bucket)?;
        let (src_id, src) = load_object(&tx, src_bucket, src_key)?.ok_or(Error::NoSuchKey)?;
        check_copy_condition(&src, source)?;
        require_bucket(&tx, dst_bucket)?;
        let info = ObjectInfo {
            bucket: dst_bucket.into(),
            key: dst_key.into(),
            last_modified_ms: now_ms,
            metadata: metadata.clone().unwrap_or_else(|| src.metadata.clone()),
            ..src
        };
        if src_bucket == dst_bucket && src_key == dst_key {
            if metadata.is_none() {
                return Err(Error::InvalidRequest(
                    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.".into(),
                ));
            }
            tx.execute(
                "UPDATE objects SET metadata=?2, last_modified_ms=?3 WHERE id=?1",
                params![src_id, to_json(&info.metadata), now_ms],
            )?;
        } else {
            let id = replace_object(&tx, &info)?;
            tx.execute(
                "INSERT INTO chunks(object_id, seq, offset, size, data)
                 SELECT ?1, seq, offset, size, data FROM chunks WHERE object_id=?2",
                params![id, src_id],
            )?;
        }
        tx.commit()?;
        Ok(info)
    }

    /// Lists keys in UTF-8 binary order, grouping by `delimiter` into common prefixes.
    pub fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult, Error> {
        let tx = self.conn.unchecked_transaction()?;
        require_bucket(&tx, bucket)?;
        let mut out = ListResult::default();
        let max = req.max_keys.min(MAX_LIST);
        if max == 0 {
            return Ok(out);
        }
        let prefix = req.prefix.as_bytes();
        let marker = req.after.as_bytes();
        let delimiter = req.delimiter.as_bytes();
        let upper = prefix_upper_bound(prefix);
        // (position, inclusive)
        let mut cursor: (Vec<u8>, bool) = if marker >= prefix {
            (marker.to_vec(), false)
        } else {
            (prefix.to_vec(), true)
        };
        let mut stmt = tx.prepare(
            "SELECT id, key, size, etag, last_modified_ms, metadata, checksum FROM objects
             WHERE bucket = ?1 AND (key > ?2 OR (?3 AND key = ?2)) AND (?4 IS NULL OR key < ?4)
             ORDER BY key LIMIT 256",
        )?;
        let mut last_prefix: Option<Vec<u8>> = None;
        'scan: loop {
            let rows = stmt
                .query_map(params![bucket, cursor.0, cursor.1, upper], |r| {
                    decode_object(r, bucket)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let fetched = rows.len();
            let mut jumped = false;
            for (_, info) in rows {
                let key = info.key.as_bytes();
                let rest = &key[prefix.len()..];
                if let Some(i) = (!delimiter.is_empty())
                    .then(|| find(rest, delimiter))
                    .flatten()
                {
                    let cp = &key[..prefix.len() + i + delimiter.len()];
                    if cp > marker && last_prefix.as_deref() != Some(cp) {
                        if out.objects.len() + out.common_prefixes.len() == max {
                            out.truncated = true;
                            break 'scan;
                        }
                        out.common_prefixes
                            .push(String::from_utf8_lossy(cp).into_owned());
                        last_prefix = Some(cp.to_vec());
                    }
                    match prefix_upper_bound(cp) {
                        Some(next) => cursor = (next, true),
                        None => break 'scan,
                    }
                    jumped = true;
                    break;
                }
                if out.objects.len() + out.common_prefixes.len() == max {
                    out.truncated = true;
                    break 'scan;
                }
                cursor = (key.to_vec(), false);
                out.objects.push(info);
            }
            if !jumped && fetched < 256 {
                break;
            }
        }
        if out.truncated {
            let last_key = out.objects.last().map(|o| o.key.clone());
            let last_cp = out.common_prefixes.last().cloned();
            out.next_marker = last_key.into_iter().chain(last_cp).max();
        }
        Ok(out)
    }
}

// ---- helpers shared with multipart ----

pub(crate) fn to_json<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("serializable")
}

pub(crate) fn from_json<T: for<'de> Deserialize<'de>>(s: &str, col: usize) -> rusqlite::Result<T> {
    serde_json::from_str(s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(col, rusqlite::types::Type::Text, Box::new(e))
    })
}

pub(crate) fn key_string(bytes: Vec<u8>, col: usize) -> rusqlite::Result<String> {
    String::from_utf8(bytes).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(col, rusqlite::types::Type::Blob, Box::new(e))
    })
}

fn decode_object(r: &rusqlite::Row<'_>, bucket: &str) -> rusqlite::Result<(i64, ObjectInfo)> {
    let checksum: Option<String> = r.get(6)?;
    Ok((
        r.get(0)?,
        ObjectInfo {
            bucket: bucket.into(),
            key: key_string(r.get(1)?, 1)?,
            size: r.get(2)?,
            etag: r.get(3)?,
            last_modified_ms: r.get(4)?,
            metadata: from_json(&r.get::<_, String>(5)?, 5)?,
            checksum: checksum.map(|c| from_json(&c, 6)).transpose()?,
        },
    ))
}

fn load_bucket(conn: &Connection, name: &str) -> Result<Option<Bucket>, Error> {
    Ok(conn
        .query_row(
            "SELECT name, region, created_at_ms FROM buckets WHERE name=?1",
            [name],
            |r| {
                Ok(Bucket {
                    name: r.get(0)?,
                    region: r.get(1)?,
                    created_at_ms: r.get(2)?,
                })
            },
        )
        .optional()?)
}

pub(crate) fn require_bucket(conn: &Connection, name: &str) -> Result<(), Error> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM buckets WHERE name=?1)",
        [name],
        |r| r.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(Error::NoSuchBucket)
    }
}

pub(crate) fn load_object(
    conn: &Connection,
    bucket: &str,
    key: &str,
) -> Result<Option<(i64, ObjectInfo)>, Error> {
    Ok(conn
        .query_row(
            "SELECT id, key, size, etag, last_modified_ms, metadata, checksum FROM objects
             WHERE bucket=?1 AND key=?2",
            params![bucket, key.as_bytes()],
            |r| decode_object(r, bucket),
        )
        .optional()?)
}

fn delete_key(conn: &Connection, bucket: &str, key: &str) -> Result<(), Error> {
    conn.execute(
        "DELETE FROM objects WHERE bucket=?1 AND key=?2",
        params![bucket, key.as_bytes()],
    )?;
    Ok(())
}

/// Replaces the row for `info.key` and returns the new object id (chunks are added by the caller).
pub(crate) fn replace_object(conn: &Connection, info: &ObjectInfo) -> Result<i64, Error> {
    delete_key(conn, &info.bucket, &info.key)?;
    conn.execute(
        "INSERT INTO objects(bucket, key, size, etag, last_modified_ms, metadata, checksum)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            info.bucket,
            info.key.as_bytes(),
            info.size as i64,
            info.etag,
            info.last_modified_ms,
            to_json(&info.metadata),
            info.checksum.as_ref().map(to_json),
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

pub(crate) fn check_write_condition(
    conn: &Connection,
    bucket: &str,
    key: &str,
    cond: &WriteCondition,
) -> Result<(), Error> {
    if !cond.if_none_match_any && cond.if_match.is_none() {
        return Ok(());
    }
    let existing = load_object(conn, bucket, key)?;
    if cond.if_none_match_any && existing.is_some() {
        return Err(Error::PreconditionFailed);
    }
    if let Some(m) = &cond.if_match {
        match existing {
            None => return Err(Error::NoSuchKey),
            Some((_, info)) if !etag_matches(m, &info.etag) => {
                return Err(Error::PreconditionFailed);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Copy-source conditions report every failure as PreconditionFailed (never 304).
pub(crate) fn check_copy_condition(info: &ObjectInfo, cond: &ReadCondition) -> Result<(), Error> {
    match check_read_condition(info, cond) {
        Err(Error::NotModified) => Err(Error::PreconditionFailed),
        other => other,
    }
}

/// S3 precedence: If-Match beats If-Unmodified-Since; If-None-Match beats If-Modified-Since.
fn check_read_condition(info: &ObjectInfo, cond: &ReadCondition) -> Result<(), Error> {
    let modified = info.last_modified_ms.div_euclid(1000);
    let secs = |ms: i64| ms.div_euclid(1000);
    if let Some(m) = &cond.if_match {
        if !etag_matches(m, &info.etag) {
            return Err(Error::PreconditionFailed);
        }
    } else if cond
        .if_unmodified_since_ms
        .is_some_and(|t| modified > secs(t))
    {
        return Err(Error::PreconditionFailed);
    }
    if let Some(n) = &cond.if_none_match {
        if etag_matches(n, &info.etag) {
            return Err(Error::NotModified);
        }
    } else if cond
        .if_modified_since_ms
        .is_some_and(|t| modified <= secs(t))
    {
        return Err(Error::NotModified);
    }
    Ok(())
}

pub(crate) fn etag_matches(header: &str, etag: &str) -> bool {
    header
        .split(',')
        .map(str::trim)
        .any(|t| t == "*" || t.strip_prefix("W/").unwrap_or(t).trim_matches('"') == etag)
}

pub(crate) fn read_range(
    conn: &Connection,
    object_id: i64,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity((end - start) as usize);
    let mut stmt = conn.prepare(
        "SELECT substr(data, max(?2, offset) - offset + 1, min(?3, offset + size) - max(?2, offset))
         FROM chunks WHERE object_id = ?1 AND offset < ?3 AND offset + size > ?2 ORDER BY seq",
    )?;
    let mut rows = stmt.query(params![object_id, start as i64, end as i64])?;
    while let Some(row) = rows.next()? {
        out.extend_from_slice(row.get_ref(0)?.as_blob().map_err(rusqlite::Error::from)?);
    }
    if out.len() as u64 != end - start {
        return Err(Error::Storage(rusqlite::Error::InvalidQuery));
    }
    Ok(out)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Smallest byte string greater than every string starting with `prefix`.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut v = prefix.to_vec();
    while let Some(last) = v.pop() {
        if last < 0xff {
            v.push(last + 1);
            return Some(v);
        }
    }
    None
}

pub(crate) fn validate_key(key: &str) -> Result<(), Error> {
    if key.is_empty() {
        return Err(Error::InvalidArgument(
            "Object key must not be empty".into(),
        ));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(Error::KeyTooLong);
    }
    Ok(())
}

/// General purpose bucket naming rules.
pub fn valid_bucket_name(name: &str) -> bool {
    let b = name.as_bytes();
    (3..=63).contains(&b.len())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'.' || *c == b'-')
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && !name.contains("..")
        && name.parse::<std::net::Ipv4Addr>().is_err()
        && !["xn--", "sthree-", "amzn-s3-demo-"]
            .iter()
            .any(|p| name.starts_with(p))
        && !["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|s| name.ends_with(s))
}
