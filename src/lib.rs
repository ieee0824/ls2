//! Local S3 core. Synchronous and SQLite-backed; callers supply Unix milliseconds.
pub mod checksum;
mod cors;
mod lifecycle;
mod multipart;
mod notification;
mod schema;
pub mod server;
mod tagging;
mod versions;

pub use checksum::{Checksum, ChecksumAlgorithm, ChecksumInput};
pub use cors::{CorsRule, MAX_CORS_RULES, find_cors_rule};
pub use lifecycle::{
    Expiration, LifecycleFilter, LifecycleRule, LifecycleRun, MAX_LIFECYCLE_RULES,
    NoncurrentExpiration, parse_iso8601,
};
pub use multipart::{CompletedPart, PartInfo, UploadInfo};
use notification::{EventObject, Principal, emit};
pub use notification::{
    MAX_DELIVERY_ATTEMPTS, NotificationStats, PendingNotification, QueueConfiguration,
};
pub use tagging::{MAX_BUCKET_TAGS, MAX_OBJECT_TAGS, Tags, tags_from_pairs};
pub use versions::{VersionEntry, VersionListRequest, VersionListResult};

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
/// Version ID of objects written while versioning was not enabled.
pub const NULL_VERSION: &str = "null";

#[derive(Debug)]
pub enum Error {
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    NoSuchVersion,
    /// The addressed object is a delete marker. `requested` is true when the marker
    /// was named by version ID (405 in S3) rather than being the latest version (404).
    DeleteMarker {
        version_id: String,
        last_modified_ms: i64,
        requested: bool,
    },
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidBucketName,
    KeyTooLong,
    MetadataTooLarge,
    InvalidTag(String),
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
            Self::NoSuchVersion => "NoSuchVersion",
            Self::DeleteMarker {
                requested: false, ..
            } => "NoSuchKey",
            Self::DeleteMarker {
                requested: true, ..
            } => "MethodNotAllowed",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::KeyTooLong => "KeyTooLongError",
            Self::MetadataTooLarge => "MetadataTooLarge",
            Self::InvalidTag(_) => "InvalidTag",
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
            Self::NoSuchVersion => "The specified version does not exist.",
            Self::DeleteMarker {
                requested: false, ..
            } => "The specified key does not exist.",
            Self::DeleteMarker {
                requested: true, ..
            } => "The specified method is not allowed against this resource.",
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
            | Self::InvalidTag(s)
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

/// Bucket versioning state. A bucket never returns to "unversioned" once enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Versioning {
    Enabled,
    Suspended,
}

impl Versioning {
    pub fn name(self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Suspended => "Suspended",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(Self::Enabled),
            "Suspended" => Some(Self::Suspended),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Bucket {
    pub name: String,
    pub region: String,
    pub created_at_ms: i64,
    /// `None` until versioning is first configured.
    pub versioning: Option<Versioning>,
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
    /// [`NULL_VERSION`] unless written while versioning was enabled.
    pub version_id: String,
    pub tags: Tags,
}

/// Result of deleting a key or a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    /// The version removed, or the delete marker created; `None` in unversioned buckets.
    pub version_id: Option<String>,
    /// A delete marker was created, or the removed version was a delete marker.
    pub delete_marker: bool,
}

/// One key (optionally one version) of DeleteObjects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectId {
    pub key: String,
    pub version_id: Option<String>,
}

/// The source of CopyObject / UploadPartCopy.
#[derive(Debug, Clone, Default)]
pub struct CopySource {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    /// `x-amz-copy-source-if-*`; every failure is PreconditionFailed.
    pub condition: ReadCondition,
}

/// Directives of CopyObject; `None` copies the source value (the COPY directive).
#[derive(Debug, Clone, Default)]
pub struct CopyOptions {
    pub metadata: Option<Metadata>,
    pub tags: Option<Tags>,
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
    pub tags: Tags,
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

/// A located object for incremental reads (see [`Ls2::read_object`]).
#[derive(Debug, Clone)]
pub struct ObjectHandle {
    pub info: ObjectInfo,
    /// Half-open byte range to send: the applied Range, or the whole object.
    pub start: u64,
    pub end: u64,
    /// Whether a Range header was applied.
    pub partial: bool,
    id: i64,
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
    pub versioning: Option<Versioning>,
    /// Current (latest, not deleted) objects.
    pub objects: u64,
    /// Stored versions, including noncurrent versions and delete markers.
    pub versions: u64,
    /// Bytes stored across all versions.
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
    fn init(mut conn: Connection) -> Result<Self, Error> {
        schema::init(&mut conn)?;
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
            "SELECT name, region, created_at_ms, versioning FROM buckets
             WHERE name > ?1 AND substr(name, 1, length(?2)) = ?2 ORDER BY name LIMIT ?3",
        )?;
        let mut buckets = stmt
            .query_map(params![after, prefix, limit as i64 + 1], decode_bucket)?
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
            "SELECT b.name, b.region, b.created_at_ms, b.versioning,
                (SELECT count(*) FROM objects o
                 WHERE o.bucket = b.name AND o.is_latest = 1 AND o.delete_marker = 0),
                (SELECT count(*) FROM objects o WHERE o.bucket = b.name),
                (SELECT coalesce(sum(size), 0) FROM objects o WHERE o.bucket = b.name),
                (SELECT count(*) FROM uploads u WHERE u.bucket = b.name)
             FROM buckets b ORDER BY b.name",
        )?;
        let rows = stmt.query_map([], |r| {
            let bucket = decode_bucket(r)?;
            Ok(BucketStats {
                name: bucket.name,
                region: bucket.region,
                created_at_ms: bucket.created_at_ms,
                versioning: bucket.versioning,
                objects: r.get(4)?,
                versions: r.get(5)?,
                bytes: r.get(6)?,
                multipart_uploads: r.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// PutBucketVersioning. Enabled buckets give each write a new version ID;
    /// suspended buckets write the `null` version again.
    pub fn set_bucket_versioning(&mut self, bucket: &str, state: Versioning) -> Result<(), Error> {
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        tx.execute(
            "UPDATE buckets SET versioning=?2 WHERE name=?1",
            params![bucket, state.name()],
        )?;
        tx.commit()?;
        Ok(())
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
        let tx = self.write_tx()?;
        let versioning = bucket_versioning(&tx, bucket)?;
        let info = ObjectInfo {
            bucket: bucket.into(),
            key: key.into(),
            size: data.len() as u64,
            etag: checksum::hex(&md5),
            last_modified_ms: now_ms,
            metadata: opts.metadata,
            checksum,
            version_id: new_version_id(versioning),
            tags: opts.tags,
        };
        check_write_condition(&tx, bucket, key, &opts.condition)?;
        let id = write_version(&tx, &info, versioning)?;
        if !data.is_empty() {
            tx.execute(
                "INSERT INTO chunks(object_id, seq, offset, size, data) VALUES (?1, 0, 0, ?2, ?3)",
                params![id, data.len() as i64, data],
            )?;
        }
        let object = EventObject::created(&info);
        emit(
            &tx,
            bucket,
            "ObjectCreated:Put",
            &object,
            Principal::Request,
            now_ms,
        )?;
        tx.commit()?;
        Ok(info)
    }

    /// Metadata of the latest version (`version_id: None`) or of one version.
    pub fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectInfo, Error> {
        Ok(resolve_object(&self.conn, bucket, key, version_id)?.1)
    }

    pub fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
        cond: &ReadCondition,
    ) -> Result<GetOutput, Error> {
        let tx = self.conn.unchecked_transaction()?;
        let handle = open_object(&tx, bucket, key, None, range, cond)?;
        let data = read_range(&tx, handle.id, handle.start, handle.end)?;
        Ok(GetOutput {
            range: handle.partial.then_some((handle.start, handle.end)),
            info: handle.info,
            data,
        })
    }

    /// Evaluates conditions and the range without reading the body.
    /// Read the bytes later with [`Ls2::read_object`], piece by piece.
    pub fn open_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        range: Option<ByteRange>,
        cond: &ReadCondition,
    ) -> Result<ObjectHandle, Error> {
        open_object(&self.conn, bucket, key, version_id, range, cond)
    }

    /// Reads `[offset, offset + len)` of the object behind `handle` (clamped to its size).
    /// Fails if the object was replaced or deleted after `open_object`, so a long
    /// download never mixes two versions.
    pub fn read_object(
        &self,
        handle: &ObjectHandle,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, Error> {
        let tx = self.conn.unchecked_transaction()?;
        let unchanged: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE id=?1 AND etag=?2 AND last_modified_ms=?3)",
            params![handle.id, handle.info.etag, handle.info.last_modified_ms],
            |r| r.get(0),
        )?;
        if !unchanged {
            return Err(Error::InvalidRequest(
                "The object was modified while it was being read".into(),
            ));
        }
        let start = offset.min(handle.info.size);
        let end = offset.saturating_add(len as u64).min(handle.info.size);
        read_range(&tx, handle.id, start, end)
    }

    /// DeleteObject. Without a version ID this removes the object (unversioned bucket) or
    /// adds a delete marker (versioned bucket); with one, it removes that version for good.
    /// Deleting a missing key or version succeeds, as in S3.
    pub fn delete_object(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        now_ms: i64,
    ) -> Result<DeleteOutcome, Error> {
        let tx = self.write_tx()?;
        let versioning = bucket_versioning(&tx, bucket)?;
        let outcome = delete_and_notify(&tx, bucket, key, version_id, versioning, now_ms)?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Deletes keys in one transaction; per-key results in input order.
    pub fn delete_objects(
        &mut self,
        bucket: &str,
        objects: &[ObjectId],
        now_ms: i64,
    ) -> Result<Vec<Result<DeleteOutcome, Error>>, Error> {
        let tx = self.write_tx()?;
        let versioning = bucket_versioning(&tx, bucket)?;
        let mut results = Vec::with_capacity(objects.len());
        for o in objects {
            results.push(match validate_key(&o.key) {
                Ok(()) => {
                    match delete_and_notify(
                        &tx,
                        bucket,
                        &o.key,
                        o.version_id.as_deref(),
                        versioning,
                        now_ms,
                    ) {
                        Err(Error::Storage(e)) => return Err(Error::Storage(e)),
                        other => other,
                    }
                }
                Err(e) => Err(e),
            });
        }
        tx.commit()?;
        Ok(results)
    }

    /// Server-side copy. `metadata: None` copies the source metadata (COPY directive).
    /// The copy keeps the source ETag and checksum and gets its own version ID.
    /// Returns the new object and the version ID of the source that was copied.
    pub fn copy_object(
        &mut self,
        source: &CopySource,
        dst_bucket: &str,
        dst_key: &str,
        options: CopyOptions,
        now_ms: i64,
    ) -> Result<(ObjectInfo, String), Error> {
        validate_key(dst_key)?;
        let CopyOptions { metadata, tags } = options;
        if let Some(m) = &metadata {
            m.validate()?;
        }
        let tx = self.write_tx()?;
        let (src_id, src) = resolve_copy_source(&tx, source)?;
        check_copy_condition(&src, &source.condition)?;
        let versioning = bucket_versioning(&tx, dst_bucket)?;
        // Copying the latest version onto itself must change something; copying an older
        // version onto its key restores it.
        let same_key = source.bucket == dst_bucket && source.key == dst_key;
        let copies_latest =
            same_key && load_latest(&tx, dst_bucket, dst_key)?.is_some_and(|r| r.id == src_id);
        if copies_latest && metadata.is_none() {
            return Err(Error::InvalidRequest(
                "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.".into(),
            ));
        }
        let info = ObjectInfo {
            bucket: dst_bucket.into(),
            key: dst_key.into(),
            last_modified_ms: now_ms,
            metadata: metadata.unwrap_or_else(|| src.metadata.clone()),
            version_id: new_version_id(versioning),
            tags: tags.unwrap_or_else(|| src.tags.clone()),
            ..src.clone()
        };
        let replaces_source = source.bucket == dst_bucket
            && source.key == dst_key
            && src.version_id == NULL_VERSION
            && versioning != Some(Versioning::Enabled);
        if replaces_source {
            // The new `null` version would replace its own source: update it in place.
            tx.execute(
                "UPDATE objects SET metadata=?2, last_modified_ms=?3, tags=?4 WHERE id=?1",
                params![
                    src_id,
                    to_json(&info.metadata),
                    now_ms,
                    tagging::tags_json(&info.tags)
                ],
            )?;
        } else {
            let id = write_version(&tx, &info, versioning)?;
            tx.execute(
                "INSERT INTO chunks(object_id, seq, offset, size, data)
                 SELECT ?1, seq, offset, size, data FROM chunks WHERE object_id=?2",
                params![id, src_id],
            )?;
        }
        let object = EventObject::created(&info);
        emit(
            &tx,
            dst_bucket,
            "ObjectCreated:Copy",
            &object,
            Principal::Request,
            now_ms,
        )?;
        tx.commit()?;
        Ok((info, src.version_id))
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
        let mut stmt = tx.prepare(&format!(
            "SELECT {OBJECT_COLUMNS} FROM objects
             WHERE bucket = ?1 AND is_latest = 1 AND delete_marker = 0
               AND (key > ?2 OR (?3 AND key = ?2)) AND (?4 IS NULL OR key < ?4)
             ORDER BY key LIMIT 256"
        ))?;
        let mut last_prefix: Option<Vec<u8>> = None;
        'scan: loop {
            let rows = stmt
                .query_map(params![bucket, cursor.0, cursor.1, upper], |r| {
                    decode_row(r, bucket)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let fetched = rows.len();
            let mut jumped = false;
            for Row { info, .. } in rows {
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

pub(crate) const OBJECT_COLUMNS: &str = "id, key, size, etag, last_modified_ms, metadata, checksum, version_id, is_latest, delete_marker, tags";

/// One stored version (object or delete marker).
pub(crate) struct Row {
    pub id: i64,
    pub info: ObjectInfo,
    pub is_latest: bool,
    pub delete_marker: bool,
}

pub(crate) fn decode_row(r: &rusqlite::Row<'_>, bucket: &str) -> rusqlite::Result<Row> {
    let checksum: Option<String> = r.get(6)?;
    Ok(Row {
        id: r.get(0)?,
        info: ObjectInfo {
            bucket: bucket.into(),
            key: key_string(r.get(1)?, 1)?,
            size: r.get(2)?,
            etag: r.get(3)?,
            last_modified_ms: r.get(4)?,
            metadata: from_json(&r.get::<_, String>(5)?, 5)?,
            checksum: checksum.map(|c| from_json(&c, 6)).transpose()?,
            version_id: r.get(7)?,
            tags: tagging::decode_tags(r.get(10)?, 10)?,
        },
        is_latest: r.get(8)?,
        delete_marker: r.get(9)?,
    })
}

fn decode_bucket(r: &rusqlite::Row<'_>) -> rusqlite::Result<Bucket> {
    let versioning: Option<String> = r.get(3)?;
    Ok(Bucket {
        name: r.get(0)?,
        region: r.get(1)?,
        created_at_ms: r.get(2)?,
        versioning: versioning.as_deref().and_then(Versioning::parse),
    })
}

fn load_bucket(conn: &Connection, name: &str) -> Result<Option<Bucket>, Error> {
    Ok(conn
        .query_row(
            "SELECT name, region, created_at_ms, versioning FROM buckets WHERE name=?1",
            [name],
            decode_bucket,
        )
        .optional()?)
}

pub(crate) fn require_bucket(conn: &Connection, name: &str) -> Result<(), Error> {
    bucket_versioning(conn, name).map(|_| ())
}

/// The bucket's versioning state; NoSuchBucket if it does not exist.
pub(crate) fn bucket_versioning(
    conn: &Connection,
    name: &str,
) -> Result<Option<Versioning>, Error> {
    let state: Option<Option<String>> = conn
        .query_row(
            "SELECT versioning FROM buckets WHERE name=?1",
            [name],
            |r| r.get(0),
        )
        .optional()?;
    Ok(state
        .ok_or(Error::NoSuchBucket)?
        .as_deref()
        .and_then(Versioning::parse))
}

pub(crate) fn new_version_id(versioning: Option<Versioning>) -> String {
    match versioning {
        Some(Versioning::Enabled) => uuid::Uuid::new_v4().simple().to_string(),
        _ => NULL_VERSION.into(),
    }
}

/// Version IDs are `null` or 32 lowercase hex digits; anything else is rejected as in S3.
pub(crate) fn validate_version_id(v: &str) -> Result<(), Error> {
    if v == NULL_VERSION
        || (v.len() == 32 && v.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')))
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "Invalid version id specified".into(),
        ))
    }
}

fn load_one(
    conn: &Connection,
    bucket: &str,
    filter: &str,
    args: &[&dyn rusqlite::ToSql],
) -> Result<Option<Row>, Error> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {OBJECT_COLUMNS} FROM objects WHERE bucket=?1 AND key=?2 AND {filter}"
            ),
            args,
            |r| decode_row(r, bucket),
        )
        .optional()?)
}

pub(crate) fn load_latest(
    conn: &Connection,
    bucket: &str,
    key: &str,
) -> Result<Option<Row>, Error> {
    load_one(
        conn,
        bucket,
        "is_latest = 1",
        params![bucket, key.as_bytes()],
    )
}

pub(crate) fn load_version(
    conn: &Connection,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Option<Row>, Error> {
    load_one(
        conn,
        bucket,
        "version_id = ?3",
        params![bucket, key.as_bytes(), version_id],
    )
}

/// The latest version unless it is a delete marker.
pub(crate) fn load_current(
    conn: &Connection,
    bucket: &str,
    key: &str,
) -> Result<Option<(i64, ObjectInfo)>, Error> {
    Ok(load_latest(conn, bucket, key)?
        .filter(|r| !r.delete_marker)
        .map(|r| (r.id, r.info)))
}

/// The object addressed by a read: the latest version or the named one.
pub(crate) fn resolve_object(
    conn: &Connection,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(i64, ObjectInfo), Error> {
    require_bucket(conn, bucket)?;
    let row = match version_id {
        None => load_latest(conn, bucket, key)?.ok_or(Error::NoSuchKey)?,
        Some(v) => {
            validate_version_id(v)?;
            load_version(conn, bucket, key, v)?.ok_or(Error::NoSuchVersion)?
        }
    };
    if row.delete_marker {
        return Err(Error::DeleteMarker {
            version_id: row.info.version_id,
            last_modified_ms: row.info.last_modified_ms,
            requested: version_id.is_some(),
        });
    }
    Ok((row.id, row.info))
}

/// A copy source may not name a delete marker by version ID (400 in S3).
pub(crate) fn resolve_copy_source(
    conn: &Connection,
    source: &CopySource,
) -> Result<(i64, ObjectInfo), Error> {
    resolve_object(conn, &source.bucket, &source.key, source.version_id.as_deref()).map_err(
        |e| match e {
            Error::DeleteMarker {
                requested: true, ..
            } => Error::InvalidRequest(
                "The source of a copy request may not specifically refer to a delete marker by version id.".into(),
            ),
            e => e,
        },
    )
}

/// Marks the newest remaining version of `key` as the latest one.
fn refresh_latest(conn: &Connection, bucket: &str, key: &str) -> Result<(), Error> {
    conn.execute(
        "UPDATE objects SET is_latest = (id = (SELECT max(id) FROM objects WHERE bucket=?1 AND key=?2))
         WHERE bucket=?1 AND key=?2",
        params![bucket, key.as_bytes()],
    )?;
    Ok(())
}

/// Inserts `info` as the new latest version and returns its row id (chunks are added by
/// the caller). Unless versioning is enabled, it replaces the existing `null` version.
pub(crate) fn write_version(
    conn: &Connection,
    info: &ObjectInfo,
    versioning: Option<Versioning>,
) -> Result<i64, Error> {
    insert_row(conn, info, false, versioning)
}

fn insert_row(
    conn: &Connection,
    info: &ObjectInfo,
    delete_marker: bool,
    versioning: Option<Versioning>,
) -> Result<i64, Error> {
    if versioning != Some(Versioning::Enabled) {
        conn.execute(
            "DELETE FROM objects WHERE bucket=?1 AND key=?2 AND version_id=?3",
            params![info.bucket, info.key.as_bytes(), NULL_VERSION],
        )?;
    }
    conn.execute(
        "INSERT INTO objects(bucket, key, version_id, is_latest, delete_marker,
                             size, etag, last_modified_ms, metadata, checksum, tags)
         VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            info.bucket,
            info.key.as_bytes(),
            info.version_id,
            delete_marker,
            info.size as i64,
            info.etag,
            info.last_modified_ms,
            to_json(&info.metadata),
            info.checksum.as_ref().map(to_json),
            tagging::tags_json(&info.tags),
        ],
    )?;
    let id = conn.last_insert_rowid();
    refresh_latest(conn, &info.bucket, &info.key)?;
    Ok(id)
}

fn delete_in(
    conn: &Connection,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    versioning: Option<Versioning>,
    now_ms: i64,
) -> Result<(DeleteOutcome, bool), Error> {
    if let Some(v) = version_id {
        validate_version_id(v)?;
        let removed = load_version(conn, bucket, key, v)?;
        conn.execute(
            "DELETE FROM objects WHERE bucket=?1 AND key=?2 AND version_id=?3",
            params![bucket, key.as_bytes(), v],
        )?;
        refresh_latest(conn, bucket, key)?;
        let existed = removed.is_some();
        let outcome = DeleteOutcome {
            version_id: Some(v.into()),
            delete_marker: removed.is_some_and(|r| r.delete_marker),
        };
        return Ok((outcome, existed));
    }
    if versioning.is_none() {
        let deleted = conn.execute(
            "DELETE FROM objects WHERE bucket=?1 AND key=?2",
            params![bucket, key.as_bytes()],
        )?;
        let outcome = DeleteOutcome {
            version_id: None,
            delete_marker: false,
        };
        return Ok((outcome, deleted > 0));
    }
    // Versioned: a delete marker becomes the latest version (even for a missing key).
    let marker = ObjectInfo {
        bucket: bucket.into(),
        key: key.into(),
        size: 0,
        etag: String::new(),
        last_modified_ms: now_ms,
        metadata: Metadata::default(),
        checksum: None,
        version_id: new_version_id(versioning),
        tags: Tags::new(),
    };
    insert_row(conn, &marker, true, versioning)?;
    let outcome = DeleteOutcome {
        version_id: Some(marker.version_id),
        delete_marker: true,
    };
    Ok((outcome, true))
}

/// DeleteObject as requested by a client, with its `ObjectRemoved` event.
fn delete_and_notify(
    conn: &Connection,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    versioning: Option<Versioning>,
    now_ms: i64,
) -> Result<DeleteOutcome, Error> {
    let (outcome, existed) = delete_in(conn, bucket, key, version_id, versioning, now_ms)?;
    if existed {
        let event = if version_id.is_none() && outcome.delete_marker {
            "ObjectRemoved:DeleteMarkerCreated"
        } else {
            "ObjectRemoved:Delete"
        };
        let object = EventObject::removed(key, outcome.version_id.as_deref());
        emit(conn, bucket, event, &object, Principal::Request, now_ms)?;
    }
    Ok(outcome)
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
    let existing = load_current(conn, bucket, key)?;
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

fn open_object(
    conn: &Connection,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    range: Option<ByteRange>,
    cond: &ReadCondition,
) -> Result<ObjectHandle, Error> {
    let (id, info) = resolve_object(conn, bucket, key, version_id)?;
    check_read_condition(&info, cond)?;
    let resolved = range.map(|r| r.resolve(info.size)).transpose()?;
    let (start, end) = resolved.unwrap_or((0, info.size));
    Ok(ObjectHandle {
        info,
        start,
        end,
        partial: resolved.is_some(),
        id,
    })
}

/// Reads `[start, end)` with incremental BLOB I/O, so only the needed pages are loaded.
pub(crate) fn read_range(
    conn: &Connection,
    object_id: i64,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, Error> {
    let mut out = vec![0; (end - start) as usize];
    if out.is_empty() {
        return Ok(out);
    }
    let mut stmt = conn.prepare(
        "SELECT rowid, offset, size FROM chunks
         WHERE object_id = ?1 AND offset < ?3 AND offset + size > ?2 ORDER BY seq",
    )?;
    let chunks = stmt
        .query_map(params![object_id, start as i64, end as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, u64>(1)?,
                r.get::<_, u64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut filled = 0u64;
    for (rowid, offset, size) in chunks {
        let from = start.max(offset);
        let to = end.min(offset + size);
        if from != start + filled {
            break;
        }
        let blob = conn.blob_open(rusqlite::DatabaseName::Main, "chunks", "data", rowid, true)?;
        let dst = &mut out[(from - start) as usize..(to - start) as usize];
        blob.read_at_exact(dst, (from - offset) as usize)?;
        filled += to - from;
    }
    if filled != end - start {
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
