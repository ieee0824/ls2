//! Multipart uploads. Parts live in SQLite until completion copies them into object chunks.
use crate::checksum::{self, Checksum, ChecksumAlgorithm, ChecksumInput};
use crate::{
    ByteRange, CopySource, Error, Ls2, MAX_LIST, MAX_PART_NUMBER, MIN_PART_SIZE, Metadata,
    ObjectInfo, WriteCondition, bucket_versioning, check_write_condition, from_json, key_string,
    new_version_id, require_bucket, to_json, validate_key, write_version,
};
use md5::{Digest, Md5};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UploadInfo {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub initiated_at_ms: i64,
    pub metadata: Metadata,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartInfo {
    pub part_number: u32,
    pub size: u64,
    /// Unquoted hex MD5.
    pub etag: String,
    pub last_modified_ms: i64,
    pub checksum: Option<Checksum>,
}

/// One `<Part>` of CompleteMultipartUpload.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    pub part_number: u32,
    pub etag: String,
    pub checksum: Option<Checksum>,
}

fn decode_upload(r: &rusqlite::Row<'_>) -> rusqlite::Result<UploadInfo> {
    let algorithm: Option<String> = r.get(5)?;
    Ok(UploadInfo {
        upload_id: r.get(0)?,
        bucket: r.get(1)?,
        key: key_string(r.get(2)?, 2)?,
        initiated_at_ms: r.get(3)?,
        metadata: from_json(&r.get::<_, String>(4)?, 4)?,
        checksum_algorithm: algorithm.as_deref().and_then(ChecksumAlgorithm::parse),
    })
}

const UPLOAD_COLUMNS: &str = "id, bucket, key, initiated_at_ms, metadata, checksum_algorithm";

/// The upload must belong to the bucket and key named in the request.
fn load_upload(
    conn: &Connection,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<UploadInfo, Error> {
    require_bucket(conn, bucket)?;
    conn.query_row(
        &format!("SELECT {UPLOAD_COLUMNS} FROM uploads WHERE id=?1 AND bucket=?2 AND key=?3"),
        params![upload_id, bucket, key.as_bytes()],
        decode_upload,
    )
    .optional()?
    .ok_or(Error::NoSuchUpload)
}

fn decode_part(r: &rusqlite::Row<'_>) -> rusqlite::Result<PartInfo> {
    let checksum: Option<String> = r.get(4)?;
    Ok(PartInfo {
        part_number: r.get(0)?,
        size: r.get(1)?,
        etag: r.get(2)?,
        last_modified_ms: r.get(3)?,
        checksum: checksum.map(|c| from_json(&c, 4)).transpose()?,
    })
}

impl Ls2 {
    pub fn create_multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        metadata: Metadata,
        checksum_algorithm: Option<ChecksumAlgorithm>,
        now_ms: i64,
    ) -> Result<String, Error> {
        validate_key(key)?;
        metadata.validate()?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        tx.execute(
            "INSERT INTO uploads(id, bucket, key, initiated_at_ms, metadata, checksum_algorithm)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id,
                bucket,
                key.as_bytes(),
                now_ms,
                to_json(&metadata),
                checksum_algorithm.map(ChecksumAlgorithm::name)
            ],
        )?;
        tx.commit()?;
        Ok(id)
    }

    /// Stores (or replaces) one part. When the upload declared a checksum algorithm,
    /// the part checksum must use it and is computed if the client sent none.
    #[allow(clippy::too_many_arguments)]
    pub fn upload_part(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        checksum: ChecksumInput,
        content_md5: Option<[u8; 16]>,
        now_ms: i64,
    ) -> Result<PartInfo, Error> {
        if !(1..=MAX_PART_NUMBER).contains(&part_number) {
            return Err(Error::InvalidArgument(format!(
                "Part number must be an integer between 1 and {MAX_PART_NUMBER}, inclusive"
            )));
        }
        let md5 = checksum::verify_md5(data, content_md5)?;
        let tx = self.write_tx()?;
        let upload = load_upload(&tx, bucket, key, upload_id)?;
        if let (Some(expected), Some(actual)) = (upload.checksum_algorithm, checksum.algorithm())
            && expected != actual
        {
            return Err(Error::InvalidRequest(format!(
                "Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum Type: {}",
                expected.name().to_lowercase(),
                actual.name().to_lowercase()
            )));
        }
        let checksum = checksum.resolve(data, upload.checksum_algorithm)?;
        let part = PartInfo {
            part_number,
            size: data.len() as u64,
            etag: checksum::hex(&md5),
            last_modified_ms: now_ms,
            checksum,
        };
        tx.execute(
            "INSERT OR REPLACE INTO parts(upload_id, part_number, size, etag, last_modified_ms, checksum, data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                upload_id,
                part_number,
                part.size as i64,
                part.etag,
                now_ms,
                part.checksum.as_ref().map(to_json),
                data
            ],
        )?;
        tx.commit()?;
        Ok(part)
    }

    /// UploadPartCopy: a byte range (or all) of an existing object becomes a part.
    /// Returns the part and the version ID of the source that was copied.
    #[allow(clippy::too_many_arguments)]
    pub fn upload_part_copy(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        source: &CopySource,
        range: Option<ByteRange>,
        now_ms: i64,
    ) -> Result<(PartInfo, String), Error> {
        load_upload(&self.conn, bucket, key, upload_id)?;
        crate::resolve_copy_source(&self.conn, source)?;
        let handle = self
            .open_object(
                &source.bucket,
                &source.key,
                source.version_id.as_deref(),
                range,
                &source.condition,
            )
            .map_err(|e| match e {
                Error::NotModified => Error::PreconditionFailed,
                e => e,
            })?;
        let data = self.read_object(&handle, handle.start, (handle.end - handle.start) as usize)?;
        let part = self.upload_part(
            bucket,
            key,
            upload_id,
            part_number,
            &data,
            ChecksumInput::Default,
            None,
            now_ms,
        )?;
        Ok((part, handle.info.version_id))
    }

    /// Assembles the listed parts into an object. Parts must be ascending, match the stored
    /// ETag (and checksum when given), and all but the last must be at least 5 MiB.
    pub fn complete_multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
        condition: &WriteCondition,
        now_ms: i64,
    ) -> Result<ObjectInfo, Error> {
        if parts.is_empty() {
            return Err(Error::InvalidRequest(
                "You must specify at least one part".into(),
            ));
        }
        if parts
            .windows(2)
            .any(|w| w[0].part_number >= w[1].part_number)
        {
            return Err(Error::InvalidPartOrder);
        }
        let tx = self.write_tx()?;
        let upload = load_upload(&tx, bucket, key, upload_id)?;
        let mut stored = Vec::with_capacity(parts.len());
        {
            let mut stmt = tx.prepare(
                "SELECT part_number, size, etag, last_modified_ms, checksum FROM parts
                 WHERE upload_id=?1 AND part_number=?2",
            )?;
            for p in parts {
                let invalid = || {
                    Error::InvalidPart(
                        "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.".into(),
                    )
                };
                let part = stmt
                    .query_row(params![upload_id, p.part_number], decode_part)
                    .optional()?
                    .ok_or_else(invalid)?;
                if p.etag.trim().trim_matches('"') != part.etag {
                    return Err(invalid());
                }
                if let Some(c) = &p.checksum
                    && part.checksum.as_ref() != Some(c)
                {
                    return Err(invalid());
                }
                stored.push(part);
            }
        }
        if stored[..stored.len() - 1]
            .iter()
            .any(|p| p.size < MIN_PART_SIZE)
        {
            return Err(Error::EntityTooSmall);
        }
        check_write_condition(&tx, bucket, key, condition)?;
        let versioning = bucket_versioning(&tx, bucket)?;
        let mut digest = Md5::new();
        for part in &stored {
            digest.update(hex_decode(&part.etag));
        }
        let info = ObjectInfo {
            bucket: bucket.into(),
            key: key.into(),
            size: stored.iter().map(|p| p.size).sum(),
            etag: format!("{}-{}", checksum::hex(&digest.finalize()), stored.len()),
            last_modified_ms: now_ms,
            metadata: upload.metadata,
            checksum: None,
            version_id: new_version_id(versioning),
        };
        let id = write_version(&tx, &info, versioning)?;
        let mut offset = 0u64;
        for (seq, part) in stored.iter().enumerate() {
            if part.size > 0 {
                tx.execute(
                    "INSERT INTO chunks(object_id, seq, offset, size, data)
                     SELECT ?1, ?2, ?3, size, data FROM parts WHERE upload_id=?4 AND part_number=?5",
                    params![id, seq as i64, offset as i64, upload_id, part.part_number],
                )?;
            }
            offset += part.size;
        }
        tx.execute("DELETE FROM uploads WHERE id=?1", [upload_id])?;
        tx.commit()?;
        Ok(info)
    }

    pub fn abort_multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), Error> {
        let tx = self.write_tx()?;
        load_upload(&tx, bucket, key, upload_id)?;
        tx.execute("DELETE FROM uploads WHERE id=?1", [upload_id])?;
        tx.commit()?;
        Ok(())
    }

    /// Parts after `marker` in number order. Returns `(upload, parts, truncated)`.
    pub fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        marker: u32,
        max_parts: usize,
    ) -> Result<(UploadInfo, Vec<PartInfo>, bool), Error> {
        let tx = self.conn.unchecked_transaction()?;
        let upload = load_upload(&tx, bucket, key, upload_id)?;
        let limit = max_parts.min(MAX_LIST);
        let mut parts = tx
            .prepare(
                "SELECT part_number, size, etag, last_modified_ms, checksum FROM parts
                 WHERE upload_id=?1 AND part_number > ?2 ORDER BY part_number LIMIT ?3",
            )?
            .query_map(params![upload_id, marker, limit as i64 + 1], decode_part)?
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = parts.len() > limit;
        parts.truncate(limit);
        Ok((upload, parts, truncated))
    }

    /// In-progress uploads ordered by key, then initiation time.
    /// `upload_id_marker` is honored only together with `key_marker`.
    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: &str,
        max_uploads: usize,
    ) -> Result<(Vec<UploadInfo>, bool), Error> {
        let tx = self.conn.unchecked_transaction()?;
        require_bucket(&tx, bucket)?;
        let limit = max_uploads.min(MAX_LIST);
        let marker_upload: Option<(i64, String)> = if upload_id_marker.is_empty() {
            None
        } else {
            tx.query_row(
                "SELECT initiated_at_ms, id FROM uploads WHERE id=?1 AND bucket=?2 AND key=?3",
                params![upload_id_marker, bucket, key_marker.as_bytes()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
        };
        let (marker_time, marker_id) = marker_upload.unwrap_or((i64::MAX, String::new()));
        let mut uploads = tx
            .prepare(&format!(
                "SELECT {UPLOAD_COLUMNS} FROM uploads
                 WHERE bucket=?1 AND substr(key, 1, length(?2)) = ?2
                   AND (key > ?3 OR (key = ?3 AND (initiated_at_ms > ?4 OR (initiated_at_ms = ?4 AND id > ?5))))
                 ORDER BY key, initiated_at_ms, id LIMIT ?6"
            ))?
            .query_map(
                params![
                    bucket,
                    prefix.as_bytes(),
                    key_marker.as_bytes(),
                    marker_time,
                    marker_id,
                    limit as i64 + 1
                ],
                decode_upload,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = uploads.len() > limit;
        uploads.truncate(limit);
        Ok((uploads, truncated))
    }
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .filter_map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect()
}
