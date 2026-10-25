//! ListObjectVersions: every version and delete marker, keys ascending and newest first.
use crate::{
    Error, Ls2, MAX_LIST, OBJECT_COLUMNS, ObjectInfo, Row, decode_row, find, load_version,
    prefix_upper_bound, require_bucket, validate_version_id,
};
use rusqlite::params;

#[derive(Debug, Clone, Default)]
pub struct VersionListRequest {
    pub prefix: String,
    pub delimiter: String,
    /// Start after this key (all of its versions), or after `version_id_marker` within it.
    pub key_marker: String,
    pub version_id_marker: String,
    pub max_keys: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionEntry {
    /// Size, ETag and metadata are empty for delete markers.
    pub info: ObjectInfo,
    pub is_latest: bool,
    pub delete_marker: bool,
}

#[derive(Debug, Clone, Default)]
pub struct VersionListResult {
    pub versions: Vec<VersionEntry>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Set when truncated: the last version or common prefix returned.
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

/// Scan position over `(key ASC, id DESC)`.
enum Cursor {
    /// Every version of keys `>= key`.
    From(Vec<u8>),
    /// Keys `> key`.
    After(Vec<u8>),
    /// Versions of `key` older than row `id`, then keys `> key`.
    Within(Vec<u8>, i64),
}

impl Ls2 {
    pub fn list_object_versions(
        &self,
        bucket: &str,
        req: &VersionListRequest,
    ) -> Result<VersionListResult, Error> {
        let tx = self.conn.unchecked_transaction()?;
        require_bucket(&tx, bucket)?;
        let mut out = VersionListResult::default();
        let max = req.max_keys.min(MAX_LIST);
        if max == 0 {
            return Ok(out);
        }
        let prefix = req.prefix.as_bytes();
        let marker = req.key_marker.as_bytes();
        let delimiter = req.delimiter.as_bytes();
        let upper = prefix_upper_bound(prefix);
        let mut cursor = if req.key_marker.is_empty() || marker < prefix {
            Cursor::From(prefix.to_vec())
        } else if req.version_id_marker.is_empty() {
            Cursor::After(marker.to_vec())
        } else {
            validate_version_id(&req.version_id_marker)?;
            let row = load_version(&tx, bucket, &req.key_marker, &req.version_id_marker)?
                .ok_or_else(|| Error::InvalidArgument("Invalid version id specified".into()))?;
            Cursor::Within(marker.to_vec(), row.id)
        };
        let mut stmt = tx.prepare(&format!(
            "SELECT {OBJECT_COLUMNS} FROM objects
             WHERE bucket = ?1 AND (key > ?2 OR (key = ?2 AND (?3 OR id < ?4)))
               AND (?5 IS NULL OR key < ?5)
             ORDER BY key, id DESC LIMIT 256"
        ))?;
        let mut last_prefix: Option<Vec<u8>> = None;
        'scan: loop {
            let (key, all, below) = match &cursor {
                Cursor::From(k) => (k.clone(), true, i64::MIN),
                Cursor::After(k) => (k.clone(), false, i64::MIN),
                Cursor::Within(k, id) => (k.clone(), false, *id),
            };
            let rows = stmt
                .query_map(params![bucket, key, all, below, upper], |r| {
                    decode_row(r, bucket)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let fetched = rows.len();
            let mut jumped = false;
            for Row {
                id,
                info,
                is_latest,
                delete_marker,
            } in rows
            {
                let key = info.key.as_bytes();
                let rest = &key[prefix.len()..];
                if let Some(i) = (!delimiter.is_empty())
                    .then(|| find(rest, delimiter))
                    .flatten()
                {
                    let cp = &key[..prefix.len() + i + delimiter.len()];
                    if cp > marker && last_prefix.as_deref() != Some(cp) {
                        if out.versions.len() + out.common_prefixes.len() == max {
                            out.truncated = true;
                            break 'scan;
                        }
                        out.common_prefixes
                            .push(String::from_utf8_lossy(cp).into_owned());
                        last_prefix = Some(cp.to_vec());
                        out.next_key_marker = Some(String::from_utf8_lossy(cp).into_owned());
                        out.next_version_id_marker = None;
                    }
                    match prefix_upper_bound(cp) {
                        Some(next) => cursor = Cursor::From(next),
                        None => break 'scan,
                    }
                    jumped = true;
                    break;
                }
                if out.versions.len() + out.common_prefixes.len() == max {
                    out.truncated = true;
                    break 'scan;
                }
                cursor = Cursor::Within(key.to_vec(), id);
                out.next_key_marker = Some(info.key.clone());
                out.next_version_id_marker = Some(info.version_id.clone());
                out.versions.push(VersionEntry {
                    info,
                    is_latest,
                    delete_marker,
                });
            }
            if !jumped && fetched < 256 {
                break;
            }
        }
        if !out.truncated {
            out.next_key_marker = None;
            out.next_version_id_marker = None;
        }
        Ok(out)
    }
}
