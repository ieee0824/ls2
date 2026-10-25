//! SQLite schema and explicit, versioned migrations (`PRAGMA user_version`).
use crate::Error;
use rusqlite::Connection;

/// Version written by this build. Newer databases are refused.
pub(crate) const SCHEMA_VERSION: i64 = 4;

/// Latest schema, used for new databases.
const CREATE: &str = "
CREATE TABLE buckets (
    name TEXT PRIMARY KEY,
    region TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    versioning TEXT,
    tags TEXT,
    cors TEXT,
    lifecycle TEXT,
    notification TEXT);
CREATE TABLE objects (
    id INTEGER PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    key BLOB NOT NULL,
    version_id TEXT NOT NULL,
    is_latest INTEGER NOT NULL,
    delete_marker INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    last_modified_ms INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    checksum TEXT,
    tags TEXT,
    UNIQUE (bucket, key, version_id));
CREATE INDEX objects_versions ON objects (bucket, key, id);
CREATE INDEX objects_current ON objects (bucket, key) WHERE is_latest = 1 AND delete_marker = 0;
CREATE TABLE chunks (
    object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (object_id, seq));
CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    key BLOB NOT NULL,
    initiated_at_ms INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    checksum_algorithm TEXT,
    tags TEXT);
CREATE INDEX uploads_by_key ON uploads (bucket, key, initiated_at_ms, id);
CREATE TABLE parts (
    upload_id TEXT NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    last_modified_ms INTEGER NOT NULL,
    checksum TEXT,
    data BLOB NOT NULL,
    PRIMARY KEY (upload_id, part_number));
CREATE TABLE notifications (
    id INTEGER PRIMARY KEY,
    queue_arn TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_ms INTEGER NOT NULL,
    last_error TEXT,
    failed INTEGER NOT NULL DEFAULT 0);
CREATE INDEX notifications_due ON notifications (failed, next_attempt_ms, id);
CREATE INDEX notifications_queue ON notifications (queue_arn, id) WHERE failed = 0;
CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
";

/// 1 → 2: object versions. `UNIQUE(bucket, key)` becomes `UNIQUE(bucket, key, version_id)`,
/// which SQLite can only do by rebuilding the table. Existing objects become the `null`
/// version, as in S3 when versioning is first enabled.
const V2: &str = "
ALTER TABLE buckets ADD COLUMN versioning TEXT;
CREATE TABLE objects_v2 (
    id INTEGER PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    key BLOB NOT NULL,
    version_id TEXT NOT NULL,
    is_latest INTEGER NOT NULL,
    delete_marker INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    last_modified_ms INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    checksum TEXT,
    UNIQUE (bucket, key, version_id));
INSERT INTO objects_v2 (id, bucket, key, version_id, is_latest, delete_marker,
                        size, etag, last_modified_ms, metadata, checksum)
    SELECT id, bucket, key, 'null', 1, 0, size, etag, last_modified_ms, metadata, checksum
    FROM objects;
DROP TABLE objects;
ALTER TABLE objects_v2 RENAME TO objects;
CREATE INDEX objects_versions ON objects (bucket, key, id);
CREATE INDEX objects_current ON objects (bucket, key) WHERE is_latest = 1 AND delete_marker = 0;
";

/// 2 → 3: object, upload and bucket tags; bucket CORS configuration.
const V3: &str = "
ALTER TABLE objects ADD COLUMN tags TEXT;
ALTER TABLE uploads ADD COLUMN tags TEXT;
ALTER TABLE buckets ADD COLUMN tags TEXT;
ALTER TABLE buckets ADD COLUMN cors TEXT;
";

/// 3 → 4: lifecycle and notification configuration, the notification outbox and
/// the event sequencer.
const V4: &str = "
ALTER TABLE buckets ADD COLUMN lifecycle TEXT;
ALTER TABLE buckets ADD COLUMN notification TEXT;
CREATE TABLE notifications (
    id INTEGER PRIMARY KEY,
    queue_arn TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_ms INTEGER NOT NULL,
    last_error TEXT,
    failed INTEGER NOT NULL DEFAULT 0);
CREATE INDEX notifications_due ON notifications (failed, next_attempt_ms, id);
CREATE INDEX notifications_queue ON notifications (queue_arn, id) WHERE failed = 0;
CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
";

/// `(target version, SQL)` applied in order to reach [`SCHEMA_VERSION`].
const MIGRATIONS: &[(i64, &str)] = &[(2, V2), (3, V3), (4, V4)];

pub(crate) fn init(conn: &mut Connection) -> Result<(), Error> {
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.execute_batch("PRAGMA journal_mode=WAL")?;
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(Error::InvalidRequest(format!(
            "Database schema version {version} is newer than this ls2 ({SCHEMA_VERSION})"
        )));
    }
    if version == 0 {
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch(CREATE)?;
        tx.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION}"))?;
        tx.commit()?;
    } else {
        // Table rebuilds need foreign keys off, or dropping `objects` would cascade to chunks.
        conn.execute_batch("PRAGMA foreign_keys=OFF")?;
        for (target, sql) in MIGRATIONS.iter().filter(|(v, _)| *v > version) {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute_batch(sql)?;
            let broken: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |r| r.get(0),
            )?;
            if broken {
                return Err(Error::InvalidRequest(format!(
                    "Migration to schema version {target} left dangling references"
                )));
            }
            tx.execute_batch(&format!("PRAGMA user_version={target}"))?;
            tx.commit()?;
        }
    }
    conn.execute_batch("PRAGMA foreign_keys=ON")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Schema written by ls2 0.1 (user_version 1).
    const V1: &str = "
    CREATE TABLE buckets (name TEXT PRIMARY KEY, region TEXT NOT NULL, created_at_ms INTEGER NOT NULL);
    CREATE TABLE objects (
        id INTEGER PRIMARY KEY,
        bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
        key BLOB NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL,
        last_modified_ms INTEGER NOT NULL, metadata TEXT NOT NULL, checksum TEXT,
        UNIQUE (bucket, key));
    CREATE TABLE chunks (
        object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
        seq INTEGER NOT NULL, offset INTEGER NOT NULL, size INTEGER NOT NULL, data BLOB NOT NULL,
        PRIMARY KEY (object_id, seq));
    CREATE TABLE uploads (
        id TEXT PRIMARY KEY,
        bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
        key BLOB NOT NULL, initiated_at_ms INTEGER NOT NULL, metadata TEXT NOT NULL,
        checksum_algorithm TEXT);
    CREATE INDEX uploads_by_key ON uploads (bucket, key, initiated_at_ms, id);
    CREATE TABLE parts (
        upload_id TEXT NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
        part_number INTEGER NOT NULL, size INTEGER NOT NULL, etag TEXT NOT NULL,
        last_modified_ms INTEGER NOT NULL, checksum TEXT, data BLOB NOT NULL,
        PRIMARY KEY (upload_id, part_number));
    INSERT INTO buckets VALUES ('bkt', 'us-east-1', 1);
    INSERT INTO objects VALUES (7, 'bkt', CAST('k' AS BLOB), 5, 'etag', 2, '{}', NULL);
    INSERT INTO chunks VALUES (7, 0, 0, 5, CAST('hello' AS BLOB));
    PRAGMA user_version=1;
    ";

    #[test]
    fn migrates_v1_objects_to_null_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.sqlite");
        Connection::open(&path).unwrap().execute_batch(V1).unwrap();

        let s = crate::Ls2::open(&path).unwrap();
        let out = s
            .get_object("bkt", "k", None, &crate::ReadCondition::default())
            .unwrap();
        assert_eq!(out.data, b"hello");
        assert_eq!(out.info.version_id, crate::NULL_VERSION);
        assert_eq!(s.bucket("bkt").unwrap().versioning, None);
        drop(s);
        // Reopening an up-to-date database is a no-op.
        crate::Ls2::open(&path).unwrap();
    }

    #[test]
    fn refuses_newer_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.sqlite");
        Connection::open(&path)
            .unwrap()
            .execute_batch(&format!("PRAGMA user_version={}", SCHEMA_VERSION + 1))
            .unwrap();
        assert!(matches!(
            crate::Ls2::open(&path),
            Err(Error::InvalidRequest(_))
        ));
    }
}
