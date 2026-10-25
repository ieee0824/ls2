//! Event notifications to SQS queues (delivered to lqs by the HTTP server).
//!
//! Events are written to the `notifications` outbox in the same transaction as the
//! change that caused them, so they are never lost and never sent for a write that
//! rolled back. Delivery happens later; a failed delivery never undoes the write.
use crate::{Error, Ls2, from_json, require_bucket, to_json};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

/// Deliveries are retried with exponential backoff up to this many attempts.
pub const MAX_DELIVERY_ATTEMPTS: u32 = 10;
const MAX_BACKOFF_MS: i64 = 60_000;

/// Event types ls2 can produce.
const EVENTS: &[&str] = &[
    "s3:ObjectCreated:*",
    "s3:ObjectCreated:Put",
    "s3:ObjectCreated:Post",
    "s3:ObjectCreated:Copy",
    "s3:ObjectCreated:CompleteMultipartUpload",
    "s3:ObjectRemoved:*",
    "s3:ObjectRemoved:Delete",
    "s3:ObjectRemoved:DeleteMarkerCreated",
    "s3:ObjectTagging:*",
    "s3:ObjectTagging:Put",
    "s3:ObjectTagging:Delete",
    "s3:LifecycleExpiration:*",
    "s3:LifecycleExpiration:Delete",
    "s3:LifecycleExpiration:DeleteMarkerCreated",
];

/// Valid in S3 but never produced by ls2 (no restore, replication, ACLs or transitions).
const UNSUPPORTED_EVENTS: &[&str] = &[
    "s3:ObjectRestore:",
    "s3:Replication:",
    "s3:ReducedRedundancyLostObject",
    "s3:IntelligentTiering",
    "s3:LifecycleTransition",
    "s3:ObjectAcl:Put",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfiguration {
    /// Generated when the configuration does not name one, as in S3.
    pub id: Option<String>,
    /// `arn:aws:sqs:<region>:<account>:<queue name>`
    pub queue_arn: String,
    pub events: Vec<String>,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

impl QueueConfiguration {
    fn matches(&self, event: &str, key: &str) -> bool {
        let full = format!("s3:{event}");
        self.events.iter().any(|e| match e.strip_suffix('*') {
            Some(category) => full.starts_with(category),
            None => *e == full,
        }) && self.prefix.as_deref().is_none_or(|p| key.starts_with(p))
            && self.suffix.as_deref().is_none_or(|s| key.ends_with(s))
    }

    /// The queue name from the ARN.
    pub fn queue_name(&self) -> &str {
        self.queue_arn.rsplit(':').next().unwrap_or_default()
    }
}

pub(crate) fn validate(configs: &[QueueConfiguration]) -> Result<(), Error> {
    for c in configs {
        let parts: Vec<&str> = c.queue_arn.split(':').collect();
        if parts.len() != 6 || parts[..3] != ["arn", "aws", "sqs"] || parts[5].is_empty() {
            return Err(Error::InvalidArgument(format!(
                "Invalid queue ARN: {}",
                c.queue_arn
            )));
        }
        if parts[5].ends_with(".fifo") {
            return Err(Error::InvalidArgument(
                "FIFO SQS queues are not supported as event notification destinations".into(),
            ));
        }
        if c.events.is_empty() {
            return Err(Error::InvalidArgument(
                "A notification configuration needs at least one event".into(),
            ));
        }
        for e in &c.events {
            if UNSUPPORTED_EVENTS.iter().any(|u| e.starts_with(u)) {
                return Err(Error::NotImplemented(format!(
                    "Event {e} is never produced by ls2"
                )));
            }
            if !EVENTS.contains(&e.as_str()) {
                return Err(Error::InvalidArgument(format!(
                    "The event is not supported for notifications: {e}"
                )));
            }
        }
    }
    // S3 rejects configurations that could deliver the same event twice.
    for (i, a) in configs.iter().enumerate() {
        for b in &configs[i + 1..] {
            let shared = a.events.iter().any(|x| {
                b.events.iter().any(|y| {
                    let (x, y) = (x.trim_end_matches('*'), y.trim_end_matches('*'));
                    x.starts_with(y) || y.starts_with(x)
                })
            });
            let overlap = |p: &Option<String>, q: &Option<String>, f: fn(&str, &str) -> bool| {
                let (p, q) = (p.as_deref().unwrap_or(""), q.as_deref().unwrap_or(""));
                f(p, q) || f(q, p)
            };
            if shared
                && overlap(&a.prefix, &b.prefix, |p, q| p.starts_with(q))
                && overlap(&a.suffix, &b.suffix, |p, q| p.ends_with(q))
            {
                return Err(Error::InvalidArgument(
                    "Configurations overlap. Configurations on the same bucket cannot share a common event type.".into(),
                ));
            }
        }
    }
    Ok(())
}

/// Who caused an event (`userIdentity.principalId`).
#[derive(Debug, Clone, Copy)]
pub(crate) enum Principal {
    Request,
    Lifecycle,
}

/// The object part of an event record.
pub(crate) struct EventObject<'a> {
    pub key: &'a str,
    pub size: Option<u64>,
    pub etag: Option<&'a str>,
    pub version_id: Option<&'a str>,
}

impl<'a> EventObject<'a> {
    pub fn created(info: &'a crate::ObjectInfo) -> Self {
        Self {
            key: &info.key,
            size: Some(info.size),
            etag: Some(&info.etag),
            version_id: Some(&info.version_id),
        }
    }
    pub fn removed(key: &'a str, version_id: Option<&'a str>) -> Self {
        Self {
            key,
            size: None,
            etag: None,
            version_id,
        }
    }
}

/// Keys in events are URL-encoded with `+` for spaces, as S3 does.
const EVENT_KEY: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'*')
    .remove(b'/')
    .remove(b' ');

fn load_configs(conn: &Connection, bucket: &str) -> Result<Vec<QueueConfiguration>, Error> {
    let raw: Option<Option<String>> = conn
        .query_row(
            "SELECT notification FROM buckets WHERE name=?1",
            [bucket],
            |r| r.get(0),
        )
        .optional()?;
    Ok(raw
        .flatten()
        .map(|c| from_json(&c, 0))
        .transpose()?
        .unwrap_or_default())
}

fn next_sequencer(conn: &Connection) -> Result<String, Error> {
    let value: i64 = conn.query_row(
        "INSERT INTO counters(name, value) VALUES ('sequencer', 1)
         ON CONFLICT(name) DO UPDATE SET value = value + 1 RETURNING value",
        [],
        |r| r.get(0),
    )?;
    Ok(format!("{value:018X}"))
}

/// New events are due at once: `now_ms` may be an injected time (a lifecycle run
/// "in the future"), which must not delay delivery.
fn enqueue(conn: &Connection, queue_arn: &str, body: &str, now_ms: i64) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO notifications(queue_arn, body, created_at_ms, next_attempt_ms)
         VALUES (?1, ?2, ?3, 0)",
        params![queue_arn, body, now_ms],
    )?;
    Ok(())
}

/// Records `event` (e.g. `ObjectCreated:Put`) for every matching queue configuration.
pub(crate) fn emit(
    conn: &Connection,
    bucket: &str,
    event: &str,
    object: &EventObject<'_>,
    principal: Principal,
    now_ms: i64,
) -> Result<(), Error> {
    let configs = load_configs(conn, bucket)?;
    let matching: Vec<&QueueConfiguration> = configs
        .iter()
        .filter(|c| c.matches(event, object.key))
        .collect();
    if matching.is_empty() {
        return Ok(());
    }
    let (region, versioning): (String, Option<String>) = conn.query_row(
        "SELECT region, versioning FROM buckets WHERE name=?1",
        [bucket],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let mut s3_object = serde_json::json!({
        "key": utf8_percent_encode(object.key, EVENT_KEY).to_string().replace(' ', "+"),
        "sequencer": next_sequencer(conn)?,
    });
    if let Some(size) = object.size {
        s3_object["size"] = size.into();
    }
    if let Some(etag) = object.etag {
        s3_object["eTag"] = etag.into();
    }
    if versioning.is_some()
        && let Some(v) = object.version_id
    {
        s3_object["versionId"] = v.into();
    }
    let principal = match principal {
        Principal::Request => crate::server::OWNER_ID,
        Principal::Lifecycle => "s3.amazonaws.com",
    };
    let request_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_uppercase();
    for c in matching {
        let record = serde_json::json!({
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": region,
            "eventTime": crate::server::iso8601(now_ms),
            "eventName": event,
            "userIdentity": { "principalId": principal },
            "requestParameters": { "sourceIPAddress": "127.0.0.1" },
            "responseElements": { "x-amz-request-id": request_id, "x-amz-id-2": request_id },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": c.id,
                "bucket": {
                    "name": bucket,
                    "ownerIdentity": { "principalId": crate::server::OWNER_ID },
                    "arn": format!("arn:aws:s3:::{bucket}"),
                },
                "object": s3_object,
            },
        });
        let body = serde_json::json!({ "Records": [record] }).to_string();
        enqueue(conn, &c.queue_arn, &body, now_ms)?;
    }
    Ok(())
}

/// One event waiting to be sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNotification {
    pub id: i64,
    pub queue_arn: String,
    pub body: String,
    pub attempts: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationStats {
    pub pending: u64,
    /// Gave up after [`MAX_DELIVERY_ATTEMPTS`]; kept for inspection.
    pub failed: u64,
    pub last_error: Option<String>,
}

impl Ls2 {
    /// PutBucketNotificationConfiguration. An empty list removes the configuration.
    /// Each queue receives an `s3:TestEvent`, as S3 sends one to validate destinations.
    pub fn put_bucket_notification(
        &mut self,
        bucket: &str,
        mut configs: Vec<QueueConfiguration>,
        now_ms: i64,
    ) -> Result<Vec<QueueConfiguration>, Error> {
        validate(&configs)?;
        for c in &mut configs {
            c.id.get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
        }
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        tx.execute(
            "UPDATE buckets SET notification=?2 WHERE name=?1",
            params![bucket, (!configs.is_empty()).then(|| to_json(&configs))],
        )?;
        let mut queues: Vec<&str> = configs.iter().map(|c| c.queue_arn.as_str()).collect();
        queues.sort_unstable();
        queues.dedup();
        for queue in queues {
            let id = uuid::Uuid::new_v4().simple().to_string()[..16].to_uppercase();
            let body = serde_json::json!({
                "Service": "Amazon S3",
                "Event": "s3:TestEvent",
                "Time": crate::server::iso8601(now_ms),
                "Bucket": bucket,
                "RequestId": id,
                "HostId": id,
            });
            enqueue(&tx, queue, &body.to_string(), now_ms)?;
        }
        tx.commit()?;
        Ok(configs)
    }

    pub fn get_bucket_notification(&self, bucket: &str) -> Result<Vec<QueueConfiguration>, Error> {
        require_bucket(&self.conn, bucket)?;
        load_configs(&self.conn, bucket)
    }

    /// Events due for delivery at `now_ms`, oldest first. An event waits while an
    /// earlier one for the same queue is waiting for a retry, so each queue receives
    /// events in order (until an event is given up on).
    pub fn due_notifications(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<PendingNotification>, Error> {
        let rows = self
            .conn
            .prepare(
                "SELECT id, queue_arn, body, attempts FROM notifications n
                 WHERE failed = 0 AND next_attempt_ms <= ?1
                   AND NOT EXISTS (
                     SELECT 1 FROM notifications p
                     WHERE p.queue_arn = n.queue_arn AND p.failed = 0 AND p.id < n.id
                       AND p.next_attempt_ms > ?1)
                 ORDER BY id LIMIT ?2",
            )?
            .query_map(params![now_ms, limit as i64], |r| {
                Ok(PendingNotification {
                    id: r.get(0)?,
                    queue_arn: r.get(1)?,
                    body: r.get(2)?,
                    attempts: r.get(3)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// Removes a delivered event.
    pub fn notification_delivered(&mut self, id: i64) -> Result<(), Error> {
        self.conn
            .execute("DELETE FROM notifications WHERE id=?1", [id])?;
        Ok(())
    }

    /// Schedules a retry with exponential backoff, or gives up after the last attempt.
    pub fn notification_failed(&mut self, id: i64, now_ms: i64, error: &str) -> Result<(), Error> {
        self.conn.execute(
            "UPDATE notifications SET
                attempts = attempts + 1,
                last_error = ?3,
                failed = (attempts + 1 >= ?4),
                next_attempt_ms = ?2 + min(?5, 500 * (1 << min(attempts, 20)))
             WHERE id=?1",
            params![id, now_ms, error, MAX_DELIVERY_ATTEMPTS, MAX_BACKOFF_MS],
        )?;
        Ok(())
    }

    pub fn notification_stats(&self) -> Result<NotificationStats, Error> {
        Ok(self.conn.query_row(
            "SELECT
                (SELECT count(*) FROM notifications WHERE failed = 0),
                (SELECT count(*) FROM notifications WHERE failed = 1),
                (SELECT last_error FROM notifications WHERE last_error IS NOT NULL
                 ORDER BY next_attempt_ms DESC LIMIT 1)",
            [],
            |r| {
                Ok(NotificationStats {
                    pending: r.get(0)?,
                    failed: r.get(1)?,
                    last_error: r.get(2)?,
                })
            },
        )?)
    }
}
