//! Bucket lifecycle: expiration of current and noncurrent versions, expired delete
//! markers, and incomplete multipart uploads. The caller supplies the time, so a run
//! at any instant is deterministic.
use crate::notification::{EventObject, Principal, emit};
use crate::{
    Error, Ls2, OBJECT_COLUMNS, ObjectInfo, Row, Tags, Versioning, bucket_versioning, decode_row,
    delete_in, from_json, prefix_upper_bound, to_json,
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

pub const MAX_LIFECYCLE_RULES: usize = 1000;
pub(crate) const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleFilter {
    pub prefix: String,
    /// Every tag must be present with the same value.
    pub tags: Tags,
    pub size_greater_than: Option<u64>,
    pub size_less_than: Option<u64>,
}

impl LifecycleFilter {
    fn matches(&self, info: &ObjectInfo, delete_marker: bool) -> bool {
        if !info.key.starts_with(&self.prefix) {
            return false;
        }
        if delete_marker {
            // Delete markers carry no tags or size.
            return self.tags.is_empty()
                && self.size_greater_than.is_none()
                && self.size_less_than.is_none();
        }
        self.tags.iter().all(|(k, v)| info.tags.get(k) == Some(v))
            && self.size_greater_than.is_none_or(|n| info.size > n)
            && self.size_less_than.is_none_or(|n| info.size < n)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expiration {
    pub days: Option<u32>,
    /// Midnight UTC, Unix milliseconds.
    pub date_ms: Option<i64>,
    pub expired_object_delete_marker: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoncurrentExpiration {
    pub noncurrent_days: u32,
    /// Keep this many newest noncurrent versions regardless of age.
    pub newer_noncurrent_versions: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: Option<String>,
    pub enabled: bool,
    pub filter: LifecycleFilter,
    pub expiration: Option<Expiration>,
    pub noncurrent_expiration: Option<NoncurrentExpiration>,
    pub abort_incomplete_upload_days: Option<u32>,
}

/// What one lifecycle run did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleRun {
    /// Current versions deleted (unversioned buckets).
    pub expired: u64,
    /// Current versions hidden behind a new delete marker (versioned buckets).
    pub delete_markers_created: u64,
    pub noncurrent_expired: u64,
    pub delete_markers_removed: u64,
    pub uploads_aborted: u64,
}

/// S3 rounds lifecycle times up to the next midnight UTC.
pub(crate) fn ceil_day(ms: i64) -> i64 {
    (ms + DAY_MS - 1).div_euclid(DAY_MS) * DAY_MS
}

fn after_days(ms: i64, days: u32) -> i64 {
    ceil_day(ms + i64::from(days) * DAY_MS)
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fff](Z|+00:00)` into Unix milliseconds.
pub fn parse_iso8601(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z').or_else(|| s.strip_suffix("+00:00"))?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-');
    let (y, m, day): (i64, i64, i64) = (
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
    );
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut t = hms.split(':');
    let (hh, mm, ss): (i64, i64, i64) = (
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
    );
    if d.next().is_some()
        || t.next().is_some()
        || !(1..=12).contains(&m)
        || !(1..=31).contains(&day)
        || hh > 23
        || mm > 59
        || ss > 60
        || !frac.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let ms: i64 = format!("{frac:0<3}")[..3].parse().ok()?;
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + hh * 3600 + mm * 60 + ss) * 1000) + ms)
}

fn validate(rules: &[LifecycleRule]) -> Result<(), Error> {
    if rules.is_empty() || rules.len() > MAX_LIFECYCLE_RULES {
        return Err(Error::InvalidArgument(format!(
            "A lifecycle configuration must have between 1 and {MAX_LIFECYCLE_RULES} rules"
        )));
    }
    let mut ids = std::collections::BTreeSet::new();
    for rule in rules {
        if let Some(id) = &rule.id {
            if id.len() > 255 {
                return Err(Error::InvalidArgument(
                    "ID length should not exceed allowed limit of 255".into(),
                ));
            }
            if !ids.insert(id) {
                return Err(Error::InvalidArgument(
                    "Rule ID must be unique. Found same ID for more than one rule".into(),
                ));
            }
        }
        if rule.expiration.is_none()
            && rule.noncurrent_expiration.is_none()
            && rule.abort_incomplete_upload_days.is_none()
        {
            return Err(Error::InvalidRequest(
                "At least one action needs to be specified in a rule".into(),
            ));
        }
        if let Some(e) = &rule.expiration {
            let set = [
                e.days.is_some(),
                e.date_ms.is_some(),
                e.expired_object_delete_marker,
            ]
            .iter()
            .filter(|b| **b)
            .count();
            if set != 1 {
                return Err(Error::InvalidRequest(
                    "Expiration must specify exactly one of Days, Date or ExpiredObjectDeleteMarker"
                        .into(),
                ));
            }
            if e.days == Some(0) {
                return Err(Error::InvalidArgument(
                    "'Days' for Expiration action must be a positive integer".into(),
                ));
            }
            if e.date_ms.is_some_and(|d| d.rem_euclid(DAY_MS) != 0) {
                return Err(Error::InvalidArgument(
                    "'Date' must be at midnight GMT".into(),
                ));
            }
            if e.expired_object_delete_marker && !rule.filter.tags.is_empty() {
                return Err(Error::InvalidRequest(
                    "ExpiredObjectDeleteMarker cannot be specified with object tags".into(),
                ));
            }
        }
        if let Some(n) = &rule.noncurrent_expiration {
            if n.noncurrent_days == 0 {
                return Err(Error::InvalidArgument(
                    "'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer".into(),
                ));
            }
            if n.newer_noncurrent_versions
                .is_some_and(|v| !(1..=100).contains(&v))
            {
                return Err(Error::InvalidArgument(
                    "'NewerNoncurrentVersions' must be between 1 and 100".into(),
                ));
            }
        }
        if let Some(days) = rule.abort_incomplete_upload_days {
            if days == 0 {
                return Err(Error::InvalidArgument(
                    "'DaysAfterInitiation' for AbortIncompleteMultipartUpload action must be a positive integer".into(),
                ));
            }
            if !rule.filter.tags.is_empty() {
                return Err(Error::InvalidRequest(
                    "AbortIncompleteMultipartUpload cannot be specified with Tags.".into(),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn load_rules(conn: &Connection, bucket: &str) -> Result<Vec<LifecycleRule>, Error> {
    let rules: Option<String> = conn.query_row(
        "SELECT lifecycle FROM buckets WHERE name=?1",
        [bucket],
        |r| r.get(0),
    )?;
    Ok(rules
        .map(|r| from_json(&r, 0))
        .transpose()?
        .unwrap_or_default())
}

impl Ls2 {
    pub fn put_bucket_lifecycle(
        &mut self,
        bucket: &str,
        mut rules: Vec<LifecycleRule>,
    ) -> Result<(), Error> {
        validate(&rules)?;
        // S3 names unnamed rules.
        for r in &mut rules {
            r.id.get_or_insert_with(|| uuid::Uuid::new_v4().simple().to_string());
        }
        let tx = self.write_tx()?;
        bucket_versioning(&tx, bucket)?;
        tx.execute(
            "UPDATE buckets SET lifecycle=?2 WHERE name=?1",
            params![bucket, to_json(&rules)],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// `None` when the bucket has no lifecycle configuration.
    pub fn get_bucket_lifecycle(&self, bucket: &str) -> Result<Option<Vec<LifecycleRule>>, Error> {
        bucket_versioning(&self.conn, bucket)?;
        let rules = load_rules(&self.conn, bucket)?;
        Ok((!rules.is_empty()).then_some(rules))
    }

    pub fn delete_bucket_lifecycle(&mut self, bucket: &str) -> Result<(), Error> {
        let tx = self.write_tx()?;
        bucket_versioning(&tx, bucket)?;
        tx.execute("UPDATE buckets SET lifecycle=NULL WHERE name=?1", [bucket])?;
        tx.commit()?;
        Ok(())
    }

    /// When the current version expires, and by which rule (`x-amz-expiration`).
    pub fn object_expiration(
        &self,
        info: &ObjectInfo,
    ) -> Result<Option<(i64, Option<String>)>, Error> {
        let rules = load_rules(&self.conn, &info.bucket)?;
        Ok(rules
            .iter()
            .filter(|r| r.enabled && r.filter.matches(info, false))
            .filter_map(|r| {
                let e = r.expiration.as_ref()?;
                let at = match (e.days, e.date_ms) {
                    (Some(days), _) => after_days(info.last_modified_ms, days),
                    (None, Some(date)) => date,
                    _ => return None,
                };
                Some((at, r.id.clone()))
            })
            .min_by_key(|(at, _)| *at))
    }

    /// Applies every enabled lifecycle rule as of `now_ms`.
    pub fn apply_lifecycle(&mut self, now_ms: i64) -> Result<LifecycleRun, Error> {
        let buckets: Vec<String> = self
            .conn
            .prepare("SELECT name FROM buckets WHERE lifecycle IS NOT NULL ORDER BY name")?
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        let mut run = LifecycleRun::default();
        for bucket in buckets {
            let tx = self.write_tx()?;
            let versioning = bucket_versioning(&tx, &bucket)?;
            for rule in load_rules(&tx, &bucket)?.iter().filter(|r| r.enabled) {
                apply_rule(&tx, &bucket, versioning, rule, now_ms, &mut run)?;
            }
            tx.commit()?;
        }
        Ok(run)
    }
}

/// Every version under `prefix`, keys ascending and newest first.
fn versions_under(conn: &Connection, bucket: &str, prefix: &str) -> Result<Vec<Row>, Error> {
    let upper = prefix_upper_bound(prefix.as_bytes());
    let rows = conn
        .prepare(&format!(
            "SELECT {OBJECT_COLUMNS} FROM objects
             WHERE bucket = ?1 AND key >= ?2 AND (?3 IS NULL OR key < ?3)
             ORDER BY key, id DESC"
        ))?
        .query_map(params![bucket, prefix.as_bytes(), upper], |r| {
            decode_row(r, bucket)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn apply_rule(
    conn: &Connection,
    bucket: &str,
    versioning: Option<Versioning>,
    rule: &LifecycleRule,
    now_ms: i64,
    run: &mut LifecycleRun,
) -> Result<(), Error> {
    let filter = &rule.filter;
    let rows = versions_under(conn, bucket, &filter.prefix)?;
    // Group into keys; each group is newest first.
    let mut groups: Vec<&[Row]> = Vec::new();
    let mut start = 0;
    for i in 1..=rows.len() {
        if i == rows.len() || rows[i].info.key != rows[start].info.key {
            groups.push(&rows[start..i]);
            start = i;
        }
    }
    let event = |object: EventObject<'_>, name: &str| {
        emit(conn, bucket, name, &object, Principal::Lifecycle, now_ms)
    };
    for versions in groups {
        let latest = &versions[0];
        let key = latest.info.key.as_str();
        if let Some(e) = &rule.expiration {
            let due = |r: &Row| match (e.days, e.date_ms) {
                (Some(days), _) => after_days(r.info.last_modified_ms, days) <= now_ms,
                (None, Some(date)) => date <= now_ms,
                _ => false,
            };
            if !latest.delete_marker && filter.matches(&latest.info, false) && due(latest) {
                let (outcome, _) = delete_in(conn, bucket, key, None, versioning, now_ms)?;
                if outcome.delete_marker {
                    run.delete_markers_created += 1;
                    event(
                        EventObject::removed(key, outcome.version_id.as_deref()),
                        "LifecycleExpiration:DeleteMarkerCreated",
                    )?;
                } else {
                    run.expired += 1;
                    event(
                        EventObject::removed(key, None),
                        "LifecycleExpiration:Delete",
                    )?;
                }
                // The new delete marker is younger than any rule; nothing else for this key.
                continue;
            }
            if e.expired_object_delete_marker
                && versions.len() == 1
                && latest.delete_marker
                && filter.matches(&latest.info, true)
            {
                let version = latest.info.version_id.as_str();
                delete_in(conn, bucket, key, Some(version), versioning, now_ms)?;
                run.delete_markers_removed += 1;
                event(
                    EventObject::removed(key, Some(version)),
                    "LifecycleExpiration:Delete",
                )?;
                continue;
            }
        }
        if let Some(n) = &rule.noncurrent_expiration {
            let keep = n.newer_noncurrent_versions.unwrap_or(0) as usize;
            // A version became noncurrent when its successor was written.
            for (i, pair) in versions.windows(2).enumerate() {
                let (newer, version) = (&pair[0], &pair[1]);
                if i < keep || !filter.matches(&version.info, version.delete_marker) {
                    continue;
                }
                if after_days(newer.info.last_modified_ms, n.noncurrent_days) <= now_ms {
                    let id = version.info.version_id.as_str();
                    delete_in(conn, bucket, key, Some(id), versioning, now_ms)?;
                    run.noncurrent_expired += 1;
                    event(
                        EventObject::removed(key, Some(id)),
                        "LifecycleExpiration:Delete",
                    )?;
                }
            }
        }
    }
    if let Some(days) = rule.abort_incomplete_upload_days {
        let uploads: Vec<(String, Vec<u8>, i64)> = conn
            .prepare("SELECT id, key, initiated_at_ms FROM uploads WHERE bucket=?1")?
            .query_map([bucket], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<_, _>>()?;
        for (id, key, initiated) in uploads {
            if key.starts_with(filter.prefix.as_bytes()) && after_days(initiated, days) <= now_ms {
                conn.execute("DELETE FROM uploads WHERE id=?1", [&id])?;
                run.uploads_aborted += 1;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_round_trip_and_midnight_rounding() {
        let t = parse_iso8601("2014-01-15T10:30:00.000Z").unwrap();
        assert_eq!(crate::server::iso8601(t), "2014-01-15T10:30:00.000Z");
        assert_eq!(parse_iso8601("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_iso8601("2024-02-29T23:59:59.5+00:00").unwrap() % 1000,
            500
        );
        assert_eq!(parse_iso8601("2024-13-01T00:00:00Z"), None);
        assert_eq!(parse_iso8601("2024-01-01"), None);
        // S3 docs: created 2014-01-15 10:30 UTC, Days=3 → expires 2014-01-19 00:00 UTC.
        assert_eq!(
            after_days(t, 3),
            parse_iso8601("2014-01-19T00:00:00Z").unwrap()
        );
    }
}
