//! Object and bucket tags.
use crate::{Error, Ls2, from_json, require_bucket, resolve_object, to_json, validate_key};
use rusqlite::{OptionalExtension, params};
use std::collections::BTreeMap;

/// Tag set: unique keys, kept sorted.
pub type Tags = BTreeMap<String, String>;

pub const MAX_OBJECT_TAGS: usize = 10;
pub const MAX_BUCKET_TAGS: usize = 50;

/// Builds a tag set from `(key, value)` pairs, enforcing the S3 tag rules.
pub fn tags_from_pairs(
    pairs: impl IntoIterator<Item = (String, String)>,
    max: usize,
) -> Result<Tags, Error> {
    let mut tags = Tags::new();
    for (k, v) in pairs {
        validate_tag(&k, &v)?;
        if tags.insert(k, v).is_some() {
            return Err(Error::InvalidTag(
                "Cannot provide multiple Tags with the same key".into(),
            ));
        }
    }
    if tags.len() > max {
        return Err(Error::InvalidTag(if max == MAX_OBJECT_TAGS {
            "Object tags cannot be greater than 10".into()
        } else {
            "Bucket tag count cannot be greater than 50".into()
        }));
    }
    Ok(tags)
}

fn validate_tag(key: &str, value: &str) -> Result<(), Error> {
    let allowed = |s: &str| {
        s.chars()
            .all(|c| c.is_alphanumeric() || c == ' ' || "+-=._:/@".contains(c))
    };
    if key.is_empty() || !allowed(key) {
        return Err(Error::InvalidTag(
            "The TagKey you have provided is invalid".into(),
        ));
    }
    if key.chars().count() > 128 {
        return Err(Error::InvalidTag(
            "The TagKey you have provided is too long, max 128".into(),
        ));
    }
    if key.starts_with("aws:") {
        return Err(Error::InvalidTag(
            "Your TagKey cannot be prefixed with aws:".into(),
        ));
    }
    if !allowed(value) {
        return Err(Error::InvalidTag(
            "The TagValue you have provided is invalid".into(),
        ));
    }
    if value.chars().count() > 256 {
        return Err(Error::InvalidTag(
            "The TagValue you have provided is too long, max 256".into(),
        ));
    }
    Ok(())
}

pub(crate) fn tags_json(tags: &Tags) -> Option<String> {
    (!tags.is_empty()).then(|| to_json(tags))
}

pub(crate) fn decode_tags(s: Option<String>, col: usize) -> rusqlite::Result<Tags> {
    s.map(|s| from_json(&s, col))
        .transpose()
        .map(Option::unwrap_or_default)
}

impl Ls2 {
    /// PutObjectTagging. Returns the version ID that was tagged.
    pub fn put_object_tagging(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Tags,
    ) -> Result<String, Error> {
        validate_key(key)?;
        let tx = self.write_tx()?;
        let (id, info) = resolve_object(&tx, bucket, key, version_id)?;
        tx.execute(
            "UPDATE objects SET tags=?2 WHERE id=?1",
            params![id, tags_json(&tags)],
        )?;
        tx.commit()?;
        Ok(info.version_id)
    }

    /// GetObjectTagging: `(version ID, tags)`.
    pub fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(String, Tags), Error> {
        let (_, info) = resolve_object(&self.conn, bucket, key, version_id)?;
        Ok((info.version_id, info.tags))
    }

    /// DeleteObjectTagging. Returns the version ID whose tags were removed.
    pub fn delete_object_tagging(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<String, Error> {
        self.put_object_tagging(bucket, key, version_id, Tags::new())
    }

    pub fn put_bucket_tagging(&mut self, bucket: &str, tags: Tags) -> Result<(), Error> {
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        tx.execute(
            "UPDATE buckets SET tags=?2 WHERE name=?1",
            params![bucket, tags_json(&tags)],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// `None` when the bucket has no tag set (`NoSuchTagSet` in S3).
    pub fn get_bucket_tagging(&self, bucket: &str) -> Result<Option<Tags>, Error> {
        let tags: Option<Option<String>> = self
            .conn
            .query_row("SELECT tags FROM buckets WHERE name=?1", [bucket], |r| {
                r.get(0)
            })
            .optional()?;
        let tags = tags.ok_or(Error::NoSuchBucket)?;
        Ok(tags.map(|t| from_json(&t, 0)).transpose()?)
    }

    pub fn delete_bucket_tagging(&mut self, bucket: &str) -> Result<(), Error> {
        self.put_bucket_tagging(bucket, Tags::new())
    }
}
