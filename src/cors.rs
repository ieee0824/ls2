//! Bucket CORS configuration and rule matching.
use crate::{Error, Ls2, from_json, require_bucket, to_json};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub const MAX_CORS_RULES: usize = 100;
const METHODS: [&str; 5] = ["GET", "PUT", "POST", "DELETE", "HEAD"];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

impl CorsRule {
    /// Whether the rule allows this origin and method, and (for preflight) every
    /// requested header. Header names compare case-insensitively.
    pub fn allows(&self, origin: &str, method: &str, request_headers: &[String]) -> bool {
        self.allowed_origins.iter().any(|o| wildcard(o, origin))
            && self.allowed_methods.iter().any(|m| m == method)
            && request_headers.iter().all(|h| {
                self.allowed_headers
                    .iter()
                    .any(|a| wildcard(&a.to_ascii_lowercase(), &h.to_ascii_lowercase()))
            })
    }
}

/// `pattern` with at most one `*`, which matches any (possibly empty) substring.
fn wildcard(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some((head, tail)) => {
            value.len() >= head.len() + tail.len()
                && value.starts_with(head)
                && value.ends_with(tail)
        }
    }
}

/// The first rule that allows the request, as S3 evaluates them in order.
pub fn find_cors_rule<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: &str,
    request_headers: &[String],
) -> Option<&'a CorsRule> {
    rules
        .iter()
        .find(|r| r.allows(origin, method, request_headers))
}

fn validate(rules: &[CorsRule]) -> Result<(), Error> {
    if rules.is_empty() || rules.len() > MAX_CORS_RULES {
        return Err(Error::InvalidArgument(format!(
            "A CORS configuration must have between 1 and {MAX_CORS_RULES} rules"
        )));
    }
    for rule in rules {
        if rule.allowed_origins.is_empty() || rule.allowed_methods.is_empty() {
            return Err(Error::InvalidRequest(
                "A CORS rule needs at least one AllowedOrigin and one AllowedMethod".into(),
            ));
        }
        if let Some(m) = rule
            .allowed_methods
            .iter()
            .find(|m| !METHODS.contains(&m.as_str()))
        {
            return Err(Error::InvalidRequest(format!(
                "Found unsupported HTTP method in CORS config. Unsupported method is {m}"
            )));
        }
        for (kind, values) in [
            ("AllowedOrigin", &rule.allowed_origins),
            ("AllowedHeader", &rule.allowed_headers),
        ] {
            if let Some(v) = values.iter().find(|v| v.matches('*').count() > 1) {
                return Err(Error::InvalidRequest(format!(
                    "{kind} \"{v}\" can not have more than one wildcard."
                )));
            }
        }
    }
    Ok(())
}

impl Ls2 {
    pub fn put_bucket_cors(&mut self, bucket: &str, rules: Vec<CorsRule>) -> Result<(), Error> {
        validate(&rules)?;
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        tx.execute(
            "UPDATE buckets SET cors=?2 WHERE name=?1",
            params![bucket, to_json(&rules)],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// `None` when the bucket has no CORS configuration.
    pub fn get_bucket_cors(&self, bucket: &str) -> Result<Option<Vec<CorsRule>>, Error> {
        let cors: Option<Option<String>> = self
            .conn
            .query_row("SELECT cors FROM buckets WHERE name=?1", [bucket], |r| {
                r.get(0)
            })
            .optional()?;
        let cors = cors.ok_or(Error::NoSuchBucket)?;
        Ok(cors.map(|c| from_json(&c, 0)).transpose()?)
    }

    pub fn delete_bucket_cors(&mut self, bucket: &str) -> Result<(), Error> {
        let tx = self.write_tx()?;
        require_bucket(&tx, bucket)?;
        tx.execute("UPDATE buckets SET cors=NULL WHERE name=?1", [bucket])?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcards_and_headers() {
        assert!(wildcard("http://*.example.com", "http://app.example.com"));
        assert!(!wildcard("http://*.example.com", "http://example.com"));
        assert!(wildcard("*", "anything"));
        let rule = CorsRule {
            allowed_origins: vec!["http://localhost:*".into()],
            allowed_methods: vec!["PUT".into()],
            allowed_headers: vec!["Content-*".into(), "x-amz-*".into()],
            ..Default::default()
        };
        let headers = |h: &[&str]| h.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(rule.allows(
            "http://localhost:3000",
            "PUT",
            &headers(&["content-type", "X-Amz-Date"])
        ));
        assert!(!rule.allows("http://localhost:3000", "PUT", &headers(&["authorization"])));
        assert!(!rule.allows("http://localhost:3000", "GET", &[]));
        assert!(!rule.allows("https://evil.test", "PUT", &[]));
    }
}
