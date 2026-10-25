//! Lifecycle configuration XML and the `x-amz-expiration` header.
use super::{
    S3Error, header_value, http_date, invalid_argument, malformed_xml, not_implemented, tag,
};
use crate::{
    Expiration, LifecycleFilter, LifecycleRule, Ls2, NoncurrentExpiration, ObjectInfo, Tags,
    lifecycle::DAY_MS, parse_iso8601, tags_from_pairs,
};
use axum::http::HeaderMap;
use serde::Deserialize;
use serde::de::IgnoredAny;

/// Sent by S3 on Put/GetBucketLifecycleConfiguration; transitions are not simulated.
pub(super) const TRANSITION_MINIMUM: &str = "all_storage_classes_128K";

pub(super) fn lifecycle_xml(rules: &[LifecycleRule]) -> String {
    let mut out = String::new();
    for r in rules {
        out += "<Rule>";
        if let Some(id) = &r.id {
            out += &tag("ID", id);
        }
        out += &format!("<Filter>{}</Filter>", filter_xml(&r.filter));
        out += &tag("Status", if r.enabled { "Enabled" } else { "Disabled" });
        if let Some(e) = &r.expiration {
            out += "<Expiration>";
            if let Some(d) = e.days {
                out += &format!("<Days>{d}</Days>");
            }
            if let Some(ms) = e.date_ms {
                out += &tag("Date", &super::iso8601(ms));
            }
            if e.expired_object_delete_marker {
                out += "<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>";
            }
            out += "</Expiration>";
        }
        if let Some(n) = &r.noncurrent_expiration {
            out += &format!(
                "<NoncurrentVersionExpiration><NoncurrentDays>{}</NoncurrentDays>",
                n.noncurrent_days
            );
            if let Some(v) = n.newer_noncurrent_versions {
                out += &format!("<NewerNoncurrentVersions>{v}</NewerNoncurrentVersions>");
            }
            out += "</NoncurrentVersionExpiration>";
        }
        if let Some(d) = r.abort_incomplete_upload_days {
            out += &format!(
                "<AbortIncompleteMultipartUpload><DaysAfterInitiation>{d}</DaysAfterInitiation></AbortIncompleteMultipartUpload>"
            );
        }
        out += "</Rule>";
    }
    out
}

fn filter_xml(f: &LifecycleFilter) -> String {
    let mut parts = Vec::new();
    if !f.prefix.is_empty() {
        parts.push(tag("Prefix", &f.prefix));
    }
    for (k, v) in &f.tags {
        parts.push(format!("<Tag>{}{}</Tag>", tag("Key", k), tag("Value", v)));
    }
    if let Some(n) = f.size_greater_than {
        parts.push(format!(
            "<ObjectSizeGreaterThan>{n}</ObjectSizeGreaterThan>"
        ));
    }
    if let Some(n) = f.size_less_than {
        parts.push(format!("<ObjectSizeLessThan>{n}</ObjectSizeLessThan>"));
    }
    match parts.len() {
        0 => "<Prefix></Prefix>".into(),
        1 => parts.remove(0),
        _ => format!("<And>{}</And>", parts.concat()),
    }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct Conditions {
    prefix: Option<String>,
    #[serde(default)]
    tag: Vec<XmlTag>,
    object_size_greater_than: Option<u64>,
    object_size_less_than: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlTag {
    key: String,
    value: Option<String>,
}

impl Conditions {
    fn count(&self) -> usize {
        usize::from(self.prefix.is_some())
            + self.tag.len()
            + usize::from(self.object_size_greater_than.is_some())
            + usize::from(self.object_size_less_than.is_some())
    }
    fn into_filter(self) -> Result<LifecycleFilter, S3Error> {
        let tags: Tags = tags_from_pairs(
            self.tag
                .into_iter()
                .map(|t| (t.key, t.value.unwrap_or_default())),
            usize::MAX,
        )?;
        if let (Some(gt), Some(lt)) = (self.object_size_greater_than, self.object_size_less_than)
            && gt >= lt
        {
            return Err(invalid_argument(
                "ObjectSizeGreaterThan must be less than ObjectSizeLessThan",
            ));
        }
        Ok(LifecycleFilter {
            prefix: self.prefix.unwrap_or_default(),
            tags,
            size_greater_than: self.object_size_greater_than,
            size_less_than: self.object_size_less_than,
        })
    }
}

pub(super) fn parse_lifecycle(body: &[u8]) -> Result<Vec<LifecycleRule>, S3Error> {
    #[derive(Deserialize)]
    struct Config {
        #[serde(rename = "Rule", default)]
        rules: Vec<Rule>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase", deny_unknown_fields)]
    struct Rule {
        #[serde(rename = "ID")]
        id: Option<String>,
        filter: Option<Filter>,
        prefix: Option<String>,
        status: String,
        expiration: Option<XmlExpiration>,
        noncurrent_version_expiration: Option<XmlNoncurrent>,
        abort_incomplete_multipart_upload: Option<Abort>,
        #[serde(default)]
        transition: Vec<IgnoredAny>,
        #[serde(default)]
        noncurrent_version_transition: Vec<IgnoredAny>,
    }
    // Not `#[serde(flatten)]`: quick-xml cannot read numbers through it.
    #[derive(Deserialize, Default)]
    #[serde(rename_all = "PascalCase")]
    struct Filter {
        prefix: Option<String>,
        #[serde(default)]
        tag: Vec<XmlTag>,
        object_size_greater_than: Option<u64>,
        object_size_less_than: Option<u64>,
        and: Option<Conditions>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct XmlExpiration {
        days: Option<u32>,
        date: Option<String>,
        expired_object_delete_marker: Option<bool>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct XmlNoncurrent {
        noncurrent_days: Option<u32>,
        newer_noncurrent_versions: Option<u32>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Abort {
        days_after_initiation: Option<u32>,
    }
    let config: Config = quick_xml::de::from_reader(body).map_err(|_| malformed_xml())?;
    config
        .rules
        .into_iter()
        .map(|r| {
            if !r.transition.is_empty() || !r.noncurrent_version_transition.is_empty() {
                return Err(not_implemented(
                    "Lifecycle transitions are not supported (ls2 has one storage class)",
                ));
            }
            let enabled = match r.status.as_str() {
                "Enabled" => true,
                "Disabled" => false,
                _ => return Err(malformed_xml()),
            };
            let filter = match (r.filter, r.prefix) {
                (Some(_), Some(_)) | (None, None) => return Err(malformed_xml()),
                (None, Some(prefix)) => LifecycleFilter {
                    prefix,
                    ..Default::default()
                },
                (Some(f), None) => {
                    let single = Conditions {
                        prefix: f.prefix,
                        tag: f.tag,
                        object_size_greater_than: f.object_size_greater_than,
                        object_size_less_than: f.object_size_less_than,
                    };
                    match f.and {
                        Some(and) if single.count() == 0 => and.into_filter()?,
                        None if single.count() <= 1 => single.into_filter()?,
                        _ => return Err(malformed_xml()),
                    }
                }
            };
            let expiration = r
                .expiration
                .map(|e| {
                    let date_ms = e
                        .date
                        .map(|d| {
                            let d = d.trim();
                            parse_iso8601(d)
                                .or_else(|| parse_iso8601(&format!("{d}T00:00:00Z")))
                                .ok_or_else(|| {
                                    invalid_argument(
                                        "Invalid date. Date must be in ISO 8601 format",
                                    )
                                })
                        })
                        .transpose()?;
                    Ok::<_, S3Error>(Expiration {
                        days: e.days,
                        date_ms,
                        expired_object_delete_marker: e.expired_object_delete_marker == Some(true),
                    })
                })
                .transpose()?;
            let noncurrent_expiration = r
                .noncurrent_version_expiration
                .map(|n| {
                    Ok::<_, S3Error>(NoncurrentExpiration {
                        noncurrent_days: n.noncurrent_days.ok_or_else(malformed_xml)?,
                        newer_noncurrent_versions: n.newer_noncurrent_versions,
                    })
                })
                .transpose()?;
            let abort_incomplete_upload_days = r
                .abort_incomplete_multipart_upload
                .map(|a| a.days_after_initiation.ok_or_else(malformed_xml))
                .transpose()?;
            Ok(LifecycleRule {
                id: r.id.filter(|id| !id.is_empty()),
                enabled,
                filter,
                expiration,
                noncurrent_expiration,
                abort_incomplete_upload_days,
            })
        })
        .collect()
}

/// `x-amz-expiration` for the current version of an object covered by a rule.
pub(super) fn expiration_header(
    ls2: &Ls2,
    h: &mut HeaderMap,
    info: &ObjectInfo,
) -> Result<(), S3Error> {
    if let Some((at, rule)) = ls2.object_expiration(info)? {
        debug_assert_eq!(at.rem_euclid(DAY_MS), 0);
        let value = format!(
            "expiry-date=\"{}\", rule-id=\"{}\"",
            http_date(at),
            rule.unwrap_or_default().replace('"', "%22")
        );
        if let Ok(v) = header_value(&value) {
            h.insert("x-amz-expiration", v);
        }
    }
    Ok(())
}
