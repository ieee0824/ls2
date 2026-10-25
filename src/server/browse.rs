//! `/_ls2/*`: health, status, and a read-only HTML browser for buckets and objects.
//! Pages are rendered on the server and every stored value is HTML-escaped.
use super::{Shared, URL_ENCODE, esc, iso8601, lock, object_body};
use crate::{Error, ListRequest, ObjectInfo, ReadCondition, VersionListRequest};
use axum::{
    Json,
    body::Body,
    http::{HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use percent_encoding::{percent_decode_str, utf8_percent_encode};

/// Keys and folders shown per page.
const PAGE_SIZE: usize = 200;
/// Bytes read for the text preview on the object page.
const PREVIEW_BYTES: usize = 64 * 1024;
const PAGE_CSP: &str = "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

pub(super) fn handle(state: &Shared, method: &Method, uri: &Uri) -> Response {
    if *method != Method::GET && *method != Method::HEAD {
        return (StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response();
    }
    let query: Vec<(String, String)> =
        serde_urlencoded::from_str(uri.query().unwrap_or_default()).unwrap_or_default();
    let param = |name: &str| {
        query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    };
    let path = uri.path().trim_start_matches("/_ls2");
    let segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
        .collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let result = match segments.as_slice() {
        ["health"] => Ok("ok".into_response()),
        ["status"] => status(state),
        ["browse"] => buckets_page(state),
        ["browse", bucket] => match param("key") {
            Some(key) => object_page(state, bucket, key, param("versionId")),
            None => folder_page(
                state,
                bucket,
                param("prefix").unwrap_or_default(),
                param("after").unwrap_or_default(),
            ),
        },
        ["download", bucket] => download(
            state,
            bucket,
            param("key").unwrap_or_default(),
            param("versionId"),
        ),
        _ => Err(Page::error(StatusCode::NOT_FOUND, "Not found")),
    };
    let mut response = result.unwrap_or_else(Page::into_response);
    if *method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    response
}

/// An error page (or plain text for health/status).
struct Page {
    status: StatusCode,
    message: String,
}

impl Page {
    fn error(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
    fn into_response(self) -> Response {
        let body = format!("<p class=\"error\">{}</p>", esc(&self.message));
        let mut response = html("Error", &[], &body);
        *response.status_mut() = self.status;
        response
    }
}

impl From<super::S3Error> for Page {
    fn from(e: super::S3Error) -> Self {
        Self::error(e.status, format!("{}: {}", e.code, e.message))
    }
}

impl From<crate::Error> for Page {
    fn from(e: crate::Error) -> Self {
        super::S3Error::from(e).into()
    }
}

fn status(state: &Shared) -> Result<Response, Page> {
    let buckets = lock(state)?.stats()?;
    Ok(Json(serde_json::json!({ "buckets": buckets })).into_response())
}

// ---- links and formatting ----

fn enc(s: &str) -> String {
    utf8_percent_encode(s, URL_ENCODE).to_string()
}

fn folder_link(bucket: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        format!("/_ls2/browse/{}", enc(bucket))
    } else {
        format!("/_ls2/browse/{}?prefix={}", enc(bucket), enc(prefix))
    }
}

fn with_version(mut link: String, version_id: Option<&str>) -> String {
    if let Some(v) = version_id {
        link += &format!("&versionId={}", enc(v));
    }
    link
}

fn object_link(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    with_version(
        format!("/_ls2/browse/{}?key={}", enc(bucket), enc(key)),
        version_id,
    )
}

fn download_link(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    with_version(
        format!("/_ls2/download/{}?key={}", enc(bucket), enc(key)),
        version_id,
    )
}

fn a(href: &str, text: &str) -> String {
    format!("<a href=\"{}\">{}</a>", esc(href), esc(text))
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// `bucket / dir/ / sub/` crumbs, each linking to its folder.
fn crumbs(bucket: &str, prefix: &str) -> Vec<String> {
    let mut out = vec![a(&folder_link(bucket, ""), bucket)];
    let mut end = 0;
    while let Some(i) = prefix[end..].find('/') {
        let next = end + i + 1;
        out.push(a(&folder_link(bucket, &prefix[..next]), &prefix[end..next]));
        end = next;
    }
    if end < prefix.len() {
        out.push(esc(&prefix[end..]));
    }
    out
}

fn html(title: &str, crumbs: &[String], body: &str) -> Response {
    let mut nav = a("/_ls2/browse", "Buckets");
    for c in crumbs {
        nav += "<span class=\"sep\">/</span>";
        nav += c;
    }
    let page = format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{} · ls2</title><style>{STYLE}</style></head><body><header><strong>ls2</strong><nav>{nav}</nav></header><main>{body}</main></body></html>",
        esc(title)
    );
    let mut response = ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], page).into_response();
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(PAGE_CSP),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

const STYLE: &str = "
:root{color-scheme:light dark;--fg:#1f2328;--muted:#59636e;--bg:#fff;--line:#d1d9e0;--head:#f6f8fa;--link:#0969da;--err:#d1242f}
@media (prefers-color-scheme:dark){:root{--fg:#e6edf3;--muted:#9198a1;--bg:#0d1117;--line:#30363d;--head:#151b23;--link:#4493f8;--err:#f85149}}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 system-ui,sans-serif;color:var(--fg);background:var(--bg)}
header{display:flex;gap:16px;align-items:baseline;padding:12px 16px;border-bottom:1px solid var(--line);background:var(--head);flex-wrap:wrap}
nav{overflow-wrap:anywhere}.sep{color:var(--muted);margin:0 6px}main{padding:16px;max-width:1200px}
a{color:var(--link);text-decoration:none}a:hover{text-decoration:underline}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600}td.num,th.num{text-align:right;white-space:nowrap}
.key,code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px;overflow-wrap:anywhere}
pre{padding:12px;border:1px solid var(--line);border-radius:6px;background:var(--head);white-space:pre-wrap;max-height:60vh;overflow:auto}
h1{font-size:18px;margin:0 0 12px;overflow-wrap:anywhere}h2{font-size:15px;margin:24px 0 8px}
.muted{color:var(--muted)}.error{color:var(--err)}.actions{margin:12px 0}
.button{display:inline-block;padding:4px 12px;border:1px solid var(--line);border-radius:6px;background:var(--head)}
.table{overflow-x:auto}
";

// ---- pages ----

fn buckets_page(state: &Shared) -> Result<Response, Page> {
    let buckets = lock(state)?.stats()?;
    let mut body = String::from("<h1>Buckets</h1>");
    if buckets.is_empty() {
        body += "<p class=\"muted\">No buckets yet.</p>";
    } else {
        body += "<div class=\"table\"><table><tr><th>Name</th><th>Region</th><th>Versioning</th><th class=\"num\">Objects</th><th class=\"num\">Versions</th><th class=\"num\">Size</th><th class=\"num\">Uploads</th><th>Created</th></tr>";
        for b in &buckets {
            body += &format!(
                "<tr><td class=\"key\">{}</td><td>{}</td><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
                a(&folder_link(&b.name, ""), &b.name),
                esc(&b.region),
                b.versioning.map_or("Off", |v| v.name()),
                b.objects,
                b.versions,
                human(b.bytes),
                b.multipart_uploads,
                iso8601(b.created_at_ms)
            );
        }
        body += "</table></div>";
    }
    Ok(html("Buckets", &[], &body))
}

fn folder_page(state: &Shared, bucket: &str, prefix: &str, after: &str) -> Result<Response, Page> {
    let ls2 = lock(state)?;
    let list = ls2.list_objects(
        bucket,
        &ListRequest {
            prefix: prefix.into(),
            delimiter: "/".into(),
            after: after.into(),
            max_keys: PAGE_SIZE,
        },
    )?;
    let (uploads, _) = ls2.list_multipart_uploads(bucket, prefix, "", "", PAGE_SIZE)?;
    drop(ls2);
    let title = format!("{bucket}/{prefix}");
    let mut body = format!("<h1 class=\"key\">{}</h1>", esc(&title));
    if list.objects.is_empty() && list.common_prefixes.is_empty() {
        body += "<p class=\"muted\">No objects.</p>";
    } else {
        body += "<div class=\"table\"><table><tr><th>Name</th><th class=\"num\">Size</th><th>Last modified</th><th>ETag</th></tr>";
        for p in &list.common_prefixes {
            body += &format!(
                "<tr><td class=\"key\">{}</td><td class=\"num muted\">folder</td><td></td><td></td></tr>",
                a(&folder_link(bucket, p), &p[prefix.len()..])
            );
        }
        for o in &list.objects {
            body += &object_row(bucket, prefix, o);
        }
        body += "</table></div>";
    }
    if let Some(next) = list.next_marker.filter(|_| list.truncated) {
        let mut href = folder_link(bucket, prefix);
        href += if prefix.is_empty() { "?" } else { "&" };
        href += &format!("after={}", enc(&next));
        body += &format!("<p class=\"actions\">{}</p>", a(&href, "Next page →"));
    }
    if !uploads.is_empty() {
        body += "<h2>Multipart uploads in progress</h2><div class=\"table\"><table><tr><th>Key</th><th>Upload ID</th><th>Initiated</th></tr>";
        for u in &uploads {
            body += &format!(
                "<tr><td class=\"key\">{}</td><td><code>{}</code></td><td>{}</td></tr>",
                esc(&u.key),
                esc(&u.upload_id),
                iso8601(u.initiated_at_ms)
            );
        }
        body += "</table></div>";
    }
    Ok(html(&title, &crumbs(bucket, prefix), &body))
}

fn object_row(bucket: &str, prefix: &str, o: &ObjectInfo) -> String {
    let name = match &o.key[prefix.len()..] {
        "" => "(folder marker)",
        rest => rest,
    };
    format!(
        "<tr><td class=\"key\">{}</td><td class=\"num\">{}</td><td>{}</td><td><code>{}</code></td></tr>",
        a(&object_link(bucket, &o.key, None), name),
        human(o.size),
        iso8601(o.last_modified_ms),
        esc(&o.etag)
    )
}

fn object_page(
    state: &Shared,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, Page> {
    let ls2 = lock(state)?;
    let versioning = ls2.bucket(bucket)?.versioning;
    let opened = match ls2.open_object(bucket, key, version_id, None, &ReadCondition::default()) {
        Ok(handle) => {
            let head = ls2.read_object(&handle, 0, PREVIEW_BYTES)?;
            Some((handle, head))
        }
        // The latest version is a delete marker: still show the version history.
        Err(Error::DeleteMarker {
            requested: false, ..
        }) => None,
        Err(e) => return Err(e.into()),
    };
    let versions = ls2.list_object_versions(
        bucket,
        &VersionListRequest {
            prefix: key.into(),
            max_keys: PAGE_SIZE,
            ..Default::default()
        },
    )?;
    drop(ls2);
    let folder = key.rfind('/').map_or("", |i| &key[..=i]);
    let mut crumbs = crumbs(bucket, folder);
    crumbs.push(esc(&key[folder.len()..]));
    let mut body = format!("<h1 class=\"key\">{}</h1>", esc(key));
    if let Some((handle, head)) = &opened {
        body += &object_details(bucket, key, version_id, &handle.info, head);
    } else {
        body += "<p class=\"muted\">The latest version is a delete marker.</p>";
    }
    let history: Vec<_> = versions
        .versions
        .iter()
        .filter(|v| v.info.key == key)
        .collect();
    if versioning.is_some() && !history.is_empty() {
        body += "<h2>Versions</h2><div class=\"table\"><table><tr><th>Version ID</th><th class=\"num\">Size</th><th>Last modified</th><th>ETag</th></tr>";
        for v in history {
            let o = &v.info;
            let id = if v.is_latest {
                format!(
                    "{} <span class=\"muted\">(latest)</span>",
                    esc(&o.version_id)
                )
            } else {
                esc(&o.version_id)
            };
            if v.delete_marker {
                body += &format!(
                    "<tr><td><code>{id}</code></td><td class=\"num muted\">delete marker</td><td>{}</td><td></td></tr>",
                    iso8601(o.last_modified_ms)
                );
            } else {
                body += &format!(
                    "<tr><td><code><a href=\"{}\">{id}</a></code></td><td class=\"num\">{}</td><td>{}</td><td><code>{}</code></td></tr>",
                    esc(&object_link(bucket, key, Some(&o.version_id))),
                    human(o.size),
                    iso8601(o.last_modified_ms),
                    esc(&o.etag)
                );
            }
        }
        body += "</table></div>";
    }
    Ok(html(key, &crumbs, &body))
}

fn object_details(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    o: &ObjectInfo,
    head: &[u8],
) -> String {
    let m = &o.metadata;
    let mut rows = vec![
        ("Size", format!("{} ({} bytes)", human(o.size), o.size)),
        ("ETag", format!("<code>{}</code>", esc(&o.etag))),
        ("Last modified", iso8601(o.last_modified_ms)),
        ("Version ID", format!("<code>{}</code>", esc(&o.version_id))),
    ];
    if let Some(c) = &o.checksum {
        rows.push((
            "Checksum",
            format!("{} <code>{}</code>", c.algorithm.name(), esc(&c.value)),
        ));
    }
    for (name, value) in [
        ("Content-Type", &m.content_type),
        ("Content-Encoding", &m.content_encoding),
        ("Content-Disposition", &m.content_disposition),
        ("Content-Language", &m.content_language),
        ("Cache-Control", &m.cache_control),
        ("Expires", &m.expires),
    ] {
        if let Some(v) = value {
            rows.push((name, format!("<code>{}</code>", esc(v))));
        }
    }
    let mut table = String::new();
    for (name, value) in rows {
        table += &format!("<tr><th>{name}</th><td>{value}</td></tr>");
    }
    for (k, v) in &m.user {
        table += &format!(
            "<tr><th><code>x-amz-meta-{}</code></th><td><code>{}</code></td></tr>",
            esc(k),
            esc(v)
        );
    }
    let mut body = format!(
        "<p class=\"actions\"><a class=\"button\" href=\"{}\">Download</a></p><div class=\"table\"><table>{table}</table></div>",
        esc(&download_link(bucket, key, version_id))
    );
    if let Some(text) = preview(head) {
        let note = if o.size as usize > head.len() {
            format!(
                " <span class=\"muted\">(first {})</span>",
                human(head.len() as u64)
            )
        } else {
            String::new()
        };
        body += &format!("<h2>Preview{note}</h2><pre>{}</pre>", esc(text));
    }
    body
}

/// UTF-8 text without NUL bytes; a character cut off by the preview limit is dropped.
fn preview(bytes: &[u8]) -> Option<&str> {
    if bytes.is_empty() || bytes.contains(&0) {
        return None;
    }
    match std::str::from_utf8(bytes) {
        Ok(s) => Some(s),
        Err(e) if e.error_len().is_none() && bytes.len() == PREVIEW_BYTES => {
            std::str::from_utf8(&bytes[..e.valid_up_to()]).ok()
        }
        Err(_) => None,
    }
}

/// Always an attachment, so stored HTML or SVG never runs in the browser's origin.
fn download(
    state: &Shared,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, Page> {
    let ls2 = lock(state)?;
    let handle = ls2.open_object(bucket, key, version_id, None, &ReadCondition::default())?;
    let info = handle.info.clone();
    let body = object_body(&ls2, state, handle)?;
    drop(ls2);
    let name = key
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or("download");
    let mut response = Response::new(body);
    let h = response.headers_mut();
    let value = |v: String| {
        HeaderValue::from_str(&v)
            .map_err(|_| Page::error(StatusCode::INTERNAL_SERVER_ERROR, "invalid header"))
    };
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    h.insert(
        header::CONTENT_DISPOSITION,
        value(format!(
            "attachment; filename*=UTF-8''{}",
            utf8_percent_encode(name, percent_encoding::NON_ALPHANUMERIC)
        ))?,
    );
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(info.size));
    h.insert(header::ETAG, value(format!("\"{}\"", info.etag))?);
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("sandbox; default-src 'none'"),
    );
    Ok(response)
}
