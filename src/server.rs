//! S3 REST/XML adapter (path-style addressing). SigV4 is accepted but not verified.
use crate::checksum::{Checksum, ChecksumAlgorithm, ChecksumInput};
use crate::{
    ByteRange, CompletedPart, Error, ListRequest, Ls2, MAX_LIST, Metadata, ObjectInfo, PutOptions,
    ReadCondition, WriteCondition,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Largest request body accepted (after which multipart upload is required).
pub const MAX_REQUEST_BYTES: usize = 256 * 1024 * 1024;
/// Account ID compared with `x-amz-expected-bucket-owner`.
pub const ACCOUNT_ID: &str = "000000000000";
/// Canonical user ID reported as owner of every bucket and object.
pub const OWNER_ID: &str = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
const OWNER_NAME: &str = "ls2";
const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const MAX_DELETE_OBJECTS: usize = 1000;

type Shared = Arc<Mutex<Ls2>>;

pub fn router(ls2: Ls2) -> Router {
    Router::new()
        .route("/_ls2/health", get(|| async { "ok" }))
        .route("/_ls2/status", get(status))
        .fallback(handle)
        .layer(DefaultBodyLimit::disable())
        .with_state(Arc::new(Mutex::new(ls2)))
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn lock(state: &Shared) -> Result<std::sync::MutexGuard<'_, Ls2>, S3Error> {
    state
        .lock()
        .map_err(|_| Error::Storage(rusqlite::Error::InvalidQuery).into())
}

async fn status(State(state): State<Shared>) -> Response {
    match lock(&state).and_then(|s| s.stats().map_err(S3Error::from)) {
        Ok(buckets) => Json(serde_json::json!({ "buckets": buckets })).into_response(),
        Err(e) => (e.status, e.message).into_response(),
    }
}

// ---- errors ----

#[derive(Debug)]
struct S3Error {
    status: StatusCode,
    code: &'static str,
    message: String,
}

fn s3err(status: StatusCode, code: &'static str, message: impl Into<String>) -> S3Error {
    S3Error {
        status,
        code,
        message: message.into(),
    }
}
fn bad(code: &'static str, message: impl Into<String>) -> S3Error {
    s3err(StatusCode::BAD_REQUEST, code, message)
}
fn not_implemented(message: impl Into<String>) -> S3Error {
    s3err(StatusCode::NOT_IMPLEMENTED, "NotImplemented", message)
}
fn invalid_argument(message: impl Into<String>) -> S3Error {
    bad("InvalidArgument", message)
}
fn malformed_xml() -> S3Error {
    bad(
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema",
    )
}

impl From<Error> for S3Error {
    fn from(e: Error) -> Self {
        let status = match &e {
            Error::NoSuchBucket | Error::NoSuchKey | Error::NoSuchUpload => StatusCode::NOT_FOUND,
            Error::BucketAlreadyOwnedByYou | Error::BucketNotEmpty => StatusCode::CONFLICT,
            Error::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            Error::NotModified => StatusCode::NOT_MODIFIED,
            Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Error::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        let message = match &e {
            Error::Storage(inner) => {
                eprintln!("storage error: {inner}");
                "We encountered an internal error. Please try again.".into()
            }
            other => other.to_string(),
        };
        S3Error {
            status,
            code: e.code(),
            message,
        }
    }
}

fn error_response(e: S3Error, r: Option<&Target>, request_id: &str, head: bool) -> Response {
    let mut builder = Response::builder().status(e.status);
    if head || e.status == StatusCode::NOT_MODIFIED {
        // HEAD and 304 responses carry no body; the code is exposed for debugging.
        builder = builder.header("x-amz-error-code", e.code);
        return builder.body(Body::empty()).expect("valid response");
    }
    let mut extra = String::new();
    if let Some(t) = r {
        match e.code {
            "NoSuchBucket" | "BucketNotEmpty" | "BucketAlreadyOwnedByYou" | "InvalidBucketName" => {
                extra = tag("BucketName", t.bucket.as_deref().unwrap_or_default());
            }
            "NoSuchKey" => extra = tag("Key", t.key.as_deref().unwrap_or_default()),
            "NoSuchUpload" => {
                extra = tag("UploadId", t.query("uploadId").unwrap_or_default());
            }
            _ => {}
        }
        extra += &tag("Resource", &t.path);
    }
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message>{extra}<RequestId>{request_id}</RequestId><HostId>{request_id}</HostId></Error>",
        e.code,
        esc(&e.message)
    );
    builder
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .expect("valid response")
}

// ---- XML helpers ----

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\r' => out.push_str("&#13;"),
            c => out.push(c),
        }
    }
    out
}
fn tag(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", esc(value))
}
fn xml_doc(root: &str, inner: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{NS}\">{inner}</{root}>")
}
fn xml_response(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/xml")], body).into_response()
}
fn owner() -> String {
    format!("<ID>{OWNER_ID}</ID><DisplayName>{OWNER_NAME}</DisplayName>")
}
fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

/// `encoding-type=url`: everything but unreserved characters and `/`.
const URL_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');
fn enc_tag(name: &str, value: &str, url: bool) -> String {
    if url {
        format!(
            "<{name}>{}</{name}>",
            utf8_percent_encode(value, URL_ENCODE)
        )
    } else {
        tag(name, value)
    }
}

pub(crate) fn iso8601(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60,
        ms.rem_euclid(1000)
    )
}
fn http_date(ms: i64) -> String {
    httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64))
}

// ---- request parsing ----

/// Query parameters that only carry authentication or SDK hints.
const IGNORED_QUERY: &[&str] = &[
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "X-Amz-Content-Sha256",
    "AWSAccessKeyId",
    "Signature",
    "Expires",
    "x-id",
];

/// `x-amz-*` request headers accepted by every operation.
const COMMON_HEADERS: &[&str] = &[
    "x-amz-content-sha256",
    "x-amz-date",
    "x-amz-security-token",
    "x-amz-user-agent",
    "x-amz-expected-bucket-owner",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-decoded-content-length",
    "x-amz-trailer",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
];

#[derive(Clone)]
struct Target {
    path: String,
    bucket: Option<String>,
    key: Option<String>,
    query: Vec<(String, String)>,
}

impl Target {
    fn query(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
    fn has(&self, name: &str) -> bool {
        self.query.iter().any(|(k, _)| k == name)
    }
}

struct Req<'a> {
    target: Target,
    headers: &'a HeaderMap,
    payload: Payload,
    now_ms: i64,
}

/// Decoded request body plus its integrity information.
struct Payload {
    data: Bytes,
    checksum: ChecksumInput,
    content_md5: Option<[u8; 16]>,
}

impl Payload {
    /// Verifies integrity headers for operations that do not store the body.
    fn verify(&self) -> Result<(), S3Error> {
        crate::checksum::verify_md5(&self.data, self.content_md5)?;
        self.checksum.resolve(&self.data, None)?;
        Ok(())
    }
}

impl Req<'_> {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
    fn query(&self, name: &str) -> Option<&str> {
        self.target.query(name)
    }
    fn has(&self, name: &str) -> bool {
        self.target.has(name)
    }
    fn bucket(&self) -> &str {
        self.target.bucket.as_deref().unwrap_or_default()
    }
    fn key(&self) -> &str {
        self.target.key.as_deref().unwrap_or_default()
    }
    /// Rejects query parameters and `x-amz-*` headers outside the operation's allowlist,
    /// so unsupported features fail loudly instead of being silently ignored.
    fn allow(&self, query: &[&str], headers: &[&str], user_metadata: bool) -> Result<(), S3Error> {
        if let Some((k, _)) = self
            .target
            .query
            .iter()
            .find(|(k, _)| !query.contains(&k.as_str()))
        {
            return Err(not_implemented(format!("Unsupported query parameter: {k}")));
        }
        for name in self.headers.keys() {
            let name = name.as_str();
            if name.starts_with("x-amz-")
                && !COMMON_HEADERS.contains(&name)
                && !headers.contains(&name)
                && !(user_metadata && name.starts_with("x-amz-meta-"))
            {
                return Err(not_implemented(format!("Unsupported header: {name}")));
            }
        }
        if let Some(class) = self.header("x-amz-storage-class")
            && class != "STANDARD"
        {
            return Err(not_implemented(format!(
                "Unsupported storage class: {class}"
            )));
        }
        if let Some(acl) = self.header("x-amz-acl")
            && !matches!(acl, "private" | "bucket-owner-full-control")
        {
            return Err(not_implemented(format!("Unsupported canned ACL: {acl}")));
        }
        Ok(())
    }
    fn max(&self, name: &str) -> Result<usize, S3Error> {
        match self.query(name) {
            None => Ok(MAX_LIST),
            Some(v) => v.parse::<usize>().map(|n| n.min(MAX_LIST)).map_err(|_| {
                invalid_argument(format!(
                    "Provided {name} not an integer or within integer range"
                ))
            }),
        }
    }
    fn url_encoding(&self) -> Result<bool, S3Error> {
        match self.query("encoding-type") {
            None => Ok(false),
            Some("url") => Ok(true),
            Some(_) => Err(invalid_argument(
                "Invalid Encoding Method specified in Request",
            )),
        }
    }
    fn metadata(&self) -> Result<Metadata, S3Error> {
        let text = |name: header::HeaderName| self.header(name.as_str()).map(str::to_owned);
        let content_encoding = self.header("content-encoding").and_then(|v| {
            let rest: Vec<&str> = v
                .split(',')
                .map(str::trim)
                .filter(|e| !e.eq_ignore_ascii_case("aws-chunked") && !e.is_empty())
                .collect();
            (!rest.is_empty()).then(|| rest.join(","))
        });
        let mut user = BTreeMap::new();
        for (name, value) in self.headers {
            if let Some(n) = name.as_str().strip_prefix("x-amz-meta-") {
                let value = std::str::from_utf8(value.as_bytes())
                    .map_err(|_| invalid_argument("User metadata must be UTF-8"))?;
                user.insert(n.to_owned(), value.to_owned());
            }
        }
        Ok(Metadata {
            content_type: text(header::CONTENT_TYPE),
            content_encoding,
            content_disposition: text(header::CONTENT_DISPOSITION),
            content_language: text(header::CONTENT_LANGUAGE),
            cache_control: text(header::CACHE_CONTROL),
            expires: text(header::EXPIRES),
            user,
        })
    }
    fn write_condition(&self) -> Result<WriteCondition, S3Error> {
        let if_none_match_any = match self.header("if-none-match") {
            None => false,
            Some(v) if v.trim() == "*" => true,
            Some(_) => return Err(not_implemented("If-None-Match only supports * for writes")),
        };
        Ok(WriteCondition {
            if_match: self.header("if-match").map(str::to_owned),
            if_none_match_any,
        })
    }
    fn read_condition(&self) -> ReadCondition {
        self.conditions("")
    }
    /// `x-amz-copy-source-if-*` headers.
    fn copy_source_condition(&self) -> ReadCondition {
        self.conditions("x-amz-copy-source-")
    }
    fn conditions(&self, prefix: &str) -> ReadCondition {
        let value = |name: &str| self.header(&format!("{prefix}{name}"));
        let date = |name: &str| {
            value(name)
                .and_then(|v| httpdate::parse_http_date(v).ok())
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
        };
        ReadCondition {
            if_match: value("if-match").map(str::to_owned),
            if_none_match: value("if-none-match").map(str::to_owned),
            if_modified_since_ms: date("if-modified-since"),
            if_unmodified_since_ms: date("if-unmodified-since"),
        }
    }
    /// `x-amz-copy-source: [/]bucket/key` (URL-encoded).
    fn copy_source(&self) -> Result<(String, String), S3Error> {
        let raw = self.header("x-amz-copy-source").unwrap_or_default();
        let (path, query) = raw.split_once('?').unwrap_or((raw, ""));
        if !query.is_empty() {
            return Err(not_implemented(
                "Copying a specific version is not supported",
            ));
        }
        let decoded = percent_decode_str(path)
            .decode_utf8()
            .map_err(|_| invalid_argument("Invalid copy source encoding"))?;
        let decoded = decoded.strip_prefix('/').unwrap_or(&decoded);
        match decoded.split_once('/') {
            Some((b, k)) if !b.is_empty() && !k.is_empty() => Ok((b.to_owned(), k.to_owned())),
            _ => Err(invalid_argument(
                "Copy Source must mention the source bucket and key: sourcebucket/sourcekey",
            )),
        }
    }
    fn part_number(&self) -> Result<u32, S3Error> {
        self.query("partNumber")
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| {
                invalid_argument("Part number must be an integer between 1 and 10000, inclusive")
            })
    }
    fn upload_id(&self) -> &str {
        self.query("uploadId").unwrap_or_default()
    }
}

fn parse_target(uri: &Uri) -> Result<Target, S3Error> {
    let path = uri.path().to_owned();
    let decode = |s: &str| {
        percent_decode_str(s)
            .decode_utf8()
            .map(|c| c.into_owned())
            .map_err(|_| bad("InvalidURI", "Couldn't parse the specified URI."))
    };
    let rest = path.strip_prefix('/').unwrap_or(&path);
    let (bucket, key) = match rest.split_once('/') {
        Some((b, k)) => (b, Some(k)),
        None => (rest, None),
    };
    let bucket = (!bucket.is_empty()).then(|| decode(bucket)).transpose()?;
    let key = key.filter(|k| !k.is_empty()).map(decode).transpose()?;
    let query: Vec<(String, String)> = serde_urlencoded::from_str(uri.query().unwrap_or_default())
        .map_err(|_| bad("InvalidURI", "Couldn't parse the specified URI."))?;
    let query = query
        .into_iter()
        .filter(|(k, _)| !IGNORED_QUERY.contains(&k.as_str()))
        .collect();
    Ok(Target {
        path,
        bucket,
        key,
        query,
    })
}

/// Decodes an `aws-chunked` body: `hex-size[;ext]\r\n<data>\r\n ... 0[;ext]\r\n<trailers>\r\n`.
/// Trailing headers of an `aws-chunked` body, names lowercased.
pub type Trailers = Vec<(String, String)>;

pub fn decode_aws_chunked(body: &[u8]) -> Option<(Vec<u8>, Trailers)> {
    fn line(body: &[u8], pos: &mut usize) -> Option<std::ops::Range<usize>> {
        let start = *pos;
        let len = body[start..].windows(2).position(|w| w == b"\r\n")?;
        *pos = start + len + 2;
        Some(start..start + len)
    }
    let mut data = Vec::new();
    let mut pos = 0;
    loop {
        let header = std::str::from_utf8(&body[line(body, &mut pos)?]).ok()?;
        let size = usize::from_str_radix(header.split(';').next()?.trim(), 16).ok()?;
        if size == 0 {
            break;
        }
        let chunk = body.get(pos..pos + size)?;
        data.extend_from_slice(chunk);
        pos += size;
        if body.get(pos..pos + 2)? != b"\r\n" {
            return None;
        }
        pos += 2;
    }
    let mut trailers = Vec::new();
    while pos < body.len() {
        let range = match line(body, &mut pos) {
            Some(r) => r,
            None => {
                let r = pos..body.len();
                pos = body.len();
                r
            }
        };
        let text = std::str::from_utf8(&body[range]).ok()?;
        if text.is_empty() {
            continue;
        }
        let (k, v) = text.split_once(':')?;
        trailers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
    }
    Some((data, trailers))
}

fn parse_payload(headers: &HeaderMap, body: Bytes) -> Result<Payload, S3Error> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let sha256 = header("x-amz-content-sha256").unwrap_or_default();
    let chunked = sha256.starts_with("STREAMING-")
        || header("content-encoding").is_some_and(|v| {
            v.split(',')
                .any(|e| e.trim().eq_ignore_ascii_case("aws-chunked"))
        });
    let incomplete = || {
        bad(
            "IncompleteBody",
            "You did not provide the number of bytes specified by the Content-Length HTTP header.",
        )
    };
    let (data, trailers) = if chunked {
        let (data, trailers) = decode_aws_chunked(&body)
            .ok_or_else(|| bad("InvalidRequest", "Invalid aws-chunked request body"))?;
        (Bytes::from(data), trailers)
    } else {
        if sha256.len() == 64 && sha256.bytes().all(|c| c.is_ascii_hexdigit()) {
            let actual = crate::checksum::hex(&Sha256::digest(&body));
            if !actual.eq_ignore_ascii_case(sha256) {
                return Err(bad(
                    "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.",
                ));
            }
        }
        (body, Vec::new())
    };
    if let Some(len) = header("x-amz-decoded-content-length")
        && len.parse::<usize>().ok() != Some(data.len())
    {
        return Err(incomplete());
    }
    let declared: Vec<String> = header("x-amz-trailer")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    for name in &declared {
        if !trailers.iter().any(|(k, _)| k == name) {
            return Err(incomplete());
        }
    }
    let mut values = Vec::new();
    for algorithm in ChecksumAlgorithm::ALL {
        if let Some(v) = header(algorithm.header()) {
            values.push(Checksum {
                algorithm,
                value: v.trim().to_owned(),
            });
        }
        if let Some((_, v)) = trailers.iter().find(|(k, _)| k == algorithm.header()) {
            values.push(Checksum {
                algorithm,
                value: v.clone(),
            });
        }
    }
    if values.len() > 1 {
        return Err(bad(
            "InvalidRequest",
            "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.",
        ));
    }
    let sdk_algorithm = header("x-amz-sdk-checksum-algorithm")
        .map(|v| {
            ChecksumAlgorithm::parse(v).ok_or_else(|| {
                bad(
                    "InvalidRequest",
                    "Value for x-amz-sdk-checksum-algorithm header is invalid.",
                )
            })
        })
        .transpose()?;
    let checksum = match (values.pop(), sdk_algorithm) {
        (Some(c), Some(a)) if c.algorithm != a => {
            return Err(bad(
                "InvalidRequest",
                "Value for x-amz-sdk-checksum-algorithm header is invalid.",
            ));
        }
        (Some(c), _) => ChecksumInput::Verify(c),
        (None, Some(a)) => ChecksumInput::Compute(a),
        (None, None) => ChecksumInput::Default,
    };
    let content_md5 = header("content-md5")
        .map(|v| {
            STANDARD
                .decode(v.trim())
                .ok()
                .and_then(|b| <[u8; 16]>::try_from(b).ok())
                .ok_or_else(|| {
                    bad(
                        "InvalidDigest",
                        "The Content-MD5 you specified is not valid.",
                    )
                })
        })
        .transpose()?;
    Ok(Payload {
        data,
        checksum,
        content_md5,
    })
}

// ---- dispatch ----

async fn handle(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let request_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_uppercase();
    let head = method == Method::HEAD;
    let target = parse_target(&uri);
    let result = async {
        let target = target
            .as_ref()
            .map_err(|e| s3err(e.status, e.code, e.message.clone()))?;
        if let Some(owner) = headers.get("x-amz-expected-bucket-owner")
            && owner.as_bytes() != ACCOUNT_ID.as_bytes()
        {
            return Err(s3err(
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "Access Denied",
            ));
        }
        let too_large = || {
            bad(
                "EntityTooLarge",
                "Your proposed upload exceeds the maximum allowed size",
            )
        };
        let declared = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok());
        if declared.is_some_and(|n| n > MAX_REQUEST_BYTES) {
            return Err(too_large());
        }
        let body = axum::body::to_bytes(body, MAX_REQUEST_BYTES)
            .await
            .map_err(|_| too_large())?;
        let payload = parse_payload(&headers, body)?;
        let req = Req {
            target: target.clone(),
            headers: &headers,
            payload,
            now_ms: now(),
        };
        let mut ls2 = lock(&state)?;
        dispatch(&mut ls2, &method, &req)
    }
    .await;
    let mut response = match result {
        Ok(r) => r,
        Err(e) => error_response(e, target.as_ref().ok(), &request_id, head),
    };
    let h = response.headers_mut();
    h.insert(
        "x-amz-request-id",
        HeaderValue::from_str(&request_id).expect("ascii"),
    );
    h.insert(
        "x-amz-id-2",
        HeaderValue::from_str(&request_id).expect("ascii"),
    );
    h.insert(header::SERVER, HeaderValue::from_static("ls2"));
    response
}

fn dispatch(ls2: &mut Ls2, method: &Method, r: &Req) -> Result<Response, S3Error> {
    match (&r.target.bucket, &r.target.key) {
        (None, _) if method == Method::GET => list_buckets(ls2, r),
        (None, _) => Err(s3err(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        )),
        (Some(_), None) => bucket_op(ls2, method, r),
        (Some(_), Some(_)) => object_op(ls2, method, r),
    }
}

fn method_not_allowed() -> S3Error {
    s3err(
        StatusCode::METHOD_NOT_ALLOWED,
        "MethodNotAllowed",
        "The specified method is not allowed against this resource.",
    )
}

fn bucket_op(ls2: &mut Ls2, method: &Method, r: &Req) -> Result<Response, S3Error> {
    let bucket = r.bucket();
    match *method {
        Method::GET if r.has("location") => {
            r.allow(&["location"], &[], false)?;
            let region = ls2.bucket(bucket)?.region;
            let value = if region == crate::DEFAULT_REGION {
                String::new()
            } else {
                esc(&region)
            };
            Ok(xml_response(xml_doc("LocationConstraint", &value)))
        }
        Method::GET if r.has("versioning") => {
            r.allow(&["versioning"], &[], false)?;
            ls2.bucket(bucket)?;
            Ok(xml_response(xml_doc("VersioningConfiguration", "")))
        }
        Method::GET if r.has("uploads") => list_uploads(ls2, r),
        Method::GET if r.has("list-type") => list_objects_v2(ls2, r),
        Method::GET => list_objects_v1(ls2, r),
        Method::HEAD => {
            r.allow(&[], &[], false)?;
            let b = ls2.bucket(bucket)?;
            Ok(([("x-amz-bucket-region", b.region)], ()).into_response())
        }
        Method::PUT => {
            r.allow(&[], &["x-amz-acl"], false)?;
            r.payload.verify()?;
            #[derive(Deserialize)]
            #[serde(rename_all = "PascalCase")]
            struct Config {
                location_constraint: Option<String>,
            }
            let region = if r.payload.data.iter().all(u8::is_ascii_whitespace) {
                None
            } else {
                let config: Config = quick_xml::de::from_reader(r.payload.data.as_ref())
                    .map_err(|_| malformed_xml())?;
                config.location_constraint
            };
            ls2.create_bucket(bucket, region.as_deref(), r.now_ms)?;
            Ok(([(header::LOCATION, format!("/{bucket}"))], ()).into_response())
        }
        Method::DELETE => {
            r.allow(&[], &[], false)?;
            ls2.delete_bucket(bucket)?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Method::POST if r.has("delete") => delete_objects(ls2, r),
        _ => Err(method_not_allowed()),
    }
}

fn object_op(ls2: &mut Ls2, method: &Method, r: &Req) -> Result<Response, S3Error> {
    match *method {
        Method::PUT if r.has("uploadId") || r.has("partNumber") => {
            if r.headers.contains_key("x-amz-copy-source") {
                upload_part_copy(ls2, r)
            } else {
                upload_part(ls2, r)
            }
        }
        Method::PUT if r.headers.contains_key("x-amz-copy-source") => copy_object(ls2, r),
        Method::PUT => put_object(ls2, r),
        Method::GET if r.has("uploadId") => list_parts(ls2, r),
        Method::GET if r.has("tagging") => {
            r.allow(&["tagging"], &[], false)?;
            ls2.head_object(r.bucket(), r.key())?;
            Ok(xml_response(xml_doc("Tagging", "<TagSet></TagSet>")))
        }
        Method::GET | Method::HEAD => get_object(ls2, r, *method == Method::HEAD),
        Method::DELETE if r.has("uploadId") => {
            r.allow(&["uploadId"], &[], false)?;
            ls2.abort_multipart_upload(r.bucket(), r.key(), r.upload_id())?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Method::DELETE => {
            r.allow(&[], &[], false)?;
            ls2.delete_object(r.bucket(), r.key())?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Method::POST if r.has("uploads") => create_upload(ls2, r),
        Method::POST if r.has("uploadId") => complete_upload(ls2, r),
        _ => Err(method_not_allowed()),
    }
}

// ---- service / bucket operations ----

fn list_buckets(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &[
            "prefix",
            "max-buckets",
            "continuation-token",
            "bucket-region",
        ],
        &[],
        false,
    )?;
    let prefix = r.query("prefix").unwrap_or_default();
    let limit = match r.query("max-buckets") {
        None => 10_000,
        Some(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=10_000).contains(n))
            .ok_or_else(|| invalid_argument("Invalid max-buckets"))?,
    };
    let after = r.query("continuation-token").unwrap_or_default();
    let (buckets, truncated) = ls2.list_buckets(prefix, after, limit)?;
    let region = r.query("bucket-region");
    let mut inner = format!("<Owner>{}</Owner><Buckets>", owner());
    for b in buckets
        .iter()
        .filter(|b| region.is_none_or(|x| x == b.region))
    {
        inner += &format!(
            "<Bucket>{}{}{}</Bucket>",
            tag("Name", &b.name),
            tag("CreationDate", &iso8601(b.created_at_ms)),
            tag("BucketRegion", &b.region)
        );
    }
    inner += "</Buckets>";
    if truncated && let Some(last) = buckets.last() {
        inner += &tag("ContinuationToken", &last.name);
    }
    if !prefix.is_empty() {
        inner += &tag("Prefix", prefix);
    }
    Ok(xml_response(xml_doc("ListAllMyBucketsResult", &inner)))
}

fn object_entry(o: &ObjectInfo, url: bool, with_owner: bool) -> String {
    let mut s = format!(
        "<Contents>{}{}{}",
        enc_tag("Key", &o.key, url),
        tag("LastModified", &iso8601(o.last_modified_ms)),
        tag("ETag", &quoted(&o.etag))
    );
    if let Some(c) = &o.checksum {
        s += &tag("ChecksumAlgorithm", c.algorithm.name());
        s += "<ChecksumType>FULL_OBJECT</ChecksumType>";
    }
    s += &format!("<Size>{}</Size>", o.size);
    if with_owner {
        s += &format!("<Owner>{}</Owner>", owner());
    }
    s + "<StorageClass>STANDARD</StorageClass></Contents>"
}

fn list_objects_v2(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &[
            "list-type",
            "prefix",
            "delimiter",
            "max-keys",
            "continuation-token",
            "start-after",
            "encoding-type",
            "fetch-owner",
        ],
        &[],
        false,
    )?;
    if r.query("list-type") != Some("2") {
        return Err(invalid_argument("Invalid list-type"));
    }
    let url = r.url_encoding()?;
    let token = r
        .query("continuation-token")
        .map(|t| {
            STANDARD
                .decode(t)
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| invalid_argument("The continuation token provided is incorrect"))
        })
        .transpose()?;
    let start_after = r.query("start-after").unwrap_or_default();
    let req = ListRequest {
        prefix: r.query("prefix").unwrap_or_default().into(),
        delimiter: r.query("delimiter").unwrap_or_default().into(),
        after: token.clone().unwrap_or_else(|| start_after.into()),
        max_keys: r.max("max-keys")?,
    };
    let result = ls2.list_objects(r.bucket(), &req)?;
    let with_owner = r.query("fetch-owner") == Some("true");
    let mut inner = tag("Name", r.bucket()) + &enc_tag("Prefix", &req.prefix, url);
    if !req.delimiter.is_empty() {
        inner += &enc_tag("Delimiter", &req.delimiter, url);
    }
    inner += &format!(
        "<MaxKeys>{}</MaxKeys><KeyCount>{}</KeyCount><IsTruncated>{}</IsTruncated>",
        req.max_keys,
        result.objects.len() + result.common_prefixes.len(),
        result.truncated
    );
    if url {
        inner += "<EncodingType>url</EncodingType>";
    }
    if let Some(t) = r.query("continuation-token") {
        inner += &tag("ContinuationToken", t);
    }
    if let Some(next) = &result.next_marker {
        inner += &tag("NextContinuationToken", &STANDARD.encode(next));
    }
    if !start_after.is_empty() {
        inner += &enc_tag("StartAfter", start_after, url);
    }
    for o in &result.objects {
        inner += &object_entry(o, url, with_owner);
    }
    for p in &result.common_prefixes {
        inner += &format!(
            "<CommonPrefixes>{}</CommonPrefixes>",
            enc_tag("Prefix", p, url)
        );
    }
    Ok(xml_response(xml_doc("ListBucketResult", &inner)))
}

fn list_objects_v1(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &["prefix", "delimiter", "marker", "max-keys", "encoding-type"],
        &[],
        false,
    )?;
    let url = r.url_encoding()?;
    let req = ListRequest {
        prefix: r.query("prefix").unwrap_or_default().into(),
        delimiter: r.query("delimiter").unwrap_or_default().into(),
        after: r.query("marker").unwrap_or_default().into(),
        max_keys: r.max("max-keys")?,
    };
    let result = ls2.list_objects(r.bucket(), &req)?;
    let mut inner = tag("Name", r.bucket())
        + &enc_tag("Prefix", &req.prefix, url)
        + &enc_tag("Marker", &req.after, url);
    if !req.delimiter.is_empty() {
        inner += &enc_tag("Delimiter", &req.delimiter, url);
        if let Some(next) = &result.next_marker {
            inner += &enc_tag("NextMarker", next, url);
        }
    }
    inner += &format!(
        "<MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>",
        req.max_keys, result.truncated
    );
    if url {
        inner += "<EncodingType>url</EncodingType>";
    }
    for o in &result.objects {
        inner += &object_entry(o, url, true);
    }
    for p in &result.common_prefixes {
        inner += &format!(
            "<CommonPrefixes>{}</CommonPrefixes>",
            enc_tag("Prefix", p, url)
        );
    }
    Ok(xml_response(xml_doc("ListBucketResult", &inner)))
}

fn delete_objects(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(&["delete"], &[], false)?;
    r.payload.verify()?;
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Delete {
        quiet: Option<bool>,
        #[serde(rename = "Object", default)]
        objects: Vec<Entry>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Entry {
        key: String,
        version_id: Option<String>,
        #[serde(rename = "ETag")]
        etag: Option<String>,
        last_modified_time: Option<String>,
        size: Option<String>,
    }
    let delete: Delete =
        quick_xml::de::from_reader(r.payload.data.as_ref()).map_err(|_| malformed_xml())?;
    if delete.objects.is_empty() || delete.objects.len() > MAX_DELETE_OBJECTS {
        return Err(malformed_xml());
    }
    if delete.objects.iter().any(|o| o.version_id.is_some()) {
        return Err(not_implemented(
            "Deleting specific versions is not supported",
        ));
    }
    if delete
        .objects
        .iter()
        .any(|o| o.etag.is_some() || o.last_modified_time.is_some() || o.size.is_some())
    {
        return Err(not_implemented("Conditional deletes are not supported"));
    }
    let keys: Vec<String> = delete.objects.into_iter().map(|o| o.key).collect();
    let results = ls2.delete_objects(r.bucket(), &keys)?;
    let quiet = delete.quiet.unwrap_or(false);
    let mut inner = String::new();
    for (key, result) in keys.iter().zip(results) {
        match result {
            Ok(()) if !quiet => inner += &format!("<Deleted>{}</Deleted>", tag("Key", key)),
            Ok(()) => {}
            Err(e) => {
                inner += &format!(
                    "<Error>{}{}{}</Error>",
                    tag("Key", key),
                    tag("Code", e.code()),
                    tag("Message", &e.to_string())
                );
            }
        }
    }
    Ok(xml_response(xml_doc("DeleteResult", &inner)))
}

fn list_uploads(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &[
            "uploads",
            "prefix",
            "max-uploads",
            "key-marker",
            "upload-id-marker",
            "encoding-type",
        ],
        &[],
        false,
    )?;
    let url = r.url_encoding()?;
    let prefix = r.query("prefix").unwrap_or_default();
    let key_marker = r.query("key-marker").unwrap_or_default();
    let id_marker = r.query("upload-id-marker").unwrap_or_default();
    let max = r.max("max-uploads")?;
    let (uploads, truncated) =
        ls2.list_multipart_uploads(r.bucket(), prefix, key_marker, id_marker, max)?;
    let mut inner = tag("Bucket", r.bucket())
        + &enc_tag("KeyMarker", key_marker, url)
        + &tag("UploadIdMarker", id_marker);
    if truncated && let Some(last) = uploads.last() {
        inner += &enc_tag("NextKeyMarker", &last.key, url);
        inner += &tag("NextUploadIdMarker", &last.upload_id);
    }
    if !prefix.is_empty() {
        inner += &enc_tag("Prefix", prefix, url);
    }
    inner += &format!("<MaxUploads>{max}</MaxUploads><IsTruncated>{truncated}</IsTruncated>");
    if url {
        inner += "<EncodingType>url</EncodingType>";
    }
    for u in &uploads {
        inner += &format!(
            "<Upload>{}{}<Initiator>{o}</Initiator><Owner>{o}</Owner><StorageClass>STANDARD</StorageClass>{}{}</Upload>",
            enc_tag("Key", &u.key, url),
            tag("UploadId", &u.upload_id),
            tag("Initiated", &iso8601(u.initiated_at_ms)),
            u.checksum_algorithm
                .map(|a| tag("ChecksumAlgorithm", a.name()))
                .unwrap_or_default(),
            o = owner(),
        );
    }
    Ok(xml_response(xml_doc("ListMultipartUploadsResult", &inner)))
}

// ---- object operations ----

fn header_value(s: &str) -> Result<HeaderValue, S3Error> {
    HeaderValue::from_str(s).map_err(|_| Error::Storage(rusqlite::Error::InvalidQuery).into())
}

fn checksum_headers(h: &mut HeaderMap, checksum: Option<&Checksum>) -> Result<(), S3Error> {
    if let Some(c) = checksum {
        h.insert(
            HeaderName::from_static(c.algorithm.header()),
            header_value(&c.value)?,
        );
        h.insert(
            "x-amz-checksum-type",
            HeaderValue::from_static("FULL_OBJECT"),
        );
    }
    Ok(())
}

fn put_object(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(&[], &["x-amz-storage-class", "x-amz-acl"], true)?;
    let info = ls2.put_object(
        r.bucket(),
        r.key(),
        &r.payload.data,
        PutOptions {
            metadata: r.metadata()?,
            checksum: r.payload.checksum.clone(),
            content_md5: r.payload.content_md5,
            condition: r.write_condition()?,
        },
        r.now_ms,
    )?;
    let mut response = StatusCode::OK.into_response();
    let h = response.headers_mut();
    h.insert(header::ETAG, header_value(&quoted(&info.etag))?);
    checksum_headers(h, info.checksum.as_ref())?;
    Ok(response)
}

const RESPONSE_OVERRIDES: [(&str, HeaderName); 6] = [
    ("response-content-type", header::CONTENT_TYPE),
    ("response-content-language", header::CONTENT_LANGUAGE),
    ("response-expires", header::EXPIRES),
    ("response-cache-control", header::CACHE_CONTROL),
    ("response-content-disposition", header::CONTENT_DISPOSITION),
    ("response-content-encoding", header::CONTENT_ENCODING),
];

fn get_object(ls2: &mut Ls2, r: &Req, head: bool) -> Result<Response, S3Error> {
    let overrides: Vec<&str> = RESPONSE_OVERRIDES.iter().map(|(q, _)| *q).collect();
    r.allow(&overrides, &["x-amz-checksum-mode"], false)?;
    let range = match r.header("range") {
        Some(v) if v.contains(',') => {
            return Err(not_implemented("Multiple ranges are not supported"));
        }
        Some(v) => ByteRange::parse(v),
        None => None,
    };
    let cond = r.read_condition();
    let (info, data, applied) = if head {
        let info = ls2.head_object_if(r.bucket(), r.key(), &cond)?;
        let applied = range.map(|x| x.resolve(info.size)).transpose()?;
        (info, None, applied)
    } else {
        let out = ls2.get_object(r.bucket(), r.key(), range, &cond)?;
        (out.info, Some(out.data), out.range)
    };
    let m = &info.metadata;
    let mut response = Response::new(Body::empty());
    let h = response.headers_mut();
    h.insert(header::ETAG, header_value(&quoted(&info.etag))?);
    h.insert(
        header::LAST_MODIFIED,
        header_value(&http_date(info.last_modified_ms))?,
    );
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(
        header::CONTENT_TYPE,
        header_value(m.content_type.as_deref().unwrap_or("binary/octet-stream"))?,
    );
    for (name, value) in [
        (header::CONTENT_ENCODING, &m.content_encoding),
        (header::CONTENT_DISPOSITION, &m.content_disposition),
        (header::CONTENT_LANGUAGE, &m.content_language),
        (header::CACHE_CONTROL, &m.cache_control),
        (header::EXPIRES, &m.expires),
    ] {
        if let Some(v) = value {
            h.insert(name, header_value(v)?);
        }
    }
    for (k, v) in &m.user {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(format!("x-amz-meta-{k}")),
            HeaderValue::from_str(v),
        ) {
            h.insert(name, value);
        }
    }
    for (q, name) in RESPONSE_OVERRIDES {
        if let Some(v) = r.query(q) {
            h.insert(name, header_value(v)?);
        }
    }
    h.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));
    if let Some(parts) = info.etag.split_once('-').map(|(_, n)| n) {
        h.insert("x-amz-mp-parts-count", header_value(parts)?);
    }
    if applied.is_none()
        && r.header("x-amz-checksum-mode")
            .is_some_and(|v| v.eq_ignore_ascii_case("ENABLED"))
    {
        checksum_headers(h, info.checksum.as_ref())?;
    }
    let length = match applied {
        Some((start, end)) => {
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                header_value(&format!("bytes {start}-{}/{}", end - 1, info.size))?,
            );
            end - start
        }
        None => info.size,
    };
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    if let Some(data) = data {
        *response.body_mut() = Body::from(data);
    }
    Ok(response)
}

fn copy_object(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &[],
        &[
            "x-amz-copy-source",
            "x-amz-copy-source-if-match",
            "x-amz-copy-source-if-none-match",
            "x-amz-copy-source-if-modified-since",
            "x-amz-copy-source-if-unmodified-since",
            "x-amz-metadata-directive",
            "x-amz-storage-class",
            "x-amz-acl",
        ],
        true,
    )?;
    let (src_bucket, src_key) = r.copy_source()?;
    let metadata = match r.header("x-amz-metadata-directive") {
        None | Some("COPY") => None,
        Some("REPLACE") => Some(r.metadata()?),
        Some(_) => return Err(invalid_argument("Unknown metadata directive.")),
    };
    let info = ls2.copy_object(
        &src_bucket,
        &src_key,
        &r.copy_source_condition(),
        r.bucket(),
        r.key(),
        metadata,
        r.now_ms,
    )?;
    let checksum = info
        .checksum
        .as_ref()
        .map(|c| tag(c.algorithm.xml_tag(), &c.value))
        .unwrap_or_default();
    let body = xml_doc(
        "CopyObjectResult",
        &format!(
            "{}{}{checksum}",
            tag("ETag", &quoted(&info.etag)),
            tag("LastModified", &iso8601(info.last_modified_ms))
        ),
    );
    Ok(xml_response(body))
}

fn create_upload(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &["uploads"],
        &[
            "x-amz-storage-class",
            "x-amz-acl",
            "x-amz-checksum-algorithm",
            "x-amz-checksum-type",
        ],
        true,
    )?;
    let algorithm = r
        .header("x-amz-checksum-algorithm")
        .map(|v| {
            ChecksumAlgorithm::parse(v)
                .ok_or_else(|| invalid_argument("Checksum algorithm provided is unsupported."))
        })
        .transpose()?;
    if let Some(t) = r.header("x-amz-checksum-type")
        && !matches!(t, "COMPOSITE" | "FULL_OBJECT")
    {
        return Err(invalid_argument(
            "Value for x-amz-checksum-type header is invalid.",
        ));
    }
    let id =
        ls2.create_multipart_upload(r.bucket(), r.key(), r.metadata()?, algorithm, r.now_ms)?;
    let body = xml_doc(
        "InitiateMultipartUploadResult",
        &(tag("Bucket", r.bucket()) + &tag("Key", r.key()) + &tag("UploadId", &id)),
    );
    let mut response = xml_response(body);
    if let Some(a) = algorithm {
        response.headers_mut().insert(
            "x-amz-checksum-algorithm",
            HeaderValue::from_static(a.name()),
        );
    }
    Ok(response)
}

fn upload_part(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(&["uploadId", "partNumber"], &[], false)?;
    let part = ls2.upload_part(
        r.bucket(),
        r.key(),
        r.upload_id(),
        r.part_number()?,
        &r.payload.data,
        r.payload.checksum.clone(),
        r.payload.content_md5,
        r.now_ms,
    )?;
    let mut response = StatusCode::OK.into_response();
    let h = response.headers_mut();
    h.insert(header::ETAG, header_value(&quoted(&part.etag))?);
    if let Some(c) = &part.checksum {
        h.insert(
            HeaderName::from_static(c.algorithm.header()),
            header_value(&c.value)?,
        );
    }
    Ok(response)
}

fn upload_part_copy(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &["uploadId", "partNumber"],
        &[
            "x-amz-copy-source",
            "x-amz-copy-source-range",
            "x-amz-copy-source-if-match",
            "x-amz-copy-source-if-none-match",
            "x-amz-copy-source-if-modified-since",
            "x-amz-copy-source-if-unmodified-since",
        ],
        false,
    )?;
    let (src_bucket, src_key) = r.copy_source()?;
    let range = r
        .header("x-amz-copy-source-range")
        .map(|v| match ByteRange::parse(v) {
            Some(range @ ByteRange::From { end: Some(_), .. }) => Ok(range),
            _ => Err(invalid_argument("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy")),
        })
        .transpose()?;
    let part = ls2.upload_part_copy(
        r.bucket(),
        r.key(),
        r.upload_id(),
        r.part_number()?,
        &src_bucket,
        &src_key,
        &r.copy_source_condition(),
        range,
        r.now_ms,
    )?;
    let body = xml_doc(
        "CopyPartResult",
        &(tag("ETag", &quoted(&part.etag)) + &tag("LastModified", &iso8601(part.last_modified_ms))),
    );
    Ok(xml_response(body))
}

fn complete_upload(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(
        &["uploadId"],
        &["x-amz-checksum-type", "x-amz-mp-object-size"],
        false,
    )?;
    r.payload.verify()?;
    #[derive(Deserialize)]
    struct Complete {
        #[serde(rename = "Part", default)]
        parts: Vec<Part>,
    }
    #[derive(Deserialize)]
    struct Part {
        #[serde(rename = "PartNumber")]
        part_number: u32,
        #[serde(rename = "ETag")]
        etag: String,
        #[serde(rename = "ChecksumCRC32")]
        crc32: Option<String>,
        #[serde(rename = "ChecksumCRC32C")]
        crc32c: Option<String>,
        #[serde(rename = "ChecksumCRC64NVME")]
        crc64nvme: Option<String>,
        #[serde(rename = "ChecksumSHA1")]
        sha1: Option<String>,
        #[serde(rename = "ChecksumSHA256")]
        sha256: Option<String>,
    }
    let complete: Complete =
        quick_xml::de::from_reader(r.payload.data.as_ref()).map_err(|_| malformed_xml())?;
    let parts = complete
        .parts
        .into_iter()
        .map(|p| {
            let checksum = [
                (ChecksumAlgorithm::Crc32, p.crc32),
                (ChecksumAlgorithm::Crc32c, p.crc32c),
                (ChecksumAlgorithm::Crc64Nvme, p.crc64nvme),
                (ChecksumAlgorithm::Sha1, p.sha1),
                (ChecksumAlgorithm::Sha256, p.sha256),
            ]
            .into_iter()
            .find_map(|(algorithm, v)| v.map(|value| Checksum { algorithm, value }));
            CompletedPart {
                part_number: p.part_number,
                etag: p.etag,
                checksum,
            }
        })
        .collect::<Vec<_>>();
    if let Some(size) = r.header("x-amz-mp-object-size") {
        let expected: u64 = size
            .parse()
            .map_err(|_| invalid_argument("Invalid x-amz-mp-object-size"))?;
        let total = parts_total(ls2, r, &parts)?;
        if total != expected {
            return Err(invalid_argument(
                "The provided x-amz-mp-object-size does not match the object size",
            ));
        }
    }
    let info = ls2.complete_multipart_upload(
        r.bucket(),
        r.key(),
        r.upload_id(),
        &parts,
        &r.write_condition()?,
        r.now_ms,
    )?;
    let host = r.header("host").unwrap_or("localhost");
    let location = format!(
        "http://{host}/{}/{}",
        r.bucket(),
        utf8_percent_encode(r.key(), URL_ENCODE)
    );
    let body = xml_doc(
        "CompleteMultipartUploadResult",
        &(tag("Location", &location)
            + &tag("Bucket", r.bucket())
            + &tag("Key", r.key())
            + &tag("ETag", &quoted(&info.etag))),
    );
    Ok(xml_response(body))
}

/// Total size of the listed parts, paging through ListParts.
fn parts_total(ls2: &Ls2, r: &Req, parts: &[CompletedPart]) -> Result<u64, S3Error> {
    let mut sizes = BTreeMap::new();
    let mut marker = 0;
    loop {
        let (_, page, truncated) =
            ls2.list_parts(r.bucket(), r.key(), r.upload_id(), marker, MAX_LIST)?;
        for p in &page {
            sizes.insert(p.part_number, p.size);
        }
        match (truncated, page.last()) {
            (true, Some(last)) => marker = last.part_number,
            _ => break,
        }
    }
    Ok(parts.iter().filter_map(|p| sizes.get(&p.part_number)).sum())
}

fn list_parts(ls2: &mut Ls2, r: &Req) -> Result<Response, S3Error> {
    r.allow(&["uploadId", "max-parts", "part-number-marker"], &[], false)?;
    let marker: u32 = match r.query("part-number-marker") {
        None => 0,
        Some(v) => v
            .parse()
            .map_err(|_| invalid_argument("Invalid part-number-marker"))?,
    };
    let max = r.max("max-parts")?;
    let (upload, parts, truncated) =
        ls2.list_parts(r.bucket(), r.key(), r.upload_id(), marker, max)?;
    let mut inner = format!(
        "{}{}{}<Initiator>{o}</Initiator><Owner>{o}</Owner><StorageClass>STANDARD</StorageClass><PartNumberMarker>{marker}</PartNumberMarker>",
        tag("Bucket", r.bucket()),
        tag("Key", r.key()),
        tag("UploadId", &upload.upload_id),
        o = owner()
    );
    if let Some(last) = parts.last() {
        inner += &format!(
            "<NextPartNumberMarker>{}</NextPartNumberMarker>",
            last.part_number
        );
    }
    inner += &format!("<MaxParts>{max}</MaxParts><IsTruncated>{truncated}</IsTruncated>");
    if let Some(a) = upload.checksum_algorithm {
        inner += &tag("ChecksumAlgorithm", a.name());
    }
    for p in &parts {
        let checksum = p
            .checksum
            .as_ref()
            .map(|c| tag(c.algorithm.xml_tag(), &c.value))
            .unwrap_or_default();
        inner += &format!(
            "<Part><PartNumber>{}</PartNumber>{}{}<Size>{}</Size>{checksum}</Part>",
            p.part_number,
            tag("LastModified", &iso8601(p.last_modified_ms)),
            tag("ETag", &quoted(&p.etag)),
            p.size
        );
    }
    Ok(xml_response(xml_doc("ListPartsResult", &inner)))
}
