use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ls2::{
    ByteRange, ChecksumAlgorithm, ChecksumInput, CompletedPart, Error, ListRequest, Ls2,
    MIN_PART_SIZE, Metadata, PutOptions, ReadCondition, WriteCondition, checksum::Checksum,
};
use tower::ServiceExt;

const T: i64 = 1_700_000_000_000;

fn ls2_with(bucket: &str) -> Ls2 {
    let mut s = Ls2::in_memory().unwrap();
    s.create_bucket(bucket, None, T).unwrap();
    s
}

fn put(s: &mut Ls2, bucket: &str, key: &str, data: &[u8]) -> ls2::ObjectInfo {
    s.put_object(bucket, key, data, PutOptions::default(), T)
        .unwrap()
}

fn list(s: &Ls2, prefix: &str, delimiter: &str, after: &str, max: usize) -> ls2::ListResult {
    s.list_objects(
        "bkt",
        &ListRequest {
            prefix: prefix.into(),
            delimiter: delimiter.into(),
            after: after.into(),
            max_keys: max,
        },
    )
    .unwrap()
}

// ---- core ----

#[test]
fn bucket_lifecycle() {
    let mut s = Ls2::in_memory().unwrap();
    assert!(matches!(
        s.create_bucket("Bad_Name", None, T),
        Err(Error::InvalidBucketName)
    ));
    assert!(matches!(
        s.create_bucket("192.168.0.1", None, T),
        Err(Error::InvalidBucketName)
    ));
    s.create_bucket("alpha", None, T).unwrap();
    // us-east-1 re-creation by the owner succeeds; other regions conflict.
    s.create_bucket("alpha", None, T).unwrap();
    s.create_bucket("beta", Some("ap-northeast-1"), T).unwrap();
    assert!(matches!(
        s.create_bucket("beta", Some("ap-northeast-1"), T),
        Err(Error::BucketAlreadyOwnedByYou)
    ));
    assert_eq!(s.bucket("beta").unwrap().region, "ap-northeast-1");
    let (buckets, truncated) = s.list_buckets("", "", 1).unwrap();
    assert_eq!((buckets[0].name.as_str(), truncated), ("alpha", true));

    put(&mut s, "alpha", "k", b"x");
    let id = s
        .create_multipart_upload("alpha", "u", Metadata::default(), None, T)
        .unwrap();
    assert!(matches!(
        s.delete_bucket("alpha"),
        Err(Error::BucketNotEmpty)
    ));
    s.delete_object("alpha", "k").unwrap();
    s.delete_bucket("alpha").unwrap();
    assert!(matches!(s.bucket("alpha"), Err(Error::NoSuchBucket)));
    assert!(matches!(
        s.abort_multipart_upload("alpha", "u", &id),
        Err(Error::NoSuchBucket)
    ));
}

#[test]
fn put_get_head_and_ranges() {
    let mut s = ls2_with("bkt");
    let metadata = Metadata {
        content_type: Some("text/plain".into()),
        user: [("owner".to_string(), "me".to_string())].into(),
        ..Default::default()
    };
    let info = s
        .put_object(
            "bkt",
            "dir/hello.txt",
            b"hello world",
            PutOptions {
                metadata: metadata.clone(),
                ..Default::default()
            },
            T,
        )
        .unwrap();
    assert_eq!(info.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    // S3 computes CRC64NVME when the client sends no checksum.
    assert_eq!(
        info.checksum.as_ref().unwrap().algorithm,
        ChecksumAlgorithm::Crc64Nvme
    );

    let head = s.head_object("bkt", "dir/hello.txt").unwrap();
    assert_eq!((head.size, head.metadata), (11, metadata));

    let get = |range| {
        s.get_object(
            "bkt",
            "dir/hello.txt",
            Some(range),
            &ReadCondition::default(),
        )
    };
    assert_eq!(
        get(ByteRange::From {
            start: 6,
            end: None
        })
        .unwrap()
        .data,
        b"world"
    );
    assert_eq!(
        get(ByteRange::From {
            start: 0,
            end: Some(4)
        })
        .unwrap()
        .data,
        b"hello"
    );
    assert_eq!(
        get(ByteRange::From {
            start: 6,
            end: Some(999)
        })
        .unwrap()
        .range,
        Some((6, 11))
    );
    assert_eq!(get(ByteRange::Suffix(3)).unwrap().data, b"rld");
    assert!(matches!(
        get(ByteRange::From {
            start: 11,
            end: None
        }),
        Err(Error::InvalidRange)
    ));

    assert!(matches!(
        s.head_object("bkt", "missing"),
        Err(Error::NoSuchKey)
    ));
    assert!(matches!(
        s.head_object("nope", "k"),
        Err(Error::NoSuchBucket)
    ));
    put(&mut s, "bkt", "empty", b"");
    assert_eq!(
        s.get_object("bkt", "empty", None, &ReadCondition::default())
            .unwrap()
            .data,
        b""
    );
}

#[test]
fn integrity_failures_store_nothing() {
    let mut s = ls2_with("bkt");
    let wrong = Checksum {
        algorithm: ChecksumAlgorithm::Crc32,
        value: "AAAAAA==".into(),
    };
    let opts = PutOptions {
        checksum: ChecksumInput::Verify(wrong),
        ..Default::default()
    };
    assert!(matches!(
        s.put_object("bkt", "k", b"data", opts, T),
        Err(Error::BadDigest(_))
    ));
    let opts = PutOptions {
        content_md5: Some([0; 16]),
        ..Default::default()
    };
    assert!(matches!(
        s.put_object("bkt", "k", b"data", opts, T),
        Err(Error::BadDigest(_))
    ));
    assert!(matches!(s.head_object("bkt", "k"), Err(Error::NoSuchKey)));

    let right = Checksum::compute(ChecksumAlgorithm::Sha256, b"data");
    let opts = PutOptions {
        checksum: ChecksumInput::Verify(right.clone()),
        ..Default::default()
    };
    assert_eq!(
        s.put_object("bkt", "k", b"data", opts, T).unwrap().checksum,
        Some(right)
    );
    assert!(matches!(
        s.put_object("bkt", &"k".repeat(1025), b"", PutOptions::default(), T),
        Err(Error::KeyTooLong)
    ));
}

#[test]
fn conditional_requests() {
    let mut s = ls2_with("bkt");
    let create_only = |s: &mut Ls2| {
        let condition = WriteCondition {
            if_none_match_any: true,
            ..Default::default()
        };
        s.put_object(
            "bkt",
            "k",
            b"v1",
            PutOptions {
                condition,
                ..Default::default()
            },
            T,
        )
    };
    let v1 = create_only(&mut s).unwrap();
    assert!(matches!(
        create_only(&mut s),
        Err(Error::PreconditionFailed)
    ));

    let if_match = |s: &mut Ls2, etag: &str| {
        let condition = WriteCondition {
            if_match: Some(format!("\"{etag}\"")),
            ..Default::default()
        };
        s.put_object(
            "bkt",
            "k",
            b"v2",
            PutOptions {
                condition,
                ..Default::default()
            },
            T + 5_000,
        )
    };
    assert!(matches!(
        if_match(&mut s, "0000"),
        Err(Error::PreconditionFailed)
    ));
    let v2 = if_match(&mut s, &v1.etag).unwrap();

    let read = |cond: ReadCondition| s.get_object("bkt", "k", None, &cond).map(|o| o.data);
    assert!(matches!(
        read(ReadCondition {
            if_none_match: Some(format!("\"{}\"", v2.etag)),
            ..Default::default()
        }),
        Err(Error::NotModified)
    ));
    assert!(matches!(
        read(ReadCondition {
            if_match: Some(v1.etag.clone()),
            ..Default::default()
        }),
        Err(Error::PreconditionFailed)
    ));
    assert!(matches!(
        read(ReadCondition {
            if_modified_since_ms: Some(T + 5_000),
            ..Default::default()
        }),
        Err(Error::NotModified)
    ));
    assert!(matches!(
        read(ReadCondition {
            if_unmodified_since_ms: Some(T),
            ..Default::default()
        }),
        Err(Error::PreconditionFailed)
    ));
    // If-Match wins over If-Unmodified-Since.
    assert_eq!(
        read(ReadCondition {
            if_match: Some("*".into()),
            if_unmodified_since_ms: Some(T),
            ..Default::default()
        })
        .unwrap(),
        b"v2"
    );
}

#[test]
fn listing_with_prefixes_and_pagination() {
    let mut s = ls2_with("bkt");
    for key in [
        "a.txt",
        "photos/2024/1.jpg",
        "photos/2024/2.jpg",
        "photos/2025/1.jpg",
        "photos/x.jpg",
        "z/1",
        "z/2",
    ] {
        put(&mut s, "bkt", key, b"x");
    }
    let keys = |r: &ls2::ListResult| r.objects.iter().map(|o| o.key.clone()).collect::<Vec<_>>();

    let root = list(&s, "", "/", "", 1000);
    assert_eq!(keys(&root), ["a.txt"]);
    assert_eq!(root.common_prefixes, ["photos/", "z/"]);

    let photos = list(&s, "photos/", "/", "", 1000);
    assert_eq!(keys(&photos), ["photos/x.jpg"]);
    assert_eq!(photos.common_prefixes, ["photos/2024/", "photos/2025/"]);

    // Pages count common prefixes and keys together and resume after the marker.
    let mut seen = Vec::new();
    let mut after = String::new();
    loop {
        let page = list(&s, "", "/", &after, 1);
        seen.extend(keys(&page));
        seen.extend(page.common_prefixes.clone());
        match page.next_marker {
            Some(m) => after = m,
            None => break,
        }
    }
    assert_eq!(seen, ["a.txt", "photos/", "z/"]);

    assert_eq!(
        keys(&list(&s, "photos/2024/", "", "photos/2024/1.jpg", 1000)),
        ["photos/2024/2.jpg"]
    );
    assert_eq!(
        list(&s, "", "", "", 3).next_marker.as_deref(),
        Some("photos/2024/2.jpg")
    );
    assert!(list(&s, "", "", "", 0).objects.is_empty());

    let deleted = s
        .delete_objects("bkt", &["z/1".into(), "missing".into(), String::new()])
        .unwrap();
    assert!(deleted[0].is_ok() && deleted[1].is_ok() && deleted[2].is_err());
    assert_eq!(keys(&list(&s, "z/", "", "", 1000)), ["z/2"]);
}

#[test]
fn copy_object_directives() {
    let mut s = ls2_with("bkt");
    s.create_bucket("other", None, T).unwrap();
    let src_meta = Metadata {
        content_type: Some("text/plain".into()),
        ..Default::default()
    };
    let src = s
        .put_object(
            "bkt",
            "src",
            b"payload",
            PutOptions {
                metadata: src_meta.clone(),
                ..Default::default()
            },
            T,
        )
        .unwrap();

    let any = ReadCondition::default();
    let stale = ReadCondition {
        if_match: Some("\"0000\"".into()),
        ..Default::default()
    };
    assert!(matches!(
        s.copy_object("bkt", "src", &stale, "other", "dst", None, T),
        Err(Error::PreconditionFailed)
    ));
    let same = ReadCondition {
        if_none_match: Some(src.etag.clone()),
        ..Default::default()
    };
    assert!(matches!(
        s.copy_object("bkt", "src", &same, "other", "dst", None, T),
        Err(Error::PreconditionFailed)
    ));
    let copy = s
        .copy_object("bkt", "src", &any, "other", "dst", None, T + 1)
        .unwrap();
    assert_eq!(
        (copy.etag.as_str(), &copy.metadata),
        (src.etag.as_str(), &src_meta)
    );
    assert_eq!(
        s.get_object("other", "dst", None, &ReadCondition::default())
            .unwrap()
            .data,
        b"payload"
    );

    assert!(matches!(
        s.copy_object("bkt", "src", &any, "bkt", "src", None, T),
        Err(Error::InvalidRequest(_))
    ));
    let replaced = Metadata {
        content_type: Some("application/json".into()),
        ..Default::default()
    };
    s.copy_object(
        "bkt",
        "src",
        &any,
        "bkt",
        "src",
        Some(replaced.clone()),
        T + 2,
    )
    .unwrap();
    assert_eq!(s.head_object("bkt", "src").unwrap().metadata, replaced);
    assert_eq!(
        s.get_object("bkt", "src", None, &ReadCondition::default())
            .unwrap()
            .data,
        b"payload"
    );
    assert!(matches!(
        s.copy_object("bkt", "none", &any, "bkt", "x", None, T),
        Err(Error::NoSuchKey)
    ));
}

#[test]
fn multipart_upload_round_trip() {
    let mut s = ls2_with("bkt");
    let id = s
        .create_multipart_upload(
            "bkt",
            "big",
            Metadata::default(),
            Some(ChecksumAlgorithm::Crc32),
            T,
        )
        .unwrap();
    let part1 = vec![b'a'; MIN_PART_SIZE as usize];
    let up = |s: &mut Ls2, n: u32, data: &[u8], checksum: ChecksumInput| {
        s.upload_part("bkt", "big", &id, n, data, checksum, None, T)
    };
    let p1 = up(&mut s, 1, &part1, ChecksumInput::Default).unwrap();
    // The upload's algorithm is computed when the client sends none, and enforced otherwise.
    assert_eq!(
        p1.checksum.as_ref().unwrap().algorithm,
        ChecksumAlgorithm::Crc32
    );
    assert!(matches!(
        up(
            &mut s,
            2,
            b"tail",
            ChecksumInput::Compute(ChecksumAlgorithm::Sha1)
        ),
        Err(Error::InvalidRequest(_))
    ));
    let p2 = up(&mut s, 2, b"tail", ChecksumInput::Default).unwrap();
    assert!(matches!(
        up(&mut s, 0, b"x", ChecksumInput::Default),
        Err(Error::InvalidArgument(_))
    ));

    let (_, parts, truncated) = s.list_parts("bkt", "big", &id, 0, 1).unwrap();
    assert_eq!((parts[0].part_number, truncated), (1, true));
    let (uploads, _) = s.list_multipart_uploads("bkt", "", "", "", 1000).unwrap();
    assert_eq!(uploads[0].upload_id, id);

    let done = |n: u32, etag: &str| CompletedPart {
        part_number: n,
        etag: format!("\"{etag}\""),
        checksum: None,
    };
    let complete = |s: &mut Ls2, parts: &[CompletedPart]| {
        s.complete_multipart_upload("bkt", "big", &id, parts, &WriteCondition::default(), T + 1)
    };
    assert!(matches!(
        complete(&mut s, &[done(2, &p2.etag), done(1, &p1.etag)]),
        Err(Error::InvalidPartOrder)
    ));
    assert!(matches!(
        complete(&mut s, &[done(1, "bad"), done(2, &p2.etag)]),
        Err(Error::InvalidPart(_))
    ));
    assert!(matches!(
        complete(&mut s, &[done(2, &p2.etag), done(3, &p2.etag)]),
        Err(Error::InvalidPart(_))
    ));

    let info = complete(&mut s, &[done(1, &p1.etag), done(2, &p2.etag)]).unwrap();
    assert!(info.etag.ends_with("-2"));
    assert_eq!(info.size, MIN_PART_SIZE + 4);
    let tail = s
        .get_object(
            "bkt",
            "big",
            Some(ByteRange::Suffix(6)),
            &ReadCondition::default(),
        )
        .unwrap();
    assert_eq!(tail.data, b"aatail");
    assert!(matches!(
        s.list_parts("bkt", "big", &id, 0, 10),
        Err(Error::NoSuchUpload)
    ));
}

#[test]
fn multipart_rules() {
    let mut s = ls2_with("bkt");
    let id = s
        .create_multipart_upload("bkt", "k", Metadata::default(), None, T)
        .unwrap();
    let p1 = s
        .upload_part(
            "bkt",
            "k",
            &id,
            1,
            b"small",
            ChecksumInput::Default,
            None,
            T,
        )
        .unwrap();
    let p2 = s
        .upload_part("bkt", "k", &id, 2, b"last", ChecksumInput::Default, None, T)
        .unwrap();
    let parts = [
        CompletedPart {
            part_number: 1,
            etag: p1.etag,
            checksum: None,
        },
        CompletedPart {
            part_number: 2,
            etag: p2.etag,
            checksum: None,
        },
    ];
    assert!(matches!(
        s.complete_multipart_upload("bkt", "k", &id, &parts, &WriteCondition::default(), T),
        Err(Error::EntityTooSmall)
    ));
    // Upload IDs are bound to their key.
    assert!(matches!(
        s.abort_multipart_upload("bkt", "other", &id),
        Err(Error::NoSuchUpload)
    ));
    s.abort_multipart_upload("bkt", "k", &id).unwrap();
    assert!(matches!(
        s.abort_multipart_upload("bkt", "k", &id),
        Err(Error::NoSuchUpload)
    ));

    put(&mut s, "bkt", "src", b"0123456789");
    let id = s
        .create_multipart_upload("bkt", "k", Metadata::default(), None, T)
        .unwrap();
    let copied = s
        .upload_part_copy(
            "bkt",
            "k",
            &id,
            1,
            "bkt",
            "src",
            &ReadCondition::default(),
            Some(ByteRange::From {
                start: 2,
                end: Some(4),
            }),
            T,
        )
        .unwrap();
    assert_eq!(copied.size, 3);
}

#[test]
fn state_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ls2.sqlite");
    {
        let mut s = Ls2::open(&path).unwrap();
        s.create_bucket("bkt", None, T).unwrap();
        put(&mut s, "bkt", "k", b"persisted");
        s.create_multipart_upload("bkt", "u", Metadata::default(), None, T)
            .unwrap();
    }
    let s = Ls2::open(&path).unwrap();
    assert_eq!(
        s.get_object("bkt", "k", None, &ReadCondition::default())
            .unwrap()
            .data,
        b"persisted"
    );
    assert_eq!(
        s.list_multipart_uploads("bkt", "", "", "", 10)
            .unwrap()
            .0
            .len(),
        1
    );
}

// ---- HTTP ----

struct Http(axum::Router);

impl Http {
    fn new() -> Self {
        Self(ls2::server::router(Ls2::in_memory().unwrap()))
    }
    async fn send(&self, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let res = self.0.clone().oneshot(req).await.unwrap();
        let (parts, body) = res.into_parts();
        (
            parts.status,
            parts.headers,
            to_bytes(body, usize::MAX).await.unwrap().to_vec(),
        )
    }
    async fn call(
        &self,
        method: &str,
        uri: &str,
        body: impl Into<Body>,
    ) -> (StatusCode, axum::http::HeaderMap, String) {
        let (status, headers, body) = self
            .send(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(body.into())
                    .unwrap(),
            )
            .await;
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }
}

#[tokio::test]
async fn http_object_round_trip() {
    let http = Http::new();
    assert_eq!(http.call("PUT", "/bucket", "").await.0, StatusCode::OK);
    let req = Request::put("/bucket/dir/a%20b.txt")
        .header("content-type", "text/plain")
        .header("x-amz-meta-color", "blue")
        .body(Body::from("hello world"))
        .unwrap();
    let (status, headers, _) = http.send(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["etag"], "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"");
    assert!(headers.contains_key("x-amz-checksum-crc64nvme"));

    let (status, headers, body) = http
        .call("GET", "/bucket/dir/a%20b.txt", Body::empty())
        .await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, "hello world"));
    assert_eq!(headers["content-type"], "text/plain");
    assert_eq!(headers["x-amz-meta-color"], "blue");
    assert!(headers.contains_key("x-amz-request-id"));
    assert!(!headers.contains_key("x-amz-checksum-crc64nvme"));

    let req = Request::get("/bucket/dir/a%20b.txt?response-content-type=application%2Fjson")
        .header("range", "bytes=6-")
        .header("x-amz-checksum-mode", "ENABLED")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = http.send(req).await;
    assert_eq!(
        (status, body.as_slice()),
        (StatusCode::PARTIAL_CONTENT, &b"world"[..])
    );
    assert_eq!(headers["content-range"], "bytes 6-10/11");
    assert_eq!(headers["content-type"], "application/json");
    assert!(
        !headers.contains_key("x-amz-checksum-crc64nvme"),
        "partial reads omit checksums"
    );

    let (status, headers, body) = http
        .call("HEAD", "/bucket/dir/a%20b.txt", Body::empty())
        .await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, ""));
    assert_eq!(headers["content-length"], "11");

    let (status, _, body) = http.call("GET", "/bucket/missing", Body::empty()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.contains("<Code>NoSuchKey</Code>") && body.contains("<Key>missing</Key>"),
        "{body}"
    );
    let (status, headers, body) = http.call("HEAD", "/bucket/missing", Body::empty()).await;
    assert_eq!((status, body.as_str()), (StatusCode::NOT_FOUND, ""));
    assert_eq!(headers["x-amz-error-code"], "NoSuchKey");

    let req = Request::get("/bucket/dir/a%20b.txt")
        .header("if-none-match", "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
        .body(Body::empty())
        .unwrap();
    assert_eq!(http.send(req).await.0, StatusCode::NOT_MODIFIED);

    assert_eq!(
        http.call("DELETE", "/bucket/dir/a%20b.txt", Body::empty())
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        http.call("DELETE", "/bucket", Body::empty()).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        http.call("GET", "/_ls2/health", Body::empty()).await.2,
        "ok"
    );
}

#[tokio::test]
async fn http_aws_chunked_with_trailer_checksum() {
    let http = Http::new();
    http.call("PUT", "/bucket", "").await;
    let crc = ChecksumAlgorithm::Crc32.compute(b"hello world");
    let body = format!("5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:{crc}\r\n\r\n");
    let chunked = |body: String| {
        Request::put("/bucket/k")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "11")
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("x-amz-sdk-checksum-algorithm", "CRC32")
            .body(Body::from(body))
            .unwrap()
    };
    let (status, headers, _) = http.send(chunked(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-amz-checksum-crc32"], crc.as_str());

    let req = Request::get("/bucket/k")
        .header("x-amz-checksum-mode", "ENABLED")
        .body(Body::empty())
        .unwrap();
    let (_, headers, data) = http.send(req).await;
    assert_eq!(data, b"hello world");
    assert_eq!(headers["x-amz-checksum-crc32"], crc.as_str());
    assert!(
        !headers.contains_key("content-encoding"),
        "aws-chunked is not stored"
    );

    let (status, _, body) = http.send(chunked(body.replace(&crc, "AAAAAA=="))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("BadDigest"));

    let req = Request::put("/bucket/k2")
        .header("x-amz-content-sha256", "0".repeat(64))
        .body(Body::from("x"))
        .unwrap();
    let (status, _, body) = http.send(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("XAmzContentSHA256Mismatch"));
}

#[tokio::test]
async fn http_rejects_unsupported_features() {
    let http = Http::new();
    http.call("PUT", "/bucket", "").await;
    let (status, _, body) = http.call("GET", "/bucket?acl", Body::empty()).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(body.contains("Unsupported query parameter: acl"), "{body}");

    let req = Request::put("/bucket/k")
        .header("x-amz-tagging", "a=b")
        .body(Body::from("x"))
        .unwrap();
    let (status, _, body) = http.send(req).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(String::from_utf8_lossy(&body).contains("x-amz-tagging"));

    let req = Request::put("/bucket/k")
        .header("x-amz-storage-class", "GLACIER")
        .body(Body::from("x"))
        .unwrap();
    assert_eq!(http.send(req).await.0, StatusCode::NOT_IMPLEMENTED);
    // Signature and SDK hint query parameters are accepted.
    let (status, _, _) = http
        .call("PUT", "/bucket/k?x-id=PutObject&X-Amz-Signature=abc", "x")
        .await;
    assert_eq!(status, StatusCode::OK);
    let req = Request::get("/bucket/k")
        .header("x-amz-expected-bucket-owner", "123456789012")
        .body(Body::empty())
        .unwrap();
    assert_eq!(http.send(req).await.0, StatusCode::FORBIDDEN);
    assert_eq!(
        http.call("PUT", "/Bad_Bucket", "").await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn http_listing_and_batch_delete() {
    let http = Http::new();
    http.call("PUT", "/bucket", "").await;
    for key in ["a%20b", "dir/1", "dir/2", "z"] {
        http.call("PUT", &format!("/bucket/{key}"), "x").await;
    }
    let (_, _, body) = http
        .call(
            "GET",
            "/bucket?list-type=2&delimiter=%2F&encoding-type=url&max-keys=2",
            Body::empty(),
        )
        .await;
    assert!(body.contains("<Key>a%20b</Key>"), "{body}");
    assert!(body.contains("<CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes>"));
    assert!(body.contains("<KeyCount>2</KeyCount><IsTruncated>true</IsTruncated>"));
    let token = body
        .split("<NextContinuationToken>")
        .nth(1)
        .unwrap()
        .split('<')
        .next()
        .unwrap();
    let (_, _, body) = http
        .call(
            "GET",
            &format!(
                "/bucket?list-type=2&delimiter=%2F&continuation-token={}",
                token.replace('=', "%3D")
            ),
            Body::empty(),
        )
        .await;
    assert!(
        body.contains("<Key>z</Key>") && !body.contains("dir/"),
        "{body}"
    );

    let (_, _, body) = http
        .call("GET", "/bucket?prefix=dir%2F&max-keys=1", Body::empty())
        .await;
    assert!(
        body.contains("<Key>dir/1</Key>") && body.contains("<IsTruncated>true</IsTruncated>"),
        "{body}"
    );

    let (_, _, body) = http.call("GET", "/", Body::empty()).await;
    assert!(body.contains("<Name>bucket</Name>"));

    let delete = "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>dir/1</Key></Object><Object><Key>dir/2</Key></Object></Delete>";
    let (status, _, body) = http.call("POST", "/bucket?delete", delete).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.matches("<Deleted>").count(), 2);
    let (_, _, body) = http
        .call("GET", "/bucket?list-type=2&prefix=dir", Body::empty())
        .await;
    assert!(body.contains("<KeyCount>0</KeyCount>"));
    assert_eq!(
        http.call("POST", "/bucket?delete", "<Delete>").await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn http_copy_and_multipart() {
    let http = Http::new();
    http.call("PUT", "/bucket", "").await;
    http.call("PUT", "/bucket/src", "copy me").await;
    let req = Request::put("/bucket/dst")
        .header("x-amz-copy-source", "/bucket/src")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = http.send(req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("<CopyObjectResult"));
    assert_eq!(
        http.call("GET", "/bucket/dst", Body::empty()).await.2,
        "copy me"
    );

    let (_, _, body) = http
        .call("POST", "/bucket/big?uploads", Body::empty())
        .await;
    let id = body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split('<')
        .next()
        .unwrap()
        .to_owned();
    let part1 = vec![b'x'; MIN_PART_SIZE as usize];
    let req = Request::put(format!("/bucket/big?partNumber=1&uploadId={id}"))
        .body(Body::from(part1))
        .unwrap();
    let (_, headers, _) = http.send(req).await;
    let etag1 = headers["etag"].to_str().unwrap().to_owned();
    let req = Request::put(format!("/bucket/big?partNumber=2&uploadId={id}"))
        .header("x-amz-copy-source", "bucket/src")
        .header("x-amz-copy-source-range", "bytes=0-3")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = http.send(req).await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&body);
    let etag2 = body
        .split("<ETag>")
        .nth(1)
        .unwrap()
        .split('<')
        .next()
        .unwrap()
        .replace("&quot;", "\"");

    let (_, _, body) = http
        .call("GET", &format!("/bucket/big?uploadId={id}"), Body::empty())
        .await;
    assert_eq!(body.matches("<Part>").count(), 2);
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let (status, _, body) = http
        .call(
            "POST",
            &format!("/bucket/big?uploadId={id}"),
            complete.clone(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("-2&quot;</ETag>"), "{body}");
    let req = Request::get("/bucket/big")
        .header("range", "bytes=-5")
        .body(Body::empty())
        .unwrap();
    assert_eq!(http.send(req).await.2, b"xcopy");

    let (status, _, body) = http
        .call("POST", &format!("/bucket/big?uploadId={id}"), complete)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("<UploadId>"), "{body}");
}
