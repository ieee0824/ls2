//! Lifecycle rules and event notifications (core and HTTP).
use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode},
};
use ls2::{
    Error, Expiration, LifecycleFilter, LifecycleRule, LifecycleRun, Ls2, Metadata,
    NoncurrentExpiration, PutOptions, QueueConfiguration, Tags, Versioning, parse_iso8601,
    server::Config,
};
use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tower::ServiceExt;

/// 2023-11-14T22:13:20Z.
const T: i64 = 1_700_000_000_000;
const DAY: i64 = 86_400_000;
const QUEUE: &str = "arn:aws:sqs:us-east-1:000000000000:events";

fn at(iso: &str) -> i64 {
    parse_iso8601(iso).unwrap()
}

fn ls2_with(bucket: &str) -> Ls2 {
    let mut s = Ls2::in_memory().unwrap();
    s.create_bucket(bucket, None, T).unwrap();
    s
}

fn put_at(s: &mut Ls2, key: &str, data: &[u8], tags: &[(&str, &str)], now: i64) -> ls2::ObjectInfo {
    let tags: Tags = tags
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    s.put_object(
        "bkt",
        key,
        data,
        PutOptions {
            tags,
            ..Default::default()
        },
        now,
    )
    .unwrap()
}

fn expire_after(days: u32, filter: LifecycleFilter) -> LifecycleRule {
    LifecycleRule {
        id: Some(format!("expire-{days}")),
        enabled: true,
        filter,
        expiration: Some(Expiration {
            days: Some(days),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn prefix(p: &str) -> LifecycleFilter {
    LifecycleFilter {
        prefix: p.into(),
        ..Default::default()
    }
}

fn keys(s: &Ls2) -> Vec<String> {
    s.list_objects(
        "bkt",
        &ls2::ListRequest {
            max_keys: 1000,
            ..Default::default()
        },
    )
    .unwrap()
    .objects
    .into_iter()
    .map(|o| o.key)
    .collect()
}

/// Every pending event body, oldest first.
fn events(s: &Ls2) -> Vec<Value> {
    s.due_notifications(i64::MAX, 1000)
        .unwrap()
        .into_iter()
        .map(|n| serde_json::from_str(&n.body).unwrap())
        .collect()
}

fn drain(s: &mut Ls2) {
    for n in s.due_notifications(i64::MAX, 1000).unwrap() {
        s.notification_delivered(n.id).unwrap();
    }
}

fn record(event: &Value) -> &Value {
    &event["Records"][0]
}

// ---- lifecycle (core) ----

#[test]
fn expiration_by_days_rounds_up_to_midnight() {
    let mut s = ls2_with("bkt");
    s.put_bucket_lifecycle("bkt", vec![expire_after(1, prefix("logs/"))])
        .unwrap();
    let log = put_at(&mut s, "logs/a", b"x", &[], T);
    put_at(&mut s, "data/b", b"x", &[], T);

    // Created 2023-11-14 22:13 UTC; one day later rounds up to 2023-11-16 00:00 UTC.
    let due = at("2023-11-16T00:00:00Z");
    assert_eq!(
        s.object_expiration(&log).unwrap(),
        Some((due, Some("expire-1".into())))
    );
    assert_eq!(s.apply_lifecycle(due - 1).unwrap(), LifecycleRun::default());
    let run = s.apply_lifecycle(due).unwrap();
    assert_eq!(run.expired, 1);
    assert_eq!(keys(&s), ["data/b"]);
    // Idempotent.
    assert_eq!(s.apply_lifecycle(due).unwrap(), LifecycleRun::default());
}

#[test]
fn expiration_by_date_tags_and_size() {
    let mut s = ls2_with("bkt");
    let date = at("2024-01-01T00:00:00Z");
    let rules = vec![
        LifecycleRule {
            id: Some("by-date".into()),
            enabled: true,
            filter: LifecycleFilter {
                tags: [("tmp".to_string(), "yes".to_string())].into(),
                ..Default::default()
            },
            expiration: Some(Expiration {
                date_ms: Some(date),
                ..Default::default()
            }),
            ..Default::default()
        },
        LifecycleRule {
            id: Some("big".into()),
            enabled: true,
            filter: LifecycleFilter {
                size_greater_than: Some(3),
                ..Default::default()
            },
            expiration: Some(Expiration {
                days: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        },
        LifecycleRule {
            id: Some("disabled".into()),
            enabled: false,
            ..expire_after(1, prefix(""))
        },
    ];
    s.put_bucket_lifecycle("bkt", rules).unwrap();
    put_at(&mut s, "tagged", b"x", &[("tmp", "yes")], T);
    put_at(&mut s, "other-tag", b"x", &[("tmp", "no")], T);
    put_at(&mut s, "big", b"0123", &[], T);
    put_at(&mut s, "small", b"012", &[], T);

    let run = s.apply_lifecycle(at("2023-11-16T00:00:00Z")).unwrap();
    assert_eq!(run.expired, 1, "only the big object is due");
    assert_eq!(keys(&s), ["other-tag", "small", "tagged"]);
    let run = s.apply_lifecycle(date).unwrap();
    assert_eq!(run.expired, 1);
    assert_eq!(keys(&s), ["other-tag", "small"]);
}

#[test]
fn versioned_lifecycle() {
    let mut s = ls2_with("bkt");
    s.set_bucket_versioning("bkt", Versioning::Enabled).unwrap();
    let rules = vec![
        LifecycleRule {
            id: Some("noncurrent".into()),
            enabled: true,
            filter: prefix(""),
            noncurrent_expiration: Some(NoncurrentExpiration {
                noncurrent_days: 1,
                newer_noncurrent_versions: Some(1),
            }),
            ..Default::default()
        },
        LifecycleRule {
            id: Some("markers".into()),
            enabled: true,
            filter: prefix(""),
            expiration: Some(Expiration {
                expired_object_delete_marker: true,
                ..Default::default()
            }),
            ..Default::default()
        },
        expire_after(30, prefix("current/")),
    ];
    s.put_bucket_lifecycle("bkt", rules).unwrap();
    let v1 = put_at(&mut s, "k", b"1", &[], T);
    let v2 = put_at(&mut s, "k", b"2", &[], T + 1000);
    let v3 = put_at(&mut s, "k", b"3", &[], T + 2000);
    put_at(&mut s, "current/c", b"c", &[], T);
    let versions = |s: &Ls2, key: &str| -> Vec<String> {
        s.list_object_versions(
            "bkt",
            &ls2::VersionListRequest {
                prefix: key.into(),
                max_keys: 1000,
                ..Default::default()
            },
        )
        .unwrap()
        .versions
        .into_iter()
        .map(|v| v.info.version_id)
        .collect()
    };

    // v1 and v2 became noncurrent at T+1s and T+2s; one noncurrent version is kept.
    let run = s.apply_lifecycle(at("2023-11-16T00:00:00Z")).unwrap();
    assert_eq!(run.noncurrent_expired, 1);
    assert_eq!(
        versions(&s, "k"),
        [v3.version_id.clone(), v2.version_id.clone()]
    );
    assert!(!versions(&s, "k").contains(&v1.version_id));

    // Current-version expiration in a versioned bucket adds a delete marker.
    let run = s.apply_lifecycle(at("2023-12-15T00:00:00Z")).unwrap();
    assert_eq!(run.delete_markers_created, 1);
    assert!(matches!(
        s.head_object("bkt", "current/c", None),
        Err(Error::DeleteMarker { .. })
    ));
    assert_eq!(versions(&s, "current/c").len(), 2);

    // Remove every version under "k" but leave a delete marker: it is expired next run.
    s.delete_object("bkt", "k", None, T + 3000).unwrap();
    for v in [&v2.version_id, &v3.version_id] {
        s.delete_object("bkt", "k", Some(v), T + 3000).unwrap();
    }
    assert_eq!(versions(&s, "k").len(), 1);
    let run = s.apply_lifecycle(at("2023-12-15T00:00:00Z")).unwrap();
    assert_eq!(run.delete_markers_removed, 1);
    assert!(versions(&s, "k").is_empty());
}

#[test]
fn aborts_incomplete_uploads() {
    let mut s = ls2_with("bkt");
    s.put_bucket_lifecycle(
        "bkt",
        vec![LifecycleRule {
            id: None,
            enabled: true,
            filter: prefix("tmp/"),
            abort_incomplete_upload_days: Some(2),
            ..Default::default()
        }],
    )
    .unwrap();
    // Unnamed rules get an ID, as in S3.
    assert!(
        s.get_bucket_lifecycle("bkt").unwrap().unwrap()[0]
            .id
            .is_some()
    );
    for key in ["tmp/a", "keep/b"] {
        s.create_multipart_upload("bkt", key, Metadata::default(), Tags::new(), None, T)
            .unwrap();
    }
    let run = s.apply_lifecycle(at("2023-11-17T00:00:00Z")).unwrap();
    assert_eq!(run.uploads_aborted, 1);
    let (uploads, _) = s.list_multipart_uploads("bkt", "", "", "", 1000).unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].key, "keep/b");
}

#[test]
fn lifecycle_validation() {
    let mut s = ls2_with("bkt");
    let check = |s: &mut Ls2, rules: Vec<LifecycleRule>| s.put_bucket_lifecycle("bkt", rules);
    assert!(matches!(
        check(&mut s, vec![]),
        Err(Error::InvalidArgument(_))
    ));
    let no_action = LifecycleRule {
        enabled: true,
        ..Default::default()
    };
    assert!(matches!(
        check(&mut s, vec![no_action]),
        Err(Error::InvalidRequest(_))
    ));
    let zero = expire_after(0, prefix(""));
    assert!(matches!(
        check(&mut s, vec![zero]),
        Err(Error::InvalidArgument(_))
    ));
    let not_midnight = LifecycleRule {
        expiration: Some(Expiration {
            date_ms: Some(T),
            ..Default::default()
        }),
        ..expire_after(1, prefix(""))
    };
    assert!(matches!(
        check(&mut s, vec![not_midnight]),
        Err(Error::InvalidArgument(_))
    ));
    let dup = vec![expire_after(1, prefix("a")), expire_after(1, prefix("b"))];
    assert!(matches!(check(&mut s, dup), Err(Error::InvalidArgument(_))));
    assert_eq!(s.get_bucket_lifecycle("bkt").unwrap(), None);
    assert!(matches!(
        s.get_bucket_lifecycle("missing"),
        Err(Error::NoSuchBucket)
    ));

    s.put_bucket_lifecycle("bkt", vec![expire_after(1, prefix(""))])
        .unwrap();
    s.delete_bucket_lifecycle("bkt").unwrap();
    assert_eq!(s.get_bucket_lifecycle("bkt").unwrap(), None);
}

// ---- notifications (core) ----

fn queue(events: &[&str]) -> QueueConfiguration {
    QueueConfiguration {
        id: Some("cfg".into()),
        queue_arn: QUEUE.into(),
        events: events.iter().map(|e| e.to_string()).collect(),
        ..Default::default()
    }
}

#[test]
fn notification_validation() {
    let mut s = ls2_with("bkt");
    let put = |s: &mut Ls2, c: Vec<QueueConfiguration>| s.put_bucket_notification("bkt", c, T);
    let bad_arn = QueueConfiguration {
        queue_arn: "arn:aws:sns:us-east-1:000000000000:topic".into(),
        ..queue(&["s3:ObjectCreated:*"])
    };
    assert!(matches!(
        put(&mut s, vec![bad_arn]),
        Err(Error::InvalidArgument(_))
    ));
    let fifo = QueueConfiguration {
        queue_arn: format!("{QUEUE}.fifo"),
        ..queue(&["s3:ObjectCreated:*"])
    };
    assert!(matches!(
        put(&mut s, vec![fifo]),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        put(&mut s, vec![queue(&["s3:ObjectCreated:Nope"])]),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        put(&mut s, vec![queue(&["s3:ObjectRestore:Completed"])]),
        Err(Error::NotImplemented(_))
    ));
    assert!(matches!(
        put(&mut s, vec![queue(&[])]),
        Err(Error::InvalidArgument(_))
    ));
    // Overlapping: the same event type with prefixes where one contains the other.
    let a = QueueConfiguration {
        prefix: Some("img/".into()),
        ..queue(&["s3:ObjectCreated:*"])
    };
    let b = QueueConfiguration {
        id: Some("other".into()),
        prefix: Some("img/raw/".into()),
        ..queue(&["s3:ObjectCreated:Put"])
    };
    assert!(matches!(
        put(&mut s, vec![a.clone(), b.clone()]),
        Err(Error::InvalidArgument(_))
    ));
    // Disjoint prefixes or event types are fine.
    let b = QueueConfiguration {
        prefix: Some("doc/".into()),
        ..b
    };
    let c = QueueConfiguration {
        id: Some("removed".into()),
        prefix: Some("img/".into()),
        ..queue(&["s3:ObjectRemoved:*"])
    };
    put(&mut s, vec![a, b, c]).unwrap();
    assert!(events(&s).is_empty() || events(&s).iter().all(|e| e["Event"] == "s3:TestEvent"));
    assert!(matches!(
        s.put_bucket_notification("missing", vec![], T),
        Err(Error::NoSuchBucket)
    ));
}

#[test]
fn events_are_recorded_with_writes() {
    let mut s = ls2_with("bkt");
    s.put_object("bkt", "before", b"x", PutOptions::default(), T)
        .unwrap();
    let mut created = queue(&["s3:ObjectCreated:*", "s3:ObjectTagging:*"]);
    created.suffix = Some(".txt".into());
    let removed = QueueConfiguration {
        id: None,
        ..queue(&["s3:ObjectRemoved:*", "s3:LifecycleExpiration:*"])
    };
    let stored = s
        .put_bucket_notification("bkt", vec![created, removed], T)
        .unwrap();
    assert!(stored[1].id.is_some(), "an ID is generated");
    assert_eq!(s.get_bucket_notification("bkt").unwrap(), stored);
    // One test event per queue.
    let test = events(&s);
    assert_eq!(test.len(), 1);
    assert_eq!(test[0]["Event"], "s3:TestEvent");
    assert_eq!(test[0]["Bucket"], "bkt");
    drain(&mut s);

    let info = s
        .put_object("bkt", "dir/a b+c.txt", b"hello", PutOptions::default(), T)
        .unwrap();
    s.put_object("bkt", "ignored.bin", b"x", PutOptions::default(), T)
        .unwrap();
    let e = events(&s);
    assert_eq!(e.len(), 1);
    let r = record(&e[0]);
    assert_eq!(r["eventName"], "ObjectCreated:Put");
    assert_eq!(r["eventSource"], "aws:s3");
    assert_eq!(r["eventTime"], "2023-11-14T22:13:20.000Z");
    assert_eq!(r["s3"]["configurationId"], "cfg");
    assert_eq!(r["s3"]["bucket"]["name"], "bkt");
    assert_eq!(r["s3"]["bucket"]["arn"], "arn:aws:s3:::bkt");
    assert_eq!(r["s3"]["object"]["key"], "dir/a+b%2Bc.txt");
    assert_eq!(r["s3"]["object"]["size"], 5);
    assert_eq!(r["s3"]["object"]["eTag"], info.etag.as_str());
    assert!(r["s3"]["object"].get("versionId").is_none(), "unversioned");
    let first_sequencer = r["s3"]["object"]["sequencer"].as_str().unwrap().to_owned();
    drain(&mut s);

    let src = ls2::CopySource {
        bucket: "bkt".into(),
        key: "dir/a b+c.txt".into(),
        version_id: None,
        condition: Default::default(),
    };
    s.copy_object(&src, "bkt", "copy.txt", Default::default(), T)
        .unwrap();
    s.put_object_tagging("bkt", "copy.txt", None, Tags::new(), T)
        .unwrap();
    s.delete_object_tagging("bkt", "copy.txt", None, T).unwrap();
    s.delete_object("bkt", "copy.txt", None, T).unwrap();
    // Deleting a missing key succeeds but is not an event.
    s.delete_object("bkt", "copy.txt", None, T).unwrap();
    let names: Vec<String> = events(&s)
        .iter()
        .map(|e| record(e)["eventName"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        names,
        [
            "ObjectCreated:Copy",
            "ObjectTagging:Put",
            "ObjectTagging:Delete",
            "ObjectRemoved:Delete"
        ]
    );
    let sequencer = |e: &Value| {
        record(e)["s3"]["object"]["sequencer"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert!(events(&s).iter().all(|e| sequencer(e) > first_sequencer));
    drain(&mut s);

    // Versioned: delete markers and version IDs.
    s.set_bucket_versioning("bkt", Versioning::Enabled).unwrap();
    let v = s
        .put_object("bkt", "v.txt", b"1", PutOptions::default(), T)
        .unwrap();
    let marker = s.delete_object("bkt", "v.txt", None, T).unwrap();
    s.delete_object("bkt", "v.txt", Some(&v.version_id), T)
        .unwrap();
    let e = events(&s);
    assert_eq!(
        record(&e[0])["s3"]["object"]["versionId"],
        v.version_id.as_str()
    );
    assert_eq!(
        record(&e[1])["eventName"],
        "ObjectRemoved:DeleteMarkerCreated"
    );
    assert_eq!(
        record(&e[1])["s3"]["object"]["versionId"],
        marker.version_id.unwrap().as_str()
    );
    assert_eq!(record(&e[2])["eventName"], "ObjectRemoved:Delete");
    drain(&mut s);

    // Lifecycle events come from S3 itself.
    s.put_bucket_lifecycle("bkt", vec![expire_after(1, prefix("dir/"))])
        .unwrap();
    s.apply_lifecycle(T + 2 * DAY).unwrap();
    let e = events(&s);
    assert_eq!(e.len(), 1);
    // Due at once, even though the run used a later time.
    assert_eq!(s.due_notifications(T, 10).unwrap().len(), 1);
    assert_eq!(
        record(&e[0])["eventName"],
        "LifecycleExpiration:DeleteMarkerCreated"
    );
    assert_eq!(
        record(&e[0])["userIdentity"]["principalId"],
        "s3.amazonaws.com"
    );

    // An empty configuration turns notifications off.
    s.put_bucket_notification("bkt", vec![], T).unwrap();
    assert!(s.get_bucket_notification("bkt").unwrap().is_empty());
}

#[test]
fn failed_deliveries_back_off_and_give_up() {
    let mut s = ls2_with("bkt");
    s.put_bucket_notification("bkt", vec![queue(&["s3:ObjectCreated:*"])], T)
        .unwrap();
    s.put_object("bkt", "k", b"x", PutOptions::default(), T)
        .unwrap();
    let due = s.due_notifications(T, 10).unwrap();
    assert_eq!(due.len(), 2);
    let id = due[0].id;
    s.notification_failed(id, T, "connection refused").unwrap();
    // The later event waits behind the one being retried.
    assert!(s.due_notifications(T + 499, 10).unwrap().is_empty());
    let retry = s.due_notifications(T + 500, 10).unwrap();
    assert_eq!(retry[0].attempts, 1);
    s.notification_failed(id, T + 500, "again").unwrap();
    assert!(s.due_notifications(T + 1499, 10).unwrap().is_empty());
    assert_eq!(s.due_notifications(T + 1500, 10).unwrap().len(), 2);
    for _ in 2..ls2::MAX_DELIVERY_ATTEMPTS {
        s.notification_failed(id, T, "again").unwrap();
    }
    // Given up: kept for inspection, and the next event is no longer held back.
    let due = s.due_notifications(i64::MAX, 10).unwrap();
    assert_eq!(due.len(), 1);
    assert_ne!(due[0].id, id);
    let stats = s.notification_stats().unwrap();
    assert_eq!((stats.pending, stats.failed), (1, 1));
    assert_eq!(stats.last_error.as_deref(), Some("again"));
}

// ---- HTTP ----

struct Http(axum::Router);

impl Http {
    fn new(config: Config) -> Self {
        Self(ls2::server::try_router_with_config(Ls2::in_memory().unwrap(), config).unwrap())
    }
    async fn send(&self, req: Request<Body>) -> (StatusCode, HeaderMap, String) {
        let res = self.0.clone().oneshot(req).await.unwrap();
        let (parts, body) = res.into_parts();
        let body = to_bytes(body, usize::MAX).await.unwrap();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&body).into_owned(),
        )
    }
    async fn call(&self, method: &str, uri: &str, body: &str) -> (StatusCode, HeaderMap, String) {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body.to_owned()))
            .unwrap();
        self.send(req).await
    }
}

#[tokio::test]
async fn http_lifecycle() {
    let http = Http::new(Config::default());
    http.call("PUT", "/bkt", "").await;
    let (status, _, body) = http.call("GET", "/bkt?lifecycle", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchLifecycleConfiguration"), "{body}");

    let config = "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
        <Rule><ID>logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>\
          <Expiration><Days>1</Days></Expiration></Rule>\
        <Rule><ID>tmp</ID><Filter><And><Prefix>tmp/</Prefix><Tag><Key>a</Key><Value>1</Value></Tag>\
          <ObjectSizeGreaterThan>10</ObjectSizeGreaterThan></And></Filter><Status>Disabled</Status>\
          <NoncurrentVersionExpiration><NoncurrentDays>3</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>\
        <Rule><ID>legacy</ID><Prefix>up/</Prefix><Status>Enabled</Status>\
          <Expiration><Date>2030-01-01T00:00:00.000Z</Date></Expiration>\
          <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>\
        <Rule><Filter></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>\
        </LifecycleConfiguration>";
    let (status, headers, body) = http.call("PUT", "/bkt?lifecycle", config).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(headers.contains_key("x-amz-transition-default-minimum-object-size"));
    let (_, _, body) = http.call("GET", "/bkt?lifecycle", "").await;
    for part in [
        "<Rule><ID>logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>",
        "<And><Prefix>tmp/</Prefix><Tag><Key>a</Key><Value>1</Value></Tag><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan></And>",
        "<Status>Disabled</Status><NoncurrentVersionExpiration><NoncurrentDays>3</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration>",
        "<Filter><Prefix>up/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>2030-01-01T00:00:00.000Z</Date></Expiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>",
        "<Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>",
    ] {
        assert!(body.contains(part), "missing {part} in {body}");
    }

    // x-amz-expiration on PUT, GET and HEAD.
    let (_, headers, _) = http.call("PUT", "/bkt/logs/a", "x").await;
    let expiration = headers["x-amz-expiration"].to_str().unwrap().to_owned();
    assert!(expiration.starts_with("expiry-date=\""), "{expiration}");
    assert!(
        expiration.ends_with("GMT\", rule-id=\"logs\""),
        "{expiration}"
    );
    let (_, headers, _) = http.call("HEAD", "/bkt/logs/a", "").await;
    assert_eq!(headers["x-amz-expiration"].to_str().unwrap(), expiration);
    let (_, headers, _) = http.call("GET", "/bkt/other", "").await;
    assert!(!headers.contains_key("x-amz-expiration"));

    // Running the rules: in the future the log expires.
    let (status, _, body) = http
        .call("POST", "/_ls2/lifecycle?now=2099-01-01T00:00:00Z", "")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let run: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(run["expired"], 1);
    assert_eq!(
        http.call("GET", "/bkt/logs/a", "").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        http.call("POST", "/_ls2/lifecycle?now=yesterday", "")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        http.call("GET", "/_ls2/lifecycle", "").await.0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    let cross_site = Request::post("/_ls2/lifecycle")
        .header("origin", "http://evil.test")
        .body(Body::empty())
        .unwrap();
    assert_eq!(http.send(cross_site).await.0, StatusCode::FORBIDDEN);

    // Unsupported or malformed rules fail loudly.
    let transition = "<LifecycleConfiguration><Rule><ID>t</ID><Filter/><Status>Enabled</Status>\
        <Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>";
    assert_eq!(
        http.call("PUT", "/bkt?lifecycle", transition).await.0,
        StatusCode::NOT_IMPLEMENTED
    );
    for bad in [
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        "<LifecycleConfiguration><Rule><Filter><Prefix>a</Prefix><Tag><Key>k</Key><Value>v</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        "<LifecycleConfiguration><Rule><Filter/><Status>On</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        "<LifecycleConfiguration><Rule><Filter/><Status>Enabled</Status><Unknown/></Rule></LifecycleConfiguration>",
    ] {
        let (status, _, body) = http.call("PUT", "/bkt?lifecycle", bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
    }
    let (status, _, body) = http.call("PUT", "/bkt?lifecycle", "<LifecycleConfiguration><Rule><Filter/><Status>Enabled</Status><Expiration><Date>2030-01-01T12:00:00Z</Date></Expiration></Rule></LifecycleConfiguration>").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("midnight"), "{body}");

    assert_eq!(
        http.call("DELETE", "/bkt?lifecycle", "").await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        http.call("GET", "/bkt?lifecycle", "").await.0,
        StatusCode::NOT_FOUND
    );
}

fn notification_config(prefix: &str) -> String {
    format!(
        "<NotificationConfiguration><QueueConfiguration><Id>cfg</Id><Queue>{QUEUE}</Queue>\
         <Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:Delete</Event>\
         <Filter><S3Key><FilterRule><Name>prefix</Name><Value>{prefix}</Value></FilterRule>\
         <FilterRule><Name>Suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter>\
         </QueueConfiguration></NotificationConfiguration>"
    )
}

#[tokio::test]
async fn http_notification_configuration() {
    let http = Http::new(Config::default());
    http.call("PUT", "/bkt", "").await;
    let (status, _, body) = http.call("GET", "/bkt?notification", "").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<NotificationConfiguration"), "{body}");

    // Without an SQS endpoint the destination cannot be validated.
    let (status, _, body) = http
        .call("PUT", "/bkt?notification", &notification_config("in/"))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("Unable to validate"), "{body}");
    let skip = Request::put("/bkt?notification")
        .header("x-amz-skip-destination-validation", "true")
        .body(Body::from(notification_config("in/")))
        .unwrap();
    assert_eq!(http.send(skip).await.0, StatusCode::OK);
    let (_, _, body) = http.call("GET", "/bkt?notification", "").await;
    assert!(
        body.contains(&format!("<QueueConfiguration><Id>cfg</Id><Queue>{QUEUE}</Queue><Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:Delete</Event><Filter><S3Key><FilterRule><Name>Prefix</Name><Value>in/</Value></FilterRule><FilterRule><Name>Suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></QueueConfiguration>")),
        "{body}"
    );
    let (_, _, body) = http.call("GET", "/_ls2/status", "").await;
    let status: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        status["notifications"]["pending"], 1,
        "the test event waits"
    );

    for (config, expected) in [
        (
            "<NotificationConfiguration><TopicConfiguration><Topic>arn:aws:sns:us-east-1:000000000000:t</Topic><Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>",
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            "<NotificationConfiguration><EventBridgeConfiguration></EventBridgeConfiguration></NotificationConfiguration>",
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            "<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:q</Queue><Event>s3:ObjectCreated:Bogus</Event></QueueConfiguration></NotificationConfiguration>",
            StatusCode::BAD_REQUEST,
        ),
        ("not xml", StatusCode::BAD_REQUEST),
    ] {
        let (status, _, body) = http.call("PUT", "/bkt?notification", config).await;
        assert_eq!(status, expected, "{config}: {body}");
    }
    assert_eq!(
        http.call("PUT", "/missing?notification", &notification_config(""))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    // Removing the configuration needs no destination.
    let (status, _, _) = http
        .call("PUT", "/bkt?notification", "<NotificationConfiguration/>")
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, _, body) = http.call("GET", "/bkt?notification", "").await;
    assert!(!body.contains("QueueConfiguration"), "{body}");
}

/// A minimal SQS JSON endpoint that records `SendMessage` bodies and fails the
/// first `fail_first` sends.
async fn fake_sqs(fail_first: usize) -> (String, Arc<Mutex<Vec<String>>>) {
    use axum::{http::HeaderMap, response::IntoResponse};
    let sent: Arc<Mutex<Vec<String>>> = Arc::default();
    let failures = Arc::new(Mutex::new(fail_first));
    let (recorded, port_holder) = (sent.clone(), Arc::new(Mutex::new(0u16)));
    let port_for_handler = port_holder.clone();
    let app = axum::Router::new().fallback(move |headers: HeaderMap, body: String| {
        let (sent, failures, port) = (recorded.clone(), failures.clone(), port_for_handler.clone());
        async move {
            let target = headers["x-amz-target"].to_str().unwrap().to_owned();
            let request: Value = serde_json::from_str(&body).unwrap();
            match target.as_str() {
                "AmazonSQS.GetQueueUrl" if request["QueueName"] == "events" => {
                    let port = *port.lock().unwrap();
                    let url = format!("http://127.0.0.1:{port}/000000000000/events");
                    axum::Json(serde_json::json!({ "QueueUrl": url })).into_response()
                }
                "AmazonSQS.GetQueueUrl" => (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({
                        "__type": "com.amazonaws.sqs#QueueDoesNotExist",
                        "message": "The specified queue does not exist."
                    })),
                )
                    .into_response(),
                "AmazonSQS.SendMessage" => {
                    let mut left = failures.lock().unwrap();
                    if *left > 0 {
                        *left -= 1;
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                    assert!(request["QueueUrl"].as_str().unwrap().ends_with("/events"));
                    sent.lock()
                        .unwrap()
                        .push(request["MessageBody"].as_str().unwrap().to_owned());
                    axum::Json(serde_json::json!({ "MessageId": "m" })).into_response()
                }
                other => panic!("unexpected {other}"),
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    *port_holder.lock().unwrap() = port;
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://127.0.0.1:{port}"), sent)
}

async fn wait_for(sent: &Mutex<Vec<String>>, n: usize) -> Vec<Value> {
    for _ in 0..100 {
        let got = sent.lock().unwrap().clone();
        if got.len() >= n {
            return got
                .iter()
                .map(|b| serde_json::from_str(b).unwrap())
                .collect();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("only {:?} delivered", sent.lock().unwrap());
}

#[tokio::test]
async fn http_delivers_events_to_sqs() {
    let (endpoint, sent) = fake_sqs(1).await;
    let http = Http::new(Config {
        sqs_endpoint: Some(endpoint),
        ..Config::default()
    });
    http.call("PUT", "/bkt", "").await;
    let missing = notification_config("in/").replace(":events<", ":nope<");
    let (status, _, body) = http.call("PUT", "/bkt?notification", &missing).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _, body) = http
        .call("PUT", "/bkt?notification", &notification_config("in/"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    http.call("PUT", "/bkt/in/a.txt", "hello").await;
    http.call("PUT", "/bkt/out/b.txt", "skipped").await;
    http.call("DELETE", "/bkt/in/a.txt", "").await;

    // The first send fails and is retried; order within the queue is kept.
    let got = wait_for(&sent, 3).await;
    assert_eq!(got[0]["Event"], "s3:TestEvent");
    assert_eq!(got[1]["Records"][0]["eventName"], "ObjectCreated:Put");
    assert_eq!(got[1]["Records"][0]["s3"]["object"]["key"], "in/a.txt");
    assert_eq!(got[2]["Records"][0]["eventName"], "ObjectRemoved:Delete");
    let (_, _, body) = http.call("GET", "/_ls2/status", "").await;
    let status: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status["notifications"]["pending"], 0);
    assert_eq!(status["notifications"]["failed"], 0);
}

#[tokio::test]
async fn background_lifecycle_runs() {
    let mut s = Ls2::in_memory().unwrap();
    s.create_bucket("bkt", None, T).unwrap();
    s.put_bucket_lifecycle("bkt", vec![expire_after(1, prefix(""))])
        .unwrap();
    // Old enough to be due now.
    s.put_object("bkt", "old", b"x", PutOptions::default(), T)
        .unwrap();
    let router = ls2::server::try_router_with_config(
        s,
        Config {
            lifecycle_interval: Some(Duration::from_millis(50)),
            ..Config::default()
        },
    )
    .unwrap();
    let http = Http(router);
    for _ in 0..100 {
        if http.call("HEAD", "/bkt/old", "").await.0 == StatusCode::NOT_FOUND {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("lifecycle never ran");
}

#[test]
fn workers_need_a_runtime() {
    let config = Config {
        lifecycle_interval: Some(Duration::from_secs(1)),
        ..Config::default()
    };
    assert!(ls2::server::try_router_with_config(Ls2::in_memory().unwrap(), config).is_err());
    let config = Config {
        sqs_endpoint: Some("https://sqs.example".into()),
        ..Config::default()
    };
    assert!(ls2::server::try_router_with_config(Ls2::in_memory().unwrap(), config).is_err());
}
