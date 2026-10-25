//! Event delivery to an SQS-compatible endpoint (lqs) and the notification XML.
//!
//! The worker drains the SQLite outbox; a failed send is retried with backoff and
//! never affects the S3 request that produced the event.
use super::{
    S3Error, Shared, bad, esc, invalid_argument, lock, malformed_xml, not_implemented, now, tag,
};
use crate::QueueConfiguration;
use axum::{
    body::Bytes,
    http::{Request, StatusCode, header},
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde::de::IgnoredAny;
use std::{collections::HashMap, sync::Mutex, time::Duration};

const CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the worker looks for due retries when nothing wakes it.
const IDLE_POLL: Duration = Duration::from_millis(500);
const BATCH: usize = 100;

/// A plain-HTTP SQS endpoint speaking the JSON protocol (`X-Amz-Target`).
pub(super) struct SqsClient {
    host: String,
    port: u16,
    authority: String,
    /// Queue ARN → queue URL, from `GetQueueUrl`.
    urls: Mutex<HashMap<String, String>>,
}

impl SqsClient {
    /// `http://host[:port]`. HTTPS is not supported: lqs is a local endpoint.
    pub(super) fn new(endpoint: &str) -> Result<Self, String> {
        let rest = endpoint
            .strip_prefix("http://")
            .ok_or_else(|| format!("SQS endpoint must start with http://: {endpoint}"))?;
        let authority = rest.trim_end_matches('/');
        if authority.is_empty() || authority.contains('/') {
            return Err(format!(
                "SQS endpoint must be http://host[:port]: {endpoint}"
            ));
        }
        let invalid = || format!("invalid SQS endpoint: {endpoint}");
        let (host, port) = match authority.strip_prefix('[') {
            Some(v6) => {
                let (h, rest) = v6.split_once(']').ok_or_else(invalid)?;
                if !rest.is_empty() && !rest.starts_with(':') {
                    return Err(invalid());
                }
                (h, rest.strip_prefix(':'))
            }
            None => match authority.rsplit_once(':') {
                Some((h, p)) => (h, Some(p)),
                None => (authority, None),
            },
        };
        let port = port
            .map(str::parse)
            .transpose()
            .map_err(|_| invalid())?
            .unwrap_or(80);
        Ok(Self {
            host: host.into(),
            port,
            authority: authority.into(),
            urls: Mutex::new(HashMap::new()),
        })
    }

    async fn call(
        &self,
        action: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let request = async {
            let stream = tokio::net::TcpStream::connect((self.host.as_str(), self.port))
                .await
                .map_err(|e| format!("connect {}: {e}", self.authority))?;
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .map_err(|e| e.to_string())?;
            tokio::spawn(conn);
            let request = Request::post("/")
                .header(header::HOST, &self.authority)
                .header(header::CONTENT_TYPE, "application/x-amz-json-1.0")
                .header("x-amz-target", format!("AmazonSQS.{action}"))
                .body(Full::new(Bytes::from(body.to_string())))
                .map_err(|e| e.to_string())?;
            let response = sender
                .send_request(request)
                .await
                .map_err(|e| e.to_string())?;
            let status = response.status();
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|e| e.to_string())?
                .to_bytes();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            if status != StatusCode::OK {
                let kind = value["__type"].as_str().unwrap_or("error");
                let kind = kind.rsplit('#').next().unwrap_or(kind);
                let message = value["message"]
                    .as_str()
                    .or_else(|| value["Message"].as_str())
                    .unwrap_or_default();
                return Err(format!("{action}: {status} {kind} {message}").trim().into());
            }
            Ok(value)
        };
        tokio::time::timeout(CALL_TIMEOUT, request)
            .await
            .map_err(|_| format!("{action}: timed out"))?
    }

    async fn queue_url(&self, arn: &str) -> Result<String, String> {
        if let Some(url) = self.urls.lock().ok().and_then(|u| u.get(arn).cloned()) {
            return Ok(url);
        }
        let name = arn.rsplit(':').next().unwrap_or_default();
        let value = self
            .call("GetQueueUrl", serde_json::json!({ "QueueName": name }))
            .await?;
        let url = value["QueueUrl"]
            .as_str()
            .ok_or("GetQueueUrl: no QueueUrl in response")?
            .to_owned();
        if let Ok(mut urls) = self.urls.lock() {
            urls.insert(arn.into(), url.clone());
        }
        Ok(url)
    }

    async fn send(&self, arn: &str, body: &str) -> Result<(), String> {
        let url = self.queue_url(arn).await?;
        let sent = self
            .call(
                "SendMessage",
                serde_json::json!({ "QueueUrl": url, "MessageBody": body }),
            )
            .await;
        if sent.is_err() {
            // The queue may have been recreated under another URL.
            if let Ok(mut urls) = self.urls.lock() {
                urls.remove(arn);
            }
        }
        sent.map(drop)
    }
}

/// Delivers due events until the server stops.
pub(super) async fn delivery_loop(state: Shared) {
    let Some(client) = state.sqs.clone() else {
        return;
    };
    loop {
        let due = lock(&state).and_then(|s| Ok(s.due_notifications(now(), BATCH)?));
        let due = match due {
            Ok(due) => due,
            Err(e) => {
                eprintln!("notification outbox: {}", e.message);
                Vec::new()
            }
        };
        if due.is_empty() {
            let _ = tokio::time::timeout(IDLE_POLL, state.wake.notified()).await;
            continue;
        }
        // A queue whose send failed gets nothing more this round, to keep its order.
        let mut blocked = std::collections::HashSet::new();
        for n in due {
            if blocked.contains(&n.queue_arn) {
                continue;
            }
            let result = client.send(&n.queue_arn, &n.body).await;
            if result.is_err() {
                blocked.insert(n.queue_arn.clone());
            }
            let recorded = lock(&state).and_then(|mut s| {
                match &result {
                    Ok(()) => s.notification_delivered(n.id)?,
                    Err(e) => s.notification_failed(n.id, now(), e)?,
                }
                Ok(())
            });
            if let Err(e) = &result {
                eprintln!(
                    "notification to {} failed (attempt {}): {e}",
                    n.queue_arn,
                    n.attempts + 1
                );
            }
            if let Err(e) = recorded {
                eprintln!("notification outbox: {}", e.message);
            }
        }
    }
}

/// S3 checks that every destination exists before accepting a configuration.
pub(super) async fn check_destinations(
    state: &Shared,
    configs: &[QueueConfiguration],
) -> Result<(), S3Error> {
    let unreachable = || {
        bad(
            "InvalidArgument",
            "Unable to validate the following destination configurations",
        )
    };
    let Some(client) = &state.sqs else {
        return if configs.is_empty() {
            Ok(())
        } else {
            Err(unreachable())
        };
    };
    for c in configs {
        if let Err(e) = client.queue_url(&c.queue_arn).await {
            eprintln!("notification destination {}: {e}", c.queue_arn);
            return Err(unreachable());
        }
    }
    Ok(())
}

pub(super) fn notification_xml(configs: &[QueueConfiguration]) -> String {
    let mut out = String::new();
    for c in configs {
        out += "<QueueConfiguration>";
        out += &tag("Id", c.id.as_deref().unwrap_or_default());
        out += &tag("Queue", &c.queue_arn);
        for e in &c.events {
            out += &tag("Event", e);
        }
        if c.prefix.is_some() || c.suffix.is_some() {
            out += "<Filter><S3Key>";
            for (name, value) in [("Prefix", &c.prefix), ("Suffix", &c.suffix)] {
                if let Some(v) = value {
                    out += &format!(
                        "<FilterRule><Name>{name}</Name><Value>{}</Value></FilterRule>",
                        esc(v)
                    );
                }
            }
            out += "</S3Key></Filter>";
        }
        out += "</QueueConfiguration>";
    }
    out
}

pub(super) fn parse_notification(body: &[u8]) -> Result<Vec<QueueConfiguration>, S3Error> {
    #[derive(Deserialize)]
    struct Config {
        #[serde(rename = "QueueConfiguration", default)]
        queues: Vec<Queue>,
        #[serde(rename = "TopicConfiguration", default)]
        topics: Vec<IgnoredAny>,
        #[serde(rename = "CloudFunctionConfiguration", default)]
        functions: Vec<IgnoredAny>,
        #[serde(rename = "EventBridgeConfiguration")]
        event_bridge: Option<IgnoredAny>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Queue {
        id: Option<String>,
        queue: String,
        #[serde(default)]
        event: Vec<String>,
        filter: Option<Filter>,
    }
    #[derive(Deserialize)]
    struct Filter {
        #[serde(rename = "S3Key")]
        key: Option<KeyFilter>,
    }
    #[derive(Deserialize)]
    struct KeyFilter {
        #[serde(rename = "FilterRule", default)]
        rules: Vec<Rule>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Rule {
        name: String,
        value: Option<String>,
    }
    if body.iter().all(u8::is_ascii_whitespace) {
        return Err(malformed_xml());
    }
    let config: Config = quick_xml::de::from_reader(body).map_err(|_| malformed_xml())?;
    if !config.topics.is_empty() {
        return Err(not_implemented("SNS topic notifications are not supported"));
    }
    if !config.functions.is_empty() {
        return Err(not_implemented("Lambda notifications are not supported"));
    }
    if config.event_bridge.is_some() {
        return Err(not_implemented(
            "EventBridge notifications are not supported",
        ));
    }
    config
        .queues
        .into_iter()
        .map(|q| {
            let mut c = QueueConfiguration {
                id: q.id.filter(|id| !id.is_empty()),
                queue_arn: q.queue,
                events: q.event,
                ..Default::default()
            };
            let rules = q.filter.and_then(|f| f.key).map(|k| k.rules);
            for rule in rules.unwrap_or_default() {
                let slot = match rule.name.to_ascii_lowercase().as_str() {
                    "prefix" => &mut c.prefix,
                    "suffix" => &mut c.suffix,
                    _ => {
                        return Err(invalid_argument(
                            "filter rule name must be either prefix or suffix",
                        ));
                    }
                };
                if slot.is_some() {
                    return Err(invalid_argument(
                        "Cannot specify more than one prefix or suffix rule in a filter",
                    ));
                }
                *slot = Some(rule.value.unwrap_or_default());
            }
            Ok(c)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        let c = SqsClient::new("http://127.0.0.1:9324").unwrap();
        assert_eq!((c.host.as_str(), c.port), ("127.0.0.1", 9324));
        let c = SqsClient::new("http://lqs/").unwrap();
        assert_eq!(
            (c.host.as_str(), c.port, c.authority.as_str()),
            ("lqs", 80, "lqs")
        );
        let c = SqsClient::new("http://[::1]:9324").unwrap();
        assert_eq!((c.host.as_str(), c.port), ("::1", 9324));
        assert!(SqsClient::new("https://sqs.us-east-1.amazonaws.com").is_err());
        assert!(SqsClient::new("http://lqs:9324/path").is_err());
    }
}
