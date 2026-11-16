//! Outbound HTTP notifications about device presence, so that an operator can
//! react to a device coming online, for example by starting the host that
//! serves the opposite endpoint.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use hmac::{Hmac, Mac};
use relay::Endpoint;
use serde::Serialize;
use sha2::Sha256;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ATTEMPTS: u32 = 4;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Upper bound on deliveries in flight; events beyond it are dropped rather
/// than letting a slow or unreachable receiver grow relay memory.
const MAX_IN_FLIGHT: usize = 256;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventKind {
    DeviceConnected,
    DeviceDisconnected,
}

#[derive(Debug, Serialize)]
pub(crate) struct Event {
    pub(crate) event: EventKind,
    pub(crate) id: String,
    pub(crate) endpoint: Endpoint,
    pub(crate) device_id: String,
    /// Devices of `endpoint` connected right after the event, including this
    /// one when it connected. Zero on a disconnect means the endpoint is empty.
    pub(crate) online_devices: usize,
    /// Whether any device of the opposite endpoint is connected right now.
    pub(crate) peer_online: bool,
    /// Unix time in milliseconds at which the relay observed the event.
    pub(crate) timestamp: u64,
}

impl Event {
    pub(crate) fn new(
        event: EventKind,
        id: &str,
        endpoint: Endpoint,
        device_id: &str,
        online_devices: usize,
        peer_online: bool,
    ) -> Self {
        Self {
            event,
            id: id.to_string(),
            endpoint,
            device_id: device_id.to_string(),
            online_devices,
            peer_online,
            timestamp: now_millis(),
        }
    }
}

pub(crate) struct Webhook {
    client: reqwest::Client,
    url: reqwest::Url,
    secret: Option<Vec<u8>>,
    in_flight: Arc<Semaphore>,
}

impl Webhook {
    /// Returns `None` when `url` is unset or empty, so an empty environment
    /// variable disables the webhook. An empty secret leaves requests unsigned.
    pub(crate) fn new(url: Option<String>, secret: Option<String>) -> anyhow::Result<Option<Self>> {
        let Some(url) = url.filter(|url| !url.is_empty()) else {
            return Ok(None);
        };
        let url = reqwest::Url::parse(&url)
            .map_err(|error| anyhow::anyhow!("invalid webhook URL: {error}"))?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https"),
            "webhook URL must use http or https"
        );
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("relay/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Some(Self {
            client,
            url,
            secret: secret.filter(|secret| !secret.is_empty()).map(String::into_bytes),
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }))
    }

    /// Queues an event for delivery in the background. Never blocks the caller.
    pub(crate) fn notify(self: &Arc<Self>, event: Event) {
        let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
            warn!(event = ?event.event, "webhook queue full; dropping event");
            return;
        };
        let body = match serde_json::to_vec(&event) {
            Ok(body) => body,
            Err(error) => {
                warn!(%error, "failed to serialize webhook event");
                return;
            }
        };
        let webhook = self.clone();
        tokio::spawn(async move {
            webhook.deliver(event.event, body).await;
            drop(permit);
        });
    }

    async fn deliver(&self, kind: EventKind, body: Vec<u8>) {
        let mut backoff = INITIAL_BACKOFF;
        for attempt in 1..=MAX_ATTEMPTS {
            let timestamp = (now_millis() / 1000).to_string();
            let mut request = self
                .client
                .post(self.url.clone())
                .header("content-type", "application/json")
                .header("x-relay-event", event_name(kind))
                .header("x-relay-timestamp", &timestamp);
            if let Some(secret) = &self.secret {
                request = request.header(
                    "x-relay-signature",
                    format!("sha256={}", sign(secret, &timestamp, &body)),
                );
            }
            match request.body(body.clone()).send().await {
                Ok(response) if response.status().is_success() => {
                    debug!(?kind, attempt, "webhook delivered");
                    return;
                }
                // Client errors other than throttling will not succeed on retry.
                Ok(response)
                    if response.status().is_client_error()
                        && response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS =>
                {
                    warn!(?kind, status = %response.status(), "webhook rejected");
                    return;
                }
                Ok(response) => {
                    warn!(?kind, attempt, status = %response.status(), "webhook failed");
                }
                Err(error) => {
                    warn!(?kind, attempt, %error, "webhook request failed");
                }
            }
            if attempt < MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
    }
}

fn event_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::DeviceConnected => "device_connected",
        EventKind::DeviceDisconnected => "device_disconnected",
    }
}

/// HMAC-SHA256 over `"{timestamp}.{body}"`, hex encoded.
pub(crate) fn sign(secret: &[u8], timestamp: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_known_vector() {
        // echo -n '1700000000.{}' | openssl dgst -sha256 -hmac secret
        assert_eq!(
            sign(b"secret", "1700000000", b"{}"),
            "b8569b78799ff9e3cbff0fc2d63a33a2b57f3282abd07c37ae5e8e7d79a5f163"
        );
    }
}
