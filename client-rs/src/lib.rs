//! Reliable WebSocket client for the durable relay server.
//!
//! The client owns the parts of the delivery contract the server cannot: an
//! outbox that retries until the relay returns `stored`, a cumulative receive
//! cursor with duplicate suppression, reconnection with exponential backoff,
//! and `connection_conflict` handling.
//!
//! Transports and storage are injected:
//!
//! - [`store::OutboxStore`] persists outbound messages and the receive
//!   cursor. Use [`memory::MemoryStore`] for tests, or a database- or
//!   file-backed store in production.
//! - [`ClientHandler`] receives inbound payloads and connection events.
//!
//! The WebSocket transport lives in [`transport`]; [`Client::run`] drives it
//! until stopped.

pub mod store;
pub mod transport;

use std::{future::Future, sync::Arc, time::Duration};

use serde_json::Value;
use store::OutboxStore;

/// Delay before the first reconnection attempt.
pub const MIN_RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// Ceiling for the exponential reconnection backoff.
pub const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

/// One outbound message awaiting `stored` from the relay.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboundMessage {
    pub message_id: String,
    pub payload: Value,
}

/// What happens after the relay reports this `(id, endpoint)` is in use.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConflictPolicy {
    /// Stop reconnecting; another client owns the slot.
    Terminal,
    /// Keep retrying with backoff. Correct once the relay drops silent
    /// (half-open) connections, which releases the slot automatically.
    Retry,
}

/// Callbacks invoked as the connection progresses.
pub trait ClientHandler: Send + Sync + 'static {
    /// Called for each accepted, in-order payload, before the ack is sent.
    fn on_payload(&self, payload: Value);
    /// Called when the relay accepts the connection.
    fn on_connected(&self) {}
    /// Called after every disconnect, including terminal ones.
    fn on_disconnected(&self, _error: Option<String>) {}
    /// Called when a `connection_conflict` frame arrives.
    fn on_conflict(&self) {}
}

/// Outcome of one connect cycle; drives the outer backoff loop.
enum CycleOutcome {
    Reconnect,
    Conflict,
}

pub struct Client<S, H> {
    url: Arc<String>,
    store: Arc<S>,
    handler: Arc<H>,
    conflict_policy: ConflictPolicy,
    notify_send: Arc<tokio::sync::Notify>,
}

impl<S, H> Clone for Client<S, H> {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            store: self.store.clone(),
            handler: self.handler.clone(),
            conflict_policy: self.conflict_policy,
            notify_send: self.notify_send.clone(),
        }
    }
}

impl<S, H> Client<S, H>
where
    S: OutboxStore,
    H: ClientHandler,
{
    pub fn new(
        url: String,
        store: Arc<S>,
        handler: Arc<H>,
        conflict_policy: ConflictPolicy,
    ) -> Self {
        Self {
            url: Arc::new(url),
            store,
            handler,
            conflict_policy,
            notify_send: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Enqueues a message into the store and notifies the connection loop to
    /// flush outbound frames immediately. Returns the generated message id.
    pub async fn send(&self, payload: Value) -> anyhow::Result<String> {
        let message_id = self.store.enqueue(payload).await?;
        self.notify_send.notify_one();
        Ok(message_id)
    }

    /// Notify the client that new messages have been enqueued into the store
    /// externally.
    pub fn notify_outbox(&self) {
        self.notify_send.notify_one();
    }

    /// Consuming variant of [`Client::run`] that can be `tokio::spawn`ed
    /// directly.
    pub fn into_task(self) -> impl Future<Output = anyhow::Result<()>> {
        async move { self.run_inner().await }
    }

    /// Runs connect/reconnect cycles: resends un-stored outbox messages on
    /// every connect, processes inbound frames in sequence order, and backs
    /// off exponentially. Returns only on a terminal conflict.
    pub async fn run(&self) -> anyhow::Result<()> {
        self.run_inner().await
    }

    async fn run_inner(&self) -> anyhow::Result<()> {
        let mut delay = MIN_RECONNECT_DELAY;
        loop {
            let mut was_connected = false;
            let outcome = self.run_connection(&mut was_connected).await;
            match outcome {
                Ok(CycleOutcome::Reconnect) => self.handler.on_disconnected(None),
                Ok(CycleOutcome::Conflict) => {
                    self.handler.on_conflict();
                    if self.conflict_policy == ConflictPolicy::Terminal {
                        self.handler.on_disconnected(None);
                        return Ok(());
                    }
                }
                Err(error) => self.handler.on_disconnected(Some(error.to_string())),
            }
            delay = if was_connected {
                MIN_RECONNECT_DELAY
            } else {
                delay.mul_f64(2.0).min(MAX_RECONNECT_DELAY)
            };
            tokio::time::sleep(delay).await;
        }
    }

    async fn run_connection(&self, was_connected: &mut bool) -> anyhow::Result<CycleOutcome> {
        let mut transport = transport::connect(&self.url).await?;
        self.handler.on_connected();
        *was_connected = true;

        let mut sent = std::collections::HashSet::new();
        // Nothing was acked while disconnected; resend everything un-stored.
        self.flush_outbox(&mut transport, &mut sent).await?;

        loop {
            tokio::select! {
                _ = self.notify_send.notified() => {
                    self.flush_outbox(&mut transport, &mut sent).await?;
                }
                frame = transport.next_frame() => {
                    let Some(frame) = frame? else {
                        return Ok(CycleOutcome::Reconnect);
                    };
                    match frame {
                        relay_frame::ServerFrame::Ready { .. } => {}
                        relay_frame::ServerFrame::Stored { message_id } => {
                            self.store.remove_from_outbox(&message_id).await;
                            sent.remove(&message_id);
                        }
                        relay_frame::ServerFrame::Message {
                            sequence, payload, ..
                        } => {
                            if store::is_new_sequence(self.store.last_received().await, sequence)? {
                                self.handler.on_payload(payload);
                                self.store.mark_received(sequence).await?;
                            }
                            // Ack every received sequence, duplicates included: the
                            // relay deletes cumulative pending rows on each ack.
                            transport
                                .send_frame(&relay_frame::ClientFrame::Ack { sequence })
                                .await?;
                        }
                        relay_frame::ServerFrame::Error { message } => {
                            if message.starts_with("connection_conflict:") {
                                return Ok(CycleOutcome::Conflict);
                            }
                            anyhow::bail!("relay rejected a frame: {message}");
                        }
                    }
                }
            }
        }
    }

    async fn flush_outbox(
        &self,
        transport: &mut transport::Transport,
        sent: &mut std::collections::HashSet<String>,
    ) -> anyhow::Result<()> {
        for message in self.store.outbox().await {
            if sent.insert(message.message_id.clone()) {
                transport
                    .send_frame(&relay_frame::ClientFrame::Message {
                        message_id: message.message_id,
                        payload: message.payload,
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

/// Wire frames shared with the relay server (`relay/src/lib.rs`). Inlined so
/// the client crate publishes independently of the server; keep in sync.
pub mod relay_frame {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
    pub enum Endpoint {
        #[serde(rename = "1")]
        One,
        #[serde(rename = "2")]
        Two,
    }

    impl Endpoint {
        /// Query-string value used by the relay URL.
        pub fn as_str(self) -> &'static str {
            match self {
                Self::One => "1",
                Self::Two => "2",
            }
        }
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum ClientFrame {
        Message {
            message_id: String,
            payload: Value,
        },
        /// Cumulative acknowledgement for all received sequences up to this one.
        Ack {
            sequence: u64,
        },
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum ServerFrame {
        Ready {
            endpoint: Endpoint,
        },
        Stored {
            message_id: String,
        },
        Message {
            message_id: String,
            sequence: u64,
            payload: Value,
        },
        Error {
            message: String,
        },
    }
}

/// Generate a process-unique message id prefix, e.g. `18f3a2b1c4d5-4d2e`.
pub fn message_prefix() -> String {
    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{started_at:x}-{:x}", std::process::id())
}

pub mod memory {
    use anyhow::{Result, anyhow};
    use serde_json::Value;
    use tokio::sync::Mutex;

    use super::{OutboundMessage, message_prefix};
    use crate::store::OutboxStore;

    #[derive(Default)]
    struct State {
        outbox: Vec<OutboundMessage>,
        last_received: Option<u64>,
    }

    /// Non-durable in-memory store. Payloads queued while offline survive
    /// reconnects but not process restarts; prefer a durable store in
    /// production.
    pub struct MemoryStore {
        prefix: String,
        next: std::sync::atomic::AtomicU64,
        state: Mutex<State>,
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self {
                prefix: message_prefix(),
                next: std::sync::atomic::AtomicU64::new(1),
                state: Mutex::default(),
            }
        }
    }

    impl MemoryStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait::async_trait]
    impl OutboxStore for MemoryStore {
        async fn enqueue(&self, payload: Value) -> Result<String> {
            let message_id = format!(
                "{}-{:x}",
                self.prefix,
                self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            self.state.lock().await.outbox.push(OutboundMessage {
                message_id: message_id.clone(),
                payload,
            });
            Ok(message_id)
        }

        async fn outbox(&self) -> Vec<OutboundMessage> {
            self.state.lock().await.outbox.clone()
        }

        async fn remove_from_outbox(&self, message_id: &str) {
            self.state
                .lock()
                .await
                .outbox
                .retain(|message| message.message_id != message_id);
        }

        async fn last_received(&self) -> Option<u64> {
            self.state.lock().await.last_received
        }

        async fn mark_received(&self, sequence: u64) -> Result<()> {
            let mut state = self.state.lock().await;
            if state.last_received.is_some_and(|last| sequence <= last) {
                return Err(anyhow!("receive cursor moved backwards"));
            }
            state.last_received = Some(sequence);
            Ok(())
        }
    }
}
