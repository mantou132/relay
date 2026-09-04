//! Reliable WebSocket client for the durable relay server.
//!
//! The client owns the parts of the delivery contract the server cannot: an
//! outbox that retries until the relay returns `stored`, a cumulative receive
//! cursor with duplicate suppression, reconnection with exponential backoff,
//! and session preemption handling.
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

pub use relay_frame::{ClientFrame, Endpoint, ServerFrame};

use std::{sync::Arc, time::Duration};

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

/// Callbacks invoked as the connection progresses.
pub trait ClientHandler: Send + Sync + 'static {
    /// Called for each accepted, in-order payload, before the ack is sent.
    fn on_payload(&self, payload: Value);
    /// Called when the relay accepts the connection.
    fn on_connected(&self) {}
    /// Called after every disconnect.
    fn on_disconnected(&self, _error: Option<String>) {}
    /// Called when another connection with the same device_id preempts this connection.
    fn on_preempted(&self) {}
}

pub struct Client<S, H> {
    url: Arc<String>,
    ack_head: Arc<std::sync::atomic::AtomicBool>,
    store: Arc<S>,
    handler: Arc<H>,
    notify_send: Arc<tokio::sync::Notify>,
    shutdown: Arc<tokio::sync::Notify>,
    is_closed: Arc<std::sync::atomic::AtomicBool>,
}

impl<S, H> Clone for Client<S, H> {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            ack_head: self.ack_head.clone(),
            store: self.store.clone(),
            handler: self.handler.clone(),
            notify_send: self.notify_send.clone(),
            shutdown: self.shutdown.clone(),
            is_closed: self.is_closed.clone(),
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
    ) -> Self {
        Self::new_with_ack_head(url, false, store, handler)
    }

    pub fn new_with_ack_head(
        url: String,
        ack_head: bool,
        store: Arc<S>,
        handler: Arc<H>,
    ) -> Self {
        Self {
            url: Arc::new(url),
            ack_head: Arc::new(std::sync::atomic::AtomicBool::new(ack_head)),
            store,
            handler,
            notify_send: Arc::new(tokio::sync::Notify::new()),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            is_closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Signals the client to stop running and disconnect cleanly.
    pub fn close(&self) {
        self.is_closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.shutdown.notify_waiters();
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
    pub async fn into_task(self) -> anyhow::Result<()> {
        self.run_inner().await
    }

    /// Runs connect/reconnect cycles: resends un-stored outbox messages on
    /// every connect, processes inbound frames in sequence order, and backs
    /// off exponentially.
    pub async fn run(&self) -> anyhow::Result<()> {
        self.run_inner().await
    }

    async fn run_inner(&self) -> anyhow::Result<()> {
        let mut delay = MIN_RECONNECT_DELAY;
        loop {
            if self.is_closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            let mut was_connected = false;
            tokio::select! {
                _ = self.shutdown.notified() => {
                    return Ok(());
                }
                result = self.run_connection(&mut was_connected) => {
                    match result {
                        Ok(()) => self.handler.on_disconnected(None),
                        Err(error) => {
                            let err_str = error.to_string();
                            if err_str.contains("connection_replaced") {
                                self.handler.on_preempted();
                                self.handler.on_disconnected(Some(err_str));
                                return Ok(());
                            }
                            self.handler.on_disconnected(Some(err_str));
                        }
                    }
                }
            }

            if self.is_closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            delay = if was_connected {
                MIN_RECONNECT_DELAY
            } else {
                delay.mul_f64(2.0).min(MAX_RECONNECT_DELAY)
            };

            tokio::select! {
                _ = self.shutdown.notified() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn run_connection(&self, was_connected: &mut bool) -> anyhow::Result<()> {
        let should_ack_head = self.ack_head.load(std::sync::atomic::Ordering::Relaxed);
        let connect_url = if should_ack_head {
            transport::append_ack_head(&self.url)
        } else {
            self.url.as_str().to_string()
        };
        let mut transport = transport::connect(&connect_url).await?;
        self.ack_head.store(false, std::sync::atomic::Ordering::Relaxed);

        let mut sent = std::collections::HashSet::new();
        let mut ready = false;

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    return Ok(());
                }
                _ = self.notify_send.notified() => {
                    if ready {
                        self.flush_outbox(&mut transport, &mut sent).await?;
                    }
                }
                frame = transport.next_frame() => {
                    let Some(frame) = frame? else {
                        return Ok(());
                    };
                    match frame {
                        relay_frame::ServerFrame::Ready { .. } => {
                            ready = true;
                            self.handler.on_connected();
                            *was_connected = true;
                            // Resend everything un-stored once server confirms ready.
                            self.flush_outbox(&mut transport, &mut sent).await?;
                        }
                        relay_frame::ServerFrame::Stored { message_id } => {
                            self.store.remove_from_outbox(&message_id).await;
                            sent.remove(&message_id);
                        }
                        relay_frame::ServerFrame::Message {
                            sequence, payload, ..
                        } => {
                            if store::is_new_sequence(self.store.last_received().await, sequence) {
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
                            if message.starts_with("connection_replaced:") {
                                anyhow::bail!("connection_replaced: {message}");
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

pub mod memory {
    use anyhow::{Result, anyhow};
    use serde_json::Value;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::OutboundMessage;
    use crate::store::OutboxStore;

    #[derive(Default)]
    struct State {
        outbox: Vec<OutboundMessage>,
        last_received: Option<u64>,
    }

    /// Non-durable in-memory store. Payloads queued while offline survive
    /// reconnects but not process restarts; prefer a durable store in
    /// production.
    #[derive(Default)]
    pub struct MemoryStore {
        state: Mutex<State>,
    }

    impl MemoryStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait::async_trait]
    impl OutboxStore for MemoryStore {
        async fn enqueue(&self, payload: Value) -> Result<String> {
            let message_id = Uuid::new_v4().to_string();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryStore;

    struct DummyHandler;
    impl ClientHandler for DummyHandler {
        fn on_payload(&self, _payload: serde_json::Value) {}
    }

    #[tokio::test]
    async fn client_close_exits_run_loop() {
        let store = Arc::new(MemoryStore::new());
        let handler = Arc::new(DummyHandler);
        let client = Client::new("ws://127.0.0.1:9".to_string(), store, handler);

        let c = client.clone();
        let handle = tokio::spawn(async move {
            c.run().await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        client.close();

        let res = tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
        assert!(res.is_ok(), "client.run() should exit promptly after close()");
        assert!(res.unwrap().unwrap().is_ok());
    }
}

