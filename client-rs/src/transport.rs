//! WebSocket transport for the relay client.
//!
//! Connects to the relay URL, sends protocol-level pings so the server's
//! idle timeout sees this client as alive, and decodes [`ServerFrame`]s from
//! text messages and [`Inbound::Binary`] from binary messages. The server's
//! pings are answered automatically by tungstenite.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Notify;
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};

use crate::relay_frame::{
    BinaryHeader, ClientFrame, Endpoint, ServerFrame, decode_binary_frame, encode_binary_frame,
};

const OUTBOUND_QUEUE_CAPACITY: usize = 16;

type CoalesceKey = (Option<String>, String);

struct OutboundEntry {
    message: Message,
    replace_key: Option<CoalesceKey>,
}

enum PushOutcome {
    Queued,
    Full(Message, Option<CoalesceKey>),
    Closed,
}

#[derive(Default)]
struct OutboundState {
    entries: VecDeque<OutboundEntry>,
    sender_closed: bool,
    receiver_closed: bool,
}

impl OutboundState {
    fn try_push(
        &mut self,
        message: Message,
        replace_key: Option<CoalesceKey>,
        capacity: usize,
    ) -> PushOutcome {
        if self.receiver_closed {
            return PushOutcome::Closed;
        }
        if let Some(key) = &replace_key
            && let Some(slot) = self
                .entries
                .iter_mut()
                .find(|entry| entry.replace_key.as_ref() == Some(key))
        {
            slot.message = message;
            return PushOutcome::Queued;
        }
        if self.entries.len() >= capacity {
            return PushOutcome::Full(message, replace_key);
        }
        self.entries.push_back(OutboundEntry {
            message,
            replace_key,
        });
        PushOutcome::Queued
    }

    fn pop_front(&mut self) -> Option<Message> {
        self.entries.pop_front().map(|entry| entry.message)
    }
}

struct OutboundQueue {
    state: Mutex<OutboundState>,
    capacity: usize,
    not_empty: Notify,
    not_full: Notify,
}

struct SenderGuard(Arc<OutboundQueue>);

impl Drop for SenderGuard {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("outbound queue lock poisoned")
            .sender_closed = true;
        self.0.not_empty.notify_one();
    }
}

struct OutboundReceiver {
    queue: Arc<OutboundQueue>,
}

impl OutboundReceiver {
    async fn recv(&mut self) -> Option<Message> {
        loop {
            let notified = self.queue.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let next = {
                let mut state = self
                    .queue
                    .state
                    .lock()
                    .expect("outbound queue lock poisoned");
                if let Some(message) = state.pop_front() {
                    Some(message)
                } else if state.sender_closed {
                    return None;
                } else {
                    None
                }
            };

            if let Some(message) = next {
                self.queue.not_full.notify_one();
                return Some(message);
            }

            notified.await;
        }
    }
}

impl Drop for OutboundReceiver {
    fn drop(&mut self) {
        let mut state = self
            .queue
            .state
            .lock()
            .expect("outbound queue lock poisoned");
        state.receiver_closed = true;
        state.entries.clear();
        drop(state);
        self.queue.not_full.notify_waiters();
    }
}

fn outbound_channel(capacity: usize) -> (TransportSender, OutboundReceiver) {
    let queue = Arc::new(OutboundQueue {
        state: Mutex::new(OutboundState::default()),
        capacity,
        not_empty: Notify::new(),
        not_full: Notify::new(),
    });
    (
        TransportSender(Arc::new(SenderGuard(queue.clone()))),
        OutboundReceiver { queue },
    )
}

/// One message received from the relay.
#[derive(Debug)]
pub enum Inbound {
    Frame(ServerFrame),
    /// An ephemeral binary message from the peer.
    Binary { message_id: String, body: Bytes },
}

/// Cloneable handle that writes to a connected transport from outside the
/// connection loop. Sends wait while the socket's outgoing queue is full,
/// except keyed ephemeral frames that coalesce with an already-queued frame
/// sharing the same `(target_device_id, replace_key)`.
#[derive(Clone)]
pub struct TransportSender(Arc<SenderGuard>);

impl TransportSender {
    pub async fn send_frame(&self, frame: &ClientFrame) -> Result<()> {
        let json = serde_json::to_string(frame).context("failed to encode relay frame")?;
        let replace_key = match frame {
            ClientFrame::Message {
                ephemeral: true,
                target_device_id,
                replace_key: Some(key),
                ..
            } => Some((target_device_id.clone(), key.clone())),
            _ => None,
        };
        self.send(Message::Text(json.into()), replace_key).await
    }

    pub async fn send_binary(&self, header: &BinaryHeader, body: &[u8]) -> Result<()> {
        let replace_key = header
            .replace_key
            .as_ref()
            .map(|key| (header.target_device_id.clone(), key.clone()));
        self.send(
            Message::Binary(encode_binary_frame(header, body).into()),
            replace_key,
        )
        .await
    }

    async fn send(&self, message: Message, replace_key: Option<CoalesceKey>) -> Result<()> {
        let queue = &self.0.0;
        let mut pending = Some((message, replace_key));
        loop {
            let notified = queue.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let (message, replace_key) = pending.take().expect("pending outbound message");
            let outcome = queue
                .state
                .lock()
                .expect("outbound queue lock poisoned")
                .try_push(message, replace_key, queue.capacity);
            match outcome {
                PushOutcome::Queued => {
                    queue.not_empty.notify_one();
                    return Ok(());
                }
                PushOutcome::Closed => {
                    anyhow::bail!("failed to send relay frame");
                }
                PushOutcome::Full(message, replace_key) => {
                    pending = Some((message, replace_key));
                }
            }

            notified.await;
        }
    }
}

/// Interval for client pings. Keep well under the relay's idle timeout (90 s
/// by default) so the relay never considers this connection half-open.
pub const PING_INTERVAL: Duration = Duration::from_secs(25);

type Socket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Builds the connect URL with pairing id, endpoint, and device id query parameters.
pub fn endpoint_url(
    relay_url: &str,
    relay_id: &str,
    endpoint: Endpoint,
    device_id: &str,
) -> String {
    format!(
        "{relay_url}?id={relay_id}&endpoint={}&device_id={device_id}",
        endpoint.as_str()
    )
}

/// Appends `ack_head=true` query parameter to a connect URL.
pub fn append_ack_head(url: &str) -> String {
    append_query(url, "ack_head=true")
}

/// Appends one `key=value` query parameter to a connect URL.
pub fn append_query(url: &str, parameter: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{parameter}")
}

pub async fn connect(url: &str) -> Result<Transport> {
    let (socket, _) = connect_async(url)
        .await
        .with_context(|| format!("failed to connect to relay at {url}"))?;
    let (mut sink, stream) = socket.split();
    // Pings and data frames share one sink, so a single task owns it and
    // receives both ping ticks and outbound frames from a coalescing queue.
    let (sender, mut frame_rx) = outbound_channel(OUTBOUND_QUEUE_CAPACITY);
    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PING_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.reset();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                        return;
                    }
                }
                frame = frame_rx.recv() => {
                    let Some(frame) = frame else { break };
                    if sink.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    Ok(Transport {
        sender,
        stream,
        ping_task,
    })
}

/// A connected relay socket. Dropping it aborts the ping task and closes the
/// connection.
pub struct Transport {
    sender: TransportSender,
    stream: futures_util::stream::SplitStream<Socket>,
    ping_task: tokio::task::JoinHandle<()>,
}

impl Transport {
    /// Sends one client frame as JSON text.
    pub async fn send_frame(&mut self, frame: &ClientFrame) -> Result<()> {
        self.sender.send_frame(frame).await
    }

    pub fn sender(&self) -> TransportSender {
        self.sender.clone()
    }

    /// Waits for the next server message. Returns `Ok(None)` on a clean close.
    pub async fn next_frame(&mut self) -> Result<Option<Inbound>> {
        loop {
            let Some(message) = self.stream.next().await else {
                return Ok(None);
            };
            match message {
                Ok(Message::Text(text)) => {
                    let frame = serde_json::from_str::<ServerFrame>(&text)
                        .context("relay returned an invalid frame")?;
                    return Ok(Some(Inbound::Frame(frame)));
                }
                Ok(Message::Binary(bytes)) => {
                    let (header, body_start) =
                        decode_binary_frame(&bytes).map_err(anyhow::Error::msg)?;
                    return Ok(Some(Inbound::Binary {
                        message_id: header.message_id,
                        body: bytes.slice(body_start..),
                    }));
                }
                Ok(Message::Close(_)) => return Ok(None),
                Ok(Message::Ping(_) | Message::Pong(_)) => continue,
                Ok(_) => continue,
                Err(error) => {
                    // Conflicts arrive as a text frame followed by a clean
                    // close, so transport errors here are genuine failures.
                    return Err(error.into());
                }
            }
        }
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.ping_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn endpoint_url_appends_query() {
        assert_eq!(
            endpoint_url("ws://127.0.0.1:39371/ws", "abc", Endpoint::One, "phone-1"),
            "ws://127.0.0.1:39371/ws?id=abc&endpoint=1&device_id=phone-1"
        );
    }

    #[tokio::test]
    async fn outbound_queue_coalesces_matching_target_and_replace_key() {
        let (tx, mut rx) = outbound_channel(16);

        let bin = |id: &str, target: Option<&str>, key: Option<&str>, body: &[u8]| {
            let header = BinaryHeader {
                message_id: id.into(),
                target_device_id: target.map(Into::into),
                replace_key: key.map(Into::into),
            };
            (header, body.to_vec())
        };

        // Same (target, key) coalesces; different target or key stays separate.
        let (h1, b1) = bin("m1", Some("phone_a"), Some("live"), b"a-1");
        let (h2, b2) = bin("m2", Some("phone_b"), Some("live"), b"b-1");
        let (h3, b3) = bin("m3", None, Some("live"), b"broadcast-1");
        let (h4, b4) = bin("m4", Some("phone_a"), Some("cursor"), b"cursor-1");
        let (h5, b5) = bin("m5", Some("phone_a"), Some("live"), b"a-2");
        tx.send_binary(&h1, &b1).await.unwrap();
        tx.send_binary(&h2, &b2).await.unwrap();
        tx.send_binary(&h3, &b3).await.unwrap();
        tx.send_binary(&h4, &b4).await.unwrap();
        // Durable message in between must not be dropped or reordered behind a-2.
        tx.send_frame(&ClientFrame::Message {
            message_id: "d1".into(),
            payload: json!({ "durable": 1 }),
            target_device_id: Some("phone_a".into()),
            ephemeral: false,
            replace_key: None,
        })
        .await
        .unwrap();
        tx.send_binary(&h5, &b5).await.unwrap();
        // Ephemeral text with the same (target, key) replaces the binary frame.
        tx.send_frame(&ClientFrame::Message {
            message_id: "t1".into(),
            payload: json!({ "latest": true }),
            target_device_id: Some("phone_a".into()),
            ephemeral: true,
            replace_key: Some("live".into()),
        })
        .await
        .unwrap();
        drop(tx);

        let mut received = Vec::new();
        while let Some(msg) = rx.recv().await {
            match msg {
                Message::Binary(bytes) => {
                    let (header, body_start) = decode_binary_frame(&bytes).unwrap();
                    received.push(format!(
                        "bin:{}:{}",
                        header.message_id,
                        std::str::from_utf8(&bytes[body_start..]).unwrap()
                    ));
                }
                Message::Text(text) => {
                    let frame: ClientFrame = serde_json::from_str(&text).unwrap();
                    match frame {
                        ClientFrame::Message { message_id, .. } => {
                            received.push(format!("text:{message_id}"));
                        }
                        ClientFrame::Ack { sequence } => received.push(format!("ack:{sequence}")),
                    }
                }
                other => panic!("unexpected message: {other:?}"),
            }
        }

        assert_eq!(
            received,
            vec![
                "text:t1",
                "bin:m2:b-1",
                "bin:m3:broadcast-1",
                "bin:m4:cursor-1",
                "text:d1",
            ]
        );
    }

    #[tokio::test]
    async fn outbound_queue_allows_replace_when_at_capacity() {
        let (tx, mut rx) = outbound_channel(2);

        tx.send_frame(&ClientFrame::Ack { sequence: 1 })
            .await
            .unwrap();
        let header1 = BinaryHeader {
            message_id: "f1".into(),
            target_device_id: Some("phone".into()),
            replace_key: Some("live".into()),
        };
        tx.send_binary(&header1, b"old").await.unwrap();

        // Queue is now at capacity (2). Replacing the existing key must not block.
        let header2 = BinaryHeader {
            message_id: "f2".into(),
            target_device_id: Some("phone".into()),
            replace_key: Some("live".into()),
        };
        tokio::time::timeout(
            Duration::from_millis(200),
            tx.send_binary(&header2, b"new"),
        )
        .await
        .expect("replacing an existing key should not block")
        .unwrap();

        // A distinct entry waits until the receiver frees a slot.
        let tx_blocked = tx.clone();
        let waiter = tokio::spawn(async move {
            tx_blocked
                .send_frame(&ClientFrame::Ack { sequence: 2 })
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        assert!(matches!(rx.recv().await, Some(Message::Text(_))));
        tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("blocked sender should wake after recv")
            .unwrap();

        let Some(Message::Binary(bytes)) = rx.recv().await else {
            panic!("expected coalesced binary frame");
        };
        let (header, body_start) = decode_binary_frame(&bytes).unwrap();
        assert_eq!(header.message_id, "f2");
        assert_eq!(&bytes[body_start..], b"new");
        assert!(matches!(rx.recv().await, Some(Message::Text(_))));
    }

    #[tokio::test]
    async fn outbound_queue_errors_when_receiver_dropped() {
        let (tx, rx) = outbound_channel(1);
        tx.send_frame(&ClientFrame::Ack { sequence: 1 })
            .await
            .unwrap();

        let tx_blocked = tx.clone();
        let waiter = tokio::spawn(async move {
            tx_blocked
                .send_frame(&ClientFrame::Ack { sequence: 2 })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(rx);

        let result = tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("blocked sender should exit when receiver drops")
            .unwrap();
        assert!(result.is_err());
        assert!(tx.send_frame(&ClientFrame::Ack { sequence: 3 }).await.is_err());
    }
}
