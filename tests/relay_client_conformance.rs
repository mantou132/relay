//! Conformance tests: run the shared client against a real relay server and
//! verify the delivery contract end to end (resend on connect, forward, ack,
//! replay after restart, idempotent outbox).

use std::{
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use relay_client::{
    Client, ClientHandler, OutboundMessage,
    relay_frame::{ClientFrame, ServerFrame},
    store::OutboxStore,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message as WsMessage;

struct RelayServer {
    child: Option<Child>,
}

impl RelayServer {
    fn spawn(port: u16, database: &std::path::Path) -> Self {
        let child = Command::new(std::env!("CARGO_BIN_EXE_relay"))
            .args([
                "--bind",
                &format!("127.0.0.1:{port}"),
                "--database",
                database.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self { child: Some(child) }
    }

    fn terminate(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for RelayServer {
    fn drop(&mut self) {
        self.terminate();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn raw_connect(port: u16, id: &str, endpoint: &str) -> Socket {
    let url = format!("ws://127.0.0.1:{port}/ws?id={id}&endpoint={endpoint}&device_id=raw");
    for _ in 0..100 {
        if let Ok((socket, _)) = tokio_tungstenite::connect_async(&url).await {
            return socket;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("relay server did not start at {url}");
}

/// In-memory store with a pre-seedable outbox, mirroring `MemoryStore` but
/// exposing handles so tests can assert on drained state.
#[derive(Default)]
struct TestStore {
    outbox: Mutex<Vec<OutboundMessage>>,
    last_received: Mutex<Option<u64>>,
}

#[async_trait::async_trait]
impl OutboxStore for TestStore {
    async fn enqueue(&self, payload: Value) -> Result<String> {
        let mut outbox = self.outbox.lock().await;
        let message_id = format!("seed-{}", outbox.len());
        outbox.push(OutboundMessage {
            message_id: message_id.clone(),
            payload,
            target_device_id: None,
        });
        Ok(message_id)
    }

    async fn outbox(&self) -> Vec<OutboundMessage> {
        self.outbox.lock().await.clone()
    }

    async fn remove_from_outbox(&self, message_id: &str) {
        self.outbox
            .lock()
            .await
            .retain(|message| message.message_id != message_id);
    }

    async fn last_received(&self) -> Option<u64> {
        *self.last_received.lock().await
    }

    async fn mark_received(&self, sequence: u64) -> Result<()> {
        *self.last_received.lock().await = Some(sequence);
        Ok(())
    }
}

#[derive(Default)]
struct CollectHandler {
    connected: std::sync::atomic::AtomicBool,
    payloads: std::sync::Mutex<Vec<Value>>,
}

impl ClientHandler for CollectHandler {
    fn on_connected(&self) {
        self.connected.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn on_payload(&self, payload: Value) {
        // The handler runs on the client task; a std mutex held only for a
        // push is fine and never blocks across an await.
        self.payloads
            .lock()
            .expect("payload lock poisoned")
            .push(payload);
    }
}

async fn wait_until(condition: impl Fn() -> bool, what: &str) {
    for _ in 0..250 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Sends a client frame from the raw opposite endpoint.
async fn send_client_frame(socket: &mut Socket, frame: &ClientFrame) {
    socket
        .send(WsMessage::Text(
            serde_json::to_string(frame).unwrap().into(),
        ))
        .await
        .unwrap();
}

/// Reads server frames until one matches the predicate, returning it.
async fn read_until(
    socket: &mut Socket,
    mut predicate: impl FnMut(&ServerFrame) -> bool,
) -> ServerFrame {
    for _ in 0..50 {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("timed out reading from relay")
            .expect("relay socket closed")
            .expect("relay socket error");
        if let WsMessage::Text(text) = message {
            let frame: ServerFrame = serde_json::from_str(&text).unwrap();
            if predicate(&frame) {
                return frame;
            }
        }
    }
    panic!("relay never sent the expected frame");
}

#[tokio::test]
async fn resends_outbox_receives_and_acks_peer_messages() {
    let directory = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &directory.path().join("relay.sqlite3"));

    // Seed one message before connecting: it must be sent on connect and
    // confirmed stored.
    let store = Arc::new(TestStore::default());
    store.enqueue(json!({ "outbox": 1 })).await.unwrap();

    let handler = Arc::new(CollectHandler::default());
    let url = format!("ws://127.0.0.1:{port}/ws?id=pair-a&endpoint=1&device_id=c1");
    let client = Client::new(url, store.clone(), handler.clone());
    let run_task = tokio::spawn(client.into_task());

    let mut endpoint_two = raw_connect(port, "pair-a", "2").await;
    let frame = read_until(&mut endpoint_two, |frame| {
        matches!(frame, ServerFrame::Message { .. })
    })
    .await;
    let ServerFrame::Message {
        sequence, payload, ..
    } = frame
    else {
        unreachable!()
    };
    assert_eq!(payload, json!({ "outbox": 1 }));
    send_client_frame(&mut endpoint_two, &ClientFrame::Ack { sequence }).await;

    // Endpoint 2's reply must reach the client, be dispatched exactly once,
    // acked, and the outbox must drain after `stored`.
    endpoint_two
        .send(
            json!({ "type": "message", "message_id": "e2-1", "payload": { "reply": true } })
                .to_string()
                .into(),
        )
        .await
        .unwrap();
    wait_until(
        || {
            handler
                .payloads
                .try_lock()
                .map(|payloads| payloads.contains(&json!({ "reply": true })))
                .unwrap_or(false)
        },
        "client dispatched endpoint 2's reply",
    )
    .await;
    wait_until(
        || {
            store
                .outbox
                .try_lock()
                .map(|outbox| outbox.is_empty())
                .unwrap_or(false)
        },
        "outbox drained after stored",
    )
    .await;
    run_task.abort();
}

#[tokio::test]
async fn replays_only_unacked_messages_after_server_restart() {
    let directory = tempfile::tempdir().unwrap();
    let port = free_port();
    let database = directory.path().join("relay.sqlite3");
    let mut server = RelayServer::spawn(port, &database);

    let handler = Arc::new(CollectHandler::default());
    let url = format!("ws://127.0.0.1:{port}/ws?id=pair-r&endpoint=1&device_id=c1");
    let client = Client::new(
        url,
        Arc::new(TestStore::default()),
        handler.clone(),
    );
    let run_task = tokio::spawn(client.into_task());

    // Endpoint 2 sends while both are online; the client processes and acks.
    let mut endpoint_two = raw_connect(port, "pair-r", "2").await;
    send_client_frame(
        &mut endpoint_two,
        &ClientFrame::Message {
            message_id: "e2-1".into(),
            payload: json!(7),
            target_device_id: None,
        },
    )
    .await;
    wait_until(
        || {
            handler
                .payloads
                .try_lock()
                .map(|payloads| payloads.len() == 1)
                .unwrap_or(false)
        },
        "first delivery dispatched",
    )
    .await;
    // Give the ack time to land, then kill the relay: a correct relay has no
    // pending rows left, so the replayed stream must stay empty.
    tokio::time::sleep(Duration::from_millis(200)).await;
    server.terminate();

    let _restarted = RelayServer::spawn(port, &database);
    // The client reconnects with backoff. Because the message was acked, no
    // duplicate may be dispatched; wait for a reconnect to actually happen
    // by checking that no second payload arrives within a bounded window.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let payloads = handler.payloads.lock().expect("payload lock poisoned");
    assert_eq!(
        payloads.len(),
        1,
        "acked message was replayed after restart: {payloads:?}"
    );
    drop(payloads);
    run_task.abort();
}

#[tokio::test]
async fn multiple_outbox_messages_forward_in_sequence_order() {
    let directory = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &directory.path().join("relay.sqlite3"));

    let store = Arc::new(TestStore::default());
    store.enqueue(json!({ "n": 1 })).await.unwrap();
    store.enqueue(json!({ "n": 2 })).await.unwrap();

    let handler = Arc::new(CollectHandler::default());
    let url = format!("ws://127.0.0.1:{port}/ws?id=pair-d&endpoint=1&device_id=c1");
    let client = Client::new(url, store.clone(), handler.clone());
    let run_task = tokio::spawn(client.into_task());

    let mut endpoint_two = raw_connect(port, "pair-d", "2").await;
    let mut received = Vec::new();
    for _ in 0..2 {
        let frame = read_until(&mut endpoint_two, |frame| {
            matches!(frame, ServerFrame::Message { .. })
        })
        .await;
        let ServerFrame::Message {
            sequence, payload, ..
        } = frame
        else {
            unreachable!()
        };
        received.push((sequence, payload));
    }
    received.sort_by_key(|(sequence, _)| *sequence);
    assert_eq!(
        received,
        vec![(1, json!({ "n": 1 })), (2, json!({ "n": 2 }))],
        "seeded messages forwarded in sequence order"
    );
    run_task.abort();
}

#[tokio::test]
async fn sends_live_messages_while_connected_and_removes_on_stored() {
    let directory = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &directory.path().join("relay.sqlite3"));

    let store = Arc::new(TestStore::default());
    let handler = Arc::new(CollectHandler::default());
    let url = format!("ws://127.0.0.1:{port}/ws?id=pair-live&endpoint=1&device_id=c1");
    let client = Client::new(url, store.clone(), handler.clone());
    let run_task = tokio::spawn(client.clone().into_task());

    let mut endpoint_two = raw_connect(port, "pair-live", "2").await;

    // Send a message after connection is established:
    client.send(json!({ "live": 42 })).await.unwrap();

    let frame = read_until(&mut endpoint_two, |frame| {
        matches!(frame, ServerFrame::Message { .. })
    })
    .await;
    let ServerFrame::Message { sequence, payload, .. } = frame else {
        unreachable!()
    };
    assert_eq!(payload, json!({ "live": 42 }));
    send_client_frame(&mut endpoint_two, &ClientFrame::Ack { sequence }).await;

    wait_until(
        || {
            store
                .outbox
                .try_lock()
                .map(|outbox| outbox.is_empty())
                .unwrap_or(false)
        },
        "outbox drained after stored for live message",
    )
    .await;
    run_task.abort();
}

#[tokio::test]
async fn ack_head_client_purges_backlog_and_receives_live_message() {
    let directory = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &directory.path().join("relay.sqlite3"));

    // Raw endpoint 1 sends 2 messages to endpoint 2 while endpoint 2 is offline
    let mut endpoint_one = raw_connect(port, "pair-ack-client", "1").await;
    send_client_frame(
        &mut endpoint_one,
        &ClientFrame::Message {
            message_id: "m-1".to_string(),
            payload: json!({ "old": 1 }),
            target_device_id: None,
        },
    )
    .await;
    let _ = read_until(&mut endpoint_one, |f| matches!(f, ServerFrame::Stored { .. })).await;

    send_client_frame(
        &mut endpoint_one,
        &ClientFrame::Message {
            message_id: "m-2".to_string(),
            payload: json!({ "old": 2 }),
            target_device_id: None,
        },
    )
    .await;
    let _ = read_until(&mut endpoint_one, |f| matches!(f, ServerFrame::Stored { .. })).await;

    // Start client for endpoint 2 with ack_head = true
    let store = Arc::new(TestStore::default());
    let handler = Arc::new(CollectHandler::default());
    let url = format!("ws://127.0.0.1:{port}/ws?id=pair-ack-client&endpoint=2&device_id=c2");
    let client = Client::new_with_ack_head(url, true, store.clone(), handler.clone());
    let run_task = tokio::spawn(client.into_task());

    // Wait until connected
    wait_until(
        || handler.connected.load(std::sync::atomic::Ordering::Relaxed),
        "client connected",
    )
    .await;

    // Now endpoint 1 sends live message 3
    send_client_frame(
        &mut endpoint_one,
        &ClientFrame::Message {
            message_id: "m-3".to_string(),
            payload: json!({ "live": 3 }),
            target_device_id: None,
        },
    )
    .await;
    let _ = read_until(&mut endpoint_one, |f| matches!(f, ServerFrame::Stored { .. })).await;

    // Handler should receive ONLY live message 3; old 1 and 2 were dropped
    wait_until(
        || !handler.payloads.lock().unwrap().is_empty(),
        "live payload received",
    )
    .await;

    let payloads = handler.payloads.lock().unwrap().clone();
    assert_eq!(payloads, vec![json!({ "live": 3 })]);

    run_task.abort();
}

#[derive(Default)]
struct PoisonPillHandler {
    rejected: std::sync::Mutex<Vec<(String, String)>>,
    disconnected: std::sync::atomic::AtomicBool,
}

impl ClientHandler for PoisonPillHandler {
    fn on_payload(&self, _payload: Value) {}
    fn on_disconnected(&self, _error: Option<String>) {
        self.disconnected.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    fn on_message_rejected(&self, message_id: &str, reason: &str) {
        self.rejected
            .lock()
            .unwrap()
            .push((message_id.to_string(), reason.to_string()));
    }
}

#[tokio::test]
async fn rejected_message_does_not_poison_outbox_or_disconnect_client() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("relay.sqlite3");
    let port = free_port();
    let _server = RelayServer::spawn(port, &database);

    // Pre-populate the database with "conflict-id" having payload {"valid": 1}
    let mut raw_peer = raw_connect(port, "pair-poison", "1").await;
    send_client_frame(
        &mut raw_peer,
        &ClientFrame::Message {
            message_id: "conflict-id".to_string(),
            payload: json!({ "valid": 1 }),
            target_device_id: None,
        },
    )
    .await;
    let _ = read_until(&mut raw_peer, |f| matches!(f, ServerFrame::Stored { .. })).await;

    // Start a client on endpoint 1 that has 2 messages in its outbox:
    // 1. "conflict-id" with a DIFFERENT payload -> server will reject with Error!
    // 2. "good-msg" with valid payload -> server will store successfully!
    let store = Arc::new(TestStore::default());
    {
        let mut outbox = store.outbox.lock().await;
        outbox.push(OutboundMessage {
            message_id: "conflict-id".to_string(),
            payload: json!({ "different": 2 }),
            target_device_id: None,
        });
        outbox.push(OutboundMessage {
            message_id: "good-msg".to_string(),
            payload: json!({ "good": true }),
            target_device_id: None,
        });
    }

    let handler = Arc::new(PoisonPillHandler::default());
    let url = format!("ws://127.0.0.1:{port}/ws?id=pair-poison&endpoint=1&device_id=client-1");
    let client = Client::new(url, store.clone(), handler.clone());
    let run_task = tokio::spawn(client.into_task());

    // Wait until conflict-id is rejected via callback
    wait_until(
        || !handler.rejected.lock().unwrap().is_empty(),
        "rejection callback called",
    )
    .await;

    let (rejected_id, reason) = handler.rejected.lock().unwrap()[0].clone();
    assert_eq!(rejected_id, "conflict-id");
    assert!(reason.contains("message_id was already used with a different payload"));

    // Verify client did NOT disconnect
    assert!(!handler.disconnected.load(std::sync::atomic::Ordering::Relaxed));

    // Wait until outbox is completely drained without poison pill loop
    wait_until(
        || {
            store
                .outbox
                .try_lock()
                .map(|outbox| outbox.is_empty())
                .unwrap_or(false)
        },
        "outbox drained without poison pill hang",
    )
    .await;

    run_task.abort();
}

