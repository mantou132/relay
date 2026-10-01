use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relay::{BinaryHeader, ClientFrame, ServerFrame, decode_binary_frame, encode_binary_frame};
use relay_client::{Client, ClientHandler, memory::MemoryStore, store::OutboxStore};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

struct RelayServer {
    child: Child,
}

impl RelayServer {
    fn spawn(port: u16, database: &std::path::Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_relay"))
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
        Self { child }
    }
}

impl Drop for RelayServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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

fn url(port: u16, id: &str, endpoint: &str, device_id: &str) -> String {
    format!("ws://127.0.0.1:{port}/ws?id={id}&endpoint={endpoint}&device_id={device_id}")
}

/// Connects and consumes the `ready` frame.
async fn connect(port: u16, id: &str, endpoint: &str, device_id: &str) -> Socket {
    let url = url(port, id, endpoint, device_id);
    for _ in 0..100 {
        if let Ok((mut socket, _)) = tokio_tungstenite::connect_async(&url).await {
            assert!(matches!(next(&mut socket).await, Some(Message::Text(text)) if text.contains("ready")));
            return socket;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("relay server did not start at {url}");
}

/// Next data message, or `None` if nothing arrives shortly.
async fn next(socket: &mut Socket) -> Option<Message> {
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), socket.next())
            .await
            .ok()?
            .expect("relay socket closed")
            .expect("relay socket error");
        if !matches!(message, Message::Ping(_) | Message::Pong(_)) {
            return Some(message);
        }
    }
}

async fn next_frame(socket: &mut Socket) -> Option<ServerFrame> {
    match next(socket).await? {
        Message::Text(text) => Some(serde_json::from_str(&text).unwrap()),
        other => panic!("expected a text frame, got {other:?}"),
    }
}

async fn send(socket: &mut Socket, frame: &ClientFrame) {
    let json = serde_json::to_string(frame).unwrap();
    socket.send(Message::Text(json.into())).await.unwrap();
}

fn ephemeral(message_id: &str, payload: Value, target_device_id: Option<&str>) -> ClientFrame {
    ClientFrame::Message {
        message_id: message_id.into(),
        payload,
        target_device_id: target_device_id.map(Into::into),
        ephemeral: true,
    }
}

#[tokio::test]
async fn ephemeral_text_is_forwarded_live_and_never_stored() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    let mut one = connect(port, "pair-e", "1", "host").await;
    let mut two = connect(port, "pair-e", "2", "phone").await;

    send(&mut one, &ephemeral("e-1", json!({ "live": 1 }), None)).await;
    assert_eq!(
        next_frame(&mut two).await,
        Some(ServerFrame::Ephemeral {
            message_id: "e-1".into(),
            payload: json!({ "live": 1 }),
        })
    );
    // No `stored` for the sender.
    assert_eq!(next_frame(&mut one).await, None);

    // Nothing is replayed on reconnect, and an offline peer is reported.
    drop(two);
    tokio::time::sleep(Duration::from_millis(100)).await;
    send(&mut one, &ephemeral("e-2", json!(2), None)).await;
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Undeliverable { message_id, .. }) if message_id == "e-2"
    ));
    let mut two = connect(port, "pair-e", "2", "phone").await;
    assert_eq!(next_frame(&mut two).await, None);

    // Targeting a device that is not connected is undeliverable too.
    send(&mut one, &ephemeral("e-3", json!(3), Some("tablet"))).await;
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Undeliverable { message_id, .. }) if message_id == "e-3"
    ));
    assert_eq!(next_frame(&mut two).await, None);
}

#[tokio::test]
async fn binary_frames_are_forwarded_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    let mut one = connect(port, "pair-b", "1", "host").await;
    let mut phone = connect(port, "pair-b", "2", "phone").await;
    let mut tablet = connect(port, "pair-b", "2", "tablet").await;

    let body: Vec<u8> = (0..=255).cycle().take(3 * 1024 * 1024).collect();
    let header = BinaryHeader {
        message_id: "b-1".into(),
        target_device_id: Some("phone".into()),
    };
    let frame = encode_binary_frame(&header, &body);
    one.send(Message::Binary(frame.clone().into())).await.unwrap();

    let Some(Message::Binary(received)) = next(&mut phone).await else {
        panic!("phone did not receive the binary frame");
    };
    assert_eq!(received.as_ref(), frame.as_slice());
    let (received_header, body_start) = decode_binary_frame(&received).unwrap();
    assert_eq!(received_header, header);
    assert_eq!(&received[body_start..], body.as_slice());
    assert!(next(&mut tablet).await.is_none());

    // A malformed binary frame is reported without dropping the connection.
    one.send(Message::Binary(vec![0, 9, b'{'].into())).await.unwrap();
    assert!(matches!(next_frame(&mut one).await, Some(ServerFrame::Error { .. })));
    send(&mut one, &ephemeral("after", json!(1), Some("tablet"))).await;
    assert!(matches!(next_frame(&mut tablet).await, Some(ServerFrame::Ephemeral { .. })));
}

#[derive(Default)]
struct Collect {
    payloads: Mutex<Vec<Value>>,
    binaries: Mutex<Vec<(String, Bytes)>>,
    undeliverable: Mutex<Vec<String>>,
}

impl ClientHandler for Collect {
    fn on_payload(&self, payload: Value) {
        self.payloads.lock().unwrap().push(payload);
    }

    fn on_binary(&self, message_id: &str, body: Bytes) {
        self.binaries
            .lock()
            .unwrap()
            .push((message_id.to_string(), body));
    }

    fn on_undeliverable(&self, message_id: &str, _reason: &str) {
        self.undeliverable
            .lock()
            .unwrap()
            .push(message_id.to_string());
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

#[tokio::test]
async fn rust_client_sends_and_receives_ephemeral_messages() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    let handler = Arc::new(Collect::default());
    let store = Arc::new(MemoryStore::new());
    let client = Client::new(url(port, "pair-c", "1", "host"), store.clone(), handler.clone());

    // Ephemeral sends fail fast instead of queueing while disconnected.
    assert!(client.send_ephemeral(json!(0), None, None).await.is_err());

    let task = tokio::spawn(client.clone().into_task());
    let mut peer = connect(port, "pair-c", "2", "phone").await;
    let message_id = loop {
        if let Ok(id) = client.send_binary(b"bytes", None, None).await {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    let Some(Message::Binary(received)) = next(&mut peer).await else {
        panic!("peer did not receive the binary frame");
    };
    let (header, body_start) = decode_binary_frame(&received).unwrap();
    assert_eq!(header.message_id, message_id);
    assert_eq!(&received[body_start..], b"bytes");

    client.send_ephemeral(json!({ "text": 1 }), None, None).await.unwrap();
    assert!(matches!(next_frame(&mut peer).await, Some(ServerFrame::Ephemeral { .. })));
    assert!(store.outbox().await.is_empty());

    send(&mut peer, &ephemeral("from-peer", json!("hi"), None)).await;
    let header = BinaryHeader {
        message_id: "bin-from-peer".into(),
        target_device_id: None,
    };
    peer.send(Message::Binary(encode_binary_frame(&header, b"\x00\x01").into()))
        .await
        .unwrap();
    wait_until(
        || handler.binaries.lock().unwrap().len() == 1,
        "binary from peer",
    )
    .await;
    assert_eq!(*handler.payloads.lock().unwrap(), vec![json!("hi")]);
    assert_eq!(
        handler.binaries.lock().unwrap()[0],
        ("bin-from-peer".to_string(), Bytes::from_static(b"\x00\x01"))
    );

    drop(peer);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // A caller-supplied id is what the undeliverable report names.
    let lost = client
        .send_ephemeral(json!(1), None, Some("rpc-42".into()))
        .await
        .unwrap();
    assert_eq!(lost, "rpc-42");
    wait_until(
        || handler.undeliverable.lock().unwrap().contains(&lost),
        "undeliverable report",
    )
    .await;

    client.close();
    task.await.unwrap().unwrap();
}
