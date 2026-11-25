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
    ephemeral_with_key(message_id, payload, target_device_id, None)
}

fn ephemeral_with_key(
    message_id: &str,
    payload: Value,
    target_device_id: Option<&str>,
    replace_key: Option<&str>,
) -> ClientFrame {
    ClientFrame::Message {
        message_id: message_id.into(),
        payload,
        target_device_id: target_device_id.map(Into::into),
        ephemeral: true,
        replace_key: replace_key.map(Into::into),
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
        replace_key: None,
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

#[tokio::test]
async fn replace_key_validation_and_durable_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    let mut one = connect(port, "pair-val", "1", "host").await;
    let mut two = connect(port, "pair-val", "2", "phone").await;

    // Empty replace_key on ephemeral text is rejected.
    send(&mut one, &ephemeral_with_key("bad-1", json!(1), None, Some(""))).await;
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Rejected { message_id, reason })
            if message_id == "bad-1" && reason.contains("replace_key")
    ));

    // Too-long replace_key (> 256 bytes) on ephemeral text is rejected.
    let too_long = "x".repeat(257);
    send(
        &mut one,
        &ephemeral_with_key("bad-2", json!(2), None, Some(&too_long)),
    )
    .await;
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Rejected { message_id, reason })
            if message_id == "bad-2" && reason.contains("replace_key")
    ));

    // Empty and too-long replace_key on binary frames are rejected.
    let bad_bin_empty = BinaryHeader {
        message_id: "bad-bin-1".into(),
        target_device_id: None,
        replace_key: Some(String::new()),
    };
    one.send(Message::Binary(encode_binary_frame(&bad_bin_empty, b"x").into()))
        .await
        .unwrap();
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Rejected { message_id, reason })
            if message_id == "bad-bin-1" && reason.contains("replace_key")
    ));

    let bad_bin_long = BinaryHeader {
        message_id: "bad-bin-2".into(),
        target_device_id: None,
        replace_key: Some(too_long),
    };
    one.send(Message::Binary(encode_binary_frame(&bad_bin_long, b"x").into()))
        .await
        .unwrap();
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Rejected { message_id, reason })
            if message_id == "bad-bin-2" && reason.contains("replace_key")
    ));

    // replace_key on durable (ephemeral: false) message is rejected and not forwarded.
    send(
        &mut one,
        &ClientFrame::Message {
            message_id: "durable-bad".into(),
            payload: json!({ "durable": true }),
            target_device_id: None,
            ephemeral: false,
            replace_key: Some("screen".into()),
        },
    )
    .await;
    assert!(matches!(
        next_frame(&mut one).await,
        Some(ServerFrame::Rejected { message_id, reason })
            if message_id == "durable-bad" && reason.contains("ephemeral")
    ));

    // None of the rejected messages reached the peer.
    assert_eq!(next(&mut two).await, None);
}

#[tokio::test]
async fn replace_key_coalesces_under_slow_consumer_backpressure() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    let mut sender = connect(port, "pair-slow", "1", "host").await;
    let mut slow_receiver = connect(port, "pair-slow", "2", "phone").await;

    // Durable message before the burst.
    send(
        &mut sender,
        &ClientFrame::Message {
            message_id: "durable-before".into(),
            payload: json!({ "phase": "before" }),
            target_device_id: None,
            ephemeral: false,
            replace_key: None,
        },
    )
    .await;
    assert_eq!(
        next_frame(&mut sender).await,
        Some(ServerFrame::Stored {
            message_id: "durable-before".into(),
        })
    );

    // Pause reading from slow_receiver while sending 60 x 256 KiB (~15 MiB)
    // binary frames with the same replace_key, plus distinct-key and non-keyed
    // ephemeral frames and a final durable sentinel. This saturates loopback
    // TCP buffers so the relay writer task blocks and the relay's per-peer
    // outbound queue coalesces older "live-view" frames.
    const TOTAL_VIDEO_FRAMES: usize = 60;
    const FRAME_SIZE: usize = 256 * 1024;
    for i in 0..TOTAL_VIDEO_FRAMES {
        let mut body = vec![(i % 251) as u8; FRAME_SIZE];
        body[..4].copy_from_slice(&(i as u32).to_be_bytes());
        let header = BinaryHeader {
            message_id: format!("video-{i}"),
            target_device_id: Some("phone".into()),
            replace_key: Some("live-view".into()),
        };
        sender
            .send(Message::Binary(encode_binary_frame(&header, &body).into()))
            .await
            .unwrap();
    }

    send(
        &mut sender,
        &ephemeral_with_key("cursor-1", json!({ "x": 10 }), None, Some("cursor")),
    )
    .await;
    send(
        &mut sender,
        &ephemeral("unkeyed-1", json!({ "ping": true }), None),
    )
    .await;
    for i in 0..20u32 {
        send(
            &mut sender,
            &ephemeral_with_key(
                &format!("status-{i}"),
                json!({ "status": i }),
                None,
                Some("status"),
            ),
        )
        .await;
    }

    send(
        &mut sender,
        &ClientFrame::Message {
            message_id: "durable-after".into(),
            payload: json!({ "phase": "after" }),
            target_device_id: None,
            ephemeral: false,
            replace_key: None,
        },
    )
    .await;

    // Waiting for `Stored` of durable-after proves the server's read loop has
    // processed every preceding frame while slow_receiver hasn't read yet.
    // Also confirms no `Undeliverable` frame was emitted for superseded frames.
    assert_eq!(
        next_frame(&mut sender).await,
        Some(ServerFrame::Stored {
            message_id: "durable-after".into(),
        })
    );

    // Now drain slow_receiver until durable-after arrives.
    let mut durable_ids = Vec::new();
    let mut video_indices = Vec::new();
    let mut status_ids = Vec::new();
    let mut saw_cursor = false;
    let mut saw_unkeyed = false;

    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), slow_receiver.next())
            .await
            .expect("timed out draining slow receiver")
            .expect("slow receiver socket closed")
            .expect("slow receiver socket error");
        match msg {
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Binary(bytes) => {
                let (header, body_start) = decode_binary_frame(&bytes).unwrap();
                assert_eq!(header.replace_key.as_deref(), Some("live-view"));
                let idx = u32::from_be_bytes(bytes[body_start..body_start + 4].try_into().unwrap())
                    as usize;
                assert_eq!(header.message_id, format!("video-{idx}"));
                video_indices.push(idx);
            }
            Message::Text(text) => {
                let frame: ServerFrame = serde_json::from_str(&text).unwrap();
                match frame {
                    ServerFrame::Message {
                        message_id,
                        sequence,
                        ..
                    } => {
                        durable_ids.push((sequence, message_id.clone()));
                        if message_id == "durable-after" {
                            break;
                        }
                    }
                    ServerFrame::Ephemeral { message_id, .. } => {
                        if message_id == "cursor-1" {
                            saw_cursor = true;
                        } else if message_id == "unkeyed-1" {
                            saw_unkeyed = true;
                        } else if message_id.starts_with("status-") {
                            status_ids.push(message_id);
                        }
                    }
                    other => panic!("unexpected server frame: {other:?}"),
                }
            }
            other => panic!("unexpected ws message: {other:?}"),
        }
    }

    assert_eq!(
        durable_ids,
        vec![
            (1, "durable-before".to_string()),
            (2, "durable-after".to_string()),
        ]
    );
    assert!(saw_cursor, "distinct replace_key 'cursor' must be preserved");
    assert!(saw_unkeyed, "non-keyed ephemeral message must be preserved");
    assert_eq!(
        status_ids.last().map(String::as_str),
        Some("status-19"),
        "latest 'status' ephemeral text frame must be delivered"
    );
    assert!(
        status_ids.len() < 20,
        "older 'status' frames should have coalesced under backpressure, got {status_ids:?}"
    );
    assert_eq!(
        video_indices.last().copied(),
        Some(TOTAL_VIDEO_FRAMES - 1),
        "latest 'live-view' binary frame must be delivered"
    );
    assert!(
        video_indices.len() < TOTAL_VIDEO_FRAMES,
        "older 'live-view' frames should have coalesced under backpressure, got {} of {}",
        video_indices.len(),
        TOTAL_VIDEO_FRAMES
    );
    assert!(
        video_indices.windows(2).all(|w| w[0] < w[1]),
        "delivered 'live-view' frames must remain strictly ordered: {video_indices:?}"
    );
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
    assert!(client.send_ephemeral(json!(0), None, None, None).await.is_err());

    let task = tokio::spawn(client.clone().into_task());
    let mut peer = connect(port, "pair-c", "2", "phone").await;
    let message_id = loop {
        if let Ok(id) = client.send_binary(b"bytes", None, None, None).await {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    let Some(Message::Binary(received)) = next(&mut peer).await else {
        panic!("peer did not receive the binary frame");
    };
    let (header, body_start) = decode_binary_frame(&received).unwrap();
    assert_eq!(header.message_id, message_id);
    assert_eq!(header.replace_key, None);
    assert_eq!(&received[body_start..], b"bytes");

    // Verify send_binary and send_ephemeral carry replace_key.
    let keyed_bin_id = client
        .send_binary(
            b"frame-bytes",
            Some("phone".into()),
            Some("bin-keyed".into()),
            Some("live-view".into()),
        )
        .await
        .unwrap();
    assert_eq!(keyed_bin_id, "bin-keyed");
    let Some(Message::Binary(received)) = next(&mut peer).await else {
        panic!("peer did not receive the keyed binary frame");
    };
    let (header, body_start) = decode_binary_frame(&received).unwrap();
    assert_eq!(header.message_id, "bin-keyed");
    assert_eq!(header.target_device_id.as_deref(), Some("phone"));
    assert_eq!(header.replace_key.as_deref(), Some("live-view"));
    assert_eq!(&received[body_start..], b"frame-bytes");

    client
        .send_ephemeral(
            json!({ "cursor": [1, 2] }),
            None,
            Some("eph-keyed".into()),
            Some("cursor".into()),
        )
        .await
        .unwrap();
    assert_eq!(
        next_frame(&mut peer).await,
        Some(ServerFrame::Ephemeral {
            message_id: "eph-keyed".into(),
            payload: json!({ "cursor": [1, 2] }),
        })
    );

    client
        .send_ephemeral(json!({ "text": 1 }), None, None, None)
        .await
        .unwrap();
    assert!(matches!(next_frame(&mut peer).await, Some(ServerFrame::Ephemeral { .. })));
    assert!(store.outbox().await.is_empty());

    send(&mut peer, &ephemeral("from-peer", json!("hi"), None)).await;
    let header = BinaryHeader {
        message_id: "bin-from-peer".into(),
        target_device_id: None,
        replace_key: Some("peer-stream".into()),
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
        .send_ephemeral(json!(1), None, Some("rpc-42".into()), None)
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
