//! A client must keep working when a newer relay sends frame types or fields
//! it does not know.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use relay_client::{Client, ClientHandler, memory::MemoryStore, relay_frame::ServerFrame};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

#[test]
fn unknown_frame_types_decode_as_unknown() {
    let frame: ServerFrame =
        serde_json::from_str(r#"{"type":"from_the_future","detail":{"n":1}}"#).unwrap();
    assert_eq!(frame, ServerFrame::Unknown);
}

#[derive(Default)]
struct Recorder {
    payloads: Mutex<Vec<Value>>,
    connected: AtomicUsize,
    disconnected: AtomicUsize,
}

impl ClientHandler for Recorder {
    fn on_payload(&self, payload: Value) {
        self.payloads.lock().unwrap().push(payload);
    }
    fn on_connected(&self) {
        self.connected.fetch_add(1, Ordering::SeqCst);
    }
    fn on_disconnected(&self, _error: Option<String>) {
        self.disconnected.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn client_ignores_unknown_frames_and_stays_connected() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // A fake relay that sends frames from a hypothetical newer protocol
    // around a regular ephemeral payload, then keeps the socket open.
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        for frame in [
            json!({"type": "ready", "endpoint": "1", "new_field": true}),
            json!({"type": "from_the_future", "detail": {"n": 1}}),
            json!({"type": "ephemeral", "message_id": "m1", "payload": {"ok": 1}}),
            json!({"type": "another_new_frame"}),
            json!({"type": "ephemeral", "message_id": "m2", "payload": {"ok": 2}}),
        ] {
            socket
                .send(Message::Text(frame.to_string().into()))
                .await
                .unwrap();
        }
        while let Some(Ok(_)) = socket.next().await {}
    });

    let handler = Arc::new(Recorder::default());
    let client = Client::new(
        format!("ws://127.0.0.1:{port}/ws?id=pair&endpoint=1&device_id=d"),
        Arc::new(MemoryStore::new()),
        handler.clone(),
    );
    let task = tokio::spawn(client.clone().into_task());

    for _ in 0..250 {
        if handler.payloads.lock().unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Give a wrongly failing client time to report the disconnect.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        *handler.payloads.lock().unwrap(),
        [json!({"ok": 1}), json!({"ok": 2})]
    );
    assert_eq!(handler.connected.load(Ordering::SeqCst), 1);
    assert_eq!(handler.disconnected.load(Ordering::SeqCst), 0);

    client.close();
    task.await.unwrap().unwrap();
    server.abort();
}
