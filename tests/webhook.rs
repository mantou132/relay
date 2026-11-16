use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use axum::{Router, body::Bytes, extract::State, http::HeaderMap, routing::post};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "test-secret";

struct RelayServer {
    child: Child,
}

impl RelayServer {
    fn spawn(port: u16, database: &std::path::Path, webhook_url: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_relay"))
            .args([
                "--bind",
                &format!("127.0.0.1:{port}"),
                "--database",
                database.to_str().unwrap(),
                "--webhook-url",
                webhook_url,
                "--webhook-secret",
                SECRET,
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

async fn connect(port: u16, id: &str, endpoint: &str, device_id: &str) -> Socket {
    let url = format!("ws://127.0.0.1:{port}/ws?id={id}&endpoint={endpoint}&device_id={device_id}");
    for _ in 0..100 {
        if let Ok((mut socket, _)) = tokio_tungstenite::connect_async(&url).await {
            let ready = socket.next().await.unwrap().unwrap();
            assert!(matches!(ready, Message::Text(text) if text.contains("ready")));
            return socket;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("relay server did not start at {url}");
}

/// Starts a receiver that verifies each request's signature and forwards its body.
async fn spawn_receiver() -> (String, mpsc::UnboundedReceiver<Value>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let router = Router::new()
        .route(
            "/hook",
            post(
                |State(tx): State<mpsc::UnboundedSender<Value>>,
                 headers: HeaderMap,
                 body: Bytes| async move {
                    let timestamp = headers["x-relay-timestamp"].to_str().unwrap();
                    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
                    mac.update(format!("{timestamp}.").as_bytes());
                    mac.update(&body);
                    let expected: String = mac
                        .finalize()
                        .into_bytes()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect();
                    assert_eq!(
                        headers["x-relay-signature"].to_str().unwrap(),
                        format!("sha256={expected}")
                    );
                    let event: Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(headers["x-relay-event"].to_str().unwrap(), event["event"]);
                    tx.send(event).unwrap();
                },
            ),
        )
        .with_state(tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, rx)
}

async fn next_event(rx: &mut mpsc::UnboundedReceiver<Value>) -> Value {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("webhook event did not arrive")
        .unwrap()
}

#[tokio::test]
async fn reports_connects_and_disconnects_with_peer_presence() {
    let (hook_url, mut events) = spawn_receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"), &hook_url);

    // The phone comes online while the host is down.
    let mut phone = connect(port, "pair", "2", "phone").await;
    let event = next_event(&mut events).await;
    assert_eq!(event["event"], "device_connected");
    assert_eq!(event["id"], "pair");
    assert_eq!(event["endpoint"], "2");
    assert_eq!(event["device_id"], "phone");
    assert_eq!(event["peer_online"], false);

    let mut host = connect(port, "pair", "1", "host").await;
    let event = next_event(&mut events).await;
    assert_eq!(event["device_id"], "host");
    assert_eq!(event["peer_online"], true);

    // Preemption replaces the connection without a disconnect event.
    let mut phone_again = connect(port, "pair", "2", "phone").await;
    let event = next_event(&mut events).await;
    assert_eq!(event["event"], "device_connected");
    assert_eq!(event["device_id"], "phone");
    assert_eq!(event["online_devices"], 1);
    assert_eq!(event["peer_online"], true);
    let _ = phone.close(None).await;

    phone_again.close(None).await.unwrap();
    let event = next_event(&mut events).await;
    assert_eq!(event["event"], "device_disconnected");
    assert_eq!(event["device_id"], "phone");
    assert_eq!(event["peer_online"], true);

    host.close(None).await.unwrap();
    let event = next_event(&mut events).await;
    assert_eq!(event["event"], "device_disconnected");
    assert_eq!(event["device_id"], "host");
    assert_eq!(event["peer_online"], false);

    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err(),
        "unexpected extra webhook event"
    );
}

#[tokio::test]
async fn counts_devices_per_endpoint() {
    let (hook_url, mut events) = spawn_receiver().await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"), &hook_url);

    let mut phone_a = connect(port, "pair", "2", "phone_a").await;
    let event = next_event(&mut events).await;
    assert_eq!(event["device_id"], "phone_a");
    assert_eq!(event["online_devices"], 1);
    assert_eq!(event["peer_online"], false);

    let mut phone_b = connect(port, "pair", "2", "phone_b").await;
    let event = next_event(&mut events).await;
    assert_eq!(event["device_id"], "phone_b");
    assert_eq!(event["online_devices"], 2);
    assert_eq!(event["peer_online"], false);

    let _host = connect(port, "pair", "1", "host").await;
    let event = next_event(&mut events).await;
    assert_eq!(event["endpoint"], "1");
    assert_eq!(event["online_devices"], 1);
    assert_eq!(event["peer_online"], true);

    // Leaving one phone still leaves the endpoint occupied.
    phone_a.close(None).await.unwrap();
    let event = next_event(&mut events).await;
    assert_eq!(event["event"], "device_disconnected");
    assert_eq!(event["device_id"], "phone_a");
    assert_eq!(event["online_devices"], 1);

    phone_b.close(None).await.unwrap();
    let event = next_event(&mut events).await;
    assert_eq!(event["event"], "device_disconnected");
    assert_eq!(event["device_id"], "phone_b");
    assert_eq!(event["online_devices"], 0);
    assert_eq!(event["peer_online"], true);
}
