use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::StreamExt;
use relay::ServerFrame;
use relay_client::{Client, ClientHandler, memory::MemoryStore};
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

fn url(port: u16, endpoint: &str, device_id: &str, presence: bool) -> String {
    let presence = if presence { "&presence=true" } else { "" };
    format!(
        "ws://127.0.0.1:{port}/ws?id=pair-p&endpoint={endpoint}&device_id={device_id}{presence}"
    )
}

/// Connects and consumes the `ready` frame.
async fn connect(port: u16, endpoint: &str, device_id: &str, presence: bool) -> Socket {
    connect_with_peers(port, endpoint, device_id, presence).await.0
}

/// Connects and returns the peer snapshot carried by `ready`.
async fn connect_with_peers(
    port: u16,
    endpoint: &str,
    device_id: &str,
    presence: bool,
) -> (Socket, Option<Vec<String>>) {
    let url = url(port, endpoint, device_id, presence);
    for _ in 0..100 {
        if let Ok((mut socket, _)) = tokio_tungstenite::connect_async(&url).await {
            let Some(ServerFrame::Ready { peers, .. }) = next_frame(&mut socket).await else {
                panic!("expected ready");
            };
            return (socket, peers);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("relay server did not start at {url}");
}

/// Next server frame, or `None` if nothing arrives shortly.
async fn next_frame(socket: &mut Socket) -> Option<ServerFrame> {
    loop {
        let message = tokio::time::timeout(Duration::from_millis(500), socket.next())
            .await
            .ok()?
            .expect("relay socket closed")
            .expect("relay socket error");
        match message {
            Message::Text(text) => return Some(serde_json::from_str(&text).unwrap()),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

fn connected(device_id: &str) -> Option<ServerFrame> {
    Some(ServerFrame::PeerConnected {
        device_id: device_id.into(),
        replaced: false,
    })
}

fn replaced(device_id: &str) -> Option<ServerFrame> {
    Some(ServerFrame::PeerConnected {
        device_id: device_id.into(),
        replaced: true,
    })
}

fn names(peers: &[&str]) -> Option<Vec<String>> {
    Some(peers.iter().map(|peer| peer.to_string()).collect())
}

fn disconnected(device_id: &str) -> Option<ServerFrame> {
    Some(ServerFrame::PeerDisconnected {
        device_id: device_id.into(),
    })
}

#[tokio::test]
async fn opted_in_devices_observe_peer_presence() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    // An empty snapshot says that no peer is online, without waiting.
    let (mut host, peers) = connect_with_peers(port, "1", "host", true).await;
    assert_eq!(peers, names(&[]));
    assert_eq!(next_frame(&mut host).await, None);

    // A joining device receives the peers already online in `ready`.
    let (mut phone, peers) = connect_with_peers(port, "2", "phone", true).await;
    assert_eq!(peers, names(&["host"]));
    assert_eq!(next_frame(&mut host).await, connected("phone"));

    // Devices of the same endpoint are not reported to each other.
    let (tablet, peers) = connect_with_peers(port, "2", "tablet", true).await;
    assert_eq!(peers, names(&["host"]));
    assert_eq!(next_frame(&mut host).await, connected("tablet"));
    assert_eq!(next_frame(&mut phone).await, None);

    // Another host device shows up in later snapshots, sorted.
    let host2 = connect(port, "1", "a-host", true).await;
    assert_eq!(next_frame(&mut phone).await, connected("a-host"));
    let (_tablet2, peers) = connect_with_peers(port, "2", "tablet2", true).await;
    assert_eq!(peers, names(&["a-host", "host"]));
    assert_eq!(next_frame(&mut host).await, connected("tablet2"));
    drop(host2);
    assert_eq!(next_frame(&mut phone).await, disconnected("a-host"));

    drop(tablet);
    assert_eq!(next_frame(&mut host).await, disconnected("tablet"));

    drop(host);
    assert_eq!(next_frame(&mut phone).await, disconnected("host"));
}

#[tokio::test]
async fn presence_is_opt_in_and_reports_replaced_connections() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    // Without the opt-in, no presence frames arrive, so older clients that
    // reject unknown frame types keep working.
    let (mut legacy, peers) = connect_with_peers(port, "1", "legacy", false).await;
    assert_eq!(peers, None);
    let (mut phone, peers) = connect_with_peers(port, "2", "phone", true).await;
    assert_eq!(peers, names(&["legacy"]));
    assert_eq!(next_frame(&mut legacy).await, None);

    // A reconnect that replaces the device's socket keeps it online, but is
    // reported because the device's session may have restarted.
    let replacement = connect(port, "1", "legacy", false).await;
    assert_eq!(next_frame(&mut phone).await, replaced("legacy"));
    assert!(
        matches!(next_frame(&mut legacy).await, Some(ServerFrame::Error { message }) if message.starts_with("connection_replaced"))
    );
    // The replaced socket closing does not take the device offline.
    drop(legacy);
    assert_eq!(next_frame(&mut phone).await, None);

    drop(replacement);
    assert_eq!(next_frame(&mut phone).await, disconnected("legacy"));
}

#[derive(Default)]
struct PresenceRecorder {
    events: Mutex<Vec<String>>,
}

impl ClientHandler for PresenceRecorder {
    fn on_payload(&self, _payload: serde_json::Value) {}
    fn on_peer_connected(&self, device_id: &str) {
        self.events.lock().unwrap().push(format!("+{device_id}"));
    }
    fn on_peer_disconnected(&self, device_id: &str) {
        self.events.lock().unwrap().push(format!("-{device_id}"));
    }
}

#[tokio::test]
async fn rust_client_reports_peers_and_clears_them_on_close() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let _server = RelayServer::spawn(port, &dir.path().join("relay.sqlite3"));

    let host = connect(port, "1", "host", false).await;
    let handler = Arc::new(PresenceRecorder::default());
    let client = Client::new(
        url(port, "2", "phone", false),
        Arc::new(MemoryStore::new()),
        handler.clone(),
    )
    .with_presence();
    let task = tokio::spawn(client.clone().into_task());

    let events = || handler.events.lock().unwrap().clone();
    let wait_for = |expected: &'static [&'static str]| {
        let events = &events;
        async move {
            for _ in 0..100 {
                if events() == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("expected {expected:?}, got {:?}", events());
        }
    };

    wait_for(&["+host"]).await;
    let host2 = connect(port, "1", "host2", false).await;
    wait_for(&["+host", "+host2"]).await;
    drop(host);
    wait_for(&["+host", "+host2", "-host"]).await;

    // A replacement is reported again without a disconnect in between.
    let host2_again = connect(port, "1", "host2", false).await;
    wait_for(&["+host", "+host2", "-host", "+host2"]).await;

    // Closing reports the remaining peers offline.
    client.close();
    task.await.unwrap().unwrap();
    assert_eq!(events(), ["+host", "+host2", "-host", "+host2", "-host2"]);
    drop(host2);
    drop(host2_again);
}
