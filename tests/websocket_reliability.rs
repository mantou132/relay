use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use relay::{ClientFrame, Endpoint, ServerFrame};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, Result as WebSocketResult},
};

struct RelayServer {
    child: Option<Child>,
}

impl RelayServer {
    fn spawn(port: u16, database: &Path) -> Self {
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
        Self { child: Some(child) }
    }

    fn stop(mut self) {
        self.terminate();
    }

    fn spawn_with_debug_logs(port: u16, database: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_relay"))
            .args([
                "--debug",
                "--bind",
                &format!("127.0.0.1:{port}"),
                "--database",
                database.to_str().unwrap(),
            ])
            .env_remove("RUST_LOG")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Self { child: Some(child) }
    }

    fn stop_and_collect_stderr(mut self) -> String {
        let mut child = self.child.take().unwrap();
        let _ = child.kill();
        let output = child.wait_with_output().unwrap();
        String::from_utf8(output.stderr).unwrap()
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

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(port: u16, id: &str, endpoint: Endpoint) -> Socket {
    let url = format!("ws://127.0.0.1:{port}/ws?id={id}&endpoint={endpoint}");
    for _ in 0..100 {
        match connect_async(&url).await {
            Ok((socket, _)) => return socket,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    panic!("relay server did not start at {url}");
}

async fn send(socket: &mut Socket, frame: &ClientFrame) -> WebSocketResult<()> {
    socket
        .send(Message::Text(serde_json::to_string(frame).unwrap().into()))
        .await
}

async fn receive(socket: &mut Socket) -> ServerFrame {
    loop {
        match socket.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected WebSocket message: {other:?}"),
        }
    }
}

async fn expect_ready(socket: &mut Socket, endpoint: Endpoint) {
    assert_eq!(receive(socket).await, ServerFrame::Ready { endpoint });
}

#[tokio::test]
async fn isolates_pairs_and_replays_after_disconnect_and_server_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("relay.sqlite3");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = RelayServer::spawn(port, &database);

    let mut endpoint_one = connect(port, "pair-a", Endpoint::One).await;
    expect_ready(&mut endpoint_one, Endpoint::One).await;
    let mut duplicate_endpoint_one = connect(port, "pair-a", Endpoint::One).await;
    assert_eq!(
        receive(&mut duplicate_endpoint_one).await,
        ServerFrame::Error {
            message: "connection_conflict: this id and endpoint already has an active connection"
                .to_string(),
        }
    );
    assert!(matches!(
        duplicate_endpoint_one.next().await,
        Some(Ok(Message::Close(_))) | None
    ));
    let mut unrelated_endpoint_two = connect(port, "pair-b", Endpoint::Two).await;
    expect_ready(&mut unrelated_endpoint_two, Endpoint::Two).await;

    send(
        &mut endpoint_one,
        &ClientFrame::Message {
            message_id: "endpoint_one-1".to_string(),
            payload: json!({ "request": 1 }),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut endpoint_one).await,
        ServerFrame::Stored {
            message_id: "endpoint_one-1".to_string()
        }
    );
    let mut unrelated_endpoint_one = connect(port, "pair-b", Endpoint::One).await;
    expect_ready(&mut unrelated_endpoint_one, Endpoint::One).await;
    send(
        &mut unrelated_endpoint_one,
        &ClientFrame::Message {
            message_id: "pair-b-sentinel".to_string(),
            payload: json!({ "pair": "b" }),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut unrelated_endpoint_one).await,
        ServerFrame::Stored {
            message_id: "pair-b-sentinel".to_string(),
        }
    );
    assert_eq!(
        receive(&mut unrelated_endpoint_two).await,
        ServerFrame::Message {
            message_id: "pair-b-sentinel".to_string(),
            sequence: 1,
            payload: json!({ "pair": "b" }),
        },
        "pair-b received a message belonging to another relay id"
    );

    let mut endpoint_two = connect(port, "pair-a", Endpoint::Two).await;
    expect_ready(&mut endpoint_two, Endpoint::Two).await;
    assert_eq!(
        receive(&mut endpoint_two).await,
        ServerFrame::Message {
            message_id: "endpoint_one-1".to_string(),
            sequence: 1,
            payload: json!({ "request": 1 }),
        }
    );
    send(&mut endpoint_two, &ClientFrame::Ack { sequence: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    endpoint_two.close(None).await.unwrap();

    send(
        &mut endpoint_one,
        &ClientFrame::Message {
            message_id: "endpoint_one-2".to_string(),
            payload: json!({ "request": 2 }),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut endpoint_one).await,
        ServerFrame::Stored {
            message_id: "endpoint_one-2".to_string()
        }
    );
    drop(endpoint_one);
    server.stop();

    let _restarted_server = RelayServer::spawn(port, &database);
    let mut reconnected_endpoint_two = connect(port, "pair-a", Endpoint::Two).await;
    expect_ready(&mut reconnected_endpoint_two, Endpoint::Two).await;
    assert_eq!(
        receive(&mut reconnected_endpoint_two).await,
        ServerFrame::Message {
            message_id: "endpoint_one-2".to_string(),
            sequence: 2,
            payload: json!({ "request": 2 }),
        }
    );
}

#[tokio::test]
async fn debug_logs_connections_messages_and_disconnects() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("relay.sqlite3");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = RelayServer::spawn_with_debug_logs(port, &database);

    let mut socket = connect(port, "logged-pair", Endpoint::One).await;
    expect_ready(&mut socket, Endpoint::One).await;
    send(
        &mut socket,
        &ClientFrame::Message {
            message_id: "logged-message".to_string(),
            payload: json!({ "hello": "logs" }),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut socket).await,
        ServerFrame::Stored {
            message_id: "logged-message".to_string(),
        }
    );
    socket.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let logs = server.stop_and_collect_stderr();
    assert!(logs.contains("WebSocket endpoint connected"), "{logs}");
    assert!(logs.contains("relay_id=logged-pair"), "{logs}");
    assert!(logs.contains("message_id=logged-message"), "{logs}");
    assert!(logs.contains("payload={\"hello\":\"logs\"}"), "{logs}");
    assert!(logs.contains("WebSocket endpoint disconnected"), "{logs}");
}
