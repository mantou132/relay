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

async fn connect_device(port: u16, id: &str, endpoint: Endpoint, device_id: &str) -> Socket {
    let url = format!("ws://127.0.0.1:{port}/ws?id={id}&endpoint={endpoint}&device_id={device_id}");
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
async fn multi_device_broadcast_and_switching_without_gaps() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("relay.sqlite3");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _server = RelayServer::spawn(port, &database);

    // 1. Phone A and Phone B connect to Endpoint 2 simultaneously
    let mut phone_a = connect_device(port, "pair-multi", Endpoint::Two, "phone_a").await;
    expect_ready(&mut phone_a, Endpoint::Two).await;

    let mut phone_b = connect_device(port, "pair-multi", Endpoint::Two, "phone_b").await;
    expect_ready(&mut phone_b, Endpoint::Two).await;

    // 2. Desktop connects to Endpoint 1
    let mut desktop = connect_device(port, "pair-multi", Endpoint::One, "desktop").await;
    expect_ready(&mut desktop, Endpoint::One).await;

    // 3. Desktop sends a message -> destined for Endpoint 2
    send(
        &mut desktop,
        &ClientFrame::Message {
            message_id: "m-1".to_string(),
            payload: json!({ "content": "broadcast_to_both" }),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        receive(&mut desktop).await,
        ServerFrame::Stored {
            message_id: "m-1".to_string(),
        }
    );

    // Both Phone A and Phone B should receive the message in real-time!
    let frame_a = receive(&mut phone_a).await;
    assert_eq!(
        frame_a,
        ServerFrame::Message {
            message_id: "m-1".to_string(),
            sequence: 1,
            payload: json!({ "content": "broadcast_to_both" }),
        }
    );

    let frame_b = receive(&mut phone_b).await;
    assert_eq!(
        frame_b,
        ServerFrame::Message {
            message_id: "m-1".to_string(),
            sequence: 1,
            payload: json!({ "content": "broadcast_to_both" }),
        }
    );

    // Phone A and Phone B both ack sequence 1
    send(&mut phone_a, &ClientFrame::Ack { sequence: 1 }).await.unwrap();
    send(&mut phone_b, &ClientFrame::Ack { sequence: 1 }).await.unwrap();

    // 4. Now simulate Phone B going offline (disconnects)
    phone_b.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 5. Desktop sends message 2 while Phone B is offline
    send(
        &mut desktop,
        &ClientFrame::Message {
            message_id: "m-2".to_string(),
            payload: json!({ "content": "offline_for_b" }),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut desktop).await,
        ServerFrame::Stored {
            message_id: "m-2".to_string(),
        }
    );

    // Phone A is online and receives message 2 immediately
    assert_eq!(
        receive(&mut phone_a).await,
        ServerFrame::Message {
            message_id: "m-2".to_string(),
            sequence: 2,
            payload: json!({ "content": "offline_for_b" }),
        }
    );
    send(&mut phone_a, &ClientFrame::Ack { sequence: 2 }).await.unwrap();

    // 6. Now Phone A goes offline (user switches to Phone B!)
    phone_a.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 7. Phone B reconnects! Phone B should immediately receive message 2 (which it missed)!
    let mut phone_b_reconnected = connect_device(port, "pair-multi", Endpoint::Two, "phone_b").await;
    expect_ready(&mut phone_b_reconnected, Endpoint::Two).await;

    // Phone B gets the replayed message 2
    assert_eq!(
        receive(&mut phone_b_reconnected).await,
        ServerFrame::Message {
            message_id: "m-2".to_string(),
            sequence: 2,
            payload: json!({ "content": "offline_for_b" }),
        }
    );
    send(&mut phone_b_reconnected, &ClientFrame::Ack { sequence: 2 }).await.unwrap();

    // 8. Desktop sends message 3: Phone B receives it directly!
    send(
        &mut desktop,
        &ClientFrame::Message {
            message_id: "m-3".to_string(),
            payload: json!({ "content": "seamless_continuation" }),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut desktop).await,
        ServerFrame::Stored {
            message_id: "m-3".to_string(),
        }
    );

    assert_eq!(
        receive(&mut phone_b_reconnected).await,
        ServerFrame::Message {
            message_id: "m-3".to_string(),
            sequence: 3,
            payload: json!({ "content": "seamless_continuation" }),
        }
    );
}

#[tokio::test]
async fn same_device_reconnection_preempts_old_socket() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("relay.sqlite3");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _server = RelayServer::spawn(port, &database);

    // Phone A connects
    let mut phone_a_old = connect_device(port, "pair-preempt", Endpoint::Two, "phone_a").await;
    expect_ready(&mut phone_a_old, Endpoint::Two).await;

    // Phone A reconnects with the same device_id (e.g. app reopened or network switched)
    let mut phone_a_new = connect_device(port, "pair-preempt", Endpoint::Two, "phone_a").await;
    expect_ready(&mut phone_a_new, Endpoint::Two).await;

    // The old socket should receive connection_replaced and close
    let old_frame = receive(&mut phone_a_old).await;
    assert_eq!(
        old_frame,
        ServerFrame::Error {
            message: "connection_replaced: another connection opened for this device".to_string(),
        }
    );

    // The new socket is active and operational!
    let mut desktop = connect_device(port, "pair-preempt", Endpoint::One, "desktop").await;
    expect_ready(&mut desktop, Endpoint::One).await;

    send(
        &mut desktop,
        &ClientFrame::Message {
            message_id: "preempt-1".to_string(),
            payload: json!({ "live": true }),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        receive(&mut phone_a_new).await,
        ServerFrame::Message {
            message_id: "preempt-1".to_string(),
            sequence: 1,
            payload: json!({ "live": true }),
        }
    );
}
