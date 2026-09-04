//! WebSocket transport for the relay client.
//!
//! Connects to the relay URL, sends protocol-level pings so the server's
//! idle timeout sees this client as alive, and decodes [`ServerFrame`]s from
//! text messages. The server's pings are answered automatically by
//! tungstenite.

use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};

use crate::relay_frame::{ClientFrame, Endpoint, ServerFrame};

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
    if url.contains('?') {
        format!("{url}&ack_head=true")
    } else {
        format!("{url}?ack_head=true")
    }
}

pub async fn connect(url: &str) -> Result<Transport> {
    let (socket, _) = connect_async(url)
        .await
        .with_context(|| format!("failed to connect to relay at {url}"))?;
    let (mut sink, stream) = socket.split();
    // Pings and data frames share one sink, so a single task owns it and
    // receives both ping ticks and outbound frames from a channel.
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<Message>(16);
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
        frame_tx,
        stream,
        ping_task,
    })
}

/// A connected relay socket. Dropping it aborts the ping task and closes the
/// connection.
pub struct Transport {
    frame_tx: tokio::sync::mpsc::Sender<Message>,
    stream: futures_util::stream::SplitStream<Socket>,
    ping_task: tokio::task::JoinHandle<()>,
}

impl Transport {
    /// Sends one client frame as JSON text.
    pub async fn send_frame(&mut self, frame: &ClientFrame) -> Result<()> {
        let json = serde_json::to_string(frame).context("failed to encode relay frame")?;
        self.frame_tx
            .send(Message::Text(json.into()))
            .await
            .context("failed to send relay frame")
    }

    /// Waits for the next server frame. Returns `Ok(None)` on a clean close.
    pub async fn next_frame(&mut self) -> Result<Option<ServerFrame>> {
        loop {
            let Some(message) = self.stream.next().await else {
                return Ok(None);
            };
            match message {
                Ok(Message::Text(text)) => {
                    let frame = serde_json::from_str::<ServerFrame>(&text)
                        .context("relay returned an invalid frame")?;
                    return Ok(Some(frame));
                }
                Ok(Message::Close(_)) => return Ok(None),
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_)) => continue,
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

    #[test]
    fn endpoint_url_appends_query() {
        assert_eq!(
            endpoint_url("ws://127.0.0.1:39371/ws", "abc", Endpoint::One, "phone-1"),
            "ws://127.0.0.1:39371/ws?id=abc&endpoint=1&device_id=phone-1"
        );
    }
}
