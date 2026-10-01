use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{
        ConnectInfo, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relay::{ClientFrame, Endpoint, ServerFrame, decode_binary_frame};
use serde::Deserialize;
use tokio::time::{self, Instant};
use tracing::{Instrument, debug, error, info, warn};

use crate::{
    config::{Args, Limits},
    database::{CleanupStats, Database, validate_message_id, validate_target_device_id},
    hub::{ConnectionSender, Hub, connection_channel},
    keyed_lock::KeyedLock,
};

const MAX_ID_BYTES: usize = 256;
const MAX_MESSAGE_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    database: Arc<Database>,
    hub: Arc<Hub>,
    limits: Limits,
    /// Fine-grained keyed locks per relay_id to eliminate cross-pairing contention.
    delivery: Arc<KeyedLock>,
}

#[derive(Debug, Deserialize)]
struct ConnectQuery {
    id: String,
    endpoint: Endpoint,
    device_id: String,
    #[serde(default, deserialize_with = "deserialize_optional_bool")]
    ack_head: bool,
}

fn deserialize_optional_bool<'de, D>(deserializer: D) -> std::result::Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct BoolVisitor;

    impl<'de> Visitor<'de> for BoolVisitor {
        type Value = bool;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a boolean or boolean string")
        }

        fn visit_bool<E>(self, v: bool) -> std::result::Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(true),
                "false" | "0" | "no" | "" => Ok(false),
                _ => Err(de::Error::custom(format!("invalid boolean: {v}"))),
            }
        }
    }

    deserializer.deserialize_any(BoolVisitor)
}

async fn websocket_handler(
    State(state): State<AppState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Query(query): Query<ConnectQuery>,
    websocket: WebSocketUpgrade,
) -> Response {
    if let Err(error) = validate_connection(&query) {
        return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
    }
    websocket
        .max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| serve_socket(state, query, peer_addr, socket))
}

fn validate_connection(query: &ConnectQuery) -> Result<()> {
    anyhow::ensure!(!query.id.is_empty(), "id must not be empty");
    anyhow::ensure!(query.id.len() <= MAX_ID_BYTES, "id is too long");
    anyhow::ensure!(!query.device_id.is_empty(), "device_id must not be empty");
    anyhow::ensure!(query.device_id.len() <= MAX_ID_BYTES, "device_id is too long");
    Ok(())
}

async fn serve_socket(
    state: AppState,
    query: ConnectQuery,
    peer_addr: SocketAddr,
    socket: WebSocket,
) {
    let span = tracing::debug_span!(
        "websocket",
        relay_id = %query.id,
        endpoint = %query.endpoint,
        device_id = %query.device_id,
        %peer_addr
    );
    serve_socket_inner(state, query, socket)
        .instrument(span)
        .await;
}

async fn serve_socket_inner(state: AppState, query: ConnectQuery, mut socket: WebSocket) {
    let (tx, mut rx) = connection_channel();
    let (token, replayed) = {
        let _delivery = state.delivery.lock(&query.id).await;

        if let Err(error) = state
            .database
            .register_device(&query.id, query.endpoint, &query.device_id)
            .await
        {
            error!(%error, "failed to register relay device");
            reject_socket(
                &mut socket,
                "relay_storage_unavailable: device registration failed",
            )
            .await;
            return;
        }

        if query.ack_head {
            let res = state
                .database
                .acknowledge_head(&query.id, query.endpoint, &query.device_id)
                .await;
            if let Err(error) = res {
                error!(%error, "failed to acknowledge head for relay device");
                reject_socket(
                    &mut socket,
                    "relay_storage_unavailable: acknowledge head failed",
                )
                .await;
                return;
            }
        }

        let pending = match state
            .database
            .pending_for_device(&query.id, query.endpoint, &query.device_id)
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                error!(%error, "failed to read pending relay messages");
                reject_socket(
                    &mut socket,
                    "relay_storage_unavailable: pending messages could not be read",
                )
                .await;
                return;
            }
        };
        let replayed = pending.len();
        let token = state.hub.register(
            &query.id,
            query.endpoint,
            &query.device_id,
            tx.clone(),
            pending,
        );
        (token, replayed)
    };

    debug!(
        replayed_messages = replayed,
        device_id = %query.device_id,
        "WebSocket endpoint connected"
    );

    let (mut sink, mut stream) = socket.split();
    let ping_interval = state.limits.ping_interval;
    let idle_timeout = state.limits.idle_timeout;
    let writer = tokio::spawn(async move {
        // The server pings so that a half-open peer is detected even when no
        // application traffic is flowing: writes to a dead socket eventually
        // fail, and the read loop drops the connection once pongs stop.
        let mut ping = time::interval(ping_interval);
        ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        ping.reset();
        loop {
            tokio::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Bytes::from_static(b"relay"))).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Any inbound frame counts as proof of life; pongs arrive only while the
    // peer's TCP path is intact, so a silent connection is dropped here.
    let mut last_activity = Instant::now();
    loop {
        let result = time::timeout(idle_timeout, stream.next()).await;
        let message = match result {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(error))) => {
                debug!(%error, "WebSocket read failed");
                break;
            }
            Ok(None) => break,
            Err(_elapsed) => {
                let silent_for = last_activity.elapsed();
                warn!(
                    ?silent_for,
                    "WebSocket endpoint went silent; dropping half-open connection"
                );
                break;
            }
        };
        last_activity = Instant::now();
        if !state
            .hub
            .is_current(&query.id, query.endpoint, &query.device_id, token)
        {
            break;
        }
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Binary(bytes) => {
                match decode_binary_frame(&bytes) {
                    Ok((header, body_start)) => {
                        debug!(
                            message_id = %header.message_id,
                            target_device_id = ?header.target_device_id,
                            body_bytes = bytes.len() - body_start,
                            "binary message received"
                        );
                        // Forwarded unchanged: receivers parse the same header.
                        let size = bytes.len();
                        forward_ephemeral(
                            &state,
                            &query,
                            &tx,
                            header.message_id,
                            header.target_device_id.as_deref(),
                            Message::Binary(bytes),
                            size,
                        );
                    }
                    Err(error) => {
                        debug!(%error, "client sent an invalid binary frame");
                        tx.send(ServerFrame::Error {
                            message: format!("invalid frame: {error}"),
                        });
                    }
                }
                continue;
            }
        };
        let frame = match serde_json::from_str::<ClientFrame>(&text) {
            Ok(frame) => frame,
            Err(error) => {
                debug!(%error, "client sent an invalid frame");
                tx.send(ServerFrame::Error {
                    message: format!("invalid frame: {error}"),
                });
                continue;
            }
        };

        match frame {
            ClientFrame::Message {
                message_id,
                payload,
                target_device_id,
                ephemeral: true,
            } => {
                debug!(%message_id, ?target_device_id, payload = %payload, "ephemeral message received");
                let frame = ServerFrame::Ephemeral {
                    message_id: message_id.clone(),
                    payload,
                };
                let Ok(json) = serde_json::to_string(&frame) else {
                    continue;
                };
                let size = json.len();
                forward_ephemeral(
                    &state,
                    &query,
                    &tx,
                    message_id,
                    target_device_id.as_deref(),
                    Message::Text(json.into()),
                    size,
                );
            }
            ClientFrame::Message {
                message_id,
                payload,
                target_device_id,
                ephemeral: false,
            } => {
                debug!(%message_id, ?target_device_id, payload = %payload, "message received");
                let _delivery = state.delivery.lock(&query.id).await;
                match state
                    .database
                    .store(
                        &query.id,
                        query.endpoint,
                        &message_id,
                        &payload,
                        target_device_id.as_deref(),
                    )
                    .await
                {
                    Ok(stored) => {
                        tx.send(ServerFrame::Stored {
                            message_id: message_id.clone(),
                        });
                        if let Some(pending) = stored.pending {
                            state.hub.send_to_endpoint(
                                &query.id,
                                query.endpoint.opposite(),
                                target_device_id.as_deref(),
                                pending.into_frame(),
                            );
                        }
                    }
                    Err(error) => {
                        debug!(%message_id, %error, "message rejected");
                        tx.send(ServerFrame::Rejected {
                            message_id: message_id.clone(),
                            reason: error.to_string(),
                        });
                    }
                }
            }
            ClientFrame::Ack { sequence } => {
                debug!(sequence, "acknowledgement received");
                let _delivery = state.delivery.lock(&query.id).await;
                if let Err(error) = state
                    .database
                    .acknowledge(&query.id, query.endpoint, &query.device_id, sequence)
                    .await
                {
                    debug!(sequence, %error, "acknowledgement failed");
                    tx.send(ServerFrame::Error {
                        message: error.to_string(),
                    });
                }
            }
        }
    }

    state
        .hub
        .remove(&query.id, query.endpoint, &query.device_id, token);
    writer.abort();
    let _ = writer.await;
    debug!(device_id = %query.device_id, "WebSocket endpoint disconnected");
}

/// Delivers an ephemeral message to the peer endpoint's live devices without
/// storage. The sender only hears back when no device received it.
fn forward_ephemeral(
    state: &AppState,
    query: &ConnectQuery,
    tx: &ConnectionSender,
    message_id: String,
    target_device_id: Option<&str>,
    message: Message,
    size: usize,
) {
    let valid = validate_message_id(&message_id)
        .and_then(|()| target_device_id.map_or(Ok(()), validate_target_device_id));
    if let Err(error) = valid {
        debug!(%message_id, %error, "ephemeral message rejected");
        tx.send(ServerFrame::Rejected {
            message_id,
            reason: error.to_string(),
        });
        return;
    }
    let delivered = state.hub.send_ephemeral(
        &query.id,
        query.endpoint.opposite(),
        target_device_id,
        message,
        size,
    );
    debug!(%message_id, delivered, "ephemeral message forwarded");
    if delivered == 0 {
        tx.send(ServerFrame::Undeliverable {
            message_id,
            reason: "no connected device accepted the message".to_string(),
        });
    }
}

/// Sends a terminal error frame and closes the socket, used when a connection
/// cannot be served. The close frame still reaches the client while the
/// underlying connection is healthy.
async fn reject_socket(socket: &mut WebSocket, message: &str) {
    let error = ServerFrame::Error {
        message: message.to_string(),
    };
    if let Ok(json) = serde_json::to_string(&error) {
        let _ = socket.send(Message::Text(json.into())).await;
    }
    let _ = socket.send(Message::Close(None)).await;
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

pub(crate) async fn run(args: Args) -> Result<()> {
    let limits = Limits::from_args(&args)?;
    let database = Arc::new(Database::open(&args.database, limits).await?);
    report_cleanup(database.cleanup().await?);
    spawn_cleanup(
        database.clone(),
        Duration::from_secs(args.cleanup_interval_secs),
    );

    let state = AppState {
        database,
        hub: Arc::default(),
        limits,
        delivery: Arc::default(),
    };
    let router = Router::new()
        .route("/ws", get(websocket_handler))
        .route("/health", get(health_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("failed to bind {}", args.bind))?;
    info!(bind = %args.bind, "relay listening");
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

fn spawn_cleanup(database: Arc<Database>, cleanup_interval: Duration) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + cleanup_interval,
            cleanup_interval,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            match database.cleanup().await {
                Ok(stats) => report_cleanup(stats),
                Err(error) => error!(%error, "relay cleanup failed"),
            }
        }
    });
}

fn report_cleanup(stats: CleanupStats) {
    if stats.expired_pending > 0 || stats.expired_receipts > 0 {
        info!(
            expired_pending = stats.expired_pending,
            expired_receipts = stats.expired_receipts,
            "relay cleanup completed"
        );
    }
}
