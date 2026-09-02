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
use relay::{ClientFrame, Endpoint, ServerFrame};
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{self, Instant};
use tracing::{Instrument, debug, error, info, warn};

use crate::{
    config::{Args, Limits},
    database::{CleanupStats, Database},
    hub::Hub,
};

const MAX_ID_BYTES: usize = 256;
const MAX_MESSAGE_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    database: Arc<Database>,
    hub: Arc<Hub>,
    limits: Limits,
    /// Prevents a message from landing between reconnect replay and hub registration.
    delivery: Arc<Mutex<()>>,
}

#[derive(Debug, Deserialize)]
struct ConnectQuery {
    id: String,
    endpoint: Endpoint,
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
        %peer_addr
    );
    serve_socket_inner(state, query, socket)
        .instrument(span)
        .await;
}

async fn serve_socket_inner(state: AppState, query: ConnectQuery, mut socket: WebSocket) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let registration = {
        let _delivery = state.delivery.lock().await;
        if state.hub.contains(&query.id, query.endpoint) {
            None
        } else {
            let pending = match state.database.pending(&query.id, query.endpoint).await {
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
            let token = state
                .hub
                .register(&query.id, query.endpoint, tx.clone(), pending);
            Some((token, replayed))
        }
    };

    let Some((token, replayed)) = registration else {
        debug!("WebSocket connection rejected: endpoint already connected");
        reject_socket(
            &mut socket,
            "connection_conflict: this id and endpoint already has an active connection",
        )
        .await;
        return;
    };
    debug!(replayed_messages = replayed, "WebSocket endpoint connected");

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
                frame = rx.recv() => {
                    let Some(frame) = frame else {
                        break;
                    };
                    let Ok(json) = serde_json::to_string(&frame) else {
                        continue;
                    };
                    if sink.send(Message::Text(json.into())).await.is_err() {
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
        if !state.hub.is_current(&query.id, query.endpoint, token) {
            break;
        }
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Binary(_) => continue,
        };
        let frame = match serde_json::from_str::<ClientFrame>(&text) {
            Ok(frame) => frame,
            Err(error) => {
                debug!(%error, "client sent an invalid frame");
                let _ = tx.send(ServerFrame::Error {
                    message: format!("invalid frame: {error}"),
                });
                continue;
            }
        };

        match frame {
            ClientFrame::Message {
                message_id,
                payload,
            } => {
                debug!(%message_id, payload = %payload, "message received");
                let _delivery = state.delivery.lock().await;
                match state
                    .database
                    .store(&query.id, query.endpoint, &message_id, &payload)
                    .await
                {
                    Ok(stored) => {
                        let _ = tx.send(ServerFrame::Stored {
                            message_id: message_id.clone(),
                        });
                        if let Some(pending) = stored.pending {
                            state.hub.send(
                                &query.id,
                                query.endpoint.opposite(),
                                pending.into_frame(),
                            );
                        }
                    }
                    Err(error) => {
                        debug!(%message_id, %error, "message rejected");
                        let _ = tx.send(ServerFrame::Error {
                            message: error.to_string(),
                        });
                    }
                }
            }
            ClientFrame::Ack { sequence } => {
                debug!(sequence, "acknowledgement received");
                let _delivery = state.delivery.lock().await;
                if let Err(error) = state
                    .database
                    .acknowledge(&query.id, query.endpoint, sequence)
                    .await
                {
                    debug!(sequence, %error, "acknowledgement failed");
                    let _ = tx.send(ServerFrame::Error {
                        message: error.to_string(),
                    });
                }
            }
        }
    }

    state.hub.remove(&query.id, query.endpoint, token);
    writer.abort();
    let _ = writer.await;
    debug!("WebSocket endpoint disconnected");
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
