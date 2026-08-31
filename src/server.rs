use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use relay::{ClientFrame, Endpoint, ServerFrame};
use serde::Deserialize;
use tokio::sync::mpsc;

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
    /// Serializes the short store/register/forward critical section so a
    /// message cannot land between a reconnect's database replay and its hub
    /// registration.
    delivery: Arc<Mutex<()>>,
}

#[derive(Debug, Deserialize)]
struct ConnectQuery {
    id: String,
    endpoint: Endpoint,
}

fn validate_connection(query: &ConnectQuery) -> Result<()> {
    anyhow::ensure!(!query.id.is_empty(), "id must not be empty");
    anyhow::ensure!(query.id.len() <= MAX_ID_BYTES, "id is too long");
    Ok(())
}

async fn websocket_handler(
    State(state): State<AppState>,
    Query(query): Query<ConnectQuery>,
    websocket: WebSocketUpgrade,
) -> Response {
    if let Err(error) = validate_connection(&query) {
        return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
    }
    websocket
        .max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| serve_socket(state, query, socket))
}

async fn serve_socket(state: AppState, query: ConnectQuery, mut socket: WebSocket) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let token = {
        let _delivery = state.delivery.lock().expect("delivery lock poisoned");
        if state.hub.contains(&query.id, query.endpoint) {
            None
        } else {
            let pending = match state.database.pending(&query.id, query.endpoint) {
                Ok(pending) => pending,
                Err(error) => {
                    eprintln!("failed to read pending relay messages: {error:#}");
                    return;
                }
            };
            Some(
                state
                    .hub
                    .register(&query.id, query.endpoint, tx.clone(), pending),
            )
        }
    };
    let Some(token) = token else {
        let conflict = ServerFrame::Error {
            message: "connection_conflict: this id and endpoint already has an active connection"
                .to_string(),
        };
        if let Ok(json) = serde_json::to_string(&conflict) {
            let _ = socket.send(Message::Text(json.into())).await;
        }
        let _ = socket.send(Message::Close(None)).await;
        return;
    };
    let (mut sink, mut stream) = socket.split();
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let Ok(json) = serde_json::to_string(&frame) else {
                continue;
            };
            if sink.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(result) = stream.next().await {
        if !state.hub.is_current(&query.id, query.endpoint, token) {
            break;
        }
        let message = match result {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => continue,
        };
        let frame = match serde_json::from_str::<ClientFrame>(&message) {
            Ok(frame) => frame,
            Err(error) => {
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
                let _delivery = state.delivery.lock().expect("delivery lock poisoned");
                match state
                    .database
                    .store(&query.id, query.endpoint, &message_id, &payload)
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
                        let _ = tx.send(ServerFrame::Error {
                            message: error.to_string(),
                        });
                    }
                }
            }
            ClientFrame::Ack { sequence } => {
                let _delivery = state.delivery.lock().expect("delivery lock poisoned");
                if let Err(error) = state
                    .database
                    .acknowledge(&query.id, query.endpoint, sequence)
                {
                    let _ = tx.send(ServerFrame::Error {
                        message: error.to_string(),
                    });
                }
            }
        }
    }

    state.hub.remove(&query.id, query.endpoint, token);
    writer.abort();
}

pub(crate) async fn run(args: Args) -> Result<()> {
    let limits = Limits::from_args(&args)?;
    let database = Arc::new(Database::open(&args.database, limits)?);
    report_cleanup(database.cleanup()?);
    spawn_cleanup(
        database.clone(),
        Duration::from_secs(args.cleanup_interval_secs),
    );

    let state = AppState {
        database,
        hub: Arc::default(),
        delivery: Arc::default(),
    };
    let router = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("failed to bind {}", args.bind))?;
    println!("relay listening on ws://{}/ws", args.bind);
    axum::serve(listener, router).await?;
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
            let database = database.clone();
            match tokio::task::spawn_blocking(move || database.cleanup()).await {
                Ok(Ok(stats)) => report_cleanup(stats),
                Ok(Err(error)) => eprintln!("relay cleanup failed: {error:#}"),
                Err(error) => eprintln!("relay cleanup task failed: {error}"),
            }
        }
    });
}

fn report_cleanup(stats: CleanupStats) {
    if stats.expired_pending > 0 || stats.expired_receipts > 0 {
        eprintln!(
            "relay cleanup removed {} pending messages and {} receipts",
            stats.expired_pending, stats.expired_receipts
        );
    }
}
