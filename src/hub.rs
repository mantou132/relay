use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use relay::{Endpoint, ServerFrame};
use tokio::sync::mpsc;

use crate::database::StoredMessage;

#[derive(Clone)]
struct LiveConnection {
    token: u64,
    tx: mpsc::UnboundedSender<ServerFrame>,
}

#[derive(Default)]
pub(crate) struct Hub {
    connections: Mutex<HashMap<(String, Endpoint, String), LiveConnection>>,
    next_token: AtomicU64,
}

impl Hub {
    /// Registers a connection for a given `(relay_id, endpoint, device_id)`.
    ///
    /// If an existing active connection exists for the same device, it is
    /// preempted (kicked) so mobile devices reconnecting or switching networks
    /// can take over their session immediately without waiting for timeouts.
    pub(crate) fn register(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        device_id: &str,
        tx: mpsc::UnboundedSender<ServerFrame>,
        pending: Vec<StoredMessage>,
    ) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
        let mut connections = self.connections.lock().expect("hub lock poisoned");
        let key = (relay_id.to_string(), endpoint, device_id.to_string());
        if let Some(previous) = connections.insert(
            key,
            LiveConnection {
                token,
                tx: tx.clone(),
            },
        ) {
            let _ = previous.tx.send(ServerFrame::Error {
                message: "connection_replaced: another connection opened for this device"
                    .to_string(),
            });
        }

        let _ = tx.send(ServerFrame::Ready { endpoint });
        for message in pending {
            let _ = tx.send(message.into_frame());
        }
        token
    }

    /// Broadcasts a server frame to all active devices of the specified endpoint.
    pub(crate) fn send_to_endpoint(&self, relay_id: &str, endpoint: Endpoint, frame: ServerFrame) {
        let matching: Vec<_> = {
            let connections = self.connections.lock().expect("hub lock poisoned");
            connections
                .iter()
                .filter(|((r_id, ep, _), _)| r_id == relay_id && *ep == endpoint)
                .map(|(_, conn)| conn.tx.clone())
                .collect()
        };
        for tx in matching {
            let _ = tx.send(frame.clone());
        }
    }

    pub(crate) fn is_current(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        device_id: &str,
        token: u64,
    ) -> bool {
        self.connections
            .lock()
            .expect("hub lock poisoned")
            .get(&(relay_id.to_string(), endpoint, device_id.to_string()))
            .is_some_and(|connection| connection.token == token)
    }

    pub(crate) fn remove(&self, relay_id: &str, endpoint: Endpoint, device_id: &str, token: u64) {
        let mut connections = self.connections.lock().expect("hub lock poisoned");
        let key = (relay_id.to_string(), endpoint, device_id.to_string());
        if connections
            .get(&key)
            .is_some_and(|connection| connection.token == token)
        {
            connections.remove(&key);
        }
    }
}
