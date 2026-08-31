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
    connections: Mutex<HashMap<(String, Endpoint), LiveConnection>>,
    next_token: AtomicU64,
}

impl Hub {
    pub(crate) fn contains(&self, relay_id: &str, endpoint: Endpoint) -> bool {
        self.connections
            .lock()
            .expect("hub lock poisoned")
            .contains_key(&(relay_id.to_string(), endpoint))
    }

    pub(crate) fn register(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        tx: mpsc::UnboundedSender<ServerFrame>,
        pending: Vec<StoredMessage>,
    ) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
        let mut connections = self.connections.lock().expect("hub lock poisoned");
        let previous = connections.insert(
            (relay_id.to_string(), endpoint),
            LiveConnection {
                token,
                tx: tx.clone(),
            },
        );
        debug_assert!(previous.is_none(), "active relay connection was replaced");
        let _ = tx.send(ServerFrame::Ready { endpoint });
        for message in pending {
            let _ = tx.send(message.into_frame());
        }
        token
    }

    pub(crate) fn send(&self, relay_id: &str, endpoint: Endpoint, frame: ServerFrame) {
        let connection = self
            .connections
            .lock()
            .expect("hub lock poisoned")
            .get(&(relay_id.to_string(), endpoint))
            .cloned();
        if let Some(connection) = connection {
            let _ = connection.tx.send(frame);
        }
    }

    pub(crate) fn is_current(&self, relay_id: &str, endpoint: Endpoint, token: u64) -> bool {
        self.connections
            .lock()
            .expect("hub lock poisoned")
            .get(&(relay_id.to_string(), endpoint))
            .is_some_and(|connection| connection.token == token)
    }

    pub(crate) fn remove(&self, relay_id: &str, endpoint: Endpoint, token: u64) {
        let mut connections = self.connections.lock().expect("hub lock poisoned");
        let key = (relay_id.to_string(), endpoint);
        if connections
            .get(&key)
            .is_some_and(|connection| connection.token == token)
        {
            connections.remove(&key);
        }
    }
}
