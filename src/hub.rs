use std::{
    collections::{HashMap, hash_map::Entry},
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
    /// Indexed by `(relay_id, endpoint)` -> `device_id` -> `LiveConnection`.
    /// This allows O(1) direct broadcast to all active devices on a specific endpoint,
    /// eliminating flat scans across all connections on the server.
    connections: Mutex<HashMap<(String, Endpoint), HashMap<String, LiveConnection>>>,
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
        let endpoint_connections = connections
            .entry((relay_id.to_string(), endpoint))
            .or_default();
        if let Some(previous) = endpoint_connections.insert(
            device_id.to_string(),
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

    /// Broadcasts a server frame to all active devices of the specified endpoint in O(devices on endpoint).
    pub(crate) fn send_to_endpoint(&self, relay_id: &str, endpoint: Endpoint, frame: ServerFrame) {
        let matching: Vec<_> = {
            let connections = self.connections.lock().expect("hub lock poisoned");
            connections
                .get(&(relay_id.to_string(), endpoint))
                .map(|devices| devices.values().map(|conn| conn.tx.clone()).collect())
                .unwrap_or_default()
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
            .get(&(relay_id.to_string(), endpoint))
            .and_then(|devices| devices.get(device_id))
            .is_some_and(|connection| connection.token == token)
    }

    pub(crate) fn remove(&self, relay_id: &str, endpoint: Endpoint, device_id: &str, token: u64) {
        let mut connections = self.connections.lock().expect("hub lock poisoned");
        let key = (relay_id.to_string(), endpoint);
        if let Entry::Occupied(mut entry) = connections.entry(key) {
            let devices = entry.get_mut();
            if devices
                .get(device_id)
                .is_some_and(|connection| connection.token == token)
            {
                devices.remove(device_id);
                if devices.is_empty() {
                    entry.remove();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index_lookup_broadcasts_only_to_target_endpoint_and_pairing() {
        let hub = Hub::default();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();

        // Register dev1 on (pair-1, Endpoint::One)
        let _t1 = hub.register("pair-1", Endpoint::One, "dev1", tx1, Vec::new());
        // Register dev2 on (pair-1, Endpoint::Two)
        let _t2 = hub.register("pair-1", Endpoint::Two, "dev2", tx2, Vec::new());
        // Register dev3 on (pair-2, Endpoint::Two)
        let _t3 = hub.register("pair-2", Endpoint::Two, "dev3", tx3, Vec::new());

        // Drain Ready frames
        assert!(matches!(rx1.recv().await, Some(ServerFrame::Ready { .. })));
        assert!(matches!(rx2.recv().await, Some(ServerFrame::Ready { .. })));
        assert!(matches!(rx3.recv().await, Some(ServerFrame::Ready { .. })));

        // Send to (pair-1, Endpoint::Two)
        let test_frame = ServerFrame::Stored {
            message_id: "test-stored".to_string(),
        };
        hub.send_to_endpoint("pair-1", Endpoint::Two, test_frame.clone());

        // dev2 must receive the frame
        assert_eq!(rx2.recv().await, Some(test_frame));

        // dev1 and dev3 must NOT receive the frame
        assert!(rx1.try_recv().is_err());
        assert!(rx3.try_recv().is_err());
    }

    #[tokio::test]
    async fn preemption_and_clean_removal() {
        let hub = Hub::default();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, _rx2) = mpsc::unbounded_channel();
        let t1 = hub.register("pair-x", Endpoint::One, "phone", tx1, Vec::new());
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t1));

        // Preempt with new connection on same device
        let t2 = hub.register("pair-x", Endpoint::One, "phone", tx2, Vec::new());
        assert!(!hub.is_current("pair-x", Endpoint::One, "phone", t1));
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t2));

        // rx1 must have received preemption error
        let ready1 = rx1.recv().await.unwrap();
        assert!(matches!(ready1, ServerFrame::Ready { .. }));
        let err1 = rx1.recv().await.unwrap();
        assert!(matches!(err1, ServerFrame::Error { message, .. } if message.starts_with("connection_replaced")));

        // Removing with old token does nothing
        hub.remove("pair-x", Endpoint::One, "phone", t1);
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t2));

        // Removing with current token removes the connection and drops empty endpoint map
        hub.remove("pair-x", Endpoint::One, "phone", t2);
        assert!(!hub.is_current("pair-x", Endpoint::One, "phone", t2));
        assert!(hub.connections.lock().unwrap().is_empty());
    }
}
