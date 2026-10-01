use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use axum::extract::ws::Message;
use relay::{Endpoint, ServerFrame};
use tokio::sync::mpsc;

use crate::database::StoredMessage;

/// Bytes of ephemeral messages one connection may have queued but not yet
/// written. Beyond it, further ephemeral messages skip that connection rather
/// than letting a slow receiver grow relay memory. A single message larger
/// than the budget still passes when the queue is empty.
const MAX_QUEUED_EPHEMERAL_BYTES: usize = 16 * 1024 * 1024;

enum Outgoing {
    Frame(ServerFrame),
    /// Pre-encoded so a broadcast shares one buffer across devices.
    Ephemeral { message: Message, size: usize },
}

/// Sending half of one connection's outgoing queue.
#[derive(Clone)]
pub(crate) struct ConnectionSender {
    tx: mpsc::UnboundedSender<Outgoing>,
    queued_ephemeral: Arc<AtomicUsize>,
}

impl ConnectionSender {
    pub(crate) fn send(&self, frame: ServerFrame) {
        let _ = self.tx.send(Outgoing::Frame(frame));
    }

    fn send_ephemeral(&self, message: Message, size: usize) -> bool {
        let reserved = self
            .queued_ephemeral
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                (queued == 0 || queued + size <= MAX_QUEUED_EPHEMERAL_BYTES)
                    .then_some(queued + size)
            })
            .is_ok();
        if reserved && self.tx.send(Outgoing::Ephemeral { message, size }).is_err() {
            self.queued_ephemeral.fetch_sub(size, Ordering::AcqRel);
            return false;
        }
        reserved
    }
}

/// Receiving half, owned by the connection's writer task.
pub(crate) struct ConnectionReceiver {
    rx: mpsc::UnboundedReceiver<Outgoing>,
    queued_ephemeral: Arc<AtomicUsize>,
}

impl ConnectionReceiver {
    pub(crate) async fn recv(&mut self) -> Option<Message> {
        loop {
            match self.rx.recv().await? {
                Outgoing::Frame(frame) => match serde_json::to_string(&frame) {
                    Ok(json) => return Some(Message::Text(json.into())),
                    Err(_) => continue,
                },
                Outgoing::Ephemeral { message, size } => {
                    // Released once handed to the socket writer, which is
                    // where a slow receiver's backlog actually accumulates.
                    self.queued_ephemeral.fetch_sub(size, Ordering::AcqRel);
                    return Some(message);
                }
            }
        }
    }
}

pub(crate) fn connection_channel() -> (ConnectionSender, ConnectionReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    let queued_ephemeral = Arc::new(AtomicUsize::new(0));
    (
        ConnectionSender {
            tx,
            queued_ephemeral: queued_ephemeral.clone(),
        },
        ConnectionReceiver {
            rx,
            queued_ephemeral,
        },
    )
}

#[derive(Clone)]
struct LiveConnection {
    token: u64,
    tx: ConnectionSender,
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
        tx: ConnectionSender,
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
            previous.tx.send(ServerFrame::Error {
                message: "connection_replaced: another connection opened for this device"
                    .to_string(),
            });
        }

        tx.send(ServerFrame::Ready { endpoint });
        for message in pending {
            tx.send(message.into_frame());
        }
        token
    }

    /// Broadcasts a server frame to all active devices of the specified endpoint,
    /// or only to the specified target device if `target_device_id` is provided.
    pub(crate) fn send_to_endpoint(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        target_device_id: Option<&str>,
        frame: ServerFrame,
    ) {
        for tx in self.matching(relay_id, endpoint, target_device_id) {
            tx.send(frame.clone());
        }
    }

    /// Forwards an ephemeral message to the matching live devices and returns
    /// how many accepted it.
    pub(crate) fn send_ephemeral(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        target_device_id: Option<&str>,
        message: Message,
        size: usize,
    ) -> usize {
        self.matching(relay_id, endpoint, target_device_id)
            .into_iter()
            .filter(|tx| tx.send_ephemeral(message.clone(), size))
            .count()
    }

    fn matching(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        target_device_id: Option<&str>,
    ) -> Vec<ConnectionSender> {
        {
            let connections = self.connections.lock().expect("hub lock poisoned");
            if let Some(devices) = connections.get(&(relay_id.to_string(), endpoint)) {
                if let Some(target) = target_device_id {
                    devices
                        .get(target)
                        .map(|conn| vec![conn.tx.clone()])
                        .unwrap_or_default()
                } else {
                    devices.values().map(|conn| conn.tx.clone()).collect()
                }
            } else {
                Vec::new()
            }
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

    async fn frame(rx: &mut ConnectionReceiver) -> Option<ServerFrame> {
        match rx.recv().await? {
            Message::Text(text) => serde_json::from_str(&text).ok(),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn index_lookup_broadcasts_only_to_target_endpoint_and_pairing() {
        let hub = Hub::default();
        let (tx1, mut rx1) = connection_channel();
        let (tx2, mut rx2) = connection_channel();
        let (tx3, mut rx3) = connection_channel();

        // Register dev1 on (pair-1, Endpoint::One)
        let _t1 = hub.register("pair-1", Endpoint::One, "dev1", tx1, Vec::new());
        // Register dev2 on (pair-1, Endpoint::Two)
        let _t2 = hub.register("pair-1", Endpoint::Two, "dev2", tx2, Vec::new());
        // Register dev3 on (pair-2, Endpoint::Two)
        let _t3 = hub.register("pair-2", Endpoint::Two, "dev3", tx3, Vec::new());

        // Drain Ready frames
        assert!(matches!(frame(&mut rx1).await, Some(ServerFrame::Ready { .. })));
        assert!(matches!(frame(&mut rx2).await, Some(ServerFrame::Ready { .. })));
        assert!(matches!(frame(&mut rx3).await, Some(ServerFrame::Ready { .. })));

        // Send broadcast to (pair-1, Endpoint::Two)
        let test_frame = ServerFrame::Stored {
            message_id: "test-stored".to_string(),
        };
        hub.send_to_endpoint("pair-1", Endpoint::Two, None, test_frame.clone());

        // dev2 must receive the frame
        assert_eq!(frame(&mut rx2).await, Some(test_frame));

        // dev1 and dev3 must NOT receive the frame
        assert!(rx1.rx.try_recv().is_err());
        assert!(rx3.rx.try_recv().is_err());

        // Register dev4 also on (pair-1, Endpoint::Two)
        let (tx4, mut rx4) = connection_channel();
        let _t4 = hub.register("pair-1", Endpoint::Two, "dev4", tx4, Vec::new());
        assert!(matches!(frame(&mut rx4).await, Some(ServerFrame::Ready { .. })));

        // Send targeted to dev4 only
        let targeted_frame = ServerFrame::Stored {
            message_id: "test-targeted".to_string(),
        };
        hub.send_to_endpoint("pair-1", Endpoint::Two, Some("dev4"), targeted_frame.clone());

        // dev4 receives it
        assert_eq!(frame(&mut rx4).await, Some(targeted_frame));
        // dev2 does NOT receive it!
        assert!(rx2.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn preemption_and_clean_removal() {
        let hub = Hub::default();
        let (tx1, mut rx1) = connection_channel();
        let (tx2, _rx2) = connection_channel();
        let t1 = hub.register("pair-x", Endpoint::One, "phone", tx1, Vec::new());
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t1));

        // Preempt with new connection on same device
        let t2 = hub.register("pair-x", Endpoint::One, "phone", tx2, Vec::new());
        assert!(!hub.is_current("pair-x", Endpoint::One, "phone", t1));
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t2));

        // rx1 must have received preemption error
        let ready1 = frame(&mut rx1).await.unwrap();
        assert!(matches!(ready1, ServerFrame::Ready { .. }));
        let err1 = frame(&mut rx1).await.unwrap();
        assert!(matches!(err1, ServerFrame::Error { message, .. } if message.starts_with("connection_replaced")));

        // Removing with old token does nothing
        hub.remove("pair-x", Endpoint::One, "phone", t1);
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t2));

        // Removing with current token removes the connection and drops empty endpoint map
        hub.remove("pair-x", Endpoint::One, "phone", t2);
        assert!(!hub.is_current("pair-x", Endpoint::One, "phone", t2));
        assert!(hub.connections.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ephemeral_skips_receivers_over_queue_budget() {
        let hub = Hub::default();
        let (tx, mut rx) = connection_channel();
        hub.register("pair-e", Endpoint::Two, "phone", tx, Vec::new());
        assert!(matches!(frame(&mut rx).await, Some(ServerFrame::Ready { .. })));

        let send = |target, size| {
            let message = Message::Binary(vec![0; 4].into());
            hub.send_ephemeral("pair-e", Endpoint::Two, target, message, size)
        };
        // An empty queue accepts even a message as large as the whole budget.
        assert_eq!(send(None, MAX_QUEUED_EPHEMERAL_BYTES), 1);
        assert_eq!(send(None, 1), 0);

        // Handing the message to the writer releases the budget.
        assert!(matches!(rx.recv().await, Some(Message::Binary(_))));
        assert_eq!(send(None, 1), 1);
        assert_eq!(send(Some("tablet"), 1), 0);
    }
}
