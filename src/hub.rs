use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::extract::ws::Message;
use relay::{Endpoint, ServerFrame};
use tokio::sync::Notify;

use crate::database::StoredMessage;

/// Bytes of ephemeral messages one connection may have queued but not yet
/// written. Beyond it, further ephemeral messages skip that connection rather
/// than letting a slow receiver grow relay memory. A single message larger
/// than the budget still passes when the queue is empty.
const MAX_QUEUED_EPHEMERAL_BYTES: usize = 16 * 1024 * 1024;

enum Outgoing {
    Frame(ServerFrame),
    /// Pre-encoded so a broadcast shares one buffer across devices.
    Ephemeral {
        message: Message,
        size: usize,
        replace_key: Option<String>,
    },
}

#[derive(Default)]
struct QueueState {
    entries: VecDeque<Outgoing>,
    queued_ephemeral: usize,
    sender_closed: bool,
    receiver_closed: bool,
}

impl QueueState {
    fn push_frame(&mut self, frame: ServerFrame) -> bool {
        if self.receiver_closed {
            return false;
        }
        self.entries.push_back(Outgoing::Frame(frame));
        true
    }

    fn push_ephemeral(
        &mut self,
        message: Message,
        size: usize,
        replace_key: Option<&str>,
    ) -> bool {
        if self.receiver_closed {
            return false;
        }
        if let Some(key) = replace_key {
            for entry in &mut self.entries {
                if let Outgoing::Ephemeral {
                    message: slot_message,
                    size: slot_size,
                    replace_key: Some(slot_key),
                } = entry
                    && slot_key == key
                {
                    let remaining = self.queued_ephemeral - *slot_size;
                    if remaining != 0 && remaining + size > MAX_QUEUED_EPHEMERAL_BYTES {
                        return false;
                    }
                    self.queued_ephemeral = remaining + size;
                    *slot_message = message;
                    *slot_size = size;
                    return true;
                }
            }
        }

        if self.queued_ephemeral != 0 && self.queued_ephemeral + size > MAX_QUEUED_EPHEMERAL_BYTES {
            return false;
        }
        self.entries.push_back(Outgoing::Ephemeral {
            message,
            size,
            replace_key: replace_key.map(str::to_owned),
        });
        self.queued_ephemeral += size;
        true
    }

    fn pop_front(&mut self) -> Option<Outgoing> {
        let item = self.entries.pop_front()?;
        if let Outgoing::Ephemeral { size, .. } = item {
            // Released once handed to the socket writer, which is
            // where a slow receiver's backlog actually accumulates.
            self.queued_ephemeral -= size;
        }
        Some(item)
    }
}

#[derive(Default)]
struct ConnectionQueue {
    state: Mutex<QueueState>,
    notify: Notify,
}

struct SenderGuard(Arc<ConnectionQueue>);

impl Drop for SenderGuard {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("queue lock poisoned")
            .sender_closed = true;
        self.0.notify.notify_one();
    }
}

/// Sending half of one connection's outgoing queue.
#[derive(Clone)]
pub(crate) struct ConnectionSender {
    inner: Arc<SenderGuard>,
}

impl ConnectionSender {
    pub(crate) fn send(&self, frame: ServerFrame) {
        let queued = self
            .inner
            .0
            .state
            .lock()
            .expect("queue lock poisoned")
            .push_frame(frame);
        if queued {
            self.inner.0.notify.notify_one();
        }
    }

    fn send_ephemeral(&self, message: Message, size: usize, replace_key: Option<&str>) -> bool {
        let accepted = self
            .inner
            .0
            .state
            .lock()
            .expect("queue lock poisoned")
            .push_ephemeral(message, size, replace_key);
        if accepted {
            self.inner.0.notify.notify_one();
        }
        accepted
    }
}

/// Receiving half, owned by the connection's writer task.
pub(crate) struct ConnectionReceiver {
    queue: Arc<ConnectionQueue>,
}

impl ConnectionReceiver {
    pub(crate) async fn recv(&mut self) -> Option<Message> {
        loop {
            let notified = self.queue.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let next = {
                let mut state = self.queue.state.lock().expect("queue lock poisoned");
                if let Some(item) = state.pop_front() {
                    Some(item)
                } else if state.sender_closed {
                    return None;
                } else {
                    None
                }
            };

            if let Some(item) = next {
                if let Some(message) = outgoing_to_message(item) {
                    return Some(message);
                }
                continue;
            }

            notified.await;
        }
    }

    #[cfg(test)]
    fn try_recv(&mut self) -> Option<Message> {
        loop {
            let item = self
                .queue
                .state
                .lock()
                .expect("queue lock poisoned")
                .pop_front()?;
            if let Some(message) = outgoing_to_message(item) {
                return Some(message);
            }
        }
    }
}

impl Drop for ConnectionReceiver {
    fn drop(&mut self) {
        let mut state = self.queue.state.lock().expect("queue lock poisoned");
        state.receiver_closed = true;
        state.entries.clear();
        state.queued_ephemeral = 0;
    }
}

fn outgoing_to_message(item: Outgoing) -> Option<Message> {
    match item {
        Outgoing::Frame(frame) => serde_json::to_string(&frame)
            .ok()
            .map(|json| Message::Text(json.into())),
        Outgoing::Ephemeral { message, .. } => Some(message),
    }
}

pub(crate) fn connection_channel() -> (ConnectionSender, ConnectionReceiver) {
    let queue = Arc::new(ConnectionQueue::default());
    (
        ConnectionSender {
            inner: Arc::new(SenderGuard(queue.clone())),
        },
        ConnectionReceiver { queue },
    )
}

#[derive(Clone)]
struct LiveConnection {
    token: u64,
    tx: ConnectionSender,
    /// Whether the connection receives `peer_connected` / `peer_disconnected`.
    presence: bool,
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
    ///
    /// Presence frames are queued while the hub lock is held, so every
    /// opted-in connection observes peers joining and leaving in one order
    /// consistent with the snapshot it received in `ready`.
    pub(crate) fn register(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        device_id: &str,
        tx: ConnectionSender,
        presence: bool,
        pending: Vec<StoredMessage>,
    ) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
        let mut connections = self.connections.lock().expect("hub lock poisoned");
        let endpoint_connections = connections
            .entry((relay_id.to_string(), endpoint))
            .or_default();
        let previous = endpoint_connections.insert(
            device_id.to_string(),
            LiveConnection {
                token,
                tx: tx.clone(),
                presence,
            },
        );
        let replaced = previous.is_some();
        if let Some(previous) = previous {
            previous.tx.send(ServerFrame::Error {
                message: "connection_replaced: another connection opened for this device"
                    .to_string(),
            });
        }
        // Peers also hear about a replacement: the device stays online, but
        // a new connection usually means a new session, e.g. a restarted host
        // whose stale socket had not timed out yet.
        let peers = connections.get(&(relay_id.to_string(), endpoint.opposite()));
        notify_peers(
            peers,
            |device_id| ServerFrame::PeerConnected {
                device_id,
                replaced,
            },
            device_id,
        );

        let peers = presence.then(|| {
            let mut peers: Vec<String> =
                peers.into_iter().flat_map(HashMap::keys).cloned().collect();
            peers.sort();
            peers
        });
        tx.send(ServerFrame::Ready { endpoint, peers });
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
        replace_key: Option<&str>,
        message: Message,
        size: usize,
    ) -> usize {
        self.matching(relay_id, endpoint, target_device_id)
            .into_iter()
            .filter(|tx| tx.send_ephemeral(message.clone(), size, replace_key))
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

    /// Number of devices of the endpoint that are connected.
    pub(crate) fn online_devices(&self, relay_id: &str, endpoint: Endpoint) -> usize {
        self.connections
            .lock()
            .expect("hub lock poisoned")
            .get(&(relay_id.to_string(), endpoint))
            .map_or(0, HashMap::len)
    }

    /// Removes the connection if it is still the current one for its device,
    /// returning whether it was. A preempted connection returns `false`.
    pub(crate) fn remove(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        device_id: &str,
        token: u64,
    ) -> bool {
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
                notify_peers(
                    connections.get(&(relay_id.to_string(), endpoint.opposite())),
                    |device_id| ServerFrame::PeerDisconnected { device_id },
                    device_id,
                );
                return true;
            }
        }
        false
    }
}

/// Queues a presence frame about `device_id` to every opted-in peer device.
fn notify_peers(
    peers: Option<&HashMap<String, LiveConnection>>,
    frame: impl Fn(String) -> ServerFrame,
    device_id: &str,
) {
    for peer in peers.into_iter().flat_map(HashMap::values) {
        if peer.presence {
            peer.tx.send(frame(device_id.to_string()));
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
        let _t1 = hub.register("pair-1", Endpoint::One, "dev1", tx1, false, Vec::new());
        // Register dev2 on (pair-1, Endpoint::Two)
        let _t2 = hub.register("pair-1", Endpoint::Two, "dev2", tx2, false, Vec::new());
        // Register dev3 on (pair-2, Endpoint::Two)
        let _t3 = hub.register("pair-2", Endpoint::Two, "dev3", tx3, false, Vec::new());

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
        assert!(rx1.try_recv().is_none());
        assert!(rx3.try_recv().is_none());

        // Register dev4 also on (pair-1, Endpoint::Two)
        let (tx4, mut rx4) = connection_channel();
        let _t4 = hub.register("pair-1", Endpoint::Two, "dev4", tx4, false, Vec::new());
        assert!(matches!(frame(&mut rx4).await, Some(ServerFrame::Ready { .. })));

        // Send targeted to dev4 only
        let targeted_frame = ServerFrame::Stored {
            message_id: "test-targeted".to_string(),
        };
        hub.send_to_endpoint("pair-1", Endpoint::Two, Some("dev4"), targeted_frame.clone());

        // dev4 receives it
        assert_eq!(frame(&mut rx4).await, Some(targeted_frame));
        // dev2 does NOT receive it!
        assert!(rx2.try_recv().is_none());
    }

    #[tokio::test]
    async fn preemption_and_clean_removal() {
        let hub = Hub::default();
        let (tx1, mut rx1) = connection_channel();
        let (tx2, _rx2) = connection_channel();
        let t1 = hub.register("pair-x", Endpoint::One, "phone", tx1, false, Vec::new());
        assert!(hub.is_current("pair-x", Endpoint::One, "phone", t1));

        // Preempt with new connection on same device
        let t2 = hub.register("pair-x", Endpoint::One, "phone", tx2, false, Vec::new());
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
        hub.register("pair-e", Endpoint::Two, "phone", tx, false, Vec::new());
        assert!(matches!(frame(&mut rx).await, Some(ServerFrame::Ready { .. })));

        let send = |target, size| {
            let message = Message::Binary(vec![0; 4].into());
            hub.send_ephemeral("pair-e", Endpoint::Two, target, None, message, size)
        };
        // An empty queue accepts even a message as large as the whole budget.
        assert_eq!(send(None, MAX_QUEUED_EPHEMERAL_BYTES), 1);
        assert_eq!(send(None, 1), 0);

        // Handing the message to the writer releases the budget.
        assert!(matches!(rx.recv().await, Some(Message::Binary(_))));
        assert_eq!(send(None, 1), 1);
        assert_eq!(send(Some("tablet"), 1), 0);
    }

    #[tokio::test]
    async fn ephemeral_coalesces_same_replace_key_and_preserves_other_messages() {
        let hub = Hub::default();
        let (tx, mut rx) = connection_channel();
        hub.register("pair-c", Endpoint::Two, "phone", tx, false, Vec::new());
        assert!(matches!(frame(&mut rx).await, Some(ServerFrame::Ready { .. })));

        let send_eph = |key: Option<&str>, bytes: &'static [u8]| {
            assert_eq!(
                hub.send_ephemeral(
                    "pair-c",
                    Endpoint::Two,
                    None,
                    key,
                    Message::Binary(bytes.into()),
                    bytes.len(),
                ),
                1
            );
        };
        let send_durable = |id: &str, seq: u64| {
            hub.send_to_endpoint(
                "pair-c",
                Endpoint::Two,
                None,
                ServerFrame::Message {
                    message_id: id.into(),
                    sequence: seq,
                    payload: serde_json::json!(seq),
                },
            );
        };

        send_durable("d1", 1);
        send_eph(Some("screen"), b"s1");
        send_eph(Some("cursor"), b"c1");
        send_eph(None, b"unkeyed-1");
        send_eph(Some("screen"), b"s2");
        send_durable("d2", 2);
        send_eph(Some("screen"), b"s3");
        send_eph(None, b"unkeyed-2");

        // In-place replacement updates the queued "screen" slot to s3 without
        // reordering durable messages, distinct keys, or non-keyed frames.
        assert!(matches!(
            frame(&mut rx).await,
            Some(ServerFrame::Message { sequence: 1, .. })
        ));
        assert_eq!(rx.recv().await, Some(Message::Binary(b"s3".as_slice().into())));
        assert_eq!(rx.recv().await, Some(Message::Binary(b"c1".as_slice().into())));
        assert_eq!(
            rx.recv().await,
            Some(Message::Binary(b"unkeyed-1".as_slice().into()))
        );
        assert!(matches!(
            frame(&mut rx).await,
            Some(ServerFrame::Message { sequence: 2, .. })
        ));
        assert_eq!(
            rx.recv().await,
            Some(Message::Binary(b"unkeyed-2".as_slice().into()))
        );
        assert!(rx.try_recv().is_none());
    }

    #[tokio::test]
    async fn ephemeral_replace_key_accounts_for_replaced_bytes_in_budget() {
        let hub = Hub::default();
        let (tx, mut rx) = connection_channel();
        hub.register("pair-b", Endpoint::Two, "phone", tx, false, Vec::new());
        assert!(matches!(frame(&mut rx).await, Some(ServerFrame::Ready { .. })));

        // Fill the entire budget with one keyed frame.
        assert_eq!(
            hub.send_ephemeral(
                "pair-b",
                Endpoint::Two,
                None,
                Some("screen"),
                Message::Binary(b"old".as_slice().into()),
                MAX_QUEUED_EPHEMERAL_BYTES,
            ),
            1
        );
        // A different key cannot fit while the budget is full.
        assert_eq!(
            hub.send_ephemeral(
                "pair-b",
                Endpoint::Two,
                None,
                Some("other"),
                Message::Binary(b"x".as_slice().into()),
                1,
            ),
            0
        );
        // Replacing the existing key reuses its reserved bytes even at full budget.
        assert_eq!(
            hub.send_ephemeral(
                "pair-b",
                Endpoint::Two,
                None,
                Some("screen"),
                Message::Binary(b"new-large".as_slice().into()),
                MAX_QUEUED_EPHEMERAL_BYTES,
            ),
            1
        );
        // Replacing with a smaller frame frees budget for other keys.
        assert_eq!(
            hub.send_ephemeral(
                "pair-b",
                Endpoint::Two,
                None,
                Some("screen"),
                Message::Binary(b"small".as_slice().into()),
                100,
            ),
            1
        );
        assert_eq!(
            hub.send_ephemeral(
                "pair-b",
                Endpoint::Two,
                None,
                Some("other"),
                Message::Binary(b"fits".as_slice().into()),
                100,
            ),
            1
        );

        assert_eq!(
            rx.recv().await,
            Some(Message::Binary(b"small".as_slice().into()))
        );
        assert_eq!(
            rx.recv().await,
            Some(Message::Binary(b"fits".as_slice().into()))
        );
        assert!(rx.try_recv().is_none());
    }
}
