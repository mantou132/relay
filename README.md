# Durable WebSocket Relay

A protocol-agnostic WebSocket relay that pairs two endpoints and durably
forwards opaque JSON payloads between them. The server does not inspect or
depend on the application protocol carried in `payload`.

## Run

```sh
cargo run --release -- \
  --bind 0.0.0.0:39371 \
  --database /var/lib/relay/relay.sqlite3
```

The default bind address is `0.0.0.0:39371` and the default database is
`relay.sqlite3` in the current directory. Every option also has an environment
variable:

| Option | Environment variable | Default |
| --- | --- | --- |
| `--debug` | `RELAY_DEBUG` | enabled automatically in debug builds |
| `--bind` | `RELAY_BIND` | `0.0.0.0:39371` |
| `--database` | `RELAY_DATABASE` | `relay.sqlite3` |
| `--max-pending-messages` | `RELAY_MAX_PENDING_MESSAGES` | `100000` per id and direction |
| `--max-pending-bytes` | `RELAY_MAX_PENDING_BYTES` | `1073741824` per id and direction |
| `--pending-retention-secs` | `RELAY_PENDING_RETENTION_SECS` | `3600` (1 hour) |
| `--receipt-retention-secs` | `RELAY_RECEIPT_RETENTION_SECS` | `3600` (1 hour) |
| `--device-retention-secs` | `RELAY_DEVICE_RETENTION_SECS` | `604800` (7 days) |
| `--cleanup-interval-secs` | `RELAY_CLEANUP_INTERVAL_SECS` | `600` (10 minutes) |

Debug logging records connection and disconnection details, pairing ids,
endpoints, message ids, JSON payloads, acknowledgements, and replay/forwarding
outcomes. Because pairing ids are credentials and payloads may be sensitive, do
not enable it in production unless the resulting logs are protected. Release
builds can enable it with `--debug` or `RELAY_DEBUG=true`; `RUST_LOG` can be used
to override the log filter.

For an internet-facing deployment, terminate TLS in a reverse proxy and expose
the WebSocket endpoint over `wss://`.

## Docker

The release workflow builds the `linux/amd64` image
`594mantou/relay` and publishes it to Docker Hub when a GitHub Release is
published. It can also be started manually from the Actions tab. Configure the
repository secrets `DOCKER_USERNAME` and `DOCKER_PASSWORD` before running it.

```sh
docker run --rm \
  -p 39371:39371 \
  -v relay-data:/data \
  594mantou/relay:main
```

## Connect

Each channel has two numbered endpoints (1 and 2). Connect clients as endpoint 1 or endpoint 2,
specifying the URL-encoded pairing id and a unique `device_id`:

```text
wss://relay.example/ws?id=<pair-id>&endpoint=1&device_id=desktop
wss://relay.example/ws?id=<pair-id>&endpoint=2&device_id=phone_a
wss://relay.example/ws?id=<pair-id>&endpoint=2&device_id=phone_b
```

Query parameters for `/ws`:
- `id`: Channel/user pairing identifier (non-empty string, max 256 bytes).
- `endpoint`: `1` or `2`. Messages sent from 1 are forwarded to 2, and vice versa.
- `device_id`: Unique stable identifier for this device (non-empty string, max 256 bytes).
- `ack_head`: Optional boolean (`true` / `1`). When set on connect, resets this endpoint's receive cursor to the latest head sequence, immediately acknowledging and purging unread server backlog so the device only receives subsequent live messages.

Messages are isolated by pairing id and forwarded to the opposite endpoint with
the same id:

- **Multi-Device Support & Targeted Routing**: Multiple devices with distinct `device_id`s can connect
  to the same endpoint simultaneously without conflicts. Inbound messages destined
  for that endpoint are broadcast in real-time to all currently active devices by default.
  Alternatively, messages can specify an optional `target_device_id` to route exclusively to
  a single designated device. Each device maintains its own durable acknowledgment cursor;
  broadcast messages are purged only after all registered devices have acknowledged them,
  while targeted messages are purged as soon as their designated target device acknowledges them.
- **Device Lifecycle & Inactivity Cutoff**: Inactive devices that have not connected
  within `--device-retention-secs` (default: 7 days) are automatically excluded from
  acknowledgment cursor calculations, preventing abandoned devices from permanently
  blocking pending message pruning.
- **Preemption (Takeover)**: If a connection opens with an already-active
  `(id, endpoint, device_id)`, the relay preempts (replaces) the older socket with
  a `connection_replaced` error frame, allowing mobile clients switching networks
  (e.g. WiFi to 5G) to resume immediately without waiting for half-open idle timeouts.

The pairing id is the channel's access credential. Generate it with sufficient
entropy, keep it secret, and avoid recording WebSocket query strings in proxy
access logs.

## Health Check

For container orchestrators (Kubernetes, Docker Swarm) and reverse proxies:

```text
GET /health
```
Returns `200 OK` when the service is healthy and ready to accept connections.

## Single instance

One channel's database must be served by exactly one relay process. Connection
state (which `(id, endpoint)` slots are occupied) lives in process memory, so
two relay processes sharing one SQLite file would each accept a connection for
the same `(id, endpoint)`, break duplicate detection, and deliver messages to
whichever process the client happened to reach. Run one process per SQLite
file and scale by sharding pairing ids across files, behind a load balancer
that routes all WebSockets for a pairing id to the same instance (for example
by consistent hashing on the `id` query parameter). After a failover, wait for
the old process to exit before starting the replacement on the same file.

## Wire protocol

Client to server:

```json
{"type":"message","message_id":"stable-client-generated-id","payload":{"any":"json"}}
{"type":"message","message_id":"stable-client-generated-id","payload":{"any":"json"},"target_device_id":"phone_a"}
{"type":"ack","sequence":42}
```

Server to client:

```json
{"type":"ready","endpoint":"1"}
{"type":"stored","message_id":"stable-client-generated-id"}
{"type":"rejected","message_id":"stable-client-generated-id","reason":"description"}
{"type":"message","message_id":"peer-message-id","sequence":42,"payload":{"any":"json"}}
{"type":"error","message":"description"}
```

`payload` may be any JSON value. `message_id` must be non-empty, stable across
retries, and no longer than 256 bytes. `target_device_id` is an optional string
specifying a target device on the opposite endpoint; if omitted, the message
is broadcast to all active devices on that endpoint. WebSocket messages are limited to 10 MiB.

## Reliable delivery

Delivery is at-least-once across endpoint disconnects and relay restarts:

1. A sender writes a message to its own durable outbox, then sends it with a
   stable `message_id`.
2. The sender retains the message and retries it until the relay returns
   `stored`.
3. `stored` means both the pending delivery and its idempotency receipt (storing
   a compact SHA-256 payload hash to detect conflicting payloads) were committed to SQLite.
4. The receiver processes messages in `sequence` order, durably records its
   cumulative receive cursor, and only then sends `ack`.
5. The relay deletes acknowledged pending deliveries. Broadcast messages are purged
   once acknowledged by all active devices on that endpoint (or when expired). Targeted
   messages are purged as soon as the designated target device acknowledges them.
   Unacknowledged messages matching the connecting device (broadcast or targeted to it)
   are replayed in sequence order after reconnect or process restart.
6. If a sender retries a previously stored `message_id`, the relay returns
   `stored` without assigning a second sequence. Receivers must still tolerate
   repeated delivery of the same sequence when an acknowledgement was lost.

If either pending limit is reached, the relay rejects new messages with a
`queue_full` error; it does not evict messages that are still within their
retention period. A duplicate retry of an already stored `message_id` remains
accepted even while the queue is full.

Pending messages older than the configured retention period are removed along
with their receipts. A later retry of the same `message_id` is therefore treated
as a new delivery with a new sequence. Acknowledged receipts expire separately
after the receipt-retention period (default: 1 hour).

The relay detects dead connections itself: it pings every 30 seconds and closes
a connection that has sent nothing (no frames, pongs, or pings) for 90 seconds.
This releases the `(id, endpoint)` slot of a half-open peer — one whose network
path died without a TCP close, for example after a network switch or sleep — so
the real client can reconnect. Clients should also send their own protocol-level
pings if they need to detect a dead relay faster than their TCP stack does.

The relay deliberately provides at-least-once transport, not exactly-once
application execution. An application should make side effects idempotent when
processing a delivered payload.

Cleanup runs once at startup and periodically afterwards. It removes expired
rows; SQLite runs in WAL mode with full auto-vacuum so freed pages can be
returned to the filesystem.

## Client SDKs

The repository includes official clients for Rust and TypeScript/JavaScript implementing the full delivery contract (persistent outbox, automatic retries until `stored`, cumulative acknowledgments, exponential backoff, and preemption handling).

### Rust Client (`client-rs`)

Add to `Cargo.toml`:
```toml
[dependencies]
relay-client = { path = "client-rs" } # or git reference
```

Example usage:
```rust
use std::sync::Arc;
use relay_client::{Client, ClientHandler, memory::MemoryStore, transport::endpoint_url, Endpoint};
use serde_json::json;

struct MyHandler;

impl ClientHandler for MyHandler {
    fn on_payload(&self, payload: serde_json::Value) {
        println!("Received payload: {payload}");
    }
    fn on_connected(&self) {
        println!("Connected to relay");
    }
    fn on_disconnected(&self, error: Option<String>) {
        println!("Disconnected: {error:?}");
    }
    fn on_preempted(&self) {
        println!("Session preempted by a new connection from the same device");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = endpoint_url("ws://127.0.0.1:39371/ws", "user_123", Endpoint::One, "desktop");
    let store = Arc::new(MemoryStore::new()); // Use a persistent OutboxStore in production
    let handler = Arc::new(MyHandler);

    let client = Client::new(url, store, handler);

    // Spawn client connection loop
    let client_task = client.clone();
    tokio::spawn(async move {
        if let Err(err) = client_task.run().await {
            eprintln!("Client exited: {err}");
        }
    });

    // Broadcast a message (queued to outbox and sent to all devices on opposite endpoint)
    let message_id = client.send(json!({ "hello": "world" })).await?;

    // Or send a targeted message to a specific device on opposite endpoint
    let targeted_id = client
        .send_targeted(json!({ "hello": "phone" }), Some("phone_a".to_string()))
        .await?;

    // Close client when done (disconnects and exits run loop)
    // client.close();

    Ok(())
}
```

### TypeScript Client (`client-ts`)

Import from `client-ts/src/relay-client.ts` or install `relay-client-ts`:

```typescript
import { RelayClient } from './client-ts/src/relay-client';

const client = new RelayClient({
  relayUrl: 'wss://relay.example/ws',
  relayId: 'user_12345',       // User or pairing identifier (1-256 chars)
  endpoint: '2',               // Endpoint 1 or 2
  deviceId: 'phone_a',         // Unique device identifier
  ackHead: false,              // Set true on initial connect to drop stale backlog
  onPayload: (payload) => {
    console.log('Received payload from peer:', payload);
  },
  onStateChange: (state, error) => {
    console.log('Connection state:', state, error);
  },
  onDisconnect: (error) => {
    console.warn('Disconnected:', error.message);
  },
});

// Connect to the relay
await client.connect();

// Broadcast payload to all devices on opposite endpoint
await client.send({ text: 'Hello from phone' });

// Or send payload to a specific target device
await client.send({ text: 'Hello specifically to desktop' }, 'desktop');

// Close connection (stops reconnect loop)
// client.close();
```
