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

The default bind address is `127.0.0.1:39371` and the default database is
`relay.sqlite3` in the current directory. Every option also has an environment
variable:

| Option | Environment variable | Default |
| --- | --- | --- |
| `--debug` | `RELAY_DEBUG` | enabled automatically in debug builds |
| `--bind` | `RELAY_BIND` | `127.0.0.1:39371` |
| `--database` | `RELAY_DATABASE` | `relay.sqlite3` |
| `--max-pending-messages` | `RELAY_MAX_PENDING_MESSAGES` | `10000` per id and direction |
| `--max-pending-bytes` | `RELAY_MAX_PENDING_BYTES` | `1073741824` per id and direction |
| `--pending-retention-secs` | `RELAY_PENDING_RETENTION_SECS` | `604800` (7 days) |
| `--receipt-retention-secs` | `RELAY_RECEIPT_RETENTION_SECS` | `2592000` (30 days) |
| `--cleanup-interval-secs` | `RELAY_CLEANUP_INTERVAL_SECS` | `3600` (1 hour) |

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

Each channel has two numbered endpoints. Connect clients as endpoint 1 or endpoint 2,
specifying the URL-encoded pairing id and a unique `device_id`:

```text
wss://relay.example/ws?id=<pair-id>&endpoint=1&device_id=desktop
wss://relay.example/ws?id=<pair-id>&endpoint=2&device_id=phone_a
wss://relay.example/ws?id=<pair-id>&endpoint=2&device_id=phone_b
```

Messages are isolated by pairing id and forwarded to the opposite endpoint with
the same id:

- **Multi-Device Support**: Multiple devices with distinct `device_id`s can connect
  to the same endpoint simultaneously without conflicts. Inbound messages destined
  for that endpoint are broadcast in real-time to all currently active devices. Each
  device maintains its own durable acknowledgment cursor; pending messages remain
  available for offline devices and are purged only after all registered devices of
  that endpoint have acknowledged them (or when the message retention period expires).
- **Preemption (Takeover)**: If a connection opens with an already-active
  `(id, endpoint, device_id)`, the relay preempts (replaces) the older socket with
  a `connection_replaced` error frame, allowing mobile clients switching networks
  (e.g. WiFi to 5G) to resume immediately without waiting for half-open idle timeouts.

The pairing id is the channel's access credential. Generate it with sufficient
entropy, keep it secret, and avoid recording WebSocket query strings in proxy
access logs.

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
{"type":"ack","sequence":42}
```

Server to client:

```json
{"type":"ready","endpoint":"1"}
{"type":"stored","message_id":"stable-client-generated-id"}
{"type":"message","message_id":"peer-message-id","sequence":42,"payload":{"any":"json"}}
{"type":"error","message":"description"}
```

`payload` may be any JSON value. `message_id` must be non-empty, stable across
retries, and no longer than 256 bytes. WebSocket messages are limited to 10 MiB.

## Reliable delivery

Delivery is at-least-once across endpoint disconnects and relay restarts:

1. A sender writes a message to its own durable outbox, then sends it with a
   stable `message_id`.
2. The sender retains the message and retries it until the relay returns
   `stored`.
3. `stored` means both the pending delivery and its idempotency receipt were
   committed to SQLite.
4. The receiver processes messages in `sequence` order, durably records its
   cumulative receive cursor, and only then sends `ack`.
5. The relay deletes acknowledged pending deliveries. Unacknowledged messages
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
after the longer receipt-retention period.

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
