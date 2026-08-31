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

## Connect

Each channel has two numbered endpoints. Connect one client as endpoint 1 and
the other as endpoint 2, using the same URL-encoded pairing id:

```text
wss://relay.example/ws?id=<pair-id>&endpoint=1
wss://relay.example/ws?id=<pair-id>&endpoint=2
```

Messages are isolated by pairing id and only forwarded to the opposite endpoint
with the same id. The first connection for `(id, endpoint)` remains active; another
connection for the same key receives a `connection_conflict` error and closes,
without disconnecting the existing client. Either endpoint may connect first;
messages wait in SQLite until its peer connects. The SQLite storage layer uses
SeaORM entities, transactions, and schema builders rather than handwritten SQL.

Clients should treat `connection_conflict` as terminal instead of immediately
reconnecting. They may connect again after an operator resolves the duplicate
client or after the active connection ends.

The pairing id is the channel's access credential. Generate it with sufficient
entropy, keep it secret, and avoid recording WebSocket query strings in proxy
access logs.

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

The relay deliberately provides at-least-once transport, not exactly-once
application execution. An application should make side effects idempotent when
processing a delivered payload.

Cleanup runs once at startup and periodically afterwards. It removes expired
rows; SQLite runs in WAL mode with full auto-vacuum so freed pages can be
returned to the filesystem.
