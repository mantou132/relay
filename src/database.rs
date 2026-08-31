use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use relay::{Endpoint, ServerFrame};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::config::Limits;

const MAX_ID_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredMessage {
    message_id: String,
    sequence: u64,
    payload: Value,
}

impl StoredMessage {
    pub(crate) fn into_frame(self) -> ServerFrame {
        ServerFrame::Message {
            message_id: self.message_id,
            sequence: self.sequence,
            payload: self.payload,
        }
    }
}

#[derive(Debug)]
pub(crate) struct StoreResult {
    pub(crate) pending: Option<StoredMessage>,
}

pub(crate) struct Database {
    connection: Mutex<Connection>,
    limits: Limits,
}

impl Database {
    pub(crate) fn open(path: &Path, limits: Limits) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create relay database directory {}",
                    parent.display()
                )
            })?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("failed to open relay database {}", path.display()))?;
        connection.execute_batch(
            "PRAGMA auto_vacuum = INCREMENTAL;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS counters (
               relay_id TEXT NOT NULL,
               destination TEXT NOT NULL,
               next_sequence INTEGER NOT NULL,
               PRIMARY KEY (relay_id, destination)
             );
             CREATE TABLE IF NOT EXISTS receipts (
               relay_id TEXT NOT NULL,
               source TEXT NOT NULL,
               message_id TEXT NOT NULL,
               destination TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               payload TEXT NOT NULL,
               created_at INTEGER NOT NULL,
               PRIMARY KEY (relay_id, source, message_id)
             );
             CREATE TABLE IF NOT EXISTS pending_messages (
               relay_id TEXT NOT NULL,
               destination TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               message_id TEXT NOT NULL,
               payload TEXT NOT NULL,
               payload_bytes INTEGER NOT NULL,
               created_at INTEGER NOT NULL,
               PRIMARY KEY (relay_id, destination, sequence)
             );",
        )?;
        ensure_column(
            &connection,
            "receipts",
            "created_at",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &connection,
            "pending_messages",
            "payload_bytes",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &connection,
            "pending_messages",
            "created_at",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        // Preserve queued deliveries created by the earlier role-based wire
        // format when upgrading to numbered endpoints.
        connection.execute_batch(
            "UPDATE counters
             SET destination = CASE destination
               WHEN 'host' THEN '1'
               WHEN 'app' THEN '2'
               ELSE destination
             END
             WHERE destination IN ('host', 'app');
             UPDATE receipts
             SET source = CASE source
                   WHEN 'host' THEN '1'
                   WHEN 'app' THEN '2'
                   ELSE source
                 END,
                 destination = CASE destination
                   WHEN 'host' THEN '1'
                   WHEN 'app' THEN '2'
                   ELSE destination
                 END
             WHERE source IN ('host', 'app') OR destination IN ('host', 'app');
             UPDATE pending_messages
             SET destination = CASE destination
               WHEN 'host' THEN '1'
               WHEN 'app' THEN '2'
               ELSE destination
             END
             WHERE destination IN ('host', 'app');",
        )?;
        let now = unix_timestamp()?;
        connection.execute(
            "UPDATE receipts SET created_at = ?1 WHERE created_at = 0",
            params![now],
        )?;
        connection.execute(
            "UPDATE pending_messages
             SET created_at = CASE WHEN created_at = 0 THEN ?1 ELSE created_at END,
                 payload_bytes = length(CAST(payload AS BLOB))
             WHERE created_at = 0 OR payload_bytes = 0",
            params![now],
        )?;
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS pending_messages_created_at
             ON pending_messages(created_at);
             CREATE INDEX IF NOT EXISTS receipts_created_at
             ON receipts(created_at);",
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
            limits,
        })
    }

    pub(crate) fn store(
        &self,
        relay_id: &str,
        source: Endpoint,
        message_id: &str,
        payload: &Value,
    ) -> Result<StoreResult> {
        validate_message_id(message_id)?;
        let destination = source.opposite();
        let payload_json = serde_json::to_string(payload)?;
        let payload_bytes = i64::try_from(payload_json.len()).context("payload is too large")?;
        let now = unix_timestamp()?;
        let mut connection = self.connection.lock().expect("database lock poisoned");
        let transaction = connection.transaction()?;

        let receipt = transaction
            .query_row(
                "SELECT destination, sequence, payload FROM receipts
                 WHERE relay_id = ?1 AND source = ?2 AND message_id = ?3",
                params![relay_id, source.to_string(), message_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;

        let sequence = if let Some((stored_destination, sequence, stored_payload)) = receipt {
            anyhow::ensure!(
                stored_destination == destination.to_string() && stored_payload == payload_json,
                "message_id was already used with a different payload"
            );
            sequence
        } else {
            let (pending_count, pending_bytes) = transaction.query_row(
                "SELECT count(*), coalesce(sum(payload_bytes), 0)
                 FROM pending_messages
                 WHERE relay_id = ?1 AND destination = ?2",
                params![relay_id, destination.to_string()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            anyhow::ensure!(
                pending_count < self.limits.max_pending_messages,
                "queue_full: pending message count limit reached"
            );
            anyhow::ensure!(
                payload_bytes <= self.limits.max_pending_bytes
                    && pending_bytes <= self.limits.max_pending_bytes.saturating_sub(payload_bytes),
                "queue_full: pending payload byte limit reached"
            );
            let current = transaction
                .query_row(
                    "SELECT next_sequence FROM counters
                     WHERE relay_id = ?1 AND destination = ?2",
                    params![relay_id, destination.to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .unwrap_or(0);
            let sequence = current
                .checked_add(1)
                .context("relay sequence space exhausted")?;
            transaction.execute(
                "INSERT INTO counters (relay_id, destination, next_sequence)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(relay_id, destination)
                 DO UPDATE SET next_sequence = excluded.next_sequence",
                params![relay_id, destination.to_string(), sequence],
            )?;
            transaction.execute(
                "INSERT INTO receipts
                 (relay_id, source, message_id, destination, sequence, payload, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    relay_id,
                    source.to_string(),
                    message_id,
                    destination.to_string(),
                    sequence,
                    payload_json,
                    now,
                ],
            )?;
            transaction.execute(
                "INSERT INTO pending_messages
                 (relay_id, destination, sequence, message_id, payload, payload_bytes, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    relay_id,
                    destination.to_string(),
                    sequence,
                    message_id,
                    payload_json,
                    payload_bytes,
                    now,
                ],
            )?;
            sequence
        };

        let pending = transaction
            .query_row(
                "SELECT message_id, sequence, payload FROM pending_messages
                 WHERE relay_id = ?1 AND destination = ?2 AND sequence = ?3",
                params![relay_id, destination.to_string(), sequence],
                row_to_message,
            )
            .optional()?;
        transaction.commit()?;
        Ok(StoreResult { pending })
    }

    pub(crate) fn pending(
        &self,
        relay_id: &str,
        destination: Endpoint,
    ) -> Result<Vec<StoredMessage>> {
        let connection = self.connection.lock().expect("database lock poisoned");
        let mut statement = connection.prepare(
            "SELECT message_id, sequence, payload FROM pending_messages
             WHERE relay_id = ?1 AND destination = ?2 ORDER BY sequence",
        )?;
        let messages = statement
            .query_map(params![relay_id, destination.to_string()], row_to_message)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(messages)
    }

    pub(crate) fn acknowledge(
        &self,
        relay_id: &str,
        destination: Endpoint,
        sequence: u64,
    ) -> Result<()> {
        let sequence = i64::try_from(sequence).unwrap_or(i64::MAX);
        self.connection
            .lock()
            .expect("database lock poisoned")
            .execute(
                "DELETE FROM pending_messages
                 WHERE relay_id = ?1 AND destination = ?2 AND sequence <= ?3",
                params![relay_id, destination.to_string(), sequence],
            )?;
        Ok(())
    }

    pub(crate) fn cleanup(&self) -> Result<CleanupStats> {
        let now = unix_timestamp()?;
        let pending_cutoff = now.saturating_sub(self.limits.pending_retention_secs);
        let receipt_cutoff = now.saturating_sub(self.limits.receipt_retention_secs);
        let mut connection = self.connection.lock().expect("database lock poisoned");
        let transaction = connection.transaction()?;

        // Remove the receipt together with an expired pending delivery. If the
        // original sender still has that message, retrying it creates a fresh
        // delivery instead of acknowledging a queue entry that no longer exists.
        let expired_pending_receipts = transaction.execute(
            "DELETE FROM receipts AS receipt
             WHERE EXISTS (
               SELECT 1 FROM pending_messages AS pending
               WHERE pending.relay_id = receipt.relay_id
                 AND pending.destination = receipt.destination
                 AND pending.sequence = receipt.sequence
                 AND pending.created_at < ?1
             )",
            params![pending_cutoff],
        )?;
        let expired_pending = transaction.execute(
            "DELETE FROM pending_messages WHERE created_at < ?1",
            params![pending_cutoff],
        )?;
        let expired_receipts = transaction.execute(
            "DELETE FROM receipts AS receipt
             WHERE receipt.created_at < ?1
               AND NOT EXISTS (
                 SELECT 1 FROM pending_messages AS pending
                 WHERE pending.relay_id = receipt.relay_id
                   AND pending.destination = receipt.destination
                   AND pending.sequence = receipt.sequence
               )",
            params![receipt_cutoff],
        )?;
        transaction.commit()?;
        connection.execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE);
             PRAGMA incremental_vacuum(1000);",
        )?;
        Ok(CleanupStats {
            expired_pending,
            expired_receipts: expired_pending_receipts + expired_receipts,
        })
    }
}

#[derive(Debug)]
pub(crate) struct CleanupStats {
    pub(crate) expired_pending: usize,
    pub(crate) expired_receipts: usize,
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !columns.iter().any(|existing| existing == column) {
        connection.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

fn unix_timestamp() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    i64::try_from(seconds).context("system clock is out of range")
}

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage> {
    let payload: String = row.get(2)?;
    let payload = serde_json::from_str(&payload).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            payload.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })?;
    let sequence = row.get::<_, i64>(1)?;
    let sequence = u64::try_from(sequence).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(StoredMessage {
        message_id: row.get(0)?,
        sequence,
        payload,
    })
}

fn validate_message_id(message_id: &str) -> Result<()> {
    anyhow::ensure!(!message_id.is_empty(), "message_id must not be empty");
    anyhow::ensure!(message_id.len() <= MAX_ID_BYTES, "message_id is too long");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (tempfile::TempDir, Database) {
        database_with_limits(Limits::default())
    }

    fn database_with_limits(limits: Limits) -> (tempfile::TempDir, Database) {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(&directory.path().join("relay.sqlite3"), limits).unwrap();
        (directory, database)
    }

    #[test]
    fn stores_replays_and_acknowledges_messages() {
        let (_directory, database) = database();
        let payload = serde_json::json!({ "opaque": [1, 2, 3] });
        let stored = database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .unwrap();
        assert_eq!(stored.pending.as_ref().unwrap().sequence, 1);
        assert_eq!(database.pending("pair-a", Endpoint::Two).unwrap().len(), 1);

        database.acknowledge("pair-a", Endpoint::Two, 1).unwrap();
        assert!(
            database
                .pending("pair-a", Endpoint::Two)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn duplicate_message_ids_are_idempotent_after_ack() {
        let (_directory, database) = database();
        let payload = serde_json::json!({ "request": "once" });
        database
            .store("pair-a", Endpoint::Two, "endpoint2-1", &payload)
            .unwrap();
        database.acknowledge("pair-a", Endpoint::One, 1).unwrap();

        let duplicate = database
            .store("pair-a", Endpoint::Two, "endpoint2-1", &payload)
            .unwrap();
        assert!(duplicate.pending.is_none());
        assert!(
            database
                .pending("pair-a", Endpoint::One)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn relay_ids_and_directions_are_isolated() {
        let (_directory, database) = database();
        database
            .store("pair-a", Endpoint::One, "one", &serde_json::json!(1))
            .unwrap();
        database
            .store("pair-b", Endpoint::Two, "one", &serde_json::json!(2))
            .unwrap();

        assert_eq!(database.pending("pair-a", Endpoint::Two).unwrap().len(), 1);
        assert!(
            database
                .pending("pair-a", Endpoint::One)
                .unwrap()
                .is_empty()
        );
        assert_eq!(database.pending("pair-b", Endpoint::One).unwrap().len(), 1);
        assert!(
            database
                .pending("pair-b", Endpoint::Two)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn migrates_legacy_role_names_to_numbered_endpoints() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("relay.sqlite3");
        let database = Database::open(&path, Limits::default()).unwrap();
        database
            .store(
                "pair-a",
                Endpoint::One,
                "endpoint1-1",
                &serde_json::json!(1),
            )
            .unwrap();
        database
            .connection
            .lock()
            .unwrap()
            .execute_batch(
                "UPDATE counters SET destination = 'app' WHERE destination = '2';
                 UPDATE receipts
                 SET source = 'host', destination = 'app'
                 WHERE source = '1' AND destination = '2';
                 UPDATE pending_messages SET destination = 'app' WHERE destination = '2';",
            )
            .unwrap();
        drop(database);

        let database = Database::open(&path, Limits::default()).unwrap();
        assert_eq!(database.pending("pair-a", Endpoint::Two).unwrap().len(), 1);
    }

    #[test]
    fn rejects_new_messages_when_a_direction_reaches_its_limits() {
        let limits = Limits {
            max_pending_messages: 1,
            max_pending_bytes: 1024,
            ..Limits::default()
        };
        let (_directory, database) = database_with_limits(limits);
        let payload = serde_json::json!({ "request": 1 });
        database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .unwrap();

        let error = database
            .store("pair-a", Endpoint::One, "endpoint1-2", &payload)
            .unwrap_err();
        assert!(error.to_string().starts_with("queue_full:"));

        // A retry remains idempotent even while the queue is full.
        database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .unwrap();

        let oversized = serde_json::json!("x".repeat(1024));
        let error = database
            .store("pair-b", Endpoint::One, "endpoint1-1", &oversized)
            .unwrap_err();
        assert!(error.to_string().contains("payload byte limit"));
    }

    #[test]
    fn expires_pending_messages_and_their_receipts_together() {
        let limits = Limits {
            pending_retention_secs: 1,
            receipt_retention_secs: 1,
            ..Limits::default()
        };
        let (_directory, database) = database_with_limits(limits);
        let payload = serde_json::json!({ "request": 1 });
        database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .unwrap();
        database
            .connection
            .lock()
            .unwrap()
            .execute_batch(
                "UPDATE pending_messages SET created_at = 1;
                 UPDATE receipts SET created_at = 1;",
            )
            .unwrap();

        let stats = database.cleanup().unwrap();
        assert_eq!(stats.expired_pending, 1);
        assert_eq!(stats.expired_receipts, 1);
        assert!(
            database
                .pending("pair-a", Endpoint::Two)
                .unwrap()
                .is_empty()
        );

        let retried = database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .unwrap();
        assert_eq!(retried.pending.unwrap().sequence, 2);
    }
}
