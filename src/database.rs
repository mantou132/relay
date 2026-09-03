use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use relay::{Endpoint, ServerFrame};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, Condition, ConnectOptions, ConnectionTrait, Database as SeaDatabase,
    DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, Schema,
    TransactionTrait,
    sqlx::sqlite::{SqliteAutoVacuum, SqliteJournalMode, SqliteSynchronous},
};
use serde_json::Value;

use crate::{
    config::Limits,
    entity::{counter, device, pending_message, receipt},
};

const MAX_ID_BYTES: usize = 256;
const CLEANUP_DELETE_BATCH_SIZE: usize = 100;

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
    connection: DatabaseConnection,
    limits: Limits,
}

impl Database {
    pub(crate) async fn open(path: &Path, limits: Limits) -> Result<Self> {
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

        // The actual path is supplied through SQLite's typed options so paths
        // containing URL-reserved or non-UTF-8 characters remain valid.
        let database_path = path.to_owned();
        let mut options = ConnectOptions::new("sqlite://relay.sqlite3");
        options
            // A single async connection preserves SQLite's write ordering
            // without blocking a Tokio worker on a synchronous mutex.
            .max_connections(1)
            .min_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(move |options| {
                options
                    .filename(database_path.clone())
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Full)
                    .auto_vacuum(SqliteAutoVacuum::Full)
            });
        let connection = SeaDatabase::connect(options)
            .await
            .with_context(|| format!("failed to open relay database {}", path.display()))?;

        initialize_schema(&connection).await?;
        Ok(Self { connection, limits })
    }

    pub(crate) async fn store(
        &self,
        relay_id: &str,
        source: Endpoint,
        message_id: &str,
        payload: &Value,
    ) -> Result<StoreResult> {
        validate_message_id(message_id)?;
        let destination = source.opposite().to_string();
        let source = source.to_string();
        let payload_bytes =
            i64::try_from(serde_json::to_vec(payload)?.len()).context("payload is too large")?;
        let now = unix_timestamp()?;
        let transaction = self.connection.begin().await?;

        let stored_receipt = receipt::Entity::find_by_id((
            relay_id.to_owned(),
            source.clone(),
            message_id.to_owned(),
        ))
        .one(&transaction)
        .await?;

        let sequence = if let Some(stored) = stored_receipt {
            anyhow::ensure!(
                stored.destination == destination && stored.payload == *payload,
                "message_id was already used with a different payload"
            );
            stored.sequence
        } else {
            let (pending_count, pending_bytes): (i64, Option<i64>) =
                pending_message::Entity::find()
                    .select_only()
                    .column_as(pending_message::Column::Sequence.count(), "pending_count")
                    .column_as(pending_message::Column::PayloadBytes.sum(), "pending_bytes")
                    .filter(pending_message::Column::RelayId.eq(relay_id))
                    .filter(pending_message::Column::Destination.eq(&destination))
                    .into_tuple()
                    .one(&transaction)
                    .await?
                    .context("queue totals query returned no row")?;
            let pending_bytes = pending_bytes.unwrap_or(0);
            anyhow::ensure!(
                pending_count < self.limits.max_pending_messages,
                "queue_full: pending message count limit reached"
            );
            anyhow::ensure!(
                payload_bytes <= self.limits.max_pending_bytes
                    && pending_bytes <= self.limits.max_pending_bytes.saturating_sub(payload_bytes),
                "queue_full: pending payload byte limit reached"
            );

            let counter = counter::Entity::find_by_id((relay_id.to_owned(), destination.clone()))
                .one(&transaction)
                .await?;
            let sequence = counter
                .as_ref()
                .map_or(0, |counter| counter.next_sequence)
                .checked_add(1)
                .context("relay sequence space exhausted")?;
            if let Some(counter) = counter {
                let mut counter = counter.into_active_model();
                counter.next_sequence = Set(sequence);
                counter.update(&transaction).await?;
            } else {
                counter::ActiveModel {
                    relay_id: Set(relay_id.to_owned()),
                    destination: Set(destination.clone()),
                    next_sequence: Set(sequence),
                }
                .insert(&transaction)
                .await?;
            }

            receipt::ActiveModel {
                relay_id: Set(relay_id.to_owned()),
                source: Set(source),
                message_id: Set(message_id.to_owned()),
                destination: Set(destination.clone()),
                sequence: Set(sequence),
                payload: Set(payload.clone()),
                created_at: Set(now),
            }
            .insert(&transaction)
            .await?;
            pending_message::ActiveModel {
                relay_id: Set(relay_id.to_owned()),
                destination: Set(destination.clone()),
                sequence: Set(sequence),
                message_id: Set(message_id.to_owned()),
                payload: Set(payload.clone()),
                payload_bytes: Set(payload_bytes),
                created_at: Set(now),
            }
            .insert(&transaction)
            .await?;
            sequence
        };

        let pending =
            pending_message::Entity::find_by_id((relay_id.to_owned(), destination, sequence))
                .one(&transaction)
                .await?
                .map(StoredMessage::try_from)
                .transpose()?;
        transaction.commit().await?;
        Ok(StoreResult { pending })
    }

    pub(crate) async fn register_device(
        &self,
        relay_id: &str,
        endpoint: Endpoint,
        device_id: &str,
    ) -> Result<i64> {
        let now = unix_timestamp()?;
        let transaction = self.connection.begin().await?;
        let existing = device::Entity::find_by_id((
            relay_id.to_owned(),
            endpoint.to_string(),
            device_id.to_owned(),
        ))
        .one(&transaction)
        .await?;

        let last_acked = if let Some(dev) = existing {
            let mut active = dev.clone().into_active_model();
            active.last_seen_at = Set(now);
            active.update(&transaction).await?;
            dev.last_acked_sequence
        } else {
            device::ActiveModel {
                relay_id: Set(relay_id.to_owned()),
                endpoint: Set(endpoint.to_string()),
                device_id: Set(device_id.to_owned()),
                last_acked_sequence: Set(0),
                last_seen_at: Set(now),
            }
            .insert(&transaction)
            .await?;
            0
        };
        transaction.commit().await?;
        Ok(last_acked)
    }

    #[cfg(test)]
    pub(crate) async fn pending(
        &self,
        relay_id: &str,
        destination: Endpoint,
    ) -> Result<Vec<StoredMessage>> {
        pending_message::Entity::find()
            .filter(pending_message::Column::RelayId.eq(relay_id))
            .filter(pending_message::Column::Destination.eq(destination.to_string()))
            .order_by_asc(pending_message::Column::Sequence)
            .all(&self.connection)
            .await?
            .into_iter()
            .map(StoredMessage::try_from)
            .collect()
    }

    pub(crate) async fn pending_for_device(
        &self,
        relay_id: &str,
        destination: Endpoint,
        device_id: &str,
    ) -> Result<Vec<StoredMessage>> {
        let dev = device::Entity::find_by_id((
            relay_id.to_owned(),
            destination.to_string(),
            device_id.to_owned(),
        ))
        .one(&self.connection)
        .await?;

        let last_acked = dev.map_or(0, |d| d.last_acked_sequence);

        pending_message::Entity::find()
            .filter(pending_message::Column::RelayId.eq(relay_id))
            .filter(pending_message::Column::Destination.eq(destination.to_string()))
            .filter(pending_message::Column::Sequence.gt(last_acked))
            .order_by_asc(pending_message::Column::Sequence)
            .all(&self.connection)
            .await?
            .into_iter()
            .map(StoredMessage::try_from)
            .collect()
    }

    pub(crate) async fn acknowledge(
        &self,
        relay_id: &str,
        destination: Endpoint,
        device_id: &str,
        sequence: u64,
    ) -> Result<u64> {
        let sequence = i64::try_from(sequence).unwrap_or(i64::MAX);
        let now = unix_timestamp()?;
        let transaction = self.connection.begin().await?;

        let dev = device::Entity::find_by_id((
            relay_id.to_owned(),
            destination.to_string(),
            device_id.to_owned(),
        ))
        .one(&transaction)
        .await?;

        if let Some(dev) = dev {
            if sequence > dev.last_acked_sequence {
                let mut active = dev.into_active_model();
                active.last_acked_sequence = Set(sequence);
                active.last_seen_at = Set(now);
                active.update(&transaction).await?;
            }
        } else {
            device::ActiveModel {
                relay_id: Set(relay_id.to_owned()),
                endpoint: Set(destination.to_string()),
                device_id: Set(device_id.to_owned()),
                last_acked_sequence: Set(sequence),
                last_seen_at: Set(now),
            }
            .insert(&transaction)
            .await?;
        }

        // Find the minimum acknowledged sequence across all devices registered
        // for this (relay_id, destination).
        let all_devices = device::Entity::find()
            .filter(device::Column::RelayId.eq(relay_id))
            .filter(device::Column::Endpoint.eq(destination.to_string()))
            .all(&transaction)
            .await?;

        let min_acked = all_devices
            .iter()
            .map(|d| d.last_acked_sequence)
            .min()
            .unwrap_or(sequence);

        let result = pending_message::Entity::delete_many()
            .filter(pending_message::Column::RelayId.eq(relay_id))
            .filter(pending_message::Column::Destination.eq(destination.to_string()))
            .filter(pending_message::Column::Sequence.lte(min_acked))
            .exec(&transaction)
            .await?;

        transaction.commit().await?;
        Ok(result.rows_affected)
    }

    pub(crate) async fn cleanup(&self) -> Result<CleanupStats> {
        let now = unix_timestamp()?;
        let pending_cutoff = now.saturating_sub(self.limits.pending_retention_secs);
        let receipt_cutoff = now.saturating_sub(self.limits.receipt_retention_secs);
        let transaction = self.connection.begin().await?;

        // Receipts for expired deliveries are removed in bounded batches. A
        // retry can then create a fresh delivery instead of matching a queue
        // entry that no longer exists.
        let expired = pending_message::Entity::find()
            .select_only()
            .column(pending_message::Column::RelayId)
            .column(pending_message::Column::Destination)
            .column(pending_message::Column::Sequence)
            .filter(pending_message::Column::CreatedAt.lt(pending_cutoff))
            .into_tuple::<(String, String, i64)>()
            .all(&transaction)
            .await?;
        let mut expired_pending_receipts = 0_u64;
        for batch in expired.chunks(CLEANUP_DELETE_BATCH_SIZE) {
            let condition = batch.iter().fold(Condition::any(), |condition, pending| {
                condition.add(
                    Condition::all()
                        .add(receipt::Column::RelayId.eq(&pending.0))
                        .add(receipt::Column::Destination.eq(&pending.1))
                        .add(receipt::Column::Sequence.eq(pending.2)),
                )
            });
            expired_pending_receipts += receipt::Entity::delete_many()
                .filter(condition)
                .exec(&transaction)
                .await?
                .rows_affected;
        }
        let expired_pending = pending_message::Entity::delete_many()
            .filter(pending_message::Column::CreatedAt.lt(pending_cutoff))
            .exec(&transaction)
            .await?
            .rows_affected;

        // Pending and receipt timestamps are written together, and pending
        // retention never exceeds receipt retention. Any receipt this old
        // that remains after the deletion above therefore represents an
        // already acknowledged message.
        let expired_receipts = receipt::Entity::delete_many()
            .filter(receipt::Column::CreatedAt.lt(receipt_cutoff))
            .exec(&transaction)
            .await?
            .rows_affected;
        // Expire stale devices that have not been seen for receipt_retention_secs,
        // ensuring abandoned devices do not permanently block queue pruning.
        let _expired_devices = device::Entity::delete_many()
            .filter(device::Column::LastSeenAt.lt(receipt_cutoff))
            .exec(&transaction)
            .await?
            .rows_affected;

        transaction.commit().await?;

        Ok(CleanupStats {
            expired_pending: usize::try_from(expired_pending)
                .context("expired pending count is too large")?,
            expired_receipts: usize::try_from(expired_pending_receipts + expired_receipts)
                .context("expired receipt count is too large")?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct CleanupStats {
    pub(crate) expired_pending: usize,
    pub(crate) expired_receipts: usize,
}

async fn initialize_schema(connection: &DatabaseConnection) -> Result<()> {
    let schema = Schema::new(connection.get_database_backend());
    for mut table in [
        schema.create_table_from_entity(counter::Entity),
        schema.create_table_from_entity(receipt::Entity),
        schema.create_table_from_entity(pending_message::Entity),
        schema.create_table_from_entity(device::Entity),
    ] {
        table.if_not_exists();
        connection.execute(&table).await?;
    }

    let indexes = schema
        .create_index_from_entity(receipt::Entity)
        .into_iter()
        .chain(schema.create_index_from_entity(pending_message::Entity))
        .chain(schema.create_index_from_entity(device::Entity));
    for mut index in indexes {
        index.if_not_exists();
        connection.execute(&index).await?;
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

impl TryFrom<pending_message::Model> for StoredMessage {
    type Error = anyhow::Error;

    fn try_from(model: pending_message::Model) -> Result<Self> {
        Ok(Self {
            message_id: model.message_id,
            sequence: u64::try_from(model.sequence).context("stored sequence is negative")?,
            payload: model.payload,
        })
    }
}

fn validate_message_id(message_id: &str) -> Result<()> {
    anyhow::ensure!(!message_id.is_empty(), "message_id must not be empty");
    anyhow::ensure!(message_id.len() <= MAX_ID_BYTES, "message_id is too long");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::sea_query::Expr;

    async fn database() -> (tempfile::TempDir, Database) {
        database_with_limits(Limits::default()).await
    }

    async fn database_with_limits(limits: Limits) -> (tempfile::TempDir, Database) {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(&directory.path().join("relay.sqlite3"), limits)
            .await
            .unwrap();
        (directory, database)
    }

    #[tokio::test]
    async fn stores_replays_and_acknowledges_messages() {
        let (_directory, database) = database().await;
        let payload = serde_json::json!({ "opaque": [1, 2, 3] });
        let stored = database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .await
            .unwrap();
        assert_eq!(stored.pending.as_ref().unwrap().sequence, 1);
        assert_eq!(
            database
                .pending("pair-a", Endpoint::Two)
                .await
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            database
                .acknowledge("pair-a", Endpoint::Two, "default", 1)
                .await
                .unwrap(),
            1
        );
        assert!(
            database
                .pending("pair-a", Endpoint::Two)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn duplicate_message_ids_are_idempotent_after_ack() {
        let (_directory, database) = database().await;
        let payload = serde_json::json!({ "request": "once" });
        database
            .store("pair-a", Endpoint::Two, "endpoint2-1", &payload)
            .await
            .unwrap();
        database
            .acknowledge("pair-a", Endpoint::One, "default", 1)
            .await
            .unwrap();

        let duplicate = database
            .store("pair-a", Endpoint::Two, "endpoint2-1", &payload)
            .await
            .unwrap();
        assert!(duplicate.pending.is_none());
        assert!(
            database
                .pending("pair-a", Endpoint::One)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn relay_ids_and_directions_are_isolated() {
        let (_directory, database) = database().await;
        database
            .store("pair-a", Endpoint::One, "one", &serde_json::json!(1))
            .await
            .unwrap();
        database
            .store("pair-b", Endpoint::Two, "one", &serde_json::json!(2))
            .await
            .unwrap();

        assert_eq!(
            database
                .pending("pair-a", Endpoint::Two)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            database
                .pending("pair-a", Endpoint::One)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            database
                .pending("pair-b", Endpoint::One)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            database
                .pending("pair-b", Endpoint::Two)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn rejects_new_messages_when_a_direction_reaches_its_limits() {
        let limits = Limits {
            max_pending_messages: 1,
            max_pending_bytes: 1024,
            ..Limits::default()
        };
        let (_directory, database) = database_with_limits(limits).await;
        let payload = serde_json::json!({ "request": 1 });
        database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .await
            .unwrap();

        let error = database
            .store("pair-a", Endpoint::One, "endpoint1-2", &payload)
            .await
            .unwrap_err();
        assert!(error.to_string().starts_with("queue_full:"));

        // A retry remains idempotent even while the queue is full.
        database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .await
            .unwrap();

        let oversized = serde_json::json!("x".repeat(1024));
        let error = database
            .store("pair-b", Endpoint::One, "endpoint1-1", &oversized)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("payload byte limit"));
    }

    #[tokio::test]
    async fn expires_pending_messages_and_their_receipts_together() {
        let limits = Limits {
            pending_retention_secs: 1,
            receipt_retention_secs: 1,
            ..Limits::default()
        };
        let (_directory, database) = database_with_limits(limits).await;
        let payload = serde_json::json!({ "request": 1 });
        database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .await
            .unwrap();
        pending_message::Entity::update_many()
            .col_expr(pending_message::Column::CreatedAt, Expr::value(1))
            .exec(&database.connection)
            .await
            .unwrap();
        receipt::Entity::update_many()
            .col_expr(receipt::Column::CreatedAt, Expr::value(1))
            .exec(&database.connection)
            .await
            .unwrap();

        let stats = database.cleanup().await.unwrap();
        assert_eq!(stats.expired_pending, 1);
        assert_eq!(stats.expired_receipts, 1);
        assert!(
            database
                .pending("pair-a", Endpoint::Two)
                .await
                .unwrap()
                .is_empty()
        );

        let retried = database
            .store("pair-a", Endpoint::One, "endpoint1-1", &payload)
            .await
            .unwrap();
        assert_eq!(retried.pending.unwrap().sequence, 2);
    }

    #[tokio::test]
    async fn multi_device_independent_cursors_and_purging() {
        let (_directory, database) = database().await;

        // Register two devices for Endpoint::Two
        let phone_a_acked = database
            .register_device("pair-m", Endpoint::Two, "phone_a")
            .await
            .unwrap();
        assert_eq!(phone_a_acked, 0);

        let phone_b_acked = database
            .register_device("pair-m", Endpoint::Two, "phone_b")
            .await
            .unwrap();
        assert_eq!(phone_b_acked, 0);

        // Store 3 messages destined for Endpoint::Two
        database
            .store("pair-m", Endpoint::One, "msg-1", &serde_json::json!("first"))
            .await
            .unwrap();
        database
            .store("pair-m", Endpoint::One, "msg-2", &serde_json::json!("second"))
            .await
            .unwrap();
        database
            .store("pair-m", Endpoint::One, "msg-3", &serde_json::json!("third"))
            .await
            .unwrap();

        // Both devices should see all 3 pending messages initially
        let pending_a = database
            .pending_for_device("pair-m", Endpoint::Two, "phone_a")
            .await
            .unwrap();
        assert_eq!(pending_a.len(), 3);
        let pending_b = database
            .pending_for_device("pair-m", Endpoint::Two, "phone_b")
            .await
            .unwrap();
        assert_eq!(pending_b.len(), 3);

        // phone_a acks up to sequence 2
        let deleted = database
            .acknowledge("pair-m", Endpoint::Two, "phone_a", 2)
            .await
            .unwrap();
        // Since phone_b is still at 0, min_acked is 0, so 0 messages deleted from DB
        assert_eq!(deleted, 0);

        // phone_a now only has sequence 3 pending
        let pending_a = database
            .pending_for_device("pair-m", Endpoint::Two, "phone_a")
            .await
            .unwrap();
        assert_eq!(pending_a.len(), 1);
        assert_eq!(pending_a[0].sequence, 3);

        // phone_b STILL has all 3 messages pending!
        let pending_b = database
            .pending_for_device("pair-m", Endpoint::Two, "phone_b")
            .await
            .unwrap();
        assert_eq!(pending_b.len(), 3);

        // phone_b now acks up to sequence 2
        let deleted = database
            .acknowledge("pair-m", Endpoint::Two, "phone_b", 2)
            .await
            .unwrap();
        // Now BOTH devices have acked up to sequence 2, so sequences 1 and 2 are deleted!
        assert_eq!(deleted, 2);

        // Now both devices only have sequence 3 pending
        assert_eq!(
            database
                .pending_for_device("pair-m", Endpoint::Two, "phone_a")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            database
                .pending_for_device("pair-m", Endpoint::Two, "phone_b")
                .await
                .unwrap()
                .len(),
            1
        );

        // phone_b acks sequence 3
        database
            .acknowledge("pair-m", Endpoint::Two, "phone_b", 3)
            .await
            .unwrap();
        // phone_a acks sequence 3
        let deleted = database
            .acknowledge("pair-m", Endpoint::Two, "phone_a", 3)
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        // Database pending_messages is now completely empty
        assert!(
            database
                .pending("pair-m", Endpoint::Two)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
