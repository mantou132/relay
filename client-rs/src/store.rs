//! Storage abstraction for the relay client.
//!
//! The relay's delivery contract requires the sender to retain a message
//! until the server confirms it with `stored`, and the receiver to record its
//! cursor before acknowledging. Both sides therefore need durable state; this
//! trait lets each host provide it (SQLite, localStorage, files, memory...).

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::OutboundMessage;

#[async_trait]
pub trait OutboxStore: Send + Sync + 'static {
    /// Persists a new outbound message and returns its generated id. Ids must
    /// be unique across process restarts (UUID v4 works well).
    async fn enqueue(&self, payload: Value) -> Result<String>;

    /// Returns all messages that have not yet been confirmed `stored`.
    async fn outbox(&self) -> Vec<OutboundMessage>;

    /// Removes a message after the relay returned `stored`. Removing an
    /// unknown id must be a no-op (`stored` can arrive twice).
    async fn remove_from_outbox(&self, message_id: &str);

    /// Returns the highest in-order sequence this client has processed.
    async fn last_received(&self) -> Option<u64>;

    /// Records a processed sequence. Called only after the payload was
    /// handed to the application, so a crash here causes redelivery, which
    /// is the at-least-once guarantee.
    async fn mark_received(&self, sequence: u64) -> Result<()>;
}

/// Sequence gate shared by all clients: the first observed sequence is
/// adopted as the baseline (the relay's counter outlives this client's
/// cursor), duplicates are dropped, and when a sequence gap occurs
/// (e.g. after retention purge or multi-device consumption), it logs a warning
/// and self-heals by adopting the received sequence.
pub fn is_new_sequence(last_received: Option<u64>, sequence: u64) -> bool {
    let Some(last_received) = last_received else {
        return true;
    };
    if sequence <= last_received {
        return false;
    }
    let expected = last_received.saturating_add(1);
    if sequence != expected {
        tracing::warn!(
            last_received,
            sequence,
            expected,
            "relay message sequence gap; self-healing cursor to received sequence"
        );
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adopts_first_sequence_drops_duplicates_and_self_heals_gaps() {
        assert!(is_new_sequence(None, 42));
        assert!(is_new_sequence(Some(42), 43));
        assert!(!is_new_sequence(Some(42), 42));
        assert!(!is_new_sequence(Some(42), 41));
        assert!(is_new_sequence(Some(42), 44));
    }
}
