//! Storage abstraction for the relay client.
//!
//! The relay's delivery contract requires the sender to retain a message
//! until the server confirms it with `stored`, and the receiver to record its
//! cursor before acknowledging. Both sides therefore need durable state; this
//! trait lets each host provide it (SQLite, localStorage, files, memory...).

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;

use crate::OutboundMessage;

#[async_trait]
pub trait OutboxStore: Send + Sync + 'static {
    /// Persists a new outbound message and returns its generated id. Ids must
    /// be unique across process restarts (a timestamp/pid prefix plus a
    /// counter works well).
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
/// cursor), duplicates are dropped, and gaps indicate a relay bug or a
/// tampered cursor, so they are hard errors.
pub fn is_new_sequence(last_received: Option<u64>, sequence: u64) -> Result<bool> {
    let Some(last_received) = last_received else {
        return Ok(true);
    };
    if sequence <= last_received {
        return Ok(false);
    }
    let expected = last_received
        .checked_add(1)
        .context("relay receive sequence space exhausted")?;
    anyhow::ensure!(
        sequence == expected,
        "relay message sequence gap: expected {expected}, received {sequence}"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adopts_first_sequence_then_requires_contiguity() {
        assert!(is_new_sequence(None, 42).unwrap());
        assert!(is_new_sequence(Some(42), 43).unwrap());
        assert!(!is_new_sequence(Some(42), 42).unwrap());
        assert!(is_new_sequence(Some(42), 44).is_err());
    }
}
