use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One of the two deliberately distinct endpoints of a relay channel.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Endpoint {
    #[serde(rename = "1")]
    One,
    #[serde(rename = "2")]
    Two,
}

impl Endpoint {
    pub fn opposite(self) -> Self {
        match self {
            Self::One => Self::Two,
            Self::Two => Self::One,
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::One => "1",
            Self::Two => "2",
        })
    }
}

/// Frames accepted from either endpoint. Payloads are intentionally opaque to
/// the relay, allowing callers to carry any JSON-based protocol.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    Message {
        message_id: String,
        payload: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_device_id: Option<String>,
    },
    /// Cumulative acknowledgement for all received sequences up to this one.
    Ack {
        sequence: u64,
    },
}

/// Frames emitted by the relay server.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    Ready {
        endpoint: Endpoint,
    },
    /// The server durably stored an outbound message. The sender may now remove
    /// it from its local outbox; resending the id remains idempotent.
    Stored {
        message_id: String,
    },
    /// The server rejected an outbound message. The sender removes it from its outbox
    /// to avoid retrying a permanent failure.
    Rejected {
        message_id: String,
        reason: String,
    },
    Message {
        message_id: String,
        sequence: u64,
        payload: Value,
    },
    Error {
        message: String,
    },
}
