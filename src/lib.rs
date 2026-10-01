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
        /// Forward only to currently connected devices, without storage,
        /// `stored`, sequence, or acknowledgement.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        ephemeral: bool,
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
    /// An ephemeral message from the peer. It has no sequence and is not acknowledged.
    Ephemeral {
        message_id: String,
        payload: Value,
    },
    /// An ephemeral message (text or binary) reached no device, because none
    /// was connected or every receive queue was full.
    Undeliverable {
        message_id: String,
        reason: String,
    },
    Error {
        message: String,
    },
}

/// Header of a binary WebSocket frame. Binary frames are always ephemeral and
/// are laid out as `[u16 big-endian header length][header JSON][body]`. The
/// relay forwards them unchanged.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BinaryHeader {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_device_id: Option<String>,
}

pub fn encode_binary_frame(header: &BinaryHeader, body: &[u8]) -> Vec<u8> {
    let header = serde_json::to_vec(header).expect("binary header serializes");
    let header_len = u16::try_from(header.len()).expect("binary header fits in u16");
    let mut frame = Vec::with_capacity(2 + header.len() + body.len());
    frame.extend_from_slice(&header_len.to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    frame
}

/// Returns the header and the byte offset at which the body starts.
pub fn decode_binary_frame(frame: &[u8]) -> Result<(BinaryHeader, usize), String> {
    let len_bytes: [u8; 2] = frame
        .get(..2)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or("binary frame is missing its header length")?;
    let body_start = 2 + usize::from(u16::from_be_bytes(len_bytes));
    let header = frame
        .get(2..body_start)
        .ok_or("binary frame header is truncated")?;
    let header = serde_json::from_slice(header)
        .map_err(|error| format!("invalid binary header: {error}"))?;
    Ok((header, body_start))
}
