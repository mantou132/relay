use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

const DEFAULT_MAX_PENDING_MESSAGES: u64 = 10_000;
const DEFAULT_MAX_PENDING_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_PENDING_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_RECEIPT_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 60 * 60;

#[derive(Debug, Parser)]
#[command(name = "relay")]
pub(crate) struct Args {
    /// Emit connection and message details. Debug builds enable this automatically.
    #[arg(long, env = "RELAY_DEBUG")]
    debug: bool,

    /// Address on which the WebSocket relay listens.
    #[arg(long, env = "RELAY_BIND", default_value = "127.0.0.1:39371")]
    pub(crate) bind: SocketAddr,

    /// SQLite database containing pending messages and idempotency receipts.
    #[arg(long, env = "RELAY_DATABASE", default_value = "relay.sqlite3")]
    pub(crate) database: PathBuf,

    /// Maximum unacknowledged messages for one pairing id and direction.
    #[arg(
        long,
        env = "RELAY_MAX_PENDING_MESSAGES",
        default_value_t = DEFAULT_MAX_PENDING_MESSAGES
    )]
    max_pending_messages: u64,

    /// Maximum unacknowledged payload bytes for one pairing id and direction.
    #[arg(
        long,
        env = "RELAY_MAX_PENDING_BYTES",
        default_value_t = DEFAULT_MAX_PENDING_BYTES
    )]
    max_pending_bytes: u64,

    /// Seconds before an unacknowledged message expires.
    #[arg(
        long,
        env = "RELAY_PENDING_RETENTION_SECS",
        default_value_t = DEFAULT_PENDING_RETENTION_SECS
    )]
    pending_retention_secs: u64,

    /// Seconds to retain an acknowledged message's idempotency receipt.
    #[arg(
        long,
        env = "RELAY_RECEIPT_RETENTION_SECS",
        default_value_t = DEFAULT_RECEIPT_RETENTION_SECS
    )]
    receipt_retention_secs: u64,

    /// Seconds between database cleanup passes.
    #[arg(
        long,
        env = "RELAY_CLEANUP_INTERVAL_SECS",
        default_value_t = DEFAULT_CLEANUP_INTERVAL_SECS
    )]
    pub(crate) cleanup_interval_secs: u64,
}

impl Args {
    pub(crate) fn debug_enabled(&self) -> bool {
        self.debug || cfg!(debug_assertions)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) max_pending_messages: i64,
    pub(crate) max_pending_bytes: i64,
    pub(crate) pending_retention_secs: i64,
    pub(crate) receipt_retention_secs: i64,
}

impl Limits {
    pub(crate) fn from_args(args: &Args) -> Result<Self> {
        anyhow::ensure!(
            args.max_pending_messages > 0,
            "max pending messages must be positive"
        );
        anyhow::ensure!(
            args.max_pending_bytes > 0,
            "max pending bytes must be positive"
        );
        anyhow::ensure!(
            args.pending_retention_secs > 0,
            "pending retention must be positive"
        );
        anyhow::ensure!(
            args.receipt_retention_secs > 0,
            "receipt retention must be positive"
        );
        anyhow::ensure!(
            args.cleanup_interval_secs > 0,
            "cleanup interval must be positive"
        );
        anyhow::ensure!(
            args.receipt_retention_secs >= args.pending_retention_secs,
            "receipt retention must be at least as long as pending retention"
        );
        Ok(Self {
            max_pending_messages: i64::try_from(args.max_pending_messages)
                .context("max pending messages is too large")?,
            max_pending_bytes: i64::try_from(args.max_pending_bytes)
                .context("max pending bytes is too large")?,
            pending_retention_secs: i64::try_from(args.pending_retention_secs)
                .context("pending retention is too large")?,
            receipt_retention_secs: i64::try_from(args.receipt_retention_secs)
                .context("receipt retention is too large")?,
        })
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_pending_messages: DEFAULT_MAX_PENDING_MESSAGES as i64,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES as i64,
            pending_retention_secs: DEFAULT_PENDING_RETENTION_SECS as i64,
            receipt_retention_secs: DEFAULT_RECEIPT_RETENTION_SECS as i64,
        }
    }
}
