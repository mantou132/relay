mod config;
mod database;
mod entity;
mod hub;
mod server;

use anyhow::Result;
use clap::Parser;
use std::io::IsTerminal;

use crate::config::Args;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_logging(args.debug_enabled());
    server::run(args).await
}

fn init_logging(debug: bool) {
    let default_filter = if debug { "relay=debug" } else { "relay=info" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    // ANSI styling is noise in piped or collected logs; keep it for TTYs only.
    let ansi = std::io::stderr().is_terminal();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(ansi)
        .with_target(false)
        .compact()
        .init();
}
