mod config;
mod database;
mod hub;
mod server;

use anyhow::Result;
use clap::Parser;

use crate::config::Args;

#[tokio::main]
async fn main() -> Result<()> {
    server::run(Args::parse()).await
}
