use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct WorkerArgs {
    #[arg(long)]
    init_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let args = WorkerArgs::parse();
    beam_worker::run_from_init_path(&args.init_path).await
}
