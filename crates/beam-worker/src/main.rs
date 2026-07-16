use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
struct WorkerArgs {
    #[arg(long)]
    init_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    beam_core::logging::init_tracing();

    let args = WorkerArgs::parse();
    beam_worker::run_from_init_path(&args.init_path).await
}
