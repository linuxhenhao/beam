use anyhow::Result;
use beam_core::BeamPaths;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct DaemonArgs {
    #[arg(long)]
    worker_bin: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let args = DaemonArgs::parse();
    beam_daemon::run(
        BeamPaths::discover()?,
        beam_daemon::RunOptions {
            worker_exe: args.worker_bin,
        },
    )
    .await
}
