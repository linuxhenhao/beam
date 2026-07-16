use anyhow::Result;
use beam_core::BeamPaths;
use clap::Parser;

#[derive(Debug, Parser)]
struct DaemonArgs {
    #[arg(long)]
    worker_bin: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    beam_core::logging::init_tracing();

    let args = DaemonArgs::parse();
    beam_daemon::run(
        BeamPaths::discover()?,
        beam_daemon::RunOptions {
            worker_exe: args.worker_bin,
        },
    )
    .await
}
