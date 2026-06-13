use anyhow::Result;
use beam_core::InitConfig;

use crate::adapter::{PollResult, SpawnSpec, SubmitResult};
use crate::backend::SessionBackend;

pub fn build_spawn_spec(init: &InitConfig) -> SpawnSpec {
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args: init.cli_args.clone(),
    }
}

pub async fn write_input(backend: &dyn SessionBackend, content: &str) -> Result<SubmitResult> {
    backend.raw_input(content).await?;
    Ok(SubmitResult {
        submitted: true,
        ..Default::default()
    })
}

pub fn poll() -> Result<PollResult> {
    Ok(PollResult::default())
}
