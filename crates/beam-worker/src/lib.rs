mod adapter;
mod adapters;
mod backend;
mod worker_runtime;

use std::path::Path;

use anyhow::Result;
use beam_core::InitConfig;

pub use worker_runtime::run;

#[cfg(test)]
#[path = "worker_runtime/tests.rs"]
mod tests;

pub async fn run_from_init_path(path: &Path) -> Result<()> {
    let payload = tokio::fs::read_to_string(path).await?;
    let init = serde_json::from_str::<InitConfig>(&payload)?;
    run(init).await
}
