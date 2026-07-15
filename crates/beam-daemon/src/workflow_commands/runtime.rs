use crate::AppState;

// ---------------------------------------------------------------------------
// Runtime advancement — thin wrapper so callers don't need to import
// ---------------------------------------------------------------------------

/// Thin wrapper around `crate::run_workflow_runtime_once` so that
/// workflow-command handlers can advance the runtime without importing the
/// underlying module directly.
pub async fn run_workflow_runtime_once(state: &AppState, run_id: &str, raw_def: &str) {
    crate::run_workflow_runtime_once(state, run_id, raw_def).await
}
