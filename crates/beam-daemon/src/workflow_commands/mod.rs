//! Unified workflow command handlers shared by dashboard & Lark card-action paths.
//!
//! Phase 5.1 / 5.2: approve/reject/cancel write the correct EventLog events,
//! check idempotency, and push the runtime without duplicating logic.

// Declare submodules in dependency order so re-exports work.
mod approval;
mod cancel;
mod runtime;

pub use approval::*;
pub use cancel::*;
pub use runtime::run_workflow_runtime_once;

// ---------------------------------------------------------------------------
// Shared helper macro — converts any Display error into anyhow::Error
// ---------------------------------------------------------------------------

/// Convert any Display error into an `anyhow::Error` (no logging — callers log
/// at the request execution boundary).
/// Used inside functions that return `anyhow::Result` (Lark handler, cancel handler).
macro_rules! map_anyhow {
    ($e:expr) => {
        $e.map_err(|e| anyhow::anyhow!("{}", e))?
    };
}
// Make it visible to sibling submodules.
pub(crate) use map_anyhow;

// ---------------------------------------------------------------------------
// Shared private helpers — used by approval & cancel submodules
// ---------------------------------------------------------------------------

/// Check whether a run has already reached a terminal status.
pub(super) fn is_terminal(status: &beam_core::RunStatus) -> bool {
    matches!(
        status,
        beam_core::RunStatus::Succeeded
            | beam_core::RunStatus::Failed
            | beam_core::RunStatus::Cancelled
    )
}

/// Convert resolution to a short string.
pub(super) fn resolution_str(r: beam_core::WaitResolution) -> &'static str {
    match r {
        beam_core::WaitResolution::Approved => "approved",
        beam_core::WaitResolution::Rejected => "rejected",
        beam_core::WaitResolution::External => "external",
    }
}

// ---------------------------------------------------------------------------
// Tests — externalised into a dedicated file
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
