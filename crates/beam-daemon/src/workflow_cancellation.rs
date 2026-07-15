//! Workflow active cancellation registry.
//!
//! Provides a thread-safe registry of [`CancellationToken`]s keyed by
//! workflow run / activity / node, so that `cancelRequested` events
//! propagated by `check_pending_cancels` can immediately signal any
//! in-flight dispatch to cooperatively stop.
//!
//! # Lifecycle
//!
//! 1. **Registration**: daemon hooks call `register_activity` before
//!    dispatching subagent / host-executor work, and `unregister_activity`
//!    after the dispatch completes.
//! 2. **Cancellation**: `check_pending_cancels` (core) calls
//!    `on_activities_cancelled` (daemon hook impl), which calls
//!    `cancel_activity` / `cancel_node` / `cancel_run` on the registry.
//! 3. **Observation**: the dispatch function checks the token via
//!    `token.is_cancelled()` and returns `WorkflowDispatchOutcome::Cancelled`
//!    early when appropriate.
//!
//! # Run-level vs activity-level vs node-level cancel
//!
//! - `cancel_run(run_id)` cancels every activity and node token registered
//!   under that run.
//! - `cancel_activity(run_id, activity_id)` cancels a single activity.
//! - `cancel_node(run_id, node_id)` cancels the node's token plus all
//!   activity tokens whose activity id contains `node_id` as a segment.
//!
//! Activity-level cancels are always driven by the `.cancels` field in
//! `dangling.cancels` that was observed by `check_pending_cancels`.
//!
//! NOTE: Some API methods (`register_node`, `unregister_node`, `lookup_activity`,
//! `active_activity_ids`, `total_activities`, `total_nodes`) are available for
//! future use (e.g. node-level cancellation in loop runtimes) but are not yet
//! called from the daemon hooks.  They are tested in the unit tests below.
//!
//! Task 6.3: The worker termination signal escalation (SIGINT → grace → SIGKILL)
//! lives in `lib.rs` (`terminate_workflow_worker_process`) and is invoked from
//! `run_workflow_subagent_session` when the cancellation token fires.  The
//! escalation ordering is verified via mockable tests in this module
//! (`signal_escalation_*` tests).

mod delivery;
mod registry;

pub use delivery::{ActivityTokenGuard, global_cancellation_registry};
// Only imported by path in tests; re-export kept for public API consistency.
#[allow(unused_imports)]
pub use registry::WorkflowCancellationRegistry;

#[cfg(test)]
mod tests;
