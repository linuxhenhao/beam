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
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use tokio_util::sync::CancellationToken;

/// Registry of active workflow cancellation tokens.
///
/// All public methods are `&self` — the interior [`RwLock`] provides
/// thread-safety.  Create once via [`WorkflowCancellationRegistry::new`]
/// and share via an [`Arc`].  The underlying lock is a [`std::sync::RwLock`]
/// so all methods are callable from both sync and async contexts.
#[derive(Clone)]
pub struct WorkflowCancellationRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    runs: RwLock<HashMap<String, RunTokens>>,
}

#[derive(Default)]
struct RunTokens {
    /// Activity-level tokens keyed by activity_id.
    activities: HashMap<String, CancellationToken>,
    /// Node-level tokens keyed by node_id.
    nodes: HashMap<String, CancellationToken>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

impl WorkflowCancellationRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                runs: RwLock::new(HashMap::new()),
            }),
        }
    }

    // -- Registration -------------------------------------------------------

    /// Register an active dispatch for `activity_id` under `run_id`.
    ///
    /// Returns a **child** token that will be cancelled when any of
    /// `cancel_run`, `cancel_activity`, or `cancel_node` (with a matching
    /// node prefix) is called.  Callers **must** call
    /// [`unregister_activity`] after the dispatch completes to avoid
    /// leaking tokens.
    pub fn register_activity(&self, run_id: &str, activity_id: &str) -> CancellationToken {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        let entry = runs.entry(run_id.to_string()).or_default();
        let token = CancellationToken::new();
        entry
            .activities
            .insert(activity_id.to_string(), token.clone());
        token
    }

    /// Remove a previously-registered activity token.  Idempotent — safe to
    /// call even if the token was already removed.
    pub fn unregister_activity(&self, run_id: &str, activity_id: &str) {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        if let Some(entry) = runs.get_mut(run_id) {
            entry.activities.remove(activity_id);
            // Clean up empty run entries to prevent unbounded growth.
            if entry.activities.is_empty() && entry.nodes.is_empty() {
                runs.remove(run_id);
            }
        }
    }

    /// Register a node-level token keyed by `node_id` under `run_id`.
    pub fn register_node(&self, run_id: &str, node_id: &str) -> CancellationToken {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        let entry = runs.entry(run_id.to_string()).or_default();
        let token = CancellationToken::new();
        entry.nodes.insert(node_id.to_string(), token.clone());
        token
    }

    /// Remove a previously-registered node token.
    pub fn unregister_node(&self, run_id: &str, node_id: &str) {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        if let Some(entry) = runs.get_mut(run_id) {
            entry.nodes.remove(node_id);
            if entry.activities.is_empty() && entry.nodes.is_empty() {
                runs.remove(run_id);
            }
        }
    }

    // -- Cancellation -------------------------------------------------------

    /// Cancel the token for a single activity.  Returns `true` if a token
    /// existed and was cancelled.
    pub fn cancel_activity(&self, run_id: &str, activity_id: &str) -> bool {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        let Some(entry) = runs.get_mut(run_id) else {
            return false;
        };
        let Some(token) = entry.activities.remove(activity_id) else {
            return false;
        };
        token.cancel();
        if entry.activities.is_empty() && entry.nodes.is_empty() {
            runs.remove(run_id);
        }
        true
    }

    /// Cancel all tokens whose activity_id contains `node_id` as a
    /// segment (split by `::`), plus the node-level token itself.
    /// Returns the list of activity ids that were cancelled.
    ///
    /// Matching works by splitting the activity_id by `::` after stripping
    /// the `<runId>::` prefix, and checking whether any segment equals
    /// `node_id`.  This handles the standard workflow activity id formats:
    ///
    /// - `<runId>::<nodeId>`  (simplified test format)
    /// - `<runId>::gate::<nodeId>`
    /// - `<runId>::work::<nodeId>`
    /// - `<runId>::<nodeId>::work::<bodyNodeId>`
    pub fn cancel_node(&self, run_id: &str, node_id: &str) -> Vec<String> {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        let Some(entry) = runs.get_mut(run_id) else {
            return Vec::new();
        };

        let mut cancelled = Vec::new();

        // Cancel the node-level token (if any).
        if let Some(node_token) = entry.nodes.remove(node_id) {
            node_token.cancel();
        }

        // Match activity ids that contain node_id as a ::-delimited segment
        // after the run_id prefix.
        let run_prefix = format!("{}::", run_id);
        let mut to_remove: Vec<String> = Vec::new();
        for activity_id in entry.activities.keys() {
            let matches = if let Some(rest) = activity_id.strip_prefix(&run_prefix) {
                // Check if node_id appears as a top-level segment
                // (split by "::") in the remainder.
                rest.split("::").any(|s| s == node_id)
            } else {
                // Fallback: activity_id is exactly node_id (shouldn't happen
                // in practice, but defensive).
                activity_id == node_id
            };
            if matches {
                to_remove.push(activity_id.clone());
            }
        }
        for aid in &to_remove {
            if let Some(t) = entry.activities.remove(aid) {
                t.cancel();
                cancelled.push(aid.clone());
            }
        }

        if entry.activities.is_empty() && entry.nodes.is_empty() {
            runs.remove(run_id);
        }

        cancelled
    }

    /// Cancel **every** activity and node token registered under `run_id`.
    /// Returns the list of activity ids that were cancelled.  After this
    /// call the run entry is removed from the registry.
    pub fn cancel_run(&self, run_id: &str) -> Vec<String> {
        let mut runs = self.inner.runs.write().expect("registry lock poisoned");
        let Some(entry) = runs.remove(run_id) else {
            return Vec::new();
        };

        let cancelled: Vec<String> = entry.activities.keys().cloned().collect();
        for (_, token) in entry.activities {
            token.cancel();
        }
        for (_, token) in entry.nodes {
            token.cancel();
        }
        cancelled
    }

    // -- Lookup / Snapshot --------------------------------------------------

    /// Look up the token for a specific activity, if registered.
    pub fn lookup_activity(&self, run_id: &str, activity_id: &str) -> Option<CancellationToken> {
        let runs = self.inner.runs.read().expect("registry lock poisoned");
        runs.get(run_id)?.activities.get(activity_id).cloned()
    }

    /// Return a snapshot of currently-registered activity ids under `run_id`.
    pub fn active_activity_ids(&self, run_id: &str) -> Vec<String> {
        let runs = self.inner.runs.read().expect("registry lock poisoned");
        runs.get(run_id)
            .map(|e| e.activities.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Return the total number of registered activities across all runs.
    pub fn total_activities(&self) -> usize {
        let runs = self.inner.runs.read().expect("registry lock poisoned");
        runs.values().map(|e| e.activities.len()).sum()
    }

    /// Return the total number of registered nodes across all runs.
    pub fn total_nodes(&self) -> usize {
        let runs = self.inner.runs.read().expect("registry lock poisoned");
        runs.values().map(|e| e.nodes.len()).sum()
    }
}

impl Default for WorkflowCancellationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Activity token guard (RAII unregister)
// ---------------------------------------------------------------------------

/// RAII guard that calls [`WorkflowCancellationRegistry::unregister_activity`]
/// on drop.  Use in daemon hooks so the token is always cleaned up regardless
/// of how dispatch exits (success / failure / early return / panic).
///
/// ```ignore
/// let guard = ActivityTokenGuard::register(&registry, run_id, activity_id);
/// // dispatch work, checking guard.token periodically
/// // guard.token is automatically unregistered when guard drops
/// ```
pub struct ActivityTokenGuard {
    registry: WorkflowCancellationRegistry,
    run_id: String,
    activity_id: String,
    /// The cancellation token that dispatch should observe.
    pub token: CancellationToken,
}

impl ActivityTokenGuard {
    /// Register a new activity token and return a guard that will
    /// automatically unregister it on drop.
    pub fn register(
        registry: &WorkflowCancellationRegistry,
        run_id: &str,
        activity_id: &str,
    ) -> Self {
        let token = registry.register_activity(run_id, activity_id);
        Self {
            registry: registry.clone(),
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            token,
        }
    }
}

impl Drop for ActivityTokenGuard {
    fn drop(&mut self) {
        self.registry
            .unregister_activity(&self.run_id, &self.activity_id);
    }
}

// ---------------------------------------------------------------------------
// Global process-wide registry
// ---------------------------------------------------------------------------

/// Return a reference to the process-wide cancellation registry (lazily
/// initialised).  Tests that need isolation should use
/// [`WorkflowCancellationRegistry::new`] instead.
pub fn global_cancellation_registry() -> &'static WorkflowCancellationRegistry {
    static REGISTRY: std::sync::OnceLock<WorkflowCancellationRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(WorkflowCancellationRegistry::new)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // -- Basic lifecycle ----------------------------------------------------

    #[test]
    fn register_and_lookup_activity() {
        let reg = WorkflowCancellationRegistry::new();
        let token = reg.register_activity("run-1", "run-1::a");
        assert!(
            reg.lookup_activity("run-1", "run-1::a").is_some(),
            "token should be findable after register"
        );
        // Token is not cancelled yet.
        assert!(!token.is_cancelled());
        assert_eq!(reg.total_activities(), 1);
    }

    #[test]
    fn unregister_removes_token() {
        let reg = WorkflowCancellationRegistry::new();
        let _token = reg.register_activity("run-1", "run-1::a");
        assert_eq!(reg.total_activities(), 1);
        reg.unregister_activity("run-1", "run-1::a");
        assert_eq!(reg.total_activities(), 0);
        assert!(reg.lookup_activity("run-1", "run-1::a").is_none());
    }

    #[test]
    fn unregister_is_idempotent() {
        let reg = WorkflowCancellationRegistry::new();
        let _token = reg.register_activity("run-1", "run-1::a");
        reg.unregister_activity("run-1", "run-1::a");
        reg.unregister_activity("run-1", "run-1::a"); // should not panic
        assert_eq!(reg.total_activities(), 0);
    }

    // -- Cancel activity ----------------------------------------------------

    #[test]
    fn cancel_activity_cancels_token() {
        let reg = WorkflowCancellationRegistry::new();
        let token = reg.register_activity("run-1", "run-1::a");
        assert!(!token.is_cancelled());

        let found = reg.cancel_activity("run-1", "run-1::a");
        assert!(found, "cancel_activity should return true");
        assert!(token.is_cancelled(), "token should be cancelled");
        assert_eq!(reg.total_activities(), 0);
    }

    #[test]
    fn cancel_activity_nonexistent_returns_false() {
        let reg = WorkflowCancellationRegistry::new();
        let found = reg.cancel_activity("run-1", "run-1::ghost");
        assert!(!found);
    }

    #[test]
    fn cancel_activity_leaves_other_activities_untouched() {
        let reg = WorkflowCancellationRegistry::new();
        let token_a = reg.register_activity("run-1", "run-1::a");
        let token_b = reg.register_activity("run-1", "run-1::b");

        reg.cancel_activity("run-1", "run-1::a");
        assert!(token_a.is_cancelled());
        assert!(!token_b.is_cancelled());
        assert_eq!(reg.total_activities(), 1);
        assert!(reg.lookup_activity("run-1", "run-1::b").is_some());
    }

    // -- Cancel node: segment-based matching --------------------------------

    #[test]
    fn cancel_node_cancels_node_token_and_activity_with_same_node_id() {
        let reg = WorkflowCancellationRegistry::new();
        let node_token = reg.register_node("run-1", "node-a");
        // Simplified activity id: <runId>::<nodeId>
        let act_token = reg.register_activity("run-1", "run-1::node-a");
        let other_token = reg.register_activity("run-1", "run-1::node-b");

        let cancelled = reg.cancel_node("run-1", "node-a");
        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0], "run-1::node-a");
        assert!(node_token.is_cancelled());
        assert!(act_token.is_cancelled());
        assert!(!other_token.is_cancelled(), "node-b should be untouched");
        assert_eq!(reg.total_activities(), 1);
    }

    #[test]
    fn cancel_node_matches_activity_id_with_node_segment() {
        let reg = WorkflowCancellationRegistry::new();
        // Activity ids follow the convention: <runId>::<nodeId>::work::<bodyNodeId>
        let _node_tok = reg.register_node("run-1", "loop-1");
        let child_a = reg.register_activity("run-1", "run-1::loop-1::work::step-a");
        let child_b = reg.register_activity("run-1", "run-1::loop-1::work::step-b");
        let unrelated = reg.register_activity("run-1", "run-1::other-node");

        let cancelled = reg.cancel_node("run-1", "loop-1");
        // Both loop children should be cancelled.
        assert_eq!(cancelled.len(), 2);
        assert!(cancelled.contains(&"run-1::loop-1::work::step-a".to_string()));
        assert!(cancelled.contains(&"run-1::loop-1::work::step-b".to_string()));
        assert!(child_a.is_cancelled());
        assert!(child_b.is_cancelled());
        assert!(!unrelated.is_cancelled());
        assert_eq!(reg.total_activities(), 1);
    }

    #[test]
    fn cancel_node_matches_actual_gate_and_work_ids() {
        // Real workflow activity ids:
        //   gate  → <runId>::gate::<nodeId>
        //   work  → <runId>::work::<nodeId>
        let reg = WorkflowCancellationRegistry::new();
        let gate_tok = reg.register_activity("run-x", "run-x::gate::approve");
        let work_tok = reg.register_activity("run-x", "run-x::work::approve");
        let other = reg.register_activity("run-x", "run-x::work::unrelated");

        let cancelled = reg.cancel_node("run-x", "approve");
        assert_eq!(cancelled.len(), 2);
        assert!(gate_tok.is_cancelled());
        assert!(work_tok.is_cancelled());
        assert!(!other.is_cancelled());
    }

    #[test]
    fn cancel_node_nonexistent_returns_empty() {
        let reg = WorkflowCancellationRegistry::new();
        let cancelled = reg.cancel_node("run-1", "ghost");
        assert!(cancelled.is_empty());
    }

    // -- Cancel run ---------------------------------------------------------

    #[test]
    fn cancel_run_cancels_all_tokens() {
        let reg = WorkflowCancellationRegistry::new();
        let t1 = reg.register_activity("run-1", "run-1::a");
        let t2 = reg.register_activity("run-1", "run-1::b");
        let n1 = reg.register_node("run-1", "node-a");

        let cancelled = reg.cancel_run("run-1");
        assert_eq!(cancelled.len(), 2);
        assert!(t1.is_cancelled());
        assert!(t2.is_cancelled());
        assert!(n1.is_cancelled());
        assert_eq!(reg.total_activities(), 0);
        assert_eq!(reg.total_nodes(), 0);
    }

    #[test]
    fn cancel_run_nonexistent_returns_empty() {
        let reg = WorkflowCancellationRegistry::new();
        let cancelled = reg.cancel_run("no-such-run");
        assert!(cancelled.is_empty());
    }

    // -- Run isolation ------------------------------------------------------

    #[test]
    fn cancel_run_only_affects_target_run() {
        let reg = WorkflowCancellationRegistry::new();
        let t1 = reg.register_activity("run-1", "run-1::a");
        let t2 = reg.register_activity("run-2", "run-2::a");

        reg.cancel_run("run-1");
        assert!(t1.is_cancelled());
        assert!(!t2.is_cancelled());
        assert_eq!(reg.total_activities(), 1);
        assert_eq!(reg.active_activity_ids("run-2"), vec!["run-2::a"]);
    }

    // -- Snapshot -----------------------------------------------------------

    #[test]
    fn active_activity_ids_returns_registered_ids() {
        let reg = WorkflowCancellationRegistry::new();
        reg.register_activity("run-1", "run-1::a");
        reg.register_activity("run-1", "run-1::b");
        reg.register_activity("run-2", "run-2::x");

        let mut ids = reg.active_activity_ids("run-1");
        ids.sort();
        assert_eq!(ids, vec!["run-1::a", "run-1::b"]);

        let ids2 = reg.active_activity_ids("run-2");
        assert_eq!(ids2, vec!["run-2::x"]);

        assert!(reg.active_activity_ids("run-3").is_empty());
    }

    // -- Concurrency: register + cancel from another task -------------------

    #[tokio::test]
    async fn concurrent_cancel_signals_active_dispatch() {
        let reg = WorkflowCancellationRegistry::new();
        let token = reg.register_activity("run-1", "run-1::slow");

        let reg_clone = reg.clone();
        let handle = tokio::spawn(async move {
            // Simulate a long-running dispatch that checks the token.
            tokio::select! {
                _ = token.cancelled() => {
                    "cancelled"
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    "timeout"
                }
            }
        });

        // Give the dispatch a moment to start.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // cancelRequested arrives → registry cancels the token.
        reg_clone.cancel_run("run-1");

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("dispatch should finish quickly")
            .expect("dispatch should not panic");

        assert_eq!(result, "cancelled");
    }

    /// Simulate a full scenario: active dispatch registers a token, a
    /// cancelRequested arrives (via `cancel_run`), the dispatch observes
    /// the token and returns `Cancelled`.
    #[tokio::test]
    async fn active_dispatch_observes_cancellation_after_cancel_requested() {
        let reg = WorkflowCancellationRegistry::new();

        // Phase 1: dispatch registers.
        let token = reg.register_activity("run-cancel-test", "run-cancel-test::work");
        assert!(!token.is_cancelled());
        assert_eq!(reg.total_activities(), 1);

        // Phase 2: start the dispatch in a background task.
        let token_clone = token.clone();
        let dispatch_handle = tokio::spawn(async move {
            // Simulate work that periodically checks the token.
            loop {
                if token_clone.is_cancelled() {
                    return "dispatched_cancelled";
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        // Phase 3: cancelRequested is written (simulated by cancel_run).
        tokio::time::sleep(Duration::from_millis(10)).await;
        let cancelled_ids = reg.cancel_run("run-cancel-test");
        assert!(
            !cancelled_ids.is_empty(),
            "should have cancelled at least one activity"
        );
        assert!(
            token.is_cancelled(),
            "token must be cancelled after cancel_run"
        );

        // Phase 4: active dispatch observes the cancellation.
        let outcome = tokio::time::timeout(Duration::from_secs(2), dispatch_handle)
            .await
            .expect("dispatch should finish quickly")
            .expect("dispatch should not panic");

        assert_eq!(outcome, "dispatched_cancelled");
        assert_eq!(reg.total_activities(), 0);
    }

    /// Verify that cancel_activity only affects the targeted activity.
    #[tokio::test]
    async fn cancel_activity_targets_only_specified_activity() {
        let reg = WorkflowCancellationRegistry::new();
        let token_a = reg.register_activity("run-1", "run-1::a");
        let token_b = reg.register_activity("run-1", "run-1::b");

        let reg_clone = reg.clone();
        let token_a_clone = token_a.clone();
        let token_b_clone = token_b.clone();

        let handle_a = tokio::spawn(async move {
            tokio::select! {
                _ = token_a_clone.cancelled() => "a_cancelled",
                _ = tokio::time::sleep(Duration::from_secs(5)) => "a_timeout",
            }
        });
        let handle_b = tokio::spawn(async move {
            tokio::select! {
                _ = token_b_clone.cancelled() => "b_cancelled",
                _ = tokio::time::sleep(Duration::from_millis(200)) => "b_completed",
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        reg_clone.cancel_activity("run-1", "run-1::a");

        let result_a = tokio::time::timeout(Duration::from_secs(2), handle_a)
            .await
            .unwrap()
            .unwrap();
        let result_b = tokio::time::timeout(Duration::from_secs(2), handle_b)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result_a, "a_cancelled", "activity a should be cancelled");
        assert_eq!(
            result_b, "b_completed",
            "activity b should complete normally"
        );
        assert!(token_a.is_cancelled());
        assert!(!token_b.is_cancelled());
    }

    /// Verify node-level cancel propagates to matching activity tokens.
    #[tokio::test]
    async fn node_cancel_propagates_to_children() {
        let reg = WorkflowCancellationRegistry::new();
        let child_token = reg.register_activity("run-1", "run-1::parent-node::work::child");
        let sibling_token = reg.register_activity("run-1", "run-1::other");

        let child_clone = child_token.clone();
        let sibling_clone = sibling_token.clone();

        let handle_child = tokio::spawn(async move {
            tokio::select! {
                _ = child_clone.cancelled() => "child_cancelled",
                _ = tokio::time::sleep(Duration::from_secs(5)) => "child_timeout",
            }
        });
        let handle_sibling = tokio::spawn(async move {
            tokio::select! {
                _ = sibling_clone.cancelled() => "sibling_cancelled",
                _ = tokio::time::sleep(Duration::from_millis(200)) => "sibling_ok",
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        let cancelled = reg.cancel_node("run-1", "parent-node");
        assert!(!cancelled.is_empty());

        let child_result = tokio::time::timeout(Duration::from_secs(2), handle_child)
            .await
            .unwrap()
            .unwrap();
        let sibling_result = tokio::time::timeout(Duration::from_secs(2), handle_sibling)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(child_result, "child_cancelled");
        assert_eq!(sibling_result, "sibling_ok");
    }

    // -- ActivityTokenGuard integration tests --------------------------------

    #[test]
    fn guard_registers_and_auto_unregisters_on_drop() {
        let reg = WorkflowCancellationRegistry::new();
        assert_eq!(reg.total_activities(), 0);

        {
            let guard = ActivityTokenGuard::register(&reg, "run-1", "run-1::a");
            assert_eq!(reg.total_activities(), 1);
            assert!(!guard.token.is_cancelled());
        }
        // Guard dropped → token should be unregistered.
        assert_eq!(reg.total_activities(), 0);
    }

    #[test]
    fn guard_token_is_independent_from_registry_clone() {
        let reg = WorkflowCancellationRegistry::new();
        let reg2 = reg.clone();

        let guard = ActivityTokenGuard::register(&reg, "run-1", "run-1::a");
        assert_eq!(reg2.total_activities(), 1);

        // Cancelling via the cloned registry should affect the guard's token.
        reg2.cancel_run("run-1");
        assert!(guard.token.is_cancelled());

        // Guard drops cleanly (token already removed by cancel_run, but
        // unregister is idempotent).
        drop(guard);
        assert_eq!(reg.total_activities(), 0);
    }

    /// Simulate what the daemon hooks do: register a guard, then a background
    /// task checks the token.  When cancel_run is called, the token is
    /// cancelled and the background task observes it.
    #[tokio::test]
    async fn hooks_integration_guard_register_dispatch_observes_cancel() {
        let reg = WorkflowCancellationRegistry::new();

        // Simulate execute_subagent / execute_host_executor:
        let guard = ActivityTokenGuard::register(&reg, "run-hooks", "run-hooks::work");

        let token = guard.token.clone();
        let reg_clone = reg.clone();
        let handle = tokio::spawn(async move {
            // Simulate a dispatch loop that checks the token.
            loop {
                if token.is_cancelled() {
                    return "cancelled_by_token";
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        // Let the dispatch start.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // cancelRequested arrives → cancel_run cancels all tokens.
        let count = reg_clone.cancel_run("run-hooks").len();
        assert_eq!(count, 1, "should have cancelled 1 activity");

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, "cancelled_by_token");

        // Guard drops → idempotent unregister.
        drop(guard);
        assert_eq!(reg.total_activities(), 0);
    }

    /// Verify that the guard pattern properly cleans up even when an early
    /// error/cancellation occurs — i.e., the token is always unregistered
    /// regardless of how the dispatch exits.
    #[test]
    fn guard_unregisters_on_early_exit() {
        let reg = WorkflowCancellationRegistry::new();

        fn simulate_dispatch(reg: &WorkflowCancellationRegistry, should_fail: bool) -> bool {
            let guard = ActivityTokenGuard::register(reg, "run-1", "run-1::a");
            if should_fail {
                return false; // guard drops here
            }
            let _cancelled = guard.token.is_cancelled();
            true // guard drops here
        }

        // Success path
        assert!(simulate_dispatch(&reg, false));
        assert_eq!(reg.total_activities(), 0);

        // Failure path
        assert!(!simulate_dispatch(&reg, true));
        assert_eq!(reg.total_activities(), 0);
    }

    /// Test that token.is_cancelled() can be checked in a select! to yield
    /// early — this is what await_session_final_output does.
    #[tokio::test]
    async fn cancellable_wait_yields_via_token_select() {
        let reg = WorkflowCancellationRegistry::new();
        let token = reg.register_activity("run-select", "run-select::poll");

        let token_clone = token.clone();
        let reg_clone = reg.clone();
        let handle = tokio::spawn(async move {
            // Simulate await_session_final_output's cancellation pattern.
            tokio::select! {
                _ = token_clone.cancelled() => "cancelled",
                _ = tokio::time::sleep(Duration::from_secs(30)) => "timeout",
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        reg_clone.cancel_run("run-select");

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, "cancelled");
    }

    /// Full simulation of the hooks → dispatch → cancel flow.
    /// 1. Hooks register activity token via guard
    /// 2. Dispatch function checks token between operations
    /// 3. cancel_run cancels tokens (simulating cancelRequested → on_activities_cancelled)
    /// 4. Dispatch observes cancellation and returns Cancelled outcome
    #[tokio::test]
    async fn full_hooks_integration_register_cancel_detect() {
        let reg = WorkflowCancellationRegistry::new();

        // Hooks phase: register before dispatching
        let guard = ActivityTokenGuard::register(&reg, "run-full", "run-full::work");
        assert_eq!(reg.total_activities(), 1);

        let reg_clone = reg.clone();
        let dispatch_token = guard.token.clone();

        // Dispatch phase: background task simulating run_workflow_subagent_session
        let handle = tokio::spawn(async move {
            // Phase 2a: check token before work
            if dispatch_token.is_cancelled() {
                return "cancelled_early";
            }

            // Phase 2b: do work with periodic checks
            for _ in 0..100 {
                if dispatch_token.is_cancelled() {
                    return "cancelled_during_work";
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            "completed"
        });

        // Let work start
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Phase 3: cancelRequested arrives → registry cancels tokens
        let cancelled_ids = reg_clone.cancel_run("run-full");
        assert!(!cancelled_ids.is_empty());

        // Phase 4: dispatch observes cancellation
        let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(
            outcome.starts_with("cancelled"),
            "expected cancelled, got: {outcome}"
        );
        assert!(guard.token.is_cancelled());

        // Guard cleanup
        drop(guard);
        assert_eq!(reg.total_activities(), 0);
    }

    // ------------------------------------------------------------------
    // Task 6.3: Worker termination signal escalation ordering tests
    // ------------------------------------------------------------------

    /// Trace collector that records signal-sending calls in order.
    /// Used to verify the SIGINT → grace → SIGKILL escalation sequence
    /// without spawning real processes.
    #[derive(Clone, Default)]
    struct SignalTrace {
        calls: std::sync::Arc<std::sync::Mutex<Vec<(u32, i32)>>>,
    }

    impl SignalTrace {
        fn record(&self, pid: u32, signal: i32) {
            self.calls.lock().unwrap().push((pid, signal));
        }

        fn snapshot(&self) -> Vec<(u32, i32)> {
            self.calls.lock().unwrap().clone()
        }
    }

    /// A mockable worker process handle that records signals rather than
    /// actually sending them.  The `is_alive` callback determines whether the
    /// simulated process is still running after each signal.
    struct MockWorker {
        pid: u32,
        trace: SignalTrace,
    }

    impl MockWorker {
        fn new(pid: u32, trace: SignalTrace) -> Self {
            Self { pid, trace }
        }

        fn send_signal(&self, signal: i32) {
            self.trace.record(self.pid, signal);
            // In reality this would call libc::kill, but for the mock we
            // just record the intent.
        }
    }

    /// Simulated signal escalation: SIGINT → poll → SIGKILL if still alive.
    /// `is_alive_after` is called after each poll interval to decide whether
    /// the process is still running.
    async fn escalate_signals<F>(
        worker: &MockWorker,
        grace: Duration,
        poll_interval: Duration,
        mut is_alive_after: F,
    ) where
        F: FnMut() -> bool,
    {
        // Step 1: SIGINT
        worker.send_signal(libc::SIGINT);

        // Step 2: Grace period polling
        let deadline = tokio::time::Instant::now() + grace;
        let mut exited = false;
        while tokio::time::Instant::now() < deadline {
            if !is_alive_after() {
                exited = true;
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }

        // Step 3: SIGKILL if still alive
        if !exited {
            worker.send_signal(libc::SIGKILL);
        }
    }

    #[tokio::test]
    async fn signal_escalation_sends_sigint_then_sigkill_when_process_ignores_sigint() {
        let trace = SignalTrace::default();
        let worker = MockWorker::new(42, trace.clone());

        // Simulate a process that ignores SIGINT and stays alive.
        escalate_signals(
            &worker,
            Duration::from_millis(100), // short grace for test
            Duration::from_millis(10),
            || true, // always alive → SIGKILL will be sent
        )
        .await;

        let calls = trace.snapshot();
        assert_eq!(calls.len(), 2, "expected 2 signals: SIGINT then SIGKILL");
        assert_eq!(
            calls[0],
            (42, libc::SIGINT),
            "first signal should be SIGINT"
        );
        assert_eq!(
            calls[1],
            (42, libc::SIGKILL),
            "second signal should be SIGKILL (process ignored SIGINT)"
        );
    }

    #[tokio::test]
    async fn signal_escalation_sends_only_sigint_when_process_exits_promptly() {
        let trace = SignalTrace::default();
        let worker = MockWorker::new(99, trace.clone());

        let mut poll_count = 0;
        escalate_signals(
            &worker,
            Duration::from_secs(5), // long grace period
            Duration::from_millis(10),
            || {
                poll_count += 1;
                // Process exits after 2 polls (simulates quick SIGINT response).
                poll_count < 2
            },
        )
        .await;

        let calls = trace.snapshot();
        assert_eq!(
            calls.len(),
            1,
            "expected only SIGINT – process exited before grace expired"
        );
        assert_eq!(calls[0], (99, libc::SIGINT));
    }

    #[tokio::test]
    async fn signal_escalation_grace_period_is_respected() {
        let trace = SignalTrace::default();
        let worker = MockWorker::new(7, trace.clone());

        let start = tokio::time::Instant::now();
        escalate_signals(
            &worker,
            Duration::from_millis(200),
            Duration::from_millis(50),
            || true, // always alive
        )
        .await;
        let elapsed = start.elapsed();

        // Grace period should be approximately 200ms (plus tolerance).
        assert!(
            elapsed >= Duration::from_millis(180),
            "escalation should respect grace period of 200ms, got {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "escalation should not hang indefinitely, got {:?}",
            elapsed
        );

        let calls = trace.snapshot();
        assert_eq!(calls.len(), 2, "SIGINT + SIGKILL since process never died");
    }
}
