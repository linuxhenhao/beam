//! External delivery: RAII guard and global registry.
//!
//! Provides [`ActivityTokenGuard`] for RAII-style token lifecycle and
//! [`global_cancellation_registry`] for process-wide access.

use tokio_util::sync::CancellationToken;

use super::registry::WorkflowCancellationRegistry;

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
