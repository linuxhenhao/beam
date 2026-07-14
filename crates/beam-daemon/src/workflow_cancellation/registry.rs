//! Pure state and decision logic for workflow cancellation.
//!
//! Provides a thread-safe registry of [`CancellationToken`]s keyed by
//! workflow run / activity / node.
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
