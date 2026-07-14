//! ProviderReconciler trait and registry for workflow effect reconciliation.
//!
//! ## Error handling convention
//!
//! - Missing reconciler → `manual` recovery (provider is unknown).
//! - Missing effect input when required → `manual` failure.
//! - Input hash mismatch → `manual` failure (no provider call).
//! - Retryable provider errors → transient failure (effect stays dangling).
//! - Non-retryable provider errors → `manual` failure.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use beam_core::BeamPaths;
use serde_json::Value;

use crate::AppState;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A reconciler that recovers dangling effects for a specific provider.
///
/// All trait methods are now exercised through the unified
/// `reconcile_activity` / `reconcile_provider_dangling_effects` path
/// (Task 3.2 merged resume decision tree).
#[async_trait]
pub trait ProviderReconciler: Send + Sync {
    /// The provider name this reconciler handles (e.g. `"beam-schedule"`, `"feishu-im"`).
    fn provider_name(&self) -> &str;

    /// Whether this reconciler needs the effect-input sidecar file
    /// (written before the original `effectAttempted`) to perform reconciliation.
    fn requires_effect_input(&self) -> bool;

    /// Convert raw sidecar input into a canonical (deterministic) representation
    /// suitable for idempotent re-submission.
    ///
    /// The default implementation returns the raw input unchanged.
    fn canonical_input(&self, raw_input: &Value) -> Result<Value> {
        Ok(raw_input.clone())
    }

    /// Read-only lookup: check whether the effect already exists on the
    /// provider side.
    ///
    /// Returns `Some(evidence)` if the effect was already completed,
    /// `None` if there is no record of it.
    ///
    /// The default implementation returns `None` (read-only lookup not supported).
    #[allow(unused_variables)]
    async fn read_only_lookup(
        &self,
        state: &AppState,
        paths: &BeamPaths,
        idempotency_key: &str,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    /// Idempotent submit: re-submit the effect to the provider using the
    /// canonical input.
    ///
    /// Returns `Ok(evidence)` on success (e.g. `{"messageId":"…"}`).
    ///
    /// The default implementation returns an error (idempotent submit not supported).
    #[allow(unused_variables)]
    async fn idempotent_submit(&self, state: &AppState, canonical_input: &Value) -> Result<Value> {
        anyhow::bail!(
            "idempotentSubmit is not supported for provider '{}'",
            self.provider_name()
        )
    }

    /// Whether an error from this provider is retryable (transient).
    ///
    /// Retryable errors cause the effect to remain dangling so it can be
    /// retried on the next resume cycle. Non-retryable errors result in a
    /// `manual` failure.
    fn is_retryable_error(&self, err: &anyhow::Error) -> bool;

    /// Whether this reconciler supports `readOnlyLookup`.
    ///
    /// If `readOnlyLookup` is supported and returns `None`, and
    /// `supports_idempotent_submit()` is false, the reconciler will issue a
    /// `freshRetry` (instead of falling through to idempotent submit which
    /// would fail).
    fn supports_read_only_lookup(&self) -> bool {
        false
    }

    /// Whether this reconciler supports `idempotentSubmit`.
    fn supports_idempotent_submit(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Registry of provider reconcilers, keyed by provider name.
pub struct ProviderReconcilerRegistry {
    reconcilers: HashMap<String, Box<dyn ProviderReconciler>>,
}

impl ProviderReconcilerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            reconcilers: HashMap::new(),
        }
    }

    /// Register a reconciler.
    pub fn register(&mut self, reconciler: Box<dyn ProviderReconciler>) {
        let name = reconciler.provider_name().to_string();
        self.reconcilers.insert(name, reconciler);
    }

    /// Look up a reconciler by provider name.
    pub fn get(&self, provider: &str) -> Option<&dyn ProviderReconciler> {
        self.reconcilers.get(provider).map(|b| b.as_ref())
    }

    /// Returns an iterator over all registered provider names.
    #[allow(dead_code)]
    pub fn providers(&self) -> impl Iterator<Item = &str> {
        self.reconcilers.keys().map(|k| k.as_str())
    }
}

impl Default for ProviderReconcilerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Registry factory
// ---------------------------------------------------------------------------

/// Build a registry pre-populated with all built-in provider reconcilers.
pub fn default_reconciler_registry() -> ProviderReconcilerRegistry {
    let mut reg = ProviderReconcilerRegistry::new();
    reg.register(Box::new(super::providers::BeamScheduleReconciler));
    reg.register(Box::new(super::providers::FeishuImReconciler));
    reg
}

/// Return a reference to a process-wide default reconciler registry (lazily initialized).
pub fn global_reconciler_registry() -> &'static ProviderReconcilerRegistry {
    static REGISTRY: std::sync::OnceLock<ProviderReconcilerRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(default_reconciler_registry)
}
