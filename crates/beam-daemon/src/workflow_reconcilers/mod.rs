//! Provider reconciler trait, registry, built-in implementations,
//! reconciliation decision logic, and missing-provider handling.
//!
//! ## Submodule layout
//! - [`registry`]: `ProviderReconciler` trait + `ProviderReconcilerRegistry`.
//! - [`providers`]: `BeamScheduleReconciler` + `FeishuImReconciler`.
//! - [`reconcile`]: `reconcile_activity` decision tree + result types.
//! - [`missing_provider`]: per-provider batch reconciliation + unregistered-provider catch.

mod missing_provider;
mod providers;
mod reconcile;
mod registry;

// Re-export public API surfaces so that external callers continue to use
// `workflow_reconcilers::Item` paths unchanged.
pub use missing_provider::{
    handle_missing_provider_dangling_effects, reconcile_provider_dangling_effects,
};
pub use reconcile::{ProviderResumeResult, ReconcilerRegistryCheckResult};
#[allow(unused_imports)]
pub use registry::{
    default_reconciler_registry, global_reconciler_registry, ProviderReconciler,
    ProviderReconcilerRegistry,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
// External test modules (each <800 lines) that exercise registry, providers,
// reconciliation logic, and missing-provider handling separately.

#[cfg(test)]
mod test_helpers;
#[cfg(test)]
mod test_missing_provider;
#[cfg(test)]
mod test_providers;
#[cfg(test)]
mod test_reconcile;
#[cfg(test)]
mod test_registry;
