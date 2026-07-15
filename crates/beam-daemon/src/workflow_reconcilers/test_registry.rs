//! Tests for the reconciler registry itself.

use super::registry::{
    ProviderReconciler, ProviderReconcilerRegistry, default_reconciler_registry,
    global_reconciler_registry,
};

/// Custom reconciler for testing the registry API.
struct TestReconciler;

#[async_trait::async_trait]
impl ProviderReconciler for TestReconciler {
    fn provider_name(&self) -> &str {
        "test-provider"
    }
    fn requires_effect_input(&self) -> bool {
        false
    }
    fn is_retryable_error(&self, _err: &anyhow::Error) -> bool {
        false
    }
}

// -----------------------------------------------------------------------
// Registry tests
// -----------------------------------------------------------------------

#[test]
fn registry_contains_beam_schedule_and_feishu_im() {
    let reg = default_reconciler_registry();
    let schedule = reg.get("beam-schedule");
    assert!(
        schedule.is_some(),
        "registry should contain beam-schedule reconciler"
    );
    let feishu = reg.get("feishu-im");
    assert!(
        feishu.is_some(),
        "registry should contain feishu-im reconciler"
    );
}

#[test]
fn registry_unknown_provider_returns_none() {
    let reg = default_reconciler_registry();
    assert!(reg.get("nonexistent-provider").is_none());
}

#[test]
fn registry_providers_iterates_names() {
    let reg = default_reconciler_registry();
    let names: Vec<&str> = reg.providers().collect();
    assert!(names.contains(&"beam-schedule"));
    assert!(names.contains(&"feishu-im"));
}

#[test]
fn registry_register_and_get_custom() {
    let mut reg = ProviderReconcilerRegistry::new();
    reg.register(Box::new(TestReconciler));
    assert!(reg.get("test-provider").is_some());
    assert_eq!(
        reg.get("test-provider").unwrap().provider_name(),
        "test-provider"
    );
}

#[test]
fn global_registry_is_singleton() {
    let r1 = global_reconciler_registry();
    let r2 = global_reconciler_registry();
    let p1 = r1 as *const _;
    let p2 = r2 as *const _;
    assert_eq!(
        p1, p2,
        "global reconciler registry should be the same instance"
    );
}
