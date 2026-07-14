//! Tests for missing-provider handling (no reconciler registered).

use std::collections::BTreeMap;

use beam_core::{
    BootstrapWorkflowRunInput, EventDraft, EventLog, WorkflowActor, bootstrap_workflow_run,
};
use serde_json::Value;

use super::registry::default_reconciler_registry;
use super::test_helpers::{make_state, temp_paths};

// -----------------------------------------------------------------------
// Missing reconciler → manual recovery
// -----------------------------------------------------------------------

#[tokio::test]
async fn missing_reconciler_produces_manual_recovery() {
    let paths = temp_paths("missing-reconciler");
    let _ = std::fs::remove_dir_all(paths.root());
    let run_id = "run-missing";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"unknown-provider","input":{"x":1}}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // Write effectAttempted for a provider that has no reconciler registered
    {
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": "act-1",
                    "attemptId": "act-1::att-1",
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:dummy",
                        "outputPath": "dummy",
                        "outputBytes": 1,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json",
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "effectAttempted".to_string(),
                actor: WorkflowActor::HostExecutor,
                payload: serde_json::json!({
                    "activityId": "act-1",
                    "attemptId": "act-1::att-1",
                    "idempotencyKey": "unknown-key",
                    "inputHash": "sha256:1",
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "unknown-provider",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .expect("snapshot");
    let state = make_state(&paths);
    let registry = default_reconciler_registry();
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();

    let result = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &paths.workflow_run_dir(run_id),
        "unknown-provider",
        &snapshot,
    )
    .await
    .expect("reconcile_provider_dangling_effects");

    // Should have produced manual recovery (not skipped)
    assert!(
        !result.reconciled.is_empty(),
        "should produce manual recovery"
    );
    assert_eq!(result.reconciled[0].decision, "manual");

    // Verify the EventLog has the expected manual recovery events
    let events = log.read_all().unwrap();
    let reconcile_result = events
        .iter()
        .find(|e| e.event_type == "reconcileResult")
        .expect("should have reconcileResult");
    assert_eq!(
        reconcile_result.payload["decision"], "manual",
        "decision should be manual"
    );
    assert!(
        reconcile_result.payload["evidence"]["message"]
            .as_str()
            .unwrap()
            .contains("no reconciler registered"),
        "evidence should mention missing reconciler"
    );
    let activity_failed = events
        .iter()
        .find(|e| e.event_type == "activityFailed")
        .expect("should have activityFailed");
    assert_eq!(
        activity_failed.payload["error"]["errorCode"],
        "UnknownProviderError"
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

// -----------------------------------------------------------------------
// handle_missing_provider_dangling_effects
// -----------------------------------------------------------------------

#[tokio::test]
async fn handle_missing_provider_catches_unregistered_provider() {
    let paths = temp_paths("handle-missing");
    let _ = std::fs::remove_dir_all(paths.root());
    let run_id = "run-handle-missing";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"unknown-provider","input":{"x":1}}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // Write effectAttempted for unknown provider
    {
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": "act-1",
                    "attemptId": "act-1::att-1",
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:dummy",
                        "outputPath": "dummy",
                        "outputBytes": 1,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json",
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "effectAttempted".to_string(),
                actor: WorkflowActor::HostExecutor,
                payload: serde_json::json!({
                    "activityId": "act-1",
                    "attemptId": "act-1::att-1",
                    "idempotencyKey": "unknown-key",
                    "inputHash": "sha256:1",
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "unknown-provider",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .expect("snapshot");
    let registry = default_reconciler_registry();
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();

    let (covered, missing) = super::missing_provider::handle_missing_provider_dangling_effects(
        &registry, &mut log, &snapshot,
    )
    .expect("handle_missing_provider_dangling_effects");

    assert!(
        covered.is_empty(),
        "should have no covered providers for unknown provider"
    );
    assert!(
        missing.contains(&"unknown-provider".to_string()),
        "should list unknown-provider as missing"
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn handle_missing_provider_lists_covered_providers() {
    let paths = temp_paths("handle-covered");
    let _ = std::fs::remove_dir_all(paths.root());
    let run_id = "run-handle-covered";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    {
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": "act-1",
                    "attemptId": "act-1::att-1",
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:dummy",
                        "outputPath": "dummy",
                        "outputBytes": 1,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json",
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "effectAttempted".to_string(),
                actor: WorkflowActor::HostExecutor,
                payload: serde_json::json!({
                    "activityId": "act-1",
                    "attemptId": "act-1::att-1",
                    "idempotencyKey": "test-key",
                    "inputHash": "sha256:1",
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "beam-schedule",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .expect("snapshot");
    let registry = default_reconciler_registry();
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();

    let (covered, missing) = super::missing_provider::handle_missing_provider_dangling_effects(
        &registry, &mut log, &snapshot,
    )
    .expect("handle_missing_provider_dangling_effects");

    assert!(
        covered.contains(&"beam-schedule".to_string()),
        "should list beam-schedule as covered"
    );
    assert!(missing.is_empty(), "should have no missing providers");

    let _ = std::fs::remove_dir_all(paths.root());
}
