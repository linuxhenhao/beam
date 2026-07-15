//! Tests for the `reconcile_activity` decision tree and related outcomes.

use std::collections::BTreeMap;

use beam_core::{
    BootstrapWorkflowRunInput, CreateTaskInput, EventDraft, EventLog, ParsedSchedule,
    ParsedScheduleKind, RunChatBinding, WorkflowActor, bootstrap_workflow_run, create_task,
};
use serde_json::Value;

use super::providers::{BeamScheduleReconciler, FeishuImReconciler};
use super::registry::{ProviderReconciler, default_reconciler_registry};
use super::test_helpers::{make_state, temp_paths};

// -----------------------------------------------------------------------
// read_only_lookup tests (schedule)
// -----------------------------------------------------------------------

#[tokio::test]
async fn beam_schedule_read_only_lookup_finds_existing_task() {
    let paths = temp_paths("schedule-lookup-found");
    let _ = std::fs::remove_dir_all(paths.root());
    let run_id = "run-sched-found";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();
    // Create a task with a known idempotency key
    create_task(
        &paths,
        CreateTaskInput {
            id: Some("test-key".to_string()),
            name: "demo".to_string(),
            schedule: "0 9 * * *".to_string(),
            parsed: ParsedSchedule {
                kind: ParsedScheduleKind::Cron,
                run_at: None,
                minutes: None,
                expr: Some("0 9 * * *".to_string()),
                display: "0 9 * * *".to_string(),
            },
            prompt: "demo".to_string(),
            working_dir: "/tmp".to_string(),
            chat_id: "oc_".to_string(),
            root_message_id: None,
            scope: Some("thread".to_string()),
            chat_type: None,
            lark_app_id: None,
            creator_chat_id: None,
            creator_root_message_id: None,
            creator_lark_app_id: None,
            next_run_at: None,
            repeat: None,
            deliver: None,
        },
    )
    .unwrap();

    let state = make_state(&paths);
    let r = BeamScheduleReconciler;
    let result = r
        .read_only_lookup(&state, &paths, "test-key")
        .await
        .expect("read_only_lookup");
    assert!(
        result.is_some(),
        "should find existing task by idempotency key"
    );
    let evidence = result.unwrap();
    assert_eq!(evidence["source"], "getTask");
    assert_eq!(evidence["externalRefs"]["taskId"], "test-key");

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn beam_schedule_read_only_lookup_returns_none_for_missing_task() {
    let paths = temp_paths("schedule-lookup-missing");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let r = BeamScheduleReconciler;
    let result = r
        .read_only_lookup(&state, &paths, "nonexistent-key")
        .await
        .expect("read_only_lookup");
    assert!(result.is_none(), "should return None for non-existent task");
    let _ = std::fs::remove_dir_all(paths.root());
}

// -----------------------------------------------------------------------
// Feishu reconciler trait behaviour
// -----------------------------------------------------------------------

#[tokio::test]
async fn feishu_im_read_only_lookup_always_returns_none() {
    let paths = temp_paths("feishu-readonly");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let r = FeishuImReconciler;
    let result = r
        .read_only_lookup(&state, &paths, "any-key")
        .await
        .expect("read_only_lookup");
    assert!(
        result.is_none(),
        "feishu-im should not support readOnlyLookup"
    );
    let _ = std::fs::remove_dir_all(paths.root());
}

// -----------------------------------------------------------------------
// Transient failure pathway
// -----------------------------------------------------------------------

#[tokio::test]
async fn feishu_idempotent_submit_missing_bot_returns_error() {
    let paths = temp_paths("feishu-missing-bot");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let r = FeishuImReconciler;
    let canonical = serde_json::json!({
        "larkAppId": "nonexistent-app",
        "chatId": "chat-1",
        "content": "hello"
    });
    let err = r.idempotent_submit(&state, &canonical).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("not registered"),
        "should mention bot not registered"
    );
    // Missing bot is NOT retryable
    assert!(!r.is_retryable_error(&err));
    let _ = std::fs::remove_dir_all(paths.root());
}

// -----------------------------------------------------------------------
// End-to-end: schedule reconciliation via registry
// -----------------------------------------------------------------------

#[tokio::test]
async fn reconcile_schedule_dangling_via_registry_finds_task() {
    let paths = temp_paths("registry-schedule-found");
    let _ = std::fs::remove_dir_all(paths.root());

    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
    let run_id = "run-reg-sched";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write events and create the task
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
                    "idempotencyKey": "wf-key-xyz",
                    "inputHash": "sha256:1",
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "beam-schedule",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        create_task(
            &paths,
            CreateTaskInput {
                id: Some("wf-key-xyz".to_string()),
                name: "schedule-demo".to_string(),
                schedule: "0 9 * * *".to_string(),
                parsed: ParsedSchedule {
                    kind: ParsedScheduleKind::Cron,
                    run_at: None,
                    minutes: None,
                    expr: Some("0 9 * * *".to_string()),
                    display: "0 9 * * *".to_string(),
                },
                prompt: "demo".to_string(),
                working_dir: "/tmp".to_string(),
                chat_id: "oc_".to_string(),
                root_message_id: None,
                scope: Some("thread".to_string()),
                chat_type: None,
                lark_app_id: None,
                creator_chat_id: None,
                creator_root_message_id: None,
                creator_lark_app_id: None,
                next_run_at: None,
                repeat: None,
                deliver: None,
            },
        )
        .unwrap();
    }

    let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .expect("snapshot");
    let state = make_state(&paths);
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
    let registry = default_reconciler_registry();

    let result = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &paths.workflow_run_dir(run_id),
        "beam-schedule",
        &snapshot,
    )
    .await
    .expect("reconcile");

    assert_eq!(result.reconciled.len(), 1);
    assert_eq!(result.reconciled[0].decision, "completedByIdempotentSubmit");

    let events = log.read_all().unwrap();
    assert!(
        events.iter().any(|e| e.event_type == "reconcileResult"),
        "should have reconcileResult"
    );
    assert!(
        events.iter().any(|e| e.event_type == "activitySucceeded"),
        "should have activitySucceeded"
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn reconcile_schedule_dangling_via_registry_issues_fresh_retry_when_task_missing() {
    let paths = temp_paths("registry-schedule-freshretry");
    let _ = std::fs::remove_dir_all(paths.root());

    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
    let run_id = "run-reg-sched-fr";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write events but DO NOT create the task – simulate missing effect
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
                    "idempotencyKey": "wf-key-nonexistent",
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
    let state = make_state(&paths);
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
    let registry = default_reconciler_registry();

    let result = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &paths.workflow_run_dir(run_id),
        "beam-schedule",
        &snapshot,
    )
    .await
    .expect("reconcile");

    // Should produce freshRetry (not manual and not reconciled)
    assert_eq!(
        result.fresh_retry.len(),
        1,
        "should have one freshRetry when task doesn't exist"
    );
    assert_eq!(
        result.fresh_retry[0].decision, "freshRetry",
        "decision should be freshRetry"
    );
    assert!(result.reconciled.is_empty(), "no reconciled expected");
    assert!(
        result.transient_failures.is_empty(),
        "no transient failures expected"
    );

    let events = log.read_all().unwrap();
    let reconcile_result = events
        .iter()
        .find(|e| e.event_type == "reconcileResult")
        .expect("should have reconcileResult");
    assert_eq!(
        reconcile_result.payload["decision"], "freshRetry",
        "reconcileResult decision should be freshRetry"
    );
    assert_eq!(
        reconcile_result.payload["capability"], "readOnlyLookup",
        "should use readOnlyLookup capability"
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

// -----------------------------------------------------------------------
// Hash mismatch → manual failure (no provider call)
// -----------------------------------------------------------------------

#[tokio::test]
async fn feishu_im_hash_mismatch_produces_manual_failure_without_provider_call() {
    let paths = temp_paths("feishu-hash-mismatch");
    let _ = std::fs::remove_dir_all(paths.root());
    let run_id = "run-hash-mismatch";

    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"larkAppId":"app-1","chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let run_dir = paths.workflow_run_dir(run_id);

    // Write attemptCreated + effectAttempted with a deliberately wrong inputHash
    {
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": "act-feishu-1",
                    "attemptId": "act-feishu-1::att-1",
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
                    "activityId": "act-feishu-1",
                    "attemptId": "act-feishu-1::att-1",
                    "idempotencyKey": "wf-key-feishu",
                    "inputHash": "deadbeef_wrong_hash_123",
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "feishu-im",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Write a sidecar with valid content (different from what the wrong hash represents)
    let sidecar_dir = run_dir
        .join("attempts")
        .join("act-feishu-1")
        .join("act-feishu-1::att-1");
    std::fs::create_dir_all(&sidecar_dir).unwrap();
    let sidecar_content = serde_json::json!({
        "larkAppId": "app-1",
        "chatId": "chat-1",
        "content": "hello"
    });
    std::fs::write(
        sidecar_dir.join("effect-input.json"),
        serde_json::to_vec_pretty(&sidecar_content).unwrap(),
    )
    .unwrap();

    let snapshot = beam_core::read_run_snapshot(&run_dir)
        .await
        .unwrap()
        .expect("snapshot");
    let state = make_state(&paths);
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
    let registry = default_reconciler_registry();

    let result = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &run_dir,
        "feishu-im",
        &snapshot,
    )
    .await
    .expect("reconcile_provider_dangling_effects");

    // Should produce manual recovery — NOT call the provider
    assert!(
        !result.reconciled.is_empty(),
        "should produce manual recovery"
    );
    let manual = result.reconciled.iter().find(|o| o.decision == "manual");
    assert!(
        manual.is_some(),
        "should have manual decision due to hash mismatch"
    );

    // Verify the EventLog has reconcileResult with hashMismatch evidence
    let events = log.read_all().unwrap();
    let reconcile_result = events
        .iter()
        .find(|e| e.event_type == "reconcileResult")
        .expect("should have reconcileResult");
    assert_eq!(
        reconcile_result.payload["decision"], "manual",
        "decision should be manual"
    );
    assert_eq!(
        reconcile_result.payload["evidence"]["source"], "effectInputSidecar",
        "evidence source should be effectInputSidecar"
    );
    assert_eq!(
        reconcile_result.payload["evidence"]["returned"], "hashMismatch",
        "evidence should indicate hashMismatch"
    );
    assert!(
        reconcile_result.payload["evidence"]["expectedHash"]
            .as_str()
            .unwrap()
            .contains("deadbeef"),
        "expectedHash should be the wrong hash from effectAttempted"
    );

    let activity_failed = events
        .iter()
        .find(|e| e.event_type == "activityFailed")
        .expect("should have activityFailed");
    assert_eq!(
        activity_failed.payload["error"]["errorCode"],
        "EffectInputHashMismatch"
    );
    assert_eq!(
        activity_failed.payload["error"]["errorClass"], "manual",
        "errorClass should be manual"
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn feishu_im_hash_match_falls_through_to_idempotent_submit_not_hash_mismatch() {
    // Verify that when the hash MATCHES, the code falls through to
    // idempotentSubmit (which fails because bot is missing — but the error
    // should be "bot not registered", NOT "hash mismatch").
    let paths = temp_paths("feishu-hash-match");
    let _ = std::fs::remove_dir_all(paths.root());
    let run_id = "run-hash-match";

    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"larkAppId":"app-nonexistent","chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let run_dir = paths.workflow_run_dir(run_id);

    // Compute the matching hash so the hash check passes.
    let sidecar_content = serde_json::json!({
        "larkAppId": "app-nonexistent",
        "chatId": "chat-1",
        "content": "hello"
    });
    let r = FeishuImReconciler;
    let canonical = r
        .canonical_input(&sidecar_content)
        .expect("canonical_input");
    let canonical_bytes = serde_json::to_vec(&canonical).unwrap();
    let correct_hash = crate::sha256_hex(&canonical_bytes);

    // Write attemptCreated + effectAttempted with the CORRECT hash
    {
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": "act-feishu-2",
                    "attemptId": "act-feishu-2::att-1",
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
                    "activityId": "act-feishu-2",
                    "attemptId": "act-feishu-2::att-1",
                    "idempotencyKey": "wf-key-feishu-2",
                    "inputHash": &correct_hash,
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "feishu-im",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Write the sidecar
    let sidecar_dir = run_dir
        .join("attempts")
        .join("act-feishu-2")
        .join("act-feishu-2::att-1");
    std::fs::create_dir_all(&sidecar_dir).unwrap();
    std::fs::write(
        sidecar_dir.join("effect-input.json"),
        serde_json::to_vec_pretty(&sidecar_content).unwrap(),
    )
    .unwrap();

    let snapshot = beam_core::read_run_snapshot(&run_dir)
        .await
        .unwrap()
        .expect("snapshot");
    let state = make_state(&paths);
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
    let registry = default_reconciler_registry();

    let result = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &run_dir,
        "feishu-im",
        &snapshot,
    )
    .await
    .expect("reconcile_provider_dangling_effects");

    // Should produce manual recovery because bot is missing, NOT because of hash mismatch
    let manual = result.reconciled.iter().find(|o| o.decision == "manual");
    assert!(
        manual.is_some(),
        "should have manual decision (bot missing, not hash mismatch)"
    );

    // Verify the events do NOT contain hashMismatch
    let events = log.read_all().unwrap();
    let has_hash_mismatch = events.iter().any(|e| {
        e.event_type == "reconcileResult"
            && e.payload
                .get("evidence")
                .and_then(|v| v.get("returned"))
                .and_then(|v| v.as_str())
                == Some("hashMismatch")
    });
    assert!(
        !has_hash_mismatch,
        "should NOT have hashMismatch when hash matches"
    );

    // Verify activityFailed has bot-related error (not EffectInputHashMismatch)
    let activity_failed = events
        .iter()
        .find(|e| e.event_type == "activityFailed")
        .expect("should have activityFailed");
    let error_code = activity_failed.payload["error"]["errorCode"]
        .as_str()
        .unwrap_or("");
    assert!(
        error_code != "EffectInputHashMismatch",
        "error should NOT be EffectInputHashMismatch, got: {error_code}"
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn prior_fresh_retry_does_not_write_new_events_on_second_reconciliation() {
    let paths = temp_paths("prior-freshretry-noprogress");
    let _ = std::fs::remove_dir_all(paths.root());

    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
    let run_id = "run-prior-fr";
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-a"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write dangling effectAttempted (task does NOT exist → freshRetry).
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
                    "idempotencyKey": "wf-key-nonexistent",
                    "inputHash": "sha256:1",
                    "idempotencyTtlMs": 9999999u64,
                    "provider": "beam-schedule",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let state = make_state(&paths);
    let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
    let registry = default_reconciler_registry();

    // --- First reconciliation: should write reconcileResult{decision=freshRetry} ---
    let snap1 = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .expect("snapshot 1");
    let result1 = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &paths.workflow_run_dir(run_id),
        "beam-schedule",
        &snap1,
    )
    .await
    .expect("first reconcile");

    assert_eq!(
        result1.fresh_retry.len(),
        1,
        "first call: should have freshRetry"
    );
    let events_after_first = log.read_all().unwrap();
    let count_after_first = events_after_first.len();
    let has_reconcile_result = events_after_first
        .iter()
        .any(|e| e.event_type == "reconcileResult");
    assert!(
        has_reconcile_result,
        "first call should write reconcileResult"
    );

    // --- Second reconciliation: prior freshRetry exists, must NOT write new events ---
    let snap2 = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .expect("snapshot 2");
    let _result2 = super::missing_provider::reconcile_provider_dangling_effects(
        &registry,
        &state,
        &mut log,
        &paths.workflow_run_dir(run_id),
        "beam-schedule",
        &snap2,
    )
    .await
    .expect("second reconcile");

    let events_after_second = log.read_all().unwrap();
    assert_eq!(
        events_after_second.len(),
        count_after_first,
        "second reconciliation must NOT write new events when prior freshRetry exists; \
         before={count_after_first} after={}",
        events_after_second.len()
    );

    let _ = std::fs::remove_dir_all(paths.root());
}
