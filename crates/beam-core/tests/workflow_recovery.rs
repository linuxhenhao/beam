// Phase 0: workflow current-behavior regression tests — recovery/effect scenarios.
// These tests document current behavior. Do not change behavior here;
// only add tests that express what the runtime does today.

mod support;
use support::*;

use std::collections::BTreeMap;
use std::fs;

use beam_core::{
    BeamPaths, EventLog, RunChatBinding, RunLoopStopReason, RunStatus, WorkflowNode,
    WorkflowRuntimeContext, bootstrap_workflow_run, read_run_snapshot, run_loop,
    workflow_definition::{HostExecutorNode, NodeBase, WorkflowDefinition},
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Phase 2.2: effectAttempted tests
// ---------------------------------------------------------------------------

/// Verify that hostExecutor dispatch writes `effectAttempted` into the
/// EventLog **before** the external provider hook is called.
#[tokio::test]
async fn host_executor_dispatches_effect_attempted_before_hook_call() {
    let run_dir = temp_run_dir("effat-before");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-effat-before";
    let params = BTreeMap::new();

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{
                "workflowId":"effat-before",
                "version":1,
                "nodes":{
                    "a":{"type":"hostExecutor","executor":"custom-tool","input":{"payload":"hello"}}
                }
            }"#,
            expected_workflow_id: Some("effat-before"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "effat-before".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    executor: "custom-tool".to_string(),
                    input: json!({"payload": "hello"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = SpyHooks::new();
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
    assert!(
        matches!(result.reason, RunLoopStopReason::Terminal),
        "expected Terminal, got {:?}",
        result.reason
    );
    assert_eq!(result.last_snapshot.run.status, RunStatus::Succeeded);

    // Verify the hook was actually called (so the test is meaningful)
    assert!(
        *hooks.prepare_called.lock().unwrap(),
        "prepare_host_executor must have been called"
    );
    assert!(
        *hooks.execute_called.lock().unwrap(),
        "execute_host_executor must have been called"
    );

    // Now verify event ordering: effectAttempted must exist and appear before
    // activitySucceeded in the event log.
    let events = rt.log.read_all().unwrap();
    let eff_at_idx = events
        .iter()
        .position(|e| e.event_type == "effectAttempted");
    let activity_succeeded_idx = events
        .iter()
        .position(|e| e.event_type == "activitySucceeded");

    let eff_at = eff_at_idx.expect("effectAttempted event must exist in log");
    let act_at = activity_succeeded_idx.expect("activitySucceeded event must exist in log");
    assert!(
        eff_at < act_at,
        "effectAttempted (idx {eff_at}) must appear before activitySucceeded (idx {act_at})"
    );

    // Verify effectAttempted payload fields — including the custom provider/TTL
    // returned by our SpyHooks::prepare_host_executor.
    let eff_event = &events[eff_at];
    let payload = &eff_event.payload;
    assert!(
        payload.get("activityId").and_then(Value::as_str).is_some(),
        "effectAttempted must contain activityId"
    );
    assert!(
        payload.get("attemptId").and_then(Value::as_str).is_some(),
        "effectAttempted must contain attemptId"
    );
    assert!(
        payload
            .get("idempotencyKey")
            .and_then(Value::as_str)
            .is_some(),
        "effectAttempted must contain idempotencyKey"
    );
    assert!(
        payload.get("inputHash").and_then(Value::as_str).is_some(),
        "effectAttempted must contain inputHash"
    );
    assert_eq!(
        payload.get("idempotencyTtlMs").and_then(Value::as_u64),
        Some(42_000),
        "effectAttempted.idempotencyTtlMs should be 42_000 from prepare hook"
    );
    assert_eq!(
        payload.get("provider").and_then(Value::as_str),
        Some("test-provider"),
        "effectAttempted.provider should be 'test-provider' from prepare hook"
    );

    let _ = fs::remove_dir_all(&run_dir);
}

// ---------------------------------------------------------------------------
// Phase 2.2: prepare_host_executor failure — no side-effect, no hook call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn prepare_host_executor_failure_prevents_effect_attempted_and_hook_call() {
    let run_dir = temp_run_dir("effat-prepfail");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-effat-prepfail";
    let params = BTreeMap::new();

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{
                "workflowId":"effat-prepfail",
                "version":1,
                "nodes":{
                    "a":{"type":"hostExecutor","executor":"custom-tool","input":{"payload":"invalid"}}
                }
            }"#,
            expected_workflow_id: Some("effat-prepfail"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "effat-prepfail".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    executor: "custom-tool".to_string(),
                    input: json!({"payload": "invalid"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let hooks = FailingPrepareHooks::new();
    let execute_called_ref = hooks.execute_called.clone();
    let mut hooks = hooks;

    // run_loop should fail because prepare_host_executor returned an error
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await;
    assert!(result.is_err(), "run_loop should fail when prepare fails");

    // execute_host_executor MUST NOT have been called
    assert!(
        !*execute_called_ref.lock().unwrap(),
        "execute_host_executor should NOT be called when prepare fails"
    );

    // Re-open the event log — there should be NO effectAttempted
    let log2 = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let events = log2.read_all().unwrap();
    assert!(
        !events.iter().any(|e| e.event_type == "effectAttempted"),
        "effectAttempted must NOT exist when prepare_host_executor fails"
    );
    // Also no terminal event
    assert!(
        !events.iter().any(|e| e.event_type == "activitySucceeded"),
        "activitySucceeded must NOT exist when prepare fails"
    );

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn host_executor_effect_attempted_survives_hook_failure() {
    let run_dir = temp_run_dir("effat-fail");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-effat-fail";
    let params = BTreeMap::new();

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{
                "workflowId":"effat-fail",
                "version":1,
                "nodes":{
                    "a":{"type":"hostExecutor","executor":"custom-tool","input":{"payload":"crash-me"}}
                }
            }"#,
            expected_workflow_id: Some("effat-fail"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "effat-fail".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    executor: "custom-tool".to_string(),
                    input: json!({"payload": "crash-me"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = PanicHooks;

    // run_tick should fail because the hook returned an error
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await;
    assert!(
        result.is_err(),
        "run_loop should fail when hook returns error"
    );

    // Re-open the event log to read the events that were written before
    // the failure.
    let log2 = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let events = log2.read_all().unwrap();

    let eff_at = events
        .iter()
        .position(|e| e.event_type == "effectAttempted");
    assert!(
        eff_at.is_some(),
        "effectAttempted must exist in log even after hook failure"
    );

    // There should be NO activitySucceeded / activityFailed because
    // settle_work_result was never reached.
    assert!(
        !events.iter().any(|e| e.event_type == "activitySucceeded"),
        "activitySucceeded should NOT exist when hook fails"
    );
    assert!(
        !events.iter().any(|e| e.event_type == "activityFailed"),
        "activityFailed should NOT exist when hook fails (no terminal event)"
    );

    let _ = fs::remove_dir_all(&run_dir);
}

/// Verify that the snapshot projection includes `dangling.effect_attempted`
/// when an activity has emitted `effectAttempted` but has not reached a
/// terminal status.
#[tokio::test]
async fn snapshot_projects_dangling_effect_attempted() {
    let run_dir = temp_run_dir("effat-dangling");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-effat-dangling";
    let params = BTreeMap::new();

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{
                "workflowId":"effat-dangling",
                "version":1,
                "nodes":{
                    "a":{"type":"hostExecutor","executor":"custom-tool","input":{"payload":"dangle"}}
                }
            }"#,
            expected_workflow_id: Some("effat-dangling"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "effat-dangling".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    executor: "custom-tool".to_string(),
                    input: json!({"payload": "dangle"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = PanicHooks;

    // Trigger the failing dispatch (effectAttempted will be written, then
    // the hook fails, leaving no terminal event).
    let _ = run_loop(&mut rt, &mut hooks, 10, 1).await;

    // Read the snapshot from disk — this replays the events and computes
    // dangling projections.
    let snapshot = read_run_snapshot(&rt.log.run_dir)
        .await
        .expect("snapshot read")
        .expect("snapshot present");

    // The activity should be dangling because no terminal event was written.
    assert!(
        !snapshot.dangling.activities.is_empty(),
        "expected at least one dangling activity"
    );
    assert!(
        !snapshot.dangling.effect_attempted.is_empty(),
        "dangling.effect_attempted must contain the activity that emitted effectAttempted"
    );
    // Verify the effect_attempted list includes the expected activity
    let activity_id = snapshot.dangling.effect_attempted.first().unwrap();
    assert!(
        activity_id.ends_with("::work::a"),
        "expected activity to end with ::work::a, got {activity_id}"
    );

    let _ = fs::remove_dir_all(&run_dir);
}
