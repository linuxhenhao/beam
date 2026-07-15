// Phase 0: workflow current-behavior regression tests — run scenarios.
// These tests document current behavior. Do not change behavior here;
// only add tests that express what the runtime does today.

mod support;
use support::*;

use std::collections::BTreeMap;
use std::fs;

use beam_core::{
    BeamPaths, EventLog, NodeStatus, RequestCancelInput, RunChatBinding, RunLoopStopReason,
    RunStatus, WorkflowActor, WorkflowNode, WorkflowRuntimeContext, bootstrap_workflow_run,
    request_cancel, run_loop, run_tick,
    workflow_definition::{HostExecutorNode, NodeBase, SubagentNode, WorkflowDefinition},
};
use serde_json::{Value, json};

// -- Task 0.1: DAG workflow success test --

#[tokio::test]
async fn minimal_dag_workflow_runs_to_completion() {
    let run_dir = temp_run_dir("dag");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-dag";
    let params = BTreeMap::new();
    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{
                "workflowId":"dag-smoke",
                "version":1,
                "nodes":{
                    "a":{"type":"subagent","bot":"bot-a","prompt":"step-a"},
                    "b":{"type":"subagent","bot":"bot-b","prompt":"step-b","depends":["a"]}
                }
            }"#,
            expected_workflow_id: Some("dag-smoke"),
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
            workflow_id: "dag-smoke".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([
                (
                    "a".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
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
                        bot: "bot-a".to_string(),
                        prompt: Value::String("step-a".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
                (
                    "b".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
                        base: NodeBase {
                            description: None,
                            depends: Some(vec!["a".to_string()]),
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                        bot: "bot-b".to_string(),
                        prompt: Value::String("step-b".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = FakeHooks;
    let result = run_loop(&mut rt, &mut hooks, 10, 2).await.unwrap();
    assert!(
        matches!(result.reason, RunLoopStopReason::Terminal),
        "expected Terminal, got {:?}",
        result.reason
    );
    assert_eq!(result.last_snapshot.run.status, RunStatus::Succeeded);
    // Both nodes should be succeeded
    assert!(
        result
            .last_snapshot
            .nodes
            .iter()
            .all(|n| n.status == NodeStatus::Succeeded)
    );
    let _ = fs::remove_dir_all(&run_dir);
}

// -- Task 0.1: hostExecutor execution produces terminal event test --

#[tokio::test]
async fn host_executor_run_produces_terminal_event() {
    let run_dir = temp_run_dir("host-exec");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-host-exec";
    let params = BTreeMap::new();

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            // Use custom-tool executor (non-side-effect, no gate required)
            workflow_json: r#"{
                "workflowId":"host-exec-smoke",
                "version":1,
                "nodes":{
                    "a":{"type":"hostExecutor","executor":"custom-tool","input":42}
                }
            }"#,
            expected_workflow_id: Some("host-exec-smoke"),
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
            workflow_id: "host-exec-smoke".to_string(),
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
                    input: Value::Number(42.into()),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = FakeHooks;
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
    assert!(
        matches!(result.reason, RunLoopStopReason::Terminal),
        "expected Terminal, got {:?}",
        result.reason
    );
    assert_eq!(result.last_snapshot.run.status, RunStatus::Succeeded);

    // Verify the event log contains the terminal events
    let events = rt.log.read_all().unwrap();
    let activity_succeeded = events.iter().any(|e| e.event_type == "activitySucceeded");
    let run_succeeded = events.iter().any(|e| e.event_type == "runSucceeded");
    assert!(activity_succeeded, "expected activitySucceeded event");
    assert!(run_succeeded, "expected runSucceeded event");
    let _ = fs::remove_dir_all(&run_dir);
}

// -- Task 0.1: run cancel then no longer dispatches new action test --

#[tokio::test]
async fn run_cancel_stops_further_dispatches() {
    let run_dir = temp_run_dir("cancel-dispatch");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-cancel-nodispatch";
    let params = BTreeMap::new();

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{
                "workflowId":"cancel-smoke",
                "version":1,
                "nodes":{
                    "a":{"type":"subagent","bot":"bot-a","prompt":"step-a"},
                    "b":{"type":"subagent","bot":"bot-b","prompt":"step-b"}
                }
            }"#,
            expected_workflow_id: Some("cancel-smoke"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write cancelRequested BEFORE dispatch
    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    request_cancel(
        &mut log,
        RequestCancelInput {
            target: json!({"kind": "run", "runId": run_id}),
            reason: "test cancel".to_string(),
            by: "tester".to_string(),
        },
        WorkflowActor::Human,
    )
    .await
    .unwrap();

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "cancel-smoke".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([
                (
                    "a".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
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
                        bot: "bot-a".to_string(),
                        prompt: Value::String("step-a".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
                (
                    "b".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
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
                        bot: "bot-b".to_string(),
                        prompt: Value::String("step-b".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = FakeHooks;
    let tick = run_tick(&mut rt, &mut hooks, 10).await.unwrap();
    // Cancel is pending → should dispatch zero actions
    assert_eq!(tick.actions, 0);
    assert!(tick.snapshot.run.cancelled_run_intent.is_some());
    let _ = fs::remove_dir_all(&run_dir);
}
