// Phase 0: workflow current-behavior regression tests.
// These tests document current behavior. Do not change behavior here;
// only add tests that express what the runtime does today.

use std::collections::BTreeMap;
use std::fs;

use async_trait::async_trait;
use beam_core::{
    BeamPaths, EventLog, NodeStatus, ResolveWaitInput, RunChatBinding, RunLoopStopReason,
    RunStatus, WaitResolution, WorkflowActor, WorkflowDispatchOutcome,
    WorkflowDispatchRun, WorkflowExecutionHooks, WorkflowNode, WorkflowRuntimeContext,
    bootstrap_workflow_run, read_run_snapshot, resolve_wait, run_loop, run_tick,
    workflow_definition::{
        HostExecutorNode, HumanGate, NodeBase, SubagentNode, WorkflowDefinition,
    },
};
use serde_json::{Value, json};

fn temp_run_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "beam-regression-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

#[derive(Clone)]
struct FakeHooks;

#[async_trait]
impl WorkflowExecutionHooks for FakeHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        resolved_input: Value,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: resolved_input,
            session: None,
        })
    }
}

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
    assert!(result
        .last_snapshot
        .nodes
        .iter()
        .all(|n| n.status == NodeStatus::Succeeded));
    let _ = fs::remove_dir_all(&run_dir);
}

// -- Task 0.1: humanGate approve then continue execution test --

#[tokio::test]
async fn human_gate_approve_resumes_execution() {
    let run_dir = temp_run_dir("gate-approve");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-gate";

    let workflow_json = r#"{
        "workflowId":"gate-smoke",
        "version":1,
        "nodes":{
            "a":{"type":"subagent","bot":"bot-a","prompt":"step-a","humanGate":{"stage":"before","prompt":"approve a?"}},
            "b":{"type":"subagent","bot":"bot-b","prompt":"step-b","depends":["a"]}
        }
    }"#;
    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json,
            expected_workflow_id: Some("gate-smoke"),
            params: &BTreeMap::new(),
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
            workflow_id: "gate-smoke".to_string(),
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
                            human_gate: Some(HumanGate {
                                stage: "before".to_string(),
                                prompt: Value::String("approve a?".to_string()),
                                approvers: None,
                                deadline_ms: None,
                                on_timeout: None,
                            }),
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

    // First tick: should create the gate wait, not dispatch actual work
    let tick1 = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    assert!(tick1.actions > 0, "should have dispatched gate");
    // The snap should show the gate wait, not the subagent activity (yet)
    let snapshot1 = read_run_snapshot(&rt.log.run_dir).await.unwrap().unwrap();
    assert!(
        !snapshot1.dangling.waits.is_empty(),
        "should have a pending wait"
    );

    // Approve the wait
    let wait = &snapshot1.dangling.waits[0];
    // Find the wait's attempt id and resolve it
    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let _ = resolve_wait(
        &mut log,
        ResolveWaitInput {
            activity_id: wait.clone(),
            attempt_id: format!("{wait}::att-1"),
            resolution: WaitResolution::Approved,
            by: "tester".to_string(),
            comment: None,
            output: None,
            is_decision_node: false,
        },
    )
    .await
    .unwrap();

    // Re-read the runtime context with fresh log
    let mut rt2 = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: rt.def.clone(),
        runs_base_dir: paths.workflow_runs_dir(),
    };

    // Next tick: should now dispatch node a's actual work
    let tick2 = run_tick(&mut rt2, &mut hooks, 2).await.unwrap();
    assert!(tick2.actions > 0, "should dispatch work after approve");

    // Run to completion
    let result = run_loop(&mut rt2, &mut hooks, 10, 2).await.unwrap();
    assert!(
        matches!(result.reason, RunLoopStopReason::Terminal),
        "expected Terminal, got {:?}",
        result.reason
    );
    assert_eq!(result.last_snapshot.run.status, RunStatus::Succeeded);
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
    beam_core::request_cancel(
        &mut log,
        beam_core::RequestCancelInput {
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
