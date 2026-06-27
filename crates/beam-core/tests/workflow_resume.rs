use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use async_trait::async_trait;
use beam_core::{
    BeamPaths, EventLog, RequestCancelInput, RunChatBinding, RunLoopStopReason, RunStatus,
    WorkflowActor, WorkflowDispatchOutcome, WorkflowDispatchRun, WorkflowExecutionHooks,
    WorkflowRuntimeContext, bootstrap_workflow_run, read_run_snapshot, request_cancel, run_loop,
    run_tick,
    workflow_definition::{
        HostExecutorNode, NodeBase, SubagentNode, WorkflowDefinition, WorkflowNode,
    },
};
use serde_json::Value;
use tokio::sync::Mutex;

fn temp_run_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "beam-workflow-resume-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn workflow_def() -> WorkflowDefinition {
    WorkflowDefinition {
        workflow_id: "flow-resume".to_string(),
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
                    prompt: Value::String("task-a".to_string()),
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
                    prompt: Value::String("task-b".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            ),
        ]),
    }
}

#[derive(Clone)]
struct CountingHooks {
    call_count: Arc<Mutex<usize>>,
    node_fail: Arc<Mutex<Option<String>>>,
}

impl CountingHooks {
    fn new() -> Self {
        Self {
            call_count: Arc::new(Mutex::new(0)),
            node_fail: Arc::new(Mutex::new(None)),
        }
    }

    #[allow(dead_code)]
    fn with_fail(node_id: &str) -> Self {
        Self {
            call_count: Arc::new(Mutex::new(0)),
            node_fail: Arc::new(Mutex::new(Some(node_id.to_string()))),
        }
    }
}

#[async_trait]
impl WorkflowExecutionHooks for CountingHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        node: &SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        let mut count = self.call_count.lock().await;
        *count += 1;
        let fail_node = self.node_fail.lock().await.clone();
        if fail_node.as_deref() == Some(node.bot.as_str()) || fail_node.as_deref() == Some("*") {
            return Ok(WorkflowDispatchOutcome::Failed {
                error_code: "E_TEST".to_string(),
                error_class: "TEST_FAILURE".to_string(),
                error_message: "simulated failure".to_string(),
                session: None,
            });
        }
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(format!("result-{}", resolved_prompt)),
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

#[tokio::test]
async fn resume_from_prior_tick_continues_with_remaining_nodes() {
    let run_dir = temp_run_dir("resume-continue");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let params = BTreeMap::new();
    let run_id = "run-resume-1";

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-resume","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"task-a"},"b":{"type":"subagent","bot":"bot-b","prompt":"task-b","depends":["a"]}}}"#,
            expected_workflow_id: Some("flow-resume"),
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
        def: workflow_def(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = CountingHooks::new();
    let tick = run_tick(&mut rt, &mut hooks, 1).await.unwrap();
    assert_eq!(tick.actions, 1);
    assert_eq!(*hooks.call_count.lock().await, 1);

    let snapshot1 = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot1.run.status, RunStatus::Running);

    let mut rt2 = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: workflow_def(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks2 = CountingHooks::new();

    let result = run_loop(&mut rt2, &mut hooks2, 5, 1).await.unwrap();
    assert!(matches!(
        result.reason,
        RunLoopStopReason::Terminal | RunLoopStopReason::NoProgress
    ));
    assert_eq!(*hooks2.call_count.lock().await, 1);
    assert!(result.ticks > 0);

    let final_snapshot = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(final_snapshot.run.status, RunStatus::Succeeded));

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn cancel_intent_persists_and_is_readable_after_reload() {
    let run_dir = temp_run_dir("cancel-persist");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let params = BTreeMap::new();
    let run_id = "run-cancel-persist";

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-resume","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"task-a"}}}"#,
            expected_workflow_id: Some("flow-resume"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        request_cancel(
            &mut log,
            RequestCancelInput {
                target: serde_json::json!({
                    "kind": "run",
                    "runId": run_id,
                }),
                reason: "test cancel".to_string(),
                by: "tester".to_string(),
            },
            WorkflowActor::Human,
        )
        .await
        .unwrap();
    }

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-resume".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
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
                    prompt: Value::String("task-a".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = CountingHooks::new();
    let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    assert_eq!(tick.actions, 0);
    assert!(tick.snapshot.run.cancelled_run_intent.is_some());
    assert_eq!(
        tick.snapshot
            .run
            .cancelled_run_intent
            .as_ref()
            .unwrap()
            .reason,
        "test cancel"
    );

    let mut rt2 = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-resume".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
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
                    prompt: Value::String("task-a".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks2 = CountingHooks::new();
    let tick2 = run_tick(&mut rt2, &mut hooks2, 2).await.unwrap();
    assert_eq!(tick2.actions, 0);
    assert!(tick2.snapshot.run.cancelled_run_intent.is_some());

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn resume_after_partial_concurrency_completes_remaining() {
    let run_dir = temp_run_dir("concurrent-resume");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let params = BTreeMap::new();
    let run_id = "run-concurrent-resume";

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-concurrent","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"},"c":{"type":"subagent","bot":"bot-c","prompt":"three"}}}"#,
            expected_workflow_id: Some("flow-concurrent"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let three_node_def = WorkflowDefinition {
        workflow_id: "flow-concurrent".to_string(),
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
                    prompt: Value::String("one".to_string()),
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
                    prompt: Value::String("two".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            ),
            (
                "c".to_string(),
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
                    bot: "bot-c".to_string(),
                    prompt: Value::String("three".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            ),
        ]),
    };

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: three_node_def.clone(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = CountingHooks::new();
    let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    assert_eq!(tick.actions, 2);

    let mut rt2 = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: three_node_def.clone(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks2 = CountingHooks::new();
    let result = run_loop(&mut rt2, &mut hooks2, 5, 3).await.unwrap();
    assert!(matches!(
        result.reason,
        RunLoopStopReason::Terminal | RunLoopStopReason::NoProgress
    ));
    assert_eq!(*hooks2.call_count.lock().await, 1);

    let final_snapshot = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    let succeeded: Vec<_> = final_snapshot
        .nodes
        .iter()
        .filter(|n| n.status == beam_core::NodeStatus::Succeeded)
        .map(|n| n.node_id.clone())
        .collect();
    assert_eq!(succeeded.len(), 3);
    assert!(succeeded.contains(&"a".to_string()));
    assert!(succeeded.contains(&"b".to_string()));
    assert!(succeeded.contains(&"c".to_string()));

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn reload_respects_existing_node_status_on_resume() {
    let run_dir = temp_run_dir("status-resume");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let params = BTreeMap::new();
    let run_id = "run-status-resume";

    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-resume","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"task-a"},"b":{"type":"subagent","bot":"bot-b","prompt":"task-b","depends":["a"]}}}"#,
            expected_workflow_id: Some("flow-resume"),
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
        def: workflow_def(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = CountingHooks::new();
    let tick = run_tick(&mut rt, &mut hooks, 1).await.unwrap();
    assert_eq!(tick.actions, 1);

    let snapshot1 = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    let a_status = snapshot1
        .nodes
        .iter()
        .find(|n| n.node_id == "a")
        .map(|n| n.status)
        .unwrap();
    assert_eq!(a_status, beam_core::NodeStatus::Running);

    let mut rt2 = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: workflow_def(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks2 = CountingHooks::new();
    let result = run_loop(&mut rt2, &mut hooks2, 5, 1).await.unwrap();
    assert!(matches!(
        result.reason,
        RunLoopStopReason::Terminal | RunLoopStopReason::NoProgress
    ));
    assert_eq!(*hooks2.call_count.lock().await, 1);

    let snapshot_check = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    let a_check = snapshot_check
        .nodes
        .iter()
        .find(|n| n.node_id == "a")
        .map(|n| n.status)
        .unwrap();
    assert_eq!(a_check, beam_core::NodeStatus::Succeeded);

    let _ = fs::remove_dir_all(&run_dir);
}
