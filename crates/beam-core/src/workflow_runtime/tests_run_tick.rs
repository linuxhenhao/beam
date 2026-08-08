use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::workflow_definition::{NodeBase, SubagentNode};
use crate::workflow_snapshot::NodeStatus;
use crate::{EventDraft, EventLog, RunChatBinding, WorkflowActor, WorkflowNode};

use super::test_common::{FakeHooks, temp_run_dir, workflow_def};
use super::*;

#[tokio::test]
async fn run_tick_dispatches_simple_workflow() {
    let run_dir = temp_run_dir("tick");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    fs::write(run_dir.join("params.json"), r#"{"name":"beam"}"#).unwrap();

    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
    let run_id = "run-1";
    let bootstrap = crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello ${params.name}"}}}"#,
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
    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: workflow_def(),
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = FakeHooks;
    let tick = run_tick(&mut rt, &mut hooks, 1).await.unwrap();
    assert!(tick.actions > 0);
    assert_eq!(tick.snapshot.run.workflow_id.as_deref(), Some("flow-a"));
    assert!(matches!(
        tick.snapshot
            .nodes
            .iter()
            .find(|node| node.node_id == "a")
            .map(|node| node.status),
        Some(NodeStatus::Succeeded | NodeStatus::Running | NodeStatus::Triggered)
    ));
    let _ = fs::remove_dir_all(&run_dir);
    let _ = bootstrap;
}

#[tokio::test]
async fn run_tick_honors_max_concurrency_cap() {
    let run_dir = temp_run_dir("cap");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let params: BTreeMap<String, Value> = BTreeMap::new();
    let run_id = "run-cap";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-cap","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
            expected_workflow_id: Some("flow-cap"),
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
            workflow_id: "flow-cap".to_string(),
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
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = FakeHooks;
    let tick = run_tick(&mut rt, &mut hooks, 1).await.unwrap();
    assert_eq!(tick.actions, 1);
    let snapshot = tick.snapshot;
    let attempted: Vec<_> = snapshot
        .activities
        .iter()
        .map(|a| a.activity_id.as_str())
        .collect();
    assert_eq!(attempted.len(), 1);
    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn run_tick_dispatches_multiple_actions_concurrently() {
    let run_dir = temp_run_dir("concurrent");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let params: BTreeMap<String, Value> = BTreeMap::new();
    let run_id = "run-concurrent";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-concurrent","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
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
    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
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
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    #[derive(Clone)]
    struct SleepyHooks;

    #[async_trait]
    impl WorkflowExecutionHooks for SleepyHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
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
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: resolved_input,
                session: None,
            })
        }
    }

    let mut hooks = SleepyHooks;
    let started = std::time::Instant::now();
    let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(tick.actions, 2);
    assert!(
        elapsed < std::time::Duration::from_millis(220),
        "run_tick took {:?}, expected concurrent execution under 220ms",
        elapsed
    );
    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn run_tick_serializes_actions_for_the_same_bot() {
    let run_dir = temp_run_dir("same-bot");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let params: BTreeMap<String, Value> = BTreeMap::new();
    let run_id = "run-same-bot";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-same-bot","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-shared","prompt":"one"},"b":{"type":"subagent","bot":"bot-shared","prompt":"two"},"c":{"type":"subagent","bot":"bot-other","prompt":"three"}}}"#,
            expected_workflow_id: Some("flow-same-bot"),
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
            workflow_id: "flow-same-bot".to_string(),
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
                        bot: "bot-shared".to_string(),
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
                        bot: "bot-shared".to_string(),
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
                        bot: "bot-other".to_string(),
                        prompt: Value::String("three".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    #[derive(Clone)]
    struct SerializingHooks {
        started: Arc<Mutex<Vec<String>>>,
        active_by_bot: Arc<Mutex<HashMap<String, usize>>>,
        max_active_by_bot: Arc<Mutex<HashMap<String, usize>>>,
    }

    #[async_trait]
    impl WorkflowExecutionHooks for SerializingHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            let bot = node.bot.clone();
            {
                let mut active = self.active_by_bot.lock().await;
                let entry = active.entry(bot.clone()).or_insert(0);
                *entry += 1;
                let mut max_active = self.max_active_by_bot.lock().await;
                let max_entry = max_active.entry(bot.clone()).or_insert(0);
                if *entry > *max_entry {
                    *max_entry = *entry;
                }
                assert!(
                    *entry <= 1,
                    "bot {} ran concurrently with itself: {}",
                    bot,
                    *entry
                );
            }
            self.started.lock().await.push(resolved_prompt.clone());
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            {
                let mut active = self.active_by_bot.lock().await;
                if let Some(entry) = active.get_mut(&bot) {
                    *entry = entry.saturating_sub(1);
                }
            }
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
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: resolved_input,
                session: None,
            })
        }
    }

    let started = Arc::new(Mutex::new(Vec::new()));
    let active_by_bot = Arc::new(Mutex::new(HashMap::new()));
    let max_active_by_bot = Arc::new(Mutex::new(HashMap::new()));
    let mut hooks = SerializingHooks {
        started: started.clone(),
        active_by_bot: active_by_bot.clone(),
        max_active_by_bot: max_active_by_bot.clone(),
    };

    let started_at = std::time::Instant::now();
    let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    let elapsed = started_at.elapsed();

    assert_eq!(tick.actions, 2);
    let started = started.lock().await.clone();
    assert_eq!(started.len(), 2);
    assert!(started.contains(&"one".to_string()));
    assert!(started.contains(&"three".to_string()));
    assert!(!started.contains(&"two".to_string()));
    let max_active_by_bot = max_active_by_bot.lock().await;
    assert_eq!(max_active_by_bot.get("bot-shared"), Some(&1));
    assert_eq!(max_active_by_bot.get("bot-other"), Some(&1));
    assert!(
        elapsed < std::time::Duration::from_millis(220),
        "run_tick took {:?}, expected two distinct bots to run concurrently",
        elapsed
    );
    let _ = fs::remove_dir_all(&run_dir);
}

#[derive(Clone)]
struct CancellingHooks {
    run_id: String,
    runs_base_dir: PathBuf,
    calls: Arc<Mutex<usize>>,
    completed: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkflowExecutionHooks for CancellingHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        let mut calls = self.calls.lock().await;
        *calls += 1;
        if *calls == 1 {
            let run_id = self.run_id.clone();
            let runs_base_dir = self.runs_base_dir.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let mut log = EventLog::new(run_id.clone(), runs_base_dir).unwrap();
                let _ = log.append(EventDraft {
                    event_type: "cancelRequested".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "target": { "kind": "run", "runId": run_id },
                        "reason": "cancel mid tick",
                        "by": "tester",
                    }),
                    timestamp: None,
                    payload_hash: None,
                });
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        *self.completed.lock().await += 1;
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
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: resolved_input,
            session: None,
        })
    }
}

#[tokio::test]
async fn run_tick_stops_between_actions_when_cancel_arrives() {
    let run_dir = temp_run_dir("cancel-mid-tick");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-cancel";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-cancel","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
            expected_workflow_id: Some("flow-cancel"),
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
            workflow_id: "flow-cancel".to_string(),
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
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = CancellingHooks {
        run_id: run_id.to_string(),
        runs_base_dir: paths.workflow_runs_dir(),
        calls: Arc::new(Mutex::new(0)),
        completed: Arc::new(Mutex::new(0)),
    };
    let started = std::time::Instant::now();
    let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    let elapsed = started.elapsed();
    assert!(tick.actions < 2);
    assert!(tick.snapshot.run.cancelled_run_intent.is_some());
    assert!(!tick.snapshot.activities.is_empty());
    assert!(
        elapsed < std::time::Duration::from_millis(250),
        "run_tick took {:?}, expected cancel to interrupt long-running actions",
        elapsed
    );
    assert_eq!(*hooks.completed.lock().await, 0);
    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn run_tick_skips_dispatch_when_run_cancel_is_pending() {
    let run_dir = temp_run_dir("reconcile-cancel");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-reconcile-cancel";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-reconcile","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
            expected_workflow_id: Some("flow-reconcile"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();
    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let _ = crate::request_cancel(
        &mut log,
        crate::RequestCancelInput {
            target: serde_json::json!({
                "kind": "run",
                "runId": run_id,
            }),
            reason: "cancel before dispatch".to_string(),
            by: "tester".to_string(),
        },
        WorkflowActor::Human,
    )
    .await
    .unwrap();
    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-reconcile".to_string(),
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
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = FakeHooks;
    let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    assert_eq!(tick.actions, 0);
    assert!(tick.snapshot.run.cancelled_run_intent.is_some());
    let _ = fs::remove_dir_all(&run_dir);
}

#[test]
fn orchestrator_action_is_dispatch_classifies_correctly() {
    use crate::workflow_definition::{NodeBase, SubagentNode};
    use crate::workflow_orchestrator::OrchestratorAction;
    let dispatch = OrchestratorAction::DispatchWork {
        node_id: "n1".to_string(),
        activity_id: "a1".to_string(),
        node: crate::WorkflowNode::Subagent(SubagentNode {
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
            bot: "b1".to_string(),
            prompt: serde_json::json!("p"),
            working_dir: None,
            model_overrides: None,
            tool_policy: None,
        })
        .into(),
    };
    assert!(dispatch.is_dispatch());

    let settle = OrchestratorAction::CompleteNodeSucceeded {
        node_id: "n1".to_string(),
        last_activity_id: "a1".to_string(),
        output_ref: None,
    };
    assert!(!settle.is_dispatch());
}
