use std::collections::BTreeMap;
use std::fs;

use async_trait::async_trait;
use serde_json::Value;

use crate::workflow_definition::{
    DecisionNode, HumanGate, LoopNode, LoopOutputProjection, LoopTerminate, NodeBase, SubagentNode,
};
use crate::{EventDraft, EventLog, RunChatBinding, WorkflowActor, WorkflowNode};

use super::test_common::{FakeHooks, temp_run_dir};
use super::*;

/// Build a minimal code-review-loop WorkflowDefinition for testing.
fn code_review_loop_def() -> WorkflowDefinition {
    WorkflowDefinition {
        workflow_id: "code-review-loop".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            (
                "implement".to_string(),
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
                    prompt: Value::String("implement".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            ),
            (
                "review".to_string(),
                WorkflowNode::Subagent(SubagentNode {
                    base: NodeBase {
                        description: None,
                        depends: Some(vec!["implement".to_string()]),
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    bot: "bot-a".to_string(),
                    prompt: Value::String("review".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            ),
            (
                "reviewDecision".to_string(),
                WorkflowNode::Decision(DecisionNode {
                    base: NodeBase {
                        description: None,
                        depends: Some(vec!["review".to_string()]),
                        human_gate: Some(HumanGate {
                            stage: "before".to_string(),
                            prompt: Value::String("approve?".to_string()),
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
                }),
            ),
            (
                "review-loop".to_string(),
                WorkflowNode::Loop(LoopNode {
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
                    max_iterations: 3,
                    body: vec![
                        "implement".to_string(),
                        "review".to_string(),
                        "reviewDecision".to_string(),
                    ],
                    terminate: LoopTerminate {
                        node: "reviewDecision".to_string(),
                        via: "humanGate".to_string(),
                    },
                    output: Some(LoopOutputProjection {
                        from: "implement".to_string(),
                    }),
                }),
            ),
        ]),
    }
}

#[tokio::test]
async fn loop_depends_met_produces_start_loop_and_first_iteration() {
    // Verify that when a loop node's depends are satisfied, the orchestrator
    // emits StartLoop + StartLoopIteration(1).
    let run_dir = temp_run_dir("loop-depends");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let params: BTreeMap<String, Value> = BTreeMap::new();
    let run_id = "run-loop-depends";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-loop-dep","version":1,"nodes":{"pre":{"type":"subagent","bot":"bot-x","prompt":"setup"},"work":{"type":"subagent","bot":"bot-x","prompt":"work"},"dec":{"type":"decision","depends":["work"],"humanGate":{"stage":"approve","prompt":"continue?"}},"rl":{"type":"loop","maxIterations":3,"body":["work","dec"],"depends":["pre"],"terminate":{"node":"dec","via":"humanGate"},"output":{"from":"work"}}}}"#,
            expected_workflow_id: Some("flow-loop-dep"),
            params: &params,
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // First tick: dispatch "pre" (the loop's dependency).
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-loop-dep".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "pre".to_string(),
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
                            bot: "bot-x".to_string(),
                            prompt: Value::String("setup".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "dec".to_string(),
                        WorkflowNode::Decision(DecisionNode {
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
                        }),
                    ),
                    (
                        "rl".to_string(),
                        WorkflowNode::Loop(LoopNode {
                            base: NodeBase {
                                description: None,
                                depends: Some(vec!["pre".to_string()]),
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            max_iterations: 3,
                            body: vec!["dec".to_string()],
                            terminate: LoopTerminate {
                                node: "dec".to_string(),
                                via: "humanGate".to_string(),
                            },
                            output: Some(LoopOutputProjection {
                                from: "dec".to_string(),
                            }),
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
        assert!(
            _result.ticks > 0 || matches!(_result.reason, RunLoopStopReason::Terminal),
            "expected pre dispatch; reason={:?}",
            _result.reason
        );
    }

    // Now "pre" should be succeeded, so the loop can start.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-loop-dep".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "pre".to_string(),
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
                            bot: "bot-x".to_string(),
                            prompt: Value::String("setup".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "dec".to_string(),
                        WorkflowNode::Decision(DecisionNode {
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
                        }),
                    ),
                    (
                        "rl".to_string(),
                        WorkflowNode::Loop(LoopNode {
                            base: NodeBase {
                                description: None,
                                depends: Some(vec!["pre".to_string()]),
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            max_iterations: 3,
                            body: vec!["dec".to_string()],
                            terminate: LoopTerminate {
                                node: "dec".to_string(),
                                via: "humanGate".to_string(),
                            },
                            output: Some(LoopOutputProjection {
                                from: "dec".to_string(),
                            }),
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();
        let loop_started = events.iter().any(|e| {
            e.event_type == "loopStarted"
                && e.payload.get("loopId").and_then(Value::as_str) == Some("rl")
        });
        let iter_started = events.iter().any(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("loopId").and_then(Value::as_str) == Some("rl")
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
        });
        assert!(loop_started, "expected loopStarted event for rl");
        assert!(
            iter_started,
            "expected loopIterationStarted with iteration 1 for rl"
        );
    }

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn code_review_loop_reaches_human_gate_wait_with_correct_activity_id() {
    // Drive the code-review-loop workflow via run_loop until it reaches an
    // open human-gate wait.  Verify the activity-id format.
    let run_dir = temp_run_dir("crl-gate");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-crl-gate";
    let def = code_review_loop_def();
    let workflow_json = serde_json::to_string(&def).unwrap();
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
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
        def,
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = FakeHooks;
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

    // Should have stopped at AwaitingWait (human gate open for reviewDecision).
    assert_eq!(
        result.reason,
        RunLoopStopReason::AwaitingWait,
        "expected AwaitingWait for open human gate"
    );

    let events = rt.log.read_all().unwrap();

    // Verify loop lifecycle events
    let loop_started = events.iter().find(|e| e.event_type == "loopStarted");
    assert!(loop_started.is_some(), "missing loopStarted");
    let iter_started = events.iter().find(|e| {
        e.event_type == "loopIterationStarted"
            && e.payload.get("iteration") == Some(&Value::Number(1.into()))
    });
    assert!(iter_started.is_some(), "missing loopIterationStarted(1)");

    // Verify activity IDs use the correct loop-scoped format
    let implement_work_id = format!("{}::loop::review-loop.1::work::implement", run_id);
    let review_work_id = format!("{}::loop::review-loop.1::work::review", run_id);
    let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

    let has_implement = events.iter().any(|e| {
        e.event_type == "attemptCreated"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&implement_work_id)
    });
    let has_review = events.iter().any(|e| {
        e.event_type == "attemptCreated"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&review_work_id)
    });
    let has_decision_gate = events.iter().any(|e| {
        e.event_type == "waitCreated"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&decision_gate_id)
    });

    assert!(
        has_implement,
        "missing implement work dispatch: {}",
        implement_work_id
    );
    assert!(
        has_review,
        "missing review work dispatch: {}",
        review_work_id
    );
    assert!(
        has_decision_gate,
        "missing reviewDecision gate wait: {}",
        decision_gate_id
    );

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn reject_decision_enters_next_iteration() {
    // Drive to the gate wait, then write a waitResolved(rejected).
    // run_loop should emit FinishLoopIteration(rejected) and StartLoopIteration(2).
    let run_dir = temp_run_dir("crl-reject");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-crl-reject";
    let def = code_review_loop_def();
    let workflow_json = serde_json::to_string(&def).unwrap();
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

    // First: drive to AwaitingWait.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }

    // Write waitResolved (rejected).
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &decision_gate_id,
                    "resolution": "rejected",
                    "by": "reviewer",
                    "comment": "needs work",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Second: run_loop again — should handle the rejection.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();

        // Verify FinishLoopIteration(rejected) for iteration 1.
        let iter1_finished = events.iter().any(|e| {
            e.event_type == "loopIterationFinished"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
                && e.payload.get("resolution").and_then(Value::as_str) == Some("rejected")
        });
        assert!(
            iter1_finished,
            "expected loopIterationFinished with resolution=rejected for iteration 1"
        );

        // Verify StartLoopIteration(2).
        let iter2_started = events.iter().any(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(2)
        });
        assert!(
            iter2_started,
            "expected loopIterationStarted with iteration=2"
        );

        // Verify iteration 2 dispatches work (body nodes run again).
        let implement_work_v2 = format!("{}::loop::review-loop.2::work::implement", run_id);
        let has_iter2_implement = events.iter().any(|e| {
            e.event_type == "attemptCreated"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&implement_work_v2)
        });
        assert!(
            has_iter2_implement,
            "expected iteration 2 to dispatch implement work: {}",
            implement_work_v2
        );
    }

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn approve_decision_finishes_loop_and_run_succeeds() {
    // Drive to gate wait, write waitResolved(approved), run_loop emits
    // FinishLoop(approved) and the run succeeds.
    let run_dir = temp_run_dir("crl-approve");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-crl-approve";
    let def = code_review_loop_def();
    let workflow_json = serde_json::to_string(&def).unwrap();
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

    // First: drive to AwaitingWait.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }

    // Write waitResolved (approved).
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &decision_gate_id,
                    "resolution": "approved",
                    "by": "reviewer",
                    "comment": "lgtm",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Second: run_loop again — should approve and finish.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();

        // Verify loopFinished with approved resolution.
        let loop_finished = events.iter().any(|e| {
            e.event_type == "loopFinished"
                && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
                && e.payload.get("resolution").and_then(Value::as_str) == Some("approved")
        });
        assert!(
            loop_finished,
            "expected loopFinished with resolution=approved"
        );

        // Verify runSucceeded (since loop is the only top-level node).
        let run_succeeded = events.iter().any(|e| e.event_type == "runSucceeded");
        assert!(run_succeeded, "expected run to succeed after loop approval");

        // Verify loop output is produced (from implement node).
        let snap = read_snapshot(&rt).await.unwrap();
        let loop_output_key = format!("{}::work::review-loop", run_id);
        assert!(
            snap.outputs.contains_key(&loop_output_key),
            "expected loop output under {}",
            loop_output_key
        );
    }

    let _ = fs::remove_dir_all(&run_dir);
}

/// Hook that fails every subagent call — used to test body failure.
#[derive(Clone)]
struct FailingBodyHooks;

#[async_trait]
impl WorkflowExecutionHooks for FailingBodyHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        _resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Failed {
            error_code: "TestFailure".to_string(),
            error_class: "fatal".to_string(),
            error_message: "simulated body failure".to_string(),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        _resolved_input: Value,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Failed {
            error_code: "TestFailure".to_string(),
            error_class: "fatal".to_string(),
            error_message: "simulated body failure".to_string(),
            session: None,
        })
    }
}

#[tokio::test]
async fn body_failure_causes_loop_failed() {
    // When a body node fails (subagent returns Failed), the loop should
    // immediately fail with FinishLoop(failed).
    let run_dir = temp_run_dir("crl-body-fail");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-crl-body-fail";
    let def = code_review_loop_def();
    let workflow_json = serde_json::to_string(&def).unwrap();
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
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
        def,
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let mut hooks = FailingBodyHooks;
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

    let events = rt.log.read_all().unwrap();

    // Verify loopStarted and loopIterationStarted(1) were written.
    let loop_started = events.iter().any(|e| e.event_type == "loopStarted");
    assert!(loop_started, "expected loopStarted");

    let iter_started = events.iter().any(|e| {
        e.event_type == "loopIterationStarted"
            && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
    });
    assert!(iter_started, "expected loopIterationStarted(1)");

    // Body node (implement) should have failed.
    let implement_work_id = format!("{}::loop::review-loop.1::work::implement", run_id);
    let has_implement_fail = events.iter().any(|e| {
        e.event_type == "activityFailed"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&implement_work_id)
    });
    assert!(
        has_implement_fail,
        "expected implement work to fail: {}",
        implement_work_id
    );

    // Loop should have failed.
    let loop_failed = events.iter().any(|e| {
        e.event_type == "loopFinished"
            && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
            && e.payload.get("resolution").and_then(Value::as_str) == Some("failed")
    });
    assert!(
        loop_failed,
        "expected loopFinished with resolution=failed after body failure"
    );

    // Run should have failed (loop failed → run failed).
    assert!(
        matches!(result.reason, RunLoopStopReason::Terminal),
        "expected terminal; reason={:?}",
        result.reason
    );
    let run_failed = events.iter().any(|e| e.event_type == "runFailed");
    assert!(run_failed, "expected run to fail after loop body failure");

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn max_iterations_reject_causes_loop_failed() {
    // Reject at the last allowed iteration (maxIterations=3, iteration 3).
    // Should produce FinishLoop(failed) with MaxIterationsReached.
    let run_dir = temp_run_dir("crl-maxiter");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-crl-maxiter";
    let def = code_review_loop_def();
    let workflow_json = serde_json::to_string(&def).unwrap();
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Helper to drive to an open gate wait at a specific iteration.
    // We'll go through iterations 1→2→3, rejecting each time until the
    // final one causes failure.

    let iter1_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

    // Iteration 1: drive to gate, reject.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }
    // Reject iteration 1.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &iter1_gate_id,
                    "resolution": "rejected",
                    "by": "reviewer",
                    "comment": "redo",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }
    // Process rejection → iteration 2 should start.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();
        let iter2_started = events.iter().any(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(2)
        });
        assert!(iter2_started, "expected iteration 2 started");
    }

    // Iteration 2: drive to gate, reject.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }
    // Reject iteration 2.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let iter2_gate_id = format!("{}::loop::review-loop.2::gate::reviewDecision", run_id);
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &iter2_gate_id,
                    "resolution": "rejected",
                    "by": "reviewer",
                    "comment": "still no",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }
    // Process → iteration 3.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();
        let iter3_started = events.iter().any(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(3)
        });
        assert!(iter3_started, "expected iteration 3 started");
    }

    // Iteration 3: drive to gate, reject (max iterations hit).
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }
    // Reject iteration 3 (final iteration) → loop should fail.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let iter3_gate_id = format!("{}::loop::review-loop.3::gate::reviewDecision", run_id);
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &iter3_gate_id,
                    "resolution": "rejected",
                    "by": "reviewer",
                    "comment": "still no",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }
    // Process → loop and run should fail.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();

        // loopFinished with failed.
        let loop_failed = events.iter().any(|e| {
            e.event_type == "loopFinished"
                && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
                && e.payload.get("resolution").and_then(Value::as_str) == Some("failed")
                && e.payload.get("errorCode").and_then(Value::as_str)
                    == Some("MaxIterationsReached")
        });
        assert!(
            loop_failed,
            "expected loopFinished with failed/MaxIterationsReached"
        );

        assert!(
            matches!(result.reason, RunLoopStopReason::Terminal),
            "expected terminal; reason={:?}",
            result.reason
        );
        let run_failed = events.iter().any(|e| e.event_type == "runFailed");
        assert!(
            run_failed,
            "expected run to fail when max iterations reached"
        );
    }

    let _ = fs::remove_dir_all(&run_dir);
}
