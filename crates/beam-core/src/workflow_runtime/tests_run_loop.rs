use std::collections::BTreeMap;
use std::fs;

use serde_json::Value;

use crate::workflow_definition::{HostExecutorNode, NodeBase, SubagentNode};
use crate::{EventDraft, EventLog, RunChatBinding, WorkflowActor, WorkflowNode};

use super::test_common::{FakeHooks, temp_run_dir, workflow_def};
use super::*;

#[tokio::test]
async fn run_loop_stops_when_progress_is_exhausted() {
    let run_dir = temp_run_dir("loop");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
    let run_id = "run-1";
    crate::bootstrap_workflow_run(
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
    let result = run_loop(&mut rt, &mut hooks, 3, 1).await.unwrap();
    assert!(matches!(
        result.reason,
        RunLoopStopReason::Terminal | RunLoopStopReason::NoProgress
    ));
    assert!(result.ticks > 0);
    let _ = fs::remove_dir_all(&run_dir);
}
#[tokio::test]
async fn open_wait_makes_run_loop_return_awaiting_wait() {
    let run_dir = temp_run_dir("open-wait");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-open-wait";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-open-wait","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-schedule","input":{"prompt":"approve"},"humanGate":{"stage":"gate","prompt":"approve?","approvers":["admin"]}},"sink":{"type":"subagent","bot":"bot-a","prompt":"done","depends":["gate"]}}}"#,
            expected_workflow_id: Some("flow-open-wait"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write a waitCreated event directly — this simulates a workflow that
    // dispatched a gate and created a wait but no resolution has arrived yet.
    let gate_activity_id = format!("{}::gate::gate", run_id);
    let gate_attempt_id = format!("{}::gate::gate::att-1", run_id);
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "gate",
                    "activityId": &gate_activity_id,
                    "attemptId": &gate_attempt_id,
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:aa",
                        "outputPath": "/tmp/aa",
                        "outputBytes": 2,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json"
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "activityId": &gate_activity_id,
                    "attemptId": &gate_attempt_id,
                    "nodeId": "gate",
                    "waitKind": "human-gate",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-open-wait".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([
                (
                    "gate".to_string(),
                    WorkflowNode::HostExecutor(HostExecutorNode {
                        base: NodeBase {
                            description: None,
                            depends: None,
                            human_gate: Some(crate::workflow_definition::HumanGate {
                                stage: "gate".to_string(),
                                prompt: Value::String("approve?".to_string()),
                                approvers: Some(vec!["admin".to_string()]),
                                deadline_ms: None,
                                on_timeout: None,
                            }),
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: Some(true),
                        },
                        executor: "beam-schedule".to_string(),
                        input: serde_json::json!({"prompt":"approve"}),
                    }),
                ),
                (
                    "sink".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
                        base: NodeBase {
                            description: None,
                            depends: Some(vec!["gate".to_string()]),
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                        bot: "bot-a".to_string(),
                        prompt: Value::String("done".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    // Verify snapshot shows open wait (in waits, not in wait_resolutions)
    let snap = read_snapshot(&rt).await.unwrap();
    assert!(
        !snap.dangling.waits.is_empty(),
        "expected open wait in dangling.waits, got {:?}",
        snap.dangling.waits
    );
    assert!(
        snap.dangling.wait_resolutions.is_empty(),
        "expected no wait_resolutions for open wait"
    );

    let mut hooks = FakeHooks;
    let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
    assert_eq!(
        result.reason,
        RunLoopStopReason::AwaitingWait,
        "expected AwaitingWait for open wait, got {:?}",
        result.reason
    );

    let _ = fs::remove_dir_all(&run_dir);
}

/// Verify that a resolved wait without a terminal event (dangling
/// wait_resolution) gets materialized during run_loop's recovery phase,
/// allowing the workflow to continue.
#[tokio::test]
async fn run_loop_materializes_terminal_for_resolved_wait() {
    let run_dir = temp_run_dir("resolved-wait");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-resolved-wait";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-resolved-wait","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-schedule","input":{"prompt":"approve"},"humanGate":{"stage":"gate","prompt":"approve?","approvers":["admin"]}},"sink":{"type":"subagent","bot":"bot-a","prompt":"done","depends":["gate"]}}}"#,
            expected_workflow_id: Some("flow-resolved-wait"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write attemptCreated + waitCreated + waitResolved (approved) but NO terminal.
    // This simulates a crash after the wait was resolved but before the terminal
    // event was written.
    let gate_activity_id = format!("{}::gate::gate", run_id);
    let gate_attempt_id = format!("{}::gate::gate::att-1", run_id);
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "gate",
                    "activityId": &gate_activity_id,
                    "attemptId": &gate_attempt_id,
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:aa",
                        "outputPath": "/tmp/aa",
                        "outputBytes": 2,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json"
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "activityId": &gate_activity_id,
                    "attemptId": &gate_attempt_id,
                    "nodeId": "gate",
                    "waitKind": "human-gate",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &gate_activity_id,
                    "resolution": "approved",
                    "by": "admin",
                    "comment": "go ahead",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-resolved-wait".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([
                (
                    "gate".to_string(),
                    WorkflowNode::HostExecutor(HostExecutorNode {
                        base: NodeBase {
                            description: None,
                            depends: None,
                            human_gate: Some(crate::workflow_definition::HumanGate {
                                stage: "gate".to_string(),
                                prompt: Value::String("approve?".to_string()),
                                approvers: Some(vec!["admin".to_string()]),
                                deadline_ms: None,
                                on_timeout: None,
                            }),
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: Some(true),
                        },
                        executor: "beam-schedule".to_string(),
                        input: serde_json::json!({"prompt":"approve"}),
                    }),
                ),
                (
                    "sink".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
                        base: NodeBase {
                            description: None,
                            depends: Some(vec!["gate".to_string()]),
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                        bot: "bot-a".to_string(),
                        prompt: Value::String("done".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    // Verify snapshot shows resolved wait in wait_resolutions, NOT in waits
    let snap = read_snapshot(&rt).await.unwrap();
    assert!(
        snap.dangling.waits.is_empty(),
        "expected no open waits after resolution, got {:?}",
        snap.dangling.waits
    );
    assert!(
        !snap.dangling.wait_resolutions.is_empty(),
        "expected resolved wait in wait_resolutions, got {:?}",
        snap.dangling.wait_resolutions
    );
    assert_eq!(
        snap.dangling.wait_resolutions,
        vec![gate_activity_id.clone()],
        "expected gate activity in wait_resolutions"
    );

    let mut hooks = FakeHooks;
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

    // run_loop should have materialized the terminal event (activitySucceeded
    // for approved wait), allowing the orchestrator to proceed past the gate.
    let final_snap = read_snapshot(&rt).await.unwrap();
    assert!(
        final_snap.dangling.wait_resolutions.is_empty(),
        "expected no remaining wait_resolutions after recovery, got {:?}",
        final_snap.dangling.wait_resolutions
    );

    let events = rt.log.read_all().unwrap();
    let recovered_terminal = events.iter().any(|e| {
        e.event_type == "activitySucceeded"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&gate_activity_id)
            && e.actor == WorkflowActor::Scheduler
    });
    assert!(
        recovered_terminal,
        "expected a scheduler activitySucceeded for the gate after wait resolution recovery"
    );

    // The loop should either have progressed (ticks > 0) or stopped at terminal.
    assert!(
        result.ticks > 0 || matches!(result.reason, RunLoopStopReason::Terminal),
        "expected progress after wait resolution recovery; reason={:?} ticks={}",
        result.reason,
        result.ticks
    );

    let _ = fs::remove_dir_all(&run_dir);
}

/// Verify that cancelRequested (run) propagation writes activityCanceled
/// for an open human-gate activity before writing runCanceled.
#[tokio::test]
async fn cancel_propagation_writes_activity_canceled_before_run_canceled() {
    let run_dir = temp_run_dir("cancel-propagate");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-cancel-propagate";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-cancel-propagate","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-schedule","input":{"prompt":"approve"},"humanGate":{"stage":"gate","prompt":"approve?","approvers":["admin"]}},"sink":{"type":"subagent","bot":"bot-a","prompt":"done","depends":["gate"]}}}"#,
            expected_workflow_id: Some("flow-cancel-propagate"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // First run_loop tick: dispatches the human-gate (creates a wait).
    let gate_activity_id = format!("{}::gate::gate", run_id);
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-cancel-propagate".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "gate".to_string(),
                        WorkflowNode::HostExecutor(HostExecutorNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: Some(crate::workflow_definition::HumanGate {
                                    stage: "gate".to_string(),
                                    prompt: Value::String("approve?".to_string()),
                                    approvers: Some(vec!["admin".to_string()]),
                                    deadline_ms: None,
                                    on_timeout: None,
                                }),
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: Some(true),
                            },
                            executor: "beam-schedule".to_string(),
                            input: serde_json::json!({"prompt":"approve"}),
                        }),
                    ),
                    (
                        "sink".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: Some(vec!["gate".to_string()]),
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("done".to_string()),
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
        let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }

    // Write cancelRequested (run).
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = crate::request_cancel(
            &mut log,
            crate::RequestCancelInput {
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

    // Second run_loop: should propagate cancel (activityCanceled → runCanceled).
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-cancel-propagate".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "gate".to_string(),
                        WorkflowNode::HostExecutor(HostExecutorNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: Some(crate::workflow_definition::HumanGate {
                                    stage: "gate".to_string(),
                                    prompt: Value::String("approve?".to_string()),
                                    approvers: Some(vec!["admin".to_string()]),
                                    deadline_ms: None,
                                    on_timeout: None,
                                }),
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: Some(true),
                            },
                            executor: "beam-schedule".to_string(),
                            input: serde_json::json!({"prompt":"approve"}),
                        }),
                    ),
                    (
                        "sink".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: Some(vec!["gate".to_string()]),
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("done".to_string()),
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
        let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::Terminal);
    }

    // Verify event order: activityCanceled appears before runCanceled.
    let log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let events = log.read_all().unwrap();
    let has_cancel_requested = events.iter().any(|e| e.event_type == "cancelRequested");
    assert!(has_cancel_requested, "should have cancelRequested");

    // Find positions of activityCanceled (for the gate) and runCanceled.
    let mut activity_canceled_pos: Option<usize> = None;
    let mut run_canceled_pos: Option<usize> = None;
    for (i, e) in events.iter().enumerate() {
        if e.event_type == "activityCanceled"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&gate_activity_id)
        {
            activity_canceled_pos = Some(i);
        }
        if e.event_type == "runCanceled" {
            run_canceled_pos = Some(i);
        }
    }

    assert!(
        activity_canceled_pos.is_some(),
        "should have activityCanceled for the gate activity"
    );
    assert!(run_canceled_pos.is_some(), "should have runCanceled");
    assert!(
        activity_canceled_pos.unwrap() < run_canceled_pos.unwrap(),
        "activityCanceled ({}) must appear before runCanceled ({})",
        activity_canceled_pos.unwrap(),
        run_canceled_pos.unwrap()
    );

    let _ = fs::remove_dir_all(&run_dir);
}

/// Verify that cancel propagation is idempotent: running check_pending_cancels
/// again on an already-cancelled run does not write duplicate events.
#[tokio::test]
async fn cancel_propagation_is_idempotent_after_run_is_cancelled() {
    let run_dir = temp_run_dir("cancel-idempotent");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-cancel-idempotent";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-cancel-idem","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello"}}}"#,
            expected_workflow_id: Some("flow-cancel-idem"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Write cancelRequested before any dispatch.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = crate::request_cancel(
            &mut log,
            crate::RequestCancelInput {
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

    // First run_loop: should write runCanceled.
    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-cancel-idem".to_string(),
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
                        prompt: Value::String("hello".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                )]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::Terminal);
    }

    // Second run_loop: should be idempotent (no duplicate runCanceled).
    let run_canceled_count_before: usize;
    {
        let log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let events = log.read_all().unwrap();
        run_canceled_count_before = events
            .iter()
            .filter(|e| e.event_type == "runCanceled")
            .count();
        assert_eq!(
            run_canceled_count_before, 1,
            "should have exactly 1 runCanceled after first propagation"
        );
    }

    {
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-cancel-idem".to_string(),
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
                        prompt: Value::String("hello".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                )]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let _ = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
    }

    let log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let events = log.read_all().unwrap();
    let run_canceled_count_after = events
        .iter()
        .filter(|e| e.event_type == "runCanceled")
        .count();
    assert_eq!(
        run_canceled_count_after, 1,
        "second run_loop should not produce duplicate runCanceled"
    );

    let _ = fs::remove_dir_all(&run_dir);
}
