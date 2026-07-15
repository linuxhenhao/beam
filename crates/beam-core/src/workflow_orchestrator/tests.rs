use std::collections::BTreeMap;

use serde_json::Value;

use super::*;
use crate::workflow_definition::NodeBase;
use crate::workflow_snapshot::{ActivityStatus, NodeStatus};
use crate::{
    ActivityState, LoopIterationState, LoopIterationStatus, LoopNode, LoopSnapshotDTO, LoopStatus,
    NodeState, RunChatBinding, RunState, RunStatus, WorkflowOutputRef,
};

fn output_ref(name: &str) -> WorkflowOutputRef {
    WorkflowOutputRef {
        output_hash: format!("sha256:{name}"),
        output_path: format!("/tmp/{name}.json"),
        output_bytes: 4,
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    }
}

fn snapshot() -> RunSnapshotDTO {
    RunSnapshotDTO {
        run_id: "run-1".to_string(),
        run: RunState {
            run_id: "run-1".to_string(),
            status: RunStatus::Running,
            workflow_id: Some("flow-a".to_string()),
            revision_id: Some("rev-a".to_string()),
            initiator: Some("cli".to_string()),
            input: None,
            output: None,
            failed_node_id: None,
            root_cause_event_id: None,
            cancel_origin_event_id: None,
            bot_snapshots: None,
            cancelled_run_intent: None,
            cancelled_node_intents: BTreeMap::new(),
        },
        last_seq: 2,
        nodes: Vec::new(),
        activities: Vec::new(),
        loops: None,
        dangling: crate::DanglingSnapshot {
            activities: Vec::new(),
            effect_attempted: Vec::new(),
            waits: Vec::new(),
            wait_resolutions: Vec::new(),
            cancels: Vec::new(),
        },
        outputs: BTreeMap::new(),
        attempt_io: BTreeMap::new(),
        chat_binding: Some(RunChatBinding {
            chat_id: "chat-1".to_string(),
            lark_app_id: "app-1".to_string(),
        }),
        updated_at: 1,
    }
}

fn subagent_node(depends: &[&str]) -> WorkflowNode {
    WorkflowNode::Subagent(crate::SubagentNode {
        base: NodeBase {
            description: None,
            depends: Some(depends.iter().map(|s| s.to_string()).collect()),
            human_gate: None,
            retry_policy: None,
            timeout_ms: None,
            max_output_bytes: None,
            output_schema: None,
            unsafe_allow_ungated: None,
        },
        bot: "bot-a".to_string(),
        prompt: Value::String("hi".to_string()),
        working_dir: None,
        model_overrides: None,
        tool_policy: None,
    })
}

fn host_node(depends: &[&str]) -> WorkflowNode {
    WorkflowNode::HostExecutor(crate::HostExecutorNode {
        base: NodeBase {
            description: None,
            depends: Some(depends.iter().map(|s| s.to_string()).collect()),
            human_gate: None,
            retry_policy: None,
            timeout_ms: None,
            max_output_bytes: None,
            output_schema: None,
            unsafe_allow_ungated: None,
        },
        executor: "feishu-send".to_string(),
        input: Value::Null,
    })
}

fn gate_node(depends: &[&str]) -> WorkflowNode {
    WorkflowNode::Subagent(crate::SubagentNode {
        base: NodeBase {
            description: None,
            depends: Some(depends.iter().map(|s| s.to_string()).collect()),
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
        bot: "bot-a".to_string(),
        prompt: Value::String("hi".to_string()),
        working_dir: None,
        model_overrides: None,
        tool_policy: None,
    })
}

#[test]
fn decide_next_actions_dispatches_root_work_then_downstream() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("a".to_string(), subagent_node(&[])),
            ("b".to_string(), host_node(&["a"])),
        ]),
    };
    let mut snap = snapshot();
    let actions = decide_next_actions(&snap, &def);
    assert!(matches!(
        actions.as_slice(),
        [OrchestratorAction::DispatchWork { node_id, activity_id, .. }]
        if node_id == "a" && activity_id == "run-1::work::a"
    ));

    snap.nodes = vec![NodeState {
        node_id: "a".to_string(),
        status: NodeStatus::Succeeded,
        activity_id: Some("run-1::work::a".to_string()),
        retry_count: 0,
        next_attempt_at: None,
        error_class: None,
        condition_event_id: None,
        cancel_origin_event_id: None,
    }];
    snap.activities = vec![ActivityState {
        activity_id: "run-1::work::a".to_string(),
        attempts: vec![],
        status: ActivityStatus::Succeeded,
        current_attempt_id: None,
        owner_node_id: Some("a".to_string()),
    }];
    snap.outputs
        .insert("run-1::work::a".to_string(), output_ref("a"));
    let actions = decide_next_actions(&snap, &def);
    assert!(matches!(
        actions.as_slice(),
        [OrchestratorAction::DispatchWork { node_id, activity_id, .. }]
        if node_id == "b" && activity_id == "run-1::work::b"
    ));
}

#[test]
fn decide_next_actions_completes_simple_run_when_single_sink_has_output() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("a".to_string(), subagent_node(&[])),
            ("b".to_string(), host_node(&["a"])),
        ]),
    };
    let mut snap = snapshot();
    snap.nodes = vec![
        NodeState {
            node_id: "a".to_string(),
            status: NodeStatus::Succeeded,
            activity_id: Some("run-1::work::a".to_string()),
            retry_count: 0,
            next_attempt_at: None,
            error_class: None,
            condition_event_id: None,
            cancel_origin_event_id: None,
        },
        NodeState {
            node_id: "b".to_string(),
            status: NodeStatus::Succeeded,
            activity_id: Some("run-1::work::b".to_string()),
            retry_count: 0,
            next_attempt_at: None,
            error_class: None,
            condition_event_id: None,
            cancel_origin_event_id: None,
        },
    ];
    snap.activities = vec![ActivityState {
        activity_id: "run-1::work::b".to_string(),
        attempts: vec![],
        status: ActivityStatus::Succeeded,
        current_attempt_id: None,
        owner_node_id: Some("b".to_string()),
    }];
    snap.outputs
        .insert("run-1::work::b".to_string(), output_ref("b"));
    let actions = decide_next_actions(&snap, &def);
    assert!(matches!(
        actions.as_slice(),
        [OrchestratorAction::CompleteRunSucceeded { sink_node_id, .. }]
        if sink_node_id == "b"
    ));
}

#[test]
fn decide_next_actions_reports_node_failure_before_run_failure() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("a".to_string(), subagent_node(&[])),
            ("b".to_string(), host_node(&["a"])),
        ]),
    };
    let mut snap = snapshot();
    snap.nodes = vec![NodeState {
        node_id: "a".to_string(),
        status: NodeStatus::Failed,
        activity_id: Some("run-1::work::a".to_string()),
        retry_count: 0,
        next_attempt_at: None,
        error_class: Some("fatal".to_string()),
        condition_event_id: None,
        cancel_origin_event_id: None,
    }];
    let actions = decide_next_actions(&snap, &def);
    assert!(matches!(
        actions.as_slice(),
        [OrchestratorAction::CompleteRunFailed { failed_node_id }]
        if failed_node_id == "a"
    ));
}

#[test]
fn decide_next_actions_dispatches_gate_before_work() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([("a".to_string(), gate_node(&[]))]),
    };
    let snap = snapshot();
    let actions = decide_next_actions(&snap, &def);
    assert!(matches!(
        actions.as_slice(),
        [OrchestratorAction::DispatchGate { node_id, activity_id, .. }]
        if node_id == "a" && activity_id == "run-1::gate::a"
    ));
}

// ── Loop dispatch tests (Task 8.2) ──

fn decision_node(human_gate: HumanGate) -> WorkflowNode {
    WorkflowNode::Decision(crate::DecisionNode {
        base: NodeBase {
            description: None,
            depends: None,
            human_gate: Some(human_gate),
            retry_policy: None,
            timeout_ms: None,
            max_output_bytes: None,
            output_schema: None,
            unsafe_allow_ungated: None,
        },
    })
}

fn loop_node_with_body(body: &[&str], max_iterations: u64) -> WorkflowNode {
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
        max_iterations,
        body: body.iter().map(|s| s.to_string()).collect(),
        terminate: crate::LoopTerminate {
            node: body.last().unwrap_or(&"").to_string(),
            via: "humanGate".to_string(),
        },
        output: None,
    })
}

impl Default for HumanGate {
    fn default() -> Self {
        HumanGate {
            stage: "before".to_string(),
            prompt: Value::String("approve?".to_string()),
            approvers: None,
            deadline_ms: None,
            on_timeout: None,
        }
    }
}

#[test]
fn loop_start_when_deps_met_and_no_loop_state() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("d".to_string(), decision_node(HumanGate::default())),
            ("l".to_string(), loop_node_with_body(&["d"], 3)),
        ]),
    };
    let snap = snapshot();
    let actions = decide_next_actions(&snap, &def);
    assert!(
        actions.len() >= 2,
        "expected StartLoop and StartLoopIteration, got {:?}",
        actions
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, OrchestratorAction::StartLoop { .. }))
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        OrchestratorAction::StartLoopIteration { iteration: 1, .. }
    )));
}

#[test]
fn loop_with_succeeded_decision_gate_finishes_loop() {
    // Create a snapshot where:
    // - The loop has been started (loop=Running, iteration 1 Running)
    // - The body node (decision) gate activity has Succeeded
    // The orchestrator should emit FinishLoopIteration(approved) + FinishLoop(approved).
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("d".to_string(), decision_node(HumanGate::default())),
            ("l".to_string(), loop_node_with_body(&["d"], 3)),
        ]),
    };

    let mut snap = snapshot();
    let run_id = &snap.run.run_id;
    let decision_gate_id = format!("{run_id}::loop::l.1::gate::d");

    // Set up loop state
    snap.loops = Some(BTreeMap::from([(
        "l".to_string(),
        LoopSnapshotDTO {
            loop_id: "l".to_string(),
            status: LoopStatus::Running,
            iteration: 1,
            max_iterations: 3,
            iterations: vec![LoopIterationState {
                iteration: 1,
                status: LoopIterationStatus::Running,
                body_activity_ids: vec![decision_gate_id.clone()],
                decision_activity_id: None,
                wait_resolved_event_id: None,
                decision_by: None,
                decision_comment: None,
                timed_out: None,
            }],
            output: None,
            error_code: None,
            error_class: None,
        },
    )]));

    // Set the gate activity as Succeeded
    snap.activities = vec![ActivityState {
        activity_id: decision_gate_id.clone(),
        attempts: vec![],
        status: ActivityStatus::Succeeded,
        current_attempt_id: None,
        owner_node_id: Some("d".to_string()),
    }];

    let actions = decide_next_actions(&snap, &def);
    assert!(
        actions.iter().any(|a| matches!(a, OrchestratorAction::FinishLoopIteration { resolution, .. } if resolution == "approved")),
        "expected FinishLoopIteration(approved), got: {actions:?}"
    );
    assert!(
        actions.iter().any(|a| matches!(a, OrchestratorAction::FinishLoop { resolution, .. } if resolution == "approved")),
        "expected FinishLoop(approved), got: {actions:?}"
    );
}

#[test]
fn loop_with_failed_decision_gate_max_iter_reached_fails_loop() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("d".to_string(), decision_node(HumanGate::default())),
            ("l".to_string(), loop_node_with_body(&["d"], 2)),
        ]),
    };

    let mut snap = snapshot();
    let run_id = &snap.run.run_id;
    let decision_gate_id = format!("{run_id}::loop::l.2::gate::d");

    // Loop at iteration 2 (max_iterations=2) with Failed gate
    snap.loops = Some(BTreeMap::from([(
        "l".to_string(),
        LoopSnapshotDTO {
            loop_id: "l".to_string(),
            status: LoopStatus::Running,
            iteration: 2,
            max_iterations: 2,
            iterations: vec![LoopIterationState {
                iteration: 2,
                status: LoopIterationStatus::Running,
                body_activity_ids: vec![decision_gate_id.clone()],
                decision_activity_id: None,
                wait_resolved_event_id: None,
                decision_by: None,
                decision_comment: None,
                timed_out: None,
            }],
            output: None,
            error_code: None,
            error_class: None,
        },
    )]));

    snap.activities = vec![ActivityState {
        activity_id: decision_gate_id,
        attempts: vec![],
        status: ActivityStatus::Failed,
        current_attempt_id: None,
        owner_node_id: Some("d".to_string()),
    }];

    let actions = decide_next_actions(&snap, &def);
    assert!(
        actions.iter().any(|a| matches!(a, OrchestratorAction::FinishLoop { resolution, .. } if resolution == "failed")),
        "expected FinishLoop(failed), got: {actions:?}"
    );
    assert!(
        actions.iter().any(|a| matches!(a, OrchestratorAction::FinishLoop { error_code, .. } if error_code.as_deref() == Some("MaxIterationsReached"))),
        "expected MaxIterationsReached, got: {actions:?}"
    );
}

#[test]
fn loop_with_rejected_decision_below_max_starts_next_iteration() {
    let def = WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([
            ("d".to_string(), decision_node(HumanGate::default())),
            ("l".to_string(), loop_node_with_body(&["d"], 3)),
        ]),
    };

    let mut snap = snapshot();
    let run_id = &snap.run.run_id;
    let decision_gate_id = format!("{run_id}::loop::l.1::gate::d");

    snap.loops = Some(BTreeMap::from([(
        "l".to_string(),
        LoopSnapshotDTO {
            loop_id: "l".to_string(),
            status: LoopStatus::Running,
            iteration: 1,
            max_iterations: 3,
            iterations: vec![LoopIterationState {
                iteration: 1,
                status: LoopIterationStatus::Running,
                body_activity_ids: vec![decision_gate_id.clone()],
                decision_activity_id: None,
                wait_resolved_event_id: None,
                decision_by: None,
                decision_comment: None,
                timed_out: None,
            }],
            output: None,
            error_code: None,
            error_class: None,
        },
    )]));

    snap.activities = vec![ActivityState {
        activity_id: decision_gate_id,
        attempts: vec![],
        status: ActivityStatus::Failed,
        current_attempt_id: None,
        owner_node_id: Some("d".to_string()),
    }];

    let actions = decide_next_actions(&snap, &def);
    assert!(
        actions.iter().any(|a| matches!(a, OrchestratorAction::FinishLoopIteration { resolution, .. } if resolution == "rejected")),
        "expected FinishLoopIteration(rejected), got: {actions:?}"
    );
    assert!(
        actions.iter().any(|a| matches!(
            a,
            OrchestratorAction::StartLoopIteration { iteration: 2, .. }
        )),
        "expected StartLoopIteration(2), got: {actions:?}"
    );
}
