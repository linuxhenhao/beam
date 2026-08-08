use crate::workflow_snapshot::ActivityStatus;
use crate::{HumanGate, WorkflowNode};

use super::topology::{activity_state, derive_error_class};
use super::{AdvanceDecision, OrchestratorAction};

pub(super) fn decide_node_advancement(
    snapshot: &crate::RunSnapshotDTO,
    node: &WorkflowNode,
    node_id: &str,
    gate_activity_id: &str,
    work_activity_id: &str,
) -> AdvanceDecision {
    match node {
        WorkflowNode::Decision(node) => {
            let Some(gate) = activity_state(snapshot, gate_activity_id) else {
                let Some(gate_cfg) = node.base.human_gate.as_ref() else {
                    return AdvanceDecision {
                        actions: Vec::new(),
                        is_succeeded: false,
                        is_failed: false,
                    };
                };
                return AdvanceDecision {
                    actions: vec![OrchestratorAction::DispatchGate {
                        node_id: node_id.to_string(),
                        activity_id: gate_activity_id.to_string(),
                        human_gate: gate_cfg.clone(),
                    }],
                    is_succeeded: false,
                    is_failed: false,
                };
            };
            match gate.status {
                ActivityStatus::Succeeded => AdvanceDecision {
                    actions: vec![OrchestratorAction::CompleteNodeSucceeded {
                        node_id: node_id.to_string(),
                        last_activity_id: gate_activity_id.to_string(),
                        output_ref: None,
                    }],
                    is_succeeded: true,
                    is_failed: false,
                },
                ActivityStatus::Failed | ActivityStatus::TimedOut => AdvanceDecision {
                    actions: vec![OrchestratorAction::CompleteNodeFailed {
                        node_id: node_id.to_string(),
                        last_activity_id: gate_activity_id.to_string(),
                        error_class: if gate.status == ActivityStatus::TimedOut {
                            "userFault".to_string()
                        } else {
                            derive_error_class(gate)
                        },
                    }],
                    is_succeeded: false,
                    is_failed: true,
                },
                _ => AdvanceDecision {
                    actions: Vec::new(),
                    is_succeeded: false,
                    is_failed: false,
                },
            }
        }
        WorkflowNode::Loop(_) => AdvanceDecision {
            actions: Vec::new(),
            is_succeeded: false,
            is_failed: false,
        },
        WorkflowNode::Subagent(node) => decide_plain_node(
            snapshot,
            node.base.human_gate.as_ref(),
            node_id,
            gate_activity_id,
            work_activity_id,
            WorkflowNode::Subagent(node.clone()),
        ),
        WorkflowNode::HostExecutor(node) => decide_plain_node(
            snapshot,
            node.base.human_gate.as_ref(),
            node_id,
            gate_activity_id,
            work_activity_id,
            WorkflowNode::HostExecutor(node.clone()),
        ),
    }
}

fn decide_plain_node(
    snapshot: &crate::RunSnapshotDTO,
    human_gate: Option<&HumanGate>,
    node_id: &str,
    gate_activity_id: &str,
    work_activity_id: &str,
    node: WorkflowNode,
) -> AdvanceDecision {
    if let Some(gate_cfg) = human_gate {
        let Some(gate) = activity_state(snapshot, gate_activity_id) else {
            return AdvanceDecision {
                actions: vec![OrchestratorAction::DispatchGate {
                    node_id: node_id.to_string(),
                    activity_id: gate_activity_id.to_string(),
                    human_gate: gate_cfg.clone(),
                }],
                is_succeeded: false,
                is_failed: false,
            };
        };
        match gate.status {
            ActivityStatus::Failed | ActivityStatus::TimedOut => {
                return AdvanceDecision {
                    actions: vec![OrchestratorAction::CompleteNodeFailed {
                        node_id: node_id.to_string(),
                        last_activity_id: gate_activity_id.to_string(),
                        error_class: if gate.status == ActivityStatus::TimedOut {
                            "userFault".to_string()
                        } else {
                            derive_error_class(gate)
                        },
                    }],
                    is_succeeded: false,
                    is_failed: true,
                };
            }
            ActivityStatus::Succeeded => {}
            _ => {
                return AdvanceDecision {
                    actions: Vec::new(),
                    is_succeeded: false,
                    is_failed: false,
                };
            }
        }
    }

    let Some(work) = activity_state(snapshot, work_activity_id) else {
        return AdvanceDecision {
            actions: vec![OrchestratorAction::DispatchWork {
                node_id: node_id.to_string(),
                activity_id: work_activity_id.to_string(),
                node: Box::new(node),
            }],
            is_succeeded: false,
            is_failed: false,
        };
    };
    match work.status {
        ActivityStatus::Succeeded => {
            let output_ref = snapshot.outputs.get(work_activity_id).cloned();
            AdvanceDecision {
                actions: vec![OrchestratorAction::CompleteNodeSucceeded {
                    node_id: node_id.to_string(),
                    last_activity_id: work_activity_id.to_string(),
                    output_ref,
                }],
                is_succeeded: true,
                is_failed: false,
            }
        }
        ActivityStatus::Failed | ActivityStatus::TimedOut => AdvanceDecision {
            actions: vec![OrchestratorAction::CompleteNodeFailed {
                node_id: node_id.to_string(),
                last_activity_id: work_activity_id.to_string(),
                error_class: if work.status == ActivityStatus::TimedOut {
                    "retryable".to_string()
                } else {
                    derive_error_class(work)
                },
            }],
            is_succeeded: false,
            is_failed: true,
        },
        _ => AdvanceDecision {
            actions: Vec::new(),
            is_succeeded: false,
            is_failed: false,
        },
    }
}
