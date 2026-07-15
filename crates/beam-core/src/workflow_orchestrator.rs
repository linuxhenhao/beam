use crate::workflow_snapshot::NodeStatus;
use crate::{HumanGate, RunSnapshotDTO, WorkflowDefinition, WorkflowNode, WorkflowOutputRef};

mod dag;
mod loops;
mod topology;

pub use topology::topological_order;

#[cfg(test)]
#[path = "workflow_orchestrator/tests.rs"]
mod tests;

#[derive(Debug, Clone, PartialEq)]
pub enum OrchestratorAction {
    DispatchGate {
        node_id: String,
        activity_id: String,
        human_gate: HumanGate,
    },
    DispatchWork {
        node_id: String,
        activity_id: String,
        node: WorkflowNode,
    },
    CompleteNodeSucceeded {
        node_id: String,
        last_activity_id: String,
        output_ref: Option<WorkflowOutputRef>,
    },
    CompleteNodeFailed {
        node_id: String,
        last_activity_id: String,
        error_class: String,
    },
    CompleteRunSucceeded {
        output_ref: WorkflowOutputRef,
        sink_node_id: String,
    },
    CompleteRunFailed {
        failed_node_id: String,
    },
    /// Begin executing a loop node (writes `loopStarted`).
    StartLoop {
        node_id: String,
        max_iterations: u64,
    },
    /// Begin a single iteration of a loop (writes `loopIterationStarted`).
    StartLoopIteration {
        node_id: String,
        iteration: u64,
    },
    /// Finish a single iteration of a loop (writes `loopIterationFinished`).
    FinishLoopIteration {
        node_id: String,
        iteration: u64,
        /// Resolution for this iteration: `approved`, `rejected`, `failed`, or `cancelled`.
        resolution: String,
        decision_activity_id: Option<String>,
        wait_resolved_event_id: Option<String>,
        by: Option<String>,
        comment: Option<String>,
        timed_out: Option<bool>,
    },
    /// Finish the entire loop node (writes `loopFinished`).
    FinishLoop {
        node_id: String,
        final_iteration: u64,
        /// Resolution for the overall loop: `approved`, `failed`, or `cancelled`.
        resolution: String,
        output_ref: Option<WorkflowOutputRef>,
        error_code: Option<String>,
        error_class: Option<String>,
    },
}

impl OrchestratorAction {
    pub fn is_dispatch(&self) -> bool {
        matches!(self, Self::DispatchGate { .. } | Self::DispatchWork { .. })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct AdvanceDecision {
    pub(super) actions: Vec<OrchestratorAction>,
    pub(super) is_succeeded: bool,
    pub(super) is_failed: bool,
}

pub fn decide_next_actions(
    snapshot: &RunSnapshotDTO,
    def: &WorkflowDefinition,
) -> Vec<OrchestratorAction> {
    if matches!(
        snapshot.run.status,
        crate::RunStatus::Succeeded | crate::RunStatus::Failed | crate::RunStatus::Cancelled
    ) {
        return Vec::new();
    }
    if snapshot.run.cancelled_run_intent.is_some() {
        return Vec::new();
    }

    let order = topology::topological_order(def);
    let body_owner = topology::build_body_owner_map(def);
    let mut actions = Vec::new();
    let mut failed_node_id: Option<String> = None;
    let mut pending_count: usize = 0;

    for node_id in order {
        if body_owner.contains_key(&node_id) {
            continue;
        }
        let Some(node) = def.nodes.get(&node_id) else {
            continue;
        };
        // ── Loop dispatch ──
        if let WorkflowNode::Loop(loop_node) = node {
            let deps_ok = topology::node_depends(node)
                .iter()
                .all(|dep| topology::dependency_is_succeeded(snapshot, dep));
            if !deps_ok {
                pending_count += 1;
                continue;
            }
            let advance = loops::decide_loop_advancement(snapshot, def, &node_id, loop_node);
            if advance.is_succeeded {
                actions.extend(advance.actions);
                continue;
            }
            if advance.is_failed {
                failed_node_id.get_or_insert(node_id.clone());
                actions.extend(advance.actions);
                continue;
            }
            if advance.actions.is_empty() {
                pending_count += 1;
            } else {
                actions.extend(advance.actions);
            }
            continue;
        }

        if let Some(node_state) = topology::node_state(snapshot, &node_id) {
            if matches!(
                node_state.status,
                NodeStatus::Succeeded | NodeStatus::Skipped | NodeStatus::Cancelled
            ) {
                continue;
            }
            if node_state.status == NodeStatus::Failed {
                failed_node_id.get_or_insert(node_id.clone());
                continue;
            }
        }

        let deps_ok = topology::node_depends(node)
            .iter()
            .all(|dep| topology::dependency_is_succeeded(snapshot, dep));
        if !deps_ok {
            pending_count += 1;
            continue;
        }

        let gate_id = topology::gate_activity_id(&snapshot.run.run_id, &node_id);
        let work_id = topology::work_activity_id(&snapshot.run.run_id, &node_id);
        let advance = dag::decide_node_advancement(snapshot, node, &node_id, &gate_id, &work_id);
        if advance.is_succeeded {
            actions.extend(advance.actions);
            continue;
        }
        if advance.is_failed {
            actions.extend(advance.actions);
            continue;
        }
        if advance.actions.is_empty() {
            pending_count += 1;
        } else {
            actions.extend(advance.actions);
        }
    }

    if actions.is_empty() {
        if let Some(node_id) = failed_node_id {
            return vec![OrchestratorAction::CompleteRunFailed {
                failed_node_id: node_id,
            }];
        }
        if pending_count == 0 {
            let sinks = topology::find_sinks(def);
            if sinks.len() == 1 {
                let sink_id = sinks[0].clone();
                let sink_output_id = format!("{}::work::{}", snapshot.run.run_id, sink_id);
                if let Some(output_ref) = snapshot.outputs.get(&sink_output_id) {
                    return vec![OrchestratorAction::CompleteRunSucceeded {
                        output_ref: output_ref.clone(),
                        sink_node_id: sink_id,
                    }];
                }
            }
        }
    }

    actions
}
