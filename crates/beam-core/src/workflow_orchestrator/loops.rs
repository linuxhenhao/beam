use crate::workflow_snapshot::ActivityStatus;
use crate::{
    LoopIterationStatus, LoopNode, LoopStatus, RunSnapshotDTO, WorkflowDefinition, WorkflowNode,
};

use super::topology::{
    activity_state, body_topological_order, dependency_is_succeeded, loop_gate_activity_id,
    loop_work_activity_id, node_depends, node_human_gate,
};
use super::{AdvanceDecision, OrchestratorAction};

/// Extract wait-resolution metadata from a gate activity for populating
/// `FinishLoopIteration` payload fields.
fn extract_wait_resolution_meta(
    snapshot: &RunSnapshotDTO,
    gate_activity_id: &str,
) -> (Option<String>, Option<String>, Option<String>, Option<bool>) {
    let Some(activity) = snapshot
        .activities
        .iter()
        .find(|a| a.activity_id == gate_activity_id)
    else {
        return (None, None, None, None);
    };
    let Some(latest) = activity.attempts.last() else {
        return (None, None, None, None);
    };
    let Some(wait) = latest.wait.as_ref() else {
        return (None, None, None, None);
    };
    let Some(resolution) = wait.resolution.as_ref() else {
        return (None, None, None, None);
    };
    let timed_out = match resolution.kind.as_str() {
        "deadlineExceeded" => Some(true),
        _ => Some(false),
    };
    (
        resolution.event_id.clone(),
        resolution.by.clone(),
        resolution.comment.clone(),
        timed_out,
    )
}

/// Decide the state-transition actions for a loop node.
///
/// Checks whether the loop should start, dispatch body nodes for the running
/// iteration, or transition to the next / final state.
pub(super) fn decide_loop_advancement(
    snapshot: &RunSnapshotDTO,
    def: &WorkflowDefinition,
    loop_id: &str,
    loop_node: &LoopNode,
) -> AdvanceDecision {
    let run_id = &snapshot.run.run_id;
    let loop_state = snapshot.loops.as_ref().and_then(|loops| loops.get(loop_id));

    match loop_state {
        None => {
            // Loop hasn't started → StartLoop + StartLoopIteration(1)
            AdvanceDecision {
                actions: vec![
                    OrchestratorAction::StartLoop {
                        node_id: loop_id.to_string(),
                        max_iterations: loop_node.max_iterations,
                    },
                    OrchestratorAction::StartLoopIteration {
                        node_id: loop_id.to_string(),
                        iteration: 1,
                    },
                ],
                is_succeeded: false,
                is_failed: false,
            }
        }
        Some(ls) => match ls.status {
            LoopStatus::Running => {
                // Find the currently-running iteration.
                let running = ls
                    .iterations
                    .iter()
                    .find(|it| matches!(it.status, LoopIterationStatus::Running));
                match running {
                    Some(iter) => process_loop_iteration_body(
                        snapshot,
                        def,
                        run_id,
                        loop_id,
                        loop_node,
                        iter.iteration,
                    ),
                    None => {
                        // No running iteration but loop is Running — safety:
                        // if the last iteration was just finished (rejected), a
                        // new StartLoopIteration was already emitted.  Fallback
                        // to checking whether we should start the next.
                        let last_iter = ls.iteration;
                        if last_iter < loop_node.max_iterations {
                            AdvanceDecision {
                                actions: vec![OrchestratorAction::StartLoopIteration {
                                    node_id: loop_id.to_string(),
                                    iteration: last_iter + 1,
                                }],
                                is_succeeded: false,
                                is_failed: false,
                            }
                        } else {
                            AdvanceDecision {
                                actions: vec![],
                                is_succeeded: false,
                                is_failed: false,
                            }
                        }
                    }
                }
            }
            LoopStatus::Succeeded => AdvanceDecision {
                actions: vec![],
                is_succeeded: true,
                is_failed: false,
            },
            LoopStatus::Failed | LoopStatus::Cancelled => AdvanceDecision {
                actions: vec![],
                is_succeeded: false,
                is_failed: true,
            },
        },
    }
}

/// Process the body nodes of a single loop iteration.
///
/// Walks body nodes in topological order.  Returns the first actionable
/// decision: dispatch gate/work, finish the iteration, or finish the loop.
fn process_loop_iteration_body(
    snapshot: &RunSnapshotDTO,
    def: &WorkflowDefinition,
    run_id: &str,
    loop_id: &str,
    loop_node: &LoopNode,
    iteration: u64,
) -> AdvanceDecision {
    let body_order = body_topological_order(def, &loop_node.body);
    let terminate_node_id = &loop_node.terminate.node;

    for node_id in &body_order {
        let Some(node) = def.nodes.get(node_id) else {
            continue;
        };

        // Check if this node's intra-body depends are met.
        let deps_ok = node_depends(node).iter().all(|dep| {
            if loop_node.body.contains(dep) {
                // Intra-body dependency → check loop-scoped work activity.
                let work_id = loop_work_activity_id(run_id, loop_id, iteration, dep);
                activity_state(snapshot, &work_id)
                    .map(|a| a.status == ActivityStatus::Succeeded)
                    .unwrap_or(false)
            } else {
                // External dependency → check globally.
                dependency_is_succeeded(snapshot, dep)
            }
        });
        if !deps_ok {
            return AdvanceDecision {
                actions: vec![],
                is_succeeded: false,
                is_failed: false,
            };
        }

        let is_terminate = node_id == terminate_node_id;

        if is_terminate {
            // ── Terminate / decision node ──
            match node {
                WorkflowNode::Decision(decision_node) => {
                    let gate_cfg = match decision_node.base.human_gate.as_ref() {
                        Some(cfg) => cfg,
                        None => {
                            // Decision node without humanGate — shouldn't happen
                            // per validation, but skip silently.
                            continue;
                        }
                    };
                    let gate_id = loop_gate_activity_id(run_id, loop_id, iteration, node_id);
                    let Some(gate) = activity_state(snapshot, &gate_id) else {
                        return AdvanceDecision {
                            actions: vec![OrchestratorAction::DispatchGate {
                                node_id: node_id.clone(),
                                activity_id: gate_id,
                                human_gate: gate_cfg.clone(),
                            }],
                            is_succeeded: false,
                            is_failed: false,
                        };
                    };
                    match gate.status {
                        ActivityStatus::Succeeded => {
                            // Approved → iteration + loop succeeded.
                            let loop_output = loop_node.output.as_ref().and_then(|out| {
                                let source_work_id =
                                    loop_work_activity_id(run_id, loop_id, iteration, &out.from);
                                snapshot.outputs.get(&source_work_id).cloned()
                            });
                            let (wait_resolved_event_id, by, comment, timed_out) =
                                extract_wait_resolution_meta(snapshot, &gate_id);
                            return AdvanceDecision {
                                actions: vec![
                                    OrchestratorAction::FinishLoopIteration {
                                        node_id: loop_id.to_string(),
                                        iteration,
                                        resolution: "approved".to_string(),
                                        decision_activity_id: Some(gate_id),
                                        wait_resolved_event_id,
                                        by,
                                        comment,
                                        timed_out,
                                    },
                                    OrchestratorAction::FinishLoop {
                                        node_id: loop_id.to_string(),
                                        final_iteration: iteration,
                                        resolution: "approved".to_string(),
                                        output_ref: loop_output,
                                        error_code: None,
                                        error_class: None,
                                    },
                                ],
                                is_succeeded: false,
                                is_failed: false,
                            };
                        }
                        ActivityStatus::Failed | ActivityStatus::TimedOut => {
                            // Rejected/timed-out → next iteration or loop failed.
                            if iteration >= loop_node.max_iterations {
                                // Max iterations reached → loop failed.
                                let (wait_resolved_event_id, by, comment, timed_out) =
                                    extract_wait_resolution_meta(snapshot, &gate_id);
                                return AdvanceDecision {
                                    actions: vec![
                                        OrchestratorAction::FinishLoopIteration {
                                            node_id: loop_id.to_string(),
                                            iteration,
                                            resolution: "rejected".to_string(),
                                            decision_activity_id: Some(gate_id),
                                            wait_resolved_event_id,
                                            by,
                                            comment,
                                            timed_out,
                                        },
                                        OrchestratorAction::FinishLoop {
                                            node_id: loop_id.to_string(),
                                            final_iteration: iteration,
                                            resolution: "failed".to_string(),
                                            output_ref: None,
                                            error_code: Some("MaxIterationsReached".to_string()),
                                            error_class: Some("fatal".to_string()),
                                        },
                                    ],
                                    is_succeeded: false,
                                    is_failed: false,
                                };
                            }
                            // Start next iteration.
                            let (wait_resolved_event_id, by, comment, timed_out) =
                                extract_wait_resolution_meta(snapshot, &gate_id);
                            return AdvanceDecision {
                                actions: vec![
                                    OrchestratorAction::FinishLoopIteration {
                                        node_id: loop_id.to_string(),
                                        iteration,
                                        resolution: "rejected".to_string(),
                                        decision_activity_id: Some(gate_id),
                                        wait_resolved_event_id,
                                        by,
                                        comment,
                                        timed_out,
                                    },
                                    OrchestratorAction::StartLoopIteration {
                                        node_id: loop_id.to_string(),
                                        iteration: iteration + 1,
                                    },
                                ],
                                is_succeeded: false,
                                is_failed: false,
                            };
                        }
                        _ => {
                            // Gate is in progress (Waiting, etc.).
                            return AdvanceDecision {
                                actions: vec![],
                                is_succeeded: false,
                                is_failed: false,
                            };
                        }
                    }
                }
                _ => {
                    // Terminate node must be Decision type.  Skip for now
                    // (full validation in Task 8.3).
                    continue;
                }
            }
        } else {
            // ── Regular body node (subagent / hostExecutor) ──

            // Process gate if required.
            if let Some(human_gate) = node_human_gate(node) {
                let gate_id = loop_gate_activity_id(run_id, loop_id, iteration, node_id);
                let Some(gate) = activity_state(snapshot, &gate_id) else {
                    return AdvanceDecision {
                        actions: vec![OrchestratorAction::DispatchGate {
                            node_id: node_id.clone(),
                            activity_id: gate_id,
                            human_gate: human_gate.clone(),
                        }],
                        is_succeeded: false,
                        is_failed: false,
                    };
                };
                match gate.status {
                    ActivityStatus::Failed | ActivityStatus::TimedOut => {
                        // Gate failed → body node failed → loop failed.
                        return AdvanceDecision {
                            actions: vec![
                                OrchestratorAction::FinishLoopIteration {
                                    node_id: loop_id.to_string(),
                                    iteration,
                                    resolution: "failed".to_string(),
                                    decision_activity_id: None,
                                    wait_resolved_event_id: None,
                                    by: None,
                                    comment: None,
                                    timed_out: None,
                                },
                                OrchestratorAction::FinishLoop {
                                    node_id: loop_id.to_string(),
                                    final_iteration: iteration,
                                    resolution: "failed".to_string(),
                                    output_ref: None,
                                    error_code: Some("BodyNodeGateFailed".to_string()),
                                    error_class: Some("fatal".to_string()),
                                },
                            ],
                            is_succeeded: false,
                            is_failed: false,
                        };
                    }
                    ActivityStatus::Succeeded => {
                        // Gate passed, proceed to work.
                    }
                    _ => {
                        return AdvanceDecision {
                            actions: vec![],
                            is_succeeded: false,
                            is_failed: false,
                        };
                    }
                }
            }

            // Dispatch / check work activity.
            let work_id = loop_work_activity_id(run_id, loop_id, iteration, node_id);
            let Some(work) = activity_state(snapshot, &work_id) else {
                return AdvanceDecision {
                    actions: vec![OrchestratorAction::DispatchWork {
                        node_id: node_id.clone(),
                        activity_id: work_id,
                        node: node.clone(),
                    }],
                    is_succeeded: false,
                    is_failed: false,
                };
            };

            match work.status {
                ActivityStatus::Succeeded => {
                    // Body node done → continue to next.
                    continue;
                }
                ActivityStatus::Failed | ActivityStatus::TimedOut => {
                    // Body node failed → loop failed.
                    return AdvanceDecision {
                        actions: vec![
                            OrchestratorAction::FinishLoopIteration {
                                node_id: loop_id.to_string(),
                                iteration,
                                resolution: "failed".to_string(),
                                decision_activity_id: None,
                                wait_resolved_event_id: None,
                                by: None,
                                comment: None,
                                timed_out: None,
                            },
                            OrchestratorAction::FinishLoop {
                                node_id: loop_id.to_string(),
                                final_iteration: iteration,
                                resolution: "failed".to_string(),
                                output_ref: None,
                                error_code: Some("BodyNodeFailed".to_string()),
                                error_class: Some("fatal".to_string()),
                            },
                        ],
                        is_succeeded: false,
                        is_failed: false,
                    };
                }
                _ => {
                    // Work in progress.
                    return AdvanceDecision {
                        actions: vec![],
                        is_succeeded: false,
                        is_failed: false,
                    };
                }
            }
        }
    }

    // All body nodes done — nothing actionable right now.
    AdvanceDecision {
        actions: vec![],
        is_succeeded: false,
        is_failed: false,
    }
}
