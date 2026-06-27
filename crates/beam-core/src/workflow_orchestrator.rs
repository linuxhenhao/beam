use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use crate::workflow_snapshot::{ActivityStatus, NodeStatus};
use crate::{
    ActivityState, HumanGate, LoopIterationStatus, LoopNode, LoopStatus, NodeState, RunSnapshotDTO,
    WorkflowDefinition, WorkflowNode, WorkflowOutputRef,
};

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

    let order = topological_order(def);
    let body_owner = build_body_owner_map(def);
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
            let deps_ok = node_depends(node)
                .iter()
                .all(|dep| dependency_is_succeeded(snapshot, dep));
            if !deps_ok {
                pending_count += 1;
                continue;
            }
            let advance = decide_loop_advancement(snapshot, def, &node_id, loop_node);
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

        if let Some(node_state) = node_state(snapshot, &node_id) {
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

        let deps_ok = node_depends(node)
            .iter()
            .all(|dep| dependency_is_succeeded(snapshot, dep));
        if !deps_ok {
            pending_count += 1;
            continue;
        }

        let gate_id = gate_activity_id(&snapshot.run.run_id, &node_id);
        let work_id = work_activity_id(&snapshot.run.run_id, &node_id);
        let advance = decide_node_advancement(snapshot, node, &node_id, &gate_id, &work_id);
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
            let sinks = find_sinks(def);
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

#[derive(Debug, Clone, PartialEq)]
struct AdvanceDecision {
    actions: Vec<OrchestratorAction>,
    is_succeeded: bool,
    is_failed: bool,
}

fn decide_node_advancement(
    snapshot: &RunSnapshotDTO,
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
    snapshot: &RunSnapshotDTO,
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
                node,
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
fn decide_loop_advancement(
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

pub fn topological_order(def: &WorkflowDefinition) -> Vec<String> {
    let mut indegree: BTreeMap<String, usize> = def
        .nodes
        .keys()
        .map(|node_id| (node_id.clone(), 0))
        .collect();
    let mut outgoing: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (node_id, node) in &def.nodes {
        for dep in node_depends(node) {
            *indegree.entry(node_id.clone()).or_insert(0) += 1;
            outgoing
                .entry(dep.clone())
                .or_default()
                .push(node_id.clone());
        }
    }

    let mut ready: BTreeSet<String> = indegree
        .iter()
        .filter_map(|(node_id, degree)| (*degree == 0).then_some(node_id.clone()))
        .collect();
    let mut order = Vec::with_capacity(def.nodes.len());
    while let Some(node_id) = ready.iter().next().cloned() {
        ready.take(&node_id);
        order.push(node_id.clone());
        if let Some(children) = outgoing.get(&node_id) {
            for child in children {
                if let Some(entry) = indegree.get_mut(child) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        ready.insert(child.clone());
                    }
                }
            }
        }
    }
    if order.len() != def.nodes.len() {
        return def.nodes.keys().cloned().collect();
    }
    order
}

fn build_body_owner_map(def: &WorkflowDefinition) -> HashMap<String, String> {
    let mut owner = HashMap::new();
    for (loop_id, node) in &def.nodes {
        if let WorkflowNode::Loop(loop_node) = node {
            for body_id in &loop_node.body {
                owner.insert(body_id.clone(), loop_id.clone());
            }
        }
    }
    owner
}

fn find_sinks(def: &WorkflowDefinition) -> Vec<String> {
    let body_owner = build_body_owner_map(def);
    let mut referenced = BTreeSet::new();
    for (node_id, node) in &def.nodes {
        if body_owner.contains_key(node_id) {
            continue;
        }
        for dep in node_depends(node) {
            referenced.insert(dep.clone());
        }
    }
    def.nodes
        .iter()
        .filter_map(|(node_id, node)| {
            if body_owner.contains_key(node_id) {
                return None;
            }
            if matches!(node, WorkflowNode::Decision(_)) {
                return None;
            }
            (!referenced.contains(node_id)).then_some(node_id.clone())
        })
        .collect()
}

fn dependency_is_succeeded(snapshot: &RunSnapshotDTO, node_id: &str) -> bool {
    if let Some(node) = node_state(snapshot, node_id) {
        if node.status == NodeStatus::Succeeded {
            return true;
        }
    }
    if let Some(loop_state) = snapshot.loops.as_ref().and_then(|loops| loops.get(node_id)) {
        return matches!(loop_state.status, LoopStatus::Succeeded);
    }
    false
}

fn node_state<'a>(snapshot: &'a RunSnapshotDTO, node_id: &str) -> Option<&'a NodeState> {
    snapshot.nodes.iter().find(|node| node.node_id == node_id)
}

fn activity_state<'a>(
    snapshot: &'a RunSnapshotDTO,
    activity_id: &str,
) -> Option<&'a ActivityState> {
    snapshot
        .activities
        .iter()
        .find(|activity| activity.activity_id == activity_id)
}

fn node_depends(node: &WorkflowNode) -> &[String] {
    match node {
        WorkflowNode::Subagent(node) => node.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::HostExecutor(node) => node.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::Loop(node) => node.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::Decision(node) => node.base.depends.as_deref().unwrap_or(&[]),
    }
}

fn derive_error_class(activity: &ActivityState) -> String {
    let Some(last) = activity.attempts.last() else {
        return "fatal".to_string();
    };
    last.error
        .as_ref()
        .and_then(|value| value.get("errorClass"))
        .and_then(Value::as_str)
        .unwrap_or("fatal")
        .to_string()
}

fn gate_activity_id(run_id: &str, node_id: &str) -> String {
    format!("{run_id}::gate::{node_id}")
}

fn work_activity_id(run_id: &str, node_id: &str) -> String {
    format!("{run_id}::work::{node_id}")
}

/// Build a loop-scoped gate activity id:
///   `<runId>::loop::<loopId>.<N>::gate::<bodyNodeId>`
fn loop_gate_activity_id(run_id: &str, loop_id: &str, iteration: u64, node_id: &str) -> String {
    format!("{run_id}::loop::{loop_id}.{iteration}::gate::{node_id}")
}

/// Build a loop-scoped work activity id:
///   `<runId>::loop::<loopId>.<N>::work::<bodyNodeId>`
fn loop_work_activity_id(run_id: &str, loop_id: &str, iteration: u64, node_id: &str) -> String {
    format!("{run_id}::loop::{loop_id}.{iteration}::work::{node_id}")
}

/// Return the HumanGate config for any node type, if present.
fn node_human_gate(node: &WorkflowNode) -> Option<&HumanGate> {
    match node {
        WorkflowNode::Subagent(n) => n.base.human_gate.as_ref(),
        WorkflowNode::HostExecutor(n) => n.base.human_gate.as_ref(),
        WorkflowNode::Loop(n) => n.base.human_gate.as_ref(),
        WorkflowNode::Decision(n) => n.base.human_gate.as_ref(),
    }
}

/// Compute a stable topological order for the given body nodes, considering
/// only edges where both ends are in the body set.
fn body_topological_order(def: &WorkflowDefinition, body_nodes: &[String]) -> Vec<String> {
    let body_set: BTreeSet<&String> = body_nodes.iter().collect();
    let mut indegree: BTreeMap<&String, usize> = body_nodes.iter().map(|id| (id, 0)).collect();
    let mut outgoing: BTreeMap<&String, Vec<&String>> = BTreeMap::new();

    for node_id in body_nodes {
        let Some(node) = def.nodes.get(node_id) else {
            continue;
        };
        for dep in node_depends(node) {
            if body_set.contains(dep) {
                *indegree.get_mut(node_id).unwrap() += 1;
                outgoing.entry(dep).or_default().push(node_id);
            }
        }
    }

    // Kahn's algorithm
    let mut ready: BTreeSet<&String> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut order = Vec::with_capacity(body_nodes.len());

    while let Some(id) = ready.iter().next().cloned() {
        ready.take(id);
        order.push(id.clone());
        if let Some(children) = outgoing.get(id) {
            for child in children {
                if let Some(entry) = indegree.get_mut(child) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        ready.insert(child);
                    }
                }
            }
        }
    }

    if order.len() != body_nodes.len() {
        // Fallback: use original order if cycle detected or some nodes unreachable
        return body_nodes.to_vec();
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow_definition::NodeBase;
    use crate::workflow_snapshot::{ActivityStatus, NodeStatus};
    use crate::{ActivityState, NodeState, RunChatBinding, RunState, RunStatus, WorkflowOutputRef};
    use crate::{LoopIterationState, LoopSnapshotDTO};

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
}
