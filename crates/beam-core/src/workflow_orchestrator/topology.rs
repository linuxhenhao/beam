use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use crate::workflow_snapshot::NodeStatus;
use crate::{
    ActivityState, HumanGate, LoopStatus, NodeState, RunSnapshotDTO, WorkflowDefinition,
    WorkflowNode,
};

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

pub(super) fn build_body_owner_map(def: &WorkflowDefinition) -> HashMap<String, String> {
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

pub(super) fn find_sinks(def: &WorkflowDefinition) -> Vec<String> {
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

pub(super) fn dependency_is_succeeded(snapshot: &RunSnapshotDTO, node_id: &str) -> bool {
    if let Some(node) = node_state(snapshot, node_id)
        && node.status == NodeStatus::Succeeded
    {
        return true;
    }
    if let Some(loop_state) = snapshot.loops.as_ref().and_then(|loops| loops.get(node_id)) {
        return matches!(loop_state.status, LoopStatus::Succeeded);
    }
    false
}

pub(super) fn node_state<'a>(snapshot: &'a RunSnapshotDTO, node_id: &str) -> Option<&'a NodeState> {
    snapshot.nodes.iter().find(|node| node.node_id == node_id)
}

pub(super) fn activity_state<'a>(
    snapshot: &'a RunSnapshotDTO,
    activity_id: &str,
) -> Option<&'a ActivityState> {
    snapshot
        .activities
        .iter()
        .find(|activity| activity.activity_id == activity_id)
}

pub(super) fn node_depends(node: &WorkflowNode) -> &[String] {
    match node {
        WorkflowNode::Subagent(node) => node.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::HostExecutor(node) => node.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::Loop(node) => node.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::Decision(node) => node.base.depends.as_deref().unwrap_or(&[]),
    }
}

pub(super) fn derive_error_class(activity: &ActivityState) -> String {
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

pub(super) fn gate_activity_id(run_id: &str, node_id: &str) -> String {
    format!("{run_id}::gate::{node_id}")
}

pub(super) fn work_activity_id(run_id: &str, node_id: &str) -> String {
    format!("{run_id}::work::{node_id}")
}

/// Build a loop-scoped gate activity id:
///   `<runId>::loop::<loopId>.<N>::gate::<bodyNodeId>`
pub(super) fn loop_gate_activity_id(
    run_id: &str,
    loop_id: &str,
    iteration: u64,
    node_id: &str,
) -> String {
    format!("{run_id}::loop::{loop_id}.{iteration}::gate::{node_id}")
}

/// Build a loop-scoped work activity id:
///   `<runId>::loop::<loopId>.<N>::work::<bodyNodeId>`
pub(super) fn loop_work_activity_id(
    run_id: &str,
    loop_id: &str,
    iteration: u64,
    node_id: &str,
) -> String {
    format!("{run_id}::loop::{loop_id}.{iteration}::work::{node_id}")
}

/// Return the HumanGate config for any node type, if present.
pub(super) fn node_human_gate(node: &WorkflowNode) -> Option<&HumanGate> {
    match node {
        WorkflowNode::Subagent(n) => n.base.human_gate.as_ref(),
        WorkflowNode::HostExecutor(n) => n.base.human_gate.as_ref(),
        WorkflowNode::Loop(n) => n.base.human_gate.as_ref(),
        WorkflowNode::Decision(n) => n.base.human_gate.as_ref(),
    }
}

/// Compute a stable topological order for the given body nodes, considering
/// only edges where both ends are in the body set.
pub(super) fn body_topological_order(
    def: &WorkflowDefinition,
    body_nodes: &[String],
) -> Vec<String> {
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
