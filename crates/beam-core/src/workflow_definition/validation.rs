use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Context;
use anyhow::Result;

use super::schema::{WorkflowDefinition, WorkflowNode};

pub fn parse_workflow_definition(raw: &str) -> Result<WorkflowDefinition> {
    let def: WorkflowDefinition =
        serde_json::from_str(raw).context("failed to parse workflow json")?;
    validate_workflow_definition(&def)?;
    Ok(def)
}

/// Side-effect executors that MUST be gated (humanGate or unsafeAllowUngated).
const SIDE_EFFECT_EXECUTORS: &[&str] = &["feishu-send", "feishu-reply", "beam-schedule"];

fn is_side_effect_executor(executor: &str) -> bool {
    SIDE_EFFECT_EXECUTORS.contains(&executor)
}

pub fn validate_workflow_definition(def: &WorkflowDefinition) -> Result<()> {
    if def.workflow_id.trim().is_empty() {
        anyhow::bail!("workflowId 缺失");
    }
    if def.version == 0 {
        anyhow::bail!("version must be positive");
    }
    if def.nodes.is_empty() {
        anyhow::bail!("Workflow must declare at least one node");
    }
    for node_id in def.nodes.keys() {
        // Node id must match ^[A-Za-z0-9_.-]+$ (non-empty)
        if node_id.is_empty() {
            anyhow::bail!("nodeId '' rejected: must match ^[A-Za-z0-9_.-]+$");
        }
        if !node_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '-')
        {
            anyhow::bail!(
                "nodeId '{}' rejected: must match ^[A-Za-z0-9_.-]+$",
                node_id
            );
        }
        // Preserve existing path-traversal rejection
        if node_id == "." || node_id == ".." || node_id.contains("..") {
            anyhow::bail!(
                "nodeId '{}' rejected: path-traversal style ids are not allowed",
                node_id
            );
        }
    }
    for (node_id, node) in &def.nodes {
        match node {
            WorkflowNode::HostExecutor(host) => {
                // Side-effect executors must be gated
                if is_side_effect_executor(&host.executor)
                    && host.base.human_gate.is_none()
                    && !host.base.unsafe_allow_ungated.unwrap_or(false)
                {
                    anyhow::bail!(
                        "nodeId '{}': side-effect executor '{}' must have a humanGate or set unsafeAllowUngated: true",
                        node_id,
                        host.executor
                    );
                }
            }
            _ => {}
        }
    }
    for (node_id, node) in &def.nodes {
        for dep in node_depends(node) {
            if !def.nodes.contains_key(dep) {
                anyhow::bail!("Node '{}' depends on unknown node '{}'", node_id, dep);
            }
            if dep == node_id {
                anyhow::bail!("Node '{}' depends on itself", node_id);
            }
        }
    }
    detect_cycles(def)?;

    // Loop definition validation (Task 8.3)
    validate_loop_definitions(def)?;

    // Check for at least one scheduler-visible root node
    // (exclude body nodes and Decision nodes — they're dispatched inside their loop context)
    let body_owner = build_body_owner_map(def);
    let has_root = def.nodes.iter().any(|(node_id, node)| {
        if body_owner.contains_key(node_id) {
            return false;
        }
        if matches!(node, WorkflowNode::Decision(_)) {
            return false;
        }
        node_depends(node).is_empty()
    });
    if !has_root {
        anyhow::bail!(
            "Workflow has no scheduler-visible root node (every non-loop-body, non-decision node has dependencies)"
        );
    }
    Ok(())
}

fn node_depends(node: &WorkflowNode) -> &[String] {
    match node {
        WorkflowNode::Subagent(n) => n.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::HostExecutor(n) => n.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::Loop(n) => n.base.depends.as_deref().unwrap_or(&[]),
        WorkflowNode::Decision(n) => n.base.depends.as_deref().unwrap_or(&[]),
    }
}

fn detect_cycles(def: &WorkflowDefinition) -> Result<()> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mark {
        Temp,
        Perm,
    }
    fn visit(
        node_id: &str,
        def: &WorkflowDefinition,
        marks: &mut BTreeMap<String, Mark>,
        stack: &mut BTreeSet<String>,
    ) -> Result<()> {
        if matches!(marks.get(node_id), Some(Mark::Perm)) {
            return Ok(());
        }
        if matches!(marks.get(node_id), Some(Mark::Temp)) {
            anyhow::bail!("Workflow graph contains a cycle at '{}'", node_id);
        }
        marks.insert(node_id.to_string(), Mark::Temp);
        stack.insert(node_id.to_string());
        for dep in node_depends(def.nodes.get(node_id).expect("node must exist")) {
            visit(dep, def, marks, stack)?;
        }
        stack.remove(node_id);
        marks.insert(node_id.to_string(), Mark::Perm);
        Ok(())
    }

    let mut marks = BTreeMap::new();
    let mut stack = BTreeSet::new();
    for node_id in def.nodes.keys() {
        visit(node_id, def, &mut marks, &mut stack)?;
    }
    Ok(())
}

/// Build a map from each body node id → its owning loop node id.
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

/// Validate loop definitions per Task 8.3 rules:
/// 1. body nodes must exist
/// 2. body nodes cannot be loops (no nested loops)
/// 3. terminate.node must exist, be in body, be a human-gated Decision, and
///    terminate.via must be "humanGate"; all Decisions must have a loop owner
/// 4. each loop body can have at most one Decision (the terminate.node)
/// 5. body external deps must appear in loop.depends
/// 6. external nodes cannot depend on loop body nodes
/// 7. output.from must identify an output-producing body node
/// 8. sink loops must declare output.from
fn validate_loop_definitions(def: &WorkflowDefinition) -> Result<()> {
    let body_owner = build_body_owner_map(def);

    // Track which Decision node is owned by which loop (via terminate.node)
    let mut decision_loop_owner: HashMap<String, String> = HashMap::new();

    for (loop_id, node) in &def.nodes {
        let loop_node = match node {
            WorkflowNode::Loop(ln) => ln,
            _ => continue,
        };

        // Rule 1: body nodes must exist in workflow nodes
        for body_id in &loop_node.body {
            if !def.nodes.contains_key(body_id) {
                anyhow::bail!(
                    "loop '{}' body node '{}' not found in workflow nodes",
                    loop_id,
                    body_id
                );
            }
        }

        // Rule 2: body nodes cannot be Loop (no nested loops)
        for body_id in &loop_node.body {
            if matches!(def.nodes.get(body_id), Some(WorkflowNode::Loop(_))) {
                anyhow::bail!(
                    "loop '{}' body node '{}' cannot be a Loop node (nested loops are not supported)",
                    loop_id,
                    body_id
                );
            }
        }

        // Rule 3a: terminate.node must exist
        let term_node_id = &loop_node.terminate.node;
        if !def.nodes.contains_key(term_node_id) {
            anyhow::bail!(
                "loop '{}' terminate.node '{}' not found in workflow nodes",
                loop_id,
                term_node_id
            );
        }

        // Rule 3a: terminate.node must be in the loop body
        if !loop_node.body.contains(term_node_id) {
            anyhow::bail!(
                "loop '{}' terminate.node '{}' must be in the loop body",
                loop_id,
                term_node_id
            );
        }

        // Rule 3a: terminate.node must be a Decision node. Gate details are
        // checked after structural validation to preserve useful diagnostics.
        match def.nodes.get(term_node_id) {
            Some(WorkflowNode::Decision(_)) => {
                // Each Decision can belong to at most one loop
                if let Some(existing_owner) =
                    decision_loop_owner.insert(term_node_id.clone(), loop_id.clone())
                {
                    anyhow::bail!(
                        "Decision node '{}' is used as terminate.node by multiple loops: '{}' and '{}'",
                        term_node_id,
                        existing_owner,
                        loop_id
                    );
                }
            }
            _ => {
                anyhow::bail!(
                    "loop '{}' terminate.node '{}' must be a Decision node, got {:?}",
                    loop_id,
                    term_node_id,
                    std::mem::discriminant(def.nodes.get(term_node_id).unwrap())
                );
            }
        }

        // Rule 4: each loop body can have at most one Decision (the terminate.node)
        for body_id in &loop_node.body {
            if body_id != term_node_id
                && matches!(def.nodes.get(body_id), Some(WorkflowNode::Decision(_)))
            {
                anyhow::bail!(
                    "loop '{}' has Decision node '{}' in body that is not the terminate.node; \
                     each loop body can have at most one Decision node (which must be the terminate.node)",
                    loop_id,
                    body_id
                );
            }
        }

        // Rule 5: body external deps must be declared in loop.depends
        let loop_depends: BTreeSet<&str> = loop_node
            .base
            .depends
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect())
            .unwrap_or_default();
        for body_id in &loop_node.body {
            if let Some(body_node) = def.nodes.get(body_id) {
                for dep in node_depends(body_node) {
                    if !loop_node.body.contains(dep) {
                        // dep is external to the loop body
                        if !loop_depends.contains(dep.as_str()) {
                            anyhow::bail!(
                                "loop '{}' body node '{}' depends on external node '{}'; \
                                 all external dependencies of body nodes must be declared in the loop's depends",
                                loop_id,
                                body_id,
                                dep
                            );
                        }
                    }
                }
            }
        }
    }

    // Rule 3b: all Decision nodes must be owned by some loop (no standalone Decision)
    for (node_id, node) in &def.nodes {
        if matches!(node, WorkflowNode::Decision(_)) {
            if !decision_loop_owner.contains_key(node_id) {
                anyhow::bail!(
                    "Decision node '{}' is standalone; Decision nodes must be used as a loop's terminate.node and reside in that loop's body",
                    node_id
                );
            }
        }
    }

    // Rule 6: non-body nodes (including loop nodes themselves) cannot depend on
    // loop body nodes.  External nodes that need a loop's result must depend on
    // the loop node itself, not on individual body nodes.
    for (node_id, node) in &def.nodes {
        if body_owner.contains_key(node_id) {
            continue; // skip body nodes themselves
        }
        if matches!(node, WorkflowNode::Decision(_)) {
            continue; // Decision nodes not owned by a loop are already rejected by rule 3b;
            // owned Decision nodes are body nodes (skip above)
        }
        for dep in node_depends(node) {
            if body_owner.contains_key(dep) {
                let owner = body_owner.get(dep).unwrap();
                anyhow::bail!(
                    "node '{}' depends on loop body node '{}'; \
                     nodes must depend on the loop node '{}' instead of its body node",
                    node_id,
                    dep,
                    owner
                );
            }
        }
    }

    // Rule 7: every declared output must come from an output-producing body node.
    for (loop_id, node) in &def.nodes {
        let WorkflowNode::Loop(loop_node) = node else {
            continue;
        };
        let Some(output) = &loop_node.output else {
            continue;
        };
        if !loop_node.body.contains(&output.from) {
            anyhow::bail!(
                "loop '{}' output.from '{}' must identify a node in the loop body",
                loop_id,
                output.from
            );
        }
        if !matches!(
            def.nodes.get(&output.from),
            Some(WorkflowNode::Subagent(_) | WorkflowNode::HostExecutor(_))
        ) {
            anyhow::bail!(
                "loop '{}' output.from '{}' must identify an output-producing Subagent or HostExecutor body node",
                loop_id,
                output.from
            );
        }
    }

    // Rule 8: sink loops must declare output.from
    // A loop is a "sink" if no non-body, non-decision node depends on it.
    let sinks = find_non_body_sinks(def, &body_owner);
    for sink_id in &sinks {
        if let Some(WorkflowNode::Loop(loop_node)) = def.nodes.get(sink_id) {
            if loop_node.output.is_none() {
                anyhow::bail!(
                    "sink loop '{}' must declare output.from (the loop is not depended on by any external node)",
                    sink_id
                );
            }
        }
    }

    // Loop termination is only implemented through a human gate.
    for (loop_id, node) in &def.nodes {
        let WorkflowNode::Loop(loop_node) = node else {
            continue;
        };
        if loop_node.terminate.via != "humanGate" {
            anyhow::bail!(
                "loop '{}' terminate.via must be 'humanGate', got '{}'",
                loop_id,
                loop_node.terminate.via
            );
        }
        let decision = match def.nodes.get(&loop_node.terminate.node) {
            Some(WorkflowNode::Decision(decision)) => decision,
            _ => continue,
        };
        if decision.base.human_gate.is_none() {
            anyhow::bail!(
                "loop '{}' terminate.node '{}' must declare humanGate",
                loop_id,
                loop_node.terminate.node
            );
        }
    }

    Ok(())
}

/// Find sink nodes excluding body nodes and Decision nodes.
/// A node is a sink if no non-body, non-decision node depends on it.
fn find_non_body_sinks(
    def: &WorkflowDefinition,
    body_owner: &HashMap<String, String>,
) -> Vec<String> {
    let mut referenced: BTreeSet<String> = BTreeSet::new();
    for (node_id, node) in &def.nodes {
        if body_owner.contains_key(node_id) {
            continue;
        }
        if matches!(node, WorkflowNode::Decision(_)) {
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
