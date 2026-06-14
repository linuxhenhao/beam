use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ParamDef {
    #[serde(rename = "type")]
    pub param_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    pub max_attempts: u64,
    pub backoff: String,
    pub base_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HumanGate {
    pub stage: String,
    pub prompt: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_timeout: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LoopTerminate {
    pub node: String,
    pub via: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LoopOutputProjection {
    pub from: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeBase {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human_gate: Option<HumanGate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsafe_allow_ungated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubagentNode {
    #[serde(flatten)]
    pub base: NodeBase,
    pub bot: String,
    pub prompt: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_overrides: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostExecutorNode {
    #[serde(flatten)]
    pub base: NodeBase,
    pub executor: String,
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LoopNode {
    #[serde(flatten)]
    pub base: NodeBase,
    pub max_iterations: u64,
    pub body: Vec<String>,
    pub terminate: LoopTerminate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<LoopOutputProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DecisionNode {
    #[serde(flatten)]
    pub base: NodeBase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WorkflowNode {
    Subagent(SubagentNode),
    HostExecutor(HostExecutorNode),
    Loop(LoopNode),
    Decision(DecisionNode),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDefinition {
    pub workflow_id: String,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<BTreeMap<String, ParamDef>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<WorkflowDefaults>,
    pub nodes: BTreeMap<String, WorkflowNode>,
}

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
            anyhow::bail!(
                "nodeId '' rejected: must match ^[A-Za-z0-9_.-]+$"
            );
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
/// 3. terminate.node must exist, be in body, be a Decision; all Decisions must have a loop owner
/// 4. each loop body can have at most one Decision (the terminate.node)
/// 5. body external deps must appear in loop.depends
/// 6. external nodes cannot depend on loop body nodes
/// 7. sink loops must declare output.from
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

        // Rule 3a: terminate.node must be a Decision node
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

    // Rule 7: sink loops must declare output.from
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_workflow_definition_accepts_basic_graph() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId":"flow-a",
                "version":1,
                "nodes":{
                    "a":{"type":"subagent","bot":"bot-a","prompt":"hi"},
                    "b":{"type":"hostExecutor","executor":"feishu-send","input":1,"depends":["a"],"humanGate":{"stage":"before","prompt":"approve?"}}
                }
            }"#,
        )
        .expect("definition");
        assert_eq!(def.workflow_id, "flow-a");
        assert_eq!(def.nodes.len(), 2);
    }

    // -- Task 1.1: node id validation --

    #[test]
    fn reject_node_id_with_slash() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"node/a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("nodeId"), "got: {err}");
    }

    #[test]
    fn reject_node_id_with_space() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"node a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("nodeId"), "got: {err}");
    }

    #[test]
    fn reject_node_id_dotdot() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"..":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("nodeId"), "got: {err}");
    }

    #[test]
    fn reject_node_id_containing_dotdot() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a..b":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("nodeId"), "got: {err}");
    }

    #[test]
    fn reject_empty_node_id() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("nodeId"), "got: {err}");
    }

    #[test]
    fn accept_node_id_with_dash() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"node-a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .expect("dash ok");
        assert!(def.nodes.contains_key("node-a"));
    }

    #[test]
    fn accept_node_id_with_underscore() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"node_a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .expect("underscore ok");
        assert!(def.nodes.contains_key("node_a"));
    }

    #[test]
    fn accept_node_id_with_dot() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"node.a":{"type":"subagent","bot":"b","prompt":"p"}}}"#,
        )
        .expect("dot ok");
        assert!(def.nodes.contains_key("node.a"));
    }

    // -- Task 1.2: side-effect executor gate validation --

    #[test]
    fn reject_ungated_feishu_send() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":1}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("humanGate") || err.to_string().contains("side-effect"),
            "got: {err}"
        );
    }

    #[test]
    fn reject_ungated_feishu_reply() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-reply","input":1}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("humanGate") || err.to_string().contains("side-effect"),
            "got: {err}"
        );
    }

    #[test]
    fn reject_ungated_beam_schedule() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":1}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("humanGate") || err.to_string().contains("side-effect"),
            "got: {err}"
        );
    }

    #[test]
    fn accept_gated_feishu_send() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":1,"humanGate":{"stage":"before","prompt":"ok?"}}}}"#,
        )
        .expect("gated feishu-send ok");
        assert!(def.nodes.contains_key("a"));
    }

    #[test]
    fn accept_unsafe_allow_ungated_feishu_send() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":1,"unsafeAllowUngated":true}}}"#,
        )
        .expect("unsafeAllowUngated ok");
        assert!(def.nodes.contains_key("a"));
    }

    #[test]
    fn accept_ungated_non_side_effect_executor() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"custom-tool","input":1}}}"#,
        )
        .expect("non-side-effect ok");
        assert!(def.nodes.contains_key("a"));
    }

    // -- Task 8.2: loop nodes are now accepted (validation deferred to Task 8.3) --

    #[test]
    fn accept_loop_node_with_minimal_body() {
        // Loop with minimal body + decision terminate node + output.from (sink loop requires it)
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
        )
        .expect("loop accepted");
        assert!(def.nodes.contains_key("l"));
    }

    #[test]
    fn reject_standalone_decision_node() {
        // Task 8.3: standalone Decision nodes (not used as a loop terminate.node) are rejected
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("standalone"),
            "expected 'standalone' in error, got: {err}"
        );
    }

    #[test]
    fn ordinary_dag_workflow_accepted() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"subagent","bot":"b","prompt":"p"},"c":{"type":"subagent","bot":"b","prompt":"q","depends":["a"]}}}"#,
        )
        .expect("ordinary DAG ok");
        assert_eq!(def.nodes.len(), 2);
    }

    // -- Task 8.2: code-review-loop workflow now parses successfully --
    #[test]
    fn accept_code_review_loop_workflow_json() {
        let raw = include_str!("../../../workflows/code-review-loop.workflow.json");
        let def = parse_workflow_definition(raw).expect("code-review-loop parsed");
        assert!(def.nodes.contains_key("review-loop"));
        assert!(def.nodes.contains_key("implement"));
        assert!(def.nodes.contains_key("review"));
        assert!(def.nodes.contains_key("reviewDecision"));
    }

    // -- Task 9.2: subagent-approval-feishu-send example parses --
    #[test]
    fn accept_subagent_approval_feishu_send_workflow_json() {
        let raw =
            include_str!("../../../workflows/subagent-approval-feishu-send.workflow.json");
        let def = parse_workflow_definition(raw).expect("subagent-approval-feishu-send parsed");
        assert_eq!(def.workflow_id, "subagent-approval-feishu-send");
        assert_eq!(def.nodes.len(), 2);
        assert!(def.nodes.contains_key("draft"));
        assert!(def.nodes.contains_key("send"));
        // send depends on draft
        let send_node = def.nodes.get("send").unwrap();
        match send_node {
            // send is a gated feishu-send: humanGate present, unsafeAllowUngated absent
            WorkflowNode::HostExecutor(n) => {
                assert_eq!(n.executor, "feishu-send");
                assert_eq!(n.base.depends.as_deref(), Some(vec!["draft".to_string()].as_slice()));
                assert!(n.base.human_gate.is_some(), "send must have humanGate");
                assert!(!n.base.unsafe_allow_ungated.unwrap_or(false), "send must NOT use unsafeAllowUngated");
            }
            _ => panic!("expected hostExecutor"),
        }
    }

    // -- Task 8.3: loop definition validation --

    // Rule 1: body nodes must exist
    #[test]
    fn reject_loop_with_missing_body_node() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["missing","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("body node") && err.to_string().contains("missing"),
            "expected body node 'missing' error, got: {err}"
        );
    }

    // Rule 2: body node cannot be a loop (no nested loops)
    #[test]
    fn reject_loop_with_nested_loop_body() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"loop","maxIterations":2,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("nested loop") || err.to_string().contains("cannot be a Loop"),
            "expected nested loop error, got: {err}"
        );
    }

    // Rule 3a: terminate.node must exist
    #[test]
    fn reject_loop_with_missing_terminate_node() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"nonexistent","via":"humanGate"},"output":{"from":"d"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("terminate.node") && err.to_string().contains("nonexistent"),
            "expected terminate.node not found error, got: {err}"
        );
    }

    // Rule 3a: terminate.node must be in loop body
    #[test]
    fn reject_loop_with_terminate_node_not_in_body() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"x":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"x","via":"humanGate"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("terminate.node") && err.to_string().contains("body"),
            "expected terminate.node must be in body error, got: {err}"
        );
    }

    // Rule 3a: terminate.node must be a Decision node
    #[test]
    fn reject_loop_with_non_decision_terminate_node() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["a"],"terminate":{"node":"a","via":"humanGate"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Decision node"),
            "expected Decision node error, got: {err}"
        );
    }

    // Rule 3b: Decision node cannot be standalone
    #[test]
    fn reject_decision_not_used_by_any_loop() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"a":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision"}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("standalone"),
            "expected standalone Decision error, got: {err}"
        );
    }

    // Rule 3b: Decision node must be in body of the loop that uses it
    #[test]
    fn reject_decision_outside_loop_body() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":[],"terminate":{"node":"d","via":"humanGate"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("body"),
            "expected body error for terminate.node not in body, got: {err}"
        );
    }

    // Rule 3b: same Decision cannot be used by multiple loops
    #[test]
    fn reject_decision_used_by_multiple_loops() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"l1":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}},"l2":{"type":"loop","maxIterations":3,"body":["d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"d"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("multiple loops"),
            "expected 'multiple loops' error, got: {err}"
        );
    }

    // Rule 4: each loop body can have at most one Decision (the terminate.node)
    #[test]
    fn reject_loop_with_extra_decision_in_body() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision"},"d2":{"type":"decision"},"l":{"type":"loop","maxIterations":3,"body":["d1","d2"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"d1"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("at most one Decision"),
            "expected 'at most one Decision' error, got: {err}"
        );
    }

    // Rule 5: body external deps must appear in loop.depends
    #[test]
    fn reject_body_node_with_undeclared_external_dependency() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"ext":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p","depends":["ext"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("external") && err.to_string().contains("depends"),
            "expected external dep must be declared in loop.depends error, got: {err}"
        );
    }

    #[test]
    fn accept_body_node_with_external_dependency_declared_in_loop_depends() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"ext":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p","depends":["ext"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"depends":["ext"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .expect("loop with declared external dep accepted");
        assert!(def.nodes.contains_key("l"));
    }

    // Rule 6: external nodes cannot depend on loop body nodes
    #[test]
    fn reject_external_node_depending_on_loop_body_node() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"ext":{"type":"subagent","bot":"b","prompt":"p","depends":["inner"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("depends on loop body node"),
            "expected depends on loop body node error, got: {err}"
        );
    }

    #[test]
    fn accept_external_node_depending_on_loop_node_instead_of_body_node() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"ext":{"type":"subagent","bot":"b","prompt":"p","depends":["l"]},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .expect("external node depends on loop node accepted");
        assert!(def.nodes.contains_key("l"));
    }

    // Rule 6 (cont): loop node itself must not depend on body nodes
    // (either its own body or another loop's body)
    #[test]
    fn reject_loop_depends_on_own_body_node() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"depends":["inner"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("body node"),
            "expected 'body node' error for loop depending on own body node, got: {err}"
        );
    }

    #[test]
    fn reject_loop_depends_on_another_loop_body_node() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision"},"a":{"type":"subagent","bot":"b","prompt":"p"},"l1":{"type":"loop","maxIterations":3,"body":["a","d1"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"a"}},"d2":{"type":"decision"},"l2":{"type":"loop","maxIterations":3,"body":["d2"],"depends":["a"],"terminate":{"node":"d2","via":"humanGate"},"output":{"from":"d2"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("body node") && err.to_string().contains("'l2'"),
            "expected 'body node' error for l2 depending on another loop's body node, got: {err}"
        );
    }

    // Also verify that loop depends on another loop (not body node) is still accepted
    #[test]
    fn accept_loop_depends_on_another_loop_node() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision"},"a":{"type":"subagent","bot":"b","prompt":"p"},"l1":{"type":"loop","maxIterations":3,"body":["a","d1"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"a"}},"d2":{"type":"decision"},"l2":{"type":"loop","maxIterations":3,"body":["d2"],"depends":["l1"],"terminate":{"node":"d2","via":"humanGate"},"output":{"from":"d2"}}}}"#,
        )
        .expect("loop depends on another loop node accepted");
        assert!(def.nodes.contains_key("l1"));
        assert!(def.nodes.contains_key("l2"));
    }

    // Rule 7: sink loop must declare output.from
    #[test]
    fn reject_sink_loop_without_output_from() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("output.from"),
            "expected 'output.from' error for sink loop, got: {err}"
        );
    }

    #[test]
    fn accept_sink_loop_with_output_from() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .expect("sink loop with output.from accepted");
        assert!(def.nodes.contains_key("l"));
    }

    #[test]
    fn accept_non_sink_loop_without_output_from() {
        // Loop is not a sink because external node 'ext' depends on it
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"terminate":{"node":"d","via":"humanGate"}},"ext":{"type":"subagent","bot":"b","prompt":"p","depends":["l"]}}}"#,
        )
        .expect("non-sink loop without output.from accepted");
        assert!(def.nodes.contains_key("l"));
    }

    // Edge case: loop with depends on external node but no body external deps (valid)
    #[test]
    fn accept_loop_with_explicit_depends_and_no_body_external_deps() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"ext":{"type":"subagent","bot":"b","prompt":"p"},"d":{"type":"decision"},"inner":{"type":"subagent","bot":"b","prompt":"p"},"l":{"type":"loop","maxIterations":3,"body":["inner","d"],"depends":["ext"],"terminate":{"node":"d","via":"humanGate"},"output":{"from":"inner"}}}}"#,
        )
        .expect("loop with explicit depends accepted");
        assert!(def.nodes.contains_key("l"));
    }

    // Regression: multiple loops in same workflow
    #[test]
    fn accept_two_independent_loops() {
        let def = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d1":{"type":"decision"},"inner1":{"type":"subagent","bot":"b","prompt":"p"},"l1":{"type":"loop","maxIterations":3,"body":["inner1","d1"],"terminate":{"node":"d1","via":"humanGate"},"output":{"from":"inner1"}},"d2":{"type":"decision"},"inner2":{"type":"subagent","bot":"b","prompt":"p"},"l2":{"type":"loop","maxIterations":3,"body":["inner2","d2"],"depends":["l1"],"terminate":{"node":"d2","via":"humanGate"},"output":{"from":"inner2"}}}}"#,
        )
        .expect("two independent loops accepted");
        assert!(def.nodes.contains_key("l1"));
        assert!(def.nodes.contains_key("l2"));
    }
}
