use std::collections::{BTreeMap, BTreeSet};

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
        // Reject unimplemented node types
        match node {
            WorkflowNode::Loop(_) => {
                anyhow::bail!(
                    "nodeId '{}': loop runtime is not implemented yet",
                    node_id
                );
            }
            WorkflowNode::Decision(_) => {
                anyhow::bail!(
                    "nodeId '{}': loop runtime is not implemented yet (standalone Decision requires loop)",
                    node_id
                );
            }
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
    if !def.nodes.values().any(|node| node_depends(node).is_empty()) {
        anyhow::bail!(
            "Workflow has no scheduler-visible root node (every non-loop-body node has dependencies)"
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

    // -- Task 1.3: reject unimplemented loop and standalone Decision --

    #[test]
    fn reject_loop_node_current_behavior() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"l":{"type":"loop","maxIterations":3,"body":[],"terminate":{"node":"d","via":"approve"}}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("loop runtime is not implemented yet"),
            "got: {err}"
        );
    }

    #[test]
    fn reject_standalone_decision_node_current_behavior() {
        let err = parse_workflow_definition(
            r#"{"workflowId":"f","version":1,"nodes":{"d":{"type":"decision"}}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("loop runtime is not implemented yet"),
            "got: {err}"
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

    // -- Task 1.3: real code-review-loop workflow must fail due to unimplemented loop --
    #[test]
    fn reject_code_review_loop_workflow_json() {
        let raw = include_str!("../../../workflows/code-review-loop.workflow.json");
        let err = parse_workflow_definition(raw).unwrap_err();
        assert!(
            err.to_string().contains("loop runtime is not implemented yet"),
            "got: {err}"
        );
    }
}
