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
        if node_id == "." || node_id == ".." || node_id.contains("..") {
            anyhow::bail!(
                "nodeId '{}' rejected: path-traversal style ids are not allowed",
                node_id
            );
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
}
