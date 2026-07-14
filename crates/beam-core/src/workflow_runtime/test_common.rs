use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use super::*;
use crate::workflow_definition::{HostExecutorNode, NodeBase, SubagentNode};

#[derive(Clone)]
pub(crate) struct FakeHooks;

#[async_trait]
impl WorkflowExecutionHooks for FakeHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        resolved_input: Value,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: resolved_input,
            session: None,
        })
    }
}

pub(crate) fn temp_run_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "beam-workflow-runtime-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

pub(crate) fn workflow_def() -> WorkflowDefinition {
    WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([(
            "a".to_string(),
            WorkflowNode::Subagent(SubagentNode {
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
                bot: "bot-a".to_string(),
                prompt: Value::String("hello ${params.name}".to_string()),
                working_dir: None,
                model_overrides: None,
                tool_policy: None,
            }),
        )]),
    }
}
/// Returns a minimal workflow JSON with a single subagent node.
pub(crate) fn min_workflow_json(workflow_id: &str, node_id: &str) -> String {
    format!(
        r#"{{"workflowId":"{workflow_id}","version":1,"nodes":{{"{node_id}":{{"type":"subagent","bot":"bot-x","prompt":"ok"}}}}}}"#
    )
}
