use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::validation::normalize_workflow_params;
use crate::{BeamPaths, EventDraft, EventLog, WorkflowActor, parse_workflow_definition};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunChatBinding {
    pub chat_id: String,
    pub lark_app_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowOutputRef {
    pub output_hash: String,
    pub output_path: String,
    pub output_bytes: usize,
    pub output_schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowRunBootstrap {
    pub run_id: String,
    pub workflow_id: String,
    pub revision_id: String,
    pub input_ref: WorkflowOutputRef,
}

#[derive(Debug, Clone)]
pub struct BootstrapWorkflowRunInput<'a> {
    pub run_id: &'a str,
    pub workflow_json: &'a str,
    pub expected_workflow_id: Option<&'a str>,
    pub params: &'a BTreeMap<String, Value>,
    pub initiator: &'a str,
    pub chat_binding: Option<RunChatBinding>,
}

pub fn mint_workflow_run_id(workflow_id: &str, now_ms: u64) -> String {
    let safe = workflow_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '_',
        })
        .collect::<String>();
    format!("{}-{}", safe, now_ms)
}

pub fn bootstrap_workflow_run(
    paths: &BeamPaths,
    input: BootstrapWorkflowRunInput<'_>,
) -> Result<WorkflowRunBootstrap> {
    let workflow = parse_workflow_definition(input.workflow_json)?;
    let workflow_id = workflow.workflow_id.clone();
    if let Some(expected) = input.expected_workflow_id
        && expected != workflow_id
    {
        anyhow::bail!(
            "workflowId mismatch: requested={} file={}",
            expected,
            workflow_id
        );
    }

    // Validate / normalize params before creating any run artifacts
    let normalized_params = normalize_workflow_params(&workflow, input.params)?;

    let run_dir = paths.workflow_run_dir(input.run_id);
    fs::create_dir_all(run_dir.join("blobs"))?;
    fs::write(run_dir.join("workflow.json"), input.workflow_json)?;
    if let Some(binding) = input.chat_binding {
        fs::write(
            run_dir.join("chat-binding.json"),
            serde_json::to_vec_pretty(&binding)?,
        )?;
    }

    let params_json = serde_json::to_vec(&normalized_params)?;
    let params_hash = sha256_hex(&params_json);
    let input_path = run_dir.join("blobs").join(&params_hash);
    fs::write(&input_path, &params_json)?;
    let input_ref = WorkflowOutputRef {
        output_hash: format!("sha256:{}", params_hash),
        output_path: input_path.display().to_string(),
        output_bytes: params_json.len(),
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    };

    let mut log = EventLog::new(input.run_id.to_string(), paths.workflow_runs_dir())?;
    let revision_id = sha256_hex(&serde_json::to_vec(&workflow)?);
    let _run_created = log.append(EventDraft {
        event_type: "runCreated".to_string(),
        actor: WorkflowActor::System,
        payload: serde_json::json!({
            "workflowId": workflow_id,
            "revisionId": revision_id,
            "inputRef": input_ref,
            "initiator": input.initiator,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    let _run_started = log.append(EventDraft {
        event_type: "runStarted".to_string(),
        actor: WorkflowActor::Scheduler,
        payload: serde_json::json!({}),
        timestamp: None,
        payload_hash: None,
    })?;

    Ok(WorkflowRunBootstrap {
        run_id: input.run_id.to_string(),
        workflow_id,
        revision_id,
        input_ref,
    })
}

/// Normalize and validate workflow parameters against the definition's params
/// schema. Returns a canonical `BTreeMap<String, Value>` suitable for writing
/// as the run input blob.
///
/// If the workflow has no `params` definition (or an empty one), any supplied
/// params are rejected — no parameters are allowed without a declaration.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    lower_hex(&hasher.finalize())
}

pub(crate) fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn read_workflow_definition_from_path(path: &Path) -> Result<String> {
    fs::read_to_string(path).with_context(|| format!("读取 {} 失败", path.display()))
}
