use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::WorkflowOutputRef;
use crate::workflow_binding::LoopContext;

pub(crate) fn gate_attempt_id(activity_id: &str) -> String {
    format!("{activity_id}::att-1")
}

pub(crate) fn work_attempt_id(activity_id: &str, attempt_number: u64) -> String {
    format!("{activity_id}::att-{attempt_number}")
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn split_prompt(
    log: &mut crate::EventLog,
    resolved_prompt: &str,
) -> Result<PromptField> {
    if resolved_prompt.len() <= 1024 {
        return Ok(PromptField {
            prompt: Some(resolved_prompt.to_string()),
            prompt_ref: None,
            prompt_preview: None,
        });
    }
    let prompt_ref = write_json_blob(log, Value::String(resolved_prompt.to_string()))?;
    Ok(PromptField {
        prompt: None,
        prompt_ref: Some(prompt_ref),
        prompt_preview: Some(make_prompt_preview(resolved_prompt)),
    })
}

#[derive(Debug)]
pub(crate) struct PromptField {
    pub prompt: Option<String>,
    pub prompt_ref: Option<WorkflowOutputRef>,
    pub prompt_preview: Option<String>,
}

fn make_prompt_preview(full: &str) -> String {
    const MAX: usize = 480;
    if full.chars().count() <= MAX {
        return full.to_string();
    }
    let suffix = "…(完整内容见 dashboard)";
    let budget = MAX.saturating_sub(suffix.chars().count());
    let mut out = String::new();
    for ch in full.chars().take(budget) {
        out.push(ch);
    }
    out.push_str(suffix);
    out
}

pub(crate) fn write_json_blob(
    log: &mut crate::EventLog,
    value: Value,
) -> Result<WorkflowOutputRef> {
    let bytes = serde_json::to_vec(&value)?;
    let hash = sha256_hex(&bytes);
    let path = PathBuf::from(&log.blob_dir).join(&hash);
    fs::write(&path, &bytes)?;
    Ok(WorkflowOutputRef {
        output_hash: format!("sha256:{hash}"),
        output_path: path.display().to_string(),
        output_bytes: bytes.len(),
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    })
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    lower_hex(&hasher.finalize())
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Derive a deterministic idempotency key for a workflow host-executor attempt.
///
/// The key is `wf_` prefixed with a SHA-256 hex fragment of the canonical
/// (workflowId, revisionId, runId, nodeId, attemptId) seed.  This function
/// lives in `beam-core` so the runtime can emit `effectAttempted` events
/// before delegating to the daemon's executor hooks.
pub fn derive_workflow_idempotency_key(
    workflow_id: &str,
    revision_id: &str,
    run_id: &str,
    node_id: &str,
    attempt_id: &str,
) -> String {
    let seed = serde_json::json!({
        "attemptId": attempt_id,
        "nodeId": node_id,
        "revisionId": revision_id,
        "runId": run_id,
        "workflowId": workflow_id,
    });
    let mut hasher = Sha256::new();
    let canonical = serde_json::to_vec(&seed).expect("workflow idempotency seed serializable");
    hasher.update(&canonical);
    let hash = lower_hex(&hasher.finalize());
    let namespace = "wf_";
    let max_len = 50usize;
    let hash_len = max_len.saturating_sub(namespace.len());
    format!("{namespace}{}", &hash[..hash_len.min(hash.len())])
}

/// Return (provider, idempotency_ttl_ms) metadata for a known host executor.
///
/// This mapping lives in `beam-core` so that `effectAttempted` events can be
/// emitted with accurate provider / TTL information without depending on the
/// daemon's HostExecutor trait.
pub fn get_host_executor_provider_meta(executor_name: &str) -> (&'static str, u64) {
    match executor_name {
        "feishu-send" | "feishu-reply" => ("feishu-im", 60_000),
        "beam-schedule" => ("beam-schedule", 86_400_000),
        _ => ("manual", 300_000),
    }
}

pub(crate) fn loop_context_from_activity(activity_id: &str) -> Option<LoopContext<'_>> {
    let loop_start = activity_id.find("::loop::")?;
    let after_loop = &activity_id[loop_start + "::loop::".len()..];
    let iter_end = after_loop.find("::")?;
    let loop_part = &after_loop[..iter_end];
    let (loop_id, iteration) = loop_part.rsplit_once('.')?;
    let iteration = iteration.parse().ok()?;
    Some(LoopContext { loop_id, iteration })
}
