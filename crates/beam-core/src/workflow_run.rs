use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    pub params: &'a BTreeMap<String, String>,
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
    if let Some(expected) = input.expected_workflow_id {
        if expected != workflow_id {
            anyhow::bail!(
                "workflowId mismatch: requested={} file={}",
                expected,
                workflow_id
            );
        }
    }

    let run_dir = paths.workflow_run_dir(input.run_id);
    fs::create_dir_all(run_dir.join("blobs"))?;
    fs::write(run_dir.join("workflow.json"), input.workflow_json)?;
    if let Some(binding) = input.chat_binding {
        fs::write(
            run_dir.join("chat-binding.json"),
            serde_json::to_vec_pretty(&binding)?,
        )?;
    }

    let params_json = serde_json::to_vec(input.params)?;
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn read_workflow_definition_from_path(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path).with_context(|| format!("读取 {} 失败", path.display()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-workflow-run-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[test]
    fn mint_workflow_run_id_sanitizes_input() {
        let run_id = mint_workflow_run_id("flow/a:b", 123);
        assert!(run_id.starts_with("flow_a_b-123"));
        assert!(!run_id.contains('/'));
        assert!(!run_id.contains(':'));
    }

    #[test]
    fn bootstrap_workflow_run_writes_snapshot_and_events() {
        let paths = temp_paths("bootstrap");
        let params = BTreeMap::from([(String::from("foo"), String::from("bar"))]);
        let result = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-1",
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"node-a":{"type":"subagent","bot":"bot-a","prompt":"hi"}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .expect("bootstrap");
        assert_eq!(result.run_id, "run-1");
        assert_eq!(result.workflow_id, "flow-a");
        assert!(
            paths
                .workflow_run_dir("run-1")
                .join("workflow.json")
                .exists()
        );
        assert!(
            paths
                .workflow_run_dir("run-1")
                .join("chat-binding.json")
                .exists()
        );
        assert!(paths.workflow_run_dir("run-1").join("blobs").exists());
        let log = EventLog::new("run-1", paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "runCreated");
        assert_eq!(events[1].event_type, "runStarted");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_workflow_run_hashes_canonical_definition_bytes() {
        let params = BTreeMap::new();
        let raw_a = r#"{"workflowId":"flow-a","version":1,"nodes":{"node-a":{"type":"subagent","bot":"bot-a","prompt":"hi","workingDir":"/tmp/demo"}}}"#;
        let raw_b = r#"
        {
            "nodes": {
                "node-a": {
                    "prompt": "hi",
                    "type": "subagent",
                    "bot": "bot-a",
                    "workingDir": "/tmp/demo"
                }
            },
            "version": 1,
            "workflowId": "flow-a"
        }
        "#;

        let paths_a = temp_paths("bootstrap-canonical-a");
        let paths_b = temp_paths("bootstrap-canonical-b");
        let rev_a = bootstrap_workflow_run(
            &paths_a,
            BootstrapWorkflowRunInput {
                run_id: "run-a",
                workflow_json: raw_a,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: None,
            },
        )
        .expect("bootstrap a")
        .revision_id;
        let rev_b = bootstrap_workflow_run(
            &paths_b,
            BootstrapWorkflowRunInput {
                run_id: "run-b",
                workflow_json: raw_b,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: None,
            },
        )
        .expect("bootstrap b")
        .revision_id;
        assert_eq!(rev_a, rev_b);
        let _ = std::fs::remove_dir_all(paths_a.root());
        let _ = std::fs::remove_dir_all(paths_b.root());
    }
}
