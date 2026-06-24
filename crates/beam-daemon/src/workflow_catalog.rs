//! Workflow catalog, definition loading, run listing, and text-command parsing.
//!
//! Extracted from `lib.rs` (round 3 structural refactor) to separate
//! workflow catalog/definition DTOs, listing helpers, text-command parsing,
//! and run-summary building from route handlers and app wiring.
//!
//! This module handles:
//! - Workflow text command parsing (`/workflow run` / `/workflow cancel`)
//! - Workflow definition discovery and catalog listing
//! - Workflow definition loading (canonical hashing)
//! - Workflow run listing with filtering
//! - Workflow run error extraction and summary building

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Result;
use axum::http::StatusCode;
use beam_core::{
    BeamPaths, RunStatus, event_seq_from_id, infer_run_status, parse_workflow_definition,
    read_run_events_pure, read_run_snapshot,
};
use serde_json::Value;

use crate::{internal_error, sha256_hex};

// ── Text command parsing ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkflowTextCommand {
    Run {
        workflow_id: String,
        raw_params: HashMap<String, String>,
    },
    Cancel {
        run_id: String,
    },
    Invalid {
        error: String,
        usage: String,
    },
}

pub(crate) fn parse_workflow_text_command(text: &str) -> Option<WorkflowTextCommand> {
    let trimmed = text.trim();
    if !trimmed.starts_with("/workflow") {
        return None;
    }

    let rest = trimmed["/workflow".len()..].trim();
    let usage =
        "用法：/workflow run <id> [key=value ...]\n或：/workflow cancel <runId>".to_string();

    // Extract the subcommand (first whitespace-delimited token).
    let sub_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let subcommand = &rest[..sub_end];
    let sub_rest = rest[sub_end..].trim();

    match subcommand {
        "cancel" => {
            if sub_rest.is_empty() {
                return Some(WorkflowTextCommand::Invalid {
                    error: "缺少 runId".to_string(),
                    usage,
                });
            }
            let id_end = sub_rest.find(char::is_whitespace).unwrap_or(sub_rest.len());
            let run_id = &sub_rest[..id_end];
            if sub_rest[id_end..].trim().len() > 0 {
                return Some(WorkflowTextCommand::Invalid {
                    error: "/workflow cancel 只接受 runId".to_string(),
                    usage,
                });
            }
            Some(WorkflowTextCommand::Cancel {
                run_id: run_id.to_string(),
            })
        }
        "run" => {
            if sub_rest.is_empty() {
                return Some(WorkflowTextCommand::Invalid {
                    error: "缺少 workflow id".to_string(),
                    usage,
                });
            }
            let id_end = sub_rest.find(char::is_whitespace).unwrap_or(sub_rest.len());
            let workflow_id = &sub_rest[..id_end];
            let params_str = sub_rest[id_end..].trim();

            match tokenize_workflow_params(params_str) {
                Ok(raw_params) => Some(WorkflowTextCommand::Run {
                    workflow_id: workflow_id.to_string(),
                    raw_params,
                }),
                Err(error) => Some(WorkflowTextCommand::Invalid { error, usage }),
            }
        }
        _ => Some(WorkflowTextCommand::Invalid {
            error: "只支持 /workflow run / cancel 子命令".to_string(),
            usage,
        }),
    }
}

/// Tokenize `key=value` parameters from a raw string using shell-like word parsing.
///
/// Delegates word splitting to [`shell_words::split`], which handles:
/// - single-quoted strings (no escape inside)
/// - double-quoted strings (backslash-escape for `$`, `` ` ``, `"`, `\\`, newline)
/// - unquoted backslash escapes
/// - adjacent quoted/unquoted concatenation (e.g. `"b"c` → `bc`)
///
/// After splitting, each word must be in `key=value` form (split on the first `=`).
/// Validation rules:
/// - Each token must contain `=`
/// - Key must be non-empty
/// - Duplicate keys are rejected
/// - Unclosed quotes → [`shell_words::ParseError`] returned as `Invalid`
fn tokenize_workflow_params(input: &str) -> Result<HashMap<String, String>, String> {
    let tokens = shell_words::split(input).map_err(|e| format!("参数引号不匹配: {}", e))?;

    let mut params = HashMap::new();
    for token in &tokens {
        // Split on first `=` only.
        let eq_pos = token
            .find('=')
            .ok_or_else(|| format!("参数必须是 key=value 形式：{}", token))?;

        let key = &token[..eq_pos];
        let value = &token[eq_pos + 1..];

        if key.is_empty() {
            return Err("参数名不能为空".to_string());
        }
        if params.contains_key(key) {
            return Err(format!("重复参数：{}", key));
        }
        params.insert(key.to_string(), value.to_string());
    }
    Ok(params)
}

// ── Catalog / run DTOs ──────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowCatalogEntry {
    pub(crate) workflow_id: String,
    pub(crate) version: u64,
    pub(crate) path: String,
    pub(crate) revision_id: String,
    pub(crate) param_count: usize,
    pub(crate) required_param_count: usize,
    pub(crate) node_count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowCatalogDefinition {
    pub(crate) definition: Value,
    pub(crate) revision_id: String,
    pub(crate) path: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowRunRow {
    pub(crate) run_id: String,
    pub(crate) workflow_id: String,
    pub(crate) status: String,
    pub(crate) last_seq: u64,
    pub(crate) d_ef: usize,
    pub(crate) d_act: usize,
    pub(crate) d_wait: usize,
    pub(crate) updated_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failed_node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chat_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lark_app_id: Option<String>,
}

// ── Definition search / listing / loading ───────────────────────────────

pub(crate) fn workflow_definition_search_dirs() -> Vec<PathBuf> {
    let mut dirs = vec![PathBuf::from("workflows")];
    if let Some(home) = std::env::var_os("HOME") {
        dirs.push(PathBuf::from(home).join(".beam/workflows"));
    }
    dirs
}

pub(crate) async fn load_workflow_definition_path(workflow_id: &str) -> Result<PathBuf> {
    let mut candidates = vec![
        std::env::current_dir()?
            .join("workflows")
            .join(format!("{workflow_id}.workflow.json")),
    ];
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join(".beam/workflows")
                .join(format!("{workflow_id}.workflow.json")),
        );
    }
    for candidate in &candidates {
        if tokio::fs::metadata(candidate).await.is_ok() {
            return Ok(candidate.clone());
        }
    }
    anyhow::bail!(
        "Workflow '{}' not found. Looked in:\n{}",
        workflow_id,
        candidates
            .iter()
            .map(|p| format!("- {}", p.display()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

pub(crate) async fn list_workflow_definitions() -> Result<Vec<WorkflowCatalogEntry>> {
    list_workflow_definitions_in(workflow_definition_search_dirs()).await
}

pub(crate) async fn list_workflow_definitions_in(
    dirs: Vec<PathBuf>,
) -> Result<Vec<WorkflowCatalogEntry>> {
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    for dir in dirs {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if !entry.file_type().await?.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !name.ends_with(".workflow.json") {
                continue;
            }
            let raw = match tokio::fs::read_to_string(&path).await {
                Ok(raw) => raw,
                Err(_) => continue,
            };
            let def = match parse_workflow_definition(&raw) {
                Ok(def) => def,
                Err(_) => continue,
            };
            if !seen.insert(def.workflow_id.clone()) {
                continue;
            }
            let params = def
                .params
                .as_ref()
                .map(|m| m.values())
                .into_iter()
                .flatten();
            let param_count = def.params.as_ref().map(|m| m.len()).unwrap_or(0);
            let required_param_count = params.filter(|p| p.required.unwrap_or(false)).count();
            let revision_id = sha256_hex(&serde_json::to_vec(&def)?);
            let workflow_id = def.workflow_id.clone();
            let version = def.version;
            let node_count = def.nodes.len();
            entries.push(WorkflowCatalogEntry {
                workflow_id,
                version,
                path: path.display().to_string(),
                revision_id,
                param_count,
                required_param_count,
                node_count,
            });
        }
    }
    entries.sort_by(|a, b| a.workflow_id.cmp(&b.workflow_id));
    Ok(entries)
}

pub(crate) async fn load_workflow_catalog_definition(
    workflow_id: &str,
) -> Result<Option<WorkflowCatalogDefinition>> {
    load_workflow_catalog_definition_in(
        workflow_id,
        load_workflow_definition_candidates(workflow_id),
    )
    .await
}

pub(crate) async fn load_workflow_catalog_definition_in(
    workflow_id: &str,
    paths: Vec<PathBuf>,
) -> Result<Option<WorkflowCatalogDefinition>> {
    for path in paths {
        let raw = match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        let def = match parse_workflow_definition(&raw) {
            Ok(def) => def,
            Err(_) => continue,
        };
        if def.workflow_id != workflow_id {
            continue;
        }
        let revision_id = sha256_hex(&serde_json::to_vec(&def)?);
        return Ok(Some(WorkflowCatalogDefinition {
            definition: serde_json::to_value(&def)?,
            revision_id,
            path: path.display().to_string(),
        }));
    }
    Ok(None)
}

pub(crate) fn load_workflow_definition_candidates(workflow_id: &str) -> Vec<PathBuf> {
    let mut candidates = vec![
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("workflows")
            .join(format!("{workflow_id}.workflow.json")),
    ];
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join(".beam/workflows")
                .join(format!("{workflow_id}.workflow.json")),
        );
    }
    candidates
}

// ── Run listing / error extraction / summary ────────────────────────────

pub(crate) async fn list_workflow_runs(
    paths: &BeamPaths,
    all: bool,
    statuses: Option<HashSet<String>>,
) -> Result<Vec<WorkflowRunRow>> {
    let mut rows = Vec::new();
    let mut rd = match tokio::fs::read_dir(paths.workflow_runs_dir()).await {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(rows),
        Err(err) => return Err(err.into()),
    };
    while let Some(entry) = rd.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let run_id = entry.file_name().to_string_lossy().to_string();
        let Some(snapshot) = read_run_snapshot(&paths.workflow_run_dir(&run_id))
            .await
            .ok()
            .flatten()
        else {
            continue;
        };
        let status = format!("{:?}", snapshot.run.status).to_ascii_lowercase();
        if let Some(statuses) = statuses.as_ref() {
            if !statuses.contains(&status) {
                continue;
            }
        } else if !all
            && matches!(
                snapshot.run.status,
                RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
            )
        {
            continue;
        }
        rows.push(project_workflow_run_row(&run_id, &snapshot));
    }
    rows.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    Ok(rows)
}

pub(crate) fn project_workflow_run_row(
    run_id: &str,
    snapshot: &beam_core::RunSnapshotDTO,
) -> WorkflowRunRow {
    let effect_set: HashSet<_> = snapshot.dangling.effect_attempted.iter().cloned().collect();
    let wait_set: HashSet<_> = snapshot.dangling.waits.iter().cloned().collect();
    let wr_set: HashSet<_> = snapshot.dangling.wait_resolutions.iter().cloned().collect();
    let d_act = snapshot
        .dangling
        .activities
        .iter()
        .filter(|activity_id| {
            !effect_set.contains(*activity_id)
                && !wait_set.contains(*activity_id)
                && !wr_set.contains(*activity_id)
        })
        .count();
    let (error_code, error_class, error_message) = find_workflow_run_error(snapshot);
    WorkflowRunRow {
        run_id: run_id.to_string(),
        workflow_id: snapshot
            .run
            .workflow_id
            .clone()
            .unwrap_or_else(|| "?".to_string()),
        status: format!("{:?}", snapshot.run.status).to_ascii_lowercase(),
        last_seq: snapshot.last_seq,
        d_ef: snapshot.dangling.effect_attempted.len(),
        d_act,
        d_wait: snapshot.dangling.waits.len(),
        updated_at: snapshot.updated_at,
        failed_node_id: snapshot.run.failed_node_id.clone(),
        error_code,
        error_class,
        error_message,
        chat_id: snapshot.chat_binding.as_ref().map(|b| b.chat_id.clone()),
        lark_app_id: snapshot
            .chat_binding
            .as_ref()
            .map(|b| b.lark_app_id.clone()),
    }
}

pub(crate) fn find_workflow_run_error(
    snapshot: &beam_core::RunSnapshotDTO,
) -> (Option<String>, Option<String>, Option<String>) {
    if !matches!(
        snapshot.run.status,
        RunStatus::Failed | RunStatus::Cancelled
    ) {
        return (None, None, None);
    }
    let mut preferred = snapshot
        .run
        .failed_node_id
        .as_ref()
        .map(|failed_node| {
            snapshot
                .activities
                .iter()
                .filter(|activity| activity.owner_node_id.as_deref() == Some(failed_node.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut all = snapshot.activities.iter().collect::<Vec<_>>();
    all.retain(|activity| {
        !preferred
            .iter()
            .any(|p| p.activity_id == activity.activity_id)
    });
    preferred.extend(all);
    for activity in preferred {
        for attempt in activity.attempts.iter().rev() {
            if let Some(err) = attempt.error.as_ref() {
                let code = err
                    .get("errorCode")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let class = err
                    .get("errorClass")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let message = err
                    .get("errorMessage")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                if code.is_some() || class.is_some() || message.is_some() {
                    return (code, class, message);
                }
            }
        }
    }
    (None, None, None)
}

#[allow(dead_code)]
pub(crate) async fn build_workflow_run_summary(
    paths: &BeamPaths,
    run_id: &str,
) -> Result<Option<Value>, (StatusCode, String)> {
    let Some(events) =
        read_run_events_pure(&paths.workflow_run_dir(run_id)).map_err(internal_error)?
    else {
        return Ok(None);
    };
    if events.is_empty() {
        return Ok(None);
    }
    let workflow_id = events
        .iter()
        .find_map(|ev| {
            if ev.event_type != "runCreated" {
                return None;
            }
            ev.payload
                .get("workflowId")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_string());
    let last_event_type = events.last().map(|ev| ev.event_type.clone());
    Ok(Some(serde_json::json!({
        "runId": run_id,
        "workflowId": workflow_id,
        "status": infer_run_status(&events),
        "lastSeq": events.last().map(|ev| event_seq_from_id(&ev.event_id)).unwrap_or_default(),
        "events": events.len(),
        "runDir": paths.workflow_run_dir(run_id).display().to_string(),
        "lastEventType": last_event_type,
    })))
}
