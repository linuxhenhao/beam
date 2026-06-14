//! Unified workflow command handlers shared by dashboard & Lark card-action paths.
//!
//! Phase 5.1 / 5.2: approve/reject/cancel write the correct EventLog events,
//! check idempotency, and push the runtime without duplicating logic.

use axum::{Json, http::StatusCode};
use beam_core::{
    EventLog, ResolveWaitInput, WaitResolution, WorkflowActor, parse_workflow_definition,
    read_run_snapshot, request_cancel, resolve_wait,
};
use serde_json::{Value, json};

use crate::{AppState, internal_error};

/// Convert any Display error into an `anyhow::Error`, logging on the way.
/// Used inside functions that return `anyhow::Result` (Lark handler, cancel handler).
macro_rules! map_anyhow {
    ($e:expr) => {
        $e.map_err(|e| {
            tracing::error!("workflow_commands: {}", e);
            anyhow::anyhow!("{}", e)
        })?
    };
}

/// Outcome of an approve/reject command.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApproveOrRejectOutcome {
    pub ok: bool,
    pub run_id: String,
    pub activity_id: String,
    pub attempt_id: String,
    pub resolution: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    #[serde(default)]
    pub already_resolved: bool,
    #[serde(default)]
    pub already_terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_decision_node: Option<bool>,
    /// If not ok, this holds the HTTP status code and a machine-readable error tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_hint: Option<String>,
}

/// Outcome of a cancel-run command.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunOutcome {
    pub ok: bool,
    pub run_id: String,
    pub status: String,
    #[serde(default)]
    pub already_cancelled: bool,
    #[serde(default)]
    pub already_terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_hint: Option<String>,
}

// ---------------------------------------------------------------------------
// Core helpers
// ---------------------------------------------------------------------------

/// Check whether a run has already reached a terminal status.
fn is_terminal(status: &beam_core::RunStatus) -> bool {
    matches!(
        status,
        beam_core::RunStatus::Succeeded
            | beam_core::RunStatus::Failed
            | beam_core::RunStatus::Cancelled
    )
}

/// Convert resolution to a short string.
fn resolution_str(r: WaitResolution) -> &'static str {
    match r {
        WaitResolution::Approved => "approved",
        WaitResolution::Rejected => "rejected",
        WaitResolution::External => "external",
    }
}

// ---------------------------------------------------------------------------
// Approve / reject wait — unified implementation
// ---------------------------------------------------------------------------

/// Execute an approve or reject command for a **dashboard-originated** request.
///
/// Dashboard uses the "single dangling human-gate wait" heuristic (no
/// `activity_id` / `attempt_id` in the request).  If the run has multiple
/// waits or has an approver allowlist the call is rejected.
pub async fn dashboard_approve_or_reject_wait(
    state: &AppState,
    run_id: &str,
    resolution: WaitResolution,
    comment: Option<String>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let run_dir = state.paths.workflow_run_dir(run_id);
    let Some(snapshot) = read_run_snapshot(&run_dir).await.map_err(internal_error)? else {
        return Err((StatusCode::NOT_FOUND, "workflow run not found".to_string()));
    };

    if is_terminal(&snapshot.run.status) {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "runId": run_id,
                "resolution": resolution_str(resolution),
                "activityId": "",
                "attemptId": "",
                "resolvedAt": snapshot.updated_at,
                "lastSeq": snapshot.last_seq,
                "alreadyTerminal": true,
            })),
        ));
    }

    let raw_def = tokio::fs::read_to_string(run_dir.join("workflow.json"))
        .await
        .map_err(internal_error)?;
    let def = parse_workflow_definition(&raw_def).map_err(internal_error)?;

    let mut candidates: Vec<(String, String, Option<Vec<String>>, Option<String>)> = Vec::new();
    for activity_id in &snapshot.dangling.waits {
        let Some(activity) = snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
        else {
            continue;
        };
        let Some(attempt) = activity.attempts.last() else {
            continue;
        };
        let Some(wait) = attempt.wait.as_ref() else {
            continue;
        };
        if wait.wait_kind != "human-gate" {
            continue;
        }
        candidates.push((
            activity_id.clone(),
            attempt.attempt_id.clone(),
            wait.approvers.clone(),
            activity.owner_node_id.clone(),
        ));
    }

    if candidates.is_empty() {
        return Err((
            StatusCode::CONFLICT,
            serde_json::to_string(&json!({
                "ok": false,
                "error": "no_open_wait",
                "hint": "No pending humanGate wait on this run.",
            }))
            .unwrap_or_else(|_| r#"{"ok":false,"error":"no_open_wait"}"#.to_string()),
        ));
    }
    if candidates.len() > 1 {
        return Err((
            StatusCode::CONFLICT,
            serde_json::to_string(&json!({
                "ok": false,
                "error": "ambiguous_wait",
                "hint": format!(
                    "Run has {} pending humanGate waits; dashboard cannot pick one yet. Use the Lark approval card.",
                    candidates.len()
                ),
            }))
            .unwrap_or_else(|_| r#"{"ok":false,"error":"ambiguous_wait"}"#.to_string()),
        ));
    }
    let (activity_id, attempt_id, approvers, owner_node_id) = candidates.remove(0);
    if approvers
        .as_ref()
        .map(|items| !items.is_empty())
        .unwrap_or(false)
    {
        return Err((
            StatusCode::FORBIDDEN,
            serde_json::to_string(&json!({
                "ok": false,
                "error": "needs_lark_approval",
                "hint": "This gate has an approver allowlist; the Lark approval card is the only path that authenticates the approver identity.",
            }))
            .unwrap_or_else(|_| r#"{"ok":false,"error":"needs_lark_approval"}"#.to_string()),
        ));
    }

    let is_decision_node = owner_node_id
        .as_deref()
        .and_then(|node_id| def.nodes.get(node_id))
        .map(|node| matches!(node, beam_core::WorkflowNode::Decision(_)))
        .unwrap_or(false);

    let mut log = EventLog::new(run_id.to_string(), state.paths.workflow_runs_dir())
        .map_err(internal_error)?;
    let _resolved = resolve_wait(
        &mut log,
        ResolveWaitInput {
            activity_id: activity_id.clone(),
            attempt_id: attempt_id.clone(),
            resolution,
            by: "dashboard".to_string(),
            comment: comment.clone(),
            output: None,
            is_decision_node,
        },
    )
    .await
    .map_err(internal_error)?;

    let events = log.read_all().map_err(internal_error)?;
    let resolved_at = events
        .iter()
        .rev()
        .find(|event| {
            event.event_type == "waitResolved"
                && event.payload.get("activityId").and_then(Value::as_str)
                    == Some(activity_id.as_str())
        })
        .map(|event| event.timestamp)
        .unwrap_or(snapshot.updated_at);

    run_workflow_runtime_once(state, run_id, &raw_def).await;

    let Some(after) = read_run_snapshot(&run_dir).await.map_err(internal_error)? else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to re-read after resolve".to_string(),
        ));
    };

    Ok((
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "runId": run_id,
            "resolution": resolution_str(resolution),
            "activityId": activity_id,
            "attemptId": attempt_id,
            "resolvedAt": resolved_at,
            "lastSeq": after.last_seq,
            "alreadyResolved": false,
        })),
    ))
}

/// Execute an approve or reject command for a **Lark-card-originated** request.
///
/// This path receives explicit `activity_id` / `attempt_id` from the card
/// payload and validates:
/// - The wait is an open human-gate wait.
/// - If an approver allowlist exists, `operator_open_id` is checked.
/// - Idempotency: already resolved / terminal waits are returned without
///   re-writing events (alreadyResolved / alreadyTerminal semantics).
pub async fn lark_approve_or_reject_wait(
    state: &AppState,
    run_id: &str,
    activity_id: &str,
    attempt_id: &str,
    operator_open_id: &str,
    resolution: WaitResolution,
    comment: Option<String>,
) -> anyhow::Result<ApproveOrRejectOutcome> {
    let run_dir = state.paths.workflow_run_dir(run_id);
    let Some(snapshot) = map_anyhow!(read_run_snapshot(&run_dir).await) else {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("run_not_found".to_string()),
            error_hint: Some("workflow run not found".to_string()),
        });
    };

    if is_terminal(&snapshot.run.status) {
        return Ok(ApproveOrRejectOutcome {
            ok: true,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: Some(snapshot.updated_at),
            last_seq: Some(snapshot.last_seq),
            already_resolved: false,
            already_terminal: true,
            is_decision_node: None,
            error_code: None,
            error_hint: None,
        });
    }

    let raw_def = map_anyhow!(tokio::fs::read_to_string(run_dir.join("workflow.json")).await);
    let def = map_anyhow!(parse_workflow_definition(&raw_def));

    // Locate the specific activity.
    let Some(activity) = snapshot
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id)
    else {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("activity_not_found".to_string()),
            error_hint: Some(format!("Activity {} not found in snapshot", activity_id)),
        });
    };

    let Some(attempt) = activity.attempts.last() else {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("attempt_not_found".to_string()),
            error_hint: Some("No attempts found on activity".to_string()),
        });
    };

    // Verify the attempt_id matches the latest attempt.
    if attempt.attempt_id != attempt_id {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("stale_attempt".to_string()),
            error_hint: Some(format!(
                "Attempt {} is not the latest attempt (current: {})",
                attempt_id, attempt.attempt_id
            )),
        });
    };

    // Check that this is an open human-gate wait.
    let Some(wait) = attempt.wait.as_ref() else {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("not_a_wait".to_string()),
            error_hint: Some("Activity does not have a wait".to_string()),
        });
    };

    if wait.wait_kind != "human-gate" {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("not_human_gate".to_string()),
            error_hint: Some(format!("Wait kind '{}' is not human-gate", wait.wait_kind)),
        });
    };

    // Check activity is in dangling waits.
    if !snapshot.dangling.waits.contains(&activity_id.to_string()) {
        // Already resolved (or not a wait) - idempotent success.
        return Ok(ApproveOrRejectOutcome {
            ok: true,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: Some(snapshot.updated_at),
            last_seq: Some(snapshot.last_seq),
            already_resolved: true,
            already_terminal: false,
            is_decision_node: None,
            error_code: None,
            error_hint: None,
        });
    }

    // Check approver allowlist.
    if let Some(approvers) = wait.approvers.as_ref().filter(|v| !v.is_empty()) {
        if !approvers.contains(&operator_open_id.to_string()) {
            return Ok(ApproveOrRejectOutcome {
                ok: false,
                run_id: run_id.to_string(),
                activity_id: activity_id.to_string(),
                attempt_id: attempt_id.to_string(),
                resolution: resolution_str(resolution).to_string(),
                resolved_at: None,
                last_seq: None,
                already_resolved: false,
                already_terminal: false,
                is_decision_node: None,
                error_code: Some("not_approved".to_string()),
                error_hint: Some("Operator is not in the approver allowlist".to_string()),
            });
        }
    }

    let is_decision_node = activity
        .owner_node_id
        .as_deref()
        .and_then(|node_id| def.nodes.get(node_id))
        .map(|node| matches!(node, beam_core::WorkflowNode::Decision(_)))
        .unwrap_or(false);

    let mut log = map_anyhow!(EventLog::new(
        run_id.to_string(),
        state.paths.workflow_runs_dir()
    ));
    let resolved = map_anyhow!(
        resolve_wait(
            &mut log,
            ResolveWaitInput {
                activity_id: activity_id.to_string(),
                attempt_id: attempt_id.to_string(),
                resolution,
                by: operator_open_id.to_string(),
                comment: comment.clone(),
                output: None,
                is_decision_node,
            },
        )
        .await
    );

    let events = map_anyhow!(log.read_all());
    let resolved_at = events
        .iter()
        .rev()
        .find(|event| {
            event.event_type == "waitResolved"
                && event.payload.get("activityId").and_then(Value::as_str) == Some(activity_id)
        })
        .map(|event| event.timestamp)
        .unwrap_or(snapshot.updated_at);

    let _ = resolved; // keep reference alive

    run_workflow_runtime_once(state, run_id, &raw_def).await;

    let Some(after) = map_anyhow!(read_run_snapshot(&run_dir).await) else {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("re_read_failed".to_string()),
            error_hint: Some("Failed to re-read after resolve".to_string()),
        });
    };

    Ok(ApproveOrRejectOutcome {
        ok: true,
        run_id: run_id.to_string(),
        activity_id: activity_id.to_string(),
        attempt_id: attempt_id.to_string(),
        resolution: resolution_str(resolution).to_string(),
        resolved_at: Some(resolved_at),
        last_seq: Some(after.last_seq),
        already_resolved: false,
        already_terminal: false,
        is_decision_node: Some(is_decision_node),
        error_code: None,
        error_hint: None,
    })
}

// ---------------------------------------------------------------------------
// Cancel run — unified implementation
// ---------------------------------------------------------------------------

/// Execute a cancel-run command.
///
/// Writes exactly one `cancelRequested` event to the log (NOT `runCanceled`).
/// The runtime is then advanced via `run_workflow_runtime_once` so that the
/// cancel propagates downstream (nodes, activities, etc.).
///
/// Idempotency:
/// - If the run is already terminal → returns alreadyTerminal.
/// - If `cancelRequested` was already written for this run → returns
///   alreadyCancelled (does NOT duplicate the event or write `runCanceled`).
pub async fn cancel_run(
    state: &AppState,
    run_id: &str,
    reason: Option<String>,
) -> anyhow::Result<CancelRunOutcome> {
    let run_dir = state.paths.workflow_run_dir(run_id);
    let Some(snapshot) = map_anyhow!(read_run_snapshot(&run_dir).await) else {
        return Ok(CancelRunOutcome {
            ok: false,
            run_id: run_id.to_string(),
            status: "unknown".to_string(),
            already_cancelled: false,
            already_terminal: false,
            last_seq: None,
            error_code: Some("run_not_found".to_string()),
            error_hint: Some("workflow run not found".to_string()),
        });
    };

    if is_terminal(&snapshot.run.status) {
        return Ok(CancelRunOutcome {
            ok: true,
            run_id: run_id.to_string(),
            status: serde_json::to_string(&snapshot.run.status)
                .unwrap_or_else(|_| "unknown".to_string())
                .trim_matches('"')
                .to_string(),
            already_cancelled: false,
            already_terminal: true,
            last_seq: Some(snapshot.last_seq),
            error_code: None,
            error_hint: None,
        });
    }

    // Idempotency: if there is already a cancelled_run_intent, don't re-write.
    if snapshot.run.cancelled_run_intent.is_some() {
        return Ok(CancelRunOutcome {
            ok: true,
            run_id: run_id.to_string(),
            status: "running".to_string(),
            already_cancelled: true,
            already_terminal: false,
            last_seq: Some(snapshot.last_seq),
            error_code: None,
            error_hint: None,
        });
    }

    let reason = reason
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "cancelled via beam daemon".to_string());
    let mut log = map_anyhow!(EventLog::new(
        run_id.to_string(),
        state.paths.workflow_runs_dir()
    ));
    let cancel_requested = map_anyhow!(
        request_cancel(
            &mut log,
            beam_core::RequestCancelInput {
                target: serde_json::json!({ "kind": "run", "runId": run_id }),
                reason,
                by: "beam-daemon".to_string(),
            },
            WorkflowActor::Human,
        )
        .await
    );

    let cancel_event_id = cancel_requested
        .get("eventId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    // IMPORTANT: We do NOT call complete_run_cancel here.
    // The runtime (run_workflow_runtime_once) will propagate the cancel
    // to nodes and activities, eventually writing runCanceled when the
    // entire tree has been cancelled.
    let _ = cancel_event_id;

    // Read workflow.json and push the runtime so the cancel propagates.
    let raw_def = match tokio::fs::read_to_string(run_dir.join("workflow.json")).await {
        Ok(def) => def,
        Err(_) => {
            // Fallback: if we can't read the definition, just return success
            // (the cancelRequested event is already written).
            let Some(updated) = map_anyhow!(read_run_snapshot(&run_dir).await) else {
                return Ok(CancelRunOutcome {
                    ok: true,
                    run_id: run_id.to_string(),
                    status: "running".to_string(),
                    already_cancelled: false,
                    already_terminal: false,
                    last_seq: None,
                    error_code: None,
                    error_hint: None,
                });
            };
            return Ok(CancelRunOutcome {
                ok: true,
                run_id: run_id.to_string(),
                status: "running".to_string(),
                already_cancelled: false,
                already_terminal: false,
                last_seq: Some(updated.last_seq),
                error_code: None,
                error_hint: None,
            });
        }
    };

    run_workflow_runtime_once(state, run_id, &raw_def).await;

    let Some(updated) = map_anyhow!(read_run_snapshot(&run_dir).await) else {
        return Ok(CancelRunOutcome {
            ok: true,
            run_id: run_id.to_string(),
            status: "running".to_string(),
            already_cancelled: false,
            already_terminal: false,
            last_seq: None,
            error_code: None,
            error_hint: None,
        });
    };

    Ok(CancelRunOutcome {
        ok: true,
        run_id: run_id.to_string(),
        status: serde_json::to_string(&updated.run.status)
            .unwrap_or_else(|_| "unknown".to_string())
            .trim_matches('"')
            .to_string(),
        already_cancelled: false,
        already_terminal: false,
        last_seq: Some(updated.last_seq),
        error_code: None,
        error_hint: None,
    })
}

// ---------------------------------------------------------------------------
// Runtime advancement — thin wrapper so callers don't need to import
// ---------------------------------------------------------------------------

pub async fn run_workflow_runtime_once(state: &AppState, run_id: &str, raw_def: &str) {
    crate::run_workflow_runtime_once(state, run_id, raw_def).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::{BeamPaths, BootstrapWorkflowRunInput, bootstrap_workflow_run};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-wf-cmds-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    fn make_state(paths: &BeamPaths) -> AppState {
        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        AppState {
            paths: paths.clone(),
            started_at: chrono::Utc::now(),
            sessions: std::sync::Arc::new(
                tokio::sync::Mutex::new(std::collections::HashMap::new()),
            ),
            workers: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            attempt_resumes: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            shutdown: std::sync::Arc::new(tokio::sync::Mutex::new(Some(_shutdown_tx))),
            options: crate::RunOptions {
                worker_exe: std::path::PathBuf::from("/bin/true"),
            },
            http: reqwest::Client::new(),
            config: beam_core::Config::default(),
            bots: std::sync::Arc::new(std::collections::HashMap::new()),
            lark_tokens: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            chat_mode_cache: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            recent_lark_events: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            inflight_final_output_turns: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            workflow_progress_cards: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            ask_pending: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            grant_pending: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            dashboard_token: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            external_host: "localhost".to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // Lark approve / reject tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn lark_approve_writes_wait_resolved() {
        let paths = temp_paths("lark-approve");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-lark-approve";

        // Bootstrap a tiny workflow with a human-gate node.
        // Human-gate wait via hostExecutor node with humanGate field.
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
        let _bootstrap = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        // Advance runtime once to create the wait.
        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // Read snapshot to get activity_id / attempt_id.
        let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .expect("snapshot")
            .expect("snapshot exists");
        assert!(
            !snap.dangling.waits.is_empty(),
            "expected dangling waits after runtime boot"
        );
        let activity_id = snap.dangling.waits[0].clone();
        let activity = snap
            .activities
            .iter()
            .find(|a| a.activity_id == activity_id)
            .expect("activity");
        let attempt_id = activity
            .attempts
            .last()
            .expect("attempt")
            .attempt_id
            .clone();

        // Execute approve via Lark path.
        let outcome = lark_approve_or_reject_wait(
            &state,
            run_id,
            &activity_id,
            &attempt_id,
            "user_approver",
            WaitResolution::Approved,
            None,
        )
        .await
        .expect("lark approve");

        assert!(outcome.ok, "approve should succeed: {:?}", outcome);
        assert!(!outcome.already_resolved);
        assert!(!outcome.already_terminal);

        // Verify the event log contains waitResolved.
        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let resolved_events: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "waitResolved")
            .collect();
        assert_eq!(
            resolved_events.len(),
            1,
            "should have exactly one waitResolved event, got {}",
            resolved_events.len()
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn lark_reject_writes_wait_resolved_rejected() {
        let paths = temp_paths("lark-reject");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-lark-reject";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .expect("snapshot")
            .expect("snapshot exists");
        let activity_id = snap.dangling.waits[0].clone();
        let activity = snap
            .activities
            .iter()
            .find(|a| a.activity_id == activity_id)
            .expect("activity");
        let attempt_id = activity
            .attempts
            .last()
            .expect("attempt")
            .attempt_id
            .clone();

        let outcome = lark_approve_or_reject_wait(
            &state,
            run_id,
            &activity_id,
            &attempt_id,
            "user_approver",
            WaitResolution::Rejected,
            Some("nope".to_string()),
        )
        .await
        .expect("lark reject");

        assert!(outcome.ok, "reject should succeed: {:?}", outcome);
        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let resolved: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "waitResolved")
            .collect();
        assert_eq!(
            resolved.len(),
            1,
            "should have exactly one waitResolved event"
        );
        let res_event = resolved[0];
        assert_eq!(
            res_event.payload["resolution"], "rejected",
            "resolution should be rejected"
        );
        assert_eq!(
            res_event.payload["by"], "user_approver",
            "by should be the approver"
        );
        assert_eq!(res_event.payload["comment"], "nope");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn lark_approve_repeated_is_idempotent() {
        let paths = temp_paths("lark-idem");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-lark-idem";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .expect("snapshot")
            .expect("snapshot exists");
        let activity_id = snap.dangling.waits[0].clone();
        let activity = snap
            .activities
            .iter()
            .find(|a| a.activity_id == activity_id)
            .expect("activity");
        let attempt_id = activity
            .attempts
            .last()
            .expect("attempt")
            .attempt_id
            .clone();

        // First approve.
        let outcome1 = lark_approve_or_reject_wait(
            &state,
            run_id,
            &activity_id,
            &attempt_id,
            "user_approver",
            WaitResolution::Approved,
            None,
        )
        .await
        .expect("first approve");
        assert!(outcome1.ok);
        assert!(!outcome1.already_resolved);

        // Second approve — should be idempotent (alreadyResolved).
        let outcome2 = lark_approve_or_reject_wait(
            &state,
            run_id,
            &activity_id,
            &attempt_id,
            "user_approver",
            WaitResolution::Approved,
            None,
        )
        .await
        .expect("second approve");
        assert!(outcome2.ok, "second approve should succeed: {:?}", outcome2);
        // After the first approve, the runtime may finish the run entirely,
        // so the second call may see either alreadyResolved or alreadyTerminal.
        assert!(
            outcome2.already_resolved || outcome2.already_terminal,
            "second approve should be idempotent (alreadyResolved or alreadyTerminal), got: ok={}, already_resolved={}, already_terminal={}",
            outcome2.ok,
            outcome2.already_resolved,
            outcome2.already_terminal,
        );

        // Verify only one waitResolved event was written.
        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let resolved_count = events
            .iter()
            .filter(|e| e.event_type == "waitResolved")
            .count();
        assert_eq!(
            resolved_count, 1,
            "should have exactly one waitResolved event, got {}",
            resolved_count
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn lark_approve_with_approver_allowlist() {
        let paths = temp_paths("lark-allowlist");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-lark-allowlist";

        // Include an approver allowlist: only "user_a" and "user_b".
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?","approvers":["user_a","user_b"]}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .expect("snapshot")
            .expect("snapshot exists");
        let activity_id = snap.dangling.waits[0].clone();
        let activity = snap
            .activities
            .iter()
            .find(|a| a.activity_id == activity_id)
            .expect("activity");
        let attempt_id = activity
            .attempts
            .last()
            .expect("attempt")
            .attempt_id
            .clone();

        // Non-approved user.
        let outcome_denied = lark_approve_or_reject_wait(
            &state,
            run_id,
            &activity_id,
            &attempt_id,
            "user_c",
            WaitResolution::Approved,
            None,
        )
        .await
        .expect("denied approve");
        assert!(!outcome_denied.ok);
        assert_eq!(outcome_denied.error_code.as_deref(), Some("not_approved"));

        // Approved user.
        let outcome_ok = lark_approve_or_reject_wait(
            &state,
            run_id,
            &activity_id,
            &attempt_id,
            "user_a",
            WaitResolution::Approved,
            None,
        )
        .await
        .expect("allowed approve");
        assert!(outcome_ok.ok);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn lark_approve_already_terminal_is_idempotent() {
        let paths = temp_paths("lark-terminal");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-lark-terminal";

        // Use a node that succeeds immediately (no wait).
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"true"},"unsafeAllowUngated":true}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let outcome = lark_approve_or_reject_wait(
            &state,
            run_id,
            "nonexistent-activity",
            "nonexistent-attempt",
            "user",
            WaitResolution::Approved,
            None,
        )
        .await
        .expect("approve on terminal run");

        // The run is Succeeded (terminal), so the outcome should reflect that.
        assert!(outcome.already_terminal || !outcome.ok);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    // -----------------------------------------------------------------------
    // Cancel tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cancel_run_writes_cancel_requested_not_run_canceled() {
        let paths = temp_paths("cancel-write");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-cancel-write";

        // Use a human-gate workflow so the run stays in Waiting state.
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let outcome = cancel_run(&state, run_id, Some("test cancel".to_string()))
            .await
            .expect("cancel");

        assert!(outcome.ok, "cancel should succeed: {:?}", outcome);
        assert!(!outcome.already_cancelled);
        assert!(!outcome.already_terminal);

        // Verify the log contains cancelRequested but NOT runCanceled.
        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let has_cancel_requested = events.iter().any(|e| e.event_type == "cancelRequested");
        let has_run_canceled = events.iter().any(|e| e.event_type == "runCanceled");

        assert!(has_cancel_requested, "should have cancelRequested event");
        assert!(
            !has_run_canceled,
            "should NOT have runCanceled event (runtime propagates it)"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn cancel_run_repeated_is_idempotent() {
        let paths = temp_paths("cancel-idem");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-cancel-idem";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // First cancel.
        let outcome1 = cancel_run(&state, run_id, Some("first".to_string()))
            .await
            .expect("first cancel");
        assert!(outcome1.ok);
        assert!(!outcome1.already_cancelled);

        // Second cancel — should be idempotent.
        let outcome2 = cancel_run(&state, run_id, Some("second".to_string()))
            .await
            .expect("second cancel");
        assert!(outcome2.ok);
        assert!(
            outcome2.already_cancelled,
            "second cancel should be alreadyCancelled"
        );

        // Verify only one cancelRequested event was written.
        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let cancel_count = events
            .iter()
            .filter(|e| e.event_type == "cancelRequested")
            .count();
        assert_eq!(
            cancel_count, 1,
            "should have exactly one cancelRequested event, got {}",
            cancel_count
        );
        // Also verify no runCanceled was written.
        let has_run_canceled = events.iter().any(|e| e.event_type == "runCanceled");
        assert!(!has_run_canceled);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn cancel_do_not_write_run_canceled_immediately_after_cancel_requested() {
        let paths = temp_paths("cancel-no-direct");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-cancel-no-direct";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let outcome = cancel_run(&state, run_id, Some("test".to_string()))
            .await
            .expect("cancel");

        assert!(outcome.ok);

        // Assert handler does not write runCanceled.
        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let has_cancel_requested = events.iter().any(|e| e.event_type == "cancelRequested");
        let has_run_canceled = events.iter().any(|e| e.event_type == "runCanceled");
        assert!(has_cancel_requested, "should have cancelRequested event");
        assert!(
            !has_run_canceled,
            "handler should NOT call complete_run_cancel"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn cancel_already_terminal_is_idempotent() {
        let paths = temp_paths("cancel-terminal");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-cancel-terminal";

        // HostExecutor that finishes immediately.
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"true"},"unsafeAllowUngated":true}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let outcome = cancel_run(&state, run_id, Some("late cancel".to_string()))
            .await
            .expect("cancel on terminal");

        assert!(outcome.ok);
        assert!(outcome.already_terminal);
        assert!(!outcome.already_cancelled);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn cancel_run_nonexistent_returns_error() {
        let paths = temp_paths("cancel-nofound");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);

        let outcome = cancel_run(&state, "nonexistent-run", None)
            .await
            .expect("cancel nonexistent");
        assert!(!outcome.ok);
        assert_eq!(outcome.error_code.as_deref(), Some("run_not_found"));

        let _ = std::fs::remove_dir_all(paths.root());
    }

    // -----------------------------------------------------------------------
    // Dashboard approve / reject tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn dashboard_approve_writes_wait_resolved() {
        let paths = temp_paths("dash-approve");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-dash-approve";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let result = dashboard_approve_or_reject_wait(
            &state,
            run_id,
            WaitResolution::Approved,
            Some("looks good".to_string()),
        )
        .await
        .expect("dash approve");
        assert_eq!(result.0, StatusCode::OK);

        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let resolved_count = events
            .iter()
            .filter(|e| e.event_type == "waitResolved")
            .count();
        assert_eq!(resolved_count, 1);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn dashboard_reject_writes_wait_resolved_rejected() {
        let paths = temp_paths("dash-reject");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-dash-reject";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let result =
            dashboard_approve_or_reject_wait(&state, run_id, WaitResolution::Rejected, None)
                .await
                .expect("dash reject");
        assert_eq!(result.0, StatusCode::OK);

        let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("read events");
        let resolved: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "waitResolved")
            .collect();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].payload["resolution"], "rejected");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn dashboard_approve_already_terminal_is_idempotent() {
        let paths = temp_paths("dash-terminal");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-dash-terminal";

        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"true"},"unsafeAllowUngated":true}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let result =
            dashboard_approve_or_reject_wait(&state, run_id, WaitResolution::Approved, None)
                .await
                .expect("dash terminal approve");
        let (status, body) = result;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["alreadyTerminal"], true);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn dashboard_approve_no_wait_returns_already_terminal() {
        let paths = temp_paths("dash-nowait");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-dash-nowait";

        // HostExecutor finishes immediately, no wait.  The handler returns
        // Ok instead of Err when the run is already terminal (idempotency).
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"true"},"unsafeAllowUngated":true}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &std::collections::BTreeMap::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let result =
            dashboard_approve_or_reject_wait(&state, run_id, WaitResolution::Approved, None).await;
        assert!(result.is_ok(), "already-terminal run should succeed");
        let (status, body) = result.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["alreadyTerminal"], true);

        let _ = std::fs::remove_dir_all(paths.root());
    }
}
