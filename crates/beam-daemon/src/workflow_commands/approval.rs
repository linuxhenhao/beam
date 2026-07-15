use axum::{Json, http::StatusCode};
use beam_core::{
    EventLog, ResolveWaitInput, WaitResolution, parse_workflow_definition, read_run_snapshot,
    resolve_wait,
};
use serde_json::{Value, json};

use super::map_anyhow;
use crate::{AppState, internal_error};

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

    if super::is_terminal(&snapshot.run.status) {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "runId": run_id,
                "resolution": super::resolution_str(resolution),
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

    super::run_workflow_runtime_once(state, run_id, &raw_def).await;

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
            "resolution": super::resolution_str(resolution),
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
            resolution: super::resolution_str(resolution).to_string(),
            resolved_at: None,
            last_seq: None,
            already_resolved: false,
            already_terminal: false,
            is_decision_node: None,
            error_code: Some("run_not_found".to_string()),
            error_hint: Some("workflow run not found".to_string()),
        });
    };

    if super::is_terminal(&snapshot.run.status) {
        return Ok(ApproveOrRejectOutcome {
            ok: true,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: super::resolution_str(resolution).to_string(),
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
            resolution: super::resolution_str(resolution).to_string(),
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
            resolution: super::resolution_str(resolution).to_string(),
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
            resolution: super::resolution_str(resolution).to_string(),
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
            resolution: super::resolution_str(resolution).to_string(),
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
            resolution: super::resolution_str(resolution).to_string(),
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
            resolution: super::resolution_str(resolution).to_string(),
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
                resolution: super::resolution_str(resolution).to_string(),
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

    super::run_workflow_runtime_once(state, run_id, &raw_def).await;

    let Some(after) = map_anyhow!(read_run_snapshot(&run_dir).await) else {
        return Ok(ApproveOrRejectOutcome {
            ok: false,
            run_id: run_id.to_string(),
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            resolution: super::resolution_str(resolution).to_string(),
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
        resolution: super::resolution_str(resolution).to_string(),
        resolved_at: Some(resolved_at),
        last_seq: Some(after.last_seq),
        already_resolved: false,
        already_terminal: false,
        is_decision_node: Some(is_decision_node),
        error_code: None,
        error_hint: None,
    })
}
