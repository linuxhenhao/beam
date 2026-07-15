use beam_core::{EventLog, WorkflowActor, read_run_snapshot, request_cancel};
use serde_json::Value;

use crate::AppState;

use super::{is_terminal, map_anyhow};

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
    //
    // Cancel any active dispatch tokens immediately so that in-flight
    // subagent/host-executor dispatches can observe the cancellation and
    // return early.  The EventLog-based propagation through
    // check_pending_cancels / on_activities_cancelled will also cancel
    // tokens, but that only runs at the start of the next run_loop tick.
    let _ = cancel_event_id;
    {
        let reg = crate::workflow_cancellation::global_cancellation_registry();
        let count = reg.cancel_run(run_id).len();
        if count > 0 {
            tracing::info!(
                "cancellation registry: cancel_run({}) cancelled {} active dispatch tokens",
                run_id,
                count
            );
        }
    }

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

    super::run_workflow_runtime_once(state, run_id, &raw_def).await;

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
