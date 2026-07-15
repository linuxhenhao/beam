//! Recovery invocation logic.
//!
//! Reconciliation of Feishu IM dangling effects, wait/cancel/worker-crashed
//! recovery helpers, and cold-attach (attempt resume) infrastructure.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path as StdPath, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use beam_core::{
    ActivityState, BeamPaths, EventDraft, EventLog, WorkflowActor, WorkflowEventEnvelope,
    WorkflowNode, WorkflowOutputRef,
};
use chrono::Utc;
use serde_json::Value;
use tokio;

use crate::{
    AppState, AttemptResumeEntry, AttemptResumeSidecar, AttemptResumeWaitOutcome,
    FeishuResumeOutcome, FeishuResumeResult, FeishuTransientFailure,
    is_lark_message_withdrawn_error, is_retryable_feishu_resume_error, lark_reply_message,
    lark_send_chat_message, sha256_hex, write_json_blob,
};

// ---------------------------------------------------------------------------
// Feishu IM resume helpers
// ---------------------------------------------------------------------------

/// Append a pair of failure events (`reconcileResult` + `activityFailed`) to
/// the event log for a Feishu IM resume that could not be completed.
pub(crate) async fn append_feishu_resume_failure(
    log: &mut EventLog,
    activity_id: &str,
    attempt_id: &str,
    idempotency_key: &str,
    decision: &str,
    capability: &str,
    error_code: &str,
    error_class: &str,
    error_message: String,
    evidence: Value,
) -> Result<()> {
    let _ = log.append(EventDraft {
        event_type: "reconcileResult".to_string(),
        actor: WorkflowActor::System,
        payload: serde_json::json!({
            "activityId": activity_id,
            "attemptId": attempt_id,
            "idempotencyKey": idempotency_key,
            "capability": capability,
            "decision": decision,
            "evidence": evidence,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    let _ = log.append(EventDraft {
        event_type: "activityFailed".to_string(),
        actor: WorkflowActor::System,
        payload: serde_json::json!({
            "activityId": activity_id,
            "attemptId": attempt_id,
            "error": {
                "errorCode": error_code,
                "errorClass": error_class,
                "errorMessage": error_message,
            }
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(())
}

/// Build a [`FeishuTransientFailure`] from individual parameters.
///
/// The error class is always set to `"retryable"`.
pub(crate) fn build_feishu_transient_failure(
    activity_id: &str,
    attempt_id: &str,
    provider: &str,
    idempotency_key: &str,
    error_code: &str,
    error_message: String,
) -> FeishuTransientFailure {
    FeishuTransientFailure {
        activity_id: activity_id.to_string(),
        attempt_id: attempt_id.to_string(),
        provider: provider.to_string(),
        idempotency_key: idempotency_key.to_string(),
        error_code: error_code.to_string(),
        error_class: "retryable".to_string(),
        error_message,
    }
}

// ---------------------------------------------------------------------------
// Attempt resume infrastructure (cold attach)
// ---------------------------------------------------------------------------

/// Build a deterministic lookup key for an attempt resume entry.
pub(crate) fn attempt_resume_key(run_id: &str, activity_id: &str, attempt_id: &str) -> String {
    format!("{run_id}\n{activity_id}\n{attempt_id}")
}

/// Write (or update) the attempt resume sidecar JSON file on disk.
pub(crate) async fn write_attempt_resume_sidecar(
    paths: &BeamPaths,
    entry: &AttemptResumeEntry,
    status: &str,
) -> Result<()> {
    let sidecar = AttemptResumeSidecar {
        schema_version: 1,
        resume_id: entry.resume_id.clone(),
        run_id: entry.run_id.clone(),
        activity_id: entry.activity_id.clone(),
        attempt_id: entry.attempt_id.clone(),
        session_id: entry.session_id.clone(),
        original_session_id: entry.original_session_id.clone(),
        cli_session_id: entry.cli_session_id.clone(),
        web_port: entry.web_port,
        write_token: entry.write_token.clone(),
        status: status.to_string(),
        lark_app_id: entry.lark_app_id.clone(),
        bot_name: entry.bot_name.clone(),
        cli_id: entry.cli_id.clone(),
        working_dir: entry.working_dir.clone(),
        log_path: entry.log_path.clone(),
        started_at: entry.started_at,
        updated_at: Utc::now().timestamp_millis().max(0) as u64,
        closed_at: if status == "closed" {
            Some(Utc::now().timestamp_millis().max(0) as u64)
        } else {
            None
        },
        close_reason: entry.close_reason.clone(),
    };
    tokio::fs::create_dir_all(
        paths
            .attempt_resume_dir(&entry.run_id, &entry.activity_id, &entry.attempt_id)
            .join(&entry.resume_id),
    )
    .await?;
    let path = paths.attempt_resume_json(
        &entry.run_id,
        &entry.activity_id,
        &entry.attempt_id,
        &entry.resume_id,
    );
    tokio::fs::write(&path, serde_json::to_vec_pretty(&sidecar)?).await?;
    Ok(())
}

/// Wait (polling) for an attempt-resume entry to become ready (web_port +
/// write_token populated) or for the sidecar to indicate terminal failure.
pub(crate) async fn wait_for_attempt_resume_ready(
    state: &AppState,
    key: &str,
    sidecar_path: &str,
) -> AttemptResumeWaitOutcome {
    loop {
        let entry = {
            let resumes = state.attempt_resumes.lock().await;
            resumes.get(key).cloned()
        };
        let Some(entry) = entry else {
            let sidecar = tokio::fs::read_to_string(sidecar_path).await;
            if let Ok(raw) = sidecar {
                if let Ok(parsed) = serde_json::from_str::<AttemptResumeSidecar>(&raw) {
                    let close_reason = parsed.close_reason.unwrap_or_else(|| {
                        if parsed.status == "closed" {
                            "worker_exited_before_ready".to_string()
                        } else {
                            "attempt_resume_closed".to_string()
                        }
                    });
                    let error = if close_reason.contains("worker_error") {
                        "worker_error"
                    } else {
                        "worker_exited_before_ready"
                    };
                    return AttemptResumeWaitOutcome::Failed {
                        error: error.to_string(),
                        message: Some(close_reason),
                    };
                }
            }
            return AttemptResumeWaitOutcome::Failed {
                error: "worker_exited_before_ready".to_string(),
                message: None,
            };
        };
        if entry.web_port.is_some() && entry.write_token.is_some() {
            return AttemptResumeWaitOutcome::Ready(entry);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Feishu IM dangling effects reconciliation
// ---------------------------------------------------------------------------

/// Reconcile all dangling Feishu IM effects for a workflow run.
///
/// For each activity with an `effect_attempted` targeting `"feishu-im"`:
/// 1. Attempt to recover a prior reconcile result.
/// 2. Load the effect input sidecar and parse it as [`FeishuResumeInput`].
/// 3. Resolve the target bot from the application state.
/// 4. Submit the message (send or reply) and record the outcome.
pub(crate) async fn resume_feishu_im_dangling_effects(
    log: &mut EventLog,
    state: &AppState,
    run_dir: &StdPath,
    snapshot: &beam_core::RunSnapshotDTO,
) -> Result<FeishuResumeResult> {
    let mut reconciled = Vec::new();
    let mut fresh_retry = Vec::new();
    let mut transient_failures = Vec::new();
    let mut skipped = Vec::new();

    for activity_id in &snapshot.dangling.effect_attempted {
        let Some(activity) = snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
        else {
            skipped.push(activity_id.clone());
            continue;
        };
        let Some(latest) = activity.attempts.last() else {
            skipped.push(activity_id.clone());
            continue;
        };
        let Some(effect_attempted) = latest.effect_attempted.as_ref() else {
            skipped.push(activity_id.clone());
            continue;
        };
        if effect_attempted.provider != "feishu-im" {
            skipped.push(activity_id.clone());
            continue;
        }

        if let Some(recovery) =
            beam_core::recover_prior_reconcile_result(log, activity_id, latest).await?
        {
            match recovery {
                beam_core::PriorReconcileRecoveryOutcome::Recovered {
                    activity_id,
                    attempt_id,
                    decision,
                } => {
                    reconciled.push(FeishuResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision,
                    });
                }
                beam_core::PriorReconcileRecoveryOutcome::FreshRetry {
                    activity_id,
                    attempt_id,
                } => {
                    fresh_retry.push(FeishuResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision: "freshRetry".to_string(),
                    });
                }
            }
            continue;
        }

        let Some(raw_input) =
            beam_core::load_effect_input_sidecar(run_dir, activity_id, &latest.attempt_id).await?
        else {
            append_feishu_resume_failure(
                log,
                activity_id,
                &latest.attempt_id,
                &effect_attempted.idempotency_key,
                "manual",
                "idempotentSubmit",
                "MissingEffectInputSidecar",
                "manual",
                "effect input sidecar is missing".to_string(),
                serde_json::json!({
                    "source": "effectInputSidecar",
                    "returned": "missing",
                }),
            )
            .await?;
            skipped.push(activity_id.clone());
            continue;
        };

        let input = match crate::parse_feishu_resume_input(&raw_input) {
            Ok(value) => value,
            Err(err) => {
                append_feishu_resume_failure(
                    log,
                    activity_id,
                    &latest.attempt_id,
                    &effect_attempted.idempotency_key,
                    "manual",
                    "idempotentSubmit",
                    "InvalidEffectInput",
                    "manual",
                    err.to_string(),
                    serde_json::json!({
                        "source": "effectInputSidecar",
                        "returned": "invalid",
                    }),
                )
                .await?;
                skipped.push(activity_id.clone());
                continue;
            }
        };

        let Some(bot) = state.bots.get(&input.lark_app_id).cloned() else {
            append_feishu_resume_failure(
                log,
                activity_id,
                &latest.attempt_id,
                &effect_attempted.idempotency_key,
                "manual",
                "idempotentSubmit",
                "UnknownProviderError",
                "manual",
                format!("bot '{}' is not registered.", input.lark_app_id),
                serde_json::json!({
                    "source": "botRegistry",
                    "returned": "missing",
                }),
            )
            .await?;
            skipped.push(activity_id.clone());
            continue;
        };

        let (submit_kind, submit_result) = if let Some(chat_id) = input.chat_id.as_deref() {
            (
                "send",
                lark_send_chat_message(state, &bot, chat_id, &input.content).await,
            )
        } else if let Some(root_message_id) = input.root_message_id.as_deref() {
            (
                "reply",
                lark_reply_message(state, &bot, root_message_id, &input.content).await,
            )
        } else {
            append_feishu_resume_failure(
                log,
                activity_id,
                &latest.attempt_id,
                &effect_attempted.idempotency_key,
                "manual",
                "idempotentSubmit",
                "InvalidEffectInput",
                "manual",
                "feishu-im effect input missing chatId/rootMessageId".to_string(),
                serde_json::json!({
                    "source": "effectInputSidecar",
                    "returned": "missing-target",
                }),
            )
            .await?;
            skipped.push(activity_id.clone());
            continue;
        };

        match submit_result {
            Ok(message_id) => {
                let output_ref =
                    write_json_blob(log, serde_json::json!({ "messageId": message_id.clone() }))?;
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": &latest.attempt_id,
                        "idempotencyKey": effect_attempted.idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "completedByIdempotentSubmit",
                        "evidence": {
                            "source": "lark",
                            "submitKind": submit_kind,
                            "messageId": message_id,
                        },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                let _ = log.append(EventDraft {
                    event_type: "activitySucceeded".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": &latest.attempt_id,
                        "outputRef": output_ref,
                        "externalRefs": { "messageId": message_id },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                reconciled.push(FeishuResumeOutcome {
                    activity_id: activity_id.clone(),
                    attempt_id: latest.attempt_id.clone(),
                    decision: "completedByIdempotentSubmit".to_string(),
                });
            }
            Err(err) if is_lark_message_withdrawn_error(&err) => {
                append_feishu_resume_failure(
                    log,
                    activity_id,
                    &latest.attempt_id,
                    &effect_attempted.idempotency_key,
                    "manual",
                    "idempotentSubmit",
                    "MessageWithdrawnError",
                    "manual",
                    err.to_string(),
                    serde_json::json!({
                        "source": "lark",
                        "submitKind": submit_kind,
                    }),
                )
                .await?;
                reconciled.push(FeishuResumeOutcome {
                    activity_id: activity_id.clone(),
                    attempt_id: latest.attempt_id.clone(),
                    decision: "manual".to_string(),
                });
            }
            Err(err) if is_retryable_feishu_resume_error(&err) => {
                transient_failures.push(build_feishu_transient_failure(
                    activity_id,
                    &latest.attempt_id,
                    "feishu-im",
                    &effect_attempted.idempotency_key,
                    "FeishuSubmitRetryable",
                    err.to_string(),
                ));
            }
            Err(err) => {
                append_feishu_resume_failure(
                    log,
                    activity_id,
                    &latest.attempt_id,
                    &effect_attempted.idempotency_key,
                    "manual",
                    "idempotentSubmit",
                    "FeishuSubmitFailed",
                    "manual",
                    err.to_string(),
                    serde_json::json!({
                        "source": "lark",
                        "submitKind": submit_kind,
                    }),
                )
                .await?;
                reconciled.push(FeishuResumeOutcome {
                    activity_id: activity_id.clone(),
                    attempt_id: latest.attempt_id.clone(),
                    decision: "manual".to_string(),
                });
            }
        }
    }

    Ok(FeishuResumeResult {
        reconciled,
        fresh_retry,
        transient_failures,
        skipped,
    })
}

// ---------------------------------------------------------------------------
// Recovery helpers (wait, cancel, worker-crashed)
// ---------------------------------------------------------------------------

/// Check whether an activity is owned by a `Decision` node in the workflow
/// definition.
pub(crate) fn resolve_activity_node_is_decision(
    workflow_def: &beam_core::WorkflowDefinition,
    activity: &ActivityState,
) -> bool {
    let Some(owner_node_id) = activity.owner_node_id.as_deref() else {
        return false;
    };
    matches!(
        workflow_def.nodes.get(owner_node_id),
        Some(WorkflowNode::Decision(_))
    )
}

/// Append events for a recovered wait resolution.
///
/// Returns a JSON summary of the recovery or `None` if the activity has no
/// wait or the wait has no resolution.
pub(crate) fn append_resume_wait_recovery(
    log: &mut EventLog,
    workflow_def: &beam_core::WorkflowDefinition,
    activity: &ActivityState,
) -> Result<Option<Value>> {
    let latest = activity
        .attempts
        .last()
        .context("activity missing latest attempt")?;
    let Some(wait) = latest.wait.as_ref() else {
        return Ok(None);
    };
    let Some(resolution) = wait.resolution.as_ref() else {
        return Ok(None);
    };
    let attempt_id = latest.attempt_id.clone();
    let activity_id = activity.activity_id.clone();
    let is_decision_node = resolve_activity_node_is_decision(workflow_def, activity);
    let terminal_event = match resolution.kind.as_str() {
        "resolved" => {
            if matches!(resolution.resolution.as_deref(), Some("rejected")) && !is_decision_node {
                log.append(EventDraft {
                    event_type: "activityFailed".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "error": {
                            "errorCode": "InputValidationFailed",
                            "errorClass": "userFault",
                            "errorMessage": format!(
                                "Recovered wait terminal: rejected by {}{}",
                                resolution.by.clone().unwrap_or_default(),
                                resolution
                                    .comment
                                    .as_ref()
                                    .map(|c| format!(": {}", c))
                                    .unwrap_or_default()
                            ),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?
            } else {
                let external_refs = serde_json::json!({
                    "resolution": resolution.resolution,
                    "by": resolution.by,
                    "comment": resolution.comment,
                });
                let output_ref = {
                    let bytes = serde_json::to_vec(&external_refs)?;
                    let hash = sha256_hex(&bytes);
                    let path = PathBuf::from(&log.blob_dir).join(&hash);
                    fs::write(&path, &bytes)?;
                    WorkflowOutputRef {
                        output_hash: format!("sha256:{hash}"),
                        output_path: path.display().to_string(),
                        output_bytes: bytes.len(),
                        output_schema_version: 1,
                        content_type: Some("application/json".to_string()),
                    }
                };
                log.append(EventDraft {
                    event_type: "activitySucceeded".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "outputRef": output_ref,
                        "externalRefs": external_refs,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?
            }
        }
        "deadlineExceeded" => {
            if matches!(wait.on_timeout.as_deref(), Some("success")) {
                let external_refs = serde_json::json!({ "defaultedToTimeout": true, "deadlineAt": resolution.deadline_at });
                let output_ref = {
                    let bytes = serde_json::to_vec(&external_refs)?;
                    let hash = sha256_hex(&bytes);
                    let path = PathBuf::from(&log.blob_dir).join(&hash);
                    fs::write(&path, &bytes)?;
                    WorkflowOutputRef {
                        output_hash: format!("sha256:{hash}"),
                        output_path: path.display().to_string(),
                        output_bytes: bytes.len(),
                        output_schema_version: 1,
                        content_type: Some("application/json".to_string()),
                    }
                };
                log.append(EventDraft {
                    event_type: "activitySucceeded".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "outputRef": output_ref,
                        "externalRefs": external_refs,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?
            } else {
                log.append(EventDraft {
                    event_type: "activityFailed".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "error": {
                            "errorCode": "WaitDeadlineExceeded",
                            "errorClass": "userFault",
                            "errorMessage": format!(
                                "Recovered wait terminal: deadline ({}) exceeded at {}",
                                resolution.deadline_at.unwrap_or_default(),
                                resolution.exceeded_at_ms.unwrap_or_default()
                            ),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(serde_json::json!({
        "activityId": activity_id,
        "attemptId": attempt_id,
        "kind": if terminal_event.event_type == "activitySucceeded" { "succeeded" } else { "failed" },
        "source": resolution.kind,
        "terminalEvent": crate::workflow_event_json(&terminal_event),
    })))
}

/// Append events for a recovered cancel request.
///
/// Returns a JSON summary of the recovery or `None` if the activity has no
/// cancel request.
pub(crate) fn append_resume_cancel_recovery(
    log: &mut EventLog,
    event_index: &HashMap<String, WorkflowEventEnvelope>,
    activity: &ActivityState,
) -> Result<Option<Value>> {
    let latest = activity
        .attempts
        .last()
        .context("activity missing latest attempt")?;
    let Some(cancel) = latest.cancel_request.as_ref() else {
        return Ok(None);
    };
    let activity_id = activity.activity_id.clone();
    let attempt_id = latest.attempt_id.clone();
    let reconcile_event = event_index.values().find_map(|event| {
        if event.event_type != "reconcileResult" {
            return None;
        }
        if event.payload.get("activityId").and_then(Value::as_str) != Some(activity_id.as_str()) {
            return None;
        }
        if event.payload.get("attemptId").and_then(Value::as_str) != Some(attempt_id.as_str()) {
            return None;
        }
        if let Some(effect_attempted) = latest.effect_attempted.as_ref() {
            if event.payload.get("idempotencyKey").and_then(Value::as_str)
                != Some(effect_attempted.idempotency_key.as_str())
            {
                return None;
            }
        }
        Some(event)
    });
    let reconcile_decision = reconcile_event
        .as_ref()
        .and_then(|event| event.payload.get("decision").and_then(Value::as_str))
        .map(|value| value.to_string())
        .or_else(|| {
            latest
                .latest_reconcile_result
                .as_ref()
                .map(|rr| rr.decision.clone())
        });
    let terminal_event = if latest.effect_attempted.is_none() {
        Some(log.append(EventDraft {
            event_type: "activityCanceled".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": attempt_id,
                "cancelOriginEventId": cancel.cancel_origin_event_id,
            }),
            timestamp: None,
            payload_hash: None,
        })?)
    } else {
        match reconcile_decision.as_deref() {
            Some("completedByIdempotentSubmit") | Some("freshRetry") => Some(log.append(EventDraft {
                event_type: "activityCanceled".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "cancelOriginEventId": cancel.cancel_origin_event_id,
                }),
                timestamp: None,
                payload_hash: None,
            })?),
            Some("manual") => Some(log.append(EventDraft {
                event_type: "activityFailed".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "error": {
                        "errorCode": latest
                            .latest_reconcile_result
                            .as_ref()
                            .and_then(|rr| rr.evidence.get("errorCode"))
                            .and_then(Value::as_str)
                            .unwrap_or("UnknownProviderError"),
                        "errorClass": "manual",
                        "errorMessage": format!(
                            "Recovered from prior crashed reconcile cycle (decision=manual, cancelOriginEventId={}).",
                            cancel.cancel_origin_event_id
                        ),
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })?),
            _ => None,
        }
    };
    let terminal_event = match terminal_event {
        Some(event) => event,
        None => return Ok(None),
    };
    let reconcile_event = reconcile_event.map(|event| crate::workflow_event_json(event));
    let kind = if terminal_event.event_type == "activityCanceled" {
        "cancelled"
    } else {
        "failed"
    };
    Ok(Some(serde_json::json!({
        "activityId": activity_id,
        "attemptId": attempt_id,
        "cancelOriginEventId": cancel.cancel_origin_event_id,
        "delivered": cancel.delivered,
        "kind": kind,
        "reconcileDecision": reconcile_decision,
        "reconcileEvent": reconcile_event,
        "terminalEvent": crate::workflow_event_json(&terminal_event),
    })))
}

/// Append events for a worker-crashed recovery.
///
/// Returns a JSON summary of the recovery or `None` if the activity already
/// has effect-attempted, wait, or cancel-request state (those are handled
/// by other recovery helpers).
pub(crate) fn append_resume_worker_crashed(
    log: &mut EventLog,
    activity: &ActivityState,
) -> Result<Option<Value>> {
    let latest = activity
        .attempts
        .last()
        .context("activity missing latest attempt")?;
    if latest.effect_attempted.is_some() || latest.wait.is_some() || latest.cancel_request.is_some()
    {
        return Ok(None);
    }
    let terminal_event = log.append(EventDraft {
        event_type: "activityFailed".to_string(),
        actor: WorkflowActor::System,
        payload: serde_json::json!({
            "activityId": activity.activity_id,
            "attemptId": latest.attempt_id,
            "error": {
                "errorCode": "WorkerCrashed",
                "errorClass": "retryable",
                "errorMessage": "Worker process exited before the activity reached a terminal state.",
            },
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(Some(serde_json::json!({
        "activityId": activity.activity_id,
        "attemptId": latest.attempt_id,
        "terminalEvent": crate::workflow_event_json(&terminal_event),
    })))
}
