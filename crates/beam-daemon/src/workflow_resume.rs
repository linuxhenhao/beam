//! Workflow resume and cold-attach logic.
//!
//! Extracted from `lib.rs` (Task 9.1) to separate the resume/recovery/dangling-effects
//! machinery from route handlers and app wiring.
//!
//! This module handles:
//! - Feishu IM dangling effect reconciliation (idempotent re-submission)
//! - Attempt resume infrastructure (cold-attach worker lifecycle)
//! - Wait/cancel/worker-crashed recovery helpers
//! - Resume response building

// Some functions are only used from `lib.rs` route handlers via the
// `pub(crate) use workflow_resume::*;` re-export, and a few legacy helpers
// (resume_feishu_im_dangling_effects and its callees) are only referenced
// by tests. The compiler's `dead_code` lint doesn't count re-exported or
// test-only usage.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path as StdPath, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use beam_core::{
    ActivityState, BeamPaths, EventDraft, EventLog, RunStatus, ScheduleResumeOutcome,
    ScheduleResumeResult, WorkflowActor, WorkflowEventEnvelope, WorkflowNode, WorkflowOutputRef,
    event_seq_from_id,
};
use chrono::Utc;
use serde_json::Value;
// sha256_hex and write_json_blob are accessed via crate::

use tokio;

use crate::{
    AppState, AttemptResumeEntry, AttemptResumeRequest, AttemptResumeSidecar,
    AttemptResumeWaitOutcome, FeishuResumeInput, FeishuResumeOutcome, FeishuResumeResult,
    FeishuTransientFailure, is_lark_message_withdrawn_error, is_retryable_feishu_resume_error,
    lark_reply_message, lark_send_chat_message, sha256_hex, workflow_reconcilers, write_json_blob,
};

// ---------------------------------------------------------------------------
// Feishu IM resume helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_feishu_resume_input(raw: &Value) -> Result<FeishuResumeInput> {
    serde_json::from_value::<FeishuResumeInput>(raw.clone())
        .context("invalid feishu-im effect input")
}

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

pub(crate) fn attempt_resume_key(run_id: &str, activity_id: &str, attempt_id: &str) -> String {
    format!("{run_id}\n{activity_id}\n{attempt_id}")
}

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

pub(crate) fn parse_attempt_resume_request_body(
    body: &[u8],
) -> Result<AttemptResumeRequest, (axum::http::StatusCode, String)> {
    use axum::http::StatusCode;
    if body.is_empty() {
        return Ok(AttemptResumeRequest { reason: None });
    }
    serde_json::from_slice(body).map_err(|_| (StatusCode::BAD_REQUEST, "bad_json".to_string()))
}

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

        let input = match parse_feishu_resume_input(&raw_input) {
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
// Resume response builders
// ---------------------------------------------------------------------------

pub(crate) fn feishu_outcome_json(outcome: &FeishuResumeOutcome) -> Value {
    serde_json::json!({
        "activityId": outcome.activity_id,
        "attemptId": outcome.attempt_id,
        "decision": outcome.decision,
    })
}

pub(crate) fn transient_failure_json(failure: &FeishuTransientFailure) -> Value {
    serde_json::json!({
        "activityId": failure.activity_id,
        "attemptId": failure.attempt_id,
        "provider": failure.provider,
        "idempotencyKey": failure.idempotency_key,
        "errorCode": failure.error_code,
        "errorClass": failure.error_class,
        "errorMessage": failure.error_message,
    })
}

pub(crate) fn resume_started_event_json(event: &WorkflowEventEnvelope) -> Value {
    serde_json::json!({
        "eventId": event.event_id,
        "runId": event.run_id,
        "timestamp": event.timestamp,
        "schemaVersion": event.schema_version,
        "actor": event.actor,
        "type": event.event_type,
        "payload": event.payload,
        "payloadHash": event.payload_hash,
    })
}

/// Convert a unified `ProviderResumeResult` (from the reconciler registry path)
/// to the legacy `ScheduleResumeResult` for backward-compatible API responses.
pub(crate) fn provider_result_to_schedule_result(
    result: workflow_reconcilers::ProviderResumeResult,
) -> ScheduleResumeResult {
    ScheduleResumeResult {
        reconciled: result
            .reconciled
            .into_iter()
            .map(|o| ScheduleResumeOutcome {
                activity_id: o.activity_id,
                attempt_id: o.attempt_id,
                decision: o.decision,
            })
            .collect(),
        fresh_retry: result
            .fresh_retry
            .into_iter()
            .map(|o| ScheduleResumeOutcome {
                activity_id: o.activity_id,
                attempt_id: o.attempt_id,
                decision: o.decision,
            })
            .collect(),
        skipped: result.skipped,
    }
}

/// Convert a unified `ProviderResumeResult` (from the reconciler registry path)
/// to the legacy `FeishuResumeResult` for backward-compatible API responses.
pub(crate) fn provider_result_to_feishu_result(
    result: workflow_reconcilers::ProviderResumeResult,
) -> FeishuResumeResult {
    FeishuResumeResult {
        reconciled: result
            .reconciled
            .into_iter()
            .map(|o| FeishuResumeOutcome {
                activity_id: o.activity_id,
                attempt_id: o.attempt_id,
                decision: o.decision,
            })
            .collect(),
        fresh_retry: result
            .fresh_retry
            .into_iter()
            .map(|o| FeishuResumeOutcome {
                activity_id: o.activity_id,
                attempt_id: o.attempt_id,
                decision: o.decision,
            })
            .collect(),
        transient_failures: result
            .transient_failures
            .into_iter()
            .map(|f| FeishuTransientFailure {
                activity_id: f.activity_id,
                attempt_id: f.attempt_id,
                provider: f.provider,
                idempotency_key: f.idempotency_key,
                error_code: f.error_code,
                error_class: "retryable".to_string(),
                error_message: f.error_message,
            })
            .collect(),
        skipped: result.skipped,
    }
}

pub(crate) fn build_workflow_resume_response(
    run_id: String,
    status: RunStatus,
    already_terminal: bool,
    last_seq: u64,
    resume_started_event: Option<&WorkflowEventEnvelope>,
    event_index: &HashMap<String, WorkflowEventEnvelope>,
    snapshot: &beam_core::RunSnapshotDTO,
    schedule_result: &ScheduleResumeResult,
    feishu_result: &FeishuResumeResult,
    registry_result: &workflow_reconcilers::ReconcilerRegistryCheckResult,
    worker_crashed_outcomes: Vec<Value>,
    wait_recovery_outcomes: Vec<Value>,
    cancel_recovery_outcomes: Vec<Value>,
) -> Value {
    let resume_started_event_id = resume_started_event
        .as_ref()
        .map(|event| event.event_id.clone());
    let resume_started_event_seq = resume_started_event
        .as_ref()
        .map(|event| event_seq_from_id(&event.event_id));
    let reconciled = schedule_result.reconciled.len() + feishu_result.reconciled.len();
    let fresh_retry = schedule_result.fresh_retry.len() + feishu_result.fresh_retry.len();
    let skipped = schedule_result.skipped.len() + feishu_result.skipped.len();
    let transient_failures: Vec<Value> = feishu_result
        .transient_failures
        .iter()
        .map(transient_failure_json)
        .collect();
    let reconcile_outcomes: Vec<Value> = schedule_result
        .reconciled
        .iter()
        .map(|outcome| {
            build_resume_reconcile_outcome(
                event_index,
                resume_started_event_seq,
                "beam-schedule",
                "readOnlyLookup",
                &outcome.activity_id,
                &outcome.attempt_id,
                &outcome.decision,
            )
        })
        .chain(schedule_result.fresh_retry.iter().map(|outcome| {
            build_resume_reconcile_outcome(
                event_index,
                resume_started_event_seq,
                "beam-schedule",
                "readOnlyLookup",
                &outcome.activity_id,
                &outcome.attempt_id,
                &outcome.decision,
            )
        }))
        .chain(feishu_result.reconciled.iter().map(|outcome| {
            build_resume_reconcile_outcome(
                event_index,
                resume_started_event_seq,
                "feishu-im",
                "idempotentSubmit",
                &outcome.activity_id,
                &outcome.attempt_id,
                &outcome.decision,
            )
        }))
        .chain(feishu_result.fresh_retry.iter().map(|outcome| {
            build_resume_reconcile_outcome(
                event_index,
                resume_started_event_seq,
                "feishu-im",
                "idempotentSubmit",
                &outcome.activity_id,
                &outcome.attempt_id,
                &outcome.decision,
            )
        }))
        .collect();
    serde_json::json!({
        "ok": true,
        "runId": run_id,
        "status": status,
        "alreadyTerminal": already_terminal,
        "lastSeq": last_seq,
        "resumeStartedEventId": resume_started_event_id,
        "resumeStartedEvent": resume_started_event.map(resume_started_event_json),
        "snapshot": snapshot,
        "reconcileOutcomes": reconcile_outcomes,
        "workerCrashedOutcomes": worker_crashed_outcomes,
        "waitRecoveryOutcomes": wait_recovery_outcomes,
        "cancelRecoveryOutcomes": cancel_recovery_outcomes,
        "reconciled": reconciled,
        "freshRetry": fresh_retry,
        "transientFailures": transient_failures,
        "skipped": skipped,
        "scheduleReconciled": schedule_result.reconciled.len(),
        "scheduleFreshRetry": schedule_result.fresh_retry.len(),
        "scheduleSkipped": schedule_result.skipped.len(),
        "scheduleOutcomes": schedule_result
            .reconciled
            .iter()
            .chain(schedule_result.fresh_retry.iter())
            .map(|outcome| serde_json::json!({
                "activityId": outcome.activity_id,
                "attemptId": outcome.attempt_id,
                "decision": outcome.decision,
            }))
            .collect::<Vec<_>>(),
        "feishuReconciled": feishu_result.reconciled.len(),
        "feishuFreshRetry": feishu_result.fresh_retry.len(),
        "feishuTransientFailures": transient_failures,
        "feishuSkipped": feishu_result.skipped.len(),
        "feishuOutcomes": feishu_result
            .reconciled
            .iter()
            .chain(feishu_result.fresh_retry.iter())
            .map(feishu_outcome_json)
            .collect::<Vec<_>>(),
        "registryCoveredProviders": &registry_result.covered_providers,
        "registryMissingProviders": &registry_result.missing_providers,
        "registryChecked": true,
    })
}

pub(crate) fn build_resume_reconcile_outcome(
    event_index: &HashMap<String, WorkflowEventEnvelope>,
    resume_started_event_seq: Option<u64>,
    provider: &str,
    capability: &str,
    activity_id: &str,
    attempt_id: &str,
    decision: &str,
) -> Value {
    let reconcile_event = event_index
        .values()
        .filter(|event| {
            event.event_type == "reconcileResult"
                && event.payload.get("activityId").and_then(Value::as_str) == Some(activity_id)
                && event.payload.get("attemptId").and_then(Value::as_str) == Some(attempt_id)
                && event.payload.get("capability").and_then(Value::as_str) == Some(capability)
                && event.payload.get("decision").and_then(Value::as_str) == Some(decision)
        })
        .max_by_key(|event| event_seq_from_id(&event.event_id));
    let recovered = match (resume_started_event_seq, reconcile_event) {
        (Some(start_seq), Some(event)) => event_seq_from_id(&event.event_id) < start_seq,
        _ => false,
    };
    let evidence = reconcile_event
        .as_ref()
        .and_then(|event| event.payload.get("evidence").cloned())
        .unwrap_or(Value::Null);
    let terminal_event = event_index
        .values()
        .filter(|event| {
            matches!(
                event.event_type.as_str(),
                "activitySucceeded" | "activityFailed" | "activityCanceled"
            ) && event.payload.get("activityId").and_then(Value::as_str) == Some(activity_id)
                && event.payload.get("attemptId").and_then(Value::as_str) == Some(attempt_id)
        })
        .max_by_key(|event| event_seq_from_id(&event.event_id))
        .map(|event| workflow_event_json(event));
    let reconcile_event_json = if recovered {
        Value::Null
    } else {
        reconcile_event
            .as_ref()
            .map(|event| workflow_event_json(event))
            .unwrap_or(Value::Null)
    };
    serde_json::json!({
        "activityId": activity_id,
        "attemptId": attempt_id,
        "idempotencyKey": reconcile_event
            .as_ref()
            .and_then(|event| event.payload.get("idempotencyKey").and_then(Value::as_str))
            .unwrap_or_default(),
        "provider": provider,
        "capability": capability,
        "decision": decision,
        "evidence": evidence,
        "terminalEvent": terminal_event.unwrap_or(Value::Null),
        "reconcileEvent": reconcile_event_json,
        "recovered": recovered,
    })
}

pub(crate) fn workflow_event_json(event: &WorkflowEventEnvelope) -> Value {
    serde_json::json!({
        "eventId": event.event_id,
        "runId": event.run_id,
        "timestamp": event.timestamp,
        "schemaVersion": event.schema_version,
        "actor": event.actor,
        "type": event.event_type,
        "payload": event.payload,
        "payloadHash": event.payload_hash,
    })
}

// ---------------------------------------------------------------------------
// Recovery helpers (wait, cancel, worker-crashed)
// ---------------------------------------------------------------------------

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
        "terminalEvent": workflow_event_json(&terminal_event),
    })))
}

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
    let reconcile_event = reconcile_event.map(|event| workflow_event_json(event));
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
        "terminalEvent": workflow_event_json(&terminal_event),
    })))
}

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
        "terminalEvent": workflow_event_json(&terminal_event),
    })))
}
