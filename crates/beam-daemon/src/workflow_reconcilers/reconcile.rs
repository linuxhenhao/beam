//! Core reconciliation logic: `reconcile_activity` and related types.
//!
//! This module holds the decision tree for reconciling a single dangling
//! effect, plus the result/output types shared across the reconciler
//! subsystem.

use anyhow::Result;
use beam_core::{EventDraft, EventLog, WorkflowActor};
use serde_json::Value;

use super::registry::ProviderReconciler;
use crate::AppState;

// ---------------------------------------------------------------------------
// Outcome enum
// ---------------------------------------------------------------------------

/// Outcome of reconciling a single dangling effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileActivityOutcome {
    /// Effect was successfully reconciled (terminal event written).
    Reconciled {
        activity_id: String,
        attempt_id: String,
        decision: String,
    },
    /// Effect should be retried from scratch.
    FreshRetry {
        activity_id: String,
        attempt_id: String,
    },
    /// Provider error is transient – effect remains dangling.
    TransientFailure {
        activity_id: String,
        attempt_id: String,
        provider: String,
        idempotency_key: String,
        error_code: String,
        error_message: String,
    },
    /// Provider not registered – manual failure written.
    ManualRecovery {
        activity_id: String,
        attempt_id: String,
        reason: String,
    },
    /// This activity was skipped (not applicable to this reconciler).
    #[allow(dead_code)]
    Skipped { activity_id: String, reason: String },
}

// ---------------------------------------------------------------------------
// Result types (mirror the existing ScheduleResumeResult / FeishuResumeResult shapes)
// ---------------------------------------------------------------------------

/// Summary of the reconciler registry check performed during resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerRegistryCheckResult {
    /// Providers for which a reconciler exists (and provider-specific recovery was used).
    pub covered_providers: Vec<String>,
    /// Providers for which no reconciler exists (manual recovery was written).
    pub missing_providers: Vec<String>,
}

/// Outcome for a single activity within a provider's resume result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResumeOutcome {
    pub activity_id: String,
    pub attempt_id: String,
    pub decision: String,
}

/// Transient failure that should be retried on a future resume cycle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTransientFailure {
    pub activity_id: String,
    pub attempt_id: String,
    pub provider: String,
    pub idempotency_key: String,
    pub error_code: String,
    pub error_message: String,
}

/// Full result of reconciling dangling effects for a single provider.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderResumeResult {
    pub reconciled: Vec<ProviderResumeOutcome>,
    pub fresh_retry: Vec<ProviderResumeOutcome>,
    pub transient_failures: Vec<ProviderTransientFailure>,
    pub skipped: Vec<String>,
}

// ---------------------------------------------------------------------------
// Reconciliation helpers
// ---------------------------------------------------------------------------

/// Run reconciliation for a single dangling activity using the given reconciler.
///
/// This function:
/// 1. Checks for prior `reconcileResult` recovery (covers crash-recovery of a previous reconcile cycle).
/// 2. If the reconciler supports `readOnlyLookup`, tries that first.
/// 3. If the reconciler supports `idempotentSubmit`, tries that next.
/// 4. On success, writes `reconcileResult` + `activitySucceeded`.
/// 5. On failure, writes `reconcileResult` + `activityFailed` (manual recovery) or returns transient failure.
///
/// If `expected_input_hash` is provided and the reconciler requires effect input,
/// the canonical input hash is validated against the expected hash from the
/// original `effectAttempted` event before calling `idempotentSubmit`.
/// A mismatch results in `manual` recovery without contacting the provider.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_activity(
    reconciler: &dyn ProviderReconciler,
    state: &AppState,
    log: &mut EventLog,
    run_dir: &std::path::Path,
    activity_id: &str,
    attempt_id: &str,
    idempotency_key: &str,
    sidecar_input: Option<&Value>,
    expected_input_hash: Option<&str>,
) -> Result<Vec<ReconcileActivityOutcome>> {
    let mut outcomes = Vec::new();

    // --- Step 1: Check prior reconcileResult recovery ---
    let snapshot = beam_core::read_run_snapshot(run_dir)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing run snapshot for activity {}", activity_id))?;
    let activity = snapshot
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id);
    if let Some(latest) = activity.and_then(|a| a.attempts.last()) {
        if let Some(recovery) =
            beam_core::recover_prior_reconcile_result(log, activity_id, latest).await?
        {
            match recovery {
                beam_core::PriorReconcileRecoveryOutcome::Recovered {
                    activity_id,
                    attempt_id,
                    decision,
                } => {
                    outcomes.push(ReconcileActivityOutcome::Reconciled {
                        activity_id,
                        attempt_id,
                        decision,
                    });
                    return Ok(outcomes);
                }
                beam_core::PriorReconcileRecoveryOutcome::FreshRetry {
                    activity_id,
                    attempt_id,
                } => {
                    outcomes.push(ReconcileActivityOutcome::FreshRetry {
                        activity_id,
                        attempt_id,
                    });
                    return Ok(outcomes);
                }
            }
        }
    }

    // --- Step 2: Try readOnlyLookup (only if the reconciler declares support) ---
    if reconciler.supports_read_only_lookup() {
        match reconciler
            .read_only_lookup(state, &state.paths, idempotency_key)
            .await
        {
            Ok(Some(evidence)) => {
                let external_refs = evidence
                    .get("externalRefs")
                    .cloned()
                    .and_then(|v| v.as_object().cloned().map(Value::Object))
                    .unwrap_or_else(|| evidence.clone());
                let output_ref = crate::write_json_blob(log, external_refs.clone())?;
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "readOnlyLookup",
                        "decision": "completedByIdempotentSubmit",
                        "evidence": evidence,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                let _ = log.append(EventDraft {
                    event_type: "activitySucceeded".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "outputRef": output_ref,
                        "externalRefs": { "taskId": external_refs.get("taskId") },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::Reconciled {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    decision: "completedByIdempotentSubmit".to_string(),
                });
                return Ok(outcomes);
            }
            Ok(None) => {
                // readOnlyLookup found nothing.
                // If this reconciler does NOT support idempotentSubmit, issue
                // freshRetry so the caller can recreate the effect from scratch.
                if !reconciler.supports_idempotent_submit() {
                    let _ = log.append(EventDraft {
                        event_type: "reconcileResult".to_string(),
                        actor: WorkflowActor::System,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "idempotencyKey": idempotency_key,
                            "capability": "readOnlyLookup",
                            "decision": "freshRetry",
                            "evidence": {
                                "source": "getTask",
                                "returned": "undefined",
                            },
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    outcomes.push(ReconcileActivityOutcome::FreshRetry {
                        activity_id: activity_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                    });
                    return Ok(outcomes);
                }
                // Otherwise fall through to idempotentSubmit
            }
            Err(err) => {
                // readOnlyLookup failed – treat as transient unless we have idempotentSubmit fallback
                if reconciler.is_retryable_error(&err) {
                    outcomes.push(ReconcileActivityOutcome::TransientFailure {
                        activity_id: activity_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                        provider: reconciler.provider_name().to_string(),
                        idempotency_key: idempotency_key.to_string(),
                        error_code: "ReconcilerReadOnlyLookupError".to_string(),
                        error_message: format!("{:#}", err),
                    });
                    return Ok(outcomes);
                }
                // Non-retryable read-only error: fall through to try idempotentSubmit
            }
        }
    }

    // --- Step 3: Try idempotentSubmit ---
    let canonical_input = if let Some(raw) = sidecar_input {
        match reconciler.canonical_input(raw) {
            Ok(ci) => {
                // --- Validate input hash against the original effectAttempted.inputHash ---
                if let Some(expected) = expected_input_hash {
                    if !expected.is_empty() {
                        let actual_bytes = serde_json::to_vec(&ci)?;
                        let actual_hash = crate::sha256_hex(&actual_bytes);
                        if actual_hash != expected {
                            let _ = log.append(EventDraft {
                                event_type: "reconcileResult".to_string(),
                                actor: WorkflowActor::System,
                                payload: serde_json::json!({
                                    "activityId": activity_id,
                                    "attemptId": attempt_id,
                                    "idempotencyKey": idempotency_key,
                                    "capability": "idempotentSubmit",
                                    "decision": "manual",
                                    "evidence": {
                                        "source": "effectInputSidecar",
                                        "returned": "hashMismatch",
                                        "expectedHash": expected,
                                        "actualHash": actual_hash,
                                    },
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
                                        "errorCode": "EffectInputHashMismatch",
                                        "errorClass": "manual",
                                        "errorMessage": format!(
                                            "effect input hash mismatch: expected {expected}, got {actual_hash}"
                                        ),
                                    }
                                }),
                                timestamp: None,
                                payload_hash: None,
                            })?;
                            outcomes.push(ReconcileActivityOutcome::ManualRecovery {
                                activity_id: activity_id.to_string(),
                                attempt_id: attempt_id.to_string(),
                                reason: format!(
                                    "effect input hash mismatch: expected {expected}, got {actual_hash}"
                                ),
                            });
                            return Ok(outcomes);
                        }
                    }
                }
                Some(ci)
            }
            Err(err) => {
                // Invalid input – manual failure
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "manual",
                        "evidence": {
                            "source": "effectInputSidecar",
                            "returned": "invalid",
                        },
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
                            "errorCode": "InvalidEffectInput",
                            "errorClass": "manual",
                            "errorMessage": format!("{:#}", err),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::ManualRecovery {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    reason: format!("invalid effect input: {:#}", err),
                });
                return Ok(outcomes);
            }
        }
    } else if reconciler.requires_effect_input() {
        // Sidecar missing but required – manual failure
        let _ = log.append(EventDraft {
            event_type: "reconcileResult".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": attempt_id,
                "idempotencyKey": idempotency_key,
                "capability": "idempotentSubmit",
                "decision": "manual",
                "evidence": {
                    "source": "effectInputSidecar",
                    "returned": "missing",
                },
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
                    "errorCode": "MissingEffectInputSidecar",
                    "errorClass": "manual",
                    "errorMessage": "effect input sidecar is missing".to_string(),
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        outcomes.push(ReconcileActivityOutcome::ManualRecovery {
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            reason: "missing effect input sidecar".to_string(),
        });
        return Ok(outcomes);
    } else {
        None
    };

    match reconciler
        .idempotent_submit(state, canonical_input.as_ref().unwrap_or(&Value::Null))
        .await
    {
        Ok(evidence) => {
            let external_refs = evidence
                .get("externalRefs")
                .cloned()
                .unwrap_or_else(|| evidence.clone());
            let output_ref = crate::write_json_blob(log, external_refs.clone())?;
            let _ = log.append(EventDraft {
                event_type: "reconcileResult".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "idempotencyKey": idempotency_key,
                    "capability": "idempotentSubmit",
                    "decision": "completedByIdempotentSubmit",
                    "evidence": evidence,
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            let _ = log.append(EventDraft {
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
            })?;
            outcomes.push(ReconcileActivityOutcome::Reconciled {
                activity_id: activity_id.to_string(),
                attempt_id: attempt_id.to_string(),
                decision: "completedByIdempotentSubmit".to_string(),
            });
        }
        Err(err) => {
            if crate::is_lark_message_withdrawn_error(&err) {
                // Lark message was withdrawn – manual failure (can't resend)
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "manual",
                        "evidence": {
                            "source": "lark",
                            "submitKind": "sendOrReply",
                        },
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
                            "errorCode": "MessageWithdrawnError",
                            "errorClass": "manual",
                            "errorMessage": format!("{:#}", err),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::Reconciled {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    decision: "manual".to_string(),
                });
            } else if reconciler.is_retryable_error(&err) {
                outcomes.push(ReconcileActivityOutcome::TransientFailure {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    provider: reconciler.provider_name().to_string(),
                    idempotency_key: idempotency_key.to_string(),
                    error_code: "FeishuSubmitRetryable".to_string(),
                    error_message: format!("{:#}", err),
                });
            } else {
                // Non-retryable error – manual failure
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "manual",
                        "evidence": {
                            "source": "lark",
                            "submitKind": "sendOrReply",
                        },
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
                            "errorCode": "FeishuSubmitFailed",
                            "errorClass": "manual",
                            "errorMessage": format!("{:#}", err),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::Reconciled {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    decision: "manual".to_string(),
                });
            }
        }
    }

    Ok(outcomes)
}
