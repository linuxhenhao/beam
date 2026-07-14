//! Provider-level reconciliation: reconciling all dangling effects for a
//! single provider, and catching unregistered providers after
//! registered-provider reconciliation completes.

use anyhow::Result;
use beam_core::{EventDraft, EventLog, WorkflowActor};

use super::reconcile;
use super::reconcile::{
    ProviderResumeOutcome, ProviderResumeResult, ProviderTransientFailure, ReconcileActivityOutcome,
};
use super::registry::ProviderReconcilerRegistry;
use crate::AppState;

/// Run reconciliation for all dangling effects of a single provider.
///
/// This function:
/// 1. Looks up the reconciler for the given provider.
/// 2. If no reconciler is found, writes manual failures for all matching dangling effects.
/// 3. Otherwise, delegates to `reconcile_activity` for each matching dangling activity.
pub async fn reconcile_provider_dangling_effects(
    registry: &ProviderReconcilerRegistry,
    state: &AppState,
    log: &mut EventLog,
    run_dir: &std::path::Path,
    provider: &str,
    snapshot: &beam_core::RunSnapshotDTO,
) -> Result<ProviderResumeResult> {
    let reconciler = registry.get(provider);

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
        if effect_attempted.provider != provider {
            skipped.push(activity_id.clone());
            continue;
        }

        let Some(reconciler) = reconciler else {
            // No reconciler registered for this provider → manual recovery
            let _ = log.append(EventDraft {
                event_type: "reconcileResult".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": &latest.attempt_id,
                    "idempotencyKey": &effect_attempted.idempotency_key,
                    "capability": "manual",
                    "decision": "manual",
                    "evidence": {
                        "source": "reconcilerRegistry",
                        "returned": "missing",
                        "message": format!("no reconciler registered for provider '{}'", provider),
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
                    "attemptId": &latest.attempt_id,
                    "error": {
                        "errorCode": "UnknownProviderError",
                        "errorClass": "manual",
                        "errorMessage": format!("no reconciler registered for provider '{}'", provider),
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            reconciled.push(ProviderResumeOutcome {
                activity_id: activity_id.clone(),
                attempt_id: latest.attempt_id.clone(),
                decision: "manual".to_string(),
            });
            continue;
        };

        // Load sidecar if needed
        let sidecar = if reconciler.requires_effect_input() {
            beam_core::load_effect_input_sidecar(run_dir, activity_id, &latest.attempt_id).await?
        } else {
            None
        };

        let outcomes = reconcile::reconcile_activity(
            reconciler,
            state,
            log,
            run_dir,
            activity_id,
            &latest.attempt_id,
            &effect_attempted.idempotency_key,
            sidecar.as_ref(),
            Some(&effect_attempted.input_hash),
        )
        .await?;

        for outcome in outcomes {
            match outcome {
                ReconcileActivityOutcome::Reconciled {
                    activity_id,
                    attempt_id,
                    decision,
                } => {
                    reconciled.push(ProviderResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision,
                    });
                }
                ReconcileActivityOutcome::FreshRetry {
                    activity_id,
                    attempt_id,
                } => {
                    fresh_retry.push(ProviderResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision: "freshRetry".to_string(),
                    });
                }
                ReconcileActivityOutcome::TransientFailure {
                    activity_id,
                    attempt_id,
                    provider: p,
                    idempotency_key,
                    error_code,
                    error_message,
                } => {
                    transient_failures.push(ProviderTransientFailure {
                        activity_id,
                        attempt_id,
                        provider: p,
                        idempotency_key,
                        error_code,
                        error_message,
                    });
                }
                ReconcileActivityOutcome::ManualRecovery {
                    activity_id,
                    attempt_id,
                    reason: _,
                } => {
                    reconciled.push(ProviderResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision: "manual".to_string(),
                    });
                }
                ReconcileActivityOutcome::Skipped { .. } => {
                    // Already counted in skipped
                }
            }
        }
    }

    Ok(ProviderResumeResult {
        reconciled,
        fresh_retry,
        transient_failures,
        skipped,
    })
}

/// Scan all dangling `effectAttempted` activities and write `manual` recovery
/// events for any provider that has **no reconciler registered**.
///
/// Returns the list of providers for which a reconciler **was found** (so that
/// caller can continue with provider-specific recovery for those).
pub fn handle_missing_provider_dangling_effects(
    registry: &ProviderReconcilerRegistry,
    log: &mut EventLog,
    snapshot: &beam_core::RunSnapshotDTO,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut covered_providers = Vec::new();
    let mut missing_providers = Vec::new();

    for activity_id in &snapshot.dangling.effect_attempted {
        let Some(activity) = snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
        else {
            continue;
        };
        let Some(latest) = activity.attempts.last() else {
            continue;
        };
        let Some(effect_attempted) = latest.effect_attempted.as_ref() else {
            continue;
        };
        let provider = &effect_attempted.provider;

        if registry.get(provider).is_some() {
            if !covered_providers.contains(provider) {
                covered_providers.push(provider.clone());
            }
            continue;
        }

        // No reconciler → manual recovery
        if !missing_providers.contains(provider) {
            missing_providers.push(provider.clone());
        }

        let _ = log.append(EventDraft {
            event_type: "reconcileResult".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": &latest.attempt_id,
                "idempotencyKey": &effect_attempted.idempotency_key,
                "capability": "manual",
                "decision": "manual",
                "evidence": {
                    "source": "reconcilerRegistry",
                    "returned": "missing",
                    "message": format!("no reconciler registered for provider '{}'", provider),
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
                "attemptId": &latest.attempt_id,
                "error": {
                    "errorCode": "UnknownProviderError",
                    "errorClass": "manual",
                    "errorMessage": format!("no reconciler registered for provider '{}'", provider),
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?;
    }

    Ok((covered_providers, missing_providers))
}
