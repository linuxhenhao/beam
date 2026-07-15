//! Resume response JSON builders.
//!
//! Converts internal resume/reconciliation results into the JSON payload
//! returned by the daemon's workflow-resume HTTP endpoint.

#![allow(dead_code)]

use std::collections::HashMap;

use beam_core::{
    RunStatus, ScheduleResumeOutcome, ScheduleResumeResult, WorkflowEventEnvelope,
    event_seq_from_id,
};
use serde_json::Value;

use crate::{
    FeishuResumeOutcome, FeishuResumeResult, FeishuTransientFailure, workflow_reconcilers,
};

/// Serialize a [`FeishuResumeOutcome`] into the JSON shape expected by the
/// resume response (`activityId` / `attemptId` / `decision`).
pub(crate) fn feishu_outcome_json(outcome: &FeishuResumeOutcome) -> Value {
    serde_json::json!({
        "activityId": outcome.activity_id,
        "attemptId": outcome.attempt_id,
        "decision": outcome.decision,
    })
}

/// Serialize a [`FeishuTransientFailure`] into the JSON shape expected by the
/// resume response.
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

/// Serialize a resume-started [`WorkflowEventEnvelope`] into the JSON shape
/// expected by the resume response.
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

/// Convert a unified [`ProviderResumeResult`] (from the reconciler registry
/// path) to the legacy [`ScheduleResumeResult`] for backward-compatible API
/// responses.
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

/// Convert a unified [`ProviderResumeResult`] (from the reconciler registry
/// path) to the legacy [`FeishuResumeResult`] for backward-compatible API
/// responses.
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

/// Build the top-level JSON response body for a workflow-resume endpoint call.
///
/// Merges schedule reconciler outcomes, Feishu IM outcomes, wait/cancel/
/// worker-crashed recovery outcomes, and registry-check metadata into a single
/// response payload.
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

/// Build a single reconcile-outcome entry for the resume response.
///
/// Looks up the matching reconcile/terminal events from `event_index` and
/// determines whether the outcome was recovered from a prior daemon instance
/// (i.e. the reconcile event's sequence is less than the resume-started
/// event's sequence).
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

/// Serialize a [`WorkflowEventEnvelope`] into a JSON representation.
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
