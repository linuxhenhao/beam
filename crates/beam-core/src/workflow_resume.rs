use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::workflow_snapshot::ActivityStatus;
use crate::{
    AttemptState, BeamPaths, EventDraft, EventLog, WorkflowActor, WorkflowOutputRef, get_task,
    read_run_snapshot,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleResumeOutcome {
    pub activity_id: String,
    pub attempt_id: String,
    pub decision: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleResumeResult {
    pub reconciled: Vec<ScheduleResumeOutcome>,
    pub fresh_retry: Vec<ScheduleResumeOutcome>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PriorReconcileRecoveryOutcome {
    Recovered {
        activity_id: String,
        attempt_id: String,
        decision: String,
    },
    FreshRetry {
        activity_id: String,
        attempt_id: String,
    },
}

pub async fn recover_prior_reconcile_result(
    log: &mut EventLog,
    activity_id: &str,
    attempt: &AttemptState,
) -> Result<Option<PriorReconcileRecoveryOutcome>> {
    let Some(rr) = attempt.latest_reconcile_result.as_ref() else {
        return Ok(None);
    };
    if matches!(
        attempt.status,
        ActivityStatus::Succeeded
            | ActivityStatus::Failed
            | ActivityStatus::TimedOut
            | ActivityStatus::Cancelled
    ) {
        return Ok(None);
    }

    match rr.decision.as_str() {
        "completedByIdempotentSubmit" => {
            let external_refs = rr
                .evidence
                .get("externalRefs")
                .cloned()
                .and_then(|value| value.as_object().cloned().map(serde_json::Value::Object));
            let Some(external_refs) = external_refs else {
                let _ = log.append(EventDraft {
                    event_type: "activityFailed".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt.attempt_id,
                        "error": {
                            "errorCode": "CorruptLog",
                            "errorClass": "manual",
                            "errorMessage": "Prior reconcileResult{decision=completedByIdempotentSubmit} is missing evidence.externalRefs (or it is not an object) — refusing to fabricate an activitySucceeded from empty refs.",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                return Ok(Some(PriorReconcileRecoveryOutcome::Recovered {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt.attempt_id.clone(),
                    decision: "manual".to_string(),
                }));
            };
            let output_ref = write_json_blob(&mut *log, serde_json::json!(&external_refs))?;
            let _ = log.append(EventDraft {
                event_type: "activitySucceeded".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt.attempt_id,
                    "outputRef": output_ref,
                    "externalRefs": external_refs,
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            Ok(Some(PriorReconcileRecoveryOutcome::Recovered {
                activity_id: activity_id.to_string(),
                attempt_id: attempt.attempt_id.clone(),
                decision: "completedByIdempotentSubmit".to_string(),
            }))
        }
        "manual" => {
            let error_code = rr
                .evidence
                .get("errorCode")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("UnknownProviderError");
            let _ = log.append(EventDraft {
                event_type: "activityFailed".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt.attempt_id,
                    "error": {
                        "errorCode": error_code,
                        "errorClass": "manual",
                        "errorMessage": format!(
                            "Recovered from prior crashed reconcile cycle (decision=manual, errorCode={}).",
                            error_code
                        ),
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            Ok(Some(PriorReconcileRecoveryOutcome::Recovered {
                activity_id: activity_id.to_string(),
                attempt_id: attempt.attempt_id.clone(),
                decision: "manual".to_string(),
            }))
        }
        "freshRetry" => Ok(Some(PriorReconcileRecoveryOutcome::FreshRetry {
            activity_id: activity_id.to_string(),
            attempt_id: attempt.attempt_id.clone(),
        })),
        "replayed" => {
            let _ = log.append(EventDraft {
                event_type: "activityFailed".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt.attempt_id,
                    "error": {
                        "errorCode": "CorruptLog",
                        "errorClass": "manual",
                        "errorMessage": "Prior reconcileResult decision=replayed but no terminal event present — log inconsistency.",
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            Ok(Some(PriorReconcileRecoveryOutcome::Recovered {
                activity_id: activity_id.to_string(),
                attempt_id: attempt.attempt_id.clone(),
                decision: "manual".to_string(),
            }))
        }
        _ => Ok(None),
    }
}

pub async fn resume_schedule_dangling_effects(
    log: &mut EventLog,
    paths: &BeamPaths,
    daemon_id: &str,
    reason: Option<&str>,
) -> Result<ScheduleResumeResult> {
    let last_seen_event_id = log
        .read_all()?
        .last()
        .map(|event| event.event_id.clone())
        .unwrap_or_default();
    let snapshot = read_run_snapshot(&log.run_dir)
        .await?
        .context("workflow resume requires an existing run snapshot")?;
    let _ = log.append(EventDraft {
        event_type: "resumeStarted".to_string(),
        actor: WorkflowActor::System,
        payload: serde_json::json!({
            "daemonId": daemon_id,
            "lastSeenEventId": last_seen_event_id,
            "reason": reason,
        }),
        timestamp: None,
        payload_hash: None,
    })?;

    let mut reconciled = Vec::new();
    let mut fresh_retry = Vec::new();
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
        if effect_attempted.provider != "beam-schedule" {
            skipped.push(activity_id.clone());
            continue;
        }

        if let Some(recovery) =
            recover_prior_reconcile_result(&mut *log, activity_id, latest).await?
        {
            match recovery {
                PriorReconcileRecoveryOutcome::Recovered {
                    activity_id,
                    attempt_id,
                    decision,
                } => {
                    reconciled.push(ScheduleResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision,
                    });
                }
                PriorReconcileRecoveryOutcome::FreshRetry {
                    activity_id,
                    attempt_id,
                } => {
                    fresh_retry.push(ScheduleResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision: "freshRetry".to_string(),
                    });
                }
            }
            continue;
        }

        match get_task(paths, &effect_attempted.idempotency_key)? {
            Some(task) => {
                let external_refs = serde_json::json!({ "taskId": task.id });
                let output_ref = write_json_blob(&mut *log, external_refs.clone())?;
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "idempotencyKey": effect_attempted.idempotency_key,
                        "capability": "readOnlyLookup",
                        "decision": "completedByIdempotentSubmit",
                        "evidence": {
                            "source": "getTask",
                            "externalRefs": external_refs,
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
                        "attemptId": latest.attempt_id,
                        "outputRef": output_ref,
                        "externalRefs": { "taskId": task.id },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                reconciled.push(ScheduleResumeOutcome {
                    activity_id: activity_id.clone(),
                    attempt_id: latest.attempt_id.clone(),
                    decision: "completedByIdempotentSubmit".to_string(),
                });
            }
            None => {
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "idempotencyKey": effect_attempted.idempotency_key,
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
                fresh_retry.push(ScheduleResumeOutcome {
                    activity_id: activity_id.clone(),
                    attempt_id: latest.attempt_id.clone(),
                    decision: "freshRetry".to_string(),
                });
            }
        }
    }

    Ok(ScheduleResumeResult {
        reconciled,
        fresh_retry,
        skipped,
    })
}

fn write_json_blob(log: &mut EventLog, value: Value) -> Result<WorkflowOutputRef> {
    let bytes = serde_json::to_vec(&value)?;
    let hash = sha256_hex(&bytes);
    let path = PathBuf::from(&log.blob_dir).join(&hash);
    fs::write(&path, &bytes)?;
    Ok(WorkflowOutputRef {
        output_hash: format!("sha256:{hash}"),
        output_path: path.display().to_string(),
        output_bytes: bytes.len(),
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BootstrapWorkflowRunInput, CreateTaskInput, ParsedSchedule, ParsedScheduleKind,
        RunChatBinding, bootstrap_workflow_run, create_task,
    };
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-workflow-resume-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[tokio::test]
    async fn schedule_resume_reconciles_found_task() {
        let paths = temp_paths("found");
        let _ = std::fs::remove_dir_all(paths.root());
        let params: BTreeMap<String, Value> = BTreeMap::from([
            (String::from("name"), Value::String("beam".to_string())),
        ]);
        let run_id = "run-1";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo daily 9am","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"Schedule demo","workingDir":"/tmp/beam-schedule-demo","chatId":"oc_workflow_demo","scope":"thread"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "wf_key",
                        "inputHash": "sha256:1",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "beam-schedule",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            create_task(
                &paths,
                CreateTaskInput {
                    id: Some("wf_key".to_string()),
                    name: "schedule-demo daily 9am".to_string(),
                    schedule: "0 9 * * *".to_string(),
                    parsed: ParsedSchedule {
                        kind: ParsedScheduleKind::Cron,
                        run_at: None,
                        minutes: None,
                        expr: Some("0 9 * * *".to_string()),
                        display: "0 9 * * *".to_string(),
                    },
                    prompt: "Schedule demo".to_string(),
                    working_dir: "/tmp/beam-schedule-demo".to_string(),
                    chat_id: "oc_workflow_demo".to_string(),
                    root_message_id: None,
                    scope: Some("thread".to_string()),
                    chat_type: None,
                    lark_app_id: None,
                    creator_chat_id: None,
                    creator_root_message_id: None,
                    creator_lark_app_id: None,
                    next_run_at: None,
                    repeat: None,
                    deliver: None,
                },
            )
            .unwrap();
        }
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let result = resume_schedule_dangling_effects(&mut log, &paths, "daemon-1", None)
            .await
            .unwrap();
        assert_eq!(result.reconciled.len(), 1);
        assert_eq!(result.reconciled[0].decision, "completedByIdempotentSubmit");
        let events = log.read_all().unwrap();
        assert!(events.iter().any(|e| e.event_type == "reconcileResult"));
        assert!(events.iter().any(|e| e.event_type == "activitySucceeded"));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn recover_prior_reconcile_result_materializes_terminal_without_recalling_provider() {
        let paths = temp_paths("prior-reconcile");
        let _ = std::fs::remove_dir_all(paths.root());
        let run_id = "run-recover-1";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-im","input":{"larkAppId":"app-1","chatId":"chat-1","content":"hello"}}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": paths.workflow_run_dir(run_id).join("blobs").join("dummy").display().to_string(),
                            "outputBytes": 2,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "wf_key",
                        "inputHash": "sha256:1",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "feishu-im",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "wf_key",
                        "capability": "idempotentSubmit",
                        "decision": "completedByIdempotentSubmit",
                        "evidence": {
                            "externalRefs": { "messageId": "msg-1" }
                        },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();

            let snapshot = read_run_snapshot(&log.run_dir).await.unwrap().unwrap();
            let attempt = &snapshot.activities[0].attempts[0];
            let outcome = recover_prior_reconcile_result(&mut log, "act-1", attempt)
                .await
                .unwrap()
                .expect("recovery");
            match outcome {
                PriorReconcileRecoveryOutcome::Recovered { decision, .. } => {
                    assert_eq!(decision, "completedByIdempotentSubmit");
                }
                other => panic!("unexpected recovery outcome: {:?}", other),
            }
        }
        let events = EventLog::new(run_id, paths.workflow_runs_dir())
            .unwrap()
            .read_all()
            .unwrap();
        assert!(events.iter().any(|e| e.event_type == "activitySucceeded"));
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
