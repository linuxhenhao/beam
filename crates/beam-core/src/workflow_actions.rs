use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{EventDraft, EventLog, WorkflowActor, WorkflowOutputRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitKind {
    HumanGate,
    Time,
    Condition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitResolution {
    Approved,
    Rejected,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOnTimeout {
    Fail,
    Success,
}

#[derive(Debug, Clone)]
pub struct CreateWaitInput {
    pub activity_id: String,
    pub attempt_id: String,
    pub node_id: String,
    pub wait_kind: WaitKind,
    pub deadline_at: Option<u64>,
    pub prompt: Option<String>,
    pub prompt_ref: Option<WorkflowOutputRef>,
    pub prompt_preview: Option<String>,
    pub approvers: Option<Vec<String>>,
    pub on_timeout: Option<WaitOnTimeout>,
}

#[derive(Debug, Clone)]
pub struct ResolveWaitInput {
    pub activity_id: String,
    pub attempt_id: String,
    pub resolution: WaitResolution,
    pub by: String,
    pub comment: Option<String>,
    pub output: Option<Value>,
    pub is_decision_node: bool,
}

#[derive(Debug, Clone)]
pub struct ExpireWaitInput {
    pub activity_id: String,
    pub attempt_id: String,
    pub deadline_at: u64,
    pub exceeded_at_ms: u64,
    pub on_timeout: Option<WaitOnTimeout>,
}

#[derive(Debug, Clone)]
pub struct RequestCancelInput {
    pub target: Value,
    pub reason: String,
    pub by: String,
}

#[derive(Debug, Clone)]
pub struct DeliverCancelInput {
    pub target: Value,
    pub activity_id: String,
}

#[derive(Debug, Clone)]
pub struct CompleteActivityCancelInput {
    pub activity_id: String,
    pub attempt_id: String,
    pub cancel_origin_event_id: String,
}

#[derive(Debug, Clone)]
pub struct CompleteNodeCancelInput {
    pub node_id: String,
    pub cancel_origin_event_id: String,
}

#[derive(Debug, Clone)]
pub struct CompleteRunCancelInput {
    pub cancel_origin_event_id: String,
}

pub async fn create_wait(log: &mut EventLog, input: CreateWaitInput) -> Result<Value> {
    Ok(serde_json::to_value(log.append(EventDraft {
        event_type: "waitCreated".to_string(),
        actor: WorkflowActor::Scheduler,
        payload: serde_json::json!({
            "activityId": input.activity_id,
            "attemptId": input.attempt_id,
            "nodeId": input.node_id,
            "waitKind": wait_kind_str(input.wait_kind),
            "deadlineAt": input.deadline_at,
            "prompt": input.prompt,
            "promptRef": input.prompt_ref,
            "promptPreview": input.prompt_preview,
            "approvers": input.approvers,
            "onTimeout": input.on_timeout.map(wait_timeout_str),
        }),
        timestamp: None,
        payload_hash: None,
    })?)?)
}

pub async fn resolve_wait(log: &mut EventLog, input: ResolveWaitInput) -> Result<Value> {
    let mut external_refs = serde_json::Map::from_iter([
        (
            "resolution".to_string(),
            Value::String(wait_resolution_str(input.resolution).to_string()),
        ),
        ("by".to_string(), Value::String(input.by.clone())),
        (
            "comment".to_string(),
            Value::String(input.comment.clone().unwrap_or_default()),
        ),
    ]);
    if let Some(extra) = input.output.clone() {
        match extra {
            Value::Object(map) => {
                for (key, value) in map {
                    external_refs.insert(key, value);
                }
            }
            value => {
                external_refs.insert("output".to_string(), value);
            }
        }
    }
    let output_ref = write_json_blob(log, Value::Object(external_refs.clone()))?;
    let resolution_event = log.append(EventDraft {
        event_type: "waitResolved".to_string(),
        actor: match input.resolution {
            WaitResolution::External => WorkflowActor::System,
            _ => WorkflowActor::Human,
        },
        payload: serde_json::json!({
            "activityId": input.activity_id,
            "resolution": wait_resolution_str(input.resolution),
            "by": input.by,
            "comment": input.comment,
        }),
        timestamp: None,
        payload_hash: None,
    })?;

    let terminal = match input.resolution {
        WaitResolution::Rejected if !input.is_decision_node => log.append(EventDraft {
            event_type: "activityFailed".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "activityId": input.activity_id,
                "attemptId": input.attempt_id,
                "error": {
                    "errorCode": "InputValidationFailed",
                    "errorClass": "userFault",
                    "errorMessage": format!(
                        "Wait resolved with rejected by {}{}",
                        input.by,
                        input.comment.as_ref().map(|c| format!(": {}", c)).unwrap_or_default()
                    ),
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?,
        _ => log.append(EventDraft {
            event_type: "activitySucceeded".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "activityId": input.activity_id,
                "attemptId": input.attempt_id,
                "outputRef": output_ref,
                "externalRefs": external_refs,
            }),
            timestamp: None,
            payload_hash: None,
        })?,
    };

    Ok(serde_json::json!({
        "resolutionEventId": resolution_event.event_id,
        "terminalEventId": terminal.event_id,
    }))
}

pub async fn expire_wait(log: &mut EventLog, input: ExpireWaitInput) -> Result<Value> {
    let _deadline_event = log.append(EventDraft {
        event_type: "waitDeadlineExceeded".to_string(),
        actor: WorkflowActor::Scheduler,
        payload: serde_json::json!({
            "activityId": input.activity_id,
            "deadlineAt": input.deadline_at,
            "exceededAtMs": input.exceeded_at_ms,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    let terminal = match input.on_timeout.unwrap_or(WaitOnTimeout::Fail) {
        WaitOnTimeout::Fail => log.append(EventDraft {
            event_type: "activityFailed".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "activityId": input.activity_id,
                "attemptId": input.attempt_id,
                "error": {
                    "errorCode": "WaitDeadlineExceeded",
                    "errorClass": "userFault",
                    "errorMessage": "Wait deadline exceeded"
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?,
        WaitOnTimeout::Success => log.append(EventDraft {
            event_type: "activitySucceeded".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "activityId": input.activity_id,
                "attemptId": input.attempt_id,
                "externalRefs": {
                    "defaultedToTimeout": true,
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?,
    };
    Ok(serde_json::json!({
        "deadlineEventId": "todo",
        "terminalEventId": terminal.event_id,
    }))
}

pub async fn request_cancel(
    log: &mut EventLog,
    input: RequestCancelInput,
    actor: WorkflowActor,
) -> Result<Value> {
    let event = log.append(EventDraft {
        event_type: "cancelRequested".to_string(),
        actor,
        payload: serde_json::json!({
            "target": input.target,
            "reason": input.reason,
            "by": input.by,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(serde_json::json!({ "eventId": event.event_id }))
}

pub async fn deliver_cancel(
    log: &mut EventLog,
    input: DeliverCancelInput,
    actor: WorkflowActor,
) -> Result<Value> {
    let event = log.append(EventDraft {
        event_type: "cancelDelivered".to_string(),
        actor,
        payload: serde_json::json!({
            "target": input.target,
            "activityId": input.activity_id,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(serde_json::json!({ "eventId": event.event_id }))
}

pub async fn complete_activity_cancel(
    log: &mut EventLog,
    input: CompleteActivityCancelInput,
    actor: WorkflowActor,
) -> Result<Value> {
    let event = log.append(EventDraft {
        event_type: "activityCanceled".to_string(),
        actor,
        payload: serde_json::json!({
            "activityId": input.activity_id,
            "attemptId": input.attempt_id,
            "cancelOriginEventId": input.cancel_origin_event_id,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(serde_json::json!({ "eventId": event.event_id }))
}

pub async fn complete_node_cancel(
    log: &mut EventLog,
    input: CompleteNodeCancelInput,
    actor: WorkflowActor,
) -> Result<Value> {
    let event = log.append(EventDraft {
        event_type: "nodeCanceled".to_string(),
        actor,
        payload: serde_json::json!({
            "nodeId": input.node_id,
            "cancelOriginEventId": input.cancel_origin_event_id,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(serde_json::json!({ "eventId": event.event_id }))
}

pub async fn complete_run_cancel(
    log: &mut EventLog,
    input: CompleteRunCancelInput,
    actor: WorkflowActor,
) -> Result<Value> {
    let event = log.append(EventDraft {
        event_type: "runCanceled".to_string(),
        actor,
        payload: serde_json::json!({
            "cancelOriginEventId": input.cancel_origin_event_id,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    Ok(serde_json::json!({ "eventId": event.event_id }))
}

fn wait_kind_str(kind: WaitKind) -> &'static str {
    match kind {
        WaitKind::HumanGate => "human-gate",
        WaitKind::Time => "time",
        WaitKind::Condition => "condition",
    }
}

fn wait_resolution_str(resolution: WaitResolution) -> &'static str {
    match resolution {
        WaitResolution::Approved => "approved",
        WaitResolution::Rejected => "rejected",
        WaitResolution::External => "external",
    }
}

fn wait_timeout_str(timeout: WaitOnTimeout) -> &'static str {
    match timeout {
        WaitOnTimeout::Fail => "fail",
        WaitOnTimeout::Success => "success",
    }
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
    use crate::BeamPaths;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-workflow-actions-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )))
    }

    #[tokio::test]
    async fn cancel_and_wait_helpers_write_expected_events() {
        let paths = temp_paths("write");
        let mut log = EventLog::new("run-1", paths.workflow_runs_dir()).expect("log");
        let wait = create_wait(
            &mut log,
            CreateWaitInput {
                activity_id: "run-1::gate::node-a".to_string(),
                attempt_id: "run-1::gate::node-a::att-1".to_string(),
                node_id: "node-a".to_string(),
                wait_kind: WaitKind::HumanGate,
                deadline_at: None,
                prompt: Some("approve?".to_string()),
                prompt_ref: None,
                prompt_preview: None,
                approvers: None,
                on_timeout: Some(WaitOnTimeout::Fail),
            },
        )
        .await
        .expect("create wait");
        assert_eq!(wait["event_id"], "run-1-1");

        let cancel = request_cancel(
            &mut log,
            RequestCancelInput {
                target: serde_json::json!({ "kind": "run", "runId": "run-1" }),
                reason: "stop".to_string(),
                by: "tester".to_string(),
            },
            WorkflowActor::Human,
        )
        .await
        .expect("request cancel");
        assert_eq!(cancel["eventId"], "run-1-2");

        let events = log.read_all().expect("events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "waitCreated");
        assert_eq!(events[1].event_type, "cancelRequested");

        let resolved = resolve_wait(
            &mut log,
            ResolveWaitInput {
                activity_id: "run-1::gate::node-a".to_string(),
                attempt_id: "run-1::gate::node-a::att-1".to_string(),
                resolution: WaitResolution::Approved,
                by: "tester".to_string(),
                comment: Some("ok".to_string()),
                output: None,
                is_decision_node: false,
            },
        )
        .await
        .expect("resolve");
        assert_eq!(resolved["resolutionEventId"], "run-1-3");
    }

    #[tokio::test]
    async fn decision_node_reject_resolves_to_success() {
        let paths = temp_paths("decision");
        let mut log = EventLog::new("run-2", paths.workflow_runs_dir()).expect("log");
        let _ = create_wait(
            &mut log,
            CreateWaitInput {
                activity_id: "run-2::work::node-decision::att-1".to_string(),
                attempt_id: "run-2::work::node-decision::att-1".to_string(),
                node_id: "node-decision".to_string(),
                wait_kind: WaitKind::HumanGate,
                deadline_at: None,
                prompt: Some("choose".to_string()),
                prompt_ref: None,
                prompt_preview: None,
                approvers: None,
                on_timeout: Some(WaitOnTimeout::Fail),
            },
        )
        .await
        .expect("create wait");
        let _ = resolve_wait(
            &mut log,
            ResolveWaitInput {
                activity_id: "run-2::work::node-decision::att-1".to_string(),
                attempt_id: "run-2::work::node-decision::att-1".to_string(),
                resolution: WaitResolution::Rejected,
                by: "tester".to_string(),
                comment: Some("nope".to_string()),
                output: None,
                is_decision_node: true,
            },
        )
        .await
        .expect("resolve");
        let events = log.read_all().expect("events");
        assert_eq!(events[1].event_type, "waitResolved");
        assert_eq!(events[2].event_type, "activitySucceeded");
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
