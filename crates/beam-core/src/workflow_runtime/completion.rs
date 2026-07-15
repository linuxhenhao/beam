use anyhow::Result;
use serde_json::Value;

use crate::workflow_orchestrator::OrchestratorAction;
use crate::{EventDraft, EventLog, WorkflowActor};

use super::WorkflowDispatchOutcome;
use super::helpers::write_json_blob;

pub async fn complete_node_succeeded(
    log: &mut EventLog,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    if let OrchestratorAction::CompleteNodeSucceeded {
        node_id,
        last_activity_id,
        ..
    } = action
    {
        let _ = log.append(EventDraft {
            event_type: "nodeSucceeded".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "nodeId": node_id,
                "lastActivityId": last_activity_id,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("complete_node_succeeded called with wrong action")
    }
}

pub async fn complete_node_failed(
    log: &mut EventLog,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    if let OrchestratorAction::CompleteNodeFailed {
        node_id,
        last_activity_id,
        error_class,
    } = action
    {
        let _ = log.append(EventDraft {
            event_type: "nodeFailed".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "nodeId": node_id,
                "lastActivityId": last_activity_id,
                "errorClass": error_class,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("complete_node_failed called with wrong action")
    }
}

pub async fn complete_run_succeeded(
    log: &mut EventLog,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    if let OrchestratorAction::CompleteRunSucceeded { output_ref, .. } = action {
        let _ = log.append(EventDraft {
            event_type: "runSucceeded".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "outputRef": output_ref,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("complete_run_succeeded called with wrong action")
    }
}

pub async fn complete_run_failed(
    log: &mut EventLog,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    if let OrchestratorAction::CompleteRunFailed { failed_node_id } = action {
        let root_cause_event_id = find_root_cause_event_id(log, failed_node_id).await?;
        let _ = log.append(EventDraft {
            event_type: "runFailed".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "failedNodeId": failed_node_id,
                "rootCauseEventId": root_cause_event_id,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("complete_run_failed called with wrong action")
    }
}

pub(crate) async fn settle_work_result(
    log: &mut EventLog,
    activity_id: &str,
    attempt_id: &str,
    result: WorkflowDispatchOutcome,
) -> Result<WorkflowDispatchOutcome> {
    match &result {
        WorkflowDispatchOutcome::Succeeded { output, .. } => {
            let output_ref = write_json_blob(log, output.clone())?;
            let _ = log.append(EventDraft {
                event_type: "activitySucceeded".to_string(),
                actor: WorkflowActor::Worker,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "outputRef": output_ref,
                }),
                timestamp: None,
                payload_hash: None,
            })?;
        }
        WorkflowDispatchOutcome::Failed {
            error_code,
            error_class,
            error_message,
            ..
        } => {
            let _ = log.append(EventDraft {
                event_type: "activityFailed".to_string(),
                actor: WorkflowActor::Worker,
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
        }
        WorkflowDispatchOutcome::Cancelled {
            cancel_origin_event_id,
            ..
        } => {
            let _ = log.append(EventDraft {
                event_type: "activityCanceled".to_string(),
                actor: WorkflowActor::Worker,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "cancelOriginEventId": cancel_origin_event_id,
                }),
                timestamp: None,
                payload_hash: None,
            })?;
        }
    }
    Ok(result)
}

async fn find_root_cause_event_id(log: &EventLog, node_id: &str) -> Result<String> {
    let events = log.read_all()?;
    let mut node_failed_event_id: Option<String> = None;
    let mut activity_failed_event_id: Option<String> = None;
    let mut loop_finished_event_id: Option<String> = None;
    let mut node_activities = std::collections::BTreeSet::new();
    for e in &events {
        match e.event_type.as_str() {
            "attemptCreated" => {
                if e.payload.get("nodeId").and_then(Value::as_str) == Some(node_id)
                    && let Some(activity_id) = e.payload.get("activityId").and_then(Value::as_str)
                {
                    node_activities.insert(activity_id.to_string());
                }
            }
            "activityFailed" => {
                if let Some(activity_id) = e.payload.get("activityId").and_then(Value::as_str)
                    && node_activities.contains(activity_id)
                {
                    activity_failed_event_id = Some(e.event_id.clone());
                }
            }
            "nodeFailed" => {
                if e.payload.get("nodeId").and_then(Value::as_str) == Some(node_id) {
                    node_failed_event_id = Some(e.event_id.clone());
                }
            }
            "loopFinished" => {
                if e.payload.get("loopId").and_then(Value::as_str) == Some(node_id)
                    && e.payload.get("resolution").and_then(Value::as_str) != Some("approved")
                {
                    loop_finished_event_id = Some(e.event_id.clone());
                }
            }
            _ => {}
        }
    }
    Ok(activity_failed_event_id
        .or(node_failed_event_id)
        .or(loop_finished_event_id)
        .unwrap_or_else(|| {
            events
                .first()
                .map(|e| e.event_id.clone())
                .unwrap_or_default()
        }))
}
