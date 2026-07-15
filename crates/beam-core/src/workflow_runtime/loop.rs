use anyhow::Result;

use crate::workflow_orchestrator::OrchestratorAction;
use crate::{EventDraft, EventLog, WorkflowActor};

pub async fn start_loop(log: &mut EventLog, action: &crate::OrchestratorAction) -> Result<()> {
    if let OrchestratorAction::StartLoop {
        node_id,
        max_iterations,
    } = action
    {
        let _ = log.append(EventDraft {
            event_type: "loopStarted".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "loopId": node_id,
                "maxIterations": max_iterations,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("start_loop called with wrong action")
    }
}

pub async fn start_loop_iteration(
    log: &mut EventLog,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    if let OrchestratorAction::StartLoopIteration { node_id, iteration } = action {
        let _ = log.append(EventDraft {
            event_type: "loopIterationStarted".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "loopId": node_id,
                "iteration": iteration,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("start_loop_iteration called with wrong action")
    }
}

pub async fn finish_loop_iteration(
    log: &mut EventLog,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    if let OrchestratorAction::FinishLoopIteration {
        node_id,
        iteration,
        resolution,
        decision_activity_id,
        wait_resolved_event_id,
        by,
        comment,
        timed_out,
    } = action
    {
        let _ = log.append(EventDraft {
            event_type: "loopIterationFinished".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "loopId": node_id,
                "iteration": iteration,
                "resolution": resolution,
                "decisionActivityId": decision_activity_id,
                "waitResolvedEventId": wait_resolved_event_id,
                "by": by,
                "comment": comment,
                "timedOut": timed_out,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("finish_loop_iteration called with wrong action")
    }
}

pub async fn finish_loop(log: &mut EventLog, action: &crate::OrchestratorAction) -> Result<()> {
    if let OrchestratorAction::FinishLoop {
        node_id,
        final_iteration,
        resolution,
        output_ref,
        error_code,
        error_class,
    } = action
    {
        let _ = log.append(EventDraft {
            event_type: "loopFinished".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({
                "loopId": node_id,
                "finalIteration": final_iteration,
                "resolution": resolution,
                "outputRef": output_ref,
                "errorCode": error_code,
                "errorClass": error_class,
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        Ok(())
    } else {
        anyhow::bail!("finish_loop called with wrong action")
    }
}
