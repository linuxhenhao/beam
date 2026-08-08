use anyhow::{Context, Result};
use serde_json::Value;

use crate::workflow_binding::{BindingContext, resolve_bindings, resolve_bound_string};
use crate::workflow_orchestrator::OrchestratorAction;
use crate::workflow_sidecar::write_effect_input_sidecar;
use crate::{EventDraft, RunSnapshotDTO, WorkflowActor, WorkflowNode, read_run_snapshot};

use super::completion::settle_work_result;
use super::helpers::{
    derive_workflow_idempotency_key, gate_attempt_id, loop_context_from_activity, now_ms,
    sha256_hex, split_prompt, work_attempt_id, write_json_blob,
};
use super::{WorkflowDispatchOutcome, WorkflowDispatchRun, WorkflowRuntimeContext};

async fn read_snapshot(rt: &WorkflowRuntimeContext) -> Result<RunSnapshotDTO> {
    read_run_snapshot(&rt.log.run_dir)
        .await?
        .context("workflow runtime requires an existing run snapshot")
}

pub async fn dispatch_gate(
    rt: &mut WorkflowRuntimeContext,
    action: &crate::OrchestratorAction,
) -> Result<()> {
    match action {
        OrchestratorAction::DispatchGate {
            node_id,
            activity_id,
            human_gate,
        } => {
            let attempt_id = gate_attempt_id(activity_id);
            let input_ref = write_json_blob(
                &mut rt.log,
                serde_json::json!({
                    "kind": "human-gate",
                    "prompt": human_gate.prompt,
                    "approvers": human_gate.approvers,
                }),
            )?;
            rt.log.append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": node_id,
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "attemptNumber": 1,
                    "inputRef": input_ref,
                }),
                timestamp: None,
                payload_hash: None,
            })?;

            let snap = read_snapshot(rt).await?;
            let ctx = BindingContext {
                snapshot: &snap,
                def: &rt.def,
                run_dir: &rt.log.run_dir,
                loop_context: loop_context_from_activity(activity_id),
            };
            let resolved_prompt = resolve_bound_string(&human_gate.prompt, &ctx).await?;
            let prompt_field = split_prompt(&mut rt.log, &resolved_prompt)?;
            let _ = crate::create_wait(
                &mut rt.log,
                crate::CreateWaitInput {
                    activity_id: activity_id.clone(),
                    attempt_id,
                    node_id: node_id.clone(),
                    wait_kind: crate::WaitKind::HumanGate,
                    deadline_at: human_gate.deadline_ms.map(|ms| now_ms() + ms),
                    prompt: prompt_field.prompt,
                    prompt_ref: prompt_field.prompt_ref,
                    prompt_preview: prompt_field.prompt_preview,
                    approvers: human_gate.approvers.clone(),
                    on_timeout: human_gate.on_timeout.as_deref().map(|v| match v {
                        "success" => crate::WaitOnTimeout::Success,
                        _ => crate::WaitOnTimeout::Fail,
                    }),
                },
            )
            .await?;
            Ok(())
        }
        _ => anyhow::bail!("dispatch_gate called with non-gate action"),
    }
}

pub async fn dispatch_work<H: super::WorkflowExecutionHooks>(
    rt: &mut WorkflowRuntimeContext,
    hooks: &mut H,
    action: &crate::OrchestratorAction,
) -> Result<WorkflowDispatchOutcome> {
    match action {
        OrchestratorAction::DispatchWork {
            node_id,
            activity_id,
            node,
        } => {
            let attempt_id = work_attempt_id(activity_id, 1);
            let input_ref = write_json_blob(
                &mut rt.log,
                serde_json::json!({
                    "kind": match node.as_ref() {
                        WorkflowNode::Subagent(_) => "subagent",
                        WorkflowNode::HostExecutor(_) => "hostExecutor",
                        WorkflowNode::Loop(_) => "loop",
                        WorkflowNode::Decision(_) => "decision",
                    },
                    "bot_or_executor": match node.as_ref() {
                        WorkflowNode::Subagent(n) => Value::String(n.bot.clone()),
                        WorkflowNode::HostExecutor(n) => Value::String(n.executor.clone()),
                        _ => Value::Null,
                    },
                    "prompt_or_input": match node.as_ref() {
                        WorkflowNode::Subagent(n) => n.prompt.clone(),
                        WorkflowNode::HostExecutor(n) => n.input.clone(),
                        _ => Value::Null,
                    }
                }),
            )?;
            rt.log.append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": node_id,
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "attemptNumber": 1,
                    "inputRef": input_ref,
                }),
                timestamp: None,
                payload_hash: None,
            })?;

            let snap = read_snapshot(rt).await?;
            let bind_ctx = BindingContext {
                snapshot: &snap,
                def: &rt.def,
                run_dir: &rt.log.run_dir,
                loop_context: loop_context_from_activity(activity_id),
            };

            match node.as_ref() {
                WorkflowNode::Subagent(subagent) => {
                    let resolved_prompt = resolve_bound_string(&subagent.prompt, &bind_ctx).await?;
                    rt.log.append(EventDraft {
                        event_type: "activityRunning".to_string(),
                        actor: WorkflowActor::Scheduler,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": attempt_id,
                            "leaseId": format!("lease-{}", attempt_id),
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    let result = hooks
                        .execute_subagent(
                            WorkflowDispatchRun {
                                run_id: &rt.log.run_id,
                                workflow_id: snap.run.workflow_id.as_deref().unwrap_or(""),
                                revision_id: snap.run.revision_id.as_deref().unwrap_or(""),
                                activity_id,
                                attempt_id: &attempt_id,
                                node_id,
                            },
                            subagent,
                            resolved_prompt,
                        )
                        .await?;
                    settle_work_result(&mut rt.log, activity_id, &attempt_id, result).await
                }
                WorkflowNode::HostExecutor(executor) => {
                    let resolved_input = resolve_bindings(&executor.input, &bind_ctx).await?;

                    // --- prepare (parse + canonicalise) BEFORE any side-effect ---
                    let prepared = hooks
                        .prepare_host_executor(&executor.executor, &resolved_input)
                        .context("prepare_host_executor failed")?;

                    // --- write effect-input.json using the canonical input ---
                    let _ = write_effect_input_sidecar(
                        &rt.log,
                        activity_id,
                        &attempt_id,
                        &prepared.canonical_input,
                    )
                    .await?;

                    // --- emit effectAttempted BEFORE calling the external provider ---
                    let idempotency_key = derive_workflow_idempotency_key(
                        snap.run.workflow_id.as_deref().unwrap_or(""),
                        snap.run.revision_id.as_deref().unwrap_or(""),
                        &rt.log.run_id,
                        node_id,
                        &attempt_id,
                    );
                    let input_bytes = serde_json::to_vec(&prepared.canonical_input)?;
                    let input_hash = sha256_hex(&input_bytes);
                    rt.log.append(EventDraft {
                        event_type: "effectAttempted".to_string(),
                        actor: WorkflowActor::Scheduler,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": attempt_id,
                            "idempotencyKey": idempotency_key,
                            "inputHash": input_hash,
                            "idempotencyTtlMs": prepared.idempotency_ttl_ms,
                            "provider": prepared.provider,
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;

                    let result = hooks
                        .execute_host_executor(
                            WorkflowDispatchRun {
                                run_id: &rt.log.run_id,
                                workflow_id: snap.run.workflow_id.as_deref().unwrap_or(""),
                                revision_id: snap.run.revision_id.as_deref().unwrap_or(""),
                                activity_id,
                                attempt_id: &attempt_id,
                                node_id,
                            },
                            executor,
                            prepared.parsed_input,
                        )
                        .await?;
                    settle_work_result(&mut rt.log, activity_id, &attempt_id, result).await
                }
                WorkflowNode::Loop(_) | WorkflowNode::Decision(_) => {
                    anyhow::bail!("dispatch_work received unsupported node type")
                }
            }
        }
        _ => anyhow::bail!("dispatch_work called with non-work action"),
    }
}
