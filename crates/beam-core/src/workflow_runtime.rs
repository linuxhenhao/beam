use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use crate::workflow_binding::{
    BindingContext, LoopContext, resolve_bindings, resolve_bound_string,
};
use crate::workflow_definition::{HostExecutorNode, SubagentNode};
use crate::workflow_orchestrator::OrchestratorAction;
use crate::workflow_sidecar::write_effect_input_sidecar;
use crate::{
    EventDraft, EventLog, RunSnapshotDTO, RunStatus, WorkflowActor, WorkflowDefinition,
    WorkflowNode, WorkflowOutputRef, decide_next_actions, read_run_snapshot,
};

#[derive(Debug, Clone)]
pub struct WorkflowRuntimeContext {
    pub log: EventLog,
    pub def: WorkflowDefinition,
    pub runs_base_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDispatchSession {
    pub session_id: String,
    pub bot_name: String,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub cli_session_id: Option<String>,
    pub lark_app_id: Option<String>,
    pub cli_id: Option<String>,
    pub working_dir: Option<String>,
    pub web_port: Option<u16>,
    pub log_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowDispatchOutcome {
    Succeeded {
        output: Value,
        session: Option<WorkflowDispatchSession>,
    },
    Failed {
        error_code: String,
        error_class: String,
        error_message: String,
        session: Option<WorkflowDispatchSession>,
    },
    Cancelled {
        cancel_origin_event_id: String,
        session: Option<WorkflowDispatchSession>,
    },
}

#[derive(Debug, Clone)]
pub struct WorkflowDispatchRun<'a> {
    pub run_id: &'a str,
    pub workflow_id: &'a str,
    pub revision_id: &'a str,
    pub activity_id: &'a str,
    pub attempt_id: &'a str,
    pub node_id: &'a str,
}

/// The result of preparing a host-executor call: parsed input, canonical
/// (effect) input, and provider metadata.  The runtime writes the canonical
/// input to `effect-input.json`, emits `effectAttempted`, and then passes the
/// parsed input to `execute_host_executor`.
#[derive(Debug, Clone)]
pub struct HostExecutorPrepareResult {
    /// Input after executor-specific parsing/validation (feeds `execute_host_executor`).
    pub parsed_input: Value,
    /// Canonical, deterministic form of the effect input (feeds `effect-input.json`
    /// and the `inputHash` in `effectAttempted`).
    pub canonical_input: Value,
    /// Provider identifier for the `effectAttempted` event (e.g. `"feishu-im"`).
    pub provider: String,
    /// Idempotency TTL in milliseconds for the `effectAttempted` event.
    pub idempotency_ttl_ms: u64,
}

#[async_trait]
pub trait WorkflowExecutionHooks {
    async fn execute_subagent(
        &mut self,
        ctx: WorkflowDispatchRun<'_>,
        node: &SubagentNode,
        resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome>;

    async fn execute_host_executor(
        &mut self,
        ctx: WorkflowDispatchRun<'_>,
        node: &HostExecutorNode,
        // Parsed input as returned by `prepare_host_executor`.
        parsed_input: Value,
    ) -> Result<WorkflowDispatchOutcome>;

    /// Prepare a host-executor call: parse/validate the resolved input and
    /// return the parsed form, the canonical (effect) form, and the provider
    /// metadata.  Called by the runtime **before** writing `effect-input.json`
    /// and emitting `effectAttempted`.
    ///
    /// The default implementation uses `get_host_executor_provider_meta` for
    /// provider/TTL and treats `resolved_input` as both parsed and canonical
    /// input — matching the legacy behaviour.
    fn prepare_host_executor(
        &self,
        executor_name: &str,
        resolved_input: &Value,
    ) -> Result<HostExecutorPrepareResult> {
        let (provider, idempotency_ttl_ms) = get_host_executor_provider_meta(executor_name);
        Ok(HostExecutorPrepareResult {
            parsed_input: resolved_input.clone(),
            canonical_input: resolved_input.clone(),
            provider: provider.to_string(),
            idempotency_ttl_ms,
        })
    }

    /// Attempt to recover dangling effects before the next tick.
    ///
    /// Called by `run_loop` before each `run_tick` to resolve any activities
    /// that were left in a dangling state (e.g. `effectAttempted` was written
    /// but the daemon crashed before writing a terminal event).
    ///
    /// The hook should inspect `snapshot.dangling.effect_attempted` and, for
    /// each matching provider, attempt reconciliation (idempotent re-submit,
    /// read-only lookup, or manual failure).  It writes events directly to
    /// `log`.  The runtime will re-read the snapshot on the next loop
    /// iteration, picking up any terminal events written here.
    ///
    /// The default implementation does nothing (no recovery).
    async fn recover_dangling_effects(
        &mut self,
        _log: &mut EventLog,
        snapshot: &RunSnapshotDTO,
    ) -> Result<RecoveryResult> {
        Ok(RecoveryResult {
            had_progress: false,
            has_remaining_dangling: !snapshot.dangling.effect_attempted.is_empty(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTickResult {
    pub actions: usize,
    pub snapshot: RunSnapshotDTO,
}

#[derive(Debug, Clone)]
struct ScheduledAction {
    action: OrchestratorAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunLoopStopReason {
    Terminal,
    NoProgress,
    AwaitingWait,
    MaxTicks,
}

/// Result of a recovery attempt during run_loop's pre-tick recovery phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryResult {
    /// Whether any events were written (i.e., recovery made progress).
    pub had_progress: bool,
    /// Whether there are still unrecovered dangling effects after this attempt.
    pub has_remaining_dangling: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLoopResult {
    pub reason: RunLoopStopReason,
    pub ticks: usize,
    pub last_snapshot: RunSnapshotDTO,
}

pub async fn run_tick<H: WorkflowExecutionHooks + Clone + Send + 'static>(
    rt: &mut WorkflowRuntimeContext,
    hooks: &mut H,
    max_concurrency: usize,
) -> Result<RunTickResult> {
    let snapshot = read_snapshot(rt).await?;
    if matches!(
        snapshot.run.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
    ) {
        return Ok(RunTickResult {
            actions: 0,
            snapshot,
        });
    }

    if snapshot_has_pending_cancel(&snapshot) {
        return Ok(RunTickResult {
            actions: 0,
            snapshot,
        });
    }

    let actions = select_tick_actions(
        decide_next_actions(&snapshot, &rt.def),
        &rt.def,
        max_concurrency,
    );
    if actions.is_empty() {
        return Ok(RunTickResult {
            actions: 0,
            snapshot,
        });
    }

    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    for scheduled in actions.into_iter() {
        let mut rt_clone = rt.clone();
        let mut hooks_clone = hooks.clone();
        join_set.spawn(async move {
            apply_orchestrator_action(&mut rt_clone, &mut hooks_clone, scheduled.action).await
        });
    }

    let mut applied = 0usize;
    let mut cancel_poll = tokio::time::interval(Duration::from_millis(20));
    cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut cancel_seen = false;
    let mut cancel_abort_deadline: Option<Instant> = None;

    while !join_set.is_empty() {
        tokio::select! {
            result = join_set.join_next() => {
                let Some(result) = result else {
                    break;
                };
                match result {
                    Ok(Ok(())) => {
                        applied += 1;
                        let snapshot = read_snapshot(rt).await?;
                        if snapshot_has_pending_cancel(&snapshot)
                            || matches!(
                                snapshot.run.status,
                                RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
                            )
                        {
                            if snapshot_has_pending_cancel(&snapshot) && !cancel_seen {
                                cancel_seen = true;
                                cancel_abort_deadline =
                                    Some(Instant::now() + Duration::from_millis(120));
                            }
                            if matches!(
                                snapshot.run.status,
                                RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
                            ) {
                                join_set.abort_all();
                                return Ok(RunTickResult {
                                    actions: applied,
                                    snapshot,
                                });
                            }
                        }
                    }
                    Ok(Err(err)) => {
                        if cancel_seen {
                            continue;
                        }
                        join_set.abort_all();
                        return Err(err);
                    }
                    Err(err) => {
                        if cancel_seen {
                            continue;
                        }
                        join_set.abort_all();
                        return Err(anyhow::anyhow!(err));
                    }
                }
            }
            _ = cancel_poll.tick(), if !cancel_seen => {
                let snapshot = read_snapshot(rt).await?;
                if snapshot_has_pending_cancel(&snapshot) {
                    cancel_seen = true;
                    cancel_abort_deadline = Some(Instant::now() + Duration::from_millis(120));
                    join_set.abort_all();
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)), if cancel_seen => {
                if let Some(deadline) = cancel_abort_deadline {
                    if Instant::now() >= deadline {
                        join_set.abort_all();
                    }
                }
            }
        }
    }

    let snapshot = read_snapshot(rt).await?;
    Ok(RunTickResult {
        actions: applied,
        snapshot,
    })
}

pub async fn run_loop<H: WorkflowExecutionHooks + Clone + Send + 'static>(
    rt: &mut WorkflowRuntimeContext,
    hooks: &mut H,
    max_ticks: usize,
    max_concurrency: usize,
) -> Result<RunLoopResult> {
    let mut ticks = 0usize;
    loop {
        if ticks >= max_ticks {
            let snapshot = read_snapshot(rt).await?;
            return Ok(RunLoopResult {
                reason: RunLoopStopReason::MaxTicks,
                ticks,
                last_snapshot: snapshot,
            });
        }

        check_pending_cancels(rt).await?;

        // --- Recovery phase: handle dangling effects before decide_next_actions ---
        // This ensures that crashed/restarted workflows with dangling
        // effectAttempted activities get reconciled before any new work is
        // dispatched.  If recovery writes events, we re-read the snapshot and
        // restart the loop (without incrementing ticks) so the orchestrator
        // sees the updated state.
        let pre_recovery_snapshot = read_snapshot(rt).await?;
        if matches!(
            pre_recovery_snapshot.run.status,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        ) {
            return Ok(RunLoopResult {
                reason: RunLoopStopReason::Terminal,
                ticks,
                last_snapshot: pre_recovery_snapshot,
            });
        }

        if !pre_recovery_snapshot
            .dangling
            .effect_attempted
            .is_empty()
        {
            let recovery = hooks
                .recover_dangling_effects(&mut rt.log, &pre_recovery_snapshot)
                .await?;
            if recovery.had_progress {
                // Recovery wrote events — re-read the snapshot on the next
                // iteration and re-evaluate (replay → decide_next_actions).
                continue;
            }
            // Recovery could not make progress (e.g. transient provider
            // errors).  Fall through to run_tick so the orchestrator can
            // determine whether other dispatchable actions exist or whether
            // the loop should stop with NoProgress / AwaitingWait.
        }

        let tick = run_tick(rt, hooks, max_concurrency).await?;
        ticks += 1;
        if tick.snapshot.run.status == RunStatus::Succeeded
            || tick.snapshot.run.status == RunStatus::Failed
            || tick.snapshot.run.status == RunStatus::Cancelled
        {
            return Ok(RunLoopResult {
                reason: RunLoopStopReason::Terminal,
                ticks,
                last_snapshot: tick.snapshot,
            });
        }
        if tick.actions == 0 {
            let has_waits = !tick.snapshot.dangling.waits.is_empty()
                && tick
                    .snapshot
                    .dangling
                    .waits
                    .iter()
                    .any(|w| !tick.snapshot.dangling.cancels.contains(w));
            let reason = if has_waits {
                RunLoopStopReason::AwaitingWait
            } else {
                RunLoopStopReason::NoProgress
            };
            return Ok(RunLoopResult {
                reason,
                ticks,
                last_snapshot: tick.snapshot,
            });
        }
    }
}

async fn check_pending_cancels(rt: &mut WorkflowRuntimeContext) -> Result<()> {
    let _events = rt.log.read_all()?;
    let snapshot = read_snapshot(rt).await?;
    for activity_id in &snapshot.dangling.cancels {
        if !snapshot.dangling.activities.contains(activity_id) {
            let attempt_id = snapshot
                .activities
                .iter()
                .find(|a| &a.activity_id == activity_id)
                .and_then(|a| a.current_attempt_id.clone())
                .unwrap_or_else(|| format!("{}-attempt-1", activity_id));
            let _ = crate::complete_activity_cancel(
                &mut rt.log,
                crate::CompleteActivityCancelInput {
                    activity_id: activity_id.clone(),
                    attempt_id,
                    cancel_origin_event_id: String::new(),
                },
                WorkflowActor::Scheduler,
            );
        }
    }

    if let Some(ref intent) = snapshot.run.cancelled_run_intent {
        if snapshot.run.status != RunStatus::Cancelled {
            let _ = crate::complete_run_cancel(
                &mut rt.log,
                crate::CompleteRunCancelInput {
                    cancel_origin_event_id: intent.cancel_origin_event_id.clone(),
                },
                WorkflowActor::Scheduler,
            );
        }
    }

    if !snapshot.run.cancelled_node_intents.is_empty() {
        for (node_id, intent) in &snapshot.run.cancelled_node_intents {
            let _ = crate::complete_node_cancel(
                &mut rt.log,
                crate::CompleteNodeCancelInput {
                    node_id: node_id.clone(),
                    cancel_origin_event_id: intent.cancel_origin_event_id.clone(),
                },
                WorkflowActor::Scheduler,
            );
        }
    }

    Ok(())
}

fn snapshot_has_pending_cancel(snapshot: &RunSnapshotDTO) -> bool {
    snapshot.run.cancelled_run_intent.is_some() || !snapshot.run.cancelled_node_intents.is_empty()
}

fn select_tick_actions(
    actions: Vec<OrchestratorAction>,
    def: &WorkflowDefinition,
    max_concurrency: usize,
) -> Vec<ScheduledAction> {
    let limit = max_concurrency.max(1);
    let mut selected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for action in actions {
        let serialization_key = action_serialization_key(def, &action);
        if seen.insert(serialization_key.clone()) {
            selected.push(ScheduledAction { action });
            if selected.len() >= limit {
                break;
            }
        }
    }
    selected
}

fn action_serialization_key(_def: &WorkflowDefinition, action: &OrchestratorAction) -> String {
    match action {
        OrchestratorAction::DispatchWork { node_id, node, .. } => {
            let bot_key = match node {
                WorkflowNode::Subagent(node) => Some(format!("bot:{}", node.bot)),
                WorkflowNode::HostExecutor(node) => Some(format!("executor:{}", node.executor)),
                WorkflowNode::Loop(_) | WorkflowNode::Decision(_) => None,
            };
            bot_key.unwrap_or_else(|| format!("node:{node_id}"))
        }
        OrchestratorAction::DispatchGate { node_id, .. } => format!("gate:{node_id}"),
        OrchestratorAction::CompleteNodeSucceeded { node_id, .. }
        | OrchestratorAction::CompleteNodeFailed { node_id, .. } => {
            format!("node:{node_id}")
        }
        OrchestratorAction::CompleteRunSucceeded { sink_node_id, .. } => {
            format!("run:{sink_node_id}:succeeded")
        }
        OrchestratorAction::CompleteRunFailed { failed_node_id } => {
            format!("run:{failed_node_id}:failed")
        }
    }
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
                loop_context: None,
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

pub async fn dispatch_work<H: WorkflowExecutionHooks>(
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
                    "kind": match node {
                        WorkflowNode::Subagent(_) => "subagent",
                        WorkflowNode::HostExecutor(_) => "hostExecutor",
                        WorkflowNode::Loop(_) => "loop",
                        WorkflowNode::Decision(_) => "decision",
                    },
                    "bot_or_executor": match node {
                        WorkflowNode::Subagent(n) => Value::String(n.bot.clone()),
                        WorkflowNode::HostExecutor(n) => Value::String(n.executor.clone()),
                        _ => Value::Null,
                    },
                    "prompt_or_input": match node {
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

            match node {
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

async fn apply_orchestrator_action<H: WorkflowExecutionHooks>(
    rt: &mut WorkflowRuntimeContext,
    hooks: &mut H,
    action: OrchestratorAction,
) -> Result<()> {
    match action {
        OrchestratorAction::DispatchGate { .. } => dispatch_gate(rt, &action).await?,
        OrchestratorAction::DispatchWork { .. } => {
            let _ = dispatch_work(rt, hooks, &action).await?;
        }
        OrchestratorAction::CompleteNodeSucceeded { .. } => {
            complete_node_succeeded(&mut rt.log, &action).await?
        }
        OrchestratorAction::CompleteNodeFailed { .. } => {
            complete_node_failed(&mut rt.log, &action).await?
        }
        OrchestratorAction::CompleteRunSucceeded { .. } => {
            complete_run_succeeded(&mut rt.log, &action).await?
        }
        OrchestratorAction::CompleteRunFailed { .. } => {
            complete_run_failed(&mut rt.log, &action).await?
        }
    }
    Ok(())
}

async fn settle_work_result(
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

fn gate_attempt_id(activity_id: &str) -> String {
    format!("{activity_id}::att-1")
}

fn work_attempt_id(activity_id: &str, attempt_number: u64) -> String {
    format!("{activity_id}::att-{attempt_number}")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn split_prompt(log: &mut EventLog, resolved_prompt: &str) -> Result<PromptField> {
    if resolved_prompt.len() <= 1024 {
        return Ok(PromptField {
            prompt: Some(resolved_prompt.to_string()),
            prompt_ref: None,
            prompt_preview: None,
        });
    }
    let prompt_ref = write_json_blob(log, Value::String(resolved_prompt.to_string()))?;
    Ok(PromptField {
        prompt: None,
        prompt_ref: Some(prompt_ref),
        prompt_preview: Some(make_prompt_preview(resolved_prompt)),
    })
}

#[derive(Debug)]
struct PromptField {
    prompt: Option<String>,
    prompt_ref: Option<WorkflowOutputRef>,
    prompt_preview: Option<String>,
}

fn make_prompt_preview(full: &str) -> String {
    const MAX: usize = 480;
    if full.chars().count() <= MAX {
        return full.to_string();
    }
    let suffix = "…(完整内容见 dashboard)";
    let budget = MAX.saturating_sub(suffix.chars().count());
    let mut out = String::new();
    for ch in full.chars().take(budget) {
        out.push(ch);
    }
    out.push_str(suffix);
    out
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

/// Derive a deterministic idempotency key for a workflow host-executor attempt.
///
/// The key is `wf_` prefixed with a SHA-256 hex fragment of the canonical
/// (workflowId, revisionId, runId, nodeId, attemptId) seed.  This function
/// lives in `beam-core` so the runtime can emit `effectAttempted` events
/// before delegating to the daemon’s executor hooks.
pub fn derive_workflow_idempotency_key(
    workflow_id: &str,
    revision_id: &str,
    run_id: &str,
    node_id: &str,
    attempt_id: &str,
) -> String {
    let seed = serde_json::json!({
        "attemptId": attempt_id,
        "nodeId": node_id,
        "revisionId": revision_id,
        "runId": run_id,
        "workflowId": workflow_id,
    });
    let mut hasher = Sha256::new();
    let canonical = serde_json::to_vec(&seed).expect("workflow idempotency seed serializable");
    hasher.update(&canonical);
    let hash = format!("{:x}", hasher.finalize());
    let namespace = "wf_";
    let max_len = 50usize;
    let hash_len = max_len.saturating_sub(namespace.len());
    format!("{namespace}{}", &hash[..hash_len.min(hash.len())])
}

/// Return (provider, idempotency_ttl_ms) metadata for a known host executor.
///
/// This mapping lives in `beam-core` so that `effectAttempted` events can be
/// emitted with accurate provider / TTL information without depending on the
/// daemon’s HostExecutor trait.
pub fn get_host_executor_provider_meta(executor_name: &str) -> (&'static str, u64) {
    match executor_name {
        "feishu-send" | "feishu-reply" => ("feishu-im", 60_000),
        "beam-schedule" => ("beam-schedule", 86_400_000),
        _ => ("manual", 300_000),
    }
}

fn loop_context_from_activity(activity_id: &str) -> Option<LoopContext<'_>> {
    let loop_start = activity_id.find("::loop::")?;
    let after_loop = &activity_id[loop_start + "::loop::".len()..];
    let iter_end = after_loop.find("::")?;
    let loop_part = &after_loop[..iter_end];
    let (loop_id, iteration) = loop_part.rsplit_once('.')?;
    let iteration = iteration.parse().ok()?;
    Some(LoopContext { loop_id, iteration })
}

async fn read_snapshot(rt: &WorkflowRuntimeContext) -> Result<RunSnapshotDTO> {
    read_run_snapshot(&rt.log.run_dir)
        .await?
        .context("workflow runtime requires an existing run snapshot")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DanglingSnapshot;
    use crate::RunChatBinding;
    use crate::RunState;
    use crate::workflow_definition::NodeBase;
    use crate::workflow_snapshot::NodeStatus;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct FakeHooks;

    #[async_trait]
    impl WorkflowExecutionHooks for FakeHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: Value::String(resolved_prompt),
                session: None,
            })
        }

        async fn execute_host_executor(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &HostExecutorNode,
            resolved_input: Value,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: resolved_input,
                session: None,
            })
        }
    }

    fn temp_run_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "beam-workflow-runtime-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ))
    }

    fn workflow_def() -> WorkflowDefinition {
        WorkflowDefinition {
            workflow_id: "flow-a".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::Subagent(SubagentNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: None,
                    },
                    bot: "bot-a".to_string(),
                    prompt: Value::String("hello ${params.name}".to_string()),
                    working_dir: None,
                    model_overrides: None,
                    tool_policy: None,
                }),
            )]),
        }
    }

    #[tokio::test]
    async fn run_tick_dispatches_simple_workflow() {
        let run_dir = temp_run_dir("tick");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        fs::write(run_dir.join("params.json"), r#"{"name":"beam"}"#).unwrap();

        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let params = BTreeMap::from([(String::from("name"), String::from("beam"))]);
        let run_id = "run-1";
        let bootstrap = crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello ${params.name}"}}}"#,
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
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: workflow_def(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let tick = run_tick(&mut rt, &mut hooks, 1).await.unwrap();
        assert!(tick.actions > 0);
        assert_eq!(tick.snapshot.run.workflow_id.as_deref(), Some("flow-a"));
        assert!(matches!(
            tick.snapshot
                .nodes
                .iter()
                .find(|node| node.node_id == "a")
                .map(|node| node.status),
            Some(NodeStatus::Succeeded | NodeStatus::Running | NodeStatus::Triggered)
        ));
        let _ = fs::remove_dir_all(&run_dir);
        let _ = bootstrap;
    }

    #[tokio::test]
    async fn run_tick_honors_max_concurrency_cap() {
        let run_dir = temp_run_dir("cap");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let params = BTreeMap::new();
        let run_id = "run-cap";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-cap","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
                expected_workflow_id: Some("flow-cap"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-cap".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "a".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("one".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "b".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-b".to_string(),
                            prompt: Value::String("two".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let tick = run_tick(&mut rt, &mut hooks, 1).await.unwrap();
        assert_eq!(tick.actions, 1);
        let snapshot = tick.snapshot;
        let attempted: Vec<_> = snapshot
            .activities
            .iter()
            .map(|a| a.activity_id.as_str())
            .collect();
        assert_eq!(attempted.len(), 1);
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn run_tick_dispatches_multiple_actions_concurrently() {
        let run_dir = temp_run_dir("concurrent");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let params = BTreeMap::new();
        let run_id = "run-concurrent";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-concurrent","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
                expected_workflow_id: Some("flow-concurrent"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-concurrent".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "a".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("one".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "b".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-b".to_string(),
                            prompt: Value::String("two".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        #[derive(Clone)]
        struct SleepyHooks;

        #[async_trait]
        impl WorkflowExecutionHooks for SleepyHooks {
            async fn execute_subagent(
                &mut self,
                _ctx: WorkflowDispatchRun<'_>,
                _node: &SubagentNode,
                resolved_prompt: String,
            ) -> Result<WorkflowDispatchOutcome> {
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                Ok(WorkflowDispatchOutcome::Succeeded {
                    output: Value::String(resolved_prompt),
                    session: None,
                })
            }

            async fn execute_host_executor(
                &mut self,
                _ctx: WorkflowDispatchRun<'_>,
                _node: &HostExecutorNode,
                resolved_input: Value,
            ) -> Result<WorkflowDispatchOutcome> {
                Ok(WorkflowDispatchOutcome::Succeeded {
                    output: resolved_input,
                    session: None,
                })
            }
        }

        let mut hooks = SleepyHooks;
        let started = std::time::Instant::now();
        let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
        let elapsed = started.elapsed();
        assert_eq!(tick.actions, 2);
        assert!(
            elapsed < std::time::Duration::from_millis(220),
            "run_tick took {:?}, expected concurrent execution under 220ms",
            elapsed
        );
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn run_tick_serializes_actions_for_the_same_bot() {
        let run_dir = temp_run_dir("same-bot");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let params = BTreeMap::new();
        let run_id = "run-same-bot";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-same-bot","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-shared","prompt":"one"},"b":{"type":"subagent","bot":"bot-shared","prompt":"two"},"c":{"type":"subagent","bot":"bot-other","prompt":"three"}}}"#,
                expected_workflow_id: Some("flow-same-bot"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-same-bot".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "a".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-shared".to_string(),
                            prompt: Value::String("one".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "b".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-shared".to_string(),
                            prompt: Value::String("two".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "c".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-other".to_string(),
                            prompt: Value::String("three".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        #[derive(Clone)]
        struct SerializingHooks {
            started: Arc<Mutex<Vec<String>>>,
            active_by_bot: Arc<Mutex<HashMap<String, usize>>>,
            max_active_by_bot: Arc<Mutex<HashMap<String, usize>>>,
        }

        #[async_trait]
        impl WorkflowExecutionHooks for SerializingHooks {
            async fn execute_subagent(
                &mut self,
                _ctx: WorkflowDispatchRun<'_>,
                node: &SubagentNode,
                resolved_prompt: String,
            ) -> Result<WorkflowDispatchOutcome> {
                let bot = node.bot.clone();
                {
                    let mut active = self.active_by_bot.lock().await;
                    let entry = active.entry(bot.clone()).or_insert(0);
                    *entry += 1;
                    let mut max_active = self.max_active_by_bot.lock().await;
                    let max_entry = max_active.entry(bot.clone()).or_insert(0);
                    if *entry > *max_entry {
                        *max_entry = *entry;
                    }
                    assert!(
                        *entry <= 1,
                        "bot {} ran concurrently with itself: {}",
                        bot,
                        *entry
                    );
                }
                self.started.lock().await.push(resolved_prompt.clone());
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                {
                    let mut active = self.active_by_bot.lock().await;
                    if let Some(entry) = active.get_mut(&bot) {
                        *entry = entry.saturating_sub(1);
                    }
                }
                Ok(WorkflowDispatchOutcome::Succeeded {
                    output: Value::String(resolved_prompt),
                    session: None,
                })
            }

            async fn execute_host_executor(
                &mut self,
                _ctx: WorkflowDispatchRun<'_>,
                _node: &HostExecutorNode,
                resolved_input: Value,
            ) -> Result<WorkflowDispatchOutcome> {
                Ok(WorkflowDispatchOutcome::Succeeded {
                    output: resolved_input,
                    session: None,
                })
            }
        }

        let started = Arc::new(Mutex::new(Vec::new()));
        let active_by_bot = Arc::new(Mutex::new(HashMap::new()));
        let max_active_by_bot = Arc::new(Mutex::new(HashMap::new()));
        let mut hooks = SerializingHooks {
            started: started.clone(),
            active_by_bot: active_by_bot.clone(),
            max_active_by_bot: max_active_by_bot.clone(),
        };

        let started_at = std::time::Instant::now();
        let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
        let elapsed = started_at.elapsed();

        assert_eq!(tick.actions, 2);
        let started = started.lock().await.clone();
        assert_eq!(started.len(), 2);
        assert!(started.contains(&"one".to_string()));
        assert!(started.contains(&"three".to_string()));
        assert!(!started.contains(&"two".to_string()));
        let max_active_by_bot = max_active_by_bot.lock().await;
        assert_eq!(max_active_by_bot.get("bot-shared"), Some(&1));
        assert_eq!(max_active_by_bot.get("bot-other"), Some(&1));
        assert!(
            elapsed < std::time::Duration::from_millis(220),
            "run_tick took {:?}, expected two distinct bots to run concurrently",
            elapsed
        );
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[derive(Clone)]
    struct CancellingHooks {
        run_id: String,
        runs_base_dir: PathBuf,
        calls: Arc<Mutex<usize>>,
        completed: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl WorkflowExecutionHooks for CancellingHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            let mut calls = self.calls.lock().await;
            *calls += 1;
            if *calls == 1 {
                let run_id = self.run_id.clone();
                let runs_base_dir = self.runs_base_dir.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    let mut log = EventLog::new(run_id.clone(), runs_base_dir).unwrap();
                    let _ = log.append(EventDraft {
                        event_type: "cancelRequested".to_string(),
                        actor: WorkflowActor::Human,
                        payload: serde_json::json!({
                            "target": { "kind": "run", "runId": run_id },
                            "reason": "cancel mid tick",
                            "by": "tester",
                        }),
                        timestamp: None,
                        payload_hash: None,
                    });
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            *self.completed.lock().await += 1;
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: Value::String(resolved_prompt),
                session: None,
            })
        }

        async fn execute_host_executor(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &HostExecutorNode,
            resolved_input: Value,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: resolved_input,
                session: None,
            })
        }
    }

    #[tokio::test]
    async fn run_tick_stops_between_actions_when_cancel_arrives() {
        let run_dir = temp_run_dir("cancel-mid-tick");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-cancel";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-cancel","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
                expected_workflow_id: Some("flow-cancel"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-cancel".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "a".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("one".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "b".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-b".to_string(),
                            prompt: Value::String("two".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = CancellingHooks {
            run_id: run_id.to_string(),
            runs_base_dir: paths.workflow_runs_dir(),
            calls: Arc::new(Mutex::new(0)),
            completed: Arc::new(Mutex::new(0)),
        };
        let started = std::time::Instant::now();
        let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
        let elapsed = started.elapsed();
        assert!(tick.actions < 2);
        assert!(tick.snapshot.run.cancelled_run_intent.is_some());
        assert!(tick.snapshot.activities.len() >= 1);
        assert!(
            elapsed < std::time::Duration::from_millis(250),
            "run_tick took {:?}, expected cancel to interrupt long-running actions",
            elapsed
        );
        assert_eq!(*hooks.completed.lock().await, 0);
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn run_tick_skips_dispatch_when_run_cancel_is_pending() {
        let run_dir = temp_run_dir("reconcile-cancel");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-reconcile-cancel";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-reconcile","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"one"},"b":{"type":"subagent","bot":"bot-b","prompt":"two"}}}"#,
                expected_workflow_id: Some("flow-reconcile"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = crate::request_cancel(
            &mut log,
            crate::RequestCancelInput {
                target: serde_json::json!({
                    "kind": "run",
                    "runId": run_id,
                }),
                reason: "cancel before dispatch".to_string(),
                by: "tester".to_string(),
            },
            WorkflowActor::Human,
        )
        .await
        .unwrap();
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-reconcile".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "a".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("one".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                    (
                        "b".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-b".to_string(),
                            prompt: Value::String("two".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let tick = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
        assert_eq!(tick.actions, 0);
        assert!(tick.snapshot.run.cancelled_run_intent.is_some());
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn orchestrator_action_is_dispatch_classifies_correctly() {
        use crate::workflow_definition::{NodeBase, SubagentNode};
        use crate::workflow_orchestrator::OrchestratorAction;
        let dispatch = OrchestratorAction::DispatchWork {
            node_id: "n1".to_string(),
            activity_id: "a1".to_string(),
            node: crate::WorkflowNode::Subagent(SubagentNode {
                base: NodeBase {
                    description: None,
                    depends: None,
                    human_gate: None,
                    retry_policy: None,
                    timeout_ms: None,
                    max_output_bytes: None,
                    output_schema: None,
                    unsafe_allow_ungated: None,
                },
                bot: "b1".to_string(),
                prompt: serde_json::json!("p"),
                working_dir: None,
                model_overrides: None,
                tool_policy: None,
            }),
        };
        assert!(dispatch.is_dispatch());

        let settle = OrchestratorAction::CompleteNodeSucceeded {
            node_id: "n1".to_string(),
            last_activity_id: "a1".to_string(),
            output_ref: None,
        };
        assert!(!settle.is_dispatch());
    }

    #[tokio::test]
    async fn run_loop_stops_when_progress_is_exhausted() {
        let run_dir = temp_run_dir("loop");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let params = BTreeMap::from([(String::from("name"), String::from("beam"))]);
        let run_id = "run-1";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello ${params.name}"}}}"#,
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
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: workflow_def(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 3, 1).await.unwrap();
        assert!(matches!(
            result.reason,
            RunLoopStopReason::Terminal | RunLoopStopReason::NoProgress
        ));
        assert!(result.ticks > 0);
        let _ = fs::remove_dir_all(&run_dir);
    }

    // --- Recovery phase tests (Task 4.1) ---

    /// A hook that counts how many times `recover_dangling_effects` is called.
    #[derive(Clone)]
    struct RecoveryCountingHooks {
        recovery_calls: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl WorkflowExecutionHooks for RecoveryCountingHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: Value::String(resolved_prompt),
                session: None,
            })
        }

        async fn execute_host_executor(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &HostExecutorNode,
            resolved_input: Value,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: resolved_input,
                session: None,
            })
        }

        async fn recover_dangling_effects(
            &mut self,
            _log: &mut EventLog,
            _snapshot: &RunSnapshotDTO,
        ) -> Result<RecoveryResult> {
            *self.recovery_calls.lock().await += 1;
            Ok(RecoveryResult {
                had_progress: false,
                has_remaining_dangling: true,
            })
        }
    }

    #[tokio::test]
    async fn run_loop_calls_recovery_when_dangling_effects_present() {
        let run_dir = temp_run_dir("recovery-call");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-recovery-call";
        let _ = crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-recovery","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-recovery"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        // Activity id that the orchestrator expects for node "a" work.
        let work_activity_id = format!("{}::work::a", run_id);
        let work_attempt_id = format!("{}::work::a::att-1", run_id);

        // Write events that create a dangling effectAttempted activity matching
        // the orchestrator's work activity id.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": &work_activity_id,
                        "attemptId": &work_attempt_id,
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:aa",
                            "outputPath": "/tmp/aa",
                            "outputBytes": 2,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json"
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
                        "activityId": &work_activity_id,
                        "attemptId": &work_attempt_id,
                        "idempotencyKey": "wf_key_recovery",
                        "inputHash": "sha256:bb",
                        "idempotencyTtlMs": 60000u64,
                        "provider": "feishu-im",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-recovery".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([(
                    "a".to_string(),
                    WorkflowNode::HostExecutor(HostExecutorNode {
                        base: NodeBase {
                            description: None,
                            depends: None,
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: Some(true),
                        },
                        executor: "feishu-send".to_string(),
                        input: serde_json::json!({"chatId":"chat-1","content":"hello"}),
                    }),
                )]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        let recovery_calls = Arc::new(Mutex::new(0usize));
        let mut hooks = RecoveryCountingHooks {
            recovery_calls: recovery_calls.clone(),
        };
        let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();

        let calls = *recovery_calls.lock().await;
        assert!(
            calls > 0,
            "expected recover_dangling_effects to be called at least once, got {calls}"
        );
        // Since recovery returned had_progress=false, the loop should fall
        // through to run_tick and eventually stop (not loop forever on recovery).
        assert!(
            matches!(
                result.reason,
                RunLoopStopReason::NoProgress | RunLoopStopReason::AwaitingWait
            ),
            "expected NoProgress or AwaitingWait, got {:?}",
            result.reason
        );
        let _ = fs::remove_dir_all(&run_dir);
    }

    /// A hook that simulates successful recovery: writes activitySucceeded for
    /// every dangling effectAttempted activity.
    #[derive(Clone)]
    struct RecoveringHooks;

    #[async_trait]
    impl WorkflowExecutionHooks for RecoveringHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: Value::String(resolved_prompt),
                session: None,
            })
        }

        async fn execute_host_executor(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &HostExecutorNode,
            resolved_input: Value,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: resolved_input,
                session: None,
            })
        }

        async fn recover_dangling_effects(
            &mut self,
            log: &mut EventLog,
            snapshot: &RunSnapshotDTO,
        ) -> Result<RecoveryResult> {
            let mut had_progress = false;
            for activity_id in &snapshot.dangling.effect_attempted {
                if let Some(activity) = snapshot
                    .activities
                    .iter()
                    .find(|a| &a.activity_id == activity_id)
                {
                    if let Some(latest) = activity.attempts.last() {
                        let output_ref = WorkflowOutputRef {
                            output_hash: "sha256:recovered".to_string(),
                            output_path: "/tmp/recovered".to_string(),
                            output_bytes: 3,
                            output_schema_version: 1,
                            content_type: Some("application/json".to_string()),
                        };
                        let _ = log.append(EventDraft {
                            event_type: "activitySucceeded".to_string(),
                            actor: WorkflowActor::System,
                            payload: serde_json::json!({
                                "activityId": activity_id,
                                "attemptId": &latest.attempt_id,
                                "outputRef": output_ref,
                            }),
                            timestamp: None,
                            payload_hash: None,
                        })?;
                        had_progress = true;
                    }
                }
            }
            Ok(RecoveryResult {
                had_progress,
                has_remaining_dangling: false,
            })
        }
    }

    #[tokio::test]
    async fn run_loop_replays_after_recovery_writes_events() {
        let run_dir = temp_run_dir("recovery-replay");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-recovery-replay";
        let _ = crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-replay","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-replay"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        // Activity id that the orchestrator expects for node "a" work.
        let work_activity_id = format!("{}::work::a", run_id);
        let work_attempt_id = format!("{}::work::a::att-1", run_id);

        // Write events that create a dangling effectAttempted activity
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": &work_activity_id,
                        "attemptId": &work_attempt_id,
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:aa",
                            "outputPath": "/tmp/aa",
                            "outputBytes": 2,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json"
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
                        "activityId": &work_activity_id,
                        "attemptId": &work_attempt_id,
                        "idempotencyKey": "wf_key_replay",
                        "inputHash": "sha256:bb",
                        "idempotencyTtlMs": 60000u64,
                        "provider": "feishu-im",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-replay".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([(
                    "a".to_string(),
                    WorkflowNode::HostExecutor(HostExecutorNode {
                        base: NodeBase {
                            description: None,
                            depends: None,
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: Some(true),
                        },
                        executor: "feishu-send".to_string(),
                        input: serde_json::json!({"chatId":"chat-1","content":"hello"}),
                    }),
                )]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        let mut hooks = RecoveringHooks;
        let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();

        // After recovery writes activitySucceeded, the orchestrator should
        // be able to complete the node and the run.  Verify that the run
        // reached a terminal state or made measurable progress.
        let final_snapshot = read_snapshot(&rt).await.unwrap();
        assert!(
            matches!(
                final_snapshot.run.status,
                RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
            ) || result.ticks > 0,
            "expected recovery to allow progress; reason={:?} ticks={} status={:?}",
            result.reason,
            result.ticks,
            final_snapshot.run.status
        );
        // Verify the recovery event was written
        let events = rt.log.read_all().unwrap();
        let has_recovered = events
            .iter()
            .any(|e| e.event_type == "activitySucceeded" && e.actor == WorkflowActor::System);
        assert!(has_recovered, "expected a system activitySucceeded from recovery");
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn run_loop_no_infinite_loop_when_recovery_cannot_progress() {
        let run_dir = temp_run_dir("recovery-stuck");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-recovery-stuck";
        let _ = crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-stuck","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-stuck"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        // Activity id that the orchestrator expects for node "a" work.
        let work_activity_id = format!("{}::work::a", run_id);
        let work_attempt_id = format!("{}::work::a::att-1", run_id);

        // Write events that create a dangling effectAttempted activity
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": &work_activity_id,
                        "attemptId": &work_attempt_id,
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:aa",
                            "outputPath": "/tmp/aa",
                            "outputBytes": 2,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json"
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
                        "activityId": &work_activity_id,
                        "attemptId": &work_attempt_id,
                        "idempotencyKey": "wf_key_stuck",
                        "inputHash": "sha256:bb",
                        "idempotencyTtlMs": 60000u64,
                        "provider": "unknown-provider",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-stuck".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([(
                    "a".to_string(),
                    WorkflowNode::HostExecutor(HostExecutorNode {
                        base: NodeBase {
                            description: None,
                            depends: None,
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: Some(true),
                        },
                        executor: "feishu-send".to_string(),
                        input: serde_json::json!({"chatId":"chat-1","content":"hello"}),
                    }),
                )]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        // Use FakeHooks which returns had_progress=false for recovery.
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        // The loop must NOT return MaxTicks (that would mean it consumed all
        // ticks in unsuccessful recovery attempts).  With no real progress,
        // it should return NoProgress or AwaitingWait.
        assert!(
            !matches!(result.reason, RunLoopStopReason::MaxTicks),
            "expected run_loop to stop without exhausting max_ticks on unrecoverable dangling effects"
        );
        assert!(
            matches!(
                result.reason,
                RunLoopStopReason::NoProgress | RunLoopStopReason::AwaitingWait
            ),
            "expected NoProgress or AwaitingWait, got {:?}",
            result.reason
        );
        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn default_recovery_result_has_correct_semantics() {
        // Default implementation: had_progress=false always.
        // has_remaining_dangling should match whether the snapshot actually
        // contains dangling effect_attempted activities.

        // Snapshot with no dangling effects → has_remaining_dangling=false
        let empty_snapshot = RunSnapshotDTO {
            run_id: "r".to_string(),
            run: RunState {
                run_id: "r".to_string(),
                status: RunStatus::Running,
                workflow_id: None,
                revision_id: None,
                initiator: None,
                input: None,
                output: None,
                failed_node_id: None,
                root_cause_event_id: None,
                cancel_origin_event_id: None,
                bot_snapshots: None,
                cancelled_run_intent: None,
                cancelled_node_intents: BTreeMap::new(),
            },
            last_seq: 0,
            nodes: vec![],
            activities: vec![],
            loops: None,
            dangling: DanglingSnapshot {
                activities: vec![],
                effect_attempted: vec![],
                waits: vec![],
                cancels: vec![],
            },
            outputs: BTreeMap::new(),
            attempt_io: BTreeMap::new(),
            chat_binding: None,
            updated_at: 0,
        };

        // Need a real EventLog for the &mut reference, but FakeHooks doesn't use it.
        let run_dir = temp_run_dir("recovery-semantics");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let mut log = EventLog::new("r", paths.workflow_runs_dir()).unwrap();

        // Use the trait method via a concrete type to test default impl.
        // We call it through a concrete struct that doesn't override the default.
        #[derive(Clone)]
        struct DefaultingHooks;
        #[async_trait]
        impl WorkflowExecutionHooks for DefaultingHooks {
            async fn execute_subagent(
                &mut self,
                _ctx: WorkflowDispatchRun<'_>,
                _node: &SubagentNode,
                _resolved_prompt: String,
            ) -> Result<WorkflowDispatchOutcome> {
                unreachable!()
            }
            async fn execute_host_executor(
                &mut self,
                _ctx: WorkflowDispatchRun<'_>,
                _node: &HostExecutorNode,
                _resolved_input: Value,
            ) -> Result<WorkflowDispatchOutcome> {
                unreachable!()
            }
            // Uses default recover_dangling_effects
        }

        let mut hooks = DefaultingHooks;

        // Empty: has_remaining_dangling should be false
        let result = hooks.recover_dangling_effects(&mut log, &empty_snapshot).await.unwrap();
        assert!(!result.had_progress, "default: had_progress must be false");
        assert!(!result.has_remaining_dangling, "empty dangling → has_remaining_dangling must be false");

        // With dangling effects: has_remaining_dangling should be true
        let dangling_snapshot = RunSnapshotDTO {
            dangling: DanglingSnapshot {
                effect_attempted: vec!["act-1".to_string()],
                ..empty_snapshot.dangling.clone()
            },
            ..empty_snapshot.clone()
        };
        let result2 = hooks.recover_dangling_effects(&mut log, &dangling_snapshot).await.unwrap();
        assert!(!result2.had_progress, "default: had_progress must be false");
        assert!(result2.has_remaining_dangling, "dangling present → has_remaining_dangling must be true");

        let _ = fs::remove_dir_all(&run_dir);
    }
}
