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

    /// Called after `check_pending_cancels` has written cancel events for the
    /// given activities and nodes.  The hook receives the lists of activity IDs
    /// that were just cancelled, allowing daemon-level cancellation registries
    /// to cancel active dispatch tokens.
    ///
    /// Default implementation is a no-op.
    async fn on_activities_cancelled(
        &mut self,
        _activity_ids: &[String],
        _node_ids: &[String],
        _run_id: &str,
    ) {
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

        check_pending_cancels(rt, hooks).await?;

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

        if !pre_recovery_snapshot.dangling.effect_attempted.is_empty() {
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

        // --- Wait resolution recovery: materialise terminal events for
        //     activities where waitResolved / waitDeadlineExceeded was written
        //     but the terminal (activitySucceeded / activityFailed) is missing.
        //     This is deterministic (no external calls needed), so it runs
        //     inline in run_loop rather than through the hooks. ---
        if !pre_recovery_snapshot.dangling.wait_resolutions.is_empty() {
            let had_progress = resolve_wait_terminals(rt, &pre_recovery_snapshot).await?;
            if had_progress {
                continue;
            }
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

async fn check_pending_cancels<H: WorkflowExecutionHooks + Send>(
    rt: &mut WorkflowRuntimeContext,
    hooks: &mut H,
) -> Result<()> {
    let _events = rt.log.read_all()?;
    let snapshot = read_snapshot(rt).await?;
    let mut cancelled_activities: Vec<String> = Vec::new();
    let mut cancelled_nodes: Vec<String> = Vec::new();

    for activity_id in &snapshot.dangling.cancels {
        cancelled_activities.push(activity_id.clone());
        let attempt_id = snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
            .and_then(|a| a.current_attempt_id.clone())
            .unwrap_or_else(|| format!("{}-attempt-1", activity_id));
        let origin = snapshot
            .run
            .cancelled_run_intent
            .as_ref()
            .map(|i| i.cancel_origin_event_id.clone())
            .or_else(|| {
                snapshot
                    .run
                    .cancelled_node_intents
                    .values()
                    .next()
                    .map(|i| i.cancel_origin_event_id.clone())
            })
            .unwrap_or_default();
        let _ = crate::complete_activity_cancel(
            &mut rt.log,
            crate::CompleteActivityCancelInput {
                activity_id: activity_id.clone(),
                attempt_id,
                cancel_origin_event_id: origin,
            },
            WorkflowActor::Scheduler,
        )
        .await;
    }

    if let Some(ref intent) = snapshot.run.cancelled_run_intent {
        if snapshot.run.status != RunStatus::Cancelled {
            let _ = crate::complete_run_cancel(
                &mut rt.log,
                crate::CompleteRunCancelInput {
                    cancel_origin_event_id: intent.cancel_origin_event_id.clone(),
                },
                WorkflowActor::Scheduler,
            )
            .await;
        }
    }

    if !snapshot.run.cancelled_node_intents.is_empty() {
        for (node_id, intent) in &snapshot.run.cancelled_node_intents {
            cancelled_nodes.push(node_id.clone());
            let _ = crate::complete_node_cancel(
                &mut rt.log,
                crate::CompleteNodeCancelInput {
                    node_id: node_id.clone(),
                    cancel_origin_event_id: intent.cancel_origin_event_id.clone(),
                },
                WorkflowActor::Scheduler,
            )
            .await;
        }
    }

    // Notify hooks so daemon can cancel active dispatch tokens.
    if !cancelled_activities.is_empty() || !cancelled_nodes.is_empty() {
        hooks
            .on_activities_cancelled(&cancelled_activities, &cancelled_nodes, &rt.log.run_id)
            .await;
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
    let mut dispatch_count: usize = 0;
    for action in actions {
        let serialization_key = action_serialization_key(def, &action);
        if seen.insert(serialization_key.clone()) {
            let is_dispatch = action.is_dispatch();
            // Settle actions (FinishLoop, CompleteNodeSucceeded, etc.)
            // don't count against the concurrency limit — they are fast
            // and may need to be dispatched in pairs (e.g. FinishLoopIteration
            // + FinishLoop for the same loop node).
            if !is_dispatch || dispatch_count < limit {
                if is_dispatch {
                    dispatch_count += 1;
                }
                selected.push(ScheduledAction { action });
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
        OrchestratorAction::StartLoop { node_id, .. } => {
            format!("loop:start:{node_id}")
        }
        OrchestratorAction::StartLoopIteration { node_id, .. } => {
            format!("loop:iter-start:{node_id}")
        }
        OrchestratorAction::FinishLoopIteration { node_id, .. } => {
            format!("loop:iter-finish:{node_id}")
        }
        OrchestratorAction::FinishLoop { node_id, .. } => {
            format!("loop:finish:{node_id}")
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
        OrchestratorAction::StartLoop { .. } => start_loop(&mut rt.log, &action).await?,
        OrchestratorAction::StartLoopIteration { .. } => {
            start_loop_iteration(&mut rt.log, &action).await?
        }
        OrchestratorAction::FinishLoopIteration { .. } => {
            finish_loop_iteration(&mut rt.log, &action).await?
        }
        OrchestratorAction::FinishLoop { .. } => finish_loop(&mut rt.log, &action).await?,
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

/// Materialise terminal events (activitySucceeded / activityFailed) for
/// activities that have a recorded wait resolution (waitResolved /
/// waitDeadlineExceeded) but are missing the terminal activity event.
///
/// This handles the dangling wait resolution case where the daemon crashed
/// after writing the wait resolution but before writing the terminal event.
/// Returns true if at least one terminal event was written.
async fn resolve_wait_terminals(
    rt: &mut WorkflowRuntimeContext,
    snapshot: &RunSnapshotDTO,
) -> Result<bool> {
    let mut had_progress = false;
    for activity_id in &snapshot.dangling.wait_resolutions {
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
        let Some(wait) = latest.wait.as_ref() else {
            continue;
        };
        let Some(resolution) = wait.resolution.as_ref() else {
            continue;
        };
        let attempt_id = &latest.attempt_id;

        match resolution.kind.as_str() {
            "resolved" => {
                if matches!(resolution.resolution.as_deref(), Some("rejected")) {
                    // reject → activityFailed (non-decision nodes)
                    rt.log.append(EventDraft {
                        event_type: "activityFailed".to_string(),
                        actor: WorkflowActor::Scheduler,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": attempt_id,
                            "error": {
                                "errorCode": "InputValidationFailed",
                                "errorClass": "userFault",
                                "errorMessage": format!(
                                    "Recovered wait terminal: rejected by {}{}",
                                    resolution.by.clone().unwrap_or_default(),
                                    resolution.comment.as_ref()
                                        .map(|c| format!(": {}", c))
                                        .unwrap_or_default()
                                ),
                            }
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    had_progress = true;
                } else {
                    // approved / external → activitySucceeded
                    let external_refs = serde_json::json!({
                        "resolution": resolution.resolution,
                        "by": resolution.by,
                        "comment": resolution.comment,
                    });
                    let output_ref = write_json_blob(&mut rt.log, external_refs.clone())?;
                    rt.log.append(EventDraft {
                        event_type: "activitySucceeded".to_string(),
                        actor: WorkflowActor::Scheduler,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": attempt_id,
                            "outputRef": output_ref,
                            "externalRefs": external_refs,
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    had_progress = true;
                }
            }
            "deadlineExceeded" => {
                if matches!(wait.on_timeout.as_deref(), Some("success")) {
                    let external_refs = serde_json::json!({
                        "defaultedToTimeout": true,
                        "deadlineAt": resolution.deadline_at,
                    });
                    let output_ref = write_json_blob(&mut rt.log, external_refs.clone())?;
                    rt.log.append(EventDraft {
                        event_type: "activitySucceeded".to_string(),
                        actor: WorkflowActor::Scheduler,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": attempt_id,
                            "outputRef": output_ref,
                            "externalRefs": external_refs,
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    had_progress = true;
                } else {
                    // fail (default) → activityFailed
                    rt.log.append(EventDraft {
                        event_type: "activityFailed".to_string(),
                        actor: WorkflowActor::Scheduler,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": attempt_id,
                            "error": {
                                "errorCode": "WaitDeadlineExceeded",
                                "errorClass": "userFault",
                                "errorMessage": "Recovered wait terminal: deadline exceeded",
                            }
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    had_progress = true;
                }
            }
            _ => {}
        }
    }
    Ok(had_progress)
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
    use crate::LoopIterationStatus;
    use crate::LoopStatus;
    use crate::RunChatBinding;
    use crate::RunState;
    use crate::workflow_definition::NodeBase;
    use crate::workflow_definition::{
        DecisionNode, HumanGate, LoopNode, LoopOutputProjection, LoopTerminate,
    };
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
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
        let run_id = "run-1";
        let bootstrap = crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello ${params.name}"}}}"#,
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
        let params: BTreeMap<String, Value> = BTreeMap::new();
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
        let params: BTreeMap<String, Value> = BTreeMap::new();
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
        let params: BTreeMap<String, Value> = BTreeMap::new();
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
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
        let run_id = "run-1";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello ${params.name}"}}}"#,
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
        assert!(
            has_recovered,
            "expected a system activitySucceeded from recovery"
        );
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
                wait_resolutions: vec![],
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
        let result = hooks
            .recover_dangling_effects(&mut log, &empty_snapshot)
            .await
            .unwrap();
        assert!(!result.had_progress, "default: had_progress must be false");
        assert!(
            !result.has_remaining_dangling,
            "empty dangling → has_remaining_dangling must be false"
        );

        // With dangling effects: has_remaining_dangling should be true
        let dangling_snapshot = RunSnapshotDTO {
            dangling: DanglingSnapshot {
                effect_attempted: vec!["act-1".to_string()],
                ..empty_snapshot.dangling.clone()
            },
            ..empty_snapshot.clone()
        };
        let result2 = hooks
            .recover_dangling_effects(&mut log, &dangling_snapshot)
            .await
            .unwrap();
        assert!(!result2.had_progress, "default: had_progress must be false");
        assert!(
            result2.has_remaining_dangling,
            "dangling present → has_remaining_dangling must be true"
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    // --- Wait resolution projection tests (Task 4.2) ---

    /// Verify that an open wait (waitCreated without resolution) causes
    /// run_loop to return AwaitingWait rather than NoProgress.
    #[tokio::test]
    async fn open_wait_makes_run_loop_return_awaiting_wait() {
        let run_dir = temp_run_dir("open-wait");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-open-wait";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-open-wait","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-schedule","input":{"prompt":"approve"},"humanGate":{"stage":"gate","prompt":"approve?","approvers":["admin"]}},"sink":{"type":"subagent","bot":"bot-a","prompt":"done","depends":["gate"]}}}"#,
                expected_workflow_id: Some("flow-open-wait"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Write a waitCreated event directly — this simulates a workflow that
        // dispatched a gate and created a wait but no resolution has arrived yet.
        let gate_activity_id = format!("{}::gate::gate", run_id);
        let gate_attempt_id = format!("{}::gate::gate::att-1", run_id);
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "gate",
                        "activityId": &gate_activity_id,
                        "attemptId": &gate_attempt_id,
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
                    event_type: "waitCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "activityId": &gate_activity_id,
                        "attemptId": &gate_attempt_id,
                        "nodeId": "gate",
                        "waitKind": "human-gate",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-open-wait".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "gate".to_string(),
                        WorkflowNode::HostExecutor(HostExecutorNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: Some(crate::workflow_definition::HumanGate {
                                    stage: "gate".to_string(),
                                    prompt: Value::String("approve?".to_string()),
                                    approvers: Some(vec!["admin".to_string()]),
                                    deadline_ms: None,
                                    on_timeout: None,
                                }),
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: Some(true),
                            },
                            executor: "beam-schedule".to_string(),
                            input: serde_json::json!({"prompt":"approve"}),
                        }),
                    ),
                    (
                        "sink".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: Some(vec!["gate".to_string()]),
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("done".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        // Verify snapshot shows open wait (in waits, not in wait_resolutions)
        let snap = read_snapshot(&rt).await.unwrap();
        assert!(
            !snap.dangling.waits.is_empty(),
            "expected open wait in dangling.waits, got {:?}",
            snap.dangling.waits
        );
        assert!(
            snap.dangling.wait_resolutions.is_empty(),
            "expected no wait_resolutions for open wait"
        );

        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
        assert_eq!(
            result.reason,
            RunLoopStopReason::AwaitingWait,
            "expected AwaitingWait for open wait, got {:?}",
            result.reason
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    /// Verify that a resolved wait without a terminal event (dangling
    /// wait_resolution) gets materialized during run_loop's recovery phase,
    /// allowing the workflow to continue.
    #[tokio::test]
    async fn run_loop_materializes_terminal_for_resolved_wait() {
        let run_dir = temp_run_dir("resolved-wait");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-resolved-wait";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-resolved-wait","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-schedule","input":{"prompt":"approve"},"humanGate":{"stage":"gate","prompt":"approve?","approvers":["admin"]}},"sink":{"type":"subagent","bot":"bot-a","prompt":"done","depends":["gate"]}}}"#,
                expected_workflow_id: Some("flow-resolved-wait"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Write attemptCreated + waitCreated + waitResolved (approved) but NO terminal.
        // This simulates a crash after the wait was resolved but before the terminal
        // event was written.
        let gate_activity_id = format!("{}::gate::gate", run_id);
        let gate_attempt_id = format!("{}::gate::gate::att-1", run_id);
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "gate",
                        "activityId": &gate_activity_id,
                        "attemptId": &gate_attempt_id,
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
                    event_type: "waitCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "activityId": &gate_activity_id,
                        "attemptId": &gate_attempt_id,
                        "nodeId": "gate",
                        "waitKind": "human-gate",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &gate_activity_id,
                        "resolution": "approved",
                        "by": "admin",
                        "comment": "go ahead",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-resolved-wait".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::from([
                    (
                        "gate".to_string(),
                        WorkflowNode::HostExecutor(HostExecutorNode {
                            base: NodeBase {
                                description: None,
                                depends: None,
                                human_gate: Some(crate::workflow_definition::HumanGate {
                                    stage: "gate".to_string(),
                                    prompt: Value::String("approve?".to_string()),
                                    approvers: Some(vec!["admin".to_string()]),
                                    deadline_ms: None,
                                    on_timeout: None,
                                }),
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: Some(true),
                            },
                            executor: "beam-schedule".to_string(),
                            input: serde_json::json!({"prompt":"approve"}),
                        }),
                    ),
                    (
                        "sink".to_string(),
                        WorkflowNode::Subagent(SubagentNode {
                            base: NodeBase {
                                description: None,
                                depends: Some(vec!["gate".to_string()]),
                                human_gate: None,
                                retry_policy: None,
                                timeout_ms: None,
                                max_output_bytes: None,
                                output_schema: None,
                                unsafe_allow_ungated: None,
                            },
                            bot: "bot-a".to_string(),
                            prompt: Value::String("done".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    ),
                ]),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };

        // Verify snapshot shows resolved wait in wait_resolutions, NOT in waits
        let snap = read_snapshot(&rt).await.unwrap();
        assert!(
            snap.dangling.waits.is_empty(),
            "expected no open waits after resolution, got {:?}",
            snap.dangling.waits
        );
        assert!(
            !snap.dangling.wait_resolutions.is_empty(),
            "expected resolved wait in wait_resolutions, got {:?}",
            snap.dangling.wait_resolutions
        );
        assert_eq!(
            snap.dangling.wait_resolutions,
            vec![gate_activity_id.clone()],
            "expected gate activity in wait_resolutions"
        );

        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        // run_loop should have materialized the terminal event (activitySucceeded
        // for approved wait), allowing the orchestrator to proceed past the gate.
        let final_snap = read_snapshot(&rt).await.unwrap();
        assert!(
            final_snap.dangling.wait_resolutions.is_empty(),
            "expected no remaining wait_resolutions after recovery, got {:?}",
            final_snap.dangling.wait_resolutions
        );

        let events = rt.log.read_all().unwrap();
        let recovered_terminal = events.iter().any(|e| {
            e.event_type == "activitySucceeded"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&gate_activity_id)
                && e.actor == WorkflowActor::Scheduler
        });
        assert!(
            recovered_terminal,
            "expected a scheduler activitySucceeded for the gate after wait resolution recovery"
        );

        // The loop should either have progressed (ticks > 0) or stopped at terminal.
        assert!(
            result.ticks > 0 || matches!(result.reason, RunLoopStopReason::Terminal),
            "expected progress after wait resolution recovery; reason={:?} ticks={}",
            result.reason,
            result.ticks
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    /// Verify that cancelRequested (run) propagation writes activityCanceled
    /// for an open human-gate activity before writing runCanceled.
    #[tokio::test]
    async fn cancel_propagation_writes_activity_canceled_before_run_canceled() {
        let run_dir = temp_run_dir("cancel-propagate");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-cancel-propagate";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-cancel-propagate","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-schedule","input":{"prompt":"approve"},"humanGate":{"stage":"gate","prompt":"approve?","approvers":["admin"]}},"sink":{"type":"subagent","bot":"bot-a","prompt":"done","depends":["gate"]}}}"#,
                expected_workflow_id: Some("flow-cancel-propagate"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // First run_loop tick: dispatches the human-gate (creates a wait).
        let gate_activity_id = format!("{}::gate::gate", run_id);
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: WorkflowDefinition {
                    workflow_id: "flow-cancel-propagate".to_string(),
                    version: 1,
                    params: None,
                    defaults: None,
                    nodes: BTreeMap::from([
                        (
                            "gate".to_string(),
                            WorkflowNode::HostExecutor(HostExecutorNode {
                                base: NodeBase {
                                    description: None,
                                    depends: None,
                                    human_gate: Some(crate::workflow_definition::HumanGate {
                                        stage: "gate".to_string(),
                                        prompt: Value::String("approve?".to_string()),
                                        approvers: Some(vec!["admin".to_string()]),
                                        deadline_ms: None,
                                        on_timeout: None,
                                    }),
                                    retry_policy: None,
                                    timeout_ms: None,
                                    max_output_bytes: None,
                                    output_schema: None,
                                    unsafe_allow_ungated: Some(true),
                                },
                                executor: "beam-schedule".to_string(),
                                input: serde_json::json!({"prompt":"approve"}),
                            }),
                        ),
                        (
                            "sink".to_string(),
                            WorkflowNode::Subagent(SubagentNode {
                                base: NodeBase {
                                    description: None,
                                    depends: Some(vec!["gate".to_string()]),
                                    human_gate: None,
                                    retry_policy: None,
                                    timeout_ms: None,
                                    max_output_bytes: None,
                                    output_schema: None,
                                    unsafe_allow_ungated: None,
                                },
                                bot: "bot-a".to_string(),
                                prompt: Value::String("done".to_string()),
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
            let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }

        // Write cancelRequested (run).
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = crate::request_cancel(
                &mut log,
                crate::RequestCancelInput {
                    target: serde_json::json!({
                        "kind": "run",
                        "runId": run_id,
                    }),
                    reason: "test cancel".to_string(),
                    by: "tester".to_string(),
                },
                WorkflowActor::Human,
            )
            .await
            .unwrap();
        }

        // Second run_loop: should propagate cancel (activityCanceled → runCanceled).
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: WorkflowDefinition {
                    workflow_id: "flow-cancel-propagate".to_string(),
                    version: 1,
                    params: None,
                    defaults: None,
                    nodes: BTreeMap::from([
                        (
                            "gate".to_string(),
                            WorkflowNode::HostExecutor(HostExecutorNode {
                                base: NodeBase {
                                    description: None,
                                    depends: None,
                                    human_gate: Some(crate::workflow_definition::HumanGate {
                                        stage: "gate".to_string(),
                                        prompt: Value::String("approve?".to_string()),
                                        approvers: Some(vec!["admin".to_string()]),
                                        deadline_ms: None,
                                        on_timeout: None,
                                    }),
                                    retry_policy: None,
                                    timeout_ms: None,
                                    max_output_bytes: None,
                                    output_schema: None,
                                    unsafe_allow_ungated: Some(true),
                                },
                                executor: "beam-schedule".to_string(),
                                input: serde_json::json!({"prompt":"approve"}),
                            }),
                        ),
                        (
                            "sink".to_string(),
                            WorkflowNode::Subagent(SubagentNode {
                                base: NodeBase {
                                    description: None,
                                    depends: Some(vec!["gate".to_string()]),
                                    human_gate: None,
                                    retry_policy: None,
                                    timeout_ms: None,
                                    max_output_bytes: None,
                                    output_schema: None,
                                    unsafe_allow_ungated: None,
                                },
                                bot: "bot-a".to_string(),
                                prompt: Value::String("done".to_string()),
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
            let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::Terminal);
        }

        // Verify event order: activityCanceled appears before runCanceled.
        let log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let events = log.read_all().unwrap();
        let has_cancel_requested = events.iter().any(|e| e.event_type == "cancelRequested");
        assert!(has_cancel_requested, "should have cancelRequested");

        // Find positions of activityCanceled (for the gate) and runCanceled.
        let mut activity_canceled_pos: Option<usize> = None;
        let mut run_canceled_pos: Option<usize> = None;
        for (i, e) in events.iter().enumerate() {
            if e.event_type == "activityCanceled"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&gate_activity_id)
            {
                activity_canceled_pos = Some(i);
            }
            if e.event_type == "runCanceled" {
                run_canceled_pos = Some(i);
            }
        }

        assert!(
            activity_canceled_pos.is_some(),
            "should have activityCanceled for the gate activity"
        );
        assert!(run_canceled_pos.is_some(), "should have runCanceled");
        assert!(
            activity_canceled_pos.unwrap() < run_canceled_pos.unwrap(),
            "activityCanceled ({}) must appear before runCanceled ({})",
            activity_canceled_pos.unwrap(),
            run_canceled_pos.unwrap()
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    /// Verify that cancel propagation is idempotent: running check_pending_cancels
    /// again on an already-cancelled run does not write duplicate events.
    #[tokio::test]
    async fn cancel_propagation_is_idempotent_after_run_is_cancelled() {
        let run_dir = temp_run_dir("cancel-idempotent");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-cancel-idempotent";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-cancel-idem","version":1,"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello"}}}"#,
                expected_workflow_id: Some("flow-cancel-idem"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Write cancelRequested before any dispatch.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = crate::request_cancel(
                &mut log,
                crate::RequestCancelInput {
                    target: serde_json::json!({
                        "kind": "run",
                        "runId": run_id,
                    }),
                    reason: "test cancel".to_string(),
                    by: "tester".to_string(),
                },
                WorkflowActor::Human,
            )
            .await
            .unwrap();
        }

        // First run_loop: should write runCanceled.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: WorkflowDefinition {
                    workflow_id: "flow-cancel-idem".to_string(),
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
                            prompt: Value::String("hello".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    )]),
                },
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::Terminal);
        }

        // Second run_loop: should be idempotent (no duplicate runCanceled).
        let run_canceled_count_before: usize;
        {
            let log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let events = log.read_all().unwrap();
            run_canceled_count_before = events
                .iter()
                .filter(|e| e.event_type == "runCanceled")
                .count();
            assert_eq!(
                run_canceled_count_before, 1,
                "should have exactly 1 runCanceled after first propagation"
            );
        }

        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: WorkflowDefinition {
                    workflow_id: "flow-cancel-idem".to_string(),
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
                            prompt: Value::String("hello".to_string()),
                            working_dir: None,
                            model_overrides: None,
                            tool_policy: None,
                        }),
                    )]),
                },
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _ = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
        }

        let log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let events = log.read_all().unwrap();
        let run_canceled_count_after = events
            .iter()
            .filter(|e| e.event_type == "runCanceled")
            .count();
        assert_eq!(
            run_canceled_count_after, 1,
            "second run_loop should not produce duplicate runCanceled"
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    // --- Loop lifecycle action/event tests (Task 8.1) ---

    /// Returns a minimal workflow JSON with a single subagent node.
    fn min_workflow_json(workflow_id: &str, node_id: &str) -> String {
        format!(
            r#"{{"workflowId":"{workflow_id}","version":1,"nodes":{{"{node_id}":{{"type":"subagent","bot":"bot-x","prompt":"ok"}}}}}}"#
        )
    }

    #[tokio::test]
    async fn start_loop_writes_loop_started_event() {
        let run_dir = temp_run_dir("loop-start");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-start";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-start", "a"),
                expected_workflow_id: Some("flow-loop-start"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let action = OrchestratorAction::StartLoop {
            node_id: "loop-1".to_string(),
            max_iterations: 5,
        };
        start_loop(&mut log, &action).await.unwrap();

        let events = log.read_all().unwrap();
        let loop_events: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "loopStarted")
            .collect();
        assert_eq!(
            loop_events.len(),
            1,
            "expected exactly one loopStarted event"
        );
        let ev = loop_events[0];
        assert_eq!(ev.payload["loopId"], "loop-1");
        assert_eq!(ev.payload["maxIterations"], 5);
        assert_eq!(ev.actor, WorkflowActor::Scheduler);

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn start_loop_iteration_writes_event() {
        let run_dir = temp_run_dir("loop-iter-start");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-iter-start";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-iter-start", "a"),
                expected_workflow_id: Some("flow-loop-iter-start"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let action = OrchestratorAction::StartLoopIteration {
            node_id: "loop-1".to_string(),
            iteration: 2,
        };
        start_loop_iteration(&mut log, &action).await.unwrap();

        let events = log.read_all().unwrap();
        let loop_events: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "loopIterationStarted")
            .collect();
        assert_eq!(
            loop_events.len(),
            1,
            "expected exactly one loopIterationStarted event"
        );
        let ev = loop_events[0];
        assert_eq!(ev.payload["loopId"], "loop-1");
        assert_eq!(ev.payload["iteration"], 2);
        assert_eq!(ev.actor, WorkflowActor::Scheduler);

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn finish_loop_iteration_writes_event() {
        let run_dir = temp_run_dir("loop-iter-finish");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-iter-finish";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-iter-finish", "a"),
                expected_workflow_id: Some("flow-loop-iter-finish"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let action = OrchestratorAction::FinishLoopIteration {
            node_id: "loop-1".to_string(),
            iteration: 3,
            resolution: "approved".to_string(),
            decision_activity_id: Some("run-loop-iter-finish::gate::loop-1".to_string()),
            wait_resolved_event_id: None,
            by: Some("tester".to_string()),
            comment: Some("looks good".to_string()),
            timed_out: Some(false),
        };
        finish_loop_iteration(&mut log, &action).await.unwrap();

        let events = log.read_all().unwrap();
        let loop_events: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "loopIterationFinished")
            .collect();
        assert_eq!(
            loop_events.len(),
            1,
            "expected exactly one loopIterationFinished event"
        );
        let ev = loop_events[0];
        assert_eq!(ev.payload["loopId"], "loop-1");
        assert_eq!(ev.payload["iteration"], 3);
        assert_eq!(ev.payload["resolution"], "approved");
        assert_eq!(ev.payload["by"], "tester");
        assert_eq!(ev.payload["comment"], "looks good");
        assert_eq!(ev.payload["timedOut"], false);
        assert_eq!(ev.actor, WorkflowActor::Scheduler);

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn finish_loop_writes_loop_finished_event() {
        let run_dir = temp_run_dir("loop-finish");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-finish";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-finish", "a"),
                expected_workflow_id: Some("flow-loop-finish"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let output_ref = WorkflowOutputRef {
            output_hash: "sha256:abc".to_string(),
            output_path: "/tmp/loop-out.json".to_string(),
            output_bytes: 10,
            output_schema_version: 1,
            content_type: Some("application/json".to_string()),
        };
        let action = OrchestratorAction::FinishLoop {
            node_id: "loop-1".to_string(),
            final_iteration: 3,
            resolution: "approved".to_string(),
            output_ref: Some(output_ref.clone()),
            error_code: None,
            error_class: None,
        };
        finish_loop(&mut log, &action).await.unwrap();

        let events = log.read_all().unwrap();
        let loop_events: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "loopFinished")
            .collect();
        assert_eq!(
            loop_events.len(),
            1,
            "expected exactly one loopFinished event"
        );
        let ev = loop_events[0];
        assert_eq!(ev.payload["loopId"], "loop-1");
        assert_eq!(ev.payload["finalIteration"], 3);
        assert_eq!(ev.payload["resolution"], "approved");
        assert_eq!(ev.actor, WorkflowActor::Scheduler);

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn all_loop_actions_produce_correct_event_sequence() {
        // Write a full loop lifecycle and verify event order and types.
        let run_dir = temp_run_dir("loop-sequence");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-seq";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-seq", "a"),
                expected_workflow_id: Some("flow-loop-seq"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();

        // Start loop
        start_loop(
            &mut log,
            &OrchestratorAction::StartLoop {
                node_id: "loop-1".to_string(),
                max_iterations: 3,
            },
        )
        .await
        .unwrap();

        // Iteration 1
        start_loop_iteration(
            &mut log,
            &OrchestratorAction::StartLoopIteration {
                node_id: "loop-1".to_string(),
                iteration: 1,
            },
        )
        .await
        .unwrap();
        finish_loop_iteration(
            &mut log,
            &OrchestratorAction::FinishLoopIteration {
                node_id: "loop-1".to_string(),
                iteration: 1,
                resolution: "approved".to_string(),
                decision_activity_id: None,
                wait_resolved_event_id: None,
                by: None,
                comment: None,
                timed_out: None,
            },
        )
        .await
        .unwrap();

        // Iteration 2 (rejected)
        start_loop_iteration(
            &mut log,
            &OrchestratorAction::StartLoopIteration {
                node_id: "loop-1".to_string(),
                iteration: 2,
            },
        )
        .await
        .unwrap();
        finish_loop_iteration(
            &mut log,
            &OrchestratorAction::FinishLoopIteration {
                node_id: "loop-1".to_string(),
                iteration: 2,
                resolution: "rejected".to_string(),
                decision_activity_id: Some(format!("{}::gate::loop-1", run_id)),
                wait_resolved_event_id: None,
                by: Some("reviewer".to_string()),
                comment: Some("needs work".to_string()),
                timed_out: Some(false),
            },
        )
        .await
        .unwrap();

        // Finish loop (overall approved after 2 iterations)
        let output_ref = WorkflowOutputRef {
            output_hash: "sha256:xyz".to_string(),
            output_path: "/tmp/xyz.json".to_string(),
            output_bytes: 8,
            output_schema_version: 1,
            content_type: Some("application/json".to_string()),
        };
        finish_loop(
            &mut log,
            &OrchestratorAction::FinishLoop {
                node_id: "loop-1".to_string(),
                final_iteration: 2,
                resolution: "approved".to_string(),
                output_ref: Some(output_ref.clone()),
                error_code: None,
                error_class: None,
            },
        )
        .await
        .unwrap();

        let events = log.read_all().unwrap();
        let loop_event_types: Vec<_> = events
            .iter()
            .filter(|e| e.event_type.starts_with("loop"))
            .map(|e| e.event_type.as_str())
            .collect();
        assert_eq!(
            loop_event_types,
            vec![
                "loopStarted",
                "loopIterationStarted",
                "loopIterationFinished",
                "loopIterationStarted",
                "loopIterationFinished",
                "loopFinished",
            ],
            "expected full loop lifecycle events in order"
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn replay_builds_snapshot_loops_from_loop_events() {
        // Write loop events directly to the events file, then replay and check
        // snapshot.loops.
        let run_dir = temp_run_dir("loop-replay");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-replay";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-replay", "a"),
                expected_workflow_id: Some("flow-loop-replay"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        // The bootstrap already wrote runCreated + runStarted. Now append loop events directly.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopStarted".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "maxIterations": 3,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopIterationStarted".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "iteration": 1,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopIterationFinished".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "iteration": 1,
                        "resolution": "approved",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopFinished".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "finalIteration": 1,
                        "resolution": "approved",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Read snapshot and verify loops
        let rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-loop-replay".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::new(),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let snapshot = read_snapshot(&rt).await.unwrap();

        let loops = snapshot
            .loops
            .as_ref()
            .expect("snapshot.loops should be Some after loop events");
        assert!(
            loops.contains_key("loop-1"),
            "loop-1 should be in snapshot.loops"
        );

        let loop_state = loops.get("loop-1").unwrap();
        assert_eq!(loop_state.loop_id, "loop-1");
        assert_eq!(
            loop_state.status,
            LoopStatus::Succeeded,
            "loop should be Succeeded (approved)"
        );
        assert_eq!(loop_state.max_iterations, 3);
        assert_eq!(loop_state.iteration, 1, "finalIteration should be 1");
        assert_eq!(
            loop_state.iterations.len(),
            1,
            "should have 1 iteration recorded"
        );

        let iteration = &loop_state.iterations[0];
        assert_eq!(iteration.iteration, 1);
        assert_eq!(iteration.status, LoopIterationStatus::Approved);

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn replay_loop_failed_sets_status_and_dangling_iteration() {
        // Simulate a loop that fails midway: loopStarted → iter1 start/finish
        // (approved) → iter2 started → loopFinished with resolution=failed.
        // The inflight iteration should be marked Failed.
        let run_dir = temp_run_dir("loop-failed-replay");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-loop-failed-replay";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &min_workflow_json("flow-loop-failed-replay", "a"),
                expected_workflow_id: Some("flow-loop-failed-replay"),
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
                    event_type: "loopStarted".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "maxIterations": 5,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopIterationStarted".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "iteration": 1,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopIterationFinished".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "iteration": 1,
                        "resolution": "approved",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "loopIterationStarted".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "iteration": 2,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            // Loop finishes with failure while iteration 2 is still running
            let _ = log
                .append(EventDraft {
                    event_type: "loopFinished".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "loopId": "loop-1",
                        "finalIteration": 2,
                        "resolution": "failed",
                        "errorCode": "LoopFailedMidIteration",
                        "errorClass": "fatal",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: WorkflowDefinition {
                workflow_id: "flow-loop-failed-replay".to_string(),
                version: 1,
                params: None,
                defaults: None,
                nodes: BTreeMap::new(),
            },
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let snapshot = read_snapshot(&rt).await.unwrap();

        let loops = snapshot
            .loops
            .as_ref()
            .expect("snapshot.loops should be Some after loop events");
        let loop_state = loops.get("loop-1").unwrap();
        assert_eq!(
            loop_state.status,
            LoopStatus::Failed,
            "loop should be Failed"
        );
        assert_eq!(loop_state.error_class.as_deref(), Some("fatal"));
        assert_eq!(
            loop_state.error_code.as_deref(),
            Some("LoopFailedMidIteration")
        );
        assert_eq!(loop_state.iteration, 2);
        assert_eq!(loop_state.iterations.len(), 2);

        // Iteration 1 should be Approved
        let iter1 = loop_state
            .iterations
            .iter()
            .find(|it| it.iteration == 1)
            .unwrap();
        assert_eq!(iter1.status, LoopIterationStatus::Approved);

        // Iteration 2 should be Failed (inflight → Failed on loop finish failure)
        let iter2 = loop_state
            .iterations
            .iter()
            .find(|it| it.iteration == 2)
            .unwrap();
        assert_eq!(
            iter2.status,
            LoopIterationStatus::Failed,
            "inflight iteration should be Failed"
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    // ── Task 8.2: loop dispatch pass tests ──

    /// Build a minimal code-review-loop WorkflowDefinition for testing.
    fn code_review_loop_def() -> WorkflowDefinition {
        WorkflowDefinition {
            workflow_id: "code-review-loop".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([
                (
                    "implement".to_string(),
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
                        prompt: Value::String("implement".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
                (
                    "review".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
                        base: NodeBase {
                            description: None,
                            depends: Some(vec!["implement".to_string()]),
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                        bot: "bot-a".to_string(),
                        prompt: Value::String("review".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
                (
                    "reviewDecision".to_string(),
                    WorkflowNode::Decision(DecisionNode {
                        base: NodeBase {
                            description: None,
                            depends: Some(vec!["review".to_string()]),
                            human_gate: Some(HumanGate {
                                stage: "before".to_string(),
                                prompt: Value::String("approve?".to_string()),
                                approvers: None,
                                deadline_ms: None,
                                on_timeout: None,
                            }),
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                    }),
                ),
                (
                    "review-loop".to_string(),
                    WorkflowNode::Loop(LoopNode {
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
                        max_iterations: 3,
                        body: vec![
                            "implement".to_string(),
                            "review".to_string(),
                            "reviewDecision".to_string(),
                        ],
                        terminate: LoopTerminate {
                            node: "reviewDecision".to_string(),
                            via: "humanGate".to_string(),
                        },
                        output: Some(LoopOutputProjection {
                            from: "implement".to_string(),
                        }),
                    }),
                ),
            ]),
        }
    }

    #[tokio::test]
    async fn loop_depends_met_produces_start_loop_and_first_iteration() {
        // Verify that when a loop node's depends are satisfied, the orchestrator
        // emits StartLoop + StartLoopIteration(1).
        let run_dir = temp_run_dir("loop-depends");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let params: BTreeMap<String, Value> = BTreeMap::new();
        let run_id = "run-loop-depends";
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-loop-dep","version":1,"nodes":{"pre":{"type":"subagent","bot":"bot-x","prompt":"setup"},"work":{"type":"subagent","bot":"bot-x","prompt":"work"},"dec":{"type":"decision","depends":["work"],"humanGate":{"stage":"approve","prompt":"continue?"}},"rl":{"type":"loop","maxIterations":3,"body":["work","dec"],"depends":["pre"],"terminate":{"node":"dec","via":"humanGate"},"output":{"from":"work"}}}}"#,
                expected_workflow_id: Some("flow-loop-dep"),
                params: &params,
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        // First tick: dispatch "pre" (the loop's dependency).
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: WorkflowDefinition {
                    workflow_id: "flow-loop-dep".to_string(),
                    version: 1,
                    params: None,
                    defaults: None,
                    nodes: BTreeMap::from([
                        (
                            "pre".to_string(),
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
                                bot: "bot-x".to_string(),
                                prompt: Value::String("setup".to_string()),
                                working_dir: None,
                                model_overrides: None,
                                tool_policy: None,
                            }),
                        ),
                        (
                            "dec".to_string(),
                            WorkflowNode::Decision(DecisionNode {
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
                            }),
                        ),
                        (
                            "rl".to_string(),
                            WorkflowNode::Loop(LoopNode {
                                base: NodeBase {
                                    description: None,
                                    depends: Some(vec!["pre".to_string()]),
                                    human_gate: None,
                                    retry_policy: None,
                                    timeout_ms: None,
                                    max_output_bytes: None,
                                    output_schema: None,
                                    unsafe_allow_ungated: None,
                                },
                                max_iterations: 3,
                                body: vec!["dec".to_string()],
                                terminate: LoopTerminate {
                                    node: "dec".to_string(),
                                    via: "humanGate".to_string(),
                                },
                                output: Some(LoopOutputProjection {
                                    from: "dec".to_string(),
                                }),
                            }),
                        ),
                    ]),
                },
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();
            assert!(
                _result.ticks > 0 || matches!(_result.reason, RunLoopStopReason::Terminal),
                "expected pre dispatch; reason={:?}",
                _result.reason
            );
        }

        // Now "pre" should be succeeded, so the loop can start.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: WorkflowDefinition {
                    workflow_id: "flow-loop-dep".to_string(),
                    version: 1,
                    params: None,
                    defaults: None,
                    nodes: BTreeMap::from([
                        (
                            "pre".to_string(),
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
                                bot: "bot-x".to_string(),
                                prompt: Value::String("setup".to_string()),
                                working_dir: None,
                                model_overrides: None,
                                tool_policy: None,
                            }),
                        ),
                        (
                            "dec".to_string(),
                            WorkflowNode::Decision(DecisionNode {
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
                            }),
                        ),
                        (
                            "rl".to_string(),
                            WorkflowNode::Loop(LoopNode {
                                base: NodeBase {
                                    description: None,
                                    depends: Some(vec!["pre".to_string()]),
                                    human_gate: None,
                                    retry_policy: None,
                                    timeout_ms: None,
                                    max_output_bytes: None,
                                    output_schema: None,
                                    unsafe_allow_ungated: None,
                                },
                                max_iterations: 3,
                                body: vec!["dec".to_string()],
                                terminate: LoopTerminate {
                                    node: "dec".to_string(),
                                    via: "humanGate".to_string(),
                                },
                                output: Some(LoopOutputProjection {
                                    from: "dec".to_string(),
                                }),
                            }),
                        ),
                    ]),
                },
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();
            let loop_started = events.iter().any(|e| {
                e.event_type == "loopStarted"
                    && e.payload.get("loopId").and_then(Value::as_str) == Some("rl")
            });
            let iter_started = events.iter().any(|e| {
                e.event_type == "loopIterationStarted"
                    && e.payload.get("loopId").and_then(Value::as_str) == Some("rl")
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
            });
            assert!(loop_started, "expected loopStarted event for rl");
            assert!(
                iter_started,
                "expected loopIterationStarted with iteration 1 for rl"
            );
        }

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn code_review_loop_reaches_human_gate_wait_with_correct_activity_id() {
        // Drive the code-review-loop workflow via run_loop until it reaches an
        // open human-gate wait.  Verify the activity-id format.
        let run_dir = temp_run_dir("crl-gate");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-crl-gate";
        let def = code_review_loop_def();
        let workflow_json = serde_json::to_string(&def).unwrap();
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
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
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FakeHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        // Should have stopped at AwaitingWait (human gate open for reviewDecision).
        assert_eq!(
            result.reason,
            RunLoopStopReason::AwaitingWait,
            "expected AwaitingWait for open human gate"
        );

        let events = rt.log.read_all().unwrap();

        // Verify loop lifecycle events
        let loop_started = events.iter().find(|e| e.event_type == "loopStarted");
        assert!(loop_started.is_some(), "missing loopStarted");
        let iter_started = events.iter().find(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("iteration") == Some(&Value::Number(1.into()))
        });
        assert!(iter_started.is_some(), "missing loopIterationStarted(1)");

        // Verify activity IDs use the correct loop-scoped format
        let implement_work_id = format!("{}::loop::review-loop.1::work::implement", run_id);
        let review_work_id = format!("{}::loop::review-loop.1::work::review", run_id);
        let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

        let has_implement = events.iter().any(|e| {
            e.event_type == "attemptCreated"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&implement_work_id)
        });
        let has_review = events.iter().any(|e| {
            e.event_type == "attemptCreated"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&review_work_id)
        });
        let has_decision_gate = events.iter().any(|e| {
            e.event_type == "waitCreated"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&decision_gate_id)
        });

        assert!(
            has_implement,
            "missing implement work dispatch: {}",
            implement_work_id
        );
        assert!(
            has_review,
            "missing review work dispatch: {}",
            review_work_id
        );
        assert!(
            has_decision_gate,
            "missing reviewDecision gate wait: {}",
            decision_gate_id
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn reject_decision_enters_next_iteration() {
        // Drive to the gate wait, then write a waitResolved(rejected).
        // run_loop should emit FinishLoopIteration(rejected) and StartLoopIteration(2).
        let run_dir = temp_run_dir("crl-reject");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-crl-reject";
        let def = code_review_loop_def();
        let workflow_json = serde_json::to_string(&def).unwrap();
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

        // First: drive to AwaitingWait.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }

        // Write waitResolved (rejected).
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &decision_gate_id,
                        "resolution": "rejected",
                        "by": "reviewer",
                        "comment": "needs work",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Second: run_loop again — should handle the rejection.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def,
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();

            // Verify FinishLoopIteration(rejected) for iteration 1.
            let iter1_finished = events.iter().any(|e| {
                e.event_type == "loopIterationFinished"
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
                    && e.payload.get("resolution").and_then(Value::as_str) == Some("rejected")
            });
            assert!(
                iter1_finished,
                "expected loopIterationFinished with resolution=rejected for iteration 1"
            );

            // Verify StartLoopIteration(2).
            let iter2_started = events.iter().any(|e| {
                e.event_type == "loopIterationStarted"
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(2)
            });
            assert!(
                iter2_started,
                "expected loopIterationStarted with iteration=2"
            );

            // Verify iteration 2 dispatches work (body nodes run again).
            let implement_work_v2 = format!("{}::loop::review-loop.2::work::implement", run_id);
            let has_iter2_implement = events.iter().any(|e| {
                e.event_type == "attemptCreated"
                    && e.payload.get("activityId").and_then(Value::as_str)
                        == Some(&implement_work_v2)
            });
            assert!(
                has_iter2_implement,
                "expected iteration 2 to dispatch implement work: {}",
                implement_work_v2
            );
        }

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn approve_decision_finishes_loop_and_run_succeeds() {
        // Drive to gate wait, write waitResolved(approved), run_loop emits
        // FinishLoop(approved) and the run succeeds.
        let run_dir = temp_run_dir("crl-approve");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-crl-approve";
        let def = code_review_loop_def();
        let workflow_json = serde_json::to_string(&def).unwrap();
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

        // First: drive to AwaitingWait.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }

        // Write waitResolved (approved).
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &decision_gate_id,
                        "resolution": "approved",
                        "by": "reviewer",
                        "comment": "lgtm",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Second: run_loop again — should approve and finish.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def,
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();

            // Verify loopFinished with approved resolution.
            let loop_finished = events.iter().any(|e| {
                e.event_type == "loopFinished"
                    && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
                    && e.payload.get("resolution").and_then(Value::as_str) == Some("approved")
            });
            assert!(
                loop_finished,
                "expected loopFinished with resolution=approved"
            );

            // Verify runSucceeded (since loop is the only top-level node).
            let run_succeeded = events.iter().any(|e| e.event_type == "runSucceeded");
            assert!(run_succeeded, "expected run to succeed after loop approval");

            // Verify loop output is produced (from implement node).
            let snap = read_snapshot(&rt).await.unwrap();
            let loop_output_key = format!("{}::work::review-loop", run_id);
            assert!(
                snap.outputs.contains_key(&loop_output_key),
                "expected loop output under {}",
                loop_output_key
            );
        }

        let _ = fs::remove_dir_all(&run_dir);
    }

    /// Hook that fails every subagent call — used to test body failure.
    #[derive(Clone)]
    struct FailingBodyHooks;

    #[async_trait]
    impl WorkflowExecutionHooks for FailingBodyHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            _resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Failed {
                error_code: "TestFailure".to_string(),
                error_class: "fatal".to_string(),
                error_message: "simulated body failure".to_string(),
                session: None,
            })
        }

        async fn execute_host_executor(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &HostExecutorNode,
            _resolved_input: Value,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Failed {
                error_code: "TestFailure".to_string(),
                error_class: "fatal".to_string(),
                error_message: "simulated body failure".to_string(),
                session: None,
            })
        }
    }

    #[tokio::test]
    async fn body_failure_causes_loop_failed() {
        // When a body node fails (subagent returns Failed), the loop should
        // immediately fail with FinishLoop(failed).
        let run_dir = temp_run_dir("crl-body-fail");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-crl-body-fail";
        let def = code_review_loop_def();
        let workflow_json = serde_json::to_string(&def).unwrap();
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
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
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let mut hooks = FailingBodyHooks;
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();

        // Verify loopStarted and loopIterationStarted(1) were written.
        let loop_started = events.iter().any(|e| e.event_type == "loopStarted");
        assert!(loop_started, "expected loopStarted");

        let iter_started = events.iter().any(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
        });
        assert!(iter_started, "expected loopIterationStarted(1)");

        // Body node (implement) should have failed.
        let implement_work_id = format!("{}::loop::review-loop.1::work::implement", run_id);
        let has_implement_fail = events.iter().any(|e| {
            e.event_type == "activityFailed"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&implement_work_id)
        });
        assert!(
            has_implement_fail,
            "expected implement work to fail: {}",
            implement_work_id
        );

        // Loop should have failed.
        let loop_failed = events.iter().any(|e| {
            e.event_type == "loopFinished"
                && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
                && e.payload.get("resolution").and_then(Value::as_str) == Some("failed")
        });
        assert!(
            loop_failed,
            "expected loopFinished with resolution=failed after body failure"
        );

        // Run should have failed (loop failed → run failed).
        assert!(
            matches!(result.reason, RunLoopStopReason::Terminal),
            "expected terminal; reason={:?}",
            result.reason
        );
        let run_failed = events.iter().any(|e| e.event_type == "runFailed");
        assert!(run_failed, "expected run to fail after loop body failure");

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn max_iterations_reject_causes_loop_failed() {
        // Reject at the last allowed iteration (maxIterations=3, iteration 3).
        // Should produce FinishLoop(failed) with MaxIterationsReached.
        let run_dir = temp_run_dir("crl-maxiter");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-crl-maxiter";
        let def = code_review_loop_def();
        let workflow_json = serde_json::to_string(&def).unwrap();
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
                params: &BTreeMap::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Helper to drive to an open gate wait at a specific iteration.
        // We'll go through iterations 1→2→3, rejecting each time until the
        // final one causes failure.

        let iter1_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);

        // Iteration 1: drive to gate, reject.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }
        // Reject iteration 1.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &iter1_gate_id,
                        "resolution": "rejected",
                        "by": "reviewer",
                        "comment": "redo",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }
        // Process rejection → iteration 2 should start.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();
            let iter2_started = events.iter().any(|e| {
                e.event_type == "loopIterationStarted"
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(2)
            });
            assert!(iter2_started, "expected iteration 2 started");
        }

        // Iteration 2: drive to gate, reject.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }
        // Reject iteration 2.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let iter2_gate_id = format!("{}::loop::review-loop.2::gate::reviewDecision", run_id);
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &iter2_gate_id,
                        "resolution": "rejected",
                        "by": "reviewer",
                        "comment": "still no",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }
        // Process → iteration 3.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();
            let iter3_started = events.iter().any(|e| {
                e.event_type == "loopIterationStarted"
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(3)
            });
            assert!(iter3_started, "expected iteration 3 started");
        }

        // Iteration 3: drive to gate, reject (max iterations hit).
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }
        // Reject iteration 3 (final iteration) → loop should fail.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let iter3_gate_id = format!("{}::loop::review-loop.3::gate::reviewDecision", run_id);
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &iter3_gate_id,
                        "resolution": "rejected",
                        "by": "reviewer",
                        "comment": "still no",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }
        // Process → loop and run should fail.
        {
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def,
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let mut hooks = FakeHooks;
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();

            // loopFinished with failed.
            let loop_failed = events.iter().any(|e| {
                e.event_type == "loopFinished"
                    && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
                    && e.payload.get("resolution").and_then(Value::as_str) == Some("failed")
                    && e.payload.get("errorCode").and_then(Value::as_str)
                        == Some("MaxIterationsReached")
            });
            assert!(
                loop_failed,
                "expected loopFinished with failed/MaxIterationsReached"
            );

            assert!(
                matches!(result.reason, RunLoopStopReason::Terminal),
                "expected terminal; reason={:?}",
                result.reason
            );
            let run_failed = events.iter().any(|e| e.event_type == "runFailed");
            assert!(
                run_failed,
                "expected run to fail when max iterations reached"
            );
        }

        let _ = fs::remove_dir_all(&run_dir);
    }

    // ── Task 8.2: tests using the real code-review-loop.workflow.json ──

    /// Like FakeHooks but returns a JSON object with common workflow output
    /// fields so that $ref bindings (implement.output.code,
    /// review.output.preview) work.
    #[derive(Clone)]
    struct RichFakeHooks;

    #[async_trait]
    impl WorkflowExecutionHooks for RichFakeHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            _resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: serde_json::json!({"code": "mock-code", "summary": "mock-summary", "preview": "looks good"}),
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

    /// Hook that records resolved subagent prompts for verification.
    #[derive(Clone)]
    struct RecordingHooks {
        prompts: Arc<Mutex<Vec<(String, String)>>>, // (node_id, resolved_prompt)
    }

    #[async_trait]
    impl WorkflowExecutionHooks for RecordingHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            self.prompts
                .lock()
                .await
                .push((_ctx.node_id.to_string(), resolved_prompt.clone()));
            // Produce a JSON object with common workflow output fields so
            // that $ref bindings (implement.output.code,
            // review.output.preview) resolve correctly.
            let out = serde_json::json!({"code": "mock-code", "summary": "mock-summary", "preview": "looks good"});
            Ok(WorkflowDispatchOutcome::Succeeded {
                output: out,
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
    async fn real_code_review_loop_iter1_reaches_awaiting_wait() {
        // Load the real workflow JSON, bootstrap with task param,
        // and verify iteration 1 reaches AwaitingWait without errors
        // from ${reviewDecision.previous.comment}.
        let raw = include_str!("../../../workflows/code-review-loop.workflow.json");
        let def = crate::parse_workflow_definition(raw).expect("parse real code-review-loop");
        let workflow_json = serde_json::to_string(&def).unwrap();

        let run_dir = temp_run_dir("real-crl-iter1");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-real-crl-1";
        let params: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("task"),
            Value::String("add CLI echo command".to_string()),
        )]);
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        let prompts = Arc::new(Mutex::new(Vec::new()));
        let mut hooks = RecordingHooks {
            prompts: prompts.clone(),
        };

        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        assert_eq!(
            result.reason,
            RunLoopStopReason::AwaitingWait,
            "expected AwaitingWait for open human gate"
        );

        // Verify implement's resolved prompt contains the task and does NOT
        // error on ${reviewDecision.previous.comment} (iteration 1 → empty).
        let recorded = prompts.lock().await;
        let implement_prompt = recorded
            .iter()
            .find(|(node_id, _)| node_id == "implement")
            .map(|(_, p)| p.clone())
            .expect("implement must have been dispatched");
        eprintln!("implement prompt iter1: {implement_prompt}");
        assert!(
            implement_prompt.contains("add CLI echo command"),
            "implement prompt should contain the task param"
        );
        // In iteration 1, .previous.comment resolves to empty string.
        assert!(
            !implement_prompt.contains("ERROR")
                && !implement_prompt.contains("no previous iteration"),
            "implement prompt should not contain binding errors: {implement_prompt}"
        );

        // Verify loop-scoped activity IDs exist
        let events = rt.log.read_all().unwrap();
        let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);
        let has_gate_wait = events.iter().any(|e| {
            e.event_type == "waitCreated"
                && e.payload.get("activityId").and_then(Value::as_str) == Some(&decision_gate_id)
        });
        assert!(has_gate_wait, "expected gate wait for {}", decision_gate_id);

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn real_code_review_loop_reject_enters_iter2_with_comment() {
        // Iteration 1: drive to gate, reject with comment.
        // Iteration 2: verify implement's resolved prompt contains the
        // reject comment from the previous iteration.
        let raw = include_str!("../../../workflows/code-review-loop.workflow.json");
        let def = crate::parse_workflow_definition(raw).expect("parse real code-review-loop");
        let workflow_json = serde_json::to_string(&def).unwrap();

        let run_dir = temp_run_dir("real-crl-reject");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-real-crl-rej";
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("task"), Value::String("add test".to_string()))]);
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        let prompts = Arc::new(Mutex::new(Vec::new()));

        // Iteration 1: drive to AwaitingWait.
        {
            let mut hooks = RecordingHooks {
                prompts: prompts.clone(),
            };
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }

        // Reject iteration 1 with a comment.
        let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &decision_gate_id,
                        "resolution": "rejected",
                        "by": "reviewer",
                        "comment": "needs more tests",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Process rejection → iteration 2 should start.
        {
            let mut hooks = RecordingHooks {
                prompts: prompts.clone(),
            };
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            // Verify loopIterationFinished metadata
            let events = rt.log.read_all().unwrap();
            let iter1_finished = events.iter().find(|e| {
                e.event_type == "loopIterationFinished"
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
            });
            assert!(
                iter1_finished.is_some(),
                "expected loopIterationFinished for iteration 1"
            );
            let payload = &iter1_finished.unwrap().payload;
            assert_eq!(
                payload.get("resolution").and_then(Value::as_str),
                Some("rejected"),
                "resolution should be rejected"
            );
            assert_eq!(
                payload.get("by").and_then(Value::as_str),
                Some("reviewer"),
                "by should be reviewer"
            );
            assert_eq!(
                payload.get("comment").and_then(Value::as_str),
                Some("needs more tests"),
                "comment should be preserved"
            );
            assert_eq!(
                payload.get("decisionActivityId").and_then(Value::as_str),
                Some(decision_gate_id.as_str()),
                "decisionActivityId should be the gate id"
            );

            let iter2_started = events.iter().any(|e| {
                e.event_type == "loopIterationStarted"
                    && e.payload.get("iteration").and_then(Value::as_u64) == Some(2)
            });
            assert!(iter2_started, "expected iteration 2 started");
        }

        // Now verify iteration 2 implement prompt contains the reject comment.
        // The runtime already dispatched implement in iteration 2 during the
        // above run_loop call.
        let recorded = prompts.lock().await;
        let iter2_implement = recorded
            .iter()
            .filter(|(node_id, _)| node_id == "implement")
            .last()
            .map(|(_, p)| p.clone())
            .expect("implement iter2 must have been dispatched");
        eprintln!("implement prompt iter2: {iter2_implement}");
        assert!(
            iter2_implement.contains("needs more tests"),
            "iter2 implement prompt should contain reject comment 'needs more tests': {iter2_implement}"
        );

        let _ = fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn real_code_review_loop_approve_succeeds() {
        // Load real workflow, drive to gate, approve, verify loop/run succeeded.
        let raw = include_str!("../../../workflows/code-review-loop.workflow.json");
        let def = crate::parse_workflow_definition(raw).expect("parse real code-review-loop");
        let workflow_json = serde_json::to_string(&def).unwrap();

        let run_dir = temp_run_dir("real-crl-approve");
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(run_dir.join("blobs")).unwrap();
        let paths = crate::BeamPaths::from_root(run_dir.clone());
        let run_id = "run-real-crl-app";
        let params: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("task"),
            Value::String("add feature".to_string()),
        )]);
        crate::bootstrap_workflow_run(
            &paths,
            crate::BootstrapWorkflowRunInput {
                run_id,
                workflow_json: &workflow_json,
                expected_workflow_id: Some("code-review-loop"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Iteration 1: drive to AwaitingWait.
        {
            let mut hooks = RichFakeHooks;
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def: def.clone(),
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
            assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
        }

        // Approve.
        let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "waitResolved".to_string(),
                    actor: WorkflowActor::Human,
                    payload: serde_json::json!({
                        "activityId": &decision_gate_id,
                        "resolution": "approved",
                        "by": "approver",
                        "comment": "lgtm",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Process approval → loop and run succeed.
        {
            let mut hooks = RichFakeHooks;
            let mut rt = WorkflowRuntimeContext {
                log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
                def,
                runs_base_dir: paths.workflow_runs_dir(),
            };
            let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

            let events = rt.log.read_all().unwrap();

            let loop_finished = events.iter().find(|e| {
                e.event_type == "loopFinished"
                    && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
            });
            assert!(loop_finished.is_some(), "expected loopFinished");
            let payload = &loop_finished.unwrap().payload;
            assert_eq!(
                payload.get("resolution").and_then(Value::as_str),
                Some("approved")
            );

            let run_succeeded = events.iter().any(|e| e.event_type == "runSucceeded");
            assert!(run_succeeded, "expected run to succeed after loop approval");

            assert!(matches!(result.reason, RunLoopStopReason::Terminal));

            // Verify loop output exists.
            let snap = read_snapshot(&rt).await.unwrap();
            let loop_output_key = format!("{}::work::review-loop", run_id);
            assert!(
                snap.outputs.contains_key(&loop_output_key),
                "expected loop output"
            );
        }

        let _ = fs::remove_dir_all(&run_dir);
    }
}
