use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use crate::workflow_definition::HostExecutorNode;
use crate::workflow_definition::SubagentNode;
use crate::workflow_orchestrator::OrchestratorAction;
use crate::{
    EventDraft, EventLog, RunSnapshotDTO, RunStatus, WorkflowActor, WorkflowDefinition,
    WorkflowNode, decide_next_actions, read_run_snapshot,
};

mod completion;
mod dispatch;
mod helpers;
pub mod r#loop;

#[cfg(test)]
mod test_common;
#[cfg(test)]
mod tests_cr_loop;
#[cfg(test)]
mod tests_loop;
#[cfg(test)]
mod tests_real_cr;
#[cfg(test)]
mod tests_recovery;
#[cfg(test)]
mod tests_run_loop;
#[cfg(test)]
mod tests_run_tick;

// Re-export public submodule items.
pub use completion::{
    complete_node_failed, complete_node_succeeded, complete_run_failed, complete_run_succeeded,
};
pub use dispatch::{dispatch_gate, dispatch_work};
pub use helpers::{derive_workflow_idempotency_key, get_host_executor_provider_meta};
pub use r#loop::{finish_loop, finish_loop_iteration, start_loop, start_loop_iteration};

use helpers::write_json_blob;

#[derive(Debug, Clone)]
pub struct WorkflowRuntimeContext {
    pub log: EventLog,
    pub def: WorkflowDefinition,
    pub runs_base_dir: std::path::PathBuf,
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
                if let Some(deadline) = cancel_abort_deadline
                    && Instant::now() >= deadline {
                        join_set.abort_all();
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

    if let Some(ref intent) = snapshot.run.cancelled_run_intent
        && snapshot.run.status != RunStatus::Cancelled
    {
        let _ = crate::complete_run_cancel(
            &mut rt.log,
            crate::CompleteRunCancelInput {
                cancel_origin_event_id: intent.cancel_origin_event_id.clone(),
            },
            WorkflowActor::Scheduler,
        )
        .await;
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
            let bot_key = match node.as_ref() {
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

pub(crate) async fn read_snapshot(rt: &WorkflowRuntimeContext) -> Result<RunSnapshotDTO> {
    read_run_snapshot(&rt.log.run_dir)
        .await?
        .context("workflow runtime requires an existing run snapshot")
}
