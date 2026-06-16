//! Workflow execution dispatch: execution hooks, subagent/host-executor
//! dispatch, worker process management, and bootstrap helpers.
//!
//! Extracted from `lib.rs` (Task 9.1 "继续拆分") to separate workflow
//! execution glue from route handlers and app wiring.
//!
//! This module handles:
//! - `DaemonWorkflowExecutionHooks`: the `WorkflowExecutionHooks` impl
//!   that bridges the core runtime to daemon-side dispatch.
//! - Subagent session dispatch (`run_workflow_subagent_session`).
//! - Host-executor dispatch (`run_workflow_host_executor`).
//! - Worker process termination (`terminate_workflow_worker_process`).
//! - Bootstrap helpers (`bootstrap_and_start_workflow_run`,
//!   `run_workflow_runtime_once`, `derive_workflow_idempotency_key`).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{Path as AxumPath, State};
use beam_core::{
    BootstrapWorkflowRunInput, RunChatBinding, SessionScope, WorkflowDispatchOutcome,
    WorkflowDispatchRun, WorkflowDispatchSession, WorkflowExecutionHooks,
    bootstrap_workflow_run, mint_workflow_run_id, parse_workflow_output,
    with_workflow_output_protocol,
};
use chrono::Utc;
use serde_json::Value;

use crate::{
    AppState, SessionCreateSpec, await_session_final_output, close_session,
    create_session_internal, expand_tilde,
    workflow_cancellation, workflow_host_executors, workflow_reconcilers,
    workflow_runtime_driver,
};

// ---------------------------------------------------------------------------
// DaemonWorkflowExecutionHooks
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct DaemonWorkflowExecutionHooks {
    pub(crate) state: AppState,
}

#[async_trait::async_trait]
impl WorkflowExecutionHooks for DaemonWorkflowExecutionHooks {
    async fn execute_subagent(
        &mut self,
        ctx: WorkflowDispatchRun<'_>,
        node: &beam_core::SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        let guard = workflow_cancellation::ActivityTokenGuard::register(
            workflow_cancellation::global_cancellation_registry(),
            ctx.run_id,
            ctx.activity_id,
        );
        run_workflow_subagent_session(
            &self.state,
            ctx,
            node,
            resolved_prompt,
            Some(&guard.token),
        )
        .await
    }

    async fn execute_host_executor(
        &mut self,
        ctx: WorkflowDispatchRun<'_>,
        node: &beam_core::HostExecutorNode,
        // Already parsed by `prepare_host_executor`.
        parsed_input: Value,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        let guard = workflow_cancellation::ActivityTokenGuard::register(
            workflow_cancellation::global_cancellation_registry(),
            ctx.run_id,
            ctx.activity_id,
        );
        run_workflow_host_executor(
            &self.state,
            ctx,
            node,
            parsed_input,
            Some(&guard.token),
        )
        .await
    }

    fn prepare_host_executor(
        &self,
        executor_name: &str,
        resolved_input: &Value,
    ) -> anyhow::Result<beam_core::HostExecutorPrepareResult> {
        let registry = workflow_host_executors::global_host_executor_registry();
        if let Some(executor) = registry.get(executor_name) {
            let parsed = executor.parse_input(resolved_input).map_err(|err| {
                anyhow::anyhow!("{} parse_input failed: {:#}", executor.name(), err)
            })?;
            let canonical = executor.canonical_input(&parsed).map_err(|err| {
                anyhow::anyhow!("{} canonical_input failed: {:#}", executor.name(), err)
            })?;
            Ok(beam_core::HostExecutorPrepareResult {
                parsed_input: parsed,
                canonical_input: canonical,
                provider: executor.provider().to_string(),
                idempotency_ttl_ms: executor.idempotency_ttl_ms(),
            })
        } else {
            // Fall back to the default implementation (legacy path).
            let (provider, idempotency_ttl_ms) =
                beam_core::get_host_executor_provider_meta(executor_name);
            Ok(beam_core::HostExecutorPrepareResult {
                parsed_input: resolved_input.clone(),
                canonical_input: resolved_input.clone(),
                provider: provider.to_string(),
                idempotency_ttl_ms,
            })
        }
    }

    async fn recover_dangling_effects(
        &mut self,
        log: &mut beam_core::EventLog,
        _snapshot: &beam_core::RunSnapshotDTO,
    ) -> anyhow::Result<beam_core::RecoveryResult> {
        let registry = workflow_reconcilers::global_reconciler_registry();
        let run_dir = self.state.paths.workflow_run_dir(&log.run_id);

        // Record event count before any reconciliation — only *new* events
        // written during this recovery pass count as progress.
        let events_before = log.read_all()?.len();

        // Step 1: Reconcile dangling effects for all registered providers.
        // Re-read the snapshot after each provider because earlier providers
        // may have written terminal events that affect subsequent lookups.
        let known_providers: Vec<String> = registry.providers().map(|s| s.to_string()).collect();
        for provider in &known_providers {
            let current_snapshot = beam_core::read_run_snapshot(&run_dir)
                .await?
                .ok_or_else(|| anyhow::anyhow!("snapshot disappeared during recovery"))?;
            let _ = workflow_reconcilers::reconcile_provider_dangling_effects(
                registry,
                &self.state,
                log,
                &run_dir,
                provider,
                &current_snapshot,
            )
            .await?;
        }

        // Step 2: Handle dangling effects from providers with no reconciler.
        let after_registered = beam_core::read_run_snapshot(&run_dir)
            .await?
            .ok_or_else(|| anyhow::anyhow!("snapshot disappeared after registered providers"))?;
        let _ = workflow_reconcilers::handle_missing_provider_dangling_effects(
            registry,
            log,
            &after_registered,
        )?;

        // Determine progress by event-count delta: only events that were
        // actually appended during this recovery pass count as progress.
        // This correctly handles cases like a prior freshRetry that yields
        // a result without writing any new event.
        let events_after = log.read_all()?.len();
        let had_progress = events_after > events_before;

        // Check remaining dangling effects after full recovery pass.
        let final_snapshot = beam_core::read_run_snapshot(&run_dir)
            .await?
            .ok_or_else(|| anyhow::anyhow!("snapshot disappeared after full recovery"))?;
        let has_remaining = !final_snapshot.dangling.effect_attempted.is_empty();

        Ok(beam_core::RecoveryResult {
            had_progress,
            has_remaining_dangling: has_remaining,
        })
    }

    async fn on_activities_cancelled(
        &mut self,
        activity_ids: &[String],
        node_ids: &[String],
        run_id: &str,
    ) {
        let registry = workflow_cancellation::global_cancellation_registry();

        // If the entire run is being cancelled, cancel all tokens at once.
        // We detect this by checking whether the run-level cancel intent is
        // present in the snapshot.
        if let Ok(Some(snap)) = beam_core::read_run_snapshot(
            &self.state.paths.workflow_run_dir(run_id),
        )
        .await
        {
            if snap.run.cancelled_run_intent.is_some() {
                let count = registry.cancel_run(run_id).len();
                if count > 0 {
                    tracing::debug!(
                        "cancellation registry: cancelled run {} ({} activities)",
                        run_id,
                        count
                    );
                }
                return;
            }
        }

        // Node-level cancels: cancel each node's tokens.
        for node_id in node_ids {
            let count = registry.cancel_node(run_id, node_id).len();
            if count > 0 {
                tracing::debug!(
                    "cancellation registry: cancelled node {} in run {} ({} activities)",
                    node_id,
                    run_id,
                    count
                );
            }
        }

        // Individual activity cancels.
        for activity_id in activity_ids {
            if registry.cancel_activity(run_id, activity_id) {
                tracing::debug!(
                    "cancellation registry: cancelled activity {} in run {}",
                    activity_id,
                    run_id
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Worker process management
// ---------------------------------------------------------------------------

/// Terminate a workflow worker process with escalating signals.
///
/// Sends SIGINT first, then polls (via `try_wait` on the tokio child handle,
/// falling back to `kill(pid, 0)` when no handle is available) whether the
/// process has exited.  Escalates to SIGKILL after a 5-second grace period
/// if the process is still alive.
///
/// # Why `try_wait` before `kill(pid, 0)`
///
/// A child that has exited but not yet been reaped (zombie) still has a PID
/// entry, so `kill(pid, 0)` returns success.  That makes us wait the full
/// 5-second grace *and* send a pointless SIGKILL even though the worker
/// already honoured SIGINT.  `try_wait` reaps the zombie and returns
/// `Some(exit_status)`, allowing us to break out of the grace loop
/// immediately.
///
/// This is called from the cancellation path of `run_workflow_subagent_session`
/// to ensure the worker process is forcefully killed rather than relying solely
/// on the gentle `DaemonToWorker::Close` message (which a stuck worker may
/// never process).
///
/// # Safety
///
/// Uses `libc::kill` to send signals.  The `worker_pid` must belong to the
/// current process's child (or the caller must have permission to signal it).
pub(crate) async fn terminate_workflow_worker_process(state: &AppState, session_id: &str) {
    let worker_pid = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(session_id)
            .and_then(|s| s.worker_pid)
    };
    let Some(pid) = worker_pid else {
        tracing::debug!(
            "terminate_workflow_worker_process: no worker_pid for session {}",
            session_id
        );
        return;
    };

    // Step 1: Send SIGINT to request graceful shutdown.
    let sigint_ok = unsafe { libc::kill(pid as i32, libc::SIGINT) == 0 };
    tracing::info!(
        "terminate_workflow_worker: SIGINT sent to pid={} (ok={})",
        pid,
        sigint_ok
    );

    // Step 2: Grace period – poll every 200 ms up to 5 seconds.
    //
    // We prefer `try_wait()` on the tokio child handle because it correctly
    // detects (and reaps) zombie children that `kill(pid, 0)` would still
    // report as alive.  When no child handle is available (e.g. the worker
    // was already removed from the workers map), we fall back to the less
    // reliable `kill(pid, 0)`.
    let grace_duration = std::time::Duration::from_secs(5);
    let poll_interval = std::time::Duration::from_millis(200);
    let deadline = tokio::time::Instant::now() + grace_duration;
    let mut process_exited = false;
    // Track whether we have (or once had) a child handle so we know
    // when to trust try_wait results over kill(pid, 0).
    let mut has_handle = false;

    while tokio::time::Instant::now() < deadline {
        // --- try_wait path (preferred) ---
        // Lock is held *only* for the synchronous try_wait call, never
        // across the .await sleep below.
        let try_wait_exited = {
            let mut workers = state.workers.lock().await;
            match workers.get_mut(session_id) {
                Some(handle) => {
                    has_handle = true;
                    match handle.child.try_wait() {
                        Ok(Some(_status)) => {
                            // Child exited and was reaped — done.
                            true
                        }
                        Ok(None) => {
                            // Still running.
                            false
                        }
                        Err(_) => {
                            // Child already reaped or OS error — treat as
                            // gone.
                            true
                        }
                    }
                }
                None => false,
            }
        }; // mutex guard dropped here

        if try_wait_exited {
            process_exited = true;
            tracing::info!(
                "terminate_workflow_worker: pid={} exited (detected via try_wait)",
                pid
            );
            break;
        }

        // --- kill(pid, 0) fallback ---
        // Only use this when we've never seen a child handle.  If we *have*
        // a handle and try_wait said "still running", we trust that over the
        // zombie-prone kill(pid, 0) check.
        if !has_handle {
            let alive = unsafe { libc::kill(pid as i32, 0) == 0 };
            if !alive {
                process_exited = true;
                tracing::info!(
                    "terminate_workflow_worker: pid={} exited (detected via kill(0))",
                    pid
                );
                break;
            }
        }

        tokio::time::sleep(poll_interval).await;
    }

    // Step 3: Escalate to SIGKILL if the process is still alive.
    if !process_exited {
        let sigkill_ok = unsafe { libc::kill(pid as i32, libc::SIGKILL) == 0 };
        tracing::warn!(
            "terminate_workflow_worker: SIGKILL sent to pid={} (ok={}) – did not exit after SIGINT + {:?}",
            pid,
            sigkill_ok,
            grace_duration,
        );
    }
}

// ---------------------------------------------------------------------------
// Workflow subagent / host-executor dispatch
// ---------------------------------------------------------------------------

async fn run_workflow_subagent_session(
    state: &AppState,
    ctx: WorkflowDispatchRun<'_>,
    node: &beam_core::SubagentNode,
    resolved_prompt: String,
    cancel_token: Option<&tokio_util::sync::CancellationToken>,
) -> Result<WorkflowDispatchOutcome> {
    let Some(bot) = state.bots.get(&node.bot).cloned() else {
        return Ok(WorkflowDispatchOutcome::Failed {
            error_code: "UnknownProviderError".to_string(),
            error_class: "manual".to_string(),
            error_message: format!("bot '{}' is not registered.", node.bot),
            session: None,
        });
    };

    // Check cancellation before doing heavy work.
    if cancel_token.map_or(false, |t| t.is_cancelled()) {
        return Ok(WorkflowDispatchOutcome::Cancelled {
            cancel_origin_event_id: String::new(),
            session: None,
        });
    }

    let working_dir = expand_tilde(
        &node
            .working_dir
            .clone()
            .or_else(|| bot.working_dir.clone())
            .or_else(|| state.config.daemon.working_dirs.first().cloned())
            .unwrap_or_else(|| ".".to_string()),
    );
    let title = node
        .base
        .description
        .clone()
        .unwrap_or_else(|| format!("workflow {} {}", ctx.run_id, ctx.node_id));
    let session = create_session_internal(
        state,
        SessionCreateSpec {
            title,
            chat_id: format!("workflow-{}", ctx.run_id),
            chat_type: Some("local".to_string()),
            root_message_id: ctx.run_id.to_string(),
            quote_target_id: None,
            scope: SessionScope::Thread,
            working_dir: working_dir.clone(),
            cli_id: bot.cli_id.clone(),
            cli_bin: bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
            cli_args: Vec::new(),
            backend_type: bot
                .backend_type
                .clone()
                .unwrap_or_else(|| state.config.daemon.backend_type.clone()),
            prompt: with_workflow_output_protocol(&resolved_prompt),
            lark_app_id: "local".to_string(),
            owner_open_id: None,
            adopted_from: None,
        },
    )
    .await
    .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let session_id = session.session_id.clone();
    let output =
        match await_session_final_output(
            state,
            &session_id,
            Duration::from_secs(180),
            cancel_token,
        )
        .await
        {
            Ok(output) => output,
            Err(err) => {
                // Distinguish cancellation from other failures.
                if cancel_token.map_or(false, |t| t.is_cancelled()) {
                    // Forcefully terminate the worker process before cleaning up
                    // the session.  The gentle DaemonToWorker::Close message may
                    // never be processed by a stuck worker, so we escalate via
                    // SIGINT → grace → SIGKILL.
                    terminate_workflow_worker_process(state, &session_id).await;
                    let _ =
                        close_session(State(state.clone()), AxumPath(session_id.clone())).await;
                    return Ok(WorkflowDispatchOutcome::Cancelled {
                        cancel_origin_event_id: String::new(),
                        session: Some(WorkflowDispatchSession {
                            session_id,
                            bot_name: node.bot.clone(),
                            started_at: Utc::now().timestamp_millis().max(0) as u64,
                            ended_at: Some(Utc::now().timestamp_millis().max(0) as u64),
                            cli_session_id: None,
                            lark_app_id: Some("local".to_string()),
                            cli_id: Some(bot.cli_id.clone()),
                            working_dir: Some(working_dir),
                            web_port: None,
                            log_path: None,
                        }),
                    });
                }

                // Non-cancellation failure: gentle close.
                let _ = close_session(State(state.clone()), AxumPath(session_id.clone())).await;
                return Ok(WorkflowDispatchOutcome::Failed {
                    error_code: "WorkerCrashed".to_string(),
                    error_class: "retryable".to_string(),
                    error_message: err.to_string(),
                    session: Some(WorkflowDispatchSession {
                        session_id,
                        bot_name: node.bot.clone(),
                        started_at: Utc::now().timestamp_millis().max(0) as u64,
                        ended_at: Some(Utc::now().timestamp_millis().max(0) as u64),
                        cli_session_id: None,
                        lark_app_id: Some("local".to_string()),
                        cli_id: Some(bot.cli_id.clone()),
                        working_dir: Some(working_dir),
                        web_port: None,
                        log_path: None,
                    }),
                });
            }
        };
    let parsed_output = parse_workflow_output(&output).unwrap_or(Value::String(output.clone()));
    let _ = close_session(State(state.clone()), AxumPath(session_id.clone())).await;
    Ok(WorkflowDispatchOutcome::Succeeded {
        output: parsed_output,
        session: Some(WorkflowDispatchSession {
            session_id,
            bot_name: bot.name.clone().unwrap_or_else(|| node.bot.clone()),
            started_at: Utc::now().timestamp_millis().max(0) as u64,
            ended_at: Some(Utc::now().timestamp_millis().max(0) as u64),
            cli_session_id: None,
            lark_app_id: Some("local".to_string()),
            cli_id: Some(bot.cli_id.clone()),
            working_dir: Some(working_dir),
            web_port: None,
            log_path: None,
        }),
    })
}

pub(crate) async fn run_workflow_host_executor(
    state: &AppState,
    ctx: WorkflowDispatchRun<'_>,
    node: &beam_core::HostExecutorNode,
    // Already parsed by `prepare_host_executor`.
    parsed_input: Value,
    cancel_token: Option<&tokio_util::sync::CancellationToken>,
) -> Result<WorkflowDispatchOutcome> {
    // Check cancellation before dispatching to provider.
    if cancel_token.map_or(false, |t| t.is_cancelled()) {
        return Ok(WorkflowDispatchOutcome::Cancelled {
            cancel_origin_event_id: String::new(),
            session: None,
        });
    }

    let registry = workflow_host_executors::global_host_executor_registry();
    let executor = match registry.resolve(&node.executor) {
        Ok(executor) => executor,
        Err(outcome) => return Ok(outcome),
    };

    // NOTE: We do NOT interrupt the provider future mid-flight, because
    // effectAttempted has already been written before this call and the
    // provider must be allowed to complete (or fail on its own).  The token
    // is registered so that if cancellation is detected *before* provider
    // invocation, we bail early.  Task 6.3 will handle real worker kill.
    match executor.invoke(state, &ctx, node, &parsed_input).await {
        Ok(outcome) => Ok(outcome),
        Err(err) => Ok(executor.classify_error(&err, &ctx)),
    }
}

// ---------------------------------------------------------------------------
// Bootstrap and runtime helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(crate) fn derive_workflow_idempotency_key(
    workflow_id: &str,
    revision_id: &str,
    run_id: &str,
    node_id: &str,
    attempt_id: &str,
) -> String {
    beam_core::derive_workflow_idempotency_key(
        workflow_id,
        revision_id,
        run_id,
        node_id,
        attempt_id,
    )
}

pub(crate) async fn run_workflow_runtime_once(state: &AppState, run_id: &str, workflow_json: &str) {
    workflow_runtime_driver::run(state, run_id, workflow_json).await;
}

pub(crate) async fn bootstrap_and_start_workflow_run(
    state: &AppState,
    workflow_id: &str,
    raw_def: &str,
    params: &BTreeMap<String, Value>,
    initiator: &str,
    chat_binding: Option<RunChatBinding>,
) -> Result<beam_core::WorkflowRunBootstrap> {
    let run_id = mint_workflow_run_id(workflow_id, Utc::now().timestamp_millis().max(0) as u64);
    let bootstrap = bootstrap_workflow_run(
        &state.paths,
        BootstrapWorkflowRunInput {
            run_id: &run_id,
            workflow_json: raw_def,
            expected_workflow_id: Some(workflow_id),
            params,
            initiator,
            chat_binding,
        },
    )?;
    run_workflow_runtime_once(state, bootstrap.run_id.as_str(), raw_def).await;
    Ok(bootstrap)
}
