use super::*;

/// Handle workflow text commands (/workflow run|cancel) from Lark messages.
/// Extracted from the inline block inside `handle_lark_event_payload`.
pub(crate) async fn handle_workflow_text_command(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    chat_id: &str,
    app_id: &str,
    text: &str,
) -> Result<Option<Json<Value>>, (StatusCode, String)> {
    if let Some(workflow_command) = parse_workflow_text_command(text) {
        match workflow_command {
            WorkflowTextCommand::Invalid { error, usage } => {
                let _ =
                    lark_reply_message(state, bot, message_id, &format!("{}\n{}", error, usage))
                        .await;
                return Ok(Some(Json(
                    serde_json::json!({ "ok": true, "workflow": "invalid" }),
                )));
            }
            WorkflowTextCommand::Run {
                workflow_id,
                raw_params,
            } => {
                let params_map: BTreeMap<String, Value> = raw_params
                    .into_iter()
                    .map(|(k, v)| (k, Value::String(v)))
                    .collect();
                let params = if params_map.is_empty() {
                    String::new()
                } else {
                    params_map
                        .iter()
                        .map(|(key, value)| format!("{}={}", key, value))
                        .collect::<Vec<_>>()
                        .join(" ")
                };
                let def_path = load_workflow_definition_path(&workflow_id)
                    .await
                    .map_err(internal_error)?;
                let raw_def = tokio::fs::read_to_string(&def_path)
                    .await
                    .map_err(internal_error)?;
                let bootstrap = match bootstrap_and_start_workflow_run(
                    state,
                    &workflow_id,
                    &raw_def,
                    &params_map,
                    "lark",
                    Some(RunChatBinding {
                        chat_id: chat_id.to_string(),
                        lark_app_id: app_id.to_string(),
                    }),
                )
                .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        let reply = format!("workflow run failed: {}", e);
                        let _ = lark_reply_message(state, bot, message_id, &reply).await;
                        return Ok(Some(Json(serde_json::json!({
                            "ok": true,
                            "workflow": "failed",
                        }))));
                    }
                };
                let reply = if params.is_empty() {
                    format!(
                        "workflow run queued: {}\nrunId: {}",
                        bootstrap.workflow_id, bootstrap.run_id
                    )
                } else {
                    format!(
                        "workflow run queued: {} {}\nrunId: {}",
                        bootstrap.workflow_id, params, bootstrap.run_id
                    )
                };
                let _ = lark_reply_message(state, bot, message_id, &reply).await;
                return Ok(Some(Json(serde_json::json!({
                    "ok": true,
                    "workflow": "run",
                    "runId": bootstrap.run_id,
                }))));
            }
            WorkflowTextCommand::Cancel { run_id } => {
                let reply = format!("workflow cancel requested: {}", run_id);
                let _ = lark_reply_message(state, bot, message_id, &reply).await;
                return Ok(Some(Json(
                    serde_json::json!({ "ok": true, "workflow": "cancel" }),
                )));
            }
        }
    }
    Ok(None)
}

pub(crate) async fn start_workflow_attempt_resume(
    State(state): State<AppState>,
    AxumPath((run_id, activity_id, attempt_id)): AxumPath<(String, String, String)>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    if !run_id.trim().is_empty() && !activity_id.trim().is_empty() && !attempt_id.trim().is_empty()
    {
        let key = attempt_resume_key(&run_id, &activity_id, &attempt_id);
        if let Some(existing) = {
            let resumes = state.attempt_resumes.lock().await;
            resumes.get(&key).cloned()
        } {
            if let (Some(_web_port), Some(_write_token)) = (existing.web_port, existing.write_token)
            {
                let terminal_url = build_terminal_url_with_ticket(
                    &terminal_base_url(
                        &current_external_host(&state).await,
                        state.config.web.proxy_base_port,
                        &existing.session_id,
                    ),
                    &existing.session_id,
                    terminal_auth::TerminalPermission::Write,
                );
                return Ok((
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "ok": true,
                        "resumeId": existing.resume_id,
                        "runId": existing.run_id,
                        "activityId": existing.activity_id,
                        "attemptId": existing.attempt_id,
                        "sessionId": existing.session_id,
                        "originalSessionId": existing.original_session_id,
                        "cliSessionId": existing.cli_session_id,
                        "webPort": state.config.web.proxy_base_port,
                        "url": terminal_url,
                        "alreadyRunning": true,
                        "startedAt": existing.started_at,
                        "logPath": existing.log_path,
                        "sidecarPath": existing.sidecar_path,
                    })),
                ));
            }
            return match wait_for_attempt_resume_ready(&state, &key, &existing.sidecar_path).await {
                AttemptResumeWaitOutcome::Ready(waiting) => {
                    if let (Some(_web_port), Some(_write_token)) =
                        (waiting.web_port, waiting.write_token.clone())
                    {
                        let terminal_url = build_terminal_url_with_ticket(
                            &terminal_base_url(
                                &current_external_host(&state).await,
                                state.config.web.proxy_base_port,
                                &waiting.session_id,
                            ),
                            &waiting.session_id,
                            terminal_auth::TerminalPermission::Write,
                        );
                        Ok((
                            StatusCode::OK,
                            Json(serde_json::json!({
                                "ok": true,
                                "resumeId": waiting.resume_id,
                                "runId": waiting.run_id,
                                "activityId": waiting.activity_id,
                                "attemptId": waiting.attempt_id,
                                "sessionId": waiting.session_id,
                                "originalSessionId": waiting.original_session_id,
                                "cliSessionId": waiting.cli_session_id,
                                "webPort": state.config.web.proxy_base_port,
                                "url": terminal_url,
                                "alreadyRunning": false,
                                "startedAt": waiting.started_at,
                                "logPath": waiting.log_path,
                                "sidecarPath": waiting.sidecar_path,
                            })),
                        ))
                    } else {
                        Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "attempt_resume_ready_missing_port".to_string(),
                        ))
                    }
                }
                AttemptResumeWaitOutcome::Failed { error, message } => Err((
                    match error.as_str() {
                        "worker_error" | "worker_exited_before_ready" => {
                            StatusCode::INTERNAL_SERVER_ERROR
                        }
                        "attempt_resume_closed" => StatusCode::CONFLICT,
                        _ => StatusCode::INTERNAL_SERVER_ERROR,
                    },
                    message.unwrap_or(error),
                )),
            };
        }
    } else {
        return Err((StatusCode::BAD_REQUEST, "bad_id".to_string()));
    }

    let run_dir = state.paths.workflow_run_dir(&run_id);
    let Some(snapshot) = read_run_snapshot(&run_dir).await.map_err(internal_error)? else {
        return Err((StatusCode::NOT_FOUND, "unknown_run".to_string()));
    };
    let Some(terminal) = snapshot
        .attempt_io
        .get(&attempt_id)
        .and_then(|io| io.terminal.clone())
    else {
        return Err((StatusCode::NOT_FOUND, "no_terminal_sidecar".to_string()));
    };
    if terminal.lark_app_id.is_none() {
        return Err((StatusCode::CONFLICT, "missing_lark_app_id".to_string()));
    }
    let bot_app_id = terminal.lark_app_id.clone().unwrap_or_default();
    let Some(bot) = state.bots.get(&bot_app_id).cloned() else {
        return Err((StatusCode::CONFLICT, "bot_not_registered".to_string()));
    };
    if bot.cli_id.trim().is_empty()
        || !matches!(
            bot.cli_id.as_str(),
            "coco" | "claude-code" | "codex" | "traex" | "hermes" | "antigravity"
        )
    {
        return Err((StatusCode::CONFLICT, "resume_unsupported_cli".to_string()));
    }
    if matches!(bot.cli_id.as_str(), "antigravity") && terminal.cli_session_id.is_none() {
        return Err((StatusCode::CONFLICT, "missing_cli_session_id".to_string()));
    }

    let resume_id = format!(
        "resume-{}-{}",
        Utc::now().timestamp_millis().max(0),
        Uuid::new_v4().simple()
    );
    let resume_dir = state
        .paths
        .attempt_resume_dir(&run_id, &activity_id, &attempt_id)
        .join(&resume_id);
    tokio::fs::create_dir_all(&resume_dir)
        .await
        .map_err(internal_error)?;
    let log_path = resume_dir.join("terminal.log");
    let sidecar_path = resume_dir.join("resume.json");

    let session_id = Uuid::new_v4().to_string();
    let working_dir = terminal
        .working_dir
        .clone()
        .unwrap_or_else(|| ".".to_string());
    let started_at = Utc::now().timestamp_millis().max(0) as u64;
    let session = Session {
        session_id: session_id.clone(),
        title: format!("workflow resume {} {}", run_id, activity_id),
        chat_id: format!("wf-resume-chat-{run_id}"),
        chat_type: Some("local".to_string()),
        root_message_id: format!("wf-resume-root-{attempt_id}"),
        quote_target_id: None,
        scope: SessionScope::Thread,
        status: SessionStatus::Active,
        created_at: Utc::now(),
        closed_at: None,
        working_dir: Some(working_dir.clone()),
        lark_app_id: "local".to_string(),
        owner_open_id: None,
        quote_target_sender_open_id: None,
        worker_pid: None,
        cli_id: Some(bot.cli_id.clone()),
        cli_bin: Some(bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone())),
        cli_args: Vec::new(),
        cli_session_id: None,
        last_cli_input: None,
        stream_card_id: None,
        stream_card_nonce: None,
        display_mode: None,
        current_screen: None,
        last_screen_status: None,
        usage_limit: None,
        current_image_key: None,
        tui_prompt_card_id: None,
        tui_prompt_options: Vec::new(),
        tui_prompt_multi_select: None,
        tui_toggled_indices: Vec::new(),
        pending_response_card_id: None,
        pending_response_card_state: None,
        last_patched_response_card_id: None,
        terminal_url: None,
        last_final_output_turn_id: None,
        last_final_output: None,
        last_explicit_send_at: None,
        adopted_from: None,
        bot_name: None,
        bot_open_id: None,
        disable_cli_bypass: false,
        initial_prompt: None,
        model: None,
        locale: None,
        resume_session_id: None,
        thread_id: None,
        agent_attention: None,
        current_turn_id: None,
    };
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session_id.clone(), session.clone());
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot)
            .await
            .map_err(internal_error)?;
    }

    let init = InitConfig {
        session_id: session_id.clone(),
        title: session.title.clone(),
        chat_id: session.chat_id.clone(),
        root_message_id: session.root_message_id.clone(),
        working_dir: working_dir.clone(),
        cli_id: bot.cli_id.clone(),
        cli_bin: bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
        cli_args: Vec::new(),
        prompt: String::new(),
        resume: true,
        cli_session_id: terminal.cli_session_id.clone(),
        lark_app_id: bot.lark_app_id.clone(),
        lark_app_secret: bot.lark_app_secret.clone(),
        prompt_turn_id: None,
        owner_open_id: None,
        adopted_from: None,
        adopt_restored_from_metadata: false,
        screen_analyzer: state.config.screen_analyzer.clone(),
        bot_name: None,
        bot_open_id: None,
        disable_cli_bypass: false,
        initial_prompt: None,
        model: None,
        locale: None,
        resume_session_id: None,
    };
    let key = attempt_resume_key(&run_id, &activity_id, &attempt_id);
    let entry = AttemptResumeEntry {
        resume_id: resume_id.clone(),
        run_id: run_id.clone(),
        activity_id: activity_id.clone(),
        attempt_id: attempt_id.clone(),
        session_id: session_id.clone(),
        original_session_id: terminal.session_id.clone(),
        cli_session_id: terminal.cli_session_id.clone(),
        lark_app_id: bot.lark_app_id.clone(),
        bot_name: bot.name.clone().or_else(|| terminal.bot_name.clone()),
        cli_id: bot.cli_id.clone(),
        working_dir: working_dir.clone(),
        log_path: log_path.display().to_string(),
        sidecar_path: sidecar_path.display().to_string(),
        started_at,
        updated_at: started_at,
        web_port: None,
        write_token: None,
        close_reason: None,
    };
    {
        let mut resumes = state.attempt_resumes.lock().await;
        resumes.insert(key.clone(), entry.clone());
    }
    write_attempt_resume_sidecar(&state.paths, &entry, "starting")
        .await
        .map_err(internal_error)?;

    if let Err(err) = spawn_worker(state.clone(), session.clone(), init).await {
        {
            let mut resumes = state.attempt_resumes.lock().await;
            resumes.remove(&key);
        }
        let mut failed_entry = entry.clone();
        failed_entry.close_reason = Some(format!("worker_init_failed:{err}"));
        write_attempt_resume_sidecar(&state.paths, &failed_entry, "closed")
            .await
            .map_err(internal_error)?;
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("worker_init_failed:{err}"),
        ));
    }

    let ready =
        wait_for_attempt_resume_ready(&state, &key, &sidecar_path.display().to_string()).await;
    let ready_entry = match ready {
        AttemptResumeWaitOutcome::Ready(entry) => entry,
        AttemptResumeWaitOutcome::Failed { error, message } => {
            return Err((
                match error.as_str() {
                    "worker_error" | "worker_exited_before_ready" => {
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                    "attempt_resume_closed" => StatusCode::CONFLICT,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                message.unwrap_or(error),
            ));
        }
    };
    let web_port = ready_entry.web_port.unwrap_or_default();
    let _write_token = ready_entry.write_token.clone().unwrap_or_default();
    let updated_entry = {
        let mut resumes = state.attempt_resumes.lock().await;
        if let Some(existing) = resumes.get_mut(&key) {
            existing.web_port = Some(web_port);
            existing.write_token = Some(_write_token.clone());
            existing.updated_at = Utc::now().timestamp_millis().max(0) as u64;
            Some(existing.clone())
        } else {
            None
        }
    };
    if let Some(entry) = updated_entry {
        write_attempt_resume_sidecar(&state.paths, &entry, "live")
            .await
            .map_err(internal_error)?;
    }
    let terminal_url = build_terminal_url_with_ticket(
        &terminal_base_url(
            &current_external_host(&state).await,
            state.config.web.proxy_base_port,
            &session_id,
        ),
        &session_id,
        terminal_auth::TerminalPermission::Write,
    );
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "resumeId": resume_id,
            "runId": run_id,
            "activityId": activity_id,
            "attemptId": attempt_id,
            "sessionId": session_id,
            "originalSessionId": terminal.session_id,
            "cliSessionId": terminal.cli_session_id,
            "webPort": state.config.web.proxy_base_port,
            "url": terminal_url,
            "alreadyRunning": false,
            "startedAt": started_at,
            "logPath": log_path.display().to_string(),
            "sidecarPath": sidecar_path.display().to_string(),
        })),
    ))
}

pub(crate) async fn end_workflow_attempt_resume(
    State(state): State<AppState>,
    AxumPath((run_id, activity_id, attempt_id)): AxumPath<(String, String, String)>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let req = parse_attempt_resume_request_body(&body)?;
    let key = attempt_resume_key(&run_id, &activity_id, &attempt_id);
    let entry = {
        let mut resumes = state.attempt_resumes.lock().await;
        resumes.remove(&key)
    };
    let Some(mut entry) = entry else {
        return Err((StatusCode::NOT_FOUND, "resume_not_running".to_string()));
    };
    entry.close_reason = Some(
        req.reason
            .unwrap_or_else(|| "ended_by_dashboard".to_string()),
    );
    entry.updated_at = Utc::now().timestamp_millis().max(0) as u64;
    let _ = write_attempt_resume_sidecar(&state.paths, &entry, "closed").await;
    let _ = close_session(State(state.clone()), AxumPath(entry.session_id.clone())).await;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "resumeId": entry.resume_id,
            "status": "closed",
            "closeReason": entry.close_reason.unwrap_or_else(|| "ended_by_dashboard".to_string()),
            "closedAt": entry.updated_at,
        })),
    ))
}

pub(crate) async fn approve_workflow_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    Json(req): Json<WorkflowWaitActionRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    workflow_commands::dashboard_approve_or_reject_wait(
        &state,
        &run_id,
        WaitResolution::Approved,
        req.comment,
    )
    .await
}

pub(crate) async fn reject_workflow_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    Json(req): Json<WorkflowWaitActionRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    workflow_commands::dashboard_approve_or_reject_wait(
        &state,
        &run_id,
        WaitResolution::Rejected,
        req.comment,
    )
    .await
}

pub(crate) async fn cancel_workflow_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    Json(req): Json<WorkflowCancelRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let outcome = workflow_commands::cancel_run(&state, &run_id, req.reason)
        .await
        .map_err(internal_error)?;

    if outcome.ok {
        Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": true,
                "runId": outcome.run_id,
                "status": outcome.status,
                "alreadyCancelled": outcome.already_cancelled,
                "alreadyTerminal": outcome.already_terminal,
                "lastSeq": outcome.last_seq,
            })),
        ))
    } else {
        Err((
            StatusCode::from_u16(
                outcome
                    .error_code
                    .as_deref()
                    .and_then(|_| Some(404_u16))
                    .unwrap_or(500),
            )
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            outcome
                .error_hint
                .unwrap_or_else(|| "cancel failed".to_string()),
        ))
    }
}

pub(crate) async fn resume_workflow_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    Json(req): Json<WorkflowResumeRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let run_dir = state.paths.workflow_run_dir(&run_id);
    let Some(snapshot) = read_run_snapshot(&run_dir).await.map_err(internal_error)? else {
        return Err((StatusCode::NOT_FOUND, "workflow run not found".to_string()));
    };
    if matches!(
        snapshot.run.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
    ) {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": true,
                "runId": run_id,
                "status": snapshot.run.status,
                "alreadyTerminal": true,
                "lastSeq": snapshot.last_seq,
                "snapshot": snapshot,
            })),
        ));
    }

    let mut log =
        EventLog::new(run_id.clone(), state.paths.workflow_runs_dir()).map_err(internal_error)?;

    let last_seen_event_id = log
        .read_all()
        .map_err(internal_error)?
        .last()
        .map(|event| event.event_id.clone())
        .unwrap_or_default();
    let _ = log
        .append(EventDraft {
            event_type: "resumeStarted".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "daemonId": "beam-daemon",
                "lastSeenEventId": last_seen_event_id,
                "reason": req.reason.as_deref(),
            }),
            timestamp: None,
            payload_hash: None,
        })
        .map_err(internal_error)?;

    let reconciler_registry = workflow_reconcilers::global_reconciler_registry();

    let schedule_result_raw = workflow_reconcilers::reconcile_provider_dangling_effects(
        reconciler_registry,
        &state,
        &mut log,
        &run_dir,
        "beam-schedule",
        &snapshot,
    )
    .await
    .map_err(internal_error)?;

    let Some(after_schedule_snapshot) =
        read_run_snapshot(&run_dir).await.map_err(internal_error)?
    else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to re-read workflow after schedule resume".to_string(),
        ));
    };

    let feishu_result_raw = workflow_reconcilers::reconcile_provider_dangling_effects(
        reconciler_registry,
        &state,
        &mut log,
        &run_dir,
        "feishu-im",
        &after_schedule_snapshot,
    )
    .await
    .map_err(internal_error)?;

    let after_feishu_snapshot = read_run_snapshot(&run_dir).await.map_err(internal_error)?;
    let (registry_covered, registry_missing) =
        if let Some(after_feishu) = after_feishu_snapshot.as_ref() {
            workflow_reconcilers::handle_missing_provider_dangling_effects(
                reconciler_registry,
                &mut log,
                after_feishu,
            )
            .map_err(internal_error)?
        } else {
            (Vec::new(), Vec::new())
        };
    let registry_result = workflow_reconcilers::ReconcilerRegistryCheckResult {
        covered_providers: registry_covered,
        missing_providers: registry_missing,
    };

    let schedule_result = provider_result_to_schedule_result(schedule_result_raw);
    let feishu_result = provider_result_to_feishu_result(feishu_result_raw);

    let raw_def = tokio::fs::read_to_string(run_dir.join("workflow.json"))
        .await
        .map_err(internal_error)?;
    let workflow_def = parse_workflow_definition(&raw_def).map_err(internal_error)?;
    let pre_runtime_snapshot = read_run_snapshot(&run_dir).await.map_err(internal_error)?;
    let log_events = log.read_all().map_err(internal_error)?;
    let resume_started_event = log_events
        .iter()
        .rev()
        .find(|event| event.event_type == "resumeStarted")
        .cloned();
    let event_index: HashMap<String, beam_core::WorkflowEventEnvelope> = log_events
        .into_iter()
        .map(|event| (event.event_id.clone(), event))
        .collect();

    let mut wait_recovery_outcomes = Vec::new();
    let mut cancel_recovery_outcomes = Vec::new();
    let mut worker_crashed_outcomes = Vec::new();
    if let Some(snapshot_before_runtime) = pre_runtime_snapshot.as_ref() {
        for activity_id in &snapshot_before_runtime.dangling.waits {
            if let Some(activity) = snapshot_before_runtime
                .activities
                .iter()
                .find(|candidate| &candidate.activity_id == activity_id)
            {
                if let Some(outcome) =
                    append_resume_wait_recovery(&mut log, &workflow_def, activity)
                        .map_err(internal_error)?
                {
                    wait_recovery_outcomes.push(outcome);
                }
            }
        }
        for activity_id in &snapshot_before_runtime.dangling.cancels {
            if let Some(activity) = snapshot_before_runtime
                .activities
                .iter()
                .find(|candidate| &candidate.activity_id == activity_id)
            {
                if let Some(outcome) =
                    append_resume_cancel_recovery(&mut log, &event_index, activity)
                        .map_err(internal_error)?
                {
                    cancel_recovery_outcomes.push(outcome);
                }
            }
        }
        for activity_id in &snapshot_before_runtime.dangling.activities {
            if let Some(activity) = snapshot_before_runtime
                .activities
                .iter()
                .find(|candidate| &candidate.activity_id == activity_id)
            {
                if let Some(outcome) =
                    append_resume_worker_crashed(&mut log, activity).map_err(internal_error)?
                {
                    worker_crashed_outcomes.push(outcome);
                }
            }
        }
    }
    run_workflow_runtime_once(&state, &run_id, &raw_def).await;

    let Some(updated) = read_run_snapshot(&run_dir).await.map_err(internal_error)? else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to re-read resumed workflow".to_string(),
        ));
    };

    Ok((
        StatusCode::OK,
        Json(build_workflow_resume_response(
            run_id,
            updated.run.status,
            false,
            updated.last_seq,
            resume_started_event.as_ref(),
            &event_index,
            &updated,
            &schedule_result,
            &feishu_result,
            &registry_result,
            worker_crashed_outcomes,
            wait_recovery_outcomes,
            cancel_recovery_outcomes,
        )),
    ))
}

// ---------------------------------------------------------------------------
// Workflow card action handler (wf_approve / wf_reject / wf_cancel)
// Called from session_card_actions::handle_session_card_action.
// ---------------------------------------------------------------------------

pub(crate) async fn handle_workflow_card_action(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(run_id) = action
        .workflow_run_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing workflow run id",
        )));
    };
    let Some(activity_id) = action
        .workflow_activity_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing workflow activity id",
        )));
    };
    let Some(attempt_id) = action
        .workflow_attempt_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing workflow attempt id",
        )));
    };
    let Some(card_nonce) = action
        .card_nonce
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing workflow card nonce",
        )));
    };
    let operator = action.operator_open_id.as_deref().unwrap_or("unknown");
    let comment = action.workflow_comment.as_deref();

    // Load existing frozen card records for idempotency.
    let mut workflow_cards = load_workflow_approval_cards(&state.paths, run_id)
        .await
        .map_err(internal_error)?;

    // If the card was already frozen (repeated click), still succeed
    if workflow_cards.contains_key(card_nonce) {
        return Ok(Json(serde_json::json!({
            "toast": {
                "type": "success",
                "content": format!("workflow {} already recorded", action.action),
            }
        })));
    }

    // Phase 5.1/5.2: write EventLog events AND push the runtime
    let action_str = action.action.as_str();
    let handler_result = match action_str {
        "wf_approve" | "wf_reject" => {
            let resolution = if action_str == "wf_approve" {
                WaitResolution::Approved
            } else {
                WaitResolution::Rejected
            };
            workflow_commands::lark_approve_or_reject_wait(
                state,
                run_id,
                activity_id,
                attempt_id,
                operator,
                resolution,
                comment.map(|s| s.to_string()),
            )
            .await
            .map(|outcome| {
                if outcome.ok {
                    Ok(format!(
                        "workflow {} recorded",
                        action_str.trim_start_matches("wf_")
                    ))
                } else {
                    Err(outcome
                        .error_hint
                        .unwrap_or_else(|| "unknown error".to_string()))
                }
            })
        }
        "wf_cancel" => workflow_commands::cancel_run(state, run_id, comment.map(|s| s.to_string()))
            .await
            .map(|outcome| {
                if outcome.ok {
                    Ok("workflow cancel recorded".to_string())
                } else {
                    Err(outcome
                        .error_hint
                        .unwrap_or_else(|| "cancel failed".to_string()))
                }
            }),
        _ => unreachable!(),
    };

    let (response_content, is_success) = match handler_result {
        Ok(Ok(msg)) => (msg, true),
        Ok(Err(err)) => (err, false),
        Err(err) => (format!("workflow action failed: {}", err), false),
    };

    if !is_success {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            &response_content,
        )));
    }

    // Event was written successfully — now freeze the card.
    let workflow_card = serde_json::from_str::<Value>(&build_workflow_approval_resolved_card(
        action_str,
        run_id,
        action.workflow_id.as_deref(),
        action.workflow_revision_id.as_deref(),
        action.workflow_node_id.as_deref().unwrap_or(activity_id),
        activity_id,
        attempt_id,
        operator,
        comment,
    ))
    .unwrap_or_else(|_| serde_json::json!({}));
    if let Some(message_id) = workflow_approval_target_message_id(action) {
        let card_json = serde_json::to_string(&workflow_card).unwrap_or_else(|_| "{}".to_string());
        match lark_update_card(state, bot, &message_id, &card_json).await {
            Ok(()) => {
                workflow_cards.insert(
                    card_nonce.to_string(),
                    FrozenCard {
                        message_id,
                        content: response_content.clone(),
                        title: format!("workflow approval {}/{}", run_id, activity_id),
                        display_mode: None,
                        image_key: None,
                    },
                );
                let _ = save_workflow_approval_cards(&state.paths, run_id, &workflow_cards).await;
                Ok(Json(build_lark_card_action_toast(
                    "success",
                    &response_content,
                )))
            }
            Err(err) => {
                warn!(
                    "lark card update failed for {} after event write: {}",
                    run_id, err
                );
                Ok(Json(build_lark_card_action_toast(
                    "warning",
                    &format!("events recorded, but card update failed: {}", err),
                )))
            }
        }
    } else {
        Ok(Json(serde_json::json!({
            "toast": {
                "type": "success",
                "content": response_content,
            },
            "card": {
                "type": "raw",
                "data": workflow_card,
            }
        })))
    }
}
