use super::*;

/// Upper bound for a worker to report `Ready` after being spawned. When the
/// terminal backend hangs during startup (e.g. a zellij server crash that
/// leaves `zellij attach --create-background` retrying forever), the worker
/// would otherwise never send `Ready` and the session would silently stay
/// active without any card or error.
pub(crate) const WORKER_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_schedule_task(
    state: &AppState,
    task_id: &str,
    prompt: &str,
    working_dir: &str,
    chat_id: &str,
    lark_app_id: Option<&str>,
    root_message_id: Option<&str>,
    scope: Option<&str>,
    chat_type: Option<&ScheduleChatType>,
    _task_name: &str,
) -> Result<()> {
    use beam_core::{ScheduleChatType, SessionScope};
    let lark_app_id = lark_app_id.unwrap_or("local");
    let bot = state
        .bots
        .get(lark_app_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("bot not found for schedule task {}", task_id))?;

    // Create session for this schedule execution
    let session_id = Uuid::new_v4().to_string();
    let scope = match scope.unwrap_or("chat") {
        "thread" => SessionScope::Thread,
        _ => SessionScope::Chat,
    };
    let chat_type_str = match chat_type {
        Some(ScheduleChatType::Group) => Some("group"),
        Some(ScheduleChatType::P2p) => Some("p2p"),
        Some(ScheduleChatType::TopicGroup) => Some("topicGroup"),
        None => None,
    };
    let session = Session {
        session_id: session_id.clone(),
        title: format!("schedule:{}", task_id),
        chat_id: chat_id.to_string(),
        root_message_id: root_message_id.unwrap_or(chat_id).to_string(),
        chat_type: chat_type_str.map(|s| s.to_string()),
        scope,
        status: SessionStatus::Active,
        created_at: Utc::now(),
        working_dir: Some(working_dir.to_string()),
        lark_app_id: lark_app_id.to_string(),
        cli_id: Some(bot.cli_id.clone()),
        cli_bin: bot.cli_bin.clone(),
        cgroup_slice: bot.cgroup_slice.clone(),
        owner_open_id: Some(String::new()),
        quote_target_sender_open_id: None,
        bot_name: None,
        bot_open_id: None,
        cli_session_id: None,
        last_cli_input: None,
        stream_card_id: None,
        stream_card_nonce: None,
        display_mode: None,
        current_screen: None,
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
        model: None,
        locale: None,
        resume_session_id: None,
        disable_cli_bypass: false,
        initial_prompt: None,
        thread_id: None,
        usage_limit: None,
        cli_args: Vec::new(),
        quote_target_id: None,
        worker_pid: None,
        last_screen_status: None,
        closed_at: None,
        agent_attention: None,
        current_turn_id: None,
    };

    // Persist session
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(session_id.clone(), session.clone());
        let snapshot = sessions.clone();
        drop(sessions);
        persist_sessions(&state.paths, &snapshot).await?;
    }

    // Build init config and spawn worker
    let init = build_init_from_session(&session, &state.config, &state.bots)?;
    spawn_worker(state.clone(), session.clone(), init).await?;

    // Send the schedule prompt as input
    let msg = DaemonToWorker::Message {
        content: prompt.to_string(),
        turn_id: Uuid::new_v4().to_string(),
    };
    send_worker_message(&state.workers, &session_id, &msg).await?;

    Ok(())
}

pub(crate) async fn spawn_worker(
    state: AppState,
    session: Session,
    init: InitConfig,
) -> Result<()> {
    tokio::fs::create_dir_all(state.paths.run_dir()).await?;
    let init_path = state.paths.worker_init_json(&session.session_id);
    tokio::fs::write(&init_path, serde_json::to_vec_pretty(&init)?).await?;

    let mut child = Command::new(&state.options.worker_exe)
        .arg("__worker")
        .arg("--init-path")
        .arg(&init_path)
        // The daemon itself may have been started from inside a session (e.g.
        // `beam restart` issued by a session CLI) and thus carry that session's
        // env. Session-scoped vars must never leak into other sessions'
        // workers (they would misroute `beam send` / ask-hook resolution).
        .env_remove("BEAM_SESSION_ID")
        .env_remove("BEAM_CHAT_ID")
        .env_remove("BEAM_LARK_APP_ID")
        .env_remove("BEAM_ROOT_MESSAGE_ID")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "failed to spawn worker via {}",
                state.options.worker_exe.display()
            )
        })?;

    let stdin = child.stdin.take().context("worker stdin was not piped")?;
    let stdout = child.stdout.take().context("worker stdout was not piped")?;
    let worker_pid = child.id();

    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            if let Some(entry) = sessions.get_mut(&session.session_id) {
                entry.worker_pid = worker_pid;
                // First turn of a prompt-driven session must mirror the
                // worker's turn id, otherwise an explicit send cannot mark
                // that turn as answered and the worker's final output is
                // delivered a second time.
                if let Some(turn_id) = init.prompt_turn_id.clone() {
                    entry.current_turn_id = Some(turn_id);
                }
            }
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot).await?;
    }

    state.workers.lock().await.insert(
        session.session_id.clone(),
        WorkerHandle {
            child,
            stdin: Arc::new(Mutex::new(stdin)),
        },
    );

    let session_id = session.session_id.clone();
    let session_id_for_task = session_id.clone();
    let watchdog_state = state.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<WorkerToDaemon>(&line) {
                Ok(WorkerToDaemon::Ready { zellij_session }) => {
                    {
                        let external_host = current_external_host(&state).await;
                        let snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                entry.terminal_url = Some(terminal_base_url(
                                    &external_host,
                                    state.config.web.proxy_base_port,
                                    &session_id_for_task,
                                ));
                                entry.last_screen_status = Some(ScreenStatus::Starting);
                            }
                            sessions.clone()
                        };
                        let _ = persist_sessions(&state.paths, &snapshot).await;
                        // Record the zellij session name for this beam session
                        if !zellij_session.is_empty() {
                            info!(
                                "worker ready: beam session {} -> zellij session {}",
                                session_id_for_task, zellij_session
                            );
                        }
                        // Mark any attempt resume entries as ready (signal via web_port=1)
                        {
                            let mut resumes = state.attempt_resumes.lock().await;
                            for entry in resumes.values_mut() {
                                if entry.session_id == session_id_for_task
                                    && entry.web_port.is_none()
                                {
                                    entry.web_port = Some(1);
                                    entry.write_token = Some(String::new());
                                }
                            }
                        }
                    }
                    let _ =
                        patch_lark_streaming_card(&state, &session_id_for_task, "starting").await;
                    let _ =
                        resend_display_mode_after_worker_ready(&state, &session_id_for_task).await;
                }
                Ok(WorkerToDaemon::ScreenUpdate {
                    content,
                    status,
                    usage_limit,
                }) => {
                    {
                        let snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                entry.current_screen = Some(content);
                                entry.last_screen_status = Some(status);
                                entry.usage_limit = usage_limit.clone();
                            }
                            sessions.clone()
                        };
                        let _ = persist_sessions(&state.paths, &snapshot).await;
                    }
                    if let Some(usage_limit) = usage_limit.clone() {
                        arm_usage_limit_retry_timer(
                            state.clone(),
                            session_id_for_task.clone(),
                            usage_limit,
                        );
                    }
                    let _ = patch_lark_streaming_card(
                        &state,
                        &session_id_for_task,
                        screen_status_card_label(status),
                    )
                    .await;
                }
                Ok(WorkerToDaemon::ScreenshotUploaded {
                    image_key,
                    status,
                    usage_limit,
                    turn_id,
                }) => {
                    // Compare-and-swap (CAS) guard: only accept screenshot
                    // uploads when the session is Active and the turn_id
                    // matches the session's current_turn_id.
                    let cas_ok = match &turn_id {
                        Some(tid) => {
                            let (sess_status, sess_turn) = {
                                let sessions = state.sessions.lock().await;
                                let entry = match sessions.get(&session_id_for_task) {
                                    Some(e) => e,
                                    None => {
                                        debug!(
                                            component = "worker_lifecycle",
                                            operation = "screenshot_cas",
                                            outcome = "discarded",
                                            session_id = %session_id_for_task,
                                            received_turn = %tid,
                                            "ScreenshotUploaded discarded: session not found"
                                        );
                                        continue;
                                    }
                                };
                                (entry.status, entry.current_turn_id.clone())
                            };
                            if sess_status != SessionStatus::Active {
                                debug!(
                                    component = "worker_lifecycle",
                                    operation = "screenshot_cas",
                                    outcome = "discarded",
                                    session_id = %session_id_for_task,
                                    session_status = ?sess_status,
                                    received_turn = %tid,
                                    current_turn = ?sess_turn,
                                    "ScreenshotUploaded discarded: session not Active"
                                );
                                continue;
                            }
                            if sess_turn.as_deref() != Some(tid.as_str()) {
                                debug!(
                                    component = "worker_lifecycle",
                                    operation = "screenshot_cas",
                                    outcome = "discarded",
                                    session_id = %session_id_for_task,
                                    received_turn = %tid,
                                    current_turn = ?sess_turn,
                                    "ScreenshotUploaded discarded: turn_id mismatch"
                                );
                                continue;
                            }
                            true
                        }
                        None => {
                            // No turn_id from old worker; use existing path.
                            true
                        }
                    };
                    if cas_ok {
                        {
                            let snapshot = {
                                let mut sessions = state.sessions.lock().await;
                                if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                    entry.current_image_key = Some(image_key);
                                    entry.last_screen_status = Some(status);
                                    entry.usage_limit = usage_limit.clone();
                                }
                                sessions.clone()
                            };
                            let _ = persist_sessions(&state.paths, &snapshot).await;
                        }
                        if let Some(usage_limit) = usage_limit.clone() {
                            arm_usage_limit_retry_timer(
                                state.clone(),
                                session_id_for_task.clone(),
                                usage_limit,
                            );
                        }
                        let _ = patch_lark_streaming_card(
                            &state,
                            &session_id_for_task,
                            screen_status_card_label(status),
                        )
                        .await;
                    }
                }
                Ok(WorkerToDaemon::CliSessionId { cli_session_id }) => {
                    let snapshot = {
                        let mut sessions = state.sessions.lock().await;
                        if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                            entry.cli_session_id = Some(cli_session_id);
                        }
                        sessions.clone()
                    };
                    let _ = persist_sessions(&state.paths, &snapshot).await;
                }
                Ok(WorkerToDaemon::TuiPrompt {
                    description,
                    options,
                    multi_select,
                }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    if let Some(session) = snapshot
                        && session.lark_app_id != "local"
                        && session.tui_prompt_card_id.is_none()
                        && !session.root_message_id.is_empty()
                        && let Some(bot) = state.bots.get(&session.lark_app_id)
                    {
                        match lark_reply_card_with_opts(
                            &state,
                            bot,
                            &session.root_message_id,
                            &build_tui_prompt_card(
                                &session.root_message_id,
                                &session.session_id,
                                &description,
                                &options,
                                multi_select,
                                &[],
                                session.locale.as_deref(),
                            ),
                            session.scope == SessionScope::Thread,
                        )
                        .await
                        {
                            Ok(card_id) => {
                                let snapshot = {
                                    let mut sessions = state.sessions.lock().await;
                                    if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                        entry.tui_prompt_card_id = Some(card_id);
                                        entry.tui_prompt_options = options.clone();
                                        entry.tui_prompt_multi_select = Some(multi_select);
                                        entry.tui_toggled_indices.clear();
                                    }
                                    sessions.clone()
                                };
                                let _ = persist_sessions(&state.paths, &snapshot).await;
                            }
                            Err(err) => warn!(
                                "failed to deliver tui prompt card for {}: {}",
                                session_id_for_task, err
                            ),
                        }
                    }
                }
                Ok(WorkerToDaemon::TuiPromptResolved { selected_text }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    if let Some(session) = snapshot {
                        if let Some(card_id) = session.tui_prompt_card_id.as_deref()
                            && session.lark_app_id != "local"
                            && let Some(bot) = state.bots.get(&session.lark_app_id)
                        {
                            let _ = lark_update_card(
                                &state,
                                bot,
                                card_id,
                                &build_tui_prompt_resolved_card(
                                    selected_text.as_deref(),
                                    session.locale.as_deref(),
                                ),
                            )
                            .await;
                        }
                        let snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                entry.tui_prompt_card_id = None;
                                entry.tui_prompt_options.clear();
                                entry.tui_prompt_multi_select = None;
                                entry.tui_toggled_indices.clear();
                            }
                            sessions.clone()
                        };
                        let _ = persist_sessions(&state.paths, &snapshot).await;
                    }
                }
                Ok(WorkerToDaemon::PromptReady) => {
                    {
                        let snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                entry.last_screen_status = Some(ScreenStatus::Idle);
                            }
                            sessions.clone()
                        };
                        let _ = persist_sessions(&state.paths, &snapshot).await;
                    }
                    let _ = patch_lark_streaming_card(&state, &session_id_for_task, "idle").await;
                }
                Ok(WorkerToDaemon::FinalOutput {
                    content,
                    turn_id,
                    kind,
                    user_text,
                }) => {
                    let Some(turn_key) = final_output_turn_key(&session_id_for_task, &turn_id)
                    else {
                        continue;
                    };
                    {
                        let sessions_snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            let Some(entry) = sessions.get_mut(&session_id_for_task) else {
                                continue;
                            };
                            if should_skip_worker_final_output(
                                entry,
                                &turn_id,
                                &content,
                                chrono::Utc::now(),
                            ) {
                                continue;
                            }
                            entry.last_screen_status = Some(ScreenStatus::Idle);
                            sessions.clone()
                        };
                        let mut inflight = state.inflight_final_output_turns.lock().await;
                        if !inflight.insert(turn_key.clone()) {
                            continue;
                        }
                        let _ = persist_sessions(&state.paths, &sessions_snapshot).await;
                    };
                    let _ = patch_lark_streaming_card(&state, &session_id_for_task, "idle").await;
                    schedule_final_output_delivery(
                        state.clone(),
                        session_id_for_task.clone(),
                        content,
                        Some(turn_id),
                        kind,
                        user_text,
                        0,
                    );
                }
                Ok(WorkerToDaemon::UserNotify { message }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    if let Some(session) = snapshot
                        && session.lark_app_id != "local"
                        && let Some(bot) = state.bots.get(&session.lark_app_id)
                    {
                        let _ = match session.scope {
                            SessionScope::Thread if !session.root_message_id.is_empty() => {
                                lark_reply_message_with_opts(
                                    &state,
                                    bot,
                                    &session.root_message_id,
                                    &message,
                                    true,
                                )
                                .await
                            }
                            _ => {
                                lark_send_chat_message(&state, bot, &session.chat_id, &message)
                                    .await
                            }
                        };
                    }
                }
                Ok(WorkerToDaemon::TranscriptChoices { candidates, .. }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    let mut card_sent = false;
                    if let Some(ref session) = snapshot
                        && session.lark_app_id != "local"
                        && !session.root_message_id.is_empty()
                        && let Some(bot) = state.bots.get(&session.lark_app_id)
                    {
                        info!(
                            "sending transcript select card for session {} with {} candidates",
                            session_id_for_task,
                            candidates.len()
                        );
                        let card = build_transcript_select_card(
                            &session.root_message_id,
                            &session.session_id,
                            &candidates,
                            session.locale.as_deref(),
                        );
                        if let Err(err) = lark_reply_card_with_opts(
                            &state,
                            bot,
                            &session.root_message_id,
                            &card,
                            session.scope == SessionScope::Thread,
                        )
                        .await
                        {
                            warn!(
                                "failed to send transcript select card for {}: {}",
                                session_id_for_task, err
                            );
                        }
                        card_sent = true;
                    }
                    if !card_sent {
                        debug!(
                            "transcript choices received but card cannot be sent: session={} lark_app_id={:?} root_msg={} bot_exists={}",
                            session_id_for_task,
                            snapshot
                                .as_ref()
                                .map(|s| s.lark_app_id.as_str())
                                .unwrap_or("none"),
                            snapshot
                                .as_ref()
                                .map(|s| s.root_message_id.as_str())
                                .unwrap_or("none"),
                            snapshot
                                .as_ref()
                                .and_then(|s| state.bots.get(&s.lark_app_id))
                                .is_some(),
                        );
                    }
                }
                Ok(WorkerToDaemon::AdoptPreamble {
                    user_text,
                    assistant_text,
                }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    if let Some(session) = snapshot
                        && session.lark_app_id != "local"
                        && let Some(bot) = state.bots.get(&session.lark_app_id)
                    {
                        let recipient_open_id =
                            final_output_footer_recipient_open_id(&state.paths, &session);
                        let card = build_contextual_reply_card(
                            "📜 /adopt 前最后一轮",
                            "📜 Last turn before /adopt",
                            Some(&user_text),
                            &assistant_text,
                            session.cli_id.as_deref().unwrap_or("助手"),
                            session.cli_id.as_deref().unwrap_or("Assistant"),
                            recipient_open_id.as_deref(),
                        );
                        if let Err(err) = lark_reply_card_with_opts(
                            &state,
                            bot,
                            &session.root_message_id,
                            &card,
                            session.scope == SessionScope::Thread,
                        )
                        .await
                        {
                            warn!(
                                "failed to deliver adopt preamble for {}: {}",
                                session_id_for_task, err
                            );
                        }
                    }
                }
                Ok(WorkerToDaemon::CliExit { code, signal }) => {
                    // CLI/pane death is not a user close. Keep the beam
                    // session Active so the next inbound message reattaches
                    // (same as unexpected worker EOF). `/close` is what
                    // marks Closed.
                    warn!(
                        session = %session_id_for_task,
                        ?code,
                        ?signal,
                        "CLI exit reported; keeping session active for reattach"
                    );
                    {
                        let snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                apply_reported_cli_exit(entry);
                            }
                            sessions.clone()
                        };
                        let _ = persist_sessions(&state.paths, &snapshot).await;
                    }
                    let _ =
                        patch_lark_streaming_card(&state, &session_id_for_task, "CLI 已断开").await;
                    break;
                }
                Ok(WorkerToDaemon::Error { message }) => {
                    warn!("worker {} error: {}", session_id_for_task, message);
                }
                Ok(WorkerToDaemon::Heartbeat {
                    processing_since_ms,
                }) => {
                    let was_unresponsive = {
                        let mut health = state.worker_health.lock().await;
                        let entry = health.entry(session_id_for_task.clone()).or_insert(
                            WorkerHealthEntry {
                                last_heartbeat: Instant::now(),
                                processing_since_ms: None,
                                unresponsive: false,
                            },
                        );
                        let was_unresponsive = entry.unresponsive;
                        entry.last_heartbeat = Instant::now();
                        entry.processing_since_ms = processing_since_ms;
                        entry.unresponsive = false;
                        was_unresponsive
                    };
                    if was_unresponsive {
                        info!(
                            "worker for session {} is responsive again",
                            session_id_for_task
                        );
                        // Restore the card label to the current screen status.
                        let status = {
                            let sessions = state.sessions.lock().await;
                            sessions
                                .get(&session_id_for_task)
                                .and_then(|s| s.last_screen_status)
                        };
                        if let Some(status) = status {
                            let _ = patch_lark_streaming_card(
                                &state,
                                &session_id_for_task,
                                screen_status_card_label(status),
                            )
                            .await;
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        "failed to parse worker message for {}: {}",
                        session_id_for_task, err
                    );
                }
            }
        }

        // Worker stdout hit EOF: the worker process exited (graceful CliExit,
        // daemon-requested close/restart, or an unexpected kill/crash).
        handle_worker_eof(&state, &session_id_for_task, worker_pid).await;
    });

    // Watchdog: if the worker never reports Ready (e.g. the terminal backend
    // hangs or dies during startup), surface the failure to the user instead
    // of leaving the session silently active.
    {
        let state = watchdog_state;
        let watchdog_session_id = session_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(WORKER_READY_TIMEOUT).await;
            let session = {
                let sessions = state.sessions.lock().await;
                sessions.get(&watchdog_session_id).cloned()
            };
            let Some(session) = session else {
                return;
            };
            // The Ready handler sets terminal_url; a non-active session is
            // already handled elsewhere (e.g. CliExit).
            if session.terminal_url.is_some() || session.status != SessionStatus::Active {
                return;
            }
            warn!(
                "worker for session {} did not report Ready within {:?}",
                watchdog_session_id, WORKER_READY_TIMEOUT
            );
            notify_worker_ready_timeout(&state, &session).await;
        });
    }

    info!("spawned worker for session {}", session_id);
    Ok(())
}

/// Notify the user (via Lark) that a session's worker failed to become ready
/// within [`WORKER_READY_TIMEOUT`]. No-op for local (non-Lark) sessions.
async fn notify_worker_ready_timeout(state: &AppState, session: &Session) {
    if session.lark_app_id == "local" {
        return;
    }
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return;
    };
    let message = if crate::prompt::is_zh_locale(session.locale.as_deref()) {
        format!(
            "⚠️ session「{}」启动超时：worker 未在 {} 秒内向 daemon 报告就绪，终端后端可能启动失败（如 zellij 异常）。请尝试重新创建 session。",
            session.title,
            WORKER_READY_TIMEOUT.as_secs()
        )
    } else {
        format!(
            "⚠️ Session \"{}\" startup timed out: the worker did not report ready within {}s. The terminal backend may have failed to start (e.g. zellij crash). Please try creating the session again.",
            session.title,
            WORKER_READY_TIMEOUT.as_secs()
        )
    };
    let result = match session.scope {
        SessionScope::Thread if !session.root_message_id.is_empty() => {
            lark_reply_message_with_opts(state, bot, &session.root_message_id, &message, true).await
        }
        _ => lark_send_chat_message(state, bot, &session.chat_id, &message).await,
    };
    if let Err(err) = result {
        warn!(
            "failed to notify worker-ready timeout for session {}: {}",
            session.session_id, err
        );
    }
}

/// Periodic watchdog: flag sessions whose worker stopped heartbeating (hung,
/// or dead but not yet reaped) so the state is visible on the session card
/// and via `beam status`. Only workers that have sent at least one heartbeat
/// are judged; older workers simply stay "unknown".
pub(crate) fn spawn_worker_health_watchdog(state: AppState) {
    tokio::spawn(async move {
        const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(45);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let stale_sessions: Vec<(String, Option<u64>)> = {
                let workers = state.workers.lock().await;
                let sessions = state.sessions.lock().await;
                let mut health = state.worker_health.lock().await;
                let now = Instant::now();
                let mut stale = Vec::new();
                for (session_id, entry) in health.iter_mut() {
                    let worker_present = workers.contains_key(session_id);
                    let session_active = sessions
                        .get(session_id)
                        .map(|s| s.status == SessionStatus::Active)
                        .unwrap_or(false);
                    if !worker_present || !session_active || entry.unresponsive {
                        continue;
                    }
                    if now.duration_since(entry.last_heartbeat) > STALE_AFTER {
                        entry.unresponsive = true;
                        stale.push((session_id.clone(), entry.processing_since_ms));
                    }
                }
                stale
            };
            for (session_id, processing_since_ms) in stale_sessions {
                match processing_since_ms {
                    Some(start_ms) => {
                        let stuck_ms =
                            (Utc::now().timestamp_millis().max(0) as u64).saturating_sub(start_ms);
                        warn!(
                            "worker for session {} is unresponsive: no heartbeat for >{}s; message loop stuck processing for {}ms",
                            session_id,
                            STALE_AFTER.as_secs(),
                            stuck_ms
                        );
                    }
                    None => {
                        warn!(
                            "worker for session {} is unresponsive: no heartbeat for >{}s",
                            session_id,
                            STALE_AFTER.as_secs()
                        );
                    }
                }
                let _ = patch_lark_streaming_card(&state, &session_id, "worker 无响应").await;
            }
        }
    });
}

/// CLI process exit is not a user close. Clear the worker pid and leave
/// the session Active so the next inbound message can reattach.
pub(crate) fn apply_reported_cli_exit(session: &mut Session) {
    session.worker_pid = None;
}

/// Called when a worker's stdout reaches EOF (the process exited). If the
/// workers table still holds *this* worker's handle, remove it so the next
/// `ensure_worker_for_session` respawns (resumes) the worker instead of
/// no-oping on a stale entry. Intentional teardowns (session already Closed
/// via `/close`, or the handle already removed by close/restart) are no-ops.
async fn handle_worker_eof(state: &AppState, session_id: &str, worker_pid: Option<u32>) {
    let removed = {
        let mut workers = state.workers.lock().await;
        let is_current = workers.get(session_id).and_then(|h| h.child.id()) == worker_pid;
        if is_current {
            workers.remove(session_id)
        } else {
            None
        }
    };
    let Some(mut handle) = removed else {
        return;
    };
    let _ = handle.child.wait().await;
    state.worker_health.lock().await.remove(session_id);
    let session_status = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).map(|s| s.status)
    };
    if session_status != Some(SessionStatus::Active) {
        return;
    }
    // Worker process gone (CliExit, kill, or crash): keep the session
    // Active and let the next message revive it via ensure_worker.
    // build_init_from_session uses resume:true, so the new worker reattaches
    // to the still-live zellij session and CLI context is preserved.
    warn!(
        "worker for session {} exited; session kept active (next message will respawn it)",
        session_id
    );
    let snapshot = {
        let mut sessions = state.sessions.lock().await;
        if let Some(entry) = sessions.get_mut(session_id) {
            entry.worker_pid = None;
        }
        sessions.clone()
    };
    let _ = persist_sessions(&state.paths, &snapshot).await;
}

pub(crate) fn build_init_from_session(
    session: &Session,
    config: &Config,
    bots: &HashMap<String, BotConfig>,
) -> Result<InitConfig> {
    let lark_app_secret = bots
        .get(&session.lark_app_id)
        .map(|b| b.lark_app_secret.clone())
        .unwrap_or_default();
    Ok(InitConfig {
        session_id: session.session_id.clone(),
        title: session.title.clone(),
        chat_id: session.chat_id.clone(),
        root_message_id: session.root_message_id.clone(),
        working_dir: session
            .working_dir
            .clone()
            .context("session missing working_dir")?,
        cli_id: session.cli_id.clone().context("session missing cli_id")?,
        cli_bin: session.cli_bin.clone().context("session missing cli_bin")?,
        // cgroup slice is host config: prefer the live bot so a bots.json
        // edit applies on restart. Fall back to the session copy when the
        // bot is gone (e.g. local/API sessions).
        cgroup_slice: bots
            .get(&session.lark_app_id)
            .map(|b| b.cgroup_slice.clone())
            .unwrap_or_else(|| session.cgroup_slice.clone()),
        cli_args: session.cli_args.clone(),
        prompt: String::new(),
        resume: true,
        cli_session_id: session.cli_session_id.clone(),
        lark_app_id: session.lark_app_id.clone(),
        lark_app_secret,
        prompt_turn_id: None,
        owner_open_id: session.owner_open_id.clone(),
        adopted_from: session.adopted_from.clone(),
        adopt_restored_from_metadata: session.adopted_from.is_some(),
        screen_analyzer: config.screen_analyzer.clone(),
        bot_name: session.bot_name.clone(),
        bot_open_id: session.bot_open_id.clone(),
        disable_cli_bypass: session.disable_cli_bypass,
        initial_prompt: session.initial_prompt.clone(),
        model: session.model.clone(),
        locale: session.locale.clone(),
        resume_session_id: session.resume_session_id.clone(),
    })
}

pub(crate) async fn send_worker_message(
    workers: &Arc<Mutex<HashMap<String, WorkerHandle>>>,
    session_id: &str,
    msg: &DaemonToWorker,
) -> Result<()> {
    let line = serde_json::to_string(msg)?;
    let write_result = {
        let workers_guard = workers.lock().await;
        let handle = workers_guard
            .get(session_id)
            .with_context(|| format!("worker not running for session {}", session_id))?;
        let mut stdin = handle.stdin.lock().await;
        let write = async {
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await?;
            Ok::<(), std::io::Error>(())
        };
        write.await
    };
    match write_result {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {
            // The worker process is gone but its handle lingered: drop the
            // stale entry so the next ensure_worker_for_session respawns
            // (resumes) the worker instead of no-oping, and surface the
            // failure to the caller instead of silently "succeeding".
            let removed = workers.lock().await.remove(session_id);
            if let Some(mut handle) = removed {
                let _ = handle.child.wait().await;
            }
            Err(anyhow::anyhow!(
                "worker stdin broken pipe for session {}; stale worker removed, a retry will respawn it",
                session_id
            ))
        }
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::apply_reported_cli_exit;
    use crate::tests::test_helpers::make_session;
    use beam_core::SessionStatus;

    #[test]
    fn reported_cli_exit_keeps_session_active() {
        let mut session = make_session("s1");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.worker_pid = Some(42);
        apply_reported_cli_exit(&mut session);
        assert_eq!(session.status, SessionStatus::Active);
        assert!(session.closed_at.is_none());
        assert!(session.worker_pid.is_none());
    }
}
