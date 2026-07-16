use super::*;

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
    let _ = tokio::spawn(async move {
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
                            for (_, entry) in resumes.iter_mut() {
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
                    if let Some(session) = snapshot {
                        if session.lark_app_id != "local"
                            && session.tui_prompt_card_id.is_none()
                            && !session.root_message_id.is_empty()
                        {
                            if let Some(bot) = state.bots.get(&session.lark_app_id) {
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
                                            if let Some(entry) =
                                                sessions.get_mut(&session_id_for_task)
                                            {
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
                    }
                }
                Ok(WorkerToDaemon::TuiPromptResolved { selected_text }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    if let Some(session) = snapshot {
                        if let Some(card_id) = session.tui_prompt_card_id.as_deref() {
                            if session.lark_app_id != "local" {
                                if let Some(bot) = state.bots.get(&session.lark_app_id) {
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
                            }
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
                    if let Some(session) = snapshot {
                        if session.lark_app_id != "local" {
                            if let Some(bot) = state.bots.get(&session.lark_app_id) {
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
                                        lark_send_chat_message(
                                            &state,
                                            bot,
                                            &session.chat_id,
                                            &message,
                                        )
                                        .await
                                    }
                                };
                            }
                        }
                    }
                }
                Ok(WorkerToDaemon::TranscriptChoices { candidates, .. }) => {
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.get(&session_id_for_task).cloned()
                    };
                    let mut card_sent = false;
                    if let Some(ref session) = snapshot {
                        if session.lark_app_id != "local" && !session.root_message_id.is_empty() {
                            if let Some(bot) = state.bots.get(&session.lark_app_id) {
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
                        }
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
                    if let Some(session) = snapshot {
                        if session.lark_app_id != "local" {
                            if let Some(bot) = state.bots.get(&session.lark_app_id) {
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
                    }
                }
                Ok(WorkerToDaemon::CliExit { .. }) => {
                    {
                        let snapshot = {
                            let mut sessions = state.sessions.lock().await;
                            if let Some(entry) = sessions.get_mut(&session_id_for_task) {
                                entry.status = SessionStatus::Closed;
                                entry.closed_at = Some(Utc::now());
                                entry.worker_pid = None;
                            }
                            sessions.clone()
                        };
                        let _ = persist_sessions(&state.paths, &snapshot).await;
                    }
                    let _ = patch_lark_streaming_card(&state, &session_id_for_task, "closed").await;
                    break;
                }
                Ok(WorkerToDaemon::Error { message }) => {
                    warn!("worker {} error: {}", session_id_for_task, message);
                }
                Err(err) => {
                    warn!(
                        "failed to parse worker message for {}: {}",
                        session_id_for_task, err
                    );
                }
            }
        }
    });

    info!("spawned worker for session {}", session_id);
    Ok(())
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
    let workers_guard = workers.lock().await;
    let handle = workers_guard
        .get(session_id)
        .with_context(|| format!("worker not running for session {}", session_id))?;
    let mut stdin = handle.stdin.lock().await;
    if let Err(e) = stdin
        .write_all(serde_json::to_string(msg)?.as_bytes())
        .await
    {
        if e.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(e.into());
    }
    if let Err(e) = stdin.write_all(b"\n").await {
        if e.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(e.into());
    }
    if let Err(e) = stdin.flush().await {
        if e.kind() == std::io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(e.into());
    }
    Ok(())
}
