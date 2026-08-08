use super::*;

// ---------------------------------------------------------------------------
// Standalone session operation functions
// ---------------------------------------------------------------------------

pub(crate) async fn send_input(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<SessionInputRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    ensure_worker_for_session(&state, &session_id)
        .await
        .map_err(internal_error)?;
    let turn_id = next_session_turn_id();
    // Atomically persist last_cli_input and the new current_turn_id in a
    // single write, BEFORE begin_lark_turn_card.  This ensures that the
    // daemon already has the new turn_id for the CAS guard when the
    // coordinator processes TurnStarted and sends the first screenshot
    // upload, rejecting any stale uploads from the prior turn.
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            let session = sessions
                .get_mut(&session_id)
                .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
            session.last_cli_input = Some(req.content.clone());
            session.current_turn_id = Some(turn_id.clone());
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot)
            .await
            .map_err(internal_error)?;
    }
    if let Err(err) = begin_lark_turn_card(&state, &session_id, "starting").await {
        warn!("failed to begin lark turn card for {}: {}", session_id, err);
    }
    let msg = if req.raw {
        DaemonToWorker::RawInput {
            content: req.content,
            turn_id,
        }
    } else {
        DaemonToWorker::Message {
            content: req.content,
            turn_id,
        }
    };
    if let Err(err) = send_worker_message(&state.workers, &session_id, &msg).await {
        // The worker died between ensure_worker_for_session and the send
        // (e.g. killed; broken pipe already removed the stale handle).
        // Respawn (resume) once and retry the same turn: the message never
        // reached the CLI, so resending it cannot duplicate a turn.
        warn!(
            "[{}] send to worker failed ({}), respawning worker and retrying once",
            session_id, err
        );
        ensure_worker_for_session(&state, &session_id)
            .await
            .map_err(internal_error)?;
        send_worker_message(&state.workers, &session_id, &msg)
            .await
            .map_err(internal_error)?;
    }
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn close_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let session = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?
    };
    if ensure_worker_for_session(&state, &session_id).await.is_ok() {
        send_worker_message(&state.workers, &session_id, &DaemonToWorker::Close)
            .await
            .map_err(internal_error)?;
    } else if session.adopted_from.is_none() {
        let _ = std::process::Command::new("zellij")
            .args(["delete-session", &session_zellij_target(&session), "-f"])
            .output();
    }

    let mut workers = state.workers.lock().await;
    if let Some(mut handle) = workers.remove(&session_id) {
        let _ = handle.child.wait().await;
    }
    let snapshot = {
        let mut sessions = state.sessions.lock().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            session.status = SessionStatus::Closed;
            session.closed_at = Some(Utc::now());
            session.worker_pid = None;
            clear_pending_response_tracking(session);
        }
        sessions.clone()
    };
    persist_sessions(&state.paths, &snapshot)
        .await
        .map_err(internal_error)?;
    if let Err(err) = clear_pending_response_patch_marker(&state.paths, &session_id).await {
        warn!(
            "failed to clear pending response marker for {}: {}",
            session_id, err
        );
    }
    if let Err(err) = delete_frozen_cards(&state.paths, &session_id).await {
        warn!("failed to delete frozen cards for {}: {}", session_id, err);
    }
    if session.adopted_from.is_none() {
        let target = session_zellij_target(&session);
        let _ = std::process::Command::new("zellij")
            .args(["delete-session", &target, "-f"])
            .output();
    }
    Ok(StatusCode::OK)
}

pub(crate) async fn restart_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<RestartSessionRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let session = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?
    };
    let target = session_zellij_target(&session);

    if let Some(adopted) = session
        .adopted_from
        .as_ref()
        .and_then(|v| v.zellij_session.as_ref())
    {
        if !zellij_has_session(adopted) {
            return Err((
                StatusCode::CONFLICT,
                "adopted zellij session no longer exists".to_string(),
            ));
        }
    }

    let _ = send_worker_message(&state.workers, &session_id, &DaemonToWorker::Close).await;
    {
        let mut workers = state.workers.lock().await;
        if let Some(mut handle) = workers.remove(&session_id) {
            let _ = handle.child.wait().await;
        }
    }
    if session.adopted_from.is_none() {
        let _ = std::process::Command::new("zellij")
            .args(["delete-session", &target, "-f"])
            .output();
    }
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            if let Some(entry) = sessions.get_mut(&session_id) {
                entry.status = SessionStatus::Active;
                entry.closed_at = None;
                entry.worker_pid = None;
                entry.terminal_url = None;
                entry.current_screen = None;
                entry.last_screen_status = None;
                entry.usage_limit = None;
                entry.current_image_key = None;
                entry.stream_card_nonce = None;
                entry.last_final_output_turn_id = None;
                clear_pending_response_tracking(entry);
            }
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot)
            .await
            .map_err(internal_error)?;
    }
    if let Err(err) = clear_pending_response_patch_marker(&state.paths, &session_id).await {
        warn!(
            "failed to clear pending response marker for {}: {}",
            session_id, err
        );
    }

    let prompt_turn_id = (!req.prompt.is_empty()).then(next_session_turn_id);
    let init = InitConfig {
        prompt: req.prompt,
        prompt_turn_id,
        resume: false,
        ..build_init_from_session(&session, &state.config, &state.bots).map_err(internal_error)?
    };
    spawn_worker(state.clone(), session, init)
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn resume_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<ResumeSessionRequest>,
) -> Result<(StatusCode, Json<SessionSummary>), (StatusCode, String)> {
    let session = {
        let sessions = state.sessions.lock().await;
        validate_resume_target(&sessions, &session_id)?
    };

    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            let entry = sessions
                .get_mut(&session_id)
                .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
            entry.status = SessionStatus::Active;
            entry.closed_at = None;
            entry.worker_pid = None;
            entry.terminal_url = None;
            entry.current_screen = None;
            entry.last_screen_status = None;
            entry.usage_limit = None;
            entry.current_image_key = None;
            entry.stream_card_nonce = None;
            entry.last_final_output_turn_id = None;
            clear_pending_response_tracking(entry);
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot)
            .await
            .map_err(internal_error)?;
    }
    if let Err(err) = clear_pending_response_patch_marker(&state.paths, &session_id).await {
        warn!(
            "failed to clear pending response marker for {}: {}",
            session_id, err
        );
    }

    let prompt_turn_id = (!req.prompt.is_empty()).then(next_session_turn_id);
    let init = InitConfig {
        prompt: req.prompt,
        prompt_turn_id,
        resume: true,
        ..build_init_from_session(&session, &state.config, &state.bots).map_err(internal_error)?
    };
    spawn_worker(state.clone(), session.clone(), init)
        .await
        .map_err(internal_error)?;

    let resumed = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?
    };
    Ok((StatusCode::ACCEPTED, Json(SessionSummary::from(&resumed))))
}

pub(crate) async fn refresh_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    ensure_worker_for_session(&state, &session_id)
        .await
        .map_err(internal_error)?;
    send_worker_message(&state.workers, &session_id, &DaemonToWorker::RefreshScreen)
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn list_zellij_adopt_candidates()
-> Result<Json<Vec<ZellijAdoptCandidate>>, (StatusCode, String)> {
    Ok(Json(discover_zellij_adopt_candidates()))
}

pub(crate) async fn adopt_zellij_session(
    State(state): State<AppState>,
    Json(req): Json<AdoptZellijSessionRequest>,
) -> Result<(StatusCode, Json<SessionSummary>), (StatusCode, String)> {
    if !zellij_has_session(&req.zellij_session) {
        return Err((
            StatusCode::NOT_FOUND,
            "zellij session not found".to_string(),
        ));
    }
    let pane_id = req.zellij_pane_id.clone();
    let candidate = discover_zellij_adopt_candidates()
        .into_iter()
        .find(|item| item.zellij_session == req.zellij_session && item.zellij_pane_id == pane_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "zellij pane not found".to_string()))?;

    let session_id = Uuid::new_v4().to_string();
    let mut adopted_from = AdoptedFrom {
        tmux_target: None,
        zellij_session: Some(req.zellij_session.clone()),
        zellij_pane_id: Some(pane_id.clone()),
        original_cli_pid: candidate.cli_pid.unwrap_or(0),
        session_id: None,
        cli_id: Some(req.cli_id.clone()),
        cwd: if req.cwd.is_empty() {
            candidate.cwd.clone()
        } else {
            req.cwd.clone()
        },
        pane_cols: req.pane_cols.or(candidate.pane_cols),
        pane_rows: req.pane_rows.or(candidate.pane_rows),
    };
    let adopted_cli_session_id = resolve_opencode_adopt_session(&req.cli_id, &adopted_from.cwd);
    if let Some(cli_session_id) = adopted_cli_session_id.clone() {
        info!(
            "resolved opencode session during adopt: beam_session={} cli_session_id={}",
            session_id, cli_session_id
        );
        adopted_from.session_id = Some(cli_session_id);
    }
    let title = req
        .title
        .clone()
        .unwrap_or_else(|| format!("adopt {}", req.zellij_session));
    let lark_app_id = req.lark_app_id.unwrap_or_else(|| "local".to_string());
    let chat_id = req.chat_id.unwrap_or_else(|| "local".to_string());
    let chat_type = req.chat_type.or_else(|| Some("local".to_string()));
    let root_message_id = req.root_message_id.unwrap_or_else(|| session_id.clone());
    let scope = req.scope.unwrap_or(SessionScope::Thread);
    let lark_app_secret = state
        .bots
        .get(&lark_app_id)
        .map(|bot| bot.lark_app_secret.clone())
        .unwrap_or_default();
    let session = Session {
        session_id: session_id.clone(),
        title,
        chat_id: chat_id.clone(),
        chat_type: chat_type.clone(),
        root_message_id: root_message_id.clone(),
        quote_target_id: None,
        scope,
        status: SessionStatus::Active,
        created_at: Utc::now(),
        closed_at: None,
        working_dir: Some(adopted_from.cwd.clone()),
        lark_app_id: lark_app_id.clone(),
        owner_open_id: req.owner_open_id.clone(),
        quote_target_sender_open_id: req.owner_open_id.clone(),
        worker_pid: None,
        cli_id: Some(req.cli_id.clone()),
        cli_bin: Some(req.cli_bin.clone()),
        cli_args: Vec::new(),
        cli_session_id: adopted_cli_session_id.clone(),
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
        adopted_from: Some(adopted_from.clone()),
        bot_name: None,
        bot_open_id: None,
        disable_cli_bypass: false,
        initial_prompt: None,
        model: None,
        locale: None,
        resume_session_id: None,
        thread_id: req.thread_id.clone(),
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
        working_dir: adopted_from.cwd.clone(),
        cli_id: req.cli_id,
        cli_bin: req.cli_bin,
        cli_args: Vec::new(),
        prompt: String::new(),
        resume: false,
        cli_session_id: adopted_cli_session_id,
        lark_app_id,
        lark_app_secret,
        prompt_turn_id: None,
        owner_open_id: req.owner_open_id,
        adopted_from: Some(adopted_from),
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
    spawn_worker(state.clone(), session.clone(), init)
        .await
        .map_err(internal_error)?;
    Ok((StatusCode::CREATED, Json(SessionSummary::from(&session))))
}

pub(crate) async fn final_output(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<FinalOutputRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    match handle_final_output_request(&state, &session_id, req).await {
        Ok(()) => Ok(StatusCode::ACCEPTED),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

pub(crate) async fn ensure_worker_for_session(state: &AppState, session_id: &str) -> Result<()> {
    {
        let workers = state.workers.lock().await;
        if workers.contains_key(session_id) {
            return Ok(());
        }
    }

    let session = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(session_id)
            .cloned()
            .with_context(|| format!("session not found: {}", session_id))?
    };
    if session.status != SessionStatus::Active {
        anyhow::bail!("session {} is not active", session_id);
    }
    {
        let target = session_zellij_target(&session);
        if !zellij_has_session(&target) {
            anyhow::bail!("zellij session is not available for {}", session_id);
        }
    }

    let init = build_init_from_session(&session, &state.config, &state.bots)?;
    spawn_worker(state.clone(), session, init).await
}

// ---------------------------------------------------------------------------
// Event outcome dispatch (from handle_lark_event_payload's match on outcome)
// ---------------------------------------------------------------------------

pub(crate) async fn dispatch_event_outcome(
    state: &AppState,
    bot: &BotConfig,
    app_id: &str,
    parsed: &ParsedLarkInboundMessage,
    text: &str,
    custom_trigger: Option<&CustomTrigger>,
    inferred_locale: &str,
    scope: &SessionScope,
    anchor: &str,
    sender_open_id: Option<&str>,
    talk: Option<&TalkEvaluation>,
    message_id: &str,
    chat_id: &str,
    existing: Option<Session>,
    outcome: LarkEventOutcome,
) -> Result<Json<Value>, (StatusCode, String)> {
    match outcome {
        LarkEventOutcome::ReplyOnly { reply } => {
            let _ = lark_reply_message(state, bot, message_id, &reply).await;
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::CloseSession { reply } => {
            if let Some(session) = existing {
                let result =
                    close_session(State(state.clone()), AxumPath(session.session_id.clone())).await;
                match result {
                    Ok(status) => {
                        let fallback = build_close_result_reply(&session, Ok(status));
                        let card = build_closed_session_card(&session);
                        if lark_reply_card(state, bot, message_id, &card)
                            .await
                            .is_err()
                        {
                            let _ = lark_reply_message(state, bot, message_id, &fallback).await;
                        }
                    }
                    Err((_, err)) => {
                        let reply = build_close_result_reply(&session, Err(err.as_str()));
                        let _ = lark_reply_message(state, bot, message_id, &reply).await;
                    }
                }
            } else {
                let _ = lark_reply_message(state, bot, message_id, &reply).await;
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::RestartSession { reply } => {
            if let Some(session) = existing {
                let result = restart_session(
                    State(state.clone()),
                    AxumPath(session.session_id.clone()),
                    Json(RestartSessionRequest {
                        prompt: String::new(),
                    }),
                )
                .await;
                let reply = match result {
                    Ok(status) => build_restart_result_reply(Ok(status)),
                    Err((_, err)) => build_restart_result_reply(Err(err.as_str())),
                };
                let _ = lark_reply_message(state, bot, message_id, &reply).await;
            } else {
                let _ = lark_reply_message(state, bot, message_id, &reply).await;
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::ShowCard { reply } => {
            if let Some(session) = existing {
                match post_or_refresh_lark_session_card(state, &session.session_id).await {
                    Ok(LarkCardDeliveryPlan::PostNew | LarkCardDeliveryPlan::PatchExisting) => {}
                    Ok(LarkCardDeliveryPlan::NotReady) => {
                        let _ = lark_reply_message(
                            state,
                            bot,
                            message_id,
                            build_card_not_ready_reply(),
                        )
                        .await;
                    }
                    Err(err) => {
                        let _ = lark_reply_message(
                            state,
                            bot,
                            message_id,
                            &format!("session card failed: {}", err),
                        )
                        .await;
                    }
                }
            } else {
                let _ = lark_reply_message(state, bot, message_id, &reply).await;
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::AdoptZellij { target } => {
            let (zellij_session, zellij_pane_id) = match target.split_once(':') {
                Some((s, p)) => (s.to_string(), p.to_string()),
                None => (target.clone(), "terminal_0".to_string()),
            };
            let result = adopt_zellij_session(
                State(state.clone()),
                Json(AdoptZellijSessionRequest {
                    zellij_session,
                    zellij_pane_id,
                    cli_id: bot.cli_id.clone(),
                    cli_bin: bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
                    title: Some(format!("adopt {}", target)),
                    cwd: String::new(),
                    pane_cols: None,
                    pane_rows: None,
                    lark_app_id: Some(app_id.to_string()),
                    chat_id: Some(chat_id.to_string()),
                    chat_type: parsed.chat_type.clone(),
                    root_message_id: Some(message_id.to_string()),
                    scope: Some(*scope),
                    thread_id: parsed.thread_id.clone(),
                    owner_open_id: parsed.sender_open_id.clone(),
                }),
            )
            .await;
            let reply_in_thread = *scope == SessionScope::Thread;
            match result {
                Ok((_, Json(session))) => {
                    let reply = build_adopt_zellij_result_reply(Ok(&session));
                    let _ = lark_reply_message_with_opts(
                        state,
                        bot,
                        message_id,
                        &reply,
                        reply_in_thread,
                    )
                    .await;
                }
                Err((_, err)) => {
                    let reply = build_adopt_zellij_result_reply(Err(err.as_str()));
                    let _ = lark_reply_message_with_opts(
                        state,
                        bot,
                        message_id,
                        &reply,
                        reply_in_thread,
                    )
                    .await;
                }
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::AdoptList => {
            let items = discover_zellij_adopt_candidates();
            if items.is_empty() {
                let _ = lark_reply_message(
                    state,
                    bot,
                    message_id,
                    "no zellij sessions available for adoption",
                )
                .await;
            } else {
                let post = build_zellij_adopt_post_content(&items);
                let _ = lark_reply_post_message(state, bot, message_id, &post).await;
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::PassthroughInput { text } => {
            if let Some(session) = existing {
                if let Some(quota_key) = talk.and_then(|talk| talk.quota_key.as_deref()) {
                    let quota = consume_inbound_quota(state, app_id, quota_key).await?;
                    if !quota.allowed {
                        let _ = lark_reply_message(state, bot, message_id, "quota exceeded").await;
                        return Ok(Json(
                            serde_json::json!({ "ok": true, "quota": "exhausted" }),
                        ));
                    }
                }
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    if let Some(entry) = sessions.get_mut(&session.session_id) {
                        update_session_from_lark_message(entry, parsed);
                        if entry.locale.is_none() {
                            entry.locale = Some(inferred_locale.to_string());
                        }
                    }
                    sessions.clone()
                };
                let _ = persist_sessions(&state.paths, &snapshot).await;
                let _ = send_input(
                    State(state.clone()),
                    AxumPath(session.session_id.clone()),
                    Json(SessionInputRequest {
                        content: text,
                        raw: true,
                    }),
                )
                .await;
                return Ok(Json(serde_json::json!({ "ok": true, "reused": true })));
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::ReuseSession => {
            if let Some(session) = existing {
                if let Some(quota_key) = talk.and_then(|talk| talk.quota_key.as_deref()) {
                    let quota = consume_inbound_quota(state, app_id, quota_key).await?;
                    if !quota.allowed {
                        let _ = lark_reply_message(state, bot, message_id, "quota exceeded").await;
                        return Ok(Json(
                            serde_json::json!({ "ok": true, "quota": "exhausted" }),
                        ));
                    }
                }
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    if let Some(entry) = sessions.get_mut(&session.session_id) {
                        update_session_from_lark_message(entry, parsed);
                        if entry.locale.is_none() {
                            entry.locale = Some(inferred_locale.to_string());
                        }
                    }
                    sessions.clone()
                };
                let _ = persist_sessions(&state.paths, &snapshot).await;
                let session_locale = snapshot
                    .get(&session.session_id)
                    .and_then(|entry| entry.locale.as_deref())
                    .unwrap_or(inferred_locale);
                let mut reuse_content = {
                    let session_root = &session.root_message_id;
                    let raw = prompt::build_quote_hint(
                        parsed.parent_id.as_deref(),
                        &parsed.message_id,
                        *scope,
                        session_root,
                    ) + &text;
                    prompt::build_follow_up_content(
                        &raw,
                        &prompt::FollowUpContentOptions {
                            session_id: &session.session_id,
                            sender_open_id: parsed.sender_open_id.as_deref(),
                            sender_type: parsed.sender_type.as_deref(),
                            mentions: &parsed.mentions,
                            cli_id: session.cli_id.as_deref().unwrap_or("codex"),
                            locale: Some(session_locale),
                        },
                    )
                };

                // For adopted sessions, prepend beam context on the first message
                if session.adopted_from.is_some() && session.last_cli_input.is_none() {
                    let (bot_name, bot_open_id) = if app_id != "local" {
                        load_bot_identity(&state.paths, app_id)
                    } else {
                        (None, None)
                    };
                    let observed_bots = load_observed_bots_for_chat(&state.paths, app_id, chat_id);
                    let context = prompt::build_adopt_context(&prompt::AdoptContextOptions {
                        bot_name: bot_name.as_deref(),
                        bot_open_id: bot_open_id.as_deref(),
                        observed_bots: &observed_bots,
                        locale: Some(session_locale),
                    });
                    reuse_content = format!("{}\n\n{}", context, reuse_content);
                }

                let _ = send_input(
                    State(state.clone()),
                    AxumPath(session.session_id.clone()),
                    Json(SessionInputRequest {
                        content: reuse_content,
                        raw: false,
                    }),
                )
                .await;
                return Ok(Json(serde_json::json!({ "ok": true, "reused": true })));
            }
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        LarkEventOutcome::CreateSession => {
            let (effective_text, title) = match custom_trigger {
                Some(trigger) => (
                    resolve_trigger_message(text, trigger),
                    trigger.trigger.chars().take(32).collect::<String>(),
                ),
                None => (text.to_string(), text.chars().take(32).collect::<String>()),
            };
            // Acknowledge the trigger immediately so users know the task was
            // accepted before the long-running work produces output.
            if let Some(ack) = custom_trigger
                .and_then(|trigger| trigger.ack_message.as_deref())
                .filter(|ack| !ack.is_empty())
            {
                let _ = lark_reply_message(state, bot, message_id, ack).await;
            }
            let root_working_dir = dir_select::determine_root_working_dir(
                bot.working_dir.as_deref(),
                &state.config.daemon.working_dirs,
            );
            let root_message_id = parsed
                .root_id
                .clone()
                .unwrap_or_else(|| message_id.to_string());
            let quota_key = talk
                .and_then(|t| t.quota_key.as_deref())
                .map(|s| s.to_string());

            // A trigger can opt out of the directory selection card and pin
            // its own working dir; otherwise the bot-level setting applies.
            let skip_dir_select = bot.skip_working_dir_prompt
                || custom_trigger.map_or(false, |trigger| trigger.skip_dir_select);
            if skip_dir_select {
                let mentions = parsed.mentions.clone();
                let prompt_raw = prompt::build_quote_hint(
                    parsed.parent_id.as_deref(),
                    message_id,
                    *scope,
                    &root_message_id,
                ) + &effective_text;
                let prompt = if bot.cli_id == "opencode" {
                    let (bot_name, bot_open_id) = load_bot_identity(&state.paths, &bot.lark_app_id);
                    let observed_bots =
                        load_observed_bots_for_chat(&state.paths, &bot.lark_app_id, chat_id);
                    prompt::build_initial_prompt(&prompt::InitialPromptOptions {
                        user_message: &prompt_raw,
                        session_id: "pending",
                        sender_open_id,
                        sender_type: parsed.sender_type.as_deref(),
                        mentions: &mentions,
                        bot_name: bot_name.as_deref(),
                        bot_open_id: bot_open_id.as_deref(),
                        observed_bots: &observed_bots,
                        follow_ups: &Vec::new(),
                        locale: Some(inferred_locale),
                    })
                } else {
                    prompt::build_follow_up_content(
                        &prompt_raw,
                        &prompt::FollowUpContentOptions {
                            session_id: "pending",
                            sender_open_id,
                            sender_type: parsed.sender_type.as_deref(),
                            mentions: &mentions,
                            cli_id: bot.cli_id.as_str(),
                            locale: Some(inferred_locale),
                        },
                    )
                };
                if let Some(quota_key) = quota_key.as_deref() {
                    let quota = consume_inbound_quota(state, app_id, quota_key).await?;
                    if !quota.allowed {
                        let _ = lark_reply_message(state, bot, message_id, "quota exceeded").await;
                        return Ok(Json(serde_json::json!({
                            "ok": true,
                            "quota": "exhausted",
                        })));
                    }
                }
                let session = create_session_internal(
                    state,
                    build_direct_create_session_spec_from_bot(
                        bot,
                        &state.config.daemon.working_dirs,
                        custom_trigger.and_then(|trigger| trigger.working_dir.clone()),
                        title.clone(),
                        chat_id.to_string(),
                        parsed.chat_type.clone(),
                        root_message_id,
                        Some(message_id.to_string()),
                        *scope,
                        parsed.thread_id.clone(),
                        prompt,
                        app_id.to_string(),
                        sender_open_id.map(|s| s.to_string()),
                        Some(inferred_locale.to_string()),
                        None,
                    ),
                )
                .await
                .map_err(internal_error)?;
                info!(
                    app_id = %app_id,
                    chat_id = %chat_id,
                    chat_type = ?parsed.chat_type,
                    scope = ?scope,
                    message_id = %message_id,
                    session_id = %session.session_id,
                    working_dir = %root_working_dir,
                    skip_working_dir_prompt = true,
                    "created session without dir select"
                );
                return Ok(Json(serde_json::json!({
                    "ok": true,
                    "direct_create": true,
                    "sessionId": session.session_id,
                    "workingDir": root_working_dir,
                })));
            }

            let root_path = std::path::Path::new(&root_working_dir);
            let candidate_dirs = dir_select::scan_candidate_dirs(root_path);
            let recent_path = state.paths.root().join("recent-dirs.json");
            let recent_store = dir_select::load_recent_dirs(&recent_path)
                .await
                .unwrap_or_default();
            let recent_key = dir_select::build_recent_dir_key(app_id, chat_id, sender_open_id);
            let recent_dirs =
                dir_select::get_recent_dirs(&recent_store, &recent_key, &root_working_dir);
            let mut recommended: Vec<String> = Vec::new();
            recommended.push(".".to_string());
            for rd in &recent_dirs {
                if candidate_dirs.contains(rd) && !recommended.contains(rd) {
                    recommended.push(rd.clone());
                }
                if recommended.len() >= 8 {
                    break;
                }
            }
            let kwds = dir_select::tokenize_keywords(&effective_text);
            if !kwds.is_empty() && recommended.len() < 8 {
                let kw_refs: Vec<&str> = kwds.iter().map(|s| s.as_str()).collect();
                let keyword_matched = dir_select::match_dirs(&candidate_dirs, &kw_refs);
                for km in &keyword_matched {
                    if !recommended.contains(km) {
                        recommended.push(km.clone());
                    }
                    if recommended.len() >= 8 {
                        break;
                    }
                }
            }

            let pending_id = Uuid::new_v4().to_string();
            let pending = dir_select::PendingCreateSession {
                pending_id: pending_id.clone(),
                lark_app_id: app_id.to_string(),
                chat_id: chat_id.to_string(),
                chat_type: parsed.chat_type.clone(),
                message_id: message_id.to_string(),
                anchor: anchor.to_string(),
                scope: *scope,
                thread_id: parsed.thread_id.clone(),
                root_id: parsed.root_id.clone(),
                title: title.clone(),
                text: effective_text,
                sender_open_id: sender_open_id.map(|s| s.to_string()),
                sender_type: parsed.sender_type.clone(),
                locale: Some(inferred_locale.to_string()),
                parent_id: parsed.parent_id.clone(),
                mentions_json: serde_json::to_string(&parsed.mentions).unwrap_or_default(),
                quota_key,
                created_at: Utc::now().timestamp_millis(),
                cli_id: bot.cli_id.clone(),
                cli_bin: bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
                cli_args: bot.cli_args.clone(),
                root_working_dir: root_working_dir.clone(),
                candidate_dirs: candidate_dirs.clone(),
                card_message_id: None,
            };
            let card = dir_select::build_dir_select_card(
                &pending_id,
                &root_working_dir,
                &title,
                &recommended,
                &candidate_dirs,
                None,
                None,
                None,
                pending.locale.as_deref(),
            );
            let reply_in_thread = *scope == SessionScope::Thread;
            let card_message_id =
                match lark_reply_card_with_opts(state, bot, message_id, &card, reply_in_thread)
                    .await
                {
                    Ok(card_message_id) => card_message_id,
                    Err(err) => return Err(internal_error(err)),
                };
            {
                let mut pending_map = state.pending_creates.lock().await;
                let now_ms = Utc::now().timestamp_millis();
                dir_select::prune_expired_pending_creates(&mut pending_map, now_ms);
                let mut entry = pending;
                entry.card_message_id = Some(card_message_id);
                pending_map.insert(pending_id, entry);
                let snapshot: Vec<_> = pending_map.values().cloned().collect();
                drop(pending_map);
                let paths = state.paths.clone();
                tokio::task::spawn_blocking(move || {
                    let _ = beam_core::persist::atomic_write_json(
                        &paths.pending_creates_json(),
                        &snapshot,
                    );
                })
                .await
                .ok();
            }

            Ok(Json(serde_json::json!({ "ok": true, "dir_select": true })))
        }
    }
}
