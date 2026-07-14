use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LarkEventOutcome {
    CloseSession { reply: String },
    RestartSession { reply: String },
    ShowCard { reply: String },
    AdoptZellij { target: String },
    AdoptList,
    PassthroughInput { text: String },
    ReplyOnly { reply: String },
    ReuseSession,
    CreateSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedLarkInboundMessage {
    pub(crate) event_id: String,
    pub(crate) message_id: String,
    pub(crate) chat_id: String,
    pub(crate) chat_type: Option<String>,
    pub(crate) sender_type: Option<String>,
    pub(crate) scope: SessionScope,
    pub(crate) anchor: String,
    pub(crate) text: String,
    pub(crate) sender_open_id: Option<String>,
    pub(crate) mentions: Vec<LarkEventMention>,
    pub(crate) parent_id: Option<String>,
    pub(crate) root_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) locale: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LarkPreflight {
    Continue,
    Deduped,
    Denied { reply: &'static str },
    IgnoredEmptyText,
}

pub(crate) async fn handle_lark_event_payload(
    state: AppState,
    app_id: String,
    payload: Value,
    http_verification: Option<(HeaderMap, Bytes)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    if let Some(challenge) = payload.get("challenge").and_then(|v| v.as_str()) {
        return Ok(Json(serde_json::json!({ "challenge": challenge })));
    }

    let event_type = payload
        .pointer("/header/event_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event_type != "im.message.receive_v1" {
        return Ok(Json(
            serde_json::json!({ "ok": true, "ignored": event_type }),
        ));
    }

    let bot = state
        .bots
        .get(&app_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;
    if let Some((headers, body)) = http_verification.as_ref() {
        verify_lark_signature(&state, &bot, headers, body)
            .map_err(|err| (StatusCode::UNAUTHORIZED, err.to_string()))?;
        verify_lark_token(&state, &bot, &payload)
            .map_err(|err| (StatusCode::UNAUTHORIZED, err.to_string()))?;
    }
    let mut parsed = parse_lark_inbound_message(&payload)?;

    if parsed.scope == SessionScope::Chat && parsed.chat_type.as_deref() != Some("p2p") {
        let force_refresh = {
            let sessions = state.sessions.lock().await;
            sessions.values().any(|s| {
                s.scope == SessionScope::Chat
                    && s.chat_id == parsed.chat_id
                    && s.status == SessionStatus::Active
            })
        };
        match get_lark_chat_mode(&state, &bot, &parsed.chat_id, force_refresh).await {
            Ok(ChatMode::Topic) => {
                parsed.scope = SessionScope::Thread;
                parsed.chat_type = Some("topic".to_string());
                parsed.anchor = parsed
                    .thread_id
                    .clone()
                    .unwrap_or_else(|| parsed.message_id.clone());
            }
            Err(err) => {
                warn!(
                    "[{}] failed to fetch chat mode for {}: {}",
                    app_id, parsed.chat_id, err
                );
            }
            _ => {}
        }
    }

    let deduped = if let Some(key) = lark_event_dedupe_key(&app_id, &parsed.event_id) {
        dedupe_lark_event(&state, &key).await
    } else {
        false
    };
    let message_id = parsed.message_id.as_str();
    let chat_id = parsed.chat_id.as_str();
    let text = parsed.text.clone();

    let text = if let Some(stripped) = parse_force_topic_invocation(&text) {
        if parsed.scope == SessionScope::Chat {
            parsed.scope = SessionScope::Thread;
            parsed.anchor = parsed.message_id.clone();
        }
        stripped
    } else {
        text
    };
    let inferred_locale = prompt::infer_prompt_locale(&text);
    let scope = parsed.scope;
    let anchor = parsed.anchor.as_str();
    let sender_open_id = parsed.sender_open_id.clone();
    let sender_type = parsed.sender_type.as_deref();
    let self_bot_open_id = load_self_bot_open_id_for_app(&state.paths, &app_id);
    let mentioned_self_bot = current_bot_is_mentioned(&state.paths, &app_id, &parsed);
    let group_stats = if parsed.chat_type.as_deref() != Some("p2p") {
        match lark_group_stats(&state, &bot, chat_id).await {
            Ok(stats) => Some(stats),
            Err(err) => {
                warn!(
                    "[{}] failed to fetch group stats for {}: {}",
                    app_id, chat_id, err
                );
                None
            }
        }
    } else {
        None
    };
    let owns_session = {
        let sessions = state.sessions.lock().await;
        sessions.values().any(|s| {
            s.chat_id == parsed.chat_id
                && s.lark_app_id == app_id
                && s.status == SessionStatus::Active
        })
    };
    let is_oncall_chat = bot
        .oncall_chats
        .iter()
        .any(|oc| oc.chat_id == parsed.chat_id);
    let peer_ids = peer_bot_open_ids_for_app(&state.paths, &app_id);
    let is_known_peer_bot = sender_open_id
        .as_deref()
        .map(|sid| peer_ids.iter().any(|id| id == sid))
        .unwrap_or(false);
    let has_chat_grant = sender_open_id
        .as_deref()
        .map(|sid| {
            bot.chat_grants
                .get(&parsed.chat_id)
                .map(|granted| granted.iter().any(|id| id == sid))
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let has_global_grant = sender_open_id
        .as_deref()
        .map(|sid| bot.global_grants.iter().any(|id| id == sid))
        .unwrap_or(false);
    if !decide_multibot_inbound_gate(
        sender_type,
        sender_open_id.as_deref(),
        self_bot_open_id.as_deref(),
        mentioned_self_bot,
        parsed.chat_type.as_deref(),
        scope,
        is_oncall_chat,
        owns_session,
        is_known_peer_bot,
        has_chat_grant,
        has_global_grant,
        group_stats,
        &text,
    ) {
        return Ok(Json(
            serde_json::json!({ "ok": true, "ignored": "multi_bot_gate" }),
        ));
    }
    match evaluate_lark_preflight(
        &state,
        &bot,
        &text,
        chat_id,
        sender_open_id.as_deref(),
        deduped,
    ) {
        LarkPreflight::Deduped => {
            return Ok(Json(serde_json::json!({ "ok": true, "deduped": true })));
        }
        LarkPreflight::IgnoredEmptyText => {
            return Ok(Json(
                serde_json::json!({ "ok": true, "ignored": "empty_text" }),
            ));
        }
        LarkPreflight::Denied { reply } => {
            let _ = lark_reply_message(&state, &bot, message_id, reply).await;
            return Ok(Json(serde_json::json!({ "ok": true, "denied": true })));
        }
        LarkPreflight::Continue => {}
    }

    if handle_introduce_command(&state, &app_id, chat_id, message_id, &parsed).await? {
        return Ok(Json(serde_json::json!({ "ok": true, "introduced": true })));
    }

    let talk = sender_open_id
        .as_deref()
        .map(|sender| evaluate_talk_for_bot_with_state(&state, &bot, chat_id, sender));

    async fn try_handle_grant_command(
        state: &AppState,
        bot: &BotConfig,
        message_id: &str,
        lark_app_id: &str,
        chat_id: &str,
        sender_open_id: Option<&str>,
        text: &str,
    ) -> Option<()> {
        let sender = sender_open_id?;
        let ctx = grant::GrantContext {
            lark_app_id: lark_app_id.to_string(),
            chat_id: chat_id.to_string(),
            sender_open_id: sender.to_string(),
            resolved_allowed_users: state
                .bots
                .get(lark_app_id)
                .map(|b| b.allowed_users.clone())
                .unwrap_or_default(),
            peer_bot_open_ids: peer_bot_open_ids_for_app(&state.paths, lark_app_id),
        };

        let cmd = grant::parse_grant_command(text, None, &ctx)?;
        let owner_open_id = ctx.resolved_allowed_users.first()?;

        if sender != owner_open_id {
            let _ = lark_reply_message(
                state,
                bot,
                message_id,
                "permission denied: only the bot owner can grant access",
            )
            .await;
            return Some(());
        }

        let bots_path = state.paths.bots_json();
        let raw = tokio::fs::read_to_string(&bots_path).await.ok()?;
        let mut config: serde_json::Value = serde_json::from_str(&raw).ok()?;

        match &cmd.action {
            grant::GrantAction::GrantAll => {
                if let Err(e) = grant::add_allowed_chat_group(&mut config, lark_app_id, chat_id) {
                    let _ =
                        lark_reply_message(state, bot, message_id, &format!("grant failed: {}", e))
                            .await;
                    return Some(());
                }
                if let Err(e) = tokio::fs::write(
                    &bots_path,
                    serde_json::to_string_pretty(&config).unwrap_or_default(),
                )
                .await
                {
                    let _ =
                        lark_reply_message(state, bot, message_id, &format!("save failed: {}", e))
                            .await;
                    return Some(());
                }
                let _ = lark_reply_message(
                    state,
                    bot,
                    message_id,
                    "granted: all members in this chat can now talk to the bot",
                )
                .await;
                return Some(());
            }
            grant::GrantAction::Grant => {
                let targets: Vec<String> = cmd.targets.iter().map(|t| t.open_id.clone()).collect();
                if targets.is_empty() {
                    let _ =
                        lark_reply_message(state, bot, message_id, "usage: /grant @user [quota]")
                            .await;
                    return Some(());
                }
                let nonce = uuid::Uuid::new_v4().to_string();
                let card = grant::build_grant_card(&targets, &nonce, chat_id, cmd.quota);
                let mut pending = state.grant_pending.lock().await;
                for target in &targets {
                    let key = format!("{}:{}:{}", lark_app_id, chat_id, target);
                    pending.insert(
                        key,
                        grant::GrantPendingEntry {
                            nonce: nonce.clone(),
                            targets: targets.clone(),
                            quota: cmd.quota,
                            ts: Utc::now().timestamp_millis() as u64,
                            state: grant::GrantPendingState::Pending,
                        },
                    );
                }
                {
                    let snapshot: HashMap<String, grant::GrantPendingEntry> = pending
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    drop(pending);
                    grant::save_grant_pending(&state.paths, &snapshot);
                }
                let card_str = card.to_string();
                if let Err(e) = lark_reply_card(state, bot, message_id, &card_str).await {
                    warn!("failed to send grant card: {}", e);
                }
                return Some(());
            }
            grant::GrantAction::Revoke => {
                let targets: Vec<String> = cmd.targets.iter().map(|t| t.open_id.clone()).collect();
                if targets.is_empty() {
                    let _ =
                        lark_reply_message(state, bot, message_id, "usage: /revoke @user").await;
                    return Some(());
                }
                let mut results = Vec::new();
                for target in &targets {
                    match grant::revoke_grant(
                        &mut config,
                        lark_app_id,
                        chat_id,
                        target,
                        &ctx.resolved_allowed_users,
                    ) {
                        Ok(()) => results.push(format!("revoked @{}", target)),
                        Err(e) => results.push(format!("revoke @{} failed: {}", target, e)),
                    }
                }
                if let Err(e) = tokio::fs::write(
                    &bots_path,
                    serde_json::to_string_pretty(&config).unwrap_or_default(),
                )
                .await
                {
                    let _ =
                        lark_reply_message(state, bot, message_id, &format!("save failed: {}", e))
                            .await;
                    return Some(());
                }
                let _ = lark_reply_message(state, bot, message_id, &results.join("\n")).await;
                return Some(());
            }
        }
    }

    if try_handle_grant_command(
        &state,
        &bot,
        message_id,
        &app_id,
        chat_id,
        sender_open_id.as_deref(),
        &text,
    )
    .await
    .is_some()
    {
        return Ok(Json(serde_json::json!({ "ok": true, "grant": true })));
    }

    if let Some(workflow_command) = parse_workflow_text_command(&text) {
        match workflow_command {
            WorkflowTextCommand::Invalid { error, usage } => {
                let _ =
                    lark_reply_message(&state, &bot, message_id, &format!("{}\n{}", error, usage))
                        .await;
                return Ok(Json(
                    serde_json::json!({ "ok": true, "workflow": "invalid" }),
                ));
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
                    &state,
                    &workflow_id,
                    &raw_def,
                    &params_map,
                    "lark",
                    Some(RunChatBinding {
                        chat_id: chat_id.to_string(),
                        lark_app_id: app_id.clone(),
                    }),
                )
                .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        let reply = format!("workflow run failed: {}", e);
                        let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
                        return Ok(Json(serde_json::json!({
                            "ok": true,
                            "workflow": "failed",
                        })));
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
                let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
                return Ok(Json(serde_json::json!({
                    "ok": true,
                    "workflow": "run",
                    "runId": bootstrap.run_id,
                })));
            }
            WorkflowTextCommand::Cancel { run_id } => {
                let reply = format!("workflow cancel requested: {}", run_id);
                let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
                return Ok(Json(
                    serde_json::json!({ "ok": true, "workflow": "cancel" }),
                ));
            }
        }
    }

    let (existing, outcome) = {
        let sessions = state.sessions.lock().await;
        decide_lark_dispatch(&sessions, &app_id, &parsed)
    };
    info!(
        app_id = %app_id,
        chat_id = %parsed.chat_id,
        chat_type = ?parsed.chat_type,
        message_id = %parsed.message_id,
        root_id = ?parsed.root_id,
        parent_id = ?parsed.parent_id,
        thread_id = ?parsed.thread_id,
        locale = ?parsed.locale,
        scope = ?parsed.scope,
        anchor = %parsed.anchor,
        existing_session_id = ?existing.as_ref().map(|s| s.session_id.as_str()),
        existing_thread_id = ?existing.as_ref().and_then(|s| s.thread_id.as_deref()),
        existing_root_message_id = ?existing.as_ref().map(|s| s.root_message_id.as_str()),
        outcome = ?outcome,
        "lark message dispatch",
    );
    match outcome {
        LarkEventOutcome::ReplyOnly { reply } => {
            let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        LarkEventOutcome::CloseSession { reply } => {
            if let Some(session) = existing {
                let result =
                    close_session(State(state.clone()), AxumPath(session.session_id.clone())).await;
                match result {
                    Ok(status) => {
                        let fallback = build_close_result_reply(&session, Ok(status));
                        let card = build_closed_session_card(&session);
                        if lark_reply_card(&state, &bot, message_id, &card)
                            .await
                            .is_err()
                        {
                            let _ = lark_reply_message(&state, &bot, message_id, &fallback).await;
                        }
                    }
                    Err((_, err)) => {
                        let reply = build_close_result_reply(&session, Err(err.as_str()));
                        let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
                    }
                }
            } else {
                let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
            }
            return Ok(Json(serde_json::json!({ "ok": true })));
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
                let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
            } else {
                let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
            }
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        LarkEventOutcome::ShowCard { reply } => {
            if let Some(session) = existing {
                match post_or_refresh_lark_session_card(&state, &session.session_id).await {
                    Ok(LarkCardDeliveryPlan::PostNew | LarkCardDeliveryPlan::PatchExisting) => {}
                    Ok(LarkCardDeliveryPlan::NotReady) => {
                        let _ = lark_reply_message(
                            &state,
                            &bot,
                            message_id,
                            build_card_not_ready_reply(),
                        )
                        .await;
                    }
                    Err(err) => {
                        let _ = lark_reply_message(
                            &state,
                            &bot,
                            message_id,
                            &format!("session card failed: {}", err),
                        )
                        .await;
                    }
                }
            } else {
                let _ = lark_reply_message(&state, &bot, message_id, &reply).await;
            }
            return Ok(Json(serde_json::json!({ "ok": true })));
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
                    lark_app_id: Some(app_id.clone()),
                    chat_id: Some(chat_id.to_string()),
                    chat_type: parsed.chat_type.clone(),
                    root_message_id: Some(message_id.to_string()),
                    scope: Some(scope),
                    thread_id: parsed.thread_id.clone(),
                    owner_open_id: sender_open_id.clone(),
                }),
            )
            .await;
            let reply_in_thread = scope == SessionScope::Thread;
            match result {
                Ok((_, Json(session))) => {
                    let reply = build_adopt_zellij_result_reply(Ok(&session));
                    let _ = lark_reply_message_with_opts(
                        &state,
                        &bot,
                        message_id,
                        &reply,
                        reply_in_thread,
                    )
                    .await;
                }
                Err((_, err)) => {
                    let reply = build_adopt_zellij_result_reply(Err(err.as_str()));
                    let _ = lark_reply_message_with_opts(
                        &state,
                        &bot,
                        message_id,
                        &reply,
                        reply_in_thread,
                    )
                    .await;
                }
            }
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        LarkEventOutcome::AdoptList => {
            let items = discover_zellij_adopt_candidates();
            if items.is_empty() {
                let _ = lark_reply_message(
                    &state,
                    &bot,
                    message_id,
                    "no zellij sessions available for adoption",
                )
                .await;
            } else {
                let body = build_zellij_adopt_list_reply(&items);
                let _ = lark_reply_message(&state, &bot, message_id, &body).await;
            }
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        LarkEventOutcome::PassthroughInput { text } => {
            if let Some(session) = existing {
                if let Some(quota_key) = talk.as_ref().and_then(|talk| talk.quota_key.as_deref()) {
                    let quota = consume_inbound_quota(&state, &app_id, quota_key).await?;
                    if !quota.allowed {
                        let _ =
                            lark_reply_message(&state, &bot, message_id, "quota exceeded").await;
                        return Ok(Json(
                            serde_json::json!({ "ok": true, "quota": "exhausted" }),
                        ));
                    }
                }
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    if let Some(entry) = sessions.get_mut(&session.session_id) {
                        update_session_from_lark_message(entry, &parsed);
                        if entry.locale.is_none() {
                            entry.locale = Some(inferred_locale.to_string());
                        }
                    }
                    sessions.clone()
                };
                let _ = persist_sessions(&state.paths, &snapshot).await;
                let _ = send_input(
                    State(state.clone()),
                    AxumPath(session.session_id),
                    Json(SessionInputRequest {
                        content: text,
                        raw: true,
                    }),
                )
                .await;
                return Ok(Json(serde_json::json!({ "ok": true, "reused": true })));
            }
        }
        LarkEventOutcome::ReuseSession => {
            if let Some(session) = existing {
                if let Some(quota_key) = talk.as_ref().and_then(|talk| talk.quota_key.as_deref()) {
                    let quota = consume_inbound_quota(&state, &app_id, quota_key).await?;
                    if !quota.allowed {
                        let _ =
                            lark_reply_message(&state, &bot, message_id, "quota exceeded").await;
                        return Ok(Json(
                            serde_json::json!({ "ok": true, "quota": "exhausted" }),
                        ));
                    }
                }
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    if let Some(entry) = sessions.get_mut(&session.session_id) {
                        update_session_from_lark_message(entry, &parsed);
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
                        scope,
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
                // so the LLM knows how to use beam send, its identity, etc.
                if session.adopted_from.is_some() && session.last_cli_input.is_none() {
                    let (bot_name, bot_open_id) = if app_id != "local" {
                        load_bot_identity(&state.paths, &app_id)
                    } else {
                        (None, None)
                    };
                    let observed_bots = load_observed_bots_for_chat(&state.paths, &app_id, chat_id);
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
                    AxumPath(session.session_id),
                    Json(SessionInputRequest {
                        content: reuse_content,
                        raw: false,
                    }),
                )
                .await;
                return Ok(Json(serde_json::json!({ "ok": true, "reused": true })));
            }
        }
        LarkEventOutcome::CreateSession => {
            let root_working_dir = dir_select::determine_root_working_dir(
                bot.working_dir.as_deref(),
                &state.config.daemon.working_dirs,
            );
            let root_message_id = parsed
                .root_id
                .clone()
                .unwrap_or_else(|| message_id.to_string());
            let title = text.chars().take(32).collect::<String>();
            let quota_key = talk
                .as_ref()
                .and_then(|t| t.quota_key.as_deref())
                .map(|s| s.to_string());

            if bot.skip_working_dir_prompt {
                let mentions = parsed.mentions.clone();
                let prompt_raw = prompt::build_quote_hint(
                    parsed.parent_id.as_deref(),
                    &parsed.message_id,
                    scope,
                    &root_message_id,
                ) + &text;
                let prompt = if bot.cli_id == "opencode" {
                    let (bot_name, bot_open_id) = load_bot_identity(&state.paths, &bot.lark_app_id);
                    let observed_bots =
                        load_observed_bots_for_chat(&state.paths, &bot.lark_app_id, chat_id);
                    prompt::build_initial_prompt(&prompt::InitialPromptOptions {
                        user_message: &prompt_raw,
                        session_id: "pending",
                        sender_open_id: sender_open_id.as_deref(),
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
                            sender_open_id: sender_open_id.as_deref(),
                            sender_type: parsed.sender_type.as_deref(),
                            mentions: &mentions,
                            cli_id: bot.cli_id.as_str(),
                            locale: Some(inferred_locale),
                        },
                    )
                };
                if let Some(quota_key) = quota_key.as_deref() {
                    let quota = consume_inbound_quota(&state, &app_id, quota_key).await?;
                    if !quota.allowed {
                        let _ =
                            lark_reply_message(&state, &bot, message_id, "quota exceeded").await;
                        return Ok(Json(serde_json::json!({
                            "ok": true,
                            "quota": "exhausted",
                        })));
                    }
                }
                let session = create_session_internal(
                    &state,
                    build_direct_create_session_spec_from_bot(
                        &bot,
                        &state.config.daemon.working_dirs,
                        title.clone(),
                        chat_id.to_string(),
                        parsed.chat_type.clone(),
                        root_message_id,
                        Some(message_id.to_string()),
                        scope,
                        parsed.thread_id.clone(),
                        prompt,
                        app_id.clone(),
                        sender_open_id.clone(),
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
            let recent_key =
                dir_select::build_recent_dir_key(&app_id, chat_id, sender_open_id.as_deref());
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
            let kwds = dir_select::tokenize_keywords(&text);
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
                lark_app_id: app_id.clone(),
                chat_id: chat_id.to_string(),
                chat_type: parsed.chat_type.clone(),
                message_id: message_id.to_string(),
                anchor: anchor.to_string(),
                scope,
                thread_id: parsed.thread_id.clone(),
                root_id: parsed.root_id.clone(),
                title: title.clone(),
                text: text.clone(),
                sender_open_id: sender_open_id.clone(),
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
            let reply_in_thread = scope == SessionScope::Thread;
            let card_message_id =
                match lark_reply_card_with_opts(&state, &bot, message_id, &card, reply_in_thread)
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

            return Ok(Json(serde_json::json!({ "ok": true, "dir_select": true })));
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn build_terminal_link_choice_card_json(
    state: &AppState,
    session: &Session,
    permission: terminal_auth::TerminalPermission,
    header_zh: &str,
    header_en: &str,
    body_zh: &str,
    body_en: &str,
) -> String {
    let candidate_hosts = external_host_candidates(&state.config.web.host);
    let candidates = terminal_link_choice_candidates(
        session,
        permission,
        &candidate_hosts,
        state.config.web.proxy_base_port,
    );
    build_terminal_link_choice_card(session, header_zh, header_en, body_zh, body_en, &candidates)
}

pub(crate) async fn handle_lark_card_action_payload(
    state: &AppState,
    app_id: &str,
    payload: Value,
) -> Result<Json<Value>, (StatusCode, String)> {
    let bot = state
        .bots
        .get(app_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;

    async fn handle_dir_select_card_action(
        state: &AppState,
        bot: &BotConfig,
        app_id: &str,
        action: &ParsedLarkCardAction,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        let pending_id = action
            .pending_id
            .as_deref()
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing pending_id".to_string()))?;

        // Prune expired pending entries before any access
        {
            let mut pending_map = state.pending_creates.lock().await;
            let now_ms = Utc::now().timestamp_millis();
            dir_select::prune_expired_pending_creates(&mut pending_map, now_ms);
        }

        match action.action.as_str() {
            "dir_select_pick" => {
                // Read pending first for validation
                let pending = {
                    let pending_map = state.pending_creates.lock().await;
                    pending_map.get(pending_id).cloned()
                };
                let Some(pending) = pending else {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "session creation expired, please send a new message",
                    )));
                };
                if pending.lark_app_id != app_id {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "permission denied",
                    )));
                }

                let working_dir_rel = action
                    .working_dir
                    .as_deref()
                    .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing working_dir".to_string()))?;

                // Validate against the pending's root and candidates
                if !dir_select::is_valid_candidate(
                    working_dir_rel,
                    &pending.root_working_dir,
                    &pending.candidate_dirs,
                ) {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        &format!("directory '{}' is not a valid candidate", working_dir_rel),
                    )));
                }

                // Atomically remove pending to prevent double-create (dir_select_pick)
                let pending = {
                    let mut pending_map = state.pending_creates.lock().await;
                    let removed = pending_map.remove(pending_id);
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
                    removed
                };
                let Some(pending) = pending else {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "session already being created, please wait",
                    )));
                };

                let working_dir =
                    dir_select::resolve_dir(&pending.root_working_dir, working_dir_rel);

                // Consume quota if applicable
                if let Some(quota_key) = pending.quota_key.as_deref() {
                    let quota = consume_inbound_quota(state, app_id, quota_key).await?;
                    if !quota.allowed {
                        return Ok(Json(build_lark_card_action_toast(
                            "error",
                            "quota exceeded",
                        )));
                    }
                }

                create_session_from_pending(state, bot, &pending, &working_dir, working_dir_rel)
                    .await
            }

            "dir_select_filter" => {
                // Read-only: just get a clone, don't remove
                let pending = {
                    let pending_map = state.pending_creates.lock().await;
                    pending_map.get(pending_id).cloned()
                };
                let Some(pending) = pending else {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "session creation expired, please send a new message",
                    )));
                };
                if pending.lark_app_id != app_id {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "permission denied",
                    )));
                }

                let keyword = action.dir_search_keyword.as_deref().unwrap_or("").trim();

                let filtered = if keyword.is_empty() {
                    // Empty keyword → show all candidates (capped in card builder)
                    Some(pending.candidate_dirs.clone())
                } else {
                    let f = dir_select::filter_dirs(&pending.candidate_dirs, keyword);
                    Some(f)
                };

                let message = if let Some(ref f) = filtered {
                    if f.is_empty() {
                        if prompt::is_zh_locale(pending.locale.as_deref()) {
                            Some(format!(
                                "⚠️ 没有目录匹配关键词 \"{}\"，请尝试其他关键词。",
                                keyword
                            ))
                        } else {
                            Some(format!(
                                "⚠️ No directory matches keyword \"{}\". Try another keyword.",
                                keyword
                            ))
                        }
                    } else if f.len() == 1 {
                        None
                    } else {
                        None
                    }
                } else {
                    None
                };

                let card = dir_select::build_dir_select_card(
                    pending_id,
                    &pending.root_working_dir,
                    &pending.title,
                    &[],
                    &pending.candidate_dirs,
                    filtered.as_deref(),
                    if keyword.is_empty() {
                        None
                    } else {
                        Some(keyword)
                    },
                    message.as_deref(),
                    pending.locale.as_deref(),
                );

                // PATCH the card message as a fallback (primary update is via response card field)
                if let Some(card_msg_id) = &pending.card_message_id {
                    if let Err(e) = lark_update_card(state, bot, card_msg_id, &card).await {
                        warn!(
                            "dir_select_filter: PATCH card for {} failed: {:?}",
                            pending_id, e
                        );
                    }
                }

                let card_data = serde_json::from_str::<Value>(&card).unwrap_or(Value::Null);
                let toast_msg = if keyword.is_empty() {
                    if prompt::is_zh_locale(pending.locale.as_deref()) {
                        "已显示全部目录".to_string()
                    } else {
                        "Showing all directories".to_string()
                    }
                } else {
                    if prompt::is_zh_locale(pending.locale.as_deref()) {
                        format!("已筛选 \"{}\"", keyword)
                    } else {
                        format!("Filtered \"{}\"", keyword)
                    }
                };
                Ok(Json(serde_json::json!({
                    "toast": { "type": "success", "content": toast_msg },
                    "card": { "type": "raw", "data": card_data }
                })))
            }

            "dir_select_best" => {
                // Read pending first for validation & match
                let pending = {
                    let pending_map = state.pending_creates.lock().await;
                    pending_map.get(pending_id).cloned()
                };
                let Some(pending) = pending else {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "session creation expired, please send a new message",
                    )));
                };
                if pending.lark_app_id != app_id {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "permission denied",
                    )));
                }

                let keyword = action.dir_search_keyword.as_deref().unwrap_or("").trim();

                if keyword.is_empty() {
                    return Ok(Json(build_lark_card_action_toast(
                        "warning",
                        "请先输入关键词，再使用最优匹配",
                    )));
                }

                let best = dir_select::find_best_match(&pending.candidate_dirs, keyword);

                match best {
                    Some(dir) => {
                        // Validate against the pending's root and candidates
                        if !dir_select::is_valid_candidate(
                            &dir,
                            &pending.root_working_dir,
                            &pending.candidate_dirs,
                        ) {
                            return Ok(Json(build_lark_card_action_toast(
                                "error",
                                &format!("directory '{}' is not a valid candidate", dir),
                            )));
                        }

                        // Atomically remove pending to prevent double-create (dir_select_best)
                        let pending = {
                            let mut pending_map = state.pending_creates.lock().await;
                            let removed = pending_map.remove(pending_id);
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
                            removed
                        };
                        let Some(pending) = pending else {
                            return Ok(Json(build_lark_card_action_toast(
                                "error",
                                "session already being created, please wait",
                            )));
                        };

                        let working_dir = dir_select::resolve_dir(&pending.root_working_dir, &dir);

                        // Consume quota if applicable
                        if let Some(quota_key) = pending.quota_key.as_deref() {
                            let quota = consume_inbound_quota(state, app_id, quota_key).await?;
                            if !quota.allowed {
                                return Ok(Json(build_lark_card_action_toast(
                                    "error",
                                    "quota exceeded",
                                )));
                            }
                        }

                        create_session_from_pending(state, bot, &pending, &working_dir, &dir).await
                    }
                    None => {
                        // No unique match: DON'T remove pending, just refresh card
                        let filtered = dir_select::filter_dirs(&pending.candidate_dirs, keyword);
                        let message = if filtered.is_empty() {
                            if prompt::is_zh_locale(pending.locale.as_deref()) {
                                Some(format!(
                                    "⚠️ 没有目录匹配 \"{}\"，请尝试其他关键词。",
                                    keyword
                                ))
                            } else {
                                Some(format!(
                                    "⚠️ No directory matches \"{}\". Try another keyword.",
                                    keyword
                                ))
                            }
                        } else {
                            if prompt::is_zh_locale(pending.locale.as_deref()) {
                                Some(format!(
                                    "⚠️ 多个目录匹配 \"{}\"（共 {} 个），请选择其中一个。",
                                    keyword,
                                    filtered.len()
                                ))
                            } else {
                                Some(format!(
                                    "⚠️ Multiple directories match \"{}\" ({} total). Choose one.",
                                    keyword,
                                    filtered.len()
                                ))
                            }
                        };

                        let card = dir_select::build_dir_select_card(
                            pending_id,
                            &pending.root_working_dir,
                            &pending.title,
                            &[],
                            &pending.candidate_dirs,
                            Some(&filtered),
                            Some(keyword),
                            message.as_deref(),
                            pending.locale.as_deref(),
                        );

                        // PATCH the card message as a fallback (primary update is via response card field)
                        if let Some(card_msg_id) = &pending.card_message_id {
                            if let Err(e) = lark_update_card(state, bot, card_msg_id, &card).await {
                                warn!(
                                    "dir_select_best: PATCH card for {} failed: {:?}",
                                    pending_id, e
                                );
                            }
                        }

                        let card_data = serde_json::from_str::<Value>(&card).unwrap_or(Value::Null);
                        let toast_content = if prompt::is_zh_locale(pending.locale.as_deref()) {
                            "无法确定唯一最佳匹配，请从列表中选择"
                        } else {
                            "Could not determine a unique best match. Choose one from the list."
                        };
                        Ok(Json(serde_json::json!({
                            "toast": { "type": "warning", "content": toast_content },
                            "card": { "type": "raw", "data": card_data }
                        })))
                    }
                }
            }

            _ => Ok(Json(build_lark_card_action_toast(
                "error",
                "unknown dir select action",
            ))),
        }
    }

    /// Shared helper: create a session from a pending entry (already removed from map),
    /// record recent dir, update the card, and return success toast.
    async fn create_session_from_pending(
        state: &AppState,
        bot: &BotConfig,
        pending: &dir_select::PendingCreateSession,
        working_dir: &str,
        working_dir_rel: &str,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        // Build the prompt from the pending context.
        // Use root_message_id (root_id or message_id) for quote hint suppression,
        // NOT the session matching anchor (thread_id for topics).
        let root_message_id = pending
            .root_id
            .clone()
            .unwrap_or_else(|| pending.message_id.clone());
        let prompt_raw = prompt::build_quote_hint(
            pending.parent_id.as_deref(),
            &pending.message_id,
            pending.scope,
            &root_message_id,
        ) + &pending.text;

        let mentions: Vec<LarkEventMention> =
            serde_json::from_str(&pending.mentions_json).unwrap_or_default();

        let prompt = if pending.cli_id == "opencode" {
            let (bot_name, bot_open_id) = load_bot_identity(&state.paths, &pending.lark_app_id);
            let observed_bots =
                load_observed_bots_for_chat(&state.paths, &pending.lark_app_id, &pending.chat_id);
            prompt::build_initial_prompt(&prompt::InitialPromptOptions {
                user_message: &prompt_raw,
                session_id: "pending",
                sender_open_id: pending.sender_open_id.as_deref(),
                sender_type: pending.sender_type.as_deref(),
                mentions: &mentions,
                bot_name: bot_name.as_deref(),
                bot_open_id: bot_open_id.as_deref(),
                observed_bots: &observed_bots,
                follow_ups: &Vec::new(),
                locale: pending.locale.as_deref(),
            })
        } else {
            prompt::build_follow_up_content(
                &prompt_raw,
                &prompt::FollowUpContentOptions {
                    session_id: "pending",
                    sender_open_id: pending.sender_open_id.as_deref(),
                    sender_type: pending.sender_type.as_deref(),
                    mentions: &mentions,
                    cli_id: pending.cli_id.as_str(),
                    locale: pending.locale.as_deref(),
                },
            )
        };

        let session = create_session_internal(
            state,
            build_session_create_spec_from_pending(
                pending,
                pending.title.clone(),
                pending.chat_id.clone(),
                pending.chat_type.clone(),
                root_message_id,
                Some(pending.message_id.clone()),
                pending.scope,
                pending.thread_id.clone(),
                working_dir.to_string(),
                prompt,
                pending.lark_app_id.clone(),
                pending.sender_open_id.clone(),
                pending.locale.clone(),
                None,
            ),
        )
        .await
        .map_err(internal_error)?;

        // Record recent directory
        let recent_path = state.paths.root().join("recent-dirs.json");
        let mut recent_store = dir_select::load_recent_dirs(&recent_path)
            .await
            .unwrap_or_default();
        let recent_key = dir_select::build_recent_dir_key(
            &pending.lark_app_id,
            &pending.chat_id,
            pending.sender_open_id.as_deref(),
        );
        dir_select::record_recent_dir(&mut recent_store, &recent_key, working_dir_rel);
        let _ = dir_select::save_recent_dirs(&recent_path, &recent_store).await;

        // Update the dir select card to show success
        if let Some(card_msg_id) = &pending.card_message_id {
            let success_card = dir_select::build_dir_session_starting_card(
                working_dir,
                &pending.title,
                pending.locale.as_deref(),
            );
            let _ = lark_update_card(state, bot, card_msg_id, &success_card).await;
            let card_data = serde_json::from_str::<Value>(&success_card).unwrap_or(Value::Null);
            return Ok(Json(serde_json::json!({
                "toast": { "type": "success", "content": "directory selected" },
                "card": { "type": "raw", "data": card_data }
            })));
        }

        Ok(Json(build_lark_card_action_toast(
            "success",
            &format!(
                "session started: {} (dir: {})",
                session.session_id, working_dir_rel
            ),
        )))
    }

    async fn handle_grant_card_action(
        state: &AppState,
        app_id: &str,
        action: &ParsedLarkCardAction,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        let value: serde_json::Value = action
            .raw_value
            .as_deref()
            .and_then(|v| serde_json::from_str(v).ok())
            .unwrap_or_default();
        let nonce = value.get("nonce").and_then(Value::as_str).unwrap_or("");
        let targets: Vec<String> = value
            .get("targets")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let quota: Option<u32> = value
            .get("quota")
            .and_then(|v| v.as_u64())
            .map(|n| n as u32);
        let chat_id = value.get("chatId").and_then(Value::as_str).unwrap_or("");

        let operator = action.operator_open_id.as_deref().unwrap_or("");
        let owner_open = state
            .bots
            .get(app_id)
            .and_then(|b| b.allowed_users.first().cloned())
            .unwrap_or_default();
        if operator != owner_open {
            return Ok(Json(build_lark_card_action_toast(
                "error",
                "only the bot owner can approve grants",
            )));
        }

        let mut pending = state.grant_pending.lock().await;
        let valid = targets.iter().all(|t| {
            let key = format!("{}:{}:{}", app_id, chat_id, t);
            pending
                .get(&key)
                .map(|e| e.nonce == nonce && e.is_pending())
                .unwrap_or(false)
        });
        if !valid {
            return Ok(Json(build_lark_card_action_toast(
                "info",
                "grant expired or already processed",
            )));
        }
        if action.action == "grant_deny" {
            let now_ms = Utc::now().timestamp_millis().max(0) as u64;
            for t in &targets {
                if let Some(entry) = pending.get_mut(&format!("{}:{}:{}", app_id, chat_id, t)) {
                    entry.mark_denied(now_ms);
                }
            }
            let snapshot: HashMap<String, grant::GrantPendingEntry> = pending
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            drop(pending);
            grant::save_grant_pending(&state.paths, &snapshot);
            return Ok(Json(build_lark_card_action_toast(
                "success",
                &format!("grant denied for {} target(s)", targets.len()),
            )));
        }
        drop(pending);

        let bots_path = state.paths.bots_json();
        let raw = tokio::fs::read_to_string(&bots_path)
            .await
            .unwrap_or_default();
        let mut config: serde_json::Value =
            serde_json::from_str(&raw).unwrap_or(serde_json::json!([]));

        let mut results = Vec::new();
        let mut observed = Vec::new();
        let mut granted = Vec::new();
        let mut failed: Vec<(String, String)> = Vec::new();
        for target in &targets {
            let r = if action.action == "grant_chat" {
                grant::add_chat_grant(&mut config, app_id, chat_id, target, quota)
            } else {
                grant::add_global_grant(&mut config, app_id, target, quota)
            };
            match r {
                Ok(()) => {
                    let scope = if action.action == "grant_chat" {
                        "chat"
                    } else {
                        "global"
                    };
                    let q = quota.map(|q| format!(" ({} msg)", q)).unwrap_or_default();
                    results.push(format!("granted @{} ({}){}", target, scope, q));
                    granted.push(target.clone());
                    observed.push((target.clone(), target.clone()));
                }
                Err(e) => failed.push((target.clone(), e.to_string())),
            }
        }

        if let Err(e) = tokio::fs::write(
            &bots_path,
            serde_json::to_string_pretty(&config).unwrap_or_default(),
        )
        .await
        {
            return Ok(Json(build_lark_card_action_toast(
                "error",
                &format!("save failed: {}", e),
            )));
        }

        let mut pending = state.grant_pending.lock().await;
        if granted.is_empty() {
            return Ok(Json(build_lark_card_action_toast(
                "error",
                &format!(
                    "grant failed for {}",
                    failed
                        .first()
                        .map(|item| item.1.clone())
                        .unwrap_or_else(|| "unknown".to_string())
                ),
            )));
        }
        for target in &granted {
            pending.remove(&format!("{}:{}:{}", app_id, chat_id, target));
        }
        for target in &failed {
            pending.remove(&format!("{}:{}:{}", app_id, chat_id, target.0));
        }
        let snapshot: HashMap<String, grant::GrantPendingEntry> = pending
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        drop(pending);
        grant::save_grant_pending(&state.paths, &snapshot);

        if let Err(err) = record_observed_bots(&state.paths, app_id, chat_id, &observed, "grant") {
            warn!(
                "failed to persist observed bots for {} / {}: {}",
                app_id, chat_id, err
            );
        }

        let mut output = results.join("\n");
        if !failed.is_empty() {
            let fail_names = failed
                .iter()
                .map(|item| item.0.as_str())
                .collect::<Vec<_>>()
                .join("、");
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&format!("partial failed: {}", fail_names));
        }
        Ok(Json(build_lark_card_action_toast("success", &output)))
    }

    let action = parse_lark_card_action(&payload)?;
    if card_action_requires_operate(action.action.as_str())
        && !can_operate_bot_with_state(state, &bot, action.operator_open_id.as_deref())
    {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "permission denied",
        )));
    }

    if action.action.starts_with("ask_") {
        return ask::handle_ask_card_action(state, app_id, &action).await;
    }

    if matches!(
        action.action.as_str(),
        "grant_chat" | "grant_global" | "grant_deny"
    ) {
        return handle_grant_card_action(state, app_id, &action).await;
    }

    // --- Directory selection card actions ---
    if matches!(
        action.action.as_str(),
        "dir_select_pick" | "dir_select_filter" | "dir_select_best"
    ) {
        return handle_dir_select_card_action(state, &bot, app_id, &action).await;
    }

    // --- Transcript source selection ---
    if action.action == "transcript_select" {
        let Some(ref beam_session_id) = action.session_id else {
            return Ok(Json(build_lark_card_action_toast(
                "error",
                "missing session id",
            )));
        };
        let Some(ref cli_session_id) = action.cli_session_id else {
            return Ok(Json(build_lark_card_action_toast(
                "error",
                "missing cli_session_id",
            )));
        };
        info!(
            "user selected transcript source: session={} for beam session={}",
            cli_session_id, beam_session_id
        );
        if let Err(err) = send_worker_message(
            &state.workers,
            beam_session_id,
            &DaemonToWorker::SetTranscriptSource {
                cli_session_id: cli_session_id.clone(),
            },
        )
        .await
        {
            warn!(
                "failed to send SetTranscriptSource to worker {}: {}",
                beam_session_id, err
            );
            return Ok(Json(build_lark_card_action_toast(
                "error",
                "failed to deliver session selection to worker",
            )));
        }
        if let Some(clicked_msg_id) = action.clicked_message_id.as_deref() {
            let _ = lark_update_card(
                state,
                &bot,
                clicked_msg_id,
                &build_transcript_selected_card(cli_session_id, action.operator_open_id.as_deref()),
            )
            .await;
        }
        return Ok(Json(build_lark_card_action_toast(
            "success",
            "transcript source selected",
        )));
    }

    let session_id = {
        let sessions = state.sessions.lock().await;
        resolve_lark_card_action_session_id(&sessions, &app_id, &action)
    };
    let Some(session_id) = session_id else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing session id",
        )));
    };
    let session_snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(&session_id).cloned()
    };
    let Some(current_session) = session_snapshot else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "session not found",
        )));
    };
    if is_stale_stream_card_action(&action, &current_session)
        && !stale_stream_card_action_self_heals_live_session(&action.action)
        && !stale_stream_card_action_reads_frozen_snapshot(&action.action)
    {
        return Ok(Json(build_lark_card_action_toast(
            "info",
            "stale card action ignored",
        )));
    }

    match action.action.as_str() {
        "resume" => match resume_session(
            State(state.clone()),
            AxumPath(session_id.clone()),
            Json(ResumeSessionRequest {
                prompt: String::new(),
            }),
        )
        .await
        {
            Ok((_, Json(session))) => Ok(Json(build_lark_card_action_toast(
                "success",
                &format!("session resumed: {}", session.session_id),
            ))),
            Err((status, _err)) if status == StatusCode::NOT_FOUND => Ok(Json(
                build_lark_card_action_toast("error", "session not found"),
            )),
            Err((status, err))
                if status == StatusCode::CONFLICT && err == "session is not closed" =>
            {
                Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session is not closed",
                )))
            }
            Err((status, err))
                if status == StatusCode::CONFLICT
                    && err.starts_with("session anchor is already owned by active session") =>
            {
                Ok(Json(build_lark_card_action_toast("error", &err)))
            }
            Err((status, err))
                if status == StatusCode::CONFLICT
                    && err == "adopted sessions cannot be resumed yet" =>
            {
                Ok(Json(build_lark_card_action_toast("error", &err)))
            }
            Err((_, err)) => Ok(Json(build_lark_card_action_toast(
                "error",
                &format!("resume failed: {}", err),
            ))),
        },
        "restart" => match restart_session(
            State(state.clone()),
            AxumPath(session_id.clone()),
            Json(RestartSessionRequest {
                prompt: String::new(),
            }),
        )
        .await
        {
            Ok(_) => Ok(Json(build_lark_card_action_toast(
                "success",
                "session restarting",
            ))),
            Err((status, _err)) if status == StatusCode::NOT_FOUND => Ok(Json(
                build_lark_card_action_toast("error", "session not found"),
            )),
            Err((_, err)) => Ok(Json(build_lark_card_action_toast(
                "error",
                &format!("restart failed: {}", err),
            ))),
        },
        "close" => {
            let session_snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(&session_id).cloned()
            };
            let Some(session) = session_snapshot else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session not found",
                )));
            };
            match close_session(State(state.clone()), AxumPath(session_id.clone())).await {
                Ok(_status) => {
                    let closed_card = build_closed_session_card(&session);
                    if action.visibility.as_deref() == Some("private") || bot.private_card {
                        for open_id in resolve_private_card_audience(&session, &bot) {
                            let delivered =
                                match private_card_delivery(session.chat_type.as_deref()) {
                                    PrivateCardDelivery::Ephemeral => {
                                        lark_send_ephemeral_card(
                                            &state,
                                            &bot,
                                            &session.chat_id,
                                            &open_id,
                                            &closed_card,
                                        )
                                        .await
                                    }
                                    PrivateCardDelivery::DirectMessage => {
                                        lark_send_open_id_card(&state, &bot, &open_id, &closed_card)
                                            .await
                                    }
                                };
                            if let Err(err) = delivered {
                                warn!(
                                    "private close card delivery failed for {}: {}",
                                    open_id, err
                                );
                            }
                        }
                        Ok(Json(build_lark_card_action_toast(
                            "success",
                            "session closed",
                        )))
                    } else {
                        Ok(Json(serde_json::json!({
                            "toast": {
                                "type": "success",
                                "content": "session closed",
                            },
                            "card": {
                                "type": "raw",
                                "data": serde_json::from_str::<Value>(&closed_card)
                                    .unwrap_or_else(|_| serde_json::json!({}))
                            }
                        })))
                    }
                }
                Err((status, _err)) if status == StatusCode::NOT_FOUND => Ok(Json(
                    build_lark_card_action_toast("error", "session not found"),
                )),
                Err((_, err)) => Ok(Json(build_lark_card_action_toast(
                    "error",
                    &format!("close failed: {}", err),
                ))),
            }
        }
        "choose_read_only_terminal_link" | "get_read_only_link" => {
            let session_snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(&session_id).cloned()
            };
            let Some(session) = session_snapshot else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session not found",
                )));
            };
            // Check that read-only token is available (needed server-side to fulfill the ticket)
            let ro_token_available = load_zellij_web_tokens_for_card()
                .as_ref()
                .and_then(|t| t.read_only_token.as_deref())
                .map_or(false, |t| !t.is_empty());
            if !ro_token_available {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "terminal not ready",
                )));
            }
            let card_json = build_terminal_link_choice_card_json(
                &state,
                &session,
                terminal_auth::TerminalPermission::ReadOnly,
                "选择只读终端入口",
                "Choose read-only terminal entry",
                "如果某个入口打不开，请返回后选择其他入口。",
                "If one entry does not open, go back and choose another.",
            )
            .await;
            if session.lark_app_id != "local" {
                if let Some(operator_open_id) = action.operator_open_id.as_deref() {
                    let delivered = match private_card_delivery(session.chat_type.as_deref()) {
                        PrivateCardDelivery::Ephemeral => {
                            lark_send_ephemeral_card(
                                &state,
                                &bot,
                                &session.chat_id,
                                operator_open_id,
                                &card_json,
                            )
                            .await
                        }
                        PrivateCardDelivery::DirectMessage => {
                            lark_send_open_id_card(&state, &bot, operator_open_id, &card_json).await
                        }
                    };
                    return match delivered {
                        Ok(_) => Ok(Json(build_lark_card_action_toast(
                            "success",
                            "read-only link ready",
                        ))),
                        Err(err) => Ok(Json(build_lark_card_action_toast(
                            "error",
                            &format!("link delivery failed: {}", err),
                        ))),
                    };
                }
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "link delivery failed: missing operator",
                )));
            }
            let card =
                serde_json::from_str::<Value>(&card_json).unwrap_or_else(|_| serde_json::json!({}));
            Ok(Json(serde_json::json!({
                "toast": {
                    "type": "success",
                    "content": "read-only link ready",
                },
                "card": {
                    "type": "raw",
                    "data": card,
                }
            })))
        }
        "get_write_link" => {
            let session_snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(&session_id).cloned()
            };
            let Some(session) = session_snapshot else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session not found",
                )));
            };
            // Check that write token is available (needed server-side to fulfill the ticket)
            let write_token_available =
                zellij_web::load_zellij_web_tokens(&state.paths.zellij_web_tokens_json())
                    .unwrap_or(None)
                    .as_ref()
                    .and_then(|t| t.write_token.as_deref())
                    .map_or(false, |t| !t.is_empty());
            if !write_token_available {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "terminal not ready",
                )));
            }
            let card_json = build_terminal_link_choice_card_json(
                &state,
                &session,
                terminal_auth::TerminalPermission::Write,
                "选择可写终端入口",
                "Choose writable terminal entry",
                "如果某个入口打不开，请返回后选择其他入口。",
                "If one entry does not open, go back and choose another.",
            )
            .await;
            if session.lark_app_id != "local" {
                if let Some(operator_open_id) = action.operator_open_id.as_deref() {
                    let delivered = match private_card_delivery(session.chat_type.as_deref()) {
                        PrivateCardDelivery::Ephemeral => {
                            lark_send_ephemeral_card(
                                &state,
                                &bot,
                                &session.chat_id,
                                operator_open_id,
                                &card_json,
                            )
                            .await
                        }
                        PrivateCardDelivery::DirectMessage => {
                            lark_send_open_id_card(&state, &bot, operator_open_id, &card_json).await
                        }
                    };
                    return match delivered {
                        Ok(_) => Ok(Json(build_lark_card_action_toast(
                            "success",
                            "write link ready",
                        ))),
                        Err(err) => Ok(Json(build_lark_card_action_toast(
                            "error",
                            &format!("write link delivery failed: {}", err),
                        ))),
                    };
                }
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "write link delivery failed: missing operator",
                )));
            }
            let card =
                serde_json::from_str::<Value>(&card_json).unwrap_or_else(|_| serde_json::json!({}));
            Ok(Json(serde_json::json!({
                "toast": {
                    "type": "success",
                    "content": "write link ready",
                },
                "card": {
                    "type": "raw",
                    "data": card,
                }
            })))
        }
        "export_text" => {
            let session_snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(&session_id).cloned()
            };
            let Some(session) = session_snapshot else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session not found",
                )));
            };
            if session.root_message_id.is_empty() || session.lark_app_id == "local" {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "export unavailable",
                )));
            }
            let body = if let Some(frozen) =
                load_clicked_frozen_card(&state.paths, &session, action.card_nonce.as_deref())
                    .await
                    .map_err(internal_error)?
            {
                if frozen.content.trim().is_empty() {
                    "(no output yet)".to_string()
                } else {
                    let frozen_session = Session {
                        current_screen: Some(frozen.content),
                        ..session.clone()
                    };
                    build_export_text_reply(&frozen_session)
                }
            } else {
                build_export_text_reply(&session)
            };
            match lark_reply_message_with_opts(
                &state,
                &bot,
                &session.root_message_id,
                &body,
                session.scope == SessionScope::Thread,
            )
            .await
            {
                Ok(_) => Ok(Json(build_lark_card_action_toast(
                    "success",
                    "text exported",
                ))),
                Err(err) => Ok(Json(build_lark_card_action_toast(
                    "error",
                    &format!("export failed: {}", err),
                ))),
            }
        }
        "retry_last_task" => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let session_snapshot = {
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    let Some(entry) = sessions.get_mut(&session_id) else {
                        return Ok(Json(build_lark_card_action_toast(
                            "error",
                            "session not found",
                        )));
                    };
                    let Ok((updated, cli_input)) = prepare_retry_last_task(entry, now_ms) else {
                        return Ok(Json(build_lark_card_action_toast(
                            "error",
                            "retry unavailable",
                        )));
                    };
                    *entry = updated.clone();
                    let snapshot = sessions.clone();
                    (updated, cli_input, snapshot)
                };
                persist_sessions(&state.paths, &snapshot.2)
                    .await
                    .map_err(internal_error)?;
                (snapshot.0, snapshot.1)
            };
            let _ = ensure_worker_for_session(&state, &session_id).await;
            let _ = send_worker_message(
                &state.workers,
                &session_id,
                &DaemonToWorker::Message {
                    content: session_snapshot.1.clone(),
                    turn_id: next_session_turn_id(),
                },
            )
            .await;
            let card = serde_json::from_str::<Value>(&build_streaming_card(
                &session_snapshot.0,
                session_stream_status(&session_snapshot.0),
            ))
            .unwrap_or_else(|_| serde_json::json!({}));
            Ok(Json(serde_json::json!({
                "toast": {
                    "type": "success",
                    "content": "retry requested",
                },
                "card": {
                    "type": "raw",
                    "data": card,
                }
            })))
        }
        "toggle_display" | "toggle_stream" => {
            let stale_frozen_nonce = if is_stale_stream_card_action(&action, &current_session) {
                action.card_nonce.clone()
            } else {
                None
            };
            let session_snapshot = {
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    let Some(entry) = sessions.get_mut(&session_id) else {
                        return Ok(Json(build_lark_card_action_toast(
                            "error",
                            "session not found",
                        )));
                    };
                    entry.display_mode = Some(next_display_mode(entry.display_mode));
                    let updated = entry.clone();
                    let snapshot = sessions.clone();
                    (updated, snapshot)
                };
                persist_sessions(&state.paths, &snapshot.1)
                    .await
                    .map_err(internal_error)?;
                snapshot.0
            };
            if let Err(err) = ensure_worker_for_session(&state, &session_id).await {
                warn!(
                    "[{}] toggle_display ensure_worker failed: {:#}",
                    session_snapshot.session_id, err
                );
            }
            if let Err(err) = send_worker_message(
                &state.workers,
                &session_id,
                &DaemonToWorker::SetDisplayMode {
                    mode: session_snapshot.display_mode.unwrap_or(DisplayMode::Hidden),
                },
            )
            .await
            {
                warn!(
                    "[{}] toggle_display send SetDisplayMode failed: {:#}",
                    session_snapshot.session_id, err
                );
            }
            let card = serde_json::from_str::<Value>(&build_streaming_card(
                &session_snapshot,
                if session_snapshot.status == SessionStatus::Closed {
                    "closed"
                } else {
                    session_stream_status(&session_snapshot)
                },
            ))
            .unwrap_or_else(|_| serde_json::json!({}));
            match resolve_card_render_target(&action, &session_snapshot) {
                CardRenderTarget::PatchMessage(target_message_id) => {
                    let card_json =
                        serde_json::to_string(&card).unwrap_or_else(|_| "{}".to_string());
                    info!(
                        "[{}] toggle_display patch target={}, clicked={:?}, mode={:?}",
                        session_snapshot.session_id,
                        target_message_id,
                        action.clicked_message_id,
                        session_snapshot.display_mode,
                    );
                    match lark_update_card(&state, &bot, &target_message_id, &card_json).await {
                        Ok(()) => {
                            if let Some(nonce) = stale_frozen_nonce.as_deref() {
                                if let Err(err) = remove_frozen_card(
                                    &state.paths,
                                    &session_snapshot.session_id,
                                    nonce,
                                )
                                .await
                                {
                                    warn!(
                                        "failed to remove migrated frozen card {}: {}",
                                        nonce, err
                                    );
                                }
                            }
                            Ok(Json(build_lark_card_action_toast(
                                "success",
                                "display updated",
                            )))
                        }
                        Err(err) => Ok(Json(build_lark_card_action_toast(
                            "error",
                            &format!("display update failed: {}", err),
                        ))),
                    }
                }
                CardRenderTarget::CallbackRaw => {
                    if let Some(nonce) = stale_frozen_nonce.as_deref() {
                        if let Err(err) =
                            remove_frozen_card(&state.paths, &session_snapshot.session_id, nonce)
                                .await
                        {
                            warn!("failed to remove migrated frozen card {}: {}", nonce, err);
                        }
                    }
                    Ok(Json(serde_json::json!({
                        "toast": {
                            "type": "success",
                            "content": "display updated",
                        },
                        "card": {
                            "type": "raw",
                            "data": card,
                        }
                    })))
                }
            }
        }
        "refresh_screenshot" => {
            let session_snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(&session_id).cloned()
            };
            let Some(session) = session_snapshot else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session not found",
                )));
            };
            if session.display_mode != Some(DisplayMode::Screenshot) {
                return Ok(Json(build_lark_card_action_toast(
                    "info",
                    "show screenshot first",
                )));
            }
            let _ = refresh_session(State(state.clone()), AxumPath(session_id.clone())).await;
            let card = serde_json::from_str::<Value>(&build_streaming_card(
                &session,
                session_stream_status(&session),
            ))
            .unwrap_or_else(|_| serde_json::json!({}));
            match resolve_card_render_target(&action, &session) {
                CardRenderTarget::PatchMessage(message_id) => {
                    let card_json =
                        serde_json::to_string(&card).unwrap_or_else(|_| "{}".to_string());
                    match lark_update_card(&state, &bot, &message_id, &card_json).await {
                        Ok(()) => Ok(Json(build_lark_card_action_toast(
                            "success",
                            "refresh requested",
                        ))),
                        Err(err) => Ok(Json(build_lark_card_action_toast(
                            "error",
                            &format!("refresh failed: {}", err),
                        ))),
                    }
                }
                CardRenderTarget::CallbackRaw => Ok(Json(serde_json::json!({
                    "toast": {
                        "type": "success",
                        "content": "refresh requested",
                    },
                    "card": {
                        "type": "raw",
                        "data": card,
                    }
                }))),
            }
        }
        "term_action" => {
            let Some(key) = action.term_key else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "missing terminal key",
                )));
            };
            let session_snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(&session_id).cloned()
            };
            let Some(session) = session_snapshot else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "session not found",
                )));
            };
            if session.display_mode != Some(DisplayMode::Screenshot) {
                return Ok(Json(build_lark_card_action_toast(
                    "info",
                    "show screenshot first",
                )));
            }
            let _ = ensure_worker_for_session(&state, &session_id).await;
            let _ = send_worker_message(
                &state.workers,
                &session_id,
                &DaemonToWorker::TermAction { key },
            )
            .await;
            let card = serde_json::from_str::<Value>(&build_streaming_card(
                &session,
                if session.status == SessionStatus::Closed {
                    "closed"
                } else {
                    session_stream_status(&session)
                },
            ))
            .unwrap_or_else(|_| serde_json::json!({}));
            match resolve_card_render_target(&action, &session) {
                CardRenderTarget::PatchMessage(message_id) => {
                    let card_json =
                        serde_json::to_string(&card).unwrap_or_else(|_| "{}".to_string());
                    match lark_update_card(&state, &bot, &message_id, &card_json).await {
                        Ok(()) => Ok(Json(build_lark_card_action_toast(
                            "success",
                            "terminal action sent",
                        ))),
                        Err(err) => Ok(Json(build_lark_card_action_toast(
                            "error",
                            &format!("terminal action failed: {}", err),
                        ))),
                    }
                }
                CardRenderTarget::CallbackRaw => Ok(Json(serde_json::json!({
                    "toast": {
                        "type": "success",
                        "content": "terminal action sent",
                    },
                    "card": {
                        "type": "raw",
                        "data": card,
                    }
                }))),
            }
        }
        "tui_keys" => {
            let Some(keys) = action.special_keys.clone() else {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "missing tui prompt keys",
                )));
            };
            if action.option_type.as_deref() == Some("toggle") {
                let session_snapshot = {
                    let snapshot = {
                        let mut sessions = state.sessions.lock().await;
                        let Some(entry) = sessions.get_mut(&session_id) else {
                            return Ok(Json(build_lark_card_action_toast(
                                "error",
                                "session not found",
                            )));
                        };
                        let Some(selected_index) = action.selected_index else {
                            return Ok(Json(build_lark_card_action_toast(
                                "error",
                                "missing toggle index",
                            )));
                        };
                        if let Some(idx) = entry
                            .tui_toggled_indices
                            .iter()
                            .position(|value| *value == selected_index)
                        {
                            entry.tui_toggled_indices.remove(idx);
                        } else {
                            entry.tui_toggled_indices.push(selected_index);
                        }
                        let updated = entry.clone();
                        let snapshot = sessions.clone();
                        (updated, snapshot)
                    };
                    persist_sessions(&state.paths, &snapshot.1)
                        .await
                        .map_err(internal_error)?;
                    snapshot.0
                };
                let card = serde_json::from_str::<Value>(&build_tui_prompt_card(
                    &session_snapshot.root_message_id,
                    &session_snapshot.session_id,
                    &session_snapshot.title,
                    &session_snapshot.tui_prompt_options,
                    session_snapshot.tui_prompt_multi_select.unwrap_or(false),
                    &session_snapshot.tui_toggled_indices,
                    session_snapshot.locale.as_deref(),
                ))
                .unwrap_or_else(|_| serde_json::json!({}));
                return Ok(Json(serde_json::json!({
                    "toast": { "type": "success", "content": "selection updated" },
                    "card": { "type": "raw", "data": card }
                })));
            }

            let (all_keys, is_final, resolved_text, prompt_card_id, delay_ms, locale) = {
                let sessions = state.sessions.lock().await;
                let Some(session) = sessions.get(&session_id) else {
                    return Ok(Json(build_lark_card_action_toast(
                        "error",
                        "session not found",
                    )));
                };
                let mut all_keys = Vec::new();
                if !session.tui_toggled_indices.is_empty() && !session.tui_prompt_options.is_empty()
                {
                    let mut sorted = session.tui_toggled_indices.clone();
                    sorted.sort_unstable();
                    for index in sorted {
                        if let Some(option) = session.tui_prompt_options.get(index) {
                            all_keys.extend(option.keys.clone());
                        }
                    }
                }
                all_keys.extend(keys);
                let delay_ms = (all_keys.len() as u64 * 100).saturating_add(500);
                (
                    all_keys,
                    action.is_final,
                    resolve_tui_prompt_final_text(session, action.selected_text.as_deref()),
                    session.tui_prompt_card_id.clone(),
                    delay_ms,
                    session.locale.clone(),
                )
            };
            if is_final {
                let snapshot = {
                    let mut sessions = state.sessions.lock().await;
                    if let Some(entry) = sessions.get_mut(&session_id) {
                        entry.tui_prompt_card_id = None;
                        entry.tui_prompt_options.clear();
                        entry.tui_prompt_multi_select = None;
                        entry.tui_toggled_indices.clear();
                    }
                    sessions.clone()
                };
                persist_sessions(&state.paths, &snapshot)
                    .await
                    .map_err(internal_error)?;
            }
            let _ = ensure_worker_for_session(&state, &session_id).await;
            let _ = send_worker_message(
                &state.workers,
                &session_id,
                &DaemonToWorker::TuiKeys {
                    keys: all_keys,
                    is_final,
                },
            )
            .await;
            let processing_text = resolved_text.clone();
            if is_final {
                if let Some(card_id) = prompt_card_id {
                    let state = state.clone();
                    let session_id = session_id.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        let snapshot = {
                            let sessions = state.sessions.lock().await;
                            sessions.get(&session_id).cloned()
                        };
                        let Some(session) = snapshot else {
                            return;
                        };
                        if session.lark_app_id == "local" {
                            return;
                        }
                        let Some(bot) = state.bots.get(&session.lark_app_id).cloned() else {
                            return;
                        };
                        let _ = lark_update_card(
                            &state,
                            &bot,
                            &card_id,
                            &build_tui_prompt_resolved_card(
                                Some(resolved_text.as_str()),
                                session.locale.as_deref(),
                            ),
                        )
                        .await;
                    });
                }
            }
            let card = serde_json::from_str::<Value>(&build_tui_prompt_processing_card(
                Some(&processing_text),
                locale.as_deref(),
            ))
            .unwrap_or_else(|_| serde_json::json!({}));
            Ok(Json(serde_json::json!({
                "toast": {
                    "type": "success",
                    "content": "selection sent",
                },
                "card": {
                    "type": "raw",
                    "data": card,
                }
            })))
        }
        "tui_text_input" => {
            let input_text = action.input_text.clone().unwrap_or_default();
            let input_keys = action.input_keys.clone().unwrap_or_default();
            if input_text.trim().is_empty() || input_keys.is_empty() {
                return Ok(Json(build_lark_card_action_toast(
                    "error",
                    "missing tui text input",
                )));
            }
            let _ = ensure_worker_for_session(&state, &session_id).await;
            let _ = send_worker_message(
                &state.workers,
                &session_id,
                &DaemonToWorker::TuiTextInput {
                    keys: input_keys,
                    text: input_text.clone(),
                },
            )
            .await;
            let (snapshot, locale) = {
                let mut sessions = state.sessions.lock().await;
                let locale = sessions
                    .get(&session_id)
                    .and_then(|entry| entry.locale.clone());
                if let Some(entry) = sessions.get_mut(&session_id) {
                    entry.tui_prompt_card_id = None;
                    entry.tui_prompt_options.clear();
                    entry.tui_prompt_multi_select = None;
                    entry.tui_toggled_indices.clear();
                }
                (sessions.clone(), locale)
            };
            persist_sessions(&state.paths, &snapshot)
                .await
                .map_err(internal_error)?;
            let card = serde_json::from_str::<Value>(&build_tui_prompt_resolved_card(
                Some(&input_text),
                locale.as_deref(),
            ))
            .unwrap_or_else(|_| serde_json::json!({}));
            Ok(Json(serde_json::json!({
                "toast": {
                    "type": "success",
                    "content": "input sent",
                },
                "card": {
                    "type": "raw",
                    "data": card,
                }
            })))
        }
        "wf_approve" | "wf_reject" | "wf_cancel" => {
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
            // without re-writing events — the handler is still idempotent, but
            // this early return avoids touching the log at all.
            if workflow_cards.contains_key(card_nonce) {
                return Ok(Json(serde_json::json!({
                    "toast": {
                        "type": "success",
                        "content": format!("workflow {} already recorded", action.action),
                    }
                })));
            }

            // Phase 5.1/5.2: write EventLog events AND push the runtime
            // BEFORE updating the card.  On error the card stays un-frozen.
            let action_str = action.action.as_str();
            let handler_result = match action_str {
                "wf_approve" | "wf_reject" => {
                    let resolution = if action_str == "wf_approve" {
                        WaitResolution::Approved
                    } else {
                        WaitResolution::Rejected
                    };
                    workflow_commands::lark_approve_or_reject_wait(
                        &state,
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
                "wf_cancel" => {
                    workflow_commands::cancel_run(&state, run_id, comment.map(|s| s.to_string()))
                        .await
                        .map(|outcome| {
                            if outcome.ok {
                                Ok("workflow cancel recorded".to_string())
                            } else {
                                Err(outcome
                                    .error_hint
                                    .unwrap_or_else(|| "cancel failed".to_string()))
                            }
                        })
                }
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
            let workflow_card =
                serde_json::from_str::<Value>(&build_workflow_approval_resolved_card(
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
            if let Some(message_id) = workflow_approval_target_message_id(&action) {
                let card_json =
                    serde_json::to_string(&workflow_card).unwrap_or_else(|_| "{}".to_string());
                match lark_update_card(&state, &bot, &message_id, &card_json).await {
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
                        let _ = save_workflow_approval_cards(&state.paths, run_id, &workflow_cards)
                            .await;
                        Ok(Json(build_lark_card_action_toast(
                            "success",
                            &response_content,
                        )))
                    }
                    Err(err) => {
                        // Events already written; the card update is cosmetic.
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
        _ => Ok(Json(build_lark_card_action_toast(
            "info",
            "unsupported card action",
        ))),
    }
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

    // Write resumeStarted event (previously written by resume_schedule_dangling_effects).
    // This event serves as the checkpoint marker for the resume cycle and is used
    // by the response builder to distinguish recovered vs new events.
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

    // --- Unified reconciler dispatch: registered providers go through the
    //     registry-driven reconcile_provider_dangling_effects path ---
    let reconciler_registry = workflow_reconcilers::global_reconciler_registry();

    // Reconcile beam-schedule dangling effects via registry
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

    // Reconcile feishu-im dangling effects via registry
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

    // --- Reconciler registry check: handle any remaining dangling effects for
    //     providers that have no reconciler registered ---
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

    // Convert unified ProviderResumeResult to legacy types for
    // backward-compatible API response.
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

pub(crate) async fn send_input(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<SessionInputRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    ensure_worker_for_session(&state, &session_id)
        .await
        .map_err(internal_error)?;
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            let session = sessions
                .get_mut(&session_id)
                .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
            session.last_cli_input = Some(req.content.clone());
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot)
            .await
            .map_err(internal_error)?;
    }
    if let Err(err) = begin_lark_turn_card(&state, &session_id, "starting").await {
        warn!("failed to begin lark turn card for {}: {}", session_id, err);
    }

    let turn_id = next_session_turn_id();
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
    send_worker_message(&state.workers, &session_id, &msg)
        .await
        .map_err(internal_error)?;
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
        Ok(()) => {
            // For backward compatibility with old {content}-only requests,
            // also run the legacy delivery path if the request has no mention
            // decision and a non-empty content.
            Ok(StatusCode::ACCEPTED)
        }
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

pub(crate) fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    error!("{}", err);
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

async fn ensure_worker_for_session(state: &AppState, session_id: &str) -> Result<()> {
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

pub(crate) struct LarkWsEventHandler {
    pub(crate) state: AppState,
    pub(crate) app_id: String,
    pub(crate) event_type: &'static str,
}

impl EventHandler for LarkWsEventHandler {
    fn event_type(&self) -> &str {
        self.event_type
    }

    fn handle(
        &self,
        event: Event,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EventHandlerResult> + Send + '_>> {
        let state = self.state.clone();
        let app_id = self.app_id.clone();
        Box::pin(async move {
            let payload = serde_json::to_value(event)
                .map_err(|err| feishu_core::Error::SerializationError(err.to_string()))?;
            match handle_lark_event_payload(state, app_id, payload, None).await {
                Ok(_) => Ok(None),
                Err((_status, err)) => Err(feishu_core::Error::InvalidEventFormat(err)),
            }
        })
    }
}

pub(crate) struct LarkWsCardActionEventHandler {
    pub(crate) state: AppState,
    pub(crate) app_id: String,
    pub(crate) event_type: &'static str,
}

impl EventHandler for LarkWsCardActionEventHandler {
    fn event_type(&self) -> &str {
        self.event_type
    }

    fn handle(
        &self,
        event: Event,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EventHandlerResult> + Send + '_>> {
        let state = self.state.clone();
        let app_id = self.app_id.clone();
        Box::pin(async move {
            let raw = event.event.unwrap_or_default();
            let payload = normalize_lark_ws_card_action_from_raw(raw)?;

            let Json(response) = handle_lark_card_action_payload(&state, &app_id, payload)
                .await
                .map_err(|(_status, err)| feishu_core::Error::InvalidEventFormat(err))?;
            let body = serde_json::to_vec(&response)
                .map_err(|err| feishu_core::Error::SerializationError(err.to_string()))?;
            Ok(Some(EventResp::ok(body)))
        })
    }
}

pub(crate) fn spawn_lark_ws_clients(state: &AppState) {
    for bot in state.bots.values() {
        let config = feishu_core::Config::builder(&bot.lark_app_id, &bot.lark_app_secret)
            .request_timeout(Duration::from_secs(15))
            .build();
        let mut dispatcher_config = EventDispatcherConfig::new().skip_signature_verification(true);
        if let Some(token) = &bot.lark_verification_token {
            dispatcher_config = dispatcher_config.verification_token(token.clone());
        }
        if let Some(key) = &bot.lark_encrypt_key {
            dispatcher_config = dispatcher_config.encrypt_key(key.clone());
        }
        let dispatcher = EventDispatcher::new(dispatcher_config, config.logger.clone());
        let handler = LarkWsEventHandler {
            state: state.clone(),
            app_id: bot.lark_app_id.clone(),
            event_type: "im.message.receive_v1",
        };
        let card_handler = LarkWsCardActionEventHandler {
            state: state.clone(),
            app_id: bot.lark_app_id.clone(),
            event_type: "card.action.trigger",
        };
        let app_id = bot.lark_app_id.clone();
        tokio::spawn(async move {
            dispatcher.register_handler(Box::new(handler)).await;
            dispatcher.register_handler(Box::new(card_handler)).await;
            match StreamClient::builder(config)
                .stream_config(StreamConfig::default())
                .event_dispatcher(dispatcher)
                .build()
            {
                Ok(client) => {
                    eprintln!("lark ws starting for {}", app_id);
                    if let Err(err) = client.start().await {
                        eprintln!("lark ws stopped for {}: {}", app_id, err);
                    }
                }
                Err(err) => eprintln!("lark ws init failed for {}: {}", app_id, err),
            }
        });
    }
}

pub(crate) fn normalize_lark_ws_card_action(action: CardAction) -> Value {
    let mut payload = serde_json::to_value(&action).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(object) = payload.as_object_mut() {
        if let Some(open_id) = action.open_id.filter(|value| !value.trim().is_empty()) {
            object.insert(
                "operator".to_string(),
                serde_json::json!({ "open_id": open_id }),
            );
        }
        if let Some(message_id) = action
            .open_message_id
            .filter(|value| !value.trim().is_empty())
        {
            object.insert(
                "context".to_string(),
                serde_json::json!({ "open_message_id": message_id }),
            );
        }
    }
    payload
}

pub(crate) fn normalize_lark_ws_card_action_from_raw(
    raw: Value,
) -> Result<Value, feishu_core::Error> {
    let form_value_snapshot = raw.pointer("/action/form_value").cloned();
    let operator_snapshot = raw.pointer("/operator").cloned();
    let operator_id_snapshot = raw.pointer("/operator_id").cloned();
    let context_snapshot = raw.pointer("/context").cloned();

    let card_action: CardAction = serde_json::from_value(raw)
        .map_err(|err| feishu_core::Error::InvalidEventFormat(err.to_string()))?;
    let mut payload = normalize_lark_ws_card_action(card_action);

    if let Some(fv) = form_value_snapshot {
        if let Some(action) = payload.pointer_mut("/action") {
            if let Some(obj) = action.as_object_mut() {
                obj.insert("form_value".to_string(), fv);
            }
        }
    }

    if let Some(op) = operator_snapshot {
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("operator".to_string(), op);
        }
    }

    if let Some(op_id) = operator_id_snapshot {
        if let Some(obj) = payload.as_object_mut() {
            if !obj.contains_key("operator") {
                obj.insert("operator_id".to_string(), op_id);
            }
        }
    }

    if let Some(ctx) = context_snapshot {
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("context".to_string(), ctx);
        }
    }

    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn parse_feishu_resume_input_routes_send_and_reply_variants() {
        let send = serde_json::json!({
            "larkAppId": "app-1",
            "chatId": "chat-1",
            "content": "hello",
        });
        let send_input = parse_feishu_resume_input(&send).expect("send input");
        assert_eq!(send_input.lark_app_id, "app-1");
        assert_eq!(send_input.chat_id.as_deref(), Some("chat-1"));
        assert_eq!(send_input.root_message_id, None);
        assert_eq!(send_input.content, "hello");

        let reply = serde_json::json!({
            "larkAppId": "app-1",
            "rootMessageId": "msg-1",
            "content": "world",
        });
        let reply_input = parse_feishu_resume_input(&reply).expect("reply input");
        assert_eq!(reply_input.chat_id, None);
        assert_eq!(reply_input.root_message_id.as_deref(), Some("msg-1"));
        assert_eq!(reply_input.content, "world");
    }

    #[test]
    fn lark_event_dedupe_key_skips_empty_ids() {
        assert_eq!(
            lark_event_dedupe_key("app-1", "evt-1").as_deref(),
            Some("app-1:evt-1")
        );
        assert_eq!(lark_event_dedupe_key("app-1", ""), None);
        assert_eq!(lark_event_dedupe_key("app-1", "   "), None);
    }

    #[test]
    fn ws_card_action_handler_routes_toggle_display() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

            let app_id = "app-toggle-ws";
            let bot = BotConfig {
                name: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "codex".to_string(),
                cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
                working_dir: None,
                    lark_encrypt_key: None,
                lark_verification_token: None,
                allowed_users: Vec::new(),
                private_card: false,
                allowed_chat_groups: Vec::new(),
                chat_grants: std::collections::HashMap::new(),
                global_grants: Vec::new(),
                oncall_chats: Vec::new(),
                restrict_grant_commands: false,
                message_quota: None,
                quota_state: std::collections::HashMap::new(),
            };
            let state = make_state(temp_paths("toggle-ws"), HashMap::from([(app_id.to_string(), bot)]));
            let mut session = make_session("sess-toggle-ws");
            session.lark_app_id = app_id.to_string();
            session.closed_at = None;
            session.status = SessionStatus::Active;
            session.display_mode = Some(DisplayMode::Hidden);
            session.current_image_key = Some("img-2".to_string());
            session.stream_card_nonce = Some("nonce-toggle-ws".to_string());
            {
                let mut sessions = state.sessions.lock().await;
                sessions.insert(session.session_id.clone(), session.clone());
            }

            let handler = LarkWsCardActionEventHandler {
                state: state.clone(),
                app_id: app_id.to_string(),
                event_type: "card.action.trigger",
            };
            let event = mock_card_action_event(serde_json::json!({
                "open_id": "ou_user",
                "open_message_id": session.stream_card_id.clone().unwrap_or_else(|| "om-card".to_string()),
                "action": {
                    "value": {
                        "action": "toggle_display",
                        "root_id": session.root_message_id,
                        "session_id": session.session_id,
                        "cli_id": session.cli_id.clone().unwrap_or_else(|| "codex".to_string())
                    }
                }
            }));

            let resp = handler.handle(event).await.expect("event handler").expect("event resp");
            let body: Value = serde_json::from_slice(&resp.body).expect("body json");
            assert_eq!(body.pointer("/toast/type").and_then(Value::as_str), Some("success"));
            let stored = state.sessions.lock().await.get(&session.session_id).cloned().expect("stored session");
            assert_eq!(stored.display_mode, Some(DisplayMode::Screenshot));
        });
    }

    #[test]
    fn ws_card_action_handler_routes_ask_toggle_and_submit() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

            let app_id = "app-ask-ws";
            let bot = BotConfig {
                name: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "opencode".to_string(),
                cli_bin: None,
                cli_args: Vec::new(),
                skip_working_dir_prompt: false,
                model: None,
                working_dir: None,
                lark_encrypt_key: None,
                lark_verification_token: None,
                allowed_users: vec!["ou_approver".to_string()],
                private_card: false,
                allowed_chat_groups: Vec::new(),
                chat_grants: std::collections::HashMap::new(),
                global_grants: Vec::new(),
                oncall_chats: Vec::new(),
                restrict_grant_commands: false,
                message_quota: None,
                quota_state: std::collections::HashMap::new(),
            };
            let paths = temp_paths("ask-ws");
            let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

            let ask_body = serde_json::json!({
                "sessionId": "sess-ask-ws",
                "chatId": "chat-1",
                "larkAppId": app_id,
                "rootMessageId": null,
                "timeoutMs": 10_000,
                "approvers": ["ou_approver"],
                "questions": [{
                    "prompt": "Approve OpenCode permission?",
                    "multiSelect": false,
                    "options": [
                        { "key": "always", "label": "Always allow" },
                        { "key": "reject", "label": "Reject" }
                    ]
                }]
            });

            let create_state = state.clone();
            let create_task =
                tokio::spawn(
                    async move { ask::create_ask(State(create_state), Json(ask_body)).await },
                );

            let snapshot = {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                loop {
                    if let Ok(Some(snaps)) = beam_core::persist::read_json::<
                        Vec<ask::AskPendingSnapshot>,
                    >(&paths.ask_pending_json())
                    {
                        if let Some(snap) = snaps.into_iter().next() {
                            break snap;
                        }
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "ask pending snapshot was not persisted"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            };

            let handler = LarkWsCardActionEventHandler {
                state: state.clone(),
                app_id: app_id.to_string(),
                event_type: "card.action.trigger",
            };

            let toggle_event = mock_card_action_event(serde_json::json!({
                "operator": { "open_id": "ou_approver" },
                "context": { "open_message_id": "om_ask_card" },
                "action": {
                    "value": {
                        "action": "ask_toggle",
                        "ask_id": snapshot.ask_id,
                        "nonce": snapshot.nonce,
                        "question_index": 0,
                        "key": "always"
                    }
                }
            }));
            let toggle_resp = handler
                .handle(toggle_event)
                .await
                .expect("toggle event handler")
                .expect("toggle event response");
            let toggle_body: Value =
                serde_json::from_slice(&toggle_resp.body).expect("toggle body");
            assert_eq!(
                toggle_body.pointer("/toast/type").and_then(Value::as_str),
                Some("success")
            );

            let submit_event = mock_card_action_event(serde_json::json!({
                "operator": { "open_id": "ou_approver" },
                "context": { "open_message_id": "om_ask_card" },
                "action": {
                    "value": {
                        "action": "ask_submit",
                        "ask_id": snapshot.ask_id,
                        "nonce": snapshot.nonce
                    }
                }
            }));
            let submit_resp = handler
                .handle(submit_event)
                .await
                .expect("submit event handler")
                .expect("submit event response");
            let submit_body: Value =
                serde_json::from_slice(&submit_resp.body).expect("submit body");
            assert_eq!(
                submit_body
                    .pointer("/toast/content")
                    .and_then(Value::as_str),
                Some("ask submitted")
            );

            let create_response = create_task
                .await
                .expect("create task join")
                .expect("create ask");
            assert_eq!(
                create_response.0.pointer("/kind").and_then(Value::as_str),
                Some("answered")
            );
            assert_eq!(
                create_response
                    .0
                    .pointer("/answers/0/0")
                    .and_then(Value::as_str),
                Some("always")
            );
            assert_eq!(
                create_response.0.pointer("/by").and_then(Value::as_str),
                Some("ou_approver")
            );
        });
    }

    #[test]
    fn lark_message_withdrawn_helpers_recognize_code_230011() {
        let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
        assert!(is_lark_message_withdrawn_payload(payload));
        assert_eq!("DONE", "DONE");

        let err = anyhow::anyhow!("lark message withdrawn: {}", payload);
        assert!(is_lark_message_withdrawn_error(&err));

        let other = anyhow::anyhow!("lark reply failed: {{\"code\":999}}");
        assert!(!is_lark_message_withdrawn_error(&other));
    }

    #[test]
    fn normalize_lark_ws_card_action_preserves_operator_context_and_value() {
        let action = CardAction {
            open_id: Some("ou_owner".to_string()),
            open_message_id: Some("om_card".to_string()),
            action: Some(feishu_sdk::card::CardActionValue {
                value: Some(serde_json::json!({
                    "action": "toggle_display",
                    "session_id": "sess-1",
                    "card_nonce": "nonce-1",
                })),
                tag: Some("button".to_string()),
                option: None,
                timezone: None,
            }),
            ..Default::default()
        };

        let payload = normalize_lark_ws_card_action(action);
        assert_eq!(
            payload.pointer("/operator/open_id").and_then(Value::as_str),
            Some("ou_owner")
        );
        assert_eq!(
            payload
                .pointer("/context/open_message_id")
                .and_then(Value::as_str),
            Some("om_card")
        );
        assert_eq!(
            payload
                .pointer("/action/value/action")
                .and_then(Value::as_str),
            Some("toggle_display")
        );
        assert_eq!(
            payload
                .pointer("/action/value/card_nonce")
                .and_then(Value::as_str),
            Some("nonce-1")
        );
    }

    #[test]
    fn normalize_lark_ws_card_action_preserves_form_value_for_form_submit() {
        // The raw JSON includes "form_value" which must survive the
        // CardAction deserialization + normalization round-trip.
        let raw = serde_json::json!({
            "open_id": "ou_owner",
            "open_message_id": "om_card",
            "action": {
                "value": {
                    "action": "dir_select_filter",
                    "pending_id": "pending-xyz"
                },
                "tag": "button",
                "form_value": {
                    "dir_search_keyword": "home/test"
                }
            }
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

        // Verify the normalized payload has both the value fields and form_value
        assert_eq!(
            payload.pointer("/operator/open_id").and_then(Value::as_str),
            Some("ou_owner")
        );
        assert_eq!(
            payload
                .pointer("/action/value/action")
                .and_then(Value::as_str),
            Some("dir_select_filter")
        );
        assert_eq!(
            payload
                .pointer("/action/value/pending_id")
                .and_then(Value::as_str),
            Some("pending-xyz")
        );
        assert_eq!(
            payload
                .pointer("/action/form_value/dir_search_keyword")
                .and_then(Value::as_str),
            Some("home/test"),
            "form_value must be preserved through the CardAction deserialization round-trip"
        );

        // Verify parse_lark_card_action can extract the keyword
        let parsed = parse_lark_card_action(&payload).expect("parse normalized payload");
        assert_eq!(parsed.action, "dir_select_filter");
        assert_eq!(parsed.dir_search_keyword.as_deref(), Some("home/test"));
    }

    #[test]
    fn normalize_lark_ws_card_action_restores_operator_context_from_raw() {
        // Reproduction of production bug: WS card.action.trigger raw event
        // carries operator identity under /operator/open_id and message context
        // under /context/open_message_id, but feishu-sdk 0.1.2 CardAction has
        // no operator/context fields — they are silently dropped during
        // deserialization. The handler must snapshot them from raw and restore
        // them into the normalized payload so parse_lark_card_action can
        // extract operator_open_id and clicked_message_id.
        let raw = serde_json::json!({
            "operator": {
                "open_id": "ou_ac4d3f69f6c8b13349ba3f51c7b7c2cc",
                "tenant_key": "t_xxx"
            },
            "context": {
                "open_message_id": "om_abc123"
            },
            "action": {
                "value": {
                    "action": "get_write_link",
                    "session_id": "sess-1"
                },
                "tag": "button"
            },
            "token": "x-token"
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

        // Verify normalized payload has operator and context
        assert_eq!(
            payload.pointer("/operator/open_id").and_then(Value::as_str),
            Some("ou_ac4d3f69f6c8b13349ba3f51c7b7c2cc")
        );
        assert_eq!(
            payload
                .pointer("/context/open_message_id")
                .and_then(Value::as_str),
            Some("om_abc123")
        );
        assert_eq!(
            payload
                .pointer("/action/value/action")
                .and_then(Value::as_str),
            Some("get_write_link")
        );

        // Verify parse_lark_card_action can extract operator and context
        let parsed = parse_lark_card_action(&payload).expect("parse normalized payload");
        assert_eq!(parsed.action, "get_write_link");
        assert_eq!(
            parsed.operator_open_id.as_deref(),
            Some("ou_ac4d3f69f6c8b13349ba3f51c7b7c2cc"),
            "operator_open_id must be extracted from restored /operator/open_id"
        );
        assert_eq!(
            parsed.clicked_message_id.as_deref(),
            Some("om_abc123"),
            "clicked_message_id must be extracted from restored /context/open_message_id"
        );
    }

    #[test]
    fn normalize_lark_ws_card_action_preserves_choose_read_only_terminal_link_action() {
        let raw = serde_json::json!({
            "operator": {
                "open_id": "ou_choose"
            },
            "context": {
                "open_message_id": "om_choose"
            },
            "action": {
                "value": {
                    "action": "choose_read_only_terminal_link",
                    "session_id": "sess-choose"
                },
                "tag": "button"
            }
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");
        let parsed = parse_lark_card_action(&payload).expect("parse normalized payload");
        assert_eq!(parsed.action, "choose_read_only_terminal_link");
        assert_eq!(parsed.operator_open_id.as_deref(), Some("ou_choose"),);
        assert_eq!(parsed.clicked_message_id.as_deref(), Some("om_choose"),);
    }

    #[test]
    fn normalize_lark_ws_card_action_restores_operator_context_with_operator_id_fallback() {
        // When the raw event uses /operator_id instead of /operator
        // (HTTP callback path uses operator_id), still restore it.
        let raw = serde_json::json!({
            "operator_id": {
                "open_id": "ou_from_operator_id"
            },
            "context": {
                "open_message_id": "om_from_context"
            },
            "action": {
                "value": {
                    "action": "close",
                    "session_id": "sess-1"
                }
            }
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

        let parsed = parse_lark_card_action(&payload).expect("parse");
        assert_eq!(
            parsed.operator_open_id.as_deref(),
            Some("ou_from_operator_id"),
            "operator_open_id should fall back to /operator_id/open_id"
        );
        assert_eq!(
            parsed.clicked_message_id.as_deref(),
            Some("om_from_context")
        );
    }

    #[test]
    fn normalize_lark_ws_card_action_raw_operator_overrides_cardaction_open_id() {
        // When CardAction has top-level open_id AND raw has /operator,
        // the raw /operator should take precedence (it's the canonical source).
        let raw = serde_json::json!({
            "open_id": "ou_from_top_level",
            "open_message_id": "om_from_top_level",
            "operator": {
                "open_id": "ou_from_operator",
                "tenant_key": "t_xxx"
            },
            "context": {
                "open_message_id": "om_from_context"
            },
            "action": {
                "value": {
                    "action": "restart",
                    "session_id": "sess-1"
                },
                "tag": "button"
            }
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

        let parsed = parse_lark_card_action(&payload).expect("parse");
        assert_eq!(parsed.action, "restart");
        // Raw /operator/open_id wins over CardAction.open_id
        assert_eq!(
            parsed.operator_open_id.as_deref(),
            Some("ou_from_operator"),
            "raw /operator/open_id should take precedence"
        );
        // Raw /context/open_message_id wins over CardAction.open_message_id
        assert_eq!(
            parsed.clicked_message_id.as_deref(),
            Some("om_from_context"),
            "raw /context/open_message_id should take precedence"
        );
    }

    #[test]
    fn normalize_lark_ws_card_action_from_raw_uses_operator_id_when_operator_absent() {
        // When the raw WS event carries only /operator_id (no /operator),
        // the helper must restore it so parse_lark_card_action can fall back
        // to /operator_id/open_id.
        let raw = serde_json::json!({
            "operator_id": {
                "open_id": "ou_from_operator_id"
            },
            "context": {
                "open_message_id": "om_from_context"
            },
            "action": {
                "value": {
                    "action": "close",
                    "session_id": "sess-1"
                }
            }
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");
        let parsed = parse_lark_card_action(&payload).expect("parse");

        assert_eq!(
            parsed.operator_open_id.as_deref(),
            Some("ou_from_operator_id"),
            "operator_open_id must be extracted from /operator_id/open_id"
        );
        assert_eq!(
            parsed.clicked_message_id.as_deref(),
            Some("om_from_context")
        );
    }

    #[test]
    fn normalize_lark_ws_card_action_from_raw_operator_wins_over_operator_id() {
        // When the raw event carries BOTH /operator and /operator_id,
        // the operator field is canonical and operator_id must NOT
        // override it (parse_lark_card_action checks /operator first).
        let raw = serde_json::json!({
            "operator": {
                "open_id": "ou_from_operator"
            },
            "operator_id": {
                "open_id": "ou_from_operator_id"
            },
            "context": {
                "open_message_id": "om_from_context"
            },
            "action": {
                "value": {
                    "action": "restart",
                    "session_id": "sess-1"
                }
            }
        });

        let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");
        let parsed = parse_lark_card_action(&payload).expect("parse");

        assert_eq!(
            parsed.operator_open_id.as_deref(),
            Some("ou_from_operator"),
            "/operator must win over /operator_id"
        );
        assert_eq!(
            parsed.clicked_message_id.as_deref(),
            Some("om_from_context")
        );
    }

    #[test]
    fn parse_lark_inbound_message_normalizes_topic_and_mentions() {
        let payload = serde_json::json!({
            "header": { "event_id": "evt-1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-1",
                    "root_id": "root-1",
                    "thread_id": "omt-1",
                    "chat_id": "chat-1",
                    "chat_type": "group",
                    "content": "{\"text\":\"@_bot_a /close\"}",
                    "mentions": [
                        { "key": "@_bot_a", "name": "BotA" }
                    ]
                }
            }
        });
        let parsed = parse_lark_inbound_message(&payload).expect("parsed message");
        assert_eq!(parsed.event_id, "evt-1");
        assert_eq!(parsed.message_id, "msg-1");
        assert_eq!(parsed.chat_id, "chat-1");
        assert_eq!(parsed.scope, SessionScope::Thread);
        assert_eq!(parsed.anchor, "omt-1");
        assert_eq!(parsed.text, "/close");
        assert_eq!(parsed.sender_open_id.as_deref(), Some("ou_user"));
        assert_eq!(parsed.sender_type.as_deref(), Some("user"));
        assert_eq!(parsed.mentions.len(), 1);
    }

    #[test]
    fn parse_lark_inbound_message_handles_quote_bubble_group_as_chat_scope() {
        let payload = serde_json::json!({
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" } },
                "message": {
                    "message_id": "msg-2",
                    "root_id": "root-quirk",
                    "chat_id": "chat-2",
                    "chat_type": "group",
                    "content": "{\"text\":\"continue please\"}"
                }
            }
        });
        let parsed = parse_lark_inbound_message(&payload).expect("parsed message");
        assert_eq!(parsed.event_id, "msg-2");
        assert_eq!(parsed.scope, SessionScope::Chat);
        assert_eq!(parsed.anchor, "chat-2");
        assert_eq!(parsed.text, "continue please");
    }

    #[test]
    fn parse_lark_inbound_message_rejects_missing_or_invalid_payload_bits() {
        let missing_message_id = serde_json::json!({
            "event": {
                "message": {
                    "chat_id": "chat-1",
                    "content": "{\"text\":\"hi\"}"
                }
            }
        });
        let err = parse_lark_inbound_message(&missing_message_id).expect_err("missing message_id");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert_eq!(err.1, "missing message_id");

        let invalid_content = serde_json::json!({
            "event": {
                "message": {
                    "message_id": "msg-3",
                    "chat_id": "chat-3",
                    "content": "{oops"
                }
            }
        });
        let err = parse_lark_inbound_message(&invalid_content).expect_err("invalid content");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.starts_with("invalid content json: "));
    }

    #[test]
    fn resolve_and_strip_leading_mentions_supports_lark_placeholder_keys() {
        let mentions = vec![LarkEventMention {
            key: "@_bot_a".to_string(),
            name: "BotA".to_string(),
        }];
        let resolved = resolve_lark_mentions("@_bot_a /close", &mentions);
        assert_eq!(resolved, "@BotA /close");
        assert_eq!(strip_leading_mentions(&resolved, &mentions), "/close");
    }

    #[test]
    fn strip_leading_mentions_prefers_longer_names_in_multi_bot_chains() {
        let mentions = vec![
            LarkEventMention {
                key: "@_claude".to_string(),
                name: "Claude".to_string(),
            },
            LarkEventMention {
                key: "@_claude_clone".to_string(),
                name: "Claude分身".to_string(),
            },
            LarkEventMention {
                key: "@_coco".to_string(),
                name: "CoCo".to_string(),
            },
        ];
        let resolved = resolve_lark_mentions("@_claude @_claude_clone @_coco /close", &mentions);
        assert_eq!(strip_leading_mentions(&resolved, &mentions), "/close");
    }

    #[test]
    fn strip_leading_mentions_leaves_non_prefix_mentions_in_place() {
        let mentions = vec![LarkEventMention {
            key: "@_bot_a".to_string(),
            name: "BotA".to_string(),
        }];
        let resolved = resolve_lark_mentions("hello @BotA how are you", &mentions);
        assert_eq!(
            strip_leading_mentions(&resolved, &mentions),
            "hello @BotA how are you"
        );
    }

    #[test]
    fn parse_chat_info_mode_p2p_from_chat_mode() {
        assert_eq!(parse_chat_info_mode("p2p", ""), ChatMode::P2p);
        assert_eq!(parse_chat_info_mode("P2P", ""), ChatMode::P2p);
    }

    #[test]
    fn parse_chat_info_mode_topic_from_chat_mode() {
        assert_eq!(parse_chat_info_mode("topic", ""), ChatMode::Topic);
        assert_eq!(parse_chat_info_mode("topic", "chat"), ChatMode::Topic);
    }

    #[test]
    fn parse_chat_info_mode_topic_from_group_message_type() {
        assert_eq!(parse_chat_info_mode("group", "thread"), ChatMode::Topic);
        assert_eq!(
            parse_chat_info_mode("someUnknown", "thread"),
            ChatMode::Topic
        );
    }

    #[test]
    fn parse_chat_info_mode_group_when_neither() {
        assert_eq!(parse_chat_info_mode("group", "chat"), ChatMode::Group);
        assert_eq!(parse_chat_info_mode("", ""), ChatMode::Group);
        assert_eq!(parse_chat_info_mode("group", ""), ChatMode::Group);
    }

    #[test]
    fn parse_lark_inbound_message_uses_locale_field_not_text() {
        let payload = serde_json::json!({
            "header": { "event_id": "evt-locale" },
            "event": {
                "sender": {
                    "sender_type": "user",
                    "sender_id": { "open_id": "ou_user" }
                },
                "message": {
                    "message_id": "msg-locale",
                    "chat_id": "chat-locale",
                    "chat_type": "group",
                    "locale": "zh-CN",
                    "content": "{\"text\":\"please investigate this\"}"
                }
            }
        });
        let parsed = parse_lark_inbound_message(&payload).expect("valid lark message");
        assert_eq!(parsed.text, "please investigate this");
        assert_eq!(parsed.locale.as_deref(), Some("zh"));
    }

    #[test]
    fn parse_force_topic_invocation_t_only() {
        assert_eq!(parse_force_topic_invocation("/t"), Some(String::new()));
    }

    #[test]
    fn parse_force_topic_invocation_t_with_content() {
        assert_eq!(
            parse_force_topic_invocation("/t hello world"),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn parse_force_topic_invocation_topic_only() {
        assert_eq!(parse_force_topic_invocation("/topic"), Some(String::new()));
    }

    #[test]
    fn parse_force_topic_invocation_topic_with_content() {
        assert_eq!(
            parse_force_topic_invocation("/topic some question"),
            Some("some question".to_string())
        );
    }

    #[test]
    fn parse_force_topic_invocation_no_match() {
        assert_eq!(parse_force_topic_invocation("hello"), None);
        assert_eq!(parse_force_topic_invocation("/slash not topic"), None);
        assert_eq!(parse_force_topic_invocation("/tsomething"), None);
    }

    #[test]
    fn parse_force_topic_invocation_leading_whitespace() {
        assert_eq!(
            parse_force_topic_invocation("  /t hello"),
            Some("hello".to_string())
        );
    }

    #[test]
    fn is_operate_command_recognizes_adopt_variants() {
        // Exact commands
        assert!(is_operate_command("/close"));
        assert!(is_operate_command("/restart"));
        assert!(is_operate_command("/card"));
        assert!(is_operate_command("/adopt"));
        assert!(is_operate_command("/adopt list"));
        // /adopt <target> variants
        assert!(is_operate_command("/adopt foo:bar"));
        assert!(is_operate_command("/adopt mysession"));
        assert!(is_operate_command("/adopt mysession:0.1"));
        assert!(is_operate_command("/adopt zellij foo:bar"));
        // Non-operate commands
        assert!(!is_operate_command("/adoption"));
        assert!(!is_operate_command("/adoptz"));
        assert!(!is_operate_command("hello"));
        assert!(!is_operate_command("/workflow run x"));
    }

    #[test]
    fn chat_mode_from_str_maps_correctly() {
        assert_eq!(ChatMode::from("p2p"), ChatMode::P2p);
        assert_eq!(ChatMode::from("P2P"), ChatMode::P2p);
        assert_eq!(ChatMode::from("topic"), ChatMode::Topic);
        assert_eq!(ChatMode::from("group"), ChatMode::Group);
        assert_eq!(ChatMode::from(""), ChatMode::Group);
        assert_eq!(ChatMode::from("unknown"), ChatMode::Group);
    }

    #[tokio::test]
    async fn send_input_keeps_live_card_when_turn_card_begin_fails() {
        let paths = temp_paths("send-input-turn-begin-fail");
        maybe_remove_dir(&paths.root().to_path_buf());

        let state = make_state(paths.clone(), HashMap::new());
        let mut session = make_session("sess-send-input");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.lark_app_id = "app-no-bot".to_string();
        session.stream_card_id = Some("om_live_old".to_string());
        session.stream_card_nonce = Some("nonce_live_old".to_string());
        session.current_screen = Some("old output".to_string());
        session.current_image_key = Some("img_live_old".to_string());
        session.last_screen_status = Some(ScreenStatus::Working);
        session.last_final_output_turn_id = Some("turn-old".to_string());
        session.last_cli_input = Some("previous input".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let mut child = tokio::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn worker");
        let stdin = child.stdin.take().expect("worker stdin");
        {
            let mut workers = state.workers.lock().await;
            workers.insert(
                session.session_id.clone(),
                WorkerHandle {
                    child,
                    stdin: Arc::new(Mutex::new(stdin)),
                },
            );
        }

        let response = send_input(
            State(state.clone()),
            AxumPath(session.session_id.clone()),
            Json(SessionInputRequest {
                content: "hello".to_string(),
                raw: false,
            }),
        )
        .await;
        assert_eq!(response, Ok(StatusCode::ACCEPTED));

        let stored = {
            let sessions = state.sessions.lock().await;
            sessions
                .get(&session.session_id)
                .cloned()
                .expect("stored session")
        };
        assert_eq!(stored.last_cli_input.as_deref(), Some("hello"));
        assert_eq!(stored.stream_card_id.as_deref(), Some("om_live_old"));
        assert_eq!(stored.stream_card_nonce.as_deref(), Some("nonce_live_old"));
        assert_eq!(stored.current_screen.as_deref(), Some("old output"));
        assert_eq!(stored.current_image_key.as_deref(), Some("img_live_old"));
        assert_eq!(stored.last_screen_status, Some(ScreenStatus::Working));
        assert_eq!(
            stored.last_final_output_turn_id.as_deref(),
            Some("turn-old")
        );

        let mut worker = {
            let mut workers = state.workers.lock().await;
            workers.remove(&session.session_id).expect("worker handle")
        };
        let _ = worker.child.kill().await;
        let _ = worker.child.wait().await;

        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
