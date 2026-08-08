use super::*;

/// Context returned after processing the webhook event pipeline.
/// Contains all parsed/computed values needed for the dispatch phase,
/// plus an optional early response (challenge, ignored, preflight-denied, etc.).
pub(crate) struct WebhookEventContext {
    pub(crate) bot: BotConfig,
    pub(crate) parsed: ParsedLarkInboundMessage,
    pub(crate) text: String,
    pub(crate) custom_trigger: Option<CustomTrigger>,
    pub(crate) trigger_activation: bool,
    pub(crate) inferred_locale: &'static str,
    pub(crate) scope: SessionScope,
    pub(crate) anchor: String,
    pub(crate) sender_open_id: Option<String>,
    pub(crate) talk: Option<TalkEvaluation>,
    pub(crate) message_id: String,
    pub(crate) chat_id: String,
    pub(crate) early_response: Option<Json<Value>>,
}

/// Process the webhook pipeline: bot lookup, HTTP verification, message parsing,
/// scope resolution, dedup, multi-bot gate, preflight, introduce command,
/// talk evaluation. Returns a context for the dispatch phase or an early response.
pub(crate) async fn process_webhook_event_maybe_response(
    state: &AppState,
    app_id: &str,
    payload: &Value,
    http_verification: Option<(HeaderMap, Bytes)>,
) -> Result<WebhookEventContext, (StatusCode, String)> {
    // --- Bot lookup + HTTP verification ---
    let bot = state
        .bots
        .get(app_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;
    if let Some((headers, body)) = http_verification.as_ref() {
        verify_lark_signature(state, &bot, headers, body)
            .map_err(|err| (StatusCode::UNAUTHORIZED, err.to_string()))?;
        verify_lark_token(state, &bot, payload)
            .map_err(|err| (StatusCode::UNAUTHORIZED, err.to_string()))?;
    }

    // --- Parse inbound message ---
    let mut parsed = parse_lark_inbound_message(payload)?;

    // --- Chat mode / scope resolution ---
    if parsed.scope == SessionScope::Chat && parsed.chat_type.as_deref() != Some("p2p") {
        let force_refresh = {
            let sessions = state.sessions.lock().await;
            sessions.values().any(|s| {
                s.scope == SessionScope::Chat
                    && s.chat_id == parsed.chat_id
                    && s.status == SessionStatus::Active
            })
        };
        match get_lark_chat_mode(state, &bot, &parsed.chat_id, force_refresh).await {
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

    // --- Dedup ---
    let deduped = if let Some(key) = lark_event_dedupe_key(app_id, &parsed.event_id) {
        dedupe_lark_event(state, &key).await
    } else {
        false
    };

    let message_id = parsed.message_id.clone();
    let chat_id = parsed.chat_id.clone();
    let text = parsed.text.clone();

    // --- Force-topic invocation ---
    let text = if let Some(stripped) = parse_force_topic_invocation(&text) {
        if parsed.scope == SessionScope::Chat {
            parsed.scope = SessionScope::Thread;
            parsed.anchor = parsed.message_id.clone();
        }
        stripped
    } else {
        text
    };
    let custom_trigger = if parsed.chat_type.as_deref() != Some("p2p") {
        resolve_custom_trigger(&text, &bot.custom_triggers).cloned()
    } else {
        None
    };
    let inferred_locale = prompt::infer_prompt_locale(&text);
    let scope = parsed.scope;
    let anchor = parsed.anchor.clone();

    // --- Context gathering ---
    let self_bot_open_id = load_self_bot_open_id_for_app(&state.paths, app_id);
    let mentioned_self_bot = current_bot_is_mentioned(&state.paths, app_id, &parsed);
    let group_stats = if parsed.chat_type.as_deref() != Some("p2p") {
        match lark_group_stats(state, &bot, &chat_id).await {
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
                && s.lark_app_id == *app_id
                && s.status == SessionStatus::Active
        })
    };
    let anchor_has_session = {
        let sessions = state.sessions.lock().await;
        crate::lark_dispatch::resolve_existing_lark_session(&sessions, app_id, &parsed).is_some()
    };
    // A custom trigger activates only when the message's own anchor has no
    // active session: a regular group is one Chat anchor (one session per
    // group), while each topic in a topic group is its own Thread anchor, so
    // a new topic can still trigger even when another topic owns a session.
    // Inside an existing session the keyword obeys the normal group rules.
    let trigger_activation = custom_trigger.is_some() && !anchor_has_session;
    let is_oncall_chat = bot
        .oncall_chats
        .iter()
        .any(|oc| oc.chat_id == parsed.chat_id);
    let peer_ids = peer_bot_open_ids_for_app(&state.paths, app_id);
    let is_known_peer_bot = parsed
        .sender_open_id
        .as_deref()
        .map(|sid| peer_ids.iter().any(|id| id == sid))
        .unwrap_or(false);
    let has_chat_grant = parsed
        .sender_open_id
        .as_deref()
        .map(|sid| {
            bot.chat_grants
                .get(&parsed.chat_id)
                .map(|granted| granted.iter().any(|id| id == sid))
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let has_global_grant = parsed
        .sender_open_id
        .as_deref()
        .map(|sid| bot.global_grants.iter().any(|id| id == sid))
        .unwrap_or(false);
    let sender_open_id = parsed.sender_open_id.clone();
    let sender_type = parsed.sender_type.as_deref();

    // --- Multi-bot gate ---
    if !decide_multibot_inbound_gate(
        sender_type,
        sender_open_id.as_deref(),
        self_bot_open_id.as_deref(),
        mentioned_self_bot,
        trigger_activation,
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
        return Ok(WebhookEventContext {
            bot,
            parsed,
            text,
            custom_trigger: custom_trigger.clone(),
            trigger_activation,
            inferred_locale,
            scope,
            anchor,
            sender_open_id,
            talk: None,
            message_id: message_id.to_string(),
            chat_id: chat_id.to_string(),
            early_response: Some(Json(
                serde_json::json!({ "ok": true, "ignored": "multi_bot_gate" }),
            )),
        });
    }

    // --- Preflight ---
    match evaluate_lark_preflight(
        state,
        &bot,
        &text,
        &chat_id,
        sender_open_id.as_deref(),
        deduped,
        trigger_activation,
    ) {
        LarkPreflight::Deduped => {
            return Ok(WebhookEventContext {
                bot,
                parsed,
                text,
                custom_trigger: custom_trigger.clone(),
                trigger_activation,
                inferred_locale,
                scope,
                anchor,
                sender_open_id,
                talk: None,
                message_id: message_id.to_string(),
                chat_id: chat_id.to_string(),
                early_response: Some(Json(serde_json::json!({ "ok": true, "deduped": true }))),
            });
        }
        LarkPreflight::IgnoredEmptyText => {
            return Ok(WebhookEventContext {
                bot,
                parsed,
                text,
                custom_trigger: custom_trigger.clone(),
                trigger_activation,
                inferred_locale,
                scope,
                anchor,
                sender_open_id,
                talk: None,
                message_id: message_id.to_string(),
                chat_id: chat_id.to_string(),
                early_response: Some(Json(
                    serde_json::json!({ "ok": true, "ignored": "empty_text" }),
                )),
            });
        }
        LarkPreflight::Denied { reply } => {
            let _ = lark_reply_message(state, &bot, &message_id, reply).await;
            return Ok(WebhookEventContext {
                bot,
                parsed,
                text,
                custom_trigger: custom_trigger.clone(),
                trigger_activation,
                inferred_locale,
                scope,
                anchor,
                sender_open_id,
                talk: None,
                message_id: message_id.to_string(),
                chat_id: chat_id.to_string(),
                early_response: Some(Json(serde_json::json!({ "ok": true, "denied": true }))),
            });
        }
        LarkPreflight::Continue => {}
    }

    // --- Introduce command ---
    if handle_introduce_command(state, app_id, &chat_id, &message_id, &parsed).await? {
        return Ok(WebhookEventContext {
            bot,
            parsed,
            text,
            custom_trigger: custom_trigger.clone(),
            trigger_activation,
            inferred_locale,
            scope,
            anchor,
            sender_open_id,
            talk: None,
            message_id: message_id.to_string(),
            chat_id: chat_id.to_string(),
            early_response: Some(Json(serde_json::json!({ "ok": true, "introduced": true }))),
        });
    }

    // --- Talk evaluation ---
    let talk = sender_open_id
        .as_deref()
        .map(|sender| evaluate_talk_for_bot_with_state(state, &bot, &chat_id, sender));

    Ok(WebhookEventContext {
        bot,
        parsed,
        text,
        custom_trigger: custom_trigger.clone(),
        trigger_activation,
        inferred_locale,
        scope,
        anchor,
        sender_open_id,
        talk,
        message_id,
        chat_id,
        early_response: None,
    })
}
