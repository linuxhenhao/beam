use super::*;

pub(crate) async fn ensure_lark_pending_card(state: &AppState, session_id: &str) -> Result<()> {
    let snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = snapshot else {
        return Ok(());
    };
    if session.lark_app_id == "local"
        || session.root_message_id.is_empty()
        || session.stream_card_id.is_some()
    {
        return Ok(());
    }
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            if let Some(entry) = sessions.get_mut(session_id) {
                ensure_stream_card_nonce(entry);
            }
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot).await?;
    }
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = session else {
        return Ok(());
    };
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };
    let card_id = lark_reply_card_with_opts(
        state,
        bot,
        &session.root_message_id,
        &build_streaming_card(&session, "starting"),
        session.scope == SessionScope::Thread,
    )
    .await?;
    let snapshot = {
        let mut sessions = state.sessions.lock().await;
        if let Some(entry) = sessions.get_mut(session_id) {
            entry.stream_card_id = Some(card_id.clone());
            start_pending_response_turn(entry, card_id.clone());
        }
        sessions.clone()
    };
    persist_sessions(&state.paths, &snapshot).await?;
    if let Some(session) = snapshot.get(session_id) {
        let _ = recall_frozen_cards(state, session).await;
    }
    Ok(())
}

pub(crate) async fn ensure_lark_streaming_card(
    state: &AppState,
    session_id: &str,
    status: &str,
) -> Result<()> {
    let snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = snapshot else {
        return Ok(());
    };
    if session.lark_app_id == "local"
        || session.root_message_id.is_empty()
        || session.stream_card_id.is_some()
    {
        return Ok(());
    }
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            if let Some(entry) = sessions.get_mut(session_id) {
                ensure_stream_card_nonce(entry);
            }
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot).await?;
    }
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = session else {
        return Ok(());
    };
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };
    let card_id = lark_reply_card_with_opts(
        state,
        bot,
        &session.root_message_id,
        &build_streaming_card(&session, status),
        session.scope == SessionScope::Thread,
    )
    .await?;
    let snapshot = {
        let mut sessions = state.sessions.lock().await;
        if let Some(entry) = sessions.get_mut(session_id) {
            entry.stream_card_id = Some(card_id.clone());
        }
        sessions.clone()
    };
    persist_sessions(&state.paths, &snapshot).await?;
    if let Some(session) = snapshot.get(session_id) {
        let _ = recall_frozen_cards(state, session).await;
    }
    Ok(())
}

pub(crate) async fn patch_lark_streaming_card(
    state: &AppState,
    session_id: &str,
    status: &str,
) -> Result<()> {
    let snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = snapshot else {
        return Ok(());
    };
    if session.lark_app_id == "local" {
        return Ok(());
    }
    let Some(card_id) = session.stream_card_id.clone() else {
        return ensure_lark_streaming_card(state, session_id, status).await;
    };
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };
    lark_update_card(
        state,
        bot,
        &card_id,
        &build_streaming_card(&session, status),
    )
    .await
}

pub(crate) fn arm_usage_limit_retry_timer(
    state: AppState,
    session_id: String,
    usage_limit: CliUsageLimitState,
) {
    if usage_limit.retry_ready {
        return;
    }
    let delay_ms = usage_limit.retry_at_ms.saturating_sub(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    );
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let updated_session = {
            let snapshot = {
                let mut sessions = state.sessions.lock().await;
                let Some(entry) = sessions.get_mut(&session_id) else {
                    return;
                };
                let Some(current) = entry.usage_limit.as_mut() else {
                    return;
                };
                if !usage_limit_matches(current, &usage_limit) || current.retry_ready {
                    return;
                }
                current.retry_ready = true;
                Some((entry.clone(), sessions.clone()))
            };
            let Some((entry, sessions_snapshot)) = snapshot else {
                return;
            };
            if persist_sessions(&state.paths, &sessions_snapshot)
                .await
                .is_err()
            {
                return;
            }
            entry
        };
        let _ =
            patch_lark_streaming_card(&state, &session_id, session_stream_status(&updated_session))
                .await;
    });
}

pub(crate) async fn post_or_refresh_lark_session_card(
    state: &AppState,
    session_id: &str,
) -> Result<LarkCardDeliveryPlan> {
    let snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = snapshot else {
        anyhow::bail!("session not found: {}", session_id);
    };
    let plan = decide_lark_card_delivery(&session);
    match plan {
        LarkCardDeliveryPlan::NotReady => Ok(plan),
        LarkCardDeliveryPlan::PatchExisting => {
            patch_lark_streaming_card(state, session_id, session_stream_status(&session)).await?;
            Ok(plan)
        }
        LarkCardDeliveryPlan::PostNew => {
            let Some(bot) = state.bots.get(&session.lark_app_id) else {
                return Ok(LarkCardDeliveryPlan::NotReady);
            };
            let card_id = lark_reply_card_with_opts(
                state,
                bot,
                &session.root_message_id,
                &build_streaming_card(&session, session_stream_status(&session)),
                session.scope == SessionScope::Thread,
            )
            .await?;
            let snapshot = {
                let mut sessions = state.sessions.lock().await;
                if let Some(entry) = sessions.get_mut(session_id) {
                    entry.stream_card_id = Some(card_id.clone());
                }
                sessions.clone()
            };
            persist_sessions(&state.paths, &snapshot).await?;
            if let Some(session) = snapshot.get(session_id) {
                let _ = recall_frozen_cards(state, session).await;
            }
            Ok(plan)
        }
    }
}

pub(crate) fn session_anchor_matches(
    session: &Session,
    lark_app_id: &str,
    chat_id: &str,
    anchor: &str,
) -> bool {
    if session.status != SessionStatus::Active || session.lark_app_id != lark_app_id {
        return false;
    }
    match session.scope {
        SessionScope::Chat => session.chat_id == chat_id,
        SessionScope::Thread => {
            session.chat_id == chat_id
                && (session.thread_id.as_deref() == Some(anchor)
                    || (session.chat_type.as_deref() == Some("p2p")
                        && session.root_message_id == anchor))
        }
    }
}

pub(crate) fn decide_lark_routing<'a>(
    message_id: &'a str,
    chat_id: &'a str,
    chat_type: Option<&str>,
    root_id: Option<&'a str>,
    thread_id: Option<&'a str>,
) -> (SessionScope, &'a str) {
    if chat_type == Some("p2p") {
        if let Some(rid) = root_id.filter(|v| !v.is_empty()) {
            return (SessionScope::Thread, rid);
        }
        if let Some(tid) = thread_id.filter(|v| !v.is_empty()) {
            return (SessionScope::Thread, tid);
        }
        return (SessionScope::Thread, message_id);
    }
    if let Some(tid) = thread_id.filter(|v| !v.is_empty()) {
        return (SessionScope::Thread, tid);
    }
    match chat_type.unwrap_or("group") {
        "p2p" => (SessionScope::Thread, message_id),
        _ => (SessionScope::Chat, chat_id),
    }
}
