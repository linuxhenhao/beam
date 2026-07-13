use super::*;

pub(crate) async fn begin_lark_turn_card(
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
        || session.terminal_url.is_none()
    {
        return Ok(());
    }
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };
    if let Err(err) = park_stream_card(&state.paths, &session).await {
        warn!("failed to park stream card for {}: {}", session_id, err);
    }

    let mut session_for_card = session.clone();
    session_for_card.stream_card_id = None;
    session_for_card.stream_card_nonce = Some(Uuid::new_v4().simple().to_string());
    session_for_card.current_image_key = None;
    session_for_card.current_screen = None;
    session_for_card.last_screen_status = None;
    session_for_card.last_final_output_turn_id = None;
    let expected_previous_stream_card_id = session.stream_card_id.clone();
    let expected_last_cli_input = session.last_cli_input.clone();
    let new_nonce = session_for_card
        .stream_card_nonce
        .clone()
        .context("turn card nonce missing")?;
    let card_id = lark_reply_card_with_opts(
        state,
        bot,
        &session_for_card.root_message_id,
        &build_streaming_card(&session_for_card, status),
        session_for_card.scope == SessionScope::Thread,
    )
    .await?;

    let updated_session = {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            match sessions.get_mut(session_id) {
                Some(entry)
                    if entry.last_cli_input == expected_last_cli_input
                        && entry.stream_card_id == expected_previous_stream_card_id =>
                {
                    entry.stream_card_id = Some(card_id.clone());
                    entry.stream_card_nonce = Some(new_nonce.clone());
                    entry.current_image_key = None;
                    entry.current_screen = None;
                    entry.last_screen_status = None;
                    entry.last_final_output_turn_id = None;
                    Some((entry.clone(), sessions.clone()))
                }
                Some(_) | None => None,
            }
        };
        let Some((entry, sessions_snapshot)) = snapshot else {
            if let Err(err) = lark_delete_message(state, bot, &card_id).await {
                warn!(
                    "failed to delete orphan turn card {} for {}: {}",
                    card_id, session_id, err
                );
            }
            return Ok(());
        };
        persist_sessions(&state.paths, &sessions_snapshot).await?;
        entry
    };

    let state = state.clone();
    tokio::spawn(async move {
        let _ = recall_frozen_cards(&state, &updated_session).await;
    });

    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;
    use serde_json::Value;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct TurnCardServerState {
        reply_requests: Arc<Mutex<Vec<Value>>>,
        delete_requests: Arc<Mutex<Vec<String>>>,
        delete_notify: Arc<Notify>,
    }

    async fn start_turn_card_mock_lark_server(
        reply_status: StatusCode,
    ) -> (String, TurnCardServerState) {
        let state = TurnCardServerState {
            reply_requests: Arc::new(Mutex::new(Vec::new())),
            delete_requests: Arc::new(Mutex::new(Vec::new())),
            delete_notify: Arc::new(Notify::new()),
        };
        let reply_state = state.clone();
        let delete_state = state.clone();
        let app = Router::new()
            .route(
                "/auth/v3/tenant_access_token/internal",
                post(|| async {
                    Json(serde_json::json!({
                        "code": 0,
                        "tenant_access_token": "mock-token",
                        "expire": 7200,
                    }))
                }),
            )
            .route(
                "/im/v1/messages/{message_id}/reply",
                post(
                    move |AxumPath(_message_id): AxumPath<String>, Json(body): Json<Value>| {
                        let reply_state = reply_state.clone();
                        async move {
                            reply_state.reply_requests.lock().await.push(body);
                            if reply_status.is_success() {
                                (
                                    StatusCode::OK,
                                    Json(serde_json::json!({
                                        "code": 0,
                                        "data": { "message_id": "om_turn_card" },
                                    })),
                                )
                            } else {
                                (
                                    reply_status,
                                    Json(serde_json::json!({
                                        "code": 500,
                                        "msg": "reply failed",
                                    })),
                                )
                            }
                        }
                    },
                ),
            )
            .route(
                "/im/v1/messages/{message_id}",
                axum::routing::delete(move |AxumPath(message_id): AxumPath<String>| {
                    let delete_state = delete_state.clone();
                    async move {
                        delete_state.delete_requests.lock().await.push(message_id);
                        delete_state.delete_notify.notified().await;
                        Json(serde_json::json!({
                            "code": 0,
                            "msg": "ok",
                        }))
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock turn card server");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{}", addr), state)
    }

    #[derive(Clone)]
    struct TurnCardCasRaceServerState {
        reply_received: Arc<Notify>,
        reply_release: Arc<Notify>,
        delete_requests: Arc<Mutex<Vec<String>>>,
    }

    async fn start_turn_card_cas_race_server() -> (String, TurnCardCasRaceServerState) {
        let state = TurnCardCasRaceServerState {
            reply_received: Arc::new(Notify::new()),
            reply_release: Arc::new(Notify::new()),
            delete_requests: Arc::new(Mutex::new(Vec::new())),
        };
        let reply_state = state.clone();
        let delete_state = state.clone();
        let app = Router::new()
            .route(
                "/auth/v3/tenant_access_token/internal",
                post(|| async {
                    Json(serde_json::json!({
                        "code": 0,
                        "tenant_access_token": "mock-token",
                        "expire": 7200,
                    }))
                }),
            )
            .route(
                "/im/v1/messages/{message_id}/reply",
                post(
                    move |AxumPath(_message_id): AxumPath<String>, Json(_body): Json<Value>| {
                        let reply_state = reply_state.clone();
                        async move {
                            reply_state.reply_received.notify_one();
                            reply_state.reply_release.notified().await;
                            (
                                StatusCode::OK,
                                Json(serde_json::json!({
                                    "code": 0,
                                    "data": { "message_id": "om_turn_card_orphan" },
                                })),
                            )
                        }
                    },
                ),
            )
            .route(
                "/im/v1/messages/{message_id}",
                axum::routing::delete(move |AxumPath(message_id): AxumPath<String>| {
                    let delete_state = delete_state.clone();
                    async move {
                        delete_state.delete_requests.lock().await.push(message_id);
                        Json(serde_json::json!({
                            "code": 0,
                            "msg": "ok",
                        }))
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock turn card cas race server");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{}", addr), state)
    }

    #[tokio::test]
    async fn begin_lark_turn_card_replaces_live_card_and_recalls_old_card() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let (base_url, server_state) = start_turn_card_mock_lark_server(StatusCode::OK).await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("turn-begin-success");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-turn-success";
        let bot = make_bot(app_id);
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));
        let mut session = make_session("sess-turn-success");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.lark_app_id = app_id.to_string();
        session.terminal_url = Some("http://127.0.0.1:9000".to_string());
        session.stream_card_id = Some("om_old_turn".to_string());
        session.stream_card_nonce = Some("nonce_old_turn".to_string());
        session.display_mode = Some(DisplayMode::Screenshot);
        session.current_screen = Some("old output".to_string());
        session.current_image_key = Some("img_old_turn".to_string());
        session.last_screen_status = Some(ScreenStatus::Working);
        session.last_final_output_turn_id = Some("turn-old".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        begin_lark_turn_card(&state, &session.session_id, "starting")
            .await
            .expect("begin turn card");

        let stored = {
            let sessions = state.sessions.lock().await;
            sessions
                .get(&session.session_id)
                .cloned()
                .expect("stored session")
        };
        assert_eq!(stored.stream_card_id.as_deref(), Some("om_turn_card"));
        assert_ne!(stored.stream_card_nonce.as_deref(), Some("nonce_old_turn"));
        assert!(stored.stream_card_nonce.is_some());
        assert_eq!(stored.current_screen, None);
        assert_eq!(stored.current_image_key, None);
        assert_eq!(stored.last_screen_status, None);
        assert_eq!(stored.last_final_output_turn_id, None);
        assert_eq!(stored.pending_response_card_id, None);
        assert_eq!(stored.pending_response_card_state, None);

        let reply_body = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                let requests = server_state.reply_requests.lock().await;
                if let Some(body) = requests.first().cloned() {
                    break body;
                }
                drop(requests);
                assert!(
                    std::time::Instant::now() < deadline,
                    "turn card reply was not posted"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        let content = reply_body
            .get("content")
            .and_then(Value::as_str)
            .expect("reply content");
        assert!(content.contains("waiting for screenshot"));
        assert!(content.contains(stored.stream_card_nonce.as_deref().unwrap()));

        let deleted_message_id = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                let requests = server_state.delete_requests.lock().await;
                if let Some(message_id) = requests.first().cloned() {
                    break message_id;
                }
                drop(requests);
                assert!(
                    std::time::Instant::now() < deadline,
                    "old card was not recalled"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        assert_eq!(deleted_message_id, "om_old_turn");

        let frozen_before_release = load_frozen_cards(&paths, &session.session_id)
            .await
            .expect("load frozen cards before delete release");
        assert!(frozen_before_release.contains_key("nonce_old_turn"));

        server_state.delete_notify.notify_one();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let frozen_after_release = load_frozen_cards(&paths, &session.session_id)
                .await
                .expect("load frozen cards after delete release");
            if frozen_after_release.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "frozen cards were not cleared after recall"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn begin_lark_turn_card_keeps_old_live_card_when_send_fails() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let (base_url, server_state) =
            start_turn_card_mock_lark_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("turn-begin-failure");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-turn-failure";
        let bot = make_bot(app_id);
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));
        let mut session = make_session("sess-turn-failure");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.lark_app_id = app_id.to_string();
        session.terminal_url = Some("http://127.0.0.1:9000".to_string());
        session.stream_card_id = Some("om_old_turn".to_string());
        session.stream_card_nonce = Some("nonce_old_turn".to_string());
        session.current_screen = Some("old output".to_string());
        session.current_image_key = Some("img_old_turn".to_string());
        session.last_screen_status = Some(ScreenStatus::Working);
        session.last_final_output_turn_id = Some("turn-old".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let result = begin_lark_turn_card(&state, &session.session_id, "starting").await;
        assert!(result.is_err());

        let stored = {
            let sessions = state.sessions.lock().await;
            sessions
                .get(&session.session_id)
                .cloned()
                .expect("stored session")
        };
        assert_eq!(stored.stream_card_id.as_deref(), Some("om_old_turn"));
        assert_eq!(stored.stream_card_nonce.as_deref(), Some("nonce_old_turn"));
        assert_eq!(stored.current_screen.as_deref(), Some("old output"));
        assert_eq!(stored.current_image_key.as_deref(), Some("img_old_turn"));
        assert_eq!(stored.last_screen_status, Some(ScreenStatus::Working));
        assert_eq!(
            stored.last_final_output_turn_id.as_deref(),
            Some("turn-old")
        );
        assert_eq!(stored.pending_response_card_id, None);
        assert_eq!(stored.pending_response_card_state, None);

        let frozen_cards = load_frozen_cards(&paths, &session.session_id)
            .await
            .expect("load frozen cards");
        assert!(frozen_cards.contains_key("nonce_old_turn"));
        assert!(server_state.delete_requests.lock().await.is_empty());

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn begin_lark_turn_card_deletes_orphan_card_when_session_changes_before_commit() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let (base_url, server_state) = start_turn_card_cas_race_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("turn-begin-cas-race");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-turn-cas-race";
        let bot = make_bot(app_id);
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));
        let mut session = make_session("sess-turn-cas-race");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.lark_app_id = app_id.to_string();
        session.terminal_url = Some("http://127.0.0.1:9000".to_string());
        session.stream_card_id = Some("om_old_turn".to_string());
        session.stream_card_nonce = Some("nonce_old_turn".to_string());
        session.display_mode = Some(DisplayMode::Screenshot);
        session.current_screen = Some("old output".to_string());
        session.current_image_key = Some("img_old_turn".to_string());
        session.last_screen_status = Some(ScreenStatus::Working);
        session.last_final_output_turn_id = Some("turn-old".to_string());
        session.last_cli_input = Some("old input".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let state_for_task = state.clone();
        let session_id = session.session_id.clone();
        let begin_task = tokio::spawn(async move {
            begin_lark_turn_card(&state_for_task, &session_id, "starting").await
        });

        server_state.reply_received.notified().await;
        {
            let mut sessions = state.sessions.lock().await;
            let entry = sessions
                .get_mut(&session.session_id)
                .expect("session still exists");
            entry.last_cli_input = Some("other input".to_string());
            entry.stream_card_id = Some("om_live_new".to_string());
            entry.stream_card_nonce = Some("nonce_live_new".to_string());
            entry.current_screen = Some("new output".to_string());
            entry.current_image_key = Some("img_live_new".to_string());
        }
        server_state.reply_release.notify_one();

        begin_task
            .await
            .expect("begin task")
            .expect("begin turn card");

        let stored = {
            let sessions = state.sessions.lock().await;
            sessions
                .get(&session.session_id)
                .cloned()
                .expect("stored session")
        };
        assert_eq!(stored.last_cli_input.as_deref(), Some("other input"));
        assert_eq!(stored.stream_card_id.as_deref(), Some("om_live_new"));
        assert_eq!(stored.stream_card_nonce.as_deref(), Some("nonce_live_new"));
        assert_eq!(stored.current_screen.as_deref(), Some("new output"));
        assert_eq!(stored.current_image_key.as_deref(), Some("img_live_new"));

        let deleted_message_id = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                let requests = server_state.delete_requests.lock().await;
                if let Some(message_id) = requests.first().cloned() {
                    break message_id;
                }
                drop(requests);
                assert!(
                    std::time::Instant::now() < deadline,
                    "orphan card was not deleted"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        assert_eq!(deleted_message_id, "om_turn_card_orphan");
        assert_ne!(deleted_message_id, "om_live_new");

        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
