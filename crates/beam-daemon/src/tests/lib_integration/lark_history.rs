use super::*;

#[test]
fn parses_lark_history_text_and_post_content() {
    let item = serde_json::json!({
        "message_id": "om_1",
        "root_id": "om_root",
        "thread_id": "omt_1",
        "chat_id": "oc_1",
        "msg_type": "post",
        "body": {
            "content": serde_json::json!({
                "content": [[
                    { "tag": "text", "text": "hello" },
                    { "tag": "a", "text": "link" }
                ]]
            }).to_string()
        },
        "sender": { "id": "ou_1", "sender_type": "user" },
        "create_time": "1234"
    });
    let parsed = parse_lark_history_message(&item);
    assert_eq!(parsed.message_id, "om_1");
    assert_eq!(parsed.root_id.as_deref(), Some("om_root"));
    assert_eq!(parsed.content, "hello link");
    assert_eq!(parsed.create_time, Some(1234));
}

#[tokio::test]
async fn list_chat_history_returns_chronological_tail() {
    let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
    let base_url = start_mock_lark_server().await;
    let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
    let app_id = "app-history-chat";
    let bot = make_bot(app_id);
    let state = make_state(
        temp_paths("history-chat"),
        HashMap::from([(app_id.to_string(), bot.clone())]),
    );

    let raw = lark_list_chat_history(&state, &bot, "chat-1", 10)
        .await
        .expect("chat history");
    let messages = raw
        .iter()
        .map(parse_lark_history_message)
        .collect::<Vec<_>>();
    assert_eq!(
        messages
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        vec!["chat oldest", "chat newest"]
    );
}

#[tokio::test]
async fn list_thread_history_uses_session_thread_id_first() {
    let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
    let base_url = start_mock_lark_server().await;
    let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
    let app_id = "app-history-thread";
    let bot = make_bot(app_id);
    let state = make_state(
        temp_paths("history-thread"),
        HashMap::from([(app_id.to_string(), bot.clone())]),
    );
    let mut session = make_session("sess-history-thread");
    session.lark_app_id = app_id.to_string();
    session.root_message_id = "omt_not_a_message".to_string();
    session.thread_id = Some("omt_direct_thread".to_string());

    let raw = lark_list_thread_history(&state, &bot, &session, 10)
        .await
        .expect("thread history");
    let messages = raw
        .iter()
        .map(parse_lark_history_message)
        .collect::<Vec<_>>();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].thread_id.as_deref(), Some("omt_direct_thread"));
    assert_eq!(messages[0].content, "thread one");
}

#[tokio::test]
async fn quoted_message_api_fetches_message_detail() {
    let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
    let base_url = start_mock_lark_server().await;
    let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
    let app_id = "app-quoted";
    let state = make_state(
        temp_paths("quoted"),
        HashMap::from([(app_id.to_string(), make_bot(app_id))]),
    );
    let mut session = make_session("sess-quoted");
    session.lark_app_id = app_id.to_string();
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(session.session_id.clone(), session.clone());
    }

    let Json(value) = quoted_message(
        State(state),
        AxumPath((session.session_id.clone(), "om_quoted".to_string())),
    )
    .await
    .expect("quoted message");
    assert_eq!(
        value.pointer("/message/messageId").and_then(Value::as_str),
        Some("om_quoted")
    );
    assert_eq!(
        value.pointer("/message/content").and_then(Value::as_str),
        Some("quoted detail")
    );
}

#[tokio::test]
async fn quoted_message_api_errors_when_session_missing() {
    let app_id = "app-quoted-missing";
    let state = make_state(
        temp_paths("quoted-missing"),
        HashMap::from([(app_id.to_string(), make_bot(app_id))]),
    );

    let err = quoted_message(
        State(state),
        AxumPath(("missing-session".to_string(), "om_quoted".to_string())),
    )
    .await
    .expect_err("missing session should fail");
    assert_eq!(err.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn session_history_api_returns_session_scoped_messages() {
    let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
    let base_url = start_mock_lark_server().await;
    let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
    let app_id = "app-history-api";
    let state = make_state(
        temp_paths("history-api"),
        HashMap::from([(app_id.to_string(), make_bot(app_id))]),
    );
    let mut session = make_session("sess-history-api");
    session.lark_app_id = app_id.to_string();
    session.scope = SessionScope::Chat;
    session.status = SessionStatus::Active;
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(session.session_id.clone(), session.clone());
    }

    let Json(value) = session_history(
        State(state),
        AxumPath(session.session_id.clone()),
        Query(LarkHistoryQuery {
            limit: Some(10),
            scope: Some("session".to_string()),
        }),
    )
    .await
    .expect("history api");
    assert_eq!(value.get("scope").and_then(Value::as_str), Some("chat"));
    assert_eq!(value.get("total").and_then(Value::as_u64), Some(2));
    assert_eq!(
        value.pointer("/messages/0/content").and_then(Value::as_str),
        Some("chat oldest")
    );
}
