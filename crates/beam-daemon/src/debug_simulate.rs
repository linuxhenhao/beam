use super::*;

#[derive(Debug, serde::Deserialize)]
pub(crate) struct SimulateLarkMessageRequest {
    pub session_id: String,
    pub sender_open_id: String,
    pub text: String,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct SimulateLarkMessageResponse {
    pub session_id: String,
    pub event_id: String,
    pub message_id: String,
    pub handler_result: Value,
    pub current_turn_id: Option<String>,
    pub last_cli_input: Option<String>,
}

/// POST /debug/simulate/lark-message
///
/// Validates the session exists and is Active, constructs a canonical
/// im.message.receive_v1 payload derived from the session's routing metadata,
/// and calls handle_lark_event_payload.  No new dispatch paths are created.
///
/// On success returns 200 with handler_result, event_id, message_id,
/// and post-call session state.  Propagates handler errors as HTTP errors.
pub(crate) async fn simulate_lark_message_handler(
    State(state): State<AppState>,
    Json(body): Json<SimulateLarkMessageRequest>,
) -> Result<Json<SimulateLarkMessageResponse>, (StatusCode, String)> {
    // --- validate input (trim only sender_open_id; keep text verbatim) ---
    let sender_open_id = body.sender_open_id.trim().to_string();
    if sender_open_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "sender_open_id must not be empty".to_string(),
        ));
    }
    if body.text.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "text must not be empty".to_string(),
        ));
    }

    // --- resolve session ---
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(&body.session_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("session {} not found", body.session_id),
            )
        })?
    };
    if session.status != SessionStatus::Active {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("session {} is not active", body.session_id),
        ));
    }

    let app_id = session.lark_app_id.clone();
    let event_id = Uuid::new_v4().to_string();
    let message_id = Uuid::new_v4().to_string();

    // --- build canonical payload from session routing metadata ---
    let mut message_obj = serde_json::Map::new();
    message_obj.insert("message_id".to_string(), Value::String(message_id.clone()));
    message_obj.insert(
        "chat_id".to_string(),
        Value::String(session.chat_id.clone()),
    );
    if let Some(ref ct) = session.chat_type {
        message_obj.insert("chat_type".to_string(), Value::String(ct.clone()));
    }
    if session.scope == SessionScope::Thread {
        message_obj.insert(
            "root_id".to_string(),
            Value::String(session.root_message_id.clone()),
        );
        if let Some(ref tid) = session.thread_id {
            message_obj.insert("thread_id".to_string(), Value::String(tid.clone()));
        }
    }
    // content is a JSON-encoded string per Lark protocol; preserve original text
    message_obj.insert(
        "content".to_string(),
        Value::String(serde_json::json!({ "text": body.text }).to_string()),
    );

    let payload = serde_json::json!({
        "schema": "2.0",
        "header": {
            "event_type": "im.message.receive_v1",
            "event_id": event_id,
        },
        "event": {
            "sender": {
                "sender_id": { "open_id": sender_open_id },
                "sender_type": "user",
            },
            "message": Value::Object(message_obj),
        },
    });

    // --- call the real handler; propagate errors (do not wrap as 200) ---
    let handler_json = handle_lark_event_payload(state.clone(), app_id, payload, None).await?;

    // --- collect post-call session state ---
    let (current_turn_id, last_cli_input) = {
        let sessions = state.sessions.lock().await;
        match sessions.get(&body.session_id) {
            Some(s) => (s.current_turn_id.clone(), s.last_cli_input.clone()),
            None => (None, None),
        }
    };

    Ok(Json(SimulateLarkMessageResponse {
        session_id: body.session_id,
        event_id,
        message_id,
        handler_result: handler_json.0,
        current_turn_id,
        last_cli_input,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;
    use beam_core::{SessionScope, SessionStatus};
    use std::collections::HashMap;

    // shared helpers -------------------------------------------------------

    fn make_p2p_session(session_id: &str) -> Session {
        let mut s = make_session(session_id);
        s.status = SessionStatus::Active;
        s.closed_at = None;
        s.chat_id = "chat-p2p".into();
        s.chat_type = Some("p2p".into());
        s.root_message_id = "root-p2p".into();
        s.scope = SessionScope::Thread;
        s.thread_id = Some("omt_p2p_thread".into());
        s.lark_app_id = "app-1".into();
        s.last_cli_input = None;
        s.current_turn_id = None;
        s
    }

    fn make_group_thread_session(session_id: &str) -> Session {
        let mut s = make_session(session_id);
        s.status = SessionStatus::Active;
        s.closed_at = None;
        s.chat_id = "chat-grp".into();
        s.chat_type = Some("group".into());
        s.root_message_id = "root-grp".into();
        s.scope = SessionScope::Thread;
        s.thread_id = Some("omt_grp_thread".into());
        s.lark_app_id = "app-1".into();
        s.last_cli_input = None;
        s.current_turn_id = None;
        s
    }

    fn new_state(paths: beam_core::BeamPaths, bot: beam_core::BotConfig) -> AppState {
        let state = make_state(paths, HashMap::from([(bot.lark_app_id.clone(), bot)]));
        let _ = std::fs::create_dir_all(state.paths.sessions_dir());
        state
    }

    async fn insert(state: &AppState, sess: Session) {
        state
            .sessions
            .lock()
            .await
            .insert(sess.session_id.clone(), sess);
    }

    fn call(
        state: AppState,
        session_id: &str,
        sender_open_id: &str,
        text: &str,
    ) -> impl std::future::Future<Output = Result<Json<SimulateLarkMessageResponse>, (StatusCode, String)>>
    {
        simulate_lark_message_handler(
            State(state),
            Json(SimulateLarkMessageRequest {
                session_id: session_id.to_string(),
                sender_open_id: sender_open_id.to_string(),
                text: text.to_string(),
            }),
        )
    }

    // positive: p2p + worker → send_input runs, current_turn_id / last_cli_input set

    #[tokio::test]
    async fn simulate_lark_message_runs_send_input_for_p2p_session() {
        let paths = temp_paths("sim-send-input");
        maybe_remove_dir(&paths.root().to_path_buf());
        let sess = make_p2p_session("sess-w");
        let state = new_state(paths.clone(), make_bot("app-1"));
        insert(&state, sess).await;

        // spawn a /bin/cat worker so ensure_worker_for_session succeeds
        let mut child = tokio::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn /bin/cat");
        let stdin = child.stdin.take().expect("worker stdin");
        {
            let mut workers = state.workers.lock().await;
            workers.insert(
                "sess-w".to_string(),
                WorkerHandle {
                    child,
                    stdin: Arc::new(Mutex::new(stdin)),
                },
            );
        }

        let result = call(state.clone(), "sess-w", "ou_user", "hello world  ").await;
        let body = &result.expect("handler should succeed").0;
        assert_eq!(body.session_id, "sess-w");
        assert!(!body.event_id.is_empty());
        assert!(!body.message_id.is_empty());

        assert_eq!(body.handler_result.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(
            body.handler_result.get("reused"),
            Some(&Value::Bool(true)),
            "expected reused: true, got {:?}",
            body.handler_result
        );

        // verify state: send_input updated last_cli_input (wrapped in XML markup)
        // and current_turn_id
        let stored = {
            let sessions = state.sessions.lock().await;
            sessions.get("sess-w").cloned().expect("session exists")
        };
        assert!(
            stored
                .last_cli_input
                .as_deref()
                .map_or(false, |s| s.contains("hello world  ")),
            "last_cli_input must contain original text: {:?}",
            stored.last_cli_input
        );
        assert!(
            stored.current_turn_id.is_some(),
            "current_turn_id must be set after send_input"
        );
        assert_eq!(
            stored.current_turn_id.as_deref().map(|s| s.len()),
            Some(32),
            "current_turn_id should be 32-char uuid hex"
        );

        // kill worker, clean up
        {
            let mut workers = state.workers.lock().await;
            if let Some(mut wh) = workers.remove("sess-w") {
                let _ = wh.child.kill().await;
                let _ = wh.child.wait().await;
            }
        }
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    // positive: group Thread session with mock server — dispatch still hits ReuseSession

    #[tokio::test]
    async fn simulate_lark_message_group_thread_reuses_session() {
        let paths = temp_paths("sim-grp-thread");
        maybe_remove_dir(&paths.root().to_path_buf());

        let _env_lock = lark_base_url_env_lock().lock().unwrap();
        let mock_url = start_mock_lark_server().await;
        let _guard = LarkBaseUrlEnvGuard::set(&mock_url);

        let sess = make_group_thread_session("sess-grp-1");
        let state = new_state(paths.clone(), make_bot("app-1"));
        insert(&state, sess).await;

        let result = call(state.clone(), "sess-grp-1", "ou_user", "group thread msg").await;
        let body = &result.expect("handler should succeed").0;
        assert_eq!(body.session_id, "sess-grp-1");
        assert_eq!(
            body.handler_result.get("reused"),
            Some(&Value::Bool(true)),
            "expected reused: true, got {:?}",
            body.handler_result
        );

        let stored = {
            let sessions = state.sessions.lock().await;
            sessions.get("sess-grp-1").cloned().expect("session exists")
        };
        assert_eq!(
            stored.quote_target_id.as_deref(),
            Some(body.message_id.as_str()),
            "quote_target_id should equal simulated message_id"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    // negative tests -------------------------------------------------------

    #[tokio::test]
    async fn simulate_lark_message_session_not_found_404() {
        let paths = temp_paths("sim-404");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = new_state(paths.clone(), make_bot("app-1"));
        insert(&state, make_p2p_session("sess-other")).await;

        let err = call(state, "nonexistent", "ou_user", "hello")
            .await
            .expect_err("missing session → 404");
        assert_eq!(err.0, StatusCode::NOT_FOUND);
        assert!(err.1.contains("not found"), "body: {}", err.1);
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn simulate_lark_message_inactive_session_400() {
        let paths = temp_paths("sim-inactive");
        maybe_remove_dir(&paths.root().to_path_buf());
        let mut sess = make_p2p_session("sess-closed");
        sess.status = SessionStatus::Closed;
        sess.closed_at = Some(Utc::now());
        let state = new_state(paths.clone(), make_bot("app-1"));
        insert(&state, sess).await;

        let err = call(state, "sess-closed", "ou_user", "hello")
            .await
            .expect_err("inactive → 400");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("not active"), "body: {}", err.1);
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn simulate_lark_message_blank_fields_400() {
        let paths = temp_paths("sim-blank");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = new_state(paths.clone(), make_bot("app-1"));
        insert(&state, make_p2p_session("sess-b")).await;

        let err = call(state.clone(), "sess-b", "   ", "hello")
            .await
            .expect_err("blank sender");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("sender_open_id"));

        let err = call(state, "sess-b", "ou_user", "  ")
            .await
            .expect_err("blank text");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("text"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
