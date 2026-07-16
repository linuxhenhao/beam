use super::*;

pub(crate) async fn create_session(
    State(state): State<AppState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<SessionSummary>), (StatusCode, String)> {
    if req.title.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "title must not be empty".to_string(),
        ));
    }
    if req.cli_bin.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "cli_bin must not be empty".to_string(),
        ));
    }

    let session_id = Uuid::new_v4().to_string();
    let session = Session {
        session_id: session_id.clone(),
        title: req.title.clone(),
        chat_id: "local".to_string(),
        chat_type: Some("local".to_string()),
        root_message_id: session_id.clone(),
        quote_target_id: None,
        scope: SessionScope::Thread,
        status: SessionStatus::Active,
        created_at: Utc::now(),
        closed_at: None,
        working_dir: Some(expand_tilde(&req.working_dir)),
        lark_app_id: "local".to_string(),
        owner_open_id: None,
        quote_target_sender_open_id: None,
        worker_pid: None,
        cli_id: Some(req.cli_id.clone()),
        cli_bin: Some(req.cli_bin.clone()),
        cli_args: req.cli_args.clone(),
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
    ensure_lark_pending_card(&state, &session_id)
        .await
        .map_err(internal_error)?;

    let prompt_turn_id = (!req.prompt.is_empty()).then(next_session_turn_id);
    let init = InitConfig {
        session_id: session_id.clone(),
        title: req.title,
        chat_id: "local".to_string(),
        root_message_id: session_id.clone(),
        working_dir: req.working_dir,
        cli_id: req.cli_id,
        cli_bin: req.cli_bin,
        cli_args: req.cli_args,
        prompt: req.prompt,
        resume: false,
        cli_session_id: None,
        lark_app_id: "local".to_string(),
        lark_app_secret: String::new(),
        prompt_turn_id,
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

    spawn_worker(state.clone(), session.clone(), init)
        .await
        .map_err(internal_error)?;

    Ok((StatusCode::CREATED, Json(SessionSummary::from(&session))))
}

pub(crate) async fn list_sessions(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Vec<SessionSummary>> {
    let include_closed = query
        .get("all")
        .or_else(|| query.get("includeClosed"))
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let mut items = {
        let sessions = state.sessions.lock().await;
        sessions
            .values()
            .filter(|session| include_closed || session.status == SessionStatus::Active)
            .map(SessionSummary::from)
            .collect::<Vec<_>>()
    };
    items.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Json(items)
}

pub(crate) async fn get_session(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<SessionSummary>, (StatusCode, String)> {
    let sessions = state.sessions.lock().await;
    let session = sessions
        .get(&session_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
    Ok(Json(SessionSummary::from(session)))
}
