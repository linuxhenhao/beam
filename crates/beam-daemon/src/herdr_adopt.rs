//! Herdr adopt discovery and session creation (`/adopt herdr:<pane_id>`).

use super::*;

/// A Herdr pane that can be adopted. Discovery is CLI-driven
/// (`herdr agent list` / `pane list` + `pane process-info`); a pane whose
/// argv matches `CLI_SPECS` is a candidate even without agent detection
/// (Q5 = include).
#[derive(Debug, Clone)]
pub(crate) struct HerdrAdoptCandidate {
    pub(crate) workspace_id: String,
    pub(crate) pane_id: String,
    pub(crate) title: String,
    pub(crate) cwd: String,
    pub(crate) cli_pid: Option<i32>,
}

/// `herdr agent list` — recognized agents plus state and pane id.
fn discover_herdr_agents() -> Vec<HerdrAdoptCandidate> {
    let output = std::process::Command::new("herdr")
        .args(["agent", "list"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
        return Vec::new();
    };
    let items = value
        .pointer("/result/agents")
        .or_else(|| value.pointer("/agents"))
        .or_else(|| value.as_array().map(|_| &value))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    items
        .into_iter()
        .filter_map(|item| {
            let pane_id = item
                .get("pane_id")
                .or_else(|| item.pointer("/pane/pane_id"))
                .and_then(serde_json::Value::as_str)?;
            let workspace_id = item
                .get("workspace_id")
                .or_else(|| item.pointer("/workspace/workspace_id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let cli_id = item
                .get("agent")
                .or_else(|| item.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("cli");
            let cli_pid = item
                .get("pid")
                .or_else(|| item.get("process_id"))
                .and_then(serde_json::Value::as_i64)
                .and_then(|v| i32::try_from(v).ok())
                // `agent list` on herdr 0.8.x does not carry the pid; recover
                // it via `pane process-info --pane` so adopt can pin the CLI.
                .or_else(|| herdr_foreground_pid(pane_id));
            Some(HerdrAdoptCandidate {
                workspace_id: workspace_id.to_string(),
                pane_id: pane_id.to_string(),
                title: item
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(cli_id)
                    .to_string(),
                cwd: item
                    .get("cwd")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                cli_pid,
            })
        })
        .collect()
}

/// Foreground pid for a pane via `herdr pane process-info --pane <id>`.
/// Tolerant of the nested real payload; returns `None` on any failure.
fn herdr_foreground_pid(pane_id: &str) -> Option<i32> {
    let output = std::process::Command::new("herdr")
        .args(["pane", "process-info", "--pane", pane_id])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let foreground = value
        .pointer("/result/process_info/foreground_processes")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first());
    foreground
        .and_then(|f| f.get("pid").and_then(serde_json::Value::as_i64))
        .or_else(|| {
            value
                .pointer("/result/process_info/shell_pid")
                .and_then(serde_json::Value::as_i64)
        })
        .and_then(|v| i32::try_from(v).ok())
}

/// List all adoptable Herdr panes. Skips workspaces whose label matches
/// `beam-*` so Beam never adopts its own managed panes.
pub(crate) fn discover_herdr_adopt_candidates() -> Vec<HerdrAdoptCandidate> {
    let agents = discover_herdr_agents();
    let beam_owned = {
        let output = std::process::Command::new("herdr")
            .args(["workspace", "list"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output();
        match output {
            Ok(output) if output.status.success() => {
                serde_json::from_slice::<serde_json::Value>(&output.stdout)
                    .ok()
                    .map(|value| {
                        let items = value
                            .pointer("/result/workspaces")
                            .or_else(|| value.pointer("/workspaces"))
                            .or_else(|| value.as_array().map(|_| &value))
                            .and_then(serde_json::Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        items
                            .into_iter()
                            .filter_map(|ws| {
                                let label = ws.get("label").and_then(serde_json::Value::as_str)?;
                                let workspace_id =
                                    ws.get("workspace_id").and_then(serde_json::Value::as_str)?;
                                label.starts_with("beam-").then(|| workspace_id.to_string())
                            })
                            .collect::<Vec<String>>()
                    })
                    .unwrap_or_default()
            }
            _ => Vec::new(),
        }
    };
    agents
        .into_iter()
        .filter(|candidate| !beam_owned.contains(&candidate.workspace_id))
        .collect()
}

/// Adopt a Herdr pane into a new Beam session. The worker attaches with
/// `HerdrObserveBackend` (observe + drive, never `pane run`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn adopt_herdr_pane(
    state: &AppState,
    pane_id: &str,
    cli_id: &str,
    cli_bin: &str,
    title: Option<String>,
    lark_app_id: Option<String>,
    chat_id: Option<String>,
    chat_type: Option<String>,
    root_message_id: Option<String>,
    scope: SessionScope,
    thread_id: Option<String>,
    owner_open_id: Option<String>,
) -> Result<(StatusCode, Json<SessionSummary>), (StatusCode, String)> {
    let candidate = discover_herdr_adopt_candidates()
        .into_iter()
        .find(|item| item.pane_id == pane_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "herdr pane not found".to_string()))?;
    if candidate.cli_pid.is_none() {
        return Err((
            StatusCode::CONFLICT,
            "herdr pane has no process pid; cannot adopt".to_string(),
        ));
    }

    let session_id = Uuid::new_v4().to_string();
    let adopted_from = AdoptedFrom {
        backend_kind: BackendKind::Herdr,
        tmux_target: None,
        zellij_session: None,
        zellij_pane_id: None,
        herdr_workspace_id: Some(candidate.workspace_id.clone()),
        herdr_pane_id: Some(candidate.pane_id.clone()),
        original_cli_pid: candidate.cli_pid.unwrap_or(0),
        session_id: None,
        cli_id: Some(cli_id.to_string()),
        cwd: candidate.cwd.clone(),
        pane_cols: None,
        pane_rows: None,
    };
    let lark_app_id = lark_app_id.unwrap_or_else(|| "local".to_string());
    let chat_id = chat_id.unwrap_or_else(|| "local".to_string());
    let root_message_id = root_message_id.unwrap_or_else(|| session_id.clone());
    let lark_app_secret = state
        .bots
        .get(&lark_app_id)
        .map(|bot| bot.lark_app_secret.clone())
        .unwrap_or_default();

    let session = Session {
        session_id: session_id.clone(),
        backend_kind: BackendKind::Herdr,
        herdr_session: Some(state.config.herdr.session.clone()),
        herdr_workspace_id: adopted_from.herdr_workspace_id.clone(),
        herdr_pane_id: adopted_from.herdr_pane_id.clone(),
        title: title.unwrap_or_else(|| format!("adopt {}", candidate.pane_id)),
        chat_id: chat_id.clone(),
        chat_type: chat_type.clone(),
        root_message_id: root_message_id.clone(),
        quote_target_id: None,
        scope,
        status: SessionStatus::Active,
        created_at: Utc::now(),
        closed_at: None,
        working_dir: Some(candidate.cwd.clone()),
        lark_app_id: lark_app_id.clone(),
        owner_open_id: owner_open_id.clone(),
        quote_target_sender_open_id: owner_open_id.clone(),
        worker_pid: None,
        cli_id: Some(cli_id.to_string()),
        cli_bin: Some(cli_bin.to_string()),
        cgroup_slice: None,
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
        adopted_from: Some(adopted_from.clone()),
        bot_name: None,
        bot_open_id: None,
        disable_cli_bypass: false,
        initial_prompt: None,
        model: None,
        locale: None,
        resume_session_id: None,
        thread_id: thread_id.clone(),
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
        backend_kind: BackendKind::Herdr,
        herdr_session: Some(state.config.herdr.session.clone()),
        herdr_workspace_id: adopted_from.herdr_workspace_id.clone(),
        herdr_pane_id: adopted_from.herdr_pane_id.clone(),
        title: session.title.clone(),
        chat_id: session.chat_id.clone(),
        root_message_id: session.root_message_id.clone(),
        working_dir: adopted_from.cwd.clone(),
        cli_id: cli_id.to_string(),
        cli_bin: cli_bin.to_string(),
        cgroup_slice: None,
        cli_args: Vec::new(),
        prompt: String::new(),
        resume: false,
        cli_session_id: None,
        lark_app_id: lark_app_id.clone(),
        lark_app_secret,
        prompt_turn_id: None,
        owner_open_id,
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

/// Dispatch handler for `LarkEventOutcome::AdoptHerdr`: create the session
/// and reply with the result, mirroring the zellij adopt reply shape.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_herdr_adopt_reply(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    app_id: &str,
    chat_id: &str,
    chat_type: Option<String>,
    scope: SessionScope,
    thread_id: Option<String>,
    sender_open_id: Option<String>,
    target: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let pane_id = target.trim();
    let result = adopt_herdr_pane(
        state,
        pane_id,
        &bot.cli_id,
        &bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
        Some(format!("adopt herdr:{pane_id}")),
        Some(app_id.to_string()),
        Some(chat_id.to_string()),
        chat_type,
        Some(message_id.to_string()),
        scope,
        thread_id,
        sender_open_id,
    )
    .await;
    let reply_in_thread = scope == SessionScope::Thread;
    match result {
        Ok((_, Json(session))) => {
            let reply = build_adopt_zellij_result_reply(Ok(&session));
            let _ =
                lark_reply_message_with_opts(state, bot, message_id, &reply, reply_in_thread).await;
        }
        Err((_, err)) => {
            let reply = build_adopt_zellij_result_reply(Err(err.as_str()));
            let _ =
                lark_reply_message_with_opts(state, bot, message_id, &reply, reply_in_thread).await;
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Dispatch handler for `LarkEventOutcome::AdoptList`: list zellij sessions
/// plus (when herdr is available) herdr panes in one post.
pub(crate) async fn dispatch_adopt_list_reply(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let items = discover_zellij_adopt_candidates();
    let herdr_items = discover_herdr_adopt_candidates();
    if items.is_empty() && herdr_items.is_empty() {
        let _ = lark_reply_message(
            state,
            bot,
            message_id,
            "no zellij sessions or herdr panes available for adoption",
        )
        .await;
    } else {
        let mut post = build_zellij_adopt_post_content(&items);
        if !herdr_items.is_empty() {
            post = append_herdr_adopt_post_content(post, &herdr_items);
        }
        let _ = lark_reply_post_message(state, bot, message_id, &post).await;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Dispatch handler for `LarkEventOutcome::AdoptZellij` (kept here beside the
/// herdr counterpart so both adopt paths share the reply shape).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_zellij_adopt_reply(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    app_id: &str,
    chat_id: &str,
    chat_type: Option<String>,
    scope: SessionScope,
    thread_id: Option<String>,
    owner_open_id: Option<String>,
    target: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let (zellij_session, zellij_pane_id) = match target.split_once(':') {
        Some((s, p)) => (s.to_string(), p.to_string()),
        None => (target.to_string(), "terminal_0".to_string()),
    };
    let result = crate::lark_ingress::session_actions::adopt_zellij_session(
        State(state.clone()),
        Json(crate::daemon_types::AdoptZellijSessionRequest {
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
            chat_type,
            root_message_id: Some(message_id.to_string()),
            scope: Some(scope),
            thread_id,
            owner_open_id,
        }),
    )
    .await;
    let reply_in_thread = scope == SessionScope::Thread;
    match result {
        Ok((_, Json(session))) => {
            let reply = build_adopt_zellij_result_reply(Ok(&session));
            let _ =
                lark_reply_message_with_opts(state, bot, message_id, &reply, reply_in_thread).await;
        }
        Err((_, err)) => {
            let reply = build_adopt_zellij_result_reply(Err(err.as_str()));
            let _ =
                lark_reply_message_with_opts(state, bot, message_id, &reply, reply_in_thread).await;
        }
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}
