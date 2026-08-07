use super::*;

/// Handle a card action that operates on an existing session.
pub(crate) async fn handle_session_card_action(
    state: &AppState,
    bot: &BotConfig,
    app_id: &str,
    action: &ParsedLarkCardAction,
) -> Result<Json<Value>, (StatusCode, String)> {
    let session_id = {
        let sessions = state.sessions.lock().await;
        resolve_lark_card_action_session_id(&sessions, app_id, action)
    };
    let Some(session_id) = session_id else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing session id",
        )));
    };
    info!(
        "[{}] session card action: action={}, operator={}",
        session_id,
        action.action,
        action.operator_open_id.as_deref().unwrap_or("unknown")
    );
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
    if is_stale_stream_card_action(action, &current_session)
        && !stale_stream_card_action_self_heals_live_session(&action.action)
        && !stale_stream_card_action_reads_frozen_snapshot(&action.action)
    {
        return Ok(Json(build_lark_card_action_toast(
            "info",
            "stale card action ignored",
        )));
    }

    match action.action.as_str() {
        "resume" => handle_resume(state, &session_id).await,
        "restart" => handle_restart(state, &session_id).await,
        "close" => handle_close(state, bot, action, &session_id).await,
        "choose_read_only_terminal_link" | "get_read_only_link" => {
            handle_terminal_link(state, bot, action, &session_id, true).await
        }
        "get_write_link" => handle_terminal_link(state, bot, action, &session_id, false).await,
        "export_text" => handle_export_text(state, bot, action, &session_id).await,
        "retry_last_task" => handle_retry_last_task(state, action, &session_id).await,
        "toggle_display" | "toggle_stream" => {
            handle_toggle_display(state, bot, action, &session_id, &current_session).await
        }
        "refresh_screenshot" => handle_refresh_screenshot(state, bot, action, &session_id).await,
        "term_action" => handle_term_action(state, bot, action, &session_id).await,
        "tui_keys" => handle_tui_keys(state, bot, action, &session_id).await,
        "tui_text_input" => handle_tui_text_input_with_action(state, action, &session_id).await,
        "wf_approve" | "wf_reject" | "wf_cancel" => {
            workflow_actions::handle_workflow_card_action(state, bot, action).await
        }
        _ => Ok(Json(build_lark_card_action_toast(
            "info",
            "unsupported card action",
        ))),
    }
}

// ---------------------------------------------------------------------------
// Individual card action handlers
// ---------------------------------------------------------------------------

async fn handle_resume(
    state: &AppState,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    match resume_session(
        State(state.clone()),
        AxumPath(session_id.to_string()),
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
        Err((status, err)) if status == StatusCode::CONFLICT && err == "session is not closed" => {
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
    }
}

async fn handle_restart(
    state: &AppState,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    match restart_session(
        State(state.clone()),
        AxumPath(session_id.to_string()),
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
    }
}

async fn handle_close(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let session_snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = session_snapshot else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "session not found",
        )));
    };
    match close_session(State(state.clone()), AxumPath(session_id.to_string())).await {
        Ok(_status) => {
            let closed_card = build_closed_session_card(&session);
            if action.visibility.as_deref() == Some("private") || bot.private_card {
                for open_id in resolve_private_card_audience(&session, bot) {
                    let delivered = match private_card_delivery(session.chat_type.as_deref()) {
                        PrivateCardDelivery::Ephemeral => {
                            lark_send_ephemeral_card(
                                state,
                                bot,
                                &session.chat_id,
                                &open_id,
                                &closed_card,
                            )
                            .await
                        }
                        PrivateCardDelivery::DirectMessage => {
                            lark_send_open_id_card(state, bot, &open_id, &closed_card).await
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

async fn handle_terminal_link(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
    read_only: bool,
) -> Result<Json<Value>, (StatusCode, String)> {
    let session_snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = session_snapshot else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "session not found",
        )));
    };

    if read_only {
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
    } else {
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
    }

    let toast_msg = if read_only {
        "read-only link ready"
    } else {
        "write link ready"
    };

    let card_json = build_terminal_link_choice_card_json(
        state,
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
                        state,
                        bot,
                        &session.chat_id,
                        operator_open_id,
                        &card_json,
                    )
                    .await
                }
                PrivateCardDelivery::DirectMessage => {
                    lark_send_open_id_card(state, bot, operator_open_id, &card_json).await
                }
            };
            return match delivered {
                Ok(_) => Ok(Json(build_lark_card_action_toast("success", toast_msg))),
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

    let card = serde_json::from_str::<Value>(&card_json).unwrap_or_else(|_| serde_json::json!({}));
    Ok(Json(serde_json::json!({
        "toast": {
            "type": "success",
            "content": toast_msg,
        },
        "card": {
            "type": "raw",
            "data": card,
        }
    })))
}

async fn handle_export_text(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let session_snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
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
        state,
        bot,
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

async fn handle_retry_last_task(
    state: &AppState,
    _action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let turn_id = next_session_turn_id();
    let session_snapshot = {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            let Some(entry) = sessions.get_mut(session_id) else {
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
            entry.current_turn_id = Some(turn_id.clone());
            let snapshot = sessions.clone();
            (updated, cli_input, snapshot)
        };
        persist_sessions(&state.paths, &snapshot.2)
            .await
            .map_err(internal_error)?;
        (snapshot.0, snapshot.1)
    };
    let _ = ensure_worker_for_session(state, session_id).await;
    let _ = send_worker_message(
        &state.workers,
        session_id,
        &DaemonToWorker::Message {
            content: session_snapshot.1.clone(),
            turn_id,
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

async fn handle_toggle_display(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
    current_session: &Session,
) -> Result<Json<Value>, (StatusCode, String)> {
    let stale_frozen_nonce = if is_stale_stream_card_action(action, current_session) {
        action.card_nonce.clone()
    } else {
        None
    };
    let session_snapshot = {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            let Some(entry) = sessions.get_mut(session_id) else {
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
    if let Err(err) = ensure_worker_for_session(state, session_id).await {
        warn!(
            "[{}] toggle_display ensure_worker failed: {:#}",
            session_snapshot.session_id, err
        );
    }
    if let Err(err) = send_worker_message(
        &state.workers,
        session_id,
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
    match resolve_card_render_target(action, &session_snapshot) {
        CardRenderTarget::PatchMessage(target_message_id) => {
            let card_json = serde_json::to_string(&card).unwrap_or_else(|_| "{}".to_string());
            info!(
                "[{}] toggle_display patch target={}, clicked={:?}, mode={:?}",
                session_snapshot.session_id,
                target_message_id,
                action.clicked_message_id,
                session_snapshot.display_mode,
            );
            match lark_update_card(state, bot, &target_message_id, &card_json).await {
                Ok(()) => {
                    info!(
                        "[{}] toggle_display patch succeeded: target={}, mode={:?}",
                        session_snapshot.session_id,
                        target_message_id,
                        session_snapshot.display_mode,
                    );
                    if let Some(nonce) = stale_frozen_nonce.as_deref() {
                        if let Err(err) =
                            remove_frozen_card(&state.paths, &session_snapshot.session_id, nonce)
                                .await
                        {
                            warn!("failed to remove migrated frozen card {}: {}", nonce, err);
                        }
                    }
                    Ok(Json(build_lark_card_action_toast(
                        "success",
                        "display updated",
                    )))
                }
                Err(err) => {
                    warn!(
                        "[{}] toggle_display patch failed: target={}, error={:#}",
                        session_snapshot.session_id, target_message_id, err
                    );
                    Ok(Json(build_lark_card_action_toast(
                        "error",
                        &format!("display update failed: {}", err),
                    )))
                }
            }
        }
        CardRenderTarget::CallbackRaw => {
            info!(
                "[{}] toggle_display callback-raw: clicked={:?}, mode={:?}",
                session_snapshot.session_id, action.clicked_message_id, session_snapshot.display_mode,
            );
            if let Some(nonce) = stale_frozen_nonce.as_deref() {
                if let Err(err) =
                    remove_frozen_card(&state.paths, &session_snapshot.session_id, nonce).await
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

async fn handle_refresh_screenshot(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let session_snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
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
    let _ = refresh_session(State(state.clone()), AxumPath(session_id.to_string())).await;
    let card = serde_json::from_str::<Value>(&build_streaming_card(
        &session,
        session_stream_status(&session),
    ))
    .unwrap_or_else(|_| serde_json::json!({}));
    match resolve_card_render_target(action, &session) {
        CardRenderTarget::PatchMessage(message_id) => {
            let card_json = serde_json::to_string(&card).unwrap_or_else(|_| "{}".to_string());
            match lark_update_card(state, bot, &message_id, &card_json).await {
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

async fn handle_term_action(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let Some(key) = action.term_key else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing terminal key",
        )));
    };
    let session_snapshot = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
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
    let _ = ensure_worker_for_session(state, session_id).await;
    let _ = send_worker_message(
        &state.workers,
        session_id,
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
    match resolve_card_render_target(action, &session) {
        CardRenderTarget::PatchMessage(message_id) => {
            let card_json = serde_json::to_string(&card).unwrap_or_else(|_| "{}".to_string());
            match lark_update_card(state, bot, &message_id, &card_json).await {
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

#[allow(unused_variables)]
async fn handle_tui_keys(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
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
                let Some(entry) = sessions.get_mut(session_id) else {
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
        let Some(session) = sessions.get(session_id) else {
            return Ok(Json(build_lark_card_action_toast(
                "error",
                "session not found",
            )));
        };
        let mut all_keys = Vec::new();
        if !session.tui_toggled_indices.is_empty() && !session.tui_prompt_options.is_empty() {
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
            if let Some(entry) = sessions.get_mut(session_id) {
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
    let _ = ensure_worker_for_session(state, session_id).await;
    let _ = send_worker_message(
        &state.workers,
        session_id,
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
            let session_id = session_id.to_string();
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

async fn handle_tui_text_input_with_action(
    state: &AppState,
    action: &ParsedLarkCardAction,
    session_id: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    let input_text = action.input_text.clone().unwrap_or_default();
    let input_keys = action.input_keys.clone().unwrap_or_default();
    if input_text.trim().is_empty() || input_keys.is_empty() {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing tui text input",
        )));
    }
    let _ = ensure_worker_for_session(state, session_id).await;
    let _ = send_worker_message(
        &state.workers,
        session_id,
        &DaemonToWorker::TuiTextInput {
            keys: input_keys,
            text: input_text.clone(),
        },
    )
    .await;
    let (snapshot, locale) = {
        let mut sessions = state.sessions.lock().await;
        let locale = sessions
            .get(session_id)
            .and_then(|entry| entry.locale.clone());
        if let Some(entry) = sessions.get_mut(session_id) {
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
