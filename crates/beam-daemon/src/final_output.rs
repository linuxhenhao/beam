use super::*;

pub(crate) async fn read_pending_response_patch_marker(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<Option<PendingResponsePatchMarker>> {
    match tokio::fs::read(paths.pending_response_patch_json(session_id)).await {
        Ok(bytes) => {
            let marker = serde_json::from_slice::<PendingResponsePatchMarker>(&bytes)?;
            if marker.session_id != session_id || marker.card_id.trim().is_empty() {
                return Ok(None);
            }
            if marker.state != "patching" && marker.state != "patched" {
                return Ok(None);
            }
            Ok(Some(marker))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn write_pending_response_patch_marker(
    paths: &BeamPaths,
    session_id: &str,
    card_id: &str,
) -> Result<()> {
    tokio::fs::create_dir_all(paths.pending_response_patches_dir()).await?;
    let path = paths.pending_response_patch_json(session_id);
    let tmp = path.with_extension("json.tmp");
    let marker = PendingResponsePatchMarker {
        session_id: session_id.to_string(),
        card_id: card_id.to_string(),
        state: "patching".to_string(),
        created_at: Utc::now().to_rfc3339(),
        patched_at: None,
    };
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&marker)?).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

pub(crate) async fn mark_pending_response_patch_marker_patched(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<()> {
    let Some(mut marker) = read_pending_response_patch_marker(paths, session_id).await? else {
        return Ok(());
    };
    marker.state = "patched".to_string();
    marker.patched_at = Some(Utc::now().to_rfc3339());
    let path = paths.pending_response_patch_json(session_id);
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&marker)?).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

pub(crate) async fn clear_pending_response_patch_marker(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<()> {
    let path = paths.pending_response_patch_json(session_id);
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn final_output_footer_recipient_open_id(
    paths: &BeamPaths,
    session: &Session,
) -> Option<String> {
    let owner = session.owner_open_id.as_deref()?.trim();
    if owner.is_empty() {
        return None;
    }
    let known_bot_ids = load_known_bot_open_ids_for_app(paths, &session.lark_app_id);
    if known_bot_ids.contains(owner) {
        None
    } else {
        Some(owner.to_string())
    }
}

pub(crate) fn build_final_output_footer(recipient_open_id: Option<&str>) -> Option<String> {
    let mut parts = vec![DEFAULT_BRAND_LABEL.to_string()];
    if let Some(open_id) = recipient_open_id.filter(|open_id| !open_id.trim().is_empty()) {
        parts.push(format!("发送给：<at id={}></at>", open_id));
    }
    if parts.is_empty() {
        None
    } else {
        Some(format!("<font color='grey'>{}</font>", parts.join(" · ")))
    }
}

pub(crate) fn build_contextual_reply_card(
    title_zh: &str,
    title_en: &str,
    user_text: Option<&str>,
    assistant_text: &str,
    assistant_label_zh: &str,
    assistant_label_en: &str,
    recipient_open_id: Option<&str>,
) -> String {
    let mut elements = vec![serde_json::json!({
        "tag": "markdown",
        "text_size": "heading_2_v2",
        "content": title_en,
        "i18n_content": {
            "zh_cn": title_zh,
            "en_us": title_en,
        },
    })];
    if let Some(user_text) = user_text {
        elements.push(serde_json::json!({
            "tag": "markdown",
            "content": format!(
                "**👤 You**\n\n> {}",
                if user_text.trim().is_empty() { "(empty)" } else { user_text.trim() }
            ),
            "i18n_content": {
                "zh_cn": format!(
                    "**👤 你**\n\n> {}",
                    if user_text.trim().is_empty() { "(空)" } else { user_text.trim() }
                ),
                "en_us": format!(
                    "**👤 You**\n\n> {}",
                    if user_text.trim().is_empty() { "(empty)" } else { user_text.trim() }
                ),
            },
        }));
    }
    elements.push(serde_json::json!({ "tag": "hr" }));
    elements.push(serde_json::json!({
        "tag": "markdown",
        "content": format!("**🤖 {}**", assistant_label_en),
        "i18n_content": {
            "zh_cn": format!("**🤖 {}**", assistant_label_zh),
            "en_us": format!("**🤖 {}**", assistant_label_en),
        },
    }));
    elements.push(serde_json::json!({
        "tag": "markdown",
        "content": if assistant_text.trim().is_empty() { "*(empty)*" } else { assistant_text },
        "i18n_content": {
            "zh_cn": if assistant_text.trim().is_empty() { "*(空)*" } else { assistant_text },
            "en_us": if assistant_text.trim().is_empty() { "*(empty)*" } else { assistant_text },
        },
    }));
    if let Some(footer) = build_final_output_footer(recipient_open_id) {
        let footer_text = footer.clone();
        elements.push(serde_json::json!({ "tag": "hr" }));
        elements.push(serde_json::json!({
            "tag": "markdown",
            "text_size": "notation_small_v2",
            "content": footer_text,
            "i18n_content": {
                "zh_cn": footer.clone(),
                "en_us": footer,
            },
        }));
    }
    serde_json::json!({
        "schema": "2.0",
        "config": { "update_multi": true },
        "locales": card_i18n::card_locales(),
        "body": {
            "direction": "vertical",
            "elements": elements,
        }
    })
    .to_string()
}

pub(crate) fn worker_ready_display_mode_command(session: &Session) -> Option<DaemonToWorker> {
    match session.display_mode {
        Some(DisplayMode::Screenshot) => Some(DaemonToWorker::SetDisplayMode {
            mode: DisplayMode::Screenshot,
        }),
        _ => None,
    }
}

pub(crate) async fn resend_display_mode_after_worker_ready(
    state: &AppState,
    session_id: &str,
) -> Result<()> {
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = session else {
        return Ok(());
    };
    let Some(msg) = worker_ready_display_mode_command(&session) else {
        return Ok(());
    };
    send_worker_message(&state.workers, session_id, &msg).await
}

#[allow(dead_code)]
pub(crate) fn is_pending_response_card_open(session: &Session) -> bool {
    session.pending_response_card_id.is_some()
        && session.pending_response_card_state == Some(PendingResponseCardState::Open)
}

pub(crate) fn start_pending_response_turn(session: &mut Session, message_id: String) {
    session.pending_response_card_id = Some(message_id);
    session.pending_response_card_state = Some(PendingResponseCardState::Open);
}

pub(crate) fn mark_pending_response_card_patched(session: &mut Session) {
    session.last_patched_response_card_id = session.pending_response_card_id.clone();
    session.pending_response_card_id = None;
    session.pending_response_card_state = Some(PendingResponseCardState::Patched);
}

pub(crate) fn mark_pending_response_card_patched_if_current(
    session: &mut Session,
    card_id: &str,
) -> bool {
    if session.pending_response_card_id.as_deref() != Some(card_id)
        || session.pending_response_card_state != Some(PendingResponseCardState::Open)
    {
        return false;
    }
    mark_pending_response_card_patched(session);
    true
}

#[allow(dead_code)]
pub(crate) fn claim_pending_response_card(session: &Session) -> Option<String> {
    if is_pending_response_card_open(session) {
        session.pending_response_card_id.clone()
    } else {
        None
    }
}

pub(crate) fn clear_pending_response_tracking(session: &mut Session) {
    session.pending_response_card_id = None;
    session.pending_response_card_state = None;
    session.last_patched_response_card_id = None;
}

pub(crate) fn build_final_output_card(
    content: &str,
    recipient_open_id: Option<&str>,
    kind: Option<FinalOutputKind>,
    user_text: Option<&str>,
    cli_label: Option<&str>,
) -> String {
    let mut elements = Vec::new();
    match kind.unwrap_or(FinalOutputKind::Bridge) {
        FinalOutputKind::Bridge => {
            elements.push(serde_json::json!({
                "tag": "markdown",
                "content": content,
                "i18n_content": {
                    "zh_cn": content,
                    "en_us": content,
                },
            }));
        }
        FinalOutputKind::LocalTurn => {
            return build_contextual_reply_card(
                "🖥️ 终端本地对话（在 adopted pane 中直接输入，已同步至飞书）",
                "🖥️ Local terminal conversation (type directly in the adopted pane; synced to Feishu)",
                user_text,
                content,
                cli_label.unwrap_or("助手"),
                cli_label.unwrap_or("Assistant"),
                recipient_open_id,
            );
        }
        FinalOutputKind::LocalTurnHeadless => {
            return build_contextual_reply_card(
                "🖥️ 终端本地对话续传（daemon 重启时模型正在输出）",
                "🖥️ Local terminal conversation resumed (model was still streaming when daemon restarted)",
                None,
                content,
                cli_label.unwrap_or("助手"),
                cli_label.unwrap_or("Assistant"),
                recipient_open_id,
            );
        }
    }
    if let Some(footer) = build_final_output_footer(recipient_open_id) {
        let footer_text = footer.clone();
        elements.push(serde_json::json!({ "tag": "hr" }));
        elements.push(serde_json::json!({
            "tag": "markdown",
            "text_size": "notation_small_v2",
            "content": footer_text,
            "i18n_content": {
                "zh_cn": footer.clone(),
                "en_us": footer,
            },
        }));
    }
    serde_json::json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
        },
        "locales": card_i18n::card_locales(),
        "body": {
            "direction": "vertical",
            "elements": elements,
        }
    })
    .to_string()
}

pub(crate) fn should_treat_pending_card_as_patched_by_marker(
    pending_card_id: Option<&str>,
    marker: Option<&PendingResponsePatchMarker>,
) -> bool {
    matches!(
        (pending_card_id, marker),
        (Some(card_id), Some(marker))
            if marker.state == "patched" && marker.card_id == card_id
    )
}

pub(crate) fn next_final_output_retry_delay_ms(attempt: usize) -> Option<u64> {
    FINAL_OUTPUT_RETRY_BACKOFF_MS.get(attempt).copied()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct FinalOutputRetryMarker {
    pub(crate) session_id: String,
    pub(crate) content: String,
    pub(crate) turn_id: Option<String>,
    pub(crate) kind: Option<FinalOutputKind>,
    pub(crate) user_text: Option<String>,
    pub(crate) attempt: usize,
    pub(crate) created_at: String,
}

pub(crate) fn load_final_output_retry_markers(paths: &BeamPaths) -> Vec<FinalOutputRetryMarker> {
    match beam_core::persist::read_json::<Vec<FinalOutputRetryMarker>>(
        &paths.final_output_retries_json(),
    ) {
        Ok(Some(markers)) => markers,
        Ok(None) | Err(_) => Vec::new(),
    }
}

pub(crate) fn save_final_output_retry_markers(
    paths: &BeamPaths,
    markers: &[FinalOutputRetryMarker],
) {
    if markers.is_empty() {
        let _ = std::fs::remove_file(paths.final_output_retries_json());
        return;
    }
    let _ = beam_core::persist::atomic_write_json(
        &paths.final_output_retries_json(),
        &markers.to_vec(),
    );
}

pub(crate) fn persist_final_output_retry_marker(
    state: &AppState,
    session_id: &str,
    content: String,
    turn_id: Option<String>,
    kind: Option<FinalOutputKind>,
    user_text: Option<String>,
    attempt: usize,
) {
    let mut markers = load_final_output_retry_markers(&state.paths);
    // Replace existing marker for this (session_id, turn_id) pair
    let turn_id_str = turn_id.as_deref().unwrap_or("");
    markers.retain(|m| {
        !(m.session_id == session_id && m.turn_id.as_deref().unwrap_or("") == turn_id_str)
    });
    markers.push(FinalOutputRetryMarker {
        session_id: session_id.to_string(),
        content,
        turn_id,
        kind,
        user_text,
        attempt,
        created_at: chrono::Utc::now().to_rfc3339(),
    });
    save_final_output_retry_markers(&state.paths, &markers);
}

pub(crate) fn clear_final_output_retry(state: &AppState, session_id: &str, turn_id: Option<&str>) {
    let mut markers = load_final_output_retry_markers(&state.paths);
    let before = markers.len();
    let turn_id_str = turn_id.unwrap_or("");
    markers.retain(|m| {
        !(m.session_id == session_id && m.turn_id.as_deref().unwrap_or("") == turn_id_str)
    });
    if markers.len() != before {
        save_final_output_retry_markers(&state.paths, &markers);
    }
}

pub(crate) fn final_output_turn_key(session_id: &str, turn_id: &str) -> Option<String> {
    if turn_id.is_empty() {
        None
    } else {
        Some(format!("{}:{}", session_id, turn_id))
    }
}

pub(crate) fn should_skip_worker_final_output(session: &Session, turn_id: &str) -> bool {
    !turn_id.is_empty() && session.last_final_output_turn_id.as_deref() == Some(turn_id)
}

pub(crate) fn should_abort_final_output_delivery(session: Option<&Session>) -> bool {
    session
        .map(|session| session.status == SessionStatus::Closed)
        .unwrap_or(true)
}

async fn commit_delivered_final_output(
    state: &AppState,
    session_id: &str,
    content: &str,
    turn_id: Option<&str>,
) -> Result<()> {
    let snapshot = {
        let mut sessions = state.sessions.lock().await;
        let session = sessions
            .get_mut(session_id)
            .with_context(|| format!("session not found: {}", session_id))?;
        session.last_final_output = Some(content.to_string());
        if let Some(turn_id) = turn_id.filter(|turn_id| !turn_id.is_empty()) {
            session.last_final_output_turn_id = Some(turn_id.to_string());
        }
        sessions.clone()
    };
    persist_sessions(&state.paths, &snapshot).await
}

pub(crate) async fn deliver_final_output_once(
    state: &AppState,
    session_id: &str,
    content: &str,
    turn_id: Option<&str>,
    kind: Option<FinalOutputKind>,
    user_text: Option<&str>,
) -> Result<()> {
    let (session, pending_card_id) = {
        let (session_snapshot, pending_card_id, sessions_snapshot) = {
            let mut sessions = state.sessions.lock().await;
            let session = sessions
                .get_mut(session_id)
                .with_context(|| format!("session not found: {}", session_id))?;
            let pending_card_id = claim_pending_response_card(session);
            (session.clone(), pending_card_id, sessions.clone())
        };
        persist_sessions(&state.paths, &sessions_snapshot).await?;
        (session_snapshot, pending_card_id)
    };

    if session.lark_app_id == "local" {
        commit_delivered_final_output(state, session_id, content, turn_id).await?;
        return Ok(());
    }
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };

    let footer_recipient_open_id = final_output_footer_recipient_open_id(&state.paths, &session);
    let card_json = build_final_output_card(
        content,
        footer_recipient_open_id.as_deref(),
        kind,
        user_text,
        session.cli_id.as_deref(),
    );
    let fallback_reply = || async {
        match session.scope {
            SessionScope::Thread if !session.root_message_id.is_empty() => {
                lark_reply_card_with_opts(state, bot, &session.root_message_id, &card_json, true)
                    .await
                    .map(|_| ())
            }
            _ => lark_send_chat_message(state, bot, &session.chat_id, content)
                .await
                .map(|_| ()),
        }
    };

    if let Some(pending_card_id) = pending_card_id.as_deref() {
        let still_current = {
            let sessions = state.sessions.lock().await;
            sessions
                .get(session_id)
                .and_then(claim_pending_response_card)
                .as_deref()
                == Some(pending_card_id)
        };
        if still_current {
            write_pending_response_patch_marker(&state.paths, session_id, pending_card_id).await?;
            match lark_update_card(state, bot, pending_card_id, &card_json).await {
                Ok(()) => {
                    mark_pending_response_patch_marker_patched(&state.paths, session_id).await?;
                    let updated_session = {
                        let mut sessions = state.sessions.lock().await;
                        if let Some(entry) = sessions.get_mut(session_id) {
                            mark_pending_response_card_patched_if_current(entry, pending_card_id);
                            Some(entry.clone())
                        } else {
                            None
                        }
                    };
                    let snapshot = {
                        let sessions = state.sessions.lock().await;
                        sessions.clone()
                    };
                    persist_sessions(&state.paths, &snapshot).await?;
                    clear_pending_response_patch_marker(&state.paths, session_id).await?;
                    commit_delivered_final_output(state, session_id, content, turn_id).await?;
                    if let Some(updated_session) = updated_session {
                        if updated_session.quote_target_id.as_deref().is_some()
                            && updated_session.last_patched_response_card_id.as_deref()
                                == Some(pending_card_id)
                        {
                            if let Some(quote_target_id) =
                                updated_session.quote_target_id.as_deref()
                            {
                                if let Err(err) = lark_add_reaction(
                                    state,
                                    bot,
                                    quote_target_id,
                                    COMPLETED_REACTION_EMOJI_TYPE,
                                )
                                .await
                                {
                                    warn!(
                                        "failed to add completion reaction to {}: {}",
                                        quote_target_id, err
                                    );
                                }
                            }
                        }
                    }
                    return Ok(());
                }
                Err(err) => {
                    let _ = clear_pending_response_patch_marker(&state.paths, session_id).await;
                    match fallback_reply().await {
                        Ok(()) => {
                            let snapshot = {
                                let mut sessions = state.sessions.lock().await;
                                if let Some(entry) = sessions.get_mut(session_id) {
                                    mark_pending_response_card_patched_if_current(
                                        entry,
                                        pending_card_id,
                                    );
                                }
                                sessions.clone()
                            };
                            persist_sessions(&state.paths, &snapshot).await?;
                            commit_delivered_final_output(state, session_id, content, turn_id)
                                .await?;
                            return Ok(());
                        }
                        Err(fallback_err) => {
                            if is_lark_message_withdrawn_error(&fallback_err) {
                                return Err(fallback_err);
                            }
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    fallback_reply().await?;
    commit_delivered_final_output(state, session_id, content, turn_id).await
}

pub(crate) fn schedule_final_output_delivery(
    state: AppState,
    session_id: String,
    content: String,
    turn_id: Option<String>,
    kind: Option<FinalOutputKind>,
    user_text: Option<String>,
    attempt: usize,
) {
    let Some(delay_ms) = next_final_output_retry_delay_ms(attempt) else {
        return;
    };
    // Persist retry marker so daemon restart can resume delivery
    persist_final_output_retry_marker(
        &state,
        &session_id,
        content.clone(),
        turn_id.clone(),
        kind,
        user_text.clone(),
        attempt,
    );
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let turn_key = turn_id
            .as_deref()
            .and_then(|turn_id| final_output_turn_key(&session_id, turn_id));

        let session_closed = {
            let sessions = state.sessions.lock().await;
            should_abort_final_output_delivery(sessions.get(&session_id))
        };
        if session_closed {
            if let Some(turn_key) = turn_key.as_deref() {
                state
                    .inflight_final_output_turns
                    .lock()
                    .await
                    .remove(turn_key);
            }
            return;
        }

        match deliver_final_output_once(
            &state,
            &session_id,
            &content,
            turn_id.as_deref(),
            kind,
            user_text.as_deref(),
        )
        .await
        {
            Ok(()) => {
                clear_final_output_retry(&state, &session_id, turn_id.as_deref());
                if let Some(turn_key) = turn_key.as_deref() {
                    state
                        .inflight_final_output_turns
                        .lock()
                        .await
                        .remove(turn_key);
                }
            }
            Err(err) => {
                if is_lark_message_withdrawn_error(&err) {
                    warn!(
                        "final output delivery for {} aborted because the root message was withdrawn",
                        session_id
                    );
                    if let Some(turn_key) = turn_key.as_deref() {
                        state
                            .inflight_final_output_turns
                            .lock()
                            .await
                            .remove(turn_key);
                    }
                    let _ = close_session(State(state.clone()), AxumPath(session_id.clone())).await;
                    return;
                }
                let next = attempt + 1;
                let Some(next_delay_ms) = next_final_output_retry_delay_ms(next) else {
                    clear_final_output_retry(&state, &session_id, turn_id.as_deref());
                    if let Some(turn_key) = turn_key.as_deref() {
                        state
                            .inflight_final_output_turns
                            .lock()
                            .await
                            .remove(turn_key);
                    }
                    warn!(
                        "final output delivery gave up for {} after {} attempts: {}",
                        session_id, next, err
                    );
                    return;
                };
                warn!(
                    "final output delivery attempt {} failed for {}: {}; retrying in {}ms",
                    next, session_id, err, next_delay_ms
                );
                schedule_final_output_delivery(
                    state, session_id, content, turn_id, kind, user_text, next,
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn resolve_tui_prompt_final_text_prefers_toggled_option_texts() {
        let mut session = make_session("session-a");
        session.tui_prompt_options = vec![
            TuiPromptOption {
                label: Some("1".to_string()),
                text: "alpha".to_string(),
                selected: false,
                option_type: Some("toggle".to_string()),
                keys: vec!["A".to_string()],
            },
            TuiPromptOption {
                label: Some("2".to_string()),
                text: "beta".to_string(),
                selected: false,
                option_type: Some("toggle".to_string()),
                keys: vec!["B".to_string()],
            },
        ];
        session.tui_toggled_indices = vec![1, 0];
        assert_eq!(
            resolve_tui_prompt_final_text(&session, Some("fallback")),
            "alpha, beta"
        );
        session.tui_toggled_indices.clear();
        assert_eq!(
            resolve_tui_prompt_final_text(&session, Some("fallback")),
            "fallback"
        );
        assert_eq!(resolve_tui_prompt_final_text(&session, None), "selection");
    }

    #[test]
    fn retryable_feishu_resume_error_detects_timeout_and_rate_limit() {
        assert!(is_retryable_feishu_resume_error(&anyhow::anyhow!(
            "request timed out"
        )));
        assert!(is_retryable_feishu_resume_error(&anyhow::anyhow!(
            "429 too many requests"
        )));
        assert!(!is_retryable_feishu_resume_error(&anyhow::anyhow!(
            "permission denied"
        )));
    }

    #[test]
    fn build_feishu_transient_failure_marks_retryable_result() {
        let failure = build_feishu_transient_failure(
            "activity-1",
            "attempt-1",
            "feishu-im",
            "idem-key-1",
            "FeishuSubmitRetryable",
            "request timed out".to_string(),
        );
        assert_eq!(failure.provider, "feishu-im");
        assert_eq!(failure.error_class, "retryable");
        assert_eq!(failure.error_code, "FeishuSubmitRetryable");
        assert_eq!(failure.idempotency_key, "idem-key-1");
    }

    #[test]
    fn build_workflow_resume_response_includes_transient_failures() {
        let schedule_result = beam_core::ScheduleResumeResult {
            reconciled: vec![beam_core::ScheduleResumeOutcome {
                activity_id: "act-s".to_string(),
                attempt_id: "att-s".to_string(),
                decision: "completedByIdempotentSubmit".to_string(),
            }],
            fresh_retry: vec![],
            skipped: vec!["skip-s".to_string()],
        };
        let feishu_result = FeishuResumeResult {
            reconciled: vec![],
            fresh_retry: vec![],
            transient_failures: vec![FeishuTransientFailure {
                activity_id: "act-f".to_string(),
                attempt_id: "att-f".to_string(),
                provider: "feishu-im".to_string(),
                idempotency_key: "idem-f".to_string(),
                error_code: "FeishuSubmitRetryable".to_string(),
                error_class: "retryable".to_string(),
                error_message: "request timed out".to_string(),
            }],
            skipped: vec!["skip-f".to_string()],
        };
        let snapshot = beam_core::RunSnapshotDTO {
            run_id: "run-1".to_string(),
            run: beam_core::RunState {
                run_id: "run-1".to_string(),
                status: RunStatus::Running,
                workflow_id: Some("flow-1".to_string()),
                revision_id: Some("rev-1".to_string()),
                initiator: Some("cli".to_string()),
                input: None,
                output: None,
                failed_node_id: None,
                root_cause_event_id: None,
                cancel_origin_event_id: None,
                bot_snapshots: None,
                cancelled_run_intent: None,
                cancelled_node_intents: Default::default(),
            },
            last_seq: 42,
            nodes: vec![],
            activities: vec![],
            loops: None,
            dangling: beam_core::DanglingSnapshot {
                activities: vec![],
                effect_attempted: vec![],
                waits: vec![],
                wait_resolutions: vec![],
                cancels: vec![],
            },
            outputs: Default::default(),
            attempt_io: Default::default(),
            chat_binding: None,
            updated_at: 123,
        };
        let resume_started_event = beam_core::WorkflowEventEnvelope {
            event_id: "run-1-43".to_string(),
            run_id: "run-1".to_string(),
            timestamp: 0,
            schema_version: 1,
            actor: beam_core::WorkflowActor::System,
            event_type: "resumeStarted".to_string(),
            payload: serde_json::json!({
                "daemonId": "beam-daemon",
                "lastSeenEventId": "run-1-42",
                "reason": null,
            }),
            payload_hash: None,
        };
        let payload = build_workflow_resume_response(
            "run-1".to_string(),
            RunStatus::Running,
            false,
            42,
            Some(&resume_started_event),
            &HashMap::new(),
            &snapshot,
            &schedule_result,
            &feishu_result,
            &workflow_reconcilers::ReconcilerRegistryCheckResult {
                covered_providers: vec!["beam-schedule".to_string(), "feishu-im".to_string()],
                missing_providers: vec![],
            },
            vec![],
            vec![],
            vec![],
        );
        assert_eq!(payload["runId"], "run-1");
        assert_eq!(payload["resumeStartedEventId"], "run-1-43");
        assert_eq!(payload["resumeStartedEvent"]["eventId"], "run-1-43");
        assert_eq!(payload["resumeStartedEvent"]["type"], "resumeStarted");
        assert_eq!(
            payload["resumeStartedEvent"]["payload"]["daemonId"],
            "beam-daemon"
        );
        assert_eq!(
            payload["resumeStartedEvent"]["payload"]["lastSeenEventId"],
            "run-1-42"
        );
        assert_eq!(payload["reconciled"], 1);
        assert_eq!(payload["freshRetry"], 0);
        assert_eq!(
            payload["reconcileOutcomes"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(payload["reconcileOutcomes"][0]["provider"], "beam-schedule");
        assert_eq!(
            payload["reconcileOutcomes"][0]["capability"],
            "readOnlyLookup"
        );
        assert_eq!(payload["reconcileOutcomes"][0]["recovered"], false);
        assert_eq!(
            payload["workerCrashedOutcomes"].as_array().map(Vec::len),
            Some(0)
        );
        assert_eq!(
            payload["waitRecoveryOutcomes"].as_array().map(Vec::len),
            Some(0)
        );
        assert_eq!(
            payload["cancelRecoveryOutcomes"].as_array().map(Vec::len),
            Some(0)
        );
        assert_eq!(
            payload["transientFailures"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            payload["transientFailures"][0]["errorCode"],
            "FeishuSubmitRetryable"
        );
        assert_eq!(payload["feishuOutcomes"].as_array().map(Vec::len), Some(0));
        assert_eq!(
            payload["scheduleOutcomes"].as_array().map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn next_display_mode_toggles_hidden_and_screenshot() {
        assert_eq!(next_display_mode(None), DisplayMode::Screenshot);
        assert_eq!(
            next_display_mode(Some(DisplayMode::Hidden)),
            DisplayMode::Screenshot
        );
        assert_eq!(
            next_display_mode(Some(DisplayMode::Screenshot)),
            DisplayMode::Hidden
        );
    }

    #[test]
    fn final_output_retry_delay_matches_three_attempt_backoff() {
        assert_eq!(next_final_output_retry_delay_ms(0), Some(0));
        assert_eq!(next_final_output_retry_delay_ms(1), Some(5_000));
        assert_eq!(next_final_output_retry_delay_ms(2), Some(15_000));
        assert_eq!(next_final_output_retry_delay_ms(3), None);
    }

    #[test]
    fn final_output_delivery_aborts_for_closed_or_missing_session() {
        assert!(should_abort_final_output_delivery(None));

        let closed = make_session("sess-closed");
        assert!(should_abort_final_output_delivery(Some(&closed)));

        let mut active = make_session("sess-active");
        active.status = SessionStatus::Active;
        active.closed_at = None;
        assert!(!should_abort_final_output_delivery(Some(&active)));
    }

    #[test]
    fn worker_final_output_dedupes_by_turn_id_instead_of_content() {
        let mut session = make_session("sess-final-output");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.last_final_output_turn_id = Some("turn-1".to_string());
        session.last_final_output = Some("done".to_string());

        assert!(should_skip_worker_final_output(&session, "turn-1"));
        assert!(!should_skip_worker_final_output(&session, "turn-2"));
        assert!(!should_skip_worker_final_output(&session, ""));
    }

    #[tokio::test]
    async fn final_output_footer_recipient_filters_known_bot_owner() {
        let paths = temp_paths("final-output-footer");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");
        std::fs::write(
            paths.root().join("bot-openids-app-1.json"),
            r#"{"Claude":"ou_bot"}"#,
        )
        .expect("write cross-ref");

        let mut bot_owner = make_session("sess-bot-owner");
        bot_owner.owner_open_id = Some("ou_bot".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &bot_owner),
            None
        );

        let mut human_owner = make_session("sess-human-owner");
        human_owner.owner_open_id = Some("ou_human".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &human_owner).as_deref(),
            Some("ou_human")
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
