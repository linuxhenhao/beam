use super::*;

/// Handle a `/grant` or similar text command from a Lark message.
/// Extracted from the inline helper inside `handle_lark_event_payload`.
pub(crate) async fn handle_grant_text_command(
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
                let _ = lark_reply_message(state, bot, message_id, &format!("grant failed: {}", e))
                    .await;
                return Some(());
            }
            if let Err(e) = tokio::fs::write(
                &bots_path,
                serde_json::to_string_pretty(&config).unwrap_or_default(),
            )
            .await
            {
                let _ = lark_reply_message(state, bot, message_id, &format!("save failed: {}", e))
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
            Some(())
        }
        grant::GrantAction::Grant => {
            let targets: Vec<String> = cmd.targets.iter().map(|t| t.open_id.clone()).collect();
            if targets.is_empty() {
                let _ =
                    lark_reply_message(state, bot, message_id, "usage: /grant @user [quota]").await;
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
            Some(())
        }
        grant::GrantAction::Revoke => {
            let targets: Vec<String> = cmd.targets.iter().map(|t| t.open_id.clone()).collect();
            if targets.is_empty() {
                let _ = lark_reply_message(state, bot, message_id, "usage: /revoke @user").await;
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
                let _ = lark_reply_message(state, bot, message_id, &format!("save failed: {}", e))
                    .await;
                return Some(());
            }
            let _ = lark_reply_message(state, bot, message_id, &results.join("\n")).await;
            Some(())
        }
    }
}

/// Handle directory selection card actions.
/// Extracted from the inline helper inside `handle_lark_card_action_payload`.
pub(crate) async fn handle_dir_select_card_action(
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

            let working_dir = dir_select::resolve_dir(&pending.root_working_dir, working_dir_rel);

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

            create_session_from_pending(state, bot, &pending, &working_dir, working_dir_rel).await
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
            if let Some(card_msg_id) = &pending.card_message_id
                && let Err(e) = lark_update_card(state, bot, card_msg_id, &card).await
            {
                warn!(
                    "dir_select_filter: PATCH card for {} failed: {:?}",
                    pending_id, e
                );
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
                    if let Some(card_msg_id) = &pending.card_message_id
                        && let Err(e) = lark_update_card(state, bot, card_msg_id, &card).await
                    {
                        warn!(
                            "dir_select_best: PATCH card for {} failed: {:?}",
                            pending_id, e
                        );
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
/// Extracted from the inline helper inside `handle_lark_card_action_payload`.
pub(crate) async fn create_session_from_pending(
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

/// Handle grant card actions (grant_chat, grant_global, grant_deny).
/// Extracted from the inline helper inside `handle_lark_card_action_payload`.
pub(crate) async fn handle_grant_card_action(
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
    let mut config: serde_json::Value = serde_json::from_str(&raw).unwrap_or(serde_json::json!([]));

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

/// Build the terminal link choice card JSON for read-only or write links.
/// Extracted from the standalone helper in the original file.
pub(crate) async fn build_terminal_link_choice_card_json(
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

/// Handle the transcript_select card action.
pub(crate) async fn handle_transcript_select(
    state: &AppState,
    bot: &BotConfig,
    action: &ParsedLarkCardAction,
) -> Result<Json<Value>, (StatusCode, String)> {
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
            bot,
            clicked_msg_id,
            &build_transcript_selected_card(cli_session_id, action.operator_open_id.as_deref()),
        )
        .await;
    }
    Ok(Json(build_lark_card_action_toast(
        "success",
        "transcript source selected",
    )))
}
