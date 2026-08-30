use super::*;

/// Whether a session is far enough along to receive streaming/screenshot
/// cards. Decoupled from `terminal_url`: Herdr sessions have no web terminal
/// in v1, so their latch is the persisted workspace/pane ids instead.
pub(crate) fn session_card_ready(session: &Session) -> bool {
    if session.lark_app_id == "local" || session.root_message_id.is_empty() {
        return false;
    }
    match session.backend_kind {
        BackendKind::Zellij => session.terminal_url.is_some(),
        BackendKind::Herdr => {
            session.herdr_workspace_id.is_some() && session.herdr_pane_id.is_some()
        }
    }
}

pub(crate) fn build_adopt_already_attached_reply(session: &Session) -> String {
    let adopted = match session.adopted_from.as_ref() {
        Some(adopted) => adopted,
        None => return "session is not adopted".to_string(),
    };
    let cli_name = adopted.cli_id.as_deref().unwrap_or("cli");
    let pane = adopted
        .tmux_target
        .as_deref()
        .map(str::to_string)
        .or_else(|| {
            adopted
                .zellij_session
                .as_deref()
                .zip(adopted.zellij_pane_id.as_deref())
                .map(|(session, pane_id)| format!("{}/{}", session, pane_id))
        })
        .unwrap_or_else(|| "unknown".to_string());
    format!(
        "session already adopted from {} ({})\ndisconnect it before running /adopt again",
        cli_name, pane
    )
}

pub(crate) fn build_adopt_zellij_result_reply(result: Result<&SessionSummary, &str>) -> String {
    match result {
        Ok(session) => format!("adopted {}", session.session_id),
        Err(err) => format!("adopt failed: {}", err),
    }
}

pub(crate) fn build_zellij_adopt_post_content(items: &[ZellijAdoptCandidate]) -> String {
    let mut paragraphs: Vec<Vec<serde_json::Value>> = vec![vec![serde_json::json!({
        "tag": "text",
        "text": "Available zellij sessions (copy a command block and send it to adopt):"
    })]];
    for item in items {
        paragraphs.push(vec![serde_json::json!({
            "tag": "text",
            "text": format!("{}  {}", item.title, item.cwd),
        })]);
        paragraphs.push(vec![serde_json::json!({
            "tag": "code_block",
            "language": "PlainText",
            "text": format!("/adopt {}:{}", item.zellij_session, item.zellij_pane_id),
        })]);
    }
    serde_json::json!({
        "zh_cn": { "title": "", "content": paragraphs },
    })
    .to_string()
}

/// Append Herdr adopt candidates to an existing adopt-list post. Commands are
/// printed with the `herdr:` prefix so users do not paste a bare `w1:p1`
/// (which the parser would treat as a zellij target).
pub(crate) fn append_herdr_adopt_post_content(
    existing: String,
    items: &[crate::herdr_adopt::HerdrAdoptCandidate],
) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(&existing).expect("existing adopt post is valid json");
    let content = value
        .pointer_mut("/zh_cn/content")
        .and_then(serde_json::Value::as_array_mut)
        .expect("adopt post content array");
    content.push(serde_json::json!([{
        "tag": "text",
        "text": "Available herdr panes (copy a command block and send it to adopt):",
    }]));
    for item in items {
        content.push(serde_json::json!([{
            "tag": "text",
            "text": format!("{}  {}", item.title, item.cwd),
        }]));
        content.push(serde_json::json!([{
            "tag": "code_block",
            "language": "PlainText",
            "text": format!("/adopt herdr:{}", item.pane_id),
        }]));
    }
    value.to_string()
}

pub(crate) fn build_closed_session_reply(session: &Session) -> String {
    format!(
        "session closed: {}\nresume: beam session resume {}",
        session.session_id, session.session_id
    )
}

pub(crate) fn build_closed_session_card(session: &Session) -> String {
    let locale = session.locale.as_deref();
    let title = if session.title.trim().is_empty() {
        session
            .cli_id
            .clone()
            .unwrap_or_else(|| session.session_id.clone())
    } else {
        session.title.clone()
    };
    let cli_name = session.cli_id.clone().unwrap_or_else(|| "cli".to_string());
    let resume_cmd = format!("beam session resume {}", session.session_id);
    let working_dir = session.working_dir.clone().unwrap_or_default();
    let body_zh = if working_dir.is_empty() {
        format!(
            "**{}**\n{} 已终止。\n恢复命令：\n```bash\n{}\n```",
            title, cli_name, resume_cmd
        )
    } else {
        format!(
            "**{}**\n{} 已终止。\n恢复命令：\n```bash\n{}\n```\n工作目录：`{}`",
            title, cli_name, resume_cmd, working_dir
        )
    };
    let body_en = if working_dir.is_empty() {
        format!(
            "**{}**\n{} terminated.\nresume with:\n```bash\n{}\n```",
            title, cli_name, resume_cmd
        )
    } else {
        format!(
            "**{}**\n{} terminated.\nresume with:\n```bash\n{}\n```\nworking dir: `{}`",
            title, cli_name, resume_cmd, working_dir
        )
    };
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "header": {
            "title": card_i18n::plain_text(locale, "会话已关闭", "session closed"),
            "template": "grey"
        },
        "elements": [
            { "tag": "markdown", "content": body_en, "i18n_content": { "zh_cn": body_zh, "en_us": body_en } },
            {
                "tag": "action",
                "actions": [{
                    "tag": "button",
                    "text": card_i18n::plain_text(locale, "恢复会话", "Resume session"),
                    "type": "primary",
                    "value": {
                        "action": "resume",
                        "root_id": session.root_message_id,
                        "session_id": session.session_id,
                        "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string())
                    }
                }]
            }
        ]
    })
    .to_string()
}

pub(crate) fn build_close_result_reply(
    session: &Session,
    result: Result<StatusCode, &str>,
) -> String {
    match result {
        Ok(_) => build_closed_session_reply(session),
        Err(err) => format!("close failed: {}", err),
    }
}

pub(crate) fn build_restart_result_reply(result: Result<StatusCode, &str>) -> String {
    match result {
        Ok(_) => "session restarting".to_string(),
        Err(err) => format!("restart failed: {}", err),
    }
}

pub(crate) fn decide_lark_card_delivery(session: &Session) -> LarkCardDeliveryPlan {
    if !session_card_ready(session) {
        return LarkCardDeliveryPlan::NotReady;
    }
    if session.stream_card_id.is_some() {
        LarkCardDeliveryPlan::PatchExisting
    } else {
        LarkCardDeliveryPlan::PostNew
    }
}

pub(crate) fn build_card_not_ready_reply() -> &'static str {
    "session card not ready"
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn session_card_ready_decouples_herdr_from_terminal_url() {
        // Zellij still needs terminal_url.
        let mut zellij = make_session("card-zellij");
        zellij.status = SessionStatus::Active;
        zellij.terminal_url = None;
        assert!(!session_card_ready(&zellij));
        zellij.terminal_url = Some("http://t".to_string());
        assert!(session_card_ready(&zellij));

        // Herdr needs workspace+pane ids and does NOT need terminal_url.
        let mut herdr = make_session("card-herdr");
        herdr.status = SessionStatus::Active;
        herdr.backend_kind = BackendKind::Herdr;
        herdr.terminal_url = None;
        assert!(!session_card_ready(&herdr));
        herdr.herdr_workspace_id = Some("w1".to_string());
        assert!(!session_card_ready(&herdr));
        herdr.herdr_pane_id = Some("w1:p1".to_string());
        assert!(session_card_ready(&herdr));
    }

    #[test]
    fn session_card_ready_rejects_local_and_empty_root() {
        let mut session = make_session("card-local");
        session.status = SessionStatus::Active;
        session.terminal_url = Some("http://t".to_string());
        session.lark_app_id = "local".to_string();
        assert!(!session_card_ready(&session));

        let mut session = make_session("card-root");
        session.status = SessionStatus::Active;
        session.terminal_url = Some("http://t".to_string());
        session.root_message_id = String::new();
        assert!(!session_card_ready(&session));
    }

    #[test]
    fn adopt_list_post_content_wraps_commands_in_code_blocks() {
        let items = vec![
            ZellijAdoptCandidate {
                zellij_session: "mysession".to_string(),
                zellij_pane_id: "terminal_0".to_string(),
                title: "claude".to_string(),
                cwd: "/home/user/proj".to_string(),
                cli_id: "claude-code".to_string(),
                cli_pid: Some(123),
                pane_cols: None,
                pane_rows: None,
            },
            ZellijAdoptCandidate {
                zellij_session: "dev".to_string(),
                zellij_pane_id: "terminal_3".to_string(),
                title: "codex".to_string(),
                cwd: "/home/user/other".to_string(),
                cli_id: "codex".to_string(),
                cli_pid: None,
                pane_cols: None,
                pane_rows: None,
            },
        ];
        let content = build_zellij_adopt_post_content(&items);
        let value: serde_json::Value = serde_json::from_str(&content).expect("valid post json");
        let paragraphs = value
            .pointer("/zh_cn/content")
            .and_then(serde_json::Value::as_array)
            .expect("post content array");
        assert_eq!(paragraphs.len(), 1 + items.len() * 2);
        assert_eq!(paragraphs[1][0]["tag"], "text");
        assert_eq!(paragraphs[1][0]["text"], "claude  /home/user/proj");
        assert_eq!(paragraphs[2][0]["tag"], "code_block");
        assert_eq!(paragraphs[2][0]["language"], "PlainText");
        assert_eq!(paragraphs[2][0]["text"], "/adopt mysession:terminal_0");
        assert_eq!(paragraphs[4][0]["text"], "/adopt dev:terminal_3");
    }

    #[test]
    fn adopt_list_post_content_herdr_append_keeps_paragraph_arrays() {
        let zellij_items = vec![ZellijAdoptCandidate {
            zellij_session: "mysession".to_string(),
            zellij_pane_id: "terminal_0".to_string(),
            title: "claude".to_string(),
            cwd: "/home/user/proj".to_string(),
            cli_id: "claude-code".to_string(),
            cli_pid: Some(123),
            pane_cols: None,
            pane_rows: None,
        }];
        let herdr_items = vec![crate::herdr_adopt::HerdrAdoptCandidate {
            workspace_id: "wN".to_string(),
            pane_id: "wN:p1".to_string(),
            title: "codex".to_string(),
            cwd: "/home/user/beam".to_string(),
            cli_pid: Some(42),
        }];
        let content = append_herdr_adopt_post_content(
            build_zellij_adopt_post_content(&zellij_items),
            &herdr_items,
        );
        let value: serde_json::Value = serde_json::from_str(&content).expect("valid post json");
        let paragraphs = value
            .pointer("/zh_cn/content")
            .and_then(serde_json::Value::as_array)
            .expect("post content array");
        for paragraph in paragraphs {
            assert!(
                paragraph.is_array(),
                "each post paragraph must be an array of elements, got: {paragraph}"
            );
        }
        assert!(content.contains("/adopt herdr:wN:p1"));
    }

    #[test]
    fn lark_reply_message_includes_reply_in_thread_when_true() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

            let paths = temp_paths("reply-thread");
            maybe_remove_dir(&paths.root().to_path_buf());

            let app_id = "app-reply";
            let bot = BotConfig {
                name: None,
                backend: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "codex".to_string(),
                cli_bin: None,
                cgroup_slice: None,
                cli_args: Vec::new(),
                skip_working_dir_prompt: false,
                model: None,
                working_dir: None,
                lark_encrypt_key: None,
                lark_verification_token: None,
                allowed_users: Vec::new(),
                private_card: false,
                allowed_chat_groups: Vec::new(),
                chat_grants: std::collections::HashMap::new(),
                global_grants: Vec::new(),
                oncall_chats: Vec::new(),
                restrict_grant_commands: false,
                message_quota: None,
                quota_state: std::collections::HashMap::new(),
                custom_triggers: Vec::new(),
            };
            let state = make_state(
                paths.clone(),
                HashMap::from([(app_id.to_string(), bot.clone())]),
            );

            let result =
                lark_reply_message_with_opts(&state, &bot, "msg-reply-1", "test message", true)
                    .await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "om_reply_mock");

            maybe_remove_dir(&paths.root().to_path_buf());
        });
    }

    #[test]
    fn user_notify_uses_reply_in_thread_for_thread_scope_session() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

            let paths = temp_paths("notify-thread");
            maybe_remove_dir(&paths.root().to_path_buf());

            let app_id = "app-notify-thread";
            let bot = BotConfig {
                name: None,
                backend: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "codex".to_string(),
                cli_bin: None,
                cgroup_slice: None,
                cli_args: Vec::new(),
                skip_working_dir_prompt: false,
                model: None,
                working_dir: None,
                lark_encrypt_key: None,
                lark_verification_token: None,
                allowed_users: Vec::new(),
                private_card: false,
                allowed_chat_groups: Vec::new(),
                chat_grants: std::collections::HashMap::new(),
                global_grants: Vec::new(),
                oncall_chats: Vec::new(),
                restrict_grant_commands: false,
                message_quota: None,
                quota_state: std::collections::HashMap::new(),
                custom_triggers: Vec::new(),
            };
            let state = make_state(
                paths.clone(),
                HashMap::from([(app_id.to_string(), bot.clone())]),
            );

            let session_id = "sess-notify-thread-1";
            let mut session = make_session(session_id);
            session.lark_app_id = app_id.to_string();
            session.scope = SessionScope::Thread;
            session.root_message_id = "root-notify-1".to_string();
            session.chat_id = "chat-notify-1".to_string();
            session.status = SessionStatus::Active;
            {
                let mut sessions = state.sessions.lock().await;
                sessions.insert(session_id.to_string(), session);
            }

            let snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(session_id).cloned().expect("stored session")
            };
            let result = match snapshot.scope {
                SessionScope::Thread if !snapshot.root_message_id.is_empty() => {
                    lark_reply_message_with_opts(
                        &state,
                        &bot,
                        &snapshot.root_message_id,
                        "notify message",
                        true,
                    )
                    .await
                }
                _ => {
                    lark_send_chat_message(&state, &bot, &snapshot.chat_id, "notify message").await
                }
            };
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "om_reply_mock");

            maybe_remove_dir(&paths.root().to_path_buf());
        });
    }

    #[test]
    fn user_notify_uses_send_for_chat_scope_session() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

            let paths = temp_paths("notify-chat");
            maybe_remove_dir(&paths.root().to_path_buf());

            let app_id = "app-notify-chat";
            let bot = BotConfig {
                name: None,
                backend: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "codex".to_string(),
                cli_bin: None,
                cgroup_slice: None,
                cli_args: Vec::new(),
                skip_working_dir_prompt: false,
                model: None,
                working_dir: None,
                lark_encrypt_key: None,
                lark_verification_token: None,
                allowed_users: Vec::new(),
                private_card: false,
                allowed_chat_groups: Vec::new(),
                chat_grants: std::collections::HashMap::new(),
                global_grants: Vec::new(),
                oncall_chats: Vec::new(),
                restrict_grant_commands: false,
                message_quota: None,
                quota_state: std::collections::HashMap::new(),
                custom_triggers: Vec::new(),
            };
            let state = make_state(
                paths.clone(),
                HashMap::from([(app_id.to_string(), bot.clone())]),
            );

            let session_id = "sess-notify-chat-1";
            let mut session = make_session(session_id);
            session.lark_app_id = app_id.to_string();
            session.scope = SessionScope::Chat;
            session.root_message_id = "root-notify-chat-1".to_string();
            session.chat_id = "chat-notify-1".to_string();
            session.status = SessionStatus::Active;
            {
                let mut sessions = state.sessions.lock().await;
                sessions.insert(session_id.to_string(), session);
            }

            let snapshot = {
                let sessions = state.sessions.lock().await;
                sessions.get(session_id).cloned().expect("stored session")
            };
            let result = match snapshot.scope {
                SessionScope::Thread if !snapshot.root_message_id.is_empty() => {
                    lark_reply_message_with_opts(
                        &state,
                        &bot,
                        &snapshot.root_message_id,
                        "notify message",
                        true,
                    )
                    .await
                }
                _ => {
                    lark_send_chat_message(&state, &bot, &snapshot.chat_id, "notify message").await
                }
            };
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "om_send_mock");

            maybe_remove_dir(&paths.root().to_path_buf());
        });
    }
}
