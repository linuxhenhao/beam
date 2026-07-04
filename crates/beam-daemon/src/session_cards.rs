use super::*;

pub(crate) fn action_uses_live_stream_card(action: &str) -> bool {
    matches!(
        action,
        "get_write_link"
            | "toggle_display"
            | "toggle_stream"
            | "refresh_screenshot"
            | "export_text"
            | "term_action"
            | "retry_last_task"
            | "restart"
            | "close"
    )
}

pub(crate) fn stale_stream_card_action_self_heals_live_session(action: &str) -> bool {
    matches!(action, "toggle_display" | "toggle_stream")
}

pub(crate) fn stale_stream_card_action_reads_frozen_snapshot(action: &str) -> bool {
    matches!(action, "export_text")
}

pub(crate) fn resolve_card_render_target(
    action: &ParsedLarkCardAction,
    session: &Session,
) -> CardRenderTarget {
    match (
        action.clicked_message_id.as_deref(),
        session.stream_card_id.as_deref(),
    ) {
        (Some(clicked), Some(live)) if clicked != live => {
            CardRenderTarget::PatchMessage(clicked.to_string())
        }
        _ => CardRenderTarget::CallbackRaw,
    }
}

pub(crate) fn is_stale_stream_card_action(
    action: &ParsedLarkCardAction,
    session: &Session,
) -> bool {
    if !action_uses_live_stream_card(&action.action) {
        return false;
    }
    match (
        action.card_nonce.as_deref(),
        session.stream_card_nonce.as_deref(),
    ) {
        (Some(clicked), Some(current)) => clicked != current,
        _ => false,
    }
}

#[cfg(test)]
pub(crate) fn truncate_card_screen(screen: &str) -> String {
    let clean = screen.replace('\r', "");
    let mut out = String::new();
    for line in clean.lines().take(36) {
        let line = if line.chars().count() > 120 {
            format!("{}...", line.chars().take(117).collect::<String>())
        } else {
            line.to_string()
        };
        out.push_str(&line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

pub(crate) fn card_text<'a>(locale: Option<&str>, zh: &'a str, en: &'a str) -> &'a str {
    if prompt::is_zh_locale(locale) { zh } else { en }
}

pub(crate) fn build_writable_session_card(session: &Session, write_url: &str) -> String {
    let locale = session.locale.as_deref();
    let title = if session.title.trim().is_empty() {
        session
            .cli_id
            .clone()
            .unwrap_or_else(|| session.session_id.clone())
    } else {
        session.title.clone()
    };
    let card_nonce = session.stream_card_nonce.clone().unwrap_or_default();
    let mut actions = vec![serde_json::json!({
        "tag": "button",
        "text": card_i18n::plain_text(locale, "打开可写终端", "Open writable terminal"),
        "type": "primary",
        "multi_url": {
            "url": write_url,
            "pc_url": write_url,
            "android_url": write_url,
            "ios_url": write_url,
        },
    })];
    if session.adopted_from.is_none() {
        actions.push(serde_json::json!({
            "tag": "button",
            "text": card_i18n::plain_text(locale, "重启", "Restart"),
            "type": "default",
            "value": {
                "action": "restart",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                "visibility": "private",
                "card_nonce": card_nonce,
            }
        }));
    }
    let (close_label_zh, close_label_en) = if session.adopted_from.is_some() {
        ("断开连接", "Disconnect")
    } else {
        ("关闭会话", "Close session")
    };
    actions.push(serde_json::json!({
        "tag": "button",
        "text": card_i18n::plain_text(locale, close_label_zh, close_label_en),
        "type": "danger",
        "value": {
            "action": "close",
            "root_id": session.root_message_id,
            "session_id": session.session_id,
            "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
            "visibility": "private",
            "card_nonce": session.stream_card_nonce.clone().unwrap_or_default(),
        }
    }));
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "title": card_i18n::plain_text(locale, format!("{} · {}", "终端", title), format!("{} · {}", "terminal", title)),
            "template": "blue"
        },
        "elements": [
            { "tag": "action", "actions": actions }
        ]
    })
    .to_string()
}

pub(crate) fn build_readonly_link_card(session: &Session, ro_url: &str, _ro_token: &str) -> String {
    let locale = session.locale.as_deref();
    let title = session
        .cli_id
        .clone()
        .unwrap_or_else(|| session.session_id.clone());
    let display_title = if session.title.trim().is_empty() {
        title.clone()
    } else {
        session.title.clone()
    };
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "title": card_i18n::plain_text(
                locale,
                format!("只读终端 · {}", display_title),
                format!("Read-only terminal · {}", display_title),
            ),
            "template": "blue"
        },
        "elements": [
            {
                "tag": "markdown",
                "content": card_text(
                    locale,
                    "**只读访问**\n\n点击下方按钮以只读模式打开终端。链接仅可使用一次。",
                    "**Read-only access**\n\nClick the button below to open the terminal in read-only mode. The link is single-use."
                ),
                "i18n_content": {
                    "zh_cn": "**只读访问**\n\n点击下方按钮以只读模式打开终端。链接仅可使用一次。",
                    "en_us": "**Read-only access**\n\nClick the button below to open the terminal in read-only mode. The link is single-use.",
                },
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": card_i18n::plain_text(locale, "打开只读终端", "Open read-only terminal"),
                        "type": "primary",
                        "multi_url": {
                            "url": ro_url,
                            "pc_url": ro_url,
                            "android_url": ro_url,
                            "ios_url": ro_url,
                        }
                    }
                ]
            }
        ]
    })
    .to_string()
}

pub(crate) fn next_display_mode(current: Option<DisplayMode>) -> DisplayMode {
    match current.unwrap_or(DisplayMode::Hidden) {
        DisplayMode::Hidden => DisplayMode::Screenshot,
        DisplayMode::Screenshot => DisplayMode::Hidden,
    }
}

pub(crate) fn screen_status_card_label(status: ScreenStatus) -> &'static str {
    match status {
        ScreenStatus::Starting => "starting",
        ScreenStatus::Working => "working",
        ScreenStatus::Idle => "idle",
        ScreenStatus::Analyzing => "analyzing",
        ScreenStatus::Limited => "limited",
    }
}

pub(crate) fn session_stream_status(session: &Session) -> &'static str {
    if matches!(session.last_screen_status, Some(ScreenStatus::Limited))
        && session
            .usage_limit
            .as_ref()
            .is_some_and(|usage_limit| usage_limit.retry_ready)
    {
        return "retry_ready";
    }
    session
        .last_screen_status
        .map(screen_status_card_label)
        .unwrap_or("idle")
}

#[cfg(test)]
pub(crate) fn render_streaming_card_body(session: &Session) -> String {
    match session.display_mode.unwrap_or(DisplayMode::Hidden) {
        DisplayMode::Hidden => "[screen hidden]".to_string(),
        DisplayMode::Screenshot => {
            truncate_card_screen(session.current_screen.as_deref().unwrap_or(""))
        }
    }
}

pub(crate) fn streaming_card_template(status: &str) -> &'static str {
    match status {
        "closed" => "grey",
        "starting" => "yellow",
        "idle" => "green",
        "retry_ready" => "green",
        "limited" => "red",
        _ => "blue",
    }
}

pub(crate) fn status_card_text<'a>(locale: Option<&str>, status: &'a str) -> &'a str {
    match status {
        "closed" => card_text(locale, "已关闭", "closed"),
        "starting" => card_text(locale, "启动中", "starting"),
        "idle" => card_text(locale, "空闲", "idle"),
        "retry_ready" => card_text(locale, "可重试", "retry ready"),
        "limited" => card_text(locale, "受限", "limited"),
        "working" => card_text(locale, "工作中", "working"),
        "analyzing" => card_text(locale, "分析中", "analyzing"),
        _ => status,
    }
}

pub(crate) fn usage_limit_matches(a: &CliUsageLimitState, b: &CliUsageLimitState) -> bool {
    a.kind == b.kind && a.retry_at_ms == b.retry_at_ms && a.retry_label == b.retry_label
}

pub(crate) fn prepare_retry_last_task(
    session: &Session,
    now_ms: u64,
) -> Result<(Session, String), &'static str> {
    let cli_input = session
        .last_cli_input
        .clone()
        .ok_or("retry last task missing")?;
    let usage_limit = session
        .usage_limit
        .as_ref()
        .ok_or("retry last task unavailable")?;
    if !usage_limit.retry_ready && usage_limit.retry_at_ms > now_ms {
        return Err("retry last task not ready");
    }
    let mut updated = session.clone();
    updated.usage_limit = None;
    updated.last_screen_status = Some(ScreenStatus::Working);
    updated.current_image_key = None;
    Ok((updated, cli_input))
}

pub(crate) fn build_export_text_reply(session: &Session) -> String {
    let content = session
        .current_screen
        .as_deref()
        .unwrap_or("")
        .trim()
        .replace('\r', "");
    if content.is_empty() {
        return "(no output yet)".to_string();
    }
    let mut out = String::new();
    for line in content.lines() {
        if out.len() + line.len() + 1 > 3500 {
            out.push_str("\n...");
            break;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

/// Load zellij web tokens from the standard paths location (for card rendering).
/// Returns None if the file doesn't exist or can't be parsed.
pub(crate) fn load_zellij_web_tokens_for_card() -> Option<zellij_web::ZellijWebTokens> {
    let paths = BeamPaths::discover().ok()?;
    zellij_web::load_zellij_web_tokens(&paths.zellij_web_tokens_json())
        .ok()
        .flatten()
}

/// Build a terminal URL with a Beam ticket attached, falling back to raw token
/// if ticket generation is not available (e.g., zellij tokens not loaded).
pub(crate) fn build_terminal_url_with_ticket(
    base_url: &str,
    session_id: &str,
    permission: terminal_auth::TerminalPermission,
) -> String {
    let ticket = terminal_auth::generate_terminal_ticket(session_id, permission);
    let sep = if base_url.contains('?') { "&" } else { "?" };
    format!(
        "{}{sep}{}={}",
        base_url,
        terminal_auth::TICKET_QUERY_PARAM,
        ticket
    )
}

pub(crate) fn build_streaming_card(session: &Session, status: &str) -> String {
    let locale = session.locale.as_deref();
    let title = if session.title.trim().is_empty() {
        session.session_id.clone()
    } else {
        session.title.clone()
    };
    let base_terminal = session.terminal_url.clone().unwrap_or_default();
    let zellij_tokens = load_zellij_web_tokens_for_card();
    let has_ro_token = zellij_tokens
        .as_ref()
        .and_then(|t| t.read_only_token.as_deref())
        .map_or(false, |t| !t.is_empty());
    let terminal = if has_ro_token && !base_terminal.is_empty() {
        build_terminal_url_with_ticket(
            &base_terminal,
            &session.session_id,
            terminal_auth::TerminalPermission::ReadOnly,
        )
    } else {
        base_terminal
    };
    let effective_status = if status == "limited"
        && session
            .usage_limit
            .as_ref()
            .is_some_and(|usage_limit| usage_limit.retry_ready)
    {
        "retry_ready"
    } else {
        status
    };
    let display_mode = session.display_mode.unwrap_or(DisplayMode::Hidden);
    let card_nonce = session.stream_card_nonce.clone();
    let mut elements = vec![
        serde_json::json!({
            "tag": "markdown",
            "content": format!("{} `{}`", "session", session.session_id),
            "i18n_content": {
                "zh_cn": format!("{} `{}`", "会话", session.session_id),
                "en_us": format!("{} `{}`", "session", session.session_id),
            }
        }),
        serde_json::json!({ "tag": "hr" }),
    ];
    if status == "limited" {
        if let Some(usage_limit) = session.usage_limit.as_ref() {
            let retry_label = &usage_limit.retry_label;
            let usage_zh = if usage_limit.retry_ready {
                format!("限制已解除。{} 后可重试。", retry_label)
            } else {
                format!("用量受限。请在 {} 后重试。", retry_label)
            };
            let usage_en = if usage_limit.retry_ready {
                format!("limit cleared. Retry is ready after {}.", retry_label)
            } else {
                format!("usage limited. Try again at {}.", retry_label)
            };
            elements.push(serde_json::json!({
                "tag": "markdown",
                "content": usage_en,
                "i18n_content": {
                    "zh_cn": usage_zh,
                    "en_us": usage_en,
                }
            }));
            elements.push(serde_json::json!({ "tag": "hr" }));
        }
    }
    if display_mode == DisplayMode::Screenshot {
        if let Some(image_key) = session.current_image_key.as_deref() {
            elements.push(serde_json::json!({
                "tag": "img",
                "img_key": image_key,
                "alt": { "tag": "plain_text", "content": "" },
                "mode": "fit_horizontal",
                "preview": true
            }));
        } else {
            elements.push(serde_json::json!({
                "tag": "markdown",
                "content": "waiting for screenshot",
                "i18n_content": {
                    "zh_cn": "等待截图",
                    "en_us": "waiting for screenshot",
                }
            }));
        }
    }
    let (toggle_label_zh, toggle_label_en) = match display_mode {
        DisplayMode::Hidden => ("显示截图", "Show screenshot"),
        DisplayMode::Screenshot => ("隐藏截图", "Hide screenshot"),
    };
    let action_nonce = card_nonce.clone();
    let mut actions: Vec<serde_json::Value> = Vec::new();

    if display_mode == DisplayMode::Screenshot {
        actions.push(serde_json::json!({
            "tag": "button",
            "text": card_i18n::plain_text(locale, "刷新截图", "Refresh screenshot"),
            "type": "default",
            "value": {
                "action": "refresh_screenshot",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                "card_nonce": card_nonce.clone(),
            }
        }));
    }
    actions.push(serde_json::json!({
        "tag": "button",
        "text": card_i18n::plain_text(locale, toggle_label_zh, toggle_label_en),
        "type": "default",
        "value": {
            "action": "toggle_display",
            "root_id": session.root_message_id,
            "session_id": session.session_id,
            "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
            "card_nonce": card_nonce.clone(),
        }
    }));
    actions.push(serde_json::json!({
        "tag": "button",
        "text": card_i18n::plain_text(locale, "打开只读终端", "Open read-only terminal"),
        "type": "primary",
        "multi_url": {
            "url": terminal,
            "pc_url": terminal,
            "android_url": terminal,
            "ios_url": terminal,
        },
    }));

    actions.push(serde_json::json!({
        "tag": "button",
        "text": card_i18n::plain_text(locale, "私发可写链接", "Send write link privately"),
        "type": "default",
        "value": {
            "action": "get_write_link",
            "root_id": session.root_message_id,
            "session_id": session.session_id,
            "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
            "card_nonce": action_nonce,
        }
    }));
    if status == "limited"
        && session
            .usage_limit
            .as_ref()
            .is_some_and(|usage_limit| usage_limit.retry_ready)
    {
        actions.push(serde_json::json!({
            "tag": "button",
            "text": card_i18n::plain_text(locale, "重试上次任务", "Retry last task"),
            "type": "primary",
            "value": {
                "action": "retry_last_task",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                "card_nonce": card_nonce.clone(),
            }
        }));
    }
    if session.adopted_from.is_none() {
        actions.push(serde_json::json!({
            "tag": "button",
            "text": card_i18n::plain_text(locale, "重启", "Restart"),
            "type": "default",
            "value": {
                "action": "restart",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                "card_nonce": card_nonce.clone(),
            }
        }));
    }
    actions.push(serde_json::json!({
        "tag": "button",
        "text": card_i18n::plain_text(
            locale,
            if session.adopted_from.is_some() { "断开连接" } else { "关闭会话" },
            if session.adopted_from.is_some() { "Disconnect" } else { "Close session" },
        ),
        "type": "danger",
        "value": {
            "action": "close",
            "root_id": session.root_message_id,
            "session_id": session.session_id,
            "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
            "card_nonce": card_nonce.clone(),
        }
    }));
    elements.push(serde_json::json!({
        "tag": "action",
        "actions": actions
    }));
    if display_mode == DisplayMode::Screenshot {
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": [
                serde_json::json!({
                    "tag": "button",
                    "text": card_i18n::plain_text(locale, "导出文本", "Export text"),
                    "type": "default",
                    "value": {
                        "action": "export_text",
                        "root_id": session.root_message_id,
                        "session_id": session.session_id,
                        "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                        "card_nonce": card_nonce.clone(),
                    }
                }),
            ]
        }));
        let key_button = |label_zh: &str, label_en: &str, key: &str| {
            serde_json::json!({
                "tag": "button",
                "text": card_i18n::plain_text(locale, label_zh, label_en),
                "type": "default",
                "value": {
                    "action": "term_action",
                    "key": key,
                    "root_id": session.root_message_id,
                    "session_id": session.session_id,
                    "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                    "card_nonce": card_nonce.clone(),
                }
            })
        };
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": [
                key_button("Esc", "Esc", "esc"),
                key_button("^C", "^C", "ctrlc"),
                key_button("Tab", "Tab", "tab"),
                key_button("Space", "Space", "space"),
                key_button("Enter", "Enter", "enter"),
            ]
        }));
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": [
                key_button("左", "Left", "left"),
                key_button("上", "Up", "up"),
                key_button("下", "Down", "down"),
                key_button("右", "Right", "right"),
                key_button("上半页", "Half Pg Up", "half_page_up"),
                key_button("下半页", "Half Pg Down", "half_page_down"),
            ]
        }));
    }
    serde_json::json!({
        "config": { "wide_screen_mode": true, "enable_forward": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "template": streaming_card_template(effective_status),
            "title": card_i18n::plain_text(
                locale,
                format!("{} · {}", title, status_card_text(Some("zh"), effective_status)),
                format!("{} · {}", title, status_card_text(Some("en"), effective_status)),
            )
        },
        "elements": elements
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn streaming_card_template_matches_expected_status_colors() {
        assert_eq!(streaming_card_template("starting"), "yellow");
        assert_eq!(streaming_card_template("working"), "blue");
        assert_eq!(streaming_card_template("idle"), "green");
        assert_eq!(streaming_card_template("limited"), "red");
        assert_eq!(streaming_card_template("closed"), "grey");
    }

    #[test]
    fn screen_status_card_label_matches_worker_statuses() {
        assert_eq!(screen_status_card_label(ScreenStatus::Starting), "starting");
        assert_eq!(screen_status_card_label(ScreenStatus::Working), "working");
        assert_eq!(screen_status_card_label(ScreenStatus::Idle), "idle");
        assert_eq!(
            screen_status_card_label(ScreenStatus::Analyzing),
            "analyzing"
        );
        assert_eq!(screen_status_card_label(ScreenStatus::Limited), "limited");
    }

    #[test]
    fn session_stream_status_uses_last_screen_status_and_defaults_idle() {
        let mut session = make_session("sess-status");
        assert_eq!(session_stream_status(&session), "idle");

        session.last_screen_status = Some(ScreenStatus::Working);
        assert_eq!(session_stream_status(&session), "working");

        session.last_screen_status = Some(ScreenStatus::Analyzing);
        assert_eq!(session_stream_status(&session), "analyzing");

        session.last_screen_status = Some(ScreenStatus::Limited);
        session.usage_limit = Some(CliUsageLimitState {
            limited: true,
            kind: beam_core::CliUsageLimitKind::Usage,
            retry_at_ms: 42,
            retry_label: "3:15 PM".to_string(),
            retry_ready: true,
        });
        assert_eq!(session_stream_status(&session), "retry_ready");
    }

    #[test]
    fn build_adopt_helpers_render_stable_replies() {
        let session = make_session("sess-1");
        let summary = SessionSummary::from(&session);
        assert_eq!(
            build_adopt_zellij_result_reply(Ok(&summary)),
            "adopted sess-1"
        );
        assert_eq!(
            build_adopt_zellij_result_reply(Err("session not found")),
            "adopt failed: session not found"
        );
    }

    #[test]
    fn build_closed_session_card_contains_resume_button_and_command() {
        let mut session = make_session("sess-9");
        session.title = "Fix beam".to_string();
        session.working_dir = Some("/repo/beam".to_string());
        session.cli_id = Some("codex".to_string());
        session.root_message_id = "root-9".to_string();

        let card: Value =
            serde_json::from_str(&build_closed_session_card(&session)).expect("valid card json");
        assert_eq!(
            card.pointer("/header/title/content")
                .and_then(Value::as_str),
            Some("session closed")
        );
        let body = card
            .pointer("/elements/0/content")
            .and_then(Value::as_str)
            .expect("markdown body");
        assert!(body.contains("Fix beam"));
        assert!(body.contains("beam session resume sess-9"));
        assert!(body.contains("/repo/beam"));
        assert_eq!(
            card.pointer("/elements/1/actions/0/value/action")
                .and_then(Value::as_str),
            Some("resume")
        );
        assert_eq!(
            card.pointer("/elements/1/actions/0/value/session_id")
                .and_then(Value::as_str),
            Some("sess-9")
        );
    }

    #[test]
    fn build_writable_session_card_contains_write_restart_and_close_buttons() {
        let mut session = make_session("sess-7");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.title = "Investigate".to_string();
        let write_url = "http://proxy.example.com/s/sess-7?token=abc";
        let card: Value = serde_json::from_str(&build_writable_session_card(&session, write_url))
            .expect("valid card json");
        let actions = card
            .pointer("/elements/0/actions")
            .and_then(Value::as_array)
            .expect("actions array");
        assert_eq!(actions.len(), 3);
        assert_eq!(
            actions[0].pointer("/multi_url/url").and_then(Value::as_str),
            Some("http://proxy.example.com/s/sess-7?token=abc")
        );
        assert_eq!(
            actions[1].pointer("/value/action").and_then(Value::as_str),
            Some("restart")
        );
        assert_eq!(
            actions[2].pointer("/value/action").and_then(Value::as_str),
            Some("close")
        );
        assert_eq!(
            actions[1]
                .pointer("/value/visibility")
                .and_then(Value::as_str),
            Some("private")
        );
    }

    #[test]
    fn build_writable_session_card_adopted_shows_disconnect_without_restart() {
        let mut session = make_session("sess-7-adopted");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.title = "Adopted".to_string();
        session.adopted_from = Some(AdoptedFrom {
            zellij_session: Some("my-session".to_string()),
            zellij_pane_id: Some("pane-1".to_string()),
            original_cli_pid: 9999,
            cwd: "/home/user".to_string(),
            ..Default::default()
        });
        let write_url = "http://proxy.example.com/s/sess-7-adopted?token=abc";
        let card: Value = serde_json::from_str(&build_writable_session_card(&session, write_url))
            .expect("valid card json");
        let actions = card
            .pointer("/elements/0/actions")
            .and_then(Value::as_array)
            .expect("actions array");
        // Adopted: 2 actions — "Open writable terminal" + "Disconnect" (no restart)
        assert_eq!(actions.len(), 2);
        assert_eq!(
            actions[0].pointer("/multi_url/url").and_then(Value::as_str),
            Some("http://proxy.example.com/s/sess-7-adopted?token=abc")
        );
        assert_eq!(
            actions[1].pointer("/value/action").and_then(Value::as_str),
            Some("close")
        );
        assert_eq!(
            actions[1].pointer("/text/content").and_then(Value::as_str),
            Some("Disconnect")
        );
        // No restart action present
        let action_names: Vec<&str> = actions
            .iter()
            .filter_map(|a| a.pointer("/value/action").and_then(Value::as_str))
            .collect();
        assert!(!action_names.contains(&"restart"));
    }

    #[test]
    fn build_streaming_card_keeps_hidden_mode_actions_minimal() {
        let mut session = make_session("sess-8");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        // Use a clean URL without legacy token to test ticket-based auth
        session.terminal_url = Some("http://127.0.0.1:9000/s/sess-8".to_string());
        session.current_screen = Some("hello".to_string());
        session.stream_card_nonce = Some("nonce-live".to_string());
        let card: Value =
            serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
        let body = card
            .pointer("/elements/0/content")
            .and_then(Value::as_str)
            .expect("markdown body");
        assert!(
            !body.contains("Open read-only terminal"),
            "markdown should not contain Open read-only terminal link"
        );
        let actions = card
            .pointer("/elements/2/actions")
            .and_then(Value::as_array)
            .expect("actions array");
        // Collect action names for presence check (order may vary depending on token availability)
        let action_names: Vec<&str> = actions
            .iter()
            .filter_map(|a| a.pointer("/value/action").and_then(Value::as_str))
            .collect();
        assert!(
            action_names.contains(&"toggle_display"),
            "should have toggle_display action"
        );
        assert!(
            !action_names.contains(&"get_read_only_link"),
            "should not have get_read_only_link action"
        );
        assert!(
            action_names.contains(&"get_write_link"),
            "should have get_write_link action"
        );
        // Check URL starts with base (may have ticket appended)
        let url = actions
            .iter()
            .find_map(|a| a.pointer("/multi_url/url").and_then(Value::as_str))
            .expect("url should exist");
        let terminal_action = actions
            .iter()
            .find(|a| a.pointer("/multi_url/url").and_then(Value::as_str) == Some(url))
            .expect("terminal action should exist");
        assert_eq!(
            terminal_action
                .pointer("/text/content")
                .and_then(Value::as_str),
            Some("Open read-only terminal")
        );
        assert!(
            url.starts_with("http://127.0.0.1:9000/s/sess-8"),
            "url should start with base: {url}"
        );
        assert!(card.pointer("/elements/3").is_none());
    }

    #[test]
    fn build_streaming_card_uses_chinese_labels_for_zh_locale() {
        let mut session = make_session("sess-zh");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.locale = Some("zh".to_string());
        session.terminal_url = Some("http://127.0.0.1:9000/s/sess-zh".to_string());
        let card: Value =
            serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
        assert_eq!(
            card.pointer("/header/title/content")
                .and_then(Value::as_str),
            Some("session sess-zh · 空闲")
        );
        let actions = card
            .pointer("/elements/2/actions")
            .and_then(Value::as_array)
            .expect("actions array");
        assert_eq!(
            actions[0].pointer("/text/content").and_then(Value::as_str),
            Some("显示截图")
        );
        assert_eq!(
            actions[1].pointer("/text/content").and_then(Value::as_str),
            Some("打开只读终端")
        );
        assert_eq!(
            actions[2].pointer("/text/content").and_then(Value::as_str),
            Some("私发可写链接")
        );
    }

    #[test]
    fn build_readonly_link_card_omits_stale_five_minute_copy() {
        let session = make_session("sess-ro");
        let card: Value = serde_json::from_str(&build_readonly_link_card(
            &session,
            "http://proxy.example.com/s/sess-ro",
            "",
        ))
        .expect("valid card json");
        let body = card
            .pointer("/elements/0/content")
            .and_then(Value::as_str)
            .expect("markdown body");
        assert!(body.contains("single-use"));
        assert!(
            !body.contains("5 minutes"),
            "read-only link copy should not mention the stale 5 minute TTL"
        );
    }

    #[test]
    fn build_streaming_card_uses_starting_template() {
        let mut session = make_session("sess-starting");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        let card: Value = serde_json::from_str(&build_streaming_card(&session, "starting"))
            .expect("valid card json");
        assert_eq!(
            card.pointer("/header/template").and_then(Value::as_str),
            Some("yellow")
        );
    }

    #[test]
    fn build_streaming_card_adds_term_action_rows_in_screenshot_mode() {
        let mut session = make_session("sess-11");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.terminal_url = Some("http://127.0.0.1:9000/?token=abc".to_string());
        session.current_screen = Some("hello".to_string());
        session.display_mode = Some(DisplayMode::Screenshot);
        let card: Value =
            serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
        assert_eq!(
            card.pointer("/elements/5/actions/0/value/action")
                .and_then(Value::as_str),
            Some("term_action")
        );
        assert_eq!(
            card.pointer("/elements/6/actions/5/value/key")
                .and_then(Value::as_str),
            Some("half_page_down")
        );
        assert_eq!(
            card.pointer("/elements/6/actions/5/text/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("下半页")
        );
        assert_eq!(
            card.pointer("/elements/6/actions/5/text/i18n_content/en_us")
                .and_then(Value::as_str),
            Some("Half Pg Down")
        );
        assert_eq!(
            card.pointer("/elements/3/actions/0/value/action")
                .and_then(Value::as_str),
            Some("refresh_screenshot")
        );
    }

    #[test]
    fn refresh_screenshot_in_hidden_mode_returns_info_toast() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let app_id = "app-refresh";
            let bot = BotConfig {
                name: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "codex".to_string(),
                cli_bin: None,
                cli_args: Vec::new(),
                skip_working_dir_prompt: false,
                model: None,
                working_dir: None,
                lark_encrypt_key: None,
                lark_verification_token: None,
                allowed_users: vec!["ou_owner".to_string()],
                private_card: false,
                allowed_chat_groups: Vec::new(),
                chat_grants: std::collections::HashMap::new(),
                global_grants: Vec::new(),
                oncall_chats: Vec::new(),
                restrict_grant_commands: false,
                message_quota: None,
                quota_state: std::collections::HashMap::new(),
            };
            let state = make_state(
                temp_paths("refresh-hidden"),
                HashMap::from([(app_id.to_string(), bot)]),
            );
            let mut session = make_session("sess-refresh");
            session.lark_app_id = app_id.to_string();
            session.closed_at = None;
            session.status = SessionStatus::Active;
            session.display_mode = Some(DisplayMode::Hidden);
            session.stream_card_nonce = Some("nonce-refresh".to_string());
            {
                let mut sessions = state.sessions.lock().await;
                sessions.insert(session.session_id.clone(), session.clone());
            }

            let payload = serde_json::json!({
                "operator": { "open_id": "ou_other" },
                "action": { "value": {
                    "action": "refresh_screenshot",
                    "root_id": session.root_message_id,
                    "session_id": session.session_id,
                    "cli_id": session.cli_id.unwrap_or_else(|| "codex".to_string()),
                } }
            });

            let response = handle_lark_card_action_payload(&state, app_id, payload)
                .await
                .expect("handler response");
            assert_eq!(
                response.0.pointer("/toast/type").and_then(Value::as_str),
                Some("info")
            );
            assert_eq!(
                response.0.pointer("/toast/content").and_then(Value::as_str),
                Some("show screenshot first")
            );
        });
    }

    #[test]
    fn toggle_display_returns_a_screenshot_card_response() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
            let app_id = "app-toggle";
            let bot = BotConfig {
                name: None,
                lark_app_id: app_id.to_string(),
                lark_app_secret: "secret".to_string(),
                cli_id: "codex".to_string(),
                cli_bin: None,
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
            };
            let state = make_state(
                temp_paths("toggle-display"),
                HashMap::from([(app_id.to_string(), bot)]),
            );
            let mut session = make_session("sess-toggle");
            session.lark_app_id = app_id.to_string();
            session.closed_at = None;
            session.status = SessionStatus::Active;
            session.display_mode = Some(DisplayMode::Hidden);
            session.current_image_key = None;
            session.stream_card_nonce = Some("nonce-toggle".to_string());
            {
                let mut sessions = state.sessions.lock().await;
                sessions.insert(session.session_id.clone(), session.clone());
            }

            let payload = serde_json::json!({
                "operator": { "open_id": "ou_user" },
                "action": { "value": {
                    "action": "toggle_display",
                    "root_id": session.root_message_id,
                    "session_id": session.session_id,
                    "cli_id": session.cli_id.unwrap_or_else(|| "codex".to_string()),
                } }
            });

            let response = handle_lark_card_action_payload(&state, app_id, payload)
                .await
                .expect("handler response");
            assert_eq!(
                response.0.pointer("/toast/type").and_then(Value::as_str),
                Some("success")
            );
            assert_eq!(
                response.0.pointer("/card/type").and_then(Value::as_str),
                Some("raw")
            );
            assert_eq!(
                response
                    .0
                    .pointer("/card/data/elements/2/content")
                    .and_then(Value::as_str),
                Some("waiting for screenshot")
            );
            assert_eq!(
                response
                    .0
                    .pointer("/card/data/elements/3/actions/0/text/content")
                    .and_then(Value::as_str),
                Some("Refresh screenshot")
            );
            let stored = state
                .sessions
                .lock()
                .await
                .get(&session.session_id)
                .cloned()
                .expect("stored session");
            assert_eq!(stored.display_mode, Some(DisplayMode::Screenshot));
        });
    }

    #[test]
    fn build_streaming_card_shows_retry_button_when_limit_is_ready() {
        let mut session = make_session("sess-limit");
        session.last_screen_status = Some(ScreenStatus::Limited);
        session.usage_limit = Some(CliUsageLimitState {
            limited: true,
            kind: beam_core::CliUsageLimitKind::Usage,
            retry_at_ms: 42,
            retry_label: "3:15 PM".to_string(),
            retry_ready: true,
        });
        let card: Value = serde_json::from_str(&build_streaming_card(&session, "limited"))
            .expect("valid card json");
        assert_eq!(
            card.pointer("/header/template").and_then(Value::as_str),
            Some("green")
        );
        assert_eq!(
            card.pointer("/elements/2/content").and_then(Value::as_str),
            Some("limit cleared. Retry is ready after 3:15 PM.")
        );
        let found_retry = card
            .get("elements")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|element| element.get("actions").and_then(Value::as_array))
            .flatten()
            .any(|action| {
                action.pointer("/value/action").and_then(Value::as_str) == Some("retry_last_task")
            });
        assert!(found_retry);
    }

    #[test]
    fn build_streaming_card_renders_image_in_screenshot_mode_when_available() {
        let mut session = make_session("sess-image");
        session.display_mode = Some(DisplayMode::Screenshot);
        session.current_image_key = Some("img_v2_abc".to_string());
        session.current_screen = Some("should not render".to_string());
        let card: Value =
            serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
        assert_eq!(
            card.pointer("/elements/2/img_key").and_then(Value::as_str),
            Some("img_v2_abc")
        );
    }

    #[test]
    fn build_streaming_card_adopted_shows_disconnect_without_restart() {
        let mut session = make_session("sess-adopted-stream");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.adopted_from = Some(AdoptedFrom {
            zellij_session: Some("my-session".to_string()),
            zellij_pane_id: Some("pane-1".to_string()),
            original_cli_pid: 9999,
            cwd: "/home/user".to_string(),
            ..Default::default()
        });
        let card: Value =
            serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
        // Collect action names for presence check (order may vary depending on token availability)
        let action_names: Vec<&str> = card
            .get("elements")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|element| element.get("actions").and_then(Value::as_array))
            .flatten()
            .filter_map(|a| a.pointer("/value/action").and_then(Value::as_str))
            .collect();
        // Restart must NOT appear for adopted sessions
        assert!(
            !action_names.contains(&"restart"),
            "restart should not appear for adopted session"
        );
        // Close action must appear (via Disconnect label)
        assert!(
            action_names.contains(&"close"),
            "close action should be present: {action_names:?}"
        );
        // Verify the close action shows "Disconnect" text
        let close_text = card
            .get("elements")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|element| element.get("actions").and_then(Value::as_array))
            .flatten()
            .find(|a| a.pointer("/value/action").and_then(Value::as_str) == Some("close"))
            .and_then(|a| a.pointer("/text/content").and_then(Value::as_str));
        assert_eq!(close_text, Some("Disconnect"));
    }

    #[test]
    fn render_streaming_card_body_hides_content_in_hidden_mode() {
        let mut session = make_session("sess-10");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.current_screen = Some("secret output".to_string());
        session.display_mode = Some(DisplayMode::Hidden);
        assert_eq!(render_streaming_card_body(&session), "[screen hidden]");

        session.display_mode = Some(DisplayMode::Screenshot);
        assert_eq!(render_streaming_card_body(&session), "secret output");
    }

    #[test]
    fn build_export_text_reply_handles_empty_and_truncates_long_output() {
        let mut session = make_session("sess-12");
        assert_eq!(build_export_text_reply(&session), "(no output yet)");

        session.current_screen = Some(format!("{}\n{}\n", "a".repeat(2000), "b".repeat(2000)));
        let body = build_export_text_reply(&session);
        assert!(body.starts_with(&"a".repeat(2000)));
        assert!(body.contains("..."));
        assert!(body.len() <= 3504);
    }

    #[test]
    fn worker_ready_display_mode_command_only_resends_screenshot_mode() {
        let mut hidden = make_session("sess-hidden");
        hidden.status = SessionStatus::Active;
        hidden.closed_at = None;
        hidden.display_mode = Some(DisplayMode::Hidden);
        assert_eq!(worker_ready_display_mode_command(&hidden), None);

        let mut screenshot = make_session("sess-shot");
        screenshot.status = SessionStatus::Active;
        screenshot.closed_at = None;
        screenshot.display_mode = Some(DisplayMode::Screenshot);
        assert_eq!(
            worker_ready_display_mode_command(&screenshot),
            Some(DaemonToWorker::SetDisplayMode {
                mode: DisplayMode::Screenshot
            })
        );
    }

    #[test]
    fn build_report_post_content_mentions_owner_and_preserves_line_breaks() {
        let mut session = make_session("sess-report");
        session.owner_open_id = Some("ou_owner".to_string());
        let payload = build_report_post_content(&session, "first line\nsecond line");
        let value: Value = serde_json::from_str(&payload).expect("json");
        let content = value["zh_cn"]["content"].as_array().expect("content");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0].as_array().unwrap()[0]["tag"], "at");
        assert_eq!(content[0].as_array().unwrap()[0]["user_id"], "ou_owner");
        assert_eq!(content[0].as_array().unwrap()[2]["text"], "first line");
        assert_eq!(content[1].as_array().unwrap()[0]["text"], "second line");
    }

    #[test]
    fn session_summary_carries_last_screen_status() {
        let mut session = make_session("sess-summary");
        session.current_screen = Some("hello".to_string());
        session.last_screen_status = Some(ScreenStatus::Limited);
        session.quote_target_id = Some("om_user".to_string());
        session.pending_response_card_id = Some("om_pending".to_string());
        session.pending_response_card_state = Some(PendingResponseCardState::Open);
        session.last_patched_response_card_id = Some("om_done".to_string());
        session.last_final_output_turn_id = Some("turn-9".to_string());

        let summary = SessionSummary::from(&session);
        assert_eq!(summary.current_screen.as_deref(), Some("hello"));
        assert_eq!(summary.last_screen_status, Some(ScreenStatus::Limited));
        assert_eq!(summary.quote_target_id.as_deref(), Some("om_user"));
        assert_eq!(
            summary.pending_response_card_id.as_deref(),
            Some("om_pending")
        );
        assert_eq!(
            summary.pending_response_card_state,
            Some(PendingResponseCardState::Open)
        );
        assert_eq!(
            summary.last_patched_response_card_id.as_deref(),
            Some("om_done")
        );
        assert_eq!(summary.last_final_output_turn_id.as_deref(), Some("turn-9"));
    }

    #[test]
    fn build_quote_hint_includes_text_when_parent_id_differs() {
        let hint =
            prompt::build_quote_hint(Some("quoted-1"), "msg-1", SessionScope::Thread, "root-1");
        assert!(hint.contains("quoted-1"));
    }

    #[test]
    fn build_quote_hint_empty_when_no_parent_id() {
        assert_eq!(
            prompt::build_quote_hint(None, "msg-1", SessionScope::Thread, "root-1"),
            ""
        );
    }

    #[test]
    fn build_quote_hint_empty_when_parent_id_matches_message_id() {
        assert_eq!(
            prompt::build_quote_hint(Some("msg-1"), "msg-1", SessionScope::Thread, "root-1"),
            ""
        );
    }

    #[test]
    fn build_follow_up_content_wraps_in_user_message() {
        let mentions: Vec<LarkEventMention> = vec![];
        let opts = prompt::FollowUpContentOptions {
            session_id: "test-session",
            sender_open_id: None,
            sender_type: None,
            mentions: &mentions,
            cli_id: "codex",
            locale: None,
        };
        let result = prompt::build_follow_up_content("hello", &opts);
        assert!(result.contains("<user_message>"));
        assert!(result.contains("hello"));
    }

    #[test]
    fn build_follow_up_content_includes_mentions() {
        let mentions = vec![LarkEventMention {
            key: "ou_123".to_string(),
            name: "Alice".to_string(),
        }];
        let opts = prompt::FollowUpContentOptions {
            session_id: "test-session",
            sender_open_id: None,
            sender_type: None,
            mentions: &mentions,
            cli_id: "codex",
            locale: None,
        };
        let result = prompt::build_follow_up_content("hi", &opts);
        assert!(result.contains("<mentions>"));
        assert!(result.contains("Alice"));
        assert!(result.contains("ou_123"));
    }

    #[test]
    fn build_follow_up_content_skips_beam_reminder_for_mira() {
        let mentions: Vec<LarkEventMention> = vec![];
        let opts = prompt::FollowUpContentOptions {
            session_id: "test-session",
            sender_open_id: None,
            sender_type: None,
            mentions: &mentions,
            cli_id: "mira",
            locale: None,
        };
        let result = prompt::build_follow_up_content("hi", &opts);
        assert!(!result.contains("beam_reminder"));
    }
}
