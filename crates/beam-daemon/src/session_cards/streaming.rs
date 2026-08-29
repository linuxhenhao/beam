use super::actions::card_text;
use crate::{BackendKind, DisplayMode, ScreenStatus, Session, card_i18n};

/// Toggles between Hidden and Screenshot display modes. Used when the user clicks
/// the "Show/Hide screenshot" button on the streaming card.
pub(crate) fn next_display_mode(current: Option<DisplayMode>) -> DisplayMode {
    match current.unwrap_or(DisplayMode::Hidden) {
        DisplayMode::Hidden => DisplayMode::Screenshot,
        DisplayMode::Screenshot => DisplayMode::Hidden,
    }
}

/// Maps worker ScreenStatus to a short card label string.
pub(crate) fn screen_status_card_label(status: ScreenStatus) -> &'static str {
    match status {
        ScreenStatus::Starting => "starting",
        ScreenStatus::Working => "working",
        ScreenStatus::Idle => "idle",
        ScreenStatus::Analyzing => "analyzing",
        ScreenStatus::Limited => "limited",
    }
}

/// Computes the current streaming card status label for a session, considering
/// both the last screen status and any usage limit / retry readiness.
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

/// Returns the Lark card template color for a given status string.
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

/// Returns a human-readable status label for card headers, based on locale.
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

/// Builds the streaming session card JSON string for Lark. This is the main
/// interactive session card that shows session status, screenshot, and action buttons.
///
/// IMPORTANT: per AGENTS.md, streaming cards created via this function must NEVER
/// call `start_pending_response_turn` — that would mark the streaming card as the
/// pending response target, causing `deliver_final_output_once` to PATCH-overwrite
/// the terminal card with reply content.
pub(crate) fn build_streaming_card(session: &Session, status: &str) -> String {
    let locale = session.locale.as_deref();
    let title = if session.title.trim().is_empty() {
        session.session_id.clone()
    } else {
        session.title.clone()
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
    if status == "limited"
        && let Some(usage_limit) = session.usage_limit.as_ref()
    {
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
    // Herdr v1 has no web terminal: do not emit buttons that proxy to zellij
    // web. The card carries a `herdr agent attach` hint instead (Q6 = show).
    if session.backend_kind == BackendKind::Zellij {
        actions.push(serde_json::json!({
            "tag": "button",
            "text": card_i18n::plain_text(locale, "选择只读终端入口", "Choose read-only terminal entry"),
            "type": "primary",
            "value": {
                "action": "choose_read_only_terminal_link",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.clone().unwrap_or_else(|| "cli".to_string()),
                "card_nonce": action_nonce,
            }
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
    } else {
        elements.push(serde_json::json!({
            "tag": "markdown",
            "content": "attach with: `herdr agent attach`",
            "i18n_content": {
                "zh_cn": "用 `herdr agent attach` 连接终端",
                "en_us": "attach with: `herdr agent attach`",
            }
        }));
    }
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

/// Builds a text-only reply body for the "export text" card action.
/// Truncates long output at 3500 bytes and appends "..." if truncated.
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
