use axum::http::StatusCode;
use beam_core::{BotConfig, grant_restricted};

use crate::{
    AppState, LarkPreflight, can_operate_bot_with_state, evaluate_talk_for_bot_with_state,
    internal_error, is_operate_command, lark_reply_message, record_observed_bots,
};

pub(crate) fn lark_event_dedupe_key(app_id: &str, event_id: &str) -> Option<String> {
    let event_id = event_id.trim();
    if event_id.is_empty() {
        None
    } else {
        Some(format!("{}:{}", app_id, event_id))
    }
}

pub(crate) fn evaluate_lark_preflight(
    state: &AppState,
    bot: &BotConfig,
    text: &str,
    chat_id: &str,
    sender_open_id: Option<&str>,
    deduped: bool,
) -> LarkPreflight {
    if deduped {
        return LarkPreflight::Deduped;
    }
    if text.is_empty() {
        return LarkPreflight::IgnoredEmptyText;
    }

    let Some(sender) = sender_open_id else {
        if is_operate_command(text) {
            return LarkPreflight::Denied {
                reply: "permission denied: unknown sender",
            };
        }
        return LarkPreflight::Continue;
    };

    if is_operate_command(text) && !can_operate_bot_with_state(state, bot, Some(sender)) {
        return LarkPreflight::Denied {
            reply: "permission denied",
        };
    }

    let talk = evaluate_talk_for_bot_with_state(state, bot, chat_id, sender);
    if !talk.allowed {
        return LarkPreflight::Denied {
            reply: "permission denied: you are not authorized to talk to this bot",
        };
    }

    if grant_restricted(&talk, bot.restrict_grant_commands) && (text.starts_with('/')) {
        return LarkPreflight::Denied {
            reply: "slash commands are restricted for grant-authorized users",
        };
    }

    LarkPreflight::Continue
}

pub(crate) async fn handle_introduce_command(
    state: &AppState,
    app_id: &str,
    chat_id: &str,
    message_id: &str,
    parsed: &crate::ParsedLarkInboundMessage,
) -> Result<bool, (StatusCode, String)> {
    if !parsed.text.trim_start().starts_with("/introduce") {
        return Ok(false);
    }
    let entries = parsed
        .mentions
        .iter()
        .filter_map(|mention| {
            let open_id = mention.key.trim();
            let name = mention.name.trim();
            if open_id.is_empty() || name.is_empty() {
                None
            } else {
                Some((open_id.to_string(), name.to_string()))
            }
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Ok(true);
    }
    record_observed_bots(&state.paths, app_id, chat_id, &entries, "introduce")
        .map_err(internal_error)?;
    let summary = entries
        .iter()
        .map(|(_, name)| format!("@{}", name))
        .collect::<Vec<_>>()
        .join(" ");
    let reply = if summary.is_empty() {
        "✅ 已认识本群伙伴".to_string()
    } else {
        format!("✅ 已认识本群 {}", summary)
    };
    let bot = state
        .bots
        .get(app_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;
    let _ = lark_reply_message(state, &bot, message_id, &reply).await;
    Ok(true)
}
