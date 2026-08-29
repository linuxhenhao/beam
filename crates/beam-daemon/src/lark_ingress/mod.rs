use super::*;

pub(crate) mod card_actions;
pub(crate) mod session_actions;
pub(crate) mod session_card_actions;
#[cfg(test)]
pub(crate) mod tests;
pub(crate) mod webhook;
pub(crate) mod workflow_actions;
pub(crate) mod ws_handlers;

// Re-exports so that `pub(crate) use lark_ingress::*;` in lib.rs works.
pub(crate) use card_actions::{
    build_terminal_link_choice_card_json, handle_dir_select_card_action, handle_grant_card_action,
    handle_grant_text_command,
};
pub(crate) use session_actions::{
    adopt_zellij_session, close_session, dispatch_event_outcome, ensure_worker_for_session,
    final_output, list_zellij_adopt_candidates, refresh_session, restart_session, resume_session,
    send_input,
};

pub(crate) use webhook::process_webhook_event_maybe_response;
pub(crate) use workflow_actions::{
    approve_workflow_run, cancel_workflow_run, end_workflow_attempt_resume,
    handle_workflow_text_command, reject_workflow_run, resume_workflow_run,
    start_workflow_attempt_resume,
};
pub(crate) use ws_handlers::spawn_lark_ws_clients;
// Test-only re-exports for WS card action tests
#[cfg(test)]
pub(crate) use ws_handlers::{
    LarkWsCardActionEventHandler, normalize_lark_ws_card_action,
    normalize_lark_ws_card_action_from_raw,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LarkEventOutcome {
    CloseSession { reply: String },
    RestartSession { reply: String },
    ShowCard { reply: String },
    AdoptZellij { target: String },
    AdoptHerdr { target: String },
    AdoptList,
    PassthroughInput { text: String },
    ReplyOnly { reply: String },
    ReuseSession,
    CreateSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedLarkInboundMessage {
    pub(crate) event_id: String,
    pub(crate) message_id: String,
    pub(crate) chat_id: String,
    pub(crate) chat_type: Option<String>,
    pub(crate) sender_type: Option<String>,
    pub(crate) scope: SessionScope,
    pub(crate) anchor: String,
    pub(crate) text: String,
    pub(crate) sender_open_id: Option<String>,
    pub(crate) mentions: Vec<LarkEventMention>,
    pub(crate) parent_id: Option<String>,
    pub(crate) root_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) locale: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LarkPreflight {
    Continue,
    Deduped,
    Denied { reply: &'static str },
    IgnoredEmptyText,
}

pub(crate) fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    error!("{}", err);
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

pub(crate) async fn handle_lark_event_payload(
    state: AppState,
    app_id: String,
    payload: Value,
    http_verification: Option<(HeaderMap, Bytes)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    // --- Check challenge ---
    if let Some(challenge) = payload.get("challenge").and_then(|v| v.as_str()) {
        return Ok(Json(serde_json::json!({ "challenge": challenge })));
    }

    // --- Event type filter ---
    let event_type = payload
        .pointer("/header/event_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event_type != "im.message.receive_v1" {
        return Ok(Json(
            serde_json::json!({ "ok": true, "ignored": event_type }),
        ));
    }

    // --- Delegate the heavy processing pipeline to the webhook sub-module ---
    // This covers: bot lookup, HTTP verification, message parsing, scope resolution,
    // dedup, multi-bot gate, preflight, introduce command, talk evaluation.
    let ctx =
        process_webhook_event_maybe_response(&state, &app_id, &payload, http_verification).await?;

    // If the pipeline returned an early response, return it.
    if let Some(response) = ctx.early_response {
        return Ok(response);
    }

    // Unpack the context.
    let bot = ctx.bot;
    let parsed = ctx.parsed;
    let text = ctx.text;
    let custom_trigger = ctx.custom_trigger;
    let inferred_locale = ctx.inferred_locale;
    let scope = ctx.scope;
    let anchor = ctx.anchor;
    let sender_open_id = ctx.sender_open_id;
    let talk = ctx.talk;
    let message_id = ctx.message_id;
    let chat_id = ctx.chat_id;

    // --- Grant text command (from card_actions) ---
    if handle_grant_text_command(
        &state,
        &bot,
        &message_id,
        &app_id,
        &chat_id,
        sender_open_id.as_deref(),
        &text,
    )
    .await
    .is_some()
    {
        return Ok(Json(serde_json::json!({ "ok": true, "grant": true })));
    }

    // --- Workflow text command (from workflow_actions) ---
    if let Some(response) =
        handle_workflow_text_command(&state, &bot, &message_id, &chat_id, &app_id, &text).await?
    {
        return Ok(response);
    }

    // --- Session dispatch ---
    let (existing, outcome) = {
        let sessions = state.sessions.lock().await;
        decide_lark_dispatch(
            &sessions,
            &app_id,
            &parsed,
            custom_trigger.as_ref(),
            ctx.trigger_activation,
        )
    };
    info!(
        app_id = %app_id,
        chat_id = %parsed.chat_id,
        chat_type = ?parsed.chat_type,
        message_id = %parsed.message_id,
        root_id = ?parsed.root_id,
        parent_id = ?parsed.parent_id,
        thread_id = ?parsed.thread_id,
        locale = ?parsed.locale,
        scope = ?parsed.scope,
        anchor = %parsed.anchor,
        existing_session_id = ?existing.as_ref().map(|s| s.session_id.as_str()),
        existing_thread_id = ?existing.as_ref().and_then(|s| s.thread_id.as_deref()),
        existing_root_message_id = ?existing.as_ref().map(|s| s.root_message_id.as_str()),
        outcome = ?outcome,
        "lark message dispatch",
    );

    dispatch_event_outcome(
        &state,
        &bot,
        &app_id,
        &parsed,
        &text,
        custom_trigger.as_ref(),
        ctx.trigger_activation,
        inferred_locale,
        &scope,
        &anchor,
        sender_open_id.as_deref(),
        talk.as_ref(),
        &message_id,
        &chat_id,
        existing,
        outcome,
    )
    .await
}

pub(crate) async fn handle_lark_card_action_payload(
    state: &AppState,
    app_id: &str,
    payload: Value,
) -> Result<Json<Value>, (StatusCode, String)> {
    let bot = state
        .bots
        .get(app_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;

    let action = parse_lark_card_action(&payload)?;
    info!(
        app_id = %app_id,
        action = %action.action,
        operator = %action.operator_open_id.as_deref().unwrap_or("unknown"),
        "lark card action received"
    );

    // Permission check for operate card actions
    if card_action_requires_operate(action.action.as_str())
        && !can_operate_bot_with_state(state, &bot, action.operator_open_id.as_deref())
    {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "permission denied",
        )));
    }

    // Route to sub-handlers based on action prefix / exact match
    if action.action.starts_with("ask_") {
        return ask::handle_ask_card_action(state, app_id, &action).await;
    }

    if matches!(
        action.action.as_str(),
        "grant_chat" | "grant_global" | "grant_deny"
    ) {
        return handle_grant_card_action(state, app_id, &action).await;
    }

    if matches!(
        action.action.as_str(),
        "dir_select_pick" | "dir_select_filter" | "dir_select_best"
    ) {
        return handle_dir_select_card_action(state, &bot, app_id, &action).await;
    }

    // --- Transcript source selection ---
    if action.action == "transcript_select" {
        return card_actions::handle_transcript_select(state, &bot, &action).await;
    }

    // --- Session-scoped card actions ---
    session_card_actions::handle_session_card_action(state, &bot, app_id, &action).await
}
