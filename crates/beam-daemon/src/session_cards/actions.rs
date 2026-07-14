use crate::{
    CardRenderTarget, CliUsageLimitState, ParsedLarkCardAction, ScreenStatus, Session, prompt,
};

/// Returns true if the given card action should be routed through the live streaming card
/// (the current/active card), as opposed to a stale or frozen snapshot card.
pub(crate) fn action_uses_live_stream_card(action: &str) -> bool {
    matches!(
        action,
        "get_write_link"
            | "choose_read_only_terminal_link"
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

/// Returns true if the given card action can self-heal a stale card click by re-initializing
/// the live streaming card session state (e.g., toggling display mode).
pub(crate) fn stale_stream_card_action_self_heals_live_session(action: &str) -> bool {
    matches!(action, "toggle_display" | "toggle_stream")
}

/// Returns true if the given card action reads a frozen snapshot of the session state
/// (e.g., exporting text from a stale card), rather than requiring live session state.
pub(crate) fn stale_stream_card_action_reads_frozen_snapshot(action: &str) -> bool {
    matches!(action, "export_text")
}

/// Determines how to render a card response for the given action and session.
/// Returns either a PATCH to the clicked (non-live) message, or the raw callback-based response.
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

/// Returns true if the given card action was triggered on a stale/outdated streaming card
/// (nonce mismatch), meaning the client is interacting with an old card that should not
/// drive the live session UI.
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

/// Simple i18n-aware card text helper: returns the zh string for Chinese locale, en otherwise.
pub(crate) fn card_text<'a>(locale: Option<&str>, zh: &'a str, en: &'a str) -> &'a str {
    if prompt::is_zh_locale(locale) { zh } else { en }
}

/// Returns true if two CliUsageLimitState values represent the same effective limit
/// (same kind, retry time, and retry label). Used to avoid redundant streaming card patches.
pub(crate) fn usage_limit_matches(a: &CliUsageLimitState, b: &CliUsageLimitState) -> bool {
    a.kind == b.kind && a.retry_at_ms == b.retry_at_ms && a.retry_label == b.retry_label
}

/// Prepares the session for the "retry last task" action: clears the usage limit,
/// marks the session as "Working", and returns the last CLI input for replay.
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
