//! Core structured send and delivery logic for final output.
//!
//! This module contains:
//! - `handle_final_output_request` — structured send handler
//! - `deliver_final_output_once` — single-delivery path (worker bridge / legacy)
//! - Footer, mention, off-topic hint, and reply-card helpers
//!
//! Invariant: `deliver_final_output_once` only patches a pending response card
//! when its claim is still valid at delivery time (prevents streaming card from
//! being PATCH-overwritten into a final reply).

use std::collections::{HashMap, HashSet};

use crate::prompt::ObservedBot;
use crate::*;

// ---------------------------------------------------------------------------
// footer helpers
// ---------------------------------------------------------------------------

/// Resolve the footer recipient open_id with a human-first candidate order.
///
/// Candidate order (deduplicated and trimmed):
/// 1. `quote_target_sender_open_id` — the sender of the trigger/quote message (botmux parity)
/// 2. `owner_open_id` — the session creator
///
/// Returns the first candidate that is NOT a known bot, or `None` if all
/// candidates are empty or are known bots.
pub(crate) fn final_output_footer_recipient_open_id(
    paths: &BeamPaths,
    session: &Session,
) -> Option<String> {
    // Collect candidates in priority order: quote-target-sender > owner
    let mut candidates: Vec<&str> = Vec::with_capacity(2);
    if let Some(s) = session.quote_target_sender_open_id.as_deref() {
        let s = s.trim();
        if !s.is_empty() {
            candidates.push(s);
        }
    }
    if let Some(s) = session.owner_open_id.as_deref() {
        let s = s.trim();
        if !s.is_empty() {
            candidates.push(s);
        }
    }

    if candidates.is_empty() {
        return None;
    }

    let known_bot_ids = load_known_bot_open_ids_for_app(paths, &session.lark_app_id);

    // Dedup + human-first: return the first candidate that is NOT a known bot.
    let mut seen: HashSet<&str> = HashSet::new();
    for open_id in candidates {
        if seen.contains(open_id) {
            continue;
        }
        seen.insert(open_id);
        if !known_bot_ids.contains(open_id) {
            return Some(open_id.to_string());
        }
    }

    None
}

// ---------------------------------------------------------------------------
// contextual reply card
// ---------------------------------------------------------------------------

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
    if let Some(footer) = super::attachments::build_final_output_footer(recipient_open_id) {
        elements.push(serde_json::json!({ "tag": "hr" }));
        elements.push(serde_json::json!({
            "tag": "markdown",
            "text_size": "notation_small_v2",
            "content": footer,
            "i18n_content": {
                "zh_cn": footer,
                "en_us": footer,
            },
        }));
    }
    serde_json::json!({
        "schema": "2.0",
        "config": { "update_multi": true },
        "body": {
            "direction": "vertical",
            "elements": elements,
        }
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// mention helpers
// ---------------------------------------------------------------------------

/// Resolve the mention-back target open_id from the session.
pub(crate) fn resolve_mention_back_target(session: &Session) -> Result<String> {
    session
        .quote_target_sender_open_id
        .as_deref()
        .or(session.owner_open_id.as_deref())
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.trim().to_string())
        .with_context(|| {
            "--mention-back requires a sender to mention, but the session has no \
             quote_target_sender_open_id or owner_open_id. \
             Use --mention <open_id[:name]> or --no-mention instead."
        })
}

/// Auto-inject bot @-mentions in body text (P1-5: bot-to-bot mention).
pub(crate) fn auto_inject_bot_mentions(
    content: &str,
    bots: &[ObservedBot],
    self_open_id: Option<&str>,
) -> String {
    // Filter valid bots and skip self
    let mut bots: Vec<&ObservedBot> = bots
        .iter()
        .filter(|b| {
            let name = b.name.trim();
            let open_id = b.open_id.trim();
            if name.is_empty() || open_id.is_empty() {
                return false;
            }
            if Some(open_id) == self_open_id {
                return false;
            }
            true
        })
        .collect();

    if bots.is_empty() {
        return content.to_string();
    }

    // Sort by name length descending so longer names match first
    bots.sort_by(|a, b| b.name.len().cmp(&a.name.len()));

    let mut result = String::with_capacity(content.len());
    let mut pos = 0;

    while pos < content.len() {
        let remaining = &content[pos..];

        // Skip existing <at ...>...</at> blocks verbatim
        if remaining.starts_with("<at") {
            if let Some(end_idx) = remaining.find("</at>") {
                let block = &remaining[..end_idx + 5]; // include "</at>"
                result.push_str(block);
                pos += block.len();
                continue;
            }
            // Malformed <at without </at>: copy one char and continue
            result.push('<');
            pos += 1;
            continue;
        }

        // Check for @BotName
        if remaining.starts_with('@') {
            let mut matched: Option<&ObservedBot> = None;

            for bot in &bots {
                if bot.name.is_empty() {
                    continue;
                }
                let pattern = format!("@{}", bot.name);
                if remaining.starts_with(&pattern) {
                    let after = pos + pattern.len();
                    // Valid if end of string or followed by a boundary char
                    if after >= content.len() {
                        matched = Some(bot);
                        break;
                    }
                    let next = content[after..].chars().next().unwrap();
                    if is_mention_boundary(next) {
                        matched = Some(bot);
                        break;
                    }
                }
            }

            if let Some(bot) = matched {
                result.push_str(&format!(
                    "<at user_id=\"{}\">@{}</at>",
                    bot.open_id, bot.name
                ));
                pos += format!("@{}", bot.name).len();
                continue;
            }
        }

        // No match — copy one char
        let c = remaining.chars().next().unwrap();
        result.push(c);
        pos += c.len_utf8();
    }

    result
}

/// Returns `true` if `c` is a valid boundary after `@BotName`.
fn is_mention_boundary(c: char) -> bool {
    !c.is_alphanumeric() && c != '_'
}

// ---------------------------------------------------------------------------
// off-topic hint
// ---------------------------------------------------------------------------

/// Minimal off-topic sub-bot informational hint (P2-9).
pub(crate) fn off_topic_sub_bot_hint(
    current_session: &Session,
    mentioned_open_ids: &[String],
    sessions: &HashMap<String, Session>,
    anyway: bool,
) -> Option<String> {
    if anyway || mentioned_open_ids.is_empty() {
        return None;
    }

    let current_bot_open_id = current_session.bot_open_id.as_deref();

    for mentioned_open_id in mentioned_open_ids {
        if mentioned_open_id.trim().is_empty() {
            continue;
        }

        // Skip self-mention: the hint is about *other* sub-bots.
        if current_bot_open_id == Some(mentioned_open_id.as_str()) {
            continue;
        }

        for (sid, session) in sessions {
            // Skip the current session itself.
            if sid == &current_session.session_id {
                continue;
            }

            // Only consider sessions whose bot_open_id matches the mentioned id.
            let Some(bot_id) = session.bot_open_id.as_deref() else {
                continue;
            };
            if bot_id != mentioned_open_id {
                continue;
            }

            // Must be in the same chat.
            if session.chat_id != current_session.chat_id {
                continue;
            }

            // Must be active (not closed).
            if session.status == SessionStatus::Closed {
                continue;
            }

            // If same topic/thread → not off-topic.
            let same_root = !session.root_message_id.is_empty()
                && session.root_message_id == current_session.root_message_id;
            let same_thread = session.thread_id.as_deref().is_some()
                && current_session.thread_id.as_deref().is_some()
                && session.thread_id == current_session.thread_id;
            if same_root || same_thread {
                continue;
            }

            // Different topic in same chat → build hint.
            let root_hint = if !session.root_message_id.is_empty() {
                format!(", root_message_id={}", session.root_message_id)
            } else {
                String::new()
            };
            let thread_hint = if let Some(ref tid) = session.thread_id {
                format!(", thread_id={}", tid)
            } else {
                String::new()
            };

            return Some(format!(
                "Off-topic sub-bot hint: @{} is active in a different topic within chat {} (session={}{}{}). \
                 Consider using --into <message_id> to route there, or --anyway to suppress this hint.",
                mentioned_open_id, current_session.chat_id, sid, root_hint, thread_hint
            ));
        }
    }

    None
}

// ---------------------------------------------------------------------------
// send target resolution
// ---------------------------------------------------------------------------

/// Resolve where to send the message based on session scope and request flags.
fn resolve_send_target(
    session: &Session,
    req: &FinalOutputRequest,
) -> Result<(String, Option<String>, bool)> {
    // Override chat_id
    let chat_id = req
        .chat_id
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| session.chat_id.clone());

    // --into <message_id>: send into a specific thread
    if let Some(ref into_id) = req.into {
        let into_id = into_id.trim().to_string();
        if into_id.is_empty() {
            anyhow::bail!("--into requires a non-empty message_id");
        }
        return Ok((chat_id, Some(into_id), true));
    }

    // --top-level: always send as a chat message (not a reply)
    if req.top_level {
        return Ok((chat_id, None, false));
    }

    // Resolve based on session scope
    match session.scope {
        SessionScope::Thread => {
            if session.root_message_id.is_empty() {
                // No root to reply to; send as chat message
                Ok((chat_id, None, false))
            } else {
                Ok((chat_id, Some(session.root_message_id.clone()), true))
            }
        }
        SessionScope::Chat => {
            // Quote chain only in chat scope, and only when not explicitly disabled
            let quote_target = if req.no_quote {
                None
            } else if let Some(ref explicit_quote) = req.quote {
                Some(explicit_quote.trim().to_string())
            } else {
                session
                    .quote_target_id
                    .as_deref()
                    .filter(|v| !v.trim().is_empty())
                    .map(|v| v.trim().to_string())
            };

            match quote_target {
                Some(ref target_msg_id) if !target_msg_id.is_empty() => {
                    Ok((chat_id, Some(target_msg_id.clone()), false))
                }
                _ => Ok((chat_id, None, false)),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// reply card fallback
// ---------------------------------------------------------------------------

/// Reply with an interactive card to a target message, falling back to plain
/// text when the target was withdrawn.
pub(crate) async fn reply_card_or_fallback_plain_on_withdrawn(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    message_id: &str,
    card_json: &str,
    plain_content: &str,
    reply_in_thread: bool,
) -> Result<()> {
    match lark_reply_card_with_opts(state, bot, message_id, card_json, reply_in_thread).await {
        Ok(_) => Ok(()),
        Err(err) if should_fallback_to_plain_on_withdrawn(&err) => {
            warn!(
                "reply card target message withdrawn, falling back to plain chat message: {}",
                err
            );
            lark_send_chat_message(state, bot, chat_id, plain_content)
                .await
                .map(|_| ())
        }
        Err(err) => Err(err),
    }
}

/// Pure predicate: should a reply-card error trigger fallback to plain text?
pub(crate) fn should_fallback_to_plain_on_withdrawn(err: &anyhow::Error) -> bool {
    is_lark_message_withdrawn_error(err)
}

// ---------------------------------------------------------------------------
// state commit helper
// ---------------------------------------------------------------------------

pub(crate) async fn commit_delivered_final_output(
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

// ---------------------------------------------------------------------------
// handle_final_output_request — structured send entry point
// ---------------------------------------------------------------------------

/// Snapshot the session's current turn id for an explicit send. On success the
/// send marks that turn as answered, so the worker's final output for the same
/// turn is skipped by turn-id dedupe regardless of content differences.
pub(crate) async fn current_turn_id_for_explicit_send(
    state: &AppState,
    session_id: &str,
) -> Option<String> {
    let sessions = state.sessions.lock().await;
    sessions
        .get(session_id)
        .and_then(|session| session.current_turn_id.clone())
}

/// Core structured send handler for the daemon final-output endpoint.
pub(crate) async fn handle_final_output_request(
    state: &AppState,
    session_id: &str,
    req: FinalOutputRequest,
) -> Result<()> {
    // Captured up front (not after delivery) so a new turn starting mid-send
    // cannot be marked as answered by a send that belongs to the prior turn.
    let answered_turn_id = current_turn_id_for_explicit_send(state, session_id).await;
    // ---- reject unsupported voice early ----
    if req.voice {
        anyhow::bail!(
            "voice/tts send is not supported in this version of beam. \
             To send voice messages, upgrade to a TTS-capable build or use a separate tts tool."
        );
    }

    // ---- validate attention kind ----
    if let Some(ref kind) = req.attention {
        if !super::attention::VALID_ATTENTION_KINDS.contains(&kind.as_str()) {
            anyhow::bail!(
                "invalid attention kind \"{}\": must be one of {}",
                kind,
                super::attention::VALID_ATTENTION_KINDS.join("|")
            );
        }
    }

    // ---- validate attention usage constraints (botmux parity) ----
    if let Some(err) = super::attention::validate_attention_constraints(&req) {
        anyhow::bail!("{}", err);
    }

    // ---- validate mention policy ----
    let has_explicit_mentions = !req.mentions.is_empty();
    let mention_count = [has_explicit_mentions, req.mention_back, req.no_mention]
        .iter()
        .filter(|&&v| v)
        .count();

    // ---- backward compatibility: old { "content": "..." } requests ----
    let is_legacy_request = mention_count == 0
        && req.files.is_empty()
        && req.images.is_empty()
        && req.chat_id.is_none()
        && req.into.is_none()
        && req.quote.is_none()
        && !req.top_level
        && !req.no_quote
        && !req.voice
        && req.attention.is_none();
    if is_legacy_request {
        return deliver_final_output_once(state, session_id, &req.content, None, None, None).await;
    }

    if mention_count == 0 {
        anyhow::bail!(
            "no mention decision: you must choose exactly one of --mention-back, \
             --mention <open_id[:name]>, or --no-mention. \
             The daemon refuses messages without an explicit mention policy."
        );
    }
    if req.no_mention && mention_count > 1 {
        anyhow::bail!(
            "--no-mention cannot be combined with --mention or --mention-back. \
             Choose exactly one mention policy per send."
        );
    }

    // ---- fetch session / bot ----
    let (session, sessions_snapshot) = {
        let sessions = state.sessions.lock().await;
        let session = sessions
            .get(session_id)
            .cloned()
            .with_context(|| format!("session not found: {}", session_id))?;
        (session, sessions.clone())
    };
    if session.status == SessionStatus::Closed {
        // Refuse to deliver into a closed session's (possibly stale) topic.
        // This also surfaces a loud error to callers that resolved the wrong
        // session id (e.g. an inherited foreign BEAM_SESSION_ID).
        anyhow::bail!("session is closed: {}", session_id);
    }
    if session.lark_app_id == "local" {
        return Ok(()); // no-op for local sessions
    }
    let bot = state
        .bots
        .get(&session.lark_app_id)
        .cloned()
        .with_context(|| format!("bot not registered: {}", session.lark_app_id))?;

    // ---- resolve mention targets to open_ids ----
    let mut mention_open_ids: Vec<String> = Vec::new();

    if req.mention_back {
        let target = resolve_mention_back_target(&session)?;
        mention_open_ids.push(target);
    }

    for mt in &req.mentions {
        if mt.open_id.trim().is_empty() {
            anyhow::bail!("--mention open_id must not be empty");
        }
        mention_open_ids.push(mt.open_id.trim().to_string());
    }

    // ---- off-topic sub-bot hint (P2-9: informational, non-blocking) ----
    if let Some(hint) =
        off_topic_sub_bot_hint(&session, &mention_open_ids, &sessions_snapshot, req.anyway)
    {
        warn!("{}", hint);
    }

    // ---- build content with @-mentions ----
    let final_content = if req.no_mention {
        req.content.clone()
    } else {
        let observed_bots =
            load_observed_bots_for_chat(&state.paths, &session.lark_app_id, &session.chat_id);
        let self_open_id = load_self_bot_open_id_for_app(&state.paths, &session.lark_app_id);
        let body = auto_inject_bot_mentions(&req.content, &observed_bots, self_open_id.as_deref());

        if mention_open_ids.is_empty() {
            body
        } else {
            let at_prefix: String = mention_open_ids
                .iter()
                .map(|id| format!("<at user_id=\"{}\">@user</at> ", id))
                .collect();
            format!("{}{}", at_prefix, body)
        }
    };

    // ---- determine send target ----
    let (target_chat_id, target_message_id, reply_in_thread) = resolve_send_target(&session, &req)?;

    // ---- footer recipient: only when NOT --no-mention and owner is a real user ----
    let footer_recipient = if req.no_mention {
        None
    } else {
        final_output_footer_recipient_open_id(&state.paths, &session)
    };

    // ---- upload images to inline (partial failure is OK) ----
    let bot_ref = &bot;
    let mut image_keys: Vec<String> = Vec::new();
    let mut attachment_errors: Vec<String> = Vec::new();

    for image_path in &req.images {
        match super::attachments::upload_lark_image(state, bot_ref, image_path).await {
            Ok(key) => image_keys.push(key),
            Err(err) => {
                warn!("image upload failed for {}: {}", image_path, err);
                attachment_errors.push(format!("image {}: {}", image_path, err));
            }
        }
    }

    // ---- build card (with inlined images) ----
    let card_json = super::attachments::build_final_output_card_with_images(
        &final_content,
        footer_recipient.as_deref(),
        Some(FinalOutputKind::Bridge),
        None,
        session.cli_id.as_deref(),
        &image_keys,
    );

    // ---- deliver main card ----
    if let Some(ref target_msg_id) = target_message_id {
        // Check if there's a pending response card to patch
        let pending_card_id = {
            let sessions = state.sessions.lock().await;
            sessions
                .get(session_id)
                .and_then(super::pending::claim_pending_response_card)
        };
        if let Some(pending_card_id) = pending_card_id {
            // Patch existing pending card
            if let Err(err) = lark_update_card(state, bot_ref, &pending_card_id, &card_json).await {
                warn!("failed to patch pending card, falling back: {}", err);
                // Fall back to reply; if target was withdrawn, degrade to plain text
                reply_card_or_fallback_plain_on_withdrawn(
                    state,
                    bot_ref,
                    &target_chat_id,
                    target_msg_id,
                    &card_json,
                    &final_content,
                    reply_in_thread,
                )
                .await?;
            }
        } else {
            // Reply to the resolved message; if target was withdrawn, degrade to plain text
            reply_card_or_fallback_plain_on_withdrawn(
                state,
                bot_ref,
                &target_chat_id,
                target_msg_id,
                &card_json,
                &final_content,
                reply_in_thread,
            )
            .await?;
        }
    } else {
        // Send to chat directly
        let _ = lark_send_chat_message(state, bot_ref, &target_chat_id, &final_content).await?;
    }

    // ---- send file attachments (partial failure is OK) ----
    for file_path in &req.files {
        if let Err(err) =
            super::attachments::send_lark_file_message(state, bot_ref, &target_chat_id, file_path)
                .await
        {
            attachment_errors.push(format!("file {}: {}", file_path, err));
        }
    }

    if !attachment_errors.is_empty() {
        warn!(
            "final output sent with attachment errors for session {}: {:?}",
            session_id, attachment_errors
        );
    }

    // ---- set agent attention (after main message delivery succeeded) ----
    if let Some(ref kind) = req.attention {
        super::attention::set_session_attention(state, session_id, kind, &req.content).await?;
    }

    // Update session state. Passing the answered turn id marks this turn as
    // answered explicitly, so the worker's final output for the same turn is
    // skipped by turn-id dedupe even when its content differs from this send.
    commit_delivered_final_output(state, session_id, &req.content, answered_turn_id.as_deref())
        .await?;

    // Record explicit-send timestamp so worker final-output dedupe can skip
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id) {
                session.last_explicit_send_at = Some(Utc::now());
            }
            sessions.clone()
        };
        let _ = persist_sessions(&state.paths, &snapshot).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// deliver_final_output_once — single-delivery path (CORE INVARIANT)
// ---------------------------------------------------------------------------

/// Deliver final output once, handling pending response card patching.
///
/// This is the single-delivery path used by:
/// - Legacy backward-compatible sends
/// - Worker bridge delivery (via `schedule_final_output_delivery`)
/// - Auto-resume on daemon restart
///
/// # Streaming card isolation invariant
///
/// This function only patches a pending response card (streaming card) when:
/// 1. The card is claimed via `claim_pending_response_card` BEFORE any I/O
/// 2. At delivery time, the claim is re-validated via a second `claim_pending_response_card`
///    — if the claim was already consumed by another delivery, we fall back to
///    reply/chat send instead of overwriting the streaming card
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
            let pending_card_id = super::pending::claim_pending_response_card(session);
            (session.clone(), pending_card_id, sessions.clone())
        };
        persist_sessions(&state.paths, &sessions_snapshot).await?;
        (session_snapshot, pending_card_id)
    };

    if session.status == SessionStatus::Closed {
        // Never deliver into a closed session's (possibly stale) topic. The
        // retry scheduler already aborts on closed sessions; this guard covers
        // the direct call sites (legacy final-output requests, auto-resume).
        anyhow::bail!("session is closed: {}", session_id);
    }
    if session.lark_app_id == "local" {
        commit_delivered_final_output(state, session_id, content, turn_id).await?;
        return Ok(());
    }
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };

    let footer_recipient_open_id = final_output_footer_recipient_open_id(&state.paths, &session);
    let card_json = super::attachments::build_final_output_card(
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
                .and_then(super::pending::claim_pending_response_card)
                .as_deref()
                == Some(pending_card_id)
        };
        if still_current {
            super::pending::write_pending_response_patch_marker(
                &state.paths,
                session_id,
                pending_card_id,
            )
            .await?;
            match lark_update_card(state, bot, pending_card_id, &card_json).await {
                Ok(()) => {
                    super::pending::mark_pending_response_patch_marker_patched(
                        &state.paths,
                        session_id,
                    )
                    .await?;
                    let updated_session = {
                        let mut sessions = state.sessions.lock().await;
                        if let Some(entry) = sessions.get_mut(session_id) {
                            super::pending::mark_pending_response_card_patched_if_current(
                                entry,
                                pending_card_id,
                            );
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
                    super::pending::clear_pending_response_patch_marker(&state.paths, session_id)
                        .await?;
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
                    let _ = super::pending::clear_pending_response_patch_marker(
                        &state.paths,
                        session_id,
                    )
                    .await;
                    match fallback_reply().await {
                        Ok(()) => {
                            let snapshot = {
                                let mut sessions = state.sessions.lock().await;
                                if let Some(entry) = sessions.get_mut(session_id) {
                                    super::pending::mark_pending_response_card_patched_if_current(
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
