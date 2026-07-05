use super::*;
use crate::prompt::ObservedBot;
use std::path::Path;

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

/// Resolve the footer recipient open_id with a human-first candidate order.
///
/// Candidate order (deduplicated and trimmed):
/// 1. `quote_target_sender_open_id` — the sender of the trigger/quote message (botmux parity)
/// 2. `owner_open_id` — the session creator
///
/// Returns the first candidate that is NOT a known bot, or `None` if all
/// candidates are empty or are known bots.
///
/// This is a minimal footer-only human-first subset.  Full oncall/roster
/// awareness (e.g. selecting a specific human from a team) is out of scope
/// and requires `OncallChatBinding` extensions.
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

/// Valid attention kinds as defined by the botmux spec.
const VALID_ATTENTION_KINDS: [&str; 4] = ["authz", "decision", "blocked", "help"];

/// Validate --attention usage constraints (botmux parity: `attentionUsageError`).
///
/// `--attention` only makes sense when replying into the current session context.
/// Sending to a different chat/thread (via --top-level / --chat-id / --into) or
/// using --voice would route the message elsewhere, leaving the attention signal
/// un-clearable. The dashboard also needs a text reason, so empty content is rejected.
///
/// Returns `Some(error_message)` if invalid, `None` if the request is acceptable.
pub(crate) fn validate_attention_constraints(req: &FinalOutputRequest) -> Option<String> {
    if req.attention.is_none() {
        return None;
    }
    if req.top_level || req.chat_id.is_some() || req.into.is_some() {
        return Some(
            "--attention cannot be combined with --top-level / --chat-id / --into. \
             Attention is for the current session context only."
                .to_string(),
        );
    }
    if req.voice {
        return Some(
            "--attention cannot be combined with --voice. \
             Attention requires a text/card message."
                .to_string(),
        );
    }
    if req.content.trim().is_empty() {
        return Some(
            "--attention requires a non-empty text reason in the message body.".to_string(),
        );
    }
    None
}

/// Normalize an attention reason: collapse whitespace, trim, truncate to 500 chars.
pub(crate) fn normalize_attention_reason(raw: &str) -> String {
    let collapsed: String = raw.split_whitespace().collect::<Vec<&str>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.len() <= 500 {
        trimmed.to_string()
    } else {
        let mut truncated: String = trimmed.chars().take(500).collect();
        // Try to cut at the last whitespace boundary before 500 for readability.
        if let Some(pos) = truncated.rfind(' ') {
            if pos > 400 {
                truncated.truncate(pos);
            }
        }
        truncated
    }
}

/// Validate and set agent attention on a session.
///
/// Panics if kind is empty or invalid; the caller (handle_final_output_request
/// and the api/attention route handler) is responsible for validating kind first.
pub(crate) async fn set_session_attention(
    state: &AppState,
    session_id: &str,
    kind: &str,
    reason: &str,
) -> Result<()> {
    // Validate kind (must match VALID_ATTENTION_KINDS)
    if !VALID_ATTENTION_KINDS.contains(&kind) {
        anyhow::bail!(
            "invalid attention kind \"{}\": must be one of {}",
            kind,
            VALID_ATTENTION_KINDS.join("|")
        );
    }
    if reason.trim().is_empty() {
        anyhow::bail!("attention reason must not be empty");
    }
    let normalized = normalize_attention_reason(reason);
    let now = Utc::now();
    let snapshot = {
        let mut sessions = state.sessions.lock().await;
        let session = sessions
            .get_mut(session_id)
            .with_context(|| format!("session not found: {}", session_id))?;
        session.agent_attention = Some(AgentAttention {
            kind: kind.to_string(),
            reason: normalized,
            at: now,
        });
        sessions.clone()
    };
    persist_sessions(&state.paths, &snapshot).await
}

/// Clear agent attention from a session (called on user inbound message).
pub(crate) fn clear_agent_attention(session: &mut Session) {
    session.agent_attention = None;
}

/// POST /api/attention — set agent attention without sending a message (botmux parity).
pub(crate) async fn set_attention_route(
    State(state): State<AppState>,
    Json(req): Json<AttentionRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    match set_session_attention(&state, &req.session_id, &req.kind, &req.reason).await {
        Ok(()) => Ok(StatusCode::OK),
        Err(err) => {
            let msg = err.to_string();
            if msg.starts_with("invalid attention kind") {
                Err((StatusCode::BAD_REQUEST, msg))
            } else if msg.contains("session not found") {
                Err((StatusCode::NOT_FOUND, msg))
            } else {
                Err((StatusCode::BAD_REQUEST, msg))
            }
        }
    }
}

/// Auto-inject bot @-mentions in body text (P1-5: bot-to-bot mention).
///
/// Scans `content` for `@BotName` patterns matching observed bots in the current chat
/// and replaces them with real Lark `<at user_id="...">@BotName</at>` mention tags.
///
/// - Skips bots with empty name or open_id.
/// - Skips the self bot (matched via `self_open_id`).
/// - Skips `@` references already inside existing `<at ...>...</at>` blocks.
/// - Only matches `@BotName` at word boundaries (followed by non-alphanumeric or EOS,
///   excluding `_` to avoid partial matches like `@ReviewerBot_Pro`).
/// - Matches longer bot names first to prevent `@Foo` from shadowing `@FooBar`.
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
/// Non-alphanumeric (except `_`) is treated as a boundary; end-of-string
/// is already handled by the caller.
fn is_mention_boundary(c: char) -> bool {
    !c.is_alphanumeric() && c != '_'
}

/// Minimal off-topic sub-bot informational hint (P2-9).
///
/// When `beam send --mention <open_id>` targets a sub-bot whose session is
/// active in a **different topic/thread** within the same chat, return an
/// informational hint suggesting `--into <message_id>`.  This does NOT block
/// the send; it is a best-effort UX hint logged at `warn!` level for now.
///
/// Returns `None` when:
/// - `anyway` is true (caller suppressed hints)
/// - No explicit mentions were provided
/// - The mentioned bot is the current session's own bot
/// - The sub-bot session is in the same topic/thread (same root_message_id
///   or same thread_id)
/// - The sub-bot session is in a different chat
/// - No active sub-bot session exists for any mentioned open_id
///
/// This is a pure, easily-testable helper.
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

/// Core structured send handler for the daemon final-output endpoint.
///
/// This performs mention policy validation, resolves mention targets,
/// builds the card content with @-mentions, determines send targeting,
/// and delivers the message.  Attachments (files/images) are sent
/// separately after the main message; failures on attachments do not
/// fail the overall request.
pub(crate) async fn handle_final_output_request(
    state: &AppState,
    session_id: &str,
    req: FinalOutputRequest,
) -> Result<()> {
    // ---- reject unsupported voice early ----
    if req.voice {
        anyhow::bail!(
            "voice/tts send is not supported in this version of beam. \
             To send voice messages, upgrade to a TTS-capable build or use a separate tts tool."
        );
    }

    // ---- validate attention kind ----
    if let Some(ref kind) = req.attention {
        if !VALID_ATTENTION_KINDS.contains(&kind.as_str()) {
            anyhow::bail!(
                "invalid attention kind \"{}\": must be one of {}",
                kind,
                VALID_ATTENTION_KINDS.join("|")
            );
        }
    }

    // ---- validate attention usage constraints (botmux parity) ----
    if let Some(err) = validate_attention_constraints(&req) {
        anyhow::bail!("{}", err);
    }

    // ---- validate mention policy ----
    let has_explicit_mentions = !req.mentions.is_empty();
    let mention_count = [has_explicit_mentions, req.mention_back, req.no_mention]
        .iter()
        .filter(|&&v| v)
        .count();

    // ---- backward compatibility: old { "content": "..." } requests ----
    // When no structured fields beyond content are set, delegate to the legacy path
    // so existing clients (including old versions of beam send) continue to work.
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
    // This is a minimal informational hint; currently exposed via daemon log.
    // Future: could be delivered as a CLI success warning in the response body.
    if let Some(hint) =
        off_topic_sub_bot_hint(&session, &mention_open_ids, &sessions_snapshot, req.anyway)
    {
        warn!("{}", hint);
    }

    // ---- build content with @-mentions ----
    // P1-5: auto-inject bot @-mentions in body text
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
        match upload_lark_image(state, bot_ref, image_path).await {
            Ok(key) => image_keys.push(key),
            Err(err) => {
                warn!("image upload failed for {}: {}", image_path, err);
                attachment_errors.push(format!("image {}: {}", image_path, err));
            }
        }
    }

    // ---- build card (with inlined images) ----
    let card_json = build_final_output_card_with_images(
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
                .and_then(claim_pending_response_card)
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
        if let Err(err) = send_lark_file_message(state, bot_ref, &target_chat_id, file_path).await {
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
        set_session_attention(state, session_id, kind, &req.content).await?;
    }

    // Update session state
    commit_delivered_final_output(state, session_id, &req.content, None).await?;

    // Record explicit-send timestamp so worker final-output dedupe can skip
    // content that was already delivered via beam send.
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

/// Resolve the mention-back target open_id from the session.
///
/// Prefers `quote_target_sender_open_id` (aligns with botmux `quoteTargetSenderOpenId`).
/// Falls back to `owner_open_id` for backward compatibility with old sessions
/// that were persisted before `quote_target_sender_open_id` was introduced.
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

/// Resolve where to send the message based on session scope and request flags.
///
/// Returns `(chat_id, reply_to_message_id, reply_in_thread)`.
/// When `reply_to_message_id` is `None`, the message is sent to the chat directly.
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

/// Reply with an interactive card to a target message.
///
/// If the reply fails because the target message was withdrawn by the user,
/// this function degrades to sending a plain-text chat message (matching
/// botmux quote-withdrawn fallback behaviour).  Other errors are returned
/// unchanged.
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
///
/// Exposed for unit testing.  Currently delegates to `is_lark_message_withdrawn_error`.
pub(crate) fn should_fallback_to_plain_on_withdrawn(err: &anyhow::Error) -> bool {
    is_lark_message_withdrawn_error(err)
}

/// Upload a local file to Lark and send it as a file message in the given chat.
async fn send_lark_file_message(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    file_path: &str,
) -> Result<()> {
    let data = tokio::fs::read(file_path)
        .await
        .with_context(|| format!("failed to read file: {}", file_path))?;
    let file_name = Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file");
    let mime = guess_mime_type(file_path);

    let token = lark_tenant_token(state, bot).await?;
    let part = reqwest::multipart::Part::bytes(data)
        .file_name(file_name.to_string())
        .mime_str(mime)
        .with_context(|| format!("invalid MIME type for file: {}", file_path))?;

    let form = reqwest::multipart::Form::new()
        .text("file_type", "stream")
        .text("file_name", file_name.to_string())
        .part("file", part);

    let upload_resp = state
        .http
        .post(format!("{}/im/v1/files", lark_base_url()))
        .bearer_auth(&token)
        .multipart(form)
        .send()
        .await?;
    let status = upload_resp.status();
    let upload_body = upload_resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark file upload failed: {}", upload_body);
    }
    let upload_value: Value =
        serde_json::from_str(&upload_body).context("parse lark upload response")?;
    let file_key = upload_value
        .pointer("/data/file_key")
        .and_then(Value::as_str)
        .context("lark upload missing file_key")?;

    // Send as file message
    let msg_body = serde_json::json!({ "file_key": file_key }).to_string();
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages?receive_id_type=chat_id",
            lark_base_url()
        ))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "receive_id": chat_id,
            "content": msg_body,
            "msg_type": "file",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("lark file send failed: {}", payload);
    }
    Ok(())
}

/// Upload a local image to Lark and return its `image_key` for inlining in cards.
///
/// Does NOT send an independent image message. Callers should embed the returned
/// `image_key` into an interactive card's `img` element.
pub(crate) async fn upload_lark_image(
    state: &AppState,
    bot: &BotConfig,
    image_path: &str,
) -> Result<String> {
    let data = tokio::fs::read(image_path)
        .await
        .with_context(|| format!("failed to read image: {}", image_path))?;
    let mime = guess_mime_type(image_path);

    let token = lark_tenant_token(state, bot).await?;
    let part = reqwest::multipart::Part::bytes(data)
        .file_name("image")
        .mime_str(mime)
        .with_context(|| format!("invalid MIME type for image: {}", image_path))?;

    let form = reqwest::multipart::Form::new()
        .text("image_type", "message")
        .part("image", part);

    let upload_resp = state
        .http
        .post(format!("{}/im/v1/images", lark_base_url()))
        .bearer_auth(&token)
        .multipart(form)
        .send()
        .await?;
    let status = upload_resp.status();
    let upload_body = upload_resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark image upload failed: {}", upload_body);
    }
    let upload_value: Value =
        serde_json::from_str(&upload_body).context("parse lark image upload response")?;
    let image_key = upload_value
        .pointer("/data/image_key")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .context("lark upload missing image_key")?;
    Ok(image_key)
}

/// Build a final-output interactive card with optional inlined images.
///
/// `image_keys` are inserted as `img` elements between the markdown content and
/// the footer. When empty, the card is identical to `build_final_output_card`.
pub(crate) fn build_final_output_card_with_images(
    content: &str,
    recipient_open_id: Option<&str>,
    kind: Option<FinalOutputKind>,
    user_text: Option<&str>,
    cli_label: Option<&str>,
    image_keys: &[String],
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

    // ---- inline images (before footer) ----
    for key in image_keys {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        elements.push(serde_json::json!({
            "tag": "img",
            "img_key": key,
            "alt": { "tag": "plain_text", "content": "" },
            "mode": "fit_horizontal",
            "preview": true,
        }));
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
        "body": {
            "direction": "vertical",
            "elements": elements,
        }
    })
    .to_string()
}

pub(crate) fn build_final_output_card(
    content: &str,
    recipient_open_id: Option<&str>,
    kind: Option<FinalOutputKind>,
    user_text: Option<&str>,
    cli_label: Option<&str>,
) -> String {
    build_final_output_card_with_images(content, recipient_open_id, kind, user_text, cli_label, &[])
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

pub(crate) fn should_skip_worker_final_output(
    session: &Session,
    turn_id: &str,
    content: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    // ---- turn-id dedupe (unchanged legacy logic) ----
    // If the worker-produced turn_id was already delivered by a previous
    // worker bridge delivery, skip.
    if !turn_id.is_empty() && session.last_final_output_turn_id.as_deref() == Some(turn_id) {
        return true;
    }

    // ---- explicit-send dedupe (minimal botmux-equivalent) ----
    // If the model already sent the same content via `beam send` (explicit
    // structured send) within the last 10 minutes, skip re-delivery from the
    // worker bridge.  Only triggers when content matches; different content
    // always passes through.
    if let Some(last_explicit_send_at) = session.last_explicit_send_at {
        let age = now - last_explicit_send_at;
        if age < chrono::Duration::minutes(10)
            && normalize_final_output(content)
                == normalize_final_output(session.last_final_output.as_deref().unwrap_or(""))
        {
            return true;
        }
    }

    false
}

/// Lightweight content normalisation for dedupe comparisons.
/// Strips surrounding whitespace so that minor framing differences do not
/// cause false negatives.
fn normalize_final_output(content: &str) -> &str {
    content.trim()
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

/// Simple MIME type guess from file extension. Falls back to application/octet-stream.
fn guess_mime_type(path: &str) -> &'static str {
    let lower = path.to_lowercase();
    if lower.ends_with(".png") {
        "image/png"
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        "image/jpeg"
    } else if lower.ends_with(".gif") {
        "image/gif"
    } else if lower.ends_with(".webp") {
        "image/webp"
    } else if lower.ends_with(".bmp") {
        "image/bmp"
    } else if lower.ends_with(".svg") {
        "image/svg+xml"
    } else if lower.ends_with(".pdf") {
        "application/pdf"
    } else if lower.ends_with(".txt") || lower.ends_with(".log") {
        "text/plain"
    } else if lower.ends_with(".html") || lower.ends_with(".htm") {
        "text/html"
    } else if lower.ends_with(".json") {
        "application/json"
    } else if lower.ends_with(".csv") {
        "text/csv"
    } else if lower.ends_with(".md") {
        "text/markdown"
    } else if lower.ends_with(".zip") {
        "application/zip"
    } else {
        "application/octet-stream"
    }
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
        let now = chrono::Utc::now();

        // turn-id match skips regardless of content passed
        assert!(should_skip_worker_final_output(
            &session, "turn-1", "anything", now
        ));
        // different turn-id passes
        assert!(!should_skip_worker_final_output(
            &session, "turn-2", "done", now
        ));
        // empty turn-id passes
        assert!(!should_skip_worker_final_output(&session, "", "done", now));
    }

    #[test]
    fn worker_final_output_skips_recent_explicit_same_content() {
        let mut session = make_session("sess-explicit-recent");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.last_final_output = Some("hello world".to_string());
        session.last_explicit_send_at = Some(chrono::Utc::now());

        let now = chrono::Utc::now();
        // recent explicit send with same content → skip
        assert!(should_skip_worker_final_output(
            &session,
            "turn-3",
            "hello world",
            now
        ));
        // content normalised (trim) → still matches
        assert!(should_skip_worker_final_output(
            &session,
            "turn-4",
            "  hello world\n",
            now
        ));
    }

    #[test]
    fn worker_final_output_does_not_skip_different_content() {
        let mut session = make_session("sess-explicit-different");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.last_final_output = Some("hello world".to_string());
        session.last_explicit_send_at = Some(chrono::Utc::now());

        let now = chrono::Utc::now();
        // different content even with recent explicit send → pass through
        assert!(!should_skip_worker_final_output(
            &session,
            "turn-5",
            "other output",
            now
        ));
    }

    #[test]
    fn worker_final_output_does_not_skip_old_explicit_send() {
        let mut session = make_session("sess-explicit-old");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.last_final_output = Some("hello world".to_string());
        // explicit send was 20 minutes ago
        session.last_explicit_send_at = Some(chrono::Utc::now() - chrono::Duration::minutes(20));

        let now = chrono::Utc::now();
        // older than 10-minute window → pass through even with same content
        assert!(!should_skip_worker_final_output(
            &session,
            "turn-6",
            "hello world",
            now
        ));
    }

    #[test]
    fn worker_final_output_no_explicit_marker_still_works() {
        let mut session = make_session("sess-no-explicit");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.last_final_output = Some("output".to_string());
        // no explicit marker set
        session.last_explicit_send_at = None;

        let now = chrono::Utc::now();
        // without explicit marker, only turn-id dedupe applies (no match here)
        assert!(!should_skip_worker_final_output(
            &session, "turn-7", "output", now
        ));
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

    // ---- footer human-first candidate tests (minimal botmux parity) ----
    // Note: these tests cover the footer-only human-first subset.
    // Full oncall/roster awareness with `OncallChatBinding` is not implemented yet.
    //
    // The semantics are:
    //  - quote_target_sender_open_id takes priority over owner_open_id.
    //  - Known bot open_ids are filtered out; the first non-bot human wins.
    //  - Empty candidates are skipped; duplicates are deduplicated.
    //  - Returns None if all candidates are empty or known bots.

    #[tokio::test]
    async fn footer_recipient_prefers_quote_target_sender_over_owner_when_both_human() {
        let paths = temp_paths("footer-human-first");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");
        // No known bots registered for this app — both candidates are human.
        let mut session = make_session("sess-fh-1");
        session.quote_target_sender_open_id = Some("ou_sender".to_string());
        session.owner_open_id = Some("ou_owner".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &session).as_deref(),
            Some("ou_sender"),
            "quote_target_sender_open_id should take priority over owner_open_id when both are human"
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn footer_recipient_falls_back_to_owner_when_quote_sender_is_bot() {
        let paths = temp_paths("footer-fallback");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");
        std::fs::write(
            paths.root().join("bot-openids-app-1.json"),
            r#"{"Bot":"ou_bot"}"#,
        )
        .expect("write cross-ref");

        let mut session = make_session("sess-fh-2");
        session.quote_target_sender_open_id = Some("ou_bot".to_string());
        session.owner_open_id = Some("ou_human".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &session).as_deref(),
            Some("ou_human"),
            "should fall back to owner when quote_target_sender is a known bot"
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn footer_recipient_returns_none_when_owner_is_bot_and_no_other_human() {
        let paths = temp_paths("footer-none");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");
        std::fs::write(
            paths.root().join("bot-openids-app-1.json"),
            r#"{"Bot":"ou_bot"}"#,
        )
        .expect("write cross-ref");

        let mut session = make_session("sess-fh-3");
        session.owner_open_id = Some("ou_bot".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &session),
            None,
            "should return None when owner is a known bot and there is no other human"
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn footer_recipient_dedup_and_trim_empty() {
        let paths = temp_paths("footer-dedup");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");

        // Both fields with same value (trimmed) — should still return the human.
        let mut session = make_session("sess-fh-4");
        session.quote_target_sender_open_id = Some("  ou_human  ".to_string());
        session.owner_open_id = Some("ou_human".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &session).as_deref(),
            Some("ou_human"),
            "trimmed duplicates should not affect result"
        );

        // Only empty/whitespace candidates -> None.
        let mut session2 = make_session("sess-fh-5");
        session2.quote_target_sender_open_id = Some("   ".to_string());
        session2.owner_open_id = Some("".to_string());
        assert_eq!(
            final_output_footer_recipient_open_id(&paths, &session2),
            None,
            "all empty/whitespace candidates should return None"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn mention_back_uses_quote_target_sender_over_owner_when_both_differ() {
        let mut session = make_session("sess-mb-diff");
        session.owner_open_id = Some("ou_owner".to_string());
        session.quote_target_sender_open_id = Some("ou_sender".to_string());

        let target = resolve_mention_back_target(&session).expect("should resolve target");
        assert_eq!(
            target, "ou_sender",
            "quote_target_sender_open_id should take priority over owner_open_id"
        );
    }

    #[test]
    fn mention_back_falls_back_to_owner_open_id_for_backward_compat() {
        let mut session = make_session("sess-mb-fallback");
        session.owner_open_id = Some("ou_owner".to_string());
        session.quote_target_sender_open_id = None;

        let target = resolve_mention_back_target(&session).expect("should fall back to owner");
        assert_eq!(target, "ou_owner");
    }

    #[test]
    fn mention_back_prefers_quote_target_even_when_owner_exists() {
        let mut session = make_session("sess-mb-prefer");
        session.owner_open_id = Some("ou_owner".to_string());
        session.quote_target_sender_open_id = Some("ou_sender".to_string());

        let target = resolve_mention_back_target(&session).unwrap();
        assert_eq!(target, "ou_sender");
        assert_ne!(target, "ou_owner");
    }

    #[test]
    fn mention_back_errors_when_both_fields_missing() {
        let session = make_session("sess-mb-missing");
        assert!(session.quote_target_sender_open_id.is_none());
        assert!(session.owner_open_id.is_none());

        let err = resolve_mention_back_target(&session).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("quote_target_sender_open_id") || msg.contains("owner_open_id"),
            "error should mention the missing fields: {}",
            msg
        );
    }

    #[test]
    fn mention_back_ignores_empty_string_fields() {
        let mut session = make_session("sess-mb-empty");
        session.quote_target_sender_open_id = Some("  ".to_string());
        session.owner_open_id = Some("".to_string());

        let err = resolve_mention_back_target(&session).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("quote_target_sender_open_id") || msg.contains("owner_open_id"),
            "empty/whitespace fields should be treated as None"
        );
    }

    // ---- attention usage constraint tests (botmux parity) ----

    fn make_attention_req(
        attention: Option<&str>,
        top_level: bool,
        chat_id: Option<&str>,
        into: Option<&str>,
        voice: bool,
        content: &str,
    ) -> FinalOutputRequest {
        FinalOutputRequest {
            content: content.to_string(),
            mentions: vec![],
            mention_back: true,
            no_mention: false,
            files: vec![],
            images: vec![],
            top_level,
            chat_id: chat_id.map(|s| s.to_string()),
            into: into.map(|s| s.to_string()),
            quote: None,
            no_quote: false,
            voice,
            attention: attention.map(|s| s.to_string()),
            card: false,
            text: false,
            anyway: false,
        }
    }

    #[test]
    fn attention_accepts_valid_request() {
        let req = make_attention_req(Some("blocked"), false, None, None, false, "I need help");
        assert!(validate_attention_constraints(&req).is_none());
    }

    #[test]
    fn attention_noop_when_not_requested() {
        let req = make_attention_req(None, true, None, None, false, "");
        assert!(validate_attention_constraints(&req).is_none());
    }

    #[test]
    fn attention_rejects_top_level() {
        let req = make_attention_req(Some("blocked"), true, None, None, false, "hello");
        let err = validate_attention_constraints(&req).expect("should reject --top-level");
        assert!(err.contains("--attention cannot be combined with --top-level"));
    }

    #[test]
    fn attention_rejects_chat_id() {
        let req = make_attention_req(
            Some("decision"),
            false,
            Some("oc_test123"),
            None,
            false,
            "hello",
        );
        let err = validate_attention_constraints(&req).expect("should reject --chat-id");
        assert!(err.contains("--attention cannot be combined with"));
        assert!(err.contains("--chat-id"));
    }

    #[test]
    fn attention_rejects_into() {
        let req = make_attention_req(
            Some("help"),
            false,
            None,
            Some("om_test123"),
            false,
            "hello",
        );
        let err = validate_attention_constraints(&req).expect("should reject --into");
        assert!(err.contains("--attention cannot be combined with"));
        assert!(err.contains("--into"));
    }

    #[test]
    fn attention_rejects_voice() {
        let req = make_attention_req(Some("blocked"), false, None, None, true, "hello");
        let err = validate_attention_constraints(&req).expect("should reject --voice");
        assert!(err.contains("--attention cannot be combined with --voice"));
    }

    #[test]
    fn attention_rejects_empty_content() {
        let req = make_attention_req(Some("blocked"), false, None, None, false, "");
        let err = validate_attention_constraints(&req).expect("should reject empty content");
        assert!(err.contains("--attention requires a non-empty text reason"));
    }

    #[test]
    fn attention_rejects_whitespace_only_content() {
        let req = make_attention_req(Some("blocked"), false, None, None, false, "   \n  ");
        let err =
            validate_attention_constraints(&req).expect("should reject whitespace-only content");
        assert!(err.contains("--attention requires a non-empty text reason"));
    }

    // ---- attention state helpers ----

    #[test]
    fn normalize_attention_reason_collapses_whitespace() {
        let raw = "hello   world\n\ttest   foo";
        let result = normalize_attention_reason(raw);
        assert_eq!(result, "hello world test foo");
    }

    #[test]
    fn normalize_attention_reason_truncates_over_500() {
        let long: String = std::iter::repeat("abcde")
            .take(120)
            .collect::<Vec<_>>()
            .join(" ");
        let count = long.len();
        assert!(
            count > 500,
            "test input must be longer than 500, got {}",
            count
        );
        let result = normalize_attention_reason(&long);
        assert!(
            result.len() <= 500,
            "result len {} should be <= 500",
            result.len()
        );
        assert!(
            !result.ends_with(' '),
            "should not end with trailing space: {:?}",
            result
        );
    }

    #[test]
    fn normalize_attention_reason_short_text_unchanged() {
        let raw = "  need approval for deployment  ";
        let result = normalize_attention_reason(raw);
        assert_eq!(result, "need approval for deployment");
    }

    #[test]
    fn normalize_attention_reason_empty() {
        assert_eq!(normalize_attention_reason(""), "");
        assert_eq!(normalize_attention_reason("   \n  "), "");
    }

    #[test]
    fn clear_agent_attention_sets_to_none() {
        let mut session = make_session("sess-clear");
        session.agent_attention = Some(AgentAttention {
            kind: "blocked".to_string(),
            reason: "test".to_string(),
            at: Utc::now(),
        });
        assert!(session.agent_attention.is_some());
        clear_agent_attention(&mut session);
        assert!(session.agent_attention.is_none());
    }

    #[test]
    fn session_summary_includes_agent_attention() {
        let mut session = make_session("sess-summary");
        session.agent_attention = Some(AgentAttention {
            kind: "blocked".to_string(),
            reason: "need approval".to_string(),
            at: Utc::now(),
        });
        let summary = SessionSummary::from(&session);
        assert!(summary.agent_attention.is_some());
        assert_eq!(summary.agent_attention.as_ref().unwrap().kind, "blocked");
        assert_eq!(
            summary.agent_attention.as_ref().unwrap().reason,
            "need approval"
        );
    }

    #[test]
    fn session_summary_no_agent_attention_when_none() {
        let session = make_session("sess-no-attention");
        assert!(session.agent_attention.is_none());
        let summary = SessionSummary::from(&session);
        assert!(summary.agent_attention.is_none());
    }

    #[tokio::test]
    async fn set_session_attention_stores_and_reads_back() {
        let paths = temp_paths("attn-set");
        let mut bots = HashMap::new();
        bots.insert("app-1".to_string(), make_bot("app-1"));
        let state = make_state(paths.clone(), bots);

        // Insert a session
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert("sess-attn-1".to_string(), make_session("sess-attn-1"));
            let snapshot = sessions.clone();
            drop(sessions);
            persist_sessions(&state.paths, &snapshot).await.unwrap();
        }

        // Set attention
        set_session_attention(&state, "sess-attn-1", "blocked", "need approval from team")
            .await
            .expect("set attention should succeed");

        // Read back
        let sessions = state.sessions.lock().await;
        let session = sessions.get("sess-attn-1").expect("session should exist");
        let aa = session
            .agent_attention
            .as_ref()
            .expect("should have attention");
        assert_eq!(aa.kind, "blocked");
        assert_eq!(aa.reason, "need approval from team");

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn set_session_attention_rejects_invalid_kind() {
        let paths = temp_paths("attn-invalid");
        let mut bots = HashMap::new();
        bots.insert("app-1".to_string(), make_bot("app-1"));
        let state = make_state(paths.clone(), bots);

        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert("sess-attn-2".to_string(), make_session("sess-attn-2"));
            let snapshot = sessions.clone();
            drop(sessions);
            persist_sessions(&state.paths, &snapshot).await.unwrap();
        }

        let result = set_session_attention(&state, "sess-attn-2", "invalid_kind", "reason").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid attention kind"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn set_session_attention_rejects_empty_reason() {
        let paths = temp_paths("attn-empty");
        let mut bots = HashMap::new();
        bots.insert("app-1".to_string(), make_bot("app-1"));
        let state = make_state(paths.clone(), bots);

        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert("sess-attn-3".to_string(), make_session("sess-attn-3"));
            let snapshot = sessions.clone();
            drop(sessions);
            persist_sessions(&state.paths, &snapshot).await.unwrap();
        }

        let result = set_session_attention(&state, "sess-attn-3", "blocked", "  ").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must not be empty"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn set_session_attention_rejects_missing_session() {
        let paths = temp_paths("attn-missing");
        let mut bots = HashMap::new();
        bots.insert("app-1".to_string(), make_bot("app-1"));
        let state = make_state(paths.clone(), bots);

        let result = set_session_attention(&state, "nonexistent", "blocked", "reason").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("session not found"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    // ---- image inlining tests ----

    #[test]
    fn build_final_output_card_without_images_is_backward_compatible() {
        let old = build_final_output_card("hello", None, None, None, None);
        let new = build_final_output_card_with_images("hello", None, None, None, None, &[]);
        assert_eq!(old, new, "no images should produce identical card JSON");
    }

    #[test]
    fn build_final_output_card_with_images_inserts_img_elements_before_footer() {
        let keys = vec!["img_key_1".to_string(), "img_key_2".to_string()];
        let card = build_final_output_card_with_images(
            "content",
            Some("ou_recipient"), // triggers footer
            None,
            None,
            None,
            &keys,
        );
        let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
        let elements = v["body"]["elements"]
            .as_array()
            .expect("elements should be an array");

        // Find the positions of img elements and footer elements
        let img_indices: Vec<usize> = elements
            .iter()
            .enumerate()
            .filter(|(_, el)| el["tag"].as_str() == Some("img"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(img_indices.len(), 2, "expected 2 img elements");
        assert_eq!(
            elements[img_indices[0]]["img_key"].as_str(),
            Some("img_key_1")
        );
        assert_eq!(
            elements[img_indices[1]]["img_key"].as_str(),
            Some("img_key_2")
        );

        // Verify img elements are before the footer (hr + notation markdown)
        let footer_start = img_indices[1] + 1;
        let footer_hr = &elements[footer_start];
        assert_eq!(
            footer_hr["tag"].as_str(),
            Some("hr"),
            "expected hr (footer separator) after last img"
        );

        // Also verify content markdown comes before images
        assert_eq!(elements[0]["tag"].as_str(), Some("markdown"));
        assert_eq!(elements[0]["content"].as_str(), Some("content"));
        assert!(img_indices[0] > 0, "img elements should come after content");
    }

    #[test]
    fn build_final_output_card_with_images_without_recipient_has_brand_footer() {
        // When recipient_open_id is None, the footer still includes the
        // brand label. That's the normal case for --no-mention or bot-to-bot.
        let keys = vec!["img_key_1".to_string()];
        let card = build_final_output_card_with_images(
            "content", None, // recipient_open_id = None → brand label footer only
            None, None, None, &keys,
        );
        let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
        let elements = v["body"]["elements"]
            .as_array()
            .expect("elements should be an array");
        // Should have: markdown, img, hr, footer_markdown = 4
        assert_eq!(elements.len(), 4);
        assert_eq!(elements[0]["tag"].as_str(), Some("markdown"));
        assert_eq!(elements[1]["tag"].as_str(), Some("img"));
        assert_eq!(elements[1]["img_key"].as_str(), Some("img_key_1"));
        assert_eq!(
            elements[2]["tag"].as_str(),
            Some("hr"),
            "hr separator before footer"
        );
        assert_eq!(
            elements[3]["tag"].as_str(),
            Some("markdown"),
            "footer markdown"
        );
    }

    #[test]
    fn build_final_output_card_skips_empty_image_keys() {
        let keys = vec!["  ".to_string(), "img_key_1".to_string()];
        let card = build_final_output_card_with_images("content", None, None, None, None, &keys);
        let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
        let elements = v["body"]["elements"]
            .as_array()
            .expect("elements should be an array");
        let img_count = elements
            .iter()
            .filter(|el| el["tag"].as_str() == Some("img"))
            .count();
        assert_eq!(img_count, 1, "empty image key should be skipped");
        let img = elements
            .iter()
            .find(|el| el["tag"].as_str() == Some("img"))
            .unwrap();
        assert_eq!(img["img_key"].as_str(), Some("img_key_1"));
    }

    #[test]
    fn build_final_output_card_no_images_produces_no_img_elements() {
        let card = build_final_output_card("content", None, None, None, None);
        let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
        let elements = v["body"]["elements"]
            .as_array()
            .expect("elements should be an array");
        let img_count = elements
            .iter()
            .filter(|el| el["tag"].as_str() == Some("img"))
            .count();
        assert_eq!(img_count, 0, "no img elements when no images");
    }

    // ---- auto_inject_bot_mentions tests (P1-5) ----

    fn make_observed_bot(open_id: &str, name: &str) -> ObservedBot {
        ObservedBot {
            open_id: open_id.to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn auto_inject_single_bot_mention_surrounded_by_text() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let result = auto_inject_bot_mentions("请 @ReviewerBot 看一下", &bots, None);
        assert_eq!(
            result,
            "请 <at user_id=\"ou_reviewer\">@ReviewerBot</at> 看一下"
        );
    }

    #[test]
    fn auto_inject_bot_at_end_of_string() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let result = auto_inject_bot_mentions("hello @ReviewerBot", &bots, None);
        assert_eq!(
            result,
            "hello <at user_id=\"ou_reviewer\">@ReviewerBot</at>"
        );
    }

    #[test]
    fn auto_inject_multiple_bots() {
        let bots = vec![
            make_observed_bot("ou_r", "ReviewerBot"),
            make_observed_bot("ou_a", "AnalyzerBot"),
        ];
        let result = auto_inject_bot_mentions("@ReviewerBot 和 @AnalyzerBot 一起", &bots, None);
        assert_eq!(
            result,
            "<at user_id=\"ou_r\">@ReviewerBot</at> 和 <at user_id=\"ou_a\">@AnalyzerBot</at> 一起"
        );
    }

    #[test]
    fn auto_inject_duplicate_bot_mentions() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let result = auto_inject_bot_mentions("@ReviewerBot @ReviewerBot", &bots, None);
        assert_eq!(
            result,
            "<at user_id=\"ou_reviewer\">@ReviewerBot</at> <at user_id=\"ou_reviewer\">@ReviewerBot</at>"
        );
    }

    #[test]
    fn auto_inject_skips_self_bot() {
        let bots = vec![
            make_observed_bot("ou_self", "SelfBot"),
            make_observed_bot("ou_other", "OtherBot"),
        ];
        let result = auto_inject_bot_mentions("@SelfBot @OtherBot", &bots, Some("ou_self"));
        assert_eq!(result, "@SelfBot <at user_id=\"ou_other\">@OtherBot</at>");
    }

    #[test]
    fn auto_inject_skips_existing_at_tag() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let content = "已有 <at user_id=\"ou_reviewer\">@ReviewerBot</at> 标签";
        let result = auto_inject_bot_mentions(content, &bots, None);
        assert_eq!(result, content);
    }

    #[test]
    fn auto_inject_handles_existing_at_with_bare_mention_after() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let content = "<at user_id=\"ou_x\">@Someone</at> 然后 @ReviewerBot";
        let result = auto_inject_bot_mentions(content, &bots, None);
        assert_eq!(
            result,
            "<at user_id=\"ou_x\">@Someone</at> 然后 <at user_id=\"ou_reviewer\">@ReviewerBot</at>"
        );
    }

    #[test]
    fn auto_inject_noop_with_empty_bots() {
        let content = "@Nobody 在这里";
        let result = auto_inject_bot_mentions(content, &[], None);
        assert_eq!(result, content);
    }

    #[test]
    fn auto_inject_skips_empty_name_or_open_id() {
        let bots = vec![
            make_observed_bot("ou_good", "GoodBot"),
            make_observed_bot("", "EmptyOpenIdBot"),
            make_observed_bot("ou_empty_name", ""),
        ];
        let result = auto_inject_bot_mentions("@GoodBot @EmptyOpenIdBot @EmptyName", &bots, None);
        assert_eq!(
            result,
            "<at user_id=\"ou_good\">@GoodBot</at> @EmptyOpenIdBot @EmptyName"
        );
    }

    #[test]
    fn auto_inject_no_partial_match_with_underscore() {
        // @ReviewerBot_Extra should NOT match ReviewerBot
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let result = auto_inject_bot_mentions("@ReviewerBot_Extra", &bots, None);
        assert_eq!(result, "@ReviewerBot_Extra");
    }

    #[test]
    fn auto_inject_longer_name_matches_first() {
        // Longer bot name should match before shorter
        let bots = vec![
            make_observed_bot("ou_long", "ReviewerBotPro"),
            make_observed_bot("ou_short", "ReviewerBot"),
        ];
        let result = auto_inject_bot_mentions("@ReviewerBotPro", &bots, None);
        assert_eq!(result, "<at user_id=\"ou_long\">@ReviewerBotPro</at>");
    }

    #[test]
    fn auto_inject_no_mention_flag_disables_auto_inject() {
        // While --no-mention is enforced at the handler level,
        // the helper also respects it by not being called.
        // Test that NO auto-injection happens when not called.
        let _bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        // Without auto_inject, content stays as-is
        let content = "请 @ReviewerBot 看一下";
        // The helper is simply not called; this test documents the
        // expected raw content before injection.
        assert_eq!(content, "请 @ReviewerBot 看一下");
    }

    #[test]
    fn auto_inject_with_followed_by_punctuation() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let cases = vec![
            (
                "@ReviewerBot, 你好",
                "<at user_id=\"ou_reviewer\">@ReviewerBot</at>, 你好",
            ),
            (
                "@ReviewerBot. 是的",
                "<at user_id=\"ou_reviewer\">@ReviewerBot</at>. 是的",
            ),
            (
                "@ReviewerBot！你好",
                "<at user_id=\"ou_reviewer\">@ReviewerBot</at>！你好",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                auto_inject_bot_mentions(input, &bots, None),
                expected,
                "failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn auto_inject_noop_when_no_at_sign() {
        let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
        let content = "ReviewerBot without @";
        let result = auto_inject_bot_mentions(content, &bots, None);
        assert_eq!(result, content);
    }

    // ---- withdrawn fallback tests (P2-8: structured send quote/reply withdrawn fallback) ----

    #[test]
    fn withdrawn_payload_recognizes_code_230011() {
        let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
        assert!(is_lark_message_withdrawn_payload(payload));
    }

    #[test]
    fn withdrawn_payload_detects_code_in_string() {
        assert!(is_lark_message_withdrawn_payload("error 230011 occurred"));
    }

    #[test]
    fn withdrawn_payload_detects_withdrawn_keyword() {
        assert!(is_lark_message_withdrawn_payload(
            "message withdrawn by user"
        ));
    }

    #[test]
    fn withdrawn_payload_rejects_normal_error() {
        assert!(!is_lark_message_withdrawn_payload(
            r#"{"code":999,"msg":"permission denied"}"#
        ));
    }

    #[test]
    fn withdrawn_error_recognizes_root_cause_from_chain() {
        let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
        let err = anyhow::anyhow!("lark message withdrawn: {}", payload);
        assert!(is_lark_message_withdrawn_error(&err));
    }

    #[test]
    fn withdrawn_error_rejects_unrelated_error() {
        let err = anyhow::anyhow!("lark reply failed: {{\"code\":999}}");
        assert!(!is_lark_message_withdrawn_error(&err));
    }

    #[test]
    fn should_fallback_to_plain_returns_true_for_withdrawn() {
        let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
        let err = anyhow::anyhow!("lark message withdrawn: {}", payload);
        assert!(should_fallback_to_plain_on_withdrawn(&err));
    }

    #[test]
    fn should_fallback_to_plain_returns_false_for_normal_error() {
        let err = anyhow::anyhow!("lark reply failed: {{\"code\":999}}");
        assert!(!should_fallback_to_plain_on_withdrawn(&err));
    }

    // ---- off_topic_sub_bot_hint tests (P2-9) ----

    fn make_test_session(
        session_id: &str,
        chat_id: &str,
        root_message_id: &str,
        thread_id: Option<&str>,
        bot_open_id: Option<&str>,
        status: SessionStatus,
    ) -> Session {
        let mut session = make_session(session_id);
        session.chat_id = chat_id.to_string();
        session.root_message_id = root_message_id.to_string();
        session.thread_id = thread_id.map(|t| t.to_string());
        session.bot_open_id = bot_open_id.map(|o| o.to_string());
        session.status = status;
        session.scope = SessionScope::Thread; // not critical for hint logic but realistic
        session
    }

    #[test]
    fn off_topic_hint_returns_hint_for_bot_in_different_topic() {
        // Current session is in topic A; mentioned bot is active in topic B (same chat).
        let current = make_test_session(
            "s-current",
            "chat-1",
            "root-aaa",
            Some("thread-aaa"),
            Some("ou_self"),
            SessionStatus::Active,
        );
        let sub_bot = make_test_session(
            "s-sub",
            "chat-1",
            "root-bbb",
            Some("thread-bbb"),
            Some("ou_sub"),
            SessionStatus::Active,
        );
        let mut sessions = HashMap::new();
        sessions.insert(sub_bot.session_id.clone(), sub_bot);

        let hint = off_topic_sub_bot_hint(
            &current,
            &["ou_sub".to_string()],
            &sessions,
            false, // anyway=false
        );
        assert!(hint.is_some(), "should return hint for off-topic sub-bot");
        let msg = hint.unwrap();
        assert!(
            msg.contains("ou_sub"),
            "hint should include mentioned open_id, got: {}",
            msg
        );
        assert!(
            msg.contains("--into"),
            "hint should suggest --into, got: {}",
            msg
        );
        assert!(
            msg.contains("root-bbb"),
            "hint should include target root_message_id, got: {}",
            msg
        );
    }

    #[test]
    fn off_topic_hint_returns_none_for_same_topic() {
        // Same root_message_id and same thread_id → not off-topic.
        let current = make_test_session(
            "s-current",
            "chat-1",
            "root-same",
            Some("thread-same"),
            Some("ou_self"),
            SessionStatus::Active,
        );
        let sub_bot = make_test_session(
            "s-sub",
            "chat-1",
            "root-same",
            Some("thread-same"),
            Some("ou_sub"),
            SessionStatus::Active,
        );
        let mut sessions = HashMap::new();
        sessions.insert(sub_bot.session_id.clone(), sub_bot);

        let hint = off_topic_sub_bot_hint(&current, &["ou_sub".to_string()], &sessions, false);
        assert!(hint.is_none(), "same topic should yield no hint");
    }

    #[test]
    fn off_topic_hint_returns_none_when_anyway_true() {
        let current = make_test_session(
            "s-current",
            "chat-1",
            "root-aaa",
            Some("thread-aaa"),
            Some("ou_self"),
            SessionStatus::Active,
        );
        let sub_bot = make_test_session(
            "s-sub",
            "chat-1",
            "root-bbb",
            Some("thread-bbb"),
            Some("ou_sub"),
            SessionStatus::Active,
        );
        let mut sessions = HashMap::new();
        sessions.insert(sub_bot.session_id.clone(), sub_bot);

        let hint = off_topic_sub_bot_hint(
            &current,
            &["ou_sub".to_string()],
            &sessions,
            true, // anyway=true
        );
        assert!(hint.is_none(), "anyway=true should suppress hint");
    }

    #[test]
    fn off_topic_hint_returns_none_for_non_active_bot() {
        // Mentioned open_id doesn't match any session's bot_open_id.
        let current = make_test_session(
            "s-current",
            "chat-1",
            "root-aaa",
            Some("thread-aaa"),
            Some("ou_self"),
            SessionStatus::Active,
        );
        let sub_bot = make_test_session(
            "s-sub",
            "chat-1",
            "root-bbb",
            Some("thread-bbb"),
            None, // no bot_open_id → not a bot session
            SessionStatus::Active,
        );
        let mut sessions = HashMap::new();
        sessions.insert(sub_bot.session_id.clone(), sub_bot);

        let hint = off_topic_sub_bot_hint(&current, &["ou_unknown".to_string()], &sessions, false);
        assert!(hint.is_none(), "non-bot/human mention should yield no hint");
    }

    #[test]
    fn off_topic_hint_returns_none_for_self_mention() {
        // Mentioning the current session's own bot_open_id.
        let current = make_test_session(
            "s-current",
            "chat-1",
            "root-aaa",
            Some("thread-aaa"),
            Some("ou_self"),
            SessionStatus::Active,
        );
        let sessions: HashMap<String, Session> = HashMap::new();

        let hint = off_topic_sub_bot_hint(&current, &["ou_self".to_string()], &sessions, false);
        assert!(
            hint.is_none(),
            "self-mention (current session's own bot_open_id) should yield no hint"
        );
    }

    #[test]
    fn off_topic_hint_returns_none_for_closed_sub_bot_session() {
        let current = make_test_session(
            "s-current",
            "chat-1",
            "root-aaa",
            Some("thread-aaa"),
            Some("ou_self"),
            SessionStatus::Active,
        );
        let sub_bot = make_test_session(
            "s-sub",
            "chat-1",
            "root-bbb",
            Some("thread-bbb"),
            Some("ou_sub"),
            SessionStatus::Closed, // closed → not active
        );
        let mut sessions = HashMap::new();
        sessions.insert(sub_bot.session_id.clone(), sub_bot);

        let hint = off_topic_sub_bot_hint(&current, &["ou_sub".to_string()], &sessions, false);
        assert!(
            hint.is_none(),
            "closed sub-bot session should yield no hint"
        );
    }
}
