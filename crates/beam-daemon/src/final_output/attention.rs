//! Agent attention (oncall) subsystem — botmux parity.
//!
//! Supports /attention (set), auto-clear on user inbound message, and validation.

use crate::*;

/// Valid attention kinds as defined by the botmux spec.
pub(super) const VALID_ATTENTION_KINDS: [&str; 4] = ["authz", "decision", "blocked", "help"];

/// Validate --attention usage constraints (botmux parity: `attentionUsageError`).
///
/// `--attention` only makes sense when replying into the current session context.
/// Sending to a different chat/thread (via --top-level / --chat-id / --into) or
/// using --voice would route the message elsewhere, leaving the attention signal
/// un-clearable. The dashboard also needs a text reason, so empty content is rejected.
///
/// Returns `Some(error_message)` if invalid, `None` if the request is acceptable.
pub(crate) fn validate_attention_constraints(req: &FinalOutputRequest) -> Option<String> {
    req.attention.as_ref()?;
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
        if let Some(pos) = truncated.rfind(' ')
            && pos > 400
        {
            truncated.truncate(pos);
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
