//! Daemon-side backend helpers: session backend resolution and Ready identity.

use super::*;

/// Resolve and persist the terminal backend for a new session: adopt wins,
/// then the bot override, then the daemon default. Returns the kind plus the
/// herdr session name (from `[herdr] session`) when Herdr is selected.
pub(crate) fn resolve_session_backend(
    state: &AppState,
    lark_app_id: &str,
    adopted_from: Option<&AdoptedFrom>,
) -> (BackendKind, Option<String>) {
    let backend_kind = adopted_from
        .map(|adopted| adopted.backend_kind)
        .unwrap_or_else(|| {
            state
                .bots
                .get(lark_app_id)
                .and_then(|bot| bot.backend)
                .unwrap_or(state.config.daemon.backend)
        });
    let herdr_session =
        (backend_kind == BackendKind::Herdr).then(|| state.config.herdr.session.clone());
    (backend_kind, herdr_session)
}

/// Apply a worker `Ready` to a session: persist the mux identity and, for
/// Zellij only, set the web terminal URL. Herdr must NOT get a `terminal_url`
/// (card delivery uses `session_card_ready` on the herdr ids instead).
pub(crate) fn apply_ready_identity(
    session: &mut Session,
    backend_kind: BackendKind,
    herdr_workspace_id: Option<String>,
    herdr_pane_id: Option<String>,
    terminal_url: Option<String>,
) {
    session.backend_kind = backend_kind;
    session.herdr_workspace_id = herdr_workspace_id;
    session.herdr_pane_id = herdr_pane_id;
    if backend_kind == BackendKind::Zellij {
        session.terminal_url = terminal_url;
    }
    session.last_screen_status = Some(ScreenStatus::Starting);
}

/// Handle a worker `MuxAgentState` message: only `blocked` produces an
/// attention side effect (default reason, truncated); every other state is
/// log-only and never writes `ScreenStatus`. Returns `true` when the message
/// was fully handled (the caller may continue), `false` when the session was
/// already attending and nothing should be re-evaluated.
pub(crate) async fn handle_mux_agent_state(
    state: &AppState,
    session_id: &str,
    agent_state: &str,
    pane_id: &str,
    message: Option<&str>,
) -> bool {
    if agent_state != "blocked" {
        debug!(
            session = %session_id,
            pane_id,
            state = %agent_state,
            "mux agent state (no side effect)"
        );
        return true;
    }
    let already_attending = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(session_id)
            .and_then(|entry| entry.agent_attention.as_ref())
            .is_some()
    };
    if already_attending {
        return false;
    }
    let raw_reason = message
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("herdr agent blocked");
    let reason = crate::final_output::normalize_attention_reason(raw_reason);
    info!(
        session = %session_id,
        pane_id,
        reason = %reason,
        "herdr agent blocked -> attention"
    );
    if let Err(err) =
        crate::final_output::set_session_attention(state, session_id, "blocked", &reason).await
    {
        warn!(
            "failed to set herdr blocked attention for {}: {}",
            session_id, err
        );
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::{make_session, make_state, temp_paths};
    use std::collections::HashMap;

    #[tokio::test]
    async fn mux_agent_state_blocked_sets_attention_with_default_reason() {
        let paths = temp_paths("mux-attention");
        let state = make_state(paths, HashMap::new());
        let mut session = make_session("mux-1");
        session.status = SessionStatus::Active;
        session.backend_kind = BackendKind::Herdr;
        state
            .sessions
            .lock()
            .await
            .insert("mux-1".to_string(), session.clone());

        let handled =
            crate::backend::handle_mux_agent_state(&state, "mux-1", "blocked", "w1:p1", None).await;
        assert!(handled);
        let attention = {
            let sessions = state.sessions.lock().await;
            sessions
                .get("mux-1")
                .and_then(|s| s.agent_attention.clone())
        };
        let attention = attention.expect("attention set");
        assert_eq!(attention.kind, "blocked");
        assert_eq!(attention.reason, "herdr agent blocked");
    }

    #[tokio::test]
    async fn mux_agent_state_non_blocked_is_noop() {
        let paths = temp_paths("mux-noop");
        let state = make_state(paths, HashMap::new());
        let mut session = make_session("mux-2");
        session.status = SessionStatus::Active;
        session.backend_kind = BackendKind::Herdr;
        state
            .sessions
            .lock()
            .await
            .insert("mux-2".to_string(), session.clone());

        crate::backend::handle_mux_agent_state(&state, "mux-2", "working", "w1:p1", None).await;
        let sessions = state.sessions.lock().await;
        assert!(
            sessions
                .get("mux-2")
                .and_then(|s| s.agent_attention.as_ref())
                .is_none()
        );
    }

    #[tokio::test]
    async fn mux_agent_state_keeps_existing_attention() {
        let paths = temp_paths("mux-existing");
        let state = make_state(paths, HashMap::new());
        let mut session = make_session("mux-3");
        session.status = SessionStatus::Active;
        session.backend_kind = BackendKind::Herdr;
        session.agent_attention = Some(AgentAttention {
            kind: "blocked".to_string(),
            reason: "existing".to_string(),
            at: Utc::now(),
        });
        state
            .sessions
            .lock()
            .await
            .insert("mux-3".to_string(), session.clone());

        crate::backend::handle_mux_agent_state(&state, "mux-3", "blocked", "w1:p1", Some("new"))
            .await;
        let sessions = state.sessions.lock().await;
        let attention = sessions
            .get("mux-3")
            .and_then(|s| s.agent_attention.as_ref());
        assert_eq!(attention.map(|a| a.reason.as_str()), Some("existing"));
    }
}
