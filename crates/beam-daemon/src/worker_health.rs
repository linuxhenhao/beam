//! Periodic worker-health watchdog: flag workers that stopped heartbeating
//! so the state is visible on the session card and via `beam status`.

use super::*;

/// Upper bound for a worker to report `Ready` after being spawned. When the
/// terminal backend hangs during startup (e.g. a zellij server crash that
/// leaves `zellij attach --create-background` retrying forever), the worker
/// would otherwise never send `Ready` and the session would silently stay
/// active without any card or error.
pub(crate) const WORKER_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Notify the user when a session's worker never reported Ready in time.
///
/// The ready latch is backend-specific (`worker_ready_reported`), so a session
/// that already carries its mux identity — notably an adopted pane whose worker
/// reports no managed-handle ids — returns early and stays quiet.
pub(crate) async fn run_worker_ready_watchdog(
    state: AppState,
    session_id: String,
    timeout: std::time::Duration,
) {
    tokio::time::sleep(timeout).await;
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(&session_id).cloned()
    };
    let Some(session) = session else {
        return;
    };
    if worker_ready_reported(&session) {
        return;
    }
    warn!(
        "worker for session {} did not report Ready within {:?}",
        session_id, timeout
    );
    notify_worker_ready_timeout(&state, &session).await;
}

/// Notify the user (via Lark) that a session's worker failed to become ready
/// within [`WORKER_READY_TIMEOUT`]. No-op for local (non-Lark) sessions.
async fn notify_worker_ready_timeout(state: &AppState, session: &Session) {
    if session.lark_app_id == "local" {
        return;
    }
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return;
    };
    let message = if crate::prompt::is_zh_locale(session.locale.as_deref()) {
        format!(
            "⚠️ session「{}」启动超时：worker 未在 {} 秒内向 daemon 报告就绪，终端后端可能启动失败（如 zellij 异常）。请尝试重新创建 session。",
            session.title,
            WORKER_READY_TIMEOUT.as_secs()
        )
    } else {
        format!(
            "⚠️ Session \"{}\" startup timed out: the worker did not report ready within {}s. The terminal backend may have failed to start (e.g. zellij crash). Please try creating the session again.",
            session.title,
            WORKER_READY_TIMEOUT.as_secs()
        )
    };
    let result = match session.scope {
        SessionScope::Thread if !session.root_message_id.is_empty() => {
            lark_reply_message_with_opts(state, bot, &session.root_message_id, &message, true).await
        }
        _ => lark_send_chat_message(state, bot, &session.chat_id, &message).await,
    };
    if let Err(err) = result {
        warn!(
            "failed to notify worker-ready timeout for session {}: {}",
            session.session_id, err
        );
    }
}

/// Whether a session's worker has reported ready, per backend.
///
/// Zellij readiness is signalled by `terminal_url` (written by the Ready
/// handler). Herdr sessions keep `terminal_url` unset, so their readiness is
/// the persisted herdr pane identity instead. Non-active sessions are handled
/// elsewhere (e.g. CliExit) and always count as ready so they never get a
/// spurious "startup timeout" notice.
pub(crate) fn worker_ready_reported(session: &Session) -> bool {
    session.terminal_url.is_some()
        || (session.backend_kind == BackendKind::Herdr
            && session.herdr_workspace_id.is_some()
            && session.herdr_pane_id.is_some())
        || session.status != SessionStatus::Active
}

/// Periodic watchdog: flag sessions whose worker stopped heartbeating (hung,
/// or dead but not yet reaped) so the state is visible on the session card
/// and via `beam status`. Only workers that have sent at least one heartbeat
/// are judged; older workers simply stay "unknown".
pub(crate) fn spawn_worker_health_watchdog(state: AppState) {
    tokio::spawn(async move {
        const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(45);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let stale_sessions: Vec<(String, Option<u64>)> = {
                let workers = state.workers.lock().await;
                let sessions = state.sessions.lock().await;
                let mut health = state.worker_health.lock().await;
                let now = Instant::now();
                let mut stale = Vec::new();
                for (session_id, entry) in health.iter_mut() {
                    let worker_present = workers.contains_key(session_id);
                    let session_active = sessions
                        .get(session_id)
                        .map(|s| s.status == SessionStatus::Active)
                        .unwrap_or(false);
                    if !worker_present || !session_active || entry.unresponsive {
                        continue;
                    }
                    if now.duration_since(entry.last_heartbeat) > STALE_AFTER {
                        entry.unresponsive = true;
                        stale.push((session_id.clone(), entry.processing_since_ms));
                    }
                }
                stale
            };
            for (session_id, processing_since_ms) in stale_sessions {
                match processing_since_ms {
                    Some(start_ms) => {
                        let stuck_ms =
                            (Utc::now().timestamp_millis().max(0) as u64).saturating_sub(start_ms);
                        warn!(
                            "worker for session {} is unresponsive: no heartbeat for >{}s; message loop stuck processing for {}ms",
                            session_id,
                            STALE_AFTER.as_secs(),
                            stuck_ms
                        );
                    }
                    None => {
                        warn!(
                            "worker for session {} is unresponsive: no heartbeat for >{}s",
                            session_id,
                            STALE_AFTER.as_secs()
                        );
                    }
                }
                let _ = patch_lark_streaming_card(&state, &session_id, "worker 无响应").await;
            }
        }
    });
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::{run_worker_ready_watchdog, worker_ready_reported};
    use crate::tests::test_helpers::{
        LarkBaseUrlEnvGuard, lark_base_url_env_lock, make_bot, make_session, make_state,
        maybe_remove_dir, mock_lark_requests, start_mock_lark_server, temp_paths,
    };
    use beam_core::{BackendKind, SessionStatus};

    #[test]
    fn zellij_requires_terminal_url() {
        let mut session = make_session("s1");
        session.status = SessionStatus::Active;
        assert!(!worker_ready_reported(&session));
        session.terminal_url = Some("http://127.0.0.1:8800/s/s1".to_string());
        assert!(worker_ready_reported(&session));
    }

    #[test]
    fn herdr_uses_pane_ids() {
        let mut session = make_session("s1");
        session.status = SessionStatus::Active;
        session.backend_kind = BackendKind::Herdr;
        assert!(!worker_ready_reported(&session));
        session.herdr_workspace_id = Some("w1".to_string());
        session.herdr_pane_id = Some("w1:p1".to_string());
        assert!(worker_ready_reported(&session));
    }

    #[test]
    fn non_active_never_times_out() {
        let mut session = make_session("s1");
        session.status = SessionStatus::Closed;
        assert!(worker_ready_reported(&session));
    }

    #[tokio::test]
    async fn ready_watchdog_stays_quiet_for_adopted_herdr_after_ready() {
        // An adopted herdr session already carries its mux identity, so the
        // worker's Ready (which reports no ids: observe backends have no
        // managed handle) must not trigger a startup-timeout notice.
        let _env_lock = lark_base_url_env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("ready-watchdog-adopt");
        let app_id = "app-ready-watchdog";
        let bot = make_bot(app_id);
        let state = make_state(
            paths.clone(),
            std::collections::HashMap::from([(app_id.to_string(), bot)]),
        );
        let root_message_id = format!("om_root_watchdog_{}", uuid::Uuid::new_v4().simple());
        let session_id = "sess-ready-watchdog";
        let mut session = make_session(session_id);
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.lark_app_id = app_id.to_string();
        session.backend_kind = BackendKind::Herdr;
        session.herdr_workspace_id = Some("w8".to_string());
        session.herdr_pane_id = Some("w8:p1".to_string());
        session.root_message_id = root_message_id.clone();
        crate::backend::apply_ready_identity(&mut session, BackendKind::Herdr, None, None, None);
        state
            .sessions
            .lock()
            .await
            .insert(session_id.to_string(), session);

        run_worker_ready_watchdog(
            state.clone(),
            session_id.to_string(),
            std::time::Duration::ZERO,
        )
        .await;

        let notify_path = format!("POST /im/v1/messages/{root_message_id}/reply");
        assert!(
            !mock_lark_requests().lock().unwrap().contains(&notify_path),
            "an adopted session that reported Ready must not get a startup timeout"
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn ready_watchdog_notifies_when_the_worker_never_reported() {
        // Control: with no mux identity the watchdog must still speak up.
        let _env_lock = lark_base_url_env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("ready-watchdog-none");
        let app_id = "app-ready-watchdog-none";
        let bot = make_bot(app_id);
        let state = make_state(
            paths.clone(),
            std::collections::HashMap::from([(app_id.to_string(), bot)]),
        );
        let root_message_id = format!("om_root_watchdog_none_{}", uuid::Uuid::new_v4().simple());
        let session_id = "sess-ready-watchdog-none";
        let mut session = make_session(session_id);
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.lark_app_id = app_id.to_string();
        session.backend_kind = BackendKind::Herdr;
        session.root_message_id = root_message_id.clone();
        state
            .sessions
            .lock()
            .await
            .insert(session_id.to_string(), session);

        run_worker_ready_watchdog(
            state.clone(),
            session_id.to_string(),
            std::time::Duration::ZERO,
        )
        .await;

        let notify_path = format!("POST /im/v1/messages/{root_message_id}/reply");
        let replied = mock_lark_requests().lock().unwrap().contains(&notify_path);
        assert!(replied, "a session with no identity must still be reported");
        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
