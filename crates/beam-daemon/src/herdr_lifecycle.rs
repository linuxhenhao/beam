//! Daemon-side Herdr lifecycle: force-close managed workspaces and liveness
//! checks used by close/restart/restore dispatch.

use super::*;

/// Force-close a managed Herdr workspace by public id. Best-effort and
/// idempotent: a second close of an already-gone workspace is not an error.
pub(crate) async fn workspace_close(workspace_id: &str) {
    // herdr 0.8.2 closes immediately; `--force` does not exist on that
    // version, so try the plain form first and fall back only if a future
    // version answers `confirmation_required`.
    let output = std::process::Command::new("herdr")
        .args(["workspace", "close", workspace_id])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output();
    match output {
        Ok(out) if out.status.success() => {
            info!(workspace_id, "herdr workspace force-closed");
        }
        Ok(out) => warn!(
            workspace_id,
            status = %out.status,
            "herdr workspace close returned non-zero"
        ),
        Err(err) => warn!(workspace_id, error = %err, "herdr workspace close failed"),
    }
}

/// Whether a session's mux target is still alive for restore purposes.
/// Mirrors zellij's `zellij_has_session` for Zellij sessions and a
/// `herdr workspace get` probe for Herdr ones. This predicate is used by
/// restore (a missing mux object may mark the session Closed); it is NOT the
/// same as the live-daemon `ensure_worker_for_session` gate.
pub(crate) async fn mux_target_alive(session: &Session) -> bool {
    match session.backend_kind {
        BackendKind::Zellij => zellij_has_session(&session_zellij_target(session)),
        BackendKind::Herdr => {
            let Some(workspace_id) = session.herdr_workspace_id.as_deref() else {
                return false;
            };
            std::process::Command::new("herdr")
                .args(["workspace", "get", workspace_id])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        }
    }
}

/// Synchronous variant for restore reconciliation (which runs under a
/// synchronous `HashMap` lock and cannot await).
pub(crate) fn mux_target_alive_sync(session: &Session) -> bool {
    match session.backend_kind {
        BackendKind::Zellij => zellij_has_session(&session_zellij_target(session)),
        BackendKind::Herdr => {
            let Some(workspace_id) = session.herdr_workspace_id.as_deref() else {
                return false;
            };
            std::process::Command::new("herdr")
                .args(["workspace", "get", workspace_id])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        }
    }
}

/// Destroy the managed mux object for a session during `/close` / `/restart`
/// when the worker could not do it itself. Zellij uses
/// `zellij delete-session -f`; Herdr uses `workspace close --force`. Adopt
/// sessions are never touched.
pub(crate) async fn destroy_mux_if_managed(session: &Session) {
    if session.adopted_from.is_some() {
        return;
    }
    match session.backend_kind {
        BackendKind::Zellij => {
            let target = session_zellij_target(session);
            let _ = std::process::Command::new("zellij")
                .args(["delete-session", &target, "-f"])
                .output();
        }
        BackendKind::Herdr => {
            if let Some(workspace_id) = session.herdr_workspace_id.as_deref() {
                workspace_close(workspace_id).await;
            }
        }
    }
}
