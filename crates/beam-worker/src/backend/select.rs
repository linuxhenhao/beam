//! Backend selection from `InitConfig` — multiplexer-agnostic.

use std::sync::Arc;

use beam_core::{BackendKind, InitConfig};

use super::{
    HerdrBackend, HerdrObserveBackend, SessionBackend, ZellijBackend, ZellijObserveBackend,
};

/// Resolve the mux identity to report in `WorkerToDaemon::Ready`.
///
/// Managed Herdr sessions learn their ids from the handle that created the
/// workspace. Adopted sessions instead attach to an already-running pane
/// through an observe-only backend that has **no** handle, so their identity can
/// only come from the adopt record. Reporting `None` there is not harmless: the
/// daemon treats the herdr ids as its readiness latch (startup-timeout watchdog
/// and turn-card gate), so a `None` both fires a bogus startup timeout and stops
/// new stream cards from being created.
pub(crate) fn ready_herdr_identity(
    managed: Option<(String, String)>,
    adopted_from: Option<&beam_core::AdoptedFrom>,
) -> (Option<String>, Option<String>) {
    if let Some((workspace_id, pane_id)) = managed {
        return (Some(workspace_id), Some(pane_id));
    }
    match adopted_from {
        Some(adopted) => (
            adopted.herdr_workspace_id.clone(),
            adopted.herdr_pane_id.clone(),
        ),
        None => (None, None),
    }
}

/// The selected backend plus a human label for error messages. For Herdr
/// managed sessions a concrete handle is returned so `run_loop` can read the
/// workspace/pane ids back for `Ready`.
pub(crate) fn select_backend(
    init: &InitConfig,
    session_name: &str,
) -> (
    Arc<dyn SessionBackend>,
    &'static str,
    Option<Arc<HerdrBackend>>,
) {
    if init.backend_kind == BackendKind::Herdr {
        return select_herdr(init, session_name);
    }
    if let Some(adopted) = init.adopted_from.as_ref()
        && let Some(pane_id) = adopted.zellij_pane_id.clone()
    {
        let session = adopted
            .zellij_session
            .clone()
            .unwrap_or_else(|| session_name.to_string());
        let observe = ZellijObserveBackend::new(
            session,
            pane_id,
            u32::try_from(adopted.original_cli_pid).ok(),
        );
        return (Arc::new(observe), "observe", None);
    }
    let zellij = ZellijBackend::new(session_name.to_string());
    (Arc::new(zellij), "spawn", None)
}

fn select_herdr(
    init: &InitConfig,
    session_name: &str,
) -> (
    Arc<dyn SessionBackend>,
    &'static str,
    Option<Arc<HerdrBackend>>,
) {
    if let Some(adopted) = init.adopted_from.as_ref()
        && let Some(pane_id) = adopted.herdr_pane_id.clone()
    {
        let workspace_id = adopted.herdr_workspace_id.clone().unwrap_or_default();
        let observe = HerdrObserveBackend::new(
            workspace_id,
            pane_id,
            u32::try_from(adopted.original_cli_pid).ok(),
        );
        return (Arc::new(observe), "herdr observe", None);
    }
    let herdr = Arc::new(HerdrBackend::new(
        session_name.to_string(),
        init.working_dir.clone(),
    ));
    let handle = herdr.clone();
    (herdr, "herdr spawn", Some(handle))
}

#[cfg(test)]
mod tests {
    use super::ready_herdr_identity;

    fn adopted_herdr(workspace_id: &str, pane_id: &str) -> beam_core::AdoptedFrom {
        beam_core::AdoptedFrom {
            backend_kind: beam_core::BackendKind::Herdr,
            tmux_target: None,
            zellij_session: None,
            zellij_pane_id: None,
            herdr_workspace_id: Some(workspace_id.to_string()),
            herdr_pane_id: Some(pane_id.to_string()),
            original_cli_pid: 4242,
            session_id: None,
            cli_id: Some("traex".to_string()),
            cwd: "/tmp/project".to_string(),
            pane_cols: None,
            pane_rows: None,
        }
    }

    #[test]
    fn managed_herdr_ready_uses_handle_ids() {
        let adopted = adopted_herdr("w1", "w1:p1");
        assert_eq!(
            ready_herdr_identity(
                Some(("w9".to_string(), "w9:p2".to_string())),
                Some(&adopted)
            ),
            (Some("w9".to_string()), Some("w9:p2".to_string())),
            "a managed spawn must report the pane it created"
        );
    }

    #[test]
    fn adopted_herdr_ready_falls_back_to_adopt_record() {
        // Observe-only adopted sessions have no managed handle; without this
        // fallback Ready reports None and the daemon drops the adopt identity.
        let adopted = adopted_herdr("w8", "w8:p1");
        assert_eq!(
            ready_herdr_identity(None, Some(&adopted)),
            (Some("w8".to_string()), Some("w8:p1".to_string()))
        );
    }

    #[test]
    fn non_adopted_ready_without_handle_reports_no_ids() {
        assert_eq!(ready_herdr_identity(None, None), (None, None));
    }
}
