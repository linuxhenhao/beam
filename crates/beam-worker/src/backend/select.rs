//! Backend selection from `InitConfig` — multiplexer-agnostic.

use std::sync::Arc;

use beam_core::{BackendKind, InitConfig};

use super::{
    HerdrBackend, HerdrObserveBackend, SessionBackend, ZellijBackend, ZellijObserveBackend,
};

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
