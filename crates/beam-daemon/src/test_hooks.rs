//! Public test-only hooks for integration tests.
//!
//! The daemon's internals (terminal proxy, ticket signing) are private
//! modules, so `tests/` integration tests reach them through these
//! `#[doc(hidden)]` entry points.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::terminal_auth::{TerminalAuthState, TerminalPermission, generate_terminal_ticket};
use crate::terminal_proxy;
use crate::zellij_web::ZellijWebTokens;
use beam_core::Session;

#[doc(hidden)]
pub fn __test_resolve_external_host(bind_host: &str) -> String {
    crate::resolve_external_host(bind_host)
}

#[doc(hidden)]
pub async fn __test_start_terminal_proxy(
    host: &str,
    port: u16,
    zellij_web_port: u16,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    herdr_terminal: bool,
    herdr_max_observers_per_session: usize,
    herdr_max_observers_global: usize,
) -> anyhow::Result<u16> {
    let tokens = ZellijWebTokens::disabled(zellij_web_port);
    let auth_state = TerminalAuthState::new();
    terminal_proxy::start_proxy(
        host,
        port,
        zellij_web_port,
        sessions,
        tokens,
        auth_state,
        terminal_proxy::herdr_ws::HerdrWebLimits {
            enabled: herdr_terminal,
            max_observers_per_session: herdr_max_observers_per_session,
            max_observers_global: herdr_max_observers_global,
        },
    )
    .await
}

#[doc(hidden)]
pub fn __test_generate_terminal_ticket(session_id: &str, write: bool) -> String {
    let permission = if write {
        TerminalPermission::Write
    } else {
        TerminalPermission::ReadOnly
    };
    generate_terminal_ticket(session_id, permission)
}
