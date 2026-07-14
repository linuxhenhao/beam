//! Terminal proxy — authenticates and proxies terminal sessions to zellij web.
//!
//! Entry point: [`start_proxy`] sets up the axum router and spawns a listener.
//! Internal logic is split across submodules:
//!
//! - [`auth`] — ticket/cookie authentication
//! - [`http_forward`] — HTTP request forwarding and body rewriting
//! - [`ws_relay`] — WebSocket relay
//! - [`anchor`] — read-only render anchor and viewer counter

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, http::HeaderName};
use reqwest::Client;
use tokio::sync::Mutex;
use tracing::{info, warn};

use beam_core::session::Session;

use crate::terminal_auth::{TerminalAuthState, TerminalPermission};
use crate::zellij_web::ZellijWebTokens;

pub(crate) mod anchor;
pub(crate) mod auth;
pub(crate) mod http_forward;
pub(crate) mod ws_relay;

#[cfg(test)]
mod tests;

// ── Constants ────────────────────────────────────────────────────────────

/// Hop-by-hop headers that should NOT be forwarded (RFC 2616 13.5.1).
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
];

/// WebSocket handshake headers that should not be forwarded by the HTTP proxy.
const WEBSOCKET_HANDSHAKE_HEADERS: &[&str] = &[
    "sec-websocket-key",
    "sec-websocket-version",
    "sec-websocket-protocol",
    "sec-websocket-extensions",
];

/// Response headers that must NOT be forwarded to the browser.
/// These include zellij's Set-Cookie to prevent zellij cookie leakage.
const STRIP_RESPONSE_HEADERS: &[&str] = &["set-cookie"];

/// Avoid hot-spawning anchors if zellij rejects/fails quickly.
const ANCHOR_RESTART_COOLDOWN: Duration = Duration::from_secs(5);

// ── Header helpers ───────────────────────────────────────────────────────

fn is_hop_by_hop(name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(&name.as_str().to_lowercase().as_str())
}

fn should_strip_response_header(name: &str) -> bool {
    let lower = name.to_lowercase();
    STRIP_RESPONSE_HEADERS.contains(&lower.as_str())
}

fn is_websocket_handshake_header(name: &HeaderName) -> bool {
    WEBSOCKET_HANDSHAKE_HEADERS.contains(&name.as_str().to_lowercase().as_str())
}

fn is_websocket_upgrade(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

// ── Path/URL helpers ─────────────────────────────────────────────────────

/// Determine if the `rest` path from `/s/{session_id}/ws/{*rest}` targets
/// a terminal WebSocket (as opposed to a control WebSocket).
fn is_terminal_ws_rest(rest: &str) -> bool {
    rest == "terminal" || rest.starts_with("terminal/")
}

/// Map a beam session_id to a zellij session name.
fn zellij_session_for_beam(session: &Session) -> String {
    session
        .adopted_from
        .as_ref()
        .and_then(|a| a.zellij_session.clone())
        .unwrap_or_else(|| {
            format!(
                "beam-{}",
                &session.session_id[..8.min(session.session_id.len())]
            )
        })
}

/// Resolve beam session_id to zellij session name.
async fn resolve_zellij_session(
    sessions: &Arc<Mutex<HashMap<String, Session>>>,
    session_id: &str,
) -> Option<String> {
    let sessions = sessions.lock().await;
    sessions.get(session_id).map(|s| zellij_session_for_beam(s))
}

// ── URL builders ─────────────────────────────────────────────────────────

/// Build target URL for proxying to zellij web.
fn build_target_url(
    zellij_web_port: u16,
    zellij_session: &str,
    extra_path: &str,
    query: Option<&str>,
) -> String {
    let query_str = query
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    if extra_path.is_empty() {
        format!("http://127.0.0.1:{zellij_web_port}/{zellij_session}{query_str}")
    } else {
        format!("http://127.0.0.1:{zellij_web_port}/{zellij_session}/{extra_path}{query_str}")
    }
}

/// Build a target URL for proxying to zellij web root (no session prefix).
fn build_root_target_url(zellij_web_port: u16, path: &str, query: Option<&str>) -> String {
    let query_str = query
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    format!("http://127.0.0.1:{zellij_web_port}/{path}{query_str}")
}

/// Build a websocket target URL for proxying to zellij web.
fn build_ws_target_url(zellij_web_port: u16, path: &str, query: Option<&str>) -> String {
    let query_str = query
        .filter(|q| !q.is_empty())
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        format!("ws://127.0.0.1:{zellij_web_port}/{query_str}")
    } else {
        format!("ws://127.0.0.1:{zellij_web_port}/{path}{query_str}")
    }
}

// ── Token helpers ────────────────────────────────────────────────────────

/// Select the appropriate zellij auth token for the given permission.
fn zellij_token_for_permission(
    tokens: &ZellijWebTokens,
    permission: TerminalPermission,
) -> Option<&str> {
    match permission {
        TerminalPermission::ReadOnly => tokens.read_only_token.as_deref(),
        TerminalPermission::Write => tokens.write_token.as_deref(),
    }
    .filter(|token| !token.is_empty())
}

fn unavailable_token_message(permission: TerminalPermission) -> &'static str {
    match permission {
        TerminalPermission::ReadOnly => "read-only token not available",
        TerminalPermission::Write => "write token not available",
    }
}

// ── Shared state ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ProxyState {
    pub(crate) http_client: Client,
    pub(crate) sessions: Arc<Mutex<HashMap<String, Session>>>,
    pub(crate) zellij_web_port: u16,
    pub(crate) zellij_tokens: ZellijWebTokens,
    pub(crate) auth_state: TerminalAuthState,
    pub(crate) anchors: anchor::ZellijAnchorManager,
    pub(crate) viewer_counter: anchor::ViewerCounter,
}

// ── Entry point ──────────────────────────────────────────────────────────

pub async fn start_proxy(
    host: &str,
    port: u16,
    zellij_web_port: u16,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    zellij_tokens: ZellijWebTokens,
    auth_state: TerminalAuthState,
) -> anyhow::Result<u16> {
    let anchors = anchor::ZellijAnchorManager::default();
    let viewer_counter = anchor::ViewerCounter {
        inner: Arc::new(Mutex::new(HashMap::new())),
        anchors: anchors.clone(),
    };

    let state = ProxyState {
        http_client: Client::new(),
        sessions,
        zellij_web_port,
        zellij_tokens,
        auth_state,
        anchors,
        viewer_counter,
    };

    let app = Router::new()
        // Session main page — handles ticket/cookie auth + proxy
        .route(
            "/s/{session_id}",
            axum::routing::any(http_forward::handle_session_terminal),
        )
        .route(
            "/s/{session_id}/",
            axum::routing::any(http_forward::handle_session_terminal),
        )
        // Session-scoped WS to zellij session (e.g. /s/{sid}/ws)
        .route(
            "/s/{session_id}/ws",
            axum::routing::any(ws_relay::handle_session_ws),
        )
        // Session-scoped WS to zellij root: /ws/terminal/... and /ws/control
        .route(
            "/s/{session_id}/ws/{*rest}",
            axum::routing::any(ws_relay::handle_session_root_ws),
        )
        // Session sub-paths — handles both zellij root APIs and session assets
        .route(
            "/s/{session_id}/{*path}",
            axum::routing::any(http_forward::handle_session_path),
        )
        .fallback(http_forward::handle_not_found)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    let addr = listener.local_addr()?;
    info!(
        "terminal proxy listening on {host}:{} (zellij web on 127.0.0.1:{})",
        addr.port(),
        zellij_web_port
    );
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            warn!("terminal proxy server error: {err}");
        }
    });
    Ok(addr.port())
}
