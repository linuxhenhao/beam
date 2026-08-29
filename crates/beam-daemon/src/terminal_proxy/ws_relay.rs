//! WebSocket relay for terminal proxy.
//!
//! Handles WebSocket upgrades from the browser, resolves the beam session
//! cookie, connects to the upstream zellij WebSocket, and relays messages
//! bidirectionally.

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tracing::{debug, warn};

use crate::terminal_auth;

use super::anchor::{self, should_ensure_read_only_anchor};
use super::auth;
use super::{ProxyState, build_ws_target_url, is_terminal_ws_rest, resolve_zellij_session};

// ── Handler: /s/{session_id}/ws → zellij session WS ──────────────────────

pub(crate) async fn handle_session_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        debug!(
            component = "terminal_proxy",
            operation = "ws_upgrade",
            outcome = "not_found",
            session_id = session_id,
            "terminal proxy: WS session {session_id} not found"
        );
        return Err((StatusCode::NOT_FOUND, "session not found"));
    };

    // WS auth: check Beam cookie (browsers send cookies on WS upgrade)
    debug!(
        component = "terminal_proxy",
        operation = "ws_upgrade",
        outcome = "received",
        session_id = session_id,
        "terminal proxy: WS upgrade for session {session_id} zellij={zellij_session}"
    );
    let headers = req.headers().clone();
    let Some(auth) = auth::authenticate_via_beam_cookie(&state, &session_id, &headers).await else {
        debug!(
            component = "terminal_proxy",
            operation = "ws_upgrade",
            outcome = "missing_cookie",
            session_id = session_id,
            "terminal proxy: WS session {session_id} missing cookie"
        );
        return Err((StatusCode::UNAUTHORIZED, "terminal authentication required"));
    };

    if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
        anchor::ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
    }

    let query = req.uri().query().map(|q| q.to_string());
    let zellij_web_port = state.zellij_web_port;
    let viewer_counter = state.viewer_counter.clone();
    let zellij_session_for_count = zellij_session.clone();

    Ok(ws.on_upgrade(move |client_socket| async move {
        let ws_url = build_ws_target_url(
            zellij_web_port,
            &format!("{zellij_session_for_count}/ws"),
            query.as_deref(),
        );

        // Session-level WS always counts as a terminal viewer.
        viewer_counter.increment(&zellij_session_for_count).await;

        // Connect to zellij WS with optional cookie.
        let result = connect_ws_with_cookie(&ws_url, auth.zellij_cookie()).await;
        match result {
            Ok(zellij_ws) => {
                relay_ws(client_socket, zellij_ws).await;
            }
            Err(_err) => {
                warn!(
                    component = "terminal_proxy",
                    operation = "ws_connect",
                    outcome = "upstream_error",
                    session_id = session_id,
                    "terminal proxy: failed to connect to zellij session WS"
                );
            }
        }

        viewer_counter.decrement(&zellij_session_for_count).await;
    }))
}

// ── Handler: /s/{session_id}/ws/{*rest} → zellij root WS ────────────────

/// Handle session-scoped WS that targets zellij web root WS paths
/// (e.g. `/ws/terminal/<name>`, `/ws/control`).
///
/// These WS paths are called by zellij JS after our base href rewrite makes
/// them session-scoped.  The browser sends the Beam cookie, we look up the
/// zellij cookie and inject it into the upstream WS connection.
///
/// For `ws/terminal/<name>`: translates the terminal name to the real zellij
/// session name (e.g. `beam-...`) since zellij JS picks up the beam session ID
/// from `location.pathname`.
pub(crate) async fn handle_session_root_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path((session_id, rest)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    // Resolve actual zellij session name
    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        debug!(
            component = "terminal_proxy",
            operation = "ws_upgrade",
            outcome = "not_found",
            session_id = session_id,
            "terminal proxy: root WS session {session_id} not found"
        );
        return Err((StatusCode::NOT_FOUND, "session not found"));
    };

    // Authenticate via Beam cookie (required — no unauthenticated WS)
    debug!(
        component = "terminal_proxy",
        operation = "ws_upgrade",
        outcome = "received",
        session_id = session_id,
        "terminal proxy: root WS upgrade for session {session_id}"
    );
    let headers = req.headers().clone();
    let auth = auth::authenticate_via_beam_cookie(&state, &session_id, &headers)
        .await
        .ok_or((StatusCode::UNAUTHORIZED, "terminal authentication required"))?;
    debug!(
        component = "terminal_proxy",
        operation = "ws_upgrade",
        outcome = "success",
        session_id = session_id,
        "terminal proxy: root WS cookie auth OK for session {session_id}"
    );

    if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
        anchor::ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
    }

    // Translate the WS path: replace terminal name with actual zellij session
    let translated_path = terminal_auth::translate_root_ws_path(&rest, &zellij_session);

    let query = req.uri().query().map(|q| q.to_string());
    let zellij_web_port = state.zellij_web_port;
    let viewer_counter = state.viewer_counter.clone();
    let is_terminal = is_terminal_ws_rest(&rest);
    let zellij_session_for_count = zellij_session.clone();

    Ok(ws.on_upgrade(move |client_socket| async move {
        let ws_url = build_ws_target_url(zellij_web_port, &translated_path, query.as_deref());

        // Only terminal WebSocket paths (ws/terminal/...) count as viewers;
        // control WS does not count. The anchor's own terminal WS connections
        // are internal and never pass through here.
        if is_terminal {
            viewer_counter.increment(&zellij_session_for_count).await;
        }

        let result = connect_ws_with_cookie(&ws_url, auth.zellij_cookie()).await;
        match result {
            Ok(zellij_ws) => {
                relay_ws(client_socket, zellij_ws).await;
            }
            Err(_err) => {
                warn!(
                    component = "terminal_proxy",
                    operation = "ws_connect",
                    outcome = "upstream_error",
                    session_id = session_id,
                    "terminal proxy: failed to connect to zellij root WS"
                );
            }
        }

        if is_terminal {
            viewer_counter.decrement(&zellij_session_for_count).await;
        }
    }))
}

// ── Connect and relay ────────────────────────────────────────────────────

/// Connect to a WebSocket URL with an optional Cookie header.
/// Uses tungstenite's `ClientRequestBuilder` to build a proper WS handshake
/// request and then injects the Cookie header.
pub(crate) async fn connect_ws_with_cookie(
    url: &str,
    cookie: Option<&str>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::Error,
> {
    use axum::http::Uri;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::{ClientRequestBuilder, error::UrlError};

    let uri: Uri = url.parse().map_err(|_| {
        tokio_tungstenite::tungstenite::Error::Url(UrlError::UnableToConnect(url.to_string()))
    })?;
    let mut builder = ClientRequestBuilder::new(uri);
    if let Some(cookie) = cookie {
        builder = builder.with_header("Cookie", cookie);
    }

    debug!(
        component = "terminal_proxy",
        operation = "ws_connect",
        outcome = "connecting",
        "terminal proxy: connecting WS"
    );
    let result = connect_async(builder).await.map(|(ws, _)| ws);
    if let Err(ref _e) = result {
        warn!(
            component = "terminal_proxy",
            operation = "ws_connect",
            outcome = "upstream_error",
            "terminal proxy: WS connect failed"
        );
    } else {
        debug!(
            component = "terminal_proxy",
            operation = "ws_connect",
            outcome = "success",
            "terminal proxy: WS connect OK"
        );
    }
    result
}

/// Relay WebSocket messages between client and zellij web.
///
/// Pure relay — no message filtering.  All client messages (including
/// `TerminalResize` / `TerminalMetrics`) are forwarded to zellij web as-is.
/// The real terminal viewport is driven by the browser that owns the
/// connection; Beam does not intercept viewer resize/metrics.
async fn relay_ws(
    client: WebSocket,
    zellij: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let (mut client_sender, mut client_receiver) = client.split();
    let (mut zellij_sender, mut zellij_receiver) = zellij.split();

    loop {
        tokio::select! {
            msg = client_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let _ = zellij_sender.send(
                            tokio_tungstenite::tungstenite::Message::Text(text.to_string().into())
                        ).await;
                    }
                    Some(Ok(Message::Binary(data))) => {
                        let _ = zellij_sender.send(
                            tokio_tungstenite::tungstenite::Message::Binary(data.to_vec().into())
                        ).await;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = zellij_sender.send(
                            tokio_tungstenite::tungstenite::Message::Ping(data.to_vec().into())
                        ).await;
                    }
                    Some(Ok(Message::Pong(data))) => {
                        let _ = zellij_sender.send(
                            tokio_tungstenite::tungstenite::Message::Pong(data.to_vec().into())
                        ).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let _ = zellij_sender.send(
                            tokio_tungstenite::tungstenite::Message::Close(
                                frame.map(|f| tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                    code: f.code.into(),
                                    reason: f.reason.to_string().into(),
                                })
                            )
                        ).await;
                        break;
                    }
                    Some(Err(_)) | None => break,
                }
            }
            msg = zellij_receiver.next() => {
                match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        let _ = client_sender.send(
                            Message::Text(text.to_string().into())
                        ).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => {
                        let _ = client_sender.send(
                            Message::Binary(data.to_vec().into())
                        ).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                        let _ = client_sender.send(
                            Message::Ping(data.to_vec().into())
                        ).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(data))) => {
                        let _ = client_sender.send(
                            Message::Pong(data.to_vec().into())
                        ).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame))) => {
                        let _ = client_sender.send(
                            Message::Close(frame.map(|f| axum::extract::ws::CloseFrame {
                                code: f.code.into(),
                                reason: f.reason.to_string().into(),
                            }))
                        ).await;
                        break;
                    }
                    Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}
