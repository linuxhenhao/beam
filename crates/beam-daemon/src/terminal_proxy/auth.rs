//! Ticket/cookie authentication for terminal proxy.
//!
//! Handles zellij web login, beam ticket verification, beam cookie lookup,
//! and the [`AuthenticatedTerminal`] result used by handlers. The ticket and
//! cookie machinery is shared by both backends; only the upstream identity
//! (zellij cookie vs herdr pane ids) differs.

use axum::{
    http::{HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Redirect, Response},
};
use reqwest::{Client, header as reqwest_header};
use tracing::{debug, warn};

use beam_core::BackendKind;

use crate::terminal_auth;
use crate::terminal_auth::{
    AuthenticatedTerminal, BEAM_COOKIE_NAME, TerminalPermission, UpstreamTarget,
};

use super::anchor::{self, should_ensure_read_only_anchor};
use super::{
    ProxyState, resolve_zellij_session, unavailable_token_message, zellij_token_for_permission,
};

/// Build a Set-Cookie header value for the Beam terminal session cookie.
pub(crate) fn build_beam_set_cookie(beam_cookie: &str) -> String {
    format!("{BEAM_COOKIE_NAME}={beam_cookie}; HttpOnly; SameSite=Strict; Path=/s/; Max-Age=86400")
}

/// Call zellij web `/command/login` and return the zellij session cookie.
/// Never logs cookie/token content.
pub(crate) async fn zellij_web_login(
    client: &Client,
    zellij_web_port: u16,
    auth_token: &str,
) -> Result<String, (StatusCode, &'static str)> {
    let login_url = format!("http://127.0.0.1:{zellij_web_port}/command/login");
    let resp = client
        .post(&login_url)
        .json(&serde_json::json!({
            "auth_token": auth_token,
            "remember_me": false,
        }))
        .send()
        .await
        .map_err(|err| {
            warn!(
                component = "terminal_proxy",
                operation = "zellij_web_login",
                outcome = "upstream_unreachable",
                "terminal proxy: zellij login request failed: {err}"
            );
            (StatusCode::BAD_GATEWAY, "zellij login request failed")
        })?;

    let status = resp.status();
    let headers = resp.headers().clone();

    if !status.is_success() {
        warn!(
            component = "terminal_proxy",
            operation = "zellij_web_login",
            outcome = "upstream_error",
            status = status.as_u16(),
            "terminal proxy: zellij login returned HTTP {}",
            status.as_u16()
        );
        return Err((StatusCode::UNAUTHORIZED, "zellij login failed"));
    }

    // Extract the zellij session cookie from Set-Cookie
    let set_cookie = headers
        .get(reqwest_header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(terminal_auth::extract_zellij_set_cookie);

    match set_cookie {
        Some(cookie) => {
            debug!(
                component = "terminal_proxy",
                operation = "zellij_web_login",
                outcome = "success",
                "terminal proxy: zellij login successful"
            );
            Ok(cookie)
        }
        None => {
            warn!(
                component = "terminal_proxy",
                operation = "zellij_web_login",
                outcome = "no_set_cookie",
                "terminal proxy: zellij login succeeded but no Set-Cookie in response"
            );
            Err((StatusCode::BAD_GATEWAY, "zellij login missing Set-Cookie"))
        }
    }
}

/// Extract the Beam cookie from request Cookie header and look up the
/// backend-specific upstream identity. Returns it if valid and bound to the
/// requested session.
pub(crate) async fn authenticate_via_beam_cookie(
    state: &ProxyState,
    session_id: &str,
    headers: &HeaderMap,
) -> Option<AuthenticatedTerminal> {
    let cookie_header = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let beam_cookie = match terminal_auth::extract_beam_cookie(cookie_header) {
        Some(c) => c,
        None => {
            debug!(
                component = "terminal_proxy",
                operation = "cookie_auth",
                outcome = "missing_cookie",
                session_id = session_id,
                "terminal proxy: no beam cookie in request for session {session_id}"
            );
            return None;
        }
    };
    let entry = state.auth_state.lookup(&beam_cookie).await?;
    if entry.session_id != session_id {
        warn!(
            component = "terminal_proxy",
            operation = "cookie_auth",
            outcome = "session_mismatch",
            session_id = session_id,
            "terminal proxy: beam cookie session mismatch: cookie for {} but requested {}",
            entry.session_id,
            session_id
        );
        return None;
    }
    debug!(
        component = "terminal_proxy",
        operation = "cookie_auth",
        outcome = "success",
        session_id = session_id,
        "terminal proxy: beam cookie OK for session {session_id}"
    );
    Some(entry)
}

/// Try to authenticate via ticket, resolve the backend-specific upstream
/// identity, set the Beam cookie, and redirect to the clean URL.
///
/// - Zellij: select token → zellij web login → read-only anchor → store the
///   zellij cookie.
/// - Herdr: no upstream login; store the persisted pane identity directly.
pub(crate) async fn try_ticket_login(
    state: &ProxyState,
    session_id: &str,
    ticket: Option<&str>,
) -> Result<Response, Response> {
    let (permission, upstream) = if let Some(ticket) = ticket {
        debug!(
            component = "terminal_proxy",
            operation = "ticket_auth",
            outcome = "verifying",
            session_id = session_id,
            "terminal proxy: verifying beam ticket for session {session_id}"
        );
        let payload = state
            .auth_state
            .verify_and_consume_ticket(ticket, session_id)
            .await
            .ok_or_else(|| {
                debug!(
                    component = "terminal_proxy",
                    operation = "ticket_auth",
                    outcome = "denied",
                    session_id = session_id,
                    "terminal proxy: ticket verification failed for session {session_id}"
                );
                (
                    StatusCode::UNAUTHORIZED,
                    "invalid or expired terminal ticket",
                )
                    .into_response()
            })?;
        debug!(
            component = "terminal_proxy",
            operation = "ticket_auth",
            outcome = "success",
            session_id = session_id,
            permission = ?payload.permission,
            "terminal proxy: ticket verified for session {session_id} permission={:?}",
            payload.permission
        );

        let session = {
            let sessions = state.sessions.lock().await;
            sessions.get(session_id).cloned()
        }
        .ok_or_else(|| {
            debug!(
                component = "terminal_proxy",
                operation = "ticket_auth",
                outcome = "session_missing",
                session_id = session_id,
                "terminal proxy: session {session_id} not found"
            );
            (StatusCode::NOT_FOUND, "session not found").into_response()
        })?;

        let permission = payload.permission;
        let upstream = match session.backend_kind {
            BackendKind::Zellij => {
                let token = zellij_token_for_permission(&state.zellij_tokens, permission)
                    .ok_or_else(|| {
                        warn!(
                            component = "terminal_proxy",
                            operation = "ticket_auth",
                            outcome = "token_unavailable",
                            session_id = session_id,
                            "terminal proxy: {} unavailable for session {session_id}",
                            unavailable_token_message(permission)
                        );
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            unavailable_token_message(permission),
                        )
                            .into_response()
                    })?;
                debug!(
                    component = "terminal_proxy",
                    operation = "ticket_auth",
                    outcome = "logging_in",
                    session_id = session_id,
                    "terminal proxy: calling zellij web login for session {session_id} permission={permission:?}"
                );
                let zellij_cookie =
                    zellij_web_login(&state.http_client, state.zellij_web_port, token)
                        .await
                        .map_err(|(status, msg)| {
                            warn!(
                                component = "terminal_proxy",
                                operation = "ticket_auth",
                                outcome = "upstream_error",
                                session_id = session_id,
                                status = status.as_u16(),
                                "terminal proxy: zellij web login failed for session {session_id}: {status} {msg}"
                            );
                            (status, msg).into_response()
                        })?;
                debug!(
                    component = "terminal_proxy",
                    operation = "ticket_auth",
                    outcome = "login_ok",
                    session_id = session_id,
                    "terminal proxy: zellij web login OK for session {session_id}"
                );
                UpstreamTarget::Zellij {
                    cookie: zellij_cookie,
                }
            }
            BackendKind::Herdr => {
                let workspace_id = session.herdr_workspace_id.clone().ok_or_else(|| {
                    (StatusCode::NOT_FOUND, "herdr workspace not available").into_response()
                })?;
                let pane_id = session.herdr_pane_id.clone().ok_or_else(|| {
                    (StatusCode::NOT_FOUND, "herdr pane not available").into_response()
                })?;
                UpstreamTarget::Herdr {
                    workspace_id,
                    pane_id,
                }
            }
        };
        (permission, upstream)
    } else {
        return Err((StatusCode::UNAUTHORIZED, "terminal authentication required").into_response());
    };

    // Store in server-side cookie jar and get Beam cookie
    let beam_cookie = state
        .auth_state
        .insert(session_id.to_string(), permission, upstream)
        .await;

    if permission == TerminalPermission::ReadOnly
        && should_ensure_read_only_anchor(permission, &state.zellij_tokens)
        && let Some(zellij_session) = resolve_zellij_session(&state.sessions, session_id).await
    {
        anchor::ensure_read_only_anchor(state, session_id, &zellij_session).await;
    }

    // Build redirect to clean URL (no query params)
    let redirect_url = format!("/s/{session_id}");
    debug!(
        component = "terminal_proxy",
        operation = "ticket_auth",
        outcome = "redirect",
        session_id = session_id,
        "terminal proxy: redirecting {session_id} to {redirect_url}"
    );
    let mut response = Redirect::to(&redirect_url).into_response();
    if let Ok(header_value) = build_beam_set_cookie(&beam_cookie).parse() {
        response
            .headers_mut()
            .insert(HeaderName::from_static("set-cookie"), header_value);
    }
    Ok(response)
}
