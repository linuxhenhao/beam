//! Ticket/cookie authentication for terminal proxy.
//!
//! Handles zellij web login, beam ticket verification, beam cookie lookup,
//! and the [`AuthenticatedTerminal`] result used by handlers.

use axum::{
    http::{HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Redirect, Response},
};
use reqwest::{Client, header as reqwest_header};
use tracing::{info, warn};

use crate::terminal_auth;
use crate::terminal_auth::{BEAM_COOKIE_NAME, TerminalPermission};

use super::anchor::{self, should_ensure_read_only_anchor};
use super::{
    ProxyState, resolve_zellij_session, unavailable_token_message, zellij_token_for_permission,
};

/// Result of authenticating a request via beam cookie.
pub(crate) struct AuthenticatedTerminal {
    pub(crate) zellij_cookie: String,
    pub(crate) permission: TerminalPermission,
}

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
            warn!("terminal proxy: zellij login request failed: {err}");
            (StatusCode::BAD_GATEWAY, "zellij login request failed")
        })?;

    let status = resp.status();
    let headers = resp.headers().clone();

    if !status.is_success() {
        warn!(
            "terminal proxy: zellij login returned HTTP {}",
            status.as_u16()
        );
        return Err((StatusCode::UNAUTHORIZED, "zellij login failed"));
    }

    // Extract the zellij session cookie from Set-Cookie
    let set_cookie = headers
        .get(reqwest_header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| terminal_auth::extract_zellij_set_cookie(v));

    match set_cookie {
        Some(cookie) => {
            info!("terminal proxy: zellij login successful");
            Ok(cookie)
        }
        None => {
            warn!("terminal proxy: zellij login succeeded but no Set-Cookie in response");
            Err((StatusCode::BAD_GATEWAY, "zellij login missing Set-Cookie"))
        }
    }
}

/// Extract the Beam cookie from request Cookie header and look up the
/// corresponding zellij cookie. Returns the zellij cookie value if valid.
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
            info!("terminal proxy: no beam cookie in request for session {session_id}");
            return None;
        }
    };
    let (zellij_cookie, stored_session_id, permission) =
        state.auth_state.lookup(&beam_cookie).await?;
    // Verify the cookie is for the requested session
    if stored_session_id != session_id {
        warn!(
            "terminal proxy: beam cookie session mismatch: cookie for {} but requested {}",
            stored_session_id, session_id
        );
        return None;
    }
    info!("terminal proxy: beam cookie OK for session {session_id}");
    Some(AuthenticatedTerminal {
        zellij_cookie,
        permission,
    })
}

/// Try to authenticate via ticket, call zellij login, set Beam cookie,
/// and redirect to clean URL.
pub(crate) async fn try_ticket_login(
    state: &ProxyState,
    session_id: &str,
    ticket: Option<&str>,
) -> Result<Response, Response> {
    // Determine auth token and permission
    let (auth_token, permission): (String, TerminalPermission) = if let Some(ticket) = ticket {
        // New flow: verify ticket
        info!("terminal proxy: verifying beam ticket for session {session_id}");
        let payload = state
            .auth_state
            .verify_and_consume_ticket(ticket, session_id)
            .await
            .ok_or_else(|| {
                warn!("terminal proxy: ticket verification failed for session {session_id}");
                (
                    StatusCode::UNAUTHORIZED,
                    "invalid or expired terminal ticket",
                )
                    .into_response()
            })?;
        info!(
            "terminal proxy: ticket verified for session {session_id} permission={:?}",
            payload.permission
        );
        let token = zellij_token_for_permission(&state.zellij_tokens, payload.permission)
            .ok_or_else(|| {
                warn!(
                    "terminal proxy: {} unavailable for session {session_id}",
                    unavailable_token_message(payload.permission)
                );
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    unavailable_token_message(payload.permission),
                )
                    .into_response()
            })?;
        (token.to_string(), payload.permission)
    } else {
        return Err((StatusCode::UNAUTHORIZED, "terminal authentication required").into_response());
    };

    // Call zellij web login
    info!(
        "terminal proxy: calling zellij web login for session {session_id} permission={permission:?}"
    );
    let zellij_cookie = zellij_web_login(&state.http_client, state.zellij_web_port, &auth_token)
        .await
        .map_err(|(status, msg)| {
            warn!(
                "terminal proxy: zellij web login failed for session {session_id}: {status} {msg}"
            );
            (status, msg).into_response()
        })?;
    info!("terminal proxy: zellij web login OK for session {session_id}");

    // Store in server-side cookie jar and get Beam cookie
    let beam_cookie = state
        .auth_state
        .insert(zellij_cookie, session_id.to_string(), permission)
        .await;

    if should_ensure_read_only_anchor(permission, &state.zellij_tokens) {
        if let Some(zellij_session) = resolve_zellij_session(&state.sessions, session_id).await {
            anchor::ensure_read_only_anchor(state, session_id, &zellij_session).await;
        }
    }

    // Build redirect to clean URL (no query params)
    let redirect_url = format!("/s/{session_id}");
    info!("terminal proxy: redirecting {session_id} to {redirect_url}");
    let mut response = Redirect::to(&redirect_url).into_response();
    if let Ok(header_value) = build_beam_set_cookie(&beam_cookie).parse() {
        response
            .headers_mut()
            .insert(HeaderName::from_static("set-cookie"), header_value);
    }
    Ok(response)
}
