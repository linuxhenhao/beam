//! HTTP forwarding for terminal proxy.
//!
//! Functions for proxying HTTP requests to the upstream zellij web server,
//! including header forwarding/rewriting, body rewriting (base href), and
//! the axum handlers for session-terminal pages and session sub-paths.

use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Response},
};
use reqwest::{Client, header as reqwest_header};
use tracing::{debug, warn};

use crate::terminal_auth;
use crate::terminal_auth::TICKET_QUERY_PARAM;

use super::anchor::{self, should_ensure_read_only_anchor};
use super::auth;
use super::{
    HOP_BY_HOP, ProxyState, build_root_target_url, build_target_url, is_hop_by_hop,
    is_websocket_handshake_header, is_websocket_upgrade, resolve_zellij_session,
    should_strip_response_header,
};

// ── Header forwarding ────────────────────────────────────────────────────

/// Forward client headers to the upstream, skipping hop-by-hop headers.
/// If `injected_cookie` is provided, adds/overwrites the Cookie header.
pub(crate) fn forward_request_headers(
    headers: &HeaderMap,
    injected_cookie: Option<&str>,
) -> reqwest_header::HeaderMap {
    let mut out = reqwest_header::HeaderMap::new();
    for (name, value) in headers.iter() {
        if is_hop_by_hop(name) {
            continue;
        }
        if is_websocket_handshake_header(name) {
            continue;
        }
        // Skip the client's Cookie header — we inject our own server-side cookie.
        if name.as_str().eq_ignore_ascii_case("cookie") {
            continue;
        }
        if let Ok(name_str) = name.as_str().parse::<reqwest_header::HeaderName>() {
            let _ = out.insert(name_str, value.clone().into());
        }
    }
    // Inject server-side zellij cookie if available
    if let Some(cookie) = injected_cookie {
        if let Ok(header_name) = reqwest_header::HeaderName::from_bytes(b"cookie") {
            if let Ok(header_value) = reqwest_header::HeaderValue::from_str(cookie) {
                let _ = out.insert(header_name, header_value);
            }
        }
    }
    out
}

/// Forward upstream response headers to the client, skipping hop-by-hop
/// and stripping zellij Set-Cookie (security: never leak zellij cookie).
pub(crate) fn forward_response_headers(dest: &mut HeaderMap, src: &reqwest_header::HeaderMap) {
    for (name, value) in src.iter() {
        let lower = name.as_str().to_lowercase();
        if HOP_BY_HOP.contains(&lower.as_str())
            || lower == "content-length"
            || should_strip_response_header(&lower)
        {
            continue;
        }
        if let Ok(hname) = HeaderName::from_bytes(name.as_str().as_bytes()) {
            let _ = dest.insert(hname, value.clone().into());
        }
    }
}

// ── Body rewriting ───────────────────────────────────────────────────────

/// Determine if the response content is text-like and eligible for path rewriting.
pub(crate) fn is_text_content(content_type: &str) -> bool {
    content_type.starts_with("text/html")
        || content_type.starts_with("text/css")
        || content_type.starts_with("text/javascript")
        || content_type.starts_with("application/javascript")
        || content_type.starts_with("application/json")
}

/// Rewrite zellij-web paths to route through our session-scoped proxy.
///
/// - Rewrites `<base href="/">` to `<base href="/s/{session_id}/">` so zellij
///   JS calls go through authenticated proxy paths.
/// - Rewrites absolute asset paths to `/s/{session_id}/...`.
pub(crate) fn rewrite_asset_paths(data: &mut Vec<u8>, session_id: Option<&str>) {
    let Some(sid) = session_id else {
        return;
    };
    if let Ok(text) = String::from_utf8(data.clone()) {
        let mut rewritten = text;
        let session_prefix = format!("/s/{sid}/");
        rewritten = rewritten
            .replace("href=\"/", &format!("href=\"{session_prefix}"))
            .replace("src=\"/", &format!("src=\"{session_prefix}"))
            .replace("url(\"/", &format!("url(\"{session_prefix}"))
            .replace("\"/assets/", &format!("\"{session_prefix}assets/"))
            .replace("\"/api/", &format!("\"{session_prefix}api/"));
        *data = rewritten.into_bytes();
    }
}

// ── Core proxy functions ─────────────────────────────────────────────────

/// Proxy a request with an injected zellij cookie and optional base href rewrite.
async fn proxy_request_with_cookie(
    client: &Client,
    zellij_web_port: u16,
    zellij_session: &str,
    extra_path: &str,
    req: axum::extract::Request,
    zellij_cookie: &str,
    session_id_for_rewrite: Option<&str>,
) -> Response {
    proxy_request_raw(
        client,
        zellij_web_port,
        zellij_session,
        extra_path,
        req,
        Some(zellij_cookie),
        session_id_for_rewrite,
    )
    .await
}

/// Proxy a request to zellij web root (no session prefix).
async fn proxy_to_zellij_root(
    client: &Client,
    zellij_web_port: u16,
    path: &str,
    req: axum::extract::Request,
    injected_cookie: Option<&str>,
    session_id_for_rewrite: Option<&str>,
) -> Response {
    let method = req.method().clone();
    let query = req.uri().query();
    if is_websocket_upgrade(req.headers()) {
        warn!(
            component = "terminal_proxy",
            operation = "http_proxy",
            outcome = "protocol_error",
            "terminal proxy: rejecting websocket upgrade on HTTP proxy path"
        );
        return (
            StatusCode::UPGRADE_REQUIRED,
            "websocket upgrade must use the websocket proxy endpoint",
        )
            .into_response();
    }
    let target_url = build_root_target_url(zellij_web_port, path, query);
    let req_headers = forward_request_headers(req.headers(), injected_cookie);

    let body_bytes = match axum::body::to_bytes(req.into_body(), 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            warn!(
                component = "terminal_proxy",
                operation = "http_proxy",
                outcome = "bad_request",
                "terminal proxy: failed to read request body: {e}"
            );
            return (StatusCode::BAD_REQUEST, "failed to read request body").into_response();
        }
    };

    let mut upstream_req = client
        .request(method.clone(), &target_url)
        .headers(req_headers);
    if !body_bytes.is_empty() {
        upstream_req = upstream_req.body(body_bytes.to_vec());
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(resp) => resp,
        Err(_err) => {
            warn!(
                component = "terminal_proxy",
                operation = "http_proxy",
                outcome = "upstream_error",
                "terminal proxy: failed to proxy root request"
            );
            return (StatusCode::BAD_GATEWAY, "proxy error").into_response();
        }
    };

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();
    let content_type = resp_headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    let mut body_bytes = upstream_resp.bytes().await.unwrap_or_default().to_vec();

    if is_text_content(content_type) {
        rewrite_asset_paths(&mut body_bytes, session_id_for_rewrite);
    }

    let mut response = Response::new(axum::body::Body::from(body_bytes));
    *response.status_mut() = status;
    forward_response_headers(response.headers_mut(), &resp_headers);
    response
}

/// Core proxy: take an axum Request, build a reqwest request, forward and return response.
/// Optionally injects a zellij cookie header and rewrites base href for a session.
async fn proxy_request_raw(
    client: &Client,
    zellij_web_port: u16,
    zellij_session: &str,
    extra_path: &str,
    req: axum::extract::Request,
    injected_cookie: Option<&str>,
    session_id_for_rewrite: Option<&str>,
) -> Response {
    let method = req.method().clone();
    let query = req.uri().query();
    if is_websocket_upgrade(req.headers()) {
        warn!(
            component = "terminal_proxy",
            operation = "http_proxy",
            outcome = "protocol_error",
            "terminal proxy: rejecting websocket upgrade on HTTP proxy path"
        );
        return (
            StatusCode::UPGRADE_REQUIRED,
            "websocket upgrade must use the websocket proxy endpoint",
        )
            .into_response();
    }
    let target_url = build_target_url(zellij_web_port, zellij_session, extra_path, query);
    let req_headers = forward_request_headers(req.headers(), injected_cookie);

    // Collect body bytes
    let body_bytes = match axum::body::to_bytes(req.into_body(), 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            warn!(
                component = "terminal_proxy",
                operation = "http_proxy",
                outcome = "bad_request",
                "terminal proxy: failed to read request body: {e}"
            );
            return (StatusCode::BAD_REQUEST, "failed to read request body").into_response();
        }
    };

    // Build reqwest request
    let mut upstream_req = client
        .request(method.clone(), &target_url)
        .headers(req_headers);
    if !body_bytes.is_empty() {
        upstream_req = upstream_req.body(body_bytes.to_vec());
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(resp) => resp,
        Err(_err) => {
            warn!(
                component = "terminal_proxy",
                operation = "http_proxy",
                outcome = "upstream_error",
                "terminal proxy: failed to proxy request"
            );
            return (StatusCode::BAD_GATEWAY, "proxy error").into_response();
        }
    };

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();
    let content_type = resp_headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    let mut body_bytes = upstream_resp.bytes().await.unwrap_or_default().to_vec();

    // For text-like responses, rewrite asset paths
    if is_text_content(content_type) {
        rewrite_asset_paths(&mut body_bytes, session_id_for_rewrite);
    }

    let mut response = Response::new(axum::body::Body::from(body_bytes));
    *response.status_mut() = status;
    forward_response_headers(response.headers_mut(), &resp_headers);
    response
}

// ── Handler: /s/{session_id} ────────────────────────────────────────────

/// Handle /s/{session_id} — authenticate and proxy the terminal page.
///
/// Authentication precedence:
/// 1. Beam cookie → authenticate, inject zellij cookie, proxy HTML
/// 2. `?beam_terminal_ticket=` → verify, zellij login, set Beam cookie, redirect
/// 3. No auth → 401
pub(crate) async fn handle_session_terminal(
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    req: axum::extract::Request,
) -> Response {
    // Check if zellij session exists
    if resolve_zellij_session(&state.sessions, &session_id)
        .await
        .is_none()
    {
        debug!(
            component = "terminal_proxy",
            operation = "http_request",
            outcome = "not_found",
            session_id = session_id,
            "terminal proxy: session {session_id} not found"
        );
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    }

    let headers = req.headers().clone();
    let ticket = params.get(TICKET_QUERY_PARAM).map(|s| s.as_str());

    let path = req.uri().path().to_string();
    let has_cookie = headers.get("cookie").is_some();
    debug!(
        component = "terminal_proxy",
        operation = "http_request",
        outcome = "received",
        session_id = session_id,
        "terminal proxy: GET {path} session={session_id} ticket={} has_cookie={has_cookie}",
        ticket.is_some()
    );

    // Step 1: Try beam cookie auth (only when no auth query params)
    if ticket.is_none() {
        if let Some(auth) = auth::authenticate_via_beam_cookie(&state, &session_id, &headers).await
        {
            // Authenticated via cookie — proxy with injected zellij cookie
            debug!(
                component = "terminal_proxy",
                operation = "http_request",
                outcome = "success",
                session_id = session_id,
                "terminal proxy: cookie auth OK for session {session_id}, proxying to zellij"
            );
            let zellij_session = resolve_zellij_session(&state.sessions, &session_id)
                .await
                .unwrap();
            if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
                anchor::ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
            }
            return proxy_request_with_cookie(
                &state.http_client,
                state.zellij_web_port,
                &zellij_session,
                "",
                req,
                &auth.zellij_cookie,
                Some(&session_id), // rewrite base href for this session
            )
            .await;
        } else {
            debug!(
                component = "terminal_proxy",
                operation = "http_request",
                outcome = "missing_cookie",
                session_id = session_id,
                "terminal proxy: no valid beam cookie for session {session_id}"
            );
        }
    }

    // Step 2: Try ticket login
    if ticket.is_some() {
        debug!(
            component = "terminal_proxy",
            operation = "http_request",
            outcome = "ticket_login",
            session_id = session_id,
            "terminal proxy: trying ticket/login for session {session_id}"
        );
        match auth::try_ticket_login(&state, &session_id, ticket).await {
            Ok(response) => {
                debug!(
                    component = "terminal_proxy",
                    operation = "http_request",
                    outcome = "success",
                    session_id = session_id,
                    "terminal proxy: ticket/login OK for session {session_id}, redirecting with cookie"
                );
                return response;
            }
            Err(error_response) => {
                debug!(
                    component = "terminal_proxy",
                    operation = "http_request",
                    outcome = "auth_failed",
                    session_id = session_id,
                    "terminal proxy: ticket/login failed for session {session_id}"
                );
                return error_response;
            }
        }
    }

    // Step 4: No auth
    debug!(
        component = "terminal_proxy",
        operation = "http_request",
        outcome = "denied",
        session_id = session_id,
        "terminal proxy: no auth for session {session_id}, returning 401"
    );
    (
        StatusCode::UNAUTHORIZED,
        "terminal authentication required — provide ?beam_terminal_ticket= or login first",
    )
        .into_response()
}

// ── Handler: /s/{session_id}/{path} ──────────────────────────────────────

/// Handle /s/{session_id}/{path} — proxy to zellij web.
///
/// Routes to zellij root for known root-level API paths (command, session,
/// info, api) and to the zellij session for everything else (assets, etc.).
pub(crate) async fn handle_session_path(
    State(state): State<ProxyState>,
    Path((session_id, path)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    // All session-scoped paths require Beam cookie authentication.
    // Static assets, APIs, commands — everything needs a valid session cookie.
    debug!(
        component = "terminal_proxy",
        operation = "http_request",
        outcome = "received",
        session_id = session_id,
        "terminal proxy: path={} session={session_id} (session-scoped, checking cookie)",
        path
    );
    let Some(auth) = auth::authenticate_via_beam_cookie(&state, &session_id, req.headers()).await
    else {
        debug!(
            component = "terminal_proxy",
            operation = "http_request",
            outcome = "missing_cookie",
            session_id = session_id,
            "terminal proxy: path={} session={session_id} missing cookie, returning 401",
            path
        );
        return (StatusCode::UNAUTHORIZED, "terminal authentication required").into_response();
    };
    debug!(
        component = "terminal_proxy",
        operation = "http_request",
        outcome = "success",
        session_id = session_id,
        "terminal proxy: path={} session={session_id} cookie OK, proxying",
        path
    );

    if terminal_auth::is_zellij_root_path(&path) {
        if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
            if let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await
            {
                anchor::ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
            }
        }
        // Proxy to zellij web root (e.g. /assets/..., /command/login, /session, /info, /api/...)
        proxy_to_zellij_root(
            &state.http_client,
            state.zellij_web_port,
            &path,
            req,
            Some(&auth.zellij_cookie),
            Some(&session_id),
        )
        .await
    } else {
        // Proxy to zellij session path (rare — most paths go to root)
        let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await
        else {
            return (StatusCode::NOT_FOUND, "session not found").into_response();
        };
        proxy_request_raw(
            &state.http_client,
            state.zellij_web_port,
            &zellij_session,
            &path,
            req,
            Some(&auth.zellij_cookie),
            None,
        )
        .await
    }
}

// ── Handler: fallback ────────────────────────────────────────────────────

pub(crate) async fn handle_not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}
