use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, StatusCode, Uri},
    response::{IntoResponse, Redirect, Response},
};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, header as reqwest_header};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{
    ClientRequestBuilder, Message as TungsteniteMessage, error::UrlError,
};
use tracing::{info, warn};

use beam_core::session::Session;

use crate::terminal_auth;
use crate::terminal_auth::{
    BEAM_COOKIE_NAME, TICKET_QUERY_PARAM, TerminalAuthState, TerminalPermission,
};
use crate::zellij_web::ZellijWebTokens;

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
const FALLBACK_TERMINAL_COLS: u16 = 120;
const FALLBACK_TERMINAL_ROWS: u16 = 36;
const TERMINAL_CELL_PIXEL_WIDTH: u16 = 9;
const TERMINAL_CELL_PIXEL_HEIGHT: u16 = 18;

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

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

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

#[derive(Clone)]
struct ProxyState {
    http_client: Client,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    zellij_web_port: u16,
    zellij_tokens: ZellijWebTokens,
    auth_state: TerminalAuthState,
    anchors: ZellijAnchorManager,
}

#[derive(Clone, Default)]
struct ZellijAnchorManager {
    anchors: Arc<Mutex<HashMap<String, ZellijAnchorEntry>>>,
}

struct ZellijAnchorEntry {
    task: JoinHandle<()>,
    started_at: Instant,
}

struct AuthenticatedTerminal {
    zellij_cookie: String,
    permission: TerminalPermission,
}

/// Map a beam session_id to a zellij session name.
fn zellij_session_for_beam(session: &Session) -> String {
    session
        .adopted_from
        .as_ref()
        .and_then(|a| a.zellij_session.clone())
        .unwrap_or_else(|| {
            format!(
                "bmx-{}",
                &session.session_id[..8.min(session.session_id.len())]
            )
        })
}

pub async fn start_proxy(
    host: &str,
    port: u16,
    zellij_web_port: u16,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    zellij_tokens: ZellijWebTokens,
    auth_state: TerminalAuthState,
) -> anyhow::Result<u16> {
    let state = ProxyState {
        http_client: Client::new(),
        sessions,
        zellij_web_port,
        zellij_tokens,
        auth_state,
        anchors: ZellijAnchorManager::default(),
    };

    let app = Router::new()
        // Session main page — handles ticket/cookie auth + proxy
        .route(
            "/s/{session_id}",
            axum::routing::any(handle_session_terminal),
        )
        .route(
            "/s/{session_id}/",
            axum::routing::any(handle_session_terminal),
        )
        // Session-scoped WS to zellij session (e.g. /s/{sid}/ws)
        .route("/s/{session_id}/ws", axum::routing::any(handle_session_ws))
        // Session-scoped WS to zellij root: /ws/terminal/... and /ws/control
        .route(
            "/s/{session_id}/ws/{*rest}",
            axum::routing::any(handle_session_root_ws),
        )
        // Session sub-paths — handles both zellij root APIs and session assets
        .route(
            "/s/{session_id}/{*path}",
            axum::routing::any(handle_session_path),
        )
        .fallback(handle_not_found)
        .with_state(state);

    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
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

/// Resolve beam session_id to zellij session name.
async fn resolve_zellij_session(
    sessions: &Arc<Mutex<HashMap<String, Session>>>,
    session_id: &str,
) -> Option<String> {
    let sessions = sessions.lock().await;
    sessions.get(session_id).map(|s| zellij_session_for_beam(s))
}

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

/// Forward client headers to the upstream, skipping hop-by-hop headers.
/// If `injected_cookie` is provided, adds/overwrites the Cookie header.
fn forward_request_headers(
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
fn forward_response_headers(dest: &mut HeaderMap, src: &reqwest_header::HeaderMap) {
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

/// Determine if the response content is text-like and eligible for path rewriting.
fn is_text_content(content_type: &str) -> bool {
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
fn rewrite_asset_paths(data: &mut Vec<u8>, session_id: Option<&str>) {
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

/// Build a Set-Cookie header value for the Beam terminal session cookie.
fn build_beam_set_cookie(beam_cookie: &str) -> String {
    format!("{BEAM_COOKIE_NAME}={beam_cookie}; HttpOnly; SameSite=Strict; Path=/s/; Max-Age=86400")
}

// ── Zellij login ────────────────────────────────────────────────────────

/// Call zellij web `/command/login` and return the zellij session cookie.
/// Never logs cookie/token content.
async fn zellij_web_login(
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

// ── Read-only render anchor ─────────────────────────────────────────────

fn should_ensure_read_only_anchor(
    permission: TerminalPermission,
    tokens: &ZellijWebTokens,
) -> bool {
    permission == TerminalPermission::ReadOnly
        && tokens
            .write_token
            .as_deref()
            .is_some_and(|token| !token.is_empty())
}

fn build_anchor_control_messages(web_client_id: &str) -> [String; 2] {
    [
        serde_json::json!({
            "web_client_id": web_client_id,
            "payload": {
                "type": "TerminalResize",
                "rows": FALLBACK_TERMINAL_ROWS,
                "cols": FALLBACK_TERMINAL_COLS,
            },
        })
        .to_string(),
        serde_json::json!({
            "web_client_id": web_client_id,
            "payload": {
                "type": "TerminalMetrics",
                "cell_pixel_width": TERMINAL_CELL_PIXEL_WIDTH,
                "cell_pixel_height": TERMINAL_CELL_PIXEL_HEIGHT,
                "text_area_pixel_width": FALLBACK_TERMINAL_COLS * TERMINAL_CELL_PIXEL_WIDTH,
                "text_area_pixel_height": FALLBACK_TERMINAL_ROWS * TERMINAL_CELL_PIXEL_HEIGHT,
            },
        })
        .to_string(),
    ]
}

async fn ensure_read_only_anchor(state: &ProxyState, session_id: &str, zellij_session: &str) {
    if !should_ensure_read_only_anchor(TerminalPermission::ReadOnly, &state.zellij_tokens) {
        warn!("terminal proxy: read-only anchor skipped for {session_id}: write token unavailable");
        return;
    }

    let key = zellij_session.to_string();
    let mut anchors = state.anchors.anchors.lock().await;
    if let Some(entry) = anchors.get(&key) {
        if !entry.task.is_finished() {
            return;
        }
        if entry.started_at.elapsed() < ANCHOR_RESTART_COOLDOWN {
            return;
        }
    }

    let client = state.http_client.clone();
    let zellij_web_port = state.zellij_web_port;
    let write_token = state.zellij_tokens.write_token.clone().unwrap_or_default();
    let zellij_session_for_task = zellij_session.to_string();
    let session_id_for_log = session_id.to_string();

    let task = tokio::spawn(async move {
        if let Err(err) = run_zellij_anchor_client(
            client,
            zellij_web_port,
            zellij_session_for_task.clone(),
            write_token,
        )
        .await
        {
            warn!(
                "terminal proxy: zellij read-only anchor ended for session {} zellij={}: {}",
                session_id_for_log, zellij_session_for_task, err
            );
        }
    });

    anchors.insert(
        key,
        ZellijAnchorEntry {
            task,
            started_at: Instant::now(),
        },
    );
}

async fn run_zellij_anchor_client(
    client: Client,
    zellij_web_port: u16,
    zellij_session: String,
    write_token: String,
) -> anyhow::Result<()> {
    let zellij_cookie = zellij_web_login(&client, zellij_web_port, &write_token)
        .await
        .map_err(|(status, msg)| anyhow::anyhow!("login failed: {status} {msg}"))?;

    let session_url = format!("http://127.0.0.1:{zellij_web_port}/session");
    let session_resp = client
        .post(session_url)
        .header(reqwest_header::COOKIE, zellij_cookie.clone())
        .json(&serde_json::json!({}))
        .send()
        .await?;
    let status = session_resp.status();
    if !status.is_success() {
        anyhow::bail!("create client returned HTTP {}", status.as_u16());
    }
    let body: serde_json::Value = session_resp.json().await?;
    let web_client_id = body
        .get("web_client_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("create client missing web_client_id"))?
        .to_string();

    let control_url = build_ws_target_url(zellij_web_port, "ws/control", None);
    let terminal_url = build_ws_target_url(
        zellij_web_port,
        &format!("ws/terminal/{zellij_session}"),
        Some(&format!("web_client_id={web_client_id}")),
    );

    let mut control_ws = connect_ws_with_cookie(&control_url, Some(&zellij_cookie)).await?;
    for message in build_anchor_control_messages(&web_client_id) {
        control_ws
            .send(TungsteniteMessage::Text(message.into()))
            .await?;
    }
    let mut terminal_ws = connect_ws_with_cookie(&terminal_url, Some(&zellij_cookie)).await?;
    info!("terminal proxy: zellij read-only anchor connected for {zellij_session}");

    loop {
        tokio::select! {
            msg = terminal_ws.next() => {
                match msg {
                    Some(Ok(TungsteniteMessage::Ping(data))) => {
                        let _ = terminal_ws.send(TungsteniteMessage::Pong(data)).await;
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                }
            }
            msg = control_ws.next() => {
                match msg {
                    Some(Ok(TungsteniteMessage::Ping(data))) => {
                        let _ = control_ws.send(TungsteniteMessage::Pong(data)).await;
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                }
            }
        }
    }

    Ok(())
}

// ── Ticket-based login → cookie → redirect ──────────────────────────────

/// Try to authenticate via ticket, call zellij login, set Beam cookie,
/// and redirect to clean URL.
async fn try_ticket_login(
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
            ensure_read_only_anchor(state, session_id, &zellij_session).await;
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

/// Extract the Beam cookie from request Cookie header and look up the
/// corresponding zellij cookie. Returns the zellij cookie value if valid.
async fn authenticate_via_beam_cookie(
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

// ── Handler: /s/{session_id} ────────────────────────────────────────────

/// Handle /s/{session_id} — authenticate and proxy the terminal page.
///
/// Authentication precedence:
/// 1. Beam cookie → authenticate, inject zellij cookie, proxy HTML
/// 2. `?beam_terminal_ticket=` → verify, zellij login, set Beam cookie, redirect
/// 3. No auth → 401
async fn handle_session_terminal(
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
        warn!("terminal proxy: session {session_id} not found");
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    }

    let headers = req.headers().clone();
    let ticket = params.get(TICKET_QUERY_PARAM).map(|s| s.as_str());

    let path = req.uri().path().to_string();
    let has_cookie = headers.get("cookie").is_some();
    info!(
        "terminal proxy: GET {path} session={session_id} ticket={} has_cookie={has_cookie}",
        ticket.is_some()
    );

    // Step 1: Try beam cookie auth (only when no auth query params)
    if ticket.is_none() {
        if let Some(auth) = authenticate_via_beam_cookie(&state, &session_id, &headers).await {
            // Authenticated via cookie — proxy with injected zellij cookie
            info!("terminal proxy: cookie auth OK for session {session_id}, proxying to zellij");
            let zellij_session = resolve_zellij_session(&state.sessions, &session_id)
                .await
                .unwrap();
            if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
                ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
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
            info!("terminal proxy: no valid beam cookie for session {session_id}");
        }
    }

    // Step 2: Try ticket login
    if ticket.is_some() {
        info!("terminal proxy: trying ticket/login for session {session_id}");
        match try_ticket_login(&state, &session_id, ticket).await {
            Ok(response) => {
                info!(
                    "terminal proxy: ticket/login OK for session {session_id}, redirecting with cookie"
                );
                return response;
            }
            Err(error_response) => {
                warn!("terminal proxy: ticket/login failed for session {session_id}");
                return error_response;
            }
        }
    }

    // Step 4: No auth
    warn!("terminal proxy: no auth for session {session_id}, returning 401");
    (
        StatusCode::UNAUTHORIZED,
        "terminal authentication required — provide ?beam_terminal_ticket= or login first",
    )
        .into_response()
}

// ── Handler: /s/{session_id}/ws → zellij session WS ─────────────────────

async fn handle_session_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        warn!("terminal proxy: WS session {session_id} not found");
        return Err((StatusCode::NOT_FOUND, "session not found"));
    };

    // WS auth: check Beam cookie (browsers send cookies on WS upgrade)
    info!("terminal proxy: WS upgrade for session {session_id} zellij={zellij_session}");
    let headers = req.headers().clone();
    let Some(auth) = authenticate_via_beam_cookie(&state, &session_id, &headers).await else {
        warn!("terminal proxy: WS session {session_id} missing cookie");
        return Err((StatusCode::UNAUTHORIZED, "terminal authentication required"));
    };

    if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
        ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
    }

    let query = req.uri().query().map(|q| q.to_string());
    let zellij_web_port = state.zellij_web_port;

    Ok(ws.on_upgrade(move |client_socket| async move {
        let ws_url = build_ws_target_url(
            zellij_web_port,
            &format!("{zellij_session}/ws"),
            query.as_deref(),
        );

        // Connect to zellij WS with optional cookie
        match connect_ws_with_cookie(&ws_url, Some(&auth.zellij_cookie)).await {
            Ok(zellij_ws) => {
                relay_ws(client_socket, zellij_ws).await;
            }
            Err(err) => {
                warn!(
                    "terminal proxy: failed to connect to zellij session WS {zellij_session}: {err}"
                );
            }
        }
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
/// session name (e.g. `bmx-...`) since zellij JS picks up the beam session ID
/// from `location.pathname`.
async fn handle_session_root_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path((session_id, rest)): Path<(String, String)>,
    req: axum::extract::Request,
) -> std::result::Result<impl IntoResponse, (StatusCode, &'static str)> {
    // Resolve actual zellij session name
    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        warn!("terminal proxy: root WS session {session_id} not found");
        return Err((StatusCode::NOT_FOUND, "session not found"));
    };

    // Authenticate via Beam cookie (required — no unauthenticated WS)
    info!("terminal proxy: root WS upgrade for session {session_id} rest={rest}");
    let headers = req.headers().clone();
    let auth = authenticate_via_beam_cookie(&state, &session_id, &headers)
        .await
        .ok_or((StatusCode::UNAUTHORIZED, "terminal authentication required"))?;
    info!("terminal proxy: root WS cookie auth OK for session {session_id}");

    if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
        ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
    }

    // Translate the WS path: replace terminal name with actual zellij session
    let translated_path = terminal_auth::translate_root_ws_path(&rest, &zellij_session);

    let query = req.uri().query().map(|q| q.to_string());
    let zellij_web_port = state.zellij_web_port;
    let rest_for_log = rest.clone();

    Ok(ws.on_upgrade(move |client_socket| async move {
        let ws_url = build_ws_target_url(zellij_web_port, &translated_path, query.as_deref());

        match connect_ws_with_cookie(&ws_url, Some(&auth.zellij_cookie)).await {
            Ok(zellij_ws) => {
                relay_ws(client_socket, zellij_ws).await;
            }
            Err(err) => {
                warn!("terminal proxy: failed to connect to zellij root WS {rest_for_log}: {err}");
            }
        }
    }))
}

/// Connect to a WebSocket URL with an optional Cookie header.
/// Uses tungstenite's `ClientRequestBuilder` to build a proper WS handshake
/// request and then injects the Cookie header.
async fn connect_ws_with_cookie(
    url: &str,
    cookie: Option<&str>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::Error,
> {
    let uri: Uri = url.parse().map_err(|_| {
        tokio_tungstenite::tungstenite::Error::Url(UrlError::UnableToConnect(url.to_string()))
    })?;
    let mut builder = ClientRequestBuilder::new(uri);
    if let Some(cookie) = cookie {
        builder = builder.with_header("Cookie", cookie);
    }

    info!("terminal proxy: connecting WS to {url}");
    let result = connect_async(builder).await.map(|(ws, _)| ws);
    if let Err(ref e) = result {
        warn!("terminal proxy: WS connect to {url} failed: {e}");
    } else {
        info!("terminal proxy: WS connect to {url} OK");
    }
    result
}

// ── Handler: /s/{session_id}/{path} ─────────────────────────────────────

/// Handle /s/{session_id}/{path} — proxy to zellij web.
///
/// Routes to zellij root for known root-level API paths (command, session,
/// info, api) and to the zellij session for everything else (assets, etc.).
async fn handle_session_path(
    State(state): State<ProxyState>,
    Path((session_id, path)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    // All session-scoped paths require Beam cookie authentication.
    // Static assets, APIs, commands — everything needs a valid session cookie.
    info!(
        "terminal proxy: path={} session={session_id} (session-scoped, checking cookie)",
        path
    );
    let Some(auth) = authenticate_via_beam_cookie(&state, &session_id, req.headers()).await else {
        warn!(
            "terminal proxy: path={} session={session_id} missing cookie, returning 401",
            path
        );
        return (StatusCode::UNAUTHORIZED, "terminal authentication required").into_response();
    };
    info!(
        "terminal proxy: path={} session={session_id} cookie OK, proxying",
        path
    );

    if terminal_auth::is_zellij_root_path(&path) {
        if should_ensure_read_only_anchor(auth.permission, &state.zellij_tokens) {
            if let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await
            {
                ensure_read_only_anchor(&state, &session_id, &zellij_session).await;
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

async fn handle_not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

// ── Core proxy functions ────────────────────────────────────────────────

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
            "terminal proxy: rejecting websocket upgrade on HTTP proxy path {} {}",
            method,
            req.uri().path()
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
            warn!("terminal proxy: failed to read request body: {e}");
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
        Err(err) => {
            warn!(
                "terminal proxy: failed to proxy root {} {}: {err}",
                method, target_url
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
            "terminal proxy: rejecting websocket upgrade on HTTP proxy path {} {}",
            method,
            req.uri().path()
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
            warn!("terminal proxy: failed to read request body: {e}");
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
        Err(err) => {
            warn!(
                "terminal proxy: failed to proxy {} {}: {err}",
                method, target_url
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

/// Relay WebSocket messages between client and zellij web.
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

// ── tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_set_cookie_header() {
        // Verify Set-Cookie is in the strip list
        assert!(should_strip_response_header("set-cookie"));
        assert!(should_strip_response_header("Set-Cookie"));
        assert!(should_strip_response_header("SET-COOKIE"));
    }

    #[test]
    fn content_length_not_forwarded() {
        let mut dest = HeaderMap::new();
        let mut src = reqwest_header::HeaderMap::new();
        src.insert(
            reqwest_header::CONTENT_LENGTH,
            reqwest_header::HeaderValue::from_static("42"),
        );
        src.insert(
            reqwest_header::CONTENT_TYPE,
            reqwest_header::HeaderValue::from_static("text/html"),
        );
        forward_response_headers(&mut dest, &src);
        assert!(dest.get("content-length").is_none());
        assert!(dest.get("content-type").is_some());
    }

    #[test]
    fn websocket_handshake_headers_not_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::UPGRADE,
            axum::http::HeaderValue::from_static("websocket"),
        );
        headers.insert(
            axum::http::header::CONNECTION,
            axum::http::HeaderValue::from_static("Upgrade"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("sec-websocket-key"),
            axum::http::HeaderValue::from_static("abc123"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("sec-websocket-version"),
            axum::http::HeaderValue::from_static("13"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("sec-websocket-protocol"),
            axum::http::HeaderValue::from_static("chat"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("sec-websocket-extensions"),
            axum::http::HeaderValue::from_static("permessage-deflate"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("x-forwarded-for"),
            axum::http::HeaderValue::from_static("127.0.0.1"),
        );

        let forwarded = forward_request_headers(&headers, Some("beam=cookie"));
        assert!(forwarded.get("sec-websocket-key").is_none());
        assert!(forwarded.get("sec-websocket-version").is_none());
        assert!(forwarded.get("sec-websocket-protocol").is_none());
        assert!(forwarded.get("sec-websocket-extensions").is_none());
        assert!(forwarded.get("upgrade").is_none());
        assert!(forwarded.get("connection").is_none());
        assert_eq!(
            forwarded
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok()),
            Some("127.0.0.1")
        );
        assert_eq!(
            forwarded.get("cookie").and_then(|v| v.to_str().ok()),
            Some("beam=cookie")
        );
    }

    #[test]
    fn forwarded_headers_replace_external_cookie_with_internal_zellij_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("beam_terminal_session=external-cookie"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("x-request-id"),
            axum::http::HeaderValue::from_static("req-1"),
        );

        let forwarded = forward_request_headers(&headers, Some("zellij-session=internal-cookie"));

        assert_eq!(
            forwarded.get("cookie").and_then(|v| v.to_str().ok()),
            Some("zellij-session=internal-cookie")
        );
        assert_eq!(
            forwarded.get("x-request-id").and_then(|v| v.to_str().ok()),
            Some("req-1")
        );
    }

    #[test]
    fn forward_request_headers_does_not_mutate_original_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("beam_terminal_session=external-cookie"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("x-forwarded-for"),
            axum::http::HeaderValue::from_static("127.0.0.1"),
        );
        let original = headers.clone();

        let _ = forward_request_headers(&headers, Some("zellij-session=internal-cookie"));

        assert_eq!(headers, original);
    }

    #[test]
    fn build_ws_target_url_uses_ws_scheme() {
        assert_eq!(
            build_ws_target_url(8801, "ws/terminal/beam-123", None),
            "ws://127.0.0.1:8801/ws/terminal/beam-123"
        );
        assert_eq!(
            build_ws_target_url(8801, "/ws/control", Some("foo=bar")),
            "ws://127.0.0.1:8801/ws/control?foo=bar"
        );
    }

    #[test]
    fn ticket_permission_selects_matching_zellij_token() {
        let tokens = ZellijWebTokens {
            port: 1234,
            read_only_token: Some("ro-token".to_string()),
            write_token: Some("write-token".to_string()),
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };

        assert_eq!(
            zellij_token_for_permission(&tokens, TerminalPermission::ReadOnly),
            Some("ro-token")
        );
        assert_eq!(
            zellij_token_for_permission(&tokens, TerminalPermission::Write),
            Some("write-token")
        );
    }

    #[test]
    fn ticket_permission_rejects_missing_matching_zellij_token() {
        let tokens = ZellijWebTokens {
            port: 1234,
            read_only_token: Some("ro-token".to_string()),
            write_token: None,
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };

        assert_eq!(
            zellij_token_for_permission(&tokens, TerminalPermission::ReadOnly),
            Some("ro-token")
        );
        assert_eq!(
            zellij_token_for_permission(&tokens, TerminalPermission::Write),
            None
        );
    }

    #[test]
    fn read_only_permission_triggers_anchor_when_write_token_available() {
        let tokens = ZellijWebTokens {
            port: 1234,
            read_only_token: Some("ro-token".to_string()),
            write_token: Some("write-token".to_string()),
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };

        assert!(should_ensure_read_only_anchor(
            TerminalPermission::ReadOnly,
            &tokens
        ));
        assert!(!should_ensure_read_only_anchor(
            TerminalPermission::Write,
            &tokens
        ));
    }

    #[test]
    fn read_only_anchor_is_noop_without_write_token() {
        let tokens = ZellijWebTokens {
            port: 1234,
            read_only_token: Some("ro-token".to_string()),
            write_token: None,
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };

        assert!(!should_ensure_read_only_anchor(
            TerminalPermission::ReadOnly,
            &tokens
        ));
    }

    #[test]
    fn anchor_control_messages_match_zellij_wire_shape() {
        let messages = build_anchor_control_messages("client-1");
        let resize: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
        let metrics: serde_json::Value = serde_json::from_str(&messages[1]).unwrap();

        assert_eq!(resize["web_client_id"], "client-1");
        assert_eq!(resize["payload"]["type"], "TerminalResize");
        assert_eq!(resize["payload"]["rows"], 36);
        assert_eq!(resize["payload"]["cols"], 120);

        assert_eq!(metrics["web_client_id"], "client-1");
        assert_eq!(metrics["payload"]["type"], "TerminalMetrics");
        assert_eq!(metrics["payload"]["cell_pixel_width"], 9);
        assert_eq!(metrics["payload"]["cell_pixel_height"], 18);
        assert_eq!(metrics["payload"]["text_area_pixel_width"], 1080);
        assert_eq!(metrics["payload"]["text_area_pixel_height"], 648);
    }

    #[test]
    fn zellij_root_paths_identified() {
        assert!(terminal_auth::is_zellij_root_path("command/login"));
        assert!(terminal_auth::is_zellij_root_path("session"));
        assert!(terminal_auth::is_zellij_root_path("info"));
        assert!(terminal_auth::is_zellij_root_path("api/status"));
        assert!(terminal_auth::is_zellij_root_path("ws/terminal/mysess"));
        assert!(terminal_auth::is_zellij_root_path("ws/control"));
        // Static assets are root paths
        assert!(terminal_auth::is_zellij_root_path("assets/style.css"));
        assert!(terminal_auth::is_zellij_root_path("assets/auth.js"));
        assert!(terminal_auth::is_zellij_root_path("favicon.ico"));
    }

    #[test]
    fn non_root_paths_identified() {
        assert!(!terminal_auth::is_zellij_root_path(""));
        assert!(!terminal_auth::is_zellij_root_path("ws"));
    }

    #[test]
    fn ws_terminal_path_translated() {
        let result = terminal_auth::translate_root_ws_path("terminal/beam-abc-123", "bmx-beam-ab");
        assert_eq!(result, "ws/terminal/bmx-beam-ab");
    }

    #[test]
    fn ws_control_path_passthrough() {
        let result = terminal_auth::translate_root_ws_path("control", "bmx-xyz");
        assert_eq!(result, "ws/control");
    }

    #[test]
    fn rewrite_base_href_for_session() {
        let mut data = b"<html><head><base href=\"/\"></head><body></body></html>".to_vec();
        rewrite_asset_paths(&mut data, Some("my-session"));
        let result = String::from_utf8(data).unwrap();
        assert!(result.contains("<base href=\"/s/my-session/\">"));
    }

    #[test]
    fn rewrite_base_href_skipped_without_session() {
        let mut data = b"<html><head><base href=\"/\"></head><body></body></html>".to_vec();
        rewrite_asset_paths(&mut data, None);
        let result = String::from_utf8(data).unwrap();
        assert_eq!(
            result,
            "<html><head><base href=\"/\"></head><body></body></html>"
        );
    }
}
