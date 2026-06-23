use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, header as reqwest_header};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use beam_core::session::Session;

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

fn is_hop_by_hop(name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(&name.as_str().to_lowercase().as_str())
}

#[derive(Clone)]
struct ProxyState {
    http_client: Client,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    zellij_web_port: u16,
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
) -> anyhow::Result<u16> {
    let state = ProxyState {
        http_client: Client::new(),
        sessions,
        zellij_web_port,
    };

    let app = Router::new()
        // Session main page (all methods)
        .route("/s/{session_id}", axum::routing::any(handle_session_terminal))
        .route("/s/{session_id}/", axum::routing::any(handle_session_terminal))
        // Session WebSocket
        .route("/s/{session_id}/ws", axum::routing::any(handle_session_ws))
        // Session sub-paths (assets etc, all methods)
        .route("/s/{session_id}/{*path}", axum::routing::any(handle_session_path))
        // Global WebSocket route (covers zellij web absolute /ws)
        .route("/ws", axum::routing::any(handle_global_ws))
        // Global zellij asset proxy path (rewritten from HTML/JS)
        .route("/_zellij/ws", axum::routing::any(handle_global_ws))
        .route("/_zellij/{*path}", axum::routing::any(handle_global_path))
        // Fallback: proxy all other paths to zellij web (all methods)
        .fallback(handle_fallback_proxy)
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
    sessions
        .get(session_id)
        .map(|s| zellij_session_for_beam(s))
}

/// Build target URL for proxying to zellij web.
fn build_target_url(zellij_web_port: u16, zellij_session: &str, extra_path: &str, query: Option<&str>) -> String {
    let query_str = query.filter(|q| !q.is_empty()).map(|q| format!("?{q}")).unwrap_or_default();
    if extra_path.is_empty() {
        format!("http://127.0.0.1:{zellij_web_port}/{zellij_session}{query_str}")
    } else {
        format!("http://127.0.0.1:{zellij_web_port}/{zellij_session}/{extra_path}{query_str}")
    }
}

/// Forward client headers to the upstream, skipping hop-by-hop headers.
fn forward_request_headers(headers: &HeaderMap) -> reqwest_header::HeaderMap {
    let mut out = reqwest_header::HeaderMap::new();
    for (name, value) in headers.iter() {
        if is_hop_by_hop(name) {
            continue;
        }
        if let Ok(name_str) = name.as_str().parse::<reqwest_header::HeaderName>() {
            let _ = out.insert(name_str, value.clone().into());
        }
    }
    out
}

/// Forward upstream response headers to the client, skipping hop-by-hop.
fn forward_response_headers(dest: &mut HeaderMap, src: &reqwest_header::HeaderMap) {
    for (name, value) in src.iter() {
        let lower = name.as_str().to_lowercase();
        if HOP_BY_HOP.contains(&lower.as_str()) || lower == "content-length" {
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

/// Rewrite zellij-web absolute asset paths to route through our proxy.
fn rewrite_asset_paths(data: &mut Vec<u8>) {
    if let Ok(text) = String::from_utf8(data.clone()) {
        let rewritten = text
            .replace("href=\"/", "href=\"/s/_zellij/")
            .replace("src=\"/", "src=\"/s/_zellij/")
            .replace("url(\"/", "url(\"/s/_zellij/")
            .replace("\"/assets/", "\"/s/_zellij/assets/")
            .replace("\"/api/", "\"/s/_zellij/api/");
        *data = rewritten.into_bytes();
    }
}

/// Core proxy: take an axum Request, build a reqwest request, forward and return response.
async fn proxy_request(
    client: &Client,
    zellij_web_port: u16,
    zellij_session: &str,
    extra_path: &str,
    req: axum::extract::Request,
) -> Response {
    let method = req.method().clone();
    let query = req.uri().query();
    let target_url = build_target_url(zellij_web_port, zellij_session, extra_path, query);
    let req_headers = forward_request_headers(req.headers());

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
            warn!("terminal proxy: failed to proxy {} {}: {err}", method, target_url);
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
        rewrite_asset_paths(&mut body_bytes);
    }

    let mut response = Response::new(axum::body::Body::from(body_bytes));
    *response.status_mut() = status;
    forward_response_headers(response.headers_mut(), &resp_headers);
    response
}

/// Handle /s/{session_id} — serve the terminal page for a beam session (all methods).
async fn handle_session_terminal(
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    };
    proxy_request(&state.http_client, state.zellij_web_port, &zellij_session, "", req).await
}

/// Handle /s/{session_id}/ws — WebSocket upgrade.
async fn handle_session_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        return Err((StatusCode::NOT_FOUND, "session not found"));
    };

    let query = req.uri().query().map(|q| q.to_string());
    let ws_url = if let Some(ref q) = query {
        format!("ws://127.0.0.1:{}/{zellij_session}/ws?{q}", state.zellij_web_port)
    } else {
        format!("ws://127.0.0.1:{}/{zellij_session}/ws", state.zellij_web_port)
    };

    Ok(ws.on_upgrade(move |client_socket| async move {
        match connect_async(&ws_url).await {
            Ok((zellij_ws, _)) => {
                relay_ws(client_socket, zellij_ws).await;
            }
            Err(err) => {
                warn!("terminal proxy: failed to connect to zellij WS for {zellij_session}: {err}");
            }
        }
    }))
}

/// Handle /s/{session_id}/{path} — proxy asset requests (all methods).
async fn handle_session_path(
    State(state): State<ProxyState>,
    Path((session_id, path)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    if session_id == "_zellij" {
        return proxy_request(&state.http_client, state.zellij_web_port, &path, "", req).await;
    }

    let Some(zellij_session) = resolve_zellij_session(&state.sessions, &session_id).await else {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    };

    proxy_request(&state.http_client, state.zellij_web_port, &zellij_session, &path, req).await
}

/// Handle global /ws — WebSocket upgrade for zellij web root.
async fn handle_global_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    req: axum::extract::Request,
) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let zellij_port = state.zellij_web_port;
    let ws_url = if let Some(ref q) = query {
        format!("ws://127.0.0.1:{zellij_port}/{path}?{q}")
    } else {
        format!("ws://127.0.0.1:{zellij_port}/{path}")
    };
    let path_for_log = path.clone();

    ws.on_upgrade(move |client_socket| async move {
        match connect_async(&ws_url).await {
            Ok((zellij_ws, _)) => {
                relay_ws(client_socket, zellij_ws).await;
            }
            Err(err) => {
                warn!("terminal proxy: failed to connect to zellij global WS {path_for_log}: {err}");
            }
        }
    })
}

/// Handle /_zellij/{*path} — proxy to zellij web root for rewritten asset paths.
async fn handle_global_path(
    State(state): State<ProxyState>,
    Path(path): Path<String>,
    req: axum::extract::Request,
) -> Response {
    proxy_request(&state.http_client, state.zellij_web_port, &path, "", req).await
}

/// Fallback handler: proxy all other paths to zellij web root (all methods).
async fn handle_fallback_proxy(
    State(state): State<ProxyState>,
    req: axum::extract::Request,
) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    proxy_request(&state.http_client, state.zellij_web_port, &path, "", req).await
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
