use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

use beam_core::session::Session;

#[derive(Clone)]
struct ProxyState {
    http_client: Client,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

pub async fn start_proxy(
    host: &str,
    port: u16,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
) -> anyhow::Result<u16> {
    let state = ProxyState {
        http_client: Client::new(),
        sessions,
    };

    let app = Router::new()
        .route("/s/{session_id}", get(handle_terminal))
        .route("/s/{session_id}/ws", get(handle_ws))
        .fallback(fallback_404)
        .with_state(state);

    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
    let addr = listener.local_addr()?;
    info!("terminal proxy listening on {host}:{}", addr.port());
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            warn!("terminal proxy server error: {err}");
        }
    });
    Ok(addr.port())
}

async fn fallback_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "not found")
}

async fn handle_terminal(
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let web_port = {
        let sessions = state.sessions.lock().await;
        sessions.get(&session_id).and_then(|s| s.web_port)
    };
    let Some(port) = web_port else {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    };

    let uri = req.uri();
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let worker_url = format!("http://127.0.0.1:{port}/{query}");

    match state.http_client.get(&worker_url).send().await {
        Ok(resp) => {
            let status = resp.status();
            let headers = resp.headers().clone();
            let content_type = headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            let mut body = resp.bytes().await.unwrap_or_default();
            if content_type.starts_with("text/html") {
                if let Ok(html) = String::from_utf8(body.to_vec()) {
                    body = html
                        .replace(
                            "${location.host}/ws?",
                            &format!("${{location.host}}/s/{session_id}/ws?"),
                        )
                        .into_bytes()
                        .into();
                }
            }
            let mut response = Response::new(body.into());
            *response.status_mut() = status;
            for (name, value) in headers.iter() {
                if name != "transfer-encoding" && name != "content-length" {
                    response.headers_mut().insert(name, value.clone());
                }
            }
            response
        }
        Err(err) => {
            warn!("terminal proxy: failed to proxy to worker for session {session_id}: {err}");
            (StatusCode::BAD_GATEWAY, "proxy error").into_response()
        }
    }
}

async fn handle_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let web_port = {
        let sessions = state.sessions.lock().await;
        sessions.get(&session_id).and_then(|s| s.web_port)
    };
    let Some(port) = web_port else {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    };

    let query = req.uri().query();
    let worker_url = if let Some(query) = query.filter(|query| !query.is_empty()) {
        format!("ws://127.0.0.1:{port}/ws?{query}")
    } else {
        format!("ws://127.0.0.1:{port}/ws")
    };

    ws.on_upgrade(move |client_socket| async move {
        match connect_async(&worker_url).await {
            Ok((worker_ws, _)) => {
                relay_ws(client_socket, worker_ws).await;
            }
            Err(err) => {
                warn!(
                    "terminal proxy: failed to connect to worker WS for session {session_id}: {err}"
                );
            }
        }
    })
    .into_response()
}

async fn relay_ws(
    client: WebSocket,
    worker: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let (mut client_sender, mut client_receiver) = client.split();
    let (mut worker_sender, mut worker_receiver) = worker.split();

    loop {
        tokio::select! {
            msg = client_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let _ = worker_sender.send(tokio_tungstenite::tungstenite::Message::Text(text.to_string().into())).await;
                    }
                    Some(Ok(Message::Binary(data))) => {
                        let _ = worker_sender.send(tokio_tungstenite::tungstenite::Message::Binary(data.to_vec().into())).await;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = worker_sender.send(tokio_tungstenite::tungstenite::Message::Ping(data.to_vec().into())).await;
                    }
                    Some(Ok(Message::Pong(data))) => {
                        let _ = worker_sender.send(tokio_tungstenite::tungstenite::Message::Pong(data.to_vec().into())).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let _ = worker_sender.send(tokio_tungstenite::tungstenite::Message::Close(frame.map(|f| tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: f.code.into(),
                            reason: f.reason.to_string().into(),
                        }))).await;
                        break;
                    }
                    Some(Err(_)) | None => break,
                }
            }
            msg = worker_receiver.next() => {
                match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        let _ = client_sender.send(Message::Text(text.to_string().into())).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => {
                        let _ = client_sender.send(Message::Binary(data.to_vec().into())).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(data))) => {
                        let _ = client_sender.send(Message::Ping(data.to_vec().into())).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(data))) => {
                        let _ = client_sender.send(Message::Pong(data.to_vec().into())).await;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame))) => {
                        let _ = client_sender.send(Message::Close(frame.map(|f| axum::extract::ws::CloseFrame {
                            code: f.code.into(),
                            reason: f.reason.to_string().into(),
                        }))).await;
                        break;
                    }
                    Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}
