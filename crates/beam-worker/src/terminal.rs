use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
    routing::get,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, broadcast};

use crate::backend::SessionBackend;

#[derive(Clone)]
pub struct TerminalState {
    pub backend: Arc<Mutex<Box<dyn SessionBackend>>>,
    pub latest_screen: Arc<RwLock<String>>,
    pub token: String,
    pub updates: broadcast::Sender<String>,
}

#[derive(serde::Deserialize)]
pub struct AuthQuery {
    token: Option<String>,
}

pub async fn serve(state: TerminalState) -> anyhow::Result<SocketAddr> {
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(addr)
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>beam terminal</title>
  <style>
    body { margin: 0; background: #101315; color: #e8ecf1; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    #wrap { display: grid; grid-template-rows: 1fr auto; height: 100vh; }
    pre { margin: 0; padding: 16px; overflow: auto; white-space: pre-wrap; word-break: break-word; }
    form { display: grid; grid-template-columns: 1fr auto; gap: 8px; padding: 12px; border-top: 1px solid #2a2f36; }
    input, button { font: inherit; }
    input { background: #171b21; color: #e8ecf1; border: 1px solid #38414d; padding: 10px 12px; }
    button { background: #d3e36f; color: #121212; border: 0; padding: 10px 14px; font-weight: 600; }
  </style>
</head>
<body>
  <div id="wrap">
    <pre id="screen"></pre>
    <form id="f">
      <input id="i" autocomplete="off" placeholder="type and press Enter" />
      <button type="submit">Send</button>
    </form>
  </div>
  <script>
    const params = new URLSearchParams(window.location.search);
    const token = params.get('token') || '';
    const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws?token=${encodeURIComponent(token)}`);
    const screen = document.getElementById('screen');
    const form = document.getElementById('f');
    const input = document.getElementById('i');
    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'screen') screen.textContent = msg.content;
      } catch {}
    };
    form.addEventListener('submit', (e) => {
      e.preventDefault();
      const value = input.value;
      if (!value) return;
      ws.send(JSON.stringify({ type: 'input', content: value }));
      input.value = '';
    });
  </script>
</body>
</html>"#,
    )
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<AuthQuery>,
    State(state): State<TerminalState>,
) -> impl IntoResponse {
    let allowed = query.token.as_deref() == Some(state.token.as_str());
    ws.on_upgrade(move |socket| handle_socket(socket, allowed, state))
}

async fn handle_socket(mut socket: WebSocket, allowed: bool, state: TerminalState) {
    let screen = state.latest_screen.read().await.clone();
    let _ = socket
        .send(Message::Text(
            serde_json::json!({ "type": "screen", "content": screen })
                .to_string()
                .into(),
        ))
        .await;

    let mut rx = state.updates.subscribe();
    loop {
        tokio::select! {
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Text(text))) if allowed => {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            if value.get("type").and_then(|t| t.as_str()) == Some("input") {
                                if let Some(content) = value.get("content").and_then(|c| c.as_str()) {
                                    let backend = state.backend.clone();
                                    let content = content.to_string();
                                    tokio::spawn(async move {
                                        let guard = backend.lock().await;
                                        let _ = guard.raw_input(&content).await;
                                    });
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
            update = rx.recv() => {
                match update {
                    Ok(content) => {
                        let _ = socket.send(Message::Text(
                            serde_json::json!({ "type": "screen", "content": content }).to_string().into()
                        )).await;
                    }
                    Err(_) => break,
                }
            }
        }
    }
}
