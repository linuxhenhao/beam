use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::header,
    response::{Html, IntoResponse, Response},
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
    #[allow(dead_code)]
    pub updates: broadcast::Sender<String>,
}

#[derive(serde::Deserialize)]
pub struct AuthQuery {
    token: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum TerminalClientMessage {
    Input(String),
    Keys(Vec<String>),
    Write(String),
}

fn parse_client_message(text: &str) -> Option<TerminalClientMessage> {
    let value = serde_json::from_str::<serde_json::Value>(text).ok()?;
    match value.get("type").and_then(|t| t.as_str()) {
        Some("input") => value
            .get("content")
            .and_then(|c| c.as_str())
            .filter(|c| !c.is_empty())
            .map(|content| TerminalClientMessage::Input(content.to_string())),
        Some("keys") => {
            let keys = value
                .get("keys")
                .and_then(|keys| keys.as_array())?
                .iter()
                .filter_map(|key| key.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>();
            (!keys.is_empty()).then_some(TerminalClientMessage::Keys(keys))
        }
        Some("write") => {
            let content = value
                .get("content")
                .and_then(|c| c.as_str())
                .unwrap_or_default();
            if content.is_empty() {
                None
            } else {
                Some(TerminalClientMessage::Write(content.to_string()))
            }
        }
        _ => None,
    }
}

pub async fn serve(state: TerminalState) -> anyhow::Result<SocketAddr> {
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .route("/assets/xterm/xterm.js", get(serve_xterm_js))
        .route("/assets/xterm/xterm.css", get(serve_xterm_css))
        .route("/assets/xterm/addon-fit.js", get(serve_addon_fit_js))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(addr)
}

async fn serve_xterm_js() -> impl IntoResponse {
    let mut res = Response::new(axum::body::Body::from(
        include_str!("../assets/xterm/xterm.js"),
    ));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/javascript; charset=utf-8"),
    );
    res
}

async fn serve_xterm_css() -> impl IntoResponse {
    let mut res = Response::new(axum::body::Body::from(
        include_str!("../assets/xterm/xterm.css"),
    ));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/css; charset=utf-8"),
    );
    res
}

async fn serve_addon_fit_js() -> impl IntoResponse {
    let mut res = Response::new(axum::body::Body::from(
        include_str!("../assets/xterm/addon-fit.js"),
    ));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/javascript; charset=utf-8"),
    );
    res
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>beam terminal</title>
  <link rel="stylesheet" href="/assets/xterm/xterm.css">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #101315; color: #e8ecf1; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    #wrap { display: grid; grid-template-rows: 1fr auto; height: 100vh; }
    #terminal { padding: 4px 4px 0 8px; overflow: auto; }
    #status { min-height: 22px; padding: 4px 12px 6px; border-top: 1px solid #2a2f36; color: #9aa8b7; font-size: 12px; line-height: 1.4; }
    #status.ok { color: #8fd19e; }
    #status.err { color: #ff9a9a; }
    #status.ro { color: #f0c674; }
    /* Override xterm IME composition view: readable on dark theme, not a black block */
    .xterm .composition-view {
      background: #e8ecf1;
      color: #101315;
      border-radius: 3px;
      padding: 0 2px;
      z-index: 10;
    }
  </style>
</head>
<body>
  <div id="wrap">
    <div id="terminal"></div>
    <div id="status">connecting</div>
  </div>
  <script src="/assets/xterm/xterm.js"></script>
  <script src="/assets/xterm/addon-fit.js"></script>
  <script>
    (function() {
      const params = new URLSearchParams(window.location.search);
      const token = params.get('token') || '';

      const term = new Terminal({
        cols: 160,
        rows: 40,
        cursorBlink: true,
        disableStdin: false,
        allowProposedApi: true,
        fontSize: 14,
        fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
        theme: {
          background: '#101315',
          foreground: '#e8ecf1',
          cursor: '#e8ecf1',
          selectionBackground: '#3a4450',
          black: '#1a1b26',
          red: '#f7768e',
          green: '#9ece6a',
          yellow: '#e0af68',
          blue: '#7aa2f7',
          magenta: '#bb9af7',
          cyan: '#7dcfff',
          white: '#a9b1d6',
          brightBlack: '#414868',
          brightRed: '#f7768e',
          brightGreen: '#9ece6a',
          brightYellow: '#e0af68',
          brightBlue: '#7aa2f7',
          brightMagenta: '#bb9af7',
          brightCyan: '#7dcfff',
          brightWhite: '#c0caf5',
        },
      });

      // Resolve FitAddon constructor from UMD export.
      // window.FitAddon may be { FitAddon: class, __esModule: true } (ESM re-export via UMD),
      // or the class itself (older / simpler UMD).
      const FitAddonCtor = window.FitAddon && (window.FitAddon.FitAddon || window.FitAddon);
      let fitAddon = null;
      if (typeof FitAddonCtor === 'function') {
        fitAddon = new FitAddonCtor();
        term.loadAddon(fitAddon);
      }

      term.open(document.getElementById('terminal'));

      // IME composition tracking: defer full-screen redraws during composition
      // to avoid breaking IME preedit position / composition view.
      let isComposing = false;
      let pendingScreen = null;

      // Strip a single trailing newline sequence from captured viewport text.
      // tmux capture-pane and other backends typically emit output that ends
      // with a newline.  Writing that trailing CRLF/LF into a fixed-size xterm
      // whose rows already equal the viewport height would cause an extra
      // line-feed at the bottom right corner, scrolling the whole screen up
      // by one row and misaligning content and cursor.  Removing only the
      // final newline (without touching intentional blank lines or trailing
      // spaces) prevents the spurious scroll.
      const normalizeCapturedScreen = (content) => {
        if (content.endsWith('\r\n')) return content.slice(0, -2);
        if (content.endsWith('\n'))    return content.slice(0, -1);
        return content;
      };

      // Render a screen message: reset terminal, write content, then move
      // cursor to the real TUI cursor position if provided.
      const renderScreen = (msg) => {
        const content = normalizeCapturedScreen(msg.content || '');
        if (msg.cursor && typeof msg.cursor.x === 'number' && typeof msg.cursor.y === 'number') {
          // Write content first, then move cursor via CUP (Cursor Position)
          // sequence.  ANSI CUP coordinates are 1-based, backend sends 0-based.
          term.reset();
          term.write(content, () => {
            term.write(
              '\x1b[' + (msg.cursor.y + 1) + ';' + (msg.cursor.x + 1) + 'H'
            );
          });
        } else {
          term.reset();
          term.write(content);
        }
      };

      // Listen for IME composition on the terminal's hidden textarea.
      // Defer screen redraws during composition so the IME preedit view
      // is not repositioned/destroyed by term.reset() + term.write().
      const helperTextarea = document.querySelector('#terminal .xterm-helper-textarea');
      if (helperTextarea) {
        helperTextarea.addEventListener('compositionstart', () => {
          isComposing = true;
        });
        helperTextarea.addEventListener('compositionend', () => {
          isComposing = false;
          if (pendingScreen !== null) {
            const screen = pendingScreen;
            pendingScreen = null;
            try {
              renderScreen(screen);
            } catch (_) {
              pendingScreen = screen;
            }
          }
        });
      }

      const status = document.getElementById('status');
      let writable = false;
      let ws = null;

      const setStatus = (text, kind) => {
        status.textContent = text;
        status.className = kind || '';
      };

      const connect = () => {
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

        const wsUrl = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws?token=${encodeURIComponent(token)}`;
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          setStatus('connected', 'ok');
          term.focus();
        };

        ws.onclose = () => {
          setStatus('disconnected - reconnecting...', 'err');
          setTimeout(connect, 2000);
        };

        ws.onerror = () => {
          setStatus('connection error', 'err');
        };

        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            // Raw ANSI incremental stream — write directly, no reset.
            // Real terminal output continues during IME composition;
            // only full-screen redraws (screen type) are deferred.
            if (msg.type === 'output') {
              term.write(msg.content || '');
            }
            if (msg.type === 'screen') {
              if (isComposing) {
                // Defer full redraw — IME composition view is active.
                // Store the full msg so cursor is preserved on flush.
                pendingScreen = msg;
              } else {
                renderScreen(msg);
              }
            }
            if (msg.type === 'auth') {
              writable = !!msg.writable;
              if (!isComposing) {
                setStatus(
                  writable ? 'connected - writable' : 'read only',
                  writable ? 'ok' : 'ro'
                );
              }
            }
            if (msg.type === 'sent' && !isComposing) setStatus(`sent ${msg.action || 'input'}`, 'ok');
            if (msg.type === 'error' && !isComposing) setStatus(msg.message || 'send failed', 'err');
          } catch (_) {}
        };
      };

      term.onData((data) => {
        if (!writable) {
          if (!isComposing) setStatus('read only', 'ro');
          return;
        }
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'write', content: data }));
        }
      });

      // FitAddon is loaded but intentionally not used: keep fixed 160x40
      // dimensions matching the backend to prevent cursor/text misalignment.

      // Auto-focus terminal on click anywhere
      document.addEventListener('click', () => {
        try { term.focus(); } catch (_) {}
      });

      setTimeout(() => {
        term.focus();
      }, 100);

      connect();
    })();
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

fn build_screen_message(content: &str, cursor: Option<(u16, u16)>) -> String {
    let mut msg = serde_json::json!({ "type": "screen", "content": content });
    if let Some((x, y)) = cursor {
        msg["cursor"] = serde_json::json!({ "x": x, "y": y });
    }
    msg.to_string()
}

async fn read_cursor(backend: &Arc<Mutex<Box<dyn SessionBackend>>>) -> Option<(u16, u16)> {
    backend
        .lock()
        .await
        .cursor_position()
        .await
        .unwrap_or(None)
}

async fn handle_socket(mut socket: WebSocket, allowed: bool, state: TerminalState) {
    // Send initial screen snapshot for xterm.js to populate the current viewport.
    let screen = state.latest_screen.read().await.clone();
    let cursor = read_cursor(&state.backend).await;
    let _ = socket
        .send(Message::Text(
            build_screen_message(&screen, cursor).into(),
        ))
        .await;
    let _ = socket
        .send(Message::Text(
            serde_json::json!({ "type": "auth", "writable": allowed })
                .to_string()
                .into(),
        ))
        .await;

    // Subscribe to the raw ANSI incremental stream from the backend (tmux pipe-pane,
    // pty stdout/stderr, etc.) instead of the slow capture_viewport poll loop.
    let mut raw_rx = { state.backend.lock().await.subscribe() };
    loop {
        tokio::select! {
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Text(text))) if allowed => {
                        if let Some(message) = parse_client_message(&text) {
                            let backend = state.backend.clone();
                            let result = tokio::spawn(async move {
                                let guard = backend.lock().await;
                                match message {
                                    TerminalClientMessage::Input(content) => {
                                        guard.raw_input(&content).await.map(|_| "input")
                                    }
                                    TerminalClientMessage::Keys(keys) => {
                                        guard.send_special_keys(&keys).await.map(|_| "keys")
                                    }
                                    TerminalClientMessage::Write(content) => {
                                        guard.write_raw(&content).await.map(|_| "write")
                                    }
                                }
                            })
                            .await;
                            match result {
                                Ok(Ok(action)) => {
                                    let _ = socket.send(Message::Text(
                                        serde_json::json!({ "type": "sent", "action": action }).to_string().into()
                                    )).await;
                                }
                                Ok(Err(err)) => {
                                    let _ = socket.send(Message::Text(
                                        serde_json::json!({ "type": "error", "message": format!("send failed: {err}") }).to_string().into()
                                    )).await;
                                }
                                Err(err) => {
                                    let _ = socket.send(Message::Text(
                                        serde_json::json!({ "type": "error", "message": format!("send task failed: {err}") }).to_string().into()
                                    )).await;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
            chunk = raw_rx.recv() => {
                match chunk {
                    Ok(chunk) => {
                        // Send raw ANSI incremental output — xterm.js will render it
                        // directly without reset. Cursor position is maintained by
                        // ANSI sequences within the stream itself.
                        let _ = socket.send(Message::Text(
                            serde_json::json!({"type":"output", "content": chunk}).to_string().into()
                        )).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // The raw stream skipped some messages; xterm.js will be slightly
                        // out of sync until the next screen snapshot. Continue receiving.
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TerminalClientMessage, parse_client_message};

    #[test]
    fn parses_text_input_message() {
        assert_eq!(
            parse_client_message(r#"{"type":"input","content":"hello"}"#),
            Some(TerminalClientMessage::Input("hello".to_string()))
        );
    }

    #[test]
    fn ignores_empty_input_message() {
        assert_eq!(
            parse_client_message(r#"{"type":"input","content":""}"#),
            None
        );
    }

    #[test]
    fn parses_special_key_message() {
        assert_eq!(
            parse_client_message(r#"{"type":"keys","keys":["Down","Enter"]}"#),
            Some(TerminalClientMessage::Keys(vec![
                "Down".to_string(),
                "Enter".to_string()
            ]))
        );
    }

    #[test]
    fn parses_write_message() {
        assert_eq!(
            parse_client_message(r#"{"type":"write","content":"ls\n"}"#),
            Some(TerminalClientMessage::Write("ls\n".to_string()))
        );
    }

    #[test]
    fn ignores_empty_write_message() {
        assert_eq!(
            parse_client_message(r#"{"type":"write","content":""}"#),
            None
        );
    }

    #[test]
    fn ignores_write_message_without_content() {
        assert_eq!(
            parse_client_message(r#"{"type":"write"}"#),
            None
        );
    }

    #[test]
    fn ignores_empty_or_unknown_messages() {
        assert_eq!(parse_client_message(r#"{"type":"keys","keys":[]}"#), None);
        assert_eq!(parse_client_message(r#"{"type":"noop"}"#), None);
        assert_eq!(parse_client_message("not json"), None);
    }
}
