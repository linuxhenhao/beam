//! Herdr terminal WebSocket bridge.
//!
//! The browser connects to `/s/{session_id}/ws/herdr` after the ticket/cookie
//! login on the session page. The daemon spawns a `herdr terminal session
//! observe|control` subprocess per browser connection and relays:
//!
//! - readonly: `observe <pane> --cols 160 --rows 50` → NDJSON frames → browser
//! - write: `control <pane>` (no `--takeover`) ← browser input/resize, NDJSON
//!   frames → browser
//!
//! The pane identity comes from the persisted `Session.herdr_pane_id`, never
//! from the browser. Concurrency is capped with a per-session/global observer
//! limiter, and write mode is single-writer per pane (Herdr enforces the real
//! controller contract; the registry here only fails fast among Beam-originated
//! controllers).

use std::process::Stdio;
use std::sync::Arc;

use axum::{
    extract::{
        Path, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
};
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command as TokioCommand};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, warn};

use beam_core::{BackendKind, DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS};

use crate::terminal_auth::TerminalPermission;

use super::auth;
use super::{ProxyState, resolve_session};

/// Channel capacity for frames forwarded to a slow browser; excess frames are
/// dropped (the next `full:true` frame repairs the view).
const FRAME_CHANNEL_CAPACITY: usize = 256;

/// WebSocket close code used for "controller in use" (write conflict).
const CLOSE_CONTROLLER_IN_USE: u16 = 4001;
/// WebSocket close code used when the pane has been closed.
const CLOSE_TERMINAL_CLOSED: u16 = 1001;

/// Herdr browser-terminal configuration passed to the proxy.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HerdrWebLimits {
    pub(crate) enabled: bool,
    pub(crate) max_observers_per_session: usize,
    pub(crate) max_observers_global: usize,
}

// ── Observer limiter ─────────────────────────────────────────────────────

/// Per-session and global cap on concurrent `herdr terminal session observe`
/// children. Read-only viewers spawn one child each; control connections are
/// excluded (they are limited to one controller per pane).
#[derive(Clone, Default)]
pub(crate) struct HerdrObserverLimiter {
    inner: Arc<Mutex<HerdrObserverLimiterInner>>,
}

#[derive(Default)]
struct HerdrObserverLimiterInner {
    per_session: std::collections::HashMap<String, usize>,
    global: usize,
}

impl HerdrObserverLimiter {
    /// Reserve one observe slot. Returns false when either cap is exceeded.
    pub(crate) async fn try_acquire(
        &self,
        session_id: &str,
        per_session_max: usize,
        global_max: usize,
    ) -> bool {
        let mut inner = self.inner.lock().await;
        let session_count = inner.per_session.get(session_id).copied().unwrap_or(0);
        if session_count >= per_session_max || inner.global >= global_max {
            return false;
        }
        *inner.per_session.entry(session_id.to_string()).or_insert(0) += 1;
        inner.global += 1;
        true
    }

    /// Release one observe slot for `session_id`.
    pub(crate) async fn release(&self, session_id: &str) {
        let mut inner = self.inner.lock().await;
        if let Some(count) = inner.per_session.get_mut(session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inner.per_session.remove(session_id);
            }
        }
        inner.global = inner.global.saturating_sub(1);
    }
}

// ── Controller registry ──────────────────────────────────────────────────

/// Fast-fail registry of Beam-originated controllers per herdr pane. The
/// authoritative single-writer guarantee is Herdr's own controller check;
/// this map only improves UX for Beam-originated conflicts.
#[derive(Clone, Default)]
pub(crate) struct HerdrControllerRegistry {
    inner: Arc<Mutex<std::collections::HashMap<String, String>>>,
}

impl HerdrControllerRegistry {
    /// Claim `pane_id` for `session_id`. Returns false if another Beam
    /// controller already owns the pane.
    pub(crate) async fn try_acquire(&self, pane_id: &str, session_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        if inner.contains_key(pane_id) {
            return false;
        }
        inner.insert(pane_id.to_string(), session_id.to_string());
        true
    }

    /// Release a pane claim. Only clears it when the release matches the
    /// current owner, so a stale release cannot evict a newer controller.
    pub(crate) async fn release(&self, pane_id: &str, session_id: &str) {
        let mut inner = self.inner.lock().await;
        if inner.get(pane_id).is_some_and(|owner| owner == session_id) {
            inner.remove(pane_id);
        }
    }
}

// ── Frame parsing ────────────────────────────────────────────────────────

/// A `terminal.frame` NDJSON line, forwarded to the browser as-is (the bytes
/// stay base64-encoded; xterm decodes them).
#[derive(Debug, Clone)]
struct HerdrFrame {
    bytes_b64: String,
    full: bool,
}

/// Tolerantly parse one observe/control stdout line into a frame. Mirrors the
/// worker-side `parse_herdr_frame_line` field compatibility (`frame.data` /
/// `data` / `data.data` / `bytes`) but keeps the base64 payload untouched.
fn parse_herdr_ws_frame(line: &str) -> Option<HerdrFrame> {
    let value: Value = serde_json::from_str(line).ok()?;
    if !matches!(
        value.get("type").and_then(Value::as_str),
        Some("frame") | Some("terminal.frame")
    ) {
        return None;
    }
    let bytes = value
        .get("frame")
        .and_then(|f| f.get("data").and_then(Value::as_str))
        .or_else(|| {
            value
                .get("data")
                .and_then(|d| d.as_str().or_else(|| d.get("data").and_then(Value::as_str)))
        })
        .or_else(|| value.get("bytes").and_then(Value::as_str))?;
    let full = value.get("full").and_then(Value::as_bool).unwrap_or(false);
    Some(HerdrFrame {
        bytes_b64: bytes.to_string(),
        full,
    })
}

/// Translate one browser message into a herdr control NDJSON line (without the
/// trailing newline). Returns `None` for ping, malformed or irrelevant input.
fn translate_browser_message(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    match value.get("type").and_then(Value::as_str)? {
        "input" => {
            if let Some(bytes) = value.get("bytes").and_then(Value::as_str) {
                Some(json!({ "type": "terminal.input", "bytes": bytes }).to_string())
            } else {
                value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(|text| json!({ "type": "terminal.input", "text": text }).to_string())
            }
        }
        "resize" => match (
            value.get("cols").and_then(Value::as_u64),
            value.get("rows").and_then(Value::as_u64),
        ) {
            (Some(cols), Some(rows)) => {
                Some(json!({ "type": "terminal.resize", "rows": rows, "cols": cols }).to_string())
            }
            _ => None,
        },
        "ping" => None,
        _ => None,
    }
}

/// Decode base64 ANSI bytes (used only in tests and diagnostics; the browser
/// decodes payloads itself).
#[allow(dead_code)]
fn decode_frame_bytes(b64: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(b64).ok()
}

// ── Handler ──────────────────────────────────────────────────────────────

/// Handle `/s/{session_id}/ws/herdr`.
pub(crate) async fn handle_herdr_ws(
    ws: WebSocketUpgrade,
    State(state): State<ProxyState>,
    Path(session_id): Path<String>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, (StatusCode, &'static str)> {
    if !state.herdr_web.enabled {
        return Err((StatusCode::NOT_FOUND, "terminal disabled"));
    }
    let session = resolve_session(&state.sessions, &session_id)
        .await
        .ok_or((StatusCode::NOT_FOUND, "session not found"))?;
    if session.backend_kind != BackendKind::Herdr {
        return Err((StatusCode::NOT_FOUND, "session not found"));
    }
    let pane_id = session
        .herdr_pane_id
        .clone()
        .ok_or((StatusCode::NOT_FOUND, "session ended"))?;

    let headers = req.headers().clone();
    let auth = auth::authenticate_via_beam_cookie(&state, &session_id, &headers)
        .await
        .ok_or((StatusCode::UNAUTHORIZED, "terminal authentication required"))?;

    // The cookie entry carries the pane identity captured at ticket login;
    // the session is authoritative. Flag drift so a stale cookie does not
    // silently attach to the wrong pane.
    if let crate::terminal_auth::UpstreamTarget::Herdr {
        workspace_id: cookie_workspace,
        pane_id: cookie_pane,
    } = &auth.upstream
        && (session.herdr_workspace_id.as_deref() != Some(cookie_workspace.as_str())
            || session.herdr_pane_id.as_deref() != Some(cookie_pane.as_str()))
    {
        warn!(
            component = "terminal_proxy",
            operation = "herdr_ws",
            outcome = "pane_identity_drift",
            session_id = session_id,
            cookie_pane = cookie_pane,
            session_pane = session.herdr_pane_id.as_deref().unwrap_or("none"),
            "herdr pane identity changed since ticket login"
        );
    }

    debug!(
        component = "terminal_proxy",
        operation = "herdr_ws",
        outcome = "upgrade",
        session_id = session_id,
        pane_id = pane_id,
        permission = ?auth.permission,
        "herdr WS upgrade for session {session_id} pane {pane_id} permission={:?}",
        auth.permission
    );

    // Read-only observers are capped; write mode is single-writer per pane.
    let observer_slot = if auth.permission == TerminalPermission::ReadOnly {
        let acquired = state
            .herdr_observer_limiter
            .try_acquire(
                &session_id,
                state.herdr_web.max_observers_per_session,
                state.herdr_web.max_observers_global,
            )
            .await;
        if !acquired {
            warn!(
                component = "terminal_proxy",
                operation = "herdr_ws",
                outcome = "observer_limit",
                session_id = session_id,
                "herdr observer limit reached for session {session_id}"
            );
            return Err((StatusCode::SERVICE_UNAVAILABLE, "too many viewers"));
        }
        true
    } else {
        false
    };

    Ok(ws.on_upgrade(move |client_socket| async move {
        let limiter = state.herdr_observer_limiter.clone();
        let bridge_session_id = session_id.clone();
        run_herdr_bridge(
            client_socket,
            state,
            bridge_session_id,
            pane_id,
            auth.permission,
        )
        .await;
        if observer_slot {
            limiter.release(&session_id).await;
        }
    }))
}

// ── Bridge ───────────────────────────────────────────────────────────────

async fn spawn_herdr_child(
    pane_id: &str,
    permission: TerminalPermission,
) -> std::io::Result<Child> {
    let mut cmd = TokioCommand::new("herdr");
    cmd.arg("terminal").arg("session");
    match permission {
        TerminalPermission::ReadOnly => {
            cmd.arg("observe")
                .arg(pane_id)
                .arg("--cols")
                .arg(DEFAULT_TERMINAL_COLS.to_string())
                .arg("--rows")
                .arg(DEFAULT_TERMINAL_ROWS.to_string())
                .stdin(Stdio::null());
        }
        TerminalPermission::Write => {
            // Deliberately no `--takeover`: grabbing a human TUI or another
            // Beam write tab is a product decision, not a login side effect.
            cmd.arg("control").arg(pane_id).stdin(Stdio::piped());
        }
    }
    cmd.env_remove("HERDR_PANE_ID")
        .env_remove("HERDR_TAB_ID")
        .env_remove("HERDR_WORKSPACE_ID")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd.spawn()
}

/// Relay frames and control messages between the browser WS and the herdr
/// subprocess for the lifetime of the connection.
async fn run_herdr_bridge(
    client_socket: WebSocket,
    state: ProxyState,
    session_id: String,
    pane_id: String,
    permission: TerminalPermission,
) {
    let is_write = permission == TerminalPermission::Write;
    let (mut client_sender, mut client_receiver) = client_socket.split();

    if is_write
        && !state
            .herdr_controller_registry
            .try_acquire(&pane_id, &session_id)
            .await
    {
        warn!(
            component = "terminal_proxy",
            operation = "herdr_ws",
            outcome = "controller_in_use",
            session_id = session_id,
            pane_id = pane_id,
            "herdr controller already in use for pane {pane_id}"
        );
        let _ = client_sender
            .send(Message::Text(
                json!({ "type": "error", "message": "controller in use" })
                    .to_string()
                    .into(),
            ))
            .await;
        let _ = client_sender
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: CLOSE_CONTROLLER_IN_USE,
                reason: "controller in use".into(),
            })))
            .await;
        return;
    }

    let mut child = match spawn_herdr_child(&pane_id, permission).await {
        Ok(child) => child,
        Err(err) => {
            warn!(
                component = "terminal_proxy",
                operation = "herdr_ws",
                outcome = "spawn_failed",
                session_id = session_id,
                pane_id = pane_id,
                "failed to spawn herdr terminal session: {err}"
            );
            let _ = client_sender
                .send(Message::Text(
                    json!({ "type": "error", "message": "herdr not available" })
                        .to_string()
                        .into(),
                ))
                .await;
            let _ = client_sender
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1011,
                    reason: "herdr spawn failed".into(),
                })))
                .await;
            return;
        }
    };

    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = client_sender
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1011,
                    reason: "no herdr output".into(),
                })))
                .await;
            return;
        }
    };

    // Drain child stderr into the daemon log; never forward to the browser.
    if let Some(stderr) = child.stderr.take() {
        let pane_id = pane_id.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                warn!(
                    component = "terminal_proxy",
                    operation = "herdr_ws",
                    outcome = "child_stderr",
                    pane_id = pane_id,
                    stderr = line,
                    "herdr terminal session stderr: {line}"
                );
            }
        });
    }

    let (frame_tx, mut frame_rx) = mpsc::channel::<HerdrFrame>(FRAME_CHANNEL_CAPACITY);
    let (closed_tx, mut closed_rx) = oneshot::channel::<()>();

    let reader_pane_id = pane_id.clone();
    let reader = tokio::spawn(async move {
        let pane_id = reader_pane_id;
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.contains("terminal.closed") {
                        let _ = closed_tx.send(());
                        break;
                    }
                    if let Some(frame) = parse_herdr_ws_frame(&line)
                        && frame_tx.send(frame).await.is_err()
                    {
                        break;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        component = "terminal_proxy",
                        operation = "herdr_ws",
                        outcome = "read_error",
                        pane_id = pane_id,
                        "herdr stdout read error: {err}"
                    );
                    break;
                }
            }
        }
    });

    let hello = json!({
        "type": "hello",
        "mode": if is_write { "write" } else { "readonly" },
        "cols": DEFAULT_TERMINAL_COLS,
        "rows": DEFAULT_TERMINAL_ROWS,
    })
    .to_string();
    let mut child_stdin: Option<ChildStdin> = child.stdin.take();
    if client_sender
        .send(Message::Text(hello.into()))
        .await
        .is_err()
    {
        // Browser already gone; nothing to bridge.
        drop(child_stdin);
        reader.abort();
        if is_write {
            state
                .herdr_controller_registry
                .release(&pane_id, &session_id)
                .await;
        }
        return;
    }

    let mut controller_conflict = false;
    loop {
        tokio::select! {
            msg = client_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if is_write
                            && let Some(line) = translate_browser_message(&text)
                            && let Some(stdin) = child_stdin.as_mut()
                        {
                            let _ = stdin.write_all(line.as_bytes()).await;
                            let _ = stdin.write_all(b"\n").await;
                            let _ = stdin.flush().await;
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
            frame = frame_rx.recv() => {
                match frame {
                    Some(frame) => {
                        let msg = json!({
                            "type": "frame",
                            "bytes": frame.bytes_b64,
                            "full": frame.full,
                        })
                        .to_string();
                        if client_sender.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    None => break, // stdout EOF
                }
            }
            _ = &mut closed_rx => {
                let _ = client_sender
                    .send(Message::Text(
                        json!({ "type": "closed" }).to_string().into(),
                    ))
                    .await;
                let _ = client_sender
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: CLOSE_TERMINAL_CLOSED,
                        reason: "terminal closed".into(),
                    })))
                    .await;
                break;
            }
        }
    }

    // Graceful disconnect: try to release the controller before the child is
    // killed. Abrupt disconnects skip this and rely on stdin EOF.
    if is_write {
        if let Some(stdin) = child_stdin.as_mut() {
            let _ = stdin.write_all(b"{\"type\":\"terminal.release\"}\n").await;
            let _ = stdin.flush().await;
        }
        // Free the registry first so the next writable connection can start
        // while this child winds down. Herdr's own controller check remains
        // the authoritative gate.
        state
            .herdr_controller_registry
            .release(&pane_id, &session_id)
            .await;
        let status = tokio::time::timeout(std::time::Duration::from_secs(3), child.wait()).await;
        let exited_cleanly = matches!(status, Ok(Ok(s)) if s.success());
        if !exited_cleanly {
            // The child died without clean release (or did not exit after
            // terminal.release): most likely the herdr controller rejected us
            // (already owned elsewhere) or herdr needs stdin EOF to release.
            controller_conflict = true;
        }
        // Make sure the child is reaped after the release handshake.
        let _ = child.kill().await;
        let _ = child.wait().await;
    }

    drop(child_stdin);
    // Always end the child so the reader task observes stdout EOF and exits
    // (readonly path never waits on the child above).
    let _ = child.kill().await;
    let _ = reader.await;

    if controller_conflict {
        let _ = client_sender
            .send(Message::Text(
                json!({ "type": "error", "message": "controller in use" })
                    .to_string()
                    .into(),
            ))
            .await;
        let _ = client_sender
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: CLOSE_CONTROLLER_IN_USE,
                reason: "controller in use".into(),
            })))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bytes_field_frame() {
        let line = r#"{"type":"terminal.frame","bytes":"aGVsbG8=","full":true,"height":24,"width":80,"seq":3}"#;
        let frame = parse_herdr_ws_frame(line).expect("frame");
        assert_eq!(frame.bytes_b64, "aGVsbG8=");
        assert!(frame.full);
    }

    #[test]
    fn parses_frame_data_field_and_full_defaults_false() {
        let line = r#"{"type":"frame","data":"aGk="}"#;
        let frame = parse_herdr_ws_frame(line).expect("frame");
        assert_eq!(frame.bytes_b64, "aGk=");
        assert!(!frame.full);
    }

    #[test]
    fn parses_nested_frame_data() {
        let line = r#"{"type":"frame","frame":{"data":"eA=="}}"#;
        let frame = parse_herdr_ws_frame(line).expect("frame");
        assert_eq!(frame.bytes_b64, "eA==");
    }

    #[test]
    fn ignores_closed_and_garbage() {
        assert!(parse_herdr_ws_frame(r#"{"type":"terminal.closed"}"#).is_none());
        assert!(parse_herdr_ws_frame("not json").is_none());
        assert!(parse_herdr_ws_frame(r#"{"type":"keepalive"}"#).is_none());
    }

    #[test]
    fn translates_input_text_and_bytes() {
        assert_eq!(
            translate_browser_message(r#"{"type":"input","text":"ls\r"}"#)
                .map(|s| serde_json::from_str::<Value>(&s).unwrap()),
            Some(json!({"type":"terminal.input","text":"ls\r"}))
        );
        assert_eq!(
            translate_browser_message(r#"{"type":"input","bytes":"AAE="}"#)
                .map(|s| serde_json::from_str::<Value>(&s).unwrap()),
            Some(json!({"type":"terminal.input","bytes":"AAE="}))
        );
    }

    #[test]
    fn translates_resize_with_swapped_rows_cols() {
        assert_eq!(
            translate_browser_message(r#"{"type":"resize","cols":160,"rows":50}"#)
                .map(|s| serde_json::from_str::<Value>(&s).unwrap()),
            Some(json!({"type":"terminal.resize","rows":50,"cols":160}))
        );
    }

    #[test]
    fn ignores_ping_and_malformed() {
        assert_eq!(translate_browser_message(r#"{"type":"ping"}"#), None);
        assert_eq!(translate_browser_message("nope"), None);
        assert_eq!(translate_browser_message(r#"{"type":"input"}"#), None);
    }

    #[test]
    fn frame_bytes_roundtrip_decode() {
        let frame =
            parse_herdr_ws_frame(r#"{"type":"terminal.frame","bytes":"AAECAwQ=","full":false}"#)
                .unwrap();
        assert_eq!(
            decode_frame_bytes(&frame.bytes_b64).unwrap(),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn observer_limiter_respects_per_session_and_global_caps() {
        let limiter = HerdrObserverLimiter::default();
        for _ in 0..3 {
            assert!(limiter.try_acquire("s1", 3, 10).await);
        }
        assert!(!limiter.try_acquire("s1", 3, 10).await);
        assert!(limiter.try_acquire("s2", 3, 10).await);
        limiter.release("s1").await;
        assert!(limiter.try_acquire("s1", 3, 10).await);

        let global = HerdrObserverLimiter::default();
        assert!(global.try_acquire("a", 8, 2).await);
        assert!(global.try_acquire("b", 8, 2).await);
        assert!(!global.try_acquire("c", 8, 2).await);
        global.release("a").await;
        assert!(global.try_acquire("c", 8, 2).await);
    }

    #[tokio::test]
    async fn controller_registry_enforces_single_owner_and_release() {
        let registry = HerdrControllerRegistry::default();
        assert!(registry.try_acquire("w1:p1", "sess-a").await);
        assert!(!registry.try_acquire("w1:p1", "sess-b").await);
        // A second connection from the same session is still one controller.
        assert!(!registry.try_acquire("w1:p1", "sess-a").await);
        // Wrong owner cannot release.
        registry.release("w1:p1", "sess-b").await;
        assert!(!registry.try_acquire("w1:p1", "sess-c").await);
        registry.release("w1:p1", "sess-a").await;
        assert!(registry.try_acquire("w1:p1", "sess-c").await);
    }
}
