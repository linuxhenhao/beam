//! Read-only render anchor and viewer counter for terminal proxy.
//!
//! The read-only anchor is a background WebSocket client that keeps a
//! persistent read-only zellij connection so that `TerminalResize` commands
//! (including the debounced reset-to-default after all viewers disconnect)
//! can be sent even after all browser viewers have left.
//!
//! The [`ViewerCounter`] tracks active terminal WebSocket viewer counts and
//! triggers a debounced resize via the anchor when the count drops to zero.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use beam_core::{DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS};

use crate::terminal_auth::TerminalPermission;
use crate::zellij_web::ZellijWebTokens;

use super::auth::zellij_web_login;
use super::{ANCHOR_RESTART_COOLDOWN, build_ws_target_url};

// ── Anchor types ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ZellijAnchorManager {
    pub(crate) anchors: Arc<Mutex<HashMap<String, ZellijAnchorEntry>>>,
}

impl Default for ZellijAnchorManager {
    fn default() -> Self {
        ZellijAnchorManager {
            anchors: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub(crate) struct ZellijAnchorEntry {
    pub(crate) task: JoinHandle<()>,
    pub(crate) started_at: Instant,
    /// Command sender for internal resize requests (ResizeToDefault).
    pub(crate) cmd_tx: tokio::sync::mpsc::UnboundedSender<AnchorCommand>,
}

/// Internal command sent to the anchor task.
#[derive(Debug, Clone)]
pub(crate) enum AnchorCommand {
    /// Resize the pane back to the default 160×50 dimensions.
    ResizeToDefault,
}

// ── Viewer counter ───────────────────────────────────────────────────────

/// Per-session viewer state for debounced reset logic.
pub(crate) struct ViewerState {
    pub(crate) count: usize,
    /// Pending debounce reset task, if count has dropped to zero.
    pub(crate) pending_reset: Option<JoinHandle<()>>,
}

/// Tracks active terminal WebSocket viewer counts and coordinates
/// debounced reset-to-default resize via the anchor.
#[derive(Clone)]
pub(crate) struct ViewerCounter {
    pub(crate) inner: Arc<Mutex<HashMap<String, ViewerState>>>,
    pub(crate) anchors: ZellijAnchorManager,
}

impl ViewerCounter {
    /// Increment the terminal viewer count for `zellij_session`.
    /// Cancels any pending debounce reset.
    pub(crate) async fn increment(&self, zellij_session: &str) {
        let mut inner = self.inner.lock().await;
        let state = inner
            .entry(zellij_session.to_string())
            .or_insert(ViewerState {
                count: 0,
                pending_reset: None,
            });
        state.count += 1;
        if let Some(handle) = state.pending_reset.take() {
            handle.abort();
        }
    }

    /// Decrement the terminal viewer count for `zellij_session`.
    /// If count reaches zero and no debounce is already pending, spawn a
    /// debounce task that will send `ResizeToDefault` to the anchor after
    /// a delay (unless a new viewer connects in the meantime).
    pub(crate) async fn decrement(&self, zellij_session: &str) {
        let mut inner = self.inner.lock().await;
        let state = match inner.get_mut(zellij_session) {
            Some(s) => s,
            None => return,
        };
        if state.count > 0 {
            state.count -= 1;
        }
        // Only create a new debounce if we just reached zero AND no pending
        // task already exists (defends against unbalanced double-decrement).
        if state.count == 0 && state.pending_reset.is_none() {
            let zellij_session = zellij_session.to_string();
            let anchors = self.anchors.clone();
            let counter = self.inner.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(800)).await;
                let mut inner = counter.lock().await;
                if let Some(state) = inner.get_mut(&zellij_session)
                    && state.count == 0
                {
                    let anchors_map = anchors.anchors.lock().await;
                    if let Some(entry) = anchors_map.get(&zellij_session)
                        && !entry.task.is_finished()
                    {
                        let _ = entry.cmd_tx.send(AnchorCommand::ResizeToDefault);
                    }
                    state.pending_reset = None;
                }
            });
            state.pending_reset = Some(handle);
        }
    }
}

// ── Resize message ───────────────────────────────────────────────────────

/// Build the JSON text frame for a zellij web control-message resize.
///
/// Wire shape follows zellij's `WebClientToWebServerControlMessage`:
/// ```json
/// {
///   "web_client_id": "<id>",
///   "payload": {
///     "type": "TerminalResize",
///     "rows": <u16>,
///     "cols": <u16>
///   }
/// }
/// ```
///
/// `TerminalResize` (ResizeCause::Viewport) is the correct message for
/// the anchor because it triggers zellij's `ReevaluateMobileMode`, which
/// exits mobile layout and lets the pane adopt the requested dimensions.
///
/// Separated from the WebSocket I/O so it can be unit-tested.
pub(crate) fn build_web_resize_message(
    web_client_id: &str,
    cols: u16,
    rows: u16,
) -> serde_json::Value {
    serde_json::json!({
        "web_client_id": web_client_id,
        "payload": {
            "type": "TerminalResize",
            "rows": rows,
            "cols": cols,
        }
    })
}

// ── Anchor lifecycle ─────────────────────────────────────────────────────

pub(crate) fn should_ensure_read_only_anchor(
    permission: TerminalPermission,
    tokens: &ZellijWebTokens,
) -> bool {
    permission == TerminalPermission::ReadOnly
        && tokens
            .write_token
            .as_deref()
            .is_some_and(|token| !token.is_empty())
}

pub(crate) async fn ensure_read_only_anchor(
    state: &super::ProxyState,
    session_id: &str,
    zellij_session: &str,
) {
    if !should_ensure_read_only_anchor(TerminalPermission::ReadOnly, &state.zellij_tokens) {
        warn!(
            component = "terminal_proxy",
            operation = "anchor",
            outcome = "token_unavailable",
            session_id = session_id,
            "terminal proxy: read-only anchor skipped for {session_id}: write token unavailable"
        );
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

    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        if let Err(err) = run_zellij_anchor_client(
            client,
            zellij_web_port,
            zellij_session_for_task.clone(),
            write_token,
            cmd_rx,
        )
        .await
        {
            warn!(
                component = "terminal_proxy",
                operation = "anchor",
                outcome = "error",
                session_id = session_id_for_log,
                "terminal proxy: zellij read-only anchor ended for session {} zellij={}: {}",
                session_id_for_log,
                zellij_session_for_task,
                err
            );
        }
    });

    // Register the anchor entry so the viewer-counter debounce can reach
    // the anchor's command channel via ZellijAnchorManager.
    anchors.insert(
        key,
        ZellijAnchorEntry {
            task,
            started_at: Instant::now(),
            cmd_tx,
        },
    );
}

async fn run_zellij_anchor_client(
    client: Client,
    zellij_web_port: u16,
    zellij_session: String,
    write_token: String,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<AnchorCommand>,
) -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

    let zellij_cookie = zellij_web_login(&client, zellij_web_port, &write_token)
        .await
        .map_err(|(status, msg)| anyhow::anyhow!("login failed: {status} {msg}"))?;

    let session_url = format!("http://127.0.0.1:{zellij_web_port}/session");
    let session_resp = client
        .post(session_url)
        .header(reqwest::header::COOKIE, zellij_cookie.clone())
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

    // Connect terminal WS first — mirror browser behaviour.
    // The zellij server listener must finish attaching before a resize can
    // take effect; waiting for the first terminal frame guarantees that.
    let mut terminal_ws =
        super::ws_relay::connect_ws_with_cookie(&terminal_url, Some(&zellij_cookie)).await?;
    debug!(
        component = "terminal_proxy",
        operation = "anchor",
        outcome = "connecting",
        "terminal proxy: anchor terminal WS connected for {zellij_session}, waiting for first frame..."
    );
    {
        // Drain pings and wait for the first substantive frame (text/binary)
        // or until the socket closes. Timeout after 5 s to avoid stalling.
        let deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                msg = terminal_ws.next() => {
                    match msg {
                        Some(Ok(TungsteniteMessage::Ping(data))) => {
                            let _ = terminal_ws.send(TungsteniteMessage::Pong(data)).await;
                        }
                        Some(Ok(TungsteniteMessage::Close(_))) | None => {
                            anyhow::bail!("anchor terminal WS closed before first frame");
                        }
                        Some(Ok(_)) => break, // got a real frame
                        Some(Err(err)) => {
                            anyhow::bail!("anchor terminal WS error before first frame: {err}");
                        }
                    }
                }
                _ = &mut deadline => {
                    // No terminal frame within timeout — proceed anyway; the
                    // resize might still work.
                    warn!(
                        component = "terminal_proxy",
                        operation = "anchor",
                        outcome = "timeout",
                        "terminal proxy: anchor no terminal frame after 5 s, proceeding for {zellij_session}"
                    );
                    break;
                }
            }
        }
    }

    // Connect control WS after the terminal listener is ready.
    let mut control_ws =
        super::ws_relay::connect_ws_with_cookie(&control_url, Some(&zellij_cookie)).await?;
    debug!(
        component = "terminal_proxy",
        operation = "anchor",
        outcome = "success",
        "terminal proxy: zellij read-only anchor fully connected for {zellij_session}"
    );

    // Wait for the server to send SetConfig (or any initial message) so we
    // don't race the resize before the control channel is fully set up.
    {
        let deadline = tokio::time::sleep(std::time::Duration::from_secs(3));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                msg = control_ws.next() => {
                    match msg {
                        Some(Ok(TungsteniteMessage::Text(text))) => {
                            // Accept any server→client control message as a
                            // readiness signal (SetConfig, QueryTerminalSize,
                            // etc.).
                            let _ = text; // consumed
                            break;
                        }
                        Some(Ok(TungsteniteMessage::Ping(data))) => {
                            let _ = control_ws.send(TungsteniteMessage::Pong(data)).await;
                        }
                        Some(Ok(TungsteniteMessage::Close(_))) | None => {
                            anyhow::bail!("anchor control WS closed before SetConfig");
                        }
                        Some(Ok(_)) => break, // any non-text non-ping message
                        Some(Err(err)) => {
                            anyhow::bail!("anchor control WS error before SetConfig: {err}");
                        }
                    }
                }
                _ = &mut deadline => {
                    // No SetConfig within timeout — proceed with resize anyway.
                    warn!(
                        component = "terminal_proxy",
                        operation = "anchor",
                        outcome = "timeout",
                        "terminal proxy: anchor no SetConfig after 3 s, proceeding for {zellij_session}"
                    );
                    break;
                }
            }
        }
    }

    // ── Send initial resize using TerminalResize ──────────────────────
    // TerminalResize (ResizeCause::Viewport) triggers zellij's
    // ReevaluateMobileMode, which exits the mobile layout and lets the
    // pane adopt the requested dimensions (160×50).  We wait for the
    // terminal first frame and control SetConfig before sending so the
    // zellij server listener is fully attached.
    let initial_resize =
        build_web_resize_message(&web_client_id, DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS);
    control_ws
        .send(TungsteniteMessage::Text(initial_resize.to_string().into()))
        .await?;
    debug!(
        component = "terminal_proxy",
        operation = "anchor",
        outcome = "resize",
        "terminal proxy: anchor sent initial TerminalResize {DEFAULT_TERMINAL_COLS}x{DEFAULT_TERMINAL_ROWS} for {zellij_session}"
    );

    // ── Event loop: zellij control/terminal + internal commands ─────────
    loop {
        tokio::select! {
            // Terminal channel: discard frames, just detect close.
            msg = terminal_ws.next() => {
                match msg {
                    Some(Ok(TungsteniteMessage::Ping(data))) => {
                        let _ = terminal_ws.send(TungsteniteMessage::Pong(data)).await;
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => {
                        debug!(
                            component = "terminal_proxy",
                            operation = "anchor",
                            outcome = "closed",
                            "terminal proxy: anchor terminal WS closed for {zellij_session}"
                        );
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                }
            }
            // Control channel: keep alive, detect close.
            msg = control_ws.next() => {
                match msg {
                    Some(Ok(TungsteniteMessage::Ping(data))) => {
                        let _ = control_ws.send(TungsteniteMessage::Pong(data)).await;
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => {
                        debug!(
                            component = "terminal_proxy",
                            operation = "anchor",
                            outcome = "closed",
                            "terminal proxy: anchor control WS closed for {zellij_session}"
                        );
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                }
            }
            // Internal commands from the daemon.
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(AnchorCommand::ResizeToDefault) => {
                        let resize_json = build_web_resize_message(
                            &web_client_id,
                            DEFAULT_TERMINAL_COLS,
                            DEFAULT_TERMINAL_ROWS,
                        );
                        if let Err(e) = control_ws
                            .send(TungsteniteMessage::Text(resize_json.to_string().into()))
                            .await
                        {
                            warn!(
                                component = "terminal_proxy",
                                operation = "anchor",
                                outcome = "error",
                                "terminal proxy: anchor failed to send ResizeToDefault for {zellij_session}: {e}"
                            );
                            return Err(e.into());
                        }
                        debug!(
                            component = "terminal_proxy",
                            operation = "anchor",
                            outcome = "resize",
                            "terminal proxy: anchor reset {zellij_session} to {DEFAULT_TERMINAL_COLS}x{DEFAULT_TERMINAL_ROWS}"
                        );
                    }
                    None => {
                        // Sender dropped — parent no longer managing this anchor.
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
