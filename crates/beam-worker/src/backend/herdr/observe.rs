//! Herdr terminal session observe — the long-lived frame push.
//!
//! `herdr terminal session observe <pane_id> --cols N --rows N` streams
//! NDJSON: first the current render state, then live ANSI frames (base64 in
//! `frame.data` per the third-party bridge contract). We treat observe as a
//! *change signal*: frames feed the screenshot coordinator's
//! `Trigger::PaneUpdate`. The authoritative full viewport stays
//! `pane read --source visible --format ansi`, because observe does not
//! guarantee every frame is a full screen.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::AsyncBufReadExt;
use tokio::process::Command as TokioCommand;
use tokio::sync::broadcast;
use tracing::warn;

/// Extract the ANSI text from one observe NDJSON line. Returns `None` for
/// non-frame events (`terminal.closed`, keepalives) and unparseable lines.
pub fn parse_herdr_frame_line(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    match v.get("type").and_then(|t| t.as_str()) {
        Some("frame") | Some("terminal.frame") => {
            let data = v
                .get("frame")
                .and_then(|d| d.get("data").and_then(serde_json::Value::as_str))
                .or_else(|| {
                    v.get("data").and_then(|d| {
                        d.as_str()
                            .or_else(|| d.get("data").and_then(serde_json::Value::as_str))
                    })
                })
                // herdr 0.8.x frames carry base64 ANSI in `bytes`:
                // {"type":"terminal.frame","bytes":"<b64>","full":true,...}
                .or_else(|| v.get("bytes").and_then(serde_json::Value::as_str))?;
            // The bridge contract is base64 ANSI.
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .ok()?;
            Some(String::from_utf8_lossy(&bytes).into_owned())
        }
        Some("terminal.closed") | Some("closed") => None,
        _ => None,
    }
}

/// Spawn `herdr terminal session observe` and forward decoded frames to
/// `data_tx` until the child exits or `stop_flag` is set.
pub(super) async fn run_herdr_observe(
    pane_id: String,
    cols: u16,
    rows: u16,
    data_tx: broadcast::Sender<String>,
    stop_flag: Arc<AtomicBool>,
) {
    let mut child = match TokioCommand::new("herdr")
        .args([
            "terminal",
            "session",
            "observe",
            &pane_id,
            "--cols",
            &cols.to_string(),
            "--rows",
            &rows.to_string(),
        ])
        .env_remove("HERDR_PANE_ID")
        .env_remove("HERDR_TAB_ID")
        .env_remove("HERDR_WORKSPACE_ID")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            warn!("failed to start herdr observe for pane {}: {}", pane_id, e);
            return;
        }
    };

    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => return,
    };

    let mut lines = tokio::io::BufReader::new(stdout).lines();
    loop {
        if stop_flag.load(Ordering::Relaxed) {
            let _ = child.start_kill();
            break;
        }
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.contains("terminal.closed") {
                    break;
                }
                if let Some(frame) = parse_herdr_frame_line(&line)
                    && !frame.is_empty()
                {
                    let _ = data_tx.send(frame);
                }
            }
            Ok(None) => break,
            Err(e) => {
                warn!("herdr observe read error for {}: {}", pane_id, e);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_base64_frame_line() {
        use base64::Engine;
        let text = "\x1b[H\x1b[2Jhello";
        let b64 = base64::engine::general_purpose::STANDARD.encode(text);
        let line = format!(r#"{{"type":"frame","data":"{b64}"}}"#);
        assert_eq!(parse_herdr_frame_line(&line).as_deref(), Some(text));
    }

    #[test]
    fn parses_real_terminal_frame_bytes_field() {
        use base64::Engine;
        let text = "\x1b[2Jfull screen";
        let b64 = base64::engine::general_purpose::STANDARD.encode(text);
        let line = format!(
            r#"{{"type":"terminal.frame","bytes":"{b64}","full":true,"height":24,"width":80,"seq":1}}"#
        );
        assert_eq!(parse_herdr_frame_line(&line).as_deref(), Some(text));
    }

    #[test]
    fn ignores_closed_and_garbage_lines() {
        assert_eq!(
            parse_herdr_frame_line(r#"{"type":"terminal.closed"}"#),
            None
        );
        assert_eq!(parse_herdr_frame_line("not json"), None);
    }
}
