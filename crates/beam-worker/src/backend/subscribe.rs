use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::AsyncBufReadExt;
use tokio::process::Command as TokioCommand;
use tokio::sync::broadcast;
use tracing::warn;

pub(super) fn parse_zellij_subscribe_viewport(line: &str) -> Option<Vec<String>> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let event = v.get("event")?.as_str()?;
    match event {
        "pane_update" => {
            let viewport_arr = v
                .get("viewport")
                .or_else(|| v.get("data").and_then(|d| d.get("viewport")))
                .and_then(|vp| vp.as_array())?;
            Some(
                viewport_arr
                    .iter()
                    .filter_map(|s| s.as_str().map(ToOwned::to_owned))
                    .collect(),
            )
        }
        "pane_closed" => None,
        _ => None,
    }
}

pub(super) fn is_zellij_pane_closed(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|v| {
            v.get("event")
                .and_then(|e| e.as_str())
                .map(|e| e == "pane_closed")
        })
        .unwrap_or(false)
}

pub fn viewport_to_ansi_chunk(viewport: &[String]) -> String {
    if viewport.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(viewport.iter().map(|l| l.len() + 2).sum::<usize>() + 16);
    out.push_str("\x1b[?25l");
    out.push_str("\x1b[H");
    out.push_str("\x1b[2J");
    for (i, line) in viewport.iter().enumerate() {
        if i > 0 {
            out.push_str("\r\n");
        }
        out.push_str(line);
    }
    out.push_str("\x1b[?25h");
    out
}

#[allow(dead_code)]
pub fn numeric_pane_id(pane_id: &str) -> Option<u64> {
    if let Ok(n) = pane_id.parse::<u64>() {
        return Some(n);
    }
    pane_id.strip_prefix("terminal_")?.parse().ok()
}

#[allow(dead_code)]
pub fn parse_zellij_cursor_from_list_panes(json: &str, numeric_id: u64) -> Option<(u16, u16)> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let panes = v.as_array()?;
    for pane in panes {
        let id = pane.get("id")?.as_u64()?;
        if id != numeric_id {
            continue;
        }
        let cursor = pane.get("cursor_coordinates_in_pane")?;
        if let Some(arr) = cursor.as_array() {
            let x = arr.first()?.as_u64()? as u16;
            let y = arr.get(1)?.as_u64()? as u16;
            return Some((x, y));
        }
        let x = cursor.get("x")?.as_u64()? as u16;
        let y = cursor.get("y")?.as_u64()? as u16;
        return Some((x, y));
    }
    None
}

pub(super) async fn run_zellij_subscribe(
    session_name: String,
    pane_id: String,
    data_tx: broadcast::Sender<String>,
    stop_flag: Arc<AtomicBool>,
) {
    let mut child = match TokioCommand::new("zellij")
        .arg("--session")
        .arg(&session_name)
        .arg("subscribe")
        .arg("--pane-id")
        .arg(&pane_id)
        .arg("--ansi")
        .arg("--format")
        .arg("json")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            warn!(
                "failed to start zellij subscribe for session {}: {}",
                session_name, e
            );
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
                if is_zellij_pane_closed(&line) {
                    break;
                }
                if let Some(viewport) = parse_zellij_subscribe_viewport(&line) {
                    let chunk = viewport_to_ansi_chunk(&viewport);
                    if !chunk.is_empty() {
                        let _ = data_tx.send(chunk);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                warn!("zellij subscribe read error for {}: {}", session_name, e);
                break;
            }
        }
    }

    let _ = child.start_kill();
    let _ = child.wait().await;
}
