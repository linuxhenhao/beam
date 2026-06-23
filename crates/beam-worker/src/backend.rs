use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::io::AsyncBufReadExt;
use tokio::sync::broadcast;
use tokio::process::Command as TokioCommand;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct SpawnOpts {
    pub cwd: String,
    #[allow(dead_code)]
    pub cols: u16,
    #[allow(dead_code)]
    pub rows: u16,
    pub env: Vec<(String, String)>,
}

#[allow(dead_code)]
#[async_trait]
pub trait SessionBackend: Send + Sync {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()>;
    async fn send_text(&self, text: &str) -> Result<()>;
    async fn send_enter(&self) -> Result<()>;
    async fn send_special_keys(&self, keys: &[String]) -> Result<()>;
    async fn paste_text(&self, text: &str) -> Result<()>;
    async fn write_raw(&self, text: &str) -> Result<()>;
    async fn raw_input(&self, text: &str) -> Result<()>;
    async fn capture_viewport(&self) -> Result<String>;
    async fn capture_current_screen(&self) -> Result<String>;
    async fn is_alive(&self) -> Result<bool>;
    async fn child_pid(&self) -> Result<Option<u32>>;
    async fn kill(&mut self) -> Result<()>;
    async fn destroy_session(&mut self) -> Result<()>;
    /// Return the real cursor position as 0-based (x, y) if available.
    async fn cursor_position(&self) -> Result<Option<(u16, u16)>>;
    fn subscribe(&self) -> broadcast::Receiver<String>;
}

// ---- ZellijBackend ----

#[derive(Debug)]
pub struct ZellijBackend {
    session_name: String,
    owns_session: bool,
    pane_id: Option<String>,
    data_tx: broadcast::Sender<String>,
    tmp_config_dir: Option<PathBuf>,
    intentional_exit: Arc<AtomicBool>,
    resurrect_pid: Option<u32>,
    reattach: bool,
    subscribe_started: Arc<AtomicBool>,
    subscribe_stop: Arc<AtomicBool>,
}

impl ZellijBackend {
    pub fn new(session_name: String) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_name,
            owns_session: true,
            pane_id: None,
            data_tx,
            tmp_config_dir: None,
            intentional_exit: Arc::new(AtomicBool::new(false)),
            resurrect_pid: None,
            reattach: false,
            subscribe_started: Arc::new(AtomicBool::new(false)),
            subscribe_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    #[allow(dead_code)]
    pub fn attach_existing(target: String, reattach: bool) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_name: target,
            owns_session: false,
            pane_id: None,
            data_tx,
            tmp_config_dir: None,
            intentional_exit: Arc::new(AtomicBool::new(false)),
            resurrect_pid: None,
            reattach,
            subscribe_started: Arc::new(AtomicBool::new(false)),
            subscribe_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn has_session(name: &str) -> bool {
        match std::process::Command::new("zellij")
            .args(["list-sessions", "--no-formatting"])
            .output()
        {
            Ok(out) => {
                let s = String::from_utf8_lossy(&out.stdout);
                s.lines().any(|l| l.contains(name) && !l.contains("EXITED"))
            }
            Err(_) => false,
        }
    }

    pub fn run_zellij_action(session: &str, args: &[&str]) -> Result<String> {
        let out = Command::new("zellij")
            .arg("--session")
            .arg(session)
            .arg("action")
            .args(args)
            .output()
            .context("failed to spawn zellij action")?;
        if !out.status.success() {
            bail!(
                "zellij action failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    pub fn send_zellij_action(session: &str, args: &[&str]) -> Result<()> {
        Self::run_zellij_action(session, args).map(|_| ())
    }

    fn kdl_string(value: &str) -> String {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    }

    fn write_runtime_files(
        bin: &str,
        bin_args: &[String],
        opts: &SpawnOpts,
    ) -> Result<(PathBuf, PathBuf, PathBuf)> {
        let tmp = std::env::temp_dir().join(format!("bmx-zellij-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;
        let config_path = tmp.join("config.kdl");
        let layout_path = tmp.join("layout.kdl");

        let config = "show_startup_tips false\npane_frames false\n";
        std::fs::write(&config_path, config)?;

        let pane_command = Self::kdl_string(bin);
        let pane_args = bin_args
            .iter()
            .map(|a| Self::kdl_string(a))
            .collect::<Vec<_>>()
            .join(" ");
        let cwd = Self::kdl_string(&opts.cwd);
        let layout = format!(
            "layout {{\n    tab name=\"beam\" {{\n        pane command={} close_on_exit=true cwd={} {{\n            args {}\n        }}\n    }}\n}}\n",
            pane_command, cwd, pane_args,
        );
        std::fs::write(&layout_path, &layout)?;

        Ok((tmp, config_path, layout_path))
    }

    fn discover_pane_id(session: &str) -> Option<String> {
        let out = std::process::Command::new("zellij")
            .arg("--session")
            .arg(session)
            .args(["action", "list-panes", "--json"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
        let panes = json.as_array()?;
        for pane in panes {
            let is_plugin = pane
                .get("is_plugin")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if is_plugin {
                continue;
            }
            if let Some(id) = pane.get("id").and_then(|v| v.as_u64()) {
                return Some(format!("terminal_{}", id));
            }
        }
        None
    }

    fn pane_id_str(&self) -> &str {
        self.pane_id.as_deref().unwrap_or("terminal_0")
    }

    fn ensure_zellij_subscribe_started(&mut self) {
        if self.pane_id.is_none() {
            self.pane_id = Self::discover_pane_id(&self.session_name);
            if self.pane_id.is_none() {
                warn!(
                    "zellij session {}: failed to discover pane_id, falling back to terminal_0",
                    self.session_name
                );
            }
        }
        if !self.subscribe_started.swap(true, Ordering::SeqCst) {
            if let Some(ref pane_id) = self.pane_id {
                let session = self.session_name.clone();
                let pid = pane_id.clone();
                let tx = self.data_tx.clone();
                let stop = self.subscribe_stop.clone();
                tokio::spawn(run_zellij_subscribe(session, pid, tx, stop));
            }
        }
    }
}

#[async_trait]
impl SessionBackend for ZellijBackend {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        self.reattach = self.reattach || Self::has_session(&self.session_name);

        if self.reattach && Self::has_session(&self.session_name) {
            self.ensure_zellij_subscribe_started();
            return Ok(());
        }

        let (tmp_dir, config_path, layout_path) = Self::write_runtime_files(bin, args, &opts)?;
        self.tmp_config_dir = Some(tmp_dir);

        let zellij_args: Vec<String> = if self.reattach {
            vec![
                "--config".to_string(),
                config_path.display().to_string(),
                "attach".to_string(),
                "--create-background".to_string(),
                self.session_name.clone(),
            ]
        } else {
            vec![
                "--config".to_string(),
                config_path.display().to_string(),
                "--session".to_string(),
                self.session_name.clone(),
                "--new-session-with-layout".to_string(),
                layout_path.display().to_string(),
                "attach".to_string(),
                "--create-background".to_string(),
                self.session_name.clone(),
            ]
        };

        let mut cmd = Command::new("zellij");
        cmd.args(&zellij_args)
            .current_dir(&opts.cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (k, v) in &opts.env {
            cmd.env(k, v);
        }

        let out = cmd.output().context("failed to spawn zellij backend")?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("Session already exists") {
                self.ensure_zellij_subscribe_started();
                return Ok(());
            }
            bail!("zellij backend failed: {}", stderr.trim());
        }

        self.ensure_zellij_subscribe_started();
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        Self::send_zellij_action(
            &self.session_name,
            &["write-chars", "--pane-id", self.pane_id_str(), text],
        )
    }

    async fn send_enter(&self) -> Result<()> {
        Self::send_zellij_action(
            &self.session_name,
            &["send-keys", "--pane-id", self.pane_id_str(), "Enter"],
        )
    }

    async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
        for key in keys {
            match key.as_str() {
                "Enter" => self.send_enter().await?,
                "Down" => self.write_raw("\u{1b}[B").await?,
                "Up" => self.write_raw("\u{1b}[A").await?,
                "Left" => self.write_raw("\u{1b}[D").await?,
                "Right" => self.write_raw("\u{1b}[C").await?,
                "PageUp" => self.write_raw("\u{1b}[5~").await?,
                "PageDown" => self.write_raw("\u{1b}[6~").await?,
                "M-Enter" => self.write_raw("\u{1b}\r").await?,
                "Tab" => self.write_raw("\t").await?,
                "Space" => self.write_raw(" ").await?,
                "Escape" | "Esc" => self.write_raw("\u{1b}").await?,
                "C-c" => self.write_raw("\u{3}").await?,
                other if other.len() == 1 => self.write_raw(other).await?,
                other => bail!("unsupported special key for zellij backend: {}", other),
            }
        }
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        Self::send_zellij_action(
            &self.session_name,
            &["paste", "--pane-id", self.pane_id_str(), text],
        )
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        Self::send_zellij_action(
            &self.session_name,
            &["write-chars", "--pane-id", self.pane_id_str(), text],
        )
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.send_text(text).await?;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        let out = Self::run_zellij_action(
            &self.session_name,
            &["dump-screen", "--pane-id", self.pane_id_str()],
        )?;
        Ok(out.replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.capture_viewport().await
    }

    async fn is_alive(&self) -> Result<bool> {
        Ok(Self::has_session(&self.session_name))
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        if let Some(pid) = self.resurrect_pid {
            return Ok(Some(pid));
        }
        let out = std::process::Command::new("ps")
            .args(["-eo", "pid=,comm="])
            .output()?;
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1] != "zellij" {
                if let Ok(pid) = parts[0].parse::<u32>() {
                    return Ok(Some(pid));
                }
            }
        }
        Ok(None)
    }

    async fn kill(&mut self) -> Result<()> {
        self.subscribe_stop.store(true, Ordering::Relaxed);
        self.intentional_exit.store(true, Ordering::Relaxed);
        if let Some(tmp) = self.tmp_config_dir.take() {
            let _ = std::fs::remove_dir_all(&tmp);
        }
        Ok(())
    }

    async fn destroy_session(&mut self) -> Result<()> {
        self.kill().await?;
        if self.owns_session {
            let _ = std::process::Command::new("zellij")
                .args(["delete-session", &self.session_name, "-f"])
                .output();
        }
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        let pane_id = match self.pane_id.as_ref() {
            Some(id) => id,
            None => return Ok(None),
        };
        let numeric_id = match numeric_pane_id(pane_id) {
            Some(id) => id,
            None => return Ok(None),
        };
        let out = match std::process::Command::new("zellij")
            .arg("--session")
            .arg(&self.session_name)
            .args(["action", "list-panes", "--json", "--all"])
            .output()
        {
            Ok(out) if out.status.success() => out,
            _ => return Ok(None),
        };
        let json = String::from_utf8_lossy(&out.stdout);
        Ok(parse_zellij_cursor_from_list_panes(&json, numeric_id))
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

// ---- ZellijObserveBackend ----

pub struct ZellijObserveBackend {
    session_name: String,
    pane_id: String,
    child_pid: Option<u32>,
    data_tx: broadcast::Sender<String>,
    subscribe_started: Arc<AtomicBool>,
    subscribe_stop: Arc<AtomicBool>,
}

impl ZellijObserveBackend {
    pub fn new(session_name: String, pane_id: String, child_pid: Option<u32>) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_name,
            pane_id,
            child_pid,
            data_tx,
            subscribe_started: Arc::new(AtomicBool::new(false)),
            subscribe_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn send_action(&self, args: &[&str]) -> Result<()> {
        ZellijBackend::send_zellij_action(&self.session_name, args)
    }

    fn dump_screen(&self) -> Result<String> {
        ZellijBackend::run_zellij_action(
            &self.session_name,
            &["dump-screen", "--pane-id", self.pane_id.as_str()],
        )
    }
}

#[async_trait]
impl SessionBackend for ZellijObserveBackend {
    async fn spawn(&mut self, _bin: &str, _args: &[String], _opts: SpawnOpts) -> Result<()> {
        if !self.subscribe_started.swap(true, Ordering::SeqCst) {
            let session = self.session_name.clone();
            let pid = self.pane_id.clone();
            let tx = self.data_tx.clone();
            let stop = self.subscribe_stop.clone();
            tokio::spawn(run_zellij_subscribe(session, pid, tx, stop));
        }
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        self.send_action(&["write-chars", "--pane-id", self.pane_id.as_str(), text])
    }

    async fn send_enter(&self) -> Result<()> {
        self.send_action(&["send-keys", "--pane-id", self.pane_id.as_str(), "Enter"])
    }

    async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
        for key in keys {
            match key.as_str() {
                "Enter" => self.send_enter().await?,
                "Down" => self.write_raw("\u{1b}[B").await?,
                "Up" => self.write_raw("\u{1b}[A").await?,
                "Left" => self.write_raw("\u{1b}[D").await?,
                "Right" => self.write_raw("\u{1b}[C").await?,
                "PageUp" => self.write_raw("\u{1b}[5~").await?,
                "PageDown" => self.write_raw("\u{1b}[6~").await?,
                "M-Enter" => self.write_raw("\u{1b}\r").await?,
                "Tab" => self.write_raw("\t").await?,
                "Space" => self.write_raw(" ").await?,
                "Escape" | "Esc" => self.write_raw("\u{1b}").await?,
                "C-c" => self.write_raw("\u{3}").await?,
                other if other.len() == 1 => self.write_raw(other).await?,
                other => bail!("unsupported special key for zellij backend: {}", other),
            }
        }
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        self.send_action(&["paste", "--pane-id", self.pane_id.as_str(), text])
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        self.send_action(&["write-chars", "--pane-id", self.pane_id.as_str(), text])
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.send_text(text).await?;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        Ok(self.dump_screen()?.replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.capture_viewport().await
    }

    async fn is_alive(&self) -> Result<bool> {
        Ok(ZellijBackend::has_session(&self.session_name))
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        Ok(self.child_pid)
    }

    async fn kill(&mut self) -> Result<()> {
        self.subscribe_stop.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn destroy_session(&mut self) -> Result<()> {
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        let numeric_id = match numeric_pane_id(&self.pane_id) {
            Some(id) => id,
            None => return Ok(None),
        };
        let out = match std::process::Command::new("zellij")
            .arg("--session")
            .arg(&self.session_name)
            .args(["action", "list-panes", "--json", "--all"])
            .output()
        {
            Ok(out) if out.status.success() => out,
            _ => return Ok(None),
        };
        let json = String::from_utf8_lossy(&out.stdout);
        Ok(parse_zellij_cursor_from_list_panes(&json, numeric_id))
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

// ---- Zellij subscribe helpers ----

fn parse_zellij_subscribe_viewport(line: &str) -> Option<Vec<String>> {
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

fn is_zellij_pane_closed(line: &str) -> bool {
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
    let mut out =
        String::with_capacity(viewport.iter().map(|l| l.len() + 2).sum::<usize>() + 16);
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

async fn run_zellij_subscribe(
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

#[cfg(test)]
mod tests {
    use super::{
        numeric_pane_id,
        parse_zellij_cursor_from_list_panes, parse_zellij_subscribe_viewport,
        viewport_to_ansi_chunk, is_zellij_pane_closed,
    };

    // ---- numeric_pane_id tests ----

    #[test]
    fn test_numeric_pane_id_valid() {
        assert_eq!(numeric_pane_id("terminal_1"), Some(1));
        assert_eq!(numeric_pane_id("terminal_0"), Some(0));
        assert_eq!(numeric_pane_id("terminal_42"), Some(42));
    }

    #[test]
    fn test_numeric_pane_id_bare_number() {
        assert_eq!(numeric_pane_id("1"), Some(1));
        assert_eq!(numeric_pane_id("0"), Some(0));
        assert_eq!(numeric_pane_id("42"), Some(42));
        assert_eq!(numeric_pane_id("999"), Some(999));
    }

    #[test]
    fn test_numeric_pane_id_invalid() {
        assert_eq!(numeric_pane_id(""), None);
        assert_eq!(numeric_pane_id("terminal_"), None);
        assert_eq!(numeric_pane_id("terminal_abc"), None);
        assert_eq!(numeric_pane_id("pane_1"), None);
        assert_eq!(numeric_pane_id("abc"), None);
    }

    // ---- parse_zellij_subscribe_viewport tests ----

    #[test]
    fn parse_subscribe_pane_update_viewport() {
        let line = r#"{"event":"pane_update","pane_id":"terminal_1","data":{"viewport":["line1","line2","line3"],"scrollback":[],"is_initial":true}}"#;
        let viewport = parse_zellij_subscribe_viewport(line);
        assert_eq!(
            viewport,
            Some(vec![
                "line1".to_string(),
                "line2".to_string(),
                "line3".to_string(),
            ])
        );
    }

    #[test]
    fn parse_subscribe_pane_update_top_level_viewport() {
        let line = r#"{"event":"pane_update","pane_id":"terminal_1","viewport":["a","b"],"scrollback":null,"is_initial":true}"#;
        let viewport = parse_zellij_subscribe_viewport(line);
        assert_eq!(
            viewport,
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn parse_subscribe_pane_update_empty_viewport() {
        let line = r#"{"event":"pane_update","pane_id":"terminal_1","data":{"viewport":[],"scrollback":[],"is_initial":true}}"#;
        let viewport = parse_zellij_subscribe_viewport(line);
        assert_eq!(viewport, Some(vec![]));
    }

    #[test]
    fn parse_subscribe_pane_update_with_ansi() {
        let line = r#"{"event":"pane_update","pane_id":"terminal_1","data":{"viewport":["\u001b[32mgreen\u001b[0m","normal"],"scrollback":[],"is_initial":false}}"#;
        let viewport = parse_zellij_subscribe_viewport(line);
        assert_eq!(
            viewport,
            Some(vec![
                "\u{1b}[32mgreen\u{1b}[0m".to_string(),
                "normal".to_string(),
            ])
        );
    }

    #[test]
    fn parse_subscribe_pane_closed_returns_none() {
        let line = r#"{"event":"pane_closed","pane_id":"terminal_1"}"#;
        assert_eq!(parse_zellij_subscribe_viewport(line), None);
    }

    #[test]
    fn parse_subscribe_unknown_event() {
        let line = r#"{"event":"session_closed"}"#;
        assert_eq!(parse_zellij_subscribe_viewport(line), None);
    }

    #[test]
    fn parse_subscribe_invalid_json() {
        assert_eq!(parse_zellij_subscribe_viewport("not json"), None);
        assert_eq!(parse_zellij_subscribe_viewport(""), None);
    }

    // ---- is_zellij_pane_closed tests ----

    #[test]
    fn test_is_zellij_pane_closed_true() {
        assert!(is_zellij_pane_closed(
            r#"{"event":"pane_closed","pane_id":"terminal_1"}"#
        ));
    }

    #[test]
    fn test_is_zellij_pane_closed_false() {
        assert!(!is_zellij_pane_closed(
            r#"{"event":"pane_update","pane_id":"terminal_1","data":{}}"#
        ));
        assert!(!is_zellij_pane_closed("not json"));
        assert!(!is_zellij_pane_closed(""));
    }

    // ---- viewport_to_ansi_chunk tests ----

    #[test]
    fn viewport_to_ansi_basic() {
        let viewport = vec!["hello".to_string(), "world".to_string()];
        let chunk = viewport_to_ansi_chunk(&viewport);
        assert!(chunk.contains("\x1b[H"), "should contain home");
        assert!(chunk.contains("\x1b[2J"), "should contain clear screen");
        assert!(chunk.contains("\x1b[?25l"), "should hide cursor");
        assert!(chunk.contains("\x1b[?25h"), "should show cursor");
        assert!(chunk.contains("hello\r\nworld"), "should join lines with CRLF");
    }

    #[test]
    fn viewport_to_ansi_no_trailing_crlf() {
        let viewport = vec!["line1".to_string()];
        let chunk = viewport_to_ansi_chunk(&viewport);
        assert!(!chunk.ends_with("\r\n"), "must not trail with CRLF");
        assert!(!chunk.ends_with('\n'), "must not trail with LF");
        assert!(chunk.ends_with("line1\x1b[?25h"), "should end with last line + show cursor");
    }

    #[test]
    fn viewport_to_ansi_multiline_no_trailing_crlf() {
        let viewport = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let chunk = viewport_to_ansi_chunk(&viewport);
        assert!(!chunk.ends_with("\r\n"));
        assert!(!chunk.ends_with('\n'));
        assert!(chunk.contains("a\r\nb\r\nc"));
    }

    #[test]
    fn viewport_to_ansi_empty() {
        assert_eq!(viewport_to_ansi_chunk(&[]), "");
    }

    #[test]
    fn viewport_to_ansi_preserves_ansi_content() {
        let viewport = vec![
            "\u{1b}[32mgreen\u{1b}[0m text".to_string(),
            "\u{1b}[1mbold\u{1b}[0m".to_string(),
        ];
        let chunk = viewport_to_ansi_chunk(&viewport);
        assert!(chunk.contains("\u{1b}[32mgreen\u{1b}[0m text"));
        assert!(chunk.contains("\u{1b}[1mbold\u{1b}[0m"));
    }

    // ---- parse_zellij_cursor_from_list_panes tests ----

    #[test]
    fn parse_zellij_cursor_single_pane() {
        let json = r#"[
            {"id":1,"is_plugin":false,"cursor_coordinates_in_pane":{"x":10,"y":5}}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 1),
            Some((10, 5))
        );
    }

    #[test]
    fn parse_zellij_cursor_array_format() {
        let json = r#"[
            {"id":1,"cursor_coordinates_in_pane":[3, 7]}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 1),
            Some((3, 7))
        );
    }

    #[test]
    fn parse_zellij_cursor_array_zero() {
        let json = r#"[
            {"id":1,"cursor_coordinates_in_pane":[0, 0]}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 1),
            Some((0, 0))
        );
    }

    #[test]
    fn parse_zellij_cursor_multiple_panes() {
        let json = r#"[
            {"id":1,"is_plugin":false,"cursor_coordinates_in_pane":{"x":0,"y":0}},
            {"id":2,"is_plugin":false,"cursor_coordinates_in_pane":{"x":80,"y":24}},
            {"id":3,"is_plugin":true}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 1),
            Some((0, 0))
        );
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 2),
            Some((80, 24))
        );
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 3),
            None
        );
    }

    #[test]
    fn parse_zellij_cursor_pane_not_found() {
        let json = r#"[
            {"id":1,"cursor_coordinates_in_pane":{"x":10,"y":5}}
        ]"#;
        assert_eq!(parse_zellij_cursor_from_list_panes(json, 2), None);
    }

    #[test]
    fn parse_zellij_cursor_missing_field() {
        assert_eq!(
            parse_zellij_cursor_from_list_panes(r#"[]"#, 1),
            None
        );
        assert_eq!(
            parse_zellij_cursor_from_list_panes(
                r#"[{"id":1}]"#,
                1
            ),
            None
        );
        assert_eq!(
            parse_zellij_cursor_from_list_panes("bad json", 1),
            None
        );
    }

    #[test]
    fn parse_zellij_cursor_zero_coordinates() {
        let json = r#"[
            {"id":1,"cursor_coordinates_in_pane":{"x":0,"y":0}}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 1),
            Some((0, 0))
        );
    }

    #[test]
    fn parse_zellij_cursor_id_as_number_in_json() {
        let json = r#"[
            {"id":42,"cursor_coordinates_in_pane":{"x":5,"y":3}}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 42),
            Some((5, 3))
        );
    }
}
