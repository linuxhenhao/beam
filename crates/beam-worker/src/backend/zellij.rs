use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::broadcast;

use super::subscribe::{
    numeric_pane_id, parse_zellij_cursor_from_list_panes, run_zellij_subscribe,
};
use super::{
    RAW_INPUT_ENTER_DELAY, SessionBackend, SpawnOpts, ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS,
    ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL,
};

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
        match Command::new("zellij")
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

    /// Build `dump-screen` args for visible viewport only (no `--full`).
    pub(crate) fn dump_screen_viewport_args(pane_id: &str) -> Vec<String> {
        vec![
            "dump-screen".to_string(),
            "--pane-id".to_string(),
            pane_id.to_string(),
        ]
    }

    fn kdl_string(value: &str) -> String {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    }

    pub(super) fn write_runtime_files(
        bin: &str,
        bin_args: &[String],
        opts: &SpawnOpts,
    ) -> Result<(PathBuf, PathBuf, PathBuf)> {
        let tmp = std::env::temp_dir().join(format!("beam-zellij-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp)?;
        let config_path = tmp.join("config.kdl");
        let layout_path = tmp.join("layout.kdl");

        let config = concat!(
            "show_startup_tips false\n",
            "pane_frames false\n",
            "web_server true\n",
            "web_sharing \"on\"\n",
        );
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

    pub(super) fn parse_terminal_pane_id(stdout: &[u8]) -> Option<String> {
        let json: serde_json::Value = serde_json::from_slice(stdout).ok()?;
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

    fn discover_pane_id(session: &str) -> Option<String> {
        let out = Command::new("zellij")
            .arg("--session")
            .arg(session)
            .args(["action", "list-panes", "--json"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        Self::parse_terminal_pane_id(&out.stdout)
    }

    fn pane_id_str(&self) -> &str {
        self.pane_id.as_deref().unwrap_or("terminal_0")
    }

    async fn wait_for_zellij_pane_id(&self) -> Result<String> {
        for attempt in 0..ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS {
            if let Some(pane_id) = Self::discover_pane_id(&self.session_name) {
                return Ok(pane_id);
            }
            if attempt + 1 < ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS {
                tokio::time::sleep(ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL).await;
            }
        }
        bail!(
            "zellij session {} did not expose a terminal pane within {}ms",
            self.session_name,
            ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL.as_millis()
                * ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS as u128
        );
    }

    async fn ensure_zellij_subscribe_started(&mut self) -> Result<()> {
        if self.pane_id.is_none() {
            self.pane_id = Some(self.wait_for_zellij_pane_id().await?);
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
        Ok(())
    }
}

#[async_trait]
impl SessionBackend for ZellijBackend {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        self.reattach = self.reattach || Self::has_session(&self.session_name);

        if self.reattach && Self::has_session(&self.session_name) {
            self.ensure_zellij_subscribe_started().await?;
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
                self.ensure_zellij_subscribe_started().await?;
                return Ok(());
            }
            bail!("zellij backend failed: {}", stderr.trim());
        }

        self.ensure_zellij_subscribe_started().await?;
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
        self.paste_text(text).await?;
        tokio::time::sleep(RAW_INPUT_ENTER_DELAY).await;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        let args = Self::dump_screen_viewport_args(self.pane_id_str());
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let out = Self::run_zellij_action(&self.session_name, &args_refs)?;
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
        let out = Command::new("ps").args(["-eo", "pid=,comm="]).output()?;
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
            let _ = Command::new("zellij")
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
        let out = match Command::new("zellij")
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
