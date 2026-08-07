use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio::sync::broadcast;

use super::subscribe::{
    numeric_pane_id, parse_zellij_cursor_from_list_panes, run_zellij_subscribe,
};
use super::zellij::ZellijBackend;
use super::{RAW_INPUT_ENTER_DELAY, SessionBackend, SpawnOpts};

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

    async fn send_action(&self, args: &[&str]) -> Result<()> {
        ZellijBackend::send_zellij_action(&self.session_name, args).await
    }

    async fn dump_screen(&self) -> Result<String> {
        let args = ZellijBackend::dump_screen_viewport_args(&self.pane_id);
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        ZellijBackend::run_zellij_action(&self.session_name, &args_refs).await
    }
}

#[async_trait]
impl SessionBackend for ZellijObserveBackend {
    async fn spawn(&self, _bin: &str, _args: &[String], _opts: SpawnOpts) -> Result<()> {
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
            .await
    }

    async fn send_enter(&self) -> Result<()> {
        self.send_action(&["send-keys", "--pane-id", self.pane_id.as_str(), "Enter"])
            .await
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
            .await
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        self.send_action(&["write-chars", "--pane-id", self.pane_id.as_str(), text])
            .await
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.paste_text(text).await?;
        tokio::time::sleep(RAW_INPUT_ENTER_DELAY).await;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        Ok(self.dump_screen().await?.replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.capture_viewport().await
    }

    async fn is_alive(&self) -> Result<bool> {
        Ok(ZellijBackend::has_session(&self.session_name).await)
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        Ok(self.child_pid)
    }

    async fn kill(&self) -> Result<()> {
        self.subscribe_stop.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn destroy_session(&self) -> Result<()> {
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        let numeric_id = match numeric_pane_id(&self.pane_id) {
            Some(id) => id,
            None => return Ok(None),
        };
        let mut cmd = tokio::process::Command::new("zellij");
        cmd.arg("--session")
            .arg(&self.session_name)
            .args(["action", "list-panes", "--json", "--all"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let out = match tokio::time::timeout(crate::backend::ZELLIJ_ACTION_TIMEOUT, cmd.output()).await
        {
            Ok(Ok(out)) if out.status.success() => out,
            _ => return Ok(None),
        };
        let json = String::from_utf8_lossy(&out.stdout);
        Ok(parse_zellij_cursor_from_list_panes(&json, numeric_id))
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}
