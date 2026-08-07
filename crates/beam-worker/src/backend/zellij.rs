use std::path::PathBuf;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::warn;

use super::subscribe::{
    numeric_pane_id, parse_zellij_cursor_from_list_panes, run_zellij_subscribe,
};
use super::{
    RAW_INPUT_ENTER_DELAY, SessionBackend, SpawnOpts, ZELLIJ_ACTION_TIMEOUT,
    ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS, ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL,
    ZELLIJ_PANE_PROCESS_CHECK_ATTEMPTS, ZELLIJ_PANE_PROCESS_CHECK_INTERVAL,
    ZELLIJ_SPAWN_MAX_ATTEMPTS, ZELLIJ_SPAWN_RETRY_BACKOFF, ZELLIJ_SPAWN_TIMEOUT,
};

/// Error marker for a `zellij action` call that exceeded
/// [`ZELLIJ_ACTION_TIMEOUT`]. Used to trigger a subscribe rebuild: when the
/// zellij server wedges hard enough for an action to time out, the subscribe
/// stream is almost certainly dead too.
#[derive(Debug)]
pub(crate) struct ZellijActionTimeoutError {
    pub session: String,
    pub args: Vec<String>,
}

impl std::fmt::Display for ZellijActionTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "zellij action timed out after {}s: zellij --session {} action {}",
            ZELLIJ_ACTION_TIMEOUT.as_secs(),
            self.session,
            self.args.join(" ")
        )
    }
}

impl std::error::Error for ZellijActionTimeoutError {}

#[derive(Debug)]
pub struct ZellijBackend {
    session_name: String,
    owns_session: bool,
    pane_id: StdMutex<Option<String>>,
    data_tx: broadcast::Sender<String>,
    tmp_config_dir: StdMutex<Option<PathBuf>>,
    intentional_exit: Arc<AtomicBool>,
    resurrect_pid: Option<u32>,
    reattach: AtomicBool,
    subscribe_started: Arc<AtomicBool>,
    subscribe_stop: StdMutex<Arc<AtomicBool>>,
}

impl ZellijBackend {
    pub fn new(session_name: String) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_name,
            owns_session: true,
            pane_id: StdMutex::new(None),
            data_tx,
            tmp_config_dir: StdMutex::new(None),
            intentional_exit: Arc::new(AtomicBool::new(false)),
            resurrect_pid: None,
            reattach: AtomicBool::new(false),
            subscribe_started: Arc::new(AtomicBool::new(false)),
            subscribe_stop: StdMutex::new(Arc::new(AtomicBool::new(false))),
        }
    }

    #[allow(dead_code)]
    pub fn attach_existing(target: String, reattach: bool) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_name: target,
            owns_session: false,
            pane_id: StdMutex::new(None),
            data_tx,
            tmp_config_dir: StdMutex::new(None),
            intentional_exit: Arc::new(AtomicBool::new(false)),
            resurrect_pid: None,
            reattach: AtomicBool::new(reattach),
            subscribe_started: Arc::new(AtomicBool::new(false)),
            subscribe_stop: StdMutex::new(Arc::new(AtomicBool::new(false))),
        }
    }

    /// Probe whether a live (non-EXITED) zellij session exists. Bounded by
    /// [`ZELLIJ_ACTION_TIMEOUT`]; on timeout/error returns `Err` so callers
    /// can decide how to treat "unknown" (spawn retries treat it as absent,
    /// `is_alive` treats it as alive to avoid a false CliExit).
    async fn probe_session(name: &str) -> Result<bool> {
        let mut cmd = tokio::process::Command::new("zellij");
        cmd.args(["list-sessions", "--no-formatting"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let out = tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output())
            .await
            .context("zellij list-sessions timed out")?
            .context("failed to run zellij list-sessions")?;
        let s = String::from_utf8_lossy(&out.stdout);
        Ok(s.lines().any(|l| l.contains(name) && !l.contains("EXITED")))
    }

    pub async fn has_session(name: &str) -> bool {
        Self::probe_session(name).await.unwrap_or(false)
    }

    /// Force-delete a session this backend just created after a failed spawn
    /// attempt, so the retry starts from a clean slate. Best-effort.
    async fn teardown_broken_session(name: &str) {
        let mut cmd = tokio::process::Command::new("zellij");
        cmd.args(["delete-session", name, "-f"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let _ = tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output()).await;
    }

    /// Run a `zellij action` bounded by [`ZELLIJ_ACTION_TIMEOUT`]. A timeout
    /// kills the action client (kill_on_drop) and returns a
    /// [`ZellijActionTimeoutError`] instead of blocking a tokio thread
    /// forever on a wedged zellij server.
    pub async fn run_zellij_action(session: &str, args: &[&str]) -> Result<String> {
        let mut cmd = tokio::process::Command::new("zellij");
        cmd.arg("--session")
            .arg(session)
            .arg("action")
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let out = match tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output()).await {
            Ok(res) => res.context("failed to spawn zellij action")?,
            Err(_) => {
                return Err(ZellijActionTimeoutError {
                    session: session.to_string(),
                    args: args.iter().map(|s| s.to_string()).collect(),
                }
                .into());
            }
        };
        if !out.status.success() {
            bail!(
                "zellij action failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    pub async fn send_zellij_action(session: &str, args: &[&str]) -> Result<()> {
        Self::run_zellij_action(session, args).await.map(|_| ())
    }

    /// Instance-level action wrapper: on a timeout the subscribe stream is
    /// presumed dead as well, so it is rebuilt before the error is returned.
    async fn action(&self, args: &[&str]) -> Result<String> {
        match Self::run_zellij_action(&self.session_name, args).await {
            Ok(out) => Ok(out),
            Err(err) => {
                if err.downcast_ref::<ZellijActionTimeoutError>().is_some() {
                    self.restart_subscribe();
                }
                Err(err)
            }
        }
    }

    /// Stop the current subscribe task (if any) and spawn a fresh one.
    fn restart_subscribe(&self) {
        let pane_id = self.pane_id.lock().unwrap().clone();
        let Some(pane_id) = pane_id else {
            return;
        };
        self.subscribe_stop
            .lock()
            .unwrap()
            .store(true, Ordering::Relaxed);
        let stop = Arc::new(AtomicBool::new(false));
        *self.subscribe_stop.lock().unwrap() = stop.clone();
        tokio::spawn(run_zellij_subscribe(
            self.session_name.clone(),
            pane_id,
            self.data_tx.clone(),
            stop,
        ));
        warn!(
            "zellij action timed out; restarted subscribe for session {}",
            self.session_name
        );
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

    async fn discover_pane_id(session: &str) -> Option<String> {
        let mut cmd = tokio::process::Command::new("zellij");
        cmd.arg("--session")
            .arg(session)
            .args(["action", "list-panes", "--json"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let out = tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output())
            .await
            .ok()?
            .ok()?;
        if !out.status.success() {
            return None;
        }
        Self::parse_terminal_pane_id(&out.stdout)
    }

    fn pane_id_str(&self) -> String {
        self.pane_id
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "terminal_0".to_string())
    }

    async fn wait_for_zellij_pane_id(&self) -> Result<String> {
        for attempt in 0..ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS {
            if let Some(pane_id) = Self::discover_pane_id(&self.session_name).await {
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

    /// Confirm the pane's process was actually spawned. zellij 0.44.3
    /// intermittently reports the pane as created while the pty is never
    /// registered, leaving an empty pane with no process; poll briefly to
    /// absorb the normal fork/registration delay before declaring failure.
    async fn wait_for_pane_process(&self) -> bool {
        for attempt in 0..ZELLIJ_PANE_PROCESS_CHECK_ATTEMPTS {
            if Self::session_pane_process_running(&self.session_name) {
                return true;
            }
            if attempt + 1 < ZELLIJ_PANE_PROCESS_CHECK_ATTEMPTS {
                tokio::time::sleep(ZELLIJ_PANE_PROCESS_CHECK_INTERVAL).await;
            }
        }
        false
    }

    /// Pure matcher: does a NUL-separated /proc cmdline belong to the zellij
    /// server process of `session`?
    fn cmdline_is_session_server(cmdline: &str, session: &str) -> bool {
        let argv: Vec<&str> = cmdline.split('\0').filter(|s| !s.is_empty()).collect();
        let is_zellij_bin = argv
            .first()
            .and_then(|a| a.rsplit('/').next())
            .map(|bin| bin == "zellij")
            .unwrap_or(false);
        is_zellij_bin
            && argv.iter().any(|a| *a == "--server")
            && argv
                .iter()
                .any(|a| a.rsplit('/').next() == Some(session))
    }

    /// Parse the parent pid out of /proc/<pid>/stat contents. The comm field
    /// (field 2, parenthesised) may contain spaces, so parse from the last
    /// ')'; ppid is the 2nd field after it.
    fn parse_stat_ppid(stat: &str) -> Option<u32> {
        let rparen = stat.rfind(')')?;
        stat[rparen + 1..]
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
    }

    /// On Linux, detect the "pane created but no process" failure by finding
    /// the session's zellij server process in /proc and checking whether any
    /// process has it as parent (the pane process is a direct child of the
    /// server). Note: /proc/<pid>/task/<pid>/children is unusable on kernels
    /// built without CONFIG_PROC_CHILDREN, hence the ppid reverse scan.
    /// Returns true when the pane process exists or when it cannot be
    /// determined (assume healthy to avoid tearing down good sessions).
    #[cfg(target_os = "linux")]
    fn session_pane_process_running(session: &str) -> bool {
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return true;
        };
        let procs: Vec<std::path::PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.as_bytes().first())
                    .map(|b| b.is_ascii_digit())
                    .unwrap_or(false)
            })
            .collect();
        let server_pid = procs.iter().find_map(|path| {
            let cmdline = std::fs::read_to_string(path.join("cmdline")).ok()?;
            if !Self::cmdline_is_session_server(&cmdline, session) {
                return None;
            }
            path.file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.parse::<u32>().ok())
        });
        let Some(server_pid) = server_pid else {
            // Server not found — cannot determine; assume healthy.
            return true;
        };
        procs.iter().any(|path| {
            std::fs::read_to_string(path.join("stat"))
                .ok()
                .and_then(|stat| Self::parse_stat_ppid(&stat))
                == Some(server_pid)
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn session_pane_process_running(_session: &str) -> bool {
        true
    }

    async fn ensure_zellij_subscribe_started(&self) -> Result<()> {
        if self.pane_id.lock().unwrap().is_none() {
            let discovered = self.wait_for_zellij_pane_id().await?;
            *self.pane_id.lock().unwrap() = Some(discovered);
        }
        if !self.subscribe_started.swap(true, Ordering::SeqCst) {
            let pane_id = self.pane_id.lock().unwrap().clone();
            if let Some(pid) = pane_id {
                let session = self.session_name.clone();
                let tx = self.data_tx.clone();
                let stop = self.subscribe_stop.lock().unwrap().clone();
                tokio::spawn(run_zellij_subscribe(session, pid, tx, stop));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SessionBackend for ZellijBackend {
    async fn spawn(&self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        if self.reattach.load(Ordering::SeqCst) || Self::has_session(&self.session_name).await {
            self.reattach.store(true, Ordering::SeqCst);
            self.ensure_zellij_subscribe_started().await?;
            return Ok(());
        }

        let (tmp_dir, config_path, layout_path) = Self::write_runtime_files(bin, args, &opts)?;
        *self.tmp_config_dir.lock().unwrap() = Some(tmp_dir);

        let zellij_args: Vec<String> = if self.reattach.load(Ordering::SeqCst) {
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

        // Retry loop: zellij 0.44.3 intermittently reports the pane as
        // created while the pty is never registered (server log: "failed to
        // find terminal fd for id 0"), leaving an empty pane with no process.
        // Detect that via the pane-process check and retry on a fresh session.
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=ZELLIJ_SPAWN_MAX_ATTEMPTS {
            // Use tokio's Command (not std) so the spawn wait can be bounded
            // by a timeout; `kill_on_drop` makes sure a timed-out attach
            // client is killed instead of lingering and retrying the session
            // socket forever.
            let mut cmd = tokio::process::Command::new("zellij");
            cmd.args(&zellij_args)
                .current_dir(&opts.cwd)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            for (k, v) in &opts.env {
                cmd.env(k, v);
            }

            let out = match tokio::time::timeout(ZELLIJ_SPAWN_TIMEOUT, cmd.output()).await {
                Ok(res) => res.context("failed to spawn zellij backend")?,
                Err(_) => {
                    last_err = Some(anyhow::anyhow!(
                        "zellij backend timed out after {}s waiting for session {} to start (zellij server may have crashed during session setup)",
                        ZELLIJ_SPAWN_TIMEOUT.as_secs(),
                        self.session_name
                    ));
                    Self::teardown_broken_session(&self.session_name).await;
                    continue;
                }
            };
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                if stderr.contains("Session already exists") {
                    self.ensure_zellij_subscribe_started().await?;
                    return Ok(());
                }
                bail!("zellij backend failed: {}", stderr.trim());
            }

            match self.wait_for_zellij_pane_id().await {
                Ok(pane_id) => {
                    if self.wait_for_pane_process().await {
                        *self.pane_id.lock().unwrap() = Some(pane_id);
                        last_err = None;
                        break;
                    }
                    last_err = Some(anyhow::anyhow!(
                        "zellij session {} exposed a pane but never spawned its process (zellij pty race)",
                        self.session_name
                    ));
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
            tracing::warn!(
                "zellij spawn attempt {}/{} for session {} failed: {}",
                attempt,
                ZELLIJ_SPAWN_MAX_ATTEMPTS,
                self.session_name,
                last_err
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_default()
            );
            Self::teardown_broken_session(&self.session_name).await;
            tokio::time::sleep(ZELLIJ_SPAWN_RETRY_BACKOFF).await;
        }
        if let Some(err) = last_err {
            return Err(err);
        }

        self.ensure_zellij_subscribe_started().await?;
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        let pane_id = self.pane_id_str();
        self.action(&["write-chars", "--pane-id", &pane_id, text])
            .await
            .map(|_| ())
    }

    async fn send_enter(&self) -> Result<()> {
        let pane_id = self.pane_id_str();
        self.action(&["send-keys", "--pane-id", &pane_id, "Enter"])
            .await
            .map(|_| ())
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
        let pane_id = self.pane_id_str();
        self.action(&["paste", "--pane-id", &pane_id, text])
            .await
            .map(|_| ())
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        let pane_id = self.pane_id_str();
        self.action(&["write-chars", "--pane-id", &pane_id, text])
            .await
            .map(|_| ())
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.paste_text(text).await?;
        tokio::time::sleep(RAW_INPUT_ENTER_DELAY).await;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        let args = Self::dump_screen_viewport_args(&self.pane_id_str());
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let out = self.action(&args_refs).await?;
        Ok(out.replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.capture_viewport().await
    }

    /// Timeout/error is treated as "unknown → alive": a wedged zellij server
    /// must not trigger a false CliExit that would tear down the session.
    async fn is_alive(&self) -> Result<bool> {
        Ok(Self::probe_session(&self.session_name).await.unwrap_or(true))
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        if let Some(pid) = self.resurrect_pid {
            return Ok(Some(pid));
        }
        let mut cmd = tokio::process::Command::new("ps");
        cmd.args(["-eo", "pid=,comm="])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let out = tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output())
            .await
            .context("ps timed out")?
            .context("failed to run ps")?;
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

    async fn kill(&self) -> Result<()> {
        self.subscribe_stop
            .lock()
            .unwrap()
            .store(true, Ordering::Relaxed);
        self.intentional_exit.store(true, Ordering::Relaxed);
        let tmp = self.tmp_config_dir.lock().unwrap().take();
        if let Some(tmp) = tmp {
            let _ = std::fs::remove_dir_all(&tmp);
        }
        Ok(())
    }

    async fn destroy_session(&self) -> Result<()> {
        self.kill().await?;
        if self.owns_session {
            let mut cmd = tokio::process::Command::new("zellij");
            cmd.args(["delete-session", &self.session_name, "-f"])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true);
            let _ = tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output()).await;
        }
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        let pane_id = self.pane_id.lock().unwrap().clone();
        let pane_id = match pane_id {
            Some(id) => id,
            None => return Ok(None),
        };
        let numeric_id = match numeric_pane_id(&pane_id) {
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
        let out = match tokio::time::timeout(ZELLIJ_ACTION_TIMEOUT, cmd.output()).await {
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

#[cfg(test)]
mod tests {
    use super::ZellijBackend;

    #[test]
    fn cmdline_matcher_accepts_own_server() {
        let cmdline =
            "/home/user/.cargo/bin/zellij\0--server\0/run/user/1000/zellij/contract_version_1/beam-abc123\0";
        assert!(ZellijBackend::cmdline_is_session_server(
            cmdline, "beam-abc123"
        ));
    }

    #[test]
    fn cmdline_matcher_rejects_other_session_server() {
        let cmdline =
            "/home/user/.cargo/bin/zellij\0--server\0/run/user/1000/zellij/contract_version_1/beam-other\0";
        assert!(!ZellijBackend::cmdline_is_session_server(
            cmdline, "beam-abc123"
        ));
    }

    #[test]
    fn cmdline_matcher_rejects_attach_client_and_subscribe() {
        let attach = "/home/user/.cargo/bin/zellij\0--config\0/tmp/x/config.kdl\0attach\0--create-background\0beam-abc123\0";
        assert!(!ZellijBackend::cmdline_is_session_server(
            attach, "beam-abc123"
        ));
        let subscribe = "zellij\0--session\0beam-abc123\0subscribe\0--pane-id\0terminal_0\0";
        assert!(!ZellijBackend::cmdline_is_session_server(
            subscribe, "beam-abc123"
        ));
    }

    #[test]
    fn cmdline_matcher_rejects_non_zellij_process() {
        let cmdline = "/bin/sh\0/home/user/.beam/run/worker-wrapper.sh\0codex\0";
        assert!(!ZellijBackend::cmdline_is_session_server(
            cmdline, "beam-abc123"
        ));
    }

    #[test]
    fn parse_stat_ppid_reads_fourth_field() {
        let stat = "1234 (zellij) S 2843 1234 1234 0 -1 4194304 100 0 0 0 5 2 0 0 20 0 10 0 99 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0";
        assert_eq!(ZellijBackend::parse_stat_ppid(stat), Some(2843));
    }

    #[test]
    fn parse_stat_ppid_tolerates_comm_with_spaces_and_parens() {
        let stat = "1234 (weird ) name) S 5678 1234";
        assert_eq!(ZellijBackend::parse_stat_ppid(stat), Some(5678));
    }
}
