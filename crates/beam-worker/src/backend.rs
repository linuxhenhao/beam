use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command as TokioCommand};
use tokio::sync::{RwLock, broadcast};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct SpawnOpts {
    pub cwd: String,
    pub cols: u16,
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
    /// Backends that cannot track the cursor return `Ok(None)`.
    async fn cursor_position(&self) -> Result<Option<(u16, u16)>>;
    fn subscribe(&self) -> broadcast::Receiver<String>;
}

#[derive(Debug)]
pub struct TmuxPipeBackend {
    session_name: String,
    pane_target: String,
    fifo_path: PathBuf,
    owns_session: bool,
    create_session: bool,
    attached_pipe: bool,
    stop_flag: Arc<AtomicBool>,
    data_tx: broadcast::Sender<String>,
    recent_output: Arc<RwLock<String>>,
}

impl TmuxPipeBackend {
    pub fn new(session_name: String) -> Self {
        let fifo_path = std::env::temp_dir().join(format!("beam-{}.fifo", session_name));
        let (data_tx, _) = broadcast::channel(512);
        Self {
            pane_target: session_name.clone(),
            session_name,
            fifo_path,
            owns_session: true,
            create_session: true,
            attached_pipe: false,
            stop_flag: Arc::new(AtomicBool::new(false)),
            data_tx,
            recent_output: Arc::new(RwLock::new(String::new())),
        }
    }

    pub fn attach_existing(target: String) -> Self {
        let session_name = target
            .split(':')
            .next()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| target.clone());
        let fifo_path = std::env::temp_dir().join(format!(
            "beam-{}.fifo",
            target.replace([':', '.', '%', '@'], "_")
        ));
        let (data_tx, _) = broadcast::channel(512);
        Self {
            pane_target: target,
            session_name,
            fifo_path,
            owns_session: false,
            create_session: false,
            attached_pipe: false,
            stop_flag: Arc::new(AtomicBool::new(false)),
            data_tx,
            recent_output: Arc::new(RwLock::new(String::new())),
        }
    }

    fn tmux_env() -> Vec<(String, String)> {
        std::env::vars()
            .filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE")
            .collect()
    }

    fn run_tmux<I, S>(&self, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let out = Command::new("tmux")
            .args(args)
            .envs(Self::tmux_env())
            .output()
            .context("failed to spawn tmux")?;
        if !out.status.success() {
            bail!(
                "tmux command failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn run_tmux_with_input<I, S>(&self, args: I, input: &str) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut child = Command::new("tmux")
            .args(args)
            .envs(Self::tmux_env())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("failed to spawn tmux with stdin")?;
        if let Some(stdin) = child.stdin.as_mut() {
            use std::io::Write;
            stdin
                .write_all(input.as_bytes())
                .context("failed to write tmux stdin payload")?;
        }
        let out = child
            .wait_with_output()
            .context("failed to wait for tmux command")?;
        if !out.status.success() {
            bail!(
                "tmux command failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn create_session(&self, bin: &str, args: &[String], opts: &SpawnOpts) -> Result<()> {
        let mut cmd = Command::new("tmux");
        cmd.arg("new-session")
            .arg("-d")
            .arg("-s")
            .arg(&self.session_name)
            .arg("-x")
            .arg(opts.cols.to_string())
            .arg("-y")
            .arg(opts.rows.to_string())
            .arg("-c")
            .arg(&opts.cwd);
        for (k, v) in &opts.env {
            cmd.arg("-e").arg(format!("{}={}", k, v));
        }
        cmd.arg("--").arg(bin).args(args).envs(Self::tmux_env());
        let out = cmd.output().context("failed to create tmux session")?;
        if !out.status.success() {
            bail!(
                "tmux new-session failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn start_fifo_reader(&self) -> Result<()> {
        if self.fifo_path.exists() {
            let _ = std::fs::remove_file(&self.fifo_path);
        }
        let status = Command::new("mkfifo")
            .arg(&self.fifo_path)
            .status()
            .context("failed to run mkfifo")?;
        if !status.success() {
            bail!("mkfifo failed for {}", self.fifo_path.display());
        }

        let fifo = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&self.fifo_path)
            .context("failed to open tmux fifo")?;

        let stop = self.stop_flag.clone();
        let tx = self.data_tx.clone();
        let recent = self.recent_output.clone();

        thread::spawn(move || {
            let mut fifo = fifo;
            let mut buf = [0_u8; 8192];
            while !stop.load(Ordering::Relaxed) {
                match fifo.read(&mut buf) {
                    Ok(0) => thread::sleep(Duration::from_millis(30)),
                    Ok(n) => {
                        let chunk = String::from_utf8_lossy(&buf[..n]).into_owned();
                        if !chunk.is_empty() {
                            if let Ok(mut lock) = recent.try_write() {
                                lock.push_str(&chunk);
                                if lock.len() > 65_536 {
                                    let drain = lock.len().saturating_sub(65_536);
                                    lock.drain(..drain);
                                }
                            }
                            let _ = tx.send(chunk);
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(30));
                    }
                    Err(err) => {
                        warn!("tmux fifo read failed: {}", err);
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });

        Ok(())
    }

    fn attach_pipe(&mut self) -> Result<()> {
        self.run_tmux([
            "pipe-pane",
            "-O",
            "-t",
            self.pane_target.as_str(),
            &format!("cat > {}", self.fifo_path.display()),
        ])?;
        self.attached_pipe = true;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PtyBackend {
    child: Option<Arc<tokio::sync::Mutex<Child>>>,
    stdin: Option<Arc<tokio::sync::Mutex<ChildStdin>>>,
    data_tx: broadcast::Sender<String>,
    recent_output: Arc<RwLock<String>>,
}

impl PtyBackend {
    pub fn new() -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            child: None,
            stdin: None,
            data_tx,
            recent_output: Arc::new(RwLock::new(String::new())),
        }
    }

    async fn append_output(
        recent: Arc<RwLock<String>>,
        tx: broadcast::Sender<String>,
        chunk: Vec<u8>,
    ) {
        let chunk = String::from_utf8_lossy(&chunk).into_owned();
        if chunk.is_empty() {
            return;
        }
        {
            let mut lock = recent.write().await;
            lock.push_str(&chunk);
            if lock.len() > 65_536 {
                let drain = lock.len().saturating_sub(65_536);
                lock.drain(..drain);
            }
        }
        let _ = tx.send(chunk);
    }
}

#[async_trait]
impl SessionBackend for TmuxPipeBackend {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        if self.create_session {
            self.create_session(bin, args, &opts)?;
        }
        self.start_fifo_reader()?;
        self.attach_pipe()?;
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        self.run_tmux([
            "send-keys",
            "-t",
            self.pane_target.as_str(),
            "-l",
            "--",
            text,
        ])?;
        Ok(())
    }

    async fn send_enter(&self) -> Result<()> {
        self.run_tmux(["send-keys", "-t", self.pane_target.as_str(), "Enter"])?;
        Ok(())
    }

    async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
        let mut args = vec![
            "send-keys".to_string(),
            "-t".to_string(),
            self.pane_target.clone(),
        ];
        args.extend(keys.iter().cloned());
        self.run_tmux(args)?;
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        self.run_tmux_with_input(["load-buffer", "-"], text)?;
        self.run_tmux(["paste-buffer", "-d", "-p", "-t", self.pane_target.as_str()])?;
        Ok(())
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        self.send_text(text).await
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.send_text(text).await?;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        let out = self.run_tmux(["capture-pane", "-e", "-p", "-t", self.pane_target.as_str()])?;
        Ok(out.replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        let out = self.run_tmux([
            "capture-pane",
            "-e",
            "-p",
            "-t",
            self.pane_target.as_str(),
            "-S",
            "-",
            "-E",
            "-",
        ])?;
        Ok(out.replace('\n', "\r\n"))
    }

    async fn is_alive(&self) -> Result<bool> {
        let result = Command::new("tmux")
            .args([
                "display-message",
                "-p",
                "-t",
                self.pane_target.as_str(),
                "#{pane_id}",
            ])
            .envs(Self::tmux_env())
            .output()
            .context("failed to probe tmux pane")?;
        Ok(result.status.success() && !String::from_utf8_lossy(&result.stdout).trim().is_empty())
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        let result = Command::new("tmux")
            .args([
                "display-message",
                "-p",
                "-t",
                self.pane_target.as_str(),
                "#{pane_pid}",
            ])
            .envs(Self::tmux_env())
            .output()
            .context("failed to query tmux pane pid")?;
        if !result.status.success() {
            return Ok(None);
        }
        let raw = String::from_utf8_lossy(&result.stdout);
        Ok(raw.trim().parse::<u32>().ok())
    }

    async fn kill(&mut self) -> Result<()> {
        self.stop_flag.store(true, Ordering::Relaxed);
        if self.attached_pipe {
            let _ = self.run_tmux(["pipe-pane", "-t", self.pane_target.as_str()]);
            self.attached_pipe = false;
        }
        let _ = std::fs::remove_file(&self.fifo_path);
        Ok(())
    }

    async fn destroy_session(&mut self) -> Result<()> {
        self.kill().await?;
        if self.owns_session {
            let _ = self.run_tmux(["kill-session", "-t", self.session_name.as_str()]);
        }
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        let output = self.run_tmux([
            "display-message",
            "-p",
            "-t",
            self.pane_target.as_str(),
            "#{cursor_x} #{cursor_y}",
        ])?;
        Ok(parse_tmux_cursor_position(&output))
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

/// Parse tmux `display-message` output for cursor coordinates.
/// Expects format like `"12 3\n"` → `Some((12, 3))`.
/// Returns `None` for invalid/unparseable output.
pub fn parse_tmux_cursor_position(output: &str) -> Option<(u16, u16)> {
    let trimmed = output.trim();
    let mut parts = trimmed.split_whitespace();
    let x: u16 = parts.next()?.parse().ok()?;
    let y: u16 = parts.next()?.parse().ok()?;
    Some((x, y))
}

#[allow(dead_code)]
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

    fn has_session(name: &str) -> bool {
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

    fn run_zellij_action(session: &str, args: &[&str]) -> Result<String> {
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

    fn send_zellij_action(session: &str, args: &[&str]) -> Result<()> {
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

    fn find_cli_pid(_session_name: &str) -> Option<u32> {
        let out = std::process::Command::new("ps")
            .args(["-eo", "pid=,comm="])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1] != "zellij" {
                if let Ok(pid) = parts[0].parse::<u32>() {
                    return Some(pid);
                }
            }
        }
        None
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

    /// Ensure pane_id is discovered and the subscribe background task is started.
    /// Safe to call multiple times (idempotent via `subscribe_started`).
    fn ensure_zellij_subscribe_started(&mut self) {
        // Discover pane_id if not already known
        if self.pane_id.is_none() {
            self.pane_id = Self::discover_pane_id(&self.session_name);
            if self.pane_id.is_none() {
                warn!(
                    "zellij session {}: failed to discover pane_id, falling back to terminal_0",
                    self.session_name
                );
            }
        }
        // Start subscribe if not already started
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

        // Reattach path: reuse existing session
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

        // Discover and start subscribe for the newly created session
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
        Ok(Self::find_cli_pid(&self.session_name))
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
        // Start the zellij subscribe background task for real-time viewport updates
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

/// Parse a single JSON line from `zellij subscribe --ansi --format json` output.
/// Returns the viewport lines if the event is a `pane_update`.
/// Handles both top-level `viewport` and `data.viewport` formats.
fn parse_zellij_subscribe_viewport(line: &str) -> Option<Vec<String>> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let event = v.get("event")?.as_str()?;
    match event {
        "pane_update" => {
            // Try top-level "viewport" first (official docs format),
            // then "data.viewport" (alternative format).
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

/// Check if a `zellij subscribe` JSON line is a `pane_closed` event.
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

/// Convert a viewport (array of rendered lines) into an ANSI full-screen repaint chunk.
/// The output clears the screen, writes all lines, and positions the cursor at the end.
/// Does NOT append a trailing CRLF to avoid unwanted scroll.
pub fn viewport_to_ansi_chunk(viewport: &[String]) -> String {
    if viewport.is_empty() {
        return String::new();
    }
    let mut out =
        String::with_capacity(viewport.iter().map(|l| l.len() + 2).sum::<usize>() + 16);
    // Hide cursor
    out.push_str("\x1b[?25l");
    // Home cursor
    out.push_str("\x1b[H");
    // Clear entire screen
    out.push_str("\x1b[2J");
    // Write each line
    for (i, line) in viewport.iter().enumerate() {
        if i > 0 {
            out.push_str("\r\n");
        }
        out.push_str(line);
    }
    // Show cursor
    out.push_str("\x1b[?25h");
    out
}

/// Extract the numeric pane id from a `terminal_N` string or bare number.
/// Accepts `"terminal_42"` and bare `"42"`. Returns `None` otherwise.
pub fn numeric_pane_id(pane_id: &str) -> Option<u64> {
    // Try bare number first, then "terminal_N" format
    if let Ok(n) = pane_id.parse::<u64>() {
        return Some(n);
    }
    pane_id.strip_prefix("terminal_")?.parse().ok()
}

/// Parse `zellij action list-panes --json --all` output to find the cursor coordinates
/// for a given pane (by numeric id). Returns `None` if not found or unparseable.
/// Handles both array format `[x, y]` and object format `{"x": ..., "y": ...}`.
pub fn parse_zellij_cursor_from_list_panes(json: &str, numeric_id: u64) -> Option<(u16, u16)> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let panes = v.as_array()?;
    for pane in panes {
        let id = pane.get("id")?.as_u64()?;
        if id != numeric_id {
            continue;
        }
        let cursor = pane.get("cursor_coordinates_in_pane")?;
        // Try array format [x, y] first (official docs format), then object {x, y}
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

/// Background task that runs `zellij subscribe` and feeds viewport updates
/// as ANSI chunks into the provided `data_tx` broadcast channel.
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

#[async_trait]
impl SessionBackend for PtyBackend {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        let mut cmd = TokioCommand::new(bin);
        cmd.args(args)
            .current_dir(&opts.cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (k, v) in &opts.env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .context("failed to spawn fallback pty backend")?;
        let stdin = child.stdin.take().context("child stdin unavailable")?;
        let mut stdout = child.stdout.take().context("child stdout unavailable")?;
        let mut stderr = child.stderr.take().context("child stderr unavailable")?;
        let tx = self.data_tx.clone();
        let recent = self.recent_output.clone();
        tokio::spawn(async move {
            let mut buf = vec![0_u8; 4096];
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        PtyBackend::append_output(recent.clone(), tx.clone(), buf[..n].to_vec())
                            .await
                    }
                    Err(_) => break,
                }
            }
        });
        let tx = self.data_tx.clone();
        let recent = self.recent_output.clone();
        tokio::spawn(async move {
            let mut buf = vec![0_u8; 4096];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        PtyBackend::append_output(recent.clone(), tx.clone(), buf[..n].to_vec())
                            .await
                    }
                    Err(_) => break,
                }
            }
        });
        self.stdin = Some(Arc::new(tokio::sync::Mutex::new(stdin)));
        self.child = Some(Arc::new(tokio::sync::Mutex::new(child)));
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        let stdin = self.stdin.as_ref().context("stdin unavailable")?;
        let mut stdin = stdin.lock().await;
        stdin.write_all(text.as_bytes()).await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn send_enter(&self) -> Result<()> {
        let stdin = self.stdin.as_ref().context("stdin unavailable")?;
        let mut stdin = stdin.lock().await;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
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
                "Escape" => self.write_raw("\u{1b}").await?,
                "C-c" => self.write_raw("\u{3}").await?,
                other if other.chars().count() == 1 => self.write_raw(other).await?,
                other => bail!("unsupported special key for pty backend: {}", other),
            }
        }
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        self.write_raw(&format!("\u{1b}[200~{}\u{1b}[201~", text))
            .await
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        let stdin = self.stdin.as_ref().context("stdin unavailable")?;
        let mut stdin = stdin.lock().await;
        stdin.write_all(text.as_bytes()).await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.send_text(text).await?;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        Ok(self
            .recent_output
            .read()
            .await
            .clone()
            .replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.capture_viewport().await
    }

    async fn is_alive(&self) -> Result<bool> {
        let child = self.child.as_ref().context("child unavailable")?;
        let mut child = child.lock().await;
        Ok(child.try_wait()?.is_none())
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        let child = self.child.as_ref().context("child unavailable")?;
        let child = child.lock().await;
        Ok(child.id())
    }

    async fn kill(&mut self) -> Result<()> {
        if let Some(child) = &self.child {
            let mut child = child.lock().await;
            let _ = child.kill().await;
        }
        Ok(())
    }

    async fn destroy_session(&mut self) -> Result<()> {
        self.kill().await
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        Ok(None)
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_tmux_cursor_position, numeric_pane_id,
        parse_zellij_cursor_from_list_panes, parse_zellij_subscribe_viewport,
        viewport_to_ansi_chunk, is_zellij_pane_closed,
    };

    // ---- tmux cursor_position tests ----

    #[test]
    fn parse_valid_cursor_position() {
        assert_eq!(parse_tmux_cursor_position("12 3\n"), Some((12, 3)));
        assert_eq!(parse_tmux_cursor_position("0 0\n"), Some((0, 0)));
        assert_eq!(parse_tmux_cursor_position(" 5 10 \n"), Some((5, 10)));
    }

    #[test]
    fn parse_cursor_position_empty_or_invalid() {
        assert_eq!(parse_tmux_cursor_position(""), None);
        assert_eq!(parse_tmux_cursor_position("\n"), None);
        assert_eq!(parse_tmux_cursor_position("abc"), None);
        assert_eq!(parse_tmux_cursor_position("12"), None);
        assert_eq!(parse_tmux_cursor_position("12 abc"), None);
    }

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
        // Official docs format: viewport at top level
        let line = r#"{"event":"pane_update","pane_id":"terminal_1","viewport":["a","b"],"scrollback":null,"is_initial":true}"#;
        let viewport = parse_zellij_subscribe_viewport(line);
        assert_eq!(
            viewport,
            Some(vec!["a".to_string(), "b".to_string(),])
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
        // Must not end with \r\n
        assert!(!chunk.ends_with("\r\n"));
        assert!(!chunk.ends_with('\n'));
        // Must contain the lines joined with \r\n
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
        // Official docs format: cursor as [x, y] array
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
        // Plugin panes may not have cursor
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
        // The JSON id can be a number; our match uses as_u64()
        let json = r#"[
            {"id":42,"cursor_coordinates_in_pane":{"x":5,"y":3}}
        ]"#;
        assert_eq!(
            parse_zellij_cursor_from_list_panes(json, 42),
            Some((5, 3))
        );
    }
}
