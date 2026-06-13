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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
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
}

#[async_trait]
impl SessionBackend for ZellijBackend {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        self.reattach = self.reattach || Self::has_session(&self.session_name);
        if self.reattach && Self::has_session(&self.session_name) {
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
                return Ok(());
            }
            bail!("zellij backend failed: {}", stderr.trim());
        }

        // Discover the actual pane_id from the running session
        self.pane_id = Self::discover_pane_id(&self.session_name);
        if self.pane_id.is_none() {
            warn!(
                "zellij session {}: failed to discover pane_id, falling back to terminal_0",
                self.session_name
            );
        }
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

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

pub struct ZellijObserveBackend {
    session_name: String,
    pane_id: String,
    child_pid: Option<u32>,
    data_tx: broadcast::Sender<String>,
}

impl ZellijObserveBackend {
    pub fn new(session_name: String, pane_id: String, child_pid: Option<u32>) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_name,
            pane_id,
            child_pid,
            data_tx,
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
        Ok(())
    }

    async fn destroy_session(&mut self) -> Result<()> {
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
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

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}
