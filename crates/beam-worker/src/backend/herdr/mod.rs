//! Herdr first-class `SessionBackend`.
//!
//! Managed sessions occupy one labeled workspace (`beam-{sid8}`) on the
//! shared Herdr default session; the root pane runs the existing launch spec
//! (`env` / `systemd-run` + adapter argv). Adopted sessions observe and drive
//! a user-owned pane without ever `pane run`-ing a second CLI.
//!
//! Control plane is CLI-first (`herdr …`, JSON stdout); the raw socket is
//! only used for the long-lived observe stream. Input goes through
//! `pane send-text` / `pane send-keys` — never `terminal session control`
//! (that would steal the human TUI's input/resize) and never `agent.prompt`
//! as the v1 primary path.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{info, warn};

use beam_core::DEFAULT_TERMINAL_COLS;
use beam_core::DEFAULT_TERMINAL_ROWS;

use super::{RAW_INPUT_ENTER_DELAY, SessionBackend, SpawnOpts};

pub(crate) mod cli;
pub(crate) mod ids;
pub(crate) mod observe;

use cli::{
    HERDR_SHELL_READY_TIMEOUT, pane_list, pane_process_info, pane_read_visible, pane_run,
    pane_send_keys, pane_send_text, pane_wait_output, start_server, status_server, workspace_close,
    workspace_create, workspace_get, workspace_get_ids, workspace_list,
};
use ids::{HerdrIds, command_string, workspace_by_label};

/// Default shell-prompt regex shared by bash/zsh/sh/fish (tail prompt).
pub(crate) const HERDR_SHELL_PROMPT_REGEX: &str = r"[\$#%] ?$";

/// A managed or observed Herdr terminal identity. `ids` is populated by
/// `spawn()` and read by `run_loop` for `Ready`; it lives behind a mutex so
/// the backend can be shared as `Arc<dyn SessionBackend>`.
#[derive(Debug)]
pub struct HerdrBackend {
    session_label: String,
    cwd: String,
    observe_cols: u16,
    observe_rows: u16,
    ids: StdMutex<Option<HerdrIds>>,
    data_tx: broadcast::Sender<String>,
    observe_started: AtomicBool,
    observe_stop: Arc<AtomicBool>,
}

impl HerdrBackend {
    pub fn new(session_label: String, cwd: String) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            session_label,
            cwd,
            observe_cols: DEFAULT_TERMINAL_COLS,
            observe_rows: DEFAULT_TERMINAL_ROWS,
            ids: StdMutex::new(None),
            data_tx,
            observe_started: AtomicBool::new(false),
            observe_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Current herdr ids after a successful `spawn()`.
    pub fn herdr_ids(&self) -> Option<HerdrIds> {
        self.ids.lock().unwrap().clone()
    }

    fn set_ids(&self, ids: HerdrIds) {
        *self.ids.lock().unwrap() = Some(ids);
    }

    fn start_observe(&self) {
        if self.observe_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (workspace_id, pane_id) = match self.herdr_ids().map(|i| i.workspace_pane()) {
            Some(ids) => ids,
            None => return,
        };
        let tx = self.data_tx.clone();
        let stop = self.observe_stop.clone();
        let cols = self.observe_cols;
        let rows = self.observe_rows;
        info!(workspace_id, pane_id, "starting herdr observe");
        tokio::spawn(async move {
            observe::run_herdr_observe(pane_id, cols, rows, tx, stop).await;
        });
    }

    /// Ensure the shared herdr server is reachable, starting it headless if
    /// needed. Fail-closed: a missing/unreachable server is a hard error for
    /// managed spawn.
    async fn ensure_server(&self) -> Result<()> {
        if status_server().await.unwrap_or(false) {
            return Ok(());
        }
        warn!("herdr server not running; starting headless server");
        start_server().await?;
        Ok(())
    }

    /// Label-deduped managed spawn: reuse an existing labeled workspace when
    /// possible, otherwise create one and `pane run` the launch spec.
    async fn managed_spawn(&self, bin: &str, args: &[String]) -> Result<()> {
        self.ensure_server().await?;
        let label = self.session_label.clone();
        let entries = workspace_list().await?;
        if let Some(existing) = workspace_by_label(&entries, &label) {
            let existing_id = existing.workspace_id.clone();
            match workspace_get(&existing_id).await {
                Ok(Some(payload)) => {
                    let ids = match workspace_get_ids(&payload).ok().flatten() {
                        Some(ids) => ids,
                        None => {
                            // `workspace get` does not embed the root pane id on
                            // real herdr; recover it from `pane list`.
                            match self.pane_id_for_workspace(&existing_id).await {
                                Ok(Some(pane_id)) => HerdrIds {
                                    workspace_id: existing_id.clone(),
                                    pane_id,
                                },
                                Ok(None) => bail!(
                                    "herdr workspace {existing_id} (label {label}) exists but no pane was found; run /restart to recreate"
                                ),
                                Err(err) => {
                                    return Err(err.context(format!(
                                    "herdr workspace {existing_id} (label {label}) pane lookup failed"
                                )));
                                }
                            }
                        }
                    };
                    self.set_ids(ids.clone());
                    let info = pane_process_info(&ids.pane_id).await;
                    let foreground_alive = match info {
                        Ok(info) => {
                            !(info.argv.as_deref().map(str::trim).unwrap_or("").is_empty()
                                && info.pid.is_none())
                        }
                        Err(_) => true, // probe failure → assume alive
                    };
                    if foreground_alive {
                        info!(workspace_id = %existing_id, label, "reattached to existing herdr workspace");
                        self.start_observe();
                        return Ok(());
                    }
                    // Dead CLI: the workspace shell is still there. `pane run`
                    // the resume launch spec in the same pane so the next
                    // inbound message lands on a fresh CLI.
                    warn!(
                        workspace_id = %existing_id,
                        pane = %ids.pane_id,
                        "herdr foreground CLI is dead; re-running launch spec in existing pane"
                    );
                    self.run_launch_spec_in_pane(bin, args).await?;
                    self.start_observe();
                    return Ok(());
                }
                Ok(None) => {
                    // The label was listed but the workspace is already gone
                    // (e.g. closed between list and get). Create fresh.
                    warn!(
                        workspace_id = %existing_id,
                        label,
                        "herdr workspace disappeared between list and get; creating new workspace"
                    );
                }
                Err(err) => {
                    // Unknown is not absent: probe failure must not destroy a
                    // healthy workspace, but a hard error here is acceptable
                    // because the worker will surface it via Ready-timeout.
                    return Err(err.context(format!(
                        "herdr workspace {existing_id} (label {label}) lookup failed"
                    )));
                }
            }
        }

        let ids = workspace_create(&self.cwd, &label).await?;
        self.set_ids(ids.clone());
        self.run_launch_spec_in_pane(bin, args).await?;
        self.start_observe();
        Ok(())
    }

    /// Find the root pane id for a workspace via `herdr pane list`.
    /// `workspace get` does not embed pane ids on herdr 0.8.x, so the reuse
    /// path recovers the pane this way.
    async fn pane_id_for_workspace(&self, workspace_id: &str) -> Result<Option<String>> {
        let panes = pane_list().await?;
        Ok(panes
            .iter()
            .find(|p| p.workspace_id == workspace_id)
            .map(|p| p.pane_id.clone()))
    }

    /// Wait for the shell prompt (best-effort) then `pane run` the launch
    /// spec in the current pane. A `wait-output` timeout still proceeds; the
    /// spawn retry absorbs the residual race.
    async fn run_launch_spec_in_pane(&self, bin: &str, args: &[String]) -> Result<()> {
        let pane_id = self.herdr_ids().context("herdr ids not set")?.pane_id;
        // The root pane is a shell; wait for a prompt before `pane run`.
        // wait-output only lowers the race probability; a timeout still
        // proceeds (spawn retry absorbs the residual race).
        if let Ok(ready) = pane_wait_output(&pane_id, HERDR_SHELL_PROMPT_REGEX).await {
            if ready {
                info!(pane = %pane_id, "herdr shell ready before pane run");
            } else {
                warn!(
                    pane = %pane_id,
                    timeout_s = HERDR_SHELL_READY_TIMEOUT.as_secs(),
                    "herdr shell prompt not matched; proceeding to pane run anyway"
                );
            }
        }
        let command = command_string(bin, args);
        pane_run(&pane_id, &command).await?;
        info!(pane = %pane_id, "herdr pane run issued");
        Ok(())
    }

    async fn read_visible(&self) -> Result<String> {
        let pane_id = self
            .herdr_ids()
            .context("herdr ids not set; cannot read pane")?
            .pane_id;
        Ok(pane_read_visible(&pane_id).await?.replace('\n', "\r\n"))
    }
}

#[async_trait]
impl SessionBackend for HerdrBackend {
    async fn spawn(&self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()> {
        let _ = opts;
        self.managed_spawn(bin, args).await
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        let pane_id = self.herdr_ids().context("herdr ids not set")?.pane_id;
        pane_send_text(&pane_id, text).await
    }

    async fn send_enter(&self) -> Result<()> {
        let pane_id = self.herdr_ids().context("herdr ids not set")?.pane_id;
        pane_send_keys(&pane_id, &["enter"]).await
    }

    async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
        let pane_id = self.herdr_ids().context("herdr ids not set")?.pane_id;
        for key in keys {
            match key.as_str() {
                "Enter" => pane_send_keys(&pane_id, &["enter"]).await?,
                "Down" => pane_send_keys(&pane_id, &["down"]).await?,
                "Up" => pane_send_keys(&pane_id, &["up"]).await?,
                "Left" => pane_send_keys(&pane_id, &["left"]).await?,
                "Right" => pane_send_keys(&pane_id, &["right"]).await?,
                "PageUp" => self.write_raw("\u{1b}[5~").await?,
                "PageDown" => self.write_raw("\u{1b}[6~").await?,
                "M-Enter" => self.write_raw("\u{1b}\r").await?,
                "Tab" => pane_send_keys(&pane_id, &["tab"]).await?,
                "Space" => pane_send_keys(&pane_id, &["space"]).await?,
                "Escape" | "Esc" => pane_send_keys(&pane_id, &["esc"]).await?,
                "C-c" => pane_send_keys(&pane_id, &["ctrl+c"]).await?,
                other if other.chars().count() == 1 => self.write_raw(other).await?,
                other => bail!("unsupported special key for herdr backend: {}", other),
            }
        }
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        self.send_text(text).await
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        self.send_text(text).await
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.paste_text(text).await?;
        tokio::time::sleep(RAW_INPUT_ENTER_DELAY).await;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        self.read_visible().await
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.read_visible().await
    }

    async fn is_alive(&self) -> Result<bool> {
        let Some(ids) = self.herdr_ids() else {
            // No ids yet: probe failure / unknown is alive.
            return Ok(true);
        };
        // Workspace confirmed missing → dead.
        match workspace_get(&ids.workspace_id).await {
            Ok(None) => return Ok(false),
            Err(_) => return Ok(true), // unknown → alive
            Ok(Some(_)) => {}
        }
        let info = match pane_process_info(&ids.pane_id).await {
            Ok(info) => info,
            Err(_) => return Ok(true), // probe failure → alive
        };
        let foreground_empty =
            info.argv.as_deref().map(str::trim).unwrap_or("").is_empty() && info.pid.is_none();
        Ok(!foreground_empty)
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        let Some(ids) = self.herdr_ids() else {
            return Ok(None);
        };
        Ok(pane_process_info(&ids.pane_id)
            .await
            .ok()
            .and_then(|info| info.pid)
            .and_then(|pid| u32::try_from(pid).ok()))
    }

    async fn kill(&self) -> Result<()> {
        // Detach only: stop observe; the workspace and CLI stay running.
        self.observe_stop.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn destroy_session(&self) -> Result<()> {
        // Only `/close` (and mux-tearing `/restart`) reach here for managed
        // sessions. Force-close the labeled workspace.
        if let Some(ids) = self.herdr_ids() {
            workspace_close(&ids.workspace_id).await?;
        }
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        Ok(None)
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

/// Adopt backend: observe + drive a user-owned Herdr pane. `spawn()` only
/// starts observe; `destroy_session()` is a no-op (Beam never tears down a
/// user's workspace), and it never `pane run`s a second CLI.
#[derive(Debug)]
pub struct HerdrObserveBackend {
    workspace_id: String,
    pane_id: String,
    child_pid: Option<u32>,
    data_tx: broadcast::Sender<String>,
    observe_started: AtomicBool,
    observe_stop: Arc<AtomicBool>,
}

impl HerdrObserveBackend {
    pub fn new(workspace_id: String, pane_id: String, child_pid: Option<u32>) -> Self {
        let (data_tx, _) = broadcast::channel(512);
        Self {
            workspace_id,
            pane_id,
            child_pid,
            data_tx,
            observe_started: AtomicBool::new(false),
            observe_stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start_observe(&self) {
        if self.observe_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let pane_id = self.pane_id.clone();
        let tx = self.data_tx.clone();
        let stop = self.observe_stop.clone();
        tokio::spawn(async move {
            observe::run_herdr_observe(
                pane_id,
                DEFAULT_TERMINAL_COLS,
                DEFAULT_TERMINAL_ROWS,
                tx,
                stop,
            )
            .await;
        });
    }
}

#[async_trait]
impl SessionBackend for HerdrObserveBackend {
    async fn spawn(&self, _bin: &str, _args: &[String], _opts: SpawnOpts) -> Result<()> {
        self.start_observe();
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        pane_send_text(&self.pane_id, text).await
    }

    async fn send_enter(&self) -> Result<()> {
        pane_send_keys(&self.pane_id, &["enter"]).await
    }

    async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
        for key in keys {
            match key.as_str() {
                "Enter" => pane_send_keys(&self.pane_id, &["enter"]).await?,
                "Down" => pane_send_keys(&self.pane_id, &["down"]).await?,
                "Up" => pane_send_keys(&self.pane_id, &["up"]).await?,
                "Left" => pane_send_keys(&self.pane_id, &["left"]).await?,
                "Right" => pane_send_keys(&self.pane_id, &["right"]).await?,
                "PageUp" => self.write_raw("\u{1b}[5~").await?,
                "PageDown" => self.write_raw("\u{1b}[6~").await?,
                "M-Enter" => self.write_raw("\u{1b}\r").await?,
                "Tab" => pane_send_keys(&self.pane_id, &["tab"]).await?,
                "Space" => pane_send_keys(&self.pane_id, &["space"]).await?,
                "Escape" | "Esc" => pane_send_keys(&self.pane_id, &["esc"]).await?,
                "C-c" => pane_send_keys(&self.pane_id, &["ctrl+c"]).await?,
                other if other.chars().count() == 1 => self.write_raw(other).await?,
                other => bail!("unsupported special key for herdr backend: {}", other),
            }
        }
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        self.send_text(text).await
    }

    async fn write_raw(&self, text: &str) -> Result<()> {
        self.send_text(text).await
    }

    async fn raw_input(&self, text: &str) -> Result<()> {
        self.paste_text(text).await?;
        tokio::time::sleep(RAW_INPUT_ENTER_DELAY).await;
        self.send_enter().await
    }

    async fn capture_viewport(&self) -> Result<String> {
        Ok(pane_read_visible(&self.pane_id)
            .await?
            .replace('\n', "\r\n"))
    }

    async fn capture_current_screen(&self) -> Result<String> {
        self.capture_viewport().await
    }

    async fn is_alive(&self) -> Result<bool> {
        // Adopt: unknown → alive; a confirmed-dead pane still keeps the
        // session Active (next round observes only, never re-runs).
        match workspace_get(&self.workspace_id).await {
            Ok(None) => Ok(false),
            Err(_) => Ok(true),
            Ok(Some(_)) => {
                let info = match pane_process_info(&self.pane_id).await {
                    Ok(info) => info,
                    Err(_) => return Ok(true),
                };
                let foreground_empty = info.argv.as_deref().map(str::trim).unwrap_or("").is_empty()
                    && info.pid.is_none();
                Ok(!foreground_empty)
            }
        }
    }

    async fn child_pid(&self) -> Result<Option<u32>> {
        Ok(self.child_pid)
    }

    async fn kill(&self) -> Result<()> {
        self.observe_stop.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn destroy_session(&self) -> Result<()> {
        // Never tear down a user-owned Herdr workspace/pane.
        Ok(())
    }

    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        Ok(None)
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.data_tx.subscribe()
    }
}

#[cfg(test)]
mod hermetic_tests {
    use super::*;
    use std::sync::OnceLock;

    /// Serializes tests that mutate PATH so the fake herdr shim is found
    /// first without racing other tests in the same process.
    fn path_lock() -> &'static std::sync::Mutex<()> {
        static PATH_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
        PATH_TEST_LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// Point PATH at the fake herdr shim and return the state dir.
    fn fake_herdr_env(_guard: &std::sync::MutexGuard<'_, ()>) -> std::path::PathBuf {
        let shim_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support");
        let state = std::env::temp_dir().join(format!("fake-herdr-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&state);
        std::fs::create_dir_all(&state).expect("state dir");
        // Command::new("herdr") resolves by binary name; expose the shim
        // under that name in a PATH-prepended bin dir.
        let bin_dir = state.join("bin");
        std::fs::create_dir_all(&bin_dir).expect("bin dir");
        std::os::unix::fs::symlink(shim_dir.join("fake_herdr.sh"), bin_dir.join("herdr"))
            .expect("herdr symlink");
        let old_path = std::env::var_os("PATH").unwrap_or_default();
        unsafe {
            std::env::set_var(
                "PATH",
                format!("{}:{}", bin_dir.display(), old_path.to_string_lossy()),
            );
            std::env::set_var("FAKE_HERDR_STATE", &state);
        }
        state
    }

    fn restore_env(old_path: std::ffi::OsString, old_state: std::ffi::OsString) {
        unsafe {
            std::env::set_var("PATH", old_path);
            std::env::set_var("FAKE_HERDR_STATE", old_state);
        }
    }

    // Runs as one serialized test: every case mutates process PATH to point
    // at the fake shim, which is unsafe to do concurrently with other tests.
    #[test]
    fn herdr_backend_managed_lifecycle_against_fake_shim() {
        let guard = path_lock().lock().unwrap();
        let old_path = std::env::var_os("PATH").unwrap_or_default();
        let old_state = std::env::var_os("FAKE_HERDR_STATE").unwrap_or_default();
        let state = fake_herdr_env(&guard);
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let opts = SpawnOpts {
                cwd: "/repo".to_string(),
                cols: 160,
                rows: 50,
                env: Vec::new(),
            };

            // Managed spawn: creates a labeled workspace and runs the quoted
            // launch spec via `pane run`.
            let backend = HerdrBackend::new("beam-deadbeef".to_string(), "/repo".to_string());
            backend
                .spawn(
                    "/usr/bin/env",
                    &["BEAM_SESSION_ID=s1".to_string(), "claude".to_string()],
                    opts,
                )
                .await
                .expect("managed spawn");
            let ids = backend.herdr_ids().expect("ids after spawn");
            assert_eq!(ids.workspace_id, "w1");
            assert_eq!(ids.pane_id, "w1:p1");
            let run_log = std::fs::read_to_string(state.join("input/run.log")).expect("run log");
            assert!(run_log.contains("/usr/bin/env"));
            assert!(run_log.contains("BEAM_SESSION_ID=s1"));
            assert!(run_log.contains("claude"));

            // is_alive: foreground pid present => alive.
            assert!(backend.is_alive().await.expect("alive with foreground pid"));

            // Input goes to the pane via send-text / send-keys.
            backend.send_text("hi").await.expect("send text");
            backend.send_enter().await.expect("send enter");
            let text_log =
                std::fs::read_to_string(state.join("input/send_text.log")).expect("text log");
            assert_eq!(text_log.trim(), "hi");

            // Empty foreground => dead.
            std::fs::write(state.join("empty_foreground"), "").expect("toggle empty");
            assert!(!backend.is_alive().await.expect("dead without foreground"));
            std::fs::remove_file(state.join("empty_foreground")).expect("untoggle");

            // destroy_session force-closes the workspace.
            let wid = backend.herdr_ids().unwrap().workspace_id;
            assert!(state.join("workspaces").join(&wid).exists());
            backend.destroy_session().await.expect("destroy");
            assert!(!state.join("workspaces").join(&wid).exists());

            // Spawn again reuses the same labeled workspace (idempotent; the
            // fake shim returns the existing id for the same label).
            let backend2 = HerdrBackend::new("beam-deadbeef".to_string(), "/repo".to_string());
            backend2
                .spawn(
                    "/usr/bin/env",
                    &["claude".to_string()],
                    SpawnOpts {
                        cwd: "/repo".to_string(),
                        cols: 160,
                        rows: 50,
                        env: Vec::new(),
                    },
                )
                .await
                .expect("re-spawn after destroy");
            assert_eq!(backend2.herdr_ids().unwrap().workspace_id, "w1");

            // Dead-CLI resume: spawn a fresh backend with the same label, then
            // mark the foreground empty and re-spawn; the second spawn must
            // `pane run` the launch spec again in the same pane (not create a
            // second workspace and not just observe).
            let run_log_before =
                std::fs::read_to_string(state.join("input/run.log")).expect("run log before");
            std::fs::write(state.join("empty_foreground"), "").expect("toggle empty");
            let backend3 = HerdrBackend::new("beam-deadbeef".to_string(), "/repo".to_string());
            backend3
                .spawn(
                    "/usr/bin/env",
                    &["claude".to_string()],
                    SpawnOpts {
                        cwd: "/repo".to_string(),
                        cols: 160,
                        rows: 50,
                        env: Vec::new(),
                    },
                )
                .await
                .expect("dead-cli resume spawn");
            assert_eq!(backend3.herdr_ids().unwrap().workspace_id, "w1");
            let run_log_after =
                std::fs::read_to_string(state.join("input/run.log")).expect("run log after");
            assert!(
                run_log_after.lines().count() > run_log_before.lines().count(),
                "dead CLI must re-run the launch spec"
            );
        });
        restore_env(old_path, old_state);
    }
}
