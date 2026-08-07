use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

pub(crate) const RAW_INPUT_ENTER_DELAY: Duration = Duration::from_millis(200);
pub(crate) const ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL: Duration = Duration::from_millis(200);
pub(crate) const ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS: usize = 15;
/// Upper bound for `zellij attach --create-background` during spawn. When the
/// zellij server panics during session setup the client retries the socket
/// forever; without a timeout the worker would block here indefinitely.
pub(crate) const ZELLIJ_SPAWN_TIMEOUT: Duration = Duration::from_secs(30);
/// Spawn attempts per session. zellij 0.44.3 intermittently reports the pane
/// as created while the pty is never registered ("failed to find terminal fd
/// for id 0"), leaving an empty pane with no process; a fresh retry usually
/// lands on the healthy path.
pub(crate) const ZELLIJ_SPAWN_MAX_ATTEMPTS: usize = 2;
pub(crate) const ZELLIJ_SPAWN_RETRY_BACKOFF: Duration = Duration::from_millis(500);
pub(crate) const ZELLIJ_PANE_PROCESS_CHECK_INTERVAL: Duration = Duration::from_millis(300);
pub(crate) const ZELLIJ_PANE_PROCESS_CHECK_ATTEMPTS: usize = 3;
/// Upper bound for a single `zellij action` / probe command (write-chars,
/// paste, dump-screen, list-sessions, ...). Previously these were synchronous
/// `std::process::Command::output()` calls with no timeout: any zellij server
/// hiccup could block a tokio thread forever and cascade into a fully stuck
/// worker. Now every external call is bounded and errors out instead.
pub(crate) const ZELLIJ_ACTION_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone)]
pub struct SpawnOpts {
    pub cwd: String,
    #[allow(dead_code)]
    /// Desired terminal columns (passed as layout intent; actual pane size is
    /// managed by the terminal proxy anchor).
    pub cols: u16,
    #[allow(dead_code)]
    /// Desired terminal rows (passed as layout intent; actual pane size is
    /// managed by the terminal proxy anchor).
    pub rows: u16,
    pub env: Vec<(String, String)>,
}

/// Backend trait. All methods take `&self`: implementations synchronize
/// internally at per-operation granularity, so callers can share one
/// `Arc<dyn SessionBackend>` across tasks without an outer Mutex. This is
/// what keeps a long `write_input()` (paste + confirm loop) from blocking
/// screen capture, terminal keys, and the screenshot coordinator.
#[allow(dead_code)]
#[async_trait]
pub trait SessionBackend: Send + Sync {
    async fn spawn(&self, bin: &str, args: &[String], opts: SpawnOpts) -> Result<()>;
    async fn send_text(&self, text: &str) -> Result<()>;
    async fn send_enter(&self) -> Result<()>;
    async fn send_special_keys(&self, keys: &[String]) -> Result<()>;
    async fn paste_text(&self, text: &str) -> Result<()>;
    async fn write_raw(&self, text: &str) -> Result<()>;
    async fn raw_input(&self, text: &str) -> Result<()>;
    /// Capture the visible viewport only (current pane dimensions).
    async fn capture_viewport(&self) -> Result<String>;
    /// Capture the last visible screen (alias for capture_viewport by default).
    async fn capture_current_screen(&self) -> Result<String>;
    async fn is_alive(&self) -> Result<bool>;
    async fn child_pid(&self) -> Result<Option<u32>>;
    async fn kill(&self) -> Result<()>;
    async fn destroy_session(&self) -> Result<()>;
    /// Return the real cursor position as 0-based (x, y) if available.
    async fn cursor_position(&self) -> Result<Option<(u16, u16)>>;
    fn subscribe(&self) -> broadcast::Receiver<String>;
}

mod observe;
mod subscribe;
mod zellij;

pub use observe::ZellijObserveBackend;
pub use zellij::ZellijBackend;

#[cfg(test)]
#[path = "backend/tests.rs"]
mod tests;
