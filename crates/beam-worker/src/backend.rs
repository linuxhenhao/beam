use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

pub(crate) const RAW_INPUT_ENTER_DELAY: Duration = Duration::from_millis(200);
pub(crate) const ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL: Duration = Duration::from_millis(200);
pub(crate) const ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS: usize = 15;

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
    /// Capture the visible viewport only (current pane dimensions).
    async fn capture_viewport(&self) -> Result<String>;
    /// Capture the last visible screen (alias for capture_viewport by default).
    async fn capture_current_screen(&self) -> Result<String>;
    async fn is_alive(&self) -> Result<bool>;
    async fn child_pid(&self) -> Result<Option<u32>>;
    async fn kill(&mut self) -> Result<()>;
    async fn destroy_session(&mut self) -> Result<()>;
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
