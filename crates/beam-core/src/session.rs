use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::BackendType;
use crate::ipc::{CliUsageLimitState, DisplayMode, ScreenStatus};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionScope {
    Thread,
    Chat,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatMode {
    Group,
    Topic,
    P2p,
}

impl From<&str> for ChatMode {
    fn from(value: &str) -> Self {
        match value {
            "p2p" | "P2P" => ChatMode::P2p,
            "topic" | "TOPIC" => ChatMode::Topic,
            _ => ChatMode::Group,
        }
    }
}

impl Default for SessionScope {
    fn default() -> Self {
        Self::Thread
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Closed,
}

impl Default for SessionStatus {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PendingResponseCardState {
    Open,
    Patched,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AdoptedFrom {
    #[serde(default)]
    pub tmux_target: Option<String>,
    #[serde(default)]
    pub zellij_session: Option<String>,
    #[serde(default)]
    pub zellij_pane_id: Option<String>,
    pub original_cli_pid: i32,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub cli_id: Option<String>,
    pub cwd: String,
    #[serde(default)]
    pub pane_cols: Option<u16>,
    #[serde(default)]
    pub pane_rows: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Session {
    pub session_id: String,
    pub title: String,
    pub chat_id: String,
    pub root_message_id: String,
    #[serde(default)]
    pub chat_type: Option<String>,
    #[serde(default)]
    pub quote_target_id: Option<String>,
    #[serde(default)]
    pub scope: SessionScope,
    #[serde(default)]
    pub status: SessionStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub closed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub web_port: Option<u16>,
    #[serde(default)]
    pub worker_token: Option<String>,
    pub lark_app_id: String,
    #[serde(default)]
    pub owner_open_id: Option<String>,
    #[serde(default)]
    pub worker_pid: Option<u32>,
    #[serde(default)]
    pub cli_id: Option<String>,
    #[serde(default)]
    pub cli_bin: Option<String>,
    #[serde(default)]
    pub cli_args: Vec<String>,
    #[serde(default)]
    pub backend_type: BackendType,
    #[serde(default)]
    pub cli_session_id: Option<String>,
    #[serde(default)]
    pub last_cli_input: Option<String>,
    #[serde(default)]
    pub stream_card_id: Option<String>,
    #[serde(default)]
    pub stream_card_nonce: Option<String>,
    #[serde(default)]
    pub display_mode: Option<DisplayMode>,
    #[serde(default)]
    pub current_screen: Option<String>,
    #[serde(default)]
    pub last_screen_status: Option<ScreenStatus>,
    #[serde(default)]
    pub usage_limit: Option<CliUsageLimitState>,
    #[serde(default)]
    pub current_image_key: Option<String>,
    #[serde(default)]
    pub tui_prompt_card_id: Option<String>,
    #[serde(default)]
    pub tui_prompt_options: Vec<crate::ipc::TuiPromptOption>,
    #[serde(default)]
    pub tui_prompt_multi_select: Option<bool>,
    #[serde(default)]
    pub tui_toggled_indices: Vec<usize>,
    #[serde(default)]
    pub pending_response_card_id: Option<String>,
    #[serde(default)]
    pub pending_response_card_state: Option<PendingResponseCardState>,
    #[serde(default)]
    pub last_patched_response_card_id: Option<String>,
    #[serde(default)]
    pub terminal_url: Option<String>,
    #[serde(default)]
    pub last_final_output_turn_id: Option<String>,
    #[serde(default)]
    pub last_final_output: Option<String>,
    #[serde(default)]
    pub adopted_from: Option<AdoptedFrom>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub bot_name: Option<String>,
    #[serde(default)]
    pub bot_open_id: Option<String>,
    #[serde(default)]
    pub resume_session_id: Option<String>,
    #[serde(default)]
    pub disable_cli_bypass: bool,
    #[serde(default)]
    pub initial_prompt: Option<String>,
    /// Feishu thread_id (omt_*), stable topic identifier.
    /// Present for topic-group messages and p2p thread follow-ups that carry
    /// thread metadata.  Used as the session-matching anchor for Thread-scoped
    /// sessions.  For p2p, thread_id may be backfilled from a follow-up message
    /// after the initial session is created (first p2p session starts with
    /// thread_id=None and matches follow-ups via root_message_id).
    #[serde(default)]
    pub thread_id: Option<String>,
}
