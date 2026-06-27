use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ipc::{CliUsageLimitState, ScreenStatus},
    session::{AdoptedFrom, PendingResponseCardState, Session, SessionStatus},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiHealth {
    pub status: String,
    pub pid: u32,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonRuntimeState {
    pub pid: u32,
    pub api_addr: String,
    pub started_at: DateTime<Utc>,
    pub log_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateSessionRequest {
    pub title: String,
    pub cli_id: String,
    pub cli_bin: String,
    #[serde(default)]
    pub cli_args: Vec<String>,
    pub working_dir: String,
    #[serde(default)]
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInputRequest {
    pub content: String,
    #[serde(default)]
    pub raw: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinalOutputRequest {
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartSessionRequest {
    #[serde(default)]
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumeSessionRequest {
    #[serde(default)]
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttemptResumeRequest {
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttemptResumeStartResponse {
    pub ok: bool,
    pub resume_id: String,
    pub run_id: String,
    pub activity_id: String,
    pub attempt_id: String,
    pub session_id: String,
    pub original_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub already_running: bool,
    pub started_at: u64,
    pub log_path: String,
    pub sidecar_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttemptResumeEndResponse {
    pub ok: bool,
    pub resume_id: String,
    pub status: String,
    pub close_reason: String,
    pub closed_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TerminalInfo {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSummary {
    pub session_id: String,
    pub title: String,
    pub status: SessionStatus,
    pub chat_type: Option<String>,
    pub quote_target_id: Option<String>,
    pub cli_id: Option<String>,
    pub cli_bin: Option<String>,
    pub cli_args: Vec<String>,
    pub working_dir: Option<String>,
    pub worker_pid: Option<u32>,
    pub terminal_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_token: Option<String>,
    pub created_at: DateTime<Utc>,
    pub stream_card_nonce: Option<String>,
    pub current_screen: Option<String>,
    pub last_screen_status: Option<ScreenStatus>,
    pub usage_limit: Option<CliUsageLimitState>,
    pub current_image_key: Option<String>,
    pub tui_prompt_card_id: Option<String>,
    pub tui_prompt_options: Vec<crate::ipc::TuiPromptOption>,
    pub tui_prompt_multi_select: Option<bool>,
    pub tui_toggled_indices: Vec<usize>,
    pub pending_response_card_id: Option<String>,
    pub pending_response_card_state: Option<PendingResponseCardState>,
    pub last_patched_response_card_id: Option<String>,
    pub last_final_output_turn_id: Option<String>,
    pub last_final_output: Option<String>,
    pub adopted_from: Option<AdoptedFrom>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotSummary {
    pub lark_app_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub cli_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub allowed_users: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_chat_groups: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub oncall_chats: Vec<String>,
    pub private_card: bool,
    pub active_sessions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionGroup {
    pub chat_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub sessions: Vec<SessionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonOverview {
    pub pid: u32,
    pub started_at: DateTime<Utc>,
    pub session_count: usize,
    pub active_session_count: usize,
    pub closed_session_count: usize,
    pub bot_count: usize,
    pub worker_count: usize,
    pub config_path: String,
    pub data_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionLocateInfo {
    pub session_id: String,
    pub terminal_url: Option<String>,
    pub worker_pid: Option<u32>,
}

impl From<&Session> for SessionSummary {
    fn from(value: &Session) -> Self {
        Self {
            session_id: value.session_id.clone(),
            title: value.title.clone(),
            status: value.status,
            chat_type: value.chat_type.clone(),
            quote_target_id: value.quote_target_id.clone(),
            cli_id: value.cli_id.clone(),
            cli_bin: value.cli_bin.clone(),
            cli_args: value.cli_args.clone(),
            working_dir: value.working_dir.clone(),
            worker_pid: value.worker_pid,
            terminal_url: value.terminal_url.clone(),
            read_only_token: None,
            write_token: None,
            created_at: value.created_at,
            stream_card_nonce: value.stream_card_nonce.clone(),
            current_screen: value.current_screen.clone(),
            last_screen_status: value.last_screen_status,
            usage_limit: value.usage_limit.clone(),
            current_image_key: value.current_image_key.clone(),
            tui_prompt_card_id: value.tui_prompt_card_id.clone(),
            tui_prompt_options: value.tui_prompt_options.clone(),
            tui_prompt_multi_select: value.tui_prompt_multi_select,
            tui_toggled_indices: value.tui_toggled_indices.clone(),
            pending_response_card_id: value.pending_response_card_id.clone(),
            pending_response_card_state: value.pending_response_card_state,
            last_patched_response_card_id: value.last_patched_response_card_id.clone(),
            last_final_output_turn_id: value.last_final_output_turn_id.clone(),
            last_final_output: value.last_final_output.clone(),
            adopted_from: value.adopted_from.clone(),
        }
    }
}
