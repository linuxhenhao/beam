use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backend_kind::BackendKind;
use crate::ipc::{CliUsageLimitState, DisplayMode, ScreenStatus};

/// Agent attention state set via `--attention` flag, analogous to botmux `agentAttention`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentAttention {
    pub kind: String,
    pub reason: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SessionScope {
    #[default]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SessionStatus {
    #[default]
    Active,
    Closed,
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
    pub backend_kind: BackendKind,
    #[serde(default)]
    pub tmux_target: Option<String>,
    #[serde(default)]
    pub zellij_session: Option<String>,
    #[serde(default)]
    pub zellij_pane_id: Option<String>,
    #[serde(default)]
    pub herdr_workspace_id: Option<String>,
    #[serde(default)]
    pub herdr_pane_id: Option<String>,
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
    pub lark_app_id: String,
    #[serde(default)]
    pub owner_open_id: Option<String>,
    /// Sender open_id of the trigger/quote message for the current turn.
    /// Aligns with botmux `quoteTargetSenderOpenId`.
    /// May differ from `owner_open_id` in multi-user group chats where
    /// a non-owner triggers a follow-up turn.
    #[serde(default)]
    pub quote_target_sender_open_id: Option<String>,
    #[serde(default)]
    pub worker_pid: Option<u32>,
    #[serde(default)]
    pub cli_id: Option<String>,
    #[serde(default)]
    pub cli_bin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_slice: Option<String>,
    #[serde(default)]
    pub cli_args: Vec<String>,
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
    /// Timestamp of the most recent explicit `beam send` (structured final output).
    /// Set by `handle_final_output_request`; NOT set by worker bridge delivery.
    /// Used by `should_skip_worker_final_output` to suppress duplicate worker
    /// output when the model already sent the same content via explicit send.
    /// Minimal botmux-equivalent: botmux records turn-sends markers; Beam only
    /// needs a single timestamp for the 10-minute dedupe window.
    #[serde(default)]
    pub last_explicit_send_at: Option<DateTime<Utc>>,
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
    /// Terminal backend for this session, persisted at create/adopt time.
    /// Restore only reads the persisted value; a later config flip must not
    /// move an existing session to another mux.
    #[serde(default)]
    pub backend_kind: BackendKind,
    /// Named Herdr session escape hatch (round-trips with `InitConfig`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub herdr_session: Option<String>,
    /// Herdr public ids. Unlike zellij, these cannot be derived from the
    /// beam session id, so they are persisted when Ready arrives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub herdr_workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub herdr_pane_id: Option<String>,
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
    /// Agent attention state set via `--attention` flag.
    /// Cleared on next user inbound message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_attention: Option<AgentAttention>,
    /// The turn_id of the most recent input sent to this session.
    /// Set atomically by send_input before dispatching to the worker.
    /// Used by the daemon to validate screenshot uploads (CAS check).
    /// New/restart sessions with no input remain None.
    #[serde(default)]
    pub current_turn_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_deser_old_data_without_quote_target_sender_open_id() {
        // Old session JSON (before quote_target_sender_open_id was added)
        // must deserialize with the field defaulting to None.
        let json = r#"{
            "session_id": "test-sess-1",
            "title": "test",
            "chat_id": "chat-1",
            "root_message_id": "root-1",
            "scope": "thread",
            "status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "lark_app_id": "app-1",
            "owner_open_id": "ou_owner"
        }"#;
        let session: Session = serde_json::from_str(json).expect("should deserialize old session");
        assert_eq!(session.session_id, "test-sess-1");
        assert_eq!(session.owner_open_id.as_deref(), Some("ou_owner"));
        assert_eq!(
            session.quote_target_sender_open_id, None,
            "old sessions without the field should default to None"
        );
    }

    #[test]
    fn session_deser_with_quote_target_sender_open_id() {
        let json = r#"{
            "session_id": "test-sess-2",
            "title": "test",
            "chat_id": "chat-1",
            "root_message_id": "root-1",
            "scope": "thread",
            "status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "lark_app_id": "app-1",
            "owner_open_id": "ou_owner",
            "quote_target_sender_open_id": "ou_sender"
        }"#;
        let session: Session = serde_json::from_str(json).expect("should deserialize session");
        assert_eq!(session.owner_open_id.as_deref(), Some("ou_owner"));
        assert_eq!(
            session.quote_target_sender_open_id.as_deref(),
            Some("ou_sender"),
            "new sessions should preserve the quote target sender"
        );
    }

    #[test]
    fn session_deser_old_data_without_agent_attention() {
        // Old session JSON (before agent_attention was added)
        // must deserialize with the field defaulting to None.
        let json = r#"{
            "session_id": "test-sess-3",
            "title": "test",
            "chat_id": "chat-1",
            "root_message_id": "root-1",
            "scope": "thread",
            "status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "lark_app_id": "app-1",
            "owner_open_id": "ou_owner"
        }"#;
        let session: Session = serde_json::from_str(json).expect("should deserialize old session");
        assert_eq!(session.session_id, "test-sess-3");
        assert_eq!(
            session.agent_attention, None,
            "old sessions without the field should default to None"
        );
    }

    #[test]
    fn session_deser_with_agent_attention() {
        let json = r#"{
            "session_id": "test-sess-4",
            "title": "test",
            "chat_id": "chat-1",
            "root_message_id": "root-1",
            "scope": "thread",
            "status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "lark_app_id": "app-1",
            "owner_open_id": "ou_owner",
            "agent_attention": {
                "kind": "blocked",
                "reason": "need approval",
                "at": "2025-06-01T12:00:00Z"
            }
        }"#;
        let session: Session = serde_json::from_str(json).expect("should deserialize session");
        let aa = session
            .agent_attention
            .as_ref()
            .expect("should have agent_attention");
        assert_eq!(aa.kind, "blocked");
        assert_eq!(aa.reason, "need approval");
        assert_eq!(aa.at.to_rfc3339(), "2025-06-01T12:00:00+00:00");
    }

    #[test]
    fn session_deser_old_data_without_current_turn_id() {
        // Old session JSON (before current_turn_id was added)
        // must deserialize with the field defaulting to None.
        let json = r#"{
            "session_id": "test-sess-5",
            "title": "test",
            "chat_id": "chat-1",
            "root_message_id": "root-1",
            "scope": "thread",
            "status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "lark_app_id": "app-1"
        }"#;
        let session: Session = serde_json::from_str(json).expect("should deserialize old session");
        assert_eq!(
            session.current_turn_id, None,
            "old sessions without the field should default to None"
        );
    }

    #[test]
    fn session_deser_with_current_turn_id() {
        let json = r#"{
            "session_id": "test-sess-6",
            "title": "test",
            "chat_id": "chat-1",
            "root_message_id": "root-1",
            "scope": "thread",
            "status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "lark_app_id": "app-1",
            "current_turn_id": "turn-abc"
        }"#;
        let session: Session = serde_json::from_str(json).expect("should deserialize session");
        assert_eq!(
            session.current_turn_id.as_deref(),
            Some("turn-abc"),
            "new sessions should preserve current_turn_id"
        );
    }
}
