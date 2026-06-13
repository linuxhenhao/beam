use serde::{Deserialize, Serialize};

use crate::config::BackendType;
use crate::{config::ScreenAnalyzerConfig, session::AdoptedFrom};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DisplayMode {
    Hidden,
    Screenshot,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScreenStatus {
    Starting,
    Working,
    Idle,
    Analyzing,
    Limited,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CliUsageLimitKind {
    Usage,
    Rate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CliUsageLimitState {
    pub limited: bool,
    pub kind: CliUsageLimitKind,
    pub retry_at_ms: u64,
    pub retry_label: String,
    pub retry_ready: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TermActionKey {
    Esc,
    CtrlC,
    Tab,
    Enter,
    Space,
    Up,
    Down,
    Left,
    Right,
    HalfPageUp,
    HalfPageDown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TuiPromptOption {
    #[serde(default)]
    pub label: Option<String>,
    pub text: String,
    pub selected: bool,
    #[serde(rename = "type", default)]
    pub option_type: Option<String>,
    #[serde(default)]
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FinalOutputKind {
    Bridge,
    LocalTurn,
    LocalTurnHeadless,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitConfig {
    pub session_id: String,
    pub title: String,
    pub chat_id: String,
    pub root_message_id: String,
    pub working_dir: String,
    pub cli_id: String,
    pub cli_bin: String,
    #[serde(default)]
    pub cli_args: Vec<String>,
    pub backend_type: BackendType,
    pub prompt: String,
    #[serde(default)]
    pub resume: bool,
    #[serde(default)]
    pub cli_session_id: Option<String>,
    pub lark_app_id: String,
    pub lark_app_secret: String,
    #[serde(default)]
    pub prompt_turn_id: Option<String>,
    #[serde(default)]
    pub web_port: Option<u16>,
    #[serde(default)]
    pub owner_open_id: Option<String>,
    #[serde(default)]
    pub adopted_from: Option<AdoptedFrom>,
    #[serde(default)]
    pub adopt_restored_from_metadata: bool,
    #[serde(default)]
    pub screen_analyzer: ScreenAnalyzerConfig,
    #[serde(default)]
    pub initial_prompt: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonToWorker {
    Init(InitConfig),
    Message { content: String, turn_id: String },
    RawInput { content: String, turn_id: String },
    Close,
    Restart,
    SetDisplayMode { mode: DisplayMode },
    TermAction { key: TermActionKey },
    SpecialKeys { keys: Vec<String> },
    TuiKeys { keys: Vec<String>, is_final: bool },
    TuiTextInput { keys: Vec<String>, text: String },
    RefreshScreen,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerToDaemon {
    Ready {
        port: u16,
        token: String,
    },
    PromptReady,
    ScreenUpdate {
        content: String,
        status: ScreenStatus,
        #[serde(default)]
        usage_limit: Option<CliUsageLimitState>,
    },
    ScreenshotUploaded {
        image_key: String,
        status: ScreenStatus,
        #[serde(default)]
        usage_limit: Option<CliUsageLimitState>,
    },
    CliSessionId {
        cli_session_id: String,
    },
    CliExit {
        code: Option<i32>,
        signal: Option<String>,
    },
    TuiPrompt {
        description: String,
        options: Vec<TuiPromptOption>,
        #[serde(default)]
        multi_select: bool,
    },
    TuiPromptResolved {
        #[serde(default)]
        selected_text: Option<String>,
    },
    FinalOutput {
        content: String,
        turn_id: String,
        #[serde(default)]
        kind: Option<FinalOutputKind>,
        #[serde(default)]
        user_text: Option<String>,
    },
    AdoptPreamble {
        user_text: String,
        assistant_text: String,
    },
    UserNotify {
        message: String,
    },
    Error {
        message: String,
    },
}
