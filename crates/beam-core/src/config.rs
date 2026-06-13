use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendType {
    Pty,
    Tmux,
    Zellij,
}

impl Default for BackendType {
    fn default() -> Self {
        Self::Tmux
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonConfig {
    #[serde(default)]
    pub backend_type: BackendType,
    #[serde(default)]
    pub quiet_restart: bool,
    #[serde(default = "default_working_dirs")]
    pub working_dirs: Vec<String>,
}

fn default_working_dirs() -> Vec<String> {
    vec!["~".to_string()]
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            backend_type: BackendType::default(),
            quiet_restart: false,
            working_dirs: default_working_dirs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebConfig {
    #[serde(default = "default_web_host")]
    pub host: String,
    #[serde(default = "default_proxy_base_port")]
    pub proxy_base_port: u16,
}

fn default_web_host() -> String {
    "0.0.0.0".to_string()
}

fn default_proxy_base_port() -> u16 {
    8800
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            host: default_web_host(),
            proxy_base_port: default_proxy_base_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Config {
    #[serde(default)]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub lark: LarkConfig,
    #[serde(default, rename = "screenAnalyzer")]
    pub screen_analyzer: ScreenAnalyzerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotConfig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "larkAppId")]
    pub lark_app_id: String,
    #[serde(rename = "larkAppSecret")]
    pub lark_app_secret: String,
    #[serde(rename = "cliId")]
    pub cli_id: String,
    #[serde(rename = "cliBin", default)]
    pub cli_bin: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(rename = "workingDir", default)]
    pub working_dir: Option<String>,
    #[serde(rename = "backendType", default)]
    pub backend_type: Option<BackendType>,
    #[serde(rename = "larkEncryptKey", default)]
    pub lark_encrypt_key: Option<String>,
    #[serde(rename = "larkVerificationToken", default)]
    pub lark_verification_token: Option<String>,
    #[serde(rename = "allowedUsers", default)]
    pub allowed_users: Vec<String>,
    #[serde(rename = "privateCard", default)]
    pub private_card: bool,
    #[serde(rename = "allowedChatGroups", default)]
    pub allowed_chat_groups: Vec<String>,
    #[serde(rename = "chatGrants", default)]
    pub chat_grants: std::collections::HashMap<String, Vec<String>>,
    #[serde(rename = "globalGrants", default)]
    pub global_grants: Vec<String>,
    #[serde(rename = "oncallChats", default)]
    pub oncall_chats: Vec<OncallChatBinding>,
    #[serde(rename = "restrictGrantCommands", default)]
    pub restrict_grant_commands: bool,
    #[serde(rename = "messageQuota", default)]
    pub message_quota: Option<MessageQuotaConfig>,
    #[serde(rename = "quotaState", default)]
    pub quota_state: std::collections::HashMap<String, QuotaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OncallChatBinding {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    #[serde(rename = "workingDir", default)]
    pub working_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageQuotaConfig {
    #[serde(rename = "defaultLimit", default)]
    pub default_limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaEntry {
    pub limit: u32,
    pub used: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LarkConfig {
    #[serde(default = "default_lark_event_mode")]
    pub event_mode: String,
    #[serde(default)]
    pub verification_token: Option<String>,
    #[serde(default)]
    pub encrypt_key: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ScreenAnalyzerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub model: String,
    #[serde(default = "default_screen_analyzer_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_screen_analyzer_stable_count")]
    pub stable_count: u32,
    #[serde(default = "default_screen_analyzer_snapshot_max_chars")]
    pub snapshot_max_chars: usize,
    #[serde(default)]
    pub extra_headers: HashMap<String, String>,
    #[serde(default)]
    pub extra_body: Map<String, Value>,
}

fn default_screen_analyzer_interval_ms() -> u64 {
    2_000
}

fn default_screen_analyzer_stable_count() -> u32 {
    6
}

fn default_screen_analyzer_snapshot_max_chars() -> usize {
    8_000
}

impl Default for ScreenAnalyzerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: String::new(),
            api_key: String::new(),
            model: String::new(),
            interval_ms: default_screen_analyzer_interval_ms(),
            stable_count: default_screen_analyzer_stable_count(),
            snapshot_max_chars: default_screen_analyzer_snapshot_max_chars(),
            extra_headers: HashMap::new(),
            extra_body: Map::new(),
        }
    }
}

fn default_lark_event_mode() -> String {
    "http".to_string()
}

impl Default for LarkConfig {
    fn default() -> Self {
        Self {
            event_mode: default_lark_event_mode(),
            verification_token: None,
            encrypt_key: None,
        }
    }
}
