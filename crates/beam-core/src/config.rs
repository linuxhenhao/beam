use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::backend_kind::BackendKind;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonConfig {
    #[serde(default)]
    pub quiet_restart: bool,
    #[serde(default = "default_working_dirs")]
    pub working_dirs: Vec<String>,
    /// Terminal backend for new sessions. Existing deployments default to
    /// `zellij`; upgrades must not silently change the mux. Per-bot override
    /// lives on [`BotConfig::backend`].
    #[serde(default)]
    pub backend: BackendKind,
}

fn default_working_dirs() -> Vec<String> {
    vec!["~".to_string()]
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            quiet_restart: false,
            working_dirs: default_working_dirs(),
            backend: BackendKind::Zellij,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebConfig {
    #[serde(default = "default_web_host")]
    pub host: String,
    #[serde(default = "default_proxy_base_port")]
    pub proxy_base_port: u16,
    /// Whether the daemon starts the local zellij web server on boot.
    /// v1 defaults to `true` (existing behavior). Set to `false` to allow a
    /// herdr-only deployment to start without zellij web; only tested
    /// after PR5 lands.
    #[serde(default = "default_zellij_web")]
    pub zellij_web: bool,
    /// Emergency kill switch for the Herdr browser terminal. Defaults to
    /// `true`; set to `false` to restore the pre-web behavior (Herdr sessions
    /// get a 404 on `/s/{session_id}` and the card shows the attach hint).
    #[serde(default = "default_herdr_terminal")]
    pub herdr_terminal: bool,
    /// Max concurrent `herdr terminal session observe` children per session.
    #[serde(default = "default_herdr_max_observers_per_session")]
    pub herdr_terminal_max_observers_per_session: usize,
    /// Max concurrent `herdr terminal session observe` children daemon-wide.
    #[serde(default = "default_herdr_max_observers_global")]
    pub herdr_terminal_max_observers_global: usize,
}

fn default_web_host() -> String {
    "0.0.0.0".to_string()
}

fn default_proxy_base_port() -> u16 {
    8800
}

fn default_zellij_web() -> bool {
    true
}

fn default_herdr_terminal() -> bool {
    true
}

fn default_herdr_max_observers_per_session() -> usize {
    8
}

fn default_herdr_max_observers_global() -> usize {
    64
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            host: default_web_host(),
            proxy_base_port: default_proxy_base_port(),
            zellij_web: default_zellij_web(),
            herdr_terminal: default_herdr_terminal(),
            herdr_terminal_max_observers_per_session: default_herdr_max_observers_per_session(),
            herdr_terminal_max_observers_global: default_herdr_max_observers_global(),
        }
    }
}

/// Herdr-specific settings. Only read when a session actually runs on the
/// Herdr backend; a Zellij-default deployment never probes herdr.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HerdrConfig {
    #[serde(default = "default_herdr_min_version")]
    pub min_version: String,
    /// Named Herdr session escape hatch. v1 default is the shared `default`
    /// session; a named session hides agents from the default sidebar.
    #[serde(default = "default_herdr_session")]
    pub session: String,
    #[serde(default)]
    pub socket_path: Option<String>,
}

fn default_herdr_min_version() -> String {
    "0.8.2".to_string()
}

fn default_herdr_session() -> String {
    "default".to_string()
}

impl Default for HerdrConfig {
    fn default() -> Self {
        Self {
            min_version: default_herdr_min_version(),
            session: default_herdr_session(),
            socket_path: None,
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
    pub herdr: HerdrConfig,
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
    #[serde(rename = "cliArgs", default)]
    pub cli_args: Vec<String>,
    /// Per-bot backend override (bots.json, camelCase to match the JSON
    /// convention). `None` follows the daemon default.
    #[serde(default)]
    pub backend: Option<BackendKind>,
    /// Linux-only user systemd slice for the CLI process. Empty/omitted is unset.
    #[serde(
        rename = "cgroupSlice",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cgroup_slice: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(rename = "workingDir", default)]
    pub working_dir: Option<String>,
    #[serde(rename = "skipWorkingDirPrompt", default)]
    pub skip_working_dir_prompt: bool,
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
    /// Group-chat keywords that activate the bot without an @mention.
    /// A matching trigger can also supply the initial prompt for the
    /// session it creates.
    #[serde(rename = "customTriggers", default)]
    pub custom_triggers: Vec<CustomTrigger>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OncallChatBinding {
    #[serde(rename = "chatId")]
    pub chat_id: String,
    #[serde(rename = "workingDir", default)]
    pub working_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CustomTrigger {
    /// Keyword that activates the bot. Matched at the start of a group
    /// message when it is followed by a word boundary (whitespace,
    /// punctuation, or end of text), so a short keyword does not match
    /// inside longer words.
    #[serde(rename = "trigger")]
    pub trigger: String,
    /// Initial prompt used when this trigger creates a new session.
    /// Trailing user text after the keyword is appended after the prompt.
    #[serde(rename = "prompt", default)]
    pub prompt: Option<String>,
    /// When true, a session created by this trigger skips the directory
    /// selection card and uses `workingDir` (or the bot's default).
    #[serde(rename = "skipDirSelect", default)]
    pub skip_dir_select: bool,
    /// Working directory used when this trigger creates a session directly.
    /// Takes precedence over the bot's `workingDir`.
    #[serde(rename = "workingDir", default)]
    pub working_dir: Option<String>,
    /// Message replied immediately (as a reply to the triggering message)
    /// when this trigger creates a session, so users know the task was
    /// accepted before the longer-running work produces output.
    #[serde(rename = "ackMessage", default)]
    pub ack_message: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct LarkConfig {
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

#[cfg(test)]
mod tests {
    use super::BotConfig;

    #[test]
    fn bot_config_defaults_missing_cli_args_and_skip_prompt_fields() {
        let raw = r#"{
            "larkAppId":"app-1",
            "larkAppSecret":"secret",
            "cliId":"codex"
        }"#;
        let bot: BotConfig = serde_json::from_str(raw).expect("deserialize bot");
        assert!(bot.cli_args.is_empty());
        assert!(bot.cgroup_slice.is_none());
        assert!(!bot.skip_working_dir_prompt);
    }

    #[test]
    fn bot_config_deserializes_cgroup_slice() {
        let raw = r#"{
            "larkAppId":"app-1",
            "larkAppSecret":"secret",
            "cliId":"grok",
            "cgroupSlice":"cgtproxy-gateway.slice"
        }"#;
        let bot: BotConfig = serde_json::from_str(raw).expect("deserialize bot");
        assert_eq!(bot.cgroup_slice.as_deref(), Some("cgtproxy-gateway.slice"));
    }

    #[test]
    fn bot_config_ignores_legacy_cli_prefix_field() {
        let raw = r#"{
            "larkAppId":"app-1",
            "larkAppSecret":"secret",
            "cliId":"grok",
            "cliPrefix":["systemd-run","--user"]
        }"#;
        let bot: BotConfig = serde_json::from_str(raw).expect("deserialize bot");
        assert!(bot.cgroup_slice.is_none());
    }

    #[test]
    fn bot_config_deserializes_traex_cli_args() {
        let raw = r#"{
            "larkAppId":"app-1",
            "larkAppSecret":"secret",
            "cliId":"traex",
            "cliArgs":["-y"]
        }"#;
        let bot: BotConfig = serde_json::from_str(raw).expect("deserialize bot");
        assert_eq!(bot.cli_args, vec!["-y".to_string()]);
    }
}
