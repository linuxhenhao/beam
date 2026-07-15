use super::discover_session_id;
use crate::*;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub(crate) fn daemon_log_path(paths: &BeamPaths) -> PathBuf {
    paths.daemon_log()
}

pub(crate) fn load_bots(paths: &BeamPaths) -> Result<Vec<BotConfig>> {
    match std::fs::read_to_string(paths.bots_json()) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn load_sessions_from_store(
    paths: &BeamPaths,
) -> Result<std::collections::HashMap<String, Session>> {
    match std::fs::read(paths.session_store_json()) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(std::collections::HashMap::new())
        }
        Err(err) => Err(err.into()),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BotInfoEntry {
    #[serde(rename = "larkAppId")]
    pub(crate) lark_app_id: String,
    #[serde(rename = "botOpenId")]
    pub(crate) bot_open_id: Option<String>,
    #[serde(rename = "botName")]
    pub(crate) bot_name: Option<String>,
    #[serde(rename = "cliId")]
    pub(crate) cli_id: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct BotListEntry {
    pub(crate) name: String,
    #[serde(rename = "openId")]
    pub(crate) open_id: String,
    #[serde(rename = "isSelf")]
    pub(crate) is_self: bool,
    source: &'static str,
    #[serde(rename = "larkAppId")]
    pub(crate) lark_app_id: String,
    #[serde(rename = "workflowBot")]
    workflow_bot: String,
    capability: Option<String>,
    #[serde(rename = "hasTeamRole")]
    has_team_role: bool,
    pub(crate) mentionable: bool,
    #[serde(rename = "mentionSource")]
    pub(crate) mention_source: &'static str,
}

pub(crate) fn load_bot_info_entries(paths: &BeamPaths) -> Result<Vec<BotInfoEntry>> {
    let path = paths.root().join("bots-info.json");
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn format_bot_info_entries_for_cli(
    entries: &[BotInfoEntry],
    current_lark_app_id: &str,
) -> Vec<BotListEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            let open_id = entry.bot_open_id.as_ref()?.clone();
            let is_self = entry.lark_app_id == current_lark_app_id;
            Some(BotListEntry {
                name: entry
                    .bot_name
                    .clone()
                    .unwrap_or_else(|| entry.cli_id.clone()),
                open_id,
                is_self,
                source: "configured",
                lark_app_id: entry.lark_app_id.clone(),
                workflow_bot: entry.lark_app_id.clone(),
                capability: None,
                has_team_role: false,
                mentionable: is_self,
                mention_source: if is_self { "self" } else { "fallback" },
            })
        })
        .collect()
}

pub(crate) fn arg_value(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find_map(|pair| (pair[0] == name).then(|| pair[1].clone()))
}

pub(crate) fn cmd_bots(args: Vec<String>, paths: &BeamPaths) -> Result<()> {
    let (sub, rest) = match args.first() {
        Some(first) if !first.starts_with('-') => (first.as_str(), &args[1..]),
        _ => ("list", args.as_slice()),
    };
    if sub != "list" && sub != "ls" {
        bail!("用法: beam bots list [--session-id ID]");
    }

    let session_id = match arg_value(rest, "--session-id") {
        Some(value) => value,
        None => discover_session_id(paths).map_err(|_| {
            anyhow::anyhow!(
                "无法推断 session-id。请在 Lark 话题内的 CLI 会话中运行，或传 --session-id <id>。"
            )
        })?,
    };
    let sessions = load_sessions_from_store(paths)?;
    let session = sessions
        .get(&session_id)
        .with_context(|| format!("未找到 session {}", session_id))?;
    if session.lark_app_id.is_empty() {
        bail!("session {} 缺少 larkAppId", session_id);
    }

    let bots =
        format_bot_info_entries_for_cli(&load_bot_info_entries(paths)?, &session.lark_app_id);
    let out = serde_json::json!({
        "sessionId": session_id,
        "chatId": session.chat_id,
        "bots": bots,
        "total": bots.len(),
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
