use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Map, Value};

const OPENCODE_PLUGIN_TEMPLATE: &str = include_str!("../assets/opencode/beam-ask.js");

fn home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn write_if_changed(path: &Path, content: &str) -> Result<bool> {
    if let Ok(existing) = fs::read_to_string(path) {
        if existing == content {
            return Ok(false);
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(true)
}

fn hook_command(cli_id: &str) -> Result<String> {
    Ok(format!("beam hook {}", cli_id))
}

fn install_claude_settings(path: &Path, hook_cmd: &str) -> Result<()> {
    let mut root = match fs::read_to_string(path) {
        Ok(raw) => serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| serde_json::json!({})),
        Err(_) => serde_json::json!({}),
    };
    if !root.is_object() {
        root = serde_json::json!({});
    }
    let obj = root.as_object_mut().expect("object");
    let hooks_val = obj
        .entry("hooks".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let hooks = hooks_val.as_object_mut().expect("hooks object");
    let pre_tool = hooks
        .entry("PreToolUse".to_string())
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .expect("pretool array");

    pre_tool.retain(|group| {
        let Some(group_obj) = group.as_object() else {
            return true;
        };
        let is_target = group_obj.get("matcher").and_then(Value::as_str) == Some("AskUserQuestion");
        if !is_target {
            return true;
        }
        let cmd = group_obj
            .get("hooks")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first())
            .and_then(|hook| hook.get("command"))
            .and_then(Value::as_str);
        cmd != Some(hook_cmd)
    });

    pre_tool.push(serde_json::json!({
        "matcher": "AskUserQuestion",
        "hooks": [{
            "type": "command",
            "command": hook_cmd,
            "timeout": 86400
        }]
    }));

    let content = serde_json::to_string_pretty(&root)? + "\n";
    let _ = write_if_changed(path, &content)?;
    Ok(())
}

fn install_opencode_plugin(path: &Path) -> Result<()> {
    let _ = write_if_changed(path, OPENCODE_PLUGIN_TEMPLATE)?;
    Ok(())
}

pub fn install_hooks() -> Result<()> {
    install_hooks_at(&home_dir())
}

fn install_hooks_at(home: &Path) -> Result<()> {
    install_claude_settings(
        &home.join(".claude").join("settings.json"),
        &hook_command("claude-code")?,
    )?;
    install_opencode_plugin(
        &home
            .join(".config")
            .join("opencode")
            .join("plugins")
            .join("beam-ask.js"),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_command_uses_beam_cli() {
        let cmd = hook_command("claude-code").expect("hook cmd");
        assert!(cmd.contains("hook claude-code"));
    }

    #[test]
    fn install_hooks_writes_files() {
        let root = std::env::temp_dir().join(format!(
            "beam-hook-setup-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        install_hooks_at(&root).expect("install hooks");

        let claude = root.join(".claude").join("settings.json");
        let opencode = root
            .join(".config")
            .join("opencode")
            .join("plugins")
            .join("beam-ask.js");
        let claude_raw = std::fs::read_to_string(&claude).expect("claude settings");
        let opencode_raw = std::fs::read_to_string(&opencode).expect("opencode plugin");
        assert!(claude_raw.contains("AskUserQuestion"));
        assert!(claude_raw.contains("hook claude-code"));
        assert!(opencode_raw.contains("BeamAskPlugin"));
        assert!(opencode_raw.contains("const BEAM_CLI_ID = \"opencode\";"));
        assert!(opencode_raw.contains("question.asked"));
        assert!(opencode_raw.contains("permission.asked"));
        assert!(opencode_raw.contains("permission.replied"));
        assert!(opencode_raw.contains("spawn("));
        assert!(opencode_raw.contains("trackBackground("));
        assert!(opencode_raw.contains("postSessionIdPermissionsPermissionId"));
        assert!(opencode_raw.contains("sessionID"));
        assert!(opencode_raw.contains("client.question.reply"));
        assert!(opencode_raw.contains("patterns"));
        assert!(opencode_raw.contains("seenPermissionIds"));
        assert!(!opencode_raw.contains("spawnSync"));
        assert!(!opencode_raw.contains("fetch("));
        assert!(!opencode_raw.contains("serverUrl"));
        assert!(!opencode_raw.contains("appendFileSync"));
        assert!(!opencode_raw.contains("LOG_PATH"));
        let _ = std::fs::remove_dir_all(root);
    }
}
