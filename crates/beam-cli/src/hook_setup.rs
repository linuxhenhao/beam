use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Map, Value};

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
    let exe = env::current_exe()?;
    Ok(format!("\"{}\" hook {}", exe.display(), cli_id))
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

fn install_opencode_plugin(path: &Path, cli_id: &str) -> Result<()> {
    let exe = env::current_exe()?;
    let exe_json = serde_json::to_string(&exe.display().to_string())?;
    let cli_json = serde_json::to_string(cli_id)?;
    let content = format!(
        r#"// beam ask hook for OpenCode
import {{ spawnSync }} from "child_process";

const BEAM_BIN = {exe_json};
const BEAM_CLI_ID = {cli_json};

function runBeamHook(payload) {{
  try {{
    const result = spawnSync(BEAM_BIN, ["hook", BEAM_CLI_ID], {{
      input: JSON.stringify(payload),
      encoding: "utf-8",
      timeout: 86400000,
    }});
    if (result.status === 0 && result.stdout && result.stdout.trim()) {{
      return JSON.parse(result.stdout.trim());
    }}
  }} catch {{}}
  return undefined;
}}

function normalizePermissionPayload(input) {{
  const patterns = Array.isArray(input?.pattern)
    ? input.pattern
    : input?.pattern
      ? [input.pattern]
      : [];
  return {{
    hook_event_name: "permission.ask",
    id: input?.id ?? "",
    permission: input?.type ?? input?.title ?? "permission request",
    patterns,
    metadata: input?.metadata ?? {{}},
    tool: {{
      messageID: input?.messageID ?? "",
      callID: input?.callID ?? "",
    }},
  }};
}}

function handlePermission(input, output) {{
  const directive = runBeamHook(normalizePermissionPayload(input));
  if (!directive || directive.type !== "permission") return;
  output.status = directive.reply === "reject" ? "deny" : "allow";
}}

export const BeamAskPlugin = async ({{ serverUrl }}) => ({{
  event: async ({{ event }}) => {{
    if (event?.type !== "question.asked") return;
    const directive = runBeamHook({{
      hook_event_name: event.type,
      ...(event.properties ?? {{}}),
    }});
    if (!directive || directive.type !== "answer") return;
    await fetch(new URL(`/question/${{event.properties.id}}/reply`, serverUrl), {{
      method: "POST",
      headers: {{ "content-type": "application/json" }},
      body: JSON.stringify({{ answers: directive.answers ?? [] }}),
    }});
  }},
  "permission.asked": async (input, output) => {{
    handlePermission(input, output);
  }},
}});
"#
    );
    let _ = write_if_changed(path, &content)?;
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
        "opencode",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_command_quotes_binary_path() {
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
        assert!(opencode_raw.contains("question.asked"));
        assert!(opencode_raw.contains("permission.asked"));
        let _ = std::fs::remove_dir_all(root);
    }
}
