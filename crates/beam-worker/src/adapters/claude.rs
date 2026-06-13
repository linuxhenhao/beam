use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    ClaudeState, PendingTurnKind, PollResult, SpawnSpec, SubmitResult, drain_jsonl, file_size,
    normalize_history_text, realpath_cwd,
};
use crate::backend::SessionBackend;

const CLAUDE_SUBMIT_MARKERS: [&str; 2] = [
    "\"role\":\"user\",\"content\":\"",
    "\"operation\":\"enqueue\"",
];

pub fn create_state(init: &InitConfig) -> ClaudeState {
    let data_dir = PathBuf::from(
        std::env::var("CLAUDE_CONFIG_DIR")
            .unwrap_or_else(|_| format!("{}/.claude", std::env::var("HOME").unwrap_or_default())),
    );
    let session_jsonl = claude_jsonl_path_for_session(
        init.cli_session_id.as_deref().unwrap_or(&init.session_id),
        &init.working_dir,
        &data_dir,
    );
    ClaudeState {
        data_dir,
        session_jsonl,
        cli_pid: None,
        cli_cwd: init.working_dir.clone(),
        cli_session_id: init.cli_session_id.clone(),
        transcript_offset: 0,
        pending_tail: String::new(),
        pending_final_text: None,
        pending_final_since: None,
        emitted_final_text: None,
        adopt_mode: init.adopted_from.is_some(),
        adopt_restored_from_metadata: init.adopt_restored_from_metadata,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    }
}

pub fn build_spawn_spec(_state: &ClaudeState, init: &InitConfig) -> SpawnSpec {
    let mut args = Vec::new();
    if init.resume {
        args.push("--resume".to_string());
        args.push(
            init.cli_session_id
                .clone()
                .unwrap_or_else(|| init.session_id.clone()),
        );
    } else {
        args.push("--session-id".to_string());
        args.push(init.session_id.clone());
    }
    if !init.disable_cli_bypass {
        args.push("--dangerously-skip-permissions".to_string());
    }
    args.push("--settings".to_string());
    args.push(
        serde_json::json!({
            "skipDangerousModePermissionPrompt": true,
            "permissions": { "defaultMode": "bypassPermissions" },
        })
        .to_string(),
    );
    args.push("--disallowed-tools".to_string());
    args.push("EnterPlanMode,ExitPlanMode".to_string());
    args.extend(init.cli_args.clone());
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args,
    }
}

pub async fn write_input(
    state: &mut ClaudeState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    if state.adopt_mode {
        state
            .pending_remote_user_inputs
            .push_back(normalize_history_text(content));
    }
    refresh_claude_pid_state(state);
    let base_byte = file_size(&state.session_jsonl);
    let lines: Vec<&str> = content.split('\n').collect();
    for (index, line) in lines.iter().enumerate() {
        if !line.is_empty() {
            backend.send_text(line).await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        if index < lines.len() - 1 {
            backend.send_text("\\").await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
            backend.send_enter().await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    backend.send_enter().await?;
    for _ in 0..4 {
        if claude_submit_seen(&state.session_jsonl, base_byte)? {
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: state.cli_session_id.clone(),
                ..Default::default()
            });
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        backend.send_enter().await?;
    }
    Ok(SubmitResult {
        submitted: false,
        cli_session_id: state.cli_session_id.clone(),
        failure_reason: Some("Claude transcript did not confirm submit".to_string()),
    })
}

pub fn poll(state: &mut ClaudeState) -> Result<PollResult> {
    refresh_claude_pid_state(state);
    if state.adopt_mode && !state.adopt_preamble_emitted {
        let baseline = if state.adopt_restored_from_metadata {
            None
        } else {
            baseline_claude_adopt_preamble(&state.session_jsonl)?
        };
        state.transcript_offset = file_size(&state.session_jsonl);
        state.pending_tail.clear();
        state.pending_final_text = None;
        state.pending_final_since = None;
        state.adopt_preamble_emitted = true;
        return Ok(PollResult {
            cli_session_id: state.cli_session_id.clone(),
            final_output: None,
            final_output_kind: None,
            final_output_user_text: None,
            adopt_preamble: baseline,
            prompt_ready: false,
        });
    }
    let drain = drain_jsonl(
        &state.session_jsonl,
        state.transcript_offset,
        &state.pending_tail,
    )?;
    state.transcript_offset = drain.new_offset;
    state.pending_tail = drain.pending_tail;
    for line in &drain.lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(role) = value.pointer("/message/role").and_then(Value::as_str) {
            match role {
                "user" if state.adopt_mode => {
                    let text = extract_claude_message_text(&value);
                    if !text.trim().is_empty() {
                        let normalized = normalize_history_text(&text);
                        let kind = if state
                            .pending_remote_user_inputs
                            .front()
                            .map(|expected| *expected == normalized)
                            .unwrap_or(false)
                        {
                            let _ = state.pending_remote_user_inputs.pop_front();
                            PendingTurnKind::Remote
                        } else {
                            PendingTurnKind::Local { user_text: text }
                        };
                        state.active_turn = Some(kind);
                    }
                }
                "assistant" => {
                    let text = extract_claude_assistant_text(&value);
                    if !text.is_empty() {
                        if state.adopt_mode && state.active_turn.is_none() {
                            state.active_turn = Some(PendingTurnKind::LocalHeadless);
                        }
                        state.pending_final_text = Some(text);
                        state.pending_final_since = Some(Instant::now());
                    }
                }
                _ => {}
            }
        }
    }

    let mut result = PollResult {
        cli_session_id: state.cli_session_id.clone(),
        final_output: None,
        final_output_kind: None,
        final_output_user_text: None,
        adopt_preamble: None,
        prompt_ready: false,
    };
    if let (Some(text), Some(since)) = (&state.pending_final_text, state.pending_final_since) {
        if since.elapsed() >= Duration::from_millis(1200)
            && state.emitted_final_text.as_deref() != Some(text.as_str())
        {
            let kind = state.active_turn.take();
            state.emitted_final_text = Some(text.clone());
            result.final_output = Some(text.clone());
            match kind {
                Some(PendingTurnKind::Local { user_text }) => {
                    result.final_output_kind = Some(FinalOutputKind::LocalTurn);
                    result.final_output_user_text = Some(user_text);
                }
                Some(PendingTurnKind::LocalHeadless) => {
                    result.final_output_kind = Some(FinalOutputKind::LocalTurnHeadless);
                }
                _ => {}
            }
            result.prompt_ready = true;
        }
    }
    Ok(result)
}

fn claude_jsonl_path_for_session(session_id: &str, cwd: &str, data_dir: &Path) -> PathBuf {
    let project_hash = realpath_cwd(cwd)
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    data_dir
        .join("projects")
        .join(project_hash)
        .join(format!("{}.jsonl", session_id))
}

fn refresh_claude_pid_state(state: &mut ClaudeState) {
    let Some(pid) = state.cli_pid else {
        return;
    };
    let path = state
        .data_dir
        .join("sessions")
        .join(format!("{}.json", pid));
    let Ok(raw) = std::fs::read_to_string(path) else {
        return;
    };
    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
        return;
    };
    let Some(session_id) = value.get("sessionId").and_then(Value::as_str) else {
        return;
    };
    let Some(cwd) = value.get("cwd").and_then(Value::as_str) else {
        return;
    };
    state.cli_session_id = Some(session_id.to_string());
    state.session_jsonl = claude_jsonl_path_for_session(session_id, cwd, &state.data_dir);
    state.cli_cwd = cwd.to_string();
}

fn claude_submit_seen(path: &Path, from_byte: u64) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let size = file_size(path);
    if size <= from_byte {
        return Ok(false);
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(from_byte))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(CLAUDE_SUBMIT_MARKERS
        .iter()
        .any(|marker| text.contains(marker)))
}

fn extract_claude_assistant_text(value: &Value) -> String {
    let Some(content) = value.pointer("/message/content") else {
        return String::new();
    };
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    let Some(items) = content.as_array() else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|block| {
            if block.get("type").and_then(Value::as_str) == Some("text") {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn extract_claude_message_text(value: &Value) -> String {
    let Some(content) = value.pointer("/message/content") else {
        return String::new();
    };
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    let Some(items) = content.as_array() else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|block| {
            block
                .get("text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn baseline_claude_adopt_preamble(path: &Path) -> Result<Option<(String, String)>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)?;
    let mut pending_user: Option<String> = None;
    let mut latest_pair: Option<(String, String)> = None;
    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(role) = value.pointer("/message/role").and_then(Value::as_str) else {
            continue;
        };
        let text = extract_claude_message_text(&value);
        if text.trim().is_empty() {
            continue;
        }
        match role {
            "user" => pending_user = Some(text),
            "assistant" => {
                if let Some(user_text) = pending_user.take() {
                    latest_pair = Some((user_text, text));
                }
            }
            _ => {}
        }
    }
    Ok(latest_pair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use beam_core::FinalOutputKind;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "beam-claude-{}-{}",
            name,
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn extracts_claude_assistant_text_blocks() {
        let value = serde_json::json!({
            "message": {
                "content": [
                    { "type": "text", "text": "first" },
                    { "type": "tool_use", "name": "ignored" },
                    { "type": "text", "text": "second" }
                ]
            }
        });
        assert_eq!(extract_claude_assistant_text(&value), "first\n\nsecond");
    }

    #[test]
    fn emits_stable_assistant_text() {
        let path = temp_path("claude.jsonl");
        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"ready\"}]}}\n",
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = poll(&mut state).unwrap();
        assert!(first.final_output.is_none());
        assert!(!first.prompt_ready);
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = poll(&mut state).unwrap();
        assert_eq!(second.final_output.as_deref(), Some("ready"));
        assert!(second.prompt_ready);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_preamble_once_and_absorbs_history() {
        let path = temp_path("claude-adopt.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = poll(&mut state).unwrap();
        assert_eq!(
            first.adopt_preamble,
            Some(("ask".to_string(), "answer".to_string()))
        );
        assert!(first.final_output.is_none());
        let second = poll(&mut state).unwrap();
        assert!(second.adopt_preamble.is_none());
        assert!(second.final_output.is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_local_turn_when_user_text_is_not_from_daemon() {
        let path = temp_path("claude-adopt-local.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"local ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"local answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = poll(&mut state).unwrap();
        assert!(first.final_output.is_none());
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = poll(&mut state).unwrap();
        assert_eq!(second.final_output.as_deref(), Some("local answer"));
        assert_eq!(second.final_output_kind, Some(FinalOutputKind::LocalTurn));
        assert_eq!(second.final_output_user_text.as_deref(), Some("local ask"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_headless_local_turn_when_assistant_arrives_first() {
        let path = temp_path("claude-adopt-headless.jsonl");
        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"headless answer\"}]}}\n",
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = poll(&mut state).unwrap();
        assert!(first.final_output.is_none());
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = poll(&mut state).unwrap();
        assert_eq!(second.final_output.as_deref(), Some("headless answer"));
        assert_eq!(
            second.final_output_kind,
            Some(FinalOutputKind::LocalTurnHeadless)
        );
        assert_eq!(second.final_output_user_text, None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_keeps_remote_turn_as_bridge_output() {
        let path = temp_path("claude-adopt-remote.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"remote ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"remote answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::from([crate::adapter::normalize_history_text(
                "remote ask",
            )]),
            active_turn: None,
        };
        let first = poll(&mut state).unwrap();
        assert!(first.final_output.is_none());
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = poll(&mut state).unwrap();
        assert_eq!(second.final_output.as_deref(), Some("remote answer"));
        assert_eq!(second.final_output_kind, None);
        assert_eq!(second.final_output_user_text, None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_restored_absorbs_history_without_preamble() {
        let path = temp_path("claude-adopt-restored.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: true,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = poll(&mut state).unwrap();
        assert!(first.adopt_preamble.is_none());
        assert!(first.final_output.is_none());
        let second = poll(&mut state).unwrap();
        assert!(second.adopt_preamble.is_none());
        assert!(second.final_output.is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bridge_turn_queue_complete_cycle() {
        let path = temp_path("claude-bridge-cycle.jsonl");
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            pending_final_text: None,
            pending_final_since: None,
            emitted_final_text: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };

        let round1 = poll(&mut state).unwrap();
        assert!(round1.final_output.is_none());

        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 ask\"}]}}\n",
        )
        .unwrap();

        let round2 = poll(&mut state).unwrap();
        assert!(round2.final_output.is_none());

        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 ask\"}]}}\n\
             {\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 answer\"}]}}\n",
        )
        .unwrap();

        let round3 = poll(&mut state).unwrap();
        assert!(round3.final_output.is_none());

        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let round4 = poll(&mut state).unwrap();
        assert_eq!(round4.final_output.as_deref(), Some("turn1 answer"));
        assert!(round4.prompt_ready);

        state.pending_final_text = None;
        state.pending_final_since = None;
        state.emitted_final_text = Some("turn1 answer".to_string());

        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 ask\"}]}}\n\
             {\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 answer\"}]}}\n\
             {\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn2 ask\"}]}}\n\
             {\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"turn2 answer\"}]}}\n",
        )
        .unwrap();

        let round5 = poll(&mut state).unwrap();
        assert!(round5.final_output.is_none());

        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let round6 = poll(&mut state).unwrap();
        assert_eq!(round6.final_output.as_deref(), Some("turn2 answer"));

        let _ = std::fs::remove_file(path);
    }
}
