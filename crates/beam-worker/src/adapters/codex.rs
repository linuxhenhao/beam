use std::collections::VecDeque;
use std::fs::{File, read_dir};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    CodexState, PendingTurnKind, PollResult, SpawnSpec, SubmitResult, drain_jsonl, file_size,
    is_uuid_like, normalize_history_text,
};
use crate::backend::SessionBackend;

pub fn create_state(init: &InitConfig) -> CodexState {
    let codex_home = PathBuf::from(
        std::env::var("CODEX_HOME")
            .unwrap_or_else(|_| format!("{}/.codex", std::env::var("HOME").unwrap_or_default())),
    );
    CodexState {
        history_path: codex_home.join("history.jsonl"),
        home_dir: codex_home,
        rollout_path: None,
        cli_pid: None,
        cli_session_id: init.cli_session_id.clone(),
        transcript_offset: 0,
        pending_tail: String::new(),
        emitted_final_text: None,
        adopt_mode: init.adopted_from.is_some(),
        adopt_restored_from_metadata: init.adopt_restored_from_metadata,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    }
}

pub fn build_spawn_spec(state: &CodexState, init: &InitConfig) -> SpawnSpec {
    let mut args = Vec::new();
    if init.resume {
        if let Some(cli_session_id) = init.cli_session_id.clone().or_else(|| {
            latest_codex_session_for_beam_session(&state.history_path, &init.session_id)
        }) {
            args.push("resume".to_string());
            args.push(cli_session_id);
        }
    }
    if !init.disable_cli_bypass {
        args.push("--dangerously-bypass-approvals-and-sandbox".to_string());
    }
    args.push("--no-alt-screen".to_string());
    args.push("-C".to_string());
    args.push(init.working_dir.clone());
    args.extend(init.cli_args.clone());
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args,
    }
}

pub async fn write_input(
    state: &mut CodexState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    if state.adopt_mode {
        state
            .pending_remote_user_inputs
            .push_back(normalize_history_text(content));
    }
    for _ in 0..60 {
        let screen = backend.capture_viewport().await.unwrap_or_default();
        if screen.contains("OpenAI Codex") && screen.contains('›') {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let base_byte = file_size(&state.history_path);
    backend.paste_text(content).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.send_enter().await?;
    for _ in 0..4 {
        if let Some(cli_session_id) = codex_history_match(&state.history_path, base_byte, content)?
        {
            state.cli_session_id = Some(cli_session_id.clone());
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: Some(cli_session_id),
                ..Default::default()
            });
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        backend.send_enter().await?;
    }
    Ok(SubmitResult {
        submitted: false,
        cli_session_id: state.cli_session_id.clone(),
        failure_reason: Some("Codex history did not confirm submit".to_string()),
    })
}

pub fn poll(state: &mut CodexState) -> Result<PollResult> {
    if state.rollout_path.is_none() {
        if let Some(cli_session_id) = state.cli_session_id.clone() {
            state.rollout_path = find_codex_rollout_by_session_id(&state.home_dir, &cli_session_id);
        }
        if state.rollout_path.is_none() {
            if let Some(pid) = state.cli_pid {
                if let Some((path, cli_session_id)) = find_codex_rollout_by_pid(pid) {
                    state.rollout_path = Some(path);
                    state.cli_session_id = Some(cli_session_id);
                }
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
    let Some(path) = state.rollout_path.clone() else {
        return Ok(result);
    };
    if state.adopt_mode && !state.adopt_preamble_emitted {
        if !state.adopt_restored_from_metadata {
            result.adopt_preamble = baseline_codex_adopt_preamble(&path)?;
        }
        state.transcript_offset = file_size(&path);
        state.pending_tail.clear();
        state.adopt_preamble_emitted = true;
        return Ok(result);
    }
    let drain = drain_jsonl(&path, state.transcript_offset, &state.pending_tail)?;
    state.transcript_offset = drain.new_offset;
    state.pending_tail = drain.pending_tail;
    for line in &drain.lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("response_item") {
            continue;
        }
        let payload = value.get("payload").unwrap_or(&Value::Null);
        if payload.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        if let Some(role) = payload.get("role").and_then(Value::as_str) {
            match role {
                "user" if state.adopt_mode => {
                    let text = extract_codex_message_text(payload.get("content"));
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
                "assistant"
                    if payload.get("phase").and_then(Value::as_str) == Some("final_answer") =>
                {
                    let text = extract_codex_text(payload.get("content"), "output_text");
                    if !text.is_empty()
                        && state.emitted_final_text.as_deref() != Some(text.as_str())
                    {
                        let kind = state.active_turn.take().or_else(|| {
                            if state.adopt_mode {
                                Some(PendingTurnKind::LocalHeadless)
                            } else {
                                None
                            }
                        });
                        state.emitted_final_text = Some(text.clone());
                        result.final_output = Some(text);
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
                _ => {}
            }
        }
    }
    Ok(result)
}

fn latest_codex_session_for_beam_session(
    history_path: &Path,
    beam_session_id: &str,
) -> Option<String> {
    let raw = std::fs::read_to_string(history_path).ok()?;
    for line in raw.lines().rev() {
        if !line.contains(beam_session_id) {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(text) = value.get("text").and_then(Value::as_str) else {
            continue;
        };
        if !text.contains(beam_session_id) {
            continue;
        }
        if let Some(session_id) = value.get("session_id").and_then(Value::as_str) {
            return Some(session_id.to_string());
        }
    }
    None
}

fn codex_history_match(
    history_path: &Path,
    from_byte: u64,
    expected_text: &str,
) -> Result<Option<String>> {
    if !history_path.exists() {
        return Ok(None);
    }
    let size = file_size(history_path);
    if size <= from_byte {
        return Ok(None);
    }
    let mut file = File::open(history_path)?;
    file.seek(SeekFrom::Start(from_byte))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(actual) = value.get("text").and_then(Value::as_str) else {
            continue;
        };
        if normalize_history_text(actual) == normalize_history_text(expected_text) {
            return Ok(value
                .get("session_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned));
        }
    }
    Ok(None)
}

fn extract_codex_text(content: Option<&Value>, block_type: &str) -> String {
    let Some(items) = content.and_then(Value::as_array) else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|block| {
            if block.get("type").and_then(Value::as_str) == Some(block_type) {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("")
}

fn extract_codex_message_text(content: Option<&Value>) -> String {
    let Some(items) = content.and_then(Value::as_array) else {
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
        .join("")
}

fn baseline_codex_adopt_preamble(path: &Path) -> Result<Option<(String, String)>> {
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
        if value.get("type").and_then(Value::as_str) != Some("response_item") {
            continue;
        }
        let payload = value.get("payload").unwrap_or(&Value::Null);
        if payload.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        let Some(role) = payload.get("role").and_then(Value::as_str) else {
            continue;
        };
        match role {
            "user" => {
                let text = extract_codex_message_text(payload.get("content"));
                if !text.trim().is_empty() {
                    pending_user = Some(text);
                }
            }
            "assistant" if payload.get("phase").and_then(Value::as_str) == Some("final_answer") => {
                let text = extract_codex_text(payload.get("content"), "output_text");
                if !text.trim().is_empty() {
                    if let Some(user_text) = pending_user.take() {
                        latest_pair = Some((user_text, text));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(latest_pair)
}

fn find_codex_rollout_by_session_id(home_dir: &Path, cli_session_id: &str) -> Option<PathBuf> {
    let root = home_dir.join("sessions");
    if !root.exists() {
        return None;
    }
    let suffix = format!("-{}.jsonl", cli_session_id);
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file()
                && path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|name| name.ends_with(&suffix))
                    .unwrap_or(false)
            {
                return Some(path);
            }
        }
    }
    None
}

fn find_codex_rollout_by_pid(pid: u32) -> Option<(PathBuf, String)> {
    let fd_dir = PathBuf::from(format!("/proc/{}/fd", pid));
    let entries = read_dir(fd_dir).ok()?;
    for entry in entries.flatten() {
        let target = std::fs::read_link(entry.path()).ok()?;
        let target_str = target.to_string_lossy();
        if !target_str.ends_with(".jsonl") || !target_str.contains("/.codex/sessions/") {
            continue;
        }
        if let Some(session_id) = codex_session_id_from_rollout_path(&target_str) {
            return Some((target, session_id));
        }
    }
    None
}

fn codex_session_id_from_rollout_path(path: &str) -> Option<String> {
    let base = Path::new(path).file_name()?.to_str()?;
    if !base.starts_with("rollout-") || !base.ends_with(".jsonl") {
        return None;
    }
    let trimmed = base.strip_suffix(".jsonl")?;
    let tail = trimmed.rsplit_once('-')?.1;
    if is_uuid_like(tail) {
        return Some(tail.to_string());
    }
    if trimmed.len() >= 36 {
        let candidate = &trimmed[trimmed.len() - 36..];
        if is_uuid_like(candidate) {
            return Some(candidate.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::path::PathBuf;

    use beam_core::FinalOutputKind;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("beam-codex-{}-{}", name, uuid::Uuid::new_v4()))
    }

    #[test]
    fn rollout_path_extracts_session_id() {
        let path = "/tmp/rollout-20260603-019c6e27-e55b-73d1-87d8-4e01f1f75043.jsonl";
        assert_eq!(
            codex_session_id_from_rollout_path(path).as_deref(),
            Some("019c6e27-e55b-73d1-87d8-4e01f1f75043")
        );
    }

    #[test]
    fn history_match_finds_submitted_text() {
        let path = temp_path("history.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"text\":\"older\",\"session_id\":\"s0\"}\n",
                "{\"text\":\"hello\\nworld\",\"session_id\":\"s1\"}\n"
            ),
        )
        .unwrap();
        let found = codex_history_match(&path, 0, "hello\r\nworld").unwrap();
        assert_eq!(found.as_deref(), Some("s1"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn latest_session_uses_beam_session_marker() {
        let path = temp_path("latest-history.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"text\":\"no marker\",\"session_id\":\"s0\"}\n",
                "{\"text\":\"session beam-123 marker\",\"session_id\":\"s1\"}\n"
            ),
        )
        .unwrap();
        let found = latest_codex_session_for_beam_session(&path, "beam-123");
        assert_eq!(found.as_deref(), Some("s1"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn emits_final_output_from_rollout() {
        let path = temp_path("rollout.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"ignore\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = CodexState {
            home_dir: PathBuf::new(),
            history_path: PathBuf::new(),
            rollout_path: Some(path.clone()),
            cli_pid: None,
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            emitted_final_text: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let result = poll(&mut state).unwrap();
        assert_eq!(result.final_output.as_deref(), Some("done"));
        assert!(result.prompt_ready);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_preamble_once_and_absorbs_history() {
        let path = temp_path("codex-adopt-rollout.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = CodexState {
            home_dir: PathBuf::new(),
            history_path: PathBuf::new(),
            rollout_path: Some(path.clone()),
            cli_pid: None,
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
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
        let path = temp_path("codex-adopt-local-rollout.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"local ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"local answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = CodexState {
            home_dir: PathBuf::new(),
            history_path: PathBuf::new(),
            rollout_path: Some(path.clone()),
            cli_pid: None,
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let result = poll(&mut state).unwrap();
        assert_eq!(result.final_output.as_deref(), Some("local answer"));
        assert_eq!(result.final_output_kind, Some(FinalOutputKind::LocalTurn));
        assert_eq!(result.final_output_user_text.as_deref(), Some("local ask"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_keeps_remote_turn_as_bridge_output() {
        let path = temp_path("codex-adopt-remote-rollout.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"remote ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"remote answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = CodexState {
            home_dir: PathBuf::new(),
            history_path: PathBuf::new(),
            rollout_path: Some(path.clone()),
            cli_pid: None,
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            emitted_final_text: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::from([crate::adapter::normalize_history_text(
                "remote ask",
            )]),
            active_turn: None,
        };
        let result = poll(&mut state).unwrap();
        assert_eq!(result.final_output.as_deref(), Some("remote answer"));
        assert_eq!(result.final_output_kind, None);
        assert_eq!(result.final_output_user_text, None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_restored_absorbs_history_without_preamble() {
        let path = temp_path("codex-adopt-restored-rollout.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = CodexState {
            home_dir: PathBuf::new(),
            history_path: PathBuf::new(),
            rollout_path: Some(path.clone()),
            cli_pid: None,
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
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
    fn bridge_queue_final_answer_detection_across_turns() {
        let path = temp_path("codex-bridge-queue.jsonl");
        let mut state = CodexState {
            home_dir: PathBuf::new(),
            history_path: PathBuf::new(),
            rollout_path: Some(path.clone()),
            cli_pid: None,
            cli_session_id: Some("sid".to_string()),
            transcript_offset: 0,
            pending_tail: String::new(),
            emitted_final_text: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };

        let empty = poll(&mut state).unwrap();
        assert!(empty.final_output.is_none());

        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn1\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"thinking\"}]}}\n",
            ),
        )
        .unwrap();

        let partial = poll(&mut state).unwrap();
        assert!(partial.final_output.is_none());

        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn1\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"thinking\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"turn1 result\"}]}}\n",
            ),
        )
        .unwrap();

        let done = poll(&mut state).unwrap();
        assert_eq!(done.final_output.as_deref(), Some("turn1 result"));
        assert!(done.prompt_ready);

        state.emitted_final_text = Some("turn1 result".to_string());

        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn1\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"thinking\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"turn1 result\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn2\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"turn2 result\"}]}}\n",
            ),
        )
        .unwrap();

        let second_turn = poll(&mut state).unwrap();
        assert_eq!(second_turn.final_output.as_deref(), Some("turn2 result"));
        assert!(second_turn.prompt_ready);

        let _ = std::fs::remove_file(path);
    }
}
