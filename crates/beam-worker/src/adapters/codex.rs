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
    CodexState, PendingTurnKind, PollResult, ResolveOutcome, SpawnSpec, SubmitResult, drain_jsonl,
    file_size, is_uuid_like, normalize_history_text,
};
use crate::backend::SessionBackend;

pub fn create_state(init: &InitConfig) -> CodexState {
    let codex_home = PathBuf::from(
        std::env::var("CODEX_HOME")
            .unwrap_or_else(|_| format!("{}/.codex", std::env::var("HOME").unwrap_or_default())),
    );
    create_state_with_paths(init, codex_home.join("history.jsonl"), codex_home)
}

pub fn create_traex_state(init: &InitConfig) -> CodexState {
    let home = std::env::var("HOME").unwrap_or_default();
    let (history_path, home_dir) = traex_paths(Path::new(&home));
    create_state_with_paths(init, history_path, home_dir)
}

fn traex_paths(home: &Path) -> (PathBuf, PathBuf) {
    // Trae keeps its submit history and rollout transcripts in separate homes.
    (home.join(".trae/cli/history.json"), home.join(".traex/cli"))
}

fn create_state_with_paths(
    init: &InitConfig,
    history_path: PathBuf,
    home_dir: PathBuf,
) -> CodexState {
    CodexState {
        history_path,
        home_dir,
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
    let history_boundary = capture_history_boundary(&state.history_path)?;
    backend.paste_text(content).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.send_enter().await?;
    for _ in 0..4 {
        if let Some(cli_session_id) =
            codex_history_match(&state.history_path, &history_boundary, content)?
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
                if let ResolveOutcome::Found((path, cli_session_id)) =
                    find_codex_rollout_by_pid(pid, &state.home_dir)
                {
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
    let entries = read_history_entries(history_path).ok()?;
    for value in entries.iter().rev() {
        let Some(text) = history_text(value) else {
            continue;
        };
        if !text.contains(beam_session_id) {
            continue;
        }
        if let Some(session_id) = history_session_id(value) {
            return Some(session_id.to_string());
        }
    }
    None
}

#[derive(Debug, Clone)]
enum HistoryBoundary {
    Byte(u64),
    DocumentEntries(Vec<Value>),
}

fn capture_history_boundary(history_path: &Path) -> Result<HistoryBoundary> {
    let raw = match std::fs::read_to_string(history_path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(
                if history_path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    HistoryBoundary::DocumentEntries(Vec::new())
                } else {
                    HistoryBoundary::Byte(0)
                },
            );
        }
        Err(error) => return Err(error.into()),
    };
    Ok(parse_history_document(&raw)
        .map(HistoryBoundary::DocumentEntries)
        .unwrap_or_else(|| HistoryBoundary::Byte(raw.len() as u64)))
}

fn codex_history_match(
    history_path: &Path,
    boundary: &HistoryBoundary,
    expected_text: &str,
) -> Result<Option<String>> {
    if !history_path.exists() {
        return Ok(None);
    }
    let expected = normalize_history_text(expected_text);
    for value in read_recent_history_entries(history_path, boundary)?
        .into_iter()
        .rev()
    {
        let Some(actual) = history_text(&value) else {
            continue;
        };
        if normalize_history_text(actual) == expected {
            return Ok(history_session_id(&value).map(ToOwned::to_owned));
        }
    }
    Ok(None)
}

fn read_recent_history_entries(
    history_path: &Path,
    boundary: &HistoryBoundary,
) -> Result<Vec<Value>> {
    let raw = std::fs::read_to_string(history_path)?;
    if let Some(entries) = parse_history_document(&raw) {
        return match boundary {
            HistoryBoundary::DocumentEntries(previous) if entries.starts_with(previous) => {
                Ok(entries[previous.len()..].to_vec())
            }
            HistoryBoundary::DocumentEntries(_) => Ok(Vec::new()),
            // A JSON document that replaced JSONL has no trustworthy append boundary.
            HistoryBoundary::Byte(_) => Ok(Vec::new()),
        };
    }
    let HistoryBoundary::Byte(from_byte) = boundary else {
        return Ok(Vec::new());
    };
    let size = file_size(history_path);
    if size <= *from_byte {
        return Ok(Vec::new());
    }
    let mut file = File::open(history_path)?;
    file.seek(SeekFrom::Start(*from_byte))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(parse_history_jsonl(&text))
}

fn read_history_entries(history_path: &Path) -> Result<Vec<Value>> {
    let raw = std::fs::read_to_string(history_path)?;
    if let Some(entries) = parse_history_document(&raw) {
        return Ok(entries);
    }
    Ok(parse_history_jsonl(&raw))
}

fn parse_history_document(raw: &str) -> Option<Vec<Value>> {
    let value = serde_json::from_str::<Value>(raw).ok()?;
    match value {
        Value::Array(items) => Some(items),
        Value::Object(map) => {
            for key in ["history", "entries", "items", "data"] {
                if let Some(Value::Array(items)) = map.get(key) {
                    return Some(items.clone());
                }
            }
            if map.contains_key("text")
                || map.contains_key("session_id")
                || map.contains_key("sessionId")
                || map.contains_key("message")
            {
                return Some(vec![Value::Object(map)]);
            }
            None
        }
        _ => None,
    }
}

fn parse_history_jsonl(raw: &str) -> Vec<Value> {
    raw.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
}

fn history_text(value: &Value) -> Option<&str> {
    value
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| value.get("message").and_then(Value::as_str))
        .or_else(|| value.get("content").and_then(Value::as_str))
}

fn history_session_id(value: &Value) -> Option<&str> {
    value
        .get("session_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("sessionId").and_then(Value::as_str))
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

fn find_codex_rollout_by_pid(pid: u32, home_dir: &Path) -> ResolveOutcome<(PathBuf, String)> {
    find_codex_rollout_by_fd_dir(
        &PathBuf::from(format!("/proc/{}/fd", pid)),
        &home_dir.join("sessions"),
    )
}

fn find_codex_rollout_by_fd_dir(
    fd_dir: &Path,
    sessions_root: &Path,
) -> ResolveOutcome<(PathBuf, String)> {
    let entries = match read_dir(fd_dir) {
        Ok(e) => e,
        Err(e) => {
            return ResolveOutcome::NotFound {
                reason: format!("cannot read /proc fd dir: {}", e),
            };
        }
    };
    for entry in entries.flatten() {
        let target = match std::fs::read_link(entry.path()) {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !target
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "jsonl")
            .unwrap_or(false)
            || !target.starts_with(sessions_root)
        {
            continue;
        }
        let target_str = target.to_string_lossy();
        if let Some(session_id) = codex_session_id_from_rollout_path(&target_str) {
            return ResolveOutcome::Found((target, session_id));
        }
    }
    ResolveOutcome::NotFound {
        reason: "no codex rollout found in pid fd dir".to_string(),
    }
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
#[path = "codex_tests.rs"]
mod tests;
