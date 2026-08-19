use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    Adapter, PollResult, SpawnSpec, SubmitResult, TranscriptCursor, drain_jsonl, file_size,
    is_uuid_like, normalize_history_text, realpath_cwd,
};
use crate::backend::SessionBackend;
use crate::composer::{GROK_COMPOSER, confirm_typed_submit, sample_draft_fgs};

const UPDATES_FILE: &str = "updates.jsonl";

#[derive(Debug, Clone, Default)]
pub(crate) struct GrokState {
    grok_home: PathBuf,
    working_dir: String,
    transcript_path: Option<PathBuf>,
    cli_session_id: Option<String>,
    cursor: TranscriptCursor,
    turn_text: String,
}

fn grok_home_dir() -> PathBuf {
    std::env::var("GROK_HOME")
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".grok"))
}

fn assigned_cli_session_id(init: &InitConfig) -> Option<String> {
    if init.adopted_from.is_some() {
        return init.cli_session_id.clone();
    }
    if init.resume {
        return init
            .cli_session_id
            .clone()
            .or_else(|| init.resume_session_id.clone());
    }
    if is_uuid_like(&init.session_id) {
        return Some(init.session_id.clone());
    }
    init.cli_session_id.clone()
}

fn state_from_init(init: &InitConfig) -> GrokState {
    GrokState {
        grok_home: grok_home_dir(),
        working_dir: realpath_cwd(&init.working_dir),
        transcript_path: None,
        cli_session_id: assigned_cli_session_id(init),
        cursor: TranscriptCursor::new(),
        turn_text: String::new(),
    }
}

pub fn create(init: &InitConfig) -> Box<dyn Adapter> {
    Box::new(state_from_init(init))
}

#[async_trait]
impl Adapter for GrokState {
    fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
        let mut args = Vec::new();
        if let Some(model) = &init.model
            && !model.is_empty()
        {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        if init.resume {
            args.push("--resume".to_string());
            args.push(
                init.cli_session_id
                    .clone()
                    .or_else(|| init.resume_session_id.clone())
                    .unwrap_or_else(|| init.session_id.clone()),
            );
        } else if is_uuid_like(&init.session_id) {
            args.push("--session-id".to_string());
            args.push(init.session_id.clone());
        }
        args.extend(init.cli_args.clone());
        SpawnSpec {
            bin: init.cli_bin.clone(),
            args,
        }
    }

    async fn write_input(
        &mut self,
        backend: &dyn SessionBackend,
        content: &str,
    ) -> Result<SubmitResult> {
        let base_size = resolve_transcript_path(self)
            .as_ref()
            .map(|path| file_size(path.as_path()))
            .unwrap_or(0);

        let lines: Vec<&str> = content.split('\n').collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.is_empty() {
                backend.send_text(line).await?;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            if index < lines.len() - 1 {
                backend.send_special_keys(&["M-Enter".to_string()]).await?;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let draft_fgs = sample_draft_fgs(
            &backend.capture_viewport().await.unwrap_or_default(),
            GROK_COMPOSER,
        );
        backend.send_enter().await?;

        let confirmed = confirm_typed_submit(
            backend,
            GROK_COMPOSER,
            &draft_fgs,
            "enter",
            || {
                let Some(path) = resolve_transcript_path(self) else {
                    return Ok(false);
                };
                grok_submit_confirmed(&path, base_size, content)
            },
            || backend.send_enter(),
        )
        .await?;

        if confirmed {
            if let Some(path) = resolve_transcript_path(self) {
                update_state_for_path(self, &path);
            }
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: self.cli_session_id.clone(),
                ..Default::default()
            });
        }
        Ok(SubmitResult {
            submitted: false,
            cli_session_id: self.cli_session_id.clone(),
            failure_reason: Some("Grok did not accept the input".to_string()),
        })
    }

    fn poll(&mut self) -> Result<PollResult> {
        let Some(path) = resolve_transcript_path(self) else {
            return Ok(PollResult {
                cli_session_id: self.cli_session_id.clone(),
                ..Default::default()
            });
        };

        let lines = self.cursor.drain(&path)?;
        update_state_for_path(self, &path);

        let mut result = PollResult {
            cli_session_id: self.cli_session_id.clone(),
            ..Default::default()
        };

        let mut final_text: Option<String> = None;
        for line in &lines {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if let Some(session_id) = session_id_from_event(&value)
                && self
                    .cli_session_id
                    .as_deref()
                    .is_none_or(|known| known == session_id)
            {
                self.cli_session_id = Some(session_id);
                result.cli_session_id = self.cli_session_id.clone();
            }
            match update_kind(&value) {
                Some("user_message_chunk") => {
                    self.cursor.reset_dedupe();
                    self.turn_text.clear();
                }
                Some("agent_message_chunk") => {
                    if let Some(text) = chunk_text(&value) {
                        self.turn_text = text.to_string();
                    }
                }
                Some("turn_completed") => {
                    let reason = stop_reason(&value).unwrap_or_default();
                    if matches!(reason, "end_turn" | "stop") {
                        let text = self.turn_text.trim().to_string();
                        if !text.is_empty() {
                            final_text = Some(text);
                        }
                    }
                    self.turn_text.clear();
                }
                _ => {}
            }
        }

        if let Some(text) = final_text
            && let Some(emitted) = self.cursor.emit_if_new(&text)
        {
            result.final_output = Some(emitted);
            result.final_output_kind = Some(FinalOutputKind::Bridge);
            result.prompt_ready = true;
        }

        Ok(result)
    }
}

fn update_state_for_path(state: &mut GrokState, path: &Path) {
    state.transcript_path = Some(path.to_path_buf());
    if state.cli_session_id.is_none() {
        state.cli_session_id = grok_session_id_from_path(path);
    }
}

fn grok_session_id_from_path(path: &Path) -> Option<String> {
    path.parent()
        .and_then(|dir| dir.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| is_uuid_like(name))
        .map(ToOwned::to_owned)
}

fn path_matches_session(path: &Path, session_id: &str) -> bool {
    grok_session_id_from_path(path).as_deref() == Some(session_id)
}

fn resolve_transcript_path(state: &GrokState) -> Option<PathBuf> {
    if let Some(path) = &state.transcript_path
        && path.exists()
        && state
            .cli_session_id
            .as_deref()
            .is_none_or(|id| path_matches_session(path, id))
    {
        return Some(path.clone());
    }
    if let Some(session_id) = &state.cli_session_id {
        return transcript_for_session(&state.grok_home, &state.working_dir, session_id);
    }
    latest_grok_transcript(&state.grok_home, &state.working_dir)
}

fn transcript_for_session(
    grok_home: &Path,
    working_dir: &str,
    session_id: &str,
) -> Option<PathBuf> {
    for group in grok_session_groups(grok_home, working_dir) {
        let path = group.join(session_id).join(UPDATES_FILE);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

fn latest_grok_transcript(grok_home: &Path, working_dir: &str) -> Option<PathBuf> {
    let mut best: Option<(PathBuf, SystemTime, u64)> = None;
    for group in grok_session_groups(grok_home, working_dir) {
        let Ok(sessions) = std::fs::read_dir(&group) else {
            continue;
        };
        for session in sessions.flatten() {
            let path = session.path();
            if !path.is_dir() {
                continue;
            }
            let updates = path.join(UPDATES_FILE);
            let Ok(meta) = updates.metadata() else {
                continue;
            };
            let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let size = meta.len();
            let replace = match &best {
                None => true,
                Some((_, current_modified, current_size)) => {
                    modified > *current_modified
                        || (modified == *current_modified && size > *current_size)
                }
            };
            if replace {
                best = Some((updates, modified, size));
            }
        }
    }
    best.map(|(path, _, _)| path)
}

fn grok_session_groups(grok_home: &Path, working_dir: &str) -> Vec<PathBuf> {
    let sessions_root = grok_home.join("sessions");
    let encoded = encode_cwd(working_dir);
    let mut groups = Vec::new();
    let primary = sessions_root.join(&encoded);
    if primary.is_dir() {
        groups.push(primary);
    }
    let Ok(entries) = std::fs::read_dir(&sessions_root) else {
        return groups;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if groups.iter().any(|existing| existing == &path) {
            continue;
        }
        if group_matches_cwd(&path, working_dir) {
            groups.push(path);
        }
    }
    groups
}

fn group_matches_cwd(group: &Path, working_dir: &str) -> bool {
    let cwd_file = group.join(".cwd");
    if let Ok(text) = std::fs::read_to_string(&cwd_file) {
        let recorded = text.trim();
        if work_dir_matches(recorded, working_dir) {
            return true;
        }
    }
    group
        .file_name()
        .and_then(|name| name.to_str())
        .map(decode_cwd)
        .is_some_and(|decoded| work_dir_matches(&decoded, working_dir))
}

fn work_dir_matches(candidate: &str, working_dir: &str) -> bool {
    candidate == working_dir || realpath_cwd(candidate) == working_dir
}

fn encode_cwd(cwd: &str) -> String {
    let mut out = String::new();
    for byte in cwd.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn decode_cwd(encoded: &str) -> String {
    let bytes = encoded.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Ok(value) = u8::from_str_radix(
                std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or(""),
                16,
            )
        {
            out.push(value);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn update_kind(value: &Value) -> Option<&str> {
    value
        .pointer("/params/update/sessionUpdate")
        .and_then(Value::as_str)
}

fn chunk_text(value: &Value) -> Option<&str> {
    value
        .pointer("/params/update/content/text")
        .and_then(Value::as_str)
}

fn stop_reason(value: &Value) -> Option<&str> {
    value
        .pointer("/params/update/stop_reason")
        .and_then(Value::as_str)
}

fn session_id_from_event(value: &Value) -> Option<String> {
    value
        .pointer("/params/sessionId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
}

fn grok_submit_confirmed(path: &Path, from_byte: u64, expected_text: &str) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let size = file_size(path);
    if size <= from_byte {
        return Ok(false);
    }
    let drain = drain_jsonl(path, from_byte, "")?;
    let expected = normalize_history_text(expected_text);
    for line in &drain.lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if update_kind(&value) != Some("user_message_chunk") {
            continue;
        }
        let Some(text) = chunk_text(&value) else {
            continue;
        };
        if user_text_matches(text, &expected) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn user_text_matches(recorded: &str, expected: &str) -> bool {
    let recorded = normalize_history_text(recorded);
    if recorded == expected {
        return true;
    }
    if let Some(inner) = extract_user_message(&recorded)
        && normalize_history_text(&inner) == expected
    {
        return true;
    }
    false
}

fn extract_user_message(text: &str) -> Option<String> {
    let start = text.find("<user_message>")? + "<user_message>".len();
    let rest = text.get(start..)?;
    let end = rest.find("</user_message>")?;
    Some(rest[..end].trim().to_string())
}

#[cfg(test)]
#[path = "grok_tests.rs"]
mod tests;
