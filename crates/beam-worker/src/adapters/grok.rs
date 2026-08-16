use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    Adapter, PollResult, SpawnSpec, SubmitResult, TranscriptCursor, confirm_submit_loop,
    drain_jsonl, file_size, is_uuid_like, normalize_history_text, realpath_cwd,
};
use crate::backend::SessionBackend;

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

fn state_from_init(init: &InitConfig) -> GrokState {
    let assigned = if !init.resume && is_uuid_like(&init.session_id) {
        Some(init.session_id.clone())
    } else {
        None
    };
    GrokState {
        grok_home: grok_home_dir(),
        working_dir: realpath_cwd(&init.working_dir),
        transcript_path: None,
        cli_session_id: init.cli_session_id.clone().or(assigned),
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
        if !init.disable_cli_bypass {
            args.push("--always-approve".to_string());
        }
        args.push("--no-alt-screen".to_string());
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
        backend.send_enter().await?;

        let confirmed = confirm_submit_loop(backend, || {
            let Some(path) = resolve_transcript_path(self) else {
                return Ok(false);
            };
            grok_submit_confirmed(&path, base_size, content)
        })
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
            failure_reason: Some("Grok transcript did not confirm submit".to_string()),
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
            if let Some(session_id) = session_id_from_event(&value) {
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
                        if !self.turn_text.is_empty() {
                            self.turn_text.push_str("\n\n");
                        }
                        self.turn_text.push_str(text);
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

fn resolve_transcript_path(state: &GrokState) -> Option<PathBuf> {
    if let Some(path) = &state.transcript_path
        && path.exists()
    {
        return Some(path.clone());
    }
    if let Some(session_id) = &state.cli_session_id
        && let Some(path) = transcript_for_session(&state.grok_home, &state.working_dir, session_id)
    {
        return Some(path);
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
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use crate::adapter::test_support::{home_test_lock, set_home, temp_home, test_init};
    use async_trait::async_trait;
    use std::fs::{self, create_dir_all};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    const SAMPLE_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    fn init_for(working_dir: &str) -> InitConfig {
        InitConfig {
            session_id: SAMPLE_SESSION.to_string(),
            working_dir: working_dir.to_string(),
            cli_bin: "grok".to_string(),
            ..test_init("grok")
        }
    }

    fn session_dir(home: &Path, working_dir: &str, session_id: &str) -> PathBuf {
        home.join(".grok")
            .join("sessions")
            .join(encode_cwd(working_dir))
            .join(session_id)
    }

    fn updates_path(home: &Path, working_dir: &str, session_id: &str) -> PathBuf {
        session_dir(home, working_dir, session_id).join(UPDATES_FILE)
    }

    fn append_updates(path: &Path, lines: &[String]) {
        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }
        let mut content = fs::read_to_string(path).unwrap_or_default();
        for line in lines {
            content.push_str(line);
            content.push('\n');
        }
        fs::write(path, content).unwrap();
    }

    fn event_line(session_id: &str, update: Value) -> String {
        serde_json::json!({
            "timestamp": 1,
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": update,
            }
        })
        .to_string()
    }

    fn user_chunk(session_id: &str, text: &str) -> String {
        event_line(
            session_id,
            serde_json::json!({
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text": text},
            }),
        )
    }

    fn agent_chunk(session_id: &str, text: &str) -> String {
        event_line(
            session_id,
            serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": text},
            }),
        )
    }

    fn turn_completed(session_id: &str, reason: &str) -> String {
        serde_json::json!({
            "timestamp": 1,
            "method": "_x.ai/session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "turn_completed",
                    "stop_reason": reason,
                }
            }
        })
        .to_string()
    }

    #[derive(Clone, Default)]
    struct RecordingBackend {
        updates_path: PathBuf,
        buffer: Arc<Mutex<String>>,
        append_on_enter: bool,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingBackend {
        fn new(updates_path: PathBuf, append_on_enter: bool) -> Self {
            Self {
                updates_path,
                buffer: Arc::new(Mutex::new(String::new())),
                append_on_enter,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SessionBackend for RecordingBackend {
        async fn spawn(
            &self,
            _bin: &str,
            _args: &[String],
            _opts: crate::backend::SpawnOpts,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_text(&self, text: &str) -> Result<()> {
            self.calls.lock().unwrap().push(format!("text:{text}"));
            self.buffer.lock().unwrap().push_str(text);
            Ok(())
        }

        async fn send_enter(&self) -> Result<()> {
            self.calls.lock().unwrap().push("enter".to_string());
            if self.append_on_enter {
                let content = {
                    let mut buffer = self.buffer.lock().unwrap();
                    let content = buffer.clone();
                    buffer.clear();
                    content
                };
                if !content.is_empty() {
                    append_updates(&self.updates_path, &[user_chunk(SAMPLE_SESSION, &content)]);
                }
            }
            Ok(())
        }

        async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("keys:{}", keys.join(",")));
            if keys.iter().any(|key| key == "M-Enter") {
                self.buffer.lock().unwrap().push('\n');
            }
            Ok(())
        }

        async fn paste_text(&self, text: &str) -> Result<()> {
            self.send_text(text).await
        }
        async fn write_raw(&self, _text: &str) -> Result<()> {
            Ok(())
        }
        async fn raw_input(&self, _text: &str) -> Result<()> {
            Ok(())
        }
        async fn capture_viewport(&self) -> Result<String> {
            Ok(String::new())
        }
        async fn capture_current_screen(&self) -> Result<String> {
            Ok(String::new())
        }
        async fn is_alive(&self) -> Result<bool> {
            Ok(true)
        }
        async fn child_pid(&self) -> Result<Option<u32>> {
            Ok(None)
        }
        async fn kill(&self) -> Result<()> {
            Ok(())
        }
        async fn destroy_session(&self) -> Result<()> {
            Ok(())
        }
        async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
            Ok(None)
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            drop(tx);
            rx
        }
    }

    #[test]
    fn encode_cwd_matches_grok_session_group_names() {
        assert_eq!(encode_cwd("/home/huangyu"), "%2Fhome%2Fhuangyu");
        assert_eq!(decode_cwd("%2Fhome%2Fhuangyu"), "/home/huangyu");
        assert_eq!(encode_cwd("/tmp/beam grok"), "%2Ftmp%2Fbeam%20grok");
    }

    #[test]
    fn build_spawn_spec_defaults_to_always_approve_and_session_id() {
        let init = init_for("/tmp");
        let spec = GrokState::default().build_spawn_spec(&init);
        assert_eq!(spec.bin, "grok");
        assert!(spec.args.iter().any(|arg| arg == "--always-approve"));
        assert!(spec.args.iter().any(|arg| arg == "--no-alt-screen"));
        let session_pos = spec
            .args
            .iter()
            .position(|arg| arg == "--session-id")
            .unwrap();
        assert_eq!(spec.args[session_pos + 1], SAMPLE_SESSION);
        assert!(!spec.args.iter().any(|arg| arg == "--resume"));
    }

    #[test]
    fn build_spawn_spec_respects_disable_bypass_model_and_resume() {
        let init = InitConfig {
            resume: true,
            cli_session_id: Some("01aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee".to_string()),
            model: Some("grok-4".to_string()),
            disable_cli_bypass: true,
            ..init_for("/tmp")
        };
        let spec = GrokState::default().build_spawn_spec(&init);
        assert!(!spec.args.iter().any(|arg| arg == "--always-approve"));
        assert!(spec.args.iter().any(|arg| arg == "--no-alt-screen"));
        assert!(!spec.args.iter().any(|arg| arg == "--session-id"));
        let resume_pos = spec.args.iter().position(|arg| arg == "--resume").unwrap();
        assert_eq!(
            spec.args[resume_pos + 1],
            "01aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee"
        );
        let model_pos = spec.args.iter().position(|arg| arg == "--model").unwrap();
        assert_eq!(spec.args[model_pos + 1], "grok-4");
    }

    #[test]
    fn poll_emits_final_text_after_end_turn_and_dedupes() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-poll");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
        append_updates(
            &updates,
            &[
                user_chunk(SAMPLE_SESSION, "hello grok"),
                agent_chunk(SAMPLE_SESSION, "Grok final reply"),
                turn_completed(SAMPLE_SESSION, "end_turn"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));

        let first = state.poll().expect("poll");
        assert_eq!(first.final_output.as_deref(), Some("Grok final reply"));
        assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
        assert!(first.prompt_ready);
        assert_eq!(first.cli_session_id.as_deref(), Some(SAMPLE_SESSION));

        let second = state.poll().expect("poll again");
        assert!(second.final_output.is_none());
        assert!(!second.prompt_ready);
    }

    #[test]
    fn poll_ignores_intermediate_agent_text_until_turn_completes() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-midturn");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
        append_updates(
            &updates,
            &[
                user_chunk(SAMPLE_SESSION, "question"),
                agent_chunk(SAMPLE_SESSION, "working on it"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));

        let result = state.poll().expect("poll");
        assert!(result.final_output.is_none());
        assert!(!result.prompt_ready);
    }

    #[test]
    fn poll_joins_agent_chunks_and_skips_non_end_turn() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-join");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
        append_updates(
            &updates,
            &[
                agent_chunk(SAMPLE_SESSION, "first"),
                agent_chunk(SAMPLE_SESSION, "second"),
                turn_completed(SAMPLE_SESSION, "max_tokens"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));
        let skipped = state.poll().expect("poll skipped");
        assert!(skipped.final_output.is_none());

        append_updates(
            &updates,
            &[
                agent_chunk(SAMPLE_SESSION, "first"),
                agent_chunk(SAMPLE_SESSION, "second"),
                turn_completed(SAMPLE_SESSION, "end_turn"),
            ],
        );
        let emitted = state.poll().expect("poll joined");
        assert_eq!(emitted.final_output.as_deref(), Some("first\n\nsecond"));
    }

    #[test]
    fn poll_recovers_after_truncation_and_re_emits() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-truncate");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
        append_updates(
            &updates,
            &[
                user_chunk(SAMPLE_SESSION, "question"),
                agent_chunk(SAMPLE_SESSION, "first reply"),
                turn_completed(SAMPLE_SESSION, "end_turn"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));
        let first = state.poll().expect("poll");
        assert_eq!(first.final_output.as_deref(), Some("first reply"));

        fs::write(&updates, "").unwrap();
        append_updates(
            &updates,
            &[
                agent_chunk(SAMPLE_SESSION, "first reply"),
                turn_completed(SAMPLE_SESSION, "end_turn"),
            ],
        );
        let second = state.poll().expect("poll after truncation");
        assert_eq!(second.final_output.as_deref(), Some("first reply"));
    }

    #[test]
    fn poll_picks_latest_session_matching_work_dir() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-resolve");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let other_dir = "/tmp/beam-grok-other";
        let old_id = "11111111-1111-1111-1111-111111111111";
        let other_id = "22222222-2222-2222-2222-222222222222";
        let old = updates_path(&home, working_dir, old_id);
        append_updates(
            &old,
            &[
                agent_chunk(old_id, "old"),
                turn_completed(old_id, "end_turn"),
            ],
        );
        let other = updates_path(&home, other_dir, other_id);
        append_updates(
            &other,
            &[
                agent_chunk(other_id, "foreign"),
                turn_completed(other_id, "end_turn"),
            ],
        );
        let mut state = state_from_init(&InitConfig {
            session_id: "session-test".to_string(),
            cli_session_id: None,
            ..init_for(working_dir)
        });

        let result = state.poll().expect("poll");
        assert_eq!(result.final_output.as_deref(), Some("old"));
        assert_eq!(result.cli_session_id.as_deref(), Some(old_id));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_confirms_submit_when_prompt_recorded() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-submit");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
        append_updates(&updates, &[]);
        let mut state = state_from_init(&init_for(working_dir));
        let backend = RecordingBackend::new(updates.clone(), true);

        let result = state
            .write_input(&backend, "hello grok")
            .await
            .expect("write input");
        assert!(result.submitted);
        assert_eq!(result.failure_reason, None);
        assert_eq!(result.cli_session_id.as_deref(), Some(SAMPLE_SESSION));
        assert!(backend.calls().iter().any(|call| call == "enter"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_fails_when_transcript_does_not_confirm() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-grok-nosubmit");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-grok-work";
        let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
        append_updates(&updates, &[]);
        let mut state = state_from_init(&init_for(working_dir));
        let backend = RecordingBackend::new(updates.clone(), false);

        let result = state
            .write_input(&backend, "hello grok")
            .await
            .expect("write input");
        assert!(!result.submitted);
        assert!(
            result
                .failure_reason
                .as_deref()
                .unwrap_or("")
                .contains("did not confirm")
        );
    }

    #[test]
    fn user_text_matches_plain_and_wrapped_prompts() {
        assert!(user_text_matches("hello grok", "hello grok"));
        assert!(user_text_matches(
            "<user_message>\nhello grok\n</user_message>\n\n<session_id>x</session_id>",
            "hello grok"
        ));
        assert!(!user_text_matches("hello grok", "other"));
    }

    fn has_command(name: &str) -> bool {
        std::process::Command::new("which")
            .arg(name)
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    struct LiveGuard {
        zellij_session: String,
        working_dir: PathBuf,
        grok_session_root: Option<PathBuf>,
    }

    impl Drop for LiveGuard {
        fn drop(&mut self) {
            let _ = std::process::Command::new("zellij")
                .args(["delete-session", &self.zellij_session, "-f"])
                .output();
            let _ = fs::remove_dir_all(&self.working_dir);
            if let Some(root) = &self.grok_session_root {
                let _ = fs::remove_dir_all(root);
            }
        }
    }

    // cargo test -p beam-worker live_grok -- --ignored --nocapture
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "live test: requires locally installed and authenticated `grok` and `zellij`"]
    async fn live_grok_submit_and_poll_final_output() {
        if !has_command("grok") || !has_command("zellij") {
            eprintln!("skipping live test: `grok` or `zellij` not found in PATH");
            return;
        }

        let short = &Uuid::new_v4().to_string()[..8];
        let working_dir = std::env::temp_dir().join(format!("beam-grok-live-{short}"));
        create_dir_all(&working_dir).expect("create live working dir");
        let zellij_session = format!("beam-grok-live-{short}");
        let mut guard = LiveGuard {
            zellij_session: zellij_session.clone(),
            working_dir: working_dir.clone(),
            grok_session_root: None,
        };

        let session_id = Uuid::new_v4().to_string();
        let init = InitConfig {
            session_id: session_id.clone(),
            working_dir: working_dir.display().to_string(),
            prompt: String::new(),
            ..init_for(&working_dir.display().to_string())
        };
        let mut state = state_from_init(&init);
        let spec = state.build_spawn_spec(&init);
        assert!(spec.args.iter().any(|arg| arg == "--always-approve"));

        let backend = crate::backend::ZellijBackend::new(zellij_session);
        backend
            .spawn(
                &spec.bin,
                &spec.args,
                crate::backend::SpawnOpts {
                    cwd: init.working_dir.clone(),
                    cols: 120,
                    rows: 40,
                    env: vec![],
                },
            )
            .await
            .expect("spawn grok through zellij backend");

        let mut ready = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let viewport = backend.capture_viewport().await.unwrap_or_default();
            if viewport.to_ascii_lowercase().contains("always-approve") {
                ready = true;
                break;
            }
        }
        assert!(ready, "grok TUI did not show always-approve within 60s");

        let submit = state
            .write_input(&backend, "reply with exactly: BEAM_GROK_OK")
            .await
            .expect("write input to live grok");
        assert!(
            submit.submitted,
            "live submit was not confirmed: {:?}",
            submit.failure_reason
        );
        assert!(submit.cli_session_id.is_some());

        let mut final_output = None;
        for _ in 0..90 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let result = state.poll().expect("poll live grok");
            if result.final_output.is_some() {
                final_output = result.final_output;
                break;
            }
        }
        let final_output = final_output.expect("grok did not produce a final output within 90s");
        assert!(
            final_output.contains("BEAM_GROK_OK"),
            "unexpected final output: {final_output}"
        );

        if let Some(path) = &state.transcript_path {
            guard.grok_session_root = path.parent().and_then(Path::parent).map(Path::to_path_buf);
        }
    }
}
