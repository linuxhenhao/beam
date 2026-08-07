use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    Adapter, PollResult, SpawnSpec, SubmitResult, TranscriptCursor, confirm_submit_loop,
    drain_jsonl, file_size, normalize_history_text, realpath_cwd,
};
use crate::backend::SessionBackend;

#[derive(Debug, Clone, Default)]
pub(crate) struct KimiState {
    data_dir: PathBuf,
    working_dir: String,
    transcript_path: Option<PathBuf>,
    cli_session_id: Option<String>,
    cursor: TranscriptCursor,
}

fn state_from_init(init: &InitConfig) -> KimiState {
    let home = std::env::var("HOME").unwrap_or_default();
    KimiState {
        data_dir: PathBuf::from(home).join(".kimi-code"),
        working_dir: realpath_cwd(&init.working_dir),
        transcript_path: None,
        cli_session_id: init.cli_session_id.clone(),
        cursor: TranscriptCursor::new(),
    }
}

pub fn create(init: &InitConfig) -> Box<dyn Adapter> {
    Box::new(state_from_init(init))
}

#[async_trait]
impl Adapter for KimiState {
    fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
        let mut args = Vec::new();
        if init.resume {
            args.push("--session".to_string());
            args.push(
                init.cli_session_id
                    .clone()
                    .or_else(|| init.resume_session_id.clone())
                    .unwrap_or_else(|| init.session_id.clone()),
            );
        }
        if !init.disable_cli_bypass {
            args.push("--yolo".to_string());
        }
        if let Some(model) = &init.model {
            if !model.is_empty() {
                args.push("--model".to_string());
                args.push(model.clone());
            }
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

        backend.send_text(content).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        backend.send_enter().await?;

        let confirmed = confirm_submit_loop(backend, || {
            let Some(path) = resolve_transcript_path(self) else {
                return Ok(false);
            };
            kimi_submit_confirmed(&path, base_size, content)
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
            failure_reason: Some("Kimi transcript did not confirm submit".to_string()),
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

        let mut step_text = String::new();
        let mut final_text: Option<String> = None;
        for line in &lines {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            match value.get("type").and_then(Value::as_str) {
                Some("turn.prompt") => {
                    // A new user turn starts; allow an identical reply to be emitted again.
                    self.cursor.reset_dedupe();
                }
                Some("context.append_loop_event") => {
                    let Some(event) = value.get("event") else {
                        continue;
                    };
                    match event.get("type").and_then(Value::as_str) {
                        Some("step.begin") => step_text.clear(),
                        Some("content.part") => {
                            if let Some(text) = event
                                .get("part")
                                .filter(|part| {
                                    part.get("type").and_then(Value::as_str) == Some("text")
                                })
                                .and_then(|part| part.get("text"))
                                .and_then(Value::as_str)
                            {
                                step_text.push_str(text);
                            }
                        }
                        Some("step.end") => {
                            let finish_reason = event
                                .get("finishReason")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if matches!(finish_reason, "end_turn" | "stop") {
                                let text = step_text.trim().to_string();
                                if !text.is_empty() {
                                    final_text = Some(text);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if let Some(text) = final_text {
            if let Some(emitted) = self.cursor.emit_if_new(&text) {
                result.final_output = Some(emitted);
                result.final_output_kind = Some(FinalOutputKind::Bridge);
                result.prompt_ready = true;
            }
        }

        Ok(result)
    }
}

fn update_state_for_path(state: &mut KimiState, path: &Path) {
    state.transcript_path = Some(path.to_path_buf());
    if state.cli_session_id.is_none() {
        state.cli_session_id = kimi_session_id_from_path(path);
    }
}

fn kimi_session_id_from_path(path: &Path) -> Option<String> {
    // <sessionDir>/agents/main/wire.jsonl -> session_<uuid>
    path.parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .and_then(|dir| dir.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| name.starts_with("session_"))
        .map(ToOwned::to_owned)
}

fn resolve_transcript_path(state: &KimiState) -> Option<PathBuf> {
    if let Some(path) = &state.transcript_path
        && path.exists()
    {
        return Some(path.clone());
    }
    latest_kimi_transcript(&state.data_dir, &state.working_dir)
}

fn latest_kimi_transcript(data_dir: &Path, working_dir: &str) -> Option<PathBuf> {
    let mut best: Option<(PathBuf, SystemTime, u64)> = None;
    for (session_dir, session_work_dir) in kimi_session_dirs(data_dir) {
        if !work_dir_matches(&session_work_dir, working_dir) {
            continue;
        }
        let wire = session_dir
            .join("agents")
            .join("main")
            .join("wire.jsonl");
        let Ok(meta) = wire.metadata() else {
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
            best = Some((wire, modified, size));
        }
    }
    best.map(|(path, _, _)| path)
}

fn work_dir_matches(candidate: &str, working_dir: &str) -> bool {
    candidate == working_dir || realpath_cwd(candidate) == working_dir
}

/// List `(sessionDir, workDir)` pairs known to kimi-code.
///
/// Primary source is `<data_dir>/session_index.jsonl`; when it is missing or
/// empty, fall back to scanning `sessions/*/[session_*/]state.json`.
fn kimi_session_dirs(data_dir: &Path) -> Vec<(PathBuf, String)> {
    let index_path = data_dir.join("session_index.jsonl");
    let mut out = Vec::new();
    if let Ok(text) = std::fs::read_to_string(&index_path) {
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Some(session_dir) = value.get("sessionDir").and_then(Value::as_str) else {
                continue;
            };
            let work_dir = value
                .get("workDir")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            out.push((PathBuf::from(session_dir), work_dir));
        }
    }
    if !out.is_empty() {
        return out;
    }

    let sessions_root = data_dir.join("sessions");
    let Ok(workspaces) = std::fs::read_dir(&sessions_root) else {
        return out;
    };
    for workspace in workspaces.flatten() {
        let Ok(sessions) = std::fs::read_dir(workspace.path()) else {
            continue;
        };
        for session in sessions.flatten() {
            let state_path = session.path().join("state.json");
            let Ok(text) = std::fs::read_to_string(&state_path) else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let work_dir = value
                .get("workDir")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            out.push((session.path(), work_dir));
        }
    }
    out
}

fn kimi_submit_confirmed(path: &Path, from_byte: u64, expected_text: &str) -> Result<bool> {
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
        if value.get("type").and_then(Value::as_str) != Some("turn.prompt") {
            continue;
        }
        let Some(input) = value.get("input").and_then(Value::as_array) else {
            continue;
        };
        let text = input
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        if normalize_history_text(&text) == expected {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::test_support::{home_test_lock, set_home, temp_home, test_init};
    use async_trait::async_trait;
    use std::fs::{self, create_dir_all};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn init_for(working_dir: &str) -> InitConfig {
        InitConfig {
            working_dir: working_dir.to_string(),
            ..test_init("kimi")
        }
    }

    fn session_dir(home: &Path, session_id: &str) -> PathBuf {
        home.join(".kimi-code")
            .join("sessions")
            .join("wd_test_deadbeefcafe")
            .join(session_id)
    }

    fn wire_path(home: &Path, session_id: &str) -> PathBuf {
        session_dir(home, session_id)
            .join("agents")
            .join("main")
            .join("wire.jsonl")
    }

    fn write_session_index(home: &Path, session_id: &str, work_dir: &str) {
        let session_dir = session_dir(home, session_id);
        create_dir_all(&session_dir).unwrap();
        let index = home.join(".kimi-code").join("session_index.jsonl");
        let line = serde_json::json!({
            "sessionId": session_id,
            "sessionDir": session_dir.display().to_string(),
            "workDir": work_dir,
        });
        let mut content = fs::read_to_string(&index).unwrap_or_default();
        content.push_str(&format!("{}\n", line));
        fs::write(index, content).unwrap();
    }

    fn append_wire(path: &Path, lines: &[String]) {
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

    fn turn_prompt_line(text: &str) -> String {
        serde_json::json!({
            "type": "turn.prompt",
            "input": [{"type": "text", "text": text}],
            "origin": {"kind": "user"},
            "time": 1,
        })
        .to_string()
    }

    fn step_begin_line() -> String {
        serde_json::json!({
            "type": "context.append_loop_event",
            "event": {"type": "step.begin", "uuid": "u", "turnId": "0", "step": 1},
            "time": 2,
        })
        .to_string()
    }

    fn text_part_line(text: &str) -> String {
        serde_json::json!({
            "type": "context.append_loop_event",
            "event": {
                "type": "content.part",
                "uuid": "u2",
                "turnId": "0",
                "step": 1,
                "part": {"type": "text", "text": text},
            },
            "time": 3,
        })
        .to_string()
    }

    fn step_end_line(finish_reason: &str) -> String {
        serde_json::json!({
            "type": "context.append_loop_event",
            "event": {
                "type": "step.end",
                "uuid": "u",
                "turnId": "0",
                "step": 1,
                "finishReason": finish_reason,
            },
            "time": 4,
        })
        .to_string()
    }

    #[derive(Clone, Default)]
    struct RecordingBackend {
        wire_path: PathBuf,
        buffer: Arc<Mutex<String>>,
        append_on_enter: bool,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingBackend {
        fn new(wire_path: PathBuf, append_on_enter: bool) -> Self {
            Self {
                wire_path,
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
                    append_wire(&self.wire_path, &[turn_prompt_line(&content)]);
                }
            }
            Ok(())
        }

        async fn send_special_keys(&self, _keys: &[String]) -> Result<()> {
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
    fn build_spawn_spec_defaults_to_yolo() {
        let init = init_for("/tmp");
        let spec = KimiState::default().build_spawn_spec(&init);
        assert_eq!(spec.bin, "kimi");
        assert!(spec.args.iter().any(|arg| arg == "--yolo"));
        assert!(!spec.args.iter().any(|arg| arg == "--session"));
    }

    #[test]
    fn build_spawn_spec_respects_disable_bypass_model_and_resume() {
        let init = InitConfig {
            resume: true,
            cli_session_id: Some("session_abc".to_string()),
            model: Some("kimi-code/k3".to_string()),
            disable_cli_bypass: true,
            ..init_for("/tmp")
        };
        let spec = KimiState::default().build_spawn_spec(&init);
        assert!(!spec.args.iter().any(|arg| arg == "--yolo"));
        let session_pos = spec.args.iter().position(|arg| arg == "--session").unwrap();
        assert_eq!(spec.args[session_pos + 1], "session_abc");
        let model_pos = spec.args.iter().position(|arg| arg == "--model").unwrap();
        assert_eq!(spec.args[model_pos + 1], "kimi-code/k3");
    }

    #[test]
    fn poll_emits_final_text_after_end_turn_and_dedupes() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-kimi-poll");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-kimi-work";
        write_session_index(&home, "session_aaa", working_dir);
        let wire = wire_path(&home, "session_aaa");
        append_wire(
            &wire,
            &[
                turn_prompt_line("hello kimi"),
                step_begin_line(),
                text_part_line("Kimi final reply"),
                step_end_line("end_turn"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));

        let first = state.poll().expect("poll");
        assert_eq!(first.final_output.as_deref(), Some("Kimi final reply"));
        assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
        assert!(first.prompt_ready);
        assert_eq!(first.cli_session_id.as_deref(), Some("session_aaa"));

        let second = state.poll().expect("poll again");
        assert!(second.final_output.is_none());
        assert!(!second.prompt_ready);
    }

    #[test]
    fn poll_ignores_intermediate_tool_use_text() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-kimi-tooluse");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-kimi-work";
        write_session_index(&home, "session_bbb", working_dir);
        let wire = wire_path(&home, "session_bbb");
        append_wire(
            &wire,
            &[
                step_begin_line(),
                text_part_line("progress note"),
                step_end_line("tool_use"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));

        let result = state.poll().expect("poll");
        assert!(result.final_output.is_none());
        assert!(!result.prompt_ready);
    }

    #[test]
    fn poll_recovers_after_truncation_and_re_emits() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-kimi-truncate");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-kimi-work";
        write_session_index(&home, "session_ccc", working_dir);
        let wire = wire_path(&home, "session_ccc");
        append_wire(
            &wire,
            &[
                turn_prompt_line("question"),
                step_begin_line(),
                text_part_line("first reply"),
                step_end_line("end_turn"),
            ],
        );
        let mut state = state_from_init(&init_for(working_dir));
        let first = state.poll().expect("poll");
        assert_eq!(first.final_output.as_deref(), Some("first reply"));

        // Simulate transcript truncation (shorter than before): the same reply
        // text must be emitted again.
        fs::write(&wire, "").unwrap();
        append_wire(
            &wire,
            &[
                step_begin_line(),
                text_part_line("first reply"),
                step_end_line("end_turn"),
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
        let home = temp_home("beam-kimi-resolve");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-kimi-work";
        write_session_index(&home, "session_old", working_dir);
        let old_wire = wire_path(&home, "session_old");
        append_wire(
            &old_wire,
            &[step_begin_line(), text_part_line("old"), step_end_line("end_turn")],
        );
        // A session from another working directory must be ignored even if newer.
        let other_wire = wire_path(&home, "session_other");
        append_wire(&other_wire, &[text_part_line("foreign")]);
        write_session_index(&home, "session_other", "/tmp/beam-kimi-other");
        let mut state = state_from_init(&init_for(working_dir));

        let result = state.poll().expect("poll");
        assert_eq!(result.final_output.as_deref(), Some("old"));
        assert_eq!(result.cli_session_id.as_deref(), Some("session_old"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_confirms_submit_when_prompt_recorded() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-kimi-submit");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-kimi-work";
        write_session_index(&home, "session_ddd", working_dir);
        let wire = wire_path(&home, "session_ddd");
        append_wire(&wire, &[]);
        let mut state = state_from_init(&init_for(working_dir));
        let backend = RecordingBackend::new(wire.clone(), true);

        let result = state
            .write_input(&backend, "hello kimi")
            .await
            .expect("write input");
        assert!(result.submitted);
        assert_eq!(result.failure_reason, None);
        assert_eq!(result.cli_session_id.as_deref(), Some("session_ddd"));
        assert!(backend.calls().iter().any(|call| call == "enter"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_fails_when_transcript_does_not_confirm() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-kimi-nosubmit");
        let _guard = set_home(&home);
        let working_dir = "/tmp/beam-kimi-work";
        write_session_index(&home, "session_eee", working_dir);
        let wire = wire_path(&home, "session_eee");
        append_wire(&wire, &[]);
        let mut state = state_from_init(&init_for(working_dir));
        let backend = RecordingBackend::new(wire.clone(), false);

        let result = state
            .write_input(&backend, "hello kimi")
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

    // -----------------------------------------------------------------------
    // Live test: drive a real `kimi` CLI through the zellij backend.
    //
    // Requires a locally installed and authenticated `kimi` (kimi-code CLI)
    // and `zellij` on PATH, plus network access to the configured Kimi LLM
    // provider. Uses the real HOME so kimi-code session data and credentials
    // resolve as usual; the test cleans up the zellij session, the temporary
    // working directory, and the kimi-code session data it creates.
    //
    // Run manually with:
    //   cargo test -p beam-worker live_kimi -- --ignored --nocapture
    // -----------------------------------------------------------------------

    fn has_command(name: &str) -> bool {
        std::process::Command::new("which")
            .arg(name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    struct LiveGuard {
        zellij_session: String,
        working_dir: PathBuf,
        kimi_session_root: Option<PathBuf>,
    }

    impl Drop for LiveGuard {
        fn drop(&mut self) {
            let _ = std::process::Command::new("zellij")
                .args(["delete-session", &self.zellij_session, "-f"])
                .output();
            let _ = fs::remove_dir_all(&self.working_dir);
            if let Some(root) = &self.kimi_session_root {
                let _ = fs::remove_dir_all(root);
                // Drop the dangling session_index.jsonl entries for the
                // temporary working directory.
                let index = root
                    .parent()
                    .map(|sessions| sessions.join("session_index.jsonl"));
                if let Some(index) = index
                    && let Ok(text) = fs::read_to_string(&index)
                {
                    let working_dir = self.working_dir.display().to_string();
                    let kept = text
                        .lines()
                        .filter(|line| !line.contains(&working_dir))
                        .collect::<Vec<_>>()
                        .join("\n");
                    let _ = fs::write(
                        &index,
                        if kept.is_empty() { kept } else { format!("{kept}\n") },
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "live test: requires locally installed and authenticated `kimi` and `zellij`"]
    async fn live_kimi_submit_and_poll_final_output() {
        if !has_command("kimi") {
            eprintln!("skipping live test: `kimi` not found in PATH");
            return;
        }
        if !has_command("zellij") {
            eprintln!("skipping live test: `zellij` not found in PATH");
            return;
        }

        let short = &Uuid::new_v4().to_string()[..8];
        let working_dir = std::env::temp_dir().join(format!("beam-kimi-live-{short}"));
        create_dir_all(&working_dir).expect("create live working dir");
        let zellij_session = format!("beam-kimi-live-{short}");
        let mut guard = LiveGuard {
            zellij_session: zellij_session.clone(),
            working_dir: working_dir.clone(),
            kimi_session_root: None,
        };

        let init = InitConfig {
            working_dir: working_dir.display().to_string(),
            prompt: String::new(),
            ..init_for(&working_dir.display().to_string())
        };
        let mut state = state_from_init(&init);
        let spec = state.build_spawn_spec(&init);
        assert!(spec.args.iter().any(|arg| arg == "--yolo"));

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
            .expect("spawn kimi through zellij backend");

        // Wait for the TUI welcome screen before typing.
        let mut ready = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let viewport = backend.capture_viewport().await.unwrap_or_default();
            if viewport.contains("Welcome to Kimi Code") {
                ready = true;
                break;
            }
        }
        assert!(ready, "kimi TUI did not reach the welcome screen within 60s");

        let submit = state
            .write_input(&backend, "reply with exactly: BEAM_KIMI_OK")
            .await
            .expect("write input to live kimi");
        assert!(
            submit.submitted,
            "live submit was not confirmed: {:?}",
            submit.failure_reason
        );
        assert!(submit.cli_session_id.is_some());

        let mut final_output = None;
        for _ in 0..90 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let result = state.poll().expect("poll live kimi");
            if result.final_output.is_some() {
                final_output = result.final_output;
                break;
            }
        }
        let final_output = final_output.expect("kimi did not produce a final output within 90s");
        assert!(
            final_output.contains("BEAM_KIMI_OK"),
            "unexpected final output: {final_output}"
        );

        // Point the guard at the kimi-code session data created for the
        // temporary working directory so it gets cleaned up as well.
        if let Some(path) = &state.transcript_path {
            // <data>/sessions/<wd>/session_*/agents/main/wire.jsonl
            guard.kimi_session_root = path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::parent)
                .and_then(Path::parent)
                .map(Path::to_path_buf);
        }
    }
}
