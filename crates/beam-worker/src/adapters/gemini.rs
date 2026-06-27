use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{GeminiState, PollResult, SpawnSpec, SubmitResult, normalize_history_text};
use crate::backend::SessionBackend;

#[derive(Debug, Clone, Default)]
struct GeminiRuntimeState {
    transcript_path: Option<PathBuf>,
    cli_session_id: Option<String>,
    transcript_offset: u64,
    emitted_final_text: Option<String>,
}

static GEMINI_RUNTIME: OnceLock<Mutex<GeminiRuntimeState>> = OnceLock::new();

fn gemini_runtime() -> &'static Mutex<GeminiRuntimeState> {
    GEMINI_RUNTIME.get_or_init(|| Mutex::new(GeminiRuntimeState::default()))
}

fn reset_gemini_runtime() {
    if let Ok(mut runtime) = gemini_runtime().lock() {
        *runtime = GeminiRuntimeState::default();
    }
}

fn gemini_home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".gemini")
}

fn gemini_tmp_root() -> PathBuf {
    gemini_home_dir().join("tmp")
}

pub fn create_state(_init: &InitConfig) -> GeminiState {
    reset_gemini_runtime();
    GeminiState
}

pub fn build_spawn_spec(_state: &GeminiState, init: &InitConfig) -> SpawnSpec {
    let mut args = Vec::new();
    if !init.disable_cli_bypass {
        args.push("--yolo".to_string());
    }
    if let Some(model) = &init.model {
        if !model.is_empty() {
            args.push("--model".to_string());
            args.push(model.clone());
        }
    }
    if let Some(prompt) = &init.initial_prompt {
        args.push("-i".to_string());
        args.push(prompt.clone());
    }
    args.extend(init.cli_args.clone());
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args,
    }
}

pub async fn write_input(
    _state: &mut GeminiState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    let base_path = current_gemini_transcript_path().or_else(|| latest_gemini_transcript_path());
    let base_size = base_path
        .as_ref()
        .map(|path| file_size(path.as_path()))
        .unwrap_or_default();

    backend.send_text(content).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.send_enter().await?;

    for attempt in 0..4 {
        tokio::time::sleep(Duration::from_millis(800)).await;
        if let Some(path) =
            current_gemini_transcript_path().or_else(|| latest_gemini_transcript_path())
            && gemini_submit_confirmed(&path, base_size, content)?
        {
            update_runtime_for_path(&path);
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: runtime_snapshot().cli_session_id,
                ..Default::default()
            });
        }
        if attempt < 3 {
            backend.send_enter().await?;
        }
    }

    if let Some(path) = current_gemini_transcript_path().or_else(|| latest_gemini_transcript_path())
        && gemini_submit_confirmed(&path, base_size, content)?
    {
        update_runtime_for_path(&path);
        return Ok(SubmitResult {
            submitted: true,
            cli_session_id: runtime_snapshot().cli_session_id,
            ..Default::default()
        });
    }

    Ok(SubmitResult {
        submitted: false,
        cli_session_id: runtime_snapshot().cli_session_id,
        failure_reason: Some("Gemini transcript did not confirm submit".to_string()),
    })
}

pub fn poll(_state: &mut GeminiState) -> Result<PollResult> {
    let Some(path) = current_gemini_transcript_path().or_else(|| latest_gemini_transcript_path())
    else {
        return Ok(PollResult {
            cli_session_id: runtime_snapshot().cli_session_id,
            ..Default::default()
        });
    };

    let size = file_size(&path);
    let mut runtime = gemini_runtime().lock().expect("gemini runtime poisoned");
    if size == runtime.transcript_offset {
        return Ok(PollResult {
            cli_session_id: runtime.cli_session_id.clone(),
            ..Default::default()
        });
    }
    if size < runtime.transcript_offset {
        runtime.transcript_offset = 0;
        runtime.emitted_final_text = None;
    }

    let transcript = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read Gemini transcript {}", path.display()))?;
    let snapshot = parse_gemini_transcript(&transcript)?;
    runtime.transcript_offset = size;
    runtime.transcript_path = Some(path.clone());
    if snapshot.cli_session_id.is_some() {
        runtime.cli_session_id = snapshot.cli_session_id.clone();
    } else if runtime.cli_session_id.is_none() {
        runtime.cli_session_id = session_id_from_path(&path);
    }

    let mut result = PollResult {
        cli_session_id: runtime.cli_session_id.clone(),
        ..Default::default()
    };

    if let Some(final_text) = snapshot.final_output {
        if !final_text.is_empty()
            && runtime.emitted_final_text.as_deref() != Some(final_text.as_str())
        {
            runtime.emitted_final_text = Some(final_text.clone());
            result.final_output = Some(final_text);
            result.final_output_kind = Some(FinalOutputKind::Bridge);
            result.prompt_ready = true;
        }
    }

    Ok(result)
}

#[derive(Debug, Clone, Default)]
struct GeminiTranscriptSnapshot {
    cli_session_id: Option<String>,
    final_output: Option<String>,
}

fn parse_gemini_transcript(raw: &str) -> Result<GeminiTranscriptSnapshot> {
    let value: Value = serde_json::from_str(raw)
        .with_context(|| "failed to parse Gemini transcript JSON".to_string())?;
    let mut cli_session_id = session_id_from_value(&value);
    let messages = gemini_message_values(&value);
    let mut final_output = None;

    for message in messages {
        if let Some(message_session_id) = session_id_from_value(message) {
            cli_session_id = Some(message_session_id);
        }
        if is_gemini_assistant_message(message)
            && let Some(text) = gemini_message_text(message)
            && !text.trim().is_empty()
        {
            final_output = Some(text);
        }
    }

    Ok(GeminiTranscriptSnapshot {
        cli_session_id,
        final_output,
    })
}

fn session_id_from_value(value: &Value) -> Option<String> {
    if let Some(session_id) = value.get("sessionId").and_then(Value::as_str) {
        return Some(session_id.to_string());
    }
    if let Some(session_id) = value.get("session_id").and_then(Value::as_str) {
        return Some(session_id.to_string());
    }
    if let Some(session_id) = value.get("conversationId").and_then(Value::as_str) {
        return Some(session_id.to_string());
    }
    if let Some(session_id) = value.get("conversation_id").and_then(Value::as_str) {
        return Some(session_id.to_string());
    }
    if let Some(session_id) = value.get("id").and_then(Value::as_str) {
        return Some(session_id.to_string());
    }
    None
}

fn gemini_message_values(value: &Value) -> Vec<&Value> {
    if let Some(messages) = value.get("context").and_then(Value::as_array) {
        return messages.iter().collect();
    }
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        return messages.iter().collect();
    }
    if let Some(messages) = value.pointer("/data/messages").and_then(Value::as_array) {
        return messages.iter().collect();
    }
    if let Some(messages) = value.as_array() {
        return messages.iter().collect();
    }
    if let Some(messages) = value.get("history").and_then(Value::as_array) {
        return messages.iter().collect();
    }
    Vec::new()
}

fn is_gemini_assistant_message(message: &Value) -> bool {
    let role = message
        .get("role")
        .or_else(|| message.get("author"))
        .or_else(|| message.get("sender"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(
        role.as_str(),
        "assistant" | "model" | "assistant_final" | "ai" | "bot"
    )
}

fn is_gemini_user_message(message: &Value) -> bool {
    let role = message
        .get("role")
        .or_else(|| message.get("author"))
        .or_else(|| message.get("sender"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(role.as_str(), "user" | "human" | "prompt")
}

fn gemini_message_text(value: &Value) -> Option<String> {
    let mut text = value
        .get("text")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("display")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            value
                .get("result")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            value
                .get("content")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            value
                .get("answer")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .or_else(|| value.get("message").and_then(gemini_message_text));

    if text.is_none()
        && let Some(parts) = value.get("parts").and_then(Value::as_array)
    {
        let mut out = String::new();
        for part in parts {
            if let Some(piece) = gemini_message_text(part) {
                out.push_str(&piece);
            } else if let Some(piece) = part.as_str() {
                out.push_str(piece);
            }
        }
        if !out.is_empty() {
            text = Some(out);
        }
    }

    if text.is_none()
        && let Some(content) = value.get("content")
        && let Some(piece) = gemini_message_text(content)
    {
        text = Some(piece);
    }

    text
}

fn gemini_submit_confirmed(path: &Path, from_byte: u64, expected_text: &str) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let size = file_size(path);
    if size <= from_byte {
        return Ok(false);
    }
    let transcript = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read Gemini transcript {}", path.display()))?;
    let value: Value = serde_json::from_str(&transcript)
        .with_context(|| "failed to parse Gemini transcript JSON".to_string())?;
    let expected = normalize_history_text(expected_text);
    for message in gemini_message_values(&value) {
        if !is_gemini_user_message(message) {
            continue;
        }
        if let Some(text) = gemini_message_text(message)
            && normalize_history_text(&text) == expected
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn latest_gemini_transcript_path() -> Option<PathBuf> {
    let root = gemini_tmp_root();
    let mut best: Option<(PathBuf, SystemTime, u64)> = None;
    walk_gemini_transcripts(&root, &mut |path| {
        let Ok(meta) = path.metadata() else {
            return;
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
            best = Some((path.to_path_buf(), modified, size));
        }
    });
    best.map(|(path, _, _)| path)
}

fn current_gemini_transcript_path() -> Option<PathBuf> {
    gemini_runtime()
        .lock()
        .ok()
        .and_then(|runtime| runtime.transcript_path.clone())
        .filter(|path| path.exists())
}

fn update_runtime_for_path(path: &Path) {
    if let Ok(mut runtime) = gemini_runtime().lock() {
        runtime.transcript_path = Some(path.to_path_buf());
        runtime.transcript_offset = file_size(path);
        if runtime.cli_session_id.is_none() {
            runtime.cli_session_id = session_id_from_path(path);
        }
    }
}

fn runtime_snapshot() -> GeminiRuntimeState {
    gemini_runtime()
        .lock()
        .map(|runtime| runtime.clone())
        .unwrap_or_default()
}

fn session_id_from_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(ToOwned::to_owned)
}

fn walk_gemini_transcripts(dir: &Path, visit: &mut dyn FnMut(&Path)) {
    let Ok(entries) = read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            walk_gemini_transcripts(&path, visit);
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with("session-") && name.ends_with(".json") {
            visit(&path);
        }
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::fs;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct RecordingBackend {
        transcript_path: PathBuf,
        buffer: Arc<Mutex<String>>,
        calls: Arc<Mutex<Vec<String>>>,
        append_on_enter: bool,
        final_text: Option<String>,
    }

    impl RecordingBackend {
        fn new(
            transcript_path: PathBuf,
            append_on_enter: bool,
            final_text: Option<String>,
        ) -> Self {
            Self {
                transcript_path,
                buffer: Arc::new(Mutex::new(String::new())),
                calls: Arc::new(Mutex::new(Vec::new())),
                append_on_enter,
                final_text,
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SessionBackend for RecordingBackend {
        async fn spawn(
            &mut self,
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
                    let transcript = serde_json::json!({
                        "context": [
                            {"role": "user", "parts": [{"text": content}]},
                            {"role": "model", "parts": [{"text": self.final_text.clone().unwrap_or_else(|| "Gemini reply".to_string())}]},
                        ]
                    });
                    fs::write(
                        &self.transcript_path,
                        serde_json::to_vec_pretty(&transcript)?,
                    )
                    .context("failed to append Gemini transcript")?;
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

        async fn kill(&mut self) -> Result<()> {
            Ok(())
        }

        async fn destroy_session(&mut self) -> Result<()> {
            Ok(())
        }

        async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
            Ok(None)
        }

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
    }

    fn temp_home() -> PathBuf {
        std::env::temp_dir().join(format!("beam-gemini-{}", Uuid::new_v4()))
    }

    fn session_path(home: &Path) -> PathBuf {
        home.join(".gemini")
            .join("tmp")
            .join("project-1")
            .join("chats")
            .join("session-abc.json")
    }

    fn write_session(path: &Path, value: &Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    }

    fn test_init() -> InitConfig {
        InitConfig {
            session_id: "sid".to_string(),
            title: "title".to_string(),
            chat_id: "chat".to_string(),
            root_message_id: "root".to_string(),
            working_dir: ".".to_string(),
            cli_id: "gemini".to_string(),
            cli_bin: "gemini".to_string(),
            cli_args: vec![],

            prompt: String::new(),
            resume: false,
            cli_session_id: None,
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            prompt_turn_id: None,
            owner_open_id: None,
            adopted_from: None,
            adopt_restored_from_metadata: false,
            screen_analyzer: beam_core::ScreenAnalyzerConfig::default(),
            initial_prompt: None,
            model: None,
            locale: None,
            bot_name: None,
            bot_open_id: None,
            resume_session_id: None,
            disable_cli_bypass: false,
        }
    }

    #[test]
    fn poll_reads_final_output_from_gemini_session() {
        let home = temp_home();
        let transcript = session_path(&home);
        let value = serde_json::json!({
            "context": [
                {"role": "user", "parts": [{"text": "hello"}]},
                {"role": "model", "parts": [{"text": "Gemini final reply"}]},
            ]
        });
        write_session(&transcript, &value);
        let _home_guard = crate::adapter::home_test_lock().lock().expect("home lock");
        unsafe {
            std::env::set_var("HOME", &home);
        }
        let mut state = create_state(&test_init());

        let result = poll(&mut state).expect("poll");
        assert_eq!(result.final_output.as_deref(), Some("Gemini final reply"));
        assert_eq!(result.final_output_kind, Some(FinalOutputKind::Bridge));
        assert!(result.prompt_ready);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_confirms_submit_when_transcript_records_prompt() {
        let home = temp_home();
        let transcript = session_path(&home);
        write_session(&transcript, &serde_json::json!({ "context": [] }));
        let _home_guard = crate::adapter::home_test_lock().lock().expect("home lock");
        unsafe {
            std::env::set_var("HOME", &home);
        }
        let mut state = create_state(&test_init());
        let backend = RecordingBackend::new(
            transcript.clone(),
            true,
            Some("Gemini final reply".to_string()),
        );

        let result = write_input(&mut state, &backend, "hello gemini")
            .await
            .expect("write input");
        assert!(result.submitted);
        assert!(backend.calls().iter().any(|call| call == "enter"));
        assert_eq!(result.failure_reason, None);
    }
}
