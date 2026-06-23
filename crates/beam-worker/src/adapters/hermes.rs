use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use beam_core::InitConfig;
use serde_json::Value;

use crate::adapter::{HermesState, PollResult, SpawnSpec, SubmitResult, normalize_history_text};
use crate::backend::SessionBackend;

const CONTENT_JSON_PREFIX: &str = "\u{0}json:";

#[derive(Debug, Clone, Default)]
struct HermesRuntimeState {
    transcript_path: Option<PathBuf>,
    cli_session_id: Option<String>,
    transcript_offset: u64,
    emitted_final_text: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct HermesTranscriptSnapshot {
    cli_session_id: Option<String>,
    final_output: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct HermesMessageRow {
    #[serde(rename = "id")]
    _id: u64,
    session_id: Option<String>,
    role: String,
    content: Value,
    #[serde(rename = "timestamp")]
    _timestamp: Option<u64>,
    finish_reason: Option<String>,
}

static HERMES_RUNTIME: OnceLock<Mutex<HermesRuntimeState>> = OnceLock::new();

fn hermes_runtime() -> &'static Mutex<HermesRuntimeState> {
    HERMES_RUNTIME.get_or_init(|| Mutex::new(HermesRuntimeState::default()))
}

fn reset_hermes_runtime() {
    if let Ok(mut runtime) = hermes_runtime().lock() {
        *runtime = HermesRuntimeState::default();
    }
}

fn runtime_snapshot() -> HermesRuntimeState {
    hermes_runtime()
        .lock()
        .expect("hermes runtime poisoned")
        .clone()
}

fn hermes_home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".hermes")
}

fn hermes_state_db_path() -> PathBuf {
    hermes_home_dir().join("state.db")
}

pub fn create_state(_init: &InitConfig) -> HermesState {
    reset_hermes_runtime();
    HermesState
}

pub fn build_spawn_spec(_state: &HermesState, init: &InitConfig) -> SpawnSpec {
    let mut args = Vec::new();
    if init.resume {
        args.push("--resume".to_string());
        args.push(
            init.resume_session_id
                .clone()
                .unwrap_or_else(|| init.session_id.clone()),
        );
    }
    if !init.disable_cli_bypass {
        args.push("--yolo".to_string());
        args.push("--accept-hooks".to_string());
    }
    args.push("--pass-session-id".to_string());
    args.extend(init.cli_args.clone());
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args,
    }
}

pub async fn write_input(
    _state: &mut HermesState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    let db_path = hermes_state_db_path();
    let base_offset = current_hermes_state_offset(&db_path)?;

    backend.send_text(content).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.send_enter().await?;

    for attempt in 0..4 {
        tokio::time::sleep(Duration::from_millis(800)).await;
        if hermes_submit_confirmed(&db_path, base_offset, content)? {
            let cli_session_id = latest_hermes_session_id(&db_path)?;
            let mut runtime = hermes_runtime().lock().expect("hermes runtime poisoned");
            runtime.transcript_path = Some(db_path.clone());
            runtime.cli_session_id = cli_session_id.clone();
            runtime.transcript_offset = current_hermes_state_offset(&db_path)?;
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id,
                ..Default::default()
            });
        }
        if attempt < 3 {
            backend.send_enter().await?;
        }
    }

    let cli_session_id = latest_hermes_session_id(&db_path)?;
    Ok(SubmitResult {
        submitted: false,
        cli_session_id,
        failure_reason: Some("Hermes transcript did not confirm submit".to_string()),
    })
}

pub fn poll(_state: &mut HermesState) -> Result<PollResult> {
    let db_path = hermes_state_db_path();
    let Some(current_offset) = current_hermes_state_offset_opt(&db_path)? else {
        return Ok(PollResult {
            cli_session_id: runtime_snapshot().cli_session_id,
            ..Default::default()
        });
    };

    let mut runtime = hermes_runtime().lock().expect("hermes runtime poisoned");
    if current_offset < runtime.transcript_offset {
        runtime.transcript_offset = 0;
        runtime.emitted_final_text = None;
    }
    if current_offset == runtime.transcript_offset {
        return Ok(PollResult {
            cli_session_id: runtime.cli_session_id.clone(),
            ..Default::default()
        });
    }

    let snapshot = drain_hermes_state_db(&db_path, runtime.transcript_offset)?;
    runtime.transcript_offset = current_offset;
    runtime.transcript_path = Some(db_path.clone());
    if snapshot.cli_session_id.is_some() {
        runtime.cli_session_id = snapshot.cli_session_id.clone();
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
            result.final_output_kind = Some(beam_core::FinalOutputKind::Bridge);
            result.prompt_ready = true;
        }
    }

    Ok(result)
}

fn current_hermes_state_offset_opt(db_path: &Path) -> Result<Option<u64>> {
    if !db_path.exists() {
        return Ok(None);
    }
    current_hermes_state_offset(db_path).map(Some)
}

fn current_hermes_state_offset(db_path: &Path) -> Result<u64> {
    if !db_path.exists() {
        return Ok(0);
    }
    let script = format!(
        r#"
import sqlite3
conn = sqlite3.connect({db_path})
row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM messages").fetchone()
print(row[0] or 0)
"#,
        db_path = json_string(&db_path.display().to_string())
    );
    let proc = Command::new("python3")
        .args(["-c", &script])
        .output()
        .context("failed to query Hermes transcript offset")?;
    if !proc.status.success() {
        bail!(
            "{}",
            String::from_utf8_lossy(&proc.stderr).trim().to_string()
        );
    }
    Ok(String::from_utf8_lossy(&proc.stdout)
        .trim()
        .parse::<u64>()
        .unwrap_or(0))
}

fn latest_hermes_session_id(db_path: &Path) -> Result<Option<String>> {
    if !db_path.exists() {
        return Ok(None);
    }
    let script = format!(
        r#"
import json
import sqlite3
conn = sqlite3.connect({db_path})
row = conn.execute(
    "SELECT session_id FROM messages ORDER BY id DESC LIMIT 1"
).fetchone()
print(json.dumps(row[0], ensure_ascii=False) if row and row[0] is not None else "null")
"#,
        db_path = json_string(&db_path.display().to_string())
    );
    run_python_json(&script)
}

fn drain_hermes_state_db(db_path: &Path, from_offset: u64) -> Result<HermesTranscriptSnapshot> {
    if !db_path.exists() {
        return Ok(HermesTranscriptSnapshot::default());
    }
    let rows = query_hermes_rows(db_path, from_offset)?;
    let mut cli_session_id = None;
    let mut final_output = None;

    for row in rows {
        if row.session_id.is_some() {
            cli_session_id = row.session_id.clone();
        }
        let text = hermes_message_text(&row.content).trim().to_string();
        if text.is_empty() {
            continue;
        }
        if row.role == "assistant" && row.finish_reason.as_deref() == Some("stop") {
            final_output = Some(text);
        }
    }

    Ok(HermesTranscriptSnapshot {
        cli_session_id,
        final_output,
    })
}

fn hermes_submit_confirmed(db_path: &Path, from_offset: u64, expected_text: &str) -> Result<bool> {
    if !db_path.exists() {
        return Ok(false);
    }
    let rows = query_hermes_rows(db_path, from_offset)?;
    let expected = normalize_history_text(expected_text).trim().to_string();
    for row in rows {
        if row.role != "user" {
            continue;
        }
        let text = normalize_history_text(&hermes_message_text(&row.content))
            .trim()
            .to_string();
        if text == expected {
            return Ok(true);
        }
    }
    Ok(false)
}

fn query_hermes_rows(db_path: &Path, from_offset: u64) -> Result<Vec<HermesMessageRow>> {
    let script = format!(
        r#"
import json
import sqlite3
conn = sqlite3.connect({db_path})
conn.row_factory = sqlite3.Row
rows = conn.execute(
    """
    SELECT id, session_id, role, content, timestamp, finish_reason
    FROM messages
    WHERE id > ? AND role IN ('user', 'assistant')
    ORDER BY id
    """,
    ({from_offset},),
).fetchall()
print(json.dumps([dict(r) for r in rows], ensure_ascii=False))
"#,
        db_path = json_string(&db_path.display().to_string()),
        from_offset = from_offset
    );
    run_python_json(&script)
}

fn hermes_message_text(content: &Value) -> String {
    match content {
        Value::Null => String::new(),
        Value::String(text) => decode_hermes_content(text),
        other => stringify_hermes_content(other),
    }
}

fn decode_hermes_content(content: &str) -> String {
    if !content.starts_with(CONTENT_JSON_PREFIX) {
        return content.to_string();
    }
    match serde_json::from_str::<Value>(&content[CONTENT_JSON_PREFIX.len()..]) {
        Ok(value) => stringify_hermes_content(&value),
        Err(_) => content.to_string(),
    }
}

fn stringify_hermes_content(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                match item {
                    Value::String(text) => parts.push(text.clone()),
                    Value::Object(map) => {
                        if let Some(text) = map.get("text").and_then(Value::as_str) {
                            parts.push(text.to_string());
                        } else if let Some(text) = map.get("content").and_then(Value::as_str) {
                            parts.push(text.to_string());
                        }
                    }
                    _ => {}
                }
            }
            parts.join("")
        }
        Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                return text.to_string();
            }
            if let Some(text) = map.get("content").and_then(Value::as_str) {
                return text.to_string();
            }
            String::new()
        }
        _ => String::new(),
    }
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("json string")
}

fn run_python_json<T: serde::de::DeserializeOwned>(script: &str) -> Result<T> {
    let proc = Command::new("python3")
        .args(["-c", script])
        .output()
        .context("failed to run python3")?;
    if !proc.status.success() {
        bail!(
            "{}",
            String::from_utf8_lossy(&proc.stderr).trim().to_string()
        );
    }
    let stdout = String::from_utf8_lossy(&proc.stdout).trim().to_string();
    if stdout.is_empty() {
        bail!("python3 returned empty output");
    }
    Ok(serde_json::from_str(&stdout)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::fs;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn temp_home() -> PathBuf {
        std::env::temp_dir().join(format!("beam-hermes-{}", Uuid::new_v4()))
    }

    fn hermes_db_path(home: &Path) -> PathBuf {
        home.join(".hermes").join("state.db")
    }

    fn write_db(db_path: &Path, rows: &[(&str, &str, &str, Option<&str>, u64)]) {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).expect("create hermes dir");
        }
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT,
  role TEXT,
  content TEXT,
  timestamp INTEGER,
  finish_reason TEXT
);
""")
"#,
        );
        for row in rows {
            let (session_id, role, content, finish_reason, timestamp) = row;
            let finish_reason = finish_reason
                .map(json_string)
                .unwrap_or_else(|| "None".to_string());
            let content = json_string(content);
            script.push_str(&format!(
                r#"
conn.execute(
    "INSERT INTO messages (session_id, role, content, timestamp, finish_reason) VALUES (?, ?, ?, ?, ?)",
    ({session_id}, {role}, {content}, {timestamp}, {finish_reason}),
)
"#,
                session_id = json_string(session_id),
                role = json_string(role),
                content = content,
                timestamp = timestamp,
                finish_reason = finish_reason,
            ));
        }
        script.push_str("conn.commit()\n");
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to create hermes sqlite db");
    }

    fn append_row(
        db_path: &Path,
        session_id: &str,
        role: &str,
        content: &str,
        timestamp: u64,
        finish_reason: Option<&str>,
    ) {
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.execute(
    "INSERT INTO messages (session_id, role, content, timestamp, finish_reason) VALUES (?, ?, ?, ?, ?)",
    (__SESSION_ID__, __ROLE__, __CONTENT__, __TIMESTAMP__, __FINISH_REASON__),
)
conn.commit()
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        script = script.replace("__SESSION_ID__", &json_string(session_id));
        script = script.replace("__ROLE__", &json_string(role));
        script = script.replace("__CONTENT__", &json_string(content));
        script = script.replace("__TIMESTAMP__", &timestamp.to_string());
        script = script.replace(
            "__FINISH_REASON__",
            &finish_reason
                .map(json_string)
                .unwrap_or_else(|| "None".to_string()),
        );
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to append hermes row");
    }

    #[derive(Clone, Default)]
    struct RecordingBackend {
        db_path: PathBuf,
        buffer: Arc<Mutex<String>>,
        append_on_enter: bool,
        next_timestamp: Arc<Mutex<u64>>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingBackend {
        fn new(db_path: PathBuf, append_on_enter: bool, start_timestamp: u64) -> Self {
            Self {
                db_path,
                buffer: Arc::new(Mutex::new(String::new())),
                append_on_enter,
                next_timestamp: Arc::new(Mutex::new(start_timestamp)),
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
                    let mut next_timestamp = self.next_timestamp.lock().unwrap();
                    let timestamp = *next_timestamp + 1;
                    *next_timestamp = timestamp;
                    append_row(
                        &self.db_path,
                        "session-hermes",
                        "user",
                        &content,
                        timestamp,
                        None,
                    );
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
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            drop(tx);
            rx
        }
    }

    fn test_init() -> InitConfig {
        InitConfig {
            session_id: "sid".to_string(),
            title: "title".to_string(),
            chat_id: "chat".to_string(),
            root_message_id: "root".to_string(),
            working_dir: ".".to_string(),
            cli_id: "hermes".to_string(),
            cli_bin: "hermes".to_string(),
            cli_args: vec!["--test-flag".to_string()],

            prompt: String::new(),
            resume: false,
            cli_session_id: Some("cli-session-1".to_string()),
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            prompt_turn_id: None,
            owner_open_id: None,
            adopted_from: None,
            adopt_restored_from_metadata: false,
            screen_analyzer: beam_core::ScreenAnalyzerConfig::default(),
            initial_prompt: Some("hello".to_string()),
            model: None,
            locale: None,
            bot_name: None,
            bot_open_id: None,
            resume_session_id: None,
            disable_cli_bypass: false,
        }
    }

    #[test]
    fn poll_reads_final_output_from_state_db() {
        let home = temp_home();
        let db_path = hermes_db_path(&home);
        write_db(
            &db_path,
            &[
                ("session-hermes", "user", "hello", None, 1000),
                (
                    "session-hermes",
                    "assistant",
                    "\u{0}json:[{\"text\":\"Hermes final reply\"}]",
                    Some("stop"),
                    1200,
                ),
            ],
        );
        let _home_guard = crate::adapter::home_test_lock().lock().expect("home lock");
        unsafe {
            std::env::set_var("HOME", &home);
        }
        let mut state = create_state(&test_init());

        let result = poll(&mut state).expect("poll");
        assert_eq!(result.final_output.as_deref(), Some("Hermes final reply"));
        assert_eq!(
            result.final_output_kind,
            Some(beam_core::FinalOutputKind::Bridge)
        );
        assert!(result.prompt_ready);

        let second = poll(&mut state).expect("poll again");
        assert_eq!(second.final_output, None);
        assert_eq!(second.final_output_kind, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_confirms_submit_from_state_db() {
        let home = temp_home();
        let db_path = hermes_db_path(&home);
        write_db(
            &db_path,
            &[("session-hermes", "assistant", "ready", Some("stop"), 1000)],
        );
        let _home_guard = crate::adapter::home_test_lock().lock().expect("home lock");
        unsafe {
            std::env::set_var("HOME", &home);
        }
        let mut state = create_state(&test_init());
        let backend = RecordingBackend::new(db_path.clone(), true, 2000);

        let result = write_input(&mut state, &backend, "hello hermes")
            .await
            .expect("write input");
        assert!(result.submitted);
        assert_eq!(result.failure_reason, None);
        assert!(backend.calls().iter().any(|call| call == "enter"));
        assert_eq!(result.cli_session_id.as_deref(), Some("session-hermes"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_input_fails_when_state_db_does_not_confirm() {
        let home = temp_home();
        let db_path = hermes_db_path(&home);
        write_db(
            &db_path,
            &[("session-hermes", "assistant", "ready", Some("stop"), 1000)],
        );
        let _home_guard = crate::adapter::home_test_lock().lock().expect("home lock");
        unsafe {
            std::env::set_var("HOME", &home);
        }
        let mut state = create_state(&test_init());
        let backend = RecordingBackend::new(db_path.clone(), false, 2000);

        let result = write_input(&mut state, &backend, "hello hermes")
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
}
