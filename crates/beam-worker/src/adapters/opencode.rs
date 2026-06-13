use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use beam_core::{FinalOutputKind, InitConfig};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::adapter::{OpenCodeState, PollResult, SpawnSpec, SubmitResult};
use crate::backend::SessionBackend;

const OPENCODE_CURSOR_LOOKBACK_MS: u64 = 5_000;

#[derive(Debug, Clone, Deserialize)]
pub struct OpenCodeTranscriptSource {
    pub db_path: PathBuf,
    pub session_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenCodeSessionRow {
    id: String,
    time_updated: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenCodeMessageRow {
    message_id: String,
    session_id: String,
    message_time_created: Option<u64>,
    message_time_updated: Option<u64>,
    message_data: String,
    part_id: Option<String>,
    part_time_updated: Option<u64>,
    part_data: Option<String>,
}

#[derive(Debug, Clone)]
struct GroupedMessage {
    id: String,
    session_id: String,
    time_created: u64,
    time_updated: u64,
    data: Value,
    parts: Vec<GroupedPart>,
}

#[derive(Debug, Clone)]
struct GroupedPart {
    time_updated: u64,
    data: Value,
}

pub fn create_state(init: &InitConfig) -> OpenCodeState {
    let home = std::env::var("HOME").unwrap_or_default();
    let data_dir = PathBuf::from(format!("{}/.local/share/opencode", home));
    let expected_session_id = init.cli_session_id.clone();
    OpenCodeState {
        data_dir,
        expected_session_id,
        working_dir: init.working_dir.clone(),
        cli_session_id: init.cli_session_id.clone(),
        transcript_offset: 0,
        emitted_final_text: None,
    }
}

pub fn build_spawn_spec(_state: &OpenCodeState, init: &InitConfig) -> SpawnSpec {
    let mut args = Vec::new();
    if let Some(model) = &init.model {
        if !model.is_empty() {
            args.push("--model".to_string());
            args.push(model.clone());
        }
    }
    if let Some(prompt) = &init.initial_prompt {
        args.push("--prompt".to_string());
        args.push(prompt.clone());
    }
    args.extend(init.cli_args.clone());
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args,
    }
}

pub async fn write_input(
    state: &mut OpenCodeState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    let Some(source) = wait_for_source(state).await else {
        return Ok(SubmitResult {
            submitted: false,
            cli_session_id: state.cli_session_id.clone(),
            failure_reason: Some("OpenCode transcript source not found".to_string()),
        });
    };
    let base_offset = current_opencode_session_offset(&source)?;
    state
        .cli_session_id
        .get_or_insert_with(|| source.session_id.clone());

    backend.send_text(content).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.send_enter().await?;
    for attempt in 0..4 {
        tokio::time::sleep(Duration::from_millis(800)).await;
        if opencode_submit_confirmed(&source, base_offset, content)? {
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: state.cli_session_id.clone(),
                ..Default::default()
            });
        }
        if attempt < 3 {
            backend.send_enter().await?;
        }
    }
    Ok(SubmitResult {
        submitted: false,
        cli_session_id: state.cli_session_id.clone(),
        failure_reason: Some("OpenCode transcript did not confirm submit".to_string()),
    })
}

pub fn poll(state: &mut OpenCodeState) -> Result<PollResult> {
    let source = current_source(state);
    let Some(source) = source else {
        return Ok(PollResult {
            cli_session_id: state.cli_session_id.clone(),
            ..Default::default()
        });
    };

    let drain = drain_opencode_session(&source, state.transcript_offset)?;
    state.transcript_offset = drain.new_offset;
    if state.cli_session_id.is_none() {
        state.cli_session_id = Some(source.session_id.clone());
    }

    let mut result = PollResult {
        cli_session_id: state.cli_session_id.clone(),
        ..Default::default()
    };

    for event in drain.events {
        if event.kind != "assistant_final" {
            continue;
        }
        if !event.text.is_empty() && state.emitted_final_text.as_deref() != Some(&event.text) {
            state.emitted_final_text = Some(event.text.clone());
            result.final_output = Some(event.text);
            result.final_output_kind = Some(FinalOutputKind::Bridge);
            result.prompt_ready = true;
        }
    }

    Ok(result)
}

pub fn opencode_db_candidates(data_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            if !name.starts_with("opencode") || !name.ends_with(".db") {
                return None;
            }
            match entry.file_type() {
                Ok(ft) if ft.is_file() => Some(path),
                _ => None,
            }
        })
        .collect()
}

pub fn find_opencode_session_by_id(
    session_id: Option<&str>,
    db_paths: &[PathBuf],
) -> Option<OpenCodeTranscriptSource> {
    let session_id = session_id?;
    for db_path in db_paths {
        if !db_path.exists() {
            continue;
        }
        if let Ok(Some(row)) = query_session_by_id(db_path, session_id) {
            return Some(OpenCodeTranscriptSource {
                db_path: db_path.clone(),
                session_id: row.id,
            });
        }
    }
    None
}

pub fn find_latest_opencode_session_by_directory(
    directory: Option<&str>,
    db_paths: &[PathBuf],
) -> Option<OpenCodeTranscriptSource> {
    let directory = directory?;
    let mut best: Option<(PathBuf, OpenCodeSessionRow)> = None;
    for db_path in db_paths {
        if !db_path.exists() {
            continue;
        }
        if let Ok(Some(row)) = query_latest_session_by_directory(db_path, directory) {
            let replace = match &best {
                None => true,
                Some((_, current)) => {
                    row.time_updated.unwrap_or(0) > current.time_updated.unwrap_or(0)
                }
            };
            if replace {
                best = Some((db_path.clone(), row));
            }
        }
    }
    best.map(|(db_path, row)| OpenCodeTranscriptSource {
        db_path,
        session_id: row.id,
    })
}

pub fn drain_opencode_session(
    source: &OpenCodeTranscriptSource,
    from_offset: u64,
) -> Result<OpenCodeDrainResult> {
    if !source.db_path.exists() {
        return Ok(OpenCodeDrainResult {
            events: Vec::new(),
            new_offset: from_offset,
        });
    }
    let rows = query_changed_rows(source, from_offset)?;
    let mut grouped: BTreeMap<String, GroupedMessage> = BTreeMap::new();
    let mut new_offset = from_offset;
    for row in rows {
        let entry = grouped
            .entry(row.message_id.clone())
            .or_insert_with(|| GroupedMessage {
                id: row.message_id.clone(),
                session_id: row.session_id.clone(),
                time_created: row.message_time_created.unwrap_or(0),
                time_updated: row.message_time_updated.unwrap_or(0),
                data: parse_object(&row.message_data),
                parts: Vec::new(),
            });
        if let Some(time_updated) = row.message_time_updated {
            entry.time_updated = entry.time_updated.max(time_updated);
        }
        if let (Some(_part_id), Some(part_data)) = (row.part_id, row.part_data) {
            entry.parts.push(GroupedPart {
                time_updated: row.part_time_updated.unwrap_or(0),
                data: parse_object(&part_data),
            });
        }
    }

    let mut messages = grouped.into_values().collect::<Vec<_>>();
    messages.sort_by(|a, b| {
        a.time_created
            .cmp(&b.time_created)
            .then_with(|| a.id.cmp(&b.id))
    });

    let mut events = Vec::new();
    for message in messages {
        new_offset = new_offset.max(message.time_updated).max(
            message
                .parts
                .iter()
                .map(|part| part.time_updated)
                .max()
                .unwrap_or(0),
        );
        let role = message.data.get("role").and_then(Value::as_str);
        match role {
            Some("user") => {
                let text = text_from_parts(&message.parts);
                if text.is_empty() {
                    continue;
                }
                events.push(OpenCodeBridgeEvent {
                    uuid: format!("opencode:{}:{}", source.db_path.display(), message.id),
                    timestamp_ms: message_timestamp_ms(&message, false),
                    kind: "user".to_string(),
                    text,
                    source_session_id: Some(message.session_id.clone()),
                });
            }
            Some("assistant") => {
                if should_skip_assistant(&message.data) {
                    continue;
                }
                let text = text_from_parts(&message.parts);
                if text.is_empty() {
                    continue;
                }
                events.push(OpenCodeBridgeEvent {
                    uuid: format!("opencode:{}:{}", source.db_path.display(), message.id),
                    timestamp_ms: message_timestamp_ms(&message, true),
                    kind: "assistant_final".to_string(),
                    text,
                    source_session_id: Some(message.session_id.clone()),
                });
            }
            _ => {}
        }
    }

    Ok(OpenCodeDrainResult { events, new_offset })
}

fn current_source(state: &OpenCodeState) -> Option<OpenCodeTranscriptSource> {
    let db_paths = opencode_db_candidates(&state.data_dir);
    if let Some(source) =
        find_opencode_session_by_id(state.expected_session_id.as_deref(), &db_paths)
    {
        return Some(source);
    }
    find_latest_opencode_session_by_directory(Some(&state.working_dir), &db_paths)
}

async fn wait_for_source(state: &OpenCodeState) -> Option<OpenCodeTranscriptSource> {
    for _ in 0..12 {
        if let Some(source) = current_source(state) {
            return Some(source);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    current_source(state)
}

pub fn current_opencode_session_offset(source: &OpenCodeTranscriptSource) -> Result<u64> {
    if !source.db_path.exists() {
        return Ok(0);
    }
    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
row = conn.execute(
    """
    SELECT COALESCE(MAX(value), 0) FROM (
      SELECT time_updated AS value FROM message WHERE session_id = ?
      UNION ALL
      SELECT time_updated AS value FROM part WHERE session_id = ?
    )
    """,
    (__SESSION_ID__, __SESSION_ID__),
).fetchone()
print(row[0] or 0)
"#,
    );
    script = script.replace(
        "__DB_PATH__",
        &json_string(&source.db_path.display().to_string()),
    );
    script = script.replace("__SESSION_ID__", &json_string(&source.session_id));
    let proc = Command::new("python3")
        .args(["-c", &script])
        .output()
        .context("failed to query opencode session offset")?;
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

fn opencode_submit_confirmed(
    source: &OpenCodeTranscriptSource,
    from_offset: u64,
    expected_text: &str,
) -> Result<bool> {
    if !source.db_path.exists() {
        return Ok(false);
    }
    let drain = drain_opencode_session(source, from_offset)?;
    let prefix = expected_text.chars().take(40).collect::<String>();
    Ok(drain
        .events
        .iter()
        .any(|event| event.kind == "user" && event.text.starts_with(&prefix)))
}

fn should_skip_assistant(data: &Value) -> bool {
    data.get("error").is_some() && data.get("summary").is_none()
}

fn query_changed_rows(
    source: &OpenCodeTranscriptSource,
    offset: u64,
) -> Result<Vec<OpenCodeMessageRow>> {
    let lower_bound = offset.saturating_sub(OPENCODE_CURSOR_LOOKBACK_MS);
    let mut script = String::from(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    """
    WITH changed AS (
      SELECT m.id
      FROM message m
      LEFT JOIN part p ON p.message_id = m.id
      WHERE m.session_id = ?
        AND (m.time_updated > ? OR COALESCE(p.time_updated, 0) > ?)
      GROUP BY m.id
    )
    SELECT
      m.id AS message_id,
      m.session_id AS session_id,
      m.time_created AS message_time_created,
      m.time_updated AS message_time_updated,
      m.data AS message_data,
      p.id AS part_id,
      p.time_updated AS part_time_updated,
      p.data AS part_data
    FROM message m
    LEFT JOIN part p ON p.message_id = m.id
    WHERE m.id IN (SELECT id FROM changed)
    ORDER BY m.time_created, m.id, p.time_created, p.id
    """,
    (__SESSION_ID__, __LOWER_BOUND__, __LOWER_BOUND__),
).fetchall()
print(json.dumps([dict(r) for r in rows], ensure_ascii=False))
"#,
    );
    script = script.replace(
        "__DB_PATH__",
        &json_string(&source.db_path.display().to_string()),
    );
    script = script.replace("__SESSION_ID__", &json_string(&source.session_id));
    script = script.replace("__LOWER_BOUND__", &lower_bound.to_string());
    run_python_json(&script)
}

fn query_session_by_id(db_path: &Path, session_id: &str) -> Result<Option<OpenCodeSessionRow>> {
    let mut script = String::from(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.row_factory = sqlite3.Row
row = conn.execute(
    "SELECT id, time_updated FROM session WHERE id = ? LIMIT 1",
    (__SESSION_ID__,),
).fetchone()
print(json.dumps(dict(row), ensure_ascii=False) if row else "null")
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    script = script.replace("__SESSION_ID__", &json_string(session_id));
    run_python_json(&script)
}

fn query_latest_session_by_directory(
    db_path: &Path,
    directory: &str,
) -> Result<Option<OpenCodeSessionRow>> {
    let mut script = String::from(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.row_factory = sqlite3.Row
row = conn.execute(
    """
    SELECT id, directory, time_updated
    FROM session
    WHERE directory = ?
    ORDER BY time_updated DESC
    LIMIT 1
    """,
    (__DIRECTORY__,),
).fetchone()
print(json.dumps(dict(row), ensure_ascii=False) if row else "null")
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    script = script.replace("__DIRECTORY__", &json_string(directory));
    run_python_json(&script)
}

fn parse_object(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or(Value::Null)
}

fn text_from_parts(parts: &[GroupedPart]) -> String {
    parts
        .iter()
        .filter_map(|part| {
            if part.data.get("type").and_then(Value::as_str) != Some("text") {
                return None;
            }
            if part.data.get("ignored").and_then(Value::as_bool) == Some(true) {
                return None;
            }
            part.data
                .get("text")
                .and_then(Value::as_str)
                .and_then(|text| {
                    if text.trim().is_empty() {
                        None
                    } else {
                        Some(text.to_string())
                    }
                })
        })
        .collect::<Vec<_>>()
        .join("")
}

fn number_value(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|v| u64::try_from(v).ok()))
}

fn message_timestamp_ms(message: &GroupedMessage, assistant_final: bool) -> u64 {
    if let Some(time) = message.data.get("time").and_then(Value::as_object) {
        if assistant_final {
            if let Some(completed) = time.get("completed").and_then(number_value) {
                return completed;
            }
        }
        if let Some(created) = time.get("created").and_then(number_value) {
            return created;
        }
    }
    message.time_updated.max(message.time_created)
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("json string")
}

fn run_python_json<T: DeserializeOwned>(script: &str) -> Result<T> {
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

#[derive(Debug, Clone)]
pub struct OpenCodeBridgeEvent {
    #[allow(dead_code)]
    pub uuid: String,
    #[allow(dead_code)]
    pub timestamp_ms: u64,
    pub kind: String,
    pub text: String,
    #[allow(dead_code)]
    pub source_session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpenCodeDrainResult {
    pub events: Vec<OpenCodeBridgeEvent>,
    pub new_offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::fs;
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("beam-opencode-{}-{}", name, Uuid::new_v4()))
    }

    fn create_test_db(db_path: &Path) {
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER
);
CREATE TABLE message (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
CREATE TABLE part (
  id TEXT PRIMARY KEY,
  message_id TEXT,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
""")
conn.execute(
    "INSERT INTO session (id, directory, time_updated) VALUES (?, ?, ?)",
    ("sess-1", "/repo/opencode", 1500),
)
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ("msg-user", "sess-1", 1000, 1001, '{"role":"user","id":"msg-user"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-user", "msg-user", "sess-1", 1002, 1002, '{"type":"text","text":"hello"}'),
)
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ("msg-asst", "sess-1", 1300, 1500, '{"role":"assistant","id":"msg-asst"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-step", "msg-asst", "sess-1", 1400, 1400, '{"type":"step-start"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-text", "msg-asst", "sess-1", 1490, 1490, '{"type":"text","text":"hi there"}'),
)
conn.commit()
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to create sqlite db");
    }

    fn append_user_submit(db_path: &Path, text: &str, time_created: u64, time_updated: u64) {
        let message_id = format!("msg-{}", Uuid::new_v4());
        let part_id = format!("part-{}", Uuid::new_v4());
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    (__MESSAGE_ID__, __SESSION_ID__, __TIME_CREATED__, __TIME_UPDATED__, __MESSAGE_DATA__),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    (__PART_ID__, __MESSAGE_ID__, __SESSION_ID__, __PART_CREATED__, __PART_UPDATED__, __PART_DATA__),
)
conn.commit()
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        script = script.replace("__MESSAGE_ID__", &json_string(&message_id));
        script = script.replace("__PART_ID__", &json_string(&part_id));
        script = script.replace("__SESSION_ID__", &json_string("sess-1"));
        script = script.replace("__TIME_CREATED__", &time_created.to_string());
        script = script.replace("__TIME_UPDATED__", &time_updated.to_string());
        script = script.replace(
            "__MESSAGE_DATA__",
            &json_string(r#"{"role":"user","id":"submit"}"#),
        );
        script = script.replace("__PART_CREATED__", &(time_created + 1).to_string());
        script = script.replace("__PART_UPDATED__", &time_updated.to_string());
        script = script.replace(
            "__PART_DATA__",
            &json_string(&format!(
                r#"{{"type":"text","text":{}}}"#,
                json_string(text)
            )),
        );
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to append submit row");
    }

    #[derive(Clone, Default)]
    struct RecordingBackend {
        db_path: PathBuf,
        buffer: Arc<Mutex<String>>,
        append_on_enter: bool,
        next_time: Arc<Mutex<u64>>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingBackend {
        fn new(db_path: PathBuf, append_on_enter: bool, start_time: u64) -> Self {
            Self {
                db_path,
                buffer: Arc::new(Mutex::new(String::new())),
                append_on_enter,
                next_time: Arc::new(Mutex::new(start_time)),
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
                    let mut next_time = self.next_time.lock().unwrap();
                    let created = *next_time + 1;
                    let updated = created + 1;
                    *next_time = updated;
                    append_user_submit(&self.db_path, &content, created, updated);
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

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
    }

    #[test]
    fn opencode_reader_finds_sessions_and_final_output() {
        let root = temp_dir("poll");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_test_db(&db_path);

        let candidates = opencode_db_candidates(&data_dir);
        assert_eq!(candidates, vec![db_path.clone()]);

        let source =
            find_opencode_session_by_id(Some("sess-1"), &candidates).expect("session lookup");
        assert_eq!(source.db_path, db_path);
        assert_eq!(source.session_id, "sess-1");

        let latest = find_latest_opencode_session_by_directory(Some("/repo/opencode"), &candidates)
            .expect("directory lookup");
        assert_eq!(latest.session_id, "sess-1");

        let drain = drain_opencode_session(&source, 0).expect("drain");
        assert_eq!(drain.events.len(), 2);
        assert_eq!(drain.events[0].kind, "user");
        assert_eq!(drain.events[0].text, "hello");
        assert_eq!(drain.events[1].kind, "assistant_final");
        assert_eq!(drain.events[1].text, "hi there");

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-1".to_string()),
            working_dir: "/repo/opencode".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
        };
        let first = poll(&mut state).expect("first poll");
        assert_eq!(first.final_output.as_deref(), Some("hi there"));
        assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
        assert!(first.prompt_ready);
        assert_eq!(state.transcript_offset, 1500);
        let second = poll(&mut state).expect("second poll");
        assert!(second.final_output.is_none());
        assert!(second.prompt_ready == false);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opencode_reader_dedupes_repeat_final_output_and_recovers_offset() {
        let root = temp_dir("dedupe");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_test_db(&db_path);

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-1".to_string()),
            working_dir: "/repo/opencode".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
        };
        let first = poll(&mut state).expect("first poll");
        assert_eq!(first.final_output.as_deref(), Some("hi there"));
        assert_eq!(state.transcript_offset, 1500);

        append_user_submit(&db_path, "hello opencode", 1600, 1601);
        let second = poll(&mut state).expect("second poll");
        assert!(second.final_output.is_none());

        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
DELETE FROM part;
DELETE FROM message;
""")
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ("msg-user-2", "sess-1", 2000, 2001, '{"role":"user","id":"msg-user-2"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-user-2", "msg-user-2", "sess-1", 2002, 2002, '{"type":"text","text":"fresh"}'),
)
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ("msg-asst-2", "sess-1", 2100, 2200, '{"role":"assistant","id":"msg-asst-2"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-asst-2", "msg-asst-2", "sess-1", 2190, 2190, '{"type":"text","text":"after truncate"}'),
)
conn.commit()
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to rewrite sqlite db");

        let third = poll(&mut state).expect("third poll");
        assert_eq!(third.final_output.as_deref(), Some("after truncate"));
        assert_eq!(third.final_output_kind, Some(FinalOutputKind::Bridge));
        assert_eq!(state.transcript_offset, 2200);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn opencode_write_input_verifies_transcript_before_reporting_success() {
        let root = temp_dir("submit");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER
);
CREATE TABLE message (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
CREATE TABLE part (
  id TEXT PRIMARY KEY,
  message_id TEXT,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
""")
conn.execute(
    "INSERT INTO session (id, directory, time_updated) VALUES (?, ?, ?)",
    ("sess-1", "/repo/opencode", 1000),
)
conn.commit()
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to create sqlite db");

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-1".to_string()),
            working_dir: "/repo/opencode".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
        };
        let backend = RecordingBackend::new(db_path.clone(), true, 1000);
        let result = write_input(&mut state, &backend, "hello opencode")
            .await
            .expect("write input");
        assert!(result.submitted);
        assert_eq!(result.cli_session_id.as_deref(), Some("sess-1"));
        assert!(backend.calls().iter().any(|call| call == "enter"));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn opencode_write_input_reports_failure_when_transcript_does_not_confirm() {
        let root = temp_dir("submit-fail");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        let mut script = String::from(
            r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER
);
CREATE TABLE message (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
CREATE TABLE part (
  id TEXT PRIMARY KEY,
  message_id TEXT,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
""")
conn.execute(
    "INSERT INTO session (id, directory, time_updated) VALUES (?, ?, ?)",
    ("sess-1", "/repo/opencode", 1000),
)
conn.commit()
"#,
        );
        script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
        let status = Command::new("python3")
            .args(["-c", &script])
            .status()
            .expect("python3 available");
        assert!(status.success(), "failed to create sqlite db");

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-1".to_string()),
            working_dir: "/repo/opencode".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
        };
        let backend = RecordingBackend::new(db_path.clone(), false, 1000);
        let result = write_input(&mut state, &backend, "hello opencode")
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
        let _ = fs::remove_dir_all(root);
    }
}
