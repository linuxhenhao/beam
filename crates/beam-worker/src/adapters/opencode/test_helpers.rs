//! Shared test helpers for the opencode adapter module.

use super::*;
use crate::backend::SessionBackend;
use async_trait::async_trait;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Temp dir
// ---------------------------------------------------------------------------

pub(crate) fn temp_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("beam-opencode-{}-{}", name, Uuid::new_v4()))
}

// ---------------------------------------------------------------------------
// DB creation helpers
// ---------------------------------------------------------------------------

pub(crate) fn create_test_db(db_path: &Path) {
    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER,
  time_archived INTEGER,
  parent_id TEXT
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

pub(crate) fn append_user_submit(
    db_path: &Path,
    session_id: &str,
    text: &str,
    time_created: u64,
    time_updated: u64,
) {
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
    script = script.replace("__SESSION_ID__", &json_string(session_id));
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

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct RecordingBackend {
    db_path: PathBuf,
    buffer: Arc<Mutex<String>>,
    append_on_enter: bool,
    next_time: Arc<Mutex<u64>>,
    calls: Arc<Mutex<Vec<String>>>,
    screen_content: Arc<Mutex<String>>,
    target_session_id: String,
}

impl RecordingBackend {
    pub(crate) fn new(db_path: PathBuf, append_on_enter: bool, start_time: u64) -> Self {
        Self {
            db_path,
            buffer: Arc::new(Mutex::new(String::new())),
            append_on_enter,
            next_time: Arc::new(Mutex::new(start_time)),
            calls: Arc::new(Mutex::new(Vec::new())),
            screen_content: Arc::new(Mutex::new(String::new())),
            target_session_id: "sess-1".to_string(),
        }
    }

    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub(crate) fn with_screen(self, content: String) -> Self {
        Self {
            screen_content: Arc::new(Mutex::new(content)),
            ..self
        }
    }

    pub(crate) fn with_target_session(mut self, id: impl Into<String>) -> Self {
        self.target_session_id = id.into();
        self
    }
}

#[async_trait]
impl SessionBackend for RecordingBackend {
    async fn spawn(
        &mut self,
        _bin: &str,
        _args: &[String],
        _opts: crate::backend::SpawnOpts,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send_text(&self, text: &str) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!("text:{text}"));
        self.buffer.lock().unwrap().push_str(text);
        Ok(())
    }

    async fn send_enter(&self) -> anyhow::Result<()> {
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
                append_user_submit(
                    &self.db_path,
                    &self.target_session_id,
                    &content,
                    created,
                    updated,
                );
            }
        }
        Ok(())
    }

    async fn send_special_keys(&self, _keys: &[String]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> anyhow::Result<()> {
        self.send_text(text).await
    }

    async fn write_raw(&self, _text: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn raw_input(&self, _text: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn capture_viewport(&self) -> anyhow::Result<String> {
        Ok(self.screen_content.lock().unwrap().clone())
    }

    async fn capture_current_screen(&self) -> anyhow::Result<String> {
        self.capture_viewport().await
    }

    async fn is_alive(&self) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn child_pid(&self) -> anyhow::Result<Option<u32>> {
        Ok(None)
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn destroy_session(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn cursor_position(&self) -> anyhow::Result<Option<(u16, u16)>> {
        Ok(None)
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
}

// ---------------------------------------------------------------------------
// Multi-session DB helpers
// ---------------------------------------------------------------------------

pub(crate) fn create_db_with_sessions(db_path: &Path, sessions: &[(&str, &str, u64)]) {
    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER,
  time_archived INTEGER,
  parent_id TEXT
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
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    for &(id, directory, time_updated) in sessions {
        script.push_str(&format!(
            "conn.execute(\"INSERT INTO session (id, directory, time_updated) VALUES (?, ?, ?)\", (\"{}\", \"{}\", {}))\n",
            id, directory, time_updated
        ));
    }
    script.push_str("conn.commit()\n");
    let status = Command::new("python3")
        .args(["-c", &script])
        .status()
        .expect("python3 available");
    assert!(status.success(), "failed to create multi-session sqlite db");
}

pub(crate) fn create_db_with_session_rows(
    db_path: &Path,
    sessions: &[(&str, &str, u64, Option<u64>, Option<&str>)],
) {
    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER,
  time_archived INTEGER,
  parent_id TEXT
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
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    for &(id, directory, time_updated, time_archived, parent_id) in sessions {
        let time_archived = time_archived
            .map(|value| value.to_string())
            .unwrap_or_else(|| "None".to_string());
        let parent_id = parent_id
            .map(json_string)
            .unwrap_or_else(|| "None".to_string());
        script.push_str(&format!(
            "conn.execute(\"INSERT INTO session (id, directory, time_updated, time_archived, parent_id) VALUES (?, ?, ?, ?, ?)\", ({}, {}, {}, {}, {}))\n",
            json_string(id),
            json_string(directory),
            time_updated,
            time_archived,
            parent_id
        ));
    }
    script.push_str("conn.commit()\n");
    let status = Command::new("python3")
        .args(["-c", &script])
        .status()
        .expect("python3 available");
    assert!(
        status.success(),
        "failed to create sqlite db with session rows"
    );
}

pub(crate) fn insert_message_with_text(
    db_path: &Path,
    session_id: &str,
    message_id: &str,
    role: &str,
    text: &str,
    time_created: u64,
    time_updated: u64,
) {
    let part_id = format!("{}-part", message_id);
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
    script = script.replace("__MESSAGE_ID__", &json_string(message_id));
    script = script.replace("__PART_ID__", &json_string(&part_id));
    script = script.replace("__SESSION_ID__", &json_string(session_id));
    script = script.replace("__TIME_CREATED__", &time_created.to_string());
    script = script.replace("__TIME_UPDATED__", &time_updated.to_string());
    script = script.replace(
        "__MESSAGE_DATA__",
        &json_string(&format!(r#"{{"role":"{}","id":"{}"}}"#, role, message_id)),
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
    assert!(status.success(), "failed to insert message with text");
}
