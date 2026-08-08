//! SQLite transcript queries for opencode sessions.
//!
//! All SQLite access happens via embedded Python scripts that use the
//! `sqlite3` standard-library module against the opencode session DB files.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::types::{
    GroupedMessage, GroupedPart, OPENCODE_CURSOR_LOOKBACK_MS, OPENCODE_DIRECTORY_FALLBACK_LIMIT,
    OpenCodeBridgeEvent, OpenCodeDrainResult, OpenCodeMessageRow, OpenCodeSessionRow,
    OpenCodeTranscriptSource,
};

// ---------------------------------------------------------------------------
// Candidate DB discovery
// ---------------------------------------------------------------------------

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
    find_opencode_session_row_by_id(session_id, db_paths).map(|(db_path, row)| {
        OpenCodeTranscriptSource {
            db_path,
            session_id: row.id,
        }
    })
}

pub fn find_all_opencode_sessions_by_directory(
    directory: Option<&str>,
    db_paths: &[PathBuf],
) -> Vec<OpenCodeTranscriptSource> {
    let directory = match directory {
        Some(d) => d,
        None => return Vec::new(),
    };
    let mut results: Vec<OpenCodeTranscriptSource> = Vec::new();
    for db_path in db_paths {
        if !db_path.exists() {
            continue;
        }
        if let Ok(rows) = query_all_sessions_by_directory(db_path, directory) {
            for row in rows {
                results.push(OpenCodeTranscriptSource {
                    db_path: db_path.clone(),
                    session_id: row.id,
                });
            }
        }
    }
    results
}

pub(crate) fn find_opencode_session_row_by_id(
    session_id: Option<&str>,
    db_paths: &[PathBuf],
) -> Option<(PathBuf, OpenCodeSessionRow)> {
    let session_id = session_id?;
    for db_path in db_paths {
        if !db_path.exists() {
            continue;
        }
        if let Ok(Some(row)) = query_session_by_id(db_path, session_id) {
            return Some((db_path.clone(), row));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Drain / poll helpers
// ---------------------------------------------------------------------------

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
        bail!("{}", String::from_utf8_lossy(&proc.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&proc.stdout)
        .trim()
        .parse::<u64>()
        .unwrap_or(0))
}

pub(crate) fn opencode_submit_confirmed(
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

// ---------------------------------------------------------------------------
// Python-based SQLite helpers
// ---------------------------------------------------------------------------

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
    "SELECT id, directory, time_updated, time_archived, parent_id FROM session WHERE id = ? LIMIT 1",
    (__SESSION_ID__,),
).fetchone()
print(json.dumps(dict(row), ensure_ascii=False) if row else "null")
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    script = script.replace("__SESSION_ID__", &json_string(session_id));
    run_python_json(&script)
}

fn query_all_sessions_by_directory(
    db_path: &Path,
    directory: &str,
) -> Result<Vec<OpenCodeSessionRow>> {
    let mut script = format!(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    """
    SELECT id, directory, time_updated, time_archived, parent_id
    FROM session
    WHERE directory = ?
      AND time_archived IS NULL
      AND parent_id IS NULL
    ORDER BY time_updated DESC
    LIMIT {}
    """,
    (__DIRECTORY__,),
).fetchall()
print(json.dumps([dict(r) for r in rows], ensure_ascii=False))
"#,
        OPENCODE_DIRECTORY_FALLBACK_LIMIT
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    script = script.replace("__DIRECTORY__", &json_string(directory));
    run_python_json(&script)
}

// ---------------------------------------------------------------------------
// Message / part helpers
// ---------------------------------------------------------------------------

fn should_skip_assistant(data: &Value) -> bool {
    data.get("error").is_some() && data.get("summary").is_none()
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
        if assistant_final && let Some(completed) = time.get("completed").and_then(number_value) {
            return completed;
        }
        if let Some(created) = time.get("created").and_then(number_value) {
            return created;
        }
    }
    message.time_updated.max(message.time_created)
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

pub(crate) fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("json string")
}

pub(crate) fn run_python_json<T: DeserializeOwned>(script: &str) -> Result<T> {
    let proc = Command::new("python3")
        .args(["-c", script])
        .output()
        .context("failed to run python3")?;
    if !proc.status.success() {
        bail!("{}", String::from_utf8_lossy(&proc.stderr).trim());
    }
    let stdout = String::from_utf8_lossy(&proc.stdout).trim().to_string();
    if stdout.is_empty() {
        bail!("python3 returned empty output");
    }
    Ok(serde_json::from_str(&stdout)?)
}

// ---- transcript tail reader (used by disambiguation) ----

pub fn read_transcript_tail(source: &OpenCodeTranscriptSource) -> Result<String> {
    if !source.db_path.exists() {
        return Ok(String::new());
    }
    let mut script = String::from(
        r#"
import json
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
rows = conn.execute(
    """
    SELECT p.data
    FROM part p
    WHERE p.session_id = ?
    ORDER BY p.time_created DESC
    LIMIT __LIMIT__
    """,
    (__SESSION_ID__,),
).fetchall()
texts = []
for (data,) in rows:
    try:
        obj = json.loads(data)
        if obj.get('type') == 'text' and obj.get('text') and not obj.get('ignored'):
            texts.append(obj['text'])
    except:
        pass
texts.reverse()
print(json.dumps('\n'.join(texts), ensure_ascii=False))
"#,
    );
    script = script.replace(
        "__DB_PATH__",
        &json_string(&source.db_path.display().to_string()),
    );
    script = script.replace("__SESSION_ID__", &json_string(&source.session_id));
    script = script.replace(
        "__LIMIT__",
        &super::types::TRANSCRIPT_TAIL_PARTS.to_string(),
    );
    run_python_json(&script)
}
