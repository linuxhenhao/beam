use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use beam_core::{FinalOutputKind, InitConfig};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::adapter::{OpenCodeState, PollResult, ResolveOutcome, SpawnSpec, SubmitResult};
use crate::backend::SessionBackend;
use tracing::{debug, info};

const OPENCODE_CURSOR_LOOKBACK_MS: u64 = 5_000;

// ---- disambiguation constants ----

/// Maximum chars from the screen tail used for scoring.
const SCREEN_TAIL_CHARS: usize = 500;
/// Maximum chars from the transcript tail used for scoring.
const TRANSCRIPT_TAIL_CHARS: usize = 1000;
/// How many recent text parts to read from a candidate transcript.
const TRANSCRIPT_TAIL_PARTS: usize = 20;
/// Minimum combined similarity score required to auto-select a candidate.
const MIN_DISAMBIGUATION_SCORE: f64 = 0.12;
/// Top score must be ≥ this ratio × second score to avoid near-ties.
const MIN_SCORE_LEAD_RATIO: f64 = 1.4;
/// Directory fallback should stay small and focus on active root sessions.
const OPENCODE_DIRECTORY_FALLBACK_LIMIT: usize = 10;

#[derive(Debug, Clone, Deserialize)]
pub struct OpenCodeTranscriptSource {
    pub db_path: PathBuf,
    pub session_id: String,
}

/// Resolution of a transcript source lookup for opencode sessions.
pub type OpenCodeSourceResolution = ResolveOutcome<OpenCodeTranscriptSource>;

#[derive(Debug, Clone, Deserialize)]
struct OpenCodeSessionRow {
    id: String,
    directory: String,
    time_archived: Option<u64>,
    parent_id: Option<String>,
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
    let adopted_pid = init
        .adopted_from
        .as_ref()
        .and_then(|a| u32::try_from(a.original_cli_pid).ok());
    OpenCodeState {
        data_dir,
        expected_session_id,
        working_dir: init.working_dir.clone(),
        cli_session_id: init.cli_session_id.clone(),
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid,
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
    let source = match wait_for_source(state).await {
        ResolveOutcome::Found(source) => source,
        ResolveOutcome::Ambiguous { candidates, reason } => {
            // Try screen vs transcript disambiguation before giving up.
            match disambiguate_by_screen(state, backend, &candidates).await {
                Ok(Some(source)) => source,
                Ok(None) | Err(_) => {
                    return Ok(SubmitResult {
                        submitted: false,
                        cli_session_id: state.cli_session_id.clone(),
                        failure_reason: Some(reason),
                    });
                }
            }
        }
        ResolveOutcome::NotFound { reason } => {
            return Ok(SubmitResult {
                submitted: false,
                cli_session_id: state.cli_session_id.clone(),
                failure_reason: Some(reason),
            });
        }
    };
    let base_offset = current_opencode_session_offset(&source)?;
    state.cli_session_id = Some(source.session_id.clone());
    state.expected_session_id = Some(source.session_id.clone());

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
    let source = match source {
        ResolveOutcome::Found(source) => source,
        ResolveOutcome::Ambiguous { .. } | ResolveOutcome::NotFound { .. } => {
            // Do not auto-bind cli_session_id on ambiguous / not-found.
            return Ok(PollResult {
                cli_session_id: state.cli_session_id.clone(),
                ..Default::default()
            });
        }
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

fn find_opencode_session_row_by_id(
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

/// Parse an opencode session id from `/proc/<pid>/cmdline` by looking for a
/// `--session <id>` argument pair.
///
/// Returns `None` when the flag is absent; this is common for sessions that
/// were started without an explicit `--session`.
#[cfg(target_os = "linux")]
fn try_resolve_opencode_session_via_cmdline(pid: u32) -> Option<String> {
    let raw = std::fs::read(format!("/proc/{}/cmdline", pid)).ok()?;
    parse_session_from_cmdline(&raw)
}

/// Pure parser: extract `--session <id>` from null-separated cmdline bytes.
fn parse_session_from_cmdline(raw: &[u8]) -> Option<String> {
    let args: Vec<&str> = raw
        .split(|b| *b == 0)
        .filter_map(|b| std::str::from_utf8(b).ok())
        .filter(|s| !s.is_empty())
        .collect();
    for window in args.windows(2) {
        if window[0] == "--session" {
            return Some(window[1].to_string());
        }
    }
    None
}

/// When the worker was initialised for an adopted opencode process (the
/// `adopted_pid` field is set), attempt to resolve the session by stronger
/// signals than directory-based candidate collection alone.
///
/// Resolution order (on Linux):
/// 1. **Strong mapping**: read `/proc/<pid>/cmdline`, look for `--session <id>`.
///    If found, look up that exact session id — this is a reliable PID→session
///    link.  Return `Found` or `NotFound`.
/// 2. **Liveness gate**: if `/proc/<pid>` does not exist the adopted process
///    has died.  Return `NotFound` to fail safe rather than silently binding an
///    arbitrary historical session.
/// 3. **No mapping, but alive**: fall through by returning `None`.  The caller
///    will proceed with normal directory-based disambiguation (including
///    screen-vs-transcript scoring in [`write_input`]) — it will *not* auto-select
///    a single candidate just because the PID is alive.
///
/// On non-Linux we cannot verify liveness; the function returns `None` and
/// normal directory disambiguation is used.
#[cfg(target_os = "linux")]
fn try_adopted_pid_filter(
    state: &OpenCodeState,
    db_paths: &[PathBuf],
) -> Option<OpenCodeSourceResolution> {
    let pid = state.adopted_pid?;

    // 1. Strong PID→session mapping via --session in cmdline.
    if let Some(session_id) = try_resolve_opencode_session_via_cmdline(pid) {
        if let Some(source) = find_opencode_session_by_id(Some(&session_id), db_paths) {
            return Some(ResolveOutcome::Found(source));
        }
        return Some(ResolveOutcome::NotFound {
            reason: format!(
                "opencode session {} (from pid {} cmdline) not found in db",
                session_id, pid
            ),
        });
    }

    // 2. Liveness gate: dead process → fail safe.
    if !is_process_alive(pid) {
        return Some(ResolveOutcome::NotFound {
            reason: format!(
                "adopted opencode process (pid {}) is no longer running",
                pid
            ),
        });
    }

    // 3. Alive but no reliable session mapping → do NOT auto-select.
    // Fall through to normal directory-based disambiguation.
    None
}

#[cfg(not(target_os = "linux"))]
fn try_adopted_pid_filter(
    _state: &OpenCodeState,
    _db_paths: &[PathBuf],
) -> Option<OpenCodeSourceResolution> {
    // Cannot verify process liveness on non-Linux; fall through to
    // normal directory-based disambiguation.
    None
}

#[cfg(target_os = "linux")]
fn is_process_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{}", pid)).exists()
}

pub(crate) fn current_source(state: &OpenCodeState) -> OpenCodeSourceResolution {
    let db_paths = opencode_db_candidates(&state.data_dir);
    if let Some(source) =
        find_opencode_session_by_id(state.expected_session_id.as_deref(), &db_paths)
    {
        return ResolveOutcome::Found(source);
    }
    // When adopting, try stronger PID-based resolution; if that cannot
    // produce a definitive answer, fall through to normal cwd disambiguation
    // (including screen/transcript scoring in write_input).
    if let Some(filtered) = try_adopted_pid_filter(state, &db_paths) {
        return filtered;
    }
    if let Some(source) = resolve_opencode_session_via_logs(state, &db_paths) {
        return ResolveOutcome::Found(source);
    }
    let candidates = find_all_opencode_sessions_by_directory(Some(&state.working_dir), &db_paths);
    match candidates.len() {
        0 => ResolveOutcome::NotFound {
            reason: "OpenCode transcript source not found".to_string(),
        },
        1 => ResolveOutcome::Found(candidates.into_iter().next().unwrap()),
        n => ResolveOutcome::Ambiguous {
            candidates,
            reason: format!(
                "OpenCode transcript source ambiguous: {} sessions in directory {}",
                n, state.working_dir
            ),
        },
    }
}

async fn wait_for_source(state: &OpenCodeState) -> OpenCodeSourceResolution {
    for _ in 0..12 {
        let resolution = current_source(state);
        if matches!(resolution, ResolveOutcome::Found(_)) {
            return resolution;
        }
        if matches!(resolution, ResolveOutcome::Ambiguous { .. }) {
            return resolution;
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

fn recent_opencode_session_ids(log_dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return Vec::new();
    };
    let mut files: Vec<(u128, PathBuf)> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                return None;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok())
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            Some((modified, path))
        })
        .collect();
    files.sort_by(|a, b| b.0.cmp(&a.0));

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, path) in files {
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in content.lines().rev() {
            let mut search_start = 0;
            while let Some(pos) = line[search_start..].find("session.id=") {
                let id_start = search_start + pos + "session.id=".len();
                let id = line[id_start..]
                    .chars()
                    .take_while(|ch| {
                        !ch.is_whitespace()
                            && *ch != '"'
                            && *ch != '\''
                            && *ch != ','
                            && *ch != ')'
                            && *ch != ']'
                            && *ch != '}'
                    })
                    .collect::<String>();
                if !id.is_empty() && seen.insert(id.clone()) {
                    ids.push(id);
                }
                search_start = id_start;
            }
        }
    }
    ids
}

fn session_matches_working_dir_and_is_root(row: &OpenCodeSessionRow, working_dir: &str) -> bool {
    row.directory == working_dir && row.time_archived.is_none() && row.parent_id.is_none()
}

fn resolve_opencode_session_via_logs(
    state: &OpenCodeState,
    db_paths: &[PathBuf],
) -> Option<OpenCodeTranscriptSource> {
    let log_dir = state.data_dir.join("log");
    for session_id in recent_opencode_session_ids(&log_dir) {
        if let Some((db_path, row)) = find_opencode_session_row_by_id(Some(&session_id), db_paths)
            && session_matches_working_dir_and_is_root(&row, &state.working_dir)
        {
            return Some(OpenCodeTranscriptSource {
                db_path,
                session_id: row.id,
            });
        }
    }
    None
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

// ---- screen vs transcript disambiguation helpers ----

/// Resolve transcript source at init/adopt time, with screen disambiguation.
/// Returns candidates for user selection when automatic resolution fails.
pub(crate) async fn resolve_transcript_source(
    state: &OpenCodeState,
    backend: &dyn SessionBackend,
) -> Result<OpenCodeSourceResolution> {
    let resolution = current_source(state);
    match resolution {
        ResolveOutcome::Ambiguous { candidates, reason } => {
            match disambiguate_by_screen(state, backend, &candidates).await {
                Ok(Some(source)) => Ok(ResolveOutcome::Found(source)),
                _ => Ok(ResolveOutcome::Ambiguous { candidates, reason }),
            }
        }
        other => Ok(other),
    }
}

/// Strip ANSI escape sequences (CSI + OSC) from terminal output.
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\x1b' {
            out.push(ch);
            continue;
        }
        match chars.peek() {
            Some('[') => {
                chars.next(); // consume '['
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_alphanumeric() || c == ';' {
                        chars.next();
                        if c.is_ascii_alphabetic() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            Some(']') => {
                chars.next(); // consume ']'
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c == '\x07' || (c == '\x1b' && chars.peek() == Some(&'\\')) {
                        if c == '\x1b' {
                            chars.next(); // consume '\\'
                        }
                        break;
                    }
                }
            }
            _ => {
                // unrecognized escape – consume the ESC itself
            }
        }
    }
    out
}

/// Normalize text for similarity scoring: strip ANSI, collapse whitespace, truncate to tail.
///
/// `tail_chars` counts Rust `char`s (Unicode scalar values), not bytes, so
/// multi-byte sequences (Chinese, emoji, etc.) are safe.
fn normalize_for_scoring(text: &str, tail_chars: usize) -> String {
    let plain = strip_ansi(text);
    let collapsed: String = plain.split_whitespace().collect::<Vec<_>>().join(" ");
    let char_count = collapsed.chars().count();
    if char_count <= tail_chars {
        return collapsed;
    }
    // Take the suffix of approximately tail_chars characters.
    let skip = char_count - tail_chars;
    let tail: String = collapsed.chars().skip(skip).collect();
    // Prefer to start at a word boundary: skip the first (possibly partial)
    // word and start after the next space.
    if let Some(space_pos) = tail.find(' ') {
        // `find(' ')` returns a byte index – safe because `tail` was built
        // via `collect()` and `space_pos` + 1 always lands on a char boundary.
        tail[space_pos + 1..].to_string()
    } else {
        tail
    }
}

/// Compute a combined similarity score between two normalized text blobs.
/// Uses Jaccard on word sets (0.5 weight) + bigram overlap (0.5 weight).
fn score_texts(screen: &str, transcript: &str) -> f64 {
    if screen.is_empty() || transcript.is_empty() {
        return 0.0;
    }

    let sw: Vec<&str> = screen.split_whitespace().collect();
    let tw: Vec<&str> = transcript.split_whitespace().collect();
    if sw.is_empty() || tw.is_empty() {
        return 0.0;
    }

    let screen_set: HashSet<&str> = sw.iter().copied().collect();
    let transcript_set: HashSet<&str> = tw.iter().copied().collect();
    let inter = screen_set.intersection(&transcript_set).count() as f64;
    let union = screen_set.union(&transcript_set).count() as f64;
    let jaccard = inter / union.max(1.0);

    let screen_bigrams: HashSet<(&str, &str)> = sw.windows(2).map(|w| (w[0], w[1])).collect();
    let transcript_bigrams: HashSet<(&str, &str)> = tw.windows(2).map(|w| (w[0], w[1])).collect();
    let bg_inter = screen_bigrams.intersection(&transcript_bigrams).count() as f64;
    let bg_union = screen_bigrams.union(&transcript_bigrams).count() as f64;
    let bigram = bg_inter / bg_union.max(1.0);

    (jaccard + bigram) / 2.0
}

/// Read the last N text parts from an opencode session transcript, joined by newlines.
fn read_transcript_tail(source: &OpenCodeTranscriptSource) -> Result<String> {
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
    script = script.replace("__LIMIT__", &TRANSCRIPT_TAIL_PARTS.to_string());
    run_python_json(&script)
}

/// Try to resolve an ambiguous multi-candidate situation by comparing the
/// current TUI viewport content against each candidate's transcript tail.
///
/// Returns `Some(source)` when one candidate clearly stands out; returns
/// `None` when scores are too low or too close to call.
async fn disambiguate_by_screen(
    _state: &OpenCodeState,
    backend: &dyn SessionBackend,
    candidates: &[OpenCodeTranscriptSource],
) -> Result<Option<OpenCodeTranscriptSource>> {
    if candidates.len() < 2 {
        return Ok(candidates.first().cloned());
    }

    let screen = backend.capture_viewport().await.unwrap_or_default();
    let screen_tail = normalize_for_scoring(&screen, SCREEN_TAIL_CHARS);
    if screen_tail.is_empty() {
        return Ok(None);
    }

    let mut scored: Vec<(f64, &OpenCodeTranscriptSource)> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let transcript_text = read_transcript_tail(candidate).unwrap_or_default();
        let transcript_tail = normalize_for_scoring(&transcript_text, TRANSCRIPT_TAIL_CHARS);
        let score = score_texts(&screen_tail, &transcript_tail);
        scored.push((score, candidate));
    }

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let &(top_score, top_source) = &scored[0];

    let score_too_low = top_score < MIN_DISAMBIGUATION_SCORE;
    let score_gap_too_small = scored.len() > 1 && {
        let second_score = scored[1].0;
        second_score > 0.0 && top_score / second_score < MIN_SCORE_LEAD_RATIO
    };

    if score_too_low || score_gap_too_small {
        debug!(
            "screen disambiguation rejected: candidates={} top_score={:.3}<{:.3} or scores too close",
            candidates.len(),
            top_score,
            MIN_DISAMBIGUATION_SCORE
        );
        return Ok(None);
    }

    info!(
        "screen disambiguation selected session {} (score={:.3}, lead={:.2}x)",
        top_source.session_id,
        top_score,
        top_score / scored.get(1).map(|s| s.0).unwrap_or(1.0)
    );

    Ok(Some(top_source.clone()))
}

// ---- bridge event types ----

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

    fn append_user_submit(
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

    #[derive(Clone)]
    struct RecordingBackend {
        db_path: PathBuf,
        buffer: Arc<Mutex<String>>,
        append_on_enter: bool,
        next_time: Arc<Mutex<u64>>,
        calls: Arc<Mutex<Vec<String>>>,
        screen_content: Arc<Mutex<String>>,
        target_session_id: String,
    }

    impl RecordingBackend {
        fn new(db_path: PathBuf, append_on_enter: bool, start_time: u64) -> Self {
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

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn with_screen(self, content: String) -> Self {
            Self {
                screen_content: Arc::new(Mutex::new(content)),
                ..self
            }
        }

        fn with_target_session(mut self, id: impl Into<String>) -> Self {
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
            Ok(self.screen_content.lock().unwrap().clone())
        }

        async fn capture_current_screen(&self) -> Result<String> {
            self.capture_viewport().await
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

        let all = find_all_opencode_sessions_by_directory(Some("/repo/opencode"), &candidates);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].session_id, "sess-1");

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
            adopted_pid: None,
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
            adopted_pid: None,
        };
        let first = poll(&mut state).expect("first poll");
        assert_eq!(first.final_output.as_deref(), Some("hi there"));
        assert_eq!(state.transcript_offset, 1500);

        append_user_submit(&db_path, "sess-1", "hello opencode", 1600, 1601);
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
            adopted_pid: None,
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
            adopted_pid: None,
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

    // Helper: create a DB with multiple sessions sharing the same directory.
    fn create_db_with_sessions(db_path: &Path, sessions: &[(&str, &str, u64)]) {
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

    fn create_db_with_session_rows(
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

    // Helper: insert a message + text part for a session into an existing DB.
    fn insert_message_with_text(
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

    #[test]
    fn opencode_source_resolution_no_candidates_returns_not_found() {
        let root = temp_dir("res-no-candidates");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/nonexistent/dir".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        let resolution = current_source(&state);
        assert!(
            matches!(resolution, ResolveOutcome::NotFound { .. }),
            "expected NotFound, got {:?}",
            resolution
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opencode_source_resolution_single_candidate_returns_found_with_session_backfill() {
        let root = temp_dir("res-single-candidate");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(&db_path, &[("sess-abc", "/repo/single-project", 2000)]);

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/single-project".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        // poll should backfill cli_session_id when exactly one candidate
        let result = poll(&mut state).expect("poll");
        assert!(result.final_output.is_none());
        assert_eq!(state.cli_session_id.as_deref(), Some("sess-abc"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opencode_source_resolution_multiple_candidates_returns_ambiguous_no_auto_bind() {
        let root = temp_dir("res-multi-candidates");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-old", "/repo/shared", 1000),
                ("sess-new", "/repo/shared", 2000),
            ],
        );

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/shared".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        // poll should NOT auto-bind when ambiguous
        let result = poll(&mut state).expect("poll");
        assert!(result.cli_session_id.is_none());
        assert_eq!(state.cli_session_id, None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opencode_source_resolution_expected_session_id_exact_match_overrides_ambiguity() {
        let root = temp_dir("res-exact-match");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-one", "/repo/shared-2", 1000),
                ("sess-two", "/repo/shared-2", 2000),
                ("sess-three", "/repo/shared-2", 3000),
            ],
        );

        // Without expected_session_id → ambiguous (3 candidates)
        let state_no_expect = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/shared-2".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        let resolution = current_source(&state_no_expect);
        assert!(
            matches!(resolution, ResolveOutcome::Ambiguous { ref candidates, .. } if candidates.len() == 3),
            "expected Ambiguous with 3, got {:?}",
            resolution
        );

        // With expected_session_id pointing to sess-one → Found (exact match)
        let state_exact = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-one".to_string()),
            working_dir: "/repo/shared-2".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        let resolution = current_source(&state_exact);
        match resolution {
            ResolveOutcome::Found(source) => assert_eq!(source.session_id, "sess-one"),
            other => panic!("expected Found(sess-one), got {:?}", other),
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opencode_source_resolution_directory_fallback_filters_and_caps_results() {
        let root = temp_dir("res-dir-cap");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_session_rows(
            &db_path,
            &[
                ("sess-01", "/repo/capped", 1001, None, None),
                ("sess-02", "/repo/capped", 1002, None, None),
                ("sess-03", "/repo/capped", 1003, None, None),
                ("sess-04", "/repo/capped", 1004, None, None),
                ("sess-05", "/repo/capped", 1005, None, None),
                ("sess-06", "/repo/capped", 1006, None, None),
                ("sess-07", "/repo/capped", 1007, None, None),
                ("sess-08", "/repo/capped", 1008, None, None),
                ("sess-09", "/repo/capped", 1009, None, None),
                ("sess-10", "/repo/capped", 1010, None, None),
                ("sess-11", "/repo/capped", 1011, None, None),
                ("sess-12", "/repo/capped", 1012, None, None),
                ("sess-archived", "/repo/capped", 9999, Some(10000), None),
                ("sess-child", "/repo/capped", 9998, None, Some("sess-11")),
            ],
        );

        let candidates = opencode_db_candidates(&data_dir);
        let all = find_all_opencode_sessions_by_directory(Some("/repo/capped"), &candidates);
        let session_ids: Vec<String> = all.iter().map(|source| source.session_id.clone()).collect();
        assert_eq!(
            session_ids,
            vec![
                "sess-12".to_string(),
                "sess-11".to_string(),
                "sess-10".to_string(),
                "sess-09".to_string(),
                "sess-08".to_string(),
                "sess-07".to_string(),
                "sess-06".to_string(),
                "sess-05".to_string(),
                "sess-04".to_string(),
                "sess-03".to_string(),
            ]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opencode_source_resolution_logs_prefer_recent_matching_session() {
        let root = temp_dir("res-log");
        let data_dir = root.join("share").join("opencode");
        let log_dir = data_dir.join("log");
        fs::create_dir_all(&log_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_session_rows(
            &db_path,
            &[
                ("sess-old", "/repo/logged", 1000, None, None),
                ("sess-new", "/repo/logged", 2000, None, None),
            ],
        );
        fs::write(
            log_dir.join("worker.log"),
            "2026-07-05T12:00:00Z session.id=sess-old\n2026-07-05T12:01:00Z session.id=sess-new\n",
        )
        .unwrap();

        let state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/logged".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        let resolution = current_source(&state);
        match resolution {
            ResolveOutcome::Found(source) => assert_eq!(source.session_id, "sess-new"),
            other => panic!("expected Found(sess-new), got {:?}", other),
        }
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn opencode_write_input_returns_ambiguous_failure_for_multiple_candidates() {
        let root = temp_dir("submit-ambiguous");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-a", "/repo/ambiguous-project", 1000),
                ("sess-b", "/repo/ambiguous-project", 2000),
            ],
        );

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/ambiguous-project".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        let backend = RecordingBackend::new(db_path.clone(), false, 1000);
        let result = write_input(&mut state, &backend, "hello")
            .await
            .expect("write input");
        assert!(!result.submitted);
        let reason = result.failure_reason.as_deref().unwrap_or("");
        assert!(
            reason.contains("ambiguous"),
            "expected 'ambiguous' in: {}",
            reason
        );
        assert!(
            reason.contains("2 sessions"),
            "expected '2 sessions' in: {}",
            reason
        );
        let _ = fs::remove_dir_all(root);
    }

    // ---- screen vs transcript disambiguation tests ----

    #[tokio::test]
    async fn disambiguation_screen_matches_one_candidate_clearly_selects_it() {
        let root = temp_dir("disambig-clear");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        // Two sessions in same dir with distinct transcript content.
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-a", "/repo/disambig", 1000),
                ("sess-b", "/repo/disambig", 2000),
            ],
        );
        // Session A: assistant says "The tests all passed, 42 assertions succeeded."
        insert_message_with_text(
            &db_path,
            "sess-a",
            "msg-a1",
            "user",
            "run tests please",
            100,
            101,
        );
        insert_message_with_text(
            &db_path,
            "sess-a",
            "msg-a2",
            "assistant",
            "The tests all passed, 42 assertions succeeded.",
            200,
            201,
        );
        // Session B: assistant says "Deployment completed to production successfully."
        insert_message_with_text(&db_path, "sess-b", "msg-b1", "user", "deploy app", 100, 101);
        insert_message_with_text(
            &db_path,
            "sess-b",
            "msg-b2",
            "assistant",
            "Deployment completed to production successfully.",
            200,
            201,
        );

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/disambig".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        // screen contains text unique to sess-a
        let backend = RecordingBackend::new(db_path.clone(), true, 2001)
            .with_target_session("sess-a")
            .with_screen("The tests all passed, 42 assertions succeeded.".to_string());
        let result = write_input(&mut state, &backend, "next command")
            .await
            .expect("write input");
        assert!(result.submitted, "should auto-select sess-a and submit");
        assert_eq!(
            result.cli_session_id.as_deref(),
            Some("sess-a"),
            "should bind to sess-a"
        );
        assert_eq!(
            state.expected_session_id.as_deref(),
            Some("sess-a"),
            "poll should reuse the disambiguated source"
        );
        insert_message_with_text(
            &db_path,
            "sess-a",
            "msg-a3",
            "assistant",
            "selected session answer",
            3000,
            3001,
        );
        let poll = poll(&mut state).expect("poll");
        assert_eq!(
            poll.final_output.as_deref(),
            Some("selected session answer")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn disambiguation_screen_does_not_match_weakly_stays_ambiguous() {
        let root = temp_dir("disambig-weak");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-x", "/repo/disambig2", 1000),
                ("sess-y", "/repo/disambig2", 2000),
            ],
        );
        insert_message_with_text(&db_path, "sess-x", "msg-x1", "user", "run tests", 100, 101);
        insert_message_with_text(
            &db_path,
            "sess-x",
            "msg-x2",
            "assistant",
            "All tests passed.",
            200,
            201,
        );
        insert_message_with_text(
            &db_path,
            "sess-y",
            "msg-y1",
            "user",
            "build project",
            100,
            101,
        );
        insert_message_with_text(
            &db_path,
            "sess-y",
            "msg-y2",
            "assistant",
            "Build completed successfully.",
            200,
            201,
        );

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/disambig2".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        // screen contains unrelated text – neither candidate will score well.
        let backend = RecordingBackend::new(db_path.clone(), false, 2001)
            .with_screen("some totally unrelated content here".to_string());
        let result = write_input(&mut state, &backend, "cmd")
            .await
            .expect("write input");
        assert!(!result.submitted, "should stay ambiguous");
        let reason = result.failure_reason.as_deref().unwrap_or("");
        assert!(
            reason.contains("ambiguous"),
            "expected ambiguous, got: {}",
            reason
        );
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn disambiguation_expected_session_id_still_takes_priority() {
        let root = temp_dir("disambig-exact");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-one", "/repo/disambig3", 1000),
                ("sess-two", "/repo/disambig3", 2000),
            ],
        );
        insert_message_with_text(
            &db_path,
            "sess-one",
            "msg-o1",
            "user",
            "hello one",
            100,
            101,
        );
        insert_message_with_text(
            &db_path,
            "sess-one",
            "msg-o2",
            "assistant",
            "response one",
            200,
            201,
        );
        insert_message_with_text(
            &db_path,
            "sess-two",
            "msg-t1",
            "user",
            "hello two",
            100,
            101,
        );
        insert_message_with_text(
            &db_path,
            "sess-two",
            "msg-t2",
            "assistant",
            "detailed response from session two about deployment",
            200,
            201,
        );

        // state has expected_session_id pointing to sess-one
        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-one".to_string()),
            working_dir: "/repo/disambig3".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: None,
        };
        // screen matches sess-two much better, but expected_session_id should win
        let backend = RecordingBackend::new(db_path.clone(), true, 2001)
            .with_target_session("sess-one")
            .with_screen("detailed response from session two about deployment".to_string());
        let result = write_input(&mut state, &backend, "cmd")
            .await
            .expect("write input");
        assert!(result.submitted, "should submit via expected_session_id");
        assert_eq!(
            result.cli_session_id.as_deref(),
            Some("sess-one"),
            "expected_session_id exact match should take priority"
        );
        let _ = fs::remove_dir_all(root);
    }

    // ---- normalize_for_scoring char-safety tests ----

    #[test]
    fn normalize_chinese_text_does_not_panic_and_preserves_tail() {
        // Chinese chars are 3 bytes each in UTF-8. The old byte-index
        // slicing would likely panic on a non-char boundary.
        let text = "你好世界这是一段很长的中文文本用于测试截断功能是否正常工作";
        let result = normalize_for_scoring(text, 10);
        assert!(!result.is_empty(), "should return non-empty tail");
        assert!(
            result.chars().count() <= 10,
            "tail should be at most 10 chars (or slightly more if word-boundary snap moved start)"
        );
        // Ensure the result is valid UTF-8 (no panic already proves this).
        let _ = result.chars().collect::<Vec<_>>();
    }

    #[test]
    fn normalize_emoji_text_does_not_panic() {
        // Emoji like 🦀 are 4 bytes each. Old code would panic at byte 500.
        let mut long_emoji = String::new();
        for _ in 0..200 {
            long_emoji.push_str("🦀🌟🔥");
        }
        let result = normalize_for_scoring(&long_emoji, 100);
        assert!(
            result.chars().count() <= 100,
            "tail should be ≤ 100 chars, got {}",
            result.chars().count()
        );
    }

    #[test]
    fn normalize_mixed_ascii_chinese_does_not_panic() {
        let text = "English followed by 中文内容 mixed together 混合内容 ".repeat(20);
        let result = normalize_for_scoring(&text, 50);
        assert!(!result.is_empty());
        assert!(result.chars().count() <= 50);
        // Should contain content from near the end
        assert!(result.contains("混合") || result.contains("together"));
    }

    #[test]
    fn normalize_shorter_than_tail_returns_unchanged() {
        let text = "short text";
        let result = normalize_for_scoring(text, 500);
        // After ANSI strip + whitespace collapse, it should be identical.
        assert_eq!(result, "short text");
    }

    #[test]
    fn normalize_tail_with_no_space_returns_full_tail() {
        // A long line with no spaces (e.g., a single very long word) —
        // the word-boundary snap won't fire, and we get the raw tail.
        let text = "X".repeat(1000);
        let result = normalize_for_scoring(&text, 20);
        assert_eq!(result.chars().count(), 20);
        // All chars should be 'X'
        assert!(result.chars().all(|c| c == 'X'));
    }

    // ---- adopted_pid filtering tests ----

    // unit tests for cmdline parser (pure, no /proc needed)

    #[test]
    fn parse_cmdline_session_extracts_id_after_flag() {
        // Simulate /proc/<pid>/cmdline: null-separated "opencode\0--session\0abc-123\0"
        let raw = b"opencode\0--session\0abc-123\0";
        assert_eq!(parse_session_from_cmdline(raw), Some("abc-123".to_string()));
    }

    #[test]
    fn parse_cmdline_session_flag_with_trailing_args() {
        let raw = b"opencode\0--model\0gpt-4\0--session\0sess-x\0--prompt\0hello\0";
        assert_eq!(parse_session_from_cmdline(raw), Some("sess-x".to_string()));
    }

    #[test]
    fn parse_cmdline_session_flag_absent_returns_none() {
        let raw = b"opencode\0--model\0gpt-4\0";
        assert_eq!(parse_session_from_cmdline(raw), None);
    }

    #[test]
    fn parse_cmdline_session_empty_input_returns_none() {
        assert_eq!(parse_session_from_cmdline(b""), None);
    }

    // ---- adopted_pid + current_source tests (Linux) ----

    #[cfg(target_os = "linux")]
    #[test]
    fn adopted_pid_alive_without_session_mapping_returns_ambiguous() {
        let root = temp_dir("adopt-alive-ambig");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        // Three sessions in the same directory.
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-historic", "/repo/adopt", 1000),
                ("sess-middle", "/repo/adopt", 2000),
                ("sess-current", "/repo/adopt", 3000),
            ],
        );

        // alive pid (test process) but cmdline has no --session flag.
        // try_adopted_pid_filter must fall through to normal cwd
        // disambiguation → Ambiguous.
        let state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/adopt".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: Some(std::process::id()),
        };
        let resolution = current_source(&state);
        assert!(
            matches!(
                resolution,
                ResolveOutcome::Ambiguous { ref candidates, .. }
                if candidates.len() == 3
            ),
            "alive pid without --session mapping should remain Ambiguous (3 candidates), got {:?}",
            resolution
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn adopted_pid_dead_returns_not_found() {
        let root = temp_dir("adopt-dead");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-a", "/repo/dead", 1000),
                ("sess-b", "/repo/dead", 2000),
            ],
        );

        // Use u32::MAX — far above Linux pid_max, so /proc/<pid> won't exist.
        let dead_pid: u32 = u32::MAX;
        assert!(
            !is_process_alive(dead_pid),
            "u32::MAX should not be a live PID"
        );

        let state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/dead".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: Some(dead_pid),
        };
        let resolution = current_source(&state);
        assert!(
            matches!(resolution, ResolveOutcome::NotFound { .. }),
            "dead adopted pid should yield NotFound, got {:?}",
            resolution
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn expected_session_id_overrides_adopted_pid_filter() {
        let root = temp_dir("adopt-override");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-old", "/repo/override", 1000),
                ("sess-new", "/repo/override", 2000),
            ],
        );

        // Adopted pid alive, but expected_session_id points to an older session.
        // Exact match must still win.
        let state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: Some("sess-old".to_string()),
            working_dir: "/repo/override".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: Some(std::process::id()),
        };
        let resolution = current_source(&state);
        match resolution {
            ResolveOutcome::Found(source) => {
                assert_eq!(
                    source.session_id, "sess-old",
                    "expected_session_id exact match must take priority"
                );
            }
            other => panic!("expected Found(sess-old), got {:?}", other),
        }
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn adopted_pid_alive_no_candidates_returns_not_found() {
        let root = temp_dir("adopt-empty");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        // No DB at all — zero candidates.  Alive pid with no --session mapping
        // falls through to normal cwd disambiguation, which returns NotFound.

        let state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/empty".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: Some(std::process::id()),
        };
        let resolution = current_source(&state);
        assert!(
            matches!(resolution, ResolveOutcome::NotFound { .. }),
            "alive pid with no sessions should yield NotFound, got {:?}",
            resolution
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn adopted_pid_alive_screen_disambiguation_still_works_in_write_input() {
        let root = temp_dir("adopt-screen-disambig");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        // Two sessions in same dir with distinct transcript content.
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-a", "/repo/adopt-disambig", 1000),
                ("sess-b", "/repo/adopt-disambig", 2000),
            ],
        );
        // Session A: "The tests all passed, 42 assertions succeeded."
        insert_message_with_text(
            &db_path,
            "sess-a",
            "msg-a1",
            "user",
            "run tests please",
            100,
            101,
        );
        insert_message_with_text(
            &db_path,
            "sess-a",
            "msg-a2",
            "assistant",
            "The tests all passed, 42 assertions succeeded.",
            200,
            201,
        );
        // Session B: "Deployment completed to production successfully."
        insert_message_with_text(&db_path, "sess-b", "msg-b1", "user", "deploy app", 100, 101);
        insert_message_with_text(
            &db_path,
            "sess-b",
            "msg-b2",
            "assistant",
            "Deployment completed to production successfully.",
            200,
            201,
        );

        // alive pid, no --session mapping → current_source returns Ambiguous(2).
        // write_input should then run screen disambiguation.  Screen matches
        // sess-a, so write_input succeeds and binds to sess-a.
        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/adopt-disambig".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: Some(std::process::id()),
        };
        let backend = RecordingBackend::new(db_path.clone(), true, 2001)
            .with_target_session("sess-a")
            .with_screen("The tests all passed, 42 assertions succeeded.".to_string());
        let result = write_input(&mut state, &backend, "next command")
            .await
            .expect("write input");
        assert!(
            result.submitted,
            "screen disambiguation should auto-select sess-a"
        );
        assert_eq!(
            result.cli_session_id.as_deref(),
            Some("sess-a"),
            "should bind to sess-a via screen disambiguation"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn adopted_pid_alive_weak_screen_match_stays_ambiguous() {
        let root = temp_dir("adopt-screen-weak");
        let data_dir = root.join("share").join("opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let db_path = data_dir.join("opencode.db");
        create_db_with_sessions(
            &db_path,
            &[
                ("sess-x", "/repo/adopt-weak", 1000),
                ("sess-y", "/repo/adopt-weak", 2000),
            ],
        );
        insert_message_with_text(&db_path, "sess-x", "msg-x1", "user", "run tests", 100, 101);
        insert_message_with_text(
            &db_path,
            "sess-x",
            "msg-x2",
            "assistant",
            "All tests passed.",
            200,
            201,
        );
        insert_message_with_text(
            &db_path,
            "sess-y",
            "msg-y1",
            "user",
            "build project",
            100,
            101,
        );
        insert_message_with_text(
            &db_path,
            "sess-y",
            "msg-y2",
            "assistant",
            "Build completed successfully.",
            200,
            201,
        );

        let mut state = OpenCodeState {
            data_dir: data_dir.clone(),
            expected_session_id: None,
            working_dir: "/repo/adopt-weak".to_string(),
            cli_session_id: None,
            transcript_offset: 0,
            emitted_final_text: None,
            adopted_pid: Some(std::process::id()),
        };
        // Screen contains unrelated text → disambiguation fails, stays Ambiguous.
        let backend = RecordingBackend::new(db_path.clone(), false, 2001)
            .with_screen("some totally unrelated content here".to_string());
        let result = write_input(&mut state, &backend, "cmd")
            .await
            .expect("write input");
        assert!(!result.submitted, "weak screen match should stay ambiguous");
        let reason = result.failure_reason.as_deref().unwrap_or("");
        assert!(
            reason.contains("ambiguous"),
            "expected ambiguous, got: {}",
            reason
        );
        let _ = fs::remove_dir_all(root);
    }
}
