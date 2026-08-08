use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::workflow_projection::read_run_events_pure;
use crate::{RunChatBinding, WorkflowOutputRef};

use super::model::*;
use super::replay::{ReplaySnapshot, replay_events};

const BLOB_PREVIEW_MAX_BYTES: usize = 64 * 1024;

pub async fn read_run_snapshot(run_dir: &Path) -> Result<Option<RunSnapshotDTO>> {
    let events = match read_run_events_pure(run_dir)? {
        Some(events) if !events.is_empty() => events,
        _ => return Ok(None),
    };
    let snap = replay_events(&events)?;
    let binding = read_chat_binding_pure(run_dir).await?;
    let def = read_workflow_definition_pure(run_dir).await?;
    let attempt_io = build_attempt_io(run_dir, &snap, def.as_ref())?;
    Ok(Some(RunSnapshotDTO {
        run_id: snap.run.run_id.clone(),
        run: snap.run,
        last_seq: snap.last_seq,
        nodes: snap.nodes.into_values().collect(),
        activities: snap.activities.into_values().collect(),
        loops: if snap.loops.is_empty() {
            None
        } else {
            Some(snap.loops.into_iter().collect())
        },
        dangling: DanglingSnapshot {
            activities: snap.dangling_activities,
            effect_attempted: snap.dangling_effect_attempted,
            waits: snap.dangling_waits,
            wait_resolutions: snap.dangling_wait_resolutions,
            cancels: snap.dangling_cancels,
        },
        outputs: snap.outputs,
        attempt_io,
        chat_binding: binding,
        updated_at: events.last().map(|ev| ev.timestamp).unwrap_or_default(),
    }))
}

async fn read_chat_binding_pure(run_dir: &Path) -> Result<Option<RunChatBinding>> {
    let path = run_dir.join("chat-binding.json");
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let binding = serde_json::from_str::<RunChatBinding>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(binding))
}

async fn read_workflow_definition_pure(run_dir: &Path) -> Result<Option<Value>> {
    let path = run_dir.join("workflow.json");
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let def = serde_json::from_str::<Value>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(def))
}

fn build_attempt_io(
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: Option<&Value>,
) -> Result<BTreeMap<String, AttemptIODTO>> {
    let mut out = BTreeMap::new();
    let mut cache = HashMap::new();
    for activity in snap.activities.values() {
        for attempt in &activity.attempts {
            let mut io = AttemptIODTO {
                input: Some(preview_ref(run_dir, &attempt.input_ref, &mut cache)?),
                resolved_input: None,
                output: None,
                log: preview_attempt_log(run_dir, &activity.activity_id, &attempt.attempt_id)?,
                terminal: read_attempt_terminal(
                    run_dir,
                    &activity.activity_id,
                    &attempt.attempt_id,
                )?,
                wait_prompt: None,
            };
            if let Some(output_ref) = attempt.output.as_ref() {
                io.output = Some(preview_ref(run_dir, output_ref, &mut cache)?);
            }
            if let Some(wait) = attempt.wait.as_ref()
                && let Some(prompt_ref) = wait.prompt_ref.as_ref()
            {
                io.wait_prompt = Some(preview_ref(run_dir, prompt_ref, &mut cache)?);
            }
            if let Some(input_value) = io.input.as_ref().and_then(|preview| preview.value.clone())
                && let Some(def) = def
            {
                io.resolved_input = Some(preview_resolved_input(
                    run_dir,
                    snap,
                    def,
                    input_value,
                    &mut cache,
                )?);
            }
            out.insert(attempt.attempt_id.clone(), io);
        }
    }
    Ok(out)
}

fn preview_ref(
    run_dir: &Path,
    ref_value: &WorkflowOutputRef,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<BlobPreviewDTO> {
    if let Some(cached) = cache.get(&ref_value.output_hash) {
        return Ok(cached.clone());
    }
    let base = BlobPreviewDTO {
        output_hash: Some(ref_value.output_hash.clone()),
        output_bytes: Some(ref_value.output_bytes),
        content_type: ref_value.content_type.clone(),
        truncated: None,
        value: None,
        text: None,
        error: None,
        redacted: None,
    };
    let Some(output_path) = Some(&ref_value.output_path) else {
        return Ok(base);
    };
    if !is_path_inside(run_dir, Path::new(output_path)) {
        let mut preview = base.clone();
        preview.error = Some("outputPath is outside run directory".to_string());
        cache.insert(ref_value.output_hash.clone(), preview.clone());
        return Ok(preview);
    }
    let bytes = match fs::read(output_path) {
        Ok(bytes) => bytes,
        Err(err) => {
            let mut preview = base.clone();
            preview.error = Some(err.to_string());
            cache.insert(ref_value.output_hash.clone(), preview.clone());
            return Ok(preview);
        }
    };
    let mut preview = base;
    preview.output_bytes = Some(bytes.len());
    preview.truncated = Some(bytes.len() > BLOB_PREVIEW_MAX_BYTES);
    let slice = if bytes.len() > BLOB_PREVIEW_MAX_BYTES {
        &bytes[..BLOB_PREVIEW_MAX_BYTES]
    } else {
        &bytes[..]
    };
    let text = String::from_utf8_lossy(slice).to_string();
    if !preview.truncated.unwrap_or(false) && is_json_content(ref_value.content_type.as_deref()) {
        match serde_json::from_slice::<Value>(slice) {
            Ok(value) => preview.value = Some(value),
            Err(err) => {
                preview.text = Some(text.clone());
                preview.error = Some(format!("invalid JSON: {}", err));
            }
        }
    } else {
        preview.text = Some(text);
    }
    cache.insert(ref_value.output_hash.clone(), preview.clone());
    Ok(preview)
}

fn preview_attempt_log(
    run_dir: &Path,
    activity_id: &str,
    attempt_id: &str,
) -> Result<Option<BlobPreviewDTO>> {
    let path = run_dir
        .join("attempts")
        .join(activity_id)
        .join(attempt_id)
        .join("terminal.log");
    if !is_path_inside(run_dir, &path) {
        return Ok(Some(BlobPreviewDTO {
            output_hash: None,
            output_bytes: None,
            content_type: Some("text/plain".to_string()),
            truncated: None,
            value: None,
            text: None,
            error: Some("attempt log is outside run directory".to_string()),
            redacted: None,
        }));
    }
    let raw = match fs::read(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Ok(Some(BlobPreviewDTO {
                output_hash: None,
                output_bytes: None,
                content_type: Some("text/plain".to_string()),
                truncated: None,
                value: None,
                text: None,
                error: Some(err.to_string()),
                redacted: None,
            }));
        }
    };
    let bytes = raw.len();
    let start = bytes.saturating_sub(BLOB_PREVIEW_MAX_BYTES);
    let text = String::from_utf8_lossy(&raw[start..]).to_string();
    Ok(Some(BlobPreviewDTO {
        output_hash: None,
        output_bytes: Some(bytes),
        content_type: Some("text/plain".to_string()),
        truncated: Some(bytes > BLOB_PREVIEW_MAX_BYTES),
        value: None,
        text: Some(text),
        error: None,
        redacted: None,
    }))
}

fn read_attempt_terminal(
    run_dir: &Path,
    activity_id: &str,
    attempt_id: &str,
) -> Result<Option<AttemptTerminalDTO>> {
    let path = run_dir
        .join("attempts")
        .join(activity_id)
        .join(attempt_id)
        .join("terminal.json");
    if !is_path_inside(run_dir, &path) {
        return Ok(Some(AttemptTerminalDTO {
            session_id: String::new(),
            cli_session_id: None,
            web_port: 0,
            status: "closed".to_string(),
            lark_app_id: None,
            bot_name: None,
            cli_id: None,
            working_dir: None,
            log_path: None,
            started_at: 0,
            updated_at: 0,
            closed_at: None,
            error: Some("terminal sidecar is outside run directory".to_string()),
            has_terminal_log: None,
        }));
    }
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Ok(Some(AttemptTerminalDTO {
                session_id: String::new(),
                cli_session_id: None,
                web_port: 0,
                status: "closed".to_string(),
                lark_app_id: None,
                bot_name: None,
                cli_id: None,
                working_dir: None,
                log_path: None,
                started_at: 0,
                updated_at: 0,
                closed_at: None,
                error: Some(err.to_string()),
                has_terminal_log: None,
            }));
        }
    };
    let parsed: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(err) => {
            return Ok(Some(AttemptTerminalDTO {
                session_id: String::new(),
                cli_session_id: None,
                web_port: 0,
                status: "closed".to_string(),
                lark_app_id: None,
                bot_name: None,
                cli_id: None,
                working_dir: None,
                log_path: None,
                started_at: 0,
                updated_at: 0,
                closed_at: None,
                error: Some(format!("invalid terminal sidecar: {}", err)),
                has_terminal_log: None,
            }));
        }
    };
    if payload_u64(&parsed, "schemaVersion") != Some(1)
        || payload_str(&parsed, "sessionId").is_none()
        || payload_u64(&parsed, "webPort").is_none()
        || !matches!(
            payload_str(&parsed, "status").as_deref(),
            Some("live" | "closed")
        )
        || payload_u64(&parsed, "startedAt").is_none()
        || payload_u64(&parsed, "updatedAt").is_none()
    {
        return Ok(Some(AttemptTerminalDTO {
            session_id: String::new(),
            cli_session_id: None,
            web_port: 0,
            status: "closed".to_string(),
            lark_app_id: None,
            bot_name: None,
            cli_id: None,
            working_dir: None,
            log_path: None,
            started_at: 0,
            updated_at: 0,
            closed_at: None,
            error: Some("invalid terminal sidecar".to_string()),
            has_terminal_log: None,
        }));
    }
    let terminal_log = path.with_file_name("terminal.log");
    let has_terminal_log = fs::metadata(&terminal_log)
        .map(|meta| meta.is_file() && meta.len() > 0)
        .unwrap_or(false);
    Ok(Some(AttemptTerminalDTO {
        session_id: payload_str(&parsed, "sessionId").unwrap_or_default(),
        cli_session_id: payload_str(&parsed, "cliSessionId"),
        web_port: payload_u64(&parsed, "webPort").unwrap_or_default() as u16,
        status: payload_str(&parsed, "status").unwrap_or_else(|| "closed".to_string()),
        lark_app_id: payload_str(&parsed, "larkAppId"),
        bot_name: payload_str(&parsed, "botName"),
        cli_id: payload_str(&parsed, "cliId"),
        working_dir: payload_str(&parsed, "workingDir"),
        log_path: payload_str(&parsed, "logPath"),
        started_at: payload_u64(&parsed, "startedAt").unwrap_or_default(),
        updated_at: payload_u64(&parsed, "updatedAt").unwrap_or_default(),
        closed_at: payload_u64(&parsed, "closedAt"),
        error: None,
        has_terminal_log: Some(has_terminal_log),
    }))
}

fn preview_resolved_input(
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    raw_input: Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<BlobPreviewDTO> {
    match resolve_dashboard_bindings(&raw_input, run_dir, snap, def, cache) {
        Ok(value) => Ok(BlobPreviewDTO {
            output_hash: None,
            output_bytes: Some(serde_json::to_vec(&value)?.len()),
            content_type: Some("application/json".to_string()),
            truncated: None,
            value: Some(value),
            text: None,
            error: None,
            redacted: None,
        }),
        Err(err) => Ok(BlobPreviewDTO {
            output_hash: None,
            output_bytes: Some(serde_json::to_vec(&raw_input)?.len()),
            content_type: Some("application/json".to_string()),
            truncated: None,
            value: Some(raw_input),
            text: None,
            error: Some(format!("failed to resolve bindings: {}", err)),
            redacted: None,
        }),
    }
}

fn resolve_dashboard_bindings(
    value: &Value,
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<Value> {
    if let Some(ref_spec) = value.as_object().and_then(|obj| {
        if obj.len() == 1 {
            obj.get("$ref").and_then(Value::as_str)
        } else {
            None
        }
    }) {
        return resolve_dashboard_ref(ref_spec, run_dir, snap, def, cache);
    }
    if let Some(s) = value.as_str() {
        return interpolate_dashboard_string_refs(s, run_dir, snap, def, cache).map(Value::String);
    }
    if let Some(arr) = value.as_array() {
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(resolve_dashboard_bindings(item, run_dir, snap, def, cache)?);
        }
        return Ok(Value::Array(out));
    }
    if let Some(obj) = value.as_object() {
        let mut out = serde_json::Map::new();
        for (key, item) in obj {
            out.insert(
                key.clone(),
                resolve_dashboard_bindings(item, run_dir, snap, def, cache)?,
            );
        }
        return Ok(Value::Object(out));
    }
    Ok(value.clone())
}

fn interpolate_dashboard_string_refs(
    value: &str,
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<String> {
    if !value.contains("${") {
        return Ok(value.to_string());
    }
    let mut out = String::new();
    let mut cursor = 0;
    while let Some(start) = value[cursor..].find("${") {
        let start = cursor + start;
        out.push_str(&value[cursor..start]);
        let end = value[start + 2..]
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("unterminated string ref interpolation in '{value}'"))?
            + start
            + 2;
        let ref_spec = &value[start + 2..end];
        if ref_spec.is_empty() {
            anyhow::bail!("empty string ref interpolation in '{value}'");
        }
        let resolved = resolve_dashboard_ref(ref_spec, run_dir, snap, def, cache)?;
        out.push_str(&stringify_dashboard_interpolated_value(ref_spec, resolved)?);
        cursor = end + 1;
    }
    out.push_str(&value[cursor..]);
    Ok(out)
}

fn stringify_dashboard_interpolated_value(ref_spec: &str, value: Value) -> Result<String> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::String(s) => Ok(s),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => anyhow::bail!(
            "string interpolation '${{{}}}' resolved to {}",
            ref_spec,
            if other.is_array() { "array" } else { "object" }
        ),
    }
}

fn resolve_dashboard_ref(
    ref_spec: &str,
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<Value> {
    if let Some(rest) = ref_spec.strip_prefix("params.") {
        let input_ref = snap
            .run
            .input
            .as_ref()
            .context(format!("$ref '{ref_spec}' requires run input"))?;
        let preview = preview_ref(run_dir, input_ref, cache)?;
        let params = preview.value.clone().context(format!(
            "$ref '{ref_spec}' output preview has no JSON value"
        ))?;
        return walk_preview_path(params, rest.split('.').collect::<Vec<_>>(), ref_spec);
    }
    let Some(sep_idx) = ref_spec.find(".output.") else {
        anyhow::bail!("$ref '{ref_spec}' missing '.output.' separator");
    };
    let node_id = &ref_spec[..sep_idx];
    let path = &ref_spec[sep_idx + ".output.".len()..];
    let node = def
        .get("nodes")
        .and_then(|nodes| nodes.get(node_id))
        .context(format!(
            "$ref '{ref_spec}' targets unknown node '{node_id}'"
        ))?;
    let output_ref = snap
        .outputs
        .get(&format!("{}::work::{}", snap.run.run_id, node_id))
        .context(format!("$ref '{ref_spec}' has no successful output yet"))?;
    let preview = preview_ref(run_dir, output_ref, cache)?;
    let value = preview.value.clone().context(format!(
        "$ref '{ref_spec}' output preview has no JSON value"
    ))?;
    let root = if node.get("type").and_then(Value::as_str) == Some("hostExecutor")
        && value
            .as_object()
            .map(|obj| obj.contains_key("output"))
            .unwrap_or(false)
    {
        value.get("output").cloned().unwrap_or(Value::Null)
    } else {
        value
    };
    walk_preview_path(root, path.split('.').collect::<Vec<_>>(), ref_spec)
}

fn walk_preview_path(value: Value, segments: Vec<&str>, ref_spec: &str) -> Result<Value> {
    let mut cursor = value;
    for seg in segments {
        if cursor.is_null() {
            anyhow::bail!("$ref '{ref_spec}' hit null at '{seg}'");
        }
        if let Some(arr) = cursor.as_array() {
            let idx: usize = seg
                .parse()
                .map_err(|_| anyhow::anyhow!("$ref '{ref_spec}' array index '{seg}' invalid"))?;
            cursor = arr.get(idx).cloned().context(format!(
                "$ref '{ref_spec}' array index '{seg}' out of bounds"
            ))?;
            continue;
        }
        let obj = cursor
            .as_object()
            .context(format!("$ref '{ref_spec}' segment '{seg}' not found"))?;
        cursor = obj
            .get(seg)
            .cloned()
            .context(format!("$ref '{ref_spec}' segment '{seg}' not found"))?;
    }
    Ok(cursor)
}

fn is_json_content(content_type: Option<&str>) -> bool {
    content_type
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains("json")
}

fn is_path_inside(parent: &Path, child: &Path) -> bool {
    let parent = parent
        .canonicalize()
        .unwrap_or_else(|_| parent.to_path_buf());
    let child = child.canonicalize().unwrap_or_else(|_| child.to_path_buf());
    child.starts_with(&parent)
}

fn payload_str(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn payload_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(Value::as_u64)
}
