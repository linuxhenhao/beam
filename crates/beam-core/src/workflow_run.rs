use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    BeamPaths, EventDraft, EventLog, ParamDef, WorkflowActor, WorkflowDefinition,
    parse_workflow_definition,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunChatBinding {
    pub chat_id: String,
    pub lark_app_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowOutputRef {
    pub output_hash: String,
    pub output_path: String,
    pub output_bytes: usize,
    pub output_schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowRunBootstrap {
    pub run_id: String,
    pub workflow_id: String,
    pub revision_id: String,
    pub input_ref: WorkflowOutputRef,
}

#[derive(Debug, Clone)]
pub struct BootstrapWorkflowRunInput<'a> {
    pub run_id: &'a str,
    pub workflow_json: &'a str,
    pub expected_workflow_id: Option<&'a str>,
    pub params: &'a BTreeMap<String, Value>,
    pub initiator: &'a str,
    pub chat_binding: Option<RunChatBinding>,
}

pub fn mint_workflow_run_id(workflow_id: &str, now_ms: u64) -> String {
    let safe = workflow_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '_',
        })
        .collect::<String>();
    format!("{}-{}", safe, now_ms)
}

pub fn bootstrap_workflow_run(
    paths: &BeamPaths,
    input: BootstrapWorkflowRunInput<'_>,
) -> Result<WorkflowRunBootstrap> {
    let workflow = parse_workflow_definition(input.workflow_json)?;
    let workflow_id = workflow.workflow_id.clone();
    if let Some(expected) = input.expected_workflow_id {
        if expected != workflow_id {
            anyhow::bail!(
                "workflowId mismatch: requested={} file={}",
                expected,
                workflow_id
            );
        }
    }

    // Validate / normalize params before creating any run artifacts
    let normalized_params = normalize_workflow_params(&workflow, input.params)?;

    let run_dir = paths.workflow_run_dir(input.run_id);
    fs::create_dir_all(run_dir.join("blobs"))?;
    fs::write(run_dir.join("workflow.json"), input.workflow_json)?;
    if let Some(binding) = input.chat_binding {
        fs::write(
            run_dir.join("chat-binding.json"),
            serde_json::to_vec_pretty(&binding)?,
        )?;
    }

    let params_json = serde_json::to_vec(&normalized_params)?;
    let params_hash = sha256_hex(&params_json);
    let input_path = run_dir.join("blobs").join(&params_hash);
    fs::write(&input_path, &params_json)?;
    let input_ref = WorkflowOutputRef {
        output_hash: format!("sha256:{}", params_hash),
        output_path: input_path.display().to_string(),
        output_bytes: params_json.len(),
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    };

    let mut log = EventLog::new(input.run_id.to_string(), paths.workflow_runs_dir())?;
    let revision_id = sha256_hex(&serde_json::to_vec(&workflow)?);
    let _run_created = log.append(EventDraft {
        event_type: "runCreated".to_string(),
        actor: WorkflowActor::System,
        payload: serde_json::json!({
            "workflowId": workflow_id,
            "revisionId": revision_id,
            "inputRef": input_ref,
            "initiator": input.initiator,
        }),
        timestamp: None,
        payload_hash: None,
    })?;
    let _run_started = log.append(EventDraft {
        event_type: "runStarted".to_string(),
        actor: WorkflowActor::Scheduler,
        payload: serde_json::json!({}),
        timestamp: None,
        payload_hash: None,
    })?;

    Ok(WorkflowRunBootstrap {
        run_id: input.run_id.to_string(),
        workflow_id,
        revision_id,
        input_ref,
    })
}

/// Normalize and validate workflow parameters against the definition's params
/// schema. Returns a canonical `BTreeMap<String, Value>` suitable for writing
/// as the run input blob.
///
/// If the workflow has no `params` definition (or an empty one), any supplied
/// params are rejected — no parameters are allowed without a declaration.
pub fn normalize_workflow_params(
    def: &WorkflowDefinition,
    input: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    let Some(params_def) = &def.params else {
        // No schema defined — reject any supplied params.
        if input.is_empty() {
            return Ok(BTreeMap::new());
        }
        let unknown: Vec<String> = input.keys().cloned().collect();
        anyhow::bail!(
            "unknown workflow parameter(s): {}. No parameters are declared for this workflow.",
            unknown.join(", ")
        );
    };

    // Empty params schema — same behaviour as no schema.
    if params_def.is_empty() {
        if input.is_empty() {
            return Ok(BTreeMap::new());
        }
        let unknown: Vec<String> = input.keys().cloned().collect();
        anyhow::bail!(
            "unknown workflow parameter(s): {}. No parameters are declared for this workflow.",
            unknown.join(", ")
        );
    }

    // ── Reject unknown keys ────────────────────────────────────────────
    let defined_keys: std::collections::HashSet<&String> = params_def.keys().collect();
    let unknown: Vec<String> = input
        .keys()
        .filter(|k| !defined_keys.contains(*k))
        .cloned()
        .collect();

    if !unknown.is_empty() {
        let available: Vec<&str> = params_def.keys().map(String::as_str).collect();
        anyhow::bail!(
            "unknown workflow parameter(s): {}. Available parameters: [{}]",
            unknown.join(", "),
            available.join(", ")
        );
    }

    // ── Validate and normalize each defined param ──────────────────────
    let mut normalized = BTreeMap::new();
    let mut missing: Vec<String> = Vec::new();

    for (name, param_def) in params_def {
        match input.get(name) {
            Some(value) => {
                // Coerce string inputs to the target type declared in the
                // schema (e.g. "true" → bool, "42" → integer).  Type/syntax
                // errors from coercion are surfaced here.
                let coerced = coerce_param_value(name, param_def, value)?;

                validate_param_type(name, param_def, &coerced)?;
                validate_param_format(name, param_def, &coerced)?;

                // Special handling: required string that is blank/whitespace
                // is treated as missing (preserving previous semantics).
                if param_def.required == Some(true) && param_def.param_type == "string" {
                    if let Value::String(s) = &coerced {
                        if s.trim().is_empty() {
                            missing.push(name.clone());
                            continue;
                        }
                    }
                }
                // For non-string required types with blank input, type
                // validation already failed above (e.g. "" is not a bool).
                normalized.insert(name.clone(), coerced);
            }
            None => {
                if param_def.required == Some(true) {
                    missing.push(name.clone());
                } else if let Some(default) = &param_def.default {
                    // Default values are NOT coerced — they must already be
                    // the correct typed JSON in the schema definition.
                    validate_param_type(name, param_def, default)?;
                    validate_param_format(name, param_def, default)?;
                    normalized.insert(name.clone(), default.clone());
                }
                // else: not required, no default → not written.
            }
        }
    }

    if !missing.is_empty() {
        anyhow::bail!(
            "missing required workflow parameter(s): {}",
            missing.join(", ")
        );
    }

    Ok(normalized)
}

/// Validate a single param value against its declared type.
fn validate_param_type(name: &str, def: &ParamDef, value: &Value) -> Result<()> {
    match def.param_type.as_str() {
        "string" => {
            if !value.is_string() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'string', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "number" => {
            if !value.is_number() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'number', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "integer" => match value {
            Value::Number(n) => {
                let is_int = n.as_i64().is_some()
                    || n.as_u64().is_some()
                    || n.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false);
                if !is_int {
                    anyhow::bail!(
                        "workflow parameter '{}' expects type 'integer', got non-integer number",
                        name
                    );
                }
            }
            _ => anyhow::bail!(
                "workflow parameter '{}' expects type 'integer', got {}",
                name,
                describe_value_kind(value)
            ),
        },
        "boolean" => {
            if !value.is_boolean() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'boolean', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "object" => {
            if !value.is_object() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'object', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        "array" => {
            if !value.is_array() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'array', got {}",
                    name,
                    describe_value_kind(value)
                );
            }
        }
        unknown => {
            anyhow::bail!(
                "workflow parameter '{}' has unknown type '{}'",
                name,
                unknown
            );
        }
    }
    Ok(())
}

/// Validate the format annotation of a param value.
///
/// Format only applies to string-typed parameters.  Unknown formats and
/// format-on-non-string are hard errors (no silent ignore).
fn validate_param_format(name: &str, def: &ParamDef, value: &Value) -> Result<()> {
    let Some(format) = &def.format else {
        return Ok(());
    };

    // Format is only valid for string-typed params.
    if def.param_type != "string" {
        anyhow::bail!(
            "workflow parameter '{}' has format '{}' but type is '{}'; format is only valid for string type",
            name,
            format,
            def.param_type,
        );
    }

    let Value::String(s) = value else {
        // Type validation should have caught mismatches already; be safe.
        return Ok(());
    };

    match format.as_str() {
        "date" => validate_date(name, s),
        "date-time" => validate_date_time(name, s),
        "email" => validate_email(name, s),
        unknown => {
            anyhow::bail!(
                "workflow parameter '{}' has unknown format '{}'",
                name,
                unknown,
            );
        }
    }
}

/// Validate a `date` format string: must be a real calendar date in YYYY-MM-DD.
fn validate_date(name: &str, value: &str) -> Result<()> {
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
        anyhow::anyhow!(
            "workflow parameter '{}' with format 'date' must be a valid date (YYYY-MM-DD), got: {}",
            name,
            value,
        )
    })?;
    Ok(())
}

/// Validate a `date-time` format string: must parse as RFC 3339.
fn validate_date_time(name: &str, value: &str) -> Result<()> {
    // chrono::DateTime::parse_from_rfc3339 handles timezone offset correctly.
    chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
        anyhow::anyhow!(
            "workflow parameter '{}' with format 'date-time' must be valid RFC 3339, got: {}",
            name,
            value,
        )
    })?;
    Ok(())
}

/// Validate an `email` format string: lightweight checks (one @, non-empty
/// local/domain, domain has at least one dot, domain labels are non-empty and
/// don't start/end with `-`, no whitespace).
fn validate_email(name: &str, value: &str) -> Result<()> {
    // Must have exactly one '@'.
    let at_count = value.chars().filter(|&c| c == '@').count();
    if at_count != 1 {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' must contain exactly one '@', got: {}",
            name,
            value,
        );
    }

    let at_pos = value.find('@').unwrap();
    let local = &value[..at_pos];
    let domain = &value[at_pos + 1..];

    if local.is_empty() {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' has empty local part, got: {}",
            name,
            value,
        );
    }
    if domain.is_empty() {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' has empty domain part, got: {}",
            name,
            value,
        );
    }

    // Domain must contain at least one dot.
    if !domain.contains('.') {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' domain must contain at least one dot, got: {}",
            name,
            value,
        );
    }

    // Reject whitespace anywhere.
    if value.chars().any(|c| c.is_whitespace()) {
        anyhow::bail!(
            "workflow parameter '{}' with format 'email' must not contain whitespace, got: {}",
            name,
            value,
        );
    }

    // Domain labels must be non-empty and not start/end with '-'.
    for label in domain.split('.') {
        if label.is_empty() {
            anyhow::bail!(
                "workflow parameter '{}' with format 'email' domain has empty label, got: {}",
                name,
                value,
            );
        }
        if label.starts_with('-') || label.ends_with('-') {
            anyhow::bail!(
                "workflow parameter '{}' with format 'email' domain label must not start or end with '-', got: {}",
                name,
                value,
            );
        }
    }

    Ok(())
}

fn describe_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Coerce a string input value to the target parameter type declared in the
/// workflow params schema.
///
/// If the input value is already the target JSON type, it passes through
/// unchanged.  Only `Value::String` inputs are coerced; other mismatched
/// types are left for the subsequent type-validation step to reject.
///
/// Coercion rules per type:
/// - `string`:  pass through unchanged (no JSON parse).
/// - `boolean`: accept case-sensitive `true` / `false` strings (trimmed).
/// - `number`:  parse as JSON number via `serde_json`; rejects NaN/inf/
///              blank/objects/arrays/non-numeric JSON.
/// - `integer`: accept only decimal integer strings (e.g. `42`, `-1`);
///              rejects `1.0`, `1.5`, `1e3` and other formats.
/// - `object`:  parse string as JSON, must produce a JSON object.
/// - `array`:   parse string as JSON, must produce a JSON array.
///
/// This coercion is only applied to external user input, never to schema
/// default values (so schema authors cannot paper over type errors in the
/// definition).
fn coerce_param_value(name: &str, def: &ParamDef, value: &Value) -> Result<Value> {
    let target_type = def.param_type.as_str();

    // If the value is already the target JSON type, no coercion needed.
    let already_typed = match target_type {
        "string" => value.is_string(),
        "number" | "integer" => value.is_number(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        _ => false,
    };
    if already_typed {
        return Ok(value.clone());
    }

    // Only attempt coercion from string.
    if !value.is_string() {
        // Not a string and not the target type — let type validation handle it.
        return Ok(value.clone());
    }

    let s = value.as_str().unwrap();

    match target_type {
        "string" => {
            // string → string: no coercion, pass through as-is.
            Ok(value.clone())
        }
        "boolean" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'boolean', got empty string",
                    name
                );
            }
            match trimmed {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => anyhow::bail!(
                    "workflow parameter '{}' expects type 'boolean', got string '{}' (only 'true' or 'false' are accepted)",
                    name, trimmed
                ),
            }
        }
        "number" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'number', got empty string",
                    name
                );
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'number', failed to parse '{}' as a number: {}",
                    name, trimmed, e
                )
            })?;
            if !v.is_number() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'number', but string '{}' parsed to {}",
                    name, trimmed, describe_value_kind(&v)
                );
            }
            Ok(v)
        }
        "integer" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'integer', got empty string",
                    name
                );
            }

            // Only accept decimal integer strings: optional leading '-', then
            // only ASCII digits. Reject floats, scientific notation, etc.
            let is_decimal_integer = trimmed
                .chars()
                .enumerate()
                .all(|(i, c)| {
                    if i == 0 && c == '-' {
                        trimmed.len() > 1 // "-" alone is not valid
                    } else {
                        c.is_ascii_digit()
                    }
                });

            if !is_decimal_integer {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'integer', got string '{}' (only decimal integer strings like '42' or '-1' are accepted)",
                    name, trimmed
                );
            }

            // Parse as i128 and construct a JSON number.
            let n: i128 = trimmed.parse().map_err(|_| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'integer', failed to parse '{}'",
                    name, trimmed
                )
            })?;
            let num = serde_json::Number::from_i128(n).ok_or_else(|| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'integer', number out of range: '{}'",
                    name, trimmed
                )
            })?;
            Ok(Value::Number(num))
        }
        "object" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'object', got empty string",
                    name
                );
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'object', but string value must be valid JSON (failed to parse: {})",
                    name, e
                )
            })?;
            if !v.is_object() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'object', but string value parsed to {} (must be a JSON object like '{{\"a\":1}}')",
                    name, describe_value_kind(&v)
                );
            }
            Ok(v)
        }
        "array" => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'array', got empty string",
                    name
                );
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "workflow parameter '{}' expects type 'array', but string value must be valid JSON (failed to parse: {})",
                    name, e
                )
            })?;
            if !v.is_array() {
                anyhow::bail!(
                    "workflow parameter '{}' expects type 'array', but string value parsed to {} (must be a JSON array like '[\"a\",\"b\"]')",
                    name, describe_value_kind(&v)
                );
            }
            Ok(v)
        }
        unknown => {
            anyhow::bail!(
                "workflow parameter '{}' has unknown type '{}'",
                name,
                unknown
            );
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn read_workflow_definition_from_path(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path).with_context(|| format!("读取 {} 失败", path.display()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-workflow-run-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[test]
    fn mint_workflow_run_id_sanitizes_input() {
        let run_id = mint_workflow_run_id("flow/a:b", 123);
        assert!(run_id.starts_with("flow_a_b-123"));
        assert!(!run_id.contains('/'));
        assert!(!run_id.contains(':'));
    }

    #[test]
    fn bootstrap_workflow_run_writes_snapshot_and_events() {
        let paths = temp_paths("bootstrap");
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("foo"), Value::String("bar".to_string()))]);
        let result = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-1",
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"foo":{"type":"string"}},"nodes":{"node-a":{"type":"subagent","bot":"bot-a","prompt":"hi"}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .expect("bootstrap");
        assert_eq!(result.run_id, "run-1");
        assert_eq!(result.workflow_id, "flow-a");
        assert!(
            paths
                .workflow_run_dir("run-1")
                .join("workflow.json")
                .exists()
        );
        assert!(
            paths
                .workflow_run_dir("run-1")
                .join("chat-binding.json")
                .exists()
        );
        assert!(paths.workflow_run_dir("run-1").join("blobs").exists());
        let log = EventLog::new("run-1", paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "runCreated");
        assert_eq!(events[1].event_type, "runStarted");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_workflow_run_hashes_canonical_definition_bytes() {
        let params: BTreeMap<String, Value> = BTreeMap::new();
        let raw_a = r#"{"workflowId":"flow-a","version":1,"nodes":{"node-a":{"type":"subagent","bot":"bot-a","prompt":"hi","workingDir":"/tmp/demo"}}}"#;
        let raw_b = r#"
        {
            "nodes": {
                "node-a": {
                    "prompt": "hi",
                    "type": "subagent",
                    "bot": "bot-a",
                    "workingDir": "/tmp/demo"
                }
            },
            "version": 1,
            "workflowId": "flow-a"
        }
        "#;

        let paths_a = temp_paths("bootstrap-canonical-a");
        let paths_b = temp_paths("bootstrap-canonical-b");
        let rev_a = bootstrap_workflow_run(
            &paths_a,
            BootstrapWorkflowRunInput {
                run_id: "run-a",
                workflow_json: raw_a,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: None,
            },
        )
        .expect("bootstrap a")
        .revision_id;
        let rev_b = bootstrap_workflow_run(
            &paths_b,
            BootstrapWorkflowRunInput {
                run_id: "run-b",
                workflow_json: raw_b,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: None,
            },
        )
        .expect("bootstrap b")
        .revision_id;
        assert_eq!(rev_a, rev_b);
        let _ = std::fs::remove_dir_all(paths_a.root());
        let _ = std::fs::remove_dir_all(paths_b.root());
    }

    // -- required param validation tests --

    #[test]
    fn bootstrap_rejects_missing_required_param() {
        let paths = temp_paths("missing-param");
        let params: BTreeMap<String, Value> = BTreeMap::new(); // empty — missing "task" which is required
        let workflow_json = r#"{
            "workflowId": "flow-req",
            "version": 1,
            "params": {
                "task": {
                    "type": "string",
                    "required": true,
                    "description": "what to do"
                }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let err = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-missing",
                workflow_json,
                expected_workflow_id: Some("flow-req"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("missing required workflow parameter"),
            "got: {msg}"
        );
        assert!(msg.contains("task"), "expected 'task' in error, got: {msg}");
        // Should not have created the run directory
        let run_dir = paths.workflow_run_dir("run-missing");
        assert!(
            !run_dir.exists(),
            "run directory should NOT exist after param validation failure, but found: {}",
            run_dir.display()
        );
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_rejects_empty_required_param_value() {
        let paths = temp_paths("empty-param");
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("task"), Value::String("   ".to_string()))]); // whitespace only
        let workflow_json = r#"{
            "workflowId": "flow-req",
            "version": 1,
            "params": {
                "task": {
                    "type": "string",
                    "required": true,
                    "description": "what to do"
                }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let err = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-empty",
                workflow_json,
                expected_workflow_id: Some("flow-req"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("missing required workflow parameter"),
            "got: {msg}"
        );
        assert!(msg.contains("task"), "expected 'task' in error, got: {msg}");
        // Should not have created the run directory
        let run_dir = paths.workflow_run_dir("run-empty");
        assert!(
            !run_dir.exists(),
            "run directory should NOT exist after param validation failure, but found: {}",
            run_dir.display()
        );
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_succeeds_with_required_params_provided() {
        let paths = temp_paths("provided-param");
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("task"), Value::String("build XYZ".to_string()))]);
        let workflow_json = r#"{
            "workflowId": "flow-req",
            "version": 1,
            "params": {
                "task": {
                    "type": "string",
                    "required": true,
                    "description": "what to do"
                }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let result = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-provided",
                workflow_json,
                expected_workflow_id: Some("flow-req"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap with required params provided should succeed");
        assert_eq!(result.workflow_id, "flow-req");
        assert!(
            paths
                .workflow_run_dir("run-provided")
                .join("workflow.json")
                .exists()
        );
        let log = EventLog::new("run-provided", paths.workflow_runs_dir()).expect("log");
        let events = log.read_all().expect("events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "runCreated");
        assert_eq!(events[1].event_type, "runStarted");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_ignores_optional_param_when_missing() {
        let paths = temp_paths("optional-param");
        let params: BTreeMap<String, Value> = BTreeMap::new(); // no params, but "verbose" is not required
        let workflow_json = r#"{
            "workflowId": "flow-opt",
            "version": 1,
            "params": {
                "verbose": {
                    "type": "boolean",
                    "required": false,
                    "default": false
                }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let result = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-opt",
                workflow_json,
                expected_workflow_id: Some("flow-opt"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap should ignore missing optional param");
        assert_eq!(result.workflow_id, "flow-opt");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_rejects_multiple_missing_required_params() {
        let paths = temp_paths("multi-missing");
        let params: BTreeMap<String, Value> = BTreeMap::new(); // missing all required params
        let workflow_json = r#"{
            "workflowId": "flow-multi",
            "version": 1,
            "params": {
                "task": {
                    "type": "string",
                    "required": true,
                    "description": "what to do"
                },
                "target": {
                    "type": "string",
                    "required": true,
                    "description": "where to deploy"
                }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let err = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-multi",
                workflow_json,
                expected_workflow_id: Some("flow-multi"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("missing required workflow parameter"),
            "got: {msg}"
        );
        assert!(msg.contains("task"), "expected 'task' in error, got: {msg}");
        assert!(
            msg.contains("target"),
            "expected 'target' in error, got: {msg}"
        );
        let run_dir = paths.workflow_run_dir("run-multi");
        assert!(!run_dir.exists(), "run directory should NOT exist on failure");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    // -- JSON typed params tests (normalize_workflow_params) --

    #[test]
    fn normalize_writes_default_value_for_optional_param() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-default",
                "version": 1,
                "params": {
                    "verbose": {
                        "type": "boolean",
                        "required": false,
                        "default": false
                    },
                    "level": {
                        "type": "integer",
                        "required": false,
                        "default": 3
                    }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::new();
        let normalized = normalize_workflow_params(&def, &input).expect("should succeed");
        assert_eq!(normalized.get("verbose"), Some(&Value::Bool(false)));
        assert_eq!(
            normalized.get("level"),
            Some(&serde_json::json!(3))
        );
        // Not-required, no default: should not be written
        assert!(!normalized.contains_key("unknown_key"));
    }

    #[test]
    fn normalize_type_validation_success() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-types",
                "version": 1,
                "params": {
                    "name": { "type": "string" },
                    "count": { "type": "integer" },
                    "ratio": { "type": "number" },
                    "enabled": { "type": "boolean" },
                    "tags": { "type": "array" },
                    "meta": { "type": "object" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([
            (String::from("name"), Value::String("test".to_string())),
            (String::from("count"), serde_json::json!(42)),
            (String::from("ratio"), serde_json::json!(3.14)),
            (String::from("enabled"), Value::Bool(true)),
            (
                String::from("tags"),
                serde_json::json!(["a", "b"]),
            ),
            (
                String::from("meta"),
                serde_json::json!({"key": "val"}),
            ),
        ]);
        let _ = normalize_workflow_params(&def, &input).expect("all types valid");
    }

    #[test]
    fn normalize_type_mismatch_fails() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-type-err",
                "version": 1,
                "params": {
                    "enabled": { "type": "boolean" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("enabled"), Value::String("yes".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("enabled"), "got: {msg}");
        assert!(msg.contains("boolean"), "got: {msg}");
    }

    #[test]
    fn normalize_unknown_key_fails_when_schema_defined() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-unknown",
                "version": 1,
                "params": {
                    "task": { "type": "string" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([
            (String::from("task"), Value::String("hello".to_string())),
            (String::from("extra"), Value::String("bad".to_string())),
        ]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown workflow parameter"), "got: {msg}");
        assert!(msg.contains("extra"), "got: {msg}");
        assert!(msg.contains("task"), "expected available params list, got: {msg}");
    }

    #[test]
    fn normalize_no_schema_rejects_extra_params() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-no-schema",
                "version": 1,
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([
            (String::from("anything"), Value::String("goes".to_string())),
            (String::from("extra"), serde_json::json!({"deep": true})),
        ]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown workflow parameter"),
            "got: {msg}"
        );
        assert!(msg.contains("anything"), "got: {msg}");
        assert!(msg.contains("extra"), "got: {msg}");
        assert!(
            msg.contains("No parameters are declared"),
            "got: {msg}"
        );
    }

    #[test]
    fn normalize_no_schema_with_empty_input_succeeds() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-no-schema",
                "version": 1,
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::new();
        let normalized = normalize_workflow_params(&def, &input).expect("empty params with no schema should succeed");
        assert!(normalized.is_empty());
    }

    #[test]
    fn normalize_empty_params_schema_rejects_extra_params() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-empty-params",
                "version": 1,
                "params": {},
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("x"), Value::String("y".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown workflow parameter"), "got: {msg}");
        assert!(msg.contains("No parameters are declared"), "got: {msg}");
    }

    #[test]
    fn normalize_empty_params_schema_with_empty_input_succeeds() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-empty-params",
                "version": 1,
                "params": {},
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::new();
        let normalized = normalize_workflow_params(&def, &input)
            .expect("empty params with empty schema should succeed");
        assert!(normalized.is_empty());
    }

    #[test]
    fn normalize_default_value_type_mismatch_fails() {
        // The default value itself must match the declared type.
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-bad-default",
                "version": 1,
                "params": {
                    "enabled": {
                        "type": "boolean",
                        "required": false,
                        "default": "not-a-bool"
                    }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::new();
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("enabled"), "got: {msg}");
        assert!(msg.contains("boolean"), "got: {msg}");
    }

    #[test]
    fn normalize_unknown_param_type_fails() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-unknown-type",
                "version": 1,
                "params": {
                    "x": {
                        "type": "unknown-type-xyz",
                        "required": false
                    }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("x"), Value::String("v".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown type"), "got: {msg}");
        assert!(msg.contains("x"), "got: {msg}");
        assert!(msg.contains("unknown-type-xyz"), "got: {msg}");
    }

    #[test]
    fn normalize_integer_rejects_non_integer_number() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-int",
                "version": 1,
                "params": {
                    "count": { "type": "integer" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("count"), serde_json::json!(3.5))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("integer"), "got: {msg}");
        assert!(msg.contains("count"), "got: {msg}");
    }

    #[test]
    fn normalize_integer_accepts_integer_number() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-int-ok",
                "version": 1,
                "params": {
                    "count": { "type": "integer" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("count"), serde_json::json!(42))]);
        let _ = normalize_workflow_params(&def, &input).expect("integer 42 OK");
        // Also try 42.0 (integer-value float)
        let input2: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("count"), serde_json::json!(42.0))]);
        let _ = normalize_workflow_params(&def, &input2).expect("integer 42.0 OK");
    }

    #[test]
    fn bootstrap_integration_rejects_unknown_key_with_params_schema() {
        let paths = temp_paths("bootstrap-unknown");
        let params: BTreeMap<String, Value> = BTreeMap::from([
            (String::from("task"), Value::String("hello".to_string())),
            (String::from("bad_key"), Value::String("x".to_string())),
        ]);
        let workflow_json = r#"{
            "workflowId": "flow-req",
            "version": 1,
            "params": {
                "task": {
                    "type": "string",
                    "required": true,
                    "description": "what to do"
                }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let err = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-unk",
                workflow_json,
                expected_workflow_id: Some("flow-req"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown workflow parameter"), "got: {msg}");
        assert!(msg.contains("bad_key"), "got: {msg}");
        let run_dir = paths.workflow_run_dir("run-unk");
        assert!(!run_dir.exists(), "run dir should NOT exist");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn bootstrap_integration_default_written_to_params_blob() {
        let paths = temp_paths("bootstrap-default");
        let params: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("task"),
            Value::String("build".to_string()),
        )]);
        let workflow_json = r#"{
            "workflowId": "flow-def",
            "version": 1,
            "params": {
                "task": { "type": "string", "required": true },
                "verbose": { "type": "boolean", "required": false, "default": false }
            },
            "nodes": {
                "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
            }
        }"#;
        let result = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-def",
                workflow_json,
                expected_workflow_id: Some("flow-def"),
                params: &params,
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap with default param");
        assert_eq!(result.workflow_id, "flow-def");
        // Read the params blob to verify default was written.
        let blob_path = result.input_ref.output_path;
        let blob_bytes = std::fs::read(&blob_path).expect("read params blob");
        let blob: BTreeMap<String, Value> =
            serde_json::from_slice(&blob_bytes).expect("parse params blob");
        assert_eq!(blob.get("task"), Some(&Value::String("build".to_string())));
        assert_eq!(blob.get("verbose"), Some(&Value::Bool(false)));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    // ── format validation tests ─────────────────────────────────────────

    #[test]
    fn format_date_success() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "d": { "type": "string", "format": "date" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("d"), Value::String("2024-02-29".to_string()))]);
        let _ = normalize_workflow_params(&def, &input).expect("leap date should pass");
    }

    #[test]
    fn format_date_failure_invalid_date() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "d": { "type": "string", "format": "date" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("d"), Value::String("2023-02-29".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("date"), "got: {msg}");
        assert!(msg.contains("2023-02-29"), "got: {msg}");
    }

    #[test]
    fn format_date_failure_invalid_month() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "d": { "type": "string", "format": "date" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("d"), Value::String("2024-13-01".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("date"), "got: {msg}");
        assert!(msg.contains("2024-13-01"), "got: {msg}");
    }

    #[test]
    fn format_date_failure_wrong_format() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "d": { "type": "string", "format": "date" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("d"), Value::String("01-01-2024".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("date"), "got: {msg}");
    }

    #[test]
    fn format_date_time_success() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "ts": { "type": "string", "format": "date-time" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("ts"),
            Value::String("2026-06-17T12:34:56Z".to_string()),
        )]);
        let _ = normalize_workflow_params(&def, &input).expect("RFC3339 should pass");
        // Also test with offset
        let input2: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("ts"),
            Value::String("2026-06-17T12:34:56+08:00".to_string()),
        )]);
        let _ = normalize_workflow_params(&def, &input2).expect("RFC3339 with offset should pass");
    }

    #[test]
    fn format_date_time_failure() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "ts": { "type": "string", "format": "date-time" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("ts"),
            Value::String("2026-06-17 12:34:56".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("date-time"), "got: {msg}");
    }

    #[test]
    fn format_email_success() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "email": { "type": "string", "format": "email" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("email"),
            Value::String("user@example.com".to_string()),
        )]);
        let _ = normalize_workflow_params(&def, &input).expect("valid email should pass");
    }

    #[test]
    fn format_email_failure_no_at() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "email": { "type": "string", "format": "email" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("email"),
            Value::String("notanemail".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("email"), "got: {msg}");
    }

    #[test]
    fn format_email_failure_double_at() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "email": { "type": "string", "format": "email" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("email"),
            Value::String("a@b@c.com".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("email"), "got: {msg}");
    }

    #[test]
    fn format_email_failure_empty_local() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "email": { "type": "string", "format": "email" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("email"),
            Value::String("@example.com".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("email"), "got: {msg}");
    }

    #[test]
    fn format_email_failure_no_dot_in_domain() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "email": { "type": "string", "format": "email" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("email"),
            Value::String("user@localhost".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("dot"), "got: {msg}");
    }

    #[test]
    fn format_email_failure_whitespace() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt",
                "version": 1,
                "params": {
                    "email": { "type": "string", "format": "email" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("email"),
            Value::String("user @example.com".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("whitespace"), "got: {msg}");
    }

    #[test]
    fn format_on_non_string_type_fails() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt-nonstring",
                "version": 1,
                "params": {
                    "count": { "type": "integer", "format": "date" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("count"), serde_json::json!(42))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("count"), "got: {msg}");
        assert!(msg.contains("format"), "got: {msg}");
        assert!(msg.contains("integer"), "got: {msg}");
        assert!(msg.contains("string"), "got: {msg}");
    }

    #[test]
    fn format_unknown_format_fails() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-fmt-unknown",
                "version": 1,
                "params": {
                    "x": { "type": "string", "format": "unknown-format-xyz" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("x"), Value::String("v".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown format"), "got: {msg}");
        assert!(msg.contains("x"), "got: {msg}");
        assert!(msg.contains("unknown-format-xyz"), "got: {msg}");
    }

    // ── coercion tests ───────────────────────────────────────────────

    #[test]
    fn coerce_string_to_boolean_true_and_false() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "enabled": { "type": "boolean" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // "true" → bool true
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("enabled"), Value::String("true".to_string()))]);
        let normalized = normalize_workflow_params(&def, &input).expect("true coerces to bool");
        assert_eq!(normalized.get("enabled"), Some(&Value::Bool(true)));

        // "false" → bool false
        let input2: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("enabled"), Value::String("false".to_string()))]);
        let normalized2 =
            normalize_workflow_params(&def, &input2).expect("false coerces to bool");
        assert_eq!(normalized2.get("enabled"), Some(&Value::Bool(false)));
    }

    #[test]
    fn coerce_boolean_rejects_case_insensitive_and_junk() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "enabled": { "type": "boolean" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        for bad in &["True", "TRUE", "yes", "1", "0", "FALSE", "on", "off"] {
            let input: BTreeMap<String, Value> =
                BTreeMap::from([(String::from("enabled"), Value::String(bad.to_string()))]);
            let err = normalize_workflow_params(&def, &input).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("boolean"),
                "expected 'boolean' in error for input '{}', got: {}",
                bad, msg
            );
            assert!(
                msg.contains("enabled"),
                "expected 'enabled' in error for input '{}', got: {}",
                bad, msg
            );
        }
    }

    #[test]
    fn coerce_boolean_accepts_already_typed_value() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "enabled": { "type": "boolean" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // Already bool → passes through
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("enabled"), Value::Bool(true))]);
        let normalized =
            normalize_workflow_params(&def, &input).expect("bool passthrough");
        assert_eq!(normalized.get("enabled"), Some(&Value::Bool(true)));
    }

    #[test]
    fn coerce_number_from_strings() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "ratio": { "type": "number" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let cases: Vec<(&str, Value)> = vec![
            ("1", serde_json::json!(1)),
            ("1.5", serde_json::json!(1.5)),
            ("-2", serde_json::json!(-2)),
            ("1e3", serde_json::json!(1000.0)),
        ];
        for (input_str, expected) in &cases {
            let input: BTreeMap<String, Value> =
                BTreeMap::from([(String::from("ratio"), Value::String(input_str.to_string()))]);
            let normalized =
                normalize_workflow_params(&def, &input).expect("number string should coerce");
            assert_eq!(
                normalized.get("ratio"),
                Some(expected),
                "failed for input '{}'",
                input_str
            );
        }
    }

    #[test]
    fn coerce_number_rejects_junk() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "ratio": { "type": "number" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        for bad in &["NaN", "Infinity", "-Infinity", "not-a-number", "true", "\"hi\"", "[]"] {
            let input: BTreeMap<String, Value> =
                BTreeMap::from([(String::from("ratio"), Value::String(bad.to_string()))]);
            let err = normalize_workflow_params(&def, &input).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("number") || msg.contains("number"),
                "expected 'number' in error for input '{}', got: {}",
                bad, msg
            );
        }
    }

    #[test]
    fn coerce_integer_from_decimal_strings() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "count": { "type": "integer" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let cases: Vec<(&str, Value)> = vec![
            ("42", serde_json::json!(42)),
            ("-1", serde_json::json!(-1)),
            ("0", serde_json::json!(0)),
        ];
        for (input_str, expected) in &cases {
            let input: BTreeMap<String, Value> =
                BTreeMap::from([(String::from("count"), Value::String(input_str.to_string()))]);
            let normalized =
                normalize_workflow_params(&def, &input).expect("integer string should coerce");
            assert_eq!(
                normalized.get("count"),
                Some(expected),
                "failed for input '{}'",
                input_str
            );
        }
    }

    #[test]
    fn coerce_integer_with_whitespace_padding() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "count": { "type": "integer" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // " 42 " → trim → "42" → integer 42
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("count"), Value::String(" 42 ".to_string()))]);
        let normalized =
            normalize_workflow_params(&def, &input).expect("padded integer should coerce");
        assert_eq!(normalized.get("count"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn coerce_integer_rejects_non_decimal_formats() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "count": { "type": "integer" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        for bad in &["1.0", "1.5", "1e3", "0x10", "3.0"] {
            let input: BTreeMap<String, Value> =
                BTreeMap::from([(String::from("count"), Value::String(bad.to_string()))]);
            let err = normalize_workflow_params(&def, &input).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("integer"),
                "expected 'integer' in error for input '{}', got: {}",
                bad, msg
            );
        }
    }

    #[test]
    fn coerce_integer_accepts_already_typed_value() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "count": { "type": "integer" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // Already a JSON number → passes through
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("count"), serde_json::json!(42))]);
        let normalized =
            normalize_workflow_params(&def, &input).expect("integer passthrough");
        assert_eq!(normalized.get("count"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn coerce_object_from_json_string() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "payload": { "type": "object" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("payload"),
            Value::String(r#"{"a":1,"b":"x"}"#.to_string()),
        )]);
        let normalized =
            normalize_workflow_params(&def, &input).expect("object string should coerce");
        assert_eq!(
            normalized.get("payload"),
            Some(&serde_json::json!({"a":1,"b":"x"}))
        );
    }

    #[test]
    fn coerce_array_from_json_string() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "tags": { "type": "array" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("tags"),
            Value::String(r#"["a","b"]"#.to_string()),
        )]);
        let normalized =
            normalize_workflow_params(&def, &input).expect("array string should coerce");
        assert_eq!(
            normalized.get("tags"),
            Some(&serde_json::json!(["a", "b"]))
        );
    }

    #[test]
    fn coerce_object_rejects_non_object_json() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "payload": { "type": "object" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // Array string for object type → error
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("payload"),
            Value::String(r#"["a"]"#.to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("object"), "got: {msg}");
        assert!(msg.contains("payload"), "got: {msg}");
        assert!(msg.contains("array"), "got: {msg}");
    }

    #[test]
    fn coerce_array_rejects_non_array_json() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "tags": { "type": "array" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // Number string for array type → error
        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("tags"),
            Value::String("1".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("array"), "got: {msg}");
        assert!(msg.contains("tags"), "got: {msg}");
    }

    #[test]
    fn coerce_object_invalid_json_fails() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "payload": { "type": "object" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let input: BTreeMap<String, Value> = BTreeMap::from([(
            String::from("payload"),
            Value::String("not-json".to_string()),
        )]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("object"), "got: {msg}");
        assert!(msg.contains("valid JSON"), "got: {msg}");
    }

    #[test]
    fn coerce_default_not_coerced_for_boolean() {
        // Default of "true" (string) for boolean type should fail, because
        // defaults are NOT coerced — they must be typed JSON.
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "enabled": {
                        "type": "boolean",
                        "required": false,
                        "default": "true"
                    }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let input: BTreeMap<String, Value> = BTreeMap::new(); // use default
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("boolean"),
            "default string 'true' should fail type check for boolean, got: {}",
            msg
        );
    }

    #[test]
    fn coerce_string_param_unchanged() {
        // String-type params should not be modified.
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "name": { "type": "string" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("true".to_string()))]);
        let normalized = normalize_workflow_params(&def, &input)
            .expect("string param should not be coerced");
        // Should remain a string "true", not bool true
        assert_eq!(
            normalized.get("name"),
            Some(&Value::String("true".to_string()))
        );
    }

    #[test]
    fn coerce_boolean_with_whitespace_padding() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "enabled": { "type": "boolean" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        // "  true  " → bool true (trimmed before matching)
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("enabled"), Value::String("  true  ".to_string()))]);
        let normalized =
            normalize_workflow_params(&def, &input).expect("padded true coerces to bool");
        assert_eq!(normalized.get("enabled"), Some(&Value::Bool(true)));
    }

    #[test]
    fn coerce_empty_string_to_boolean_fails() {
        let def = parse_workflow_definition(
            r#"{
                "workflowId": "flow-coerce",
                "version": 1,
                "params": {
                    "enabled": { "type": "boolean" }
                },
                "nodes": {
                    "a": { "type": "subagent", "bot": "bot-a", "prompt": "hi" }
                }
            }"#,
        )
        .unwrap();

        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("enabled"), Value::String("".to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("boolean"), "got: {msg}");
        assert!(msg.contains("empty"), "got: {msg}");
    }
}
