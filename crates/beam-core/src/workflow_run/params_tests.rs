use crate::workflow_run::*;
use crate::*;
use serde_json::Value;
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
    assert_eq!(normalized.get("level"), Some(&serde_json::json!(3)));
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
        (
            String::from("ratio"),
            serde_json::json!(std::f64::consts::PI),
        ),
        (String::from("enabled"), Value::Bool(true)),
        (String::from("tags"), serde_json::json!(["a", "b"])),
        (String::from("meta"), serde_json::json!({"key": "val"})),
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
    assert!(
        msg.contains("task"),
        "expected available params list, got: {msg}"
    );
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
    assert!(msg.contains("unknown workflow parameter"), "got: {msg}");
    assert!(msg.contains("anything"), "got: {msg}");
    assert!(msg.contains("extra"), "got: {msg}");
    assert!(msg.contains("No parameters are declared"), "got: {msg}");
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
    let normalized = normalize_workflow_params(&def, &input)
        .expect("empty params with no schema should succeed");
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
    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("task"), Value::String("build".to_string()))]);
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
