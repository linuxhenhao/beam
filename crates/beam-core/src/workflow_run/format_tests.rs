use crate::workflow_run::*;
use crate::*;
use serde_json::Value;
use std::collections::BTreeMap;

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
