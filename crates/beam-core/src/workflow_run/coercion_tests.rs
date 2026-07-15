use crate::workflow_run::*;
use crate::*;
use serde_json::Value;
use std::collections::BTreeMap;

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
    let normalized2 = normalize_workflow_params(&def, &input2).expect("false coerces to bool");
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
            bad,
            msg
        );
        assert!(
            msg.contains("enabled"),
            "expected 'enabled' in error for input '{}', got: {}",
            bad,
            msg
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
    let normalized = normalize_workflow_params(&def, &input).expect("bool passthrough");
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

    for bad in &[
        "NaN",
        "Infinity",
        "-Infinity",
        "not-a-number",
        "true",
        "\"hi\"",
        "[]",
    ] {
        let input: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("ratio"), Value::String(bad.to_string()))]);
        let err = normalize_workflow_params(&def, &input).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("number") || msg.contains("number"),
            "expected 'number' in error for input '{}', got: {}",
            bad,
            msg
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
    let normalized = normalize_workflow_params(&def, &input).expect("padded integer should coerce");
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
            bad,
            msg
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
    let normalized = normalize_workflow_params(&def, &input).expect("integer passthrough");
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
    let normalized = normalize_workflow_params(&def, &input).expect("object string should coerce");
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
    let normalized = normalize_workflow_params(&def, &input).expect("array string should coerce");
    assert_eq!(normalized.get("tags"), Some(&serde_json::json!(["a", "b"])));
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
    let input: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("tags"), Value::String("1".to_string()))]);
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
    let normalized =
        normalize_workflow_params(&def, &input).expect("string param should not be coerced");
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
    let input: BTreeMap<String, Value> = BTreeMap::from([(
        String::from("enabled"),
        Value::String("  true  ".to_string()),
    )]);
    let normalized = normalize_workflow_params(&def, &input).expect("padded true coerces to bool");
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
