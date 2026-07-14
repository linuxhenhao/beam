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
    assert!(
        !run_dir.exists(),
        "run directory should NOT exist on failure"
    );
    let _ = std::fs::remove_dir_all(paths.root());
}

// -- JSON typed params tests (normalize_workflow_params) --
