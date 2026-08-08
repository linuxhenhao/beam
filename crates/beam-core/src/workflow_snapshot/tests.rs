use super::replay::replay_events;
use super::*;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::{WorkflowActor, WorkflowEventEnvelope};

fn temp_run_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "beam-workflow-snapshot-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn env(event_id: &str, run_id: &str, event_type: &str, payload: Value) -> WorkflowEventEnvelope {
    WorkflowEventEnvelope {
        event_id: event_id.to_string(),
        run_id: run_id.to_string(),
        timestamp: 1,
        schema_version: 1,
        actor: WorkflowActor::System,
        event_type: event_type.to_string(),
        payload,
        payload_hash: None,
    }
}

#[test]
fn replay_projects_basic_state_and_outputs() {
    let run_id = "run-1";
    let events = vec![
        env(
            "run-1-1",
            run_id,
            "runCreated",
            serde_json::json!({
                "workflowId": "flow-a",
                "revisionId": "rev-a",
                "inputRef": {
                    "outputHash": "sha256:input",
                    "outputPath": "/tmp/input.json",
                    "outputBytes": 2,
                    "outputSchemaVersion": 1,
                    "contentType": "application/json"
                },
                "initiator": "cli"
            }),
        ),
        env("run-1-2", run_id, "runStarted", serde_json::json!({})),
        env(
            "run-1-3",
            run_id,
            "attemptCreated",
            serde_json::json!({
                "activityId": "run-1::work::node-a",
                "attemptId": "run-1::work::node-a::att-1",
                "attemptNumber": 1,
                "nodeId": "node-a",
                "inputRef": {
                    "outputHash": "sha256:input",
                    "outputPath": "/tmp/input.json",
                    "outputBytes": 2,
                    "outputSchemaVersion": 1,
                    "contentType": "application/json"
                }
            }),
        ),
        env(
            "run-1-4",
            run_id,
            "activitySucceeded",
            serde_json::json!({
                "activityId": "run-1::work::node-a",
                "attemptId": "run-1::work::node-a::att-1",
                "outputRef": {
                    "outputHash": "sha256:output",
                    "outputPath": "/tmp/output.json",
                    "outputBytes": 17,
                    "outputSchemaVersion": 1,
                    "contentType": "application/json"
                },
                "externalRefs": {"ok": true}
            }),
        ),
    ];
    let snap = replay_events(&events).expect("replay");
    assert_eq!(snap.run.workflow_id.as_deref(), Some("flow-a"));
    assert_eq!(snap.run.status, RunStatus::Running);
    assert_eq!(snap.activities.len(), 1);
    assert_eq!(snap.outputs.len(), 1);
    assert_eq!(snap.dangling_activities, Vec::<String>::new());
}

#[tokio::test]
async fn read_run_snapshot_replays_outputs_and_binding() {
    let run_dir = temp_run_dir("read");
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    fs::create_dir_all(
        run_dir
            .join("attempts")
            .join("run-1::work::node-a")
            .join("run-1::work::node-a::att-1"),
    )
    .unwrap();
    fs::write(
        run_dir.join("workflow.json"),
        r#"{"workflowId":"flow-a","nodes":{"node-a":{"type":"hostExecutor"}}}"#,
    )
    .unwrap();
    fs::write(
        run_dir.join("chat-binding.json"),
        r#"{"chatId":"chat-1","larkAppId":"app-1"}"#,
    )
    .unwrap();
    fs::write(run_dir.join("blobs").join("input"), br#"{"foo":"bar"}"#).unwrap();
    fs::write(
        run_dir.join("blobs").join("output"),
        br#"{"output":{"hello":"world"},"externalRefs":{"ok":true}}"#,
    )
    .unwrap();
    fs::write(
        run_dir
            .join("attempts")
            .join("run-1::work::node-a")
            .join("run-1::work::node-a::att-1")
            .join("terminal.log"),
        "hello world",
    )
    .unwrap();
    fs::write(
        run_dir.join("attempts").join("run-1::work::node-a").join("run-1::work::node-a::att-1").join("terminal.json"),
        r#"{"schemaVersion":1,"sessionId":"sess-1","webPort":8080,"status":"live","startedAt":1,"updatedAt":2}"#,
    )
    .unwrap();
    let events = [
        env(
            "run-1-1",
            "run-1",
            "runCreated",
            serde_json::json!({
                "workflowId":"flow-a",
                "revisionId":"rev-a",
                "inputRef": {
                    "outputHash": "sha256:input",
                    "outputPath": run_dir.join("blobs").join("input"),
                    "outputBytes": 13,
                    "outputSchemaVersion": 1,
                    "contentType": "application/json"
                }
            }),
        ),
        env("run-1-2", "run-1", "runStarted", serde_json::json!({})),
        env(
            "run-1-3",
            "run-1",
            "attemptCreated",
            serde_json::json!({
                "activityId": "run-1::work::node-a",
                "attemptId": "run-1::work::node-a::att-1",
                "attemptNumber": 1,
                "nodeId": "node-a",
                "inputRef": {
                    "outputHash": "sha256:input",
                    "outputPath": run_dir.join("blobs").join("input"),
                    "outputBytes": 13,
                    "outputSchemaVersion": 1,
                    "contentType": "application/json"
                }
            }),
        ),
        env(
            "run-1-4",
            "run-1",
            "activitySucceeded",
            serde_json::json!({
                "activityId": "run-1::work::node-a",
                "attemptId": "run-1::work::node-a::att-1",
                "outputRef": {
                    "outputHash": "sha256:output",
                    "outputPath": run_dir.join("blobs").join("output"),
                    "outputBytes": 55,
                    "outputSchemaVersion": 1,
                    "contentType": "application/json"
                },
                "externalRefs": {"ok": true}
            }),
        ),
    ];
    let events_json = events
        .iter()
        .map(|ev| serde_json::to_string(ev).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(run_dir.join("events.ndjson"), events_json).unwrap();

    let snapshot = read_run_snapshot(&run_dir)
        .await
        .expect("snapshot")
        .expect("some");
    assert_eq!(snapshot.run.workflow_id.as_deref(), Some("flow-a"));
    assert_eq!(
        snapshot.chat_binding.as_ref().map(|b| b.chat_id.as_str()),
        Some("chat-1")
    );
    assert_eq!(snapshot.activities.len(), 1);
    assert_eq!(snapshot.outputs.len(), 1);
    assert!(
        snapshot
            .attempt_io
            .contains_key("run-1::work::node-a::att-1")
    );
    let io = snapshot
        .attempt_io
        .get("run-1::work::node-a::att-1")
        .expect("attempt io");
    assert!(io.input.as_ref().and_then(|p| p.value.as_ref()).is_some());
    assert!(io.output.as_ref().and_then(|p| p.value.as_ref()).is_some());
    let _ = fs::remove_dir_all(&run_dir);
}
