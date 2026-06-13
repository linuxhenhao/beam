use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use beam_core::{BeamPaths, EventDraft, EventLog, WorkflowActor};
use serde_json::json;

fn temp_paths(name: &str) -> BeamPaths {
    let base = std::env::temp_dir().join(format!(
        "beam-core-{}-{}",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    BeamPaths::from_root(base)
}

#[test]
fn event_log_appends_reads_and_tracks_seq() {
    let paths = temp_paths("workflow-event-log");
    let mut log = EventLog::new("run-1", paths.workflow_runs_dir()).expect("log");
    let event = log
        .append(EventDraft {
            event_type: "runCreated".to_string(),
            actor: WorkflowActor::System,
            payload: json!({ "workflowId": "flow-a" }),
            timestamp: Some(1_700_000_000_000),
            payload_hash: None,
        })
        .expect("append");

    assert_eq!(event.event_id, "run-1-1");
    assert_eq!(log.current_seq().expect("seq"), 1);

    let events = log.read_all().expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].run_id, "run-1");
    assert_eq!(events[0].event_type, "runCreated");

    let events_file = paths.workflow_run_dir("run-1").join("events.ndjson");
    assert!(events_file.exists());

    let _ = fs::remove_dir_all(paths.root());
}
