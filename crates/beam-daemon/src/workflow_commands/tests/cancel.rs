//! Tests for cancel-run command.
//!
//! Covers: runtime propagation (activityCanceled before runCanceled),
//! repeated idempotency, no direct complete_run_cancel call,
//! already-terminal safety, nonexistent-run error.

use super::*;

#[tokio::test]
/// Verifies that cancel_run() itself only writes cancelRequested, and
/// the runtime propagation (invoked by cancel_run) writes activityCanceled
/// (for running/open activities) before runCanceled.
async fn cancel_run_propagates_via_runtime_with_activity_canceled_before_run_canceled() {
    let paths = temp_paths("cancel-propagate");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-cancel-propagate";

    // Use a human-gate workflow so the run stays in Waiting state with
    // a running/open activity after one runtime tick.
    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
    let _ = bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: def,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "test",
            chat_binding: None,
        },
    )
    .expect("bootstrap");

    crate::run_workflow_runtime_once(&state, run_id, def).await;

    let outcome = cancel_run(&state, run_id, Some("test cancel".to_string()))
        .await
        .expect("cancel");

    assert!(outcome.ok, "cancel should succeed: {:?}", outcome);
    assert!(!outcome.already_cancelled);

    // Verify the log contains cancelRequested, and the runtime wrote
    // activityCanceled (for the gate) before runCanceled.
    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let has_cancel_requested = events.iter().any(|e| e.event_type == "cancelRequested");
    let has_activity_canceled = events.iter().any(|e| e.event_type == "activityCanceled");
    let has_run_canceled = events.iter().any(|e| e.event_type == "runCanceled");

    assert!(has_cancel_requested, "should have cancelRequested event");
    assert!(
        has_activity_canceled,
        "should have activityCanceled event for the open gate activity"
    );
    assert!(has_run_canceled, "runtime should propagate runCanceled");

    // Verify order: activityCanceled appears before runCanceled.
    let activity_pos = events
        .iter()
        .position(|e| e.event_type == "activityCanceled")
        .unwrap();
    let run_pos = events
        .iter()
        .position(|e| e.event_type == "runCanceled")
        .unwrap();
    assert!(
        activity_pos < run_pos,
        "activityCanceled (pos {}) must appear before runCanceled (pos {})",
        activity_pos,
        run_pos
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn cancel_run_repeated_is_idempotent() {
    let paths = temp_paths("cancel-idem");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-cancel-idem";

    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
    let _ = bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: def,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "test",
            chat_binding: None,
        },
    )
    .expect("bootstrap");

    crate::run_workflow_runtime_once(&state, run_id, def).await;

    // First cancel — runtime propagates fully, making run Cancelled.
    let outcome1 = cancel_run(&state, run_id, Some("first".to_string()))
        .await
        .expect("first cancel");
    assert!(outcome1.ok);
    assert!(!outcome1.already_cancelled);
    // After runtime propagation the run is terminal.
    assert_eq!(outcome1.status, "cancelled");

    // Second cancel — run is already terminal, should be idempotent.
    let outcome2 = cancel_run(&state, run_id, Some("second".to_string()))
        .await
        .expect("second cancel");
    assert!(outcome2.ok);
    assert!(
        outcome2.already_terminal,
        "second cancel should be alreadyTerminal (run is now cancelled)"
    );

    // Verify only one cancelRequested event was written.
    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let cancel_count = events
        .iter()
        .filter(|e| e.event_type == "cancelRequested")
        .count();
    assert_eq!(
        cancel_count, 1,
        "should have exactly one cancelRequested event, got {}",
        cancel_count
    );
    // The runtime writes exactly one runCanceled after the first cancel.
    let run_canceled_count = events
        .iter()
        .filter(|e| e.event_type == "runCanceled")
        .count();
    assert_eq!(
        run_canceled_count, 1,
        "should have exactly one runCanceled event, got {}",
        run_canceled_count
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

/// Verify that cancel_run() itself does NOT call complete_run_cancel —
/// the runCanceled event is always written by the runtime propagation
/// invoked from within cancel_run().
#[tokio::test]
async fn cancel_handler_does_not_directly_call_complete_run_cancel() {
    let paths = temp_paths("cancel-no-direct");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-cancel-no-direct";

    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
    let _ = bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: def,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "test",
            chat_binding: None,
        },
    )
    .expect("bootstrap");

    crate::run_workflow_runtime_once(&state, run_id, def).await;

    let outcome = cancel_run(&state, run_id, Some("test".to_string()))
        .await
        .expect("cancel");

    assert!(outcome.ok);

    // The handler writes cancelRequested; the runtime propagation writes
    // activityCanceled and runCanceled.
    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let has_cancel_requested = events.iter().any(|e| e.event_type == "cancelRequested");
    let has_activity_canceled = events.iter().any(|e| e.event_type == "activityCanceled");
    let has_run_canceled = events.iter().any(|e| e.event_type == "runCanceled");
    assert!(has_cancel_requested, "should have cancelRequested event");
    assert!(
        has_activity_canceled,
        "runtime should propagate activityCanceled"
    );
    assert!(has_run_canceled, "runtime should propagate runCanceled");

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn cancel_already_terminal_is_idempotent() {
    let paths = temp_paths("cancel-terminal");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-cancel-terminal";

    // HostExecutor that finishes immediately.
    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"true"},"unsafeAllowUngated":true}}}"#;
    let _ = bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: def,
            expected_workflow_id: Some("flow-a"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "test",
            chat_binding: None,
        },
    )
    .expect("bootstrap");

    crate::run_workflow_runtime_once(&state, run_id, def).await;

    let outcome = cancel_run(&state, run_id, Some("late cancel".to_string()))
        .await
        .expect("cancel on terminal");

    assert!(outcome.ok);
    assert!(outcome.already_terminal);
    assert!(!outcome.already_cancelled);

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn cancel_run_nonexistent_returns_error() {
    let paths = temp_paths("cancel-nofound");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);

    let outcome = cancel_run(&state, "nonexistent-run", None)
        .await
        .expect("cancel nonexistent");
    assert!(!outcome.ok);
    assert_eq!(outcome.error_code.as_deref(), Some("run_not_found"));

    let _ = std::fs::remove_dir_all(paths.root());
}
