//! Tests for dashboard-originated approve / reject commands.
//!
//! Covers: basic approve, reject, already-terminal idempotency,
//! and no-wait scenario that returns alreadyTerminal.

use super::*;

#[tokio::test]
async fn dashboard_approve_writes_wait_resolved() {
    let paths = temp_paths("dash-approve");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-dash-approve";

    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
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

    let result = dashboard_approve_or_reject_wait(
        &state,
        run_id,
        WaitResolution::Approved,
        Some("looks good".to_string()),
    )
    .await
    .expect("dash approve");
    assert_eq!(result.0, axum::http::StatusCode::OK);

    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let resolved_count = events
        .iter()
        .filter(|e| e.event_type == "waitResolved")
        .count();
    assert_eq!(resolved_count, 1);

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn dashboard_reject_writes_wait_resolved_rejected() {
    let paths = temp_paths("dash-reject");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-dash-reject";

    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
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

    let result = dashboard_approve_or_reject_wait(&state, run_id, WaitResolution::Rejected, None)
        .await
        .expect("dash reject");
    assert_eq!(result.0, axum::http::StatusCode::OK);

    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let resolved: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "waitResolved")
        .collect();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].payload["resolution"], "rejected");

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn dashboard_approve_already_terminal_is_idempotent() {
    let paths = temp_paths("dash-terminal");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-dash-terminal";

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

    let result = dashboard_approve_or_reject_wait(&state, run_id, WaitResolution::Approved, None)
        .await
        .expect("dash terminal approve");
    let (status, body) = result;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(body["alreadyTerminal"], true);

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn dashboard_approve_no_wait_returns_already_terminal() {
    let paths = temp_paths("dash-nowait");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-dash-nowait";

    // HostExecutor finishes immediately, no wait.  The handler returns
    // Ok instead of Err when the run is already terminal (idempotency).
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

    let result =
        dashboard_approve_or_reject_wait(&state, run_id, WaitResolution::Approved, None).await;
    assert!(result.is_ok(), "already-terminal run should succeed");
    let (status, body) = result.unwrap();
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(body["alreadyTerminal"], true);

    let _ = std::fs::remove_dir_all(paths.root());
}
