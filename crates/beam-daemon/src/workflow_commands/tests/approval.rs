//! Tests for Lark-card-originated approve / reject commands.
//!
//! Covers: basic approve, reject (with comment), repeated idempotency,
//! approver-allowlist enforcement, and already-terminal safety.

use super::*;

#[tokio::test]
async fn lark_approve_writes_wait_resolved() {
    let paths = temp_paths("lark-approve");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-lark-approve";

    // Bootstrap a tiny workflow with a human-gate node.
    // Human-gate wait via hostExecutor node with humanGate field.
    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?"}}}}"#;
    let _bootstrap = bootstrap_workflow_run(
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

    // Advance runtime once to create the wait.
    crate::run_workflow_runtime_once(&state, run_id, def).await;

    // Read snapshot to get activity_id / attempt_id.
    let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .expect("snapshot")
        .expect("snapshot exists");
    assert!(
        !snap.dangling.waits.is_empty(),
        "expected dangling waits after runtime boot"
    );
    let activity_id = snap.dangling.waits[0].clone();
    let activity = snap
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id)
        .expect("activity");
    let attempt_id = activity
        .attempts
        .last()
        .expect("attempt")
        .attempt_id
        .clone();

    // Execute approve via Lark path.
    let outcome = lark_approve_or_reject_wait(
        &state,
        run_id,
        &activity_id,
        &attempt_id,
        "user_approver",
        WaitResolution::Approved,
        None,
    )
    .await
    .expect("lark approve");

    assert!(outcome.ok, "approve should succeed: {:?}", outcome);
    assert!(!outcome.already_resolved);
    assert!(!outcome.already_terminal);

    // Verify the event log contains waitResolved.
    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let resolved_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "waitResolved")
        .collect();
    assert_eq!(
        resolved_events.len(),
        1,
        "should have exactly one waitResolved event, got {}",
        resolved_events.len()
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn lark_reject_writes_wait_resolved_rejected() {
    let paths = temp_paths("lark-reject");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-lark-reject";

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

    let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .expect("snapshot")
        .expect("snapshot exists");
    let activity_id = snap.dangling.waits[0].clone();
    let activity = snap
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id)
        .expect("activity");
    let attempt_id = activity
        .attempts
        .last()
        .expect("attempt")
        .attempt_id
        .clone();

    let outcome = lark_approve_or_reject_wait(
        &state,
        run_id,
        &activity_id,
        &attempt_id,
        "user_approver",
        WaitResolution::Rejected,
        Some("nope".to_string()),
    )
    .await
    .expect("lark reject");

    assert!(outcome.ok, "reject should succeed: {:?}", outcome);
    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let resolved: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "waitResolved")
        .collect();
    assert_eq!(
        resolved.len(),
        1,
        "should have exactly one waitResolved event"
    );
    let res_event = resolved[0];
    assert_eq!(
        res_event.payload["resolution"], "rejected",
        "resolution should be rejected"
    );
    assert_eq!(
        res_event.payload["by"], "user_approver",
        "by should be the approver"
    );
    assert_eq!(res_event.payload["comment"], "nope");

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn lark_approve_repeated_is_idempotent() {
    let paths = temp_paths("lark-idem");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-lark-idem";

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

    let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .expect("snapshot")
        .expect("snapshot exists");
    let activity_id = snap.dangling.waits[0].clone();
    let activity = snap
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id)
        .expect("activity");
    let attempt_id = activity
        .attempts
        .last()
        .expect("attempt")
        .attempt_id
        .clone();

    // First approve.
    let outcome1 = lark_approve_or_reject_wait(
        &state,
        run_id,
        &activity_id,
        &attempt_id,
        "user_approver",
        WaitResolution::Approved,
        None,
    )
    .await
    .expect("first approve");
    assert!(outcome1.ok);
    assert!(!outcome1.already_resolved);

    // Second approve — should be idempotent (alreadyResolved).
    let outcome2 = lark_approve_or_reject_wait(
        &state,
        run_id,
        &activity_id,
        &attempt_id,
        "user_approver",
        WaitResolution::Approved,
        None,
    )
    .await
    .expect("second approve");
    assert!(outcome2.ok, "second approve should succeed: {:?}", outcome2);
    // After the first approve, the runtime may finish the run entirely,
    // so the second call may see either alreadyResolved or alreadyTerminal.
    assert!(
        outcome2.already_resolved || outcome2.already_terminal,
        "second approve should be idempotent (alreadyResolved or alreadyTerminal), got: ok={}, already_resolved={}, already_terminal={}",
        outcome2.ok,
        outcome2.already_resolved,
        outcome2.already_terminal,
    );

    // Verify only one waitResolved event was written.
    let log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).expect("log");
    let events = log.read_all().expect("read events");
    let resolved_count = events
        .iter()
        .filter(|e| e.event_type == "waitResolved")
        .count();
    assert_eq!(
        resolved_count, 1,
        "should have exactly one waitResolved event, got {}",
        resolved_count
    );

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn lark_approve_with_approver_allowlist() {
    let paths = temp_paths("lark-allowlist");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-lark-allowlist";

    // Include an approver allowlist: only "user_a" and "user_b".
    let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeGate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"approve?","approvers":["user_a","user_b"]}}}}"#;
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

    let snap = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .expect("snapshot")
        .expect("snapshot exists");
    let activity_id = snap.dangling.waits[0].clone();
    let activity = snap
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id)
        .expect("activity");
    let attempt_id = activity
        .attempts
        .last()
        .expect("attempt")
        .attempt_id
        .clone();

    // Non-approved user.
    let outcome_denied = lark_approve_or_reject_wait(
        &state,
        run_id,
        &activity_id,
        &attempt_id,
        "user_c",
        WaitResolution::Approved,
        None,
    )
    .await
    .expect("denied approve");
    assert!(!outcome_denied.ok);
    assert_eq!(outcome_denied.error_code.as_deref(), Some("not_approved"));

    // Approved user.
    let outcome_ok = lark_approve_or_reject_wait(
        &state,
        run_id,
        &activity_id,
        &attempt_id,
        "user_a",
        WaitResolution::Approved,
        None,
    )
    .await
    .expect("allowed approve");
    assert!(outcome_ok.ok);

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn lark_approve_already_terminal_is_idempotent() {
    let paths = temp_paths("lark-terminal");
    let _ = std::fs::remove_dir_all(paths.root());
    let state = make_state(&paths);
    let run_id = "run-lark-terminal";

    // Use a node that succeeds immediately (no wait).
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

    let outcome = lark_approve_or_reject_wait(
        &state,
        run_id,
        "nonexistent-activity",
        "nonexistent-attempt",
        "user",
        WaitResolution::Approved,
        None,
    )
    .await
    .expect("approve on terminal run");

    // The run is Succeeded (terminal), so the outcome should reflect that.
    assert!(outcome.already_terminal || !outcome.ok);

    let _ = std::fs::remove_dir_all(paths.root());
}
