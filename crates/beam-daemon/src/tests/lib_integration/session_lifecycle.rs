use super::*;

#[test]
fn schedule_create_appends_to_file() {
    let tmp = std::env::temp_dir().join(format!("beam-sched-test-{}", uuid::Uuid::new_v4()));
    let paths = BeamPaths::from_root(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();

    let task = serde_json::json!({
        "scheduleId": "sched-1",
        "content": "daily at 9am",
        "createdAt": "2026-01-01T00:00:00Z",
        "status": "active",
    });
    let schedules_path = paths.schedules_json();
    std::fs::write(
        &schedules_path,
        serde_json::to_string_pretty(&vec![task]).unwrap(),
    )
    .unwrap();

    let loaded: Vec<serde_json::Value> =
        serde_json::from_str(&std::fs::read_to_string(&schedules_path).unwrap()).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0]["scheduleId"].as_str().unwrap(), "sched-1");

    let _ = std::fs::remove_dir_all(&tmp);
}

// -----------------------------------------------------------------------
// Task 6.3: Worker termination tests
// -----------------------------------------------------------------------

/// Spawn a child process, register it in both `state.sessions` and
/// `state.workers`, then call `terminate_workflow_worker_process`.
/// Verify that the *`try_wait()` path* detects the child's exit promptly
/// (well before the 5-second grace), so that a SIGINT-responsive worker
/// does not suffer a pointless full-grace wait + SIGKILL.
#[tokio::test]
async fn terminate_workflow_worker_process_exits_early_via_try_wait() {
    let paths = temp_paths("terminate-trywait");
    maybe_remove_dir(&paths.root().to_path_buf());
    let state = make_state(paths.clone(), HashMap::new());

    // Spawn a long-running "worker" (sleep 60).
    let mut child = tokio::process::Command::new("sleep")
        .arg("60")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn sleep");

    let worker_pid = child.id().expect("child should have a pid");
    let session_id = "session-trywait";

    // Register both the session *and* the worker handle so that the
    // grace poll uses try_wait() rather than the zombie-prone kill(0).
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(
            session_id.to_string(),
            Session {
                session_id: session_id.to_string(),
                worker_pid: Some(worker_pid),
                status: SessionStatus::Active,
                closed_at: None,
                ..make_session(session_id)
            },
        );
    }
    {
        let stdin = child.stdin.take().expect("stdin");
        state.workers.lock().await.insert(
            session_id.to_string(),
            WorkerHandle {
                child,
                stdin: std::sync::Arc::new(tokio::sync::Mutex::new(stdin)),
            },
        );
    }

    // Verify the process is alive before termination.
    let alive_before = unsafe { libc::kill(worker_pid as i32, 0) == 0 };
    assert!(alive_before, "child should be alive before termination");

    // Terminate — `sleep` honours SIGINT, so try_wait should detect the
    // exit within a few poll cycles.
    let start = tokio::time::Instant::now();
    terminate_workflow_worker_process(&state, session_id).await;
    let elapsed = start.elapsed();

    // The grace period is 5 s.  A SIGINT-responsive process should exit
    // *much* faster (typically < 1 s).  We use 3 s as a generous upper
    // bound to prove we didn't wait the full grace.
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "try_wait should detect exit well before 5 s grace, got {:?}",
        elapsed
    );

    // Retrieve the child and verify its exit status.
    let mut child = {
        let mut workers = state.workers.lock().await;
        workers
            .remove(session_id)
            .expect("worker handle should still be there")
            .child
    };
    let exit_status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
        .await
        .expect("wait should not time out")
        .expect("wait should succeed");

    assert!(
        !exit_status.success(),
        "sleep process should be killed by signal (exit status: {:?})",
        exit_status
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

/// Fallback path: when the child is *not* registered in `state.workers`,
/// `terminate_workflow_worker_process` falls back to `kill(pid, 0)`.  A
/// responsive child still exits, but the zombie means `kill(0)` keeps
/// returning success, so we wait the full 5-second grace and escalate to
/// SIGKILL.  This test documents the *current behaviour* of the fallback
/// and verifies the child is killed regardless.
#[tokio::test]
async fn terminate_workflow_worker_process_fallback_kills_child() {
    let paths = temp_paths("terminate-fallback");
    maybe_remove_dir(&paths.root().to_path_buf());
    let state = make_state(paths.clone(), HashMap::new());

    let mut child = tokio::process::Command::new("sleep")
        .arg("60")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn sleep");

    let worker_pid = child.id().expect("child should have a pid");
    let session_id = "session-fallback";

    // Session has worker_pid but *no* worker handle in state.workers.
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(
            session_id.to_string(),
            Session {
                session_id: session_id.to_string(),
                worker_pid: Some(worker_pid),
                status: SessionStatus::Active,
                closed_at: None,
                ..make_session(session_id)
            },
        );
    }

    let alive_before = unsafe { libc::kill(worker_pid as i32, 0) == 0 };
    assert!(alive_before, "child should be alive before termination");

    // Without a handle, the grace loop falls back to kill(pid, 0) which
    // is zombie-prone → we'll wait the full grace + send SIGKILL.  The
    // child is killed eventually.
    let start = tokio::time::Instant::now();
    terminate_workflow_worker_process(&state, session_id).await;
    let elapsed = start.elapsed();

    // Fallback path will wait close to the full 5 s grace period.
    assert!(
        elapsed >= std::time::Duration::from_secs(3),
        "fallback should wait at least most of the grace, got {:?}",
        elapsed
    );

    let exit_status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
        .await
        .expect("child wait should not time out")
        .expect("child wait should succeed");

    assert!(
        !exit_status.success(),
        "sleep process should be killed by signal (exit status: {:?})",
        exit_status
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

/// Verify that `terminate_workflow_worker_process` is a no-op when there
/// is no worker PID (session exists but worker was never spawned).
#[tokio::test]
async fn terminate_workflow_worker_process_no_pid_is_noop() {
    let paths = temp_paths("terminate-no-pid");
    maybe_remove_dir(&paths.root().to_path_buf());
    let state = make_state(paths.clone(), HashMap::new());
    let session_id = "session-no-pid";

    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(
            session_id.to_string(),
            Session {
                session_id: session_id.to_string(),
                worker_pid: None,
                status: SessionStatus::Active,
                closed_at: None,
                ..make_session(session_id)
            },
        );
    }

    // Should not panic or error.
    terminate_workflow_worker_process(&state, session_id).await;

    // Session should still exist and be active.
    {
        let sessions = state.sessions.lock().await;
        let session = sessions.get(session_id).expect("session should exist");
        assert_eq!(session.status, SessionStatus::Active);
        assert!(session.worker_pid.is_none());
    }

    maybe_remove_dir(&paths.root().to_path_buf());
}

/// Verify that when we call cancel_run on a run that has an active
/// cancellation token registered, the token is cancelled immediately
/// (existing behaviour from Task 6.2), and the registry is cleaned up.
#[tokio::test]
async fn cancel_run_clears_registry_and_session_cleanup_works() {
    use beam_core::{BootstrapWorkflowRunInput, bootstrap_workflow_run};

    let paths = temp_paths("cancel-session-cleanup");
    maybe_remove_dir(&paths.root().to_path_buf());
    let state = make_state(paths.clone(), HashMap::new());
    let run_id = "run-cancel-cleanup";

    // Bootstrap a human-gate workflow so the run stays in Waiting state.
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

    // Register a fake activity token to simulate active dispatch.
    let reg = crate::workflow_cancellation::global_cancellation_registry();
    let token = reg.register_activity(run_id, &format!("{}::work::nodeA", run_id));
    assert_eq!(reg.total_activities(), 1);

    // Cancel the run.
    let outcome = crate::workflow_commands::cancel_run(&state, run_id, Some("test".to_string()))
        .await
        .expect("cancel");

    assert!(outcome.ok);
    assert_eq!(outcome.status, "cancelled");

    // Token should be cancelled and registry cleaned up.
    assert!(token.is_cancelled());
    assert_eq!(reg.total_activities(), 0);

    maybe_remove_dir(&paths.root().to_path_buf());
}

// -----------------------------------------------------------------------
// Task 7.2: cold attach 使用统一 recovery run loop
// -----------------------------------------------------------------------

/// Verifies that cold scan discovers non-terminal runs and skips terminal
/// (succeeded/failed/cancelled) runs, even when both have chat bindings.
#[tokio::test]
async fn cold_scan_discovers_non_terminal_and_skips_terminal_runs() {
    use beam_core::{
        BootstrapWorkflowRunInput, EventDraft, EventLog, WorkflowActor, bootstrap_workflow_run,
        scan_cold_workflow_runs,
    };

    let paths = temp_paths("cold-scan-disc");
    maybe_remove_dir(&paths.root().to_path_buf());

    let lark_app_id = "app-cold-scan";
    let def = r#"{"workflowId":"flow-cs","version":1,"nodes":{"a":{"type":"subagent","bot":"bot","prompt":"hello"}}}"#;
    let params: BTreeMap<String, Value> = BTreeMap::new();
    let binding = beam_core::RunChatBinding {
        chat_id: "chat-1".to_string(),
        lark_app_id: lark_app_id.to_string(),
    };

    // Non-terminal run (no terminal event written yet — just bootstrapped).
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id: "run-nonterm",
            workflow_json: def,
            expected_workflow_id: Some("flow-cs"),
            params: &params,
            initiator: "test",
            chat_binding: Some(binding.clone()),
        },
    )
    .expect("bootstrap nonterm");

    // Terminal run — write runSucceeded manually.
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id: "run-term",
            workflow_json: def,
            expected_workflow_id: Some("flow-cs"),
            params: &params,
            initiator: "test",
            chat_binding: Some(binding),
        },
    )
    .expect("bootstrap term");
    {
        let mut log = EventLog::new("run-term", paths.workflow_runs_dir()).unwrap();
        log.append(EventDraft {
            event_type: "runSucceeded".to_string(),
            actor: WorkflowActor::Scheduler,
            payload: serde_json::json!({}),
            timestamp: None,
            payload_hash: None,
        })
        .unwrap();
    }

    let (runs, stats) = scan_cold_workflow_runs(&paths, lark_app_id).await.unwrap();
    assert_eq!(
        stats.discovered, 1,
        "only the non-terminal run should be discovered"
    );
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id, "run-nonterm");
    assert!(
        stats.skipped.is_empty(),
        "no runs should be skipped with errors"
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

/// Verifies that cold-attaching a workflow with an open human-gate wait
/// does NOT terminalize it — the unified driver / run_loop recovery
/// correctly returns AwaitingWait and leaves the wait dangling.
#[tokio::test]
async fn cold_attach_open_human_gate_wait_not_terminalized() {
    use beam_core::{
        BootstrapWorkflowRunInput, RunStatus, bootstrap_workflow_run, read_run_snapshot,
    };

    let paths = temp_paths("cold-attach-open");
    maybe_remove_dir(&paths.root().to_path_buf());
    let state = make_state(paths.clone(), HashMap::new());
    let run_id = "run-cold-open";

    // Human-gate workflow: will create a wait and stay in AwaitingWait.
    let def = r#"{"workflowId":"flow-co","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"Approve?"}}}}"#;
    let binding = beam_core::RunChatBinding {
        chat_id: "oc_test".to_string(),
        lark_app_id: "app_test".to_string(),
    };
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: def,
            expected_workflow_id: Some("flow-co"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "test",
            chat_binding: Some(binding),
        },
    )
    .expect("bootstrap");

    // Advance once to create the wait.
    crate::run_workflow_runtime_once(&state, run_id, def).await;

    // Verify we have an open wait (not terminal).
    let sn = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    assert!(!sn.dangling.waits.is_empty(), "should have an open wait");
    assert!(
        !matches!(
            sn.run.status,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        ),
        "run should NOT be terminal"
    );

    // Simulate cold attach: call the unified driver again.
    // The driver calls run_loop which has built-in recovery; it should
    // detect the open wait and return AwaitingWait, NOT terminalize.
    workflow_runtime_driver::run(&state, run_id, def).await;

    // After cold attach, the wait should still be open and the run
    // should still be non-terminal.
    let sn2 = read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !sn2.dangling.waits.is_empty(),
        "open wait should still be dangling after cold attach"
    );
    assert!(
        !matches!(
            sn2.run.status,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        ),
        "run should NOT be terminal after cold attach with open wait"
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

/// Verifies that cold-attaching a workflow whose wait was resolved but
/// whose terminal event was never written (e.g. crash after resolution)
/// correctly materializes the terminal via the unified run_loop recovery.
#[tokio::test]
async fn cold_attach_recovery_materializes_resolved_wait_terminal() {
    use beam_core::{
        BootstrapWorkflowRunInput, EventDraft, EventLog, WorkflowActor, bootstrap_workflow_run,
    };

    let paths = temp_paths("cold-attach-rec");
    maybe_remove_dir(&paths.root().to_path_buf());
    let state = make_state(paths.clone(), HashMap::new());
    let run_id = "run-cold-rec";

    // Single-node human-gate workflow.
    let def = r#"{"workflowId":"flow-cr","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"OK?"}}}}"#;
    let binding = beam_core::RunChatBinding {
        chat_id: "oc_test".to_string(),
        lark_app_id: "app_test".to_string(),
    };
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id,
            workflow_json: def,
            expected_workflow_id: Some("flow-cr"),
            params: &BTreeMap::<String, Value>::new(),
            initiator: "test",
            chat_binding: Some(binding),
        },
    )
    .expect("bootstrap");

    // Advance to create the wait, then read the wait info.
    crate::run_workflow_runtime_once(&state, run_id, def).await;

    // Grab the activity_id from the wait, and the attempt_id from the
    // activity's latest attempt, so we can craft a valid resolution event.
    let sn = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    let activity_id = sn
        .dangling
        .waits
        .first()
        .expect("should have a wait")
        .clone();
    // Sanity-check: the activity exists and has an attempt.
    let _activity = sn
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id)
        .expect("should find the waiting activity");
    assert!(
        !_activity.attempts.is_empty(),
        "activity should have at least one attempt"
    );

    // Simulate a crash scenario: write waitResolved (resolution approved)
    // but NOT activitySucceeded (terminal).  This leaves the wait in a
    // "resolved but no terminal" dangling state.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        log.append(EventDraft {
            event_type: "waitResolved".to_string(),
            actor: WorkflowActor::Human,
            payload: serde_json::json!({
                "activityId": activity_id,
                "resolution": "approved",
                "by": "test_user",
                "comment": "LGTM",
            }),
            timestamp: None,
            payload_hash: None,
        })
        .unwrap();
    }

    // Verify the snapshot now has a wait resolution but no terminal for
    // the activity — i.e. `dangling.wait_resolutions` is non-empty.
    let sn_pre = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        sn_pre.dangling.waits.is_empty(),
        "after resolution, waits should be cleared"
    );
    assert!(
        !sn_pre.dangling.wait_resolutions.is_empty(),
        "should have dangling wait resolutions (resolved but no terminal)"
    );

    // Simulate cold attach: the unified driver will call run_loop, and
    // the built-in wait-resolution recovery phase should materialize the
    // activitySucceeded terminal.
    workflow_runtime_driver::run(&state, run_id, def).await;

    // After recovery, the wait resolution should be cleared and the
    // activity should have been terminalized.
    let sn_post = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        sn_post.dangling.wait_resolutions.is_empty(),
        "after recovery, dangling wait resolutions should be cleared"
    );
    assert!(sn_post.dangling.waits.is_empty(), "no waits should remain");

    // The workflow should have progressed — since this is a single-node
    // workflow and the node has now succeeded, the run should be terminal.
    let terminal = matches!(
        sn_post.run.status,
        beam_core::RunStatus::Succeeded
            | beam_core::RunStatus::Failed
            | beam_core::RunStatus::Cancelled
    );
    assert!(
        terminal,
        "run should be terminal after recovery, got {:?}",
        sn_post.run.status
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn recent_lark_events_save_load_roundtrip() {
    let paths = temp_paths("recent-lark-events-roundtrip");
    maybe_remove_dir(&paths.root().to_path_buf());

    // Simulate a fresh event (just inserted)
    let mut events = HashMap::new();
    events.insert("evt-fresh".to_string(), std::time::Instant::now());
    // Simulate an event that's 4 minutes old (still within 5 min TTL)
    events.insert(
        "evt-old".to_string(),
        std::time::Instant::now() - std::time::Duration::from_secs(240),
    );

    save_recent_lark_events(&paths, &events).await;
    let loaded = load_recent_lark_events(&paths).await;

    // Both events should survive roundtrip (they're within the 5-min TTL)
    assert!(
        loaded.contains_key("evt-fresh"),
        "fresh event should survive roundtrip"
    );
    assert!(
        loaded.contains_key("evt-old"),
        "4-min-old event should survive roundtrip"
    );

    // The "old" event's Instant should approximate the original (within a few seconds)
    let loaded_instant = loaded.get("evt-old").unwrap();
    let elapsed = loaded_instant.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_secs(239)
            && elapsed <= std::time::Duration::from_secs(242),
        "old event elapsed should be ~240s, got {:?}",
        elapsed
    );

    let _ = std::fs::remove_file(paths.recent_lark_events_json());
    maybe_remove_dir(&paths.root().to_path_buf());
}
