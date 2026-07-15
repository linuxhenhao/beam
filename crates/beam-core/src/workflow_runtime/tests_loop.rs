use std::collections::BTreeMap;
use std::fs;

use crate::WorkflowOutputRef;
use crate::workflow_orchestrator::OrchestratorAction;
use crate::workflow_snapshot::{LoopIterationStatus, LoopStatus};
use crate::{EventDraft, EventLog, WorkflowActor};

use super::test_common::{min_workflow_json, temp_run_dir};
use super::*;

#[tokio::test]
async fn start_loop_writes_loop_started_event() {
    let run_dir = temp_run_dir("loop-start");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-start";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-start", "a"),
            expected_workflow_id: Some("flow-loop-start"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let action = OrchestratorAction::StartLoop {
        node_id: "loop-1".to_string(),
        max_iterations: 5,
    };
    start_loop(&mut log, &action).await.unwrap();

    let events = log.read_all().unwrap();
    let loop_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "loopStarted")
        .collect();
    assert_eq!(
        loop_events.len(),
        1,
        "expected exactly one loopStarted event"
    );
    let ev = loop_events[0];
    assert_eq!(ev.payload["loopId"], "loop-1");
    assert_eq!(ev.payload["maxIterations"], 5);
    assert_eq!(ev.actor, WorkflowActor::Scheduler);

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn start_loop_iteration_writes_event() {
    let run_dir = temp_run_dir("loop-iter-start");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-iter-start";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-iter-start", "a"),
            expected_workflow_id: Some("flow-loop-iter-start"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let action = OrchestratorAction::StartLoopIteration {
        node_id: "loop-1".to_string(),
        iteration: 2,
    };
    start_loop_iteration(&mut log, &action).await.unwrap();

    let events = log.read_all().unwrap();
    let loop_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "loopIterationStarted")
        .collect();
    assert_eq!(
        loop_events.len(),
        1,
        "expected exactly one loopIterationStarted event"
    );
    let ev = loop_events[0];
    assert_eq!(ev.payload["loopId"], "loop-1");
    assert_eq!(ev.payload["iteration"], 2);
    assert_eq!(ev.actor, WorkflowActor::Scheduler);

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn finish_loop_iteration_writes_event() {
    let run_dir = temp_run_dir("loop-iter-finish");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-iter-finish";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-iter-finish", "a"),
            expected_workflow_id: Some("flow-loop-iter-finish"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let action = OrchestratorAction::FinishLoopIteration {
        node_id: "loop-1".to_string(),
        iteration: 3,
        resolution: "approved".to_string(),
        decision_activity_id: Some("run-loop-iter-finish::gate::loop-1".to_string()),
        wait_resolved_event_id: None,
        by: Some("tester".to_string()),
        comment: Some("looks good".to_string()),
        timed_out: Some(false),
    };
    finish_loop_iteration(&mut log, &action).await.unwrap();

    let events = log.read_all().unwrap();
    let loop_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "loopIterationFinished")
        .collect();
    assert_eq!(
        loop_events.len(),
        1,
        "expected exactly one loopIterationFinished event"
    );
    let ev = loop_events[0];
    assert_eq!(ev.payload["loopId"], "loop-1");
    assert_eq!(ev.payload["iteration"], 3);
    assert_eq!(ev.payload["resolution"], "approved");
    assert_eq!(ev.payload["by"], "tester");
    assert_eq!(ev.payload["comment"], "looks good");
    assert_eq!(ev.payload["timedOut"], false);
    assert_eq!(ev.actor, WorkflowActor::Scheduler);

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn finish_loop_writes_loop_finished_event() {
    let run_dir = temp_run_dir("loop-finish");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-finish";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-finish", "a"),
            expected_workflow_id: Some("flow-loop-finish"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let output_ref = WorkflowOutputRef {
        output_hash: "sha256:abc".to_string(),
        output_path: "/tmp/loop-out.json".to_string(),
        output_bytes: 10,
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    };
    let action = OrchestratorAction::FinishLoop {
        node_id: "loop-1".to_string(),
        final_iteration: 3,
        resolution: "approved".to_string(),
        output_ref: Some(output_ref.clone()),
        error_code: None,
        error_class: None,
    };
    finish_loop(&mut log, &action).await.unwrap();

    let events = log.read_all().unwrap();
    let loop_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "loopFinished")
        .collect();
    assert_eq!(
        loop_events.len(),
        1,
        "expected exactly one loopFinished event"
    );
    let ev = loop_events[0];
    assert_eq!(ev.payload["loopId"], "loop-1");
    assert_eq!(ev.payload["finalIteration"], 3);
    assert_eq!(ev.payload["resolution"], "approved");
    assert_eq!(ev.actor, WorkflowActor::Scheduler);

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn all_loop_actions_produce_correct_event_sequence() {
    // Write a full loop lifecycle and verify event order and types.
    let run_dir = temp_run_dir("loop-sequence");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-seq";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-seq", "a"),
            expected_workflow_id: Some("flow-loop-seq"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();

    // Start loop
    start_loop(
        &mut log,
        &OrchestratorAction::StartLoop {
            node_id: "loop-1".to_string(),
            max_iterations: 3,
        },
    )
    .await
    .unwrap();

    // Iteration 1
    start_loop_iteration(
        &mut log,
        &OrchestratorAction::StartLoopIteration {
            node_id: "loop-1".to_string(),
            iteration: 1,
        },
    )
    .await
    .unwrap();
    finish_loop_iteration(
        &mut log,
        &OrchestratorAction::FinishLoopIteration {
            node_id: "loop-1".to_string(),
            iteration: 1,
            resolution: "approved".to_string(),
            decision_activity_id: None,
            wait_resolved_event_id: None,
            by: None,
            comment: None,
            timed_out: None,
        },
    )
    .await
    .unwrap();

    // Iteration 2 (rejected)
    start_loop_iteration(
        &mut log,
        &OrchestratorAction::StartLoopIteration {
            node_id: "loop-1".to_string(),
            iteration: 2,
        },
    )
    .await
    .unwrap();
    finish_loop_iteration(
        &mut log,
        &OrchestratorAction::FinishLoopIteration {
            node_id: "loop-1".to_string(),
            iteration: 2,
            resolution: "rejected".to_string(),
            decision_activity_id: Some(format!("{}::gate::loop-1", run_id)),
            wait_resolved_event_id: None,
            by: Some("reviewer".to_string()),
            comment: Some("needs work".to_string()),
            timed_out: Some(false),
        },
    )
    .await
    .unwrap();

    // Finish loop (overall approved after 2 iterations)
    let output_ref = WorkflowOutputRef {
        output_hash: "sha256:xyz".to_string(),
        output_path: "/tmp/xyz.json".to_string(),
        output_bytes: 8,
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    };
    finish_loop(
        &mut log,
        &OrchestratorAction::FinishLoop {
            node_id: "loop-1".to_string(),
            final_iteration: 2,
            resolution: "approved".to_string(),
            output_ref: Some(output_ref.clone()),
            error_code: None,
            error_class: None,
        },
    )
    .await
    .unwrap();

    let events = log.read_all().unwrap();
    let loop_event_types: Vec<_> = events
        .iter()
        .filter(|e| e.event_type.starts_with("loop"))
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(
        loop_event_types,
        vec![
            "loopStarted",
            "loopIterationStarted",
            "loopIterationFinished",
            "loopIterationStarted",
            "loopIterationFinished",
            "loopFinished",
        ],
        "expected full loop lifecycle events in order"
    );

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn replay_builds_snapshot_loops_from_loop_events() {
    // Write loop events directly to the events file, then replay and check
    // snapshot.loops.
    let run_dir = temp_run_dir("loop-replay");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-replay";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-replay", "a"),
            expected_workflow_id: Some("flow-loop-replay"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // The bootstrap already wrote runCreated + runStarted. Now append loop events directly.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopStarted".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "maxIterations": 3,
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopIterationStarted".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "iteration": 1,
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopIterationFinished".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "iteration": 1,
                    "resolution": "approved",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopFinished".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "finalIteration": 1,
                    "resolution": "approved",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Read snapshot and verify loops
    let rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-loop-replay".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::new(),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let snapshot = read_snapshot(&rt).await.unwrap();

    let loops = snapshot
        .loops
        .as_ref()
        .expect("snapshot.loops should be Some after loop events");
    assert!(
        loops.contains_key("loop-1"),
        "loop-1 should be in snapshot.loops"
    );

    let loop_state = loops.get("loop-1").unwrap();
    assert_eq!(loop_state.loop_id, "loop-1");
    assert_eq!(
        loop_state.status,
        LoopStatus::Succeeded,
        "loop should be Succeeded (approved)"
    );
    assert_eq!(loop_state.max_iterations, 3);
    assert_eq!(loop_state.iteration, 1, "finalIteration should be 1");
    assert_eq!(
        loop_state.iterations.len(),
        1,
        "should have 1 iteration recorded"
    );

    let iteration = &loop_state.iterations[0];
    assert_eq!(iteration.iteration, 1);
    assert_eq!(iteration.status, LoopIterationStatus::Approved);

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn replay_loop_failed_sets_status_and_dangling_iteration() {
    // Simulate a loop that fails midway: loopStarted → iter1 start/finish
    // (approved) → iter2 started → loopFinished with resolution=failed.
    // The inflight iteration should be marked Failed.
    let run_dir = temp_run_dir("loop-failed-replay");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-loop-failed-replay";
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &min_workflow_json("flow-loop-failed-replay", "a"),
            expected_workflow_id: Some("flow-loop-failed-replay"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopStarted".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "maxIterations": 5,
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopIterationStarted".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "iteration": 1,
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopIterationFinished".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "iteration": 1,
                    "resolution": "approved",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "loopIterationStarted".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "iteration": 2,
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        // Loop finishes with failure while iteration 2 is still running
        let _ = log
            .append(EventDraft {
                event_type: "loopFinished".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "loopId": "loop-1",
                    "finalIteration": 2,
                    "resolution": "failed",
                    "errorCode": "LoopFailedMidIteration",
                    "errorClass": "fatal",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-loop-failed-replay".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::new(),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let snapshot = read_snapshot(&rt).await.unwrap();

    let loops = snapshot
        .loops
        .as_ref()
        .expect("snapshot.loops should be Some after loop events");
    let loop_state = loops.get("loop-1").unwrap();
    assert_eq!(
        loop_state.status,
        LoopStatus::Failed,
        "loop should be Failed"
    );
    assert_eq!(loop_state.error_class.as_deref(), Some("fatal"));
    assert_eq!(
        loop_state.error_code.as_deref(),
        Some("LoopFailedMidIteration")
    );
    assert_eq!(loop_state.iteration, 2);
    assert_eq!(loop_state.iterations.len(), 2);

    // Iteration 1 should be Approved
    let iter1 = loop_state
        .iterations
        .iter()
        .find(|it| it.iteration == 1)
        .unwrap();
    assert_eq!(iter1.status, LoopIterationStatus::Approved);

    // Iteration 2 should be Failed (inflight → Failed on loop finish failure)
    let iter2 = loop_state
        .iterations
        .iter()
        .find(|it| it.iteration == 2)
        .unwrap();
    assert_eq!(
        iter2.status,
        LoopIterationStatus::Failed,
        "inflight iteration should be Failed"
    );

    let _ = fs::remove_dir_all(&run_dir);
}
