use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::WorkflowOutputRef;
use crate::workflow_definition::{HostExecutorNode, NodeBase, SubagentNode};
use crate::workflow_snapshot::{DanglingSnapshot, RunState, RunStatus};
use crate::{EventDraft, EventLog, RunSnapshotDTO, WorkflowActor};

use super::test_common::{FakeHooks, temp_run_dir};
use super::*;

/// A hook that counts how many times `recover_dangling_effects` is called.
#[derive(Clone)]
struct RecoveryCountingHooks {
    recovery_calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkflowExecutionHooks for RecoveryCountingHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        resolved_input: Value,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: resolved_input,
            session: None,
        })
    }

    async fn recover_dangling_effects(
        &mut self,
        _log: &mut EventLog,
        _snapshot: &RunSnapshotDTO,
    ) -> Result<RecoveryResult> {
        *self.recovery_calls.lock().await += 1;
        Ok(RecoveryResult {
            had_progress: false,
            has_remaining_dangling: true,
        })
    }
}

#[tokio::test]
async fn run_loop_calls_recovery_when_dangling_effects_present() {
    let run_dir = temp_run_dir("recovery-call");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-recovery-call";
    let _ = crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-recovery","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-recovery"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // Activity id that the orchestrator expects for node "a" work.
    let work_activity_id = format!("{}::work::a", run_id);
    let work_attempt_id = format!("{}::work::a::att-1", run_id);

    // Write events that create a dangling effectAttempted activity matching
    // the orchestrator's work activity id.
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": &work_activity_id,
                    "attemptId": &work_attempt_id,
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:aa",
                        "outputPath": "/tmp/aa",
                        "outputBytes": 2,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json"
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "effectAttempted".to_string(),
                actor: WorkflowActor::HostExecutor,
                payload: serde_json::json!({
                    "activityId": &work_activity_id,
                    "attemptId": &work_attempt_id,
                    "idempotencyKey": "wf_key_recovery",
                    "inputHash": "sha256:bb",
                    "idempotencyTtlMs": 60000u64,
                    "provider": "feishu-im",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-recovery".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: Some(true),
                    },
                    executor: "feishu-send".to_string(),
                    input: serde_json::json!({"chatId":"chat-1","content":"hello"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let recovery_calls = Arc::new(Mutex::new(0usize));
    let mut hooks = RecoveryCountingHooks {
        recovery_calls: recovery_calls.clone(),
    };
    let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();

    let calls = *recovery_calls.lock().await;
    assert!(
        calls > 0,
        "expected recover_dangling_effects to be called at least once, got {calls}"
    );
    // Since recovery returned had_progress=false, the loop should fall
    // through to run_tick and eventually stop (not loop forever on recovery).
    assert!(
        matches!(
            result.reason,
            RunLoopStopReason::NoProgress | RunLoopStopReason::AwaitingWait
        ),
        "expected NoProgress or AwaitingWait, got {:?}",
        result.reason
    );
    let _ = fs::remove_dir_all(&run_dir);
}

/// A hook that simulates successful recovery: writes activitySucceeded for
/// every dangling effectAttempted activity.
#[derive(Clone)]
struct RecoveringHooks;

#[async_trait]
impl WorkflowExecutionHooks for RecoveringHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        resolved_input: Value,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: resolved_input,
            session: None,
        })
    }

    async fn recover_dangling_effects(
        &mut self,
        log: &mut EventLog,
        snapshot: &RunSnapshotDTO,
    ) -> Result<RecoveryResult> {
        let mut had_progress = false;
        for activity_id in &snapshot.dangling.effect_attempted {
            if let Some(activity) = snapshot
                .activities
                .iter()
                .find(|a| &a.activity_id == activity_id)
            {
                if let Some(latest) = activity.attempts.last() {
                    let output_ref = WorkflowOutputRef {
                        output_hash: "sha256:recovered".to_string(),
                        output_path: "/tmp/recovered".to_string(),
                        output_bytes: 3,
                        output_schema_version: 1,
                        content_type: Some("application/json".to_string()),
                    };
                    let _ = log.append(EventDraft {
                        event_type: "activitySucceeded".to_string(),
                        actor: WorkflowActor::System,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "attemptId": &latest.attempt_id,
                            "outputRef": output_ref,
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    had_progress = true;
                }
            }
        }
        Ok(RecoveryResult {
            had_progress,
            has_remaining_dangling: false,
        })
    }
}

#[tokio::test]
async fn run_loop_replays_after_recovery_writes_events() {
    let run_dir = temp_run_dir("recovery-replay");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-recovery-replay";
    let _ = crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-replay","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-replay"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // Activity id that the orchestrator expects for node "a" work.
    let work_activity_id = format!("{}::work::a", run_id);
    let work_attempt_id = format!("{}::work::a::att-1", run_id);

    // Write events that create a dangling effectAttempted activity
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": &work_activity_id,
                    "attemptId": &work_attempt_id,
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:aa",
                        "outputPath": "/tmp/aa",
                        "outputBytes": 2,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json"
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "effectAttempted".to_string(),
                actor: WorkflowActor::HostExecutor,
                payload: serde_json::json!({
                    "activityId": &work_activity_id,
                    "attemptId": &work_attempt_id,
                    "idempotencyKey": "wf_key_replay",
                    "inputHash": "sha256:bb",
                    "idempotencyTtlMs": 60000u64,
                    "provider": "feishu-im",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-replay".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: Some(true),
                    },
                    executor: "feishu-send".to_string(),
                    input: serde_json::json!({"chatId":"chat-1","content":"hello"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = RecoveringHooks;
    let result = run_loop(&mut rt, &mut hooks, 5, 1).await.unwrap();

    // After recovery writes activitySucceeded, the orchestrator should
    // be able to complete the node and the run.  Verify that the run
    // reached a terminal state or made measurable progress.
    let final_snapshot = read_snapshot(&rt).await.unwrap();
    assert!(
        matches!(
            final_snapshot.run.status,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        ) || result.ticks > 0,
        "expected recovery to allow progress; reason={:?} ticks={} status={:?}",
        result.reason,
        result.ticks,
        final_snapshot.run.status
    );
    // Verify the recovery event was written
    let events = rt.log.read_all().unwrap();
    let has_recovered = events
        .iter()
        .any(|e| e.event_type == "activitySucceeded" && e.actor == WorkflowActor::System);
    assert!(
        has_recovered,
        "expected a system activitySucceeded from recovery"
    );
    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn run_loop_no_infinite_loop_when_recovery_cannot_progress() {
    let run_dir = temp_run_dir("recovery-stuck");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-recovery-stuck";
    let _ = crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: r#"{"workflowId":"flow-stuck","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
            expected_workflow_id: Some("flow-stuck"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: None,
        },
    )
    .unwrap();

    // Activity id that the orchestrator expects for node "a" work.
    let work_activity_id = format!("{}::work::a", run_id);
    let work_attempt_id = format!("{}::work::a::att-1", run_id);

    // Write events that create a dangling effectAttempted activity
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "attemptCreated".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "nodeId": "a",
                    "activityId": &work_activity_id,
                    "attemptId": &work_attempt_id,
                    "attemptNumber": 1,
                    "inputRef": {
                        "outputHash": "sha256:aa",
                        "outputPath": "/tmp/aa",
                        "outputBytes": 2,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json"
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "effectAttempted".to_string(),
                actor: WorkflowActor::HostExecutor,
                payload: serde_json::json!({
                    "activityId": &work_activity_id,
                    "attemptId": &work_attempt_id,
                    "idempotencyKey": "wf_key_stuck",
                    "inputHash": "sha256:bb",
                    "idempotencyTtlMs": 60000u64,
                    "provider": "unknown-provider",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "flow-stuck".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([(
                "a".to_string(),
                WorkflowNode::HostExecutor(HostExecutorNode {
                    base: NodeBase {
                        description: None,
                        depends: None,
                        human_gate: None,
                        retry_policy: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        output_schema: None,
                        unsafe_allow_ungated: Some(true),
                    },
                    executor: "feishu-send".to_string(),
                    input: serde_json::json!({"chatId":"chat-1","content":"hello"}),
                }),
            )]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    // Use FakeHooks which returns had_progress=false for recovery.
    let mut hooks = FakeHooks;
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

    // The loop must NOT return MaxTicks (that would mean it consumed all
    // ticks in unsuccessful recovery attempts).  With no real progress,
    // it should return NoProgress or AwaitingWait.
    assert!(
        !matches!(result.reason, RunLoopStopReason::MaxTicks),
        "expected run_loop to stop without exhausting max_ticks on unrecoverable dangling effects"
    );
    assert!(
        matches!(
            result.reason,
            RunLoopStopReason::NoProgress | RunLoopStopReason::AwaitingWait
        ),
        "expected NoProgress or AwaitingWait, got {:?}",
        result.reason
    );
    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn default_recovery_result_has_correct_semantics() {
    // Default implementation: had_progress=false always.
    // has_remaining_dangling should match whether the snapshot actually
    // contains dangling effect_attempted activities.

    // Snapshot with no dangling effects → has_remaining_dangling=false
    let empty_snapshot = RunSnapshotDTO {
        run_id: "r".to_string(),
        run: RunState {
            run_id: "r".to_string(),
            status: RunStatus::Running,
            workflow_id: None,
            revision_id: None,
            initiator: None,
            input: None,
            output: None,
            failed_node_id: None,
            root_cause_event_id: None,
            cancel_origin_event_id: None,
            bot_snapshots: None,
            cancelled_run_intent: None,
            cancelled_node_intents: BTreeMap::new(),
        },
        last_seq: 0,
        nodes: vec![],
        activities: vec![],
        loops: None,
        dangling: DanglingSnapshot {
            activities: vec![],
            effect_attempted: vec![],
            waits: vec![],
            wait_resolutions: vec![],
            cancels: vec![],
        },
        outputs: BTreeMap::new(),
        attempt_io: BTreeMap::new(),
        chat_binding: None,
        updated_at: 0,
    };

    // Need a real EventLog for the &mut reference, but FakeHooks doesn't use it.
    let run_dir = temp_run_dir("recovery-semantics");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(&run_dir).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let mut log = EventLog::new("r", paths.workflow_runs_dir()).unwrap();

    // Use the trait method via a concrete type to test default impl.
    // We call it through a concrete struct that doesn't override the default.
    #[derive(Clone)]
    struct DefaultingHooks;
    #[async_trait]
    impl WorkflowExecutionHooks for DefaultingHooks {
        async fn execute_subagent(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &SubagentNode,
            _resolved_prompt: String,
        ) -> Result<WorkflowDispatchOutcome> {
            unreachable!()
        }
        async fn execute_host_executor(
            &mut self,
            _ctx: WorkflowDispatchRun<'_>,
            _node: &HostExecutorNode,
            _resolved_input: Value,
        ) -> Result<WorkflowDispatchOutcome> {
            unreachable!()
        }
        // Uses default recover_dangling_effects
    }

    let mut hooks = DefaultingHooks;

    // Empty: has_remaining_dangling should be false
    let result = hooks
        .recover_dangling_effects(&mut log, &empty_snapshot)
        .await
        .unwrap();
    assert!(!result.had_progress, "default: had_progress must be false");
    assert!(
        !result.has_remaining_dangling,
        "empty dangling → has_remaining_dangling must be false"
    );

    // With dangling effects: has_remaining_dangling should be true
    let dangling_snapshot = RunSnapshotDTO {
        dangling: DanglingSnapshot {
            effect_attempted: vec!["act-1".to_string()],
            ..empty_snapshot.dangling.clone()
        },
        ..empty_snapshot.clone()
    };
    let result2 = hooks
        .recover_dangling_effects(&mut log, &dangling_snapshot)
        .await
        .unwrap();
    assert!(!result2.had_progress, "default: had_progress must be false");
    assert!(
        result2.has_remaining_dangling,
        "dangling present → has_remaining_dangling must be true"
    );

    let _ = fs::remove_dir_all(&run_dir);
}
