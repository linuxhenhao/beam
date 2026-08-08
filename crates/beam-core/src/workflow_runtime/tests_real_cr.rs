use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::workflow_definition::SubagentNode;
use crate::{EventDraft, EventLog, RunChatBinding, WorkflowActor};

use super::test_common::temp_run_dir;
use super::*;

/// fields so that $ref bindings (implement.output.code,
/// review.output.preview) work.
#[derive(Clone)]
struct RichFakeHooks;

#[async_trait]
impl WorkflowExecutionHooks for RichFakeHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        _resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: serde_json::json!({"code": "mock-code", "summary": "mock-summary", "preview": "looks good"}),
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
}

/// Hook that records resolved subagent prompts for verification.
#[derive(Clone)]
struct RecordingHooks {
    prompts: Arc<Mutex<Vec<(String, String)>>>, // (node_id, resolved_prompt)
}

#[async_trait]
impl WorkflowExecutionHooks for RecordingHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> Result<WorkflowDispatchOutcome> {
        self.prompts
            .lock()
            .await
            .push((_ctx.node_id.to_string(), resolved_prompt.clone()));
        // Produce a JSON object with common workflow output fields so
        // that $ref bindings (implement.output.code,
        // review.output.preview) resolve correctly.
        let out = serde_json::json!({"code": "mock-code", "summary": "mock-summary", "preview": "looks good"});
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: out,
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
}

#[tokio::test]
async fn real_code_review_loop_iter1_reaches_awaiting_wait() {
    // Load the real workflow JSON, bootstrap with task param,
    // and verify iteration 1 reaches AwaitingWait without errors
    // from ${reviewDecision.previous.comment}.
    let raw = include_str!("../../../../workflows/code-review-loop.workflow.json");
    let def = crate::parse_workflow_definition(raw).expect("parse real code-review-loop");
    let workflow_json = serde_json::to_string(&def).unwrap();

    let run_dir = temp_run_dir("real-crl-iter1");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-real-crl-1";
    let params: BTreeMap<String, Value> = BTreeMap::from([(
        String::from("task"),
        Value::String("add CLI echo command".to_string()),
    )]);
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let prompts = Arc::new(Mutex::new(Vec::new()));
    let mut hooks = RecordingHooks {
        prompts: prompts.clone(),
    };

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def,
        runs_base_dir: paths.workflow_runs_dir(),
    };
    let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

    assert_eq!(
        result.reason,
        RunLoopStopReason::AwaitingWait,
        "expected AwaitingWait for open human gate"
    );

    // Verify implement's resolved prompt contains the task and does NOT
    // error on ${reviewDecision.previous.comment} (iteration 1 → empty).
    let recorded = prompts.lock().await;
    let implement_prompt = recorded
        .iter()
        .find(|(node_id, _)| node_id == "implement")
        .map(|(_, p)| p.clone())
        .expect("implement must have been dispatched");
    eprintln!("implement prompt iter1: {implement_prompt}");
    assert!(
        implement_prompt.contains("add CLI echo command"),
        "implement prompt should contain the task param"
    );
    // In iteration 1, .previous.comment resolves to empty string.
    assert!(
        !implement_prompt.contains("ERROR") && !implement_prompt.contains("no previous iteration"),
        "implement prompt should not contain binding errors: {implement_prompt}"
    );

    // Verify loop-scoped activity IDs exist
    let events = rt.log.read_all().unwrap();
    let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);
    let has_gate_wait = events.iter().any(|e| {
        e.event_type == "waitCreated"
            && e.payload.get("activityId").and_then(Value::as_str) == Some(&decision_gate_id)
    });
    assert!(has_gate_wait, "expected gate wait for {}", decision_gate_id);

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn real_code_review_loop_reject_enters_iter2_with_comment() {
    // Iteration 1: drive to gate, reject with comment.
    // Iteration 2: verify implement's resolved prompt contains the
    // reject comment from the previous iteration.
    let raw = include_str!("../../../../workflows/code-review-loop.workflow.json");
    let def = crate::parse_workflow_definition(raw).expect("parse real code-review-loop");
    let workflow_json = serde_json::to_string(&def).unwrap();

    let run_dir = temp_run_dir("real-crl-reject");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-real-crl-rej";
    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("task"), Value::String("add test".to_string()))]);
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let prompts = Arc::new(Mutex::new(Vec::new()));

    // Iteration 1: drive to AwaitingWait.
    {
        let mut hooks = RecordingHooks {
            prompts: prompts.clone(),
        };
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }

    // Reject iteration 1 with a comment.
    let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &decision_gate_id,
                    "resolution": "rejected",
                    "by": "reviewer",
                    "comment": "needs more tests",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Process rejection → iteration 2 should start.
    {
        let mut hooks = RecordingHooks {
            prompts: prompts.clone(),
        };
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let _result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        // Verify loopIterationFinished metadata
        let events = rt.log.read_all().unwrap();
        let iter1_finished = events.iter().find(|e| {
            e.event_type == "loopIterationFinished"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(1)
        });
        assert!(
            iter1_finished.is_some(),
            "expected loopIterationFinished for iteration 1"
        );
        let payload = &iter1_finished.unwrap().payload;
        assert_eq!(
            payload.get("resolution").and_then(Value::as_str),
            Some("rejected"),
            "resolution should be rejected"
        );
        assert_eq!(
            payload.get("by").and_then(Value::as_str),
            Some("reviewer"),
            "by should be reviewer"
        );
        assert_eq!(
            payload.get("comment").and_then(Value::as_str),
            Some("needs more tests"),
            "comment should be preserved"
        );
        assert_eq!(
            payload.get("decisionActivityId").and_then(Value::as_str),
            Some(decision_gate_id.as_str()),
            "decisionActivityId should be the gate id"
        );

        let iter2_started = events.iter().any(|e| {
            e.event_type == "loopIterationStarted"
                && e.payload.get("iteration").and_then(Value::as_u64) == Some(2)
        });
        assert!(iter2_started, "expected iteration 2 started");
    }

    // Now verify iteration 2 implement prompt contains the reject comment.
    // The runtime already dispatched implement in iteration 2 during the
    // above run_loop call.
    let recorded = prompts.lock().await;
    let iter2_implement = recorded
        .iter()
        .rfind(|(node_id, _)| node_id == "implement")
        .map(|(_, p)| p.clone())
        .expect("implement iter2 must have been dispatched");
    eprintln!("implement prompt iter2: {iter2_implement}");
    assert!(
        iter2_implement.contains("needs more tests"),
        "iter2 implement prompt should contain reject comment 'needs more tests': {iter2_implement}"
    );

    let _ = fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn real_code_review_loop_approve_succeeds() {
    // Load real workflow, drive to gate, approve, verify loop/run succeeded.
    let raw = include_str!("../../../../workflows/code-review-loop.workflow.json");
    let def = crate::parse_workflow_definition(raw).expect("parse real code-review-loop");
    let workflow_json = serde_json::to_string(&def).unwrap();

    let run_dir = temp_run_dir("real-crl-approve");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = crate::BeamPaths::from_root(run_dir.clone());
    let run_id = "run-real-crl-app";
    let params: BTreeMap<String, Value> = BTreeMap::from([(
        String::from("task"),
        Value::String("add feature".to_string()),
    )]);
    crate::bootstrap_workflow_run(
        &paths,
        crate::BootstrapWorkflowRunInput {
            run_id,
            workflow_json: &workflow_json,
            expected_workflow_id: Some("code-review-loop"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    // Iteration 1: drive to AwaitingWait.
    {
        let mut hooks = RichFakeHooks;
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def: def.clone(),
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();
        assert_eq!(result.reason, RunLoopStopReason::AwaitingWait);
    }

    // Approve.
    let decision_gate_id = format!("{}::loop::review-loop.1::gate::reviewDecision", run_id);
    {
        let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": &decision_gate_id,
                    "resolution": "approved",
                    "by": "approver",
                    "comment": "lgtm",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    // Process approval → loop and run succeed.
    {
        let mut hooks = RichFakeHooks;
        let mut rt = WorkflowRuntimeContext {
            log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
            def,
            runs_base_dir: paths.workflow_runs_dir(),
        };
        let result = run_loop(&mut rt, &mut hooks, 10, 1).await.unwrap();

        let events = rt.log.read_all().unwrap();

        let loop_finished = events.iter().find(|e| {
            e.event_type == "loopFinished"
                && e.payload.get("loopId").and_then(Value::as_str) == Some("review-loop")
        });
        assert!(loop_finished.is_some(), "expected loopFinished");
        let payload = &loop_finished.unwrap().payload;
        assert_eq!(
            payload.get("resolution").and_then(Value::as_str),
            Some("approved")
        );

        let run_succeeded = events.iter().any(|e| e.event_type == "runSucceeded");
        assert!(run_succeeded, "expected run to succeed after loop approval");

        assert!(matches!(result.reason, RunLoopStopReason::Terminal));

        // Verify loop output exists.
        let snap = read_snapshot(&rt).await.unwrap();
        let loop_output_key = format!("{}::work::review-loop", run_id);
        assert!(
            snap.outputs.contains_key(&loop_output_key),
            "expected loop output"
        );
    }

    let _ = fs::remove_dir_all(&run_dir);
}
