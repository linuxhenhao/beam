// Phase 0: workflow current-behavior regression tests — loop/gate scenarios.
// These tests document current behavior. Do not change behavior here;
// only add tests that express what the runtime does today.

mod support;
use support::*;

use std::collections::BTreeMap;
use std::fs;

use beam_core::{
    BeamPaths, EventLog, ResolveWaitInput, RunChatBinding, RunLoopStopReason, RunStatus,
    WaitResolution, WorkflowNode, WorkflowRuntimeContext, bootstrap_workflow_run,
    read_run_snapshot, resolve_wait, run_loop, run_tick,
    workflow_definition::{HumanGate, NodeBase, SubagentNode, WorkflowDefinition},
};
use serde_json::Value;

// -- Task 0.1: humanGate approve then continue execution test --

#[tokio::test]
async fn human_gate_approve_resumes_execution() {
    let run_dir = temp_run_dir("gate-approve");
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(run_dir.join("blobs")).unwrap();
    let paths = BeamPaths::from_root(run_dir.clone());
    let run_id = "run-gate";

    let workflow_json = r#"{
        "workflowId":"gate-smoke",
        "version":1,
        "nodes":{
            "a":{"type":"subagent","bot":"bot-a","prompt":"step-a","humanGate":{"stage":"before","prompt":"approve a?"}},
            "b":{"type":"subagent","bot":"bot-b","prompt":"step-b","depends":["a"]}
        }
    }"#;
    bootstrap_workflow_run(
        &paths,
        beam_core::BootstrapWorkflowRunInput {
            run_id,
            workflow_json,
            expected_workflow_id: Some("gate-smoke"),
            params: &BTreeMap::new(),
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();

    let mut rt = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: WorkflowDefinition {
            workflow_id: "gate-smoke".to_string(),
            version: 1,
            params: None,
            defaults: None,
            nodes: BTreeMap::from([
                (
                    "a".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
                        base: NodeBase {
                            description: None,
                            depends: None,
                            human_gate: Some(HumanGate {
                                stage: "before".to_string(),
                                prompt: Value::String("approve a?".to_string()),
                                approvers: None,
                                deadline_ms: None,
                                on_timeout: None,
                            }),
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                        bot: "bot-a".to_string(),
                        prompt: Value::String("step-a".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
                (
                    "b".to_string(),
                    WorkflowNode::Subagent(SubagentNode {
                        base: NodeBase {
                            description: None,
                            depends: Some(vec!["a".to_string()]),
                            human_gate: None,
                            retry_policy: None,
                            timeout_ms: None,
                            max_output_bytes: None,
                            output_schema: None,
                            unsafe_allow_ungated: None,
                        },
                        bot: "bot-b".to_string(),
                        prompt: Value::String("step-b".to_string()),
                        working_dir: None,
                        model_overrides: None,
                        tool_policy: None,
                    }),
                ),
            ]),
        },
        runs_base_dir: paths.workflow_runs_dir(),
    };

    let mut hooks = FakeHooks;

    // First tick: should create the gate wait, not dispatch actual work
    let tick1 = run_tick(&mut rt, &mut hooks, 2).await.unwrap();
    assert!(tick1.actions > 0, "should have dispatched gate");
    // The snap should show the gate wait, not the subagent activity (yet)
    let snapshot1 = read_run_snapshot(&rt.log.run_dir).await.unwrap().unwrap();
    assert!(
        !snapshot1.dangling.waits.is_empty(),
        "should have a pending wait"
    );

    // Approve the wait
    let wait = &snapshot1.dangling.waits[0];
    // Find the wait's attempt id and resolve it
    let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
    let _ = resolve_wait(
        &mut log,
        ResolveWaitInput {
            activity_id: wait.clone(),
            attempt_id: format!("{wait}::att-1"),
            resolution: WaitResolution::Approved,
            by: "tester".to_string(),
            comment: None,
            output: None,
            is_decision_node: false,
        },
    )
    .await
    .unwrap();

    // Re-read the runtime context with fresh log
    let mut rt2 = WorkflowRuntimeContext {
        log: EventLog::new(run_id, paths.workflow_runs_dir()).unwrap(),
        def: rt.def.clone(),
        runs_base_dir: paths.workflow_runs_dir(),
    };

    // Next tick: should now dispatch node a's actual work
    let tick2 = run_tick(&mut rt2, &mut hooks, 2).await.unwrap();
    assert!(tick2.actions > 0, "should dispatch work after approve");

    // Run to completion
    let result = run_loop(&mut rt2, &mut hooks, 10, 2).await.unwrap();
    assert!(
        matches!(result.reason, RunLoopStopReason::Terminal),
        "expected Terminal, got {:?}",
        result.reason
    );
    assert_eq!(result.last_snapshot.run.status, RunStatus::Succeeded);
    let _ = fs::remove_dir_all(&run_dir);
}
