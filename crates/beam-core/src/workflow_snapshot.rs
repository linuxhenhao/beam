use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::workflow_projection::{event_seq_from_id, read_run_events_pure};
use crate::{RunChatBinding, WorkflowEventEnvelope, WorkflowOutputRef};

const BLOB_PREVIEW_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobPreviewDTO {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AttemptTerminalDTO {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_session_id: Option<String>,
    pub web_port: u16,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lark_app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
    pub started_at: u64,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_pty_log: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AttemptIODTO {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<BlobPreviewDTO>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_input: Option<BlobPreviewDTO>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<BlobPreviewDTO>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<BlobPreviewDTO>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<AttemptTerminalDTO>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_prompt: Option<BlobPreviewDTO>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RunStatus {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NodeStatus {
    Idle,
    Triggered,
    Running,
    Waiting,
    Retrying,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ActivityStatus {
    Pending,
    Acquired,
    Running,
    Waiting,
    EffectAttempting,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LoopIterationStatus {
    Running,
    Approved,
    Rejected,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LoopStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EffectAttemptedState {
    pub idempotency_key: String,
    pub input_hash: String,
    pub idempotency_ttl_ms: u64,
    pub provider: String,
    pub attempted_at_event_id: String,
    pub attempted_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileResultState {
    pub decision: String,
    pub capability: String,
    pub evidence: Value,
    pub event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WaitResolutionState {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exceeded_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WaitState {
    pub wait_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_ref: Option<WorkflowOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<WaitResolutionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CancelRequestState {
    pub cancel_origin_event_id: String,
    pub requested_by: String,
    pub reason: String,
    pub delivered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AttemptState {
    pub attempt_id: String,
    pub attempt_number: u64,
    pub input_ref: WorkflowOutputRef,
    pub status: ActivityStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_attempted: Option<EffectAttemptedState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_reconcile_result: Option<ReconcileResultState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_request: Option<CancelRequestState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<WaitState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<WorkflowOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_refs: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_origin_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActivityState {
    pub activity_id: String,
    pub attempts: Vec<AttemptState>,
    pub status: ActivityStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NodeState {
    pub node_id: String,
    pub status: NodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_id: Option<String>,
    pub retry_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_origin_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LoopIterationState {
    pub iteration: u64,
    pub status: LoopIterationStatus,
    pub body_activity_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_activity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_resolved_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timed_out: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LoopState {
    pub loop_id: String,
    pub status: LoopStatus,
    pub iteration: u64,
    pub max_iterations: u64,
    pub iterations: Vec<LoopIterationState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<WorkflowOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunState {
    pub run_id: String,
    pub status: RunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<WorkflowOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<WorkflowOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_cause_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_origin_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot_snapshots: Option<BTreeMap<String, BotSnapshot>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_run_intent: Option<CancelIntent>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cancelled_node_intents: BTreeMap<String, CancelIntent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CancelIntent {
    pub cancel_origin_event_id: String,
    pub requested_by: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BotSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lark_app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LoopSnapshotDTO {
    pub loop_id: String,
    pub status: LoopStatus,
    pub iteration: u64,
    pub max_iterations: u64,
    pub iterations: Vec<LoopIterationState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<WorkflowOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunSnapshotDTO {
    pub run_id: String,
    pub run: RunState,
    pub last_seq: u64,
    pub nodes: Vec<NodeState>,
    pub activities: Vec<ActivityState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loops: Option<BTreeMap<String, LoopSnapshotDTO>>,
    pub dangling: DanglingSnapshot,
    pub outputs: BTreeMap<String, WorkflowOutputRef>,
    pub attempt_io: BTreeMap<String, AttemptIODTO>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_binding: Option<RunChatBinding>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DanglingSnapshot {
    pub activities: Vec<String>,
    pub effect_attempted: Vec<String>,
    pub waits: Vec<String>,
    pub cancels: Vec<String>,
}

pub async fn read_run_snapshot(run_dir: &Path) -> Result<Option<RunSnapshotDTO>> {
    let events = match read_run_events_pure(run_dir)? {
        Some(events) if !events.is_empty() => events,
        _ => return Ok(None),
    };
    let snap = replay_events(&events)?;
    let binding = read_chat_binding_pure(run_dir).await?;
    let def = read_workflow_definition_pure(run_dir).await?;
    let attempt_io = build_attempt_io(run_dir, &snap, def.as_ref())?;
    Ok(Some(RunSnapshotDTO {
        run_id: snap.run.run_id.clone(),
        run: snap.run,
        last_seq: snap.last_seq,
        nodes: snap.nodes.into_values().collect(),
        activities: snap.activities.into_values().collect(),
        loops: if snap.loops.is_empty() {
            None
        } else {
            Some(
                snap.loops
                    .into_iter()
                    .map(|(key, value)| (key, value))
                    .collect(),
            )
        },
        dangling: DanglingSnapshot {
            activities: snap.dangling_activities,
            effect_attempted: snap.dangling_effect_attempted,
            waits: snap.dangling_waits,
            cancels: snap.dangling_cancels,
        },
        outputs: snap.outputs,
        attempt_io,
        chat_binding: binding,
        updated_at: events.last().map(|ev| ev.timestamp).unwrap_or_default(),
    }))
}

#[derive(Debug, Clone)]
struct ReplaySnapshot {
    run: RunState,
    nodes: BTreeMap<String, NodeState>,
    activities: BTreeMap<String, ActivityState>,
    loops: BTreeMap<String, LoopSnapshotDTO>,
    outputs: BTreeMap<String, WorkflowOutputRef>,
    last_seq: u64,
    dangling_activities: Vec<String>,
    dangling_effect_attempted: Vec<String>,
    dangling_waits: Vec<String>,
    dangling_cancels: Vec<String>,
}

fn replay_events(events: &[WorkflowEventEnvelope]) -> Result<ReplaySnapshot> {
    let first = events.first().context("replay: empty event log")?;
    if first.event_type != "runCreated" {
        anyhow::bail!(
            "replay: first event must be runCreated, got {}",
            first.event_type
        );
    }
    let mut snap = ReplaySnapshot {
        run: RunState {
            run_id: first.run_id.clone(),
            status: RunStatus::Pending,
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
        nodes: BTreeMap::new(),
        activities: BTreeMap::new(),
        loops: BTreeMap::new(),
        outputs: BTreeMap::new(),
        last_seq: 0,
        dangling_activities: Vec::new(),
        dangling_effect_attempted: Vec::new(),
        dangling_waits: Vec::new(),
        dangling_cancels: Vec::new(),
    };
    let mut waits_open = BTreeSet::new();
    let mut run_cancel_intent: Option<(String, String, String)> = None;
    let mut node_cancel_intents: BTreeMap<String, (String, String, String)> = BTreeMap::new();

    for ev in events {
        if ev.run_id != snap.run.run_id {
            anyhow::bail!(
                "replay: runId mismatch at {} — log is {}, event has {}",
                ev.event_id,
                snap.run.run_id,
                ev.run_id
            );
        }
        snap.last_seq = snap.last_seq.max(event_seq_from_id(&ev.event_id));
        if is_payload_ref(&ev.payload) {
            continue;
        }
        let payload = &ev.payload;
        match ev.event_type.as_str() {
            "runCreated" => apply_run_created(&mut snap.run, payload),
            "runStarted" => snap.run.status = RunStatus::Running,
            "runSucceeded" => apply_run_succeeded(&mut snap.run, payload),
            "runFailed" => apply_run_failed(&mut snap.run, payload),
            "runCanceled" => apply_run_canceled(&mut snap.run, payload),
            "nodeWaiting" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    get_node(&mut snap.nodes, &node_id).status = NodeStatus::Waiting;
                }
            }
            "nodeRetrying" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.status = NodeStatus::Retrying;
                    node.retry_count = node.retry_count.saturating_add(1);
                }
            }
            "nodeSucceeded" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.status = NodeStatus::Succeeded;
                    node.activity_id = payload_str(payload, "lastActivityId");
                }
            }
            "nodeFailed" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.status = NodeStatus::Failed;
                    node.activity_id = payload_str(payload, "lastActivityId");
                    node.error_class = payload_str(payload, "errorClass");
                }
            }
            "nodeSkipped" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.status = NodeStatus::Skipped;
                    node.condition_event_id = Some(ev.event_id.clone());
                }
            }
            "nodeCanceled" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.status = NodeStatus::Cancelled;
                    node.cancel_origin_event_id = payload_str(payload, "cancelOriginEventId");
                }
            }
            "loopStarted" => {
                if let Some(loop_id) = payload_str(payload, "loopId") {
                    let loop_state = get_loop(&mut snap.loops, &loop_id);
                    loop_state.status = LoopStatus::Running;
                    loop_state.max_iterations = payload_u64(payload, "maxIterations").unwrap_or(0);
                }
            }
            "loopIterationStarted" => {
                if let Some(loop_id) = payload_str(payload, "loopId") {
                    let iteration = payload_u64(payload, "iteration").unwrap_or(0);
                    let loop_state = get_loop(&mut snap.loops, &loop_id);
                    loop_state.status = LoopStatus::Running;
                    loop_state.iteration = iteration;
                    let it = get_loop_iteration(loop_state, iteration);
                    it.status = LoopIterationStatus::Running;
                }
            }
            "loopIterationFinished" => {
                if let Some(loop_id) = payload_str(payload, "loopId") {
                    let iteration = payload_u64(payload, "iteration").unwrap_or(0);
                    let loop_state = get_loop(&mut snap.loops, &loop_id);
                    loop_state.iteration = loop_state.iteration.max(iteration);
                    let it = get_loop_iteration(loop_state, iteration);
                    it.status = match payload_str(payload, "resolution")
                        .as_deref()
                        .unwrap_or("failed")
                    {
                        "approved" => LoopIterationStatus::Approved,
                        "rejected" => LoopIterationStatus::Rejected,
                        "cancelled" => LoopIterationStatus::Cancelled,
                        _ => LoopIterationStatus::Failed,
                    };
                    it.decision_activity_id = payload_str(payload, "decisionActivityId");
                    it.wait_resolved_event_id = payload_str(payload, "waitResolvedEventId");
                    it.decision_by = payload_str(payload, "by");
                    it.decision_comment = payload_str(payload, "comment");
                    it.timed_out = payload_bool(payload, "timedOut");
                }
            }
            "loopFinished" => {
                if let Some(loop_id) = payload_str(payload, "loopId") {
                    let loop_state = get_loop(&mut snap.loops, &loop_id);
                    loop_state.iteration = payload_u64(payload, "finalIteration").unwrap_or(0);
                    loop_state.status = match payload_str(payload, "resolution")
                        .as_deref()
                        .unwrap_or("failed")
                    {
                        "approved" => LoopStatus::Succeeded,
                        "cancelled" => LoopStatus::Cancelled,
                        _ => LoopStatus::Failed,
                    };
                    loop_state.output = payload_workflow_output_ref(payload, "outputRef");
                    loop_state.error_code = payload_str(payload, "errorCode");
                    loop_state.error_class = payload_str(payload, "errorClass");
                    if let Some(output_ref) = loop_state.output.clone() {
                        snap.outputs
                            .insert(format!("{}::work::{}", ev.run_id, loop_id), output_ref);
                    }
                    if loop_state.status != LoopStatus::Succeeded {
                        if let Some(inflight) = loop_state
                            .iterations
                            .iter_mut()
                            .find(|it| matches!(it.status, LoopIterationStatus::Running))
                        {
                            inflight.status = if loop_state.status == LoopStatus::Cancelled {
                                LoopIterationStatus::Cancelled
                            } else {
                                LoopIterationStatus::Failed
                            };
                        }
                    }
                }
            }
            "conditionEvaluated" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.condition_event_id = Some(ev.event_id.clone());
                }
            }
            "attemptCreated" => {
                if let (Some(activity_id), Some(node_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "nodeId"),
                    payload_str(payload, "attemptId"),
                ) {
                    let attempt_number = payload_u64(payload, "attemptNumber").unwrap_or(0);
                    let input_ref = match payload_workflow_output_ref(payload, "inputRef") {
                        Some(value) => value,
                        None => continue,
                    };
                    let activity = get_activity(&mut snap.activities, &activity_id);
                    activity.attempts.push(AttemptState {
                        attempt_id: attempt_id.clone(),
                        attempt_number,
                        input_ref,
                        status: ActivityStatus::Pending,
                        lease_id: None,
                        timeout_ms: None,
                        max_output_bytes: None,
                        effect_attempted: None,
                        latest_reconcile_result: None,
                        cancel_request: None,
                        wait: None,
                        output: None,
                        external_refs: None,
                        error: None,
                        running_ms: None,
                        cancel_origin_event_id: None,
                    });
                    activity.current_attempt_id = Some(attempt_id.clone());
                    activity.status = ActivityStatus::Pending;
                    activity.owner_node_id = Some(node_id.clone());
                    let node = get_node(&mut snap.nodes, &node_id);
                    node.activity_id = Some(activity_id.clone());
                    if attempt_number == 1 && matches!(node.status, NodeStatus::Idle) {
                        node.status = NodeStatus::Triggered;
                    }
                    if let Some((loop_id, iteration)) = parse_loop_activity_id(&activity_id) {
                        if let Some(loop_state) = snap.loops.get_mut(&loop_id) {
                            let it = get_loop_iteration(loop_state, iteration);
                            if !it.body_activity_ids.contains(&activity_id) {
                                it.body_activity_ids.push(activity_id.clone());
                            }
                        }
                    }
                }
            }
            "leaseSigned" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.lease_id = payload_str(payload, "leaseId");
                        attempt.timeout_ms = payload_u64(payload, "timeoutMs");
                        attempt.max_output_bytes = payload_u64(payload, "maxOutputBytes");
                    }
                }
            }
            "backoffScheduled" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    get_node(&mut snap.nodes, &node_id).next_attempt_at =
                        payload_u64(payload, "nextAttemptAt");
                }
            }
            "backoffElapsed" => {
                if let Some(node_id) = payload_str(payload, "nodeId") {
                    get_node(&mut snap.nodes, &node_id).next_attempt_at = None;
                }
            }
            "effectAttempted" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.effect_attempted = Some(EffectAttemptedState {
                            idempotency_key: payload_str(payload, "idempotencyKey")
                                .unwrap_or_default(),
                            input_hash: payload_str(payload, "inputHash").unwrap_or_default(),
                            idempotency_ttl_ms: payload_u64(payload, "idempotencyTtlMs")
                                .unwrap_or_default(),
                            provider: payload_str(payload, "provider").unwrap_or_default(),
                            attempted_at_event_id: ev.event_id.clone(),
                            attempted_at_ms: ev.timestamp,
                        });
                        attempt.status = ActivityStatus::EffectAttempting;
                        if let Some(activity) = snap.activities.get_mut(&activity_id) {
                            activity.status = ActivityStatus::EffectAttempting;
                        }
                    }
                }
            }
            "activitySucceeded" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    let output_ref = if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.status = ActivityStatus::Succeeded;
                        attempt.output = payload_workflow_output_ref(payload, "outputRef");
                        attempt.external_refs = payload.get("externalRefs").cloned();
                        attempt.output.clone()
                    } else {
                        None
                    };
                    if let Some(activity) = snap.activities.get_mut(&activity_id) {
                        activity.status = ActivityStatus::Succeeded;
                    }
                    if let Some(output_ref) = output_ref {
                        snap.outputs.insert(activity_id.clone(), output_ref);
                    }
                    waits_open.remove(&activity_id);
                }
            }
            "activityFailed" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.status = ActivityStatus::Failed;
                        attempt.error = payload.get("error").cloned();
                        if let Some(activity) = snap.activities.get_mut(&activity_id) {
                            activity.status = ActivityStatus::Failed;
                        }
                        waits_open.remove(&activity_id);
                    }
                }
            }
            "activityTimedOut" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.status = ActivityStatus::TimedOut;
                        attempt.running_ms = payload_u64(payload, "runningMs");
                        if let Some(activity) = snap.activities.get_mut(&activity_id) {
                            activity.status = ActivityStatus::TimedOut;
                        }
                        waits_open.remove(&activity_id);
                    }
                }
            }
            "activityRunning" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.status = ActivityStatus::Running;
                    }
                    let owner_node_id = snap
                        .activities
                        .get(&activity_id)
                        .and_then(|activity| activity.owner_node_id.clone());
                    if let Some(activity) = snap.activities.get_mut(&activity_id) {
                        activity.status = ActivityStatus::Running;
                    }
                    if let Some(owner) = owner_node_id {
                        let node = get_node(&mut snap.nodes, &owner);
                        if matches!(node.status, NodeStatus::Triggered | NodeStatus::Retrying) {
                            node.status = NodeStatus::Running;
                        }
                    }
                }
            }
            "activityWaiting" => {
                if let Some(activity_id) = payload_str(payload, "activityId") {
                    let attempt_id = snap
                        .activities
                        .get(&activity_id)
                        .and_then(|activity| activity.current_attempt_id.clone());
                    if let Some(attempt_id) = attempt_id {
                        if let Some(attempt) =
                            get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                        {
                            attempt.status = ActivityStatus::Waiting;
                        }
                        if let Some(activity) = snap.activities.get_mut(&activity_id) {
                            activity.status = ActivityStatus::Waiting;
                        }
                    }
                }
            }
            "activityCanceled" => {
                if let (Some(activity_id), Some(attempt_id)) = (
                    payload_str(payload, "activityId"),
                    payload_str(payload, "attemptId"),
                ) {
                    if let Some(attempt) =
                        get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                    {
                        attempt.status = ActivityStatus::Cancelled;
                        attempt.cancel_origin_event_id =
                            payload_str(payload, "cancelOriginEventId");
                        if let Some(activity) = snap.activities.get_mut(&activity_id) {
                            activity.status = ActivityStatus::Cancelled;
                        }
                        waits_open.remove(&activity_id);
                    }
                }
            }
            "waitCreated" => {
                if let Some(activity_id) = payload_str(payload, "activityId") {
                    waits_open.insert(activity_id.clone());
                    let attempt_id = snap
                        .activities
                        .get(&activity_id)
                        .and_then(|activity| activity.current_attempt_id.clone());
                    if let Some(attempt_id) = attempt_id {
                        if let Some(attempt) =
                            get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                        {
                            attempt.wait = Some(WaitState {
                                wait_kind: payload_str(payload, "waitKind").unwrap_or_default(),
                                deadline_at: payload_u64(payload, "deadlineAt"),
                                prompt: payload_str(payload, "prompt"),
                                prompt_ref: payload_workflow_output_ref(payload, "promptRef"),
                                prompt_preview: payload_str(payload, "promptPreview"),
                                approvers: payload_string_array(payload, "approvers"),
                                on_timeout: payload_str(payload, "onTimeout"),
                                resolution: None,
                            });
                        }
                    }
                }
            }
            "waitResolved" => {
                if let Some(activity_id) = payload_str(payload, "activityId") {
                    waits_open.remove(&activity_id);
                    let attempt_id = snap
                        .activities
                        .get(&activity_id)
                        .and_then(|activity| activity.current_attempt_id.clone());
                    if let Some(attempt_id) = attempt_id {
                        if let Some(attempt) =
                            get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                        {
                            if let Some(wait) = attempt.wait.as_mut() {
                                wait.resolution = Some(WaitResolutionState {
                                    kind: "resolved".to_string(),
                                    resolution: payload_str(payload, "resolution"),
                                    by: payload_str(payload, "by"),
                                    comment: payload_str(payload, "comment"),
                                    event_id: Some(ev.event_id.clone()),
                                    deadline_at: None,
                                    exceeded_at_ms: None,
                                });
                            }
                        }
                    }
                }
            }
            "waitDeadlineExceeded" => {
                if let Some(activity_id) = payload_str(payload, "activityId") {
                    waits_open.remove(&activity_id);
                    let attempt_id = snap
                        .activities
                        .get(&activity_id)
                        .and_then(|activity| activity.current_attempt_id.clone());
                    if let Some(attempt_id) = attempt_id {
                        if let Some(attempt) =
                            get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                        {
                            if let Some(wait) = attempt.wait.as_mut() {
                                wait.resolution = Some(WaitResolutionState {
                                    kind: "deadlineExceeded".to_string(),
                                    resolution: None,
                                    by: None,
                                    comment: None,
                                    event_id: Some(ev.event_id.clone()),
                                    deadline_at: payload_u64(payload, "deadlineAt"),
                                    exceeded_at_ms: payload_u64(payload, "exceededAtMs"),
                                });
                            }
                        }
                    }
                }
            }
            "cancelRequested" => {
                if let Some(target) = payload.get("target") {
                    if let Some(kind) = target.get("kind").and_then(Value::as_str) {
                        match kind {
                            "activity" => {
                                if let Some(activity_id) =
                                    target.get("activityId").and_then(Value::as_str)
                                {
                                    mark_activity_cancel(
                                        &mut snap.activities,
                                        activity_id,
                                        &ev,
                                        payload,
                                    );
                                }
                            }
                            "node" => {
                                if let Some(node_id) = target.get("nodeId").and_then(Value::as_str)
                                {
                                    let node_id = node_id.to_string();
                                    node_cancel_intents.entry(node_id.clone()).or_insert_with(
                                        || {
                                            (
                                                ev.event_id.clone(),
                                                payload_str(payload, "by").unwrap_or_default(),
                                                payload_str(payload, "reason").unwrap_or_default(),
                                            )
                                        },
                                    );
                                    for activity in snap.activities.values_mut() {
                                        if activity.owner_node_id.as_deref()
                                            == Some(node_id.as_str())
                                        {
                                            mark_attempt_cancel(activity, &ev, payload);
                                        }
                                    }
                                }
                            }
                            "run" => {
                                if run_cancel_intent.is_none() {
                                    run_cancel_intent = Some((
                                        ev.event_id.clone(),
                                        payload_str(payload, "by").unwrap_or_default(),
                                        payload_str(payload, "reason").unwrap_or_default(),
                                    ));
                                }
                                for activity in snap.activities.values_mut() {
                                    mark_attempt_cancel(activity, &ev, payload);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            "cancelDelivered" => {
                if let Some(activity_id) = payload_str(payload, "activityId") {
                    let attempt_id = snap
                        .activities
                        .get(&activity_id)
                        .and_then(|activity| activity.current_attempt_id.clone());
                    if let Some(attempt_id) = attempt_id {
                        if let Some(attempt) =
                            get_attempt_mut(&mut snap.activities, &activity_id, &attempt_id)
                        {
                            if let Some(cancel) = attempt.cancel_request.as_mut() {
                                cancel.delivered = true;
                            }
                        }
                    }
                }
            }
            "workerLost" | "resumeStarted" => {}
            "reconcileResult" => {
                if let Some(idempotency_key) = payload_str(payload, "idempotencyKey") {
                    for activity in snap.activities.values_mut() {
                        if let Some(attempt) = activity.attempts.iter_mut().find(|candidate| {
                            candidate
                                .effect_attempted
                                .as_ref()
                                .map(|x| x.idempotency_key.as_str())
                                == Some(idempotency_key.as_str())
                        }) {
                            attempt.latest_reconcile_result = Some(ReconcileResultState {
                                decision: payload_str(payload, "decision").unwrap_or_default(),
                                capability: payload_str(payload, "capability").unwrap_or_default(),
                                evidence: payload.get("evidence").cloned().unwrap_or(Value::Null),
                                event_id: ev.event_id.clone(),
                            });
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let mut dangling_activities = Vec::new();
    let mut dangling_effect_attempted = Vec::new();
    let mut dangling_wait_resolutions = Vec::new();
    let mut dangling_cancels = Vec::new();
    for activity in snap.activities.values() {
        let Some(latest) = activity.attempts.last() else {
            continue;
        };
        let is_terminal = matches!(
            latest.status,
            ActivityStatus::Succeeded
                | ActivityStatus::Failed
                | ActivityStatus::TimedOut
                | ActivityStatus::Cancelled
        );
        if !is_terminal {
            dangling_activities.push(activity.activity_id.clone());
            if latest.effect_attempted.is_some() {
                dangling_effect_attempted.push(activity.activity_id.clone());
            }
            if latest
                .wait
                .as_ref()
                .and_then(|w| w.resolution.as_ref())
                .is_some()
            {
                dangling_wait_resolutions.push(activity.activity_id.clone());
            }
            if latest.cancel_request.is_some() {
                dangling_cancels.push(activity.activity_id.clone());
            }
        }
    }
    dangling_activities.sort();
    dangling_effect_attempted.sort();
    dangling_wait_resolutions.sort();
    dangling_cancels.sort();

    if !matches!(snap.run.status, RunStatus::Cancelled) {
        if let Some((cancel_origin_event_id, requested_by, reason)) = run_cancel_intent {
            snap.run.cancelled_run_intent = Some(CancelIntent {
                cancel_origin_event_id,
                requested_by,
                reason,
            });
        }
    }
    for (node_id, intent) in node_cancel_intents {
        if matches!(
            snap.nodes.get(&node_id).map(|node| node.status),
            Some(NodeStatus::Cancelled)
        ) {
            continue;
        }
        snap.run.cancelled_node_intents.insert(
            node_id,
            CancelIntent {
                cancel_origin_event_id: intent.0,
                requested_by: intent.1,
                reason: intent.2,
            },
        );
    }

    Ok(ReplaySnapshot {
        run: snap.run,
        nodes: snap.nodes,
        activities: snap.activities,
        loops: snap.loops,
        outputs: snap.outputs,
        last_seq: snap.last_seq,
        dangling_activities,
        dangling_effect_attempted,
        dangling_waits: waits_open.into_iter().collect(),
        dangling_cancels,
    })
}

fn get_node<'a>(nodes: &'a mut BTreeMap<String, NodeState>, node_id: &str) -> &'a mut NodeState {
    nodes
        .entry(node_id.to_string())
        .or_insert_with(|| NodeState {
            node_id: node_id.to_string(),
            status: NodeStatus::Idle,
            activity_id: None,
            retry_count: 0,
            next_attempt_at: None,
            error_class: None,
            condition_event_id: None,
            cancel_origin_event_id: None,
        })
}

fn get_activity<'a>(
    activities: &'a mut BTreeMap<String, ActivityState>,
    activity_id: &str,
) -> &'a mut ActivityState {
    activities
        .entry(activity_id.to_string())
        .or_insert_with(|| ActivityState {
            activity_id: activity_id.to_string(),
            attempts: Vec::new(),
            status: ActivityStatus::Pending,
            current_attempt_id: None,
            owner_node_id: None,
        })
}

fn get_attempt_mut<'a>(
    activities: &'a mut BTreeMap<String, ActivityState>,
    activity_id: &str,
    attempt_id: &str,
) -> Option<&'a mut AttemptState> {
    activities
        .get_mut(activity_id)?
        .attempts
        .iter_mut()
        .find(|attempt| attempt.attempt_id == attempt_id)
}

fn get_loop<'a>(
    loops: &'a mut BTreeMap<String, LoopSnapshotDTO>,
    loop_id: &str,
) -> &'a mut LoopSnapshotDTO {
    loops
        .entry(loop_id.to_string())
        .or_insert_with(|| LoopSnapshotDTO {
            loop_id: loop_id.to_string(),
            status: LoopStatus::Running,
            iteration: 0,
            max_iterations: 0,
            iterations: Vec::new(),
            output: None,
            error_code: None,
            error_class: None,
        })
}

fn get_loop_iteration<'a>(
    loop_state: &'a mut LoopSnapshotDTO,
    iteration: u64,
) -> &'a mut LoopIterationState {
    if let Some(idx) = loop_state
        .iterations
        .iter()
        .position(|candidate| candidate.iteration == iteration)
    {
        return &mut loop_state.iterations[idx];
    }
    loop_state.iterations.push(LoopIterationState {
        iteration,
        status: LoopIterationStatus::Running,
        body_activity_ids: Vec::new(),
        decision_activity_id: None,
        wait_resolved_event_id: None,
        decision_by: None,
        decision_comment: None,
        timed_out: None,
    });
    let idx = loop_state.iterations.len() - 1;
    &mut loop_state.iterations[idx]
}

fn apply_run_created(run: &mut RunState, payload: &Value) {
    run.workflow_id = payload_str(payload, "workflowId");
    run.revision_id = payload_str(payload, "revisionId");
    run.initiator = payload_str(payload, "initiator");
    run.input = payload_workflow_output_ref(payload, "inputRef");
    if let Some(bot_snapshots) = payload.get("botSnapshots").and_then(|value| {
        serde_json::from_value::<BTreeMap<String, BotSnapshot>>(value.clone()).ok()
    }) {
        run.bot_snapshots = Some(bot_snapshots);
    }
}

fn apply_run_succeeded(run: &mut RunState, payload: &Value) {
    run.status = RunStatus::Succeeded;
    run.output = payload_workflow_output_ref(payload, "outputRef");
}

fn apply_run_failed(run: &mut RunState, payload: &Value) {
    run.status = RunStatus::Failed;
    run.failed_node_id = payload_str(payload, "failedNodeId");
    run.root_cause_event_id = payload_str(payload, "rootCauseEventId");
}

fn apply_run_canceled(run: &mut RunState, payload: &Value) {
    run.status = RunStatus::Cancelled;
    run.cancel_origin_event_id = payload_str(payload, "cancelOriginEventId");
}

fn mark_attempt_cancel(activity: &mut ActivityState, ev: &WorkflowEventEnvelope, payload: &Value) {
    let Some(attempt_id) = activity.current_attempt_id.clone() else {
        return;
    };
    let Some(attempt) = activity
        .attempts
        .iter_mut()
        .find(|attempt| attempt.attempt_id == attempt_id)
    else {
        return;
    };
    let is_terminal = matches!(
        attempt.status,
        ActivityStatus::Succeeded
            | ActivityStatus::Failed
            | ActivityStatus::TimedOut
            | ActivityStatus::Cancelled
    );
    if is_terminal || attempt.cancel_request.is_some() {
        return;
    }
    attempt.cancel_request = Some(CancelRequestState {
        cancel_origin_event_id: ev.event_id.clone(),
        requested_by: payload_str(payload, "by").unwrap_or_default(),
        reason: payload_str(payload, "reason").unwrap_or_default(),
        delivered: false,
    });
}

fn mark_activity_cancel(
    activities: &mut BTreeMap<String, ActivityState>,
    activity_id: &str,
    ev: &WorkflowEventEnvelope,
    payload: &Value,
) {
    if let Some(activity) = activities.get_mut(activity_id) {
        mark_attempt_cancel(activity, ev, payload);
    }
}

fn payload_str(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn payload_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(Value::as_u64)
}

fn payload_bool(payload: &Value, key: &str) -> Option<bool> {
    payload.get(key).and_then(Value::as_bool)
}

fn payload_string_array(payload: &Value, key: &str) -> Option<Vec<String>> {
    let arr = payload.get(key)?.as_array()?;
    Some(
        arr.iter()
            .filter_map(|item| item.as_str().map(ToOwned::to_owned))
            .collect(),
    )
}

fn payload_workflow_output_ref(payload: &Value, key: &str) -> Option<WorkflowOutputRef> {
    payload
        .get(key)
        .cloned()
        .and_then(|value| serde_json::from_value::<WorkflowOutputRef>(value).ok())
}

fn is_payload_ref(payload: &Value) -> bool {
    let Some(obj) = payload.as_object() else {
        return false;
    };
    obj.get("ref").and_then(Value::as_str).is_some()
}

async fn read_chat_binding_pure(run_dir: &Path) -> Result<Option<RunChatBinding>> {
    let path = run_dir.join("chat-binding.json");
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let binding = serde_json::from_str::<RunChatBinding>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(binding))
}

async fn read_workflow_definition_pure(run_dir: &Path) -> Result<Option<Value>> {
    let path = run_dir.join("workflow.json");
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let def = serde_json::from_str::<Value>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(def))
}

fn build_attempt_io(
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: Option<&Value>,
) -> Result<BTreeMap<String, AttemptIODTO>> {
    let mut out = BTreeMap::new();
    let mut cache = HashMap::new();
    for activity in snap.activities.values() {
        for attempt in &activity.attempts {
            let mut io = AttemptIODTO {
                input: Some(preview_ref(run_dir, &attempt.input_ref, &mut cache)?),
                resolved_input: None,
                output: None,
                log: preview_attempt_log(run_dir, &activity.activity_id, &attempt.attempt_id)?,
                terminal: read_attempt_terminal(
                    run_dir,
                    &activity.activity_id,
                    &attempt.attempt_id,
                )?,
                wait_prompt: None,
            };
            if let Some(output_ref) = attempt.output.as_ref() {
                io.output = Some(preview_ref(run_dir, output_ref, &mut cache)?);
            }
            if let Some(wait) = attempt.wait.as_ref() {
                if let Some(prompt_ref) = wait.prompt_ref.as_ref() {
                    io.wait_prompt = Some(preview_ref(run_dir, prompt_ref, &mut cache)?);
                }
            }
            if let Some(input_value) = io.input.as_ref().and_then(|preview| preview.value.clone()) {
                if let Some(def) = def {
                    io.resolved_input = Some(preview_resolved_input(
                        run_dir,
                        snap,
                        def,
                        input_value,
                        &mut cache,
                    )?);
                }
            }
            out.insert(attempt.attempt_id.clone(), io);
        }
    }
    Ok(out)
}

fn preview_ref(
    run_dir: &Path,
    ref_value: &WorkflowOutputRef,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<BlobPreviewDTO> {
    if let Some(cached) = cache.get(&ref_value.output_hash) {
        return Ok(cached.clone());
    }
    let base = BlobPreviewDTO {
        output_hash: Some(ref_value.output_hash.clone()),
        output_bytes: Some(ref_value.output_bytes),
        content_type: ref_value.content_type.clone(),
        truncated: None,
        value: None,
        text: None,
        error: None,
        redacted: None,
    };
    let Some(output_path) = Some(&ref_value.output_path) else {
        return Ok(base);
    };
    if !is_path_inside(run_dir, Path::new(output_path)) {
        let mut preview = base.clone();
        preview.error = Some("outputPath is outside run directory".to_string());
        cache.insert(ref_value.output_hash.clone(), preview.clone());
        return Ok(preview);
    }
    let bytes = match fs::read(output_path) {
        Ok(bytes) => bytes,
        Err(err) => {
            let mut preview = base.clone();
            preview.error = Some(err.to_string());
            cache.insert(ref_value.output_hash.clone(), preview.clone());
            return Ok(preview);
        }
    };
    let mut preview = base;
    preview.output_bytes = Some(bytes.len());
    preview.truncated = Some(bytes.len() > BLOB_PREVIEW_MAX_BYTES);
    let slice = if bytes.len() > BLOB_PREVIEW_MAX_BYTES {
        &bytes[..BLOB_PREVIEW_MAX_BYTES]
    } else {
        &bytes[..]
    };
    let text = String::from_utf8_lossy(slice).to_string();
    if !preview.truncated.unwrap_or(false) && is_json_content(ref_value.content_type.as_deref()) {
        match serde_json::from_slice::<Value>(slice) {
            Ok(value) => preview.value = Some(value),
            Err(err) => {
                preview.text = Some(text.clone());
                preview.error = Some(format!("invalid JSON: {}", err));
            }
        }
    } else {
        preview.text = Some(text);
    }
    cache.insert(ref_value.output_hash.clone(), preview.clone());
    Ok(preview)
}

fn preview_attempt_log(
    run_dir: &Path,
    activity_id: &str,
    attempt_id: &str,
) -> Result<Option<BlobPreviewDTO>> {
    let path = run_dir
        .join("attempts")
        .join(activity_id)
        .join(attempt_id)
        .join("terminal.log");
    if !is_path_inside(run_dir, &path) {
        return Ok(Some(BlobPreviewDTO {
            output_hash: None,
            output_bytes: None,
            content_type: Some("text/plain".to_string()),
            truncated: None,
            value: None,
            text: None,
            error: Some("attempt log is outside run directory".to_string()),
            redacted: None,
        }));
    }
    let raw = match fs::read(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Ok(Some(BlobPreviewDTO {
                output_hash: None,
                output_bytes: None,
                content_type: Some("text/plain".to_string()),
                truncated: None,
                value: None,
                text: None,
                error: Some(err.to_string()),
                redacted: None,
            }));
        }
    };
    let bytes = raw.len();
    let start = bytes.saturating_sub(BLOB_PREVIEW_MAX_BYTES);
    let text = String::from_utf8_lossy(&raw[start..]).to_string();
    Ok(Some(BlobPreviewDTO {
        output_hash: None,
        output_bytes: Some(bytes),
        content_type: Some("text/plain".to_string()),
        truncated: Some(bytes > BLOB_PREVIEW_MAX_BYTES),
        value: None,
        text: Some(text),
        error: None,
        redacted: None,
    }))
}

fn read_attempt_terminal(
    run_dir: &Path,
    activity_id: &str,
    attempt_id: &str,
) -> Result<Option<AttemptTerminalDTO>> {
    let path = run_dir
        .join("attempts")
        .join(activity_id)
        .join(attempt_id)
        .join("terminal.json");
    if !is_path_inside(run_dir, &path) {
        return Ok(Some(AttemptTerminalDTO {
            session_id: String::new(),
            cli_session_id: None,
            web_port: 0,
            status: "closed".to_string(),
            lark_app_id: None,
            bot_name: None,
            cli_id: None,
            working_dir: None,
            log_path: None,
            started_at: 0,
            updated_at: 0,
            closed_at: None,
            error: Some("terminal sidecar is outside run directory".to_string()),
            has_pty_log: None,
        }));
    }
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Ok(Some(AttemptTerminalDTO {
                session_id: String::new(),
                cli_session_id: None,
                web_port: 0,
                status: "closed".to_string(),
                lark_app_id: None,
                bot_name: None,
                cli_id: None,
                working_dir: None,
                log_path: None,
                started_at: 0,
                updated_at: 0,
                closed_at: None,
                error: Some(err.to_string()),
                has_pty_log: None,
            }));
        }
    };
    let parsed: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(err) => {
            return Ok(Some(AttemptTerminalDTO {
                session_id: String::new(),
                cli_session_id: None,
                web_port: 0,
                status: "closed".to_string(),
                lark_app_id: None,
                bot_name: None,
                cli_id: None,
                working_dir: None,
                log_path: None,
                started_at: 0,
                updated_at: 0,
                closed_at: None,
                error: Some(format!("invalid terminal sidecar: {}", err)),
                has_pty_log: None,
            }));
        }
    };
    if payload_u64(&parsed, "schemaVersion") != Some(1)
        || payload_str(&parsed, "sessionId").is_none()
        || payload_u64(&parsed, "webPort").is_none()
        || !matches!(
            payload_str(&parsed, "status").as_deref(),
            Some("live" | "closed")
        )
        || payload_u64(&parsed, "startedAt").is_none()
        || payload_u64(&parsed, "updatedAt").is_none()
    {
        return Ok(Some(AttemptTerminalDTO {
            session_id: String::new(),
            cli_session_id: None,
            web_port: 0,
            status: "closed".to_string(),
            lark_app_id: None,
            bot_name: None,
            cli_id: None,
            working_dir: None,
            log_path: None,
            started_at: 0,
            updated_at: 0,
            closed_at: None,
            error: Some("invalid terminal sidecar".to_string()),
            has_pty_log: None,
        }));
    }
    let pty_log = path.with_file_name("pty.log");
    let has_pty_log = fs::metadata(&pty_log)
        .map(|meta| meta.is_file() && meta.len() > 0)
        .unwrap_or(false);
    Ok(Some(AttemptTerminalDTO {
        session_id: payload_str(&parsed, "sessionId").unwrap_or_default(),
        cli_session_id: payload_str(&parsed, "cliSessionId"),
        web_port: payload_u64(&parsed, "webPort").unwrap_or_default() as u16,
        status: payload_str(&parsed, "status").unwrap_or_else(|| "closed".to_string()),
        lark_app_id: payload_str(&parsed, "larkAppId"),
        bot_name: payload_str(&parsed, "botName"),
        cli_id: payload_str(&parsed, "cliId"),
        working_dir: payload_str(&parsed, "workingDir"),
        log_path: payload_str(&parsed, "logPath"),
        started_at: payload_u64(&parsed, "startedAt").unwrap_or_default(),
        updated_at: payload_u64(&parsed, "updatedAt").unwrap_or_default(),
        closed_at: payload_u64(&parsed, "closedAt"),
        error: None,
        has_pty_log: Some(has_pty_log),
    }))
}

fn preview_resolved_input(
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    raw_input: Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<BlobPreviewDTO> {
    match resolve_dashboard_bindings(&raw_input, run_dir, snap, def, cache) {
        Ok(value) => Ok(BlobPreviewDTO {
            output_hash: None,
            output_bytes: Some(serde_json::to_vec(&value)?.len()),
            content_type: Some("application/json".to_string()),
            truncated: None,
            value: Some(value),
            text: None,
            error: None,
            redacted: None,
        }),
        Err(err) => Ok(BlobPreviewDTO {
            output_hash: None,
            output_bytes: Some(serde_json::to_vec(&raw_input)?.len()),
            content_type: Some("application/json".to_string()),
            truncated: None,
            value: Some(raw_input),
            text: None,
            error: Some(format!("failed to resolve bindings: {}", err)),
            redacted: None,
        }),
    }
}

fn resolve_dashboard_bindings(
    value: &Value,
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<Value> {
    if let Some(ref_spec) = value.as_object().and_then(|obj| {
        if obj.len() == 1 {
            obj.get("$ref").and_then(Value::as_str)
        } else {
            None
        }
    }) {
        return resolve_dashboard_ref(ref_spec, run_dir, snap, def, cache);
    }
    if let Some(s) = value.as_str() {
        return interpolate_dashboard_string_refs(s, run_dir, snap, def, cache).map(Value::String);
    }
    if let Some(arr) = value.as_array() {
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(resolve_dashboard_bindings(item, run_dir, snap, def, cache)?);
        }
        return Ok(Value::Array(out));
    }
    if let Some(obj) = value.as_object() {
        let mut out = serde_json::Map::new();
        for (key, item) in obj {
            out.insert(
                key.clone(),
                resolve_dashboard_bindings(item, run_dir, snap, def, cache)?,
            );
        }
        return Ok(Value::Object(out));
    }
    Ok(value.clone())
}

fn interpolate_dashboard_string_refs(
    value: &str,
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<String> {
    if !value.contains("${") {
        return Ok(value.to_string());
    }
    let mut out = String::new();
    let mut cursor = 0;
    while let Some(start) = value[cursor..].find("${") {
        let start = cursor + start;
        out.push_str(&value[cursor..start]);
        let end = value[start + 2..]
            .find('}')
            .ok_or_else(|| anyhow::anyhow!("unterminated string ref interpolation in '{value}'"))?
            + start
            + 2;
        let ref_spec = &value[start + 2..end];
        if ref_spec.is_empty() {
            anyhow::bail!("empty string ref interpolation in '{value}'");
        }
        let resolved = resolve_dashboard_ref(ref_spec, run_dir, snap, def, cache)?;
        out.push_str(&stringify_dashboard_interpolated_value(ref_spec, resolved)?);
        cursor = end + 1;
    }
    out.push_str(&value[cursor..]);
    Ok(out)
}

fn stringify_dashboard_interpolated_value(ref_spec: &str, value: Value) -> Result<String> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::String(s) => Ok(s),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => anyhow::bail!(
            "string interpolation '${{{}}}' resolved to {}",
            ref_spec,
            if other.is_array() { "array" } else { "object" }
        ),
    }
}

fn resolve_dashboard_ref(
    ref_spec: &str,
    run_dir: &Path,
    snap: &ReplaySnapshot,
    def: &Value,
    cache: &mut HashMap<String, BlobPreviewDTO>,
) -> Result<Value> {
    if let Some(rest) = ref_spec.strip_prefix("params.") {
        let input_ref = snap
            .run
            .input
            .as_ref()
            .context(format!("$ref '{ref_spec}' requires run input"))?;
        let preview = preview_ref(run_dir, input_ref, cache)?;
        let params = preview.value.clone().context(format!(
            "$ref '{ref_spec}' output preview has no JSON value"
        ))?;
        return walk_preview_path(params, rest.split('.').collect::<Vec<_>>(), ref_spec);
    }
    let Some(sep_idx) = ref_spec.find(".output.") else {
        anyhow::bail!("$ref '{ref_spec}' missing '.output.' separator");
    };
    let node_id = &ref_spec[..sep_idx];
    let path = &ref_spec[sep_idx + ".output.".len()..];
    let node = def
        .get("nodes")
        .and_then(|nodes| nodes.get(node_id))
        .context(format!(
            "$ref '{ref_spec}' targets unknown node '{node_id}'"
        ))?;
    let output_ref = snap
        .outputs
        .get(&format!("{}::work::{}", snap.run.run_id, node_id))
        .context(format!("$ref '{ref_spec}' has no successful output yet"))?;
    let preview = preview_ref(run_dir, output_ref, cache)?;
    let value = preview.value.clone().context(format!(
        "$ref '{ref_spec}' output preview has no JSON value"
    ))?;
    let root = if node.get("type").and_then(Value::as_str) == Some("hostExecutor")
        && value
            .as_object()
            .map(|obj| obj.contains_key("output"))
            .unwrap_or(false)
    {
        value.get("output").cloned().unwrap_or(Value::Null)
    } else {
        value
    };
    walk_preview_path(root, path.split('.').collect::<Vec<_>>(), ref_spec)
}

fn walk_preview_path(value: Value, segments: Vec<&str>, ref_spec: &str) -> Result<Value> {
    let mut cursor = value;
    for seg in segments {
        if cursor.is_null() {
            anyhow::bail!("$ref '{ref_spec}' hit null at '{seg}'");
        }
        if let Some(arr) = cursor.as_array() {
            let idx: usize = seg
                .parse()
                .map_err(|_| anyhow::anyhow!("$ref '{ref_spec}' array index '{seg}' invalid"))?;
            cursor = arr.get(idx).cloned().context(format!(
                "$ref '{ref_spec}' array index '{seg}' out of bounds"
            ))?;
            continue;
        }
        let obj = cursor
            .as_object()
            .context(format!("$ref '{ref_spec}' segment '{seg}' not found"))?;
        cursor = obj
            .get(seg)
            .cloned()
            .context(format!("$ref '{ref_spec}' segment '{seg}' not found"))?;
    }
    Ok(cursor)
}

fn is_json_content(content_type: Option<&str>) -> bool {
    content_type
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains("json")
}

fn parse_loop_activity_id(activity_id: &str) -> Option<(String, u64)> {
    let loop_start = activity_id.find("::loop::")?;
    let after_loop = &activity_id[loop_start + "::loop::".len()..];
    let iter_end = after_loop.find("::")?;
    let loop_part = &after_loop[..iter_end];
    let (loop_id, iteration) = loop_part.rsplit_once('.')?;
    let iteration = iteration.parse().ok()?;
    Some((loop_id.to_string(), iteration))
}

fn is_path_inside(parent: &Path, child: &Path) -> bool {
    let parent = parent
        .canonicalize()
        .unwrap_or_else(|_| parent.to_path_buf());
    let child = child.canonicalize().unwrap_or_else(|_| child.to_path_buf());
    child.starts_with(&parent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    fn env(
        event_id: &str,
        run_id: &str,
        event_type: &str,
        payload: Value,
    ) -> WorkflowEventEnvelope {
        WorkflowEventEnvelope {
            event_id: event_id.to_string(),
            run_id: run_id.to_string(),
            timestamp: 1,
            schema_version: 1,
            actor: crate::WorkflowActor::System,
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
        let events = vec![
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
}
