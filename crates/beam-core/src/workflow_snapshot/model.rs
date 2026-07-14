use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RunChatBinding;
use crate::WorkflowOutputRef;

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
    pub has_terminal_log: Option<bool>,
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
    /// Activities where a wait resolution (waitResolved or waitDeadlineExceeded)
    /// was written but the terminal activity event (activitySucceeded / activityFailed)
    /// has not been written yet.  These should be materialised by recovery/resume.
    pub wait_resolutions: Vec<String>,
    pub cancels: Vec<String>,
}
