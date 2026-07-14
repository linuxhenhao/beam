use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;

use crate::workflow_projection::event_seq_from_id;
use crate::{WorkflowEventEnvelope, WorkflowOutputRef};

use super::model::*;

#[derive(Debug, Clone)]
pub(super) struct ReplaySnapshot {
    pub(super) run: RunState,
    pub(super) nodes: BTreeMap<String, NodeState>,
    pub(super) activities: BTreeMap<String, ActivityState>,
    pub(super) loops: BTreeMap<String, LoopSnapshotDTO>,
    pub(super) outputs: BTreeMap<String, WorkflowOutputRef>,
    pub(super) last_seq: u64,
    pub(super) dangling_activities: Vec<String>,
    pub(super) dangling_effect_attempted: Vec<String>,
    pub(super) dangling_waits: Vec<String>,
    pub(super) dangling_wait_resolutions: Vec<String>,
    pub(super) dangling_cancels: Vec<String>,
}

pub(super) fn replay_events(events: &[WorkflowEventEnvelope]) -> Result<ReplaySnapshot> {
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
        dangling_wait_resolutions: Vec::new(),
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
        dangling_wait_resolutions,
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

fn parse_loop_activity_id(activity_id: &str) -> Option<(String, u64)> {
    let loop_start = activity_id.find("::loop::")?;
    let after_loop = &activity_id[loop_start + "::loop::".len()..];
    let iter_end = after_loop.find("::")?;
    let loop_part = &after_loop[..iter_end];
    let (loop_id, iteration) = loop_part.rsplit_once('.')?;
    let iteration = iteration.parse().ok()?;
    Some((loop_id.to_string(), iteration))
}
