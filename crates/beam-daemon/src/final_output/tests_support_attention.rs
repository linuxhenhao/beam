use std::collections::HashMap;

use beam_core::{AgentAttention, DisplayMode, RunStatus, SessionSummary, TuiPromptOption};
use chrono::Utc;

use super::*;
use crate::tests::test_helpers::*;
use crate::workflow_reconcilers;

#[test]
fn resolve_tui_prompt_final_text_prefers_toggled_option_texts() {
    let mut session = make_session("session-a");
    session.tui_prompt_options = vec![
        TuiPromptOption {
            label: Some("1".to_string()),
            text: "alpha".to_string(),
            selected: false,
            option_type: Some("toggle".to_string()),
            keys: vec!["A".to_string()],
        },
        TuiPromptOption {
            label: Some("2".to_string()),
            text: "beta".to_string(),
            selected: false,
            option_type: Some("toggle".to_string()),
            keys: vec!["B".to_string()],
        },
    ];
    session.tui_toggled_indices = vec![1, 0];
    assert_eq!(
        resolve_tui_prompt_final_text(&session, Some("fallback")),
        "alpha, beta"
    );
    session.tui_toggled_indices.clear();
    assert_eq!(
        resolve_tui_prompt_final_text(&session, Some("fallback")),
        "fallback"
    );
    assert_eq!(resolve_tui_prompt_final_text(&session, None), "selection");
}

#[test]
fn retryable_feishu_resume_error_detects_timeout_and_rate_limit() {
    assert!(is_retryable_feishu_resume_error(&anyhow::anyhow!(
        "request timed out"
    )));
    assert!(is_retryable_feishu_resume_error(&anyhow::anyhow!(
        "429 too many requests"
    )));
    assert!(!is_retryable_feishu_resume_error(&anyhow::anyhow!(
        "permission denied"
    )));
}

#[test]
fn build_feishu_transient_failure_marks_retryable_result() {
    let failure = build_feishu_transient_failure(
        "activity-1",
        "attempt-1",
        "feishu-im",
        "idem-key-1",
        "FeishuSubmitRetryable",
        "request timed out".to_string(),
    );
    assert_eq!(failure.provider, "feishu-im");
    assert_eq!(failure.error_class, "retryable");
    assert_eq!(failure.error_code, "FeishuSubmitRetryable");
    assert_eq!(failure.idempotency_key, "idem-key-1");
}

#[test]
fn build_workflow_resume_response_includes_transient_failures() {
    let schedule_result = beam_core::ScheduleResumeResult {
        reconciled: vec![beam_core::ScheduleResumeOutcome {
            activity_id: "act-s".to_string(),
            attempt_id: "att-s".to_string(),
            decision: "completedByIdempotentSubmit".to_string(),
        }],
        fresh_retry: vec![],
        skipped: vec!["skip-s".to_string()],
    };
    let feishu_result = FeishuResumeResult {
        reconciled: vec![],
        fresh_retry: vec![],
        transient_failures: vec![FeishuTransientFailure {
            activity_id: "act-f".to_string(),
            attempt_id: "att-f".to_string(),
            provider: "feishu-im".to_string(),
            idempotency_key: "idem-f".to_string(),
            error_code: "FeishuSubmitRetryable".to_string(),
            error_class: "retryable".to_string(),
            error_message: "request timed out".to_string(),
        }],
        skipped: vec!["skip-f".to_string()],
    };
    let snapshot = beam_core::RunSnapshotDTO {
        run_id: "run-1".to_string(),
        run: beam_core::RunState {
            run_id: "run-1".to_string(),
            status: RunStatus::Running,
            workflow_id: Some("flow-1".to_string()),
            revision_id: Some("rev-1".to_string()),
            initiator: Some("cli".to_string()),
            input: None,
            output: None,
            failed_node_id: None,
            root_cause_event_id: None,
            cancel_origin_event_id: None,
            bot_snapshots: None,
            cancelled_run_intent: None,
            cancelled_node_intents: Default::default(),
        },
        last_seq: 42,
        nodes: vec![],
        activities: vec![],
        loops: None,
        dangling: beam_core::DanglingSnapshot {
            activities: vec![],
            effect_attempted: vec![],
            waits: vec![],
            wait_resolutions: vec![],
            cancels: vec![],
        },
        outputs: Default::default(),
        attempt_io: Default::default(),
        chat_binding: None,
        updated_at: 123,
    };
    let resume_started_event = beam_core::WorkflowEventEnvelope {
        event_id: "run-1-43".to_string(),
        run_id: "run-1".to_string(),
        timestamp: 0,
        schema_version: 1,
        actor: beam_core::WorkflowActor::System,
        event_type: "resumeStarted".to_string(),
        payload: serde_json::json!({
            "daemonId": "beam-daemon",
            "lastSeenEventId": "run-1-42",
            "reason": null,
        }),
        payload_hash: None,
    };
    let payload = build_workflow_resume_response(
        "run-1".to_string(),
        RunStatus::Running,
        false,
        42,
        Some(&resume_started_event),
        &HashMap::new(),
        &snapshot,
        &schedule_result,
        &feishu_result,
        &workflow_reconcilers::ReconcilerRegistryCheckResult {
            covered_providers: vec!["beam-schedule".to_string(), "feishu-im".to_string()],
            missing_providers: vec![],
        },
        vec![],
        vec![],
        vec![],
    );
    assert_eq!(payload["runId"], "run-1");
    assert_eq!(payload["resumeStartedEventId"], "run-1-43");
    assert_eq!(payload["resumeStartedEvent"]["eventId"], "run-1-43");
    assert_eq!(payload["resumeStartedEvent"]["type"], "resumeStarted");
    assert_eq!(
        payload["resumeStartedEvent"]["payload"]["daemonId"],
        "beam-daemon"
    );
    assert_eq!(
        payload["resumeStartedEvent"]["payload"]["lastSeenEventId"],
        "run-1-42"
    );
    assert_eq!(payload["reconciled"], 1);
    assert_eq!(payload["freshRetry"], 0);
    assert_eq!(
        payload["reconcileOutcomes"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(payload["reconcileOutcomes"][0]["provider"], "beam-schedule");
    assert_eq!(
        payload["reconcileOutcomes"][0]["capability"],
        "readOnlyLookup"
    );
    assert_eq!(payload["reconcileOutcomes"][0]["recovered"], false);
    assert_eq!(
        payload["workerCrashedOutcomes"].as_array().map(Vec::len),
        Some(0)
    );
    assert_eq!(
        payload["waitRecoveryOutcomes"].as_array().map(Vec::len),
        Some(0)
    );
    assert_eq!(
        payload["cancelRecoveryOutcomes"].as_array().map(Vec::len),
        Some(0)
    );
    assert_eq!(
        payload["transientFailures"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        payload["transientFailures"][0]["errorCode"],
        "FeishuSubmitRetryable"
    );
    assert_eq!(payload["feishuOutcomes"].as_array().map(Vec::len), Some(0));
    assert_eq!(
        payload["scheduleOutcomes"].as_array().map(Vec::len),
        Some(1)
    );
}

#[test]
fn next_display_mode_toggles_hidden_and_screenshot() {
    assert_eq!(next_display_mode(None), DisplayMode::Screenshot);
    assert_eq!(
        next_display_mode(Some(DisplayMode::Hidden)),
        DisplayMode::Screenshot
    );
    assert_eq!(
        next_display_mode(Some(DisplayMode::Screenshot)),
        DisplayMode::Hidden
    );
}
// ---- attention usage constraint tests (botmux parity) ----

fn make_attention_req(
    attention: Option<&str>,
    top_level: bool,
    chat_id: Option<&str>,
    into: Option<&str>,
    voice: bool,
    content: &str,
) -> FinalOutputRequest {
    FinalOutputRequest {
        content: content.to_string(),
        mentions: vec![],
        mention_back: true,
        no_mention: false,
        files: vec![],
        images: vec![],
        top_level,
        chat_id: chat_id.map(|s| s.to_string()),
        into: into.map(|s| s.to_string()),
        quote: None,
        no_quote: false,
        voice,
        attention: attention.map(|s| s.to_string()),
        card: false,
        text: false,
        anyway: false,
    }
}

#[test]
fn attention_accepts_valid_request() {
    let req = make_attention_req(Some("blocked"), false, None, None, false, "I need help");
    assert!(validate_attention_constraints(&req).is_none());
}

#[test]
fn attention_noop_when_not_requested() {
    let req = make_attention_req(None, true, None, None, false, "");
    assert!(validate_attention_constraints(&req).is_none());
}

#[test]
fn attention_rejects_top_level() {
    let req = make_attention_req(Some("blocked"), true, None, None, false, "hello");
    let err = validate_attention_constraints(&req).expect("should reject --top-level");
    assert!(err.contains("--attention cannot be combined with --top-level"));
}

#[test]
fn attention_rejects_chat_id() {
    let req = make_attention_req(
        Some("decision"),
        false,
        Some("oc_test123"),
        None,
        false,
        "hello",
    );
    let err = validate_attention_constraints(&req).expect("should reject --chat-id");
    assert!(err.contains("--attention cannot be combined with"));
    assert!(err.contains("--chat-id"));
}

#[test]
fn attention_rejects_into() {
    let req = make_attention_req(
        Some("help"),
        false,
        None,
        Some("om_test123"),
        false,
        "hello",
    );
    let err = validate_attention_constraints(&req).expect("should reject --into");
    assert!(err.contains("--attention cannot be combined with"));
    assert!(err.contains("--into"));
}

#[test]
fn attention_rejects_voice() {
    let req = make_attention_req(Some("blocked"), false, None, None, true, "hello");
    let err = validate_attention_constraints(&req).expect("should reject --voice");
    assert!(err.contains("--attention cannot be combined with --voice"));
}

#[test]
fn attention_rejects_empty_content() {
    let req = make_attention_req(Some("blocked"), false, None, None, false, "");
    let err = validate_attention_constraints(&req).expect("should reject empty content");
    assert!(err.contains("--attention requires a non-empty text reason"));
}

#[test]
fn attention_rejects_whitespace_only_content() {
    let req = make_attention_req(Some("blocked"), false, None, None, false, "   \n  ");
    let err = validate_attention_constraints(&req).expect("should reject whitespace-only content");
    assert!(err.contains("--attention requires a non-empty text reason"));
}

// ---- attention state helpers ----

#[test]
fn normalize_attention_reason_collapses_whitespace() {
    let raw = "hello   world\n\ttest   foo";
    let result = normalize_attention_reason(raw);
    assert_eq!(result, "hello world test foo");
}

#[test]
fn normalize_attention_reason_truncates_over_500() {
    let long: String = std::iter::repeat("abcde")
        .take(120)
        .collect::<Vec<_>>()
        .join(" ");
    let count = long.len();
    assert!(
        count > 500,
        "test input must be longer than 500, got {}",
        count
    );
    let result = normalize_attention_reason(&long);
    assert!(
        result.len() <= 500,
        "result len {} should be <= 500",
        result.len()
    );
    assert!(
        !result.ends_with(' '),
        "should not end with trailing space: {:?}",
        result
    );
}

#[test]
fn normalize_attention_reason_short_text_unchanged() {
    let raw = "  need approval for deployment  ";
    let result = normalize_attention_reason(raw);
    assert_eq!(result, "need approval for deployment");
}

#[test]
fn normalize_attention_reason_empty() {
    assert_eq!(normalize_attention_reason(""), "");
    assert_eq!(normalize_attention_reason("   \n  "), "");
}

#[test]
fn clear_agent_attention_sets_to_none() {
    let mut session = make_session("sess-clear");
    session.agent_attention = Some(AgentAttention {
        kind: "blocked".to_string(),
        reason: "test".to_string(),
        at: Utc::now(),
    });
    assert!(session.agent_attention.is_some());
    clear_agent_attention(&mut session);
    assert!(session.agent_attention.is_none());
}

#[test]
fn session_summary_includes_agent_attention() {
    let mut session = make_session("sess-summary");
    session.agent_attention = Some(AgentAttention {
        kind: "blocked".to_string(),
        reason: "need approval".to_string(),
        at: Utc::now(),
    });
    let summary = SessionSummary::from(&session);
    assert!(summary.agent_attention.is_some());
    assert_eq!(summary.agent_attention.as_ref().unwrap().kind, "blocked");
    assert_eq!(
        summary.agent_attention.as_ref().unwrap().reason,
        "need approval"
    );
}

#[test]
fn session_summary_no_agent_attention_when_none() {
    let session = make_session("sess-no-attention");
    assert!(session.agent_attention.is_none());
    let summary = SessionSummary::from(&session);
    assert!(summary.agent_attention.is_none());
}

#[tokio::test]
async fn set_session_attention_stores_and_reads_back() {
    let paths = temp_paths("attn-set");
    let mut bots = HashMap::new();
    bots.insert("app-1".to_string(), make_bot("app-1"));
    let state = make_state(paths.clone(), bots);

    // Insert a session
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert("sess-attn-1".to_string(), make_session("sess-attn-1"));
        let snapshot = sessions.clone();
        drop(sessions);
        persist_sessions(&state.paths, &snapshot).await.unwrap();
    }

    // Set attention
    set_session_attention(&state, "sess-attn-1", "blocked", "need approval from team")
        .await
        .expect("set attention should succeed");

    // Read back
    let sessions = state.sessions.lock().await;
    let session = sessions.get("sess-attn-1").expect("session should exist");
    let aa = session
        .agent_attention
        .as_ref()
        .expect("should have attention");
    assert_eq!(aa.kind, "blocked");
    assert_eq!(aa.reason, "need approval from team");

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn set_session_attention_rejects_invalid_kind() {
    let paths = temp_paths("attn-invalid");
    let mut bots = HashMap::new();
    bots.insert("app-1".to_string(), make_bot("app-1"));
    let state = make_state(paths.clone(), bots);

    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert("sess-attn-2".to_string(), make_session("sess-attn-2"));
        let snapshot = sessions.clone();
        drop(sessions);
        persist_sessions(&state.paths, &snapshot).await.unwrap();
    }

    let result = set_session_attention(&state, "sess-attn-2", "invalid_kind", "reason").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("invalid attention kind"));

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn set_session_attention_rejects_empty_reason() {
    let paths = temp_paths("attn-empty");
    let mut bots = HashMap::new();
    bots.insert("app-1".to_string(), make_bot("app-1"));
    let state = make_state(paths.clone(), bots);

    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert("sess-attn-3".to_string(), make_session("sess-attn-3"));
        let snapshot = sessions.clone();
        drop(sessions);
        persist_sessions(&state.paths, &snapshot).await.unwrap();
    }

    let result = set_session_attention(&state, "sess-attn-3", "blocked", "  ").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("must not be empty"));

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn set_session_attention_rejects_missing_session() {
    let paths = temp_paths("attn-missing");
    let mut bots = HashMap::new();
    bots.insert("app-1".to_string(), make_bot("app-1"));
    let state = make_state(paths.clone(), bots);

    let result = set_session_attention(&state, "nonexistent", "blocked", "reason").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("session not found"));

    maybe_remove_dir(&paths.root().to_path_buf());
}
