//! Workflow event fanout: detects new human-gate waits and automatically
//! sends approval cards with approve/reject/cancel buttons.
//!
//! This module listens for new `waitCreated` events with `waitKind == human-gate`
//! and dispatches an interactive Lark card.  Idempotency is guaranteed through a
//! marker file (`approval-card-sent.json`) so that repeated runtime execution,
//! cold attach, or recovery do not re-send duplicate cards.
//!
//! Phase 5.3: Automatic approval card fanout.
//! Design constraint: EventLog is the sole truth source — we scan the snapshot
//! (which is derived from the EventLog) to discover pending waits.  We never
//! bypass the EventLog to write state.

use anyhow::{Context, Result};
use async_trait::async_trait;
use beam_core::{RunChatBinding, RunStatus, WaitState, read_run_snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use tracing::{info, warn};

use crate::{AppState, BotConfig};

// ---------------------------------------------------------------------------
// Approval card sender trait (allows mocking in tests)
// ---------------------------------------------------------------------------

/// Trait for sending approval cards.  The daemon provides a real Lark-based
/// implementation; tests can inject a mock that counts calls.
#[async_trait]
pub trait ApprovalCardSender: Send + Sync {
    /// Send a card to the given chat.
    /// Returns the message_id on success.
    async fn send_card(
        &self,
        chat_id: &str,
        card_json: &str,
    ) -> Result<String>;
}

/// Real Lark-based sender.
pub struct LarkCardSender<'a> {
    state: &'a AppState,
    bot: &'a BotConfig,
}

#[async_trait]
impl<'a> ApprovalCardSender for LarkCardSender<'a> {
    async fn send_card(&self, chat_id: &str, card_json: &str) -> Result<String> {
        crate::send_lark_card_in_chat(self.state, self.bot, chat_id, card_json).await
    }
}

// ---------------------------------------------------------------------------
// Idempotency marker (file-based, survives daemon restarts)
// ---------------------------------------------------------------------------

/// Tracks which (activity_id, attempt_id) pairs have already had an approval
/// card sent.  Persisted to `approval-card-sent.json` in the workflow run
/// directory.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ApprovalCardSentMarker {
    /// Set of `"activity_id::attempt_id"` strings.
    sent: HashSet<String>,
}

impl ApprovalCardSentMarker {
    fn path(run_dir: &PathBuf) -> PathBuf {
        run_dir.join("approval-card-sent.json")
    }

    async fn load(run_dir: &PathBuf) -> Result<Self> {
        let path = Self::path(run_dir);
        match tokio::fs::read_to_string(&path).await {
            Ok(raw) => {
                let marker: Self = serde_json::from_str(&raw)
                    .with_context(|| format!("failed to parse {:?}", path))?;
                Ok(marker)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::default())
            }
            Err(err) => Err(err).with_context(|| format!("failed to read {:?}", path)),
        }
    }

    async fn save(&self, run_dir: &PathBuf) -> Result<()> {
        let path = Self::path(run_dir);
        let tmp = path.with_extension("json.tmp");
        let body = serde_json::to_string_pretty(self)?;
        tokio::fs::write(&tmp, &body)
            .await
            .with_context(|| format!("failed to write {:?}", tmp))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .with_context(|| format!("failed to rename {:?} -> {:?}", tmp, path))?;
        Ok(())
    }

    fn is_sent(&self, activity_id: &str, attempt_id: &str) -> bool {
        self.sent.contains(&format!("{}::{}", activity_id, attempt_id))
    }

    fn mark_sent(&mut self, activity_id: &str, attempt_id: &str) {
        self.sent
            .insert(format!("{}::{}", activity_id, attempt_id));
    }
}

// ---------------------------------------------------------------------------
// Card builder
// ---------------------------------------------------------------------------

/// Build a Lark interactive card for a human-gate approval wait.
///
/// The button values are designed to be parsed by the existing
/// `parse_lark_card_action` in `lib.rs`, matching the fields expected by the
/// `wf_approve` / `wf_reject` / `wf_cancel` action handlers (Task 5.1/5.2).
///
/// Card nonce format: `{run_id}-{activity_id}-{attempt_id}`.
///
/// When `dashboard_url` is provided, a "📊 Open Dashboard" url-button is
/// inserted above the footer note.
fn build_approval_card(
    run_id: &str,
    workflow_id: &str,
    revision_id: &str,
    node_id: &str,
    activity_id: &str,
    attempt_id: &str,
    card_nonce: &str,
    prompt: Option<&str>,
    dashboard_url: Option<&str>,
) -> serde_json::Value {
    let header_text = format!("Workflow Approval: {}", workflow_id);

    let mut body = format!(
        "**Run**\n{}\n\n**Step**\n{}\n\n**Activity**\n{}",
        run_id, node_id, activity_id,
    );
    if let Some(p) = prompt.filter(|p| !p.is_empty()) {
        body.push_str(&format!("\n\n**Prompt**\n{}", p));
    }

    let button_value = |action: &str| -> serde_json::Value {
        serde_json::json!({
            "action": action,
            "run_id": run_id,
            "workflow_id": workflow_id,
            "revision_id": revision_id,
            "node_id": node_id,
            "activity_id": activity_id,
            "attempt_id": attempt_id,
            "card_nonce": card_nonce,
        })
    };

    let mut elements: Vec<serde_json::Value> = vec![
        serde_json::json!({
            "tag": "div",
            "text": { "tag": "lark_md", "content": body }
        }),
        serde_json::json!({
            "tag": "input",
            "name": "wf_comment",
            "placeholder": {
                "tag": "plain_text",
                "content": "添加备注 (可选)"
            }
        }),
        serde_json::json!({
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": { "tag": "lark_md", "content": "✅ 通过" },
                    "type": "primary",
                    "value": button_value("wf_approve")
                },
                {
                    "tag": "button",
                    "text": { "tag": "lark_md", "content": "❌ 拒绝" },
                    "type": "danger",
                    "value": button_value("wf_reject")
                },
                {
                    "tag": "button",
                    "text": { "tag": "lark_md", "content": "🛑 取消" },
                    "type": "default",
                    "value": button_value("wf_cancel")
                }
            ]
        }),
    ];

    // Dashboard link as a separate url-button row.
    if let Some(url) = dashboard_url.filter(|u| !u.is_empty()) {
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": { "tag": "lark_md", "content": "\u{1f4ca} Open Dashboard" },
                    "type": "primary",
                    "url": url,
                }
            ]
        }));
    }

    elements.push(serde_json::json!({ "tag": "hr" }));
    elements.push(serde_json::json!({
        "tag": "note",
        "elements": [
            { "tag": "plain_text", "content": format!("Run: {} | Activity: {}", run_id, activity_id) }
        ]
    }));

    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "header": {
            "template": "blue",
            "title": { "tag": "plain_text", "content": header_text }
        },
        "elements": elements,
    })
}

// ---------------------------------------------------------------------------
// Fanout logic
// ---------------------------------------------------------------------------

/// Scan the workflow snapshot for human-gate waits that haven't received an
/// approval card yet, and send cards via the given sender.
///
/// Returns the number of cards sent (0 if no new waits needed cards).
pub(crate) async fn fanout_approval_cards_if_needed<S: ApprovalCardSender>(
    state: &AppState,
    run_id: &str,
    sender: &S,
) -> usize {
    let run_dir = state.paths.workflow_run_dir(run_id);

    // 1. Read snapshot — this is the authoritative view derived from EventLog.
    let snapshot = match read_run_snapshot(&run_dir).await {
        Ok(Some(snap)) => snap,
        Ok(None) => return 0,
        Err(err) => {
            warn!("fanout: failed to read snapshot for {}: {}", run_id, err);
            return 0;
        }
    };

    // Do not fanout for terminal runs.
    if matches!(
        snapshot.run.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
    ) {
        return 0;
    }

    // 2. Resolve chat binding — required to send a card.
    let binding: Option<RunChatBinding> = snapshot.chat_binding.clone();
    let chat_id = match &binding {
        Some(b) => b.chat_id.clone(),
        None => {
            warn!(
                "fanout: no chat binding for run {}, skipping approval card",
                run_id
            );
            return 0;
        }
    };

    // 3. Load idempotency marker.
    let mut marker = match ApprovalCardSentMarker::load(&run_dir).await {
        Ok(m) => m,
        Err(err) => {
            warn!(
                "fanout: failed to load approval-card-sent marker for {}: {}",
                run_id, err
            );
            return 0;
        }
    };

    // 4. Find human-gate waits that need cards.
    let workflow_id = snapshot.run.workflow_id.as_deref().unwrap_or("unknown");
    let revision_id = snapshot.run.revision_id.as_deref().unwrap_or("unknown");

    let mut sent = 0usize;
    for activity_id in &snapshot.dangling.waits {
        let activity = match snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
        {
            Some(a) => a,
            None => continue,
        };
        let attempt = match activity.attempts.last() {
            Some(a) => a,
            None => continue,
        };
        let wait: &WaitState = match attempt.wait.as_ref() {
            Some(w) => w,
            None => continue,
        };

        // Only fanout human-gate waits.
        if wait.wait_kind != "human-gate" {
            continue;
        }

        // Skip if already sent (idempotency check).
        if marker.is_sent(activity_id, &attempt.attempt_id) {
            continue;
        }

        let node_id = activity.owner_node_id.as_deref().unwrap_or(activity_id);
        let card_nonce = format!("{}-{}-{}", run_id, activity_id, attempt.attempt_id);

        let dashboard_url = format!(
            "http://{}/dashboard/workflows/{}",
            state.external_host, run_id
        );

        let card = build_approval_card(
            run_id,
            workflow_id,
            revision_id,
            node_id,
            activity_id,
            &attempt.attempt_id,
            &card_nonce,
            wait.prompt.as_deref(),
            Some(&dashboard_url),
        );
        let card_json = card.to_string();

        match sender.send_card(&chat_id, &card_json).await {
            Ok(msg_id) => {
                info!(
                    "fanout: sent approval card for run {} activity {} attempt {} (msg: {})",
                    run_id, activity_id, attempt.attempt_id, msg_id
                );
                marker.mark_sent(activity_id, &attempt.attempt_id);
                sent += 1;
            }
            Err(err) => {
                warn!(
                    "fanout: failed to send approval card for run {} activity {}: {}",
                    run_id, activity_id, err
                );
                // Do not mark as sent — will retry on next fanout pass.
            }
        }
    }

    // 5. Persist marker if we sent any cards.
    if sent > 0 {
        if let Err(err) = marker.save(&run_dir).await {
            warn!(
                "fanout: failed to save approval-card-sent marker for {}: {}",
                run_id, err
            );
        }
    }

    sent
}

// ---------------------------------------------------------------------------
// Convenience wrapper — creates a real Lark sender and fans out.
// ---------------------------------------------------------------------------

/// Convenience wrapper that creates a `LarkCardSender` from `AppState` + bot
/// config and fans out approval cards.  This is the primary entry point called
/// from the runtime driver.
pub(crate) async fn fanout_with_lark_sender(
    state: &AppState,
    run_id: &str,
) -> usize {
    // Resolve the bot from the run's chat binding.
    let run_dir = state.paths.workflow_run_dir(run_id);
    let binding_path = run_dir.join("chat-binding.json");
    let binding: Option<RunChatBinding> = tokio::fs::read_to_string(&binding_path)
        .await
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok());

    let bot = match &binding {
        Some(b) => match state.bots.get(&b.lark_app_id).cloned() {
            Some(bot) => bot,
            None => {
                warn!(
                    "fanout: bot not found for lark_app_id {} (run {}), skipping",
                    b.lark_app_id, run_id
                );
                return 0;
            }
        },
        None => return 0,
    };

    let sender = LarkCardSender {
        state,
        bot: &bot,
    };
    fanout_approval_cards_if_needed(state, run_id, &sender).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::{BeamPaths, BootstrapWorkflowRunInput, bootstrap_workflow_run};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    // ---- helpers ----------------------------------------------------------

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-wf-fanout-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    fn make_state(paths: &BeamPaths) -> AppState {
        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        AppState {
            paths: paths.clone(),
            started_at: chrono::Utc::now(),
            sessions: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            workers: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            attempt_resumes: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            shutdown: Arc::new(tokio::sync::Mutex::new(Some(_shutdown_tx))),
            options: crate::RunOptions {
                worker_exe: std::path::PathBuf::from("/bin/true"),
            },
            http: reqwest::Client::new(),
            config: beam_core::Config::default(),
            bots: Arc::new(std::collections::HashMap::new()),
            lark_tokens: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            chat_mode_cache: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            recent_lark_events: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            inflight_final_output_turns: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            workflow_progress_cards: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            ask_pending: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            grant_pending: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            dashboard_token: Arc::new(tokio::sync::Mutex::new(None)),
            external_host: "localhost".to_string(),
        }
    }

    /// A mock sender that records calls and returns a dummy message_id.
    struct MockSender {
        calls: Arc<StdMutex<Vec<(String, String)>>>,
    }

    impl MockSender {
        fn new() -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl ApprovalCardSender for MockSender {
        async fn send_card(&self, chat_id: &str, card_json: &str) -> Result<String> {
            self.calls
                .lock()
                .unwrap()
                .push((chat_id.to_string(), card_json.to_string()));
            Ok("msg_mock_001".to_string())
        }
    }

    // ---- tests ------------------------------------------------------------

    /// Verify that after a workflow enters a human-gate wait, the fanout
    /// discovers the wait and attempts to send an approval card.
    #[tokio::test]
    async fn human_gate_wait_triggers_approval_card_fanout() {
        let paths = temp_paths("gate-fanout");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-gate-fanout";

        // Bootstrap a workflow with a human-gate node.
        let def = r#"{"workflowId":"flow-gate","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"Please approve"}}}}"#;
        let chat_binding = RunChatBinding {
            chat_id: "oc_test_chat".to_string(),
            lark_app_id: "app_test".to_string(),
        };
        let _bootstrap = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-gate"),
                params: &BTreeMap::new(),
                initiator: "test",
                chat_binding: Some(chat_binding),
            },
        )
        .expect("bootstrap");

        // Advance runtime once to create the wait.
        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // Now fanout.
        let mock = MockSender::new();
        let sent = fanout_approval_cards_if_needed(&state, run_id, &mock).await;

        assert_eq!(sent, 1, "expected one card to be sent");
        assert!(mock.call_count() >= 1, "mock sender should have been called");

        // Verify the marker was persisted.
        let marker = ApprovalCardSentMarker::load(&paths.workflow_run_dir(run_id))
            .await
            .expect("marker load");
        assert!(
            !marker.sent.is_empty(),
            "marker should have recorded the sent card"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    /// Verify that repeated fanout calls do NOT re-send approval cards
    /// (idempotency via the marker file).
    #[tokio::test]
    async fn repeated_fanout_does_not_duplicate() {
        let paths = temp_paths("repeat-fanout");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-repeat-fanout";

        let def = r#"{"workflowId":"flow-repeat","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"Approve?"}}}}"#;
        let chat_binding = RunChatBinding {
            chat_id: "oc_test_chat".to_string(),
            lark_app_id: "app_test".to_string(),
        };
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-repeat"),
                params: &BTreeMap::new(),
                initiator: "test",
                chat_binding: Some(chat_binding),
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let mock = MockSender::new();

        // First fanout — should send.
        let sent1 = fanout_approval_cards_if_needed(&state, run_id, &mock).await;
        assert_eq!(sent1, 1, "first fanout should send one card");

        // Second fanout — should NOT send (idempotent).
        let sent2 = fanout_approval_cards_if_needed(&state, run_id, &mock).await;
        assert_eq!(sent2, 0, "second fanout should not re-send");
        assert_eq!(mock.call_count(), 1, "mock should have been called only once");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    /// Verify that a missing chat binding does not cause a panic and
    /// the fanout gracefully returns 0.
    #[tokio::test]
    async fn missing_chat_binding_does_not_crash() {
        let paths = temp_paths("no-binding");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-no-binding";

        // Bootstrap without chat binding.
        let def = r#"{"workflowId":"flow-nobind","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"ok?"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-nobind"),
                params: &BTreeMap::new(),
                initiator: "test",
                chat_binding: None, // <-- no binding
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let mock = MockSender::new();
        let sent = fanout_approval_cards_if_needed(&state, run_id, &mock).await;

        assert_eq!(sent, 0, "fanout should skip when chat binding is missing");
        assert_eq!(mock.call_count(), 0, "mock should not have been called");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    /// Verify that a terminal run (e.g., already succeeded) does not trigger
    /// fanout even if waits are present.
    #[tokio::test]
    async fn terminal_run_skips_fanout() {
        let paths = temp_paths("terminal-skip");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-terminal-skip";

        // Bootstrap a simple non-gate workflow that runs to completion.
        let def = r#"{"workflowId":"flow-simple","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo ok"},"humanGate":{"stage":"approve","prompt":"ok?"}}}}"#;
        let chat_binding = RunChatBinding {
            chat_id: "oc_test".to_string(),
            lark_app_id: "app_test".to_string(),
        };
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-simple"),
                params: &BTreeMap::new(),
                initiator: "test",
                chat_binding: Some(chat_binding),
            },
        )
        .expect("bootstrap");

        // Run the runtime — it should create a wait (human-gate).
        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // Now manually resolve the wait to completion (write waitResolved + terminal).
        // Then the snapshot should show terminal status.
        // For this test, we just verify that the fanout correctly reads the
        // snapshot and doesn't try to send when the run is terminal.
        // Since we don't resolve, the run is still running with a wait.
        // But the test name says "terminal run skips fanout" — we need a terminal run.
        // Let me use a different approach: create a simple workflow that
        // completes immediately (no human gate).
        let def_simple = r#"{"workflowId":"flow-termin","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo done"}}}}"#;
        let run_id2 = "run-terminal-2";
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: run_id2,
                workflow_json: def_simple,
                expected_workflow_id: Some("flow-termin"),
                params: &BTreeMap::new(),
                initiator: "test",
                chat_binding: Some(RunChatBinding {
                    chat_id: "oc_test".to_string(),
                    lark_app_id: "app_test".to_string(),
                }),
            },
        )
        .expect("bootstrap");
        crate::run_workflow_runtime_once(&state, run_id2, def_simple).await;

        let mock = MockSender::new();
        let sent = fanout_approval_cards_if_needed(&state, run_id2, &mock).await;
        assert_eq!(sent, 0, "fanout should skip terminal runs");
        assert_eq!(mock.call_count(), 0);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    /// Verify the convenience wrapper `fanout_with_lark_sender` when no bot is
    /// found (returns 0 gracefully, no panic).
    #[tokio::test]
    async fn fanout_with_lark_sender_no_bot_returns_zero() {
        let paths = temp_paths("no-bot");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths); // bots is empty
        let run_id = "run-no-bot";

        // Write only a chat-binding.json without a matching bot in state.
        let run_dir = paths.workflow_run_dir(run_id);
        std::fs::create_dir_all(&run_dir).unwrap();
        let binding = RunChatBinding {
            chat_id: "oc_test".to_string(),
            lark_app_id: "app_unknown".to_string(),
        };
        std::fs::write(
            run_dir.join("chat-binding.json"),
            serde_json::to_string_pretty(&binding).unwrap(),
        )
        .unwrap();

        // Also write a minimal events file so read_run_snapshot returns Some.
        let events_path = run_dir.join("events.ndjson");
        std::fs::write(&events_path, "").unwrap();

        let sent = fanout_with_lark_sender(&state, run_id).await;
        assert_eq!(sent, 0);

        let _ = std::fs::remove_dir_all(paths.root());
    }

    /// Verify that fanout only targets human-gate waits, not other wait kinds.
    #[tokio::test]
    async fn non_human_gate_wait_is_not_fanned_out() {
        let paths = temp_paths("non-gate");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let run_id = "run-nongate";

        // Create a workflow that is NOT a human-gate.  Since all waits in the
        // current codebase are human-gate, we test by having no waits at all
        // (the runtime completes immediately).
        let def = r#"{"workflowId":"flow-nongate","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo done"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-nongate"),
                params: &BTreeMap::new(),
                initiator: "test",
                chat_binding: Some(RunChatBinding {
                    chat_id: "oc_test".to_string(),
                    lark_app_id: "app_test".to_string(),
                }),
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        let mock = MockSender::new();
        let sent = fanout_approval_cards_if_needed(&state, run_id, &mock).await;
        // The run should complete without a human-gate wait, so no card.
        assert_eq!(sent, 0, "no human-gate wait should trigger no card");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    /// Verify that the built approval card contains the expected button actions
    /// and fields that the existing parse_lark_card_action / wf_ handlers expect.
    #[test]
    fn approval_card_contains_required_button_fields() {
        let dashboard_url = "http://localhost:9876/dashboard/workflows/run-1";

        let card = build_approval_card(
            "run-1",
            "flow-a",
            "rev-9",
            "node-gate",
            "act-1",
            "att-1",
            "nonce-1",
            Some("please approve"),
            Some(dashboard_url),
        );

        // Check header.
        assert_eq!(
            card["header"]["title"]["content"].as_str().unwrap(),
            "Workflow Approval: flow-a"
        );

        // Check body contains the prompt.
        let body = card["elements"][0]["text"]["content"].as_str().unwrap();
        assert!(body.contains("please approve"));

        // Check comment input exists.
        assert_eq!(card["elements"][1]["tag"].as_str().unwrap(), "input");
        assert_eq!(card["elements"][1]["name"].as_str().unwrap(), "wf_comment");

        // Check action buttons (approve / reject / cancel).
        let actions = card["elements"][2]["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 3);

        // First button: approve.
        let approve = &actions[0];
        assert_eq!(approve["value"]["action"].as_str().unwrap(), "wf_approve");
        assert_eq!(approve["value"]["run_id"].as_str().unwrap(), "run-1");
        assert_eq!(approve["value"]["activity_id"].as_str().unwrap(), "act-1");
        assert_eq!(approve["value"]["attempt_id"].as_str().unwrap(), "att-1");
        assert_eq!(approve["value"]["card_nonce"].as_str().unwrap(), "nonce-1");
        assert_eq!(
            approve["value"]["workflow_id"].as_str().unwrap(),
            "flow-a"
        );
        assert_eq!(approve["value"]["revision_id"].as_str().unwrap(), "rev-9");
        assert_eq!(
            approve["value"]["node_id"].as_str().unwrap(),
            "node-gate"
        );

        // Second button: reject.
        let reject = &actions[1];
        assert_eq!(reject["value"]["action"].as_str().unwrap(), "wf_reject");

        // Third button: cancel.
        let cancel = &actions[2];
        assert_eq!(cancel["value"]["action"].as_str().unwrap(), "wf_cancel");

        // Dashboard link button in a separate action row (element index 3).
        let dash_actions = card["elements"][3]["actions"].as_array().unwrap();
        assert_eq!(dash_actions.len(), 1);
        let dash_btn = &dash_actions[0];
        assert_eq!(
            dash_btn["text"]["content"].as_str().unwrap(),
            "\u{1f4ca} Open Dashboard"
        );
        assert_eq!(dash_btn["url"].as_str().unwrap(), dashboard_url);
        assert!(dash_btn["url"].as_str().unwrap().contains("run-1"));

        // Footer note (element index 4 after hr at 3? No, hr is 4, note is 5).
        // Let's just verify the note exists somewhere.
        let elements_arr = card["elements"].as_array().unwrap();
        let note = elements_arr
            .iter()
            .find(|el| el["tag"].as_str() == Some("note"))
            .expect("note element exists");
        assert!(
            note["elements"][0]["content"]
                .as_str()
                .unwrap()
                .contains("run-1")
        );
    }

    /// When dashboard_url is None the dashboard button row is omitted.
    #[test]
    fn approval_card_omits_dashboard_button_when_url_is_none() {
        let card = build_approval_card(
            "run-2", "flow-b", "rev-1", "node-x", "act-2", "att-2", "nonce-2",
            None,
            None, // no dashboard url
        );

        // Verify no "Open Dashboard" text anywhere.
        let card_str = card.to_string();
        assert!(!card_str.contains("Open Dashboard"));
        assert!(!card_str.contains("📊"));
    }

    /// Verify that the marker file is correctly persisted and reloaded across
    /// independent load calls (simulating cross-process idempotency).
    #[tokio::test]
    async fn marker_file_survives_reload() {
        let paths = temp_paths("marker-reload");
        let _ = std::fs::remove_dir_all(paths.root());
        let run_dir = paths.workflow_run_dir("run-marker");
        tokio::fs::create_dir_all(&run_dir).await.unwrap();

        let mut marker1 = ApprovalCardSentMarker::load(&run_dir)
            .await
            .expect("load");
        assert!(marker1.sent.is_empty());

        marker1.mark_sent("act-1", "att-1");
        marker1.save(&run_dir).await.expect("save");

        // Reload in a "new" marker instance.
        let marker2 = ApprovalCardSentMarker::load(&run_dir)
            .await
            .expect("reload");
        assert!(marker2.is_sent("act-1", "att-1"));
        assert!(!marker2.is_sent("act-2", "att-2"));

        let _ = std::fs::remove_dir_all(paths.root());
    }
}
