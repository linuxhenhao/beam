//! Workflow runtime driver: the single entry point that loads a definition,
//! creates a runtime context, attaches the daemon execution hooks, sends
//! progress cards, fans out approval cards, and calls `run_loop`.
//!
//! This module is extracted from `lib.rs` (Task 7.1) so that trigger, approval,
//! cancel, cold attach, and dashboard resume all go through the same driver.

use beam_core::{
    EventLog, RunChatBinding, RunStatus, WorkflowRuntimeContext, parse_workflow_definition,
    read_run_snapshot, run_loop,
};
use tracing::{info, warn};

use crate::AppState;

const MAX_TICKS: usize = 128;

/// Main entry point: run (or resume) the workflow runtime for `run_id`.
pub(crate) async fn run(state: &AppState, run_id: &str, workflow_json: &str) {
    let def = match parse_workflow_definition(workflow_json) {
        Ok(def) => def,
        Err(err) => {
            warn!(
                "workflow runtime bootstrap parse failed for {}: {}",
                run_id, err
            );
            return;
        }
    };
    let log = match EventLog::new(run_id.to_string(), state.paths.workflow_runs_dir()) {
        Ok(log) => log,
        Err(err) => {
            warn!("workflow runtime log init failed for {}: {}", run_id, err);
            return;
        }
    };
    let mut rt = WorkflowRuntimeContext {
        log,
        def,
        runs_base_dir: state.paths.workflow_runs_dir(),
    };
    let mut hooks = crate::DaemonWorkflowExecutionHooks {
        state: state.clone(),
    };

    send_progress_card(state, run_id, &rt.def.workflow_id).await;

    let watch_state = state.clone();
    let watch_run_id = run_id.to_string();
    let watch_workflow_id = rt.def.workflow_id.clone();
    tokio::spawn(async move {
        let events_path = watch_state
            .paths
            .workflow_run_dir(&watch_run_id)
            .join("events.ndjson");
        let mut last_len = tokio::fs::metadata(&events_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let Ok(meta) = tokio::fs::metadata(&events_path).await else {
                break;
            };
            if meta.len() > last_len {
                last_len = meta.len();
                send_progress_card(&watch_state, &watch_run_id, &watch_workflow_id).await;
                // Fanout approval cards for any newly-created human-gate waits.
                let _ = crate::workflow_event_fanout::fanout_with_lark_sender(
                    &watch_state,
                    &watch_run_id,
                )
                .await;
            }
            let Ok(snapshot) =
                read_run_snapshot(&watch_state.paths.workflow_run_dir(&watch_run_id)).await
            else {
                break;
            };
            if let Some(sn) = snapshot {
                if matches!(
                    sn.run.status,
                    RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
                ) {
                    send_progress_card(&watch_state, &watch_run_id, &watch_workflow_id).await;
                    break;
                }
            }
        }
    });

    match run_loop(&mut rt, &mut hooks, MAX_TICKS, 4).await {
        Ok(result) => {
            info!(
                "workflow runtime finished: {} ticks={} reason={:?}",
                run_id, result.ticks, result.reason
            );
            send_progress_card(state, run_id, &rt.def.workflow_id).await;
        }
        Err(err) => {
            warn!("workflow runtime failed for {}: {}", run_id, err);
        }
    }

    // Fanout approval cards for any human-gate waits created during this
    // runtime tick.  This covers normal advancement, recovery, and resume.
    let _ = crate::workflow_event_fanout::fanout_with_lark_sender(state, run_id).await;
}

/// Send (or update) the persistent progress card for `run_id` in the
/// bound Lark chat.  No-op when no chat binding exists yet.
async fn send_progress_card(state: &AppState, run_id: &str, workflow_id: &str) {
    let run_dir = state.paths.workflow_run_dir(run_id);
    let snapshot = match read_run_snapshot(&run_dir).await {
        Ok(Some(sn)) => sn,
        _ => return,
    };

    let card = crate::workflow_progress_card::build_workflow_progress_card(
        &snapshot,
        run_id,
        workflow_id,
    );

    let binding_path = run_dir.join("chat-binding.json");
    let binding: Option<RunChatBinding> = tokio::fs::read_to_string(&binding_path)
        .await
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok());

    let (chat_id, lark_app_id) = match &binding {
        Some(b) => (b.chat_id.clone(), b.lark_app_id.clone()),
        None => return,
    };

    let bot = match state.bots.get(&lark_app_id) {
        Some(bot) => bot.clone(),
        None => return,
    };

    let card_json = card.to_string();
    let mut card_map = state.workflow_progress_cards.lock().await;
    if let Some(card_msg_id) = card_map.get(run_id) {
        let _ = crate::lark_update_card(state, &bot, card_msg_id, &card_json).await;
    } else {
        if let Ok(msg_id) =
            crate::send_lark_card_in_chat(state, &bot, &chat_id, &card_json).await
        {
            card_map.insert(run_id.to_string(), msg_id);
        }
    }
}
