use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use axum::{Json, extract::State, http::StatusCode};
use beam_core::{AskQuestion, AskRequest, AskResult};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    AppState, build_lark_card_action_toast, internal_error, lark_reply_card, send_lark_card_in_chat,
};

#[derive(Debug)]
pub(crate) struct AskPendingEntry {
    request: AskRequest,
    nonce: String,
    selections: Vec<HashSet<String>>,
    card_message_id: Option<String>,
    tx: Option<oneshot::Sender<AskResult>>,
    pub created_at_ms: i64,
}

/// Serializable snapshot of an ask pending entry (without the oneshot channel).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AskPendingSnapshot {
    pub ask_id: String,
    pub request: AskRequest,
    pub nonce: String,
    pub selections: Vec<HashSet<String>>,
    pub card_message_id: Option<String>,
    pub created_at_ms: i64,
}

/// TTL for ask pending entries (30 minutes in milliseconds).
pub const ASK_PENDING_TTL_MS: i64 = 30 * 60 * 1000;

/// Load ask pending entries from disk, pruning expired ones.
pub(crate) async fn load_ask_pending(
    paths: &beam_core::BeamPaths,
) -> std::collections::HashMap<String, AskPendingEntry> {
    let path = paths.ask_pending_json();
    let snapshots: Vec<AskPendingSnapshot> = match beam_core::persist::read_json(&path) {
        Ok(Some(snaps)) => snaps,
        _ => return Default::default(),
    };
    let total_loaded = snapshots.len();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut map = std::collections::HashMap::new();
    let mut retained = Vec::new();
    for snap in &snapshots {
        if now_ms - snap.created_at_ms > ASK_PENDING_TTL_MS {
            continue;
        }
        retained.push(snap.clone());
        map.insert(
            snap.ask_id.clone(),
            AskPendingEntry {
                request: snap.request.clone(),
                nonce: snap.nonce.clone(),
                selections: snap.selections.clone(),
                card_message_id: snap.card_message_id.clone(),
                tx: None, // oneshot channels can't survive restarts
                created_at_ms: snap.created_at_ms,
            },
        );
    }
    // Prune expired entries by rewriting the file
    if retained.len() < total_loaded {
        if retained.is_empty() {
            let _ = tokio::fs::remove_file(&path).await;
        } else {
            let path_clone = path.clone();
            let _ = tokio::task::spawn_blocking(move || {
                beam_core::persist::atomic_write_json(&path_clone, &retained)
            })
            .await;
        }
    }
    map
}

/// Save ask pending entries to disk (from a reference, without cloning).
async fn persist_ask_pending_now(
    paths: &beam_core::BeamPaths,
    map: &std::collections::HashMap<String, AskPendingEntry>,
) {
    let snapshots: Vec<AskPendingSnapshot> = map
        .iter()
        .map(|(ask_id, entry)| AskPendingSnapshot {
            ask_id: ask_id.clone(),
            request: entry.request.clone(),
            nonce: entry.nonce.clone(),
            selections: entry.selections.clone(),
            card_message_id: entry.card_message_id.clone(),
            created_at_ms: entry.created_at_ms,
        })
        .collect();
    let path = paths.ask_pending_json();
    if snapshots.is_empty() {
        let _ = tokio::fs::remove_file(&path).await;
        return;
    }
    let _ = tokio::task::spawn_blocking(move || {
        beam_core::persist::atomic_write_json(&path, &snapshots)
    })
    .await;
}

#[derive(Debug, Clone, serde::Deserialize)]
struct AskRequestBody {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "chatId")]
    chat_id: String,
    #[serde(rename = "larkAppId")]
    lark_app_id: String,
    #[serde(rename = "rootMessageId")]
    root_message_id: Option<String>,
    questions: Vec<AskQuestion>,
    #[serde(rename = "timeoutMs")]
    timeout_ms: u64,
    approvers: Vec<String>,
}

pub async fn create_ask(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let req = parse_ask_request(body)?;
    let request = AskRequest {
        session_id: req.session_id.clone(),
        chat_id: req.chat_id.clone(),
        lark_app_id: req.lark_app_id.clone(),
        root_message_id: req.root_message_id.clone(),
        questions: req.questions.clone(),
        timeout_ms: req.timeout_ms,
        approvers: resolve_ask_approvers(&state, &req).await,
    };
    info!(
        session_id = %request.session_id,
        chat_id = %request.chat_id,
        lark_app_id = %request.lark_app_id,
        question_count = request.questions.len(),
        approver_count = request.approvers.len(),
        "ask request accepted"
    );
    if request.questions.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "questions must not be empty".to_string(),
        ));
    }
    if request.approvers.is_empty() {
        warn!(
            session_id = %request.session_id,
            chat_id = %request.chat_id,
            lark_app_id = %request.lark_app_id,
            "ask request rejected: no approvers available"
        );
        return Err((StatusCode::FORBIDDEN, "no approvers available".to_string()));
    }

    let ask_id = Uuid::new_v4().simple().to_string();
    let nonce = Uuid::new_v4().simple().to_string()[..8].to_string();
    let selections = request
        .questions
        .iter()
        .map(|_| HashSet::new())
        .collect::<Vec<_>>();
    let (tx, rx) = oneshot::channel();
    let entry = AskPendingEntry {
        request: request.clone(),
        nonce: nonce.clone(),
        selections,
        card_message_id: None,
        tx: Some(tx),
        created_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    {
        let mut pending = state.ask_pending.lock().await;
        pending.insert(ask_id.clone(), entry);
        drop(pending);
        let pending = state.ask_pending.lock().await;
        persist_ask_pending_now(&state.paths, &pending).await;
    }

    let card = build_ask_card(&ask_id, &nonce, &request.questions, &[], false, None);
    let message_id = if let Some(root_message_id) = request
        .root_message_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let bot = state
            .bots
            .get(&request.lark_app_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;
        lark_reply_card(&state, &bot, root_message_id, &card)
            .await
            .map_err(internal_error)?
    } else {
        let bot = state
            .bots
            .get(&request.lark_app_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;
        send_lark_card_in_chat(&state, &bot, &request.chat_id, &card)
            .await
            .map_err(internal_error)?
    };
    info!(
        ask_id = %ask_id,
        message_id = %message_id,
        card_message_id = %message_id,
        "ask card sent"
    );
    {
        let mut pending = state.ask_pending.lock().await;
        if let Some(entry) = pending.get_mut(&ask_id) {
            entry.card_message_id = Some(message_id.clone());
        }
    }

    let result = match tokio::time::timeout(Duration::from_millis(request.timeout_ms), rx).await {
        Ok(Ok(answer)) => answer,
        _ => {
            warn!(ask_id = %ask_id, "ask timed out");
            let mut pending = state.ask_pending.lock().await;
            pending.remove(&ask_id);
            drop(pending);
            let pending = state.ask_pending.lock().await;
            persist_ask_pending_now(&state.paths, &pending).await;
            AskResult::TimedOut {
                selected: None,
                by: None,
                comment: None,
                timed_out: true,
            }
        }
    };

    Ok(Json(serde_json::to_value(result).map_err(internal_error)?))
}

fn parse_ask_request(body: serde_json::Value) -> Result<AskRequestBody, (StatusCode, String)> {
    let req: AskRequestBody = serde_json::from_value(body).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid ask body: {}", err),
        )
    })?;
    if req.session_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bad_sessionId".to_string()));
    }
    if req.chat_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bad_chatId".to_string()));
    }
    if req.lark_app_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bad_larkAppId".to_string()));
    }
    if req.timeout_ms < 1000 {
        return Err((StatusCode::BAD_REQUEST, "bad_timeoutMs".to_string()));
    }
    if req.questions.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bad_questions".to_string()));
    }
    Ok(req)
}

async fn resolve_ask_approvers(state: &AppState, req: &AskRequestBody) -> HashSet<String> {
    let explicit: HashSet<String> = req
        .approvers
        .iter()
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .collect();
    if !explicit.is_empty() {
        info!(
            session_id = %req.session_id,
            approver_count = explicit.len(),
            "ask approvers resolved from explicit request"
        );
        return explicit;
    }
    let session_owner = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&req.session_id)
            .and_then(|s| s.owner_open_id.clone())
    };
    if let Some(owner) = session_owner {
        info!(
            session_id = %req.session_id,
            owner_open_id = %owner,
            "ask approvers resolved from session owner"
        );
        return HashSet::from([owner]);
    }
    let bot = state.bots.get(&req.lark_app_id);
    let resolved: HashSet<String> = bot
        .map(|b| b.allowed_users.iter().cloned().collect())
        .unwrap_or_default();
    info!(
        session_id = %req.session_id,
        approver_count = resolved.len(),
        "ask approvers resolved from bot allowlist"
    );
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use beam_core::{BeamPaths, BotConfig, Session, SessionScope, SessionStatus};
    use chrono::Utc;
    use tokio::sync::Mutex;

    fn make_state(session_owner: Option<&str>, allowed_users: Vec<&str>) -> AppState {
        let paths = BeamPaths::from_root(
            std::env::temp_dir().join(format!("beam-ask-approvers-{}", std::process::id())),
        );
        let mut sessions = HashMap::new();
        if let Some(owner_open_id) = session_owner {
            sessions.insert(
                "sess-1".to_string(),
                Session {
                    session_id: "sess-1".to_string(),
                    title: "test".to_string(),
                    chat_id: "chat-1".to_string(),
                    root_message_id: "root-1".to_string(),
                    chat_type: Some("p2p".to_string()),
                    quote_target_id: None,
                    scope: SessionScope::Thread,
                    status: SessionStatus::Active,
                    created_at: Utc::now(),
                    closed_at: None,
                    working_dir: Some("/tmp".to_string()),
                    lark_app_id: "app-1".to_string(),
                    owner_open_id: Some(owner_open_id.to_string()),
                    worker_pid: None,
                    cli_id: Some("opencode".to_string()),
                    cli_bin: Some("opencode-cli".to_string()),
                    cli_args: Vec::new(),
                    cli_session_id: None,
                    last_cli_input: None,
                    stream_card_id: None,
                    stream_card_nonce: None,
                    display_mode: None,
                    current_screen: None,
                    last_screen_status: None,
                    usage_limit: None,
                    current_image_key: None,
                    tui_prompt_card_id: None,
                    tui_prompt_options: Vec::new(),
                    tui_prompt_multi_select: None,
                    tui_toggled_indices: Vec::new(),
                    pending_response_card_id: None,
                    pending_response_card_state: None,
                    last_patched_response_card_id: None,
                    terminal_url: None,
                    last_final_output_turn_id: None,
                    last_final_output: None,
                    adopted_from: None,
                    model: None,
                    locale: None,
                    bot_name: None,
                    bot_open_id: None,
                    resume_session_id: None,
                    disable_cli_bypass: false,
                    initial_prompt: None,
                    thread_id: Some("omt_1".to_string()),
                },
            );
        }

        let bot = BotConfig {
            name: None,
            lark_app_id: "app-1".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "opencode".to_string(),
            cli_bin: Some("opencode-cli".to_string()),
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: Some("~".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: allowed_users.into_iter().map(String::from).collect(),
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: HashMap::new(),
        };

        let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        AppState {
            paths,
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(sessions)),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
            options: crate::RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: reqwest::Client::new(),
            config: beam_core::Config::default(),
            bots: Arc::new(HashMap::from([(bot.lark_app_id.clone(), bot)])),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        }
    }

    #[tokio::test]
    async fn resolve_ask_approvers_defaults_to_session_owner_when_allowlist_empty() {
        let state = make_state(Some("ou_owner"), vec![]);
        let req = AskRequestBody {
            session_id: "sess-1".to_string(),
            chat_id: "chat-1".to_string(),
            lark_app_id: "app-1".to_string(),
            root_message_id: None,
            questions: vec![AskQuestion {
                prompt: "pick one".to_string(),
                options: vec![],
                multi_select: false,
            }],
            timeout_ms: 10_000,
            approvers: vec![],
        };

        let approvers = resolve_ask_approvers(&state, &req).await;
        assert_eq!(approvers, HashSet::from(["ou_owner".to_string()]));
    }

    #[tokio::test]
    async fn resolve_ask_approvers_uses_explicit_approvers_first() {
        let state = make_state(Some("ou_owner"), vec!["ou_allowed"]);
        let req = AskRequestBody {
            session_id: "sess-1".to_string(),
            chat_id: "chat-1".to_string(),
            lark_app_id: "app-1".to_string(),
            root_message_id: None,
            questions: vec![AskQuestion {
                prompt: "pick one".to_string(),
                options: vec![],
                multi_select: false,
            }],
            timeout_ms: 10_000,
            approvers: vec!["ou_explicit".to_string()],
        };

        let approvers = resolve_ask_approvers(&state, &req).await;
        assert_eq!(approvers, HashSet::from(["ou_explicit".to_string()]));
    }
}

fn build_ask_card(
    ask_id: &str,
    nonce: &str,
    questions: &[AskQuestion],
    selections: &[HashSet<String>],
    settled: bool,
    settled_text: Option<&str>,
) -> String {
    let mut elements = Vec::new();
    if settled {
        elements.push(serde_json::json!({
            "tag": "markdown",
            "content": settled_text.unwrap_or("ask resolved"),
        }));
    }
    for (idx, question) in questions.iter().enumerate() {
        elements.push(serde_json::json!({
            "tag": "markdown",
            "content": format!("**{}**", question.prompt),
        }));
        let mut buttons = Vec::new();
        let selected = selections.get(idx);
        for option in &question.options {
            let checked = selected
                .map(|set| set.contains(&option.key))
                .unwrap_or(false);
            buttons.push(serde_json::json!({
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": if checked {
                        format!("✓ {}", option.label)
                    } else {
                        option.label.clone()
                    }
                },
                "type": "default",
                "value": {
                    "action": "ask_toggle",
                    "ask_id": ask_id,
                    "nonce": nonce,
                    "question_index": idx,
                    "key": option.key,
                }
            }));
        }
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": buttons,
        }));
    }
    if !settled {
        elements.push(serde_json::json!({
            "tag": "action",
            "actions": [{
                "tag": "button",
                "text": { "tag": "plain_text", "content": "Submit" },
                "type": "primary",
                "value": {
                    "action": "ask_submit",
                    "ask_id": ask_id,
                    "nonce": nonce,
                }
            }],
        }));
    }
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "header": {
            "template": if settled { "green" } else { "blue" },
            "title": {
                "tag": "plain_text",
                "content": if settled { "Ask answered" } else { "Ask question" },
            },
        },
        "elements": elements,
    })
    .to_string()
}

pub async fn handle_ask_card_action(
    state: &AppState,
    _app_id: &str,
    action: &crate::ParsedLarkCardAction,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let ask_id = action.ask_id.clone().unwrap_or_default();
    let nonce = action.ask_nonce.clone().unwrap_or_default();
    if ask_id.trim().is_empty() || nonce.trim().is_empty() {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing ask id",
        )));
    }
    let mut pending = state.ask_pending.lock().await;
    let Some(entry) = pending.get_mut(&ask_id) else {
        return Ok(Json(build_lark_card_action_toast("info", "ask expired")));
    };
    // If entry was restored from disk, the oneshot channel is gone.
    if entry.tx.is_none() {
        pending.remove(&ask_id);
        drop(pending);
        let pending = state.ask_pending.lock().await;
        persist_ask_pending_now(&state.paths, &pending).await;
        return Ok(Json(build_lark_card_action_toast(
            "info",
            "ask expired (daemon restarted)",
        )));
    }
    if entry.nonce != nonce {
        return Ok(Json(build_lark_card_action_toast("info", "ask expired")));
    }
    if !action
        .operator_open_id
        .as_deref()
        .map(|open_id| entry.request.approvers.contains(open_id))
        .unwrap_or(false)
    {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "permission denied",
        )));
    }

    if action.ask_submit {
        let answers = entry
            .request
            .questions
            .iter()
            .enumerate()
            .map(|(idx, question)| {
                let sel = entry.selections.get(idx).cloned().unwrap_or_default();
                if !question.multi_select && sel.len() != 1 {
                    return Vec::new();
                }
                sel.into_iter().collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let result = AskResult::answered(
            answers.clone(),
            action.operator_open_id.clone().unwrap_or_default(),
        );
        if let Some(tx) = entry.tx.take() {
            let _ = tx.send(result);
        }
        let selections = entry.selections.clone();
        let card = build_ask_card(
            &ask_id,
            &nonce,
            &entry.request.questions,
            &selections,
            true,
            Some("Answer submitted"),
        );
        pending.remove(&ask_id);
        drop(pending);
        let pending = state.ask_pending.lock().await;
        persist_ask_pending_now(&state.paths, &pending).await;
        return Ok(Json(serde_json::json!({
            "toast": { "type": "success", "content": "ask submitted" },
            "card": { "type": "raw", "data": serde_json::from_str::<serde_json::Value>(&card).unwrap_or_else(|_| serde_json::json!({})) }
        })));
    }

    let Some(question_index) = action.ask_question_index else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing question index",
        )));
    };
    let Some(key) = action.ask_key.clone() else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "missing ask key",
        )));
    };
    let Some(question) = entry.request.questions.get(question_index) else {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "invalid ask question",
        )));
    };
    if !question.options.iter().any(|option| option.key == key) {
        return Ok(Json(build_lark_card_action_toast(
            "error",
            "invalid ask option",
        )));
    }

    let current = entry.selections.get_mut(question_index).unwrap();
    if question.multi_select {
        if current.contains(&key) {
            current.remove(&key);
        } else {
            current.insert(key);
        }
    } else {
        current.clear();
        current.insert(key);
    }

    let card = build_ask_card(
        &ask_id,
        &nonce,
        &entry.request.questions,
        &entry.selections,
        false,
        None,
    );
    let card_json =
        serde_json::from_str::<serde_json::Value>(&card).unwrap_or_else(|_| serde_json::json!({}));
    // Persist updated selections after toggle: drop lock, then re-acquire read-only to save
    drop(pending);
    {
        let pending = state.ask_pending.lock().await;
        persist_ask_pending_now(&state.paths, &pending).await;
    }
    Ok(Json(serde_json::json!({
        "toast": { "type": "success", "content": "selection updated" },
        "card": { "type": "raw", "data": card_json }
    })))
}
