use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use axum::{Json, extract::State, http::StatusCode};
use beam_core::{AskQuestion, AskRequest, AskResult};
use tokio::sync::oneshot;
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
    if request.questions.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "questions must not be empty".to_string(),
        ));
    }
    if request.approvers.is_empty() {
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
    };
    {
        let mut pending = state.ask_pending.lock().await;
        pending.insert(ask_id.clone(), entry);
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
    {
        let mut pending = state.ask_pending.lock().await;
        if let Some(entry) = pending.get_mut(&ask_id) {
            entry.card_message_id = Some(message_id.clone());
        }
    }

    let result = match tokio::time::timeout(Duration::from_millis(request.timeout_ms), rx).await {
        Ok(Ok(answer)) => answer,
        _ => {
            let mut pending = state.ask_pending.lock().await;
            pending.remove(&ask_id);
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
        return explicit;
    }
    let bot = state.bots.get(&req.lark_app_id);
    let allow = bot.map(|b| b.allowed_users.clone()).unwrap_or_default();
    let session_owner = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&req.session_id)
            .and_then(|s| s.owner_open_id.clone())
    };
    if let Some(owner) = session_owner {
        if allow.iter().any(|value| value == &owner) {
            return HashSet::from([owner]);
        }
    }
    allow.into_iter().collect()
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
    Ok(Json(serde_json::json!({
        "toast": { "type": "success", "content": "selection updated" },
        "card": { "type": "raw", "data": card_json }
    })))
}
