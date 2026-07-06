use std::borrow::ToOwned;

use anyhow::Result;
use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
};
use beam_core::{BotConfig, Session, SessionScope};
use serde_json::Value;

use crate::{AppState, internal_error, lark_base_url, lark_tenant_token};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct LarkHistoryMessage {
    #[serde(rename = "messageId")]
    pub(crate) message_id: String,
    #[serde(rename = "rootId", skip_serializing_if = "Option::is_none")]
    pub(crate) root_id: Option<String>,
    #[serde(rename = "threadId", skip_serializing_if = "Option::is_none")]
    pub(crate) thread_id: Option<String>,
    #[serde(rename = "chatId", skip_serializing_if = "Option::is_none")]
    pub(crate) chat_id: Option<String>,
    #[serde(rename = "senderId", skip_serializing_if = "Option::is_none")]
    pub(crate) sender_id: Option<String>,
    #[serde(rename = "senderType", skip_serializing_if = "Option::is_none")]
    pub(crate) sender_type: Option<String>,
    #[serde(rename = "msgType")]
    pub(crate) msg_type: String,
    pub(crate) content: String,
    #[serde(rename = "createTime", skip_serializing_if = "Option::is_none")]
    pub(crate) create_time: Option<i64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct LarkHistoryQuery {
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) scope: Option<String>,
}

pub(crate) const LARK_HISTORY_DEFAULT_LIMIT: usize = 50;
pub(crate) const LARK_HISTORY_MAX_LIMIT: usize = 200;
pub(crate) const LARK_MESSAGE_LIST_MAX_PAGE: usize = 50;

pub(crate) fn clamp_lark_history_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(LARK_HISTORY_DEFAULT_LIMIT)
        .clamp(1, LARK_HISTORY_MAX_LIMIT)
}

fn looks_like_lark_message_id(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty() && !trimmed.starts_with("omt_")
}

pub(crate) fn effective_history_scope(
    session: &Session,
    requested: Option<&str>,
) -> Result<&'static str, (StatusCode, String)> {
    let requested = requested.unwrap_or("session").trim();
    let requested = if requested.is_empty() {
        "session"
    } else {
        requested
    };
    let effective = match requested {
        "session" => match session.scope {
            SessionScope::Chat => "chat",
            SessionScope::Thread => "thread",
        },
        "thread" => "thread",
        "chat" => "chat",
        "ambient" => "ambient",
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid scope: {other}; expected session|thread|chat|ambient"),
            ));
        }
    };
    if session.scope == SessionScope::Chat && matches!(effective, "thread" | "ambient") {
        return Err((
            StatusCode::BAD_REQUEST,
            "current session is chat-scope; use --scope chat".to_string(),
        ));
    }
    Ok(effective)
}

pub(crate) fn parse_lark_history_message(item: &Value) -> LarkHistoryMessage {
    let msg_type = item
        .get("msg_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let raw_content = item
        .pointer("/body/content")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let content = render_lark_message_content(&msg_type, raw_content);
    let sender = item.get("sender");
    LarkHistoryMessage {
        message_id: item
            .get("message_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        root_id: item
            .get("root_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        thread_id: item
            .get("thread_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        chat_id: item
            .get("chat_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        sender_id: sender
            .and_then(|s| s.get("id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        sender_type: sender
            .and_then(|s| s.get("sender_type"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        msg_type,
        content,
        create_time: item
            .get("create_time")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<i64>().ok()),
    }
}

fn render_lark_message_content(msg_type: &str, raw_content: &str) -> String {
    if raw_content.trim().is_empty() {
        return String::new();
    }
    let Ok(value) = serde_json::from_str::<Value>(raw_content) else {
        return raw_content.to_string();
    };
    match msg_type {
        "text" => value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or(raw_content)
            .to_string(),
        "post" => value
            .get("content")
            .map(render_lark_post_content)
            .filter(|text| !text.trim().is_empty())
            .unwrap_or_else(|| raw_content.to_string()),
        "interactive" => value
            .get("elements")
            .or_else(|| value.get("body").and_then(|body| body.get("elements")))
            .map(render_lark_post_content)
            .filter(|text| !text.trim().is_empty())
            .unwrap_or_else(|| value.to_string()),
        _ => value.to_string(),
    }
}

fn render_lark_post_content(value: &Value) -> String {
    fn collect(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(text) => {
                if !text.trim().is_empty() {
                    out.push(text.to_string());
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect(item, out);
                }
            }
            Value::Object(map) => {
                for key in ["text", "content"] {
                    if let Some(child) = map.get(key) {
                        collect(child, out);
                    }
                }
                for key in ["elements", "fields", "columns"] {
                    if let Some(child) = map.get(key) {
                        collect(child, out);
                    }
                }
            }
            _ => {}
        }
    }
    let mut parts = Vec::new();
    collect(value, &mut parts);
    parts.join(" ")
}

async fn lark_get_json(
    state: &AppState,
    bot: &BotConfig,
    path: &str,
    query: &[(&str, String)],
) -> Result<Value> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .get(format!("{}{}", lark_base_url(), path))
        .bearer_auth(token)
        .query(query)
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("lark get failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    if value.get("code").and_then(Value::as_i64).unwrap_or(0) != 0 {
        anyhow::bail!(
            "lark get failed: {} (code: {})",
            value
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            value.get("code").and_then(Value::as_i64).unwrap_or(-1)
        );
    }
    Ok(value)
}

async fn lark_get_message_detail(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    user_card_content: bool,
) -> Result<Value> {
    let mut query = Vec::new();
    if user_card_content {
        query.push(("card_msg_content_type", "user_card_content".to_string()));
    }
    lark_get_json(state, bot, &format!("/im/v1/messages/{message_id}"), &query).await
}

async fn resolve_lark_thread_id(
    state: &AppState,
    bot: &BotConfig,
    root_message_id: &str,
) -> Result<Option<String>> {
    if !looks_like_lark_message_id(root_message_id) {
        return Ok(None);
    }
    let detail = lark_get_message_detail(state, bot, root_message_id, false).await?;
    Ok(detail
        .pointer("/data/items/0/thread_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned))
}

async fn lark_list_messages(
    state: &AppState,
    bot: &BotConfig,
    container_type: &str,
    container_id: &str,
    limit: usize,
    sort_type: &str,
) -> Result<Vec<Value>> {
    let mut items = Vec::new();
    let mut page_token: Option<String> = None;
    while items.len() < limit {
        let remaining = limit - items.len();
        let mut query = vec![
            ("container_id_type", container_type.to_string()),
            ("container_id", container_id.to_string()),
            (
                "page_size",
                remaining.min(LARK_MESSAGE_LIST_MAX_PAGE).to_string(),
            ),
            ("sort_type", sort_type.to_string()),
        ];
        if let Some(token) = page_token.as_ref() {
            query.push(("page_token", token.clone()));
        }
        let value = lark_get_json(state, bot, "/im/v1/messages", &query).await?;
        if let Some(page_items) = value.pointer("/data/items").and_then(Value::as_array) {
            items.extend(page_items.iter().cloned());
        }
        page_token = value
            .pointer("/data/page_token")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if page_token.as_deref().unwrap_or("").is_empty() {
            break;
        }
    }
    items.truncate(limit);
    Ok(items)
}

pub(crate) async fn lark_list_chat_history(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    limit: usize,
) -> Result<Vec<Value>> {
    let mut items =
        lark_list_messages(state, bot, "chat", chat_id, limit, "ByCreateTimeDesc").await?;
    items.reverse();
    Ok(items)
}

pub(crate) async fn lark_list_thread_history(
    state: &AppState,
    bot: &BotConfig,
    session: &Session,
    limit: usize,
) -> Result<Vec<Value>> {
    if let Some(thread_id) = session
        .thread_id
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        return lark_list_messages(state, bot, "thread", thread_id, limit, "ByCreateTimeAsc").await;
    }
    if let Some(thread_id) = resolve_lark_thread_id(state, bot, &session.root_message_id).await? {
        return lark_list_messages(state, bot, "thread", &thread_id, limit, "ByCreateTimeAsc")
            .await;
    }
    if !looks_like_lark_message_id(&session.root_message_id) {
        anyhow::bail!("thread history requires session.thread_id or a root message id");
    }
    let scan_limit = (limit * 4).max(50).min(LARK_HISTORY_MAX_LIMIT);
    let scanned = lark_list_chat_history(state, bot, &session.chat_id, scan_limit).await?;
    Ok(scanned
        .into_iter()
        .filter(|item| {
            item.get("message_id").and_then(Value::as_str) == Some(session.root_message_id.as_str())
                || item.get("root_id").and_then(Value::as_str)
                    == Some(session.root_message_id.as_str())
        })
        .rev()
        .take(limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect())
}

pub(crate) async fn lark_list_ambient_history(
    state: &AppState,
    bot: &BotConfig,
    session: &Session,
    limit: usize,
) -> Result<Vec<Value>> {
    let scan_limit = (limit * 4).max(50).min(LARK_HISTORY_MAX_LIMIT);
    let before_ms = if looks_like_lark_message_id(&session.root_message_id) {
        lark_get_message_detail(state, bot, &session.root_message_id, false)
            .await
            .ok()
            .and_then(|detail| {
                detail
                    .pointer("/data/items/0/create_time")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse::<i64>().ok())
            })
    } else {
        None
    };
    let root = session.root_message_id.as_str();
    let thread = session.thread_id.as_deref();
    let filtered = lark_list_chat_history(state, bot, &session.chat_id, scan_limit)
        .await?
        .into_iter()
        .filter(|item| {
            if item.get("message_id").and_then(Value::as_str) == Some(root)
                || item.get("root_id").and_then(Value::as_str) == Some(root)
            {
                return false;
            }
            if thread.is_some() && item.get("thread_id").and_then(Value::as_str) == thread {
                return false;
            }
            if let Some(before_ms) = before_ms {
                if item
                    .get("create_time")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse::<i64>().ok())
                    .is_some_and(|created| created >= before_ms)
                {
                    return false;
                }
            }
            true
        })
        .collect::<Vec<_>>();
    Ok(filtered
        .into_iter()
        .rev()
        .take(limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect())
}

pub(crate) async fn session_history(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Query(query): Query<LarkHistoryQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let limit = clamp_lark_history_limit(query.limit);
    let session = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?
    };
    let scope = effective_history_scope(&session, query.scope.as_deref())?;
    let bot = state
        .bots
        .get(&session.lark_app_id)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "session bot not found".to_string()))?;
    let raw = match scope {
        "chat" => lark_list_chat_history(&state, bot, &session.chat_id, limit).await,
        "thread" => lark_list_thread_history(&state, bot, &session, limit).await,
        "ambient" => lark_list_ambient_history(&state, bot, &session, limit).await,
        _ => unreachable!(),
    }
    .map_err(internal_error)?;
    let messages = raw
        .iter()
        .map(parse_lark_history_message)
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!({
        "sessionId": session.session_id,
        "chatId": session.chat_id,
        "scope": scope,
        "sessionScope": match session.scope { SessionScope::Chat => "chat", SessionScope::Thread => "thread" },
        "rootMessageId": session.root_message_id,
        "threadId": session.thread_id,
        "messages": messages,
        "total": messages.len(),
    })))
}

pub(crate) async fn quoted_message(
    State(state): State<AppState>,
    AxumPath((session_id, message_id)): AxumPath<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let session = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?
    };
    let bot = state
        .bots
        .get(&session.lark_app_id)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "session bot not found".to_string()))?;
    let detail = lark_get_message_detail(&state, bot, &message_id, true)
        .await
        .map_err(internal_error)?;
    let item = detail
        .pointer("/data/items/0")
        .ok_or_else(|| (StatusCode::NOT_FOUND, "message not found".to_string()))?;
    Ok(Json(serde_json::json!({
        "sessionId": session.session_id,
        "message": parse_lark_history_message(item),
    })))
}
