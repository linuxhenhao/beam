use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    middleware,
};
use beam_core::BeamPaths;
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use super::AppState;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WebhookTriggerRecord {
    pub(crate) workflow_id: String,
    pub(crate) created_at: String,
    pub(crate) secret_valid: bool,
    pub(crate) request_body: Value,
    pub(crate) run_id: Option<String>,
    pub(crate) workflow_run_id: Option<String>,
    pub(crate) status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObservedBotRecord {
    pub(crate) open_id: String,
    pub(crate) name: String,
    pub(crate) source: String,
    pub(crate) first_seen_at: u64,
    pub(crate) last_seen_at: u64,
}

pub(crate) async fn read_webhook_trigger_records(
    paths: &BeamPaths,
) -> Result<Vec<WebhookTriggerRecord>> {
    match tokio::fs::read_to_string(paths.webhook_triggers_json()).await {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
pub(crate) fn write_webhook_trigger_records(
    paths: &BeamPaths,
    records: &[WebhookTriggerRecord],
) -> Result<()> {
    if let Some(parent) = paths.webhook_triggers_json().parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        paths.webhook_triggers_json(),
        serde_json::to_string_pretty(records)? + "\n",
    )?;
    Ok(())
}

fn observed_bots_path(paths: &BeamPaths, lark_app_id: &str, chat_id: &str) -> PathBuf {
    paths
        .observed_bots_dir()
        .join(format!("observed-bots-{}-{}.json", lark_app_id, chat_id))
}

fn read_observed_bot_records(
    paths: &BeamPaths,
    lark_app_id: &str,
    chat_id: &str,
) -> Result<Vec<ObservedBotRecord>> {
    match fs::read_to_string(observed_bots_path(paths, lark_app_id, chat_id)) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

fn write_observed_bot_records(
    paths: &BeamPaths,
    lark_app_id: &str,
    chat_id: &str,
    records: &[ObservedBotRecord],
) -> Result<()> {
    let dir = paths.observed_bots_dir();
    fs::create_dir_all(&dir)?;
    fs::write(
        observed_bots_path(paths, lark_app_id, chat_id),
        serde_json::to_string_pretty(records)? + "\n",
    )?;
    Ok(())
}

pub(crate) fn record_observed_bots(
    paths: &BeamPaths,
    lark_app_id: &str,
    chat_id: &str,
    bots: &[(String, String)],
    source: &str,
) -> Result<()> {
    let now = Utc::now().timestamp_millis().max(0) as u64;
    let mut records = read_observed_bot_records(paths, lark_app_id, chat_id)?;
    let mut changed = false;
    for (open_id, name) in bots
        .iter()
        .filter(|(open_id, name)| !open_id.trim().is_empty() && !name.trim().is_empty())
    {
        let open_id = open_id.trim().to_string();
        let name = name.trim().to_string();
        if let Some(existing) = records.iter_mut().find(|entry| entry.open_id == open_id) {
            existing.name = name;
            existing.last_seen_at = now;
        } else {
            records.push(ObservedBotRecord {
                open_id,
                name,
                source: source.to_string(),
                first_seen_at: now,
                last_seen_at: now,
            });
        }
        changed = true;
    }
    if changed {
        write_observed_bot_records(paths, lark_app_id, chat_id, &records)?;
    }
    Ok(())
}

pub(crate) fn load_observed_bot_open_ids_for_app(
    paths: &BeamPaths,
    lark_app_id: &str,
) -> HashSet<String> {
    let mut out = HashSet::new();
    let dir = paths.observed_bots_dir();
    let prefix = format!("observed-bots-{}-", lark_app_id);
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !file_name.starts_with(&prefix) || !file_name.ends_with(".json") {
            continue;
        }
        if let Ok(raw) = fs::read_to_string(&path) {
            if let Ok(records) = serde_json::from_str::<Vec<ObservedBotRecord>>(&raw) {
                for record in records {
                    if !record.open_id.trim().is_empty() {
                        out.insert(record.open_id);
                    }
                }
            }
        }
    }
    out
}

pub(crate) fn load_observed_bots_for_chat(
    paths: &BeamPaths,
    lark_app_id: &str,
    chat_id: &str,
) -> Vec<super::prompt::ObservedBot> {
    read_observed_bot_records(paths, lark_app_id, chat_id)
        .map(|records| {
            records
                .into_iter()
                .map(|r| super::prompt::ObservedBot {
                    open_id: r.open_id,
                    name: r.name,
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn mint_dashboard_token() -> String {
    Uuid::new_v4().simple().to_string()
}

pub(crate) async fn dashboard_token_is_valid(state: &AppState, token: &str) -> bool {
    if token.trim().is_empty() {
        return false;
    }
    let now = Instant::now();
    let guard = state.dashboard_token.lock().await;
    guard
        .as_ref()
        .map(|entry| entry.token == token && entry.expires_at > now)
        .unwrap_or(false)
}

pub(crate) fn extract_dashboard_token(
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Option<String> {
    if let Some(value) = query_token {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    if let Some(value) = headers
        .get("x-dashboard-token")
        .and_then(|v| v.to_str().ok())
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    if let Some(value) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        let trimmed = value.trim();
        if let Some(rest) = trimmed.strip_prefix("Bearer ") {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }

    if let Some(cookie) = headers.get("cookie").and_then(|v| v.to_str().ok()) {
        for part in cookie.split(';') {
            let mut kv = part.trim().splitn(2, '=');
            let key = kv.next().unwrap_or("").trim();
            let value = kv.next().unwrap_or("").trim();
            if key == "beam-dashboard-token" && !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }

    None
}

pub(crate) async fn require_dashboard_access(
    state: &AppState,
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Result<(), (StatusCode, String)> {
    let Some(token) = extract_dashboard_token(headers, query_token) else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "dashboard token required".to_string(),
        ));
    };
    if dashboard_token_is_valid(state, &token).await {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            "dashboard token expired".to_string(),
        ))
    }
}

pub(crate) async fn dashboard_gate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Result<axum::response::Response, (StatusCode, String)> {
    let path = request.uri().path().to_string();
    if path == "/api/asks" || path.ends_with("/final-output") {
        return Ok(next.run(request).await);
    }
    let token = query.get("token").map(|s| s.as_str());
    require_dashboard_access(&state, &headers, token).await?;
    Ok(next.run(request).await)
}
