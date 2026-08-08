use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

mod api_token;
mod ask;
mod card_i18n;
mod connector_runtime;
mod connector_store;
mod daemon_types;
mod dashboard_support;
mod debug_simulate;
mod dir_select;
mod external_host_watcher;
mod final_output;
mod grant;
mod ip_resolver;
mod lark_api_helpers;
mod lark_card_builders;
mod lark_delivery;
mod lark_dispatch;
mod lark_history;
mod lark_identity;
mod lark_ingress;
mod lark_parse;
mod lark_replies;
mod lark_security;
mod lark_session_cards;
mod opencode_adopt_resolver;
mod persistence;
mod prompt;
mod route_handlers;
mod session_cards;
mod session_creation;
mod terminal_auth;
mod terminal_proxy;
mod trigger_log;
mod utils;
mod webhook_key;
mod webhook_lifecycle;
mod worker_lifecycle;
mod workflow_approval_cards;
mod workflow_cancellation;
mod workflow_catalog;
mod workflow_commands;
mod workflow_event_fanout;
mod workflow_execution;
mod workflow_host_executors;
mod workflow_progress_card;
mod workflow_reconcilers;
mod workflow_resume;
mod workflow_runtime_driver;
mod zellij_adopt;
mod zellij_web;

// Re-export daemon types (used across modules and externally)
pub use daemon_types::RunOptions;
pub(crate) use daemon_types::*;

// Re-export workflow catalog items for backward compatibility (used by route handlers and tests)
pub(crate) use workflow_catalog::*;
// Re-export workflow execution items for backward compatibility (used by route handlers and tests)
pub(crate) use workflow_execution::*;
// Re-export workflow resume items for backward compatibility (used by route handlers and tests)
pub(crate) use api_token::*;
pub(crate) use connector_runtime::*;
pub(crate) use external_host_watcher::*;
pub(crate) use final_output::*;
pub(crate) use ip_resolver::*;
pub(crate) use lark_api_helpers::*;
pub(crate) use lark_card_builders::*;
pub(crate) use lark_delivery::*;
pub(crate) use lark_dispatch::*;
#[allow(unused_imports)]
pub(crate) use lark_history::*;
pub(crate) use lark_identity::*;
pub(crate) use lark_ingress::*;
pub(crate) use lark_parse::*;
pub(crate) use lark_replies::*;
pub(crate) use lark_security::*;
pub(crate) use lark_session_cards::*;
pub(crate) use opencode_adopt_resolver::*;
pub(crate) use persistence::*;
pub(crate) use route_handlers::*;
pub(crate) use session_cards::*;
pub(crate) use session_creation::*;
pub(crate) use utils::*;
pub(crate) use worker_lifecycle::*;
pub(crate) use workflow_approval_cards::*;
pub(crate) use workflow_resume::*;
pub(crate) use zellij_adopt::*;

#[cfg(test)]
use dashboard_support::{
    WebhookTriggerRecord, extract_dashboard_token, write_webhook_trigger_records,
};
use dashboard_support::{
    dashboard_gate, dashboard_token_is_valid, load_observed_bot_open_ids_for_app,
    load_observed_bots_for_chat, mint_dashboard_token, read_webhook_trigger_records,
    record_observed_bots,
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    middleware,
    response::{IntoResponse, Redirect},
    routing::{get, get_service, post, put},
};
use base64::Engine;
use beam_core::{
    AdoptedFrom, AgentAttention, ApiHealth, AttemptResumeRequest, AttentionRequest, BeamPaths,
    BotConfig, BotSummary, ChatMode, CliUsageLimitState, ColdWorkflowRun, Config,
    CreateSessionRequest, CustomTrigger, DaemonOverview, DaemonRuntimeState, DaemonToWorker,
    DisplayMode, EventDraft, EventLog, EventWindowOpts, FinalOutputKind, FinalOutputRequest,
    InitConfig, PendingResponseCardState, RestartSessionRequest, ResumeSessionRequest,
    RunChatBinding, RunStatus, ScheduleChatType, ScreenStatus, Session, SessionGroup,
    SessionInputRequest, SessionLocateInfo, SessionScope, SessionStatus, SessionSummary,
    TalkEvaluation, TermActionKey, TranscriptChoice, TuiPromptOption, WaitResolution,
    WorkerToDaemon, WorkflowActor, WorkflowOutputRef, can_operate, evaluate_talk,
    parse_workflow_definition, read_event_window, read_run_events_pure, read_run_snapshot,
    resolve_custom_trigger, resolve_trigger_message, scan_cold_workflow_runs,
};
use chrono::Utc;
use connector_store::{
    ConnectorDefinition, ConnectorLifecycleExtractors, ConnectorLoggingPolicy,
    ConnectorPromptEnvelope, ConnectorRateLimit, ConnectorTarget, ConnectorVerify,
    delete_connector, get_connector, list_connectors, upsert_connector,
};
use feishu_sdk::{
    card::CardAction,
    core as feishu_core,
    event::{
        Event, EventDispatcher, EventDispatcherConfig, EventHandler, EventHandlerResult, EventResp,
    },
    ws::{StreamClient, StreamConfig},
};
use hmac::KeyInit;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tower_http::services::ServeDir;
use tracing::{debug, error, info, warn};
use trigger_log::{
    TriggerLogStats, list_trigger_logs, new_trigger_id as new_trigger_log_id, prune_trigger_logs,
    summarize_trigger_logs,
};
use uuid::Uuid;
use webhook_key::{
    create_webhook_secret, delete_webhook_secret, generate_webhook_secret_plaintext,
    get_webhook_secret, list_webhook_secret_refs, set_webhook_secret,
};
use webhook_lifecycle::{begin_webhook_lifecycle_firing, resolve_webhook_lifecycle_group};

pub async fn run(paths: BeamPaths, options: RunOptions) -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tokio::fs::create_dir_all(paths.run_dir()).await?;
    tokio::fs::create_dir_all(paths.logs_dir()).await?;
    tokio::fs::create_dir_all(paths.sessions_dir()).await?;

    let config = load_config(&paths)?;
    let bots = load_bot_configs(&paths)?;
    let mut sessions = load_sessions(&paths).await?;
    for session in sessions.values_mut() {
        let marker = read_pending_response_patch_marker(&paths, &session.session_id).await?;
        if should_treat_pending_card_as_patched_by_marker(
            session.pending_response_card_id.as_deref(),
            marker.as_ref(),
        ) {
            mark_pending_response_card_patched(session);
            let _ = clear_pending_response_patch_marker(&paths, &session.session_id).await;
        }
    }
    let listener = TcpListener::bind("127.0.0.1:7893").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let external_host = resolve_external_host(&config.web.host);
    let started_at = Utc::now();
    let runtime = DaemonRuntimeState {
        pid: std::process::id(),
        api_addr: addr.to_string(),
        started_at,
        log_path: paths.daemon_log().display().to_string(),
    };
    persist_runtime_state(&paths, &runtime).await?;

    // Load persisted in-memory state that should survive restarts.
    let workflow_progress_cards =
        workflow_runtime_driver::load_workflow_progress_cards(&paths).await;
    info!(
        "loaded {} persisted workflow progress cards",
        workflow_progress_cards.len()
    );

    let ask_pending_map = ask::load_ask_pending(&paths).await;
    info!(
        "loaded {} persisted ask pending entries",
        ask_pending_map.len()
    );

    let grant_pending_map = grant::load_grant_pending(&paths);
    info!(
        "loaded {} persisted grant pending entries",
        grant_pending_map.len()
    );

    let pending_creates_map = dir_select::load_pending_creates(&paths).await;
    info!(
        "loaded {} persisted pending creates",
        pending_creates_map.len()
    );

    // Load persisted recent Lark events (dedupe state).
    let recent_lark_events = load_recent_lark_events(&paths).await;
    info!(
        "loaded {} persisted recent lark events",
        recent_lark_events.len()
    );

    // Load or rotate the local api token (daily rotation, 1h grace for the
    // previous token) before any route can require it.
    let api_token_state = load_or_create_api_token(&paths).await?;

    let state = AppState {
        paths: paths.clone(),
        started_at,
        sessions: Arc::new(Mutex::new(sessions)),
        workers: Arc::new(Mutex::new(HashMap::new())),
        worker_health: Arc::new(Mutex::new(HashMap::new())),
        attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
        shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
        options,
        http: Client::new(),
        config,
        bots: Arc::new(bots),
        lark_tokens: Arc::new(Mutex::new(HashMap::new())),
        chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
        recent_lark_events: Arc::new(Mutex::new(recent_lark_events)),
        inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
        workflow_progress_cards: Arc::new(Mutex::new(workflow_progress_cards)),
        ask_pending: Arc::new(Mutex::new(ask_pending_map)),
        grant_pending: Arc::new(Mutex::new(grant_pending_map)),
        pending_creates: Arc::new(Mutex::new(pending_creates_map)),
        dashboard_token: Arc::new(Mutex::new(None)),
        api_token: Arc::new(RwLock::new(api_token_state)),
        external_host: std::sync::Arc::new(tokio::sync::RwLock::new(external_host)),
    };

    refresh_external_host(&state, true).await?;
    spawn_external_host_watcher(state.clone());
    spawn_api_token_rotator(state.clone());

    spawn_lark_ws_clients(&state);

    // Load replay nonces and rate buckets from disk into static stores.
    {
        let path = paths.replay_nonces_json();
        if let Ok(Some(map)) = beam_core::persist::read_json::<HashMap<String, u64>>(&path) {
            let now = now_ms();
            let mut guard = replay_nonce_store()
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            for (key, expiry) in map {
                if expiry > now {
                    guard.insert(key, expiry);
                }
            }
            info!("loaded {} persisted replay nonces", guard.len());
        }
        let path = paths.rate_buckets_json();
        if let Ok(Some(map)) = beam_core::persist::read_json::<HashMap<String, (u64, u64)>>(&path) {
            let mut guard = rate_bucket_store()
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            for (key, val) in map {
                guard.insert(key, val);
            }
            info!("loaded {} persisted rate buckets", guard.len());
        }
    }

    // Probe bot open_id / app_name from Lark API and persist to bots-info.json.
    // Best-effort; failures are logged and do not block startup.
    for bot in state.bots.values() {
        let paths = state.paths.clone();
        let bot = bot.clone();
        tokio::spawn(async move {
            probe_and_persist_bot_info(&paths, &bot).await;
        });
    }

    let restore_candidates = {
        let mut sessions = state.sessions.lock().await;
        let restore_candidates = reconcile_restored_sessions_with(
            &mut sessions,
            state.config.daemon.quiet_restart,
            zellij_has_session,
        );
        let snapshot = sessions.clone();
        drop(sessions);
        persist_sessions(&state.paths, &snapshot).await?;
        restore_candidates
    };
    for session in restore_candidates {
        match build_init_from_session(&session, &state.config, &state.bots) {
            Ok(init) => {
                if let Err(err) = spawn_worker(state.clone(), session.clone(), init).await {
                    warn!("failed to restore session {}: {}", session.session_id, err);
                }
            }
            Err(err) => warn!(
                "failed to rebuild init for session {}: {}",
                session.session_id, err
            ),
        }
    }
    {
        let sessions = state.sessions.lock().await;
        for session in sessions.values() {
            if let Some(usage_limit) = session.usage_limit.clone() {
                arm_usage_limit_retry_timer(state.clone(), session.session_id.clone(), usage_limit);
            }
        }
    }
    // Recover pending final output retries from before restart.
    {
        let markers = load_final_output_retry_markers(&state.paths);
        if !markers.is_empty() {
            let active_sessions: HashSet<String> = {
                let sessions = state.sessions.lock().await;
                sessions
                    .values()
                    .filter(|s| s.status == SessionStatus::Active)
                    .map(|s| s.session_id.clone())
                    .collect()
            };
            let mut recovered = 0usize;
            let mut skipped = 0usize;
            for marker in &markers {
                if !active_sessions.contains(&marker.session_id) {
                    skipped += 1;
                    continue; // Session was closed during restart
                }
                // Check idempotency: resume only if this turn has NOT yet been delivered
                let should_resume = {
                    let sessions = state.sessions.lock().await;
                    sessions
                        .get(&marker.session_id)
                        .map(|s| {
                            marker.turn_id.as_deref().map_or(true, |tid| {
                                // Resume if last_final_output_turn_id doesn't match this turn
                                s.last_final_output_turn_id.as_deref() != Some(tid)
                            })
                        })
                        .unwrap_or(false) // session not found → skip
                };
                if should_resume {
                    info!(
                        "final output retry: resuming delivery for session {} turn {:?} attempt {}",
                        marker.session_id, marker.turn_id, marker.attempt
                    );
                    schedule_final_output_delivery(
                        state.clone(),
                        marker.session_id.clone(),
                        marker.content.clone(),
                        marker.turn_id.clone(),
                        marker.kind,
                        marker.user_text.clone(),
                        marker.attempt,
                    );
                    recovered += 1;
                } else {
                    skipped += 1;
                }
            }
            info!(
                "final output retry: {} markers recovered, {} skipped (closed/duplicate)",
                recovered, skipped
            );
        }
    }

    let cold_scan_bots: Vec<String> = state.bots.keys().cloned().collect();
    for lark_app_id in &cold_scan_bots {
        match scan_cold_workflow_runs(&state.paths, lark_app_id).await {
            Ok((runs, stats)) => {
                if stats.discovered > 0 {
                    info!(
                        "cold-scan: discovered {} non-terminal workflow runs for bot {}",
                        stats.discovered, lark_app_id
                    );
                }
                for skipped in &stats.skipped {
                    warn!("cold-scan skipped: {}", skipped);
                }
                for run in runs {
                    let run_id = run.run_id.clone();
                    info!("cold-attaching workflow run {}", run_id);
                    let s = state.clone();
                    tokio::spawn(async move {
                        if let Err(err) = drive_workflow_run_after_cold_attach(s, run).await {
                            warn!("cold-attach workflow run {} failed: {}", run_id, err);
                        }
                    });
                }
            }
            Err(err) => {
                warn!("cold-scan failed for bot {}: {}", lark_app_id, err);
            }
        }
    }

    async fn drive_workflow_run_after_cold_attach(
        state: AppState,
        run: ColdWorkflowRun,
    ) -> Result<()> {
        let workflow_json =
            serde_json::to_string(&run.def).context("failed to serialize workflow definition")?;
        workflow_runtime_driver::run(&state, &run.run_id, &workflow_json).await;
        Ok(())
    }

    async fn create_schedule(
        State(state): State<AppState>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let content = body.get("content").and_then(Value::as_str).unwrap_or("");
        let schedule_id = uuid::Uuid::new_v4().to_string();
        let task = serde_json::json!({
            "scheduleId": schedule_id,
            "content": content,
            "createdAt": chrono::Utc::now().to_rfc3339(),
            "status": "active",
        });
        let schedules_path = state.paths.schedules_json();
        let mut schedules: Vec<Value> = tokio::fs::read_to_string(&schedules_path)
            .await
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        schedules.push(task.clone());
        let _ = tokio::fs::write(
            &schedules_path,
            serde_json::to_string_pretty(&schedules).unwrap_or_default(),
        )
        .await;
        Json(task)
    }

    async fn report_session(
        State(state): State<AppState>,
        AxumPath(session_id): AxumPath<String>,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        let content = body
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if content.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "content must not be empty".to_string(),
            ));
        }
        let session = {
            let sessions = state.sessions.lock().await;
            sessions.get(&session_id).cloned()
        }
        .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
        if session.lark_app_id == "local" {
            return Ok(Json(serde_json::json!({
                "ok": true,
                "sessionId": session_id,
                "local": true,
            })));
        }
        let Some(bot) = state.bots.get(&session.lark_app_id) else {
            return Err((StatusCode::NOT_FOUND, "bot not registered".to_string()));
        };
        let post = build_report_post_content(&session, &content);
        let target_message_id = session
            .quote_target_id
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        let message_id = if let Some(target_message_id) = target_message_id {
            match lark_reply_post_message(&state, bot, target_message_id, &post).await {
                Ok(message_id) => message_id,
                Err(err) => return Err((StatusCode::BAD_GATEWAY, err.to_string())),
            }
        } else {
            match lark_send_post_message(&state, bot, &session.chat_id, &post).await {
                Ok(message_id) => message_id,
                Err(err) => return Err((StatusCode::BAD_GATEWAY, err.to_string())),
            }
        };
        Ok(Json(serde_json::json!({
            "ok": true,
            "sessionId": session_id,
            "messageId": message_id,
            "targetMessageId": target_message_id,
        })))
    }

    async fn list_bots(State(state): State<AppState>) -> Json<Vec<BotSummary>> {
        let sessions = state.sessions.lock().await;
        Json(
            state
                .bots
                .iter()
                .map(|(app_id, bot)| {
                    let active = sessions
                        .values()
                        .filter(|s| s.lark_app_id == *app_id && s.status == SessionStatus::Active)
                        .count();
                    BotSummary {
                        lark_app_id: app_id.clone(),
                        name: bot.name.clone(),
                        cli_id: bot.cli_id.clone(),
                        model: bot.model.clone(),
                        allowed_users: bot.allowed_users.clone(),
                        allowed_chat_groups: bot.allowed_chat_groups.clone(),
                        oncall_chats: bot
                            .oncall_chats
                            .iter()
                            .map(|oc| oc.chat_id.clone())
                            .collect(),
                        private_card: bot.private_card,
                        active_sessions: active,
                    }
                })
                .collect(),
        )
    }

    async fn get_bot(
        State(state): State<AppState>,
        AxumPath(app_id): AxumPath<String>,
    ) -> Result<Json<BotSummary>, (StatusCode, String)> {
        let sessions = state.sessions.lock().await;
        let bot = state
            .bots
            .get(&app_id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("bot {} not found", app_id)))?;
        let active = sessions
            .values()
            .filter(|s| s.lark_app_id == app_id && s.status == SessionStatus::Active)
            .count();
        Ok(Json(BotSummary {
            lark_app_id: app_id,
            name: bot.name.clone(),
            cli_id: bot.cli_id.clone(),
            model: bot.model.clone(),
            allowed_users: bot.allowed_users.clone(),
            allowed_chat_groups: bot.allowed_chat_groups.clone(),
            oncall_chats: bot
                .oncall_chats
                .iter()
                .map(|oc| oc.chat_id.clone())
                .collect(),
            private_card: bot.private_card,
            active_sessions: active,
        }))
    }

    async fn list_session_groups(State(state): State<AppState>) -> Json<Vec<SessionGroup>> {
        let sessions = state.sessions.lock().await;
        let mut groups: HashMap<String, SessionGroup> = HashMap::new();
        for session in sessions.values() {
            let key = session.chat_id.clone();
            let summary = SessionSummary::from(session);
            groups
                .entry(key)
                .and_modify(|g| g.sessions.push(summary.clone()))
                .or_insert_with(|| SessionGroup {
                    chat_id: session.chat_id.clone(),
                    title: Some(session.title.clone()),
                    sessions: vec![summary],
                });
        }
        Json(groups.into_values().collect())
    }

    async fn locate_session(
        State(state): State<AppState>,
        AxumPath(session_id): AxumPath<String>,
    ) -> Result<Json<SessionLocateInfo>, (StatusCode, String)> {
        let sessions = state.sessions.lock().await;
        let session = sessions.get(&session_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("session {} not found", session_id),
            )
        })?;
        Ok(Json(SessionLocateInfo {
            session_id: session.session_id.clone(),
            terminal_url: session.terminal_url.clone(),
            worker_pid: session.worker_pid,
        }))
    }

    async fn overview(State(state): State<AppState>) -> Json<DaemonOverview> {
        let sessions = state.sessions.lock().await;
        let active = sessions
            .values()
            .filter(|s| s.status == SessionStatus::Active)
            .count();
        let closed = sessions
            .values()
            .filter(|s| s.status == SessionStatus::Closed)
            .count();
        Json(DaemonOverview {
            pid: std::process::id(),
            started_at: state.started_at,
            session_count: sessions.len(),
            active_session_count: active,
            closed_session_count: closed,
            bot_count: state.bots.len(),
            worker_count: state.workers.lock().await.len(),
            config_path: state.paths.config_toml().display().to_string(),
            data_dir: state.paths.root().display().to_string(),
        })
    }

    async fn preferences(State(state): State<AppState>) -> Json<Value> {
        Json(serde_json::json!({
            "web": state.config.web,
            "daemon": state.config.daemon,
            "lark": state.config.lark,
            "screenAnalyzer": state.config.screen_analyzer,
        }))
    }

    async fn auth(State(state): State<AppState>) -> Json<Value> {
        let token = mint_dashboard_token();
        let expires_at = Instant::now() + Duration::from_secs(24 * 60 * 60);
        {
            let mut guard = state.dashboard_token.lock().await;
            *guard = Some(DashboardAuthToken {
                token: token.clone(),
                expires_at,
            });
        }
        Json(serde_json::json!({
            "authenticated": true,
            "token": token,
            "loginPath": format!("/dashboard/login?token={}", token),
            "dashboardPath": "/dashboard/",
            "expiresInSeconds": expires_at
                .checked_duration_since(Instant::now())
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "mode": "ws",
            "botCount": state.bots.len(),
            "daemonPid": std::process::id(),
            "dashboard": {
                "host": state.config.web.host,
                "proxyBasePort": state.config.web.proxy_base_port,
            },
        }))
    }

    async fn dashboard_login(
        State(state): State<AppState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let Some(token) = query
            .get("token")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        else {
            return Err((
                StatusCode::BAD_REQUEST,
                "missing dashboard token".to_string(),
            ));
        };
        if !dashboard_token_is_valid(&state, token).await {
            return Err((
                StatusCode::UNAUTHORIZED,
                "dashboard token expired".to_string(),
            ));
        }
        let mut response = Redirect::temporary("/dashboard/").into_response();
        response.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            axum::http::HeaderValue::from_str(&format!(
                "beam-dashboard-token={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400",
                token
            ))
            .map_err(internal_error)?,
        );
        Ok(response)
    }

    let protected_dashboard = Router::new()
        .route(
            "/api/workflows/definitions",
            get(list_workflow_definitions_api),
        )
        .route(
            "/api/workflows/definitions/{workflow_id}",
            get(get_workflow_definition_api),
        )
        .route(
            "/api/workflows/definitions/{workflow_id}/run",
            post(trigger_workflow_definition_run_api),
        )
        .route("/api/workflows/runs", get(list_workflow_runs_api))
        .route(
            "/api/workflows/runs/{run_id}/snapshot",
            get(get_workflow_run_snapshot_api),
        )
        .route(
            "/api/workflows/runs/{run_id}/events",
            get(get_workflow_run_events),
        )
        .route(
            "/api/workflows/runs/{run_id}/approve",
            post(approve_workflow_run),
        )
        .route(
            "/api/workflows/runs/{run_id}/reject",
            post(reject_workflow_run),
        )
        .route(
            "/api/workflows/runs/{run_id}/attempts/{activity_id}/{attempt_id}/resume",
            post(start_workflow_attempt_resume),
        )
        .route(
            "/api/workflows/runs/{run_id}/attempts/{activity_id}/{attempt_id}/resume/end",
            post(end_workflow_attempt_resume),
        )
        .route(
            "/api/workflows/runs/{run_id}/cancel",
            post(cancel_workflow_run),
        )
        .route(
            "/api/workflows/runs/{run_id}/resume",
            post(resume_workflow_run),
        )
        .route("/sessions", post(create_session))
        .route("/sessions/{session_id}", get(get_session))
        .route("/sessions/{session_id}/input", post(send_input))
        .route("/sessions/{session_id}/report", post(report_session))
        .route("/sessions/{session_id}/refresh", post(refresh_session))
        .route("/sessions/{session_id}/restart", post(restart_session))
        .route("/sessions/{session_id}/resume", post(resume_session))
        .route("/sessions/{session_id}/close", post(close_session))
        .route(
            "/api/workflows/{workflow_id}/run",
            post(trigger_workflow_run),
        )
        .route("/api/workflows/{run_id}", get(get_workflow_run))
        .route("/api/trigger", post(api_trigger))
        .route(
            "/adopt/zellij",
            get(list_zellij_adopt_candidates).post(adopt_zellij_session),
        )
        .route("/api/bots", get(list_bots))
        .route("/api/bots/{app_id}", get(get_bot))
        .route("/api/preferences", get(preferences))
        .route("/api/connectors", get(connectors).post(create_connector))
        .route("/api/connectors/stats", get(connector_stats))
        .route(
            "/api/connectors/{id}",
            get(get_connector_api)
                .put(update_connector_api)
                .patch(patch_connector_api)
                .delete(delete_connector_api),
        )
        .route(
            "/api/webhook-secrets",
            get(list_webhook_secrets_api).post(create_webhook_secret_api),
        )
        .route(
            "/api/webhook-secrets/{ref}",
            put(update_webhook_secret_api).delete(delete_webhook_secret_api),
        )
        .route("/api/trigger-logs", get(trigger_logs_api))
        .route("/api/trigger-logs/prune", post(prune_trigger_logs_api))
        .route("/api/connectors/webhooks", get(list_webhook_triggers))
        .route("/api/sessions/groups", get(list_session_groups))
        .route("/api/sessions/{session_id}/locate", get(locate_session))
        .route("/api/overview", get(overview))
        .nest_service(
            "/dashboard",
            get_service(ServeDir::new("src/dashboard/web")),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            dashboard_gate,
        ));

    let open_routes = Router::new()
        .route("/health", get(health))
        .route("/shutdown", post(shutdown))
        .route("/sessions", get(list_sessions))
        .route("/api/auth", get(auth))
        .route("/dashboard/login", get(dashboard_login))
        .route("/api/schedules", post(create_schedule))
        .route("/webhook/{workflow_id}", post(handle_webhook_trigger))
        .route(
            "/sessions/{session_id}/history",
            get(lark_history::session_history),
        )
        .route(
            "/sessions/{session_id}/quoted/{message_id}",
            get(lark_history::quoted_message),
        )
        .route("/sessions/{session_id}/final-output", post(final_output))
        .route("/api/asks", post(ask::create_ask))
        .route("/api/attention", post(set_attention_route))
        .route(
            "/debug/simulate/lark-message",
            post(debug_simulate::simulate_lark_message_handler),
        );

    // Start zellij web server and ensure tokens
    let zellij_web_port = state.config.web.proxy_base_port + 1;
    zellij_web::ensure_zellij_web(zellij_web_port)
        .with_context(|| format!("failed to start zellij web server on port {zellij_web_port}"))?;
    let zellij_tokens = zellij_web::ensure_zellij_web_tokens(
        &state.paths.zellij_web_tokens_json(),
        zellij_web_port,
    )
    .with_context(|| "failed to create zellij web tokens")?;
    zellij_web::spawn_zellij_web_watchdog(zellij_web_port);

    // Start terminal proxy with auth bridge
    let proxy_host = state.config.web.host.clone();
    let proxy_port = state.config.web.proxy_base_port;
    let proxy_sessions = state.sessions.clone();
    let auth_state = terminal_auth::TerminalAuthState::new();
    // Load persisted used tickets for terminal auth anti-replay.
    auth_state
        .load_used_tickets(&paths.used_tickets_json())
        .await;
    terminal_proxy::start_proxy(
        &proxy_host,
        proxy_port,
        zellij_web_port,
        proxy_sessions,
        zellij_tokens,
        auth_state.clone(),
    )
    .await
    .with_context(|| format!("failed to start terminal proxy on {proxy_host}:{proxy_port}"))?;

    // Periodic state persistence for stores that are updated frequently.
    let periodic_paths = paths.clone();
    let periodic_auth = auth_state.clone();
    let periodic_recent_events = state.recent_lark_events.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            // Save replay nonces
            let replay_map: HashMap<String, u64> = {
                let guard = replay_nonce_store()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                guard.clone()
            };
            if !replay_map.is_empty() {
                let path = periodic_paths.replay_nonces_json();
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = beam_core::persist::atomic_write_json(&path, &replay_map);
                })
                .await;
            } else {
                let _ = tokio::fs::remove_file(periodic_paths.replay_nonces_json()).await;
            }
            // Save rate buckets
            let rate_map: HashMap<String, (u64, u64)> = {
                let guard = rate_bucket_store()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                guard.clone()
            };
            if !rate_map.is_empty() {
                let path = periodic_paths.rate_buckets_json();
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = beam_core::persist::atomic_write_json(&path, &rate_map);
                })
                .await;
            } else {
                let _ = tokio::fs::remove_file(periodic_paths.rate_buckets_json()).await;
            }
            // Save used tickets
            periodic_auth
                .save_used_tickets(&periodic_paths.used_tickets_json())
                .await;
            // Save recent Lark events dedupe state
            {
                let events = periodic_recent_events.lock().await;
                save_recent_lark_events(&periodic_paths, &events).await;
            }
        }
    });

    // Worker health watchdog: flag sessions whose worker stopped heartbeating.
    worker_lifecycle::spawn_worker_health_watchdog(state.clone());

    // Schedule loop: periodically check schedules and trigger due tasks.
    let schedule_paths = paths.clone();
    let schedule_state = state.clone();    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let tasks = match beam_core::list_tasks(&schedule_paths) {
                Ok(tasks) => tasks,
                Err(_) => continue,
            };
            let now = chrono::Utc::now();
            for task in &tasks {
                if !task.enabled {
                    continue;
                }
                let Some(next_run_at_str) = task.next_run_at.as_deref() else {
                    continue;
                };
                let Ok(next_run_at) = chrono::DateTime::parse_from_rfc3339(next_run_at_str) else {
                    continue;
                };
                let next_run_at = next_run_at.with_timezone(&chrono::Utc);
                if next_run_at > now {
                    continue;
                }
                // Task is due: execute it.
                info!("schedule: triggering task {} ({})", task.id, task.name);
                let state = schedule_state.clone();
                let task_id = task.id.clone();
                let task_prompt = task.prompt.clone();
                let task_working_dir = task.working_dir.clone();
                let task_chat_id = task.chat_id.clone();
                let task_lark_app_id = task.lark_app_id.clone();
                let task_root_message_id = task.root_message_id.clone();
                let task_scope = task.scope.clone();
                let task_chat_type = task.chat_type.clone();
                let task_name = task.name.clone();
                let paths = schedule_paths.clone();
                tokio::spawn(async move {
                    let result = execute_schedule_task(
                        &state,
                        &task_id,
                        &task_prompt,
                        &task_working_dir,
                        &task_chat_id,
                        task_lark_app_id.as_deref(),
                        task_root_message_id.as_deref(),
                        task_scope.as_deref(),
                        task_chat_type.as_ref(),
                        &task_name,
                    )
                    .await;
                    let success = result.is_ok();
                    let error = result.as_ref().err().map(|e| e.to_string());
                    let _ = beam_core::mark_run(&paths, &task_id, success, error.as_deref(), None);
                    if let Err(err) = result {
                        warn!("schedule task {} failed: {}", task_id, err);
                    }
                });
            }
        }
    });

    let app = Router::new()
        .merge(protected_dashboard)
        .merge(open_routes)
        .with_state(state);

    info!("beam daemon listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        })
        .await?;

    let _ = tokio::fs::remove_file(paths.runtime_state_json()).await;
    Ok(())
}

#[doc(hidden)]
pub fn __test_resolve_external_host(bind_host: &str) -> String {
    resolve_external_host(bind_host)
}

#[cfg(test)]
#[path = "tests/lib_integration/mod.rs"]
mod tests;
