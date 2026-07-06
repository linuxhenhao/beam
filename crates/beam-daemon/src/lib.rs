use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

mod ask;
mod card_i18n;
mod connector_runtime;
mod connector_store;
mod daemon_types;
mod dashboard_support;
mod dir_select;
mod final_output;
mod grant;
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
pub(crate) use connector_runtime::*;
pub(crate) use final_output::*;
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
    dashboard_gate, dashboard_token_is_valid, detect_external_host,
    load_observed_bot_open_ids_for_app, load_observed_bots_for_chat, mint_dashboard_token,
    read_webhook_trigger_records, record_observed_bots,
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
    CreateSessionRequest, DaemonOverview, DaemonRuntimeState, DaemonToWorker, DisplayMode,
    EventDraft, EventLog, EventWindowOpts, FinalOutputKind, FinalOutputRequest, InitConfig,
    PendingResponseCardState, RestartSessionRequest, ResumeSessionRequest, RunChatBinding,
    RunStatus, ScheduleChatType, ScreenStatus, Session, SessionGroup, SessionInputRequest,
    SessionLocateInfo, SessionScope, SessionStatus, SessionSummary, TalkEvaluation, TermActionKey,
    TranscriptChoice, TuiPromptOption, WaitResolution, WorkerToDaemon, WorkflowActor,
    WorkflowOutputRef, can_operate, evaluate_talk, grant_restricted, parse_workflow_definition,
    read_event_window, read_run_events_pure, read_run_snapshot, scan_cold_workflow_runs,
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
    let external_host = detect_external_host(&config.web.host);
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

    let state = AppState {
        paths: paths.clone(),
        started_at,
        sessions: Arc::new(Mutex::new(sessions)),
        workers: Arc::new(Mutex::new(HashMap::new())),
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
        external_host,
    };

    if matches!(
        state.config.lark.event_mode.as_str(),
        "ws" | "websocket" | "stream"
    ) {
        spawn_lark_ws_clients(&state);
    }

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
            "mode": state.config.lark.event_mode,
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
        .route("/lark/events/{app_id}", post(handle_lark_event))
        .route("/lark/cards/{app_id}", post(handle_lark_card_action))
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
        .route("/api/attention", post(set_attention_route));

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

    // Schedule loop: periodically check schedules and trigger due tasks.
    let schedule_paths = paths.clone();
    let schedule_state = state.clone();
    tokio::spawn(async move {
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

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) mod test_helpers;
    use test_helpers::*;

    use super::*;
    use beam_core::{
        BootstrapWorkflowRunInput, WorkflowDispatchOutcome, WorkflowDispatchRun,
        bootstrap_workflow_run,
    };

    #[test]
    fn parses_lark_history_text_and_post_content() {
        let item = serde_json::json!({
            "message_id": "om_1",
            "root_id": "om_root",
            "thread_id": "omt_1",
            "chat_id": "oc_1",
            "msg_type": "post",
            "body": {
                "content": serde_json::json!({
                    "content": [[
                        { "tag": "text", "text": "hello" },
                        { "tag": "a", "text": "link" }
                    ]]
                }).to_string()
            },
            "sender": { "id": "ou_1", "sender_type": "user" },
            "create_time": "1234"
        });
        let parsed = parse_lark_history_message(&item);
        assert_eq!(parsed.message_id, "om_1");
        assert_eq!(parsed.root_id.as_deref(), Some("om_root"));
        assert_eq!(parsed.content, "hello link");
        assert_eq!(parsed.create_time, Some(1234));
    }

    #[tokio::test]
    async fn list_chat_history_returns_chronological_tail() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
        let app_id = "app-history-chat";
        let bot = make_bot(app_id);
        let state = make_state(
            temp_paths("history-chat"),
            HashMap::from([(app_id.to_string(), bot.clone())]),
        );

        let raw = lark_list_chat_history(&state, &bot, "chat-1", 10)
            .await
            .expect("chat history");
        let messages = raw
            .iter()
            .map(parse_lark_history_message)
            .collect::<Vec<_>>();
        assert_eq!(
            messages
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["chat oldest", "chat newest"]
        );
    }

    #[tokio::test]
    async fn list_thread_history_uses_session_thread_id_first() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
        let app_id = "app-history-thread";
        let bot = make_bot(app_id);
        let state = make_state(
            temp_paths("history-thread"),
            HashMap::from([(app_id.to_string(), bot.clone())]),
        );
        let mut session = make_session("sess-history-thread");
        session.lark_app_id = app_id.to_string();
        session.root_message_id = "omt_not_a_message".to_string();
        session.thread_id = Some("omt_direct_thread".to_string());

        let raw = lark_list_thread_history(&state, &bot, &session, 10)
            .await
            .expect("thread history");
        let messages = raw
            .iter()
            .map(parse_lark_history_message)
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].thread_id.as_deref(), Some("omt_direct_thread"));
        assert_eq!(messages[0].content, "thread one");
    }

    #[tokio::test]
    async fn quoted_message_api_fetches_message_detail() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
        let app_id = "app-quoted";
        let state = make_state(
            temp_paths("quoted"),
            HashMap::from([(app_id.to_string(), make_bot(app_id))]),
        );
        let mut session = make_session("sess-quoted");
        session.lark_app_id = app_id.to_string();
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let Json(value) = quoted_message(
            State(state),
            AxumPath((session.session_id.clone(), "om_quoted".to_string())),
        )
        .await
        .expect("quoted message");
        assert_eq!(
            value.pointer("/message/messageId").and_then(Value::as_str),
            Some("om_quoted")
        );
        assert_eq!(
            value.pointer("/message/content").and_then(Value::as_str),
            Some("quoted detail")
        );
    }

    #[tokio::test]
    async fn quoted_message_api_errors_when_session_missing() {
        let app_id = "app-quoted-missing";
        let state = make_state(
            temp_paths("quoted-missing"),
            HashMap::from([(app_id.to_string(), make_bot(app_id))]),
        );

        let err = quoted_message(
            State(state),
            AxumPath(("missing-session".to_string(), "om_quoted".to_string())),
        )
        .await
        .expect_err("missing session should fail");
        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn session_history_api_returns_session_scoped_messages() {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
        let app_id = "app-history-api";
        let state = make_state(
            temp_paths("history-api"),
            HashMap::from([(app_id.to_string(), make_bot(app_id))]),
        );
        let mut session = make_session("sess-history-api");
        session.lark_app_id = app_id.to_string();
        session.scope = SessionScope::Chat;
        session.status = SessionStatus::Active;
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let Json(value) = session_history(
            State(state),
            AxumPath(session.session_id.clone()),
            Query(LarkHistoryQuery {
                limit: Some(10),
                scope: Some("session".to_string()),
            }),
        )
        .await
        .expect("history api");
        assert_eq!(value.get("scope").and_then(Value::as_str), Some("chat"));
        assert_eq!(value.get("total").and_then(Value::as_u64), Some(2));
        assert_eq!(
            value.pointer("/messages/0/content").and_then(Value::as_str),
            Some("chat oldest")
        );
    }

    #[tokio::test]
    async fn list_workflow_definitions_prefers_first_search_path_and_hashes_canonically() {
        let paths = temp_paths("workflow-defs");
        maybe_remove_dir(&paths.root().to_path_buf());
        let dir_a = paths.root().join("workflows-a");
        let dir_b = paths.root().join("workflows-b");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();
        let def_a = r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string","required":true}},"nodes":{"root":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"Schedule demo","workingDir":"/tmp/demo","chatId":"oc_demo","scope":"thread"},"unsafeAllowUngated":true}}}"#;
        let def_b = r#"{"workflowId":"flow-a","version":2,"nodes":{"alt":{"type":"subagent","bot":"bot-a","prompt":"hi"}}}"#;
        tokio::fs::write(dir_a.join("flow-a.workflow.json"), def_a)
            .await
            .unwrap();
        tokio::fs::write(dir_b.join("flow-a.workflow.json"), def_b)
            .await
            .unwrap();

        let defs = list_workflow_definitions_in(vec![dir_a.clone(), dir_b.clone()])
            .await
            .expect("defs");
        assert_eq!(defs.len(), 1);
        let def = &defs[0];
        assert_eq!(def.workflow_id, "flow-a");
        assert_eq!(def.version, 1);
        assert_eq!(
            def.path,
            dir_a.join("flow-a.workflow.json").display().to_string()
        );
        assert_eq!(def.param_count, 1);
        assert_eq!(def.required_param_count, 1);
        assert_eq!(def.node_count, 1);
        assert_eq!(def.revision_id.len(), 64);
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn list_workflow_runs_respects_terminal_filter_and_status_filters() {
        let paths = temp_paths("workflow-runs");
        maybe_remove_dir(&paths.root().to_path_buf());
        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-active",
                workflow_json: r#"{"workflowId":"flow-active","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello"}}}"#,
                expected_workflow_id: Some("flow-active"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-done",
                workflow_json: r#"{"workflowId":"flow-done","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello"}}}"#,
                expected_workflow_id: Some("flow-done"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-2".to_string(),
                    lark_app_id: "app-2".to_string(),
                }),
            },
        )
        .unwrap();
        {
            let mut log = EventLog::new("run-done", paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "runSucceeded".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "outputRef": {
                            "outputHash": "sha256:done",
                            "outputPath": paths.workflow_run_dir("run-done").join("blobs").join("done").display().to_string(),
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let default_rows = list_workflow_runs(&paths, false, None).await.expect("runs");
        assert_eq!(default_rows.len(), 1);
        assert_eq!(default_rows[0].run_id, "run-active");
        assert_eq!(default_rows[0].chat_id.as_deref(), Some("chat-1"));

        let all_rows = list_workflow_runs(&paths, true, None)
            .await
            .expect("all runs");
        assert_eq!(all_rows.len(), 2);

        let filtered_rows = list_workflow_runs(
            &paths,
            true,
            Some(HashSet::from([String::from("succeeded")])),
        )
        .await
        .expect("filtered");
        assert_eq!(filtered_rows.len(), 1);
        assert_eq!(filtered_rows[0].run_id, "run-done");
        assert_eq!(filtered_rows[0].status, "succeeded");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn load_workflow_catalog_definition_in_hashes_canonically() {
        let paths = temp_paths("workflow-catalog-canonical");
        maybe_remove_dir(&paths.root().to_path_buf());
        let dir_a = paths.root().join("catalog-a");
        let dir_b = paths.root().join("catalog-b");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();
        let raw_a = r#"{"workflowId":"flow-catalog","version":1,"nodes":{"root":{"type":"subagent","bot":"bot-a","prompt":"hi","workingDir":"/tmp/demo"}}}"#;
        let raw_b = r#"
        {
            "nodes": {
                "root": {
                    "workingDir": "/tmp/demo",
                    "prompt": "hi",
                    "bot": "bot-a",
                    "type": "subagent"
                }
            },
            "version": 1,
            "workflowId": "flow-catalog"
        }
        "#;
        tokio::fs::write(dir_a.join("flow-catalog.workflow.json"), raw_a)
            .await
            .unwrap();
        tokio::fs::write(dir_b.join("flow-catalog.workflow.json"), raw_b)
            .await
            .unwrap();

        let def_a = load_workflow_catalog_definition_in(
            "flow-catalog",
            vec![dir_a.join("flow-catalog.workflow.json")],
        )
        .await
        .expect("catalog a")
        .expect("catalog a present");
        let def_b = load_workflow_catalog_definition_in(
            "flow-catalog",
            vec![dir_b.join("flow-catalog.workflow.json")],
        )
        .await
        .expect("catalog b")
        .expect("catalog b present");

        assert_eq!(def_a.revision_id, def_b.revision_id);
        assert_eq!(
            def_a.path,
            dir_a
                .join("flow-catalog.workflow.json")
                .display()
                .to_string()
        );
        assert_eq!(
            def_b.path,
            dir_b
                .join("flow-catalog.workflow.json")
                .display()
                .to_string()
        );
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn webhook_trigger_records_round_trip_and_list_api_shape() {
        let paths = temp_paths("webhook-triggers");
        maybe_remove_dir(&paths.root().to_path_buf());
        let records = vec![WebhookTriggerRecord {
            workflow_id: "flow-a".to_string(),
            created_at: "2026-06-07T00:00:00Z".to_string(),
            secret_valid: true,
            request_body: serde_json::json!({"hello":"world"}),
            run_id: Some("run-1".to_string()),
            workflow_run_id: Some("run-1".to_string()),
            status: "accepted".to_string(),
        }];
        write_webhook_trigger_records(&paths, &records).expect("write records");
        let loaded = read_webhook_trigger_records(&paths)
            .await
            .expect("read records");
        assert_eq!(loaded, records);
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn dashboard_auth_helpers_support_header_and_cookie_tokens() {
        let paths = temp_paths("dashboard-auth");
        maybe_remove_dir(&paths.root().to_path_buf());
        let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
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
        };

        let token = mint_dashboard_token();
        {
            let mut guard = state.dashboard_token.lock().await;
            *guard = Some(DashboardAuthToken {
                token: token.clone(),
                expires_at: Instant::now() + Duration::from_secs(30),
            });
        }
        let header_token = extract_dashboard_token(
            &HeaderMap::from_iter([(
                axum::http::header::AUTHORIZATION,
                axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            )]),
            None,
        )
        .expect("bearer token");
        assert_eq!(header_token, token);

        let cookie_token = extract_dashboard_token(
            &HeaderMap::from_iter([(
                axum::http::header::COOKIE,
                axum::http::HeaderValue::from_str(&format!(
                    "beam-dashboard-token={}; foo=bar",
                    token
                ))
                .unwrap(),
            )]),
            None,
        )
        .expect("cookie token");
        assert_eq!(cookie_token, token);

        assert!(dashboard_token_is_valid(&state, &token).await);
        let mut expired = state.dashboard_token.lock().await;
        *expired = Some(DashboardAuthToken {
            token: token.clone(),
            expires_at: Instant::now() - Duration::from_secs(1),
        });
        drop(expired);
        assert!(!dashboard_token_is_valid(&state, &token).await);
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn beam_schedule_host_executor_creates_task_and_returns_task_id() {
        let paths = temp_paths("schedule-host");
        maybe_remove_dir(&paths.root().to_path_buf());
        let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        };
        let node = beam_core::HostExecutorNode {
            base: beam_core::workflow_definition::NodeBase {
                description: None,
                depends: None,
                human_gate: None,
                retry_policy: None,
                timeout_ms: None,
                max_output_bytes: None,
                output_schema: None,
                unsafe_allow_ungated: None,
            },
            executor: "beam-schedule".to_string(),
            input: serde_json::json!({
                "name": "schedule-demo daily 9am",
                "schedule": "0 9 * * *",
                "parsed": {
                    "kind": "cron",
                    "expr": "0 9 * * *",
                    "display": "0 9 * * *"
                },
                "prompt": "Schedule demo: run workflow self-check.",
                "workingDir": "/tmp/beam-schedule-demo",
                "chatId": "oc_workflow_demo",
                "scope": "thread"
            }),
        };
        let outcome = run_workflow_host_executor(
            &state,
            WorkflowDispatchRun {
                run_id: "run-1",
                workflow_id: "flow-a",
                revision_id: "rev-1",
                activity_id: "activity-1",
                attempt_id: "attempt-1",
                node_id: "node-1",
            },
            &node,
            node.input.clone(),
            None,
        )
        .await
        .expect("host executor");
        match outcome {
            WorkflowDispatchOutcome::Succeeded { output, session } => {
                assert_eq!(
                    output["taskId"],
                    derive_workflow_idempotency_key(
                        "flow-a",
                        "rev-1",
                        "run-1",
                        "node-1",
                        "attempt-1"
                    )
                );
                assert!(session.is_none());
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert!(paths.schedules_json().exists());
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn clear_pending_response_patch_marker_is_idempotent() {
        let paths = temp_paths("pending-marker-clear");
        maybe_remove_dir(&paths.root().to_path_buf());

        write_pending_response_patch_marker(&paths, "sess-1", "om_card")
            .await
            .expect("write marker");
        clear_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("clear once");
        clear_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("clear twice");
        let marker = read_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("read marker");
        assert!(marker.is_none());

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn park_stream_card_merges_with_existing_on_disk_entries() {
        let paths = temp_paths("park-merge");
        maybe_remove_dir(&paths.root().to_path_buf());

        let mut existing = HashMap::new();
        existing.insert(
            "persisted_a".to_string(),
            FrozenCard {
                message_id: "om_disk_a".to_string(),
                content: "old".to_string(),
                title: "older".to_string(),
                display_mode: Some(DisplayMode::Hidden),
                image_key: None,
            },
        );
        save_frozen_cards(&paths, "sess-merge", &existing)
            .await
            .expect("save existing");

        let mut session = make_session("sess-merge");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.stream_card_id = Some("om_live".to_string());
        session.stream_card_nonce = Some("nonce_live".to_string());

        park_stream_card(&paths, &session)
            .await
            .expect("park succeeds");
        let frozen_cards = load_frozen_cards(&paths, &session.session_id)
            .await
            .expect("load merged");
        assert_eq!(frozen_cards.len(), 2);
        assert!(frozen_cards.contains_key("persisted_a"));
        assert!(frozen_cards.contains_key("nonce_live"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn load_clicked_frozen_card_only_returns_stale_snapshot() {
        let paths = temp_paths("load-frozen");
        maybe_remove_dir(&paths.root().to_path_buf());

        let mut cards = HashMap::new();
        cards.insert(
            "nonce_old".to_string(),
            FrozenCard {
                message_id: "om_old".to_string(),
                content: "frozen output".to_string(),
                title: "old turn".to_string(),
                display_mode: Some(DisplayMode::Screenshot),
                image_key: None,
            },
        );
        save_frozen_cards(&paths, "sess-load", &cards)
            .await
            .expect("save succeeds");

        let mut session = make_session("sess-load");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.stream_card_nonce = Some("nonce_live".to_string());

        let stale = load_clicked_frozen_card(&paths, &session, Some("nonce_old"))
            .await
            .expect("load stale");
        assert_eq!(
            stale.as_ref().map(|card| card.content.as_str()),
            Some("frozen output")
        );

        let live = load_clicked_frozen_card(&paths, &session, Some("nonce_live"))
            .await
            .expect("load live");
        assert!(live.is_none());

        session.stream_card_nonce = None;
        let after_turn_reset = load_clicked_frozen_card(&paths, &session, Some("nonce_old"))
            .await
            .expect("load after reset");
        assert_eq!(
            after_turn_reset.as_ref().map(|card| card.content.as_str()),
            Some("frozen output")
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn dir_select_filter_response_includes_card_and_toast() {
        // Verify that a filter action response returns both toast and card fields,
        // so the Feishu client updates the card inline instead of just showing a toast.
        let card_json = dir_select::build_dir_select_card(
            "pending-1",
            "/home/user/projects",
            "test message",
            &[".".to_string(), "project-a".to_string()],
            &[
                ".".to_string(),
                "project-a".to_string(),
                "project-b".to_string(),
            ],
            Some(&["project-a".to_string()]),
            Some("project"),
            None,
            Some("zh"),
        );
        let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
        let toast_msg = "已筛选 \"project\"";
        let response = serde_json::json!({
            "toast": { "type": "success", "content": toast_msg },
            "card": { "type": "raw", "data": card_data }
        });

        // Response must contain both toast and card fields
        assert!(
            response.get("toast").is_some(),
            "response must have toast field"
        );
        assert!(
            response.get("card").is_some(),
            "response must have card field"
        );
        assert_eq!(
            response.pointer("/toast/content").and_then(Value::as_str),
            Some(toast_msg)
        );
        assert_eq!(
            response.pointer("/card/type").and_then(Value::as_str),
            Some("raw")
        );
        // The card data should contain the filtered directory button
        let card_str = response.pointer("/card/data").unwrap().to_string();
        assert!(
            card_str.contains("project-a"),
            "filtered card must show project-a"
        );
        assert!(
            card_str.contains("dir_select_pick"),
            "filtered card must retain pickable buttons"
        );
    }

    #[test]
    fn dir_select_filter_response_card_contains_filtered_dirs_only() {
        // When filtering with a keyword, the response card should show only matching dirs.
        let all_dirs: Vec<String> = vec![
            ".".to_string(),
            "project-a".to_string(),
            "project-b".to_string(),
            "other".to_string(),
        ];
        let filtered: Vec<String> = vec!["project-a".to_string(), "project-b".to_string()];

        let card_json = dir_select::build_dir_select_card(
            "pending-2",
            "/root",
            "test",
            &[],
            &all_dirs,
            Some(&filtered),
            Some("project"),
            None,
            Some("zh"),
        );
        let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
        let response = serde_json::json!({
            "toast": { "type": "success", "content": "已筛选 \"project\"" },
            "card": { "type": "raw", "data": card_data }
        });

        let card_str = response.pointer("/card/data").unwrap().to_string();
        // Must contain the matching dirs
        assert!(
            card_str.contains("project-a"),
            "card must contain project-a"
        );
        assert!(
            card_str.contains("project-b"),
            "card must contain project-b"
        );
        // Should NOT contain the non-matching dir "other"
        assert!(
            !card_str.contains("\"working_dir\":\"other\""),
            "card must NOT contain non-matching dir 'other'"
        );
        // Must still have pickable buttons
        assert!(
            card_str.contains("dir_select_pick"),
            "filtered card must retain dir_select_pick buttons"
        );
    }

    #[test]
    fn dir_select_filter_response_empty_keyword_shows_all_dirs() {
        // Empty keyword should show all candidates (clear filter / show all).
        let all_dirs: Vec<String> = vec![
            ".".to_string(),
            "project-a".to_string(),
            "project-b".to_string(),
        ];

        let card_json = dir_select::build_dir_select_card(
            "pending-3",
            "/root",
            "test",
            &[],
            &all_dirs,
            Some(&all_dirs),
            None,
            None,
            Some("zh"),
        );
        let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
        let response = serde_json::json!({
            "toast": { "type": "success", "content": "已显示全部目录" },
            "card": { "type": "raw", "data": card_data }
        });

        // Response must have both fields
        assert!(response.get("toast").is_some());
        assert!(response.get("card").is_some());

        let card_str = response.pointer("/card/data").unwrap().to_string();
        // All dirs should be present as buttons
        assert!(card_str.contains("dir_select_pick"));
        // The search keyword field should be empty (cleared)
        let v: Value = serde_json::from_str(&card_str).expect("valid card JSON");
        let elements = v["elements"].as_array().unwrap();
        let form = elements
            .iter()
            .find(|e| e["tag"].as_str() == Some("form"))
            .unwrap();
        let form_els = form["elements"].as_array().unwrap();
        let input = form_els
            .iter()
            .find(|e| e["tag"].as_str() == Some("input"))
            .unwrap();
        assert_eq!(
            input["default_value"].as_str(),
            Some(""),
            "empty keyword should clear the input field"
        );
    }

    #[test]
    fn dir_select_filter_response_empty_result_shows_warning() {
        // When no directories match, the card should show a warning message.
        let all_dirs: Vec<String> = vec![".".to_string(), "project-a".to_string()];
        let filtered: Vec<String> = vec![];

        let card_json = dir_select::build_dir_select_card(
            "pending-4",
            "/root",
            "test",
            &[],
            &all_dirs,
            Some(&filtered),
            Some("nonexistent"),
            Some("⚠️ 没有目录匹配关键词 \"nonexistent\"，请尝试其他关键词。"),
            Some("zh"),
        );
        let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
        let response = serde_json::json!({
            "toast": { "type": "success", "content": "已筛选 \"nonexistent\"" },
            "card": { "type": "raw", "data": card_data }
        });

        let card_str = response.pointer("/card/data").unwrap().to_string();
        // Must show the warning message
        assert!(
            card_str.contains("没有目录匹配"),
            "empty result card must show warning message"
        );
        assert!(
            card_str.contains("请尝试其他关键词"),
            "empty result card must suggest trying other keywords"
        );
        // Must still have the search form to allow retry
        assert!(
            card_str.contains("dir_search_keyword"),
            "empty result card must retain search input"
        );
    }

    #[test]
    fn dir_select_card_uses_action_buttons() {
        // Verify that the card exposes directory choices as clickable buttons
        // AND a select_static dropdown as an alternative entry point.
        let all_dirs: Vec<String> = (0..10).map(|i| format!("project-{}", i)).collect();
        let card_json = dir_select::build_dir_select_card(
            "pid",
            "/root",
            "test",
            &all_dirs,
            &all_dirs,
            None,
            None,
            None,
            Some("zh"),
        );
        let v: Value = serde_json::from_str(&card_json).expect("valid card JSON");
        let elements = v["elements"].as_array().unwrap();

        let action_elements: Vec<&Value> = elements
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .collect();
        assert!(
            !action_elements.is_empty(),
            "card must contain directory action groups"
        );

        let buttons: Vec<&Value> = action_elements
            .iter()
            .flat_map(|e| e["actions"].as_array().into_iter().flatten())
            .filter(|child| child["tag"].as_str() == Some("button"))
            .collect();
        // 10 dirs ≤ MAX_BUTTON_DIRS(40), so all show as buttons
        assert_eq!(buttons.len(), 10, "should have one button per directory");
        for (i, button) in buttons.iter().enumerate() {
            assert_eq!(
                button.pointer("/value/action").and_then(Value::as_str),
                Some("dir_select_pick")
            );
            assert_eq!(
                button.pointer("/value/pending_id").and_then(Value::as_str),
                Some("pid")
            );
            assert_eq!(
                button.pointer("/value/working_dir").and_then(Value::as_str),
                Some(format!("project-{}", i).as_str())
            );
        }
        // Card now includes select_static inside action.actions as alternative entry point.
        // A bare select_static at the top level is not valid in Feishu card schema.
        let select_static = elements
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .find_map(|e| {
                e["actions"].as_array().and_then(|actions| {
                    actions
                        .iter()
                        .find(|a| a["tag"].as_str() == Some("select_static"))
                })
            })
            .expect("card should contain select_static dropdown inside action.actions");
        // Verify no bare select_static at the top level
        assert!(
            elements
                .iter()
                .find(|e| e["tag"].as_str() == Some("select_static"))
                .is_none(),
            "select_static must not be a bare top-level element"
        );
        let options = select_static["options"].as_array().unwrap();
        assert_eq!(
            options.len(),
            10,
            "select_static should have all 10 options"
        );
        // Verify first option value is valid JSON with correct fields
        let first_opt_val = options[0]["value"].as_str().unwrap();
        let opt_parsed: Value = serde_json::from_str(first_opt_val).unwrap();
        assert_eq!(opt_parsed["action"].as_str(), Some("dir_select_pick"));
        assert_eq!(opt_parsed["pending_id"].as_str(), Some("pid"));
        assert!(opt_parsed["working_dir"].as_str().is_some());
    }

    #[test]
    fn grant_add_chat_grant_includes_quota_key() {
        let mut config = serde_json::json!([{
            "larkAppId": "app-1",
            "larkAppSecret": "s",
            "cliId": "codex",
            "allowedUsers": ["ou_owner"]
        }]);
        grant::add_chat_grant(&mut config, "app-1", "chat-1", "ou_user", Some(5)).unwrap();
        let bot = &config.as_array().unwrap()[0];
        let grants = bot["chatGrants"]["chat-1"].as_array().unwrap();
        assert!(grants.iter().any(|v| v.as_str() == Some("ou_user")));
        let quota = &bot["quotaState"]["chat:chat-1:ou_user"];
        assert_eq!(quota["limit"].as_u64().unwrap(), 5);
        assert_eq!(quota["used"].as_u64().unwrap(), 0);
    }

    #[test]
    fn grant_revoke_removes_from_all_lists() {
        let mut config = serde_json::json!([{
            "larkAppId": "app-1",
            "larkAppSecret": "s",
            "cliId": "codex",
            "allowedUsers": ["ou_owner", "ou_user"],
            "chatGrants": {"chat-1": ["ou_user"]},
            "globalGrants": ["ou_user"],
            "quotaState": {"chat:chat-1:ou_user": {"limit": 5, "used": 3}}
        }]);
        grant::revoke_grant(
            &mut config,
            "app-1",
            "chat-1",
            "ou_user",
            &["ou_owner".to_string()],
        )
        .unwrap();
        let bot = &config.as_array().unwrap()[0];
        assert!(
            !bot["allowedUsers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v.as_str() == Some("ou_user"))
        );
        assert!(
            bot["chatGrants"]["chat-1"]
                .as_array()
                .unwrap_or(&vec![])
                .is_empty()
        );
        assert!(bot["globalGrants"].as_array().unwrap_or(&vec![]).is_empty());
        assert!(bot["quotaState"].as_object().unwrap().is_empty());
    }

    #[test]
    fn grant_cannot_revoke_owner() {
        let mut config = serde_json::json!([{
            "larkAppId": "app-1",
            "larkAppSecret": "s",
            "cliId": "codex",
            "allowedUsers": ["ou_owner"]
        }]);
        let result = grant::revoke_grant(
            &mut config,
            "app-1",
            "chat-1",
            "ou_owner",
            &["ou_owner".to_string()],
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn inbound_quota_consumes_and_exhausts() {
        let paths = temp_paths("inbound-quota");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).unwrap();
        let bot = BotConfig {
            name: None,
            lark_app_id: "app-1".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_owner".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::from([(
                "chat-1".to_string(),
                vec!["ou_user".to_string()],
            )]),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::from([(
                "chat:chat-1:ou_user".to_string(),
                beam_core::QuotaEntry { limit: 2, used: 1 },
            )]),
        };
        let app_id = bot.lark_app_id.clone();
        std::fs::write(
            paths.bots_json(),
            serde_json::to_string_pretty(&serde_json::json!([{
                "larkAppId": "app-1",
                "larkAppSecret": "secret",
                "cliId": "codex",
                "allowedUsers": ["ou_owner"],
                "chatGrants": {"chat-1": ["ou_user"]},
                "globalGrants": [],
                "oncallChats": [],
                "restrictGrantCommands": false,
                "quotaState": {
                    "chat:chat-1:ou_user": { "limit": 2, "used": 1 }
                }
            }]))
            .unwrap(),
        )
        .unwrap();
        let state = make_state(paths.clone(), HashMap::from([(app_id, bot)]));
        let before = consume_inbound_quota(&state, "app-1", "chat:chat-1:ou_user")
            .await
            .expect("quota consume");
        assert!(before.allowed);
        assert!(before.exhausted);
        let raw = std::fs::read_to_string(paths.bots_json()).unwrap();
        let value: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            value[0]["quotaState"]["chat:chat-1:ou_user"]["used"]
                .as_u64()
                .unwrap(),
            2
        );

        let after = consume_inbound_quota(&state, "app-1", "chat:chat-1:ou_user")
            .await
            .expect("quota consume");
        assert!(!after.allowed);
        assert!(after.exhausted);
        let raw = std::fs::read_to_string(paths.bots_json()).unwrap();
        let value: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            value[0]["quotaState"]["chat:chat-1:ou_user"]["used"]
                .as_u64()
                .unwrap(),
            2
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn schedule_create_appends_to_file() {
        let tmp = std::env::temp_dir().join(format!("beam-sched-test-{}", uuid::Uuid::new_v4()));
        let paths = BeamPaths::from_root(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let task = serde_json::json!({
            "scheduleId": "sched-1",
            "content": "daily at 9am",
            "createdAt": "2026-01-01T00:00:00Z",
            "status": "active",
        });
        let schedules_path = paths.schedules_json();
        std::fs::write(
            &schedules_path,
            serde_json::to_string_pretty(&vec![task]).unwrap(),
        )
        .unwrap();

        let loaded: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(&schedules_path).unwrap()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0]["scheduleId"].as_str().unwrap(), "sched-1");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // -----------------------------------------------------------------------
    // Task 6.3: Worker termination tests
    // -----------------------------------------------------------------------

    /// Spawn a child process, register it in both `state.sessions` and
    /// `state.workers`, then call `terminate_workflow_worker_process`.
    /// Verify that the *`try_wait()` path* detects the child's exit promptly
    /// (well before the 5-second grace), so that a SIGINT-responsive worker
    /// does not suffer a pointless full-grace wait + SIGKILL.
    #[tokio::test]
    async fn terminate_workflow_worker_process_exits_early_via_try_wait() {
        let paths = temp_paths("terminate-trywait");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = make_state(paths.clone(), HashMap::new());

        // Spawn a long-running "worker" (sleep 60).
        let mut child = tokio::process::Command::new("sleep")
            .arg("60")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn sleep");

        let worker_pid = child.id().expect("child should have a pid");
        let session_id = "session-trywait";

        // Register both the session *and* the worker handle so that the
        // grace poll uses try_wait() rather than the zombie-prone kill(0).
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(
                session_id.to_string(),
                Session {
                    session_id: session_id.to_string(),
                    worker_pid: Some(worker_pid),
                    status: SessionStatus::Active,
                    closed_at: None,
                    ..make_session(session_id)
                },
            );
        }
        {
            let stdin = child.stdin.take().expect("stdin");
            state.workers.lock().await.insert(
                session_id.to_string(),
                WorkerHandle {
                    child,
                    stdin: std::sync::Arc::new(tokio::sync::Mutex::new(stdin)),
                },
            );
        }

        // Verify the process is alive before termination.
        let alive_before = unsafe { libc::kill(worker_pid as i32, 0) == 0 };
        assert!(alive_before, "child should be alive before termination");

        // Terminate — `sleep` honours SIGINT, so try_wait should detect the
        // exit within a few poll cycles.
        let start = tokio::time::Instant::now();
        terminate_workflow_worker_process(&state, session_id).await;
        let elapsed = start.elapsed();

        // The grace period is 5 s.  A SIGINT-responsive process should exit
        // *much* faster (typically < 1 s).  We use 3 s as a generous upper
        // bound to prove we didn't wait the full grace.
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "try_wait should detect exit well before 5 s grace, got {:?}",
            elapsed
        );

        // Retrieve the child and verify its exit status.
        let mut child = {
            let mut workers = state.workers.lock().await;
            workers
                .remove(session_id)
                .expect("worker handle should still be there")
                .child
        };
        let exit_status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
            .await
            .expect("wait should not time out")
            .expect("wait should succeed");

        assert!(
            !exit_status.success(),
            "sleep process should be killed by signal (exit status: {:?})",
            exit_status
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    /// Fallback path: when the child is *not* registered in `state.workers`,
    /// `terminate_workflow_worker_process` falls back to `kill(pid, 0)`.  A
    /// responsive child still exits, but the zombie means `kill(0)` keeps
    /// returning success, so we wait the full 5-second grace and escalate to
    /// SIGKILL.  This test documents the *current behaviour* of the fallback
    /// and verifies the child is killed regardless.
    #[tokio::test]
    async fn terminate_workflow_worker_process_fallback_kills_child() {
        let paths = temp_paths("terminate-fallback");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = make_state(paths.clone(), HashMap::new());

        let mut child = tokio::process::Command::new("sleep")
            .arg("60")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn sleep");

        let worker_pid = child.id().expect("child should have a pid");
        let session_id = "session-fallback";

        // Session has worker_pid but *no* worker handle in state.workers.
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(
                session_id.to_string(),
                Session {
                    session_id: session_id.to_string(),
                    worker_pid: Some(worker_pid),
                    status: SessionStatus::Active,
                    closed_at: None,
                    ..make_session(session_id)
                },
            );
        }

        let alive_before = unsafe { libc::kill(worker_pid as i32, 0) == 0 };
        assert!(alive_before, "child should be alive before termination");

        // Without a handle, the grace loop falls back to kill(pid, 0) which
        // is zombie-prone → we'll wait the full grace + send SIGKILL.  The
        // child is killed eventually.
        let start = tokio::time::Instant::now();
        terminate_workflow_worker_process(&state, session_id).await;
        let elapsed = start.elapsed();

        // Fallback path will wait close to the full 5 s grace period.
        assert!(
            elapsed >= std::time::Duration::from_secs(3),
            "fallback should wait at least most of the grace, got {:?}",
            elapsed
        );

        let exit_status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .expect("child wait should not time out")
            .expect("child wait should succeed");

        assert!(
            !exit_status.success(),
            "sleep process should be killed by signal (exit status: {:?})",
            exit_status
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    /// Verify that `terminate_workflow_worker_process` is a no-op when there
    /// is no worker PID (session exists but worker was never spawned).
    #[tokio::test]
    async fn terminate_workflow_worker_process_no_pid_is_noop() {
        let paths = temp_paths("terminate-no-pid");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = make_state(paths.clone(), HashMap::new());
        let session_id = "session-no-pid";

        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(
                session_id.to_string(),
                Session {
                    session_id: session_id.to_string(),
                    worker_pid: None,
                    status: SessionStatus::Active,
                    closed_at: None,
                    ..make_session(session_id)
                },
            );
        }

        // Should not panic or error.
        terminate_workflow_worker_process(&state, session_id).await;

        // Session should still exist and be active.
        {
            let sessions = state.sessions.lock().await;
            let session = sessions.get(session_id).expect("session should exist");
            assert_eq!(session.status, SessionStatus::Active);
            assert!(session.worker_pid.is_none());
        }

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    /// Verify that when we call cancel_run on a run that has an active
    /// cancellation token registered, the token is cancelled immediately
    /// (existing behaviour from Task 6.2), and the registry is cleaned up.
    #[tokio::test]
    async fn cancel_run_clears_registry_and_session_cleanup_works() {
        use beam_core::{BootstrapWorkflowRunInput, bootstrap_workflow_run};

        let paths = temp_paths("cancel-session-cleanup");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = make_state(paths.clone(), HashMap::new());
        let run_id = "run-cancel-cleanup";

        // Bootstrap a human-gate workflow so the run stays in Waiting state.
        let def = r#"{"workflowId":"flow-a","version":1,"nodes":{"nodeA":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hello"},"humanGate":{"stage":"approve","prompt":"wait"}}}}"#;
        let _ = bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-a"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "test",
                chat_binding: None,
            },
        )
        .expect("bootstrap");

        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // Register a fake activity token to simulate active dispatch.
        let reg = crate::workflow_cancellation::global_cancellation_registry();
        let token = reg.register_activity(run_id, &format!("{}::work::nodeA", run_id));
        assert_eq!(reg.total_activities(), 1);

        // Cancel the run.
        let outcome =
            crate::workflow_commands::cancel_run(&state, run_id, Some("test".to_string()))
                .await
                .expect("cancel");

        assert!(outcome.ok);
        assert_eq!(outcome.status, "cancelled");

        // Token should be cancelled and registry cleaned up.
        assert!(token.is_cancelled());
        assert_eq!(reg.total_activities(), 0);

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    // -----------------------------------------------------------------------
    // Task 7.2: cold attach 使用统一 recovery run loop
    // -----------------------------------------------------------------------

    /// Verifies that cold scan discovers non-terminal runs and skips terminal
    /// (succeeded/failed/cancelled) runs, even when both have chat bindings.
    #[tokio::test]
    async fn cold_scan_discovers_non_terminal_and_skips_terminal_runs() {
        use beam_core::{
            BootstrapWorkflowRunInput, EventDraft, EventLog, WorkflowActor, bootstrap_workflow_run,
            scan_cold_workflow_runs,
        };

        let paths = temp_paths("cold-scan-disc");
        maybe_remove_dir(&paths.root().to_path_buf());

        let lark_app_id = "app-cold-scan";
        let def = r#"{"workflowId":"flow-cs","version":1,"nodes":{"a":{"type":"subagent","bot":"bot","prompt":"hello"}}}"#;
        let params: BTreeMap<String, Value> = BTreeMap::new();
        let binding = beam_core::RunChatBinding {
            chat_id: "chat-1".to_string(),
            lark_app_id: lark_app_id.to_string(),
        };

        // Non-terminal run (no terminal event written yet — just bootstrapped).
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-nonterm",
                workflow_json: def,
                expected_workflow_id: Some("flow-cs"),
                params: &params,
                initiator: "test",
                chat_binding: Some(binding.clone()),
            },
        )
        .expect("bootstrap nonterm");

        // Terminal run — write runSucceeded manually.
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id: "run-term",
                workflow_json: def,
                expected_workflow_id: Some("flow-cs"),
                params: &params,
                initiator: "test",
                chat_binding: Some(binding),
            },
        )
        .expect("bootstrap term");
        {
            let mut log = EventLog::new("run-term", paths.workflow_runs_dir()).unwrap();
            log.append(EventDraft {
                event_type: "runSucceeded".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({}),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        }

        let (runs, stats) = scan_cold_workflow_runs(&paths, lark_app_id).await.unwrap();
        assert_eq!(
            stats.discovered, 1,
            "only the non-terminal run should be discovered"
        );
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id, "run-nonterm");
        assert!(
            stats.skipped.is_empty(),
            "no runs should be skipped with errors"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    /// Verifies that cold-attaching a workflow with an open human-gate wait
    /// does NOT terminalize it — the unified driver / run_loop recovery
    /// correctly returns AwaitingWait and leaves the wait dangling.
    #[tokio::test]
    async fn cold_attach_open_human_gate_wait_not_terminalized() {
        use beam_core::{
            BootstrapWorkflowRunInput, RunStatus, bootstrap_workflow_run, read_run_snapshot,
        };

        let paths = temp_paths("cold-attach-open");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = make_state(paths.clone(), HashMap::new());
        let run_id = "run-cold-open";

        // Human-gate workflow: will create a wait and stay in AwaitingWait.
        let def = r#"{"workflowId":"flow-co","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"Approve?"}}}}"#;
        let binding = beam_core::RunChatBinding {
            chat_id: "oc_test".to_string(),
            lark_app_id: "app_test".to_string(),
        };
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-co"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "test",
                chat_binding: Some(binding),
            },
        )
        .expect("bootstrap");

        // Advance once to create the wait.
        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // Verify we have an open wait (not terminal).
        let sn = read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .unwrap();
        assert!(!sn.dangling.waits.is_empty(), "should have an open wait");
        assert!(
            !matches!(
                sn.run.status,
                RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
            ),
            "run should NOT be terminal"
        );

        // Simulate cold attach: call the unified driver again.
        // The driver calls run_loop which has built-in recovery; it should
        // detect the open wait and return AwaitingWait, NOT terminalize.
        workflow_runtime_driver::run(&state, run_id, def).await;

        // After cold attach, the wait should still be open and the run
        // should still be non-terminal.
        let sn2 = read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            !sn2.dangling.waits.is_empty(),
            "open wait should still be dangling after cold attach"
        );
        assert!(
            !matches!(
                sn2.run.status,
                RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
            ),
            "run should NOT be terminal after cold attach with open wait"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    /// Verifies that cold-attaching a workflow whose wait was resolved but
    /// whose terminal event was never written (e.g. crash after resolution)
    /// correctly materializes the terminal via the unified run_loop recovery.
    #[tokio::test]
    async fn cold_attach_recovery_materializes_resolved_wait_terminal() {
        use beam_core::{
            BootstrapWorkflowRunInput, EventDraft, EventLog, WorkflowActor, bootstrap_workflow_run,
        };

        let paths = temp_paths("cold-attach-rec");
        maybe_remove_dir(&paths.root().to_path_buf());
        let state = make_state(paths.clone(), HashMap::new());
        let run_id = "run-cold-rec";

        // Single-node human-gate workflow.
        let def = r#"{"workflowId":"flow-cr","version":1,"nodes":{"gate":{"type":"hostExecutor","executor":"beam-shell","input":{"command":"echo hi"},"humanGate":{"stage":"approve","prompt":"OK?"}}}}"#;
        let binding = beam_core::RunChatBinding {
            chat_id: "oc_test".to_string(),
            lark_app_id: "app_test".to_string(),
        };
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: def,
                expected_workflow_id: Some("flow-cr"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "test",
                chat_binding: Some(binding),
            },
        )
        .expect("bootstrap");

        // Advance to create the wait, then read the wait info.
        crate::run_workflow_runtime_once(&state, run_id, def).await;

        // Grab the activity_id from the wait, and the attempt_id from the
        // activity's latest attempt, so we can craft a valid resolution event.
        let sn = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .unwrap();
        let activity_id = sn
            .dangling
            .waits
            .first()
            .expect("should have a wait")
            .clone();
        // Sanity-check: the activity exists and has an attempt.
        let _activity = sn
            .activities
            .iter()
            .find(|a| a.activity_id == activity_id)
            .expect("should find the waiting activity");
        assert!(
            !_activity.attempts.is_empty(),
            "activity should have at least one attempt"
        );

        // Simulate a crash scenario: write waitResolved (resolution approved)
        // but NOT activitySucceeded (terminal).  This leaves the wait in a
        // "resolved but no terminal" dangling state.
        {
            let mut log = EventLog::new(run_id, paths.workflow_runs_dir()).unwrap();
            log.append(EventDraft {
                event_type: "waitResolved".to_string(),
                actor: WorkflowActor::Human,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "resolution": "approved",
                    "by": "test_user",
                    "comment": "LGTM",
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
        }

        // Verify the snapshot now has a wait resolution but no terminal for
        // the activity — i.e. `dangling.wait_resolutions` is non-empty.
        let sn_pre = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            sn_pre.dangling.waits.is_empty(),
            "after resolution, waits should be cleared"
        );
        assert!(
            !sn_pre.dangling.wait_resolutions.is_empty(),
            "should have dangling wait resolutions (resolved but no terminal)"
        );

        // Simulate cold attach: the unified driver will call run_loop, and
        // the built-in wait-resolution recovery phase should materialize the
        // activitySucceeded terminal.
        workflow_runtime_driver::run(&state, run_id, def).await;

        // After recovery, the wait resolution should be cleared and the
        // activity should have been terminalized.
        let sn_post = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            sn_post.dangling.wait_resolutions.is_empty(),
            "after recovery, dangling wait resolutions should be cleared"
        );
        assert!(sn_post.dangling.waits.is_empty(), "no waits should remain");

        // The workflow should have progressed — since this is a single-node
        // workflow and the node has now succeeded, the run should be terminal.
        let terminal = matches!(
            sn_post.run.status,
            beam_core::RunStatus::Succeeded
                | beam_core::RunStatus::Failed
                | beam_core::RunStatus::Cancelled
        );
        assert!(
            terminal,
            "run should be terminal after recovery, got {:?}",
            sn_post.run.status
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn recent_lark_events_save_load_roundtrip() {
        let paths = temp_paths("recent-lark-events-roundtrip");
        maybe_remove_dir(&paths.root().to_path_buf());

        // Simulate a fresh event (just inserted)
        let mut events = HashMap::new();
        events.insert("evt-fresh".to_string(), std::time::Instant::now());
        // Simulate an event that's 4 minutes old (still within 5 min TTL)
        events.insert(
            "evt-old".to_string(),
            std::time::Instant::now() - std::time::Duration::from_secs(240),
        );

        save_recent_lark_events(&paths, &events).await;
        let loaded = load_recent_lark_events(&paths).await;

        // Both events should survive roundtrip (they're within the 5-min TTL)
        assert!(
            loaded.contains_key("evt-fresh"),
            "fresh event should survive roundtrip"
        );
        assert!(
            loaded.contains_key("evt-old"),
            "4-min-old event should survive roundtrip"
        );

        // The "old" event's Instant should approximate the original (within a few seconds)
        let loaded_instant = loaded.get("evt-old").unwrap();
        let elapsed = loaded_instant.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_secs(239)
                && elapsed <= std::time::Duration::from_secs(242),
            "old event elapsed should be ~240s, got {:?}",
            elapsed
        );

        let _ = std::fs::remove_file(paths.recent_lark_events_json());
        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
