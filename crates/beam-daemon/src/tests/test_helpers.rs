#![allow(dead_code)]
#![allow(unused_imports)]

use axum::{
    Json as AxumJson, Router,
    extract::Path as AxumPath,
    extract::Query as AxumQuery,
    routing::{get, post},
};
use beam_core::{
    AttemptState, BeamPaths, BotConfig, Config, DecisionNode, ReconcileResultState, Session,
    SessionScope, SessionStatus, WaitState, WorkflowDefinition, WorkflowNode, WorkflowOutputRef,
    workflow_definition::NodeBase,
    workflow_snapshot::{ActivityStatus, CancelRequestState},
};
use chrono::Utc;
use feishu_sdk::event::{Event, EventHeader};
use reqwest::Client;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

use crate::{AppState, RunOptions};

pub(crate) fn temp_paths(label: &str) -> BeamPaths {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    BeamPaths::from_root(std::env::temp_dir().join(format!(
        "beam-daemon-{label}-{nanos}-{}",
        std::process::id()
    )))
}

pub(crate) fn maybe_remove_dir(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

pub(crate) fn lark_base_url_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

pub(crate) struct LarkBaseUrlEnvGuard {
    old_value: Option<String>,
}

impl LarkBaseUrlEnvGuard {
    pub(crate) fn set(value: &str) -> Self {
        let old_value = std::env::var("BEAM_LARK_BASE_URL").ok();
        unsafe {
            std::env::set_var("BEAM_LARK_BASE_URL", value);
        }
        Self { old_value }
    }
}

impl Drop for LarkBaseUrlEnvGuard {
    fn drop(&mut self) {
        if let Some(old_value) = self.old_value.take() {
            unsafe {
                std::env::set_var("BEAM_LARK_BASE_URL", old_value);
            }
        } else {
            unsafe {
                std::env::remove_var("BEAM_LARK_BASE_URL");
            }
        }
    }
}

pub(crate) async fn start_mock_lark_server() -> String {
    let app = Router::new()
        .route(
            "/auth/v3/tenant_access_token/internal",
            post(|| async {
                AxumJson(serde_json::json!({
                    "code": 0,
                    "tenant_access_token": "mock-token",
                    "expire": 7200,
                }))
            }),
        )
        .route(
            "/im/v1/messages/{message_id}",
            get(|AxumPath(message_id): AxumPath<String>| async move {
                let thread_id = if message_id == "om_root_thread" {
                    "omt_mock_thread"
                } else {
                    "omt_detail_thread"
                };
                AxumJson(serde_json::json!({
                    "code": 0,
                    "data": { "items": [{
                        "message_id": message_id,
                        "chat_id": "chat-1",
                        "thread_id": thread_id,
                        "msg_type": "text",
                        "body": { "content": serde_json::json!({ "text": "quoted detail" }).to_string() },
                        "sender": { "id": "ou_detail", "sender_type": "user" },
                        "create_time": "1000",
                    }] }
                }))
            })
            .patch(|AxumPath(_message_id): AxumPath<String>| async {
                AxumJson(serde_json::json!({ "code": 0 }))
            }),
        )
        .route(
            "/im/v1/chats/{chat_id}",
            get(|AxumPath(_chat_id): AxumPath<String>| async {
                AxumJson(serde_json::json!({
                    "code": 0,
                    "data": {
                        "chat_mode": "topic",
                        "group_message_type": "thread",
                        "user_count": 1,
                        "bot_count": 0,
                    }
                }))
            }),
        )
        .route(
            "/im/v1/messages/{message_id}/reply",
            post(
                |AxumPath(_msg_id): AxumPath<String>,
                 AxumJson(_body): AxumJson<serde_json::Value>| async {
                    AxumJson(serde_json::json!({
                        "code": 0,
                        "data": { "message_id": "om_reply_mock" },
                    }))
                },
            ),
        )
        .route(
            "/im/v1/messages",
            get(|AxumQuery(params): AxumQuery<HashMap<String, String>>| async move {
                let items = match params.get("container_id_type").map(String::as_str) {
                    Some("thread") => vec![
                        serde_json::json!({
                            "message_id": "om_thread_1",
                            "chat_id": "chat-1",
                            "thread_id": params.get("container_id").cloned().unwrap_or_default(),
                            "msg_type": "text",
                            "body": { "content": serde_json::json!({ "text": "thread one" }).to_string() },
                            "sender": { "id": "ou_a", "sender_type": "user" },
                            "create_time": "1000",
                        }),
                        serde_json::json!({
                            "message_id": "om_thread_2",
                            "chat_id": "chat-1",
                            "root_id": "om_root_thread",
                            "thread_id": params.get("container_id").cloned().unwrap_or_default(),
                            "msg_type": "text",
                            "body": { "content": serde_json::json!({ "text": "thread two" }).to_string() },
                            "sender": { "id": "ou_b", "sender_type": "user" },
                            "create_time": "2000",
                        }),
                    ],
                    _ => vec![
                        serde_json::json!({
                            "message_id": "om_chat_new",
                            "chat_id": "chat-1",
                            "msg_type": "text",
                            "body": { "content": serde_json::json!({ "text": "chat newest" }).to_string() },
                            "sender": { "id": "ou_c", "sender_type": "user" },
                            "create_time": "3000",
                        }),
                        serde_json::json!({
                            "message_id": "om_chat_old",
                            "chat_id": "chat-1",
                            "msg_type": "text",
                            "body": { "content": serde_json::json!({ "text": "chat oldest" }).to_string() },
                            "sender": { "id": "ou_d", "sender_type": "user" },
                            "create_time": "2000",
                        }),
                    ],
                };
                AxumJson(serde_json::json!({
                    "code": 0,
                    "data": { "items": items, "page_token": "" },
                }))
            })
            .post(
                |AxumQuery(_params): AxumQuery<HashMap<String, String>>,
                 AxumJson(_body): AxumJson<serde_json::Value>| async {
                    AxumJson(serde_json::json!({
                        "code": 0,
                        "data": { "message_id": "om_send_mock" },
                    }))
                },
            ),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock lark server");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

pub(crate) fn mock_card_action_event(body: serde_json::Value) -> Event {
    Event {
        schema: Some("2.0".to_string()),
        header: Some(EventHeader {
            event_type: Some("card.action.trigger".to_string()),
            ..Default::default()
        }),
        event: Some(body),
        ..Default::default()
    }
}

pub(crate) fn test_output_ref() -> WorkflowOutputRef {
    WorkflowOutputRef {
        output_hash: "sha256:test".to_string(),
        output_path: "/tmp/test".to_string(),
        output_bytes: 4,
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    }
}

pub(crate) fn test_attempt(
    attempt_id: &str,
    status: ActivityStatus,
    wait: Option<WaitState>,
    effect_attempted: Option<beam_core::EffectAttemptedState>,
    cancel_request: Option<CancelRequestState>,
    reconcile_result: Option<ReconcileResultState>,
) -> AttemptState {
    AttemptState {
        attempt_id: attempt_id.to_string(),
        attempt_number: 1,
        input_ref: test_output_ref(),
        status,
        lease_id: None,
        timeout_ms: None,
        max_output_bytes: None,
        effect_attempted,
        latest_reconcile_result: reconcile_result,
        cancel_request,
        wait,
        output: None,
        external_refs: None,
        error: None,
        running_ms: None,
        cancel_origin_event_id: None,
    }
}

pub(crate) fn test_decision_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        workflow_id: "flow-a".to_string(),
        version: 1,
        params: None,
        defaults: None,
        nodes: BTreeMap::from([(
            "decision".to_string(),
            WorkflowNode::Decision(DecisionNode {
                base: NodeBase {
                    description: None,
                    depends: None,
                    human_gate: None,
                    retry_policy: None,
                    timeout_ms: None,
                    max_output_bytes: None,
                    output_schema: None,
                    unsafe_allow_ungated: None,
                },
            }),
        )]),
    }
}

pub(crate) fn make_session(session_id: &str) -> Session {
    Session {
        session_id: session_id.to_string(),
        title: format!("session {}", session_id),
        chat_id: "chat-1".to_string(),
        root_message_id: "root-1".to_string(),
        chat_type: Some("group".to_string()),
        quote_target_id: None,
        scope: SessionScope::Thread,
        status: SessionStatus::Closed,
        created_at: Utc::now(),
        closed_at: Some(Utc::now()),
        working_dir: Some("/tmp/project".to_string()),
        lark_app_id: "app-1".to_string(),
        owner_open_id: None,
        quote_target_sender_open_id: None,
        worker_pid: None,
        cli_id: Some("codex".to_string()),
        cli_bin: Some("codex".to_string()),
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
        last_explicit_send_at: None,
        adopted_from: None,
        bot_name: None,
        bot_open_id: None,
        disable_cli_bypass: false,
        initial_prompt: None,
        model: None,
        locale: None,
        resume_session_id: None,
        thread_id: None,
        agent_attention: None,
    }
}

pub(crate) fn make_bot(app_id: &str) -> BotConfig {
    BotConfig {
        name: None,
        lark_app_id: app_id.to_string(),
        lark_app_secret: "secret".to_string(),
        cli_id: "codex".to_string(),
        cli_bin: None,
        cli_args: Vec::new(),
        skip_working_dir_prompt: false,
        model: None,
        working_dir: None,
        lark_encrypt_key: None,
        lark_verification_token: None,
        allowed_users: Vec::new(),
        private_card: false,
        allowed_chat_groups: Vec::new(),
        chat_grants: std::collections::HashMap::new(),
        global_grants: Vec::new(),
        oncall_chats: Vec::new(),
        restrict_grant_commands: false,
        message_quota: None,
        quota_state: std::collections::HashMap::new(),
    }
}

pub(crate) fn make_state(paths: BeamPaths, bots: HashMap<String, BotConfig>) -> AppState {
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();
    AppState {
        paths,
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
        bots: Arc::new(bots),
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
