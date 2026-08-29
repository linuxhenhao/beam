#![allow(clippy::await_holding_lock)]

use crate::tests::test_helpers::*;

use axum::{
    Json, Router,
    extract::Path as AxumPath,
    routing::{get, post},
};
use beam_core::{BotConfig, CustomTrigger, SessionScope, SessionStatus};
use serde_json::Value;
use std::collections::HashMap;

use crate::handle_lark_event_payload;

#[test]
fn handle_lark_event_uses_api_to_detect_topic_group() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("detect-topic");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-topic";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
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
            custom_triggers: Vec::new(),
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-topic-1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-topic-1",
                    "chat_id": "chat-topic-1",
                    "chat_type": "group",
                    "content": "{\"text\":\"hello\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        // With directory selection, new sessions are NOT created immediately.
        // Instead a dir-select card is sent. Verify the pending entry was stored
        // with the correct Thread scope.
        let pending_creates = state.pending_creates.lock().await;
        assert!(
            !pending_creates.is_empty(),
            "pending create entry should be stored when no active session exists"
        );
        let pending = pending_creates.values().next().unwrap();
        assert_eq!(
            pending.scope,
            SessionScope::Thread,
            "pending create should have Thread scope when API detects topic group"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}

#[test]
fn handle_lark_event_trigger_creates_session_with_bot_default_dir() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("trigger-default-dir");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-trigger-dir";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: Some("/bot/default".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_user".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: vec![CustomTrigger {
                trigger: "/base".to_string(),
                prompt: Some("阅读群聊上下文，尝试定位和解决问题".to_string()),
                skip_dir_select: true,
                working_dir: None,
                ack_message: None,
            }],
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-trigger-1" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-trigger-1",
                    "chat_id": "chat-trigger-1",
                    "chat_type": "group",
                    "content": "{\"text\":\"/base\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        // The trigger skips the dir-select card entirely.
        let pending_creates = state.pending_creates.lock().await;
        assert!(
            pending_creates.is_empty(),
            "trigger-activated session must not create a dir-select pending entry"
        );
        drop(pending_creates);

        // A session was created directly in the bot's default working dir.
        let sessions = state.sessions.lock().await;
        let session = sessions
            .values()
            .find(|s| s.lark_app_id == app_id)
            .expect("session should be created by the trigger");
        assert_eq!(session.working_dir.as_deref(), Some("/bot/default"));
        assert_eq!(session.status, SessionStatus::Active);

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}

#[test]
fn handle_lark_event_trigger_uses_trigger_working_dir() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("trigger-working-dir");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-trigger-dir-pinned";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: Some("/bot/default".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_user".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: vec![CustomTrigger {
                trigger: "/work".to_string(),
                prompt: Some("开工".to_string()),
                skip_dir_select: true,
                working_dir: Some("/trigger/dir".to_string()),
                ack_message: None,
            }],
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-trigger-dir" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-trigger-dir",
                    "chat_id": "chat-trigger-dir",
                    "chat_type": "group",
                    "content": "{\"text\":\"/work\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        let pending_creates = state.pending_creates.lock().await;
        assert!(pending_creates.is_empty());
        drop(pending_creates);

        let sessions = state.sessions.lock().await;
        let session = sessions
            .values()
            .find(|s| s.lark_app_id == app_id)
            .expect("session should be created by the trigger");
        assert_eq!(session.working_dir.as_deref(), Some("/trigger/dir"));

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}

#[test]
fn handle_lark_event_trigger_without_skip_dir_select_shows_card() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let paths = temp_paths("trigger-dir-select");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-trigger-card";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: Some("/bot/default".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_user".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: vec![CustomTrigger {
                trigger: "/ask".to_string(),
                prompt: None,
                skip_dir_select: false,
                working_dir: None,
                ack_message: None,
            }],
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-trigger-card" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-trigger-card",
                    "chat_id": "chat-trigger-card",
                    "chat_type": "group",
                    "content": "{\"text\":\"/ask\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        // Without skipDirSelect the trigger still shows the dir-select card.
        let pending_creates = state.pending_creates.lock().await;
        assert!(
            !pending_creates.is_empty(),
            "trigger without skipDirSelect must show the directory selection card"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}

#[test]
fn handle_lark_event_trigger_sends_ack_reply() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");

        let reply_requests: std::sync::Arc<tokio::sync::Mutex<Vec<Value>>> =
            std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let reply_state = reply_requests.clone();
        let app = Router::new()
            .route(
                "/auth/v3/tenant_access_token/internal",
                post(|| async {
                    Json(serde_json::json!({
                        "code": 0,
                        "tenant_access_token": "mock-token",
                        "expire": 7200,
                    }))
                }),
            )
            .route(
                "/im/v1/chats/{chat_id}",
                get(|AxumPath(_chat_id): AxumPath<String>| async {
                    Json(serde_json::json!({
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
                    move |AxumPath(_message_id): AxumPath<String>, Json(body): Json<Value>| {
                        let reply_state = reply_state.clone();
                        async move {
                            reply_state.lock().await.push(body);
                            Json(serde_json::json!({
                                "code": 0,
                                "data": { "message_id": "om_reply" },
                            }))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let _env_guard = LarkBaseUrlEnvGuard::set(&format!("http://{}", addr));

        let paths = temp_paths("trigger-ack");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-trigger-ack";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: Some("/bot/default".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_user".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: vec![CustomTrigger {
                trigger: "/base".to_string(),
                prompt: Some("阅读群聊上下文，尝试定位和解决问题".to_string()),
                skip_dir_select: true,
                working_dir: None,
                ack_message: Some("收到，正在处理，请稍候".to_string()),
            }],
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-trigger-ack" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-trigger-ack",
                    "chat_id": "chat-trigger-ack",
                    "chat_type": "group",
                    "content": "{\"text\":\"/base\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        let requests = reply_requests.lock().await;
        assert_eq!(requests.len(), 1, "exactly one ack reply should be sent");
        let content = requests[0]
            .get("content")
            .and_then(Value::as_str)
            .expect("reply content");
        assert!(
            content.contains("收到，正在处理，请稍候"),
            "ack text missing from reply: {}",
            content
        );

        let sessions = state.sessions.lock().await;
        assert!(
            sessions.values().any(|s| s.lark_app_id == app_id),
            "session should be created after the ack"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}

#[test]
fn handle_lark_event_trigger_activates_in_new_topic_despite_chat_session() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");

        let reply_requests: std::sync::Arc<tokio::sync::Mutex<Vec<Value>>> =
            std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let reply_state = reply_requests.clone();
        let app = Router::new()
            .route(
                "/auth/v3/tenant_access_token/internal",
                post(|| async {
                    Json(serde_json::json!({
                        "code": 0,
                        "tenant_access_token": "mock-token",
                        "expire": 7200,
                    }))
                }),
            )
            .route(
                "/im/v1/chats/{chat_id}",
                get(|AxumPath(_chat_id): AxumPath<String>| async {
                    Json(serde_json::json!({
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
                    move |AxumPath(_message_id): AxumPath<String>, Json(body): Json<Value>| {
                        let reply_state = reply_state.clone();
                        async move {
                            reply_state.lock().await.push(body);
                            Json(serde_json::json!({
                                "code": 0,
                                "data": { "message_id": "om_reply" },
                            }))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let _env_guard = LarkBaseUrlEnvGuard::set(&format!("http://{}", addr));

        let paths = temp_paths("trigger-inactive");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-trigger-inactive";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: true,
            model: None,
            working_dir: Some("/bot/default".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_user".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: vec![CustomTrigger {
                trigger: "日报".to_string(),
                prompt: Some("TRIGGER_PROMPT_MARKER".to_string()),
                skip_dir_select: true,
                working_dir: Some("/trigger/dir".to_string()),
                ack_message: Some("ACK_MARKER".to_string()),
            }],
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        // The chat owns an active Chat-scope session, but the message lands in
        // a brand-new topic. Each topic is its own Thread anchor, so the
        // trigger still activates: ack, prompt, and pinned working dir apply.
        let mut seeded = make_session("seeded-chat-session");
        seeded.status = SessionStatus::Active;
        seeded.closed_at = None;
        seeded.scope = SessionScope::Chat;
        seeded.chat_id = "chat-trigger-inactive".to_string();
        seeded.lark_app_id = app_id.to_string();
        seeded.root_message_id = "seed-root".to_string();
        state
            .sessions
            .lock()
            .await
            .insert(seeded.session_id.clone(), seeded);

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-trigger-inactive" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-trigger-inactive",
                    "chat_id": "chat-trigger-inactive",
                    "chat_type": "group",
                    "content": "{\"text\":\"日报 今天修了三个 bug\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        // The trigger activates, so exactly one ack reply is sent.
        let requests = reply_requests.lock().await;
        assert_eq!(requests.len(), 1, "one ack reply should be sent");
        let content = requests[0]
            .get("content")
            .and_then(Value::as_str)
            .expect("reply content");
        assert!(
            content.contains("ACK_MARKER"),
            "ack text missing from reply: {}",
            content
        );
        drop(requests);

        // The new topic gets its own session with the trigger's pinned working
        // dir and keyword title, independent of the chat-scope session.
        let sessions = state.sessions.lock().await;
        let created = sessions
            .values()
            .find(|s| s.session_id != "seeded-chat-session")
            .expect("a session should be created for the new topic");
        assert_eq!(created.scope, SessionScope::Thread);
        assert_eq!(created.working_dir.as_deref(), Some("/trigger/dir"));
        assert_eq!(created.title, "日报");
        assert_eq!(created.chat_id, "chat-trigger-inactive");

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}

#[test]
fn handle_lark_event_trigger_keyword_in_existing_topic_does_not_reinject() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");

        let reply_requests: std::sync::Arc<tokio::sync::Mutex<Vec<Value>>> =
            std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let reply_state = reply_requests.clone();
        let app = Router::new()
            .route(
                "/auth/v3/tenant_access_token/internal",
                post(|| async {
                    Json(serde_json::json!({
                        "code": 0,
                        "tenant_access_token": "mock-token",
                        "expire": 7200,
                    }))
                }),
            )
            .route(
                "/im/v1/chats/{chat_id}",
                get(|AxumPath(_chat_id): AxumPath<String>| async {
                    Json(serde_json::json!({
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
                    move |AxumPath(_message_id): AxumPath<String>, Json(body): Json<Value>| {
                        let reply_state = reply_state.clone();
                        async move {
                            reply_state.lock().await.push(body);
                            Json(serde_json::json!({
                                "code": 0,
                                "data": { "message_id": "om_reply" },
                            }))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let _env_guard = LarkBaseUrlEnvGuard::set(&format!("http://{}", addr));

        let paths = temp_paths("trigger-existing-topic");
        maybe_remove_dir(&paths.root().to_path_buf());

        let app_id = "app-trigger-existing";
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: true,
            model: None,
            working_dir: Some("/bot/default".to_string()),
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_user".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: vec![CustomTrigger {
                trigger: "日报".to_string(),
                prompt: Some("TRIGGER_PROMPT_MARKER".to_string()),
                skip_dir_select: true,
                working_dir: Some("/trigger/dir".to_string()),
                ack_message: Some("ACK_MARKER".to_string()),
            }],
        };
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        // The message lands inside a topic that already owns an active Thread
        // session, so the trigger must not re-inject its prompt/ack.
        let mut seeded = make_session("seeded-topic-session");
        seeded.status = SessionStatus::Active;
        seeded.closed_at = None;
        seeded.scope = SessionScope::Thread;
        seeded.chat_id = "chat-trigger-existing".to_string();
        seeded.lark_app_id = app_id.to_string();
        seeded.root_message_id = "seed-root".to_string();
        seeded.thread_id = Some("omt_topic_x".to_string());
        state
            .sessions
            .lock()
            .await
            .insert(seeded.session_id.clone(), seeded);

        let payload = serde_json::json!({
            "header": { "event_type": "im.message.receive_v1", "event_id": "evt-trigger-existing" },
            "event": {
                "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                "message": {
                    "message_id": "msg-trigger-existing",
                    "chat_id": "chat-trigger-existing",
                    "chat_type": "group",
                    "thread_id": "omt_topic_x",
                    "content": "{\"text\":\"日报 继续\"}",
                    "mentions": []
                }
            }
        });

        let result =
            handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
        assert!(result.is_ok());

        // No ack: the topic already owns a session, so the keyword is just a
        // normal follow-up.
        let requests = reply_requests.lock().await;
        assert!(
            requests.is_empty(),
            "trigger must not ack inside a topic that already owns a session: {:?}",
            *requests
        );
        drop(requests);

        // No new session: the follow-up is routed to the existing topic session.
        let sessions = state.sessions.lock().await;
        assert_eq!(sessions.len(), 1, "no new session should be created");
        assert!(sessions.contains_key("seeded-topic-session"));

        maybe_remove_dir(&paths.root().to_path_buf());
    });
}
