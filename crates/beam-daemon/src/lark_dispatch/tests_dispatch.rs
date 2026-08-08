use super::*;
use crate::tests::test_helpers::*;

use axum::{
    Json, Router,
    extract::Path as AxumPath,
    routing::{get, post},
};
use beam_core::{BotConfig, CustomTrigger, SessionScope, SessionStatus};
use serde_json::Value;
use std::collections::HashMap;

use crate::{LarkEventOutcome, ParsedLarkInboundMessage, handle_lark_event_payload};

#[test]
fn decide_lark_dispatch_reuses_chat_scope_session_for_quote_bubble_messages() {
    let mut session = make_session("chat-session");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.scope = SessionScope::Chat;
    session.chat_id = "chat-2".to_string();
    session.root_message_id = "seed-root".to_string();

    let sessions = HashMap::from([(session.session_id.clone(), session.clone())]);
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-2".to_string(),
        message_id: "msg-2".to_string(),
        chat_id: "chat-2".to_string(),
        chat_type: Some("group".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Chat,
        anchor: "chat-2".to_string(),
        text: "continue please".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert_eq!(
        existing.map(|session| session.session_id),
        Some("chat-session".to_string())
    );
    assert_eq!(outcome, LarkEventOutcome::ReuseSession);
}

#[test]
fn decide_lark_dispatch_slash_trigger_creates_session() {
    let sessions = HashMap::new();
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-slash-1".to_string(),
        message_id: "msg-slash-1".to_string(),
        chat_id: "chat-slash".to_string(),
        chat_type: Some("group".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Chat,
        anchor: "chat-slash".to_string(),
        text: "/日报".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };
    let trigger = CustomTrigger {
        trigger: "/日报".to_string(),
        prompt: Some("生成今日日报".to_string()),
        skip_dir_select: false,
        working_dir: None,
        ack_message: None,
    };

    // A configured slash trigger activates the session instead of being
    // routed as a passthrough command.
    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, Some(&trigger));
    assert!(existing.is_none());
    assert_eq!(outcome, LarkEventOutcome::CreateSession);

    // Without the trigger, "/日报" is a passthrough command and is rejected
    // because no active session exists.
    let (_, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(matches!(outcome, LarkEventOutcome::ReplyOnly { .. }));
}

#[test]
fn decide_lark_dispatch_slash_trigger_keeps_passthrough_with_existing_session() {
    let mut session = make_session("slash-session");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.scope = SessionScope::Chat;
    session.chat_id = "chat-slash-2".to_string();
    session.root_message_id = "seed-root".to_string();
    let sessions = HashMap::from([(session.session_id.clone(), session)]);
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-slash-2".to_string(),
        message_id: "msg-slash-2".to_string(),
        chat_id: "chat-slash-2".to_string(),
        chat_type: Some("group".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Chat,
        anchor: "chat-slash-2".to_string(),
        text: "/日报".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };
    let trigger = CustomTrigger {
        trigger: "/日报".to_string(),
        prompt: Some("生成今日日报".to_string()),
        skip_dir_select: false,
        working_dir: None,
        ack_message: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, Some(&trigger));
    assert_eq!(
        existing.map(|session| session.session_id),
        Some("slash-session".to_string())
    );
    // With an active session the trigger gets no special treatment: the
    // slash-prefixed message keeps its normal passthrough behavior.
    assert_eq!(
        outcome,
        LarkEventOutcome::PassthroughInput {
            text: "/日报".to_string()
        }
    );
}

#[test]
fn decide_lark_dispatch_creates_new_thread_when_only_chat_scope_session_exists() {
    let mut chat_session = make_session("chat-session");
    chat_session.status = SessionStatus::Active;
    chat_session.closed_at = None;
    chat_session.scope = SessionScope::Chat;
    chat_session.chat_id = "chat-3".to_string();
    chat_session.root_message_id = "chat-3".to_string();

    let sessions = HashMap::from([(chat_session.session_id.clone(), chat_session)]);
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-3".to_string(),
        message_id: "msg-3".to_string(),
        chat_id: "chat-3".to_string(),
        chat_type: Some("group".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "real-topic-root".to_string(),
        text: "new topic please".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(existing.is_none());
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_reuses_topic_session_by_chat_id_without_thread_metadata() {
    // Without thread_id, Thread-scoped sessions no longer match
    // via chat_id fallback (the fallback was removed).  A new
    // session must be created.
    let mut topic_session = make_session("topic-session");
    topic_session.status = SessionStatus::Active;
    topic_session.closed_at = None;
    topic_session.scope = SessionScope::Thread;
    topic_session.chat_id = "topic-chat-1".to_string();
    topic_session.chat_type = Some("topic".to_string());
    topic_session.root_message_id = "first-topic-message".to_string();

    let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session)]);
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-topic-2".to_string(),
        message_id: "second-topic-message".to_string(),
        chat_id: "topic-chat-1".to_string(),
        chat_type: Some("topic".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "second-topic-message".to_string(),
        text: "same topic follow-up".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(
        existing.is_none(),
        "without thread_id on either side, no match is expected"
    );
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_does_not_reuse_group_forced_topic_by_chat_id() {
    let mut topic_session = make_session("topic-session");
    topic_session.status = SessionStatus::Active;
    topic_session.closed_at = None;
    topic_session.scope = SessionScope::Thread;
    topic_session.chat_id = "group-chat-1".to_string();
    topic_session.root_message_id = "first-forced-topic".to_string();

    let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session)]);
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-topic-2".to_string(),
        message_id: "second-forced-topic".to_string(),
        chat_id: "group-chat-1".to_string(),
        chat_type: Some("group".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "second-forced-topic".to_string(),
        text: "new forced topic".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(existing.is_none());
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_creates_new_session_when_thread_id_missing() {
    // When a topic session exists but has no thread_id, and a new
    // message arrives with root_id but no thread_id, the session
    // does NOT match (thread_id on session is None, anchor is a
    // message_id).  A new session is created.
    let mut topic_session = make_session("topic-session-id");
    topic_session.status = SessionStatus::Active;
    topic_session.closed_at = None;
    topic_session.scope = SessionScope::Thread;
    topic_session.chat_id = "topic-chat-reuse".to_string();
    topic_session.chat_type = Some("topic".to_string());
    topic_session.root_message_id = "first-topic-msg".to_string();

    let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-topic-2".to_string(),
        message_id: "second-topic-msg".to_string(),
        chat_id: "topic-chat-reuse".to_string(),
        chat_type: Some("topic".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "first-topic-msg".to_string(),
        text: "follow-up in same topic".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: Some("first-topic-msg".to_string()),
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(existing.is_none());
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_reuses_topic_session_with_root_id_and_thread_id() {
    // Thread-scoped session with thread_id="omt_thread".  A new
    // message with the same thread_id should match.
    let mut topic_session = make_session("topic-session-full");
    topic_session.status = SessionStatus::Active;
    topic_session.closed_at = None;
    topic_session.scope = SessionScope::Thread;
    topic_session.chat_id = "topic-full-chat".to_string();
    topic_session.chat_type = Some("topic".to_string());
    topic_session.root_message_id = "topic-root-msg".to_string();
    topic_session.thread_id = Some("omt_thread".to_string());

    let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-full".to_string(),
        message_id: "later-msg".to_string(),
        chat_id: "topic-full-chat".to_string(),
        chat_type: Some("topic".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "omt_thread".to_string(),
        text: "later message".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: Some("topic-root-msg".to_string()),
        thread_id: Some("omt_thread".to_string()),
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert_eq!(
        existing.map(|session| session.session_id),
        Some("topic-session-full".to_string())
    );
    assert_eq!(outcome, LarkEventOutcome::ReuseSession);
}

#[test]
fn decide_lark_dispatch_no_fallback_creates_session_when_anchor_mismatches() {
    // The chat_type-based fallback was removed.  Without thread_id
    // on the session, a new message cannot match even if it's in
    // the same chat.  A new session is created.
    let mut topic_session = make_session("topic-fb-session");
    topic_session.status = SessionStatus::Active;
    topic_session.closed_at = None;
    topic_session.scope = SessionScope::Thread;
    topic_session.chat_id = "topic-fb-chat".to_string();
    topic_session.chat_type = Some("topic".to_string());
    topic_session.root_message_id = "first-msg".to_string();

    let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-fb".to_string(),
        message_id: "second-msg".to_string(),
        chat_id: "topic-fb-chat".to_string(),
        chat_type: Some("topic".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "second-msg".to_string(),
        text: "another message".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(
        existing.is_none(),
        "no fallback: without thread_id, new session is created"
    );
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_creates_new_session_for_different_root_id_in_topic_chat() {
    // When a topic session exists with root_message_id "topic-a-root"
    // and a new message arrives in the SAME topic-mode chat but with
    // a DIFFERENT root_id ("topic-b-root"), the exact anchor match
    // fails.  Because root_id IS present (not None), the fallback by
    // chat_id should NOT trigger.  A new session should be created
    // so the different topic gets its own independent session and
    // directory selection.
    let mut topic_session = make_session("topic-existing");
    topic_session.status = SessionStatus::Active;
    topic_session.closed_at = None;
    topic_session.scope = SessionScope::Thread;
    topic_session.chat_id = "topic-multi".to_string();
    topic_session.chat_type = Some("topic".to_string());
    topic_session.root_message_id = "topic-a-root".to_string();

    let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

    // Message with different root_id — should NOT match the existing session.
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-diff-topic".to_string(),
        message_id: "topic-b-msg".to_string(),
        chat_id: "topic-multi".to_string(),
        chat_type: Some("topic".to_string()),
        sender_type: Some("user".to_string()),
        // anchor = root_id = "topic-b-root" (set by decide_lark_routing fix)
        scope: SessionScope::Thread,
        anchor: "topic-b-root".to_string(),
        text: "different topic message".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: Some("topic-b-root".to_string()),
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    // Exact match fails (root_message_id mismatch); root_id is Some
    // so fallback does NOT trigger.  Must create a new session.
    assert!(
        existing.is_none(),
        "different root_id should NOT reuse existing topic session"
    );
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_creates_new_session_for_different_thread_id() {
    // Two messages in the same chat but with different thread_ids
    // must create separate sessions.
    let mut session_a = make_session("topic-a");
    session_a.status = SessionStatus::Active;
    session_a.closed_at = None;
    session_a.scope = SessionScope::Thread;
    session_a.chat_id = "multi-topic-chat".to_string();
    session_a.thread_id = Some("omt_topic_a".to_string());

    let sessions = HashMap::from([(session_a.session_id.clone(), session_a)]);

    // Message for a DIFFERENT topic thread in the same chat
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-diff-thread".to_string(),
        message_id: "msg-topic-b".to_string(),
        chat_id: "multi-topic-chat".to_string(),
        chat_type: Some("topic".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "omt_topic_b".to_string(),
        text: "different topic".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: Some("topic-b-root".to_string()),
        thread_id: Some("omt_topic_b".to_string()),
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(
        existing.is_none(),
        "different thread_id must create a new session"
    );
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

#[test]
fn decide_lark_dispatch_p2p_follow_up_reuses_session_by_root_id() {
    // First p2p message: creates a session with root_message_id="msg-p2p-1",
    // thread_id=None, chat_type="p2p".
    let mut p2p_session = make_session("p2p-first");
    p2p_session.status = SessionStatus::Active;
    p2p_session.closed_at = None;
    p2p_session.scope = SessionScope::Thread;
    p2p_session.chat_id = "dm-chat".to_string();
    p2p_session.chat_type = Some("p2p".to_string());
    p2p_session.root_message_id = "msg-p2p-1".to_string();
    p2p_session.thread_id = None;

    let sessions = HashMap::from([(p2p_session.session_id.clone(), p2p_session)]);

    // Follow-up p2p message in the same thread: carries root_id pointing to
    // the first message.  Routing uses root_id as anchor, and
    // session_anchor_matches falls back to root_message_id because
    // thread_id is None.
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-p2p-2".to_string(),
        message_id: "msg-p2p-2".to_string(),
        chat_id: "dm-chat".to_string(),
        chat_type: Some("p2p".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "msg-p2p-1".to_string(), // root_id from routing
        text: "follow-up message".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: Some("msg-p2p-1".to_string()),
        root_id: Some("msg-p2p-1".to_string()),
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert_eq!(
        existing.as_ref().map(|s| s.session_id.as_str()),
        Some("p2p-first"),
        "p2p follow-up with root_id should reuse the existing session"
    );
    assert_eq!(outcome, LarkEventOutcome::ReuseSession);
}

#[test]
fn decide_lark_dispatch_p2p_after_thread_id_backfill_still_reuses_by_root_id() {
    // After thread_id has been backfilled, a follow-up with root_id
    // (which routes to anchor=root_id) must still match via the
    // root_message_id fallback, NOT get blocked by thread_id mismatch.
    let mut p2p_session = make_session("p2p-backfilled");
    p2p_session.status = SessionStatus::Active;
    p2p_session.closed_at = None;
    p2p_session.scope = SessionScope::Thread;
    p2p_session.chat_id = "dm-chat".to_string();
    p2p_session.chat_type = Some("p2p".to_string());
    p2p_session.root_message_id = "first-msg".to_string();
    // thread_id was backfilled from a previous follow-up
    p2p_session.thread_id = Some("omt_thread".to_string());

    let sessions = HashMap::from([(p2p_session.session_id.clone(), p2p_session)]);

    // Another follow-up in the same thread: carries both root_id and
    // thread_id.  Routing prefers root_id → anchor="first-msg".
    // Must match via root_message_id fallback even though thread_id is
    // already Some (the old thread_id.is_none() guard would block this).
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-p2p-3".to_string(),
        message_id: "msg-p2p-3".to_string(),
        chat_id: "dm-chat".to_string(),
        chat_type: Some("p2p".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "first-msg".to_string(), // root_id from routing
        text: "another follow-up".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: Some("msg-p2p-2".to_string()),
        root_id: Some("first-msg".to_string()),
        thread_id: Some("omt_thread".to_string()),
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert_eq!(
        existing.as_ref().map(|s| s.session_id.as_str()),
        Some("p2p-backfilled"),
        "p2p follow-up with root_id must reuse session even after thread_id backfill"
    );
    assert_eq!(outcome, LarkEventOutcome::ReuseSession);
}

#[test]
fn decide_lark_dispatch_p2p_new_message_does_not_reuse_session() {
    // A fresh p2p message (no root_id/thread_id) must not reuse an
    // existing p2p session, even if it's in the same p2p chat.
    let mut p2p_session = make_session("p2p-existing");
    p2p_session.status = SessionStatus::Active;
    p2p_session.closed_at = None;
    p2p_session.scope = SessionScope::Thread;
    p2p_session.chat_id = "dm-chat".to_string();
    p2p_session.chat_type = Some("p2p".to_string());
    p2p_session.root_message_id = "old-msg".to_string();
    p2p_session.thread_id = None;

    let sessions = HashMap::from([(p2p_session.session_id.clone(), p2p_session)]);

    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-p2p-new".to_string(),
        message_id: "new-msg".to_string(),
        chat_id: "dm-chat".to_string(),
        chat_type: Some("p2p".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "new-msg".to_string(), // message_id as anchor (no root_id)
        text: "brand new message".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: Vec::new(),
        parent_id: None,
        root_id: None,
        thread_id: None,
        locale: None,
    };

    let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed, None);
    assert!(
        existing.is_none(),
        "p2p new message without root_id/thread_id must not reuse old session"
    );
    assert_eq!(outcome, LarkEventOutcome::CreateSession);
}

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
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
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
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
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
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
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
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
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
