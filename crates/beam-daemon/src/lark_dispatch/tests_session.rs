use super::*;
use crate::tests::test_helpers::*;

use axum::http::StatusCode;
use beam_core::{
    AdoptedFrom, AgentAttention, BotConfig, CustomTrigger, SessionScope, SessionStatus,
};
use chrono::Utc;
use std::collections::HashMap;

use crate::{
    LarkPreflight, ParsedLarkInboundMessage, evaluate_talk_for_bot, session_anchor_matches,
};

#[test]
fn validate_resume_target_accepts_closed_non_adopted_session() {
    let candidate = make_session("closed-1");
    let sessions = HashMap::from([(candidate.session_id.clone(), candidate.clone())]);
    let resumed = validate_resume_target(&sessions, &candidate.session_id).expect("resume target");
    assert_eq!(resumed.session_id, candidate.session_id);
}

#[test]
fn validate_resume_target_rejects_active_session() {
    let mut candidate = make_session("active-1");
    candidate.status = SessionStatus::Active;
    candidate.closed_at = None;
    let sessions = HashMap::from([(candidate.session_id.clone(), candidate)]);
    let err =
        validate_resume_target(&sessions, "active-1").expect_err("active session should fail");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert_eq!(err.1, "session is not closed");
}

#[test]
fn validate_resume_target_rejects_adopted_session() {
    let mut candidate = make_session("adopted-1");
    candidate.adopted_from = Some(AdoptedFrom {
        tmux_target: Some("0:1.0".to_string()),
        zellij_session: None,
        zellij_pane_id: None,
        original_cli_pid: 123,
        session_id: None,
        cli_id: Some("codex".to_string()),
        cwd: "/tmp/project".to_string(),
        pane_cols: Some(120),
        pane_rows: Some(40),
    });
    let sessions = HashMap::from([(candidate.session_id.clone(), candidate)]);
    let err =
        validate_resume_target(&sessions, "adopted-1").expect_err("adopted session should fail");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert_eq!(err.1, "adopted sessions cannot be resumed yet");
}

#[test]
fn validate_resume_target_rejects_anchor_conflict() {
    let mut candidate = make_session("closed-1");
    candidate.thread_id = Some("thread-1".to_string());
    let mut owner = make_session("active-1");
    owner.status = SessionStatus::Active;
    owner.closed_at = None;
    owner.thread_id = Some("thread-1".to_string());

    let sessions = HashMap::from([
        (candidate.session_id.clone(), candidate),
        (owner.session_id.clone(), owner),
    ]);
    let err = validate_resume_target(&sessions, "closed-1").expect_err("conflict expected");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert_eq!(
        err.1,
        "session anchor is already owned by active session active-1"
    );
}

#[test]
fn validate_resume_target_ignores_other_scope_or_anchor() {
    let candidate = make_session("closed-1");
    let mut sibling = make_session("active-1");
    sibling.status = SessionStatus::Active;
    sibling.closed_at = None;
    sibling.scope = SessionScope::Chat;
    sibling.root_message_id = "other-root".to_string();

    let sessions = HashMap::from([
        (candidate.session_id.clone(), candidate.clone()),
        (sibling.session_id.clone(), sibling),
    ]);
    let resumed = validate_resume_target(&sessions, &candidate.session_id).expect("no conflict");
    assert_eq!(resumed.session_id, candidate.session_id);
}

#[test]
fn session_for_lark_anchor_matches_thread_scope_by_thread_id() {
    // Thread-scoped sessions now match on thread_id, not root_message_id.
    let mut thread = make_session("thread-1");
    thread.status = SessionStatus::Active;
    thread.closed_at = None;
    thread.scope = SessionScope::Thread;
    thread.chat_id = "chat-a".to_string();
    thread.root_message_id = "root-a".to_string();
    thread.thread_id = Some("anchor-a".to_string());

    let sessions = HashMap::from([(thread.session_id.clone(), thread.clone())]);
    let found = session_for_lark_anchor(&sessions, "app-1", "chat-a", "anchor-a")
        .expect("thread session should match on thread_id");
    assert_eq!(found.session_id, thread.session_id);
    assert!(session_for_lark_anchor(&sessions, "app-1", "chat-a", "anchor-b").is_none());
}

#[test]
fn session_for_lark_anchor_matches_chat_scope_without_root_message_match() {
    let mut chat = make_session("chat-1");
    chat.status = SessionStatus::Active;
    chat.closed_at = None;
    chat.scope = SessionScope::Chat;
    chat.chat_id = "chat-a".to_string();
    chat.root_message_id = "original-root".to_string();

    let sessions = HashMap::from([(chat.session_id.clone(), chat.clone())]);
    let found = session_for_lark_anchor(&sessions, "app-1", "chat-a", "different-root")
        .expect("chat session should match by chat only");
    assert_eq!(found.session_id, chat.session_id);
    assert!(session_for_lark_anchor(&sessions, "app-1", "chat-b", "different-root").is_none());
}

#[test]
fn session_anchor_matches_thread_vs_chat_scope() {
    let mut session = make_session("sess-t1");
    session.lark_app_id = "app-1".to_string();
    session.status = SessionStatus::Active;
    session.scope = SessionScope::Thread;
    session.chat_id = "chat-1".to_string();
    session.root_message_id = "root-1".to_string();
    session.thread_id = Some("thread-1".to_string());
    // Thread scope matches on thread_id
    assert!(session_anchor_matches(
        &session, "app-1", "chat-1", "thread-1"
    ));
    assert!(!session_anchor_matches(
        &session, "app-1", "chat-1", "thread-9"
    ));

    session.scope = SessionScope::Chat;
    // Chat scope matches on chat_id only
    assert!(session_anchor_matches(
        &session,
        "app-1",
        "chat-1",
        "any-anchor"
    ));
    assert!(!session_anchor_matches(
        &session,
        "app-1",
        "chat-9",
        "any-anchor"
    ));
}

#[test]
fn session_anchor_matches_p2p_falls_back_to_root_message_id() {
    // p2p first message session: Thread scope, thread_id=None,
    // root_message_id=message_id.  A follow-up p2p message with
    // root_id=message_id should match via the root_message_id fallback.
    let mut session = make_session("p2p-sess");
    session.lark_app_id = "app-1".to_string();
    session.status = SessionStatus::Active;
    session.scope = SessionScope::Thread;
    session.chat_id = "dm-chat".to_string();
    session.chat_type = Some("p2p".to_string());
    session.root_message_id = "first-msg".to_string();
    session.thread_id = None;

    // Follow-up with root_id=first-msg matches via root_message_id fallback.
    assert!(session_anchor_matches(
        &session,
        "app-1",
        "dm-chat",
        "first-msg"
    ));
    // Different root_id does NOT match.
    assert!(!session_anchor_matches(
        &session,
        "app-1",
        "dm-chat",
        "other-msg"
    ));
    // Different chat_id does NOT match.
    assert!(!session_anchor_matches(
        &session,
        "app-1",
        "other-chat",
        "first-msg"
    ));

    // After thread_id is backfilled, root_message_id fallback STILL works.
    // This is critical: p2p routing prefers root_id over thread_id, so
    // follow-ups that carry both root_id + thread_id will have anchor=root_id,
    // which must match root_message_id even though thread_id is now Some.
    session.thread_id = Some("omt_thread".to_string());
    assert!(session_anchor_matches(
        &session,
        "app-1",
        "dm-chat",
        "first-msg"
    ));
    // thread_id matching also works.
    assert!(session_anchor_matches(
        &session,
        "app-1",
        "dm-chat",
        "omt_thread"
    ));
    // A bogus anchor matches neither.
    assert!(!session_anchor_matches(
        &session, "app-1", "dm-chat", "bogus"
    ));

    // Non-p2p session with thread_id=None should NOT fall back to
    // root_message_id (only p2p sessions get the fallback).
    session.chat_type = Some("group".to_string());
    assert!(!session_anchor_matches(
        &session,
        "app-1",
        "dm-chat",
        "first-msg"
    ));
}

#[test]
fn evaluate_talk_denies_unknown_sender_with_strict_bot() {
    let bot = BotConfig {
        name: None,
        lark_app_id: "app-1".to_string(),
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
        allowed_users: vec!["ou_owner".to_string()],
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
    let talk = evaluate_talk_for_bot(&bot, "chat-1", "ou_other");
    assert!(!talk.allowed);

    let owner_talk = evaluate_talk_for_bot(&bot, "chat-1", "ou_owner");
    assert!(owner_talk.allowed);
}

#[test]
fn evaluate_lark_preflight_handles_dedupe_empty_and_permission_gate() {
    let paths = temp_paths("preflight");
    maybe_remove_dir(&paths.root().to_path_buf());
    let bot = BotConfig {
        name: None,
        lark_app_id: "app-1".to_string(),
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
        allowed_users: vec!["ou_owner".to_string()],
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
    let state = make_state(
        paths.clone(),
        HashMap::from([(bot.lark_app_id.clone(), bot.clone())]),
    );
    assert_eq!(
        evaluate_lark_preflight(
            &state,
            &bot,
            "hello",
            "chat-1",
            Some("ou_owner"),
            true,
            false
        ),
        LarkPreflight::Deduped
    );
    assert_eq!(
        evaluate_lark_preflight(&state, &bot, "", "chat-1", Some("ou_owner"), false, false),
        LarkPreflight::IgnoredEmptyText
    );
    assert_eq!(
        evaluate_lark_preflight(
            &state,
            &bot,
            "/close",
            "chat-1",
            Some("ou_other"),
            false,
            false
        ),
        LarkPreflight::Denied {
            reply: "permission denied"
        }
    );
    assert_eq!(
        evaluate_lark_preflight(
            &state,
            &bot,
            "hello",
            "chat-1",
            Some("ou_other"),
            false,
            false
        ),
        LarkPreflight::Denied {
            reply: "permission denied: you are not authorized to talk to this bot"
        }
    );
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[test]
fn evaluate_lark_preflight_allows_slash_custom_trigger_for_grant_user() {
    let paths = temp_paths("preflight-trigger");
    maybe_remove_dir(&paths.root().to_path_buf());
    let bot = BotConfig {
        lark_app_id: "app-1".to_string(),
        lark_app_secret: "secret".to_string(),
        cli_id: "codex".to_string(),
        global_grants: vec!["ou_grant_user".to_string()],
        restrict_grant_commands: true,
        custom_triggers: vec![CustomTrigger {
            trigger: "/日报".to_string(),
            prompt: None,
            skip_dir_select: false,
            working_dir: None,
            ack_message: None,
        }],
        ..make_bot("app-1")
    };
    let state = make_state(
        paths.clone(),
        HashMap::from([(bot.lark_app_id.clone(), bot.clone())]),
    );

    // A configured slash trigger is not treated as a restricted slash
    // command for grant-authorized users.
    assert_eq!(
        evaluate_lark_preflight(
            &state,
            &bot,
            "/日报",
            "chat-1",
            Some("ou_grant_user"),
            false,
            true
        ),
        LarkPreflight::Continue
    );
    // Unconfigured slash commands stay restricted for the same user.
    assert_eq!(
        evaluate_lark_preflight(
            &state,
            &bot,
            "/foo",
            "chat-1",
            Some("ou_grant_user"),
            false,
            false
        ),
        LarkPreflight::Denied {
            reply: "slash commands are restricted for grant-authorized users"
        }
    );
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[test]
fn update_session_from_lark_message_clears_agent_attention() {
    let mut session = make_session("sess-attn-clear");
    session.agent_attention = Some(AgentAttention {
        kind: "blocked".to_string(),
        reason: "test".to_string(),
        at: Utc::now(),
    });
    let parsed = ParsedLarkInboundMessage {
        event_id: "evt-1".to_string(),
        message_id: "msg-1".to_string(),
        chat_id: "chat-1".to_string(),
        chat_type: Some("group".to_string()),
        sender_type: Some("user".to_string()),
        scope: SessionScope::Thread,
        anchor: "root-1".to_string(),
        text: "hello".to_string(),
        sender_open_id: Some("ou_user".to_string()),
        mentions: vec![],
        parent_id: None,
        root_id: Some("root-1".to_string()),
        thread_id: None,
        locale: None,
    };
    assert!(
        session.agent_attention.is_some(),
        "session should start with attention"
    );
    update_session_from_lark_message(&mut session, &parsed);
    assert!(
        session.agent_attention.is_none(),
        "attention should be cleared on inbound"
    );
}
