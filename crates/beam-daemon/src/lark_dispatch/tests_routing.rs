use super::*;
use crate::tests::test_helpers::*;

use axum::http::StatusCode;
use beam_core::{AdoptedFrom, SessionScope, SessionStatus};
use std::collections::HashMap;

use crate::{
    GroupStats, LarkEventOutcome, LarkTextAction, ParsedLarkCardAction, decide_lark_routing,
    decide_multibot_inbound_gate,
};

#[test]
fn decide_lark_event_outcome_reflects_existing_session_state() {
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::Close, Some(&make_session("sess-1"))),
        LarkEventOutcome::CloseSession {
            reply: "session closed".to_string()
        }
    );
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::Close, None),
        LarkEventOutcome::CloseSession {
            reply: "no active session".to_string()
        }
    );
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::Restart, Some(&make_session("sess-1"))),
        LarkEventOutcome::RestartSession {
            reply: "session restarted".to_string()
        }
    );
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::Restart, None),
        LarkEventOutcome::RestartSession {
            reply: "no active session".to_string()
        }
    );
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::Card, None),
        LarkEventOutcome::ShowCard {
            reply: "no active session".to_string()
        }
    );
    assert_eq!(
        decide_lark_event_outcome(
            LarkTextAction::ReuseSessionInput,
            Some(&make_session("sess-1"))
        ),
        LarkEventOutcome::ReuseSession
    );
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::CreateSession, None),
        LarkEventOutcome::CreateSession
    );
}

#[test]
fn decide_lark_event_outcome_blocks_re_adopt_when_session_already_adopted() {
    let mut session = make_session("sess-1");
    session.adopted_from = Some(AdoptedFrom {
        tmux_target: Some("mysession:0.0".to_string()),
        zellij_session: None,
        zellij_pane_id: None,
        original_cli_pid: 12345,
        session_id: None,
        cli_id: Some("coco".to_string()),
        cwd: "/repo/project".to_string(),
        pane_cols: Some(120),
        pane_rows: Some(40),
    });
    assert_eq!(
        decide_lark_event_outcome(LarkTextAction::AdoptList, Some(&session)),
        LarkEventOutcome::ReplyOnly {
            reply: "session already adopted from coco (mysession:0.0)\ndisconnect it before running /adopt again".to_string()
        }
    );
    assert_eq!(
        decide_lark_event_outcome(
            LarkTextAction::AdoptZellij("0:2.0".to_string()),
            Some(&session)
        ),
        LarkEventOutcome::ReplyOnly {
            reply: "session already adopted from coco (mysession:0.0)\ndisconnect it before running /adopt again".to_string()
        }
    );
}

#[test]
fn resolve_lark_card_action_session_id_prefers_explicit_id_and_falls_back_to_root() {
    let mut session = make_session("sess-1");
    session.lark_app_id = "app-1".to_string();
    session.root_message_id = "om-root".to_string();
    session.status = SessionStatus::Active;
    session.closed_at = None;
    let sessions = HashMap::from([(session.session_id.clone(), session.clone())]);

    let direct = ParsedLarkCardAction {
        action: "restart".to_string(),
        session_id: Some("sess-explicit".to_string()),
        root_id: Some("om-root".to_string()),
        clicked_message_id: None,
        operator_open_id: Some("ou_user".to_string()),
        term_key: None,
        visibility: None,
        card_nonce: None,
        special_keys: None,
        selected_text: None,
        input_keys: None,
        input_text: None,
        option_type: None,
        selected_index: None,
        is_final: false,
        workflow_run_id: None,
        workflow_id: None,
        workflow_revision_id: None,
        workflow_node_id: None,
        workflow_activity_id: None,
        workflow_attempt_id: None,
        workflow_comment: None,
        raw_value: None,
        ask_id: None,
        ask_nonce: None,
        ask_question_index: None,
        ask_key: None,
        ask_submit: false,
        pending_id: None,
        working_dir: None,
        dir_search_keyword: None,
        cli_session_id: None,
    };
    assert_eq!(
        resolve_lark_card_action_session_id(&sessions, "app-1", &direct).as_deref(),
        Some("sess-explicit")
    );

    let fallback = ParsedLarkCardAction {
        action: "restart".to_string(),
        session_id: None,
        root_id: Some("om-root".to_string()),
        clicked_message_id: None,
        operator_open_id: Some("ou_user".to_string()),
        term_key: None,
        visibility: None,
        card_nonce: None,
        special_keys: None,
        selected_text: None,
        input_keys: None,
        input_text: None,
        option_type: None,
        selected_index: None,
        is_final: false,
        workflow_run_id: None,
        workflow_id: None,
        workflow_revision_id: None,
        workflow_node_id: None,
        workflow_activity_id: None,
        workflow_attempt_id: None,
        workflow_comment: None,
        raw_value: None,
        ask_id: None,
        ask_nonce: None,
        ask_question_index: None,
        ask_key: None,
        ask_submit: false,
        pending_id: None,
        working_dir: None,
        dir_search_keyword: None,
        cli_session_id: None,
    };
    assert_eq!(
        resolve_lark_card_action_session_id(&sessions, "app-1", &fallback).as_deref(),
        Some("sess-1")
    );
}

#[test]
fn resolve_lark_card_action_session_id_ignores_other_apps_and_closed_sessions() {
    let mut active_other_app = make_session("sess-other");
    active_other_app.lark_app_id = "app-2".to_string();
    active_other_app.root_message_id = "om-root".to_string();

    let mut closed_same_app = make_session("sess-closed");
    closed_same_app.lark_app_id = "app-1".to_string();
    closed_same_app.root_message_id = "om-root".to_string();
    closed_same_app.status = SessionStatus::Closed;

    let sessions = HashMap::from([
        (active_other_app.session_id.clone(), active_other_app),
        (closed_same_app.session_id.clone(), closed_same_app),
    ]);
    let action = ParsedLarkCardAction {
        action: "close".to_string(),
        session_id: None,
        root_id: Some("om-root".to_string()),
        clicked_message_id: None,
        operator_open_id: Some("ou_user".to_string()),
        term_key: None,
        visibility: None,
        card_nonce: None,
        special_keys: None,
        selected_text: None,
        input_keys: None,
        input_text: None,
        option_type: None,
        selected_index: None,
        is_final: false,
        workflow_run_id: None,
        workflow_id: None,
        workflow_revision_id: None,
        workflow_node_id: None,
        workflow_activity_id: None,
        workflow_attempt_id: None,
        workflow_comment: None,
        raw_value: None,
        ask_id: None,
        ask_nonce: None,
        ask_question_index: None,
        ask_key: None,
        ask_submit: false,
        pending_id: None,
        working_dir: None,
        dir_search_keyword: None,
        cli_session_id: None,
    };
    assert_eq!(
        resolve_lark_card_action_session_id(&sessions, "app-1", &action),
        None
    );
}

#[test]
fn decide_multibot_inbound_gate_requires_mention_for_foreign_bots() {
    assert!(!decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        false,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        None,
        "hello",
    ));
    assert!(decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        true,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        None,
        "hello",
    ));
}

#[test]
fn decide_multibot_inbound_gate_allows_single_user_group_without_mention() {
    assert!(decide_multibot_inbound_gate(
        Some("user"),
        Some("ou_user"),
        Some("ou_self"),
        false,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        Some(GroupStats {
            user_count: 1,
            bot_count: 1,
        }),
        "continue please",
    ));
    assert!(!decide_multibot_inbound_gate(
        Some("user"),
        Some("ou_user"),
        Some("ou_self"),
        false,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        Some(GroupStats {
            user_count: 3,
            bot_count: 2,
        }),
        "continue please",
    ));
}

#[test]
fn decide_multibot_inbound_gate_keeps_self_close_only() {
    assert!(decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_self"),
        Some("ou_self"),
        false,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        None,
        "/close",
    ));
    assert!(!decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_self"),
        Some("ou_self"),
        false,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        None,
        "status",
    ));
}

#[test]
fn decide_multibot_inbound_gate_allows_thread_scope_foreign_bot_with_mention() {
    assert!(decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        true,
        Some("group"),
        SessionScope::Thread,
        false,
        false,
        false,
        false,
        false,
        None,
        "hello",
    ));
}

#[test]
fn decide_multibot_inbound_gate_blocks_chat_scope_foreign_bot_without_grant() {
    assert!(!decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        true,
        Some("group"),
        SessionScope::Chat,
        false,
        false,
        false,
        false,
        false,
        None,
        "hello",
    ));
}

#[test]
fn decide_multibot_inbound_gate_allows_chat_scope_foreign_bot_with_chat_grant() {
    assert!(decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        true,
        Some("group"),
        SessionScope::Chat,
        false,
        false,
        false,
        true,
        false,
        None,
        "hello",
    ));
}

#[test]
fn decide_multibot_inbound_gate_allows_chat_scope_foreign_bot_if_owns_session() {
    assert!(decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        true,
        Some("group"),
        SessionScope::Chat,
        false,
        true,
        false,
        false,
        false,
        None,
        "hello",
    ));
}

#[test]
fn decide_multibot_inbound_gate_allows_chat_scope_oncall_without_grant() {
    assert!(decide_multibot_inbound_gate(
        Some("bot"),
        Some("ou_peer"),
        Some("ou_self"),
        true,
        Some("group"),
        SessionScope::Chat,
        true,
        false,
        false,
        false,
        false,
        None,
        "hello",
    ));
}

#[test]
fn decide_lark_routing_uses_thread_id_as_authoritative_topic_signal() {
    // Non-p2p messages with thread_id use thread_id as anchor (stable
    // topic identifier), NOT root_id (which is for reply semantics).
    assert_eq!(
        decide_lark_routing(
            "msg-1",
            "chat-a",
            Some("group"),
            Some("real-topic-root"),
            Some("omt_topic")
        ),
        (SessionScope::Thread, "omt_topic")
    );
    // Group without thread_id stays Chat-scoped, even with root_id
    // (root_id alone is a quote reply, not a topic signal).
    assert_eq!(
        decide_lark_routing(
            "msg-1",
            "chat-a",
            Some("group"),
            Some("quote-bubble-root"),
            None
        ),
        (SessionScope::Chat, "chat-a")
    );
}

#[test]
fn decide_lark_routing_keeps_p2p_and_topic_chats_thread_scoped() {
    // p2p always Thread-scoped with message_id anchor
    assert_eq!(
        decide_lark_routing("msg-dm", "chat-dm", Some("p2p"), None, None),
        (SessionScope::Thread, "msg-dm")
    );
    // chat_type="topic" is NOT a real Feishu receive_v1 field.
    // Without thread_id, it stays Chat-scoped (topic detection
    // happens later via get_lark_chat_mode()).
    assert_eq!(
        decide_lark_routing("msg-topic", "chat-topic", Some("topic"), None, None),
        (SessionScope::Chat, "chat-topic")
    );
}

#[test]
fn decide_lark_routing_topic_group_should_be_thread_scoped_with_message_id() {
    assert_eq!(
        decide_lark_routing("msg-1", "chat-a", Some("group"), None, None),
        (SessionScope::Chat, "chat-a")
    );
}

#[test]
fn decide_lark_routing_p2p_always_thread_scoped() {
    assert_eq!(
        decide_lark_routing("msg-1", "chat-dm", Some("p2p"), None, None),
        (SessionScope::Thread, "msg-1")
    );
}

#[test]
fn decide_lark_routing_with_thread_id_overrides_chat_type() {
    // Non-p2p with thread_id → Thread scope, anchor = thread_id
    assert_eq!(
        decide_lark_routing(
            "msg-1",
            "chat-a",
            Some("group"),
            Some("root-1"),
            Some("thread-1")
        ),
        (SessionScope::Thread, "thread-1")
    );
}

#[test]
fn decide_lark_routing_p2p_uses_root_id_as_anchor_for_follow_ups() {
    // p2p with root_id && thread_id: root_id takes priority so the
    // follow-up can match the first message's session via root_message_id.
    // When root_id == message_id (self-root), the result is the same
    // as using message_id.
    assert_eq!(
        decide_lark_routing(
            "msg-1",
            "chat-dm",
            Some("p2p"),
            Some("msg-1"),
            Some("omt_thread")
        ),
        (SessionScope::Thread, "msg-1")
    );
    // When root_id != message_id (true reply/thread follow-up), use
    // root_id so it can match the first message's root_message_id.
    assert_eq!(
        decide_lark_routing(
            "msg-2",
            "chat-dm",
            Some("p2p"),
            Some("first-msg"),
            Some("omt_thread")
        ),
        (SessionScope::Thread, "first-msg")
    );
    // p2p with root_id but no thread_id: still use root_id as anchor.
    assert_eq!(
        decide_lark_routing("msg-3", "chat-dm", Some("p2p"), Some("first-msg"), None),
        (SessionScope::Thread, "first-msg")
    );
}

#[test]
fn decide_lark_routing_p2p_with_thread_id_no_root_id_uses_thread_id() {
    // p2p message with thread_id but no root_id: use thread_id so events
    // after thread_id backfill can still match.
    assert_eq!(
        decide_lark_routing("msg-4", "chat-dm", Some("p2p"), None, Some("omt_thread")),
        (SessionScope::Thread, "omt_thread")
    );
}

#[test]
fn decide_lark_routing_group_with_thread_id_uses_thread_id_as_anchor() {
    // Non-p2p with thread_id uses thread_id as anchor, not root_id.
    // The thread_id (omt_*) is the stable topic identifier.
    assert_eq!(
        decide_lark_routing(
            "msg-2",
            "chat-a",
            Some("group"),
            Some("topic-root"),
            Some("omt_topic")
        ),
        (SessionScope::Thread, "omt_topic")
    );
}

#[test]
fn decide_lark_routing_topic_chat_type_without_thread_id_stays_chat_scoped() {
    // chat_type="topic" is NOT a real Feishu receive_v1 field.
    // Without thread_id, root_id alone is not a topic signal.
    // Topic detection happens later via get_lark_chat_mode().
    // The initial routing stays Chat-scoped.
    assert_eq!(
        decide_lark_routing(
            "msg-2",
            "topic-chat-1",
            Some("topic"),
            Some("first-topic-msg"),
            None
        ),
        (SessionScope::Chat, "topic-chat-1")
    );
}

#[test]
fn decide_lark_routing_topic_chat_type_without_metadata_stays_chat_scoped() {
    // chat_type="topic" is NOT a real Feishu receive_v1 field.
    // Without thread_id or root_id, stays Chat-scoped.
    // Topic detection happens later via get_lark_chat_mode().
    assert_eq!(
        decide_lark_routing("msg-1", "topic-chat-2", Some("topic"), None, None),
        (SessionScope::Chat, "topic-chat-2")
    );
}

#[test]
fn decide_lark_routing_group_with_root_id_but_no_thread_id_stays_chat_scoped() {
    // For group chats, root_id without thread_id is a quote-bubble
    // reply, not a topic message.  Must stay Chat-scoped so the
    // topic routing is not accidentally applied.
    assert_eq!(
        decide_lark_routing(
            "msg-1",
            "group-chat-1",
            Some("group"),
            Some("some-root"),
            None
        ),
        (SessionScope::Chat, "group-chat-1")
    );
}

#[test]
fn validate_resume_target_detects_chat_scope_anchor_conflict_by_chat_id() {
    let mut candidate = make_session("closed-chat");
    candidate.scope = SessionScope::Chat;
    candidate.chat_id = "chat-a".to_string();
    candidate.root_message_id = "closed-root".to_string();

    let mut owner = make_session("active-chat");
    owner.status = SessionStatus::Active;
    owner.closed_at = None;
    owner.scope = SessionScope::Chat;
    owner.chat_id = "chat-a".to_string();
    owner.root_message_id = "other-root".to_string();

    let sessions = HashMap::from([
        (candidate.session_id.clone(), candidate),
        (owner.session_id.clone(), owner),
    ]);
    let err =
        validate_resume_target(&sessions, "closed-chat").expect_err("chat scope conflict expected");
    assert_eq!(err.0, StatusCode::CONFLICT);
    assert_eq!(
        err.1,
        "session anchor is already owned by active session active-chat"
    );
}
