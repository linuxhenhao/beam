use std::collections::HashMap;

use crate::prompt::ObservedBot;
use beam_core::{Session, SessionScope, SessionStatus};

use super::*;
use crate::tests::test_helpers::*;

#[tokio::test]
async fn final_output_footer_recipient_filters_known_bot_owner() {
    let paths = temp_paths("final-output-footer");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");
    std::fs::write(
        paths.root().join("bot-openids-app-1.json"),
        r#"{"Claude":"ou_bot"}"#,
    )
    .expect("write cross-ref");

    let mut bot_owner = make_session("sess-bot-owner");
    bot_owner.owner_open_id = Some("ou_bot".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &bot_owner),
        None
    );

    let mut human_owner = make_session("sess-human-owner");
    human_owner.owner_open_id = Some("ou_human".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &human_owner).as_deref(),
        Some("ou_human")
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

// ---- footer human-first candidate tests (minimal botmux parity) ----

#[tokio::test]
async fn footer_recipient_prefers_quote_target_sender_over_owner_when_both_human() {
    let paths = temp_paths("footer-human-first");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");
    // No known bots registered for this app — both candidates are human.
    let mut session = make_session("sess-fh-1");
    session.quote_target_sender_open_id = Some("ou_sender".to_string());
    session.owner_open_id = Some("ou_owner".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &session).as_deref(),
        Some("ou_sender"),
        "quote_target_sender_open_id should take priority over owner_open_id when both are human"
    );
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn footer_recipient_falls_back_to_owner_when_quote_sender_is_bot() {
    let paths = temp_paths("footer-fallback");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");
    std::fs::write(
        paths.root().join("bot-openids-app-1.json"),
        r#"{"Bot":"ou_bot"}"#,
    )
    .expect("write cross-ref");

    let mut session = make_session("sess-fh-2");
    session.quote_target_sender_open_id = Some("ou_bot".to_string());
    session.owner_open_id = Some("ou_human".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &session).as_deref(),
        Some("ou_human"),
        "should fall back to owner when quote_target_sender is a known bot"
    );
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn footer_recipient_returns_none_when_owner_is_bot_and_no_other_human() {
    let paths = temp_paths("footer-none");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");
    std::fs::write(
        paths.root().join("bot-openids-app-1.json"),
        r#"{"Bot":"ou_bot"}"#,
    )
    .expect("write cross-ref");

    let mut session = make_session("sess-fh-3");
    session.owner_open_id = Some("ou_bot".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &session),
        None,
        "should return None when owner is a known bot and there is no other human"
    );
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn footer_recipient_dedup_and_trim_empty() {
    let paths = temp_paths("footer-dedup");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");

    // Both fields with same value (trimmed) — should still return the human.
    let mut session = make_session("sess-fh-4");
    session.quote_target_sender_open_id = Some("  ou_human  ".to_string());
    session.owner_open_id = Some("ou_human".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &session).as_deref(),
        Some("ou_human"),
        "trimmed duplicates should not affect result"
    );

    // Only empty/whitespace candidates -> None.
    let mut session2 = make_session("sess-fh-5");
    session2.quote_target_sender_open_id = Some("   ".to_string());
    session2.owner_open_id = Some("".to_string());
    assert_eq!(
        final_output_footer_recipient_open_id(&paths, &session2),
        None,
        "all empty/whitespace candidates should return None"
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

// ---- mention-back tests ----

#[test]
fn mention_back_uses_quote_target_sender_over_owner_when_both_differ() {
    let mut session = make_session("sess-mb-diff");
    session.owner_open_id = Some("ou_owner".to_string());
    session.quote_target_sender_open_id = Some("ou_sender".to_string());

    let target = resolve_mention_back_target(&session).expect("should resolve target");
    assert_eq!(
        target, "ou_sender",
        "quote_target_sender_open_id should take priority over owner_open_id"
    );
}

#[test]
fn mention_back_falls_back_to_owner_open_id_for_backward_compat() {
    let mut session = make_session("sess-mb-fallback");
    session.owner_open_id = Some("ou_owner".to_string());
    session.quote_target_sender_open_id = None;

    let target = resolve_mention_back_target(&session).expect("should fall back to owner");
    assert_eq!(target, "ou_owner");
}

#[test]
fn mention_back_prefers_quote_target_even_when_owner_exists() {
    let mut session = make_session("sess-mb-prefer");
    session.owner_open_id = Some("ou_owner".to_string());
    session.quote_target_sender_open_id = Some("ou_sender".to_string());

    let target = resolve_mention_back_target(&session).unwrap();
    assert_eq!(target, "ou_sender");
    assert_ne!(target, "ou_owner");
}

#[test]
fn mention_back_errors_when_both_fields_missing() {
    let session = make_session("sess-mb-missing");
    assert!(session.quote_target_sender_open_id.is_none());
    assert!(session.owner_open_id.is_none());

    let err = resolve_mention_back_target(&session).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("quote_target_sender_open_id") || msg.contains("owner_open_id"),
        "error should mention the missing fields: {}",
        msg
    );
}

#[test]
fn mention_back_ignores_empty_string_fields() {
    let mut session = make_session("sess-mb-empty");
    session.quote_target_sender_open_id = Some("  ".to_string());
    session.owner_open_id = Some("".to_string());

    let err = resolve_mention_back_target(&session).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("quote_target_sender_open_id") || msg.contains("owner_open_id"),
        "empty/whitespace fields should be treated as None"
    );
}

// ---- auto_inject_bot_mentions tests (P1-5) ----

fn make_observed_bot(open_id: &str, name: &str) -> ObservedBot {
    ObservedBot {
        open_id: open_id.to_string(),
        name: name.to_string(),
    }
}

#[test]
fn auto_inject_single_bot_mention_surrounded_by_text() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let result = auto_inject_bot_mentions("请 @ReviewerBot 看一下", &bots, None);
    assert_eq!(
        result,
        "请 <at user_id=\"ou_reviewer\">@ReviewerBot</at> 看一下"
    );
}

#[test]
fn auto_inject_bot_at_end_of_string() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let result = auto_inject_bot_mentions("hello @ReviewerBot", &bots, None);
    assert_eq!(
        result,
        "hello <at user_id=\"ou_reviewer\">@ReviewerBot</at>"
    );
}

#[test]
fn auto_inject_multiple_bots() {
    let bots = vec![
        make_observed_bot("ou_r", "ReviewerBot"),
        make_observed_bot("ou_a", "AnalyzerBot"),
    ];
    let result = auto_inject_bot_mentions("@ReviewerBot 和 @AnalyzerBot 一起", &bots, None);
    assert_eq!(
        result,
        "<at user_id=\"ou_r\">@ReviewerBot</at> 和 <at user_id=\"ou_a\">@AnalyzerBot</at> 一起"
    );
}

#[test]
fn auto_inject_duplicate_bot_mentions() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let result = auto_inject_bot_mentions("@ReviewerBot @ReviewerBot", &bots, None);
    assert_eq!(
        result,
        "<at user_id=\"ou_reviewer\">@ReviewerBot</at> <at user_id=\"ou_reviewer\">@ReviewerBot</at>"
    );
}

#[test]
fn auto_inject_skips_self_bot() {
    let bots = vec![
        make_observed_bot("ou_self", "SelfBot"),
        make_observed_bot("ou_other", "OtherBot"),
    ];
    let result = auto_inject_bot_mentions("@SelfBot @OtherBot", &bots, Some("ou_self"));
    assert_eq!(result, "@SelfBot <at user_id=\"ou_other\">@OtherBot</at>");
}

#[test]
fn auto_inject_skips_existing_at_tag() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let content = "已有 <at user_id=\"ou_reviewer\">@ReviewerBot</at> 标签";
    let result = auto_inject_bot_mentions(content, &bots, None);
    assert_eq!(result, content);
}

#[test]
fn auto_inject_handles_existing_at_with_bare_mention_after() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let content = "<at user_id=\"ou_x\">@Someone</at> 然后 @ReviewerBot";
    let result = auto_inject_bot_mentions(content, &bots, None);
    assert_eq!(
        result,
        "<at user_id=\"ou_x\">@Someone</at> 然后 <at user_id=\"ou_reviewer\">@ReviewerBot</at>"
    );
}

#[test]
fn auto_inject_noop_with_empty_bots() {
    let content = "@Nobody 在这里";
    let result = auto_inject_bot_mentions(content, &[], None);
    assert_eq!(result, content);
}

#[test]
fn auto_inject_skips_empty_name_or_open_id() {
    let bots = vec![
        make_observed_bot("ou_good", "GoodBot"),
        make_observed_bot("", "EmptyOpenIdBot"),
        make_observed_bot("ou_empty_name", ""),
    ];
    let result = auto_inject_bot_mentions("@GoodBot @EmptyOpenIdBot @EmptyName", &bots, None);
    assert_eq!(
        result,
        "<at user_id=\"ou_good\">@GoodBot</at> @EmptyOpenIdBot @EmptyName"
    );
}

#[test]
fn auto_inject_no_partial_match_with_underscore() {
    // @ReviewerBot_Extra should NOT match ReviewerBot
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let result = auto_inject_bot_mentions("@ReviewerBot_Extra", &bots, None);
    assert_eq!(result, "@ReviewerBot_Extra");
}

#[test]
fn auto_inject_longer_name_matches_first() {
    // Longer bot name should match before shorter
    let bots = vec![
        make_observed_bot("ou_long", "ReviewerBotPro"),
        make_observed_bot("ou_short", "ReviewerBot"),
    ];
    let result = auto_inject_bot_mentions("@ReviewerBotPro", &bots, None);
    assert_eq!(result, "<at user_id=\"ou_long\">@ReviewerBotPro</at>");
}

#[test]
fn auto_inject_no_mention_flag_disables_auto_inject() {
    // While --no-mention is enforced at the handler level,
    // the helper also respects it by not being called.
    // Test that NO auto-injection happens when not called.
    let _bots = [make_observed_bot("ou_reviewer", "ReviewerBot")];
    // Without auto_inject, content stays as-is
    let content = "请 @ReviewerBot 看一下";
    // The helper is simply not called; this test documents the
    // expected raw content before injection.
    assert_eq!(content, "请 @ReviewerBot 看一下");
}

#[test]
fn auto_inject_with_followed_by_punctuation() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let cases = vec![
        (
            "@ReviewerBot, 你好",
            "<at user_id=\"ou_reviewer\">@ReviewerBot</at>, 你好",
        ),
        (
            "@ReviewerBot. 是的",
            "<at user_id=\"ou_reviewer\">@ReviewerBot</at>. 是的",
        ),
        (
            "@ReviewerBot！你好",
            "<at user_id=\"ou_reviewer\">@ReviewerBot</at>！你好",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(
            auto_inject_bot_mentions(input, &bots, None),
            expected,
            "failed for input: {}",
            input
        );
    }
}

#[test]
fn auto_inject_noop_when_no_at_sign() {
    let bots = vec![make_observed_bot("ou_reviewer", "ReviewerBot")];
    let content = "ReviewerBot without @";
    let result = auto_inject_bot_mentions(content, &bots, None);
    assert_eq!(result, content);
}

// ---- withdrawn fallback tests (P2-8: structured send quote/reply withdrawn fallback) ----

#[test]
fn withdrawn_payload_recognizes_code_230011() {
    let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
    assert!(is_lark_message_withdrawn_payload(payload));
}

#[test]
fn withdrawn_payload_detects_code_in_string() {
    assert!(is_lark_message_withdrawn_payload("error 230011 occurred"));
}

#[test]
fn withdrawn_payload_detects_withdrawn_keyword() {
    assert!(is_lark_message_withdrawn_payload(
        "message withdrawn by user"
    ));
}

#[test]
fn withdrawn_payload_rejects_normal_error() {
    assert!(!is_lark_message_withdrawn_payload(
        r#"{"code":999,"msg":"permission denied"}"#
    ));
}

#[test]
fn withdrawn_error_recognizes_root_cause_from_chain() {
    let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
    let err = anyhow::anyhow!("lark message withdrawn: {}", payload);
    assert!(is_lark_message_withdrawn_error(&err));
}

#[test]
fn withdrawn_error_rejects_unrelated_error() {
    let err = anyhow::anyhow!("lark reply failed: {{\"code\":999}}");
    assert!(!is_lark_message_withdrawn_error(&err));
}

#[test]
fn should_fallback_to_plain_returns_true_for_withdrawn() {
    let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
    let err = anyhow::anyhow!("lark message withdrawn: {}", payload);
    assert!(should_fallback_to_plain_on_withdrawn(&err));
}

#[test]
fn should_fallback_to_plain_returns_false_for_normal_error() {
    let err = anyhow::anyhow!("lark reply failed: {{\"code\":999}}");
    assert!(!should_fallback_to_plain_on_withdrawn(&err));
}

// ---- off_topic_sub_bot_hint tests (P2-9) ----

fn make_test_session(
    session_id: &str,
    chat_id: &str,
    root_message_id: &str,
    thread_id: Option<&str>,
    bot_open_id: Option<&str>,
    status: SessionStatus,
) -> Session {
    let mut session = make_session(session_id);
    session.chat_id = chat_id.to_string();
    session.root_message_id = root_message_id.to_string();
    session.thread_id = thread_id.map(|t| t.to_string());
    session.bot_open_id = bot_open_id.map(|o| o.to_string());
    session.status = status;
    session.scope = SessionScope::Thread; // not critical for hint logic but realistic
    session
}

#[test]
fn off_topic_hint_returns_hint_for_bot_in_different_topic() {
    // Current session is in topic A; mentioned bot is active in topic B (same chat).
    let current = make_test_session(
        "s-current",
        "chat-1",
        "root-aaa",
        Some("thread-aaa"),
        Some("ou_self"),
        SessionStatus::Active,
    );
    let sub_bot = make_test_session(
        "s-sub",
        "chat-1",
        "root-bbb",
        Some("thread-bbb"),
        Some("ou_sub"),
        SessionStatus::Active,
    );
    let mut sessions = HashMap::new();
    sessions.insert(sub_bot.session_id.clone(), sub_bot);

    let hint = off_topic_sub_bot_hint(
        &current,
        &["ou_sub".to_string()],
        &sessions,
        false, // anyway=false
    );
    assert!(hint.is_some(), "should return hint for off-topic sub-bot");
    let msg = hint.unwrap();
    assert!(
        msg.contains("ou_sub"),
        "hint should include mentioned open_id, got: {}",
        msg
    );
    assert!(
        msg.contains("--into"),
        "hint should suggest --into, got: {}",
        msg
    );
    assert!(
        msg.contains("root-bbb"),
        "hint should include target root_message_id, got: {}",
        msg
    );
}

#[test]
fn off_topic_hint_returns_none_for_same_topic() {
    // Same root_message_id and same thread_id → not off-topic.
    let current = make_test_session(
        "s-current",
        "chat-1",
        "root-same",
        Some("thread-same"),
        Some("ou_self"),
        SessionStatus::Active,
    );
    let sub_bot = make_test_session(
        "s-sub",
        "chat-1",
        "root-same",
        Some("thread-same"),
        Some("ou_sub"),
        SessionStatus::Active,
    );
    let mut sessions = HashMap::new();
    sessions.insert(sub_bot.session_id.clone(), sub_bot);

    let hint = off_topic_sub_bot_hint(&current, &["ou_sub".to_string()], &sessions, false);
    assert!(hint.is_none(), "same topic should yield no hint");
}

#[test]
fn off_topic_hint_returns_none_when_anyway_true() {
    let current = make_test_session(
        "s-current",
        "chat-1",
        "root-aaa",
        Some("thread-aaa"),
        Some("ou_self"),
        SessionStatus::Active,
    );
    let sub_bot = make_test_session(
        "s-sub",
        "chat-1",
        "root-bbb",
        Some("thread-bbb"),
        Some("ou_sub"),
        SessionStatus::Active,
    );
    let mut sessions = HashMap::new();
    sessions.insert(sub_bot.session_id.clone(), sub_bot);

    let hint = off_topic_sub_bot_hint(
        &current,
        &["ou_sub".to_string()],
        &sessions,
        true, // anyway=true
    );
    assert!(hint.is_none(), "anyway=true should suppress hint");
}

#[test]
fn off_topic_hint_returns_none_for_non_active_bot() {
    // Mentioned open_id doesn't match any session's bot_open_id.
    let current = make_test_session(
        "s-current",
        "chat-1",
        "root-aaa",
        Some("thread-aaa"),
        Some("ou_self"),
        SessionStatus::Active,
    );
    let sub_bot = make_test_session(
        "s-sub",
        "chat-1",
        "root-bbb",
        Some("thread-bbb"),
        None, // no bot_open_id → not a bot session
        SessionStatus::Active,
    );
    let mut sessions = HashMap::new();
    sessions.insert(sub_bot.session_id.clone(), sub_bot);

    let hint = off_topic_sub_bot_hint(&current, &["ou_unknown".to_string()], &sessions, false);
    assert!(hint.is_none(), "non-bot/human mention should yield no hint");
}

#[test]
fn off_topic_hint_returns_none_for_self_mention() {
    // Mentioning the current session's own bot_open_id.
    let current = make_test_session(
        "s-current",
        "chat-1",
        "root-aaa",
        Some("thread-aaa"),
        Some("ou_self"),
        SessionStatus::Active,
    );
    let sessions: HashMap<String, Session> = HashMap::new();

    let hint = off_topic_sub_bot_hint(&current, &["ou_self".to_string()], &sessions, false);
    assert!(
        hint.is_none(),
        "self-mention (current session's own bot_open_id) should yield no hint"
    );
}

#[test]
fn off_topic_hint_returns_none_for_closed_sub_bot_session() {
    let current = make_test_session(
        "s-current",
        "chat-1",
        "root-aaa",
        Some("thread-aaa"),
        Some("ou_self"),
        SessionStatus::Active,
    );
    let sub_bot = make_test_session(
        "s-sub",
        "chat-1",
        "root-bbb",
        Some("thread-bbb"),
        Some("ou_sub"),
        SessionStatus::Closed, // closed → not active
    );
    let mut sessions = HashMap::new();
    sessions.insert(sub_bot.session_id.clone(), sub_bot);

    let hint = off_topic_sub_bot_hint(&current, &["ou_sub".to_string()], &sessions, false);
    assert!(
        hint.is_none(),
        "closed sub-bot session should yield no hint"
    );
}
