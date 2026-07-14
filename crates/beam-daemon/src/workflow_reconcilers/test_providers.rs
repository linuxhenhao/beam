//! Tests for built-in ProviderReconciler implementations
//! (BeamScheduleReconciler + FeishuImReconciler).

use super::providers::{BeamScheduleReconciler, FeishuImReconciler};
use super::registry::ProviderReconciler;

// -----------------------------------------------------------------------
// Trait implementation tests
// -----------------------------------------------------------------------

#[test]
fn beam_schedule_reconciler_metadata() {
    let r = BeamScheduleReconciler;
    assert_eq!(r.provider_name(), "beam-schedule");
    assert!(!r.requires_effect_input());
}

#[test]
fn feishu_im_reconciler_metadata() {
    let r = FeishuImReconciler;
    assert_eq!(r.provider_name(), "feishu-im");
    assert!(r.requires_effect_input());
}

#[test]
fn beam_schedule_is_not_retryable() {
    let r = BeamScheduleReconciler;
    assert!(!r.is_retryable_error(&anyhow::anyhow!("file not found")));
}

#[test]
fn feishu_im_is_retryable_detects_timeout() {
    let r = FeishuImReconciler;
    // Use an anyhow error containing retryable keywords rather than
    // constructing a private reqwest::ErrorKind directly.
    let timeout_err = anyhow::anyhow!("request timeout: timed out after 30s");
    assert!(r.is_retryable_error(&timeout_err));
}

#[test]
fn feishu_im_is_retryable_detects_rate_limit() {
    let r = FeishuImReconciler;
    assert!(r.is_retryable_error(&anyhow::anyhow!("HTTP 429: too many requests")));
}

#[test]
fn feishu_im_is_retryable_rejects_generic_error() {
    let r = FeishuImReconciler;
    assert!(!r.is_retryable_error(&anyhow::anyhow!("permission denied")));
}

#[test]
fn feishu_im_canonical_input_parses_chat_id_variant() {
    let r = FeishuImReconciler;
    let raw = serde_json::json!({
        "larkAppId": "app-1",
        "chatId": "chat-1",
        "content": "hello"
    });
    let canonical = r.canonical_input(&raw).expect("canonical input parse");
    assert_eq!(canonical["larkAppId"], "app-1");
    assert_eq!(canonical["chatId"], "chat-1");
    assert_eq!(canonical["content"], "hello");
    // canonical should NOT contain msgType
    assert!(canonical.get("msgType").is_none());
    assert!(canonical.get("rootMessageId").is_none());
}

#[test]
fn feishu_im_canonical_input_parses_reply_variant() {
    let r = FeishuImReconciler;
    let raw = serde_json::json!({
        "larkAppId": "app-1",
        "rootMessageId": "msg-1",
        "content": "reply"
    });
    let canonical = r.canonical_input(&raw).expect("canonical input parse");
    assert_eq!(canonical["larkAppId"], "app-1");
    assert_eq!(canonical["rootMessageId"], "msg-1");
    assert_eq!(canonical["content"], "reply");
    assert!(canonical.get("chatId").is_none());
}

#[test]
fn feishu_im_canonical_input_missing_target_both_still_succeeds() {
    // canonical_input does NOT validate that at least one target is present;
    // that check is deferred to idempotent_submit.
    let r = FeishuImReconciler;
    let raw = serde_json::json!({
        "larkAppId": "app-1",
        "content": "no target"
    });
    let canonical = r
        .canonical_input(&raw)
        .expect("canonical input should parse");
    assert_eq!(canonical["larkAppId"], "app-1");
    assert_eq!(canonical["content"], "no target");
    assert!(canonical.get("chatId").is_none());
    assert!(canonical.get("rootMessageId").is_none());
}

#[test]
fn beam_schedule_canonical_input_is_passthrough() {
    let r = BeamScheduleReconciler;
    let raw = serde_json::json!({"name": "test"});
    let canonical = r.canonical_input(&raw).unwrap();
    assert_eq!(canonical, raw);
}

#[test]
fn beam_schedule_supports_read_only_lookup_only() {
    let r = BeamScheduleReconciler;
    assert!(r.supports_read_only_lookup());
    assert!(!r.supports_idempotent_submit());
}

#[test]
fn feishu_im_supports_idempotent_submit_only() {
    let r = FeishuImReconciler;
    assert!(!r.supports_read_only_lookup());
    assert!(r.supports_idempotent_submit());
}

#[test]
fn feishu_im_canonical_input_rejects_missing_lark_app_id() {
    let r = FeishuImReconciler;
    let raw = serde_json::json!({
        "chatId": "chat-1",
        "content": "hello"
    });
    let err = r.canonical_input(&raw).unwrap_err();
    assert!(
        format!("{err:#}").contains("larkAppId") || format!("{err}").contains("larkAppId"),
        "should mention missing larkAppId"
    );
}
