#![allow(clippy::await_holding_lock)]

use super::*;
use crate::tests::test_helpers::*;
use crate::{BackendKind, SessionStatus};
use serde_json::Value;

#[test]
fn build_streaming_card_herdr_ready_shows_terminal_buttons() {
    let mut session = make_session("sess-herdr");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.backend_kind = BackendKind::Herdr;
    session.terminal_url = None;
    session.herdr_workspace_id = Some("w1".to_string());
    session.herdr_pane_id = Some("w1:p1".to_string());
    session.stream_card_nonce = Some("nonce-herdr".to_string());
    let card: Value = serde_json::from_str(&build_streaming_card(&session, "idle", true))
        .expect("valid card json");
    let serialized = serde_json::to_string(&card).expect("serialize card");
    assert!(
        serialized.contains("choose_read_only_terminal_link"),
        "herdr card with a ready pane must show the read-only terminal button"
    );
    assert!(
        serialized.contains("get_write_link"),
        "herdr card with a ready pane must show the write-link button"
    );
    assert!(
        !serialized.contains("herdr agent attach"),
        "herdr card with a ready pane must not show the attach hint"
    );
}

#[test]
fn build_streaming_card_herdr_not_ready_shows_attach_hint() {
    let mut session = make_session("sess-herdr");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.backend_kind = BackendKind::Herdr;
    session.terminal_url = None;
    session.stream_card_nonce = Some("nonce-herdr".to_string());
    let card: Value = serde_json::from_str(&build_streaming_card(&session, "idle", true))
        .expect("valid card json");
    let serialized = serde_json::to_string(&card).expect("serialize card");
    assert!(
        serialized.contains("herdr agent attach"),
        "herdr card without a pane must show the attach hint, got: {serialized}"
    );
    assert!(!serialized.contains("choose_read_only_terminal_link"));
}

#[test]
fn build_streaming_card_herdr_disabled_keeps_attach_hint() {
    let mut session = make_session("sess-herdr");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.backend_kind = BackendKind::Herdr;
    session.herdr_workspace_id = Some("w1".to_string());
    session.herdr_pane_id = Some("w1:p1".to_string());
    let card: Value = serde_json::from_str(&build_streaming_card(&session, "idle", false))
        .expect("valid card json");
    let serialized = serde_json::to_string(&card).expect("serialize card");
    assert!(
        serialized.contains("herdr agent attach"),
        "herdr card must fall back to the attach hint when the web terminal is disabled"
    );
    assert!(!serialized.contains("choose_read_only_terminal_link"));
}
