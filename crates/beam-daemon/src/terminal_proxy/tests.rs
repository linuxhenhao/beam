//! Tests for terminal proxy modules.
//!
//! These tests cover authentication helpers, header forwarding, URL building,
//! token selection, path rewriting, resize message construction, and the
//! [`ViewerCounter`] debounce logic.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::http::{HeaderMap, HeaderName};
use tokio::sync::Mutex;

use crate::terminal_auth::TerminalPermission;
use crate::zellij_web::ZellijWebTokens;

use super::anchor::{
    AnchorCommand, ViewerCounter, ZellijAnchorEntry, ZellijAnchorManager, build_web_resize_message,
    should_ensure_read_only_anchor,
};
use super::http_forward::{forward_request_headers, forward_response_headers, rewrite_asset_paths};
use super::{
    build_ws_target_url, is_terminal_ws_rest, should_strip_response_header,
    zellij_token_for_permission,
};

// ── Header stripping ─────────────────────────────────────────────────────

#[test]
fn strip_set_cookie_header() {
    // Verify Set-Cookie is in the strip list
    assert!(should_strip_response_header("set-cookie"));
    assert!(should_strip_response_header("Set-Cookie"));
    assert!(should_strip_response_header("SET-COOKIE"));
}

#[test]
fn content_length_not_forwarded() {
    let mut dest = HeaderMap::new();
    let mut src = reqwest::header::HeaderMap::new();
    src.insert(
        reqwest::header::CONTENT_LENGTH,
        reqwest::header::HeaderValue::from_static("42"),
    );
    src.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("text/html"),
    );
    forward_response_headers(&mut dest, &src);
    assert!(dest.get("content-length").is_none());
    assert!(dest.get("content-type").is_some());
}

#[test]
fn websocket_handshake_headers_not_forwarded() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::UPGRADE,
        axum::http::HeaderValue::from_static("websocket"),
    );
    headers.insert(
        axum::http::header::CONNECTION,
        axum::http::HeaderValue::from_static("Upgrade"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-key"),
        axum::http::HeaderValue::from_static("abc123"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-version"),
        axum::http::HeaderValue::from_static("13"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-protocol"),
        axum::http::HeaderValue::from_static("chat"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-extensions"),
        axum::http::HeaderValue::from_static("permessage-deflate"),
    );
    headers.insert(
        HeaderName::from_static("x-forwarded-for"),
        axum::http::HeaderValue::from_static("127.0.0.1"),
    );

    let forwarded = forward_request_headers(&headers, Some("beam=cookie"));
    assert!(forwarded.get("sec-websocket-key").is_none());
    assert!(forwarded.get("sec-websocket-version").is_none());
    assert!(forwarded.get("sec-websocket-protocol").is_none());
    assert!(forwarded.get("sec-websocket-extensions").is_none());
    assert!(forwarded.get("upgrade").is_none());
    assert!(forwarded.get("connection").is_none());
    assert_eq!(
        forwarded
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok()),
        Some("127.0.0.1")
    );
    assert_eq!(
        forwarded.get("cookie").and_then(|v| v.to_str().ok()),
        Some("beam=cookie")
    );
}

#[test]
fn forwarded_headers_replace_external_cookie_with_internal_zellij_cookie() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::COOKIE,
        axum::http::HeaderValue::from_static("beam_terminal_session=external-cookie"),
    );
    headers.insert(
        HeaderName::from_static("x-request-id"),
        axum::http::HeaderValue::from_static("req-1"),
    );

    let forwarded = forward_request_headers(&headers, Some("zellij-session=internal-cookie"));

    assert_eq!(
        forwarded.get("cookie").and_then(|v| v.to_str().ok()),
        Some("zellij-session=internal-cookie")
    );
    assert_eq!(
        forwarded.get("x-request-id").and_then(|v| v.to_str().ok()),
        Some("req-1")
    );
}

#[test]
fn forward_request_headers_does_not_mutate_original_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::COOKIE,
        axum::http::HeaderValue::from_static("beam_terminal_session=external-cookie"),
    );
    headers.insert(
        HeaderName::from_static("x-forwarded-for"),
        axum::http::HeaderValue::from_static("127.0.0.1"),
    );
    let original = headers.clone();

    let _ = forward_request_headers(&headers, Some("zellij-session=internal-cookie"));

    assert_eq!(headers, original);
}

// ── URL building ─────────────────────────────────────────────────────────

#[test]
fn build_ws_target_url_uses_ws_scheme() {
    assert_eq!(
        build_ws_target_url(8801, "ws/terminal/beam-123", None),
        "ws://127.0.0.1:8801/ws/terminal/beam-123"
    );
    assert_eq!(
        build_ws_target_url(8801, "/ws/control", Some("foo=bar")),
        "ws://127.0.0.1:8801/ws/control?foo=bar"
    );
}

// ── Token selection ──────────────────────────────────────────────────────

#[test]
fn ticket_permission_selects_matching_zellij_token() {
    let tokens = ZellijWebTokens {
        port: 1234,
        read_only_token: Some("ro-token".to_string()),
        write_token: Some("write-token".to_string()),
        token_name: None,
        read_only_token_name: None,
        write_token_name: None,
    };

    assert_eq!(
        zellij_token_for_permission(&tokens, TerminalPermission::ReadOnly),
        Some("ro-token")
    );
    assert_eq!(
        zellij_token_for_permission(&tokens, TerminalPermission::Write),
        Some("write-token")
    );
}

#[test]
fn ticket_permission_rejects_missing_matching_zellij_token() {
    let tokens = ZellijWebTokens {
        port: 1234,
        read_only_token: Some("ro-token".to_string()),
        write_token: None,
        token_name: None,
        read_only_token_name: None,
        write_token_name: None,
    };

    assert_eq!(
        zellij_token_for_permission(&tokens, TerminalPermission::ReadOnly),
        Some("ro-token")
    );
    assert_eq!(
        zellij_token_for_permission(&tokens, TerminalPermission::Write),
        None
    );
}

// ── Anchor gating ────────────────────────────────────────────────────────

#[test]
fn read_only_permission_triggers_anchor_when_write_token_available() {
    let tokens = ZellijWebTokens {
        port: 1234,
        read_only_token: Some("ro-token".to_string()),
        write_token: Some("write-token".to_string()),
        token_name: None,
        read_only_token_name: None,
        write_token_name: None,
    };

    assert!(should_ensure_read_only_anchor(
        TerminalPermission::ReadOnly,
        &tokens
    ));
    assert!(!should_ensure_read_only_anchor(
        TerminalPermission::Write,
        &tokens
    ));
}

#[test]
fn read_only_anchor_is_noop_without_write_token() {
    let tokens = ZellijWebTokens {
        port: 1234,
        read_only_token: Some("ro-token".to_string()),
        write_token: None,
        token_name: None,
        read_only_token_name: None,
        write_token_name: None,
    };

    assert!(!should_ensure_read_only_anchor(
        TerminalPermission::ReadOnly,
        &tokens
    ));
}

// ── Path rewriting ───────────────────────────────────────────────────────

#[test]
fn rewrite_base_href_for_session() {
    let mut data = b"<html><head><base href=\"/\"></head><body></body></html>".to_vec();
    rewrite_asset_paths(&mut data, Some("my-session"));
    let result = String::from_utf8(data).unwrap();
    assert!(result.contains("<base href=\"/s/my-session/\">"));
}

#[test]
fn rewrite_base_href_skipped_without_session() {
    let mut data = b"<html><head><base href=\"/\"></head><body></body></html>".to_vec();
    rewrite_asset_paths(&mut data, None);
    let result = String::from_utf8(data).unwrap();
    assert_eq!(
        result,
        "<html><head><base href=\"/\"></head><body></body></html>"
    );
}

// ── build_web_resize_message tests ───────────────────────────────────────

/// Verify the wire shape matches zellij's
/// `WebClientToWebServerControlMessage` with a `TerminalResize` payload.
#[test]
fn build_web_resize_message_constructs_correct_wire_shape() {
    let msg = build_web_resize_message("abc-123", 120, 36);
    assert_eq!(msg["web_client_id"], "abc-123");
    assert_eq!(msg["payload"]["type"], "TerminalResize");
    assert_eq!(msg["payload"]["cols"], 120);
    assert_eq!(msg["payload"]["rows"], 36);
    // No extra top-level keys
    let obj = msg.as_object().unwrap();
    assert_eq!(obj.len(), 2, "should only have web_client_id + payload");
}

/// `cols` and `rows` should serialize as JSON numbers (not strings).
#[test]
fn build_web_resize_message_cols_rows_are_numbers() {
    let msg = build_web_resize_message("id", 80, 24);
    let cols = &msg["payload"]["cols"];
    let rows = &msg["payload"]["rows"];
    assert!(cols.is_number(), "cols must be a number, got {:?}", cols);
    assert!(rows.is_number(), "rows must be a number, got {:?}", rows);
}

/// Round-trip: produced JSON must survive a serde parse as a generic Value.
#[test]
fn build_web_resize_message_round_trips() {
    let msg = build_web_resize_message("test-client", 100, 50);
    let json_str = msg.to_string();
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).expect("should parse as valid JSON");
    assert_eq!(parsed["web_client_id"], "test-client");
    assert_eq!(parsed["payload"]["type"], "TerminalResize");
}

/// Anchor default resize uses `TerminalResize` (not TerminalSizeSettled)
/// with 160×50 dimensions.
#[test]
fn anchor_default_resize_uses_terminal_resize_type_160x50() {
    let msg = build_web_resize_message("anchor-1", 160, 50);
    assert_eq!(msg["payload"]["type"], "TerminalResize");
    assert_eq!(msg["payload"]["cols"], 160);
    assert_eq!(msg["payload"]["rows"], 50);
}

// ── is_terminal_ws_rest tests ────────────────────────────────────────────

#[test]
fn terminal_ws_rest_paths_identified() {
    assert!(is_terminal_ws_rest("terminal"));
    assert!(is_terminal_ws_rest("terminal/beam-abc-123"));
    assert!(is_terminal_ws_rest("terminal/some-session"));
}

#[test]
fn control_ws_rest_path_not_terminal() {
    assert!(!is_terminal_ws_rest("control"));
    assert!(!is_terminal_ws_rest(""));
    assert!(!is_terminal_ws_rest("something-else"));
}

// ── ViewerCounter tests ──────────────────────────────────────────────────

/// Build a ViewerCounter backed by a ZellijAnchorManager that contains
/// a dummy (never-finishing) anchor entry with a real command channel.
/// Returns the counter and the receiver so tests can assert on commands.
fn viewer_counter_with_dummy_anchor(
    session: &str,
) -> (
    ViewerCounter,
    tokio::sync::mpsc::UnboundedReceiver<AnchorCommand>,
) {
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_task = tokio::spawn(std::future::pending::<()>());
    let entry = ZellijAnchorEntry {
        task: dummy_task,
        started_at: Instant::now(),
        cmd_tx,
    };
    let mut anchors_map = HashMap::new();
    anchors_map.insert(session.to_string(), entry);
    let anchors = ZellijAnchorManager {
        anchors: Arc::new(Mutex::new(anchors_map)),
    };
    let vc = ViewerCounter {
        inner: Arc::new(Mutex::new(HashMap::new())),
        anchors,
    };
    (vc, cmd_rx)
}

/// Increment and decrement are tracked per session.
#[tokio::test]
async fn viewer_counter_increment_decrement() {
    let (vc, _cmd_rx) = viewer_counter_with_dummy_anchor("sess-1");

    vc.increment("sess-1").await;
    vc.increment("sess-1").await;
    assert_eq!(vc.inner.lock().await.get("sess-1").unwrap().count, 2);

    vc.decrement("sess-1").await;
    assert_eq!(vc.inner.lock().await.get("sess-1").unwrap().count, 1);
}

/// Decrement for a session that has never been incremented is a no-op.
#[tokio::test]
async fn viewer_counter_decrement_below_zero_is_noop() {
    let (vc, _cmd_rx) = viewer_counter_with_dummy_anchor("s");
    vc.decrement("no-such-session").await;
    assert!(vc.inner.lock().await.get("no-such-session").is_none());
}

/// Defensive: a second decrement when count is already 0 and a debounce
/// task is pending MUST NOT spawn a second task (no leak).
#[tokio::test]
async fn viewer_counter_double_decrement_no_duplicate_debounce() {
    let (vc, _cmd_rx) = viewer_counter_with_dummy_anchor("s");

    vc.increment("s").await;
    vc.decrement("s").await; // count 0 → spawns first debounce
    vc.decrement("s").await; // count still 0, debounce already pending

    let inner = vc.inner.lock().await;
    let state = inner.get("s").unwrap();
    assert_eq!(state.count, 0);
    assert!(
        state.pending_reset.is_some(),
        "first debounce should still exist"
    );
}

/// Count 1→0: debounce fires after >800ms and sends ResizeToDefault to
/// the anchor's command channel.
#[tokio::test]
async fn viewer_counter_debounce_sends_resize_to_default_after_delay() {
    let (vc, mut cmd_rx) = viewer_counter_with_dummy_anchor("test-sess");

    vc.increment("test-sess").await;
    vc.decrement("test-sess").await; // count 0 → spawns debounce

    // The debounce task sleeps 800ms; wait long enough for it to fire.
    tokio::time::sleep(std::time::Duration::from_millis(900)).await;

    // Expect exactly one ResizeToDefault command on the channel.
    match tokio::time::timeout(std::time::Duration::from_secs(1), cmd_rx.recv()).await {
        Ok(Some(AnchorCommand::ResizeToDefault)) => { /* expected */ }
        Ok(None) => panic!("channel closed unexpectedly"),
        Err(_elapsed) => panic!("timed out waiting for ResizeToDefault"),
    }
}

/// Count 1→0→1 (reconnect during debounce window): the debounce is
/// cancelled and no ResizeToDefault is sent.
#[tokio::test]
async fn viewer_counter_debounce_skips_on_reconnect() {
    let (vc, mut cmd_rx) = viewer_counter_with_dummy_anchor("test-sess");

    vc.increment("test-sess").await;
    vc.decrement("test-sess").await; // count 0 → debounce starts
    vc.increment("test-sess").await; // reconnect immediately → abort debounce

    // Sleep past the 800ms debounce window.
    tokio::time::sleep(std::time::Duration::from_millis(900)).await;

    // The command channel should NOT contain any message.
    match tokio::time::timeout(std::time::Duration::from_millis(100), cmd_rx.recv()).await {
        Ok(None) | Err(tokio::time::error::Elapsed { .. }) => { /* expected — no message */ }
        Ok(Some(cmd)) => panic!("unexpected command after reconnect: {:?}", cmd),
    }
}
