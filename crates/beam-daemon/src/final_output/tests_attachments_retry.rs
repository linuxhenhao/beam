use std::collections::HashMap;

use beam_core::SessionStatus;

use super::*;
use crate::tests::test_helpers::*;

#[test]
fn final_output_retry_delay_matches_three_attempt_backoff() {
    assert_eq!(next_final_output_retry_delay_ms(0), Some(0));
    assert_eq!(next_final_output_retry_delay_ms(1), Some(5_000));
    assert_eq!(next_final_output_retry_delay_ms(2), Some(15_000));
    assert_eq!(next_final_output_retry_delay_ms(3), None);
}

#[test]
fn final_output_delivery_aborts_for_closed_or_missing_session() {
    assert!(should_abort_final_output_delivery(None));

    let closed = make_session("sess-closed");
    assert!(should_abort_final_output_delivery(Some(&closed)));

    let mut active = make_session("sess-active");
    active.status = SessionStatus::Active;
    active.closed_at = None;
    assert!(!should_abort_final_output_delivery(Some(&active)));
}

#[test]
fn worker_final_output_dedupes_by_turn_id_instead_of_content() {
    let mut session = make_session("sess-final-output");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.last_final_output_turn_id = Some("turn-1".to_string());
    session.last_final_output = Some("done".to_string());
    let now = chrono::Utc::now();

    // turn-id match skips regardless of content passed
    assert!(should_skip_worker_final_output(
        &session, "turn-1", "anything", now
    ));
    // different turn-id passes
    assert!(!should_skip_worker_final_output(
        &session, "turn-2", "done", now
    ));
    // empty turn-id passes
    assert!(!should_skip_worker_final_output(&session, "", "done", now));
}

#[test]
fn worker_final_output_skips_recent_explicit_same_content() {
    let mut session = make_session("sess-explicit-recent");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.last_final_output = Some("hello world".to_string());
    session.last_explicit_send_at = Some(chrono::Utc::now());

    let now = chrono::Utc::now();
    // recent explicit send with same content → skip
    assert!(should_skip_worker_final_output(
        &session,
        "turn-3",
        "hello world",
        now
    ));
    // content normalised (trim) → still matches
    assert!(should_skip_worker_final_output(
        &session,
        "turn-4",
        "  hello world\n",
        now
    ));
}

#[test]
fn worker_final_output_does_not_skip_different_content() {
    let mut session = make_session("sess-explicit-different");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.last_final_output = Some("hello world".to_string());
    session.last_explicit_send_at = Some(chrono::Utc::now());

    let now = chrono::Utc::now();
    // different content even with recent explicit send → pass through
    assert!(!should_skip_worker_final_output(
        &session,
        "turn-5",
        "other output",
        now
    ));
}

#[test]
fn worker_final_output_does_not_skip_old_explicit_send() {
    let mut session = make_session("sess-explicit-old");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.last_final_output = Some("hello world".to_string());
    // explicit send was 20 minutes ago
    session.last_explicit_send_at = Some(chrono::Utc::now() - chrono::Duration::minutes(20));

    let now = chrono::Utc::now();
    // older than 10-minute window → pass through even with same content
    assert!(!should_skip_worker_final_output(
        &session,
        "turn-6",
        "hello world",
        now
    ));
}

#[test]
fn worker_final_output_no_explicit_marker_still_works() {
    let mut session = make_session("sess-no-explicit");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.last_final_output = Some("output".to_string());
    // no explicit marker set
    session.last_explicit_send_at = None;

    let now = chrono::Utc::now();
    // without explicit marker, only turn-id dedupe applies (no match here)
    assert!(!should_skip_worker_final_output(
        &session, "turn-7", "output", now
    ));
}

// ---- image inlining tests ----

#[test]
fn build_final_output_card_without_images_is_backward_compatible() {
    let old = build_final_output_card("hello", None, None, None, None);
    let new = build_final_output_card_with_images("hello", None, None, None, None, &[]);
    assert_eq!(old, new, "no images should produce identical card JSON");
}

#[test]
fn build_final_output_card_with_images_inserts_img_elements_before_footer() {
    let keys = vec!["img_key_1".to_string(), "img_key_2".to_string()];
    let card = build_final_output_card_with_images(
        "content",
        Some("ou_recipient"), // triggers footer
        None,
        None,
        None,
        &keys,
    );
    let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
    let elements = v["body"]["elements"]
        .as_array()
        .expect("elements should be an array");

    // Find the positions of img elements and footer elements
    let img_indices: Vec<usize> = elements
        .iter()
        .enumerate()
        .filter(|(_, el)| el["tag"].as_str() == Some("img"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(img_indices.len(), 2, "expected 2 img elements");
    assert_eq!(
        elements[img_indices[0]]["img_key"].as_str(),
        Some("img_key_1")
    );
    assert_eq!(
        elements[img_indices[1]]["img_key"].as_str(),
        Some("img_key_2")
    );

    // Verify img elements are before the footer (hr + notation markdown)
    let footer_start = img_indices[1] + 1;
    let footer_hr = &elements[footer_start];
    assert_eq!(
        footer_hr["tag"].as_str(),
        Some("hr"),
        "expected hr (footer separator) after last img"
    );

    // Also verify content markdown comes before images
    assert_eq!(elements[0]["tag"].as_str(), Some("markdown"));
    assert_eq!(elements[0]["content"].as_str(), Some("content"));
    assert!(img_indices[0] > 0, "img elements should come after content");
}

#[test]
fn build_final_output_card_with_images_without_recipient_has_brand_footer() {
    // When recipient_open_id is None, the footer still includes the
    // brand label. That's the normal case for --no-mention or bot-to-bot.
    let keys = vec!["img_key_1".to_string()];
    let card = build_final_output_card_with_images(
        "content", None, // recipient_open_id = None → brand label footer only
        None, None, None, &keys,
    );
    let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
    let elements = v["body"]["elements"]
        .as_array()
        .expect("elements should be an array");
    // Should have: markdown, img, hr, footer_markdown = 4
    assert_eq!(elements.len(), 4);
    assert_eq!(elements[0]["tag"].as_str(), Some("markdown"));
    assert_eq!(elements[1]["tag"].as_str(), Some("img"));
    assert_eq!(elements[1]["img_key"].as_str(), Some("img_key_1"));
    assert_eq!(
        elements[2]["tag"].as_str(),
        Some("hr"),
        "hr separator before footer"
    );
    assert_eq!(
        elements[3]["tag"].as_str(),
        Some("markdown"),
        "footer markdown"
    );
}

#[test]
fn build_final_output_card_skips_empty_image_keys() {
    let keys = vec!["  ".to_string(), "img_key_1".to_string()];
    let card = build_final_output_card_with_images("content", None, None, None, None, &keys);
    let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
    let elements = v["body"]["elements"]
        .as_array()
        .expect("elements should be an array");
    let img_count = elements
        .iter()
        .filter(|el| el["tag"].as_str() == Some("img"))
        .count();
    assert_eq!(img_count, 1, "empty image key should be skipped");
    let img = elements
        .iter()
        .find(|el| el["tag"].as_str() == Some("img"))
        .unwrap();
    assert_eq!(img["img_key"].as_str(), Some("img_key_1"));
}

#[test]
fn build_final_output_card_no_images_produces_no_img_elements() {
    let card = build_final_output_card("content", None, None, None, None);
    let v: serde_json::Value = serde_json::from_str(&card).expect("card JSON should be valid");
    let elements = v["body"]["elements"]
        .as_array()
        .expect("elements should be an array");
    let img_count = elements
        .iter()
        .filter(|el| el["tag"].as_str() == Some("img"))
        .count();
    assert_eq!(img_count, 0, "no img elements when no images");
}

// ---- explicit-send turn marking tests ----

#[tokio::test]
async fn explicit_send_marks_current_turn_as_answered() {
    let paths = temp_paths("explicit-send-turn");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");
    let state = make_state(paths.clone(), HashMap::new());

    let mut session = make_session("sess-explicit-turn");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.current_turn_id = Some("turn-explicit-1".to_string());
    state
        .sessions
        .lock()
        .await
        .insert(session.session_id.clone(), session);

    // What handle_final_output_request records after a successful beam send.
    let answered_turn_id = current_turn_id_for_explicit_send(&state, "sess-explicit-turn").await;
    assert_eq!(answered_turn_id.as_deref(), Some("turn-explicit-1"));
    commit_delivered_final_output(
        &state,
        "sess-explicit-turn",
        "explicit reply",
        answered_turn_id.as_deref(),
    )
    .await
    .expect("commit delivered final output");

    let stored = {
        let sessions = state.sessions.lock().await;
        sessions
            .get("sess-explicit-turn")
            .cloned()
            .expect("session")
    };
    assert_eq!(
        stored.last_final_output_turn_id.as_deref(),
        Some("turn-explicit-1")
    );
    assert_eq!(stored.last_final_output.as_deref(), Some("explicit reply"));

    let now = chrono::Utc::now();
    // Same-turn worker final output is skipped even when its content differs.
    assert!(should_skip_worker_final_output(
        &stored,
        "turn-explicit-1",
        "different terminal text",
        now
    ));
    // Output from a later turn still goes through.
    assert!(!should_skip_worker_final_output(
        &stored,
        "turn-explicit-2",
        "different terminal text",
        now
    ));

    // Unknown session → no turn to mark.
    assert_eq!(
        current_turn_id_for_explicit_send(&state, "sess-missing").await,
        None
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn explicit_send_without_active_turn_marks_nothing() {
    let paths = temp_paths("explicit-send-no-turn");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).expect("mkdir root");
    let state = make_state(paths.clone(), HashMap::new());

    let mut session = make_session("sess-no-turn");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.current_turn_id = None;
    state
        .sessions
        .lock()
        .await
        .insert(session.session_id.clone(), session);

    let answered_turn_id = current_turn_id_for_explicit_send(&state, "sess-no-turn").await;
    assert_eq!(answered_turn_id, None);
    commit_delivered_final_output(&state, "sess-no-turn", "explicit reply", None)
        .await
        .expect("commit delivered final output");

    let stored = {
        let sessions = state.sessions.lock().await;
        sessions.get("sess-no-turn").cloned().expect("session")
    };
    assert_eq!(stored.last_final_output_turn_id, None);
    assert_eq!(stored.last_final_output.as_deref(), Some("explicit reply"));

    maybe_remove_dir(&paths.root().to_path_buf());
}
