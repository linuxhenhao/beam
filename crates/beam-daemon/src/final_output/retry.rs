//! Retry scheduling, deduplication markers, and persistence helpers.

use crate::*;
use std::time::Duration;

// ---------------------------------------------------------------------------
// retry delay & markers
// ---------------------------------------------------------------------------

pub(crate) fn next_final_output_retry_delay_ms(attempt: usize) -> Option<u64> {
    FINAL_OUTPUT_RETRY_BACKOFF_MS.get(attempt).copied()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct FinalOutputRetryMarker {
    pub(crate) session_id: String,
    pub(crate) content: String,
    pub(crate) turn_id: Option<String>,
    pub(crate) kind: Option<FinalOutputKind>,
    pub(crate) user_text: Option<String>,
    pub(crate) attempt: usize,
    pub(crate) created_at: String,
}

pub(crate) fn load_final_output_retry_markers(paths: &BeamPaths) -> Vec<FinalOutputRetryMarker> {
    match beam_core::persist::read_json::<Vec<FinalOutputRetryMarker>>(
        &paths.final_output_retries_json(),
    ) {
        Ok(Some(markers)) => markers,
        Ok(None) | Err(_) => Vec::new(),
    }
}

pub(crate) fn save_final_output_retry_markers(
    paths: &BeamPaths,
    markers: &[FinalOutputRetryMarker],
) {
    if markers.is_empty() {
        let _ = std::fs::remove_file(paths.final_output_retries_json());
        return;
    }
    let _ = beam_core::persist::atomic_write_json(
        &paths.final_output_retries_json(),
        &markers.to_vec(),
    );
}

pub(crate) fn persist_final_output_retry_marker(
    state: &AppState,
    session_id: &str,
    content: String,
    turn_id: Option<String>,
    kind: Option<FinalOutputKind>,
    user_text: Option<String>,
    attempt: usize,
) {
    let mut markers = load_final_output_retry_markers(&state.paths);
    // Replace existing marker for this (session_id, turn_id) pair
    let turn_id_str = turn_id.as_deref().unwrap_or("");
    markers.retain(|m| {
        !(m.session_id == session_id && m.turn_id.as_deref().unwrap_or("") == turn_id_str)
    });
    markers.push(FinalOutputRetryMarker {
        session_id: session_id.to_string(),
        content,
        turn_id,
        kind,
        user_text,
        attempt,
        created_at: chrono::Utc::now().to_rfc3339(),
    });
    save_final_output_retry_markers(&state.paths, &markers);
}

pub(crate) fn clear_final_output_retry(state: &AppState, session_id: &str, turn_id: Option<&str>) {
    let mut markers = load_final_output_retry_markers(&state.paths);
    let before = markers.len();
    let turn_id_str = turn_id.unwrap_or("");
    markers.retain(|m| {
        !(m.session_id == session_id && m.turn_id.as_deref().unwrap_or("") == turn_id_str)
    });
    if markers.len() != before {
        save_final_output_retry_markers(&state.paths, &markers);
    }
}

// ---------------------------------------------------------------------------
// turn key & dedupe
// ---------------------------------------------------------------------------

pub(crate) fn final_output_turn_key(session_id: &str, turn_id: &str) -> Option<String> {
    if turn_id.is_empty() {
        None
    } else {
        Some(format!("{}:{}", session_id, turn_id))
    }
}

pub(crate) fn should_skip_worker_final_output(
    session: &Session,
    turn_id: &str,
    content: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    // ---- turn-id dedupe (unchanged legacy logic) ----
    if !turn_id.is_empty() && session.last_final_output_turn_id.as_deref() == Some(turn_id) {
        return true;
    }

    // ---- explicit-send dedupe (minimal botmux-equivalent) ----
    if let Some(last_explicit_send_at) = session.last_explicit_send_at {
        let age = now - last_explicit_send_at;
        if age < chrono::Duration::minutes(10)
            && normalize_final_output(content)
                == normalize_final_output(session.last_final_output.as_deref().unwrap_or(""))
        {
            return true;
        }
    }

    false
}

/// Lightweight content normalisation for dedupe comparisons.
fn normalize_final_output(content: &str) -> &str {
    content.trim()
}

// ---------------------------------------------------------------------------
// abort check
// ---------------------------------------------------------------------------

pub(crate) fn should_abort_final_output_delivery(session: Option<&Session>) -> bool {
    session
        .map(|session| session.status == SessionStatus::Closed)
        .unwrap_or(true)
}

// ---------------------------------------------------------------------------
// scheduled delivery with retry
// ---------------------------------------------------------------------------

pub(crate) fn schedule_final_output_delivery(
    state: AppState,
    session_id: String,
    content: String,
    turn_id: Option<String>,
    kind: Option<FinalOutputKind>,
    user_text: Option<String>,
    attempt: usize,
) {
    let Some(delay_ms) = next_final_output_retry_delay_ms(attempt) else {
        return;
    };
    // Persist retry marker so daemon restart can resume delivery
    persist_final_output_retry_marker(
        &state,
        &session_id,
        content.clone(),
        turn_id.clone(),
        kind,
        user_text.clone(),
        attempt,
    );
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let turn_key = turn_id
            .as_deref()
            .and_then(|turn_id| final_output_turn_key(&session_id, turn_id));

        let session_closed = {
            let sessions = state.sessions.lock().await;
            should_abort_final_output_delivery(sessions.get(&session_id))
        };
        if session_closed {
            if let Some(turn_key) = turn_key.as_deref() {
                state
                    .inflight_final_output_turns
                    .lock()
                    .await
                    .remove(turn_key);
            }
            return;
        }

        match super::delivery::deliver_final_output_once(
            &state,
            &session_id,
            &content,
            turn_id.as_deref(),
            kind,
            user_text.as_deref(),
        )
        .await
        {
            Ok(()) => {
                clear_final_output_retry(&state, &session_id, turn_id.as_deref());
                if let Some(turn_key) = turn_key.as_deref() {
                    state
                        .inflight_final_output_turns
                        .lock()
                        .await
                        .remove(turn_key);
                }
            }
            Err(err) => {
                if is_lark_message_withdrawn_error(&err) {
                    warn!(
                        "final output delivery for {} aborted because the root message was withdrawn",
                        session_id
                    );
                    if let Some(turn_key) = turn_key.as_deref() {
                        state
                            .inflight_final_output_turns
                            .lock()
                            .await
                            .remove(turn_key);
                    }
                    let _ = close_session(State(state.clone()), AxumPath(session_id.clone())).await;
                    return;
                }
                let next = attempt + 1;
                let Some(next_delay_ms) = next_final_output_retry_delay_ms(next) else {
                    clear_final_output_retry(&state, &session_id, turn_id.as_deref());
                    if let Some(turn_key) = turn_key.as_deref() {
                        state
                            .inflight_final_output_turns
                            .lock()
                            .await
                            .remove(turn_key);
                    }
                    warn!(
                        "final output delivery gave up for {} after {} attempts: {}",
                        session_id, next, err
                    );
                    return;
                };
                warn!(
                    "final output delivery attempt {} failed for {}: {}; retrying in {}ms",
                    next, session_id, err, next_delay_ms
                );
                schedule_final_output_delivery(
                    state, session_id, content, turn_id, kind, user_text, next,
                );
            }
        }
    });
}
