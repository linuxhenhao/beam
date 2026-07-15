//! Streaming-card / pending-response tracking.
//!
//! These helpers manage the lifecycle of a "pending response card" (streaming card)
//! that must NOT be overwritten by `deliver_final_output_once`.
//!
//! Invariant: `deliver_final_output_once` only patches a pending response card when
//! it has been explicitly claimed via `claim_pending_response_card` and the claim is
//! still valid at delivery time.

use crate::*;

// ---------------------------------------------------------------------------
// pending response patch marker file I/O
// ---------------------------------------------------------------------------

pub(crate) async fn read_pending_response_patch_marker(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<Option<PendingResponsePatchMarker>> {
    match tokio::fs::read(paths.pending_response_patch_json(session_id)).await {
        Ok(bytes) => {
            let marker = serde_json::from_slice::<PendingResponsePatchMarker>(&bytes)?;
            if marker.session_id != session_id || marker.card_id.trim().is_empty() {
                return Ok(None);
            }
            if marker.state != "patching" && marker.state != "patched" {
                return Ok(None);
            }
            Ok(Some(marker))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn write_pending_response_patch_marker(
    paths: &BeamPaths,
    session_id: &str,
    card_id: &str,
) -> Result<()> {
    tokio::fs::create_dir_all(paths.pending_response_patches_dir()).await?;
    let path = paths.pending_response_patch_json(session_id);
    let tmp = path.with_extension("json.tmp");
    let marker = PendingResponsePatchMarker {
        session_id: session_id.to_string(),
        card_id: card_id.to_string(),
        state: "patching".to_string(),
        created_at: Utc::now().to_rfc3339(),
        patched_at: None,
    };
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&marker)?).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

pub(crate) async fn mark_pending_response_patch_marker_patched(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<()> {
    let Some(mut marker) = read_pending_response_patch_marker(paths, session_id).await? else {
        return Ok(());
    };
    marker.state = "patched".to_string();
    marker.patched_at = Some(Utc::now().to_rfc3339());
    let path = paths.pending_response_patch_json(session_id);
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&marker)?).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

pub(crate) async fn clear_pending_response_patch_marker(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<()> {
    let path = paths.pending_response_patch_json(session_id);
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

// ---------------------------------------------------------------------------
// pending response card lifecycle helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(crate) fn is_pending_response_card_open(session: &Session) -> bool {
    session.pending_response_card_id.is_some()
        && session.pending_response_card_state == Some(PendingResponseCardState::Open)
}

pub(crate) fn start_pending_response_turn(session: &mut Session, message_id: String) {
    session.pending_response_card_id = Some(message_id);
    session.pending_response_card_state = Some(PendingResponseCardState::Open);
}

pub(crate) fn mark_pending_response_card_patched(session: &mut Session) {
    session.last_patched_response_card_id = session.pending_response_card_id.clone();
    session.pending_response_card_id = None;
    session.pending_response_card_state = Some(PendingResponseCardState::Patched);
}

pub(crate) fn mark_pending_response_card_patched_if_current(
    session: &mut Session,
    card_id: &str,
) -> bool {
    if session.pending_response_card_id.as_deref() != Some(card_id)
        || session.pending_response_card_state != Some(PendingResponseCardState::Open)
    {
        return false;
    }
    mark_pending_response_card_patched(session);
    true
}

#[allow(dead_code)]
pub(crate) fn claim_pending_response_card(session: &Session) -> Option<String> {
    if is_pending_response_card_open(session) {
        session.pending_response_card_id.clone()
    } else {
        None
    }
}

pub(crate) fn clear_pending_response_tracking(session: &mut Session) {
    session.pending_response_card_id = None;
    session.pending_response_card_state = None;
    session.last_patched_response_card_id = None;
}

pub(crate) fn should_treat_pending_card_as_patched_by_marker(
    pending_card_id: Option<&str>,
    marker: Option<&PendingResponsePatchMarker>,
) -> bool {
    matches!(
        (pending_card_id, marker),
        (Some(card_id), Some(marker))
            if marker.state == "patched" && marker.card_id == card_id
    )
}

// ---------------------------------------------------------------------------
// worker-ready display-mode helpers
// ---------------------------------------------------------------------------

pub(crate) fn worker_ready_display_mode_command(session: &Session) -> Option<DaemonToWorker> {
    match session.display_mode {
        Some(DisplayMode::Screenshot) => Some(DaemonToWorker::SetDisplayMode {
            mode: DisplayMode::Screenshot,
        }),
        _ => None,
    }
}

pub(crate) async fn resend_display_mode_after_worker_ready(
    state: &AppState,
    session_id: &str,
) -> Result<()> {
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(session_id).cloned()
    };
    let Some(session) = session else {
        return Ok(());
    };
    let Some(msg) = worker_ready_display_mode_command(&session) else {
        return Ok(());
    };
    send_worker_message(&state.workers, session_id, &msg).await
}
