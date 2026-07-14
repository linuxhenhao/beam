use super::*;

pub(crate) mod connectors;
pub(crate) mod sessions;
pub(crate) mod workflows;

// Re-export all handler functions so that lib.rs's glob re-export
// (`pub(crate) use route_handlers::*;`) continues to work.
pub(crate) use connectors::*;
pub(crate) use sessions::*;
pub(crate) use workflows::*;

// ---------------------------------------------------------------------------
// General-purpose handlers that don't fit into sessions/workflows/connectors
// ---------------------------------------------------------------------------

pub(crate) async fn shutdown(
    State(state): State<AppState>,
) -> Result<StatusCode, (StatusCode, String)> {
    if let Some(tx) = state.shutdown.lock().await.take() {
        let _ = tx.send(());
    }
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn health(State(state): State<AppState>) -> Json<ApiHealth> {
    Json(ApiHealth {
        status: "ok".to_string(),
        pid: std::process::id(),
        started_at: state.started_at,
    })
}
