//! Periodic worker-health watchdog: flag workers that stopped heartbeating
//! so the state is visible on the session card and via `beam status`.

use super::*;

/// Periodic watchdog: flag sessions whose worker stopped heartbeating (hung,
/// or dead but not yet reaped) so the state is visible on the session card
/// and via `beam status`. Only workers that have sent at least one heartbeat
/// are judged; older workers simply stay "unknown".
pub(crate) fn spawn_worker_health_watchdog(state: AppState) {
    tokio::spawn(async move {
        const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(45);
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let stale_sessions: Vec<(String, Option<u64>)> = {
                let workers = state.workers.lock().await;
                let sessions = state.sessions.lock().await;
                let mut health = state.worker_health.lock().await;
                let now = Instant::now();
                let mut stale = Vec::new();
                for (session_id, entry) in health.iter_mut() {
                    let worker_present = workers.contains_key(session_id);
                    let session_active = sessions
                        .get(session_id)
                        .map(|s| s.status == SessionStatus::Active)
                        .unwrap_or(false);
                    if !worker_present || !session_active || entry.unresponsive {
                        continue;
                    }
                    if now.duration_since(entry.last_heartbeat) > STALE_AFTER {
                        entry.unresponsive = true;
                        stale.push((session_id.clone(), entry.processing_since_ms));
                    }
                }
                stale
            };
            for (session_id, processing_since_ms) in stale_sessions {
                match processing_since_ms {
                    Some(start_ms) => {
                        let stuck_ms =
                            (Utc::now().timestamp_millis().max(0) as u64).saturating_sub(start_ms);
                        warn!(
                            "worker for session {} is unresponsive: no heartbeat for >{}s; message loop stuck processing for {}ms",
                            session_id,
                            STALE_AFTER.as_secs(),
                            stuck_ms
                        );
                    }
                    None => {
                        warn!(
                            "worker for session {} is unresponsive: no heartbeat for >{}s",
                            session_id,
                            STALE_AFTER.as_secs()
                        );
                    }
                }
                let _ = patch_lark_streaming_card(&state, &session_id, "worker 无响应").await;
            }
        }
    });
}
