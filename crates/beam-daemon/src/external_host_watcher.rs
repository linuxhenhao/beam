use std::time::Duration;

use anyhow::Result;
use tracing::{debug, info, warn};

use super::*;

const EXTERNAL_HOST_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
const EXTERNAL_HOST_REFRESH_DEBOUNCE: Duration = Duration::from_secs(1);

async fn apply_external_host_update(
    state: &AppState,
    next_host: String,
    force_rewrite: bool,
) -> Result<bool> {
    let host_changed = {
        let mut guard = state.external_host.write().await;
        let host_changed = *guard != next_host;
        if host_changed {
            *guard = next_host.clone();
        }
        host_changed
    };
    if host_changed || force_rewrite {
        let changed_sessions = {
            let mut sessions = state.sessions.lock().await;
            let changed_sessions = rewrite_session_terminal_urls(
                &mut sessions,
                &next_host,
                state.config.web.proxy_base_port,
            );
            if changed_sessions > 0 {
                let snapshot = sessions.clone();
                drop(sessions);
                persist_sessions(&state.paths, &snapshot).await?;
            }
            changed_sessions
        };
        if host_changed {
            info!(
                "external host updated to {} (rewrote {} sessions)",
                next_host, changed_sessions
            );
        } else if force_rewrite && changed_sessions > 0 {
            debug!(
                "external host rewrite refreshed {} sessions for {}",
                changed_sessions, next_host
            );
        }
    }
    Ok(host_changed)
}

pub(crate) async fn refresh_external_host(state: &AppState, force_rewrite: bool) -> Result<bool> {
    let next_host = resolve_external_host(&state.config.web.host);
    apply_external_host_update(state, next_host, force_rewrite).await
}

pub(crate) fn spawn_external_host_watcher(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + EXTERNAL_HOST_REFRESH_INTERVAL,
            EXTERNAL_HOST_REFRESH_INTERVAL,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut watch =
            match netwatcher::watch_interfaces_async::<netwatcher::async_adapter::Tokio>() {
                Ok(watch) => Some(watch),
                Err(err) => {
                    warn!("failed to start external host watcher: {}", err);
                    None
                }
            };
        loop {
            match watch.as_mut() {
                Some(watch) => {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Err(err) = refresh_external_host(&state, false).await {
                                warn!("periodic external host refresh failed: {}", err);
                            }
                        }
                        _ = watch.changed() => {
                            tokio::time::sleep(EXTERNAL_HOST_REFRESH_DEBOUNCE).await;
                            if let Err(err) = refresh_external_host(&state, false).await {
                                warn!("interface-triggered external host refresh failed: {}", err);
                            }
                        }
                    }
                }
                None => {
                    interval.tick().await;
                    if let Err(err) = refresh_external_host(&state, false).await {
                        warn!("periodic external host refresh failed: {}", err);
                    }
                }
            }
        }
    });
}
