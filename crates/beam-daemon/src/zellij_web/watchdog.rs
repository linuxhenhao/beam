//! Background watchdog that restarts the zellij web server if it goes
//! offline or becomes stale.  The watchdog only checks the configured port.

use std::time::Duration;

use tracing::{info, warn};

use super::lifecycle::{ZellijWebHealth, ensure_zellij_web, zellij_web_health};

const ZELLIJ_WEB_WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);

/// Spawn a background watchdog that restarts zellij web if it goes offline or stale.
pub fn spawn_zellij_web_watchdog(port: u16) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ZELLIJ_WEB_WATCHDOG_INTERVAL);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match zellij_web_health(port) {
                ZellijWebHealth::Current => continue,
                ZellijWebHealth::StaleVersion {
                    cli_version,
                    web_version,
                } => warn!(
                    "zellij web watchdog: port {port} stale (web={web_version}, cli={cli_version}), attempting restart"
                ),
                ZellijWebHealth::Offline => {
                    warn!("zellij web watchdog: port {port} offline, attempting restart")
                }
            }
            match ensure_zellij_web(port) {
                Ok(()) => info!("zellij web watchdog: port {port} restart success"),
                Err(err) => warn!("zellij web watchdog: port {port} restart failed: {err:#}"),
            }
        }
    });
}
