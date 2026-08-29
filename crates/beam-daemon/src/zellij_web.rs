//! Manage the local zellij web server (status, start, token creation)
//! and persist tokens in BeamPaths state directory.
//!
//! ## Token creation strategy
//!
//! zellij 0.44.x does NOT support `--token-name` with `--create-*-token`;
//! it only accepts bare `--create-read-only-token` / `--create-token` and
//! auto-assigns the name `token_1`.  Creating a second token with the default
//! name fails because the name is already taken.
//!
//! Our approach:
//! 1. First try with `--token-name` (forward-compat with future zellij).
//! 2. Fall back to bare creation without `--token-name`.
//! 3. Create the **write** token first (more useful).  If it succeeds, create
//!    a read-only token.  If the read-only creation fails (name conflict),
//!    accept partial tokens (write-only).
//! 4. If the write token fails but read-only succeeds, accept read-only.
//! 5. The daemon starts regardless; missing tokens are surfaced as "terminal
//!    not ready" on the corresponding button.

mod lifecycle;
mod tokens;
mod watchdog;

use anyhow::Context;
#[allow(unused_imports)]
pub use lifecycle::{ensure_zellij_web, zellij_web_is_running, zellij_web_start};
#[allow(unused_imports)]
pub use tokens::{
    ZellijWebTokens, ensure_zellij_web_tokens, load_zellij_web_tokens, save_zellij_web_tokens,
};
pub use watchdog::spawn_zellij_web_watchdog;

/// Start the local zellij web server + tokens when `web.zellij_web` is
/// enabled; otherwise return empty tokens so the terminal proxy still runs
/// without an upstream. `tokens_path` points at the daemon's state directory.
pub fn start_zellij_web_if_enabled(
    enabled: bool,
    port: u16,
    tokens_path: &std::path::Path,
) -> anyhow::Result<ZellijWebTokens> {
    if !enabled {
        return Ok(ZellijWebTokens::disabled(port));
    }
    ensure_zellij_web(port)
        .with_context(|| format!("failed to start zellij web server on port {port}"))?;
    let tokens = ensure_zellij_web_tokens(tokens_path, port)
        .with_context(|| "failed to create zellij web tokens")?;
    spawn_zellij_web_watchdog(port);
    Ok(tokens)
}

#[cfg(test)]
mod tests;
