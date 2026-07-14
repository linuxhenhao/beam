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

#[allow(unused_imports)]
pub use lifecycle::{ensure_zellij_web, zellij_web_is_running, zellij_web_start};
#[allow(unused_imports)]
pub use tokens::{
    ensure_zellij_web_tokens, load_zellij_web_tokens, save_zellij_web_tokens, ZellijWebTokens,
};
pub use watchdog::spawn_zellij_web_watchdog;

#[cfg(test)]
mod tests;
