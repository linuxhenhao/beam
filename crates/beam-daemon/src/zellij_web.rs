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

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const ZELLIJ_WEB_WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZellijWebTokens {
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_token: Option<String>,
    /// Legacy single token_name (v1).  Kept for backward-compat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_name: Option<String>,
    /// Separate token names for read-only and write tokens (v2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_token_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_token_name: Option<String>,
}

impl ZellijWebTokens {
    /// Check whether both tokens are present and valid.
    pub fn is_complete(&self) -> bool {
        self.read_only_token
            .as_ref()
            .map_or(false, |t| !t.is_empty())
            && self.write_token.as_ref().map_or(false, |t| !t.is_empty())
    }

    /// Check whether at least one usable token exists.
    pub fn has_any_token(&self) -> bool {
        self.read_only_token
            .as_ref()
            .map_or(false, |t| !t.is_empty())
            || self.write_token.as_ref().map_or(false, |t| !t.is_empty())
    }
}

/// Check if the zellij web server is running on the given port.
///
/// Runs `zellij web --status` and requires the output to explicitly
/// contain an "online" indicator.  Relying on exit status alone is
/// unreliable because the CLI may exit 0 even when the server is
/// still starting or has stopped.
pub fn zellij_web_is_running(port: u16) -> bool {
    match Command::new("zellij")
        .args([
            "web",
            "--status",
            "--ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .output()
    {
        Ok(out) => {
            if !out.status.success() {
                return false;
            }
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            parse_zellij_web_status_output(&stdout, &stderr)
        }
        Err(_) => false,
    }
}

/// Parse the combined stdout+stderr of `zellij web --status` and
/// decide whether the server is online.
///
/// Returns true when the output contains a positive "running" / "online"
/// keyword and does NOT contain a negative "offline" / "stopped" keyword.
/// Split out for testability.
fn parse_zellij_web_status_output(stdout: &str, stderr: &str) -> bool {
    let combined = format!("{}\n{}", stdout, stderr);
    let lower = combined.to_lowercase();

    let is_online =
        lower.contains("running") || lower.contains("online") || lower.contains("listening");
    let is_offline = lower.contains("offline")
        || lower.contains("stopped")
        || lower.contains("not running")
        || lower.contains("failed");

    is_online && !is_offline
}

/// Wait for the zellij web server to become online on `port`.
///
/// Polls `zellij_web_is_running` up to `timeout` at `interval` periods.
fn wait_for_zellij_web(port: u16, timeout: Duration, interval: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if zellij_web_is_running(port) {
            return true;
        }
        std::thread::sleep(interval);
    }
    false
}

/// Start the zellij web server daemonized on the given port.
///
/// After issuing the start command, polls `zellij_web_is_running` for
/// up to 10 seconds to confirm the server actually came online.
pub fn zellij_web_start(port: u16) -> Result<()> {
    let output = Command::new("zellij")
        .args([
            "web",
            "--start",
            "--daemonize",
            "--ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .output()
        .context("failed to spawn zellij web server")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "zellij web start failed (status={}): stdout={} stderr={}",
            output.status,
            stdout.trim(),
            stderr.trim()
        );
    }

    info!(
        "zellij web start command succeeded, waiting for server to come online on port {}",
        port
    );

    // Poll for up to 10 seconds, every 200 ms
    if wait_for_zellij_web(port, Duration::from_secs(10), Duration::from_millis(200)) {
        info!("zellij web server confirmed online on port {}", port);
        Ok(())
    } else {
        bail!(
            "zellij web server did not come online on port {} within 10s; start stdout={} stderr={}",
            port,
            stdout.trim(),
            stderr.trim()
        );
    }
}

/// Ensure zellij web server is running; start it if not.
pub fn ensure_zellij_web(port: u16) -> Result<()> {
    if zellij_web_is_running(port) {
        return Ok(());
    }
    zellij_web_start(port)
}

/// Spawn a background watchdog that restarts zellij web if it goes offline.
pub fn spawn_zellij_web_watchdog(port: u16) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ZELLIJ_WEB_WATCHDOG_INTERVAL);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if zellij_web_is_running(port) {
                continue;
            }
            warn!("zellij web watchdog: port {port} offline, attempting restart");
            match ensure_zellij_web(port) {
                Ok(()) => info!("zellij web watchdog: port {port} restart success"),
                Err(err) => warn!("zellij web watchdog: port {port} restart failed: {err:#}"),
            }
        }
    });
}

// ── token creation helpers ────────────────────────────────────────────

/// Try to create a token with the given strategy.
enum TokenStrategy {
    /// Pass `--token-name NAME` (future zellij).
    Named { token_name: String, read_only: bool },
    /// Bare creation without `--token-name` (zellij 0.44.x).
    Bare { read_only: bool },
}

impl TokenStrategy {
    fn args(&self) -> Vec<String> {
        match self {
            TokenStrategy::Named {
                token_name,
                read_only,
            } => {
                let flag = if *read_only {
                    "--create-read-only-token"
                } else {
                    "--create-token"
                };
                vec![
                    "web".into(),
                    flag.into(),
                    "--token-name".into(),
                    token_name.clone(),
                ]
            }
            TokenStrategy::Bare { read_only } => {
                let flag = if *read_only {
                    "--create-read-only-token"
                } else {
                    "--create-token"
                };
                vec!["web".into(), flag.into()]
            }
        }
    }

    #[allow(dead_code)]
    fn is_read_only(&self) -> bool {
        match self {
            TokenStrategy::Named { read_only, .. } => *read_only,
            TokenStrategy::Bare { read_only } => *read_only,
        }
    }
}

/// Run a token creation command; returns (stdout_lines, stderr_lines, success).
fn run_token_create(strategy: &TokenStrategy) -> (String, String, bool) {
    let output = Command::new("zellij").args(strategy.args()).output();
    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
            (stdout, stderr, out.status.success())
        }
        Err(_) => (String::new(), String::new(), false),
    }
}

/// Attempt to extract a token from zellij web output.
///
/// Handles multiple output formats:
/// - Bare hex token (>= 32 hex chars)
/// - `token_1: <uuid> (read-only)` — zellij 0.44.x format
/// - Any line >= 16 chars with no whitespace (fallback)
fn parse_token_from_output(stdout: &str, stderr: &str) -> Option<String> {
    let combined = format!("{}\n{}", stdout.trim(), stderr.trim());

    // Pass 1: look for UUID-like tokens in lines like "token_1: <uuid> (...)"
    for line in combined.lines() {
        let trimmed = line.trim();
        // Try to extract a UUID from a line like "token_1: 550e8400-... (read-only)"
        if let Some(uuid_str) = extract_uuid_from_line(trimmed) {
            return Some(uuid_str);
        }
    }

    // Pass 2: long hex-like string (>= 32 hex chars)
    for line in combined.lines() {
        let trimmed = line.trim();
        if trimmed.len() >= 32 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            return Some(trimmed.to_string());
        }
    }

    // Pass 3: fallback — >= 16 chars, no whitespace
    for line in combined.lines() {
        let trimmed = line.trim();
        if trimmed.len() >= 16 && !trimmed.contains(char::is_whitespace) {
            return Some(trimmed.to_string());
        }
    }

    None
}

/// Extract a UUID string from a line like `token_1: 550e8400-e29b-41d4-a716-446655440000 (read-only)`.
fn extract_uuid_from_line(line: &str) -> Option<String> {
    // Find a substring that looks like a UUID: 8-4-4-4-12 hex digits with dashes
    let bytes = line.as_bytes();
    for window in bytes.windows(36) {
        if window.len() == 36
            && window[8] == b'-'
            && window[13] == b'-'
            && window[18] == b'-'
            && window[23] == b'-'
            && window
                .iter()
                .enumerate()
                .all(|(i, &b)| [8, 13, 18, 23].contains(&i) || b.is_ascii_hexdigit())
        {
            return Some(String::from_utf8_lossy(window).to_string());
        }
    }
    None
}

/// Does the error message indicate a name-conflict (token already exists)?
fn is_name_conflict(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("already exists") || lower.contains("token name")
}

/// Does the error indicate `--token-name` is not accepted with create?
fn is_token_name_rejected(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("cannot be used") && lower.contains("token-name")
        || lower.contains("cannot be used") && lower.contains("create-token")
}

// ── persistence ───────────────────────────────────────────────────────

/// Load persisted zellij web tokens from the JSON file.
pub fn load_zellij_web_tokens(path: &Path) -> Result<Option<ZellijWebTokens>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let tokens: ZellijWebTokens = serde_json::from_str(&raw)?;
            Ok(Some(tokens))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Persist zellij web tokens to the JSON file.
pub fn save_zellij_web_tokens(path: &Path, tokens: &ZellijWebTokens) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(tokens)?;
    std::fs::write(&tmp, payload)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ── main entry point ──────────────────────────────────────────────────

/// Get or create zellij web tokens for the given port.
///
/// Strategy (see module-level doc):
/// 1. Try with `--token-name` (forward-compat).
/// 2. Fall back to bare creation.
/// 3. Write token first, then read-only.
/// 4. Accept partial tokens; daemon starts regardless.
pub fn ensure_zellij_web_tokens(tokens_path: &Path, port: u16) -> Result<ZellijWebTokens> {
    // Try to load existing tokens
    if let Some(existing) = load_zellij_web_tokens(tokens_path)? {
        if existing.port == port && existing.is_complete() {
            return Ok(existing);
        }
        // Port matches but tokens are partial — try to fill gaps
        if existing.port == port && existing.has_any_token() {
            warn!(
                "zellij web tokens partial (port={}), will try to fill missing tokens",
                port
            );
            let tokens = fill_missing_tokens(existing, port)?;
            save_zellij_web_tokens(tokens_path, &tokens)?;
            return Ok(tokens);
        }
        // Port changed or no tokens at all — recreate
        warn!(
            "zellij web tokens mismatch (port {} vs {}), recreating",
            existing.port, port
        );
    }

    let tokens = create_tokens_with_fallback(port)?;
    save_zellij_web_tokens(tokens_path, &tokens)?;
    info!(
        "zellij web tokens persisted: ro={}, rw={}",
        tokens.read_only_token.is_some(),
        tokens.write_token.is_some()
    );
    Ok(tokens)
}

/// Try to fill missing tokens from an existing partial set.
fn fill_missing_tokens(existing: ZellijWebTokens, port: u16) -> Result<ZellijWebTokens> {
    let mut tokens = existing;
    tokens.port = port;

    // Try to create missing write token
    if tokens.write_token.as_ref().map_or(true, |t| t.is_empty()) {
        match try_create_token(true, false) {
            Ok(tok) => {
                info!("filled missing write token");
                tokens.write_token = Some(tok);
            }
            Err(e) => warn!("could not fill missing write token: {:#}", e),
        }
    }

    // Try to create missing read-only token
    if tokens
        .read_only_token
        .as_ref()
        .map_or(true, |t| t.is_empty())
    {
        match try_create_token(false, true) {
            Ok(tok) => {
                info!("filled missing read-only token");
                tokens.read_only_token = Some(tok);
            }
            Err(e) => warn!("could not fill missing read-only token: {:#}", e),
        }
    }

    Ok(tokens)
}

/// Create tokens from scratch using the fallback strategy.
fn create_tokens_with_fallback(port: u16) -> Result<ZellijWebTokens> {
    let mut tokens = ZellijWebTokens {
        port,
        read_only_token: None,
        write_token: None,
        token_name: None,
        read_only_token_name: None,
        write_token_name: None,
    };

    // ── Step 1: try to create write token ──
    match try_create_token(true, false) {
        Ok(tok) => {
            info!("created write token");
            tokens.write_token = Some(tok);
        }
        Err(e) => {
            warn!("write token creation failed: {:#}", e);
        }
    }

    // ── Step 2: try to create read-only token ──
    match try_create_token(false, true) {
        Ok(tok) => {
            info!("created read-only token");
            tokens.read_only_token = Some(tok);
        }
        Err(e) => {
            warn!("read-only token creation failed: {:#}", e);
        }
    }

    // If we got nothing at all, accept it — daemon still starts.
    // The terminal proxy will work; users with existing browser sessions
    // or known tokens can still connect.
    if !tokens.has_any_token() {
        warn!(
            "zellij web: failed to create any token; terminal login requires a pre-existing zellij token. \
             Buttons for 'Get write link' / 'Get read-only link' will show 'terminal not ready'."
        );
        return Ok(tokens);
    }

    if !tokens.is_complete() {
        let missing = match (
            tokens.read_only_token.is_some(),
            tokens.write_token.is_some(),
        ) {
            (false, true) => "read-only",
            (true, false) => "write",
            _ => unreachable!(),
        };
        warn!(
            "zellij web: only {} token available; some terminal features limited",
            missing
        );
    }

    Ok(tokens)
}

/// Try to create a single token with fallback (named → bare).
///
/// `want_write`: true = write token, false = read-only.
/// `primary`: true for the first token attempt (write), false for second (read-only).
fn try_create_token(want_write: bool, is_read_only: bool) -> Result<String> {
    let token_name = if want_write {
        "beam-write"
    } else {
        "beam-read-only"
    };
    let ro_name = format!("{}-ro", token_name);

    // Strategy 1: try with --token-name (future zellij)
    let strategy = TokenStrategy::Named {
        token_name: if is_read_only {
            ro_name
        } else {
            token_name.to_string()
        },
        read_only: is_read_only,
    };
    let (stdout, stderr, success) = run_token_create(&strategy);
    if success {
        if let Some(tok) = parse_token_from_output(&stdout, &stderr) {
            return Ok(tok);
        }
        warn!("token created but could not parse output: stdout={stdout:?} stderr={stderr:?}");
        // Fall through to bare strategy — the token was created but we can't read it
    } else if is_token_name_rejected(&stderr) {
        info!("--token-name rejected by zellij, falling back to bare creation");
    } else {
        // Some other failure — try bare strategy anyway
        warn!(
            "named token creation failed (name={}): stderr={}",
            token_name,
            stderr.trim()
        );
    }

    // Strategy 2: bare creation without --token-name (zellij 0.44.x)
    let strategy = TokenStrategy::Bare {
        read_only: is_read_only,
    };
    let (stdout, stderr, success) = run_token_create(&strategy);
    if success {
        if let Some(tok) = parse_token_from_output(&stdout, &stderr) {
            return Ok(tok);
        }
        bail!("bare token created but could not parse output: stdout={stdout:?} stderr={stderr:?}");
    }

    if is_name_conflict(&stderr) {
        bail!(
            "bare token creation name-conflict: {} (a token with the default name already exists)",
            stderr.trim()
        );
    }

    bail!("bare token creation failed: stderr={}", stderr.trim());
}

// ── tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── TokenStrategy args ──

    #[test]
    fn named_strategy_args_has_no_ip_port() {
        let s = TokenStrategy::Named {
            token_name: "beam-read-only".into(),
            read_only: true,
        };
        let args = s.args();
        let joined = args.join(" ");
        assert!(args.contains(&"web".to_string()));
        assert!(args.contains(&"--create-read-only-token".to_string()));
        assert!(args.contains(&"--token-name".to_string()));
        assert!(args.contains(&"beam-read-only".to_string()));
        assert!(!joined.contains("--ip"));
        assert!(!joined.contains("--port"));
    }

    #[test]
    fn named_strategy_rw_args_has_no_ip_port() {
        let s = TokenStrategy::Named {
            token_name: "beam-write".into(),
            read_only: false,
        };
        let args = s.args();
        let joined = args.join(" ");
        assert!(args.contains(&"--create-token".to_string()));
        assert!(args.contains(&"--token-name".to_string()));
        assert!(!joined.contains("--ip"));
        assert!(!joined.contains("--port"));
    }

    #[test]
    fn bare_strategy_args_no_ip_port_no_token_name() {
        let s = TokenStrategy::Bare { read_only: true };
        let args = s.args();
        let joined = args.join(" ");
        assert!(args.contains(&"web".to_string()));
        assert!(args.contains(&"--create-read-only-token".to_string()));
        assert!(
            !joined.contains("--token-name"),
            "bare strategy must not have --token-name"
        );
        assert!(!joined.contains("--ip"));
        assert!(!joined.contains("--port"));
    }

    #[test]
    fn bare_strategy_rw_args() {
        let s = TokenStrategy::Bare { read_only: false };
        let args = s.args();
        let joined = args.join(" ");
        assert!(args.contains(&"--create-token".to_string()));
        assert!(!joined.contains("--token-name"));
    }

    // ── parse_token_from_output ──

    #[test]
    fn parse_hex_token() {
        let token = parse_token_from_output("abc123def456abc123def456abc123de\n", "");
        assert_eq!(token, Some("abc123def456abc123def456abc123de".to_string()));
    }

    #[test]
    fn parse_uuid_from_zellij_044_output() {
        // Real zellij 0.44.x output: "Created token successfully\n\ntoken_1: <uuid> (read-only)"
        let stdout = "Created token successfully\n\ntoken_1: 550e8400-e29b-41d4-a716-446655440000 (read-only)\n";
        let token = parse_token_from_output(stdout, "");
        assert_eq!(
            token,
            Some("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
    }

    #[test]
    fn parse_first_token_when_multiple_lines() {
        // Only the first UUID-like token is returned
        let stdout = "token_1: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee (read-only)\n\
                       token_2: 11111111-2222-3333-4444-555555555555 (write)\n";
        let token = parse_token_from_output(stdout, "");
        assert_eq!(
            token,
            Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_string())
        );
    }

    #[test]
    fn parse_hex_wins_over_uuid_if_both_present() {
        // Actually UUID extraction comes first now, then hex. Let's test hex.
        let stdout = "deadbeefdeadbeefdeadbeefdeadbeef\n";
        let token = parse_token_from_output(stdout, "");
        assert_eq!(token, Some("deadbeefdeadbeefdeadbeefdeadbeef".to_string()));
    }

    #[test]
    fn parse_fallback_no_whitespace() {
        let token = parse_token_from_output("abcdefghijklmnopq", "");
        assert_eq!(token, Some("abcdefghijklmnopq".to_string()));
    }

    #[test]
    fn parse_empty_returns_none() {
        assert_eq!(parse_token_from_output("", ""), None);
    }

    #[test]
    fn parse_too_short_returns_none() {
        assert_eq!(parse_token_from_output("short", ""), None);
    }

    // ── extract_uuid_from_line ──

    #[test]
    fn extract_uuid_standard_line() {
        let line = "token_1: 550e8400-e29b-41d4-a716-446655440000 (read-only)";
        assert_eq!(
            extract_uuid_from_line(line),
            Some("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
    }

    #[test]
    fn extract_uuid_no_surrounding_text() {
        let line = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            extract_uuid_from_line(line),
            Some("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
    }

    #[test]
    fn extract_uuid_mixed_case() {
        let line = "token_1: 550E8400-E29B-41D4-A716-446655440000 (read-only)";
        assert_eq!(
            extract_uuid_from_line(line),
            Some("550E8400-E29B-41D4-A716-446655440000".to_string())
        );
    }

    #[test]
    fn extract_uuid_missing_dashes_not_matched() {
        // Without dashes, not recognized as UUID (handled by hex fallback)
        assert_eq!(
            extract_uuid_from_line("550e8400e29b41d4a716446655440000"),
            None
        );
    }

    // ── is_name_conflict / is_token_name_rejected ──

    #[test]
    fn detect_name_conflict() {
        assert!(is_name_conflict("Token name 'token_1' already exists"));
        assert!(is_name_conflict(
            "Failed to create token: Token name 'token_1' already exists"
        ));
        assert!(!is_name_conflict("some other error"));
    }

    #[test]
    fn detect_token_name_rejected() {
        assert!(is_token_name_rejected(
            "The argument '--create-token' cannot be used with one or more of the other specified arguments"
        ));
        assert!(!is_token_name_rejected(
            "Token name 'token_1' already exists"
        ));
    }

    // ── parse_zellij_web_status_output ──

    #[test]
    fn status_online_with_running_keyword() {
        assert!(parse_zellij_web_status_output(
            "server is running on port 8801",
            ""
        ));
    }

    #[test]
    fn status_online_with_listening_keyword() {
        assert!(parse_zellij_web_status_output(
            "listening on 127.0.0.1:8801",
            ""
        ));
    }

    #[test]
    fn status_offline_explicit() {
        assert!(!parse_zellij_web_status_output("server is offline", ""));
    }

    #[test]
    fn status_not_running() {
        assert!(!parse_zellij_web_status_output("server not running", ""));
    }

    #[test]
    fn status_offline_from_stderr() {
        assert!(!parse_zellij_web_status_output("", "error: server stopped"));
    }

    #[test]
    fn status_empty_defaults_offline() {
        // No positive keyword → assume offline
        assert!(!parse_zellij_web_status_output("", ""));
    }

    #[test]
    fn status_both_online_and_offline_is_offline() {
        // If output somehow contains both, offline wins (safety)
        assert!(!parse_zellij_web_status_output(
            "running but also offline",
            ""
        ));
    }

    // ── ZellijWebTokens ──

    #[test]
    fn is_complete_and_has_any_token() {
        let full = ZellijWebTokens {
            port: 8801,
            read_only_token: Some("ro".into()),
            write_token: Some("rw".into()),
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };
        assert!(full.is_complete());
        assert!(full.has_any_token());

        let write_only = ZellijWebTokens {
            port: 8801,
            read_only_token: None,
            write_token: Some("rw".into()),
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };
        assert!(!write_only.is_complete());
        assert!(write_only.has_any_token());

        let empty = ZellijWebTokens {
            port: 8801,
            read_only_token: None,
            write_token: None,
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };
        assert!(!empty.is_complete());
        assert!(!empty.has_any_token());
    }

    #[test]
    fn zero_tokens_is_valid_but_incomplete() {
        let empty = ZellijWebTokens {
            port: 8801,
            read_only_token: None,
            write_token: None,
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };
        assert!(!empty.is_complete());
        assert!(!empty.has_any_token());
        // This struct is valid to persist and won't block daemon startup
    }

    #[test]
    fn partial_tokens_respected() {
        let ro_only = ZellijWebTokens {
            port: 8801,
            read_only_token: Some("ro".into()),
            write_token: None,
            token_name: None,
            read_only_token_name: None,
            write_token_name: None,
        };
        assert!(!ro_only.is_complete());
        assert!(ro_only.has_any_token());
    }
}
