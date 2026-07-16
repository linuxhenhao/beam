//! Token creation and persistence for the local zellij web server.

use std::path::Path;
use std::process::Command;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// ── token data type ──────────────────────────────────────────────────

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

// ── token creation helpers ────────────────────────────────────────────

/// Try to create a token with the given strategy.
pub(crate) enum TokenStrategy {
    /// Pass `--token-name NAME` (future zellij).
    Named { token_name: String, read_only: bool },
    /// Bare creation without `--token-name` (zellij 0.44.x).
    Bare { read_only: bool },
}

impl TokenStrategy {
    pub(crate) fn args(&self) -> Vec<String> {
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

/// Run a token creation command; returns (stdout, stderr, exit_code).
/// exit_code is `Some(0)` on clean exit, `Some(n)` for non-zero exit,
/// or `None` when the command could not be executed (e.g. zellij not found).
fn run_token_create(strategy: &TokenStrategy) -> (String, String, Option<i32>) {
    let output = Command::new("zellij").args(strategy.args()).output();
    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
            (stdout, stderr, out.status.code())
        }
        Err(_) => (String::new(), String::new(), None),
    }
}

/// Attempt to extract a token from zellij web output.
///
/// Handles multiple output formats:
/// - Bare hex token (>= 32 hex chars)
/// - `token_1: <uuid> (read-only)` — zellij 0.44.x format
/// - Any line >= 16 chars with no whitespace (fallback)
pub(crate) fn parse_token_from_output(stdout: &str, stderr: &str) -> Option<String> {
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
pub(crate) fn extract_uuid_from_line(line: &str) -> Option<String> {
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
pub(crate) fn is_name_conflict(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("already exists") || lower.contains("token name")
}

/// Does the error indicate `--token-name` is not accepted with create?
pub(crate) fn is_token_name_rejected(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    lower.contains("cannot be used") && lower.contains("token-name")
        || lower.contains("cannot be used") && lower.contains("create-token")
}

// ── sanitized bare-token error helpers ─────────────────────────────────

/// Diagnostic metadata for bare token creation failures.
/// Intentionally excludes raw stdout/stderr content to prevent
/// sensitive token values from leaking into logs or error messages.
pub(crate) struct BareTokenDiag {
    pub(crate) exit_status: Option<i32>,
    pub(crate) stdout_len: usize,
    pub(crate) stderr_len: usize,
}

/// Construct a sanitized diagnostic from raw command output.
/// Only metadata (lengths, exit status) is preserved; output content
/// is intentionally dropped so callers cannot accidentally log tokens.
pub(crate) fn diag_from_output(
    stdout: &str,
    stderr: &str,
    exit_status: Option<i32>,
) -> BareTokenDiag {
    BareTokenDiag {
        exit_status,
        stdout_len: stdout.len(),
        stderr_len: stderr.len(),
    }
}

/// Error: bare token creation succeeded (exit 0) but no token could be
/// parsed from output.
pub(crate) fn err_bare_parse_failure(diag: &BareTokenDiag) -> anyhow::Error {
    anyhow::anyhow!(
        "bare token creation succeeded (exit 0) but could not parse token from output (stdout_len={}, stderr_len={})",
        diag.stdout_len,
        diag.stderr_len,
    )
}

/// Error: bare token creation failed due to a name-conflict with an
/// existing token.
pub(crate) fn err_bare_name_conflict(diag: &BareTokenDiag) -> anyhow::Error {
    anyhow::anyhow!(
        "bare token creation failed: name conflict (exit_status={:?})",
        diag.exit_status,
    )
}

/// Error: bare token creation failed for an unknown / generic reason.
pub(crate) fn err_bare_generic_failure(diag: &BareTokenDiag) -> anyhow::Error {
    anyhow::anyhow!(
        "bare token creation failed (exit_status={:?}, stdout_len={}, stderr_len={})",
        diag.exit_status,
        diag.stdout_len,
        diag.stderr_len,
    )
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
    let (stdout, stderr, exit_code) = run_token_create(&strategy);
    if exit_code == Some(0) {
        if let Some(tok) = parse_token_from_output(&stdout, &stderr) {
            return Ok(tok);
        }
        warn!(
            strategy = "named",
            read_only = is_read_only,
            exit_status = 0,
            stdout_len = stdout.len(),
            stderr_len = stderr.len(),
            "token created but could not parse output",
        );
        // Fall through to bare strategy — the token was created but we can't read it
    } else if is_token_name_rejected(&stderr) {
        info!("--token-name rejected by zellij, falling back to bare creation");
    } else {
        // Some other failure — try bare strategy anyway
        warn!(
            strategy = "named",
            read_only = is_read_only,
            exit_status = ?exit_code,
            stdout_len = stdout.len(),
            stderr_len = stderr.len(),
            "named token creation failed",
        );
    }

    // Strategy 2: bare creation without --token-name (zellij 0.44.x)
    let strategy = TokenStrategy::Bare {
        read_only: is_read_only,
    };
    let (stdout, stderr, exit_code) = run_token_create(&strategy);
    if exit_code == Some(0) {
        if let Some(tok) = parse_token_from_output(&stdout, &stderr) {
            return Ok(tok);
        }
        let diag = diag_from_output(&stdout, &stderr, exit_code);
        return Err(err_bare_parse_failure(&diag));
    }

    let diag = diag_from_output(&stdout, &stderr, exit_code);
    if is_name_conflict(&stderr) {
        return Err(err_bare_name_conflict(&diag));
    }

    return Err(err_bare_generic_failure(&diag));
}
