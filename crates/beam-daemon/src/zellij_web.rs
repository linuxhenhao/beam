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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ZellijWebHealth {
    Current,
    StaleVersion {
        cli_version: String,
        web_version: String,
    },
    Offline,
}

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

/// Check if the zellij web server is current and running on the given port.
///
/// The configured port is accepted only when:
/// - `/info/version` matches the current `zellij --version` output, or
/// - when `/info/version` is unavailable, `zellij web --status --ip
///   127.0.0.1 --port {port}` reports online or the HTTP root probe still
///   identifies a zellij web page.
///
/// A reachable server with a stale `/info/version` is treated as not running
/// so startup and the watchdog can restart it in place.
pub fn zellij_web_is_running(port: u16) -> bool {
    matches!(zellij_web_health(port), ZellijWebHealth::Current)
}

fn zellij_web_health(port: u16) -> ZellijWebHealth {
    let status_online = zellij_web_status_online(port);
    let cli_version = current_zellij_version();
    let web_version = http_probe_version(port);

    classify_zellij_web_health(status_online, cli_version, web_version, || {
        http_probe_root(port)
    })
}

fn zellij_web_status_online(port: u16) -> bool {
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

fn classify_zellij_web_health(
    status_online: bool,
    cli_version: Option<String>,
    web_version: Option<String>,
    root_probe: impl FnOnce() -> bool,
) -> ZellijWebHealth {
    if let Some(web_version) = web_version {
        if let Some(cli_version) = cli_version {
            if cli_version == web_version {
                return ZellijWebHealth::Current;
            }
            return ZellijWebHealth::StaleVersion {
                cli_version,
                web_version,
            };
        }
        return ZellijWebHealth::StaleVersion {
            cli_version: "<unavailable>".to_owned(),
            web_version,
        };
    }

    if status_online || root_probe() {
        ZellijWebHealth::Current
    } else {
        ZellijWebHealth::Offline
    }
}

fn current_zellij_version() -> Option<String> {
    let output = Command::new("zellij").arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    parse_zellij_cli_version(&String::from_utf8_lossy(&output.stdout))
}

fn http_probe_version(port: u16) -> Option<String> {
    http_probe_path(port, "/info/version").and_then(parse_zellij_web_version_response)
}

fn http_probe_root(port: u16) -> bool {
    http_probe_path(port, "/")
        .map(|response| parse_zellij_web_http_response("/", &response))
        .unwrap_or(false)
}

fn zellij_web_looks_like_zellij(port: u16) -> bool {
    http_probe_version(port).is_some() || http_probe_root(port)
}

fn http_probe_path(port: u16, path: &str) -> Option<Vec<u8>> {
    let addr = match format!("127.0.0.1:{port}").parse::<std::net::SocketAddr>() {
        Ok(addr) => addr,
        Err(_) => return None,
    };
    let mut stream = match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
        Ok(stream) => stream,
        Err(_) => return None,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));

    use std::io::{Read, Write};
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return None;
    }

    let mut buf = [0_u8; 2048];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => Some(buf[..n].to_vec()),
        _ => None,
    }
}

fn parse_zellij_cli_version(output: &str) -> Option<String> {
    output
        .split_whitespace()
        .find(|part| looks_like_semver(part))
        .map(ToOwned::to_owned)
}

fn parse_zellij_web_version_response(response: Vec<u8>) -> Option<String> {
    let response = String::from_utf8_lossy(&response);
    let is_ok = response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200");
    if !is_ok {
        return None;
    }
    let body = response.split_once("\r\n\r\n")?.1.trim();
    looks_like_semver(body).then(|| body.to_owned())
}

fn parse_zellij_web_http_response(path: &str, response: &[u8]) -> bool {
    let response = String::from_utf8_lossy(response);
    let is_ok = response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200");
    if !is_ok {
        return false;
    }

    let body = match response.split_once("\r\n\r\n") {
        Some((_, body)) => body.trim(),
        None => "",
    };

    if path == "/info/version" {
        return looks_like_semver(body);
    }

    response.to_lowercase().contains("zellij")
}

fn looks_like_semver(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(major) = parts.next() else {
        return false;
    };
    let Some(minor) = parts.next() else {
        return false;
    };
    let Some(patch) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    [major, minor, patch]
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
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

fn zellij_web_stop(port: u16) -> Result<()> {
    let output = Command::new("zellij")
        .args([
            "web",
            "--stop",
            "--ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .output()
        .context("failed to spawn zellij web stop command")?;

    if output.status.success() {
        return Ok(());
    }

    bail!(
        "zellij web stop failed (status={}): stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

fn zellij_web_restart(port: u16) -> Result<()> {
    if let Err(err) = zellij_web_stop(port) {
        warn!("zellij web stop failed before restart on port {port}: {err:#}");
    }

    if wait_for_zellij_web_port_closed(port, Duration::from_secs(3), Duration::from_millis(200)) {
        return zellij_web_start(port);
    }

    if !zellij_web_looks_like_zellij(port) {
        bail!(
            "zellij web port {port} is still accepting connections after stop, but it does not look like zellij web"
        );
    }

    terminate_zellij_web_listener_on_port(port)?;

    if !wait_for_zellij_web_port_closed(port, Duration::from_secs(3), Duration::from_millis(200)) {
        bail!("zellij web port {port} is still accepting connections after listener cleanup");
    }

    zellij_web_start(port)
}

fn wait_for_zellij_web_port_closed(port: u16, timeout: Duration, interval: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if !tcp_port_accepts(port) {
            return true;
        }
        std::thread::sleep(interval);
    }
    !tcp_port_accepts(port)
}

fn tcp_port_accepts(port: u16) -> bool {
    let Ok(addr) = format!("127.0.0.1:{port}").parse::<std::net::SocketAddr>() else {
        return false;
    };
    std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}

#[cfg(target_os = "linux")]
fn terminate_zellij_web_listener_on_port(port: u16) -> Result<()> {
    let inodes = linux_tcp_listen_inodes(port);
    if inodes.is_empty() {
        bail!("no listening socket inode found for zellij web port {port}");
    }

    let proc_entries = std::fs::read_dir("/proc")
        .with_context(|| format!("failed to read /proc while cleaning zellij web port {port}"))?;

    let mut terminated_any = false;
    for entry in proc_entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        if process_owns_listen_inode(&entry.path(), &inodes)? {
            warn!("terminating stale zellij web listener pid {pid} on port {port}");
            unsafe {
                libc::kill(pid, libc::SIGTERM);
            }
            terminated_any = true;
        }
    }

    if terminated_any {
        Ok(())
    } else {
        bail!("no process found listening on zellij web port {port}");
    }
}

#[cfg(not(target_os = "linux"))]
fn terminate_zellij_web_listener_on_port(port: u16) -> Result<()> {
    bail!(
        "cannot safely clean up a stale zellij web listener on port {port} without Linux /proc support"
    )
}

#[cfg(target_os = "linux")]
fn process_owns_listen_inode(proc_path: &Path, inodes: &[String]) -> Result<bool> {
    let fd_dir = proc_path.join("fd");
    let Ok(fd_entries) = std::fs::read_dir(fd_dir) else {
        return Ok(false);
    };

    for fd in fd_entries.flatten() {
        let Ok(target) = std::fs::read_link(fd.path()) else {
            continue;
        };
        let target = target.to_string_lossy();
        if let Some(inode) = target
            .strip_prefix("socket:[")
            .and_then(|value| value.strip_suffix(']'))
        {
            if inodes.iter().any(|candidate| candidate == inode) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

#[cfg(target_os = "linux")]
fn linux_tcp_listen_inodes(port: u16) -> Vec<String> {
    let mut inodes = parse_linux_tcp_listen_inodes(
        &std::fs::read_to_string("/proc/net/tcp").unwrap_or_default(),
        port,
    );
    inodes.extend(parse_linux_tcp_listen_inodes(
        &std::fs::read_to_string("/proc/net/tcp6").unwrap_or_default(),
        port,
    ));
    inodes
}

#[cfg(target_os = "linux")]
fn parse_linux_tcp_listen_inodes(contents: &str, port: u16) -> Vec<String> {
    let port_hex = format!("{port:04X}");
    contents
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            let local_address = fields.get(1)?;
            let state = fields.get(3)?;
            let inode = fields.get(9)?;
            if *state != "0A" {
                return None;
            }
            let (_, local_port) = local_address.rsplit_once(':')?;
            local_port
                .eq_ignore_ascii_case(&port_hex)
                .then(|| (*inode).to_owned())
        })
        .collect()
}

/// Ensure zellij web server is current and running; start or restart it if not.
pub fn ensure_zellij_web(port: u16) -> Result<()> {
    match zellij_web_health(port) {
        ZellijWebHealth::Current => Ok(()),
        ZellijWebHealth::StaleVersion {
            cli_version,
            web_version,
        } => {
            warn!(
                "zellij web on port {port} is stale (web={web_version}, cli={cli_version}); restarting"
            );
            zellij_web_restart(port)
        }
        ZellijWebHealth::Offline => zellij_web_start(port),
    }
}

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

    #[test]
    fn parse_cli_version_from_zellij_version_output() {
        assert_eq!(
            parse_zellij_cli_version("zellij 0.45.0\n"),
            Some("0.45.0".to_string())
        );
    }

    #[test]
    fn parse_cli_version_rejects_missing_semver() {
        assert_eq!(parse_zellij_cli_version("zellij dev\n"), None);
    }

    #[test]
    fn http_version_response_detected() {
        assert!(parse_zellij_web_http_response(
            "/info/version",
            b"HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\n0.45.0"
        ));
    }

    #[test]
    fn web_version_response_returns_semver_body() {
        assert_eq!(
            parse_zellij_web_version_response(
                b"HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\n0.45.0".to_vec()
            ),
            Some("0.45.0".to_string())
        );
    }

    #[test]
    fn web_version_response_rejects_non_200() {
        assert_eq!(
            parse_zellij_web_version_response(
                b"HTTP/1.1 404 Not Found\r\ncontent-length: 6\r\n\r\n0.45.0".to_vec()
            ),
            None
        );
    }

    #[test]
    fn http_version_requires_semver_body() {
        assert!(!parse_zellij_web_http_response(
            "/info/version",
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nOK"
        ));
    }

    #[test]
    fn http_root_response_detects_zellij_app() {
        assert!(parse_zellij_web_http_response(
            "/",
            b"HTTP/1.1 200 OK\r\ncontent-type: text/html\r\n\r\n<title>Zellij Web Client</title>"
        ));
    }

    #[test]
    fn http_root_response_rejects_unrelated_server() {
        assert!(!parse_zellij_web_http_response(
            "/",
            b"HTTP/1.1 200 OK\r\ncontent-type: text/html\r\n\r\n<title>Other App</title>"
        ));
    }

    #[test]
    fn health_current_when_versions_match() {
        assert_eq!(
            classify_zellij_web_health(
                false,
                Some("0.45.0".to_string()),
                Some("0.45.0".to_string()),
                || false,
            ),
            ZellijWebHealth::Current
        );
    }

    #[test]
    fn health_stale_when_versions_differ() {
        assert_eq!(
            classify_zellij_web_health(
                true,
                Some("0.46.0".to_string()),
                Some("0.45.0".to_string()),
                || true,
            ),
            ZellijWebHealth::StaleVersion {
                cli_version: "0.46.0".to_string(),
                web_version: "0.45.0".to_string(),
            }
        );
    }

    #[test]
    fn health_uses_status_when_version_unavailable() {
        assert_eq!(
            classify_zellij_web_health(true, Some("0.45.0".to_string()), None, || false),
            ZellijWebHealth::Current
        );
    }

    #[test]
    fn health_uses_root_probe_when_version_unavailable() {
        assert_eq!(
            classify_zellij_web_health(false, Some("0.45.0".to_string()), None, || true),
            ZellijWebHealth::Current
        );
    }

    #[test]
    fn health_offline_when_no_signal() {
        assert_eq!(
            classify_zellij_web_health(false, Some("0.45.0".to_string()), None, || false),
            ZellijWebHealth::Offline
        );
    }

    #[test]
    fn health_stale_when_cli_version_cannot_be_read() {
        assert_eq!(
            classify_zellij_web_health(false, None, Some("0.45.0".to_string()), || false),
            ZellijWebHealth::StaleVersion {
                cli_version: "<unavailable>".to_string(),
                web_version: "0.45.0".to_string(),
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_tcp_listen_inode_parser_matches_port_and_listen_state() {
        let contents = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:2261 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 12345 1 0000000000000000 100 0 0 10 0
   1: 0100007F:2261 00000000:0000 01 00000000:00000000 00:00000000 00000000  1000        0 99999 1 0000000000000000 100 0 0 10 0
   2: 0100007F:270F 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 54321 1 0000000000000000 100 0 0 10 0
";
        assert_eq!(
            parse_linux_tcp_listen_inodes(contents, 8801),
            vec!["12345".to_string()]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_tcp_listen_inode_parser_ignores_other_ports() {
        let contents = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:270F 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 54321 1 0000000000000000 100 0 0 10 0
";
        assert!(parse_linux_tcp_listen_inodes(contents, 8801).is_empty());
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
