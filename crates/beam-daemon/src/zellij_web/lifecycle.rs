//! Server lifecycle and health-check logic for the local zellij web server.
//!
//! The configured port is the only readiness boundary.  Watchdog, startup,
//! and restart all rely on `zellij_web_health(port)` which probes only the
//! configured port — no other port listener is used to determine success.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::{info, warn};

// ── health types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ZellijWebHealth {
    Current,
    StaleVersion {
        cli_version: String,
        web_version: String,
    },
    Offline,
}

// ── public health check ──────────────────────────────────────────────

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

pub(crate) fn zellij_web_health(port: u16) -> ZellijWebHealth {
    let status_online = zellij_web_status_online(port);
    let cli_version = current_zellij_version();
    let web_version = http_probe_version(port);

    classify_zellij_web_health(status_online, cli_version, web_version, || {
        http_probe_root(port)
    })
}

// ── status helpers ───────────────────────────────────────────────────

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

pub(crate) fn classify_zellij_web_health(
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

// ── version / http probes ────────────────────────────────────────────

fn current_zellij_version() -> Option<String> {
    let output = Command::new("zellij").arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    parse_zellij_cli_version(&String::from_utf8_lossy(&output.stdout))
}

pub(crate) fn parse_zellij_cli_version(output: &str) -> Option<String> {
    output
        .split_whitespace()
        .find(|part| looks_like_semver(part))
        .map(ToOwned::to_owned)
}

fn http_probe_version(port: u16) -> Option<String> {
    http_probe_path(port, "/info/version").and_then(parse_zellij_web_version_response)
}

pub(crate) fn parse_zellij_web_version_response(response: Vec<u8>) -> Option<String> {
    let response = String::from_utf8_lossy(&response);
    let is_ok = response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200");
    if !is_ok {
        return None;
    }
    let body = response.split_once("\r\n\r\n")?.1.trim();
    looks_like_semver(body).then(|| body.to_owned())
}

fn http_probe_root(port: u16) -> bool {
    http_probe_path(port, "/")
        .map(|response| parse_zellij_web_http_response("/", &response))
        .unwrap_or(false)
}

pub(crate) fn parse_zellij_web_http_response(path: &str, response: &[u8]) -> bool {
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
pub(crate) fn parse_zellij_web_status_output(stdout: &str, stderr: &str) -> bool {
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

// ── start / stop / restart ───────────────────────────────────────────

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

// ── Linux listener cleanup ───────────────────────────────────────────

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
pub(crate) fn parse_linux_tcp_listen_inodes(contents: &str, port: u16) -> Vec<String> {
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

// ── ensure ───────────────────────────────────────────────────────────

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
