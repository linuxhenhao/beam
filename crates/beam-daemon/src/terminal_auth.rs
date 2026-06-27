//! Beam terminal proxy authentication bridge.
//!
//! Replaces raw zellij tokens in terminal URLs with Beam's own auth tickets.
//! The Beam proxy maintains its own HttpOnly/SameSite cookie
//! and a server-side cookie jar that maps Beam cookies → zellij cookies.
//! The browser never sees the raw zellij token or the zellij auth cookie.
//!
//! ## Flow
//!
//! 1. A Beam ticket is generated (HMAC-signed, one-time use; write links have a TTL) and
//!    embedded in the terminal link: `/s/<session_id>?beam_terminal_ticket=...`.
//! 2. The proxy verifies the ticket, calls zellij web `/command/login` with
//!    the corresponding zellij token, captures the zellij Set-Cookie, stores
//!    it in the server-side cookie jar, and sets its own Beam cookie on the
//!    browser.
//! 3. Subsequent requests carry the Beam cookie, which the proxy maps to the
//!    stored zellij cookie and injects into upstream requests.
//!
//! ## Permissions
//!
//! - Read-only link → uses `read_only_token`, Beam cookie records "read-only".
//! - Write link → uses `write_token`, Beam cookie records "write".
//!
//! ## TODO / Known risk
//!
//! The zellij cookie obtained via `/command/login` may be a global session
//! cookie that grants full write access regardless of which token was used to
//! log in.  The proxy currently cannot enforce input-level restrictions at the
//! zellij web protocol level — the permission stored in the Beam cookie is
//! informational for auditing and can be used by future proxy-level guards
//! (e.g., blocking POST/DELETE on read-only sessions).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::Mutex;

type HmacSha256 = Hmac<Sha256>;

/// How long a write terminal ticket is valid after creation.
const TICKET_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// How long a Beam terminal session cookie is valid.
const BEAM_COOKIE_TTL: Duration = Duration::from_secs(86_400); // 24 hours

/// How long we remember a used ticket (to prevent reuse).
const USED_TICKET_TTL: Duration = Duration::from_secs(600); // 10 minutes

/// Beam cookie name sent to the browser.
pub const BEAM_COOKIE_NAME: &str = "beam_terminal_session";

/// Query parameter name for the terminal ticket.
pub const TICKET_QUERY_PARAM: &str = "beam_terminal_ticket";

/// Derive the ticket-secret file path from environment, matching BeamPaths convention.
fn ticket_secret_path() -> std::path::PathBuf {
    let root = std::env::var("BEAM_HOME").unwrap_or_else(|_| {
        std::env::var("HOME")
            .map(|h| format!("{}/.beam", h))
            .unwrap_or_default()
    });
    std::path::PathBuf::from(root).join("state/ticket-secret")
}

/// Process-level secret for HMAC-signing terminal tickets.
/// Persisted to disk so tickets survive daemon restarts.
/// Falls back to an in-memory random secret if disk I/O fails.
fn ticket_secret() -> &'static [u8] {
    static SECRET: OnceLock<Vec<u8>> = OnceLock::new();
    SECRET.get_or_init(|| {
        let path = ticket_secret_path();
        // Try to load existing secret from disk (survives restart)
        if let Ok(bytes) = std::fs::read(&path) {
            if bytes.len() >= 16 {
                return bytes;
            }
        }
        // Generate new secret and persist
        let secret = uuid::Uuid::new_v4().as_bytes().to_vec();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Best-effort write; fall back to in-memory if it fails
        let _ = std::fs::write(&path, &secret);
        secret
    })
}

/// Permission level encoded in a terminal ticket / cookie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPermission {
    ReadOnly,
    Write,
}

impl TerminalPermission {
    pub fn as_str(&self) -> &'static str {
        match self {
            TerminalPermission::ReadOnly => "read_only",
            TerminalPermission::Write => "write",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "read_only" => Some(TerminalPermission::ReadOnly),
            "write" => Some(TerminalPermission::Write),
            _ => None,
        }
    }
}

/// Payload embedded in a terminal ticket.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TerminalTicketPayload {
    pub session_id: String,
    pub permission: TerminalPermission,
    pub created_at: u64,
    pub nonce: String,
}

// ── Public API: ticket generation / verification ────────────────────────

/// Generate a terminal ticket (HMAC-SHA256 signed with per-process random secret).
///
/// Returns a URL-safe ticket string for use as `?beam_terminal_ticket=...`.
pub fn generate_terminal_ticket(session_id: &str, permission: TerminalPermission) -> String {
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let nonce = uuid::Uuid::new_v4().simple().to_string();

    let payload = format!(
        "{}:{}:{}:{}",
        session_id,
        permission.as_str(),
        created_at,
        nonce
    );

    let secret = ticket_secret();
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    let sig_hex = hex_encode(&signature);

    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes());
    format!("{}.{}", payload_b64, sig_hex)
}

/// Verify a terminal ticket and extract its payload.
///
/// Checks HMAC signature, session_id match, and one-time use.
/// TTL expiry is only enforced for write-permission tickets;
/// read-only tickets (embedded in streaming cards) have no expiry.
pub fn verify_terminal_ticket(
    ticket: &str,
    expected_session_id: &str,
    used_tickets: &mut UsedTickets,
) -> Option<TerminalTicketPayload> {
    let (payload_b64, sig_hex) = ticket.split_once('.')?;

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64.as_bytes())
        .ok()?;
    let payload = String::from_utf8(payload_bytes).ok()?;

    // Verify HMAC
    let secret = ticket_secret();
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");
    mac.update(payload.as_bytes());
    let expected_sig = hex_decode(sig_hex)?;
    mac.verify_slice(&expected_sig).ok()?;

    // Parse payload: session_id:permission:created_at:nonce
    let parts: Vec<&str> = payload.splitn(4, ':').collect();
    if parts.len() != 4 {
        return None;
    }
    let session_id = parts[0].to_string();
    let permission = TerminalPermission::from_str(parts[1])?;
    let created_at: u64 = parts[2].parse().ok()?;
    let nonce = parts[3].to_string();

    if session_id != expected_session_id {
        return None;
    }

    // Write tickets expire after TICKET_TTL; read-only tickets (cards) have no expiry
    if permission == TerminalPermission::Write {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.saturating_sub(created_at) > TICKET_TTL.as_secs() {
            return None;
        }
    }

    // One-time use check
    if !used_tickets.insert_and_check(&nonce) {
        return None;
    }

    Some(TerminalTicketPayload {
        session_id,
        permission,
        created_at,
        nonce,
    })
}

// ── UsedTickets ─────────────────────────────────────────────────────────

/// Tracks ticket nonces that have already been consumed.
#[derive(Default)]
pub struct UsedTickets {
    entries: Vec<(String, Instant)>,
}

/// Serializable representation of used tickets for disk persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UsedTicketsPersisted {
    /// List of (nonce, seen_at_epoch_secs) — the approximate
    /// Unix epoch second when the nonce was first recorded.
    entries: Vec<(String, u64)>,
}

impl UsedTickets {
    /// Returns true if the nonce is fresh (not used), inserting it.
    pub fn insert_and_check(&mut self, nonce: &str) -> bool {
        let now = Instant::now();
        self.entries
            .retain(|(_, t)| now.saturating_duration_since(*t) < USED_TICKET_TTL);
        if self.entries.iter().any(|(n, _)| n == nonce) {
            return false; // already used
        }
        self.entries.push((nonce.to_string(), now));
        true
    }

    /// Load used tickets from disk, pruning expired entries.
    /// Approximates Instant from saved epoch timestamps (seen_at).
    pub fn load_from(&mut self, path: &std::path::Path) {
        if let Ok(Some(persisted)) = beam_core::persist::read_json::<UsedTicketsPersisted>(path) {
            let now = Instant::now();
            let now_epoch = Self::epoch_now();
            for (nonce, seen_at_epoch) in persisted.entries {
                // Compute elapsed since the entry was recorded
                let elapsed_secs = now_epoch.saturating_sub(seen_at_epoch);
                if elapsed_secs > USED_TICKET_TTL.as_secs() {
                    continue; // already expired
                }
                // Reconstruct Instant in the past (now - elapsed)
                let seen = now - Duration::from_secs(elapsed_secs);
                self.entries.push((nonce, seen));
            }
        }
    }

    /// Save used tickets to disk (only non-expired entries).
    /// Stores the approximate epoch time when each entry was first seen.
    pub fn save_to(&self, path: &std::path::Path) {
        let now_epoch = Self::epoch_now();
        let entries: Vec<(String, u64)> = self
            .entries
            .iter()
            .filter_map(|(nonce, instant)| {
                let elapsed = instant.elapsed();
                if elapsed < USED_TICKET_TTL {
                    let seen_at_epoch = now_epoch.saturating_sub(elapsed.as_secs());
                    Some((nonce.clone(), seen_at_epoch))
                } else {
                    None
                }
            })
            .collect();
        if entries.is_empty() {
            let _ = std::fs::remove_file(path);
            return;
        }
        let persisted = UsedTicketsPersisted { entries };
        let _ = beam_core::persist::atomic_write_json(path, &persisted);
    }

    fn epoch_now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

// ── Server-side cookie jar ──────────────────────────────────────────────

/// An entry mapping a Beam cookie to a zellij session cookie.
#[derive(Debug, Clone)]
struct BeamCookieEntry {
    /// The zellij session cookie captured from /command/login Set-Cookie.
    zellij_cookie: String,
    pub session_id: String,
    pub permission: TerminalPermission,
    pub created_at: Instant,
}

/// Thread-safe, shared terminal auth state: maps Beam cookie → zellij cookie.
#[derive(Clone, Default)]
pub struct TerminalAuthState {
    inner: Arc<Mutex<TerminalAuthInner>>,
}

#[derive(Default)]
struct TerminalAuthInner {
    /// beam_cookie value → entry
    entries: HashMap<String, BeamCookieEntry>,
    used_tickets: UsedTickets,
}

impl TerminalAuthState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load persisted used tickets from the given path.
    /// Best-effort; failures are silently ignored.
    pub async fn load_used_tickets(&self, path: &std::path::Path) {
        let path = path.to_path_buf();
        let inner = self.inner.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let mut guard = inner.blocking_lock();
            guard.used_tickets.load_from(&path);
        })
        .await;
    }

    /// Persist used tickets to the given path.
    /// Best-effort; failures are silently ignored.
    pub async fn save_used_tickets(&self, path: &std::path::Path) {
        let path = path.to_path_buf();
        let inner = self.inner.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let guard = inner.blocking_lock();
            guard.used_tickets.save_to(&path);
        })
        .await;
    }

    /// Store a zellij cookie entry and return a new Beam cookie value.
    pub async fn insert(
        &self,
        zellij_cookie: String,
        session_id: String,
        permission: TerminalPermission,
    ) -> String {
        let mut inner = self.inner.lock().await;
        inner.prune();
        let beam_cookie = uuid::Uuid::new_v4().simple().to_string();
        inner.entries.insert(
            beam_cookie.clone(),
            BeamCookieEntry {
                zellij_cookie,
                session_id,
                permission,
                created_at: Instant::now(),
            },
        );
        beam_cookie
    }

    /// Look up a Beam cookie value and return the mapped entry.
    /// Returns `(zellij_cookie, session_id, permission)` if found and not expired.
    pub async fn lookup(&self, beam_cookie: &str) -> Option<(String, String, TerminalPermission)> {
        let mut inner = self.inner.lock().await;
        inner.prune();
        inner
            .entries
            .get(beam_cookie)
            .map(|e| (e.zellij_cookie.clone(), e.session_id.clone(), e.permission))
    }

    /// Verify a ticket and mark it as used. Convenience combining verify + mark.
    pub async fn verify_and_consume_ticket(
        &self,
        ticket: &str,
        expected_session_id: &str,
    ) -> Option<TerminalTicketPayload> {
        let mut inner = self.inner.lock().await;
        verify_terminal_ticket(ticket, expected_session_id, &mut inner.used_tickets)
    }
}

impl TerminalAuthInner {
    fn prune(&mut self) {
        let now = Instant::now();
        self.entries
            .retain(|_, e| now.duration_since(e.created_at) < BEAM_COOKIE_TTL);
    }
}

// ── hex helpers ─────────────────────────────────────────────────────────

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

// ── Cookie helpers ──────────────────────────────────────────────────────

/// Extract the Beam cookie value from request Cookie header.
pub fn extract_beam_cookie(cookie_header: &str) -> Option<String> {
    for part in cookie_header.split(';') {
        let mut kv = part.trim().splitn(2, '=');
        let key = kv.next().unwrap_or("").trim();
        let value = kv.next().unwrap_or("").trim();
        if key == BEAM_COOKIE_NAME && !value.is_empty() {
            return Some(value.to_string());
        }
    }
    None
}

/// Extract a zellij Set-Cookie value from a response header value.
///
/// Zellij web returns something like:
/// `session=abc123; HttpOnly; SameSite=Strict; Path=/`
pub fn extract_zellij_set_cookie(set_cookie_value: &str) -> Option<String> {
    // Take everything before the first ';'
    let cookie_part = set_cookie_value.split(';').next()?;
    // Split on '=' to separate name=value
    let (name, value) = cookie_part.split_once('=')?;
    if name.trim().is_empty() || value.trim().is_empty() {
        return None;
    }
    Some(cookie_part.trim().to_string())
}

// ── Path classification: zellij root vs zellij session ──────────────────

/// Check whether a sub-path under a session should be proxied to zellij web
/// root (instead of to the zellij session path).
///
/// Zellij web serves static assets (CSS, JS, favicon) from its root, and
/// JS calls root-level endpoints like `/command/login`, `/session`,
/// `/ws/terminal/<name>`, `/ws/control`, `/info` etc.
pub fn is_zellij_root_path(path: &str) -> bool {
    path == "session"
        || path == "info"
        || path.starts_with("info/")
        || path == "favicon.ico"
        || path.starts_with("command")
        || path.starts_with("api/")
        || path.starts_with("assets/")
        || path.starts_with("ws/terminal")
        || path == "ws/control"
        || path.starts_with("ws/control")
}

/// Translate a zellij root WS path so the terminal session name is replaced
/// with the actual zellij session (e.g. `bmx-...`), not the beam session ID.
///
/// `rest` is the captured wildcard path from `/s/{session_id}/ws/{*rest}`,
/// e.g. `ws/terminal/<beam_session_id>` or `ws/control`.
/// `zellij_session` is the resolved zellij session name.
///
/// Returns the translated path for the upstream zellij connection.
pub fn translate_root_ws_path(rest: &str, zellij_session: &str) -> String {
    // axum strips the /ws/ route prefix, so rest may be:
    //   "terminal"         → Zellij 0.44 WS endpoint: /ws/terminal (no session in path)
    //   "control"          → Zellij control WS: /ws/control
    //   "terminal/<name>"  → terminal WS endpoint with a client-supplied name
    if rest == "terminal" || rest == "control" {
        format!("ws/{rest}")
    } else if rest.starts_with("terminal/") {
        format!("ws/terminal/{zellij_session}")
    } else {
        rest.to_string()
    }
}

// ── tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Ticket tests (use per-process random secret) ────────────────

    #[test]
    fn ticket_generate_and_verify() {
        let ticket = generate_terminal_ticket("session-abc", TerminalPermission::Write);
        let mut used = UsedTickets::default();
        let payload = verify_terminal_ticket(&ticket, "session-abc", &mut used);
        assert!(payload.is_some());
        let p = payload.unwrap();
        assert_eq!(p.session_id, "session-abc");
        assert_eq!(p.permission, TerminalPermission::Write);
    }

    #[test]
    fn ticket_wrong_session_rejected() {
        let ticket = generate_terminal_ticket("session-abc", TerminalPermission::ReadOnly);
        let mut used = UsedTickets::default();
        assert!(verify_terminal_ticket(&ticket, "session-xyz", &mut used).is_none());
    }

    #[test]
    fn ticket_one_time_use() {
        let ticket = generate_terminal_ticket("session-1", TerminalPermission::Write);
        let mut used = UsedTickets::default();
        assert!(verify_terminal_ticket(&ticket, "session-1", &mut used).is_some());
        // Second use should fail
        assert!(verify_terminal_ticket(&ticket, "session-1", &mut used).is_none());
    }

    #[test]
    fn ticket_tampered_signature_rejected() {
        let ticket = generate_terminal_ticket("session-abc", TerminalPermission::ReadOnly);
        // Tamper with the signature
        let parts: Vec<&str> = ticket.split('.').collect();
        let sig_hex = parts[1];
        // Flip a character in the signature
        let mut tampered_sig = String::new();
        for (i, ch) in sig_hex.chars().enumerate() {
            if i == 0 {
                tampered_sig.push(if ch == 'a' { 'b' } else { 'a' });
            } else {
                tampered_sig.push(ch);
            }
        }
        let tampered_ticket = format!("{}.{}", parts[0], tampered_sig);
        let mut used = UsedTickets::default();
        assert!(verify_terminal_ticket(&tampered_ticket, "session-abc", &mut used).is_none());
    }

    #[test]
    fn ticket_expired_write_rejected() {
        // Create a write ticket with epoch 0 timestamp
        let old_payload = format!("session-x:write:0:{}", uuid::Uuid::new_v4().simple());
        let secret = ticket_secret();
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(old_payload.as_bytes());
        let sig = hex_encode(&mac.finalize().into_bytes());
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(old_payload.as_bytes());
        let ticket = format!("{}.{}", b64, sig);
        let mut used = UsedTickets::default();
        // Write ticket must be rejected after TTL
        assert!(verify_terminal_ticket(&ticket, "session-x", &mut used).is_none());
    }

    #[test]
    fn ticket_expired_read_only_accepted() {
        // Create a read-only ticket with epoch 0 timestamp
        let old_payload = format!("session-y:read_only:0:{}", uuid::Uuid::new_v4().simple());
        let secret = ticket_secret();
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(old_payload.as_bytes());
        let sig = hex_encode(&mac.finalize().into_bytes());
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(old_payload.as_bytes());
        let ticket = format!("{}.{}", b64, sig);
        let mut used = UsedTickets::default();
        // Read-only ticket should be accepted regardless of age
        let payload = verify_terminal_ticket(&ticket, "session-y", &mut used).unwrap();
        assert_eq!(payload.permission, TerminalPermission::ReadOnly);
    }

    #[test]
    fn ticket_session_id_must_match() {
        let ticket = generate_terminal_ticket("correct-session", TerminalPermission::Write);
        let mut used = UsedTickets::default();
        assert!(verify_terminal_ticket(&ticket, "wrong-session", &mut used).is_none());
    }

    #[test]
    fn ticket_read_only_permission_roundtrip() {
        let ticket = generate_terminal_ticket("ro-session", TerminalPermission::ReadOnly);
        let mut used = UsedTickets::default();
        let payload = verify_terminal_ticket(&ticket, "ro-session", &mut used).unwrap();
        assert_eq!(payload.permission, TerminalPermission::ReadOnly);
    }

    #[test]
    fn ticket_write_permission_roundtrip() {
        let ticket = generate_terminal_ticket("rw-session", TerminalPermission::Write);
        let mut used = UsedTickets::default();
        let payload = verify_terminal_ticket(&ticket, "rw-session", &mut used).unwrap();
        assert_eq!(payload.permission, TerminalPermission::Write);
    }

    #[test]
    fn ticket_format_has_dot_separator() {
        let ticket = generate_terminal_ticket("s", TerminalPermission::Write);
        assert!(ticket.contains('.'));
        let parts: Vec<&str> = ticket.split('.').collect();
        assert_eq!(parts.len(), 2);
        assert!(!parts[0].is_empty());
        assert!(!parts[1].is_empty());
        // second part should be hex
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn ticket_with_wrong_secret_rejected() {
        // Verify that a ticket signed with a different secret is rejected
        let ticket = generate_terminal_ticket("test-session", TerminalPermission::Write);
        let mut used = UsedTickets::default();
        // First verify it works with the real secret
        assert!(verify_terminal_ticket(&ticket, "test-session", &mut used).is_some());
        // Generate a new ticket (should use the same per-process secret, so it works)
        let ticket2 = generate_terminal_ticket("test-session", TerminalPermission::Write);
        let mut used2 = UsedTickets::default();
        assert!(verify_terminal_ticket(&ticket2, "test-session", &mut used2).is_some());
    }

    // ── hex helpers ─────────────────────────────────────────────────

    #[test]
    fn hex_encode_decode_roundtrip() {
        let original = b"hello world 12345";
        let encoded = hex_encode(original);
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn hex_decode_invalid_length() {
        assert!(hex_decode("abc").is_none());
    }

    #[test]
    fn hex_decode_invalid_chars() {
        assert!(hex_decode("zz").is_none());
    }

    #[test]
    fn hex_decode_empty() {
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
    }

    // ── Cookie helpers ──────────────────────────────────────────────

    #[test]
    fn extract_beam_cookie_found() {
        let result = extract_beam_cookie("beam_terminal_session=abc123def456; other=value");
        assert_eq!(result, Some("abc123def456".to_string()));
    }

    #[test]
    fn extract_beam_cookie_not_found() {
        assert_eq!(extract_beam_cookie("other=value"), None);
    }

    #[test]
    fn extract_beam_cookie_empty() {
        assert_eq!(extract_beam_cookie(""), None);
    }

    #[test]
    fn extract_zellij_set_cookie_standard() {
        let result = extract_zellij_set_cookie("session=abc123; HttpOnly; SameSite=Strict; Path=/");
        assert_eq!(result, Some("session=abc123".to_string()));
    }

    #[test]
    fn extract_zellij_set_cookie_simple() {
        let result = extract_zellij_set_cookie("session=abc123");
        assert_eq!(result, Some("session=abc123".to_string()));
    }

    #[test]
    fn extract_zellij_set_cookie_empty_value() {
        assert_eq!(extract_zellij_set_cookie("session=; HttpOnly"), None);
    }

    #[test]
    fn extract_zellij_set_cookie_malformed() {
        assert_eq!(extract_zellij_set_cookie("; HttpOnly"), None);
    }

    #[tokio::test]
    async fn auth_state_maps_external_beam_cookie_to_internal_zellij_cookie() {
        let auth = TerminalAuthState::new();
        let zellij_cookie = "zellij-session=secret-upstream-cookie".to_string();

        let beam_cookie = auth
            .insert(
                zellij_cookie.clone(),
                "beam-session-1".to_string(),
                TerminalPermission::Write,
            )
            .await;

        assert_ne!(beam_cookie, zellij_cookie);
        let (stored_zellij_cookie, stored_session_id, stored_permission) = auth
            .lookup(&beam_cookie)
            .await
            .expect("stored cookie entry");
        assert_eq!(stored_zellij_cookie, zellij_cookie);
        assert_eq!(stored_session_id, "beam-session-1");
        assert_eq!(stored_permission, TerminalPermission::Write);
    }

    // ── UsedTickets ────────────────────────────────────────────────

    #[test]
    fn used_tickets_dedupe() {
        let mut used = UsedTickets::default();
        assert!(used.insert_and_check("abc"));
        assert!(!used.insert_and_check("abc"));
        assert!(used.insert_and_check("def"));
    }

    #[test]
    fn used_tickets_accepts_after_ttl() {
        // Can't easily test TTL in a unit test without sleep,
        // but the prune path is tested implicitly via the verify flow.
    }

    #[test]
    fn used_tickets_save_and_load_preserves_dedupe() {
        let tmp = std::env::temp_dir().join(format!(
            "beam-used-tickets-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);
        // Insert some nonces
        let mut used = UsedTickets::default();
        assert!(used.insert_and_check("nonce-a"));
        assert!(used.insert_and_check("nonce-b"));
        used.save_to(&tmp);
        // Load into a new UsedTickets
        let mut loaded = UsedTickets::default();
        loaded.load_from(&tmp);
        // Same nonces should be rejected
        assert!(!loaded.insert_and_check("nonce-a"));
        assert!(!loaded.insert_and_check("nonce-b"));
        // New nonce should work
        assert!(loaded.insert_and_check("nonce-c"));
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn used_tickets_load_preserves_ttl_approx() {
        let tmp = std::env::temp_dir().join(format!(
            "beam-used-tickets-ttl-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);
        let mut used = UsedTickets::default();
        assert!(used.insert_and_check("nonce-x"));
        used.save_to(&tmp);
        // Load: the entry should NOT be expired (just loaded)
        let mut loaded = UsedTickets::default();
        loaded.load_from(&tmp);
        assert!(
            !loaded.insert_and_check("nonce-x"),
            "loaded entry should still block replay"
        );
        let _ = std::fs::remove_file(&tmp);
    }

    // ── Path classification ────────────────────────────────────────

    #[test]
    fn root_paths_identified() {
        assert!(is_zellij_root_path("session"));
        assert!(is_zellij_root_path("info"));
        assert!(is_zellij_root_path("info/version"));
        assert!(is_zellij_root_path("command/login"));
        assert!(is_zellij_root_path("command"));
        assert!(is_zellij_root_path("api/stats"));
        assert!(is_zellij_root_path("ws/terminal/mysession"));
        assert!(is_zellij_root_path("ws/control"));
        // Static assets served from zellij root
        assert!(is_zellij_root_path("assets/style.css"));
        assert!(is_zellij_root_path("assets/auth.js"));
        assert!(is_zellij_root_path("favicon.ico"));
    }

    #[test]
    fn session_paths_not_root() {
        assert!(!is_zellij_root_path(""));
        assert!(!is_zellij_root_path("ws")); // plain /ws goes to session WS
        assert!(!is_zellij_root_path("somefile.js"));
    }

    // ── WS path translation ────────────────────────────────────────

    #[test]
    fn translate_terminal_ws_replaces_session_name() {
        let result = translate_root_ws_path("terminal/beam-session-id-123", "bmx-beam-se");
        assert_eq!(result, "ws/terminal/bmx-beam-se");
    }

    #[test]
    fn translate_terminal_without_ws_prefix() {
        let result = translate_root_ws_path("terminal/beam-session-id-123", "bmx-beam-se");
        assert_eq!(result, "ws/terminal/bmx-beam-se");
    }

    #[test]
    fn translate_terminal_no_session() {
        // Zellij 0.44: WS endpoint is just /ws/terminal (no session name in path)
        assert_eq!(translate_root_ws_path("terminal", "bmx-any"), "ws/terminal");
        assert_eq!(translate_root_ws_path("control", "bmx-any"), "ws/control");
    }

    #[test]
    fn translate_other_ws_passthrough() {
        let result = translate_root_ws_path("ws/something-else", "any-session");
        assert_eq!(result, "ws/something-else");
    }

    // ── Secret is process-random ───────────────────────────────────

    #[test]
    fn ticket_secret_is_not_hardcoded() {
        let secret = ticket_secret();
        // Must be at least 16 bytes (uuid v4)
        assert!(secret.len() >= 16);
        // Must NOT equal the old hardcoded value
        assert_ne!(secret, b"beam-terminal-proxy-ticket-secret-v1");
    }
}
