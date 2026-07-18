//! Local API token: generation, reading, and atomic 0600 writes.
//!
//! Token format: `<issued-at unix seconds>.<64 lowercase hex chars>`. The
//! issue timestamp is embedded in the token itself — file mtime is NOT
//! trusted, because copies, backup restores, and sync tools all rewrite it.
//! The daemon owns generation and rotation; local clients (beam CLI) only
//! read the file and send the token verbatim as an `Authorization: Bearer`
//! credential. Validity is an exact string match against daemon state, so a
//! tampered timestamp never authenticates.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::BeamPaths;

/// Separator between the embedded issue timestamp and the random secret.
const TIMESTAMP_SEPARATOR: char = '.';

/// Current wall-clock time as unix seconds.
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Generate a new local API token issued at the current time.
pub fn generate_api_token() -> String {
    generate_api_token_at(now_unix_secs())
}

/// Generate a token for an explicit issue time (tests, clock control).
pub fn generate_api_token_at(issued_at_unix: u64) -> String {
    let secret = format!(
        "{}{}",
        Uuid::new_v4().as_simple(),
        Uuid::new_v4().as_simple()
    );
    format!("{issued_at_unix}{TIMESTAMP_SEPARATOR}{secret}")
}

/// Parse a token into (issued_at_unix, secret). Returns `None` for malformed
/// tokens — including legacy plain-hex tokens without a timestamp, which the
/// daemon then treats as stale and rotates out.
pub fn parse_api_token(token: &str) -> Option<(u64, &str)> {
    let (ts, secret) = token.split_once(TIMESTAMP_SEPARATOR)?;
    if secret.len() != 64 || !secret.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let issued_at = ts.parse().ok()?;
    Some((issued_at, secret))
}

/// Read the current token from disk. Returns `None` when the file is missing,
/// unreadable, or empty.
pub fn read_api_token(paths: &BeamPaths) -> Option<String> {
    fs::read_to_string(paths.api_token_file())
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|token| !token.is_empty())
}

/// Atomically write the token (tmp + rename) with 0600 permissions on Unix.
pub fn write_api_token(paths: &BeamPaths, token: &str) -> Result<()> {
    let path = paths.api_token_file();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&tmp, format!("{token}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, &path)
        .with_context(|| format!("failed to atomically write {}", path.display()))?;
    Ok(())
}

// ---- HMAC request signing ----
//
// The local api token doubles as an HMAC key and never appears on the wire:
// clients send `x-beam-ts` / `x-beam-nonce` / `x-beam-sig` headers instead.
// A sniffer on a forwarded link only sees one-time signatures, and the
// timestamp window plus nonce replay check make them unusable elsewhere.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// Header carrying the signature timestamp (unix seconds).
pub const SIG_TIMESTAMP_HEADER: &str = "x-beam-ts";
/// Header carrying the per-request nonce.
pub const SIG_NONCE_HEADER: &str = "x-beam-nonce";
/// Header carrying the hex HMAC-SHA256 signature.
pub const SIG_HEADER: &str = "x-beam-sig";

/// Maximum accepted clock skew between signer and verifier, in seconds.
pub const SIG_WINDOW_SECS: u64 = 60;

/// Generate a random per-request signature nonce.
pub fn generate_sig_nonce() -> String {
    Uuid::new_v4().simple().to_string()
}

/// Canonical string that gets HMAC-signed for one request. `path_query` is
/// the request path with query string (e.g. `/sessions/abc/input?x=1`).
pub fn signature_payload(
    ts_unix: u64,
    nonce: &str,
    method: &str,
    path_query: &str,
    body: &[u8],
) -> String {
    let body_hash = hex_encode(&Sha256::digest(body));
    format!(
        "{ts_unix}\n{nonce}\n{}\n{path_query}\n{body_hash}",
        method.to_ascii_uppercase()
    )
}

/// Compute the hex HMAC-SHA256 signature for one request.
pub fn sign_request(
    key: &str,
    ts_unix: u64,
    nonce: &str,
    method: &str,
    path_query: &str,
    body: &[u8],
) -> String {
    let payload = signature_payload(ts_unix, nonce, method, path_query, body);
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())
        .expect("hmac accepts keys of any length");
    mac.update(payload.as_bytes());
    hex_encode(&mac.finalize().into_bytes())
}

/// Constant-time verification of a presented hex signature.
pub fn verify_request_signature(
    key: &str,
    ts_unix: u64,
    nonce: &str,
    method: &str,
    path_query: &str,
    body: &[u8],
    presented_sig: &str,
) -> bool {
    let Some(sig_bytes) = hex_decode(presented_sig) else {
        return false;
    };
    let payload = signature_payload(ts_unix, nonce, method, path_query, body);
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())
        .expect("hmac accepts keys of any length");
    mac.update(payload.as_bytes());
    mac.verify_slice(&sig_bytes).is_ok()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-api-token-test-{}-{}-{}",
            label,
            nanos,
            std::process::id()
        )))
    }

    #[test]
    fn generate_embeds_timestamp_and_parses_roundtrip() {
        let now = now_unix_secs();
        let token = generate_api_token();
        let (issued_at, secret) = parse_api_token(&token).unwrap();
        assert!(issued_at.abs_diff(now) <= 1);
        assert_eq!(secret.len(), 64);
        assert!(secret.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(generate_api_token(), token);
    }

    #[test]
    fn parse_rejects_malformed_tokens() {
        assert!(parse_api_token("").is_none());
        // Legacy plain-hex token without timestamp.
        assert!(parse_api_token(&"a".repeat(64)).is_none());
        // Timestamp that does not parse.
        assert!(parse_api_token(&format!("not-a-ts.{}", "a".repeat(64))).is_none());
        // Secret too short.
        assert!(parse_api_token("123.abc").is_none());
        // Non-hex secret.
        assert!(parse_api_token(&format!("123.{}", "g".repeat(64))).is_none());
    }

    #[test]
    fn write_then_read_roundtrip_with_restrict_perms() {
        let paths = temp_paths("roundtrip");
        let token = generate_api_token();
        write_api_token(&paths, &token).unwrap();
        assert_eq!(read_api_token(&paths).as_deref(), Some(token.as_str()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(paths.api_token_file()).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "expected 0600 permissions, got {:o}", mode);
        }
        let _ = fs::remove_dir_all(paths.root());
    }

    #[test]
    fn rewrite_replaces_previous_token() {
        let paths = temp_paths("rewrite");
        write_api_token(&paths, "first").unwrap();
        write_api_token(&paths, "second").unwrap();
        assert_eq!(read_api_token(&paths).as_deref(), Some("second"));
        let _ = fs::remove_dir_all(paths.root());
    }

    #[test]
    fn read_returns_none_for_missing_or_empty_file() {
        let paths = temp_paths("missing");
        assert!(read_api_token(&paths).is_none());
        fs::create_dir_all(paths.root()).unwrap();
        fs::write(paths.api_token_file(), "  \n").unwrap();
        assert!(read_api_token(&paths).is_none());
        let _ = fs::remove_dir_all(paths.root());
    }

    #[test]
    fn signature_roundtrip_and_key_isolation() {
        let key = generate_api_token();
        let body = br#"{"content":"hello"}"#;
        let sig = sign_request(&key, 1000, "nonce-1", "POST", "/sessions/abc/input", body);
        assert!(verify_request_signature(
            &key, 1000, "nonce-1", "POST", "/sessions/abc/input", body, &sig
        ));
        // Same payload with another key must fail.
        let other_key = generate_api_token();
        assert!(!verify_request_signature(
            &other_key, 1000, "nonce-1", "POST", "/sessions/abc/input", body, &sig
        ));
    }

    #[test]
    fn signature_detects_tampering() {
        let key = generate_api_token();
        let body = b"original";
        let sig = sign_request(&key, 1000, "n", "POST", "/sessions/abc/input", body);
        // Tampered body, method, path, timestamp, or nonce all fail.
        assert!(!verify_request_signature(
            &key, 1000, "n", "POST", "/sessions/abc/input", b"tampered", &sig
        ));
        assert!(!verify_request_signature(
            &key, 1000, "n", "GET", "/sessions/abc/input", body, &sig
        ));
        assert!(!verify_request_signature(
            &key, 1000, "n", "POST", "/sessions/xyz/input", body, &sig
        ));
        assert!(!verify_request_signature(
            &key, 9999, "n", "POST", "/sessions/abc/input", body, &sig
        ));
        assert!(!verify_request_signature(
            &key, 1000, "other", "POST", "/sessions/abc/input", body, &sig
        ));
        // Malformed signature fails instead of panicking.
        assert!(!verify_request_signature(
            &key, 1000, "n", "POST", "/sessions/abc/input", body, "not-hex"
        ));
    }

    #[test]
    fn signature_method_case_is_normalized() {
        let key = generate_api_token();
        let sig = sign_request(&key, 1000, "n", "post", "/a", b"");
        assert!(verify_request_signature(&key, 1000, "n", "POST", "/a", b"", &sig));
    }
}
