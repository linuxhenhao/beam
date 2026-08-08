//! Local API token state and daily rotation.
//!
//! The token authenticates local clients (beam CLI) on routes behind
//! `dashboard_gate`: the gate accepts it as an alternative to the dashboard
//! token. It is persisted at `$BEAM_HOME/api-token` (0600) so the short-lived
//! CLI process can read it on every invocation.
//!
//! Token age comes from the timestamp embedded in the token itself (file
//! mtime is not trusted). Rotation happens once per day; the previous token
//! stays valid for a one-minute grace period so local calls in flight during
//! the rotation instant do not fail. Validity is always an exact string match
//! against daemon state — a leaked token with a tampered timestamp never
//! authenticates.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use beam_core::BeamPaths;
use beam_core::api_token::{
    SIG_WINDOW_SECS, generate_api_token, now_unix_secs, parse_api_token, read_api_token,
    verify_request_signature, write_api_token,
};
use tracing::{info, warn};

use crate::AppState;

/// How long a token stays current before rotation.
pub(crate) const API_TOKEN_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// How long the previous token stays accepted after a rotation. Only needs to
/// cover local calls in flight while the file is swapped.
pub(crate) const API_TOKEN_GRACE: Duration = Duration::from_secs(60);
/// How often the background task checks whether rotation is due.
const ROTATE_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Tolerated clock skew for embedded timestamps ahead of wall time. A token
/// dated further into the future is treated as due for rotation (broken clock
/// or tampered file).
const MAX_FUTURE_SKEW: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub(crate) struct ApiTokenState {
    current: String,
    issued_at_unix: u64,
    previous: Option<PreviousApiToken>,
    /// Recently seen signature nonces (nonce -> unix secs first seen), used
    /// for replay protection inside the signature window.
    seen_nonces: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
struct PreviousApiToken {
    token: String,
    valid_until_unix: u64,
}

fn issued_at_of(token: &str) -> Option<u64> {
    parse_api_token(token).map(|(issued_at, _)| issued_at)
}

impl ApiTokenState {
    pub(crate) fn new(current: String) -> Self {
        let issued_at_unix = issued_at_of(&current).unwrap_or_else(now_unix_secs);
        Self {
            current,
            issued_at_unix,
            previous: None,
            seen_nonces: HashMap::new(),
        }
    }

    /// Fixed-token state for tests and fixtures.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new("test-api-token".to_string())
    }

    pub(crate) fn is_valid(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        if self.current == token {
            return true;
        }
        match &self.previous {
            Some(prev) if prev.valid_until_unix > now_unix_secs() => prev.token == token,
            _ => false,
        }
    }

    /// Verify an HMAC request signature (x-beam-ts/-nonce/-sig headers)
    /// against the current key, or the previous key inside its grace window.
    /// Enforces the timestamp window and rejects nonce replays; the token
    /// itself never appears on the wire.
    pub(crate) fn verify_signature(
        &mut self,
        ts_unix: u64,
        nonce: &str,
        method: &str,
        path_query: &str,
        body: &[u8],
        presented_sig: &str,
    ) -> bool {
        let now = now_unix_secs();
        if ts_unix.abs_diff(now) > SIG_WINDOW_SECS {
            return false;
        }
        if nonce.is_empty() || nonce.len() > 128 {
            return false;
        }
        self.seen_nonces
            .retain(|_, seen_at| seen_at.abs_diff(now) <= SIG_WINDOW_SECS);
        if self.seen_nonces.contains_key(nonce) {
            return false;
        }
        let key_matches = verify_request_signature(
            &self.current,
            ts_unix,
            nonce,
            method,
            path_query,
            body,
            presented_sig,
        ) || self
            .previous
            .as_ref()
            .filter(|prev| prev.valid_until_unix > now)
            .is_some_and(|prev| {
                verify_request_signature(
                    &prev.token,
                    ts_unix,
                    nonce,
                    method,
                    path_query,
                    body,
                    presented_sig,
                )
            });
        if key_matches {
            self.seen_nonces.insert(nonce.to_string(), now);
        }
        key_matches
    }

    fn due_for_rotation(&self, now_unix: u64) -> bool {
        token_age_due(self.issued_at_unix, now_unix)
    }

    fn rotate(&mut self, new_token: String) {
        let previous = std::mem::replace(&mut self.current, new_token);
        self.issued_at_unix = issued_at_of(&self.current).unwrap_or_else(now_unix_secs);
        self.previous = Some(PreviousApiToken {
            token: previous,
            valid_until_unix: now_unix_secs() + API_TOKEN_GRACE.as_secs(),
        });
    }
}

/// Whether a token issued at `issued_at_unix` is due for rotation at
/// `now_unix`, based purely on the embedded timestamp.
fn token_age_due(issued_at_unix: u64, now_unix: u64) -> bool {
    if issued_at_unix > now_unix + MAX_FUTURE_SKEW.as_secs() {
        return true;
    }
    now_unix.saturating_sub(issued_at_unix) >= API_TOKEN_TTL.as_secs()
}

/// Load the persisted token, generating and persisting a fresh one when the
/// file is missing, malformed (e.g. legacy format without timestamp), or due
/// for rotation per its embedded timestamp. A replaced token stays valid for
/// the grace period.
pub(crate) async fn load_or_create_api_token(paths: &BeamPaths) -> Result<ApiTokenState> {
    let existing = read_api_token(paths);
    let now = now_unix_secs();
    let fresh = existing
        .as_deref()
        .and_then(issued_at_of)
        .map(|issued_at| !token_age_due(issued_at, now))
        .unwrap_or(false);
    if fresh {
        return Ok(ApiTokenState::new(
            existing.expect("fresh implies some token"),
        ));
    }
    let new_token = generate_api_token();
    write_api_token(paths, &new_token)?;
    let mut state = ApiTokenState::new(new_token);
    if let Some(old) = existing {
        state.previous = Some(PreviousApiToken {
            token: old,
            valid_until_unix: now + API_TOKEN_GRACE.as_secs(),
        });
    }
    info!("generated fresh local api token");
    Ok(state)
}

/// Rotate when due. The file is written first; in-memory state only swaps
/// after the write succeeds, so CLI readers never observe a token the daemon
/// does not accept.
pub(crate) async fn rotate_api_token_if_due(state: &AppState) -> Result<bool> {
    let now = now_unix_secs();
    if !state.api_token.read().await.due_for_rotation(now) {
        return Ok(false);
    }
    let new_token = generate_api_token();
    write_api_token(&state.paths, &new_token)?;
    state.api_token.write().await.rotate(new_token);
    info!("rotated local api token");
    Ok(true)
}

pub(crate) fn spawn_api_token_rotator(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ROTATE_CHECK_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(err) = rotate_api_token_if_due(&state).await {
                warn!("local api token rotation failed: {err:#}");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::api_token::generate_api_token_at;

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-api-token-state-test-{}-{}-{}",
            label,
            nanos,
            std::process::id()
        )))
    }

    #[test]
    fn accepts_current_and_rejects_unknown_or_empty() {
        let token = generate_api_token();
        let state = ApiTokenState::new(token.clone());
        assert!(state.is_valid(&token));
        assert!(!state.is_valid("other"));
        assert!(!state.is_valid(""));
    }

    #[test]
    fn tampered_timestamp_breaks_exact_match() {
        let now = now_unix_secs();
        let token = generate_api_token_at(now - 100);
        let state = ApiTokenState::new(token.clone());
        let (_, secret) = parse_api_token(&token).unwrap();
        // Attacker rewrites the embedded timestamp to look fresh: the string
        // no longer equals the stored token, so it never authenticates.
        let tampered = format!("{}.{}", now + 10_000, secret);
        assert!(!state.is_valid(&tampered));
    }

    #[test]
    fn previous_valid_within_grace_only() {
        let mut state = ApiTokenState::new(generate_api_token());
        let now = now_unix_secs();
        state.previous = Some(PreviousApiToken {
            token: "previous".to_string(),
            valid_until_unix: now + 60,
        });
        assert!(state.is_valid("previous"));

        state.previous = Some(PreviousApiToken {
            token: "previous".to_string(),
            valid_until_unix: now.saturating_sub(1),
        });
        assert!(!state.is_valid("previous"));
    }

    #[test]
    fn rotate_moves_current_to_previous_with_grace() {
        let old_token = generate_api_token();
        let mut state = ApiTokenState::new(old_token.clone());
        state.rotate(generate_api_token());
        assert!(state.is_valid(&state.current.clone()));
        assert!(state.is_valid(&old_token));
        assert!(!state.due_for_rotation(now_unix_secs()));
    }

    #[test]
    fn due_rotation_uses_embedded_timestamp() {
        let now = now_unix_secs();
        assert!(!token_age_due(now - 60, now));
        assert!(token_age_due(now - API_TOKEN_TTL.as_secs(), now));
        assert!(token_age_due(now - 10 * API_TOKEN_TTL.as_secs(), now));
        // Within clock skew: not due. Beyond skew: due (tampered/broken clock).
        assert!(!token_age_due(now + 60, now));
        assert!(token_age_due(now + MAX_FUTURE_SKEW.as_secs() + 60, now));
    }

    #[tokio::test]
    async fn load_or_create_generates_persisted_token() {
        let paths = temp_paths("create");
        let state = load_or_create_api_token(&paths).await.unwrap();
        let persisted = read_api_token(&paths).unwrap();
        assert!(state.is_valid(&persisted));
        assert!(parse_api_token(&persisted).is_some());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(paths.api_token_file()).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn load_or_create_reuses_fresh_token_file() {
        let paths = temp_paths("reuse");
        let token = generate_api_token();
        write_api_token(&paths, &token).unwrap();
        let state = load_or_create_api_token(&paths).await.unwrap();
        assert!(state.is_valid(&token));
        assert_eq!(read_api_token(&paths).as_deref(), Some(token.as_str()));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn load_or_create_rotates_stale_token_keeping_grace() {
        let paths = temp_paths("stale");
        let stale = generate_api_token_at(now_unix_secs() - API_TOKEN_TTL.as_secs() - 60);
        write_api_token(&paths, &stale).unwrap();
        let state = load_or_create_api_token(&paths).await.unwrap();
        let persisted = read_api_token(&paths).unwrap();
        assert_ne!(persisted, stale);
        assert!(state.is_valid(&persisted));
        // Stale token remains valid inside the one-minute grace window.
        assert!(state.is_valid(&stale));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn load_or_create_rotates_legacy_token_without_timestamp() {
        let paths = temp_paths("legacy");
        let legacy = "a".repeat(64);
        write_api_token(&paths, &legacy).unwrap();
        let state = load_or_create_api_token(&paths).await.unwrap();
        let persisted = read_api_token(&paths).unwrap();
        assert!(parse_api_token(&persisted).is_some());
        assert!(state.is_valid(&persisted));
        assert!(state.is_valid(&legacy));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    fn signed_state() -> (ApiTokenState, String) {
        let key = generate_api_token();
        (ApiTokenState::new(key.clone()), key)
    }

    #[test]
    fn verify_signature_accepts_valid_and_records_nonce() {
        let (mut state, key) = signed_state();
        let now = now_unix_secs();
        let sig = beam_core::api_token::sign_request(
            &key,
            now,
            "nonce-1",
            "POST",
            "/sessions/abc/input",
            b"body",
        );
        assert!(state.verify_signature(
            now,
            "nonce-1",
            "POST",
            "/sessions/abc/input",
            b"body",
            &sig
        ));
        // Replay with the same nonce is rejected.
        assert!(!state.verify_signature(
            now,
            "nonce-1",
            "POST",
            "/sessions/abc/input",
            b"body",
            &sig
        ));
        // A fresh nonce still works.
        let sig2 = beam_core::api_token::sign_request(
            &key,
            now,
            "nonce-2",
            "POST",
            "/sessions/abc/input",
            b"body",
        );
        assert!(state.verify_signature(
            now,
            "nonce-2",
            "POST",
            "/sessions/abc/input",
            b"body",
            &sig2
        ));
    }

    #[test]
    fn verify_signature_rejects_stale_timestamp_and_bad_sig() {
        let (mut state, key) = signed_state();
        let now = now_unix_secs();
        let stale_ts = now - SIG_WINDOW_SECS - 10;
        let sig = beam_core::api_token::sign_request(&key, stale_ts, "n", "GET", "/sessions", b"");
        assert!(!state.verify_signature(stale_ts, "n", "GET", "/sessions", b"", &sig));
        // Well-formed headers but wrong signature (signed over other data).
        let bad = beam_core::api_token::sign_request(&key, now, "n", "GET", "/other", b"");
        assert!(!state.verify_signature(now, "n", "GET", "/sessions", b"", &bad));
        // Empty or oversized nonce rejected.
        let sig = beam_core::api_token::sign_request(&key, now, "n", "GET", "/s", b"");
        assert!(!state.verify_signature(now, "", "GET", "/s", b"", &sig));
        assert!(!state.verify_signature(now, &"x".repeat(200), "GET", "/s", b"", &sig));
    }

    #[test]
    fn verify_signature_accepts_previous_key_within_grace() {
        let old_key = generate_api_token();
        let mut state = ApiTokenState::new(old_key.clone());
        state.rotate(generate_api_token());
        let now = now_unix_secs();
        let sig = beam_core::api_token::sign_request(&old_key, now, "n", "GET", "/sessions", b"");
        assert!(state.verify_signature(now, "n", "GET", "/sessions", b"", &sig));

        // After grace expiry the previous key no longer verifies.
        let old_key2 = generate_api_token();
        let mut state2 = ApiTokenState::new(old_key2.clone());
        state2.rotate(generate_api_token());
        state2.previous = state2.previous.map(|mut p| {
            p.valid_until_unix = now.saturating_sub(1);
            p
        });
        let sig2 =
            beam_core::api_token::sign_request(&old_key2, now, "n2", "GET", "/sessions", b"");
        assert!(!state2.verify_signature(now, "n2", "GET", "/sessions", b"", &sig2));
    }
}
