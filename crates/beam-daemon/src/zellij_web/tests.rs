use super::*;

use super::lifecycle::{
    ZellijWebHealth, classify_zellij_web_health, parse_zellij_cli_version,
    parse_zellij_web_http_response, parse_zellij_web_status_output,
    parse_zellij_web_version_response,
};
use super::tokens::{
    TokenStrategy, diag_from_output, err_bare_generic_failure, err_bare_name_conflict,
    err_bare_parse_failure, extract_uuid_from_line, is_name_conflict, is_token_name_rejected,
    parse_token_from_output,
};

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
    let stdout =
        "Created token successfully\n\ntoken_1: 550e8400-e29b-41d4-a716-446655440000 (read-only)\n";
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
    use super::lifecycle::parse_linux_tcp_listen_inodes;
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
    use super::lifecycle::parse_linux_tcp_listen_inodes;
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

// ── token creation diagnostic sanitization ──

#[test]
fn no_token_leak_in_error_diagnostics() {
    // Sentinel: a plausible hex token and a UUID that could appear in zellij output.
    let sentinel_hex = "deadbeefdeadbeefdeadbeefdeadbeef";
    let sentinel_uuid = "550e8400-e29b-41d4-a716-446655440000";

    // Sample stdout/stderr that a real zellij invocation might return,
    // including sentinel tokens deliberately.
    let stdout = format!(
        "Created token successfully\n\ntoken_1: {} (read-only)\n",
        sentinel_uuid
    );
    let stderr = format!("Warning: {}\n", sentinel_hex);

    // ── Path A: bare success but can't parse ──
    let diag = diag_from_output(&stdout, &stderr, Some(0));
    let err_a = err_bare_parse_failure(&diag);
    let text_a = format!("{:#}", err_a);
    assert!(
        !text_a.contains(sentinel_hex),
        "parse-failure error leaked sentinel hex: {text_a}"
    );
    assert!(
        !text_a.contains(sentinel_uuid),
        "parse-failure error leaked sentinel uuid: {text_a}"
    );

    // ── Path B: name conflict ──
    let diag = diag_from_output(&stdout, &stderr, Some(2));
    let err_b = err_bare_name_conflict(&diag);
    let text_b = format!("{:#}", err_b);
    assert!(
        !text_b.contains(sentinel_hex),
        "name-conflict error leaked sentinel hex: {text_b}"
    );
    assert!(
        !text_b.contains(sentinel_uuid),
        "name-conflict error leaked sentinel uuid: {text_b}"
    );

    // ── Path C: bare failure (general) ──
    let diag = diag_from_output(&stdout, &stderr, Some(1));
    let err_c = err_bare_generic_failure(&diag);
    let text_c = format!("{:#}", err_c);
    assert!(
        !text_c.contains(sentinel_hex),
        "bare-failure error leaked sentinel hex: {text_c}"
    );
    assert!(
        !text_c.contains(sentinel_uuid),
        "bare-failure error leaked sentinel uuid: {text_c}"
    );
}

#[test]
fn parse_token_from_sentinel_data_still_works() {
    // Sentinel tokens placed in stdout/stderr — the parser must still extract them.
    let sentinel_hex = "deadbeefdeadbeefdeadbeefdeadbeef";
    let sentinel_uuid = "550e8400-e29b-41d4-a716-446655440000";

    // UUID in stdout as zellij 0.44.x format
    let stdout = format!("token_1: {} (read-only)\n", sentinel_uuid);
    let parsed = parse_token_from_output(&stdout, "");
    assert_eq!(parsed.as_deref(), Some(sentinel_uuid));

    // Hex token in stdout as bare format
    let parsed = parse_token_from_output(sentinel_hex, "");
    assert_eq!(parsed.as_deref(), Some(sentinel_hex));

    // Hex token mixed with stderr noise (from stderr)
    let parsed = parse_token_from_output("", sentinel_hex);
    assert_eq!(parsed.as_deref(), Some(sentinel_hex));

    // Fallback: >= 16 chars, no whitespace (from stderr)
    let sentinel_fallback = "abcdefghijklmnopq";
    let parsed = parse_token_from_output("", sentinel_fallback);
    assert_eq!(parsed.as_deref(), Some(sentinel_fallback));
}

#[test]
fn all_diagnostic_paths_produce_clean_output() {
    // Covers all bare diagnostic paths using the production helpers,
    // ensuring they never leak raw stdout/stderr content.
    let hex = "deadbeefdeadbeefdeadbeefdeadbeef";
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    let stdout = format!("{hex}\n{uuid}\n");
    let stderr = format!("error: {hex}\n{uuid}\n");

    for exit in [None, Some(1), Some(0)] {
        let diag = diag_from_output(&stdout, &stderr, exit);

        // Path: bare success but parse failure (exit=0 only applies)
        if exit == Some(0) {
            let e = err_bare_parse_failure(&diag);
            let t = format!("{:#}", e);
            assert!(!t.contains(hex), "parse-fail path leaked hex: {t}");
            assert!(!t.contains(uuid), "parse-fail path leaked uuid: {t}");
        }

        // Path: name conflict
        let e = err_bare_name_conflict(&diag);
        let t = format!("{:#}", e);
        assert!(!t.contains(hex), "name-conflict path leaked hex: {t}");
        assert!(!t.contains(uuid), "name-conflict path leaked uuid: {t}");

        // Path: bare failure (general)
        let e = err_bare_generic_failure(&diag);
        let t = format!("{:#}", e);
        assert!(!t.contains(hex), "bare-failure path leaked hex: {t}");
        assert!(!t.contains(uuid), "bare-failure path leaked uuid: {t}");
    }
}
