use super::subscribe::{
    is_zellij_pane_closed, numeric_pane_id, parse_zellij_cursor_from_list_panes,
    parse_zellij_subscribe_viewport, viewport_to_ansi_chunk,
};
use super::zellij::ZellijBackend;
use super::{SpawnOpts, ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS, ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL};

#[test]
fn runtime_config_enables_zellij_web_clients() {
    let opts = SpawnOpts {
        cwd: "/tmp".to_string(),
        cols: 80,
        rows: 24,
        env: Vec::new(),
    };
    let (_tmp, config_path, _layout_path) =
        ZellijBackend::write_runtime_files("/bin/sh", &[], &opts)
            .expect("runtime files should be written");
    let config = std::fs::read_to_string(&config_path).expect("config should be readable");

    assert!(config.contains("web_server true"));
    assert!(config.contains("web_sharing \"on\""));
}

#[test]
fn zellij_pane_discovery_uses_200ms_retry_budget() {
    assert_eq!(
        ZELLIJ_PANE_DISCOVERY_RETRY_INTERVAL,
        std::time::Duration::from_millis(200)
    );
    assert_eq!(ZELLIJ_PANE_DISCOVERY_MAX_ATTEMPTS, 15);
}

#[test]
fn parse_terminal_pane_id_skips_plugin_panes() {
    let panes = br#"[
        {"id":0,"is_plugin":true},
        {"id":3,"is_plugin":false}
    ]"#;
    assert_eq!(
        ZellijBackend::parse_terminal_pane_id(panes),
        Some("terminal_3".to_string())
    );
}

#[test]
fn parse_terminal_pane_id_returns_none_without_terminal_pane() {
    let panes = br#"[
        {"id":0,"is_plugin":true},
        {"is_plugin":false}
    ]"#;
    assert_eq!(ZellijBackend::parse_terminal_pane_id(panes), None);
    assert_eq!(ZellijBackend::parse_terminal_pane_id(b"bad json"), None);
}

// ---- numeric_pane_id tests ----

#[test]
fn test_numeric_pane_id_valid() {
    assert_eq!(numeric_pane_id("terminal_1"), Some(1));
    assert_eq!(numeric_pane_id("terminal_0"), Some(0));
    assert_eq!(numeric_pane_id("terminal_42"), Some(42));
}

#[test]
fn test_numeric_pane_id_bare_number() {
    assert_eq!(numeric_pane_id("1"), Some(1));
    assert_eq!(numeric_pane_id("0"), Some(0));
    assert_eq!(numeric_pane_id("42"), Some(42));
    assert_eq!(numeric_pane_id("999"), Some(999));
}

#[test]
fn test_numeric_pane_id_invalid() {
    assert_eq!(numeric_pane_id(""), None);
    assert_eq!(numeric_pane_id("terminal_"), None);
    assert_eq!(numeric_pane_id("terminal_abc"), None);
    assert_eq!(numeric_pane_id("pane_1"), None);
    assert_eq!(numeric_pane_id("abc"), None);
}

// ---- parse_zellij_subscribe_viewport tests ----

#[test]
fn parse_subscribe_pane_update_viewport() {
    let line = r#"{"event":"pane_update","pane_id":"terminal_1","data":{"viewport":["line1","line2","line3"],"scrollback":[],"is_initial":true}}"#;
    let viewport = parse_zellij_subscribe_viewport(line);
    assert_eq!(
        viewport,
        Some(vec![
            "line1".to_string(),
            "line2".to_string(),
            "line3".to_string(),
        ])
    );
}

#[test]
fn parse_subscribe_pane_update_top_level_viewport() {
    let line = r#"{"event":"pane_update","pane_id":"terminal_1","viewport":["a","b"],"scrollback":null,"is_initial":true}"#;
    let viewport = parse_zellij_subscribe_viewport(line);
    assert_eq!(viewport, Some(vec!["a".to_string(), "b".to_string()]));
}

#[test]
fn parse_subscribe_pane_update_empty_viewport() {
    let line = r#"{"event":"pane_update","pane_id":"terminal_1","data":{"viewport":[],"scrollback":[],"is_initial":true}}"#;
    let viewport = parse_zellij_subscribe_viewport(line);
    assert_eq!(viewport, Some(vec![]));
}

#[test]
fn parse_subscribe_pane_update_with_ansi() {
    let line = r#"{"event":"pane_update","pane_id":"terminal_1","data":{"viewport":["\u001b[32mgreen\u001b[0m","normal"],"scrollback":[],"is_initial":false}}"#;
    let viewport = parse_zellij_subscribe_viewport(line);
    assert_eq!(
        viewport,
        Some(vec![
            "\u{1b}[32mgreen\u{1b}[0m".to_string(),
            "normal".to_string(),
        ])
    );
}

#[test]
fn parse_subscribe_pane_closed_returns_none() {
    let line = r#"{"event":"pane_closed","pane_id":"terminal_1"}"#;
    assert_eq!(parse_zellij_subscribe_viewport(line), None);
}

#[test]
fn parse_subscribe_unknown_event() {
    let line = r#"{"event":"session_closed"}"#;
    assert_eq!(parse_zellij_subscribe_viewport(line), None);
}

#[test]
fn parse_subscribe_invalid_json() {
    assert_eq!(parse_zellij_subscribe_viewport("not json"), None);
    assert_eq!(parse_zellij_subscribe_viewport(""), None);
}

// ---- is_zellij_pane_closed tests ----

#[test]
fn test_is_zellij_pane_closed_true() {
    assert!(is_zellij_pane_closed(
        r#"{"event":"pane_closed","pane_id":"terminal_1"}"#
    ));
}

#[test]
fn test_is_zellij_pane_closed_false() {
    assert!(!is_zellij_pane_closed(
        r#"{"event":"pane_update","pane_id":"terminal_1","data":{}}"#
    ));
    assert!(!is_zellij_pane_closed("not json"));
    assert!(!is_zellij_pane_closed(""));
}

// ---- viewport_to_ansi_chunk tests ----

#[test]
fn viewport_to_ansi_basic() {
    let viewport = vec!["hello".to_string(), "world".to_string()];
    let chunk = viewport_to_ansi_chunk(&viewport);
    assert!(chunk.contains("\x1b[H"), "should contain home");
    assert!(chunk.contains("\x1b[2J"), "should contain clear screen");
    assert!(chunk.contains("\x1b[?25l"), "should hide cursor");
    assert!(chunk.contains("\x1b[?25h"), "should show cursor");
    assert!(
        chunk.contains("hello\r\nworld"),
        "should join lines with CRLF"
    );
}

#[test]
fn viewport_to_ansi_no_trailing_crlf() {
    let viewport = vec!["line1".to_string()];
    let chunk = viewport_to_ansi_chunk(&viewport);
    assert!(!chunk.ends_with("\r\n"), "must not trail with CRLF");
    assert!(!chunk.ends_with('\n'), "must not trail with LF");
    assert!(
        chunk.ends_with("line1\x1b[?25h"),
        "should end with last line + show cursor"
    );
}

#[test]
fn viewport_to_ansi_multiline_no_trailing_crlf() {
    let viewport = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let chunk = viewport_to_ansi_chunk(&viewport);
    assert!(!chunk.ends_with("\r\n"));
    assert!(!chunk.ends_with('\n'));
    assert!(chunk.contains("a\r\nb\r\nc"));
}

#[test]
fn viewport_to_ansi_empty() {
    assert_eq!(viewport_to_ansi_chunk(&[]), "");
}

#[test]
fn viewport_to_ansi_preserves_ansi_content() {
    let viewport = vec![
        "\u{1b}[32mgreen\u{1b}[0m text".to_string(),
        "\u{1b}[1mbold\u{1b}[0m".to_string(),
    ];
    let chunk = viewport_to_ansi_chunk(&viewport);
    assert!(chunk.contains("\u{1b}[32mgreen\u{1b}[0m text"));
    assert!(chunk.contains("\u{1b}[1mbold\u{1b}[0m"));
}

// ---- parse_zellij_cursor_from_list_panes tests ----

#[test]
fn parse_zellij_cursor_single_pane() {
    let json = r#"[
        {"id":1,"is_plugin":false,"cursor_coordinates_in_pane":{"x":10,"y":5}}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 1), Some((10, 5)));
}

#[test]
fn parse_zellij_cursor_array_format() {
    let json = r#"[
        {"id":1,"cursor_coordinates_in_pane":[3, 7]}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 1), Some((3, 7)));
}

#[test]
fn parse_zellij_cursor_array_zero() {
    let json = r#"[
        {"id":1,"cursor_coordinates_in_pane":[0, 0]}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 1), Some((0, 0)));
}

#[test]
fn parse_zellij_cursor_multiple_panes() {
    let json = r#"[
        {"id":1,"is_plugin":false,"cursor_coordinates_in_pane":{"x":0,"y":0}},
        {"id":2,"is_plugin":false,"cursor_coordinates_in_pane":{"x":80,"y":24}},
        {"id":3,"is_plugin":true}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 1), Some((0, 0)));
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 2), Some((80, 24)));
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 3), None);
}

#[test]
fn parse_zellij_cursor_pane_not_found() {
    let json = r#"[
        {"id":1,"cursor_coordinates_in_pane":{"x":10,"y":5}}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 2), None);
}

#[test]
fn parse_zellij_cursor_missing_field() {
    assert_eq!(parse_zellij_cursor_from_list_panes(r#"[]"#, 1), None);
    assert_eq!(
        parse_zellij_cursor_from_list_panes(r#"[{"id":1}]"#, 1),
        None
    );
    assert_eq!(parse_zellij_cursor_from_list_panes("bad json", 1), None);
}

#[test]
fn parse_zellij_cursor_zero_coordinates() {
    let json = r#"[
        {"id":1,"cursor_coordinates_in_pane":{"x":0,"y":0}}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 1), Some((0, 0)));
}

#[test]
fn parse_zellij_cursor_id_as_number_in_json() {
    let json = r#"[
        {"id":42,"cursor_coordinates_in_pane":{"x":5,"y":3}}
    ]"#;
    assert_eq!(parse_zellij_cursor_from_list_panes(json, 42), Some((5, 3)));
}

// ---- dump_screen_viewport_args tests ----

/// ZellijBackend viewport dump args contain exactly `dump-screen --pane-id <id>`,
/// and MUST NOT include `--full`.
#[test]
fn dump_screen_viewport_args_no_full_flag() {
    let args = ZellijBackend::dump_screen_viewport_args("pane_1");
    assert_eq!(args.len(), 3);
    assert_eq!(args[0], "dump-screen");
    assert_eq!(args[1], "--pane-id");
    assert_eq!(args[2], "pane_1");
    assert!(!args.contains(&"--full".to_string()));
}

/// The helper preserves the caller-provided pane id as-is.
#[test]
fn dump_screen_viewport_args_different_pane_ids() {
    let args = ZellijBackend::dump_screen_viewport_args("terminal_99");
    assert_eq!(args[2], "terminal_99");
    assert!(!args.contains(&"--full".to_string()));
}

/// Observing backend's `dump_screen` reuses the same viewport args helper
/// and therefore also never passes `--full`.
#[test]
fn dump_screen_viewport_args_no_full_through_observe_path() {
    let args = ZellijBackend::dump_screen_viewport_args("observe_pane");
    assert_eq!(args.len(), 3);
    assert_eq!(args[0], "dump-screen");
    assert_eq!(args[1], "--pane-id");
    assert_eq!(args[2], "observe_pane");
    assert!(!args.contains(&"--full".to_string()));
}
