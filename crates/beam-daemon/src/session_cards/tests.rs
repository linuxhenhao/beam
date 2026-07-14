use super::*;
use crate::tests::test_helpers::*;
use crate::{
    AdoptedFrom, BotConfig, CliUsageLimitState, DaemonToWorker, DisplayMode, LarkEventMention,
    PendingResponseCardState, ScreenStatus, Session, SessionScope, SessionStatus, SessionSummary,
    build_adopt_zellij_result_reply, build_closed_session_card, build_report_post_content,
    handle_lark_card_action_payload, prompt, terminal_auth, worker_ready_display_mode_command,
};
use beam_core::CliUsageLimitKind;
use serde_json::Value;
use std::collections::HashMap;

// ---- Test-only helpers (originally #[cfg(test)] in session_cards.rs) ----

fn truncate_card_screen(screen: &str) -> String {
    let clean = screen.replace('\r', "");
    let mut out = String::new();
    for line in clean.lines().take(36) {
        let line = if line.chars().count() > 120 {
            format!("{}...", line.chars().take(117).collect::<String>())
        } else {
            line.to_string()
        };
        out.push_str(&line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

fn render_streaming_card_body(session: &Session) -> String {
    match session.display_mode.unwrap_or(DisplayMode::Hidden) {
        DisplayMode::Hidden => "[screen hidden]".to_string(),
        DisplayMode::Screenshot => {
            truncate_card_screen(session.current_screen.as_deref().unwrap_or(""))
        }
    }
}

// ---- Tests ----

#[test]
fn streaming_card_template_matches_expected_status_colors() {
    assert_eq!(streaming_card_template("starting"), "yellow");
    assert_eq!(streaming_card_template("working"), "blue");
    assert_eq!(streaming_card_template("idle"), "green");
    assert_eq!(streaming_card_template("limited"), "red");
    assert_eq!(streaming_card_template("closed"), "grey");
}

#[test]
fn screen_status_card_label_matches_worker_statuses() {
    assert_eq!(screen_status_card_label(ScreenStatus::Starting), "starting");
    assert_eq!(screen_status_card_label(ScreenStatus::Working), "working");
    assert_eq!(screen_status_card_label(ScreenStatus::Idle), "idle");
    assert_eq!(
        screen_status_card_label(ScreenStatus::Analyzing),
        "analyzing"
    );
    assert_eq!(screen_status_card_label(ScreenStatus::Limited), "limited");
}

#[test]
fn session_stream_status_uses_last_screen_status_and_defaults_idle() {
    let mut session = make_session("sess-status");
    assert_eq!(session_stream_status(&session), "idle");

    session.last_screen_status = Some(ScreenStatus::Working);
    assert_eq!(session_stream_status(&session), "working");

    session.last_screen_status = Some(ScreenStatus::Analyzing);
    assert_eq!(session_stream_status(&session), "analyzing");

    session.last_screen_status = Some(ScreenStatus::Limited);
    session.usage_limit = Some(CliUsageLimitState {
        limited: true,
        kind: CliUsageLimitKind::Usage,
        retry_at_ms: 42,
        retry_label: "3:15 PM".to_string(),
        retry_ready: true,
    });
    assert_eq!(session_stream_status(&session), "retry_ready");
}

#[test]
fn build_adopt_helpers_render_stable_replies() {
    let session = make_session("sess-1");
    let summary = SessionSummary::from(&session);
    assert_eq!(
        build_adopt_zellij_result_reply(Ok(&summary)),
        "adopted sess-1"
    );
    assert_eq!(
        build_adopt_zellij_result_reply(Err("session not found")),
        "adopt failed: session not found"
    );
}

#[test]
fn build_closed_session_card_contains_resume_button_and_command() {
    let mut session = make_session("sess-9");
    session.title = "Fix beam".to_string();
    session.working_dir = Some("/repo/beam".to_string());
    session.cli_id = Some("codex".to_string());
    session.root_message_id = "root-9".to_string();

    let card: Value =
        serde_json::from_str(&build_closed_session_card(&session)).expect("valid card json");
    assert_eq!(
        card.pointer("/header/title/content")
            .and_then(Value::as_str),
        Some("session closed")
    );
    let body = card
        .pointer("/elements/0/content")
        .and_then(Value::as_str)
        .expect("markdown body");
    assert!(body.contains("Fix beam"));
    assert!(body.contains("beam session resume sess-9"));
    assert!(body.contains("/repo/beam"));
    assert_eq!(
        card.pointer("/elements/1/actions/0/value/action")
            .and_then(Value::as_str),
        Some("resume")
    );
    assert_eq!(
        card.pointer("/elements/1/actions/0/value/session_id")
            .and_then(Value::as_str),
        Some("sess-9")
    );
}

#[test]
fn build_writable_session_card_lists_single_choice_button() {
    let mut session = make_session("sess-7");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.title = "Investigate".to_string();
    let write_url = "http://proxy.example.com/s/sess-7?token=abc";
    let card: Value = serde_json::from_str(&build_writable_session_card(&session, write_url))
        .expect("valid card json");
    let actions = card
        .pointer("/elements/2/actions")
        .and_then(Value::as_array)
        .expect("actions array");
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].pointer("/multi_url/url").and_then(Value::as_str),
        Some("http://proxy.example.com/s/sess-7?token=abc")
    );
    assert_eq!(
        actions[0].pointer("/text/content").and_then(Value::as_str),
        Some("Writable terminal")
    );
}

#[test]
fn build_writable_session_card_keeps_choice_copy_for_adopted_sessions() {
    let mut session = make_session("sess-7-adopted");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.title = "Adopted".to_string();
    session.adopted_from = Some(AdoptedFrom {
        zellij_session: Some("my-session".to_string()),
        zellij_pane_id: Some("pane-1".to_string()),
        original_cli_pid: 9999,
        cwd: "/home/user".to_string(),
        ..Default::default()
    });
    let write_url = "http://proxy.example.com/s/sess-7-adopted?token=abc";
    let card: Value = serde_json::from_str(&build_writable_session_card(&session, write_url))
        .expect("valid card json");
    let actions = card
        .pointer("/elements/2/actions")
        .and_then(Value::as_array)
        .expect("actions array");
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].pointer("/text/content").and_then(Value::as_str),
        Some("Writable terminal")
    );
    assert_eq!(
        actions[0].pointer("/multi_url/url").and_then(Value::as_str),
        Some("http://proxy.example.com/s/sess-7-adopted?token=abc")
    );
}

#[test]
fn build_streaming_card_keeps_hidden_mode_actions_minimal() {
    let mut session = make_session("sess-8");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    // Use a clean URL without legacy token to test ticket-based auth
    session.terminal_url = Some("http://127.0.0.1:9000/s/sess-8".to_string());
    session.current_screen = Some("hello".to_string());
    session.stream_card_nonce = Some("nonce-live".to_string());
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
    let body = card
        .pointer("/elements/0/content")
        .and_then(Value::as_str)
        .expect("markdown body");
    assert!(
        !body.contains("Open read-only terminal"),
        "markdown should not contain the old read-only terminal button copy"
    );
    let actions = card
        .pointer("/elements/2/actions")
        .and_then(Value::as_array)
        .expect("actions array");
    // Collect action names for presence check (order may vary depending on token availability)
    let action_names: Vec<&str> = actions
        .iter()
        .filter_map(|a| a.pointer("/value/action").and_then(Value::as_str))
        .collect();
    assert!(
        action_names.contains(&"toggle_display"),
        "should have toggle_display action"
    );
    assert!(
        !action_names.contains(&"get_read_only_link"),
        "should not have get_read_only_link action"
    );
    assert!(
        action_names.contains(&"choose_read_only_terminal_link"),
        "should have choose_read_only_terminal_link action"
    );
    assert!(
        action_names.contains(&"get_write_link"),
        "should have get_write_link action"
    );
    let choose_action = actions
        .iter()
        .find(|a| {
            a.pointer("/value/action").and_then(Value::as_str)
                == Some("choose_read_only_terminal_link")
        })
        .expect("choose action should exist");
    assert_eq!(
        choose_action
            .pointer("/text/content")
            .and_then(Value::as_str),
        Some("Choose read-only terminal entry")
    );
    assert!(choose_action.pointer("/multi_url").is_none());
    assert!(card.pointer("/elements/3").is_none());
}

#[test]
fn build_streaming_card_uses_chinese_labels_for_zh_locale() {
    let mut session = make_session("sess-zh");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.locale = Some("zh".to_string());
    session.terminal_url = Some("http://127.0.0.1:9000/s/sess-zh".to_string());
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
    assert_eq!(
        card.pointer("/header/title/content")
            .and_then(Value::as_str),
        Some("session sess-zh · 空闲")
    );
    let actions = card
        .pointer("/elements/2/actions")
        .and_then(Value::as_array)
        .expect("actions array");
    assert_eq!(
        actions[0].pointer("/text/content").and_then(Value::as_str),
        Some("显示截图")
    );
    assert_eq!(
        actions[1].pointer("/text/content").and_then(Value::as_str),
        Some("选择只读终端入口")
    );
    assert_eq!(
        actions[2].pointer("/text/content").and_then(Value::as_str),
        Some("私发可写链接")
    );
}

#[test]
fn build_terminal_link_choice_card_lists_multiple_ticketed_candidates() {
    let session = make_session("sess-ro");
    let candidate_a = TerminalLinkCandidate::new(
        "当前地址 / Current",
        "Current address",
        build_terminal_url_with_ticket(
            "http://proxy.example.com/s/sess-ro",
            "sess-ro",
            terminal_auth::TerminalPermission::ReadOnly,
        ),
    );
    let candidate_b = TerminalLinkCandidate::new(
        "推荐地址 / Recommended",
        "Recommended address",
        build_terminal_url_with_ticket(
            "http://lan.example.com/s/sess-ro",
            "sess-ro",
            terminal_auth::TerminalPermission::ReadOnly,
        ),
    );
    let card: Value = serde_json::from_str(&build_terminal_link_choice_card(
        &session,
        "选择只读终端入口",
        "Choose read-only terminal entry",
        "如果某个入口打不开，请返回后选择其他入口。",
        "If one entry does not open, go back and choose another.",
        &[candidate_a, candidate_b],
    ))
    .expect("valid card json");
    let actions = card
        .pointer("/elements/2/actions")
        .and_then(Value::as_array)
        .expect("actions array");
    assert_eq!(actions.len(), 2);
    assert_eq!(
        actions[0].pointer("/type").and_then(Value::as_str),
        Some("primary")
    );
    assert_eq!(
        actions[1].pointer("/type").and_then(Value::as_str),
        Some("default")
    );
    for action in actions {
        let url = action
            .pointer("/multi_url/url")
            .and_then(Value::as_str)
            .expect("multi_url url");
        assert!(
            url.contains("ticket="),
            "terminal links should carry single-use tickets: {url}"
        );
    }
    let body = card
        .pointer("/elements/0/content")
        .and_then(Value::as_str)
        .expect("markdown body");
    assert!(
        body.contains("choose another"),
        "choice copy should explain how to switch entries"
    );
}

#[test]
fn terminal_link_candidate_labels_cover_host_types() {
    assert_eq!(
        terminal_link_candidate_labels("100.64.12.34", true),
        (
            "推荐地址 100.64.12.34".to_string(),
            "Recommended address 100.64.12.34".to_string(),
        )
    );
    assert_eq!(
        terminal_link_candidate_labels("100.64.12.34", false),
        (
            "Tailscale 地址 100.64.12.34".to_string(),
            "Tailscale address 100.64.12.34".to_string(),
        )
    );
    assert_eq!(
        terminal_link_candidate_labels("192.168.31.20", false),
        (
            "局域网地址 192.168.31.20".to_string(),
            "LAN address 192.168.31.20".to_string(),
        )
    );
    assert_eq!(
        terminal_link_candidate_labels("example.com", false),
        (
            "候选地址 example.com".to_string(),
            "Candidate address example.com".to_string(),
        )
    );
    assert_eq!(
        terminal_link_candidate_labels("localhost", false),
        ("本机地址".to_string(), "Localhost".to_string())
    );
}

#[test]
fn terminal_link_choice_candidates_share_permission_logic_and_append_current_address() {
    let mut session = make_session("sess-choice");
    session.terminal_url = Some("http://old.example.com:9000/s/sess-choice".to_string());
    let candidate_hosts = vec![
        "100.64.12.34".to_string(),
        "192.168.31.20".to_string(),
        "192.168.31.20".to_string(),
        "localhost".to_string(),
    ];

    let read_only = terminal_link_choice_candidates(
        &session,
        terminal_auth::TerminalPermission::ReadOnly,
        &candidate_hosts,
        8800,
    );
    let write = terminal_link_choice_candidates(
        &session,
        terminal_auth::TerminalPermission::Write,
        &candidate_hosts,
        8800,
    );

    assert_eq!(read_only.len(), 4);
    assert_eq!(
        read_only
            .iter()
            .map(|candidate| candidate.label_en.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Recommended address 100.64.12.34",
            "LAN address 192.168.31.20",
            "Localhost",
            "Current address",
        ]
    );
    assert!(
        read_only
            .last()
            .map(|candidate| candidate.url.contains("old.example.com"))
            .unwrap_or(false)
    );
    assert_ne!(read_only[0].url, write[0].url);
    let mut used = terminal_auth::UsedTickets::default();
    let read_only_ticket = url::Url::parse(&read_only[0].url)
        .expect("read-only url")
        .query_pairs()
        .find_map(|(key, value)| {
            (key == terminal_auth::TICKET_QUERY_PARAM).then_some(value.into_owned())
        })
        .expect("read-only ticket");
    let read_only_payload =
        terminal_auth::verify_terminal_ticket(&read_only_ticket, "sess-choice", &mut used)
            .expect("read-only ticket should verify");
    assert_eq!(
        read_only_payload.permission,
        terminal_auth::TerminalPermission::ReadOnly
    );
    let write_ticket = url::Url::parse(&write[0].url)
        .expect("write url")
        .query_pairs()
        .find_map(|(key, value)| {
            (key == terminal_auth::TICKET_QUERY_PARAM).then_some(value.into_owned())
        })
        .expect("write ticket");
    let write_payload = terminal_auth::verify_terminal_ticket(
        &write_ticket,
        "sess-choice",
        &mut terminal_auth::UsedTickets::default(),
    )
    .expect("write ticket should verify");
    assert_eq!(
        write_payload.permission,
        terminal_auth::TerminalPermission::Write
    );
    assert!(
        read_only
            .iter()
            .all(|candidate| candidate.url.contains("ticket=")),
        "every candidate should be ticketed"
    );
}

#[test]
fn build_streaming_card_uses_starting_template() {
    let mut session = make_session("sess-starting");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "starting")).expect("valid card json");
    assert_eq!(
        card.pointer("/header/template").and_then(Value::as_str),
        Some("yellow")
    );
}

#[test]
fn build_streaming_card_adds_term_action_rows_in_screenshot_mode() {
    let mut session = make_session("sess-11");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.terminal_url = Some("http://127.0.0.1:9000/?token=abc".to_string());
    session.current_screen = Some("hello".to_string());
    session.display_mode = Some(DisplayMode::Screenshot);
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
    assert_eq!(
        card.pointer("/elements/5/actions/0/value/action")
            .and_then(Value::as_str),
        Some("term_action")
    );
    assert_eq!(
        card.pointer("/elements/6/actions/5/value/key")
            .and_then(Value::as_str),
        Some("half_page_down")
    );
    assert_eq!(
        card.pointer("/elements/6/actions/5/text/i18n_content/zh_cn")
            .and_then(Value::as_str),
        Some("下半页")
    );
    assert_eq!(
        card.pointer("/elements/6/actions/5/text/i18n_content/en_us")
            .and_then(Value::as_str),
        Some("Half Pg Down")
    );
    assert_eq!(
        card.pointer("/elements/3/actions/0/value/action")
            .and_then(Value::as_str),
        Some("refresh_screenshot")
    );
}

#[test]
fn refresh_screenshot_in_hidden_mode_returns_info_toast() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let app_id = "app-refresh";
        let bot = BotConfig {
            name: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_owner".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        let state = make_state(
            temp_paths("refresh-hidden"),
            HashMap::from([(app_id.to_string(), bot)]),
        );
        let mut session = make_session("sess-refresh");
        session.lark_app_id = app_id.to_string();
        session.closed_at = None;
        session.status = SessionStatus::Active;
        session.display_mode = Some(DisplayMode::Hidden);
        session.stream_card_nonce = Some("nonce-refresh".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let payload = serde_json::json!({
            "operator": { "open_id": "ou_other" },
            "action": { "value": {
                "action": "refresh_screenshot",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.unwrap_or_else(|| "codex".to_string()),
            } }
        });

        let response = handle_lark_card_action_payload(&state, app_id, payload)
            .await
            .expect("handler response");
        assert_eq!(
            response.0.pointer("/toast/type").and_then(Value::as_str),
            Some("info")
        );
        assert_eq!(
            response.0.pointer("/toast/content").and_then(Value::as_str),
            Some("show screenshot first")
        );
    });
}

#[test]
fn toggle_display_returns_a_screenshot_card_response() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);
        let app_id = "app-toggle";
        let bot = BotConfig {
            name: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: Vec::new(),
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        let state = make_state(
            temp_paths("toggle-display"),
            HashMap::from([(app_id.to_string(), bot)]),
        );
        let mut session = make_session("sess-toggle");
        session.lark_app_id = app_id.to_string();
        session.closed_at = None;
        session.status = SessionStatus::Active;
        session.display_mode = Some(DisplayMode::Hidden);
        session.current_image_key = None;
        session.stream_card_nonce = Some("nonce-toggle".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": { "value": {
                "action": "toggle_display",
                "root_id": session.root_message_id,
                "session_id": session.session_id,
                "cli_id": session.cli_id.unwrap_or_else(|| "codex".to_string()),
            } }
        });

        let response = handle_lark_card_action_payload(&state, app_id, payload)
            .await
            .expect("handler response");
        assert_eq!(
            response.0.pointer("/toast/type").and_then(Value::as_str),
            Some("success")
        );
        assert_eq!(
            response.0.pointer("/card/type").and_then(Value::as_str),
            Some("raw")
        );
        assert_eq!(
            response
                .0
                .pointer("/card/data/elements/2/content")
                .and_then(Value::as_str),
            Some("waiting for screenshot")
        );
        assert_eq!(
            response
                .0
                .pointer("/card/data/elements/3/actions/0/text/content")
                .and_then(Value::as_str),
            Some("Refresh screenshot")
        );
        let stored = state
            .sessions
            .lock()
            .await
            .get(&session.session_id)
            .cloned()
            .expect("stored session");
        assert_eq!(stored.display_mode, Some(DisplayMode::Screenshot));
    });
}

#[test]
fn build_streaming_card_shows_retry_button_when_limit_is_ready() {
    let mut session = make_session("sess-limit");
    session.last_screen_status = Some(ScreenStatus::Limited);
    session.usage_limit = Some(CliUsageLimitState {
        limited: true,
        kind: CliUsageLimitKind::Usage,
        retry_at_ms: 42,
        retry_label: "3:15 PM".to_string(),
        retry_ready: true,
    });
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "limited")).expect("valid card json");
    assert_eq!(
        card.pointer("/header/template").and_then(Value::as_str),
        Some("green")
    );
    assert_eq!(
        card.pointer("/elements/2/content").and_then(Value::as_str),
        Some("limit cleared. Retry is ready after 3:15 PM.")
    );
    let found_retry = card
        .get("elements")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|element| element.get("actions").and_then(Value::as_array))
        .flatten()
        .any(|action| {
            action.pointer("/value/action").and_then(Value::as_str) == Some("retry_last_task")
        });
    assert!(found_retry);
}

#[test]
fn build_streaming_card_renders_image_in_screenshot_mode_when_available() {
    let mut session = make_session("sess-image");
    session.display_mode = Some(DisplayMode::Screenshot);
    session.current_image_key = Some("img_v2_abc".to_string());
    session.current_screen = Some("should not render".to_string());
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
    assert_eq!(
        card.pointer("/elements/2/img_key").and_then(Value::as_str),
        Some("img_v2_abc")
    );
}

#[test]
fn build_streaming_card_adopted_shows_disconnect_without_restart() {
    let mut session = make_session("sess-adopted-stream");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.adopted_from = Some(AdoptedFrom {
        zellij_session: Some("my-session".to_string()),
        zellij_pane_id: Some("pane-1".to_string()),
        original_cli_pid: 9999,
        cwd: "/home/user".to_string(),
        ..Default::default()
    });
    let card: Value =
        serde_json::from_str(&build_streaming_card(&session, "idle")).expect("valid card json");
    // Collect action names for presence check (order may vary depending on token availability)
    let action_names: Vec<&str> = card
        .get("elements")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|element| element.get("actions").and_then(Value::as_array))
        .flatten()
        .filter_map(|a| a.pointer("/value/action").and_then(Value::as_str))
        .collect();
    // Restart must NOT appear for adopted sessions
    assert!(
        !action_names.contains(&"restart"),
        "restart should not appear for adopted session"
    );
    // Close action must appear (via Disconnect label)
    assert!(
        action_names.contains(&"close"),
        "close action should be present: {action_names:?}"
    );
    // Verify the close action shows "Disconnect" text
    let close_text = card
        .get("elements")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|element| element.get("actions").and_then(Value::as_array))
        .flatten()
        .find(|a| a.pointer("/value/action").and_then(Value::as_str) == Some("close"))
        .and_then(|a| a.pointer("/text/content").and_then(Value::as_str));
    assert_eq!(close_text, Some("Disconnect"));
}

#[test]
fn render_streaming_card_body_hides_content_in_hidden_mode() {
    let mut session = make_session("sess-10");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.current_screen = Some("secret output".to_string());
    session.display_mode = Some(DisplayMode::Hidden);
    assert_eq!(render_streaming_card_body(&session), "[screen hidden]");

    session.display_mode = Some(DisplayMode::Screenshot);
    assert_eq!(render_streaming_card_body(&session), "secret output");
}

#[test]
fn build_export_text_reply_handles_empty_and_truncates_long_output() {
    let mut session = make_session("sess-12");
    assert_eq!(build_export_text_reply(&session), "(no output yet)");

    session.current_screen = Some(format!("{}\n{}\n", "a".repeat(2000), "b".repeat(2000)));
    let body = build_export_text_reply(&session);
    assert!(body.starts_with(&"a".repeat(2000)));
    assert!(body.contains("..."));
    assert!(body.len() <= 3504);
}

#[test]
fn worker_ready_display_mode_command_only_resends_screenshot_mode() {
    let mut hidden = make_session("sess-hidden");
    hidden.status = SessionStatus::Active;
    hidden.closed_at = None;
    hidden.display_mode = Some(DisplayMode::Hidden);
    assert_eq!(worker_ready_display_mode_command(&hidden), None);

    let mut screenshot = make_session("sess-shot");
    screenshot.status = SessionStatus::Active;
    screenshot.closed_at = None;
    screenshot.display_mode = Some(DisplayMode::Screenshot);
    assert_eq!(
        worker_ready_display_mode_command(&screenshot),
        Some(DaemonToWorker::SetDisplayMode {
            mode: DisplayMode::Screenshot
        })
    );
}

#[test]
fn build_report_post_content_mentions_owner_and_preserves_line_breaks() {
    let mut session = make_session("sess-report");
    session.owner_open_id = Some("ou_owner".to_string());
    let payload = build_report_post_content(&session, "first line\nsecond line");
    let value: Value = serde_json::from_str(&payload).expect("json");
    let content = value["zh_cn"]["content"].as_array().expect("content");
    assert_eq!(content.len(), 2);
    assert_eq!(content[0].as_array().unwrap()[0]["tag"], "at");
    assert_eq!(content[0].as_array().unwrap()[0]["user_id"], "ou_owner");
    assert_eq!(content[0].as_array().unwrap()[2]["text"], "first line");
    assert_eq!(content[1].as_array().unwrap()[0]["text"], "second line");
}

#[test]
fn session_summary_carries_last_screen_status() {
    let mut session = make_session("sess-summary");
    session.current_screen = Some("hello".to_string());
    session.last_screen_status = Some(ScreenStatus::Limited);
    session.quote_target_id = Some("om_user".to_string());
    session.pending_response_card_id = Some("om_pending".to_string());
    session.pending_response_card_state = Some(PendingResponseCardState::Open);
    session.last_patched_response_card_id = Some("om_done".to_string());
    session.last_final_output_turn_id = Some("turn-9".to_string());

    let summary = SessionSummary::from(&session);
    assert_eq!(summary.current_screen.as_deref(), Some("hello"));
    assert_eq!(summary.last_screen_status, Some(ScreenStatus::Limited));
    assert_eq!(summary.quote_target_id.as_deref(), Some("om_user"));
    assert_eq!(
        summary.pending_response_card_id.as_deref(),
        Some("om_pending")
    );
    assert_eq!(
        summary.pending_response_card_state,
        Some(PendingResponseCardState::Open)
    );
    assert_eq!(
        summary.last_patched_response_card_id.as_deref(),
        Some("om_done")
    );
    assert_eq!(summary.last_final_output_turn_id.as_deref(), Some("turn-9"));
}

#[test]
fn build_quote_hint_includes_text_when_parent_id_differs() {
    let hint = prompt::build_quote_hint(Some("quoted-1"), "msg-1", SessionScope::Thread, "root-1");
    assert!(hint.contains("quoted-1"));
}

#[test]
fn build_quote_hint_empty_when_no_parent_id() {
    assert_eq!(
        prompt::build_quote_hint(None, "msg-1", SessionScope::Thread, "root-1"),
        ""
    );
}

#[test]
fn build_quote_hint_empty_when_parent_id_matches_message_id() {
    assert_eq!(
        prompt::build_quote_hint(Some("msg-1"), "msg-1", SessionScope::Thread, "root-1"),
        ""
    );
}

#[test]
fn build_follow_up_content_wraps_in_user_message() {
    let mentions: Vec<LarkEventMention> = vec![];
    let opts = prompt::FollowUpContentOptions {
        session_id: "test-session",
        sender_open_id: None,
        sender_type: None,
        mentions: &mentions,
        cli_id: "codex",
        locale: None,
    };
    let result = prompt::build_follow_up_content("hello", &opts);
    assert!(result.contains("<user_message>"));
    assert!(result.contains("hello"));
}

#[test]
fn build_follow_up_content_includes_mentions() {
    let mentions = vec![LarkEventMention {
        key: "ou_123".to_string(),
        name: "Alice".to_string(),
    }];
    let opts = prompt::FollowUpContentOptions {
        session_id: "test-session",
        sender_open_id: None,
        sender_type: None,
        mentions: &mentions,
        cli_id: "codex",
        locale: None,
    };
    let result = prompt::build_follow_up_content("hi", &opts);
    assert!(result.contains("<mentions>"));
    assert!(result.contains("Alice"));
    assert!(result.contains("ou_123"));
}

#[test]
fn build_follow_up_content_skips_beam_response_contract_for_mira() {
    let mentions: Vec<LarkEventMention> = vec![];
    let opts = prompt::FollowUpContentOptions {
        session_id: "test-session",
        sender_open_id: None,
        sender_type: None,
        mentions: &mentions,
        cli_id: "mira",
        locale: None,
    };
    let result = prompt::build_follow_up_content("hi", &opts);
    assert!(!result.contains("beam_response_contract"));
}
