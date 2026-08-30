use crate::cli_commands::{
    BotInfoEntry, active_sessions, apply_daemon_backend_choice, bin_candidates_for_cli_id,
    build_send_request, default_cli_args_for_cli_id, discover_session_id_from_pid,
    format_bot_info_entries_for_cli, format_duration, parse_cgroup_slice_input,
    parse_cli_args_input, parse_mention, parse_migrate_flags, resolve_allowed_users,
    setup_backup_file, setup_prompts_cgroup_slice, validate_simulate_lark_message_args,
};
use crate::{Cli, Command, SendArgs, SessionCommand, SimulateCommand};
use beam_core::{BeamPaths, SessionStatus, SessionSummary};
use chrono::Utc;
use clap::{FromArgMatches, Parser};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Parse SendArgs from CLI-like args in tests without deriving Parser on SendArgs.
/// Callers pass args as if run from the shell, e.g. `["beam", "send", "--mention-back", "hello"]`.
fn parse_send_args<I, T>(args: I) -> Result<SendArgs, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    use clap::Args;
    let raw: Vec<std::ffi::OsString> = args.into_iter().map(|a| a.into()).collect();
    // Skip binary name (index 0) and optional "send" subcommand (index 1).
    let start = if raw.len() > 1 && raw[1] == "send" {
        2
    } else {
        1
    };
    // try_get_matches_from strips the first arg (binary name), so prepend a dummy.
    let mut input: Vec<std::ffi::OsString> = vec!["_".into()];
    if start < raw.len() {
        input.extend(raw[start..].iter().cloned());
    }
    let cmd = clap::Command::new("send");
    let cmd = SendArgs::augment_args(cmd);
    let matches = cmd.try_get_matches_from(&input)?;
    SendArgs::from_arg_matches(&matches).map_err(|e| e.format(&mut clap::Command::new("send")))
}

fn temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("beam-cli-{label}-{nanos}-{}", std::process::id()))
}

fn paths_for(root: &Path) -> BeamPaths {
    BeamPaths::from_root(root)
}

#[test]
fn clap_accepts_top_level_list_and_bots_default_list_args() {
    let cli = Cli::try_parse_from(["beam", "list", "--plain"]).expect("parse list");
    assert!(matches!(cli.command, Command::List { plain: true }));

    let cli = Cli::try_parse_from(["beam", "ls"]).expect("parse ls alias");
    assert!(matches!(cli.command, Command::List { plain: false }));

    let cli = Cli::try_parse_from(["beam", "attach", "abc123"]).expect("parse attach");
    assert!(matches!(
        cli.command,
        Command::Attach { session_id } if session_id == "abc123"
    ));

    let cli =
        Cli::try_parse_from(["beam", "session", "attach", "abc123"]).expect("parse session attach");
    assert!(matches!(
        cli.command,
        Command::Session { command: SessionCommand::Attach { session_id } } if session_id == "abc123"
    ));

    let cli = Cli::try_parse_from(["beam", "bots", "--session-id", "sid-1"])
        .expect("parse bots default list");
    match cli.command {
        Command::Bots { args } => assert_eq!(args, ["--session-id", "sid-1"]),
        other => panic!("unexpected command: {other:?}"),
    }

    let cli = Cli::try_parse_from(["beam", "bots", "list", "--session-id", "sid-1"])
        .expect("parse bots list");
    match cli.command {
        Command::Bots { args } => assert_eq!(args, ["list", "--session-id", "sid-1"]),
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn parse_migrate_flags_accepts_dry_run_and_force() {
    let flags = parse_migrate_flags(&["--dry-run".to_string(), "--force".to_string()])
        .expect("parse migrate flags");
    assert!(flags.dry_run);
    assert!(flags.force);
}

#[test]
fn setup_backup_file_writes_bak_copy() {
    let root = temp_root("backup");
    let file = root.join("bots.json");
    fs::create_dir_all(&root).unwrap();
    fs::write(&file, "[]\n").unwrap();
    let backup = setup_backup_file(&file)
        .expect("backup file")
        .expect("backup path");
    assert!(backup.ends_with("bots.json.bak"));
    assert_eq!(fs::read_to_string(&backup).unwrap(), "[]\n");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn setup_applies_daemon_backend_to_existing_config() {
    let root = temp_root("cfg-herdr");
    let cfg = root.join("config.toml");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        &cfg,
        "[daemon]\nworking_dirs = [\"~\"]\n\n[web]\nhost = \"0.0.0.0\"\nproxy_base_port = 8800\n",
    )
    .unwrap();
    let changed = apply_daemon_backend_choice(&cfg, beam_core::BackendKind::Herdr).unwrap();
    assert!(changed);
    let raw = fs::read_to_string(&cfg).unwrap();
    assert!(raw.contains("backend = \"herdr\""));
    assert!(raw.contains("working_dirs"));
    assert!(raw.contains("proxy_base_port = 8800"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn setup_keeps_existing_herdr_backend_unchanged() {
    let root = temp_root("cfg-herdr-existing");
    let cfg = root.join("config.toml");
    fs::create_dir_all(&root).unwrap();
    fs::write(&cfg, "[daemon]\nbackend = \"herdr\"\n").unwrap();
    let changed = apply_daemon_backend_choice(&cfg, beam_core::BackendKind::Herdr).unwrap();
    assert!(!changed);
    assert_eq!(
        fs::read_to_string(&cfg).unwrap(),
        "[daemon]\nbackend = \"herdr\"\n"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn setup_replaces_explicit_zellij_backend_with_herdr() {
    let root = temp_root("cfg-replace");
    let cfg = root.join("config.toml");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        &cfg,
        "[daemon]\nbackend = \"zellij\"\nworking_dirs = [\"~\"]\n",
    )
    .unwrap();
    let changed = apply_daemon_backend_choice(&cfg, beam_core::BackendKind::Herdr).unwrap();
    assert!(changed);
    let raw = fs::read_to_string(&cfg).unwrap();
    assert!(raw.contains("backend = \"herdr\""));
    assert!(!raw.contains("backend = \"zellij\""));
    assert!(raw.contains("working_dirs"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn setup_adds_daemon_section_when_missing() {
    let root = temp_root("cfg-nodaemon");
    let cfg = root.join("config.toml");
    fs::create_dir_all(&root).unwrap();
    fs::write(&cfg, "[web]\nhost = \"0.0.0.0\"\n").unwrap();
    let changed = apply_daemon_backend_choice(&cfg, beam_core::BackendKind::Herdr).unwrap();
    assert!(changed);
    let raw = fs::read_to_string(&cfg).unwrap();
    assert!(raw.contains("[daemon]"));
    assert!(raw.contains("backend = \"herdr\""));
    assert!(raw.contains("host = \"0.0.0.0\""));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn setup_zellij_choice_leaves_existing_config_untouched() {
    let root = temp_root("cfg-zellij");
    let cfg = root.join("config.toml");
    fs::create_dir_all(&root).unwrap();
    fs::write(&cfg, "[daemon]\nworking_dirs = [\"~\"]\n").unwrap();
    let changed = apply_daemon_backend_choice(&cfg, beam_core::BackendKind::Zellij).unwrap();
    assert!(!changed);
    assert_eq!(
        fs::read_to_string(&cfg).unwrap(),
        "[daemon]\nworking_dirs = [\"~\"]\n"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn setup_defaults_traex_cli_args() {
    assert_eq!(default_cli_args_for_cli_id("traex"), vec!["-y".to_string()]);
    assert_eq!(
        default_cli_args_for_cli_id("codex"),
        vec![
            "--dangerously-bypass-approvals-and-sandbox".to_string(),
            "--no-alt-screen".to_string(),
        ]
    );
    assert_eq!(
        default_cli_args_for_cli_id("grok"),
        vec![
            "--always-approve".to_string(),
            "--no-alt-screen".to_string(),
        ]
    );
    assert_eq!(
        default_cli_args_for_cli_id("kimi"),
        vec!["--yolo".to_string()]
    );
    assert_eq!(
        default_cli_args_for_cli_id("gemini"),
        vec!["--yolo".to_string()]
    );
}

#[test]
fn setup_cli_args_prompt_keeps_defaults_on_empty_input() {
    let defaults = vec!["--safe-mode".to_string()];
    assert_eq!(parse_cli_args_input("", &defaults), defaults);
    assert_eq!(parse_cli_args_input("  ", &defaults), defaults);
}

#[test]
fn setup_cli_args_prompt_accepts_override_and_clear() {
    let defaults = vec!["--safe-mode".to_string()];
    assert_eq!(
        parse_cli_args_input("--model fast --verbose", &defaults),
        vec!["--model", "fast", "--verbose"]
    );
    assert!(parse_cli_args_input("clear", &defaults).is_empty());
    assert!(parse_cli_args_input("none", &defaults).is_empty());
}

#[test]
fn setup_cgroup_slice_prompt_is_linux_only() {
    assert_eq!(setup_prompts_cgroup_slice(), cfg!(target_os = "linux"));
}

#[test]
fn setup_cgroup_slice_defaults_to_empty() {
    assert_eq!(parse_cgroup_slice_input(""), None);
    assert_eq!(parse_cgroup_slice_input("  "), None);
    assert_eq!(parse_cgroup_slice_input("none"), None);
    assert_eq!(parse_cgroup_slice_input("clear"), None);
    assert_eq!(
        parse_cgroup_slice_input("cgtproxy-gateway.slice"),
        Some("cgtproxy-gateway.slice".to_string())
    );
}

#[test]
fn discover_session_id_prefers_explicit_env_value() {
    let root = temp_root("env");
    let paths = paths_for(&root);
    let found = discover_session_id_from_pid(&paths, 1234, Some("session-from-env"))
        .expect("discover from env");
    assert_eq!(found, "session-from-env");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn discover_session_id_reads_current_pid_marker() {
    let root = temp_root("marker");
    let paths = paths_for(&root);
    fs::create_dir_all(paths.cli_pid_markers_dir()).expect("create marker dir");
    let pid = std::process::id();
    fs::write(
        paths.cli_pid_markers_dir().join(pid.to_string()),
        "session-from-marker\n",
    )
    .expect("write marker");

    let found = discover_session_id_from_pid(&paths, pid, None).expect("discover from marker");
    assert_eq!(found, "session-from-marker");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn discover_session_id_errors_without_env_or_marker() {
    let root = temp_root("missing");
    let paths = paths_for(&root);
    let err = discover_session_id_from_pid(&paths, std::process::id(), None)
        .expect_err("missing marker should fail");
    assert!(
        err.to_string()
            .contains("could not infer session id from BEAM_SESSION_ID or cli pid markers")
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn format_bot_info_entries_matches_cli_fallback_shape() {
    let entries = vec![
        BotInfoEntry {
            lark_app_id: "cli_self".to_string(),
            bot_open_id: Some("ou_self".to_string()),
            bot_name: Some("Self Bot".to_string()),
            cli_id: "claude".to_string(),
        },
        BotInfoEntry {
            lark_app_id: "cli_peer".to_string(),
            bot_open_id: Some("ou_peer".to_string()),
            bot_name: None,
            cli_id: "codex".to_string(),
        },
        BotInfoEntry {
            lark_app_id: "cli_missing_open_id".to_string(),
            bot_open_id: None,
            bot_name: Some("Hidden".to_string()),
            cli_id: "gemini".to_string(),
        },
    ];

    let out = format_bot_info_entries_for_cli(&entries, "cli_self");
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].name, "Self Bot");
    assert_eq!(out[0].open_id, "ou_self");
    assert!(out[0].is_self);
    assert!(out[0].mentionable);
    assert_eq!(out[0].mention_source, "self");
    assert_eq!(out[1].name, "codex");
    assert!(!out[1].is_self);
    assert!(!out[1].mentionable);
    assert_eq!(out[1].mention_source, "fallback");
}

fn make_summary(id: &str, status: SessionStatus, hours_ago: i64) -> SessionSummary {
    let ts = Utc::now() - chrono::Duration::hours(hours_ago);
    SessionSummary {
        session_id: id.to_string(),
        title: format!("session {}", id),
        status,
        chat_type: None,
        quote_target_id: None,
        cli_id: Some("test-cli".to_string()),
        cli_bin: Some("test-bin".to_string()),
        cli_args: vec![],
        working_dir: Some("/home/user/project".to_string()),
        worker_pid: Some(12345),
        terminal_url: None,
        read_only_token: None,
        write_token: None,
        created_at: ts,
        stream_card_nonce: None,
        current_screen: None,
        last_screen_status: None,
        usage_limit: None,
        current_image_key: None,
        tui_prompt_card_id: None,
        tui_prompt_options: vec![],
        tui_prompt_multi_select: None,
        tui_toggled_indices: vec![],
        pending_response_card_id: None,
        pending_response_card_state: None,
        last_patched_response_card_id: None,
        last_final_output_turn_id: None,
        last_final_output: None,
        adopted_from: None,
        agent_attention: None,
        worker_unresponsive: false,
    }
}

#[test]
fn active_sessions_filters_out_closed() {
    let items = vec![
        make_summary("active-1", SessionStatus::Active, 1),
        make_summary("closed-1", SessionStatus::Closed, 2),
        make_summary("active-2", SessionStatus::Active, 3),
    ];
    let active = active_sessions(&items);
    assert_eq!(active.len(), 2);
    assert!(active.iter().all(|s| s.status == SessionStatus::Active));
}

#[test]
fn active_sessions_sorts_newest_first() {
    let items = vec![
        make_summary("old", SessionStatus::Active, 10),
        make_summary("new", SessionStatus::Active, 1),
        make_summary("mid", SessionStatus::Active, 5),
    ];
    let active = active_sessions(&items);
    assert_eq!(active[0].session_id, "new");
    assert_eq!(active[1].session_id, "mid");
    assert_eq!(active[2].session_id, "old");
}

#[test]
fn active_sessions_returns_empty_when_none_active() {
    let items = vec![
        make_summary("closed-1", SessionStatus::Closed, 1),
        make_summary("closed-2", SessionStatus::Closed, 2),
    ];
    let active = active_sessions(&items);
    assert!(active.is_empty());
}

#[test]
fn format_duration_outputs_human_readable() {
    assert_eq!(format_duration(0), "0s");
    assert_eq!(format_duration(30_000), "30s");
    assert_eq!(format_duration(120_000), "2m");
    assert_eq!(format_duration(3_600_000), "1h0m");
    assert_eq!(format_duration(3_660_000), "1h1m");
    assert_eq!(format_duration(86_400_000), "1d0h");
    assert_eq!(format_duration(90_000_000), "1d1h");
}

#[test]
fn bin_candidates_for_cli_id_returns_candidates() {
    // opencode has multiple candidates
    assert_eq!(
        bin_candidates_for_cli_id("opencode"),
        Some(&["opencode-cli", "opencode"][..])
    );
    // single-candidate CLIs
    assert_eq!(bin_candidates_for_cli_id("codex"), Some(&["codex"][..]));
    assert_eq!(
        bin_candidates_for_cli_id("claude-code"),
        Some(&["claude"][..])
    );
    assert_eq!(bin_candidates_for_cli_id("antigravity"), Some(&["agy"][..]));
}

#[test]
fn bin_candidates_for_cli_id_returns_none_for_unknown_id() {
    assert_eq!(bin_candidates_for_cli_id("unknown-cli"), None);
}

#[test]
fn resolve_allowed_users_empty_input_no_owner_returns_empty() {
    let result = resolve_allowed_users("", None);
    assert!(result.is_empty());
}

#[test]
fn resolve_allowed_users_empty_input_with_owner_inserts_owner() {
    let result = resolve_allowed_users("", Some("ou_owner"));
    assert_eq!(result, vec!["ou_owner"]);
}

#[test]
fn resolve_allowed_users_parses_comma_separated_and_inserts_owner() {
    let result = resolve_allowed_users("ou_peer, ou_colleague", Some("ou_owner"));
    assert_eq!(result.len(), 3);
    assert!(result.contains(&"ou_owner".to_string()));
    assert!(result.contains(&"ou_peer".to_string()));
    assert!(result.contains(&"ou_colleague".to_string()));
}

#[test]
fn resolve_allowed_users_does_not_duplicate_owner() {
    let result = resolve_allowed_users("ou_owner, ou_peer", Some("ou_owner"));
    assert_eq!(result.len(), 2);
    assert!(result.contains(&"ou_owner".to_string()));
    assert!(result.contains(&"ou_peer".to_string()));
}

#[test]
fn resolve_allowed_users_parses_without_owner_open_id() {
    let result = resolve_allowed_users("ou_peer, ou_colleague", None);
    assert_eq!(result, vec!["ou_peer", "ou_colleague"]);
}

#[test]
fn resolve_allowed_users_trims_whitespace_and_filters_empty() {
    let result = resolve_allowed_users("  ou_a  ,  , ou_b , ", None);
    assert_eq!(result, vec!["ou_a", "ou_b"]);
}

// ---- beam send CLI parse tests ----

#[test]
fn send_parse_mention_back_basic() {
    let args = parse_send_args(["beam", "send", "--mention-back", "hello world"]);
    let args = args.expect("parse send");
    assert!(args.mention_back);
    assert!(!args.no_mention);
    assert!(args.mention.is_empty());
}

#[test]
fn send_parse_no_mention() {
    let args = parse_send_args(["beam", "send", "--no-mention", "quiet log"]);
    let args = args.expect("parse no-mention");
    assert!(args.no_mention);
    assert!(!args.mention_back);
}

#[test]
fn send_parse_multiple_mentions() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention",
        "ou_abc:Alice",
        "--mention",
        "ou_def",
        "hey folks",
    ]);
    let args = args.expect("parse multi mention");
    assert_eq!(args.mention.len(), 2);
    assert_eq!(args.mention[0], "ou_abc:Alice");
    assert_eq!(args.mention[1], "ou_def");
}

#[test]
fn send_parse_content_file() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "--content-file",
        "/tmp/msg.txt",
    ]);
    let args = args.expect("parse content-file");
    assert!(args.content_file.is_some());
    assert_eq!(
        args.content_file.as_ref().unwrap().to_str().unwrap(),
        "/tmp/msg.txt"
    );
}

#[test]
fn send_parse_files_and_images() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--files",
        "a.pdf",
        "--files",
        "b.txt",
        "--images",
        "img1.png",
        "--image",
        "img2.jpg",
    ]);
    let args = args.expect("parse files/images");
    assert_eq!(args.files.len(), 2);
    assert_eq!(args.images.len(), 2);
}

#[test]
fn send_parse_file_alias() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--file",
        "single.pdf",
    ]);
    let args = args.expect("parse --file alias");
    assert_eq!(args.files.len(), 1);
    assert_eq!(args.files[0], "single.pdf");
}

#[test]
fn send_parse_targeting_flags() {
    let args = parse_send_args([
        "beam",
        "send",
        "--no-mention",
        "--top-level",
        "--chat-id",
        "oc_test123",
        "--into",
        "om_thread1",
        "--quote",
        "om_ref1",
        "--no-quote",
        "text",
    ]);
    let args = args.expect("parse targeting");
    assert!(args.top_level);
    assert_eq!(args.chat_id.as_deref(), Some("oc_test123"));
    assert_eq!(args.into.as_deref(), Some("om_thread1"));
    assert_eq!(args.quote.as_deref(), Some("om_ref1"));
    assert!(args.no_quote);
}

#[test]
fn send_parse_attention_with_kind() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--attention=decision",
    ]);
    let args = args.expect("parse attention=decision");
    assert_eq!(args.attention.as_deref(), Some("decision"));
}

#[test]
fn send_parse_attention_default() {
    let args = parse_send_args(["beam", "send", "--mention-back", "hello", "--attention"]);
    let args = args.expect("parse attention default");
    assert_eq!(args.attention.as_deref(), Some("blocked"));
}

#[test]
fn send_parse_voice_flag() {
    let args = parse_send_args(["beam", "send", "--no-mention", "test", "--voice"]);
    let args = args.expect("parse voice");
    assert!(args.voice);
}

#[test]
fn send_parse_anyway_flag() {
    let args = parse_send_args(["beam", "send", "--no-mention", "test", "--anyway"]);
    let args = args.expect("parse anyway");
    assert!(args.anyway);
}

#[test]
fn send_parse_card_text_noop() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--card",
        "--text",
    ]);
    let args = args.expect("parse card/text");
    assert!(args.card);
    assert!(args.text);
}

#[test]
fn send_build_rejects_no_mention_decision() {
    let args = parse_send_args(["beam", "send", "hello"]).expect("parse");
    let err = build_send_request(args).expect_err("no mention should fail");
    assert!(
        err.to_string().contains("no mention decision") || err.to_string().contains("must choose")
    );
}

#[test]
fn send_build_rejects_invalid_attention_kind() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--attention=invalid_kind",
    ])
    .expect("parse");
    let err = build_send_request(args).expect_err("invalid attention should fail");
    assert!(err.to_string().contains("invalid attention kind"));
}

// --- attention usage constraint tests (botmux parity) ---

#[test]
fn send_build_rejects_attention_with_top_level() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--attention",
        "--top-level",
    ])
    .expect("parse");
    let err = build_send_request(args).expect_err("attention + top-level should fail");
    assert!(
        err.to_string()
            .contains("--attention cannot be combined with --top-level")
    );
}

#[test]
fn send_build_rejects_attention_with_chat_id() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--attention",
        "--chat-id",
        "oc_test123",
    ])
    .expect("parse");
    let err = build_send_request(args).expect_err("attention + chat-id should fail");
    assert!(
        err.to_string()
            .contains("--attention cannot be combined with --chat-id")
    );
}

#[test]
fn send_build_rejects_attention_with_into() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--attention",
        "--into",
        "om_test123",
    ])
    .expect("parse");
    let err = build_send_request(args).expect_err("attention + into should fail");
    assert!(
        err.to_string()
            .contains("--attention cannot be combined with --into")
    );
}

#[test]
fn send_build_rejects_attention_with_voice() {
    let args = parse_send_args([
        "beam",
        "send",
        "--mention-back",
        "hello",
        "--attention",
        "--voice",
    ])
    .expect("parse");
    let err = build_send_request(args).expect_err("attention + voice should fail");
    assert!(
        err.to_string()
            .contains("--attention cannot be combined with --voice")
    );
}

#[test]
fn send_build_passes_voice_to_daemon() {
    let args =
        parse_send_args(["beam", "send", "--mention-back", "hello", "--voice"]).expect("parse");
    let req = build_send_request(args).expect("voice should not fail at CLI; daemon rejects it");
    assert!(req.voice, "voice flag must be passed to daemon");
    assert!(req.mention_back);
    assert_eq!(req.content, "hello");
}

#[test]
fn send_build_rejects_no_mention_with_mention() {
    let args = parse_send_args([
        "beam",
        "send",
        "--no-mention",
        "--mention",
        "ou_abc:Alice",
        "hello",
    ])
    .expect("parse");
    let err = build_send_request(args).expect_err("conflict should fail");
    assert!(
        err.to_string().contains("incompatible") || err.to_string().contains("cannot be combined")
    );
}

#[test]
fn send_build_rejects_no_mention_with_mention_back() {
    let args = parse_send_args(["beam", "send", "--no-mention", "--mention-back", "hello"])
        .expect("parse");
    let err = build_send_request(args).expect_err("conflict should fail");
    assert!(
        err.to_string().contains("incompatible") || err.to_string().contains("cannot be combined")
    );
}

#[test]
fn send_parse_mention_valid_formats() {
    let t1 = parse_mention("ou_123:Alice").expect("name parse");
    assert_eq!(t1.open_id, "ou_123");
    assert_eq!(t1.name.as_deref(), Some("Alice"));

    let t2 = parse_mention("ou_456").expect("bare parse");
    assert_eq!(t2.open_id, "ou_456");
    assert_eq!(t2.name, None);
}

#[test]
fn send_parse_mention_rejects_empty() {
    assert!(parse_mention("").is_err());
    assert!(parse_mention(":Name").is_err());
}

// ---- beam simulate lark-message parse tests ----

#[test]
fn simulate_lark_message_parse_success() {
    let cli = Cli::try_parse_from([
        "beam",
        "simulate",
        "lark-message",
        "--session",
        "sid-abc",
        "--sender",
        "ou_123",
        "hello world",
    ])
    .expect("parse simulate lark-message");
    match cli.command {
        Command::Simulate {
            command: SimulateCommand::LarkMessage(args),
        } => {
            assert_eq!(args.session, "sid-abc");
            assert_eq!(args.sender, "ou_123");
            assert_eq!(args.text, "hello world");
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn simulate_lark_message_missing_session_fails() {
    let err = Cli::try_parse_from([
        "beam",
        "simulate",
        "lark-message",
        "--sender",
        "ou_123",
        "hello",
    ])
    .expect_err("missing --session should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("--session") || msg.contains("session"),
        "error should mention session: {msg}"
    );
}

#[test]
fn simulate_lark_message_missing_sender_fails() {
    let err = Cli::try_parse_from([
        "beam",
        "simulate",
        "lark-message",
        "--session",
        "sid-abc",
        "hello",
    ])
    .expect_err("missing --sender should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("--sender") || msg.contains("sender"),
        "error should mention sender: {msg}"
    );
}

// ---- validate_simulate_lark_message_args tests ----

#[test]
fn validate_simulate_args_rejects_blank_session() {
    let err =
        validate_simulate_lark_message_args("  ", "ou_123", "hello").expect_err("blank session");
    assert!(err.to_string().contains("--session"));
}

#[test]
fn validate_simulate_args_rejects_blank_sender() {
    let err = validate_simulate_lark_message_args("sid", "  ", "hello").expect_err("blank sender");
    assert!(err.to_string().contains("--sender"));
}

#[test]
fn validate_simulate_args_rejects_blank_text() {
    let err = validate_simulate_lark_message_args("sid", "ou_123", "   ").expect_err("blank text");
    assert!(err.to_string().contains("text"));
}

#[test]
fn validate_simulate_args_accepts_valid_input() {
    validate_simulate_lark_message_args("sid", "ou_123", "hello world")
        .expect("valid args should pass");
}

#[test]
fn validate_simulate_args_accepts_trimmed_non_empty() {
    // Trailing/leading whitespace is ok as long as trimmed value is non-empty.
    validate_simulate_lark_message_args("  sid  ", "  ou_123  ", "  hello  ")
        .expect("trimmed non-empty should pass");
}

#[test]
fn validate_simulate_args_preserves_original_text() {
    // The helper takes &str and doesn't mutate — original text is preserved.
    let text = "  hello world  ";
    validate_simulate_lark_message_args("sid", "ou_123", text).expect("valid args should pass");
    // Original binding is untouched.
    assert_eq!(text, "  hello world  ");
}
