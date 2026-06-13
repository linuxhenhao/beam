use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::{os::unix::process::CommandExt, process::Command as StdCommand};

use anyhow::{Context, Result, bail};
use beam_core::{
    AdoptCandidate, AdoptTmuxSessionRequest, ApiHealth, BackendType, BotConfig, BeamPaths,
    CreateSessionRequest, DaemonRuntimeState, FinalOutputRequest, RestartSessionRequest,
    ResumeSessionRequest, Session, SessionInputRequest, SessionStatus, SessionSummary,
};
use clap::{Args, Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

mod ask_hook;
mod autostart;
mod global_config;
mod hook_setup;
mod register_app;
mod workflow_cli;

#[derive(Debug, Parser)]
#[command(name = "beam", version, about = "Rust core runtime for beam")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Start,
    Stop,
    Restart,
    Logs,
    Status,
    #[command(name = "list", alias = "ls")]
    List {
        #[arg(long)]
        plain: bool,
    },
    Attach {
        session_id: String,
    },
    Workflow {
        #[command(subcommand)]
        command: workflow_cli::WorkflowCommand,
    },
    Send {
        content: Option<String>,
    },
    Bots {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    Setup,
    Migrate {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Dashboard,
    Autostart {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Schedule {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Report {
        content: Option<String>,
    },
    Ask {
        content: Option<String>,
    },
    Hook {
        cli_id: Option<String>,
    },
    Voice {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Lang {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    #[command(hide = true, name = "__daemon")]
    InternalDaemon,
    #[command(hide = true, name = "__worker")]
    InternalWorker(WorkerArgs),
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    Create(SessionCreateArgs),
    List,
    Attach {
        session_id: String,
    },
    Input(SessionInputArgs),
    Refresh {
        session_id: String,
    },
    Restart {
        session_id: String,
        #[arg(long, default_value = "")]
        prompt: String,
    },
    Resume {
        session_id: String,
        #[arg(long, default_value = "")]
        prompt: String,
    },
    AdoptTmux(SessionAdoptTmuxArgs),
    DiscoverTmux,
    Close {
        session_id: String,
    },
    Info {
        session_id: String,
    },
}

#[derive(Debug, Args)]
struct SessionCreateArgs {
    #[arg(long)]
    title: String,
    #[arg(long)]
    cli_id: String,
    #[arg(long)]
    cli_bin: String,
    #[arg(long)]
    working_dir: String,
    #[arg(long, default_value = "")]
    prompt: String,
    #[arg(long, default_value = "tmux")]
    backend_type: String,
    #[arg(trailing_var_arg = true)]
    cli_args: Vec<String>,
}

#[derive(Debug, Args)]
struct SessionAdoptTmuxArgs {
    #[arg(long)]
    tmux_target: String,
    #[arg(long)]
    cli_id: String,
    #[arg(long)]
    cli_bin: String,
    #[arg(long)]
    title: Option<String>,
}

#[derive(Debug, Args)]
struct SessionInputArgs {
    session_id: String,
    content: String,
    #[arg(long)]
    raw: bool,
}

#[derive(Debug, Args)]
struct WorkerArgs {
    #[arg(long)]
    init_path: PathBuf,
}

fn find_runtime(paths: &BeamPaths) -> Result<DaemonRuntimeState> {
    let raw = std::fs::read(paths.runtime_state_json()).with_context(|| {
        format!(
            "daemon state not found at {}",
            paths.runtime_state_json().display()
        )
    })?;
    Ok(serde_json::from_slice(&raw)?)
}

async fn api_client(paths: &BeamPaths) -> Result<(Client, String)> {
    let runtime = find_runtime(paths)?;
    Ok((Client::new(), format!("http://{}", runtime.api_addr)))
}

async fn post_ask(paths: &BeamPaths, body: &serde_json::Value) -> Result<serde_json::Value> {
    let (client, base) = api_client(paths).await?;
    let resp = client
        .post(format!("{}/asks", base))
        .json(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        bail!("{}", resp.text().await.unwrap_or_default());
    }
    Ok(resp.json().await?)
}

fn discover_session_id(paths: &BeamPaths) -> Result<String> {
    let env_session_id = std::env::var("BEAM_SESSION_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    discover_session_id_from_pid(paths, std::process::id(), env_session_id.as_deref())
}

fn discover_session_id_from_pid(
    paths: &BeamPaths,
    mut pid: u32,
    env_session_id: Option<&str>,
) -> Result<String> {
    if let Some(value) = env_session_id {
        return Ok(value.to_string());
    }

    let markers = paths.cli_pid_markers_dir();
    loop {
        let candidate = markers.join(pid.to_string());
        if let Ok(raw) = std::fs::read_to_string(&candidate) {
            let session_id = raw.trim().to_string();
            if !session_id.is_empty() {
                return Ok(session_id);
            }
        }

        let stat_path = format!("/proc/{}/stat", pid);
        let stat = match std::fs::read_to_string(stat_path) {
            Ok(stat) => stat,
            Err(_) => break,
        };
        let end = match stat.rfind(')') {
            Some(end) => end,
            None => break,
        };
        let rest = stat.get(end + 2..).unwrap_or_default();
        let mut parts = rest.split_whitespace();
        let _state = parts.next();
        let ppid = match parts.next().and_then(|value| value.parse::<u32>().ok()) {
            Some(ppid) if ppid > 1 && ppid != pid => ppid,
            _ => break,
        };
        pid = ppid;
    }

    bail!("could not infer session id from BEAM_SESSION_ID or cli pid markers")
}

fn read_send_content(content: Option<String>) -> Result<String> {
    if let Some(content) = content {
        return Ok(content);
    }
    let mut body = String::new();
    use std::io::Read;
    std::io::stdin().read_to_string(&mut body)?;
    let body = body.trim_end().to_string();
    if body.is_empty() {
        bail!("send content is empty");
    }
    Ok(body)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduleRecord {
    #[serde(rename = "scheduleId")]
    schedule_id: String,
    content: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    status: String,
}

fn read_schedule_records(paths: &BeamPaths) -> Result<Vec<ScheduleRecord>> {
    match std::fs::read_to_string(paths.schedules_json()) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

fn write_schedule_records(paths: &BeamPaths, records: &[ScheduleRecord]) -> Result<()> {
    if let Some(parent) = paths.schedules_json().parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        paths.schedules_json(),
        serde_json::to_string_pretty(records)? + "\n",
    )?;
    Ok(())
}

fn cmd_schedule(args: Vec<String>, paths: &BeamPaths) -> Result<()> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("list");
    let rest = if args.is_empty() { &[][..] } else { &args[1..] };
    let mut records = read_schedule_records(paths)?;

    let find_id =
        |rest: &[String]| -> Option<String> { rest.iter().find(|s| !s.starts_with('-')).cloned() };

    match sub {
        "list" | "ls" => {
            if records.is_empty() {
                println!("暂无定时任务。");
                return Ok(());
            }
            for task in &records {
                println!(
                    "[{}] {} | {} | {}",
                    task.schedule_id, task.status, task.created_at, task.content
                );
            }
        }
        "add" => {
            let positional: Vec<String> = rest
                .iter()
                .filter(|arg| !arg.starts_with("--"))
                .cloned()
                .collect();
            if positional.is_empty() {
                anyhow::bail!("Usage: beam schedule add <schedule> <prompt>");
            }
            let raw_schedule = positional[0].clone();
            let prompt = positional
                .iter()
                .skip(1)
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            let parsed = beam_core::parse_schedule(&raw_schedule)
                .map_err(|err| anyhow::anyhow!(err))?;
            let content = if prompt.is_empty() {
                if let Some(natural) = beam_core::parse_natural_schedule(&positional.join(" "))
                {
                    natural.prompt
                } else {
                    raw_schedule.clone()
                }
            } else {
                prompt
            };
            let task = ScheduleRecord {
                schedule_id: format!(
                    "sched-{}-{}",
                    chrono::Utc::now().timestamp_millis(),
                    std::process::id()
                ),
                content,
                created_at: chrono::Utc::now().to_rfc3339(),
                status: "active".to_string(),
            };
            println!("parsed schedule: {}", parsed.display);
            println!("{}", serde_json::to_string_pretty(&task)?);
            records.push(task);
            write_schedule_records(paths, &records)?;
        }
        "remove" | "rm" | "delete" | "del" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule remove <scheduleId>");
            };
            let before = records.len();
            records.retain(|task| task.schedule_id != id);
            if records.len() == before {
                anyhow::bail!("未找到任务 {}", id);
            }
            write_schedule_records(paths, &records)?;
            println!("已删除任务 {}", id);
        }
        "pause" | "disable" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule pause <scheduleId>");
            };
            let mut found = false;
            for task in &mut records {
                if task.schedule_id == id {
                    task.status = "paused".to_string();
                    found = true;
                }
            }
            if !found {
                anyhow::bail!("未找到任务 {}", id);
            }
            write_schedule_records(paths, &records)?;
            println!("已暂停任务 {}", id);
        }
        "resume" | "enable" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule resume <scheduleId>");
            };
            let mut found = false;
            for task in &mut records {
                if task.schedule_id == id {
                    task.status = "active".to_string();
                    found = true;
                }
            }
            if !found {
                anyhow::bail!("未找到任务 {}", id);
            }
            write_schedule_records(paths, &records)?;
            println!("已恢复任务 {}", id);
        }
        "run" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule run <scheduleId>");
            };
            let Some(task) = records.iter().find(|task| task.schedule_id == id) else {
                anyhow::bail!("未找到任务 {}", id);
            };
            println!(
                "{{\"scheduleId\":\"{}\",\"content\":\"{}\",\"status\":\"{}\",\"run\":\"now\"}}",
                task.schedule_id,
                task.content.replace('"', "\\\""),
                task.status,
            );
        }
        "logs" => {
            let Some(id) = find_id(rest) else {
                anyhow::bail!("Usage: beam schedule logs <scheduleId>");
            };
            let dir = paths.schedules_output_dir().join(&id);
            if !dir.exists() {
                println!("无日志：{}", dir.display());
            } else {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    println!("{}", entry.path().display());
                }
            }
        }
        _ => {
            anyhow::bail!("未知子命令: {}", sub);
        }
    }

    Ok(())
}

fn current_exe() -> Result<PathBuf> {
    Ok(std::env::current_exe().context("failed to locate current executable")?)
}

fn daemon_log_path(paths: &BeamPaths) -> PathBuf {
    paths.daemon_log()
}

fn load_bots(paths: &BeamPaths) -> Result<Vec<BotConfig>> {
    match std::fs::read_to_string(paths.bots_json()) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

fn load_sessions_from_store(
    paths: &BeamPaths,
) -> Result<std::collections::HashMap<String, Session>> {
    match std::fs::read(paths.session_store_json()) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(std::collections::HashMap::new())
        }
        Err(err) => Err(err.into()),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BotInfoEntry {
    #[serde(rename = "larkAppId")]
    lark_app_id: String,
    #[serde(rename = "botOpenId")]
    bot_open_id: Option<String>,
    #[serde(rename = "botName")]
    bot_name: Option<String>,
    #[serde(rename = "cliId")]
    cli_id: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct BotListEntry {
    name: String,
    #[serde(rename = "openId")]
    open_id: String,
    #[serde(rename = "isSelf")]
    is_self: bool,
    source: &'static str,
    #[serde(rename = "larkAppId")]
    lark_app_id: String,
    #[serde(rename = "workflowBot")]
    workflow_bot: String,
    capability: Option<String>,
    #[serde(rename = "hasTeamRole")]
    has_team_role: bool,
    mentionable: bool,
    #[serde(rename = "mentionSource")]
    mention_source: &'static str,
}

fn load_bot_info_entries(paths: &BeamPaths) -> Result<Vec<BotInfoEntry>> {
    let path = paths.root().join("bots-info.json");
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

fn format_bot_info_entries_for_cli(
    entries: &[BotInfoEntry],
    current_lark_app_id: &str,
) -> Vec<BotListEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            let open_id = entry.bot_open_id.as_ref()?.clone();
            let is_self = entry.lark_app_id == current_lark_app_id;
            Some(BotListEntry {
                name: entry
                    .bot_name
                    .clone()
                    .unwrap_or_else(|| entry.cli_id.clone()),
                open_id,
                is_self,
                source: "configured",
                lark_app_id: entry.lark_app_id.clone(),
                workflow_bot: entry.lark_app_id.clone(),
                capability: None,
                has_team_role: false,
                mentionable: is_self,
                mention_source: if is_self { "self" } else { "fallback" },
            })
        })
        .collect()
}

fn arg_value(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find_map(|pair| (pair[0] == name).then(|| pair[1].clone()))
}

fn cmd_bots(args: Vec<String>, paths: &BeamPaths) -> Result<()> {
    let (sub, rest) = match args.first() {
        Some(first) if !first.starts_with('-') => (first.as_str(), &args[1..]),
        _ => ("list", args.as_slice()),
    };
    if sub != "list" && sub != "ls" {
        bail!("用法: beam bots list [--session-id ID]");
    }

    let session_id = match arg_value(rest, "--session-id") {
        Some(value) => value,
        None => discover_session_id(paths).map_err(|_| {
            anyhow::anyhow!(
                "无法推断 session-id。请在 Lark 话题内的 CLI 会话中运行，或传 --session-id <id>。"
            )
        })?,
    };
    let sessions = load_sessions_from_store(paths)?;
    let session = sessions
        .get(&session_id)
        .with_context(|| format!("未找到 session {}", session_id))?;
    if session.lark_app_id.is_empty() {
        bail!("session {} 缺少 larkAppId", session_id);
    }

    let bots =
        format_bot_info_entries_for_cli(&load_bot_info_entries(paths)?, &session.lark_app_id);
    let out = serde_json::json!({
        "sessionId": session_id,
        "chatId": session.chat_id,
        "bots": bots,
        "total": bots.len(),
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn daemon_state_is_live(paths: &BeamPaths) -> bool {
    let runtime = match find_runtime(paths) {
        Ok(runtime) => runtime,
        Err(_) => return false,
    };
    let addr = match runtime.api_addr.parse::<std::net::SocketAddr>() {
        Ok(addr) => addr,
        Err(_) => return false,
    };
    std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(300)).is_ok()
}

fn spawn_background_daemon(exe: &Path, paths: &BeamPaths) -> Result<()> {
    if paths.runtime_state_json().exists() {
        if daemon_state_is_live(paths) {
            bail!("daemon appears to be running already");
        }
        let _ = std::fs::remove_file(paths.runtime_state_json());
    }
    std::fs::create_dir_all(paths.logs_dir())?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(daemon_log_path(paths))?;

    let mut cmd = StdCommand::new(exe);
    let child = unsafe {
        cmd.arg("__daemon")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            })
            .spawn()
    }
    .context("failed to spawn background daemon")?;

    let _ = child.id();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BotInfoEntry, Cli, Command, SessionCommand, active_sessions, discover_session_id_from_pid,
        format_bot_info_entries_for_cli, format_duration, parse_backend_type, parse_migrate_flags,
        setup_backup_file,
    };
    use beam_core::{BackendType, BeamPaths, SessionStatus, SessionSummary};
    use chrono::Utc;
    use clap::Parser;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "beam-cli-{label}-{nanos}-{}",
            std::process::id()
        ))
    }

    fn paths_for(root: &Path) -> BeamPaths {
        BeamPaths::from_root(root)
    }

    #[test]
    fn parse_backend_type_accepts_supported_values() {
        assert_eq!(
            parse_backend_type("tmux").expect("tmux backend"),
            beam_core::BackendType::Tmux
        );
        assert_eq!(
            parse_backend_type("pty").expect("pty backend"),
            beam_core::BackendType::Pty
        );
        assert_eq!(
            parse_backend_type("zellij").expect("zellij backend"),
            beam_core::BackendType::Zellij
        );
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

        let cli = Cli::try_parse_from(["beam", "session", "attach", "abc123"])
            .expect("parse session attach");
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
            backend_type: BackendType::Pty,
            working_dir: Some("/home/user/project".to_string()),
            worker_pid: Some(12345),
            terminal_url: None,
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
}

async fn wait_for_health(paths: &BeamPaths) -> Result<ApiHealth> {
    let client = Client::new();
    for _ in 0..40 {
        if let Ok(runtime) = find_runtime(paths) {
            let url = format!("http://{}/health", runtime.api_addr);
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    return Ok(resp.json::<ApiHealth>().await?);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    bail!("daemon did not become healthy in time")
}

fn format_duration(ms: i64) -> String {
    let seconds = ms / 1000;
    if seconds < 60 {
        return format!("{}s", seconds);
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{}m", minutes);
    }
    let hours = minutes / 60;
    if hours < 24 {
        return format!("{}h{}m", hours, minutes % 60);
    }
    let days = hours / 24;
    format!("{}d{}h", days, hours % 24)
}

fn active_sessions(items: &[SessionSummary]) -> Vec<SessionSummary> {
    let mut v: Vec<SessionSummary> = items
        .iter()
        .filter(|s| s.status == SessionStatus::Active)
        .cloned()
        .collect();
    v.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    v
}

fn truncate(s: &str, max: usize) -> &str {
    if s.chars().count() <= max {
        s
    } else {
        &s[..s.floor_char_boundary(max)]
    }
}

fn shorten_home(path: &str) -> String {
    if let Ok(home) = std::env::var("HOME") {
        if path.starts_with(&home) {
            return path.replacen(&home, "~", 1);
        }
    }
    path.to_string()
}

fn print_sessions(items: &[SessionSummary]) {
    let active = active_sessions(items);
    if active.is_empty() {
        println!("没有活跃会话。");
        return;
    }

    // column widths
    let id_w = 10usize;
    let title_w = 28usize;
    let dir_w = 28usize;
    let pid_w = 8usize;
    let uptime_w = 8usize;
    let status_w = 7usize;

    // header
    let hdr = format!(
        "{:id_w$} │ {:title_w$} │ {:dir_w$} │ {:pid_w$} │ {:>uptime_w$} │ {:status_w$}",
        "id", "title", "working dir", "pid", "uptime", "status",
    );
    let sep = "─".repeat(hdr.chars().count());
    println!("{}", sep);
    println!("{}", hdr);
    println!("{}", sep);

    let now_ms = chrono::Utc::now().timestamp_millis();
    for item in &active {
        let id = truncate(&item.session_id, id_w);
        let title = truncate(&item.title, title_w);
        let work_dir = shorten_home(item.working_dir.as_deref().unwrap_or("-"));
        let dir = truncate(&work_dir, dir_w);
        let pid = item
            .worker_pid
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string());
        let uptime_ms = now_ms - item.created_at.timestamp_millis();
        let uptime = format_duration(uptime_ms.max(0));
        let status = match item.status {
            SessionStatus::Active => "active",
            SessionStatus::Closed => "closed",
        };

        println!(
            "{:id_w$} │ {:title_w$} │ {:dir_w$} │ {:>pid_w$} │ {:>uptime_w$} │ {:>status_w$}",
            id, title, dir, pid, uptime, status
        );
    }

    println!("{}", sep);
    println!("共 {} 个活跃会话", active.len());
}

async fn fetch_sessions(client: &Client, base: &str) -> Result<Vec<SessionSummary>> {
    let resp = client.get(format!("{}/sessions", base)).send().await?;
    if !resp.status().is_success() {
        bail!("{}", resp.text().await.unwrap_or_default());
    }
    Ok(resp.json::<Vec<SessionSummary>>().await?)
}

fn session_attach_target(session: &SessionSummary) -> String {
    let fallback = format!(
        "bmx-{}",
        &session.session_id[..8.min(session.session_id.len())]
    );
    match session.backend_type {
        BackendType::Tmux => session
            .adopted_from
            .as_ref()
            .and_then(|adopted| adopted.tmux_target.clone())
            .unwrap_or(fallback),
        BackendType::Zellij => session
            .adopted_from
            .as_ref()
            .and_then(|adopted| adopted.zellij_session.clone())
            .unwrap_or(fallback),
        BackendType::Pty => fallback,
    }
}

fn resolve_session_prefix(items: &[SessionSummary], prefix: &str) -> Result<SessionSummary> {
    let matches = items
        .iter()
        .filter(|session| session.session_id.starts_with(prefix))
        .cloned()
        .collect::<Vec<_>>();
    match matches.len() {
        0 => bail!("未找到匹配 \"{}\" 的活跃会话", prefix),
        1 => Ok(matches[0].clone()),
        _ => {
            eprintln!(
                "\"{}\" 匹配了 {} 个会话，请提供更长的 ID 前缀：",
                prefix,
                matches.len()
            );
            for session in matches {
                eprintln!("  {}  {}", truncate(&session.session_id, 12), session.title);
            }
            bail!("session id 前缀不唯一")
        }
    }
}

fn attach_session(session: &SessionSummary) -> Result<()> {
    let target = session_attach_target(session);
    match session.backend_type {
        BackendType::Tmux => {
            let status = StdCommand::new("tmux")
                .args(["attach-session", "-t", &target])
                .status()
                .context("failed to run tmux attach-session")?;
            if !status.success() {
                bail!("tmux attach-session failed for {}", target);
            }
        }
        BackendType::Zellij => {
            let status = StdCommand::new("zellij")
                .args(["attach", &target])
                .status()
                .context("failed to run zellij attach")?;
            if !status.success() {
                bail!("zellij attach failed for {}", target);
            }
        }
        BackendType::Pty => bail!("session {} 使用 pty 后端，不能 attach", session.session_id),
    }
    Ok(())
}

async fn cmd_attach(client: &Client, base: &str, session_id: &str) -> Result<()> {
    let items = fetch_sessions(client, base).await?;
    let session = resolve_session_prefix(&items, session_id)?;
    attach_session(&session)
}

fn print_tmux_candidates(items: &[AdoptCandidate]) {
    for item in items {
        println!(
            "{}  pid={}  cwd={}  {}",
            item.tmux_target, item.pid, item.cwd, item.title
        );
    }
}

fn parse_backend_type(raw: &str) -> Result<BackendType> {
    match raw {
        "tmux" => Ok(BackendType::Tmux),
        "pty" => Ok(BackendType::Pty),
        "zellij" => Ok(BackendType::Zellij),
        _ => bail!("unsupported backend_type: {}", raw),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    // Suppress "JoinHandle polled after completion" panic from tokio 1.52.3.
    // This is a known tokio bug triggered when a spawned task's JoinHandle is
    // dropped after the task has already completed. The panic is harmless but
    // pollutes the logs. We downgrade it to a tracing warning instead.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = info.to_string();
        if msg.contains("JoinHandle polled after completion") {
            tracing::warn!("JoinHandle dropped after task completion (known tokio 1.52 issue)");
            return;
        }
        default_hook(info);
    }));

    let cli = Cli::parse();
    match cli.command {
        Command::InternalDaemon => {
            let exe = current_exe()?;
            beam_daemon::run(
                BeamPaths::discover()?,
                beam_daemon::RunOptions { worker_exe: exe },
            )
            .await?;
        }
        Command::InternalWorker(args) => {
            beam_worker::run_from_init_path(&args.init_path).await?;
        }
        other => {
            let paths = BeamPaths::discover()?;
            match other {
                Command::Start => {
                    let exe = current_exe()?;
                    spawn_background_daemon(&exe, &paths)?;
                    let health = wait_for_health(&paths).await?;
                    println!("daemon pid={} started_at={}", health.pid, health.started_at);
                }
                Command::Stop => {
                    let (client, base) = api_client(&paths).await?;
                    client.post(format!("{}/shutdown", base)).send().await?;
                    println!("shutdown requested");
                }
                Command::Restart => {
                    if paths.runtime_state_json().exists() {
                        let (client, base) = api_client(&paths).await?;
                        let _ = client.post(format!("{}/shutdown", base)).send().await?;
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    }
                    let exe = current_exe()?;
                    spawn_background_daemon(&exe, &paths)?;
                    let health = wait_for_health(&paths).await?;
                    println!("daemon pid={} started_at={}", health.pid, health.started_at);
                }
                Command::Logs => {
                    let log = daemon_log_path(&paths);
                    let content = std::fs::read_to_string(&log).unwrap_or_default();
                    print!("{}", content);
                }
                Command::Status => {
                    let health = wait_for_health(&paths).await?;
                    println!("daemon pid={} started_at={}", health.pid, health.started_at);
                }
                Command::List { plain: _plain } => {
                    let (client, base) = api_client(&paths).await?;
                    let items = fetch_sessions(&client, &base).await?;
                    print_sessions(&items);
                }
                Command::Attach { session_id } => {
                    let (client, base) = api_client(&paths).await?;
                    cmd_attach(&client, &base, &session_id).await?;
                }
                Command::Setup => {
                    cmd_setup(&paths).await?;
                }
                Command::Migrate { args } => {
                    cmd_migrate(&paths, args).await?;
                }
                Command::Autostart { args } => {
                    cmd_autostart(&paths, args)?;
                }
                Command::Dashboard => {
                    let runtime = find_runtime(&paths)?;
                    let base = format!("http://{}", runtime.api_addr);
                    let auth = reqwest::Client::new()
                        .get(format!("{}/api/auth", base))
                        .send()
                        .await?;
                    let auth_json: serde_json::Value = auth.json().await?;
                    let url = format!(
                        "{}{}",
                        base,
                        auth_json
                            .get("loginPath")
                            .and_then(|v| v.as_str())
                            .unwrap_or("/dashboard/")
                    );
                    println!("Opening dashboard: {}", url);
                    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
                }
                Command::Schedule { args } => {
                    cmd_schedule(args, &paths)?;
                }
                Command::Report { content } => {
                    let body = read_send_content(content)?;
                    let session_id = discover_session_id(&paths)?;
                    let (client, base) = api_client(&paths).await?;
                    let resp = client
                        .post(format!("{}/sessions/{}/report", base, session_id))
                        .json(&serde_json::json!({ "content": body }))
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        bail!("{}", resp.text().await.unwrap_or_default());
                    }
                    let out: serde_json::Value = resp.json().await?;
                    println!("{}", serde_json::to_string_pretty(&out)?);
                }
                Command::Ask { content } => {
                    let body = read_send_content(content)?;
                    let session_id = discover_session_id(&paths)?;
                    let (client, base) = api_client(&paths).await?;
                    let resp = client
                        .post(format!("{}/sessions/{}/input", base, session_id))
                        .json(&SessionInputRequest {
                            content: body,
                            raw: false,
                        })
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        bail!("{}", resp.text().await.unwrap_or_default());
                    }
                    println!("ask sent to session {}", session_id);
                }
                Command::Hook { cli_id } => {
                    cmd_hook(cli_id, &paths).await?;
                }
                Command::Voice { args } => cmd_voice(args)?,
                Command::Lang { args } => cmd_lang(args)?,
                Command::Workflow { command } => {
                    workflow_cli::handle(command, &paths).await?;
                }
                Command::Send { content } => {
                    let body = read_send_content(content)?;
                    let session_id = discover_session_id(&paths)?;
                    let (client, base) = api_client(&paths).await?;
                    let resp = client
                        .post(format!("{}/sessions/{}/final-output", base, session_id))
                        .json(&FinalOutputRequest { content: body })
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        bail!("{}", resp.text().await.unwrap_or_default());
                    }
                    println!("final output accepted");
                }
                Command::Bots { args } => cmd_bots(args, &paths)?,
                Command::Session { command } => {
                    let (client, base) = api_client(&paths).await?;
                    match command {
                        SessionCommand::Create(args) => {
                            let backend_type = parse_backend_type(&args.backend_type)?;
                            let resp = client
                                .post(format!("{}/sessions", base))
                                .json(&CreateSessionRequest {
                                    title: args.title,
                                    cli_id: args.cli_id,
                                    cli_bin: args.cli_bin,
                                    cli_args: args.cli_args,
                                    working_dir: args.working_dir,
                                    prompt: args.prompt,
                                    backend_type: Some(backend_type),
                                })
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let session = resp.json::<SessionSummary>().await?;
                            println!("{}", serde_json::to_string_pretty(&session)?);
                        }
                        SessionCommand::List => {
                            let items = fetch_sessions(&client, &base).await?;
                            print_sessions(&items);
                        }
                        SessionCommand::Attach { session_id } => {
                            cmd_attach(&client, &base, &session_id).await?;
                        }
                        SessionCommand::Input(args) => {
                            let resp = client
                                .post(format!("{}/sessions/{}/input", base, args.session_id))
                                .json(&SessionInputRequest {
                                    content: args.content,
                                    raw: args.raw,
                                })
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            println!("input accepted");
                        }
                        SessionCommand::Refresh { session_id } => {
                            let resp = client
                                .post(format!("{}/sessions/{}/refresh", base, session_id))
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            println!("refresh requested");
                        }
                        SessionCommand::Restart { session_id, prompt } => {
                            let resp = client
                                .post(format!("{}/sessions/{}/restart", base, session_id))
                                .json(&RestartSessionRequest { prompt })
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            println!("restart requested");
                        }
                        SessionCommand::Resume { session_id, prompt } => {
                            let resp = client
                                .post(format!("{}/sessions/{}/resume", base, session_id))
                                .json(&ResumeSessionRequest { prompt })
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let session = resp.json::<SessionSummary>().await?;
                            println!("{}", serde_json::to_string_pretty(&session)?);
                        }
                        SessionCommand::AdoptTmux(args) => {
                            let resp = client
                                .post(format!("{}/adopt/tmux", base))
                                .json(&AdoptTmuxSessionRequest {
                                    title: args.title,
                                    tmux_target: args.tmux_target,
                                    cli_id: args.cli_id,
                                    cli_bin: args.cli_bin,
                                })
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let session = resp.json::<SessionSummary>().await?;
                            println!("{}", serde_json::to_string_pretty(&session)?);
                        }
                        SessionCommand::DiscoverTmux => {
                            let resp = client.get(format!("{}/adopt/tmux", base)).send().await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let items = resp.json::<Vec<AdoptCandidate>>().await?;
                            print_tmux_candidates(&items);
                        }
                        SessionCommand::Close { session_id } => {
                            let resp = client
                                .post(format!("{}/sessions/{}/close", base, session_id))
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            println!("session closed");
                        }
                        SessionCommand::Info { session_id } => {
                            let resp = client
                                .get(format!("{}/sessions/{}", base, session_id))
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let session = resp.json::<SessionSummary>().await?;
                            println!("{}", serde_json::to_string_pretty(&session)?);
                        }
                    }
                }
                Command::InternalDaemon | Command::InternalWorker(_) => unreachable!(),
            }
        }
    }

    Ok(())
}

fn setup_backup_file(path: &Path) -> Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let backup = path.with_extension(format!(
        "{}.bak",
        path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("bak")
    ));
    std::fs::copy(path, &backup)?;
    Ok(Some(backup))
}

async fn validate_setup_credentials(app_id: &str, app_secret: &str) -> Result<()> {
    if std::env::var("BEAM_SKIP_SETUP_VALIDATION")
        .ok()
        .as_deref()
        == Some("1")
    {
        println!("⚠️  已跳过远程凭证校验（BEAM_SKIP_SETUP_VALIDATION=1）。");
        return Ok(());
    }
    let client = reqwest::Client::new();
    let resp = client
        .post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
        .json(&serde_json::json!({
            "app_id": app_id,
            "app_secret": app_secret,
        }))
        .send()
        .await
        .context("failed to reach Feishu/Lark credential endpoint")?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_else(|_| serde_json::json!({}));
    if !status.is_success() {
        bail!("凭证校验失败: HTTP {}", status);
    }
    match body.get("code").and_then(|v| v.as_i64()).unwrap_or(-1) {
        0 => Ok(()),
        code => bail!(
            "凭证校验失败: code={} msg={}",
            code,
            body.get("msg").and_then(|v| v.as_str()).unwrap_or("")
        ),
    }
}

fn parse_backend_type_choice(input: &str) -> Option<BackendType> {
    match input.trim().to_lowercase().as_str() {
        "" | "-" | "default" => None,
        "tmux" => Some(BackendType::Tmux),
        "pty" => Some(BackendType::Pty),
        "zellij" => Some(BackendType::Zellij),
        _ => None,
    }
}

const CLI_CHOICES: &[(&str, &str, &str)] = &[
    ("claude-code", "Claude", "claude"),
    ("codex", "Codex", "codex"),
    ("coco", "CoCo", "coco"),
    ("gemini", "Gemini", "gemini"),
    ("opencode", "OpenCode", "opencode-cli"),
    ("hermes", "Hermes", "hermes"),
    ("antigravity", "Antigravity", "agy"),
];

fn detect_installed_clis() -> Vec<&'static (&'static str, &'static str, &'static str)> {
    CLI_CHOICES
        .iter()
        .filter(|(_, _, bin)| which_exists(bin))
        .collect()
}

fn which_exists(bin: &str) -> bool {
    std::process::Command::new("which")
        .arg(bin)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn prompt_cli_id() -> Result<String> {
    let installed = detect_installed_clis();
    if installed.is_empty() {
        println!("未检测到已安装的 CLI 工具。");
        let value = ask_line("请手动输入 CLI ID [claude-code]: ")?;
        let value = value.trim();
        if value.is_empty() {
            return Ok("claude-code".to_string());
        }
        let valid_ids: Vec<&str> = CLI_CHOICES.iter().map(|c| c.0).collect();
        if valid_ids.contains(&value) {
            return Ok(value.to_string());
        }
        println!(
            "不支持的 CLI ID \"{}\"，支持: {}",
            value,
            valid_ids.join(", ")
        );
        return Ok("claude-code".to_string());
    }

    println!("已检测到以下 CLI 工具:");
    for (i, (_id, label, bin)) in installed.iter().enumerate() {
        println!("  {}) {}  ({})", i + 1, label, bin);
    }

    let value = ask_line("CLI 适配器 [1]: ")?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(installed[0].0.to_string());
    }
    if let Ok(num) = value.parse::<usize>() {
        if num >= 1 && num <= installed.len() {
            return Ok(installed[num - 1].0.to_string());
        }
        println!("无效序号 \"{}\"，请输入 1-{}", num, installed.len());
    } else {
        let valid_ids: Vec<&str> = CLI_CHOICES.iter().map(|c| c.0).collect();
        if valid_ids.contains(&value) {
            return Ok(value.to_string());
        }
        println!(
            "不支持的 CLI ID \"{}\"，支持: {}",
            value,
            valid_ids.join(", ")
        );
    }
    Ok(installed[0].0.to_string())
}

async fn prompt_setup_bot() -> Result<BotConfig> {
    let name = ask_line("机器人名称（留空=不设）: ")?;
    let credentials = register_app::prompt_credentials().await?;
    let cli_id = prompt_cli_id()?;
    let working_dir = {
        let value = ask_line("默认工作目录 [~]: ")?;
        if value.trim().is_empty() {
            Some("~".to_string())
        } else {
            Some(value)
        }
    };
    let backend_type = {
        let value = ask_line("后端 [tmux/pty/zellij] [tmux]: ")?;
        parse_backend_type_choice(&value).or(Some(BackendType::Tmux))
    };
    let mut allowed_users = {
        let value = ask_line("允许用户（逗号分隔，留空=不限制）: ")?;
        value
            .split(',')
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect::<Vec<_>>()
    };
    if let Some(open_id) = credentials.user_open_id {
        if !allowed_users.iter().any(|item| item == &open_id) {
            allowed_users.push(open_id);
        }
    }

    Ok(BotConfig {
        name: if name.trim().is_empty() {
            None
        } else {
            Some(name)
        },
        lark_app_id: credentials.app_id,
        lark_app_secret: credentials.app_secret,
        cli_id,
        cli_bin: None,
        model: None,
        working_dir,
        backend_type,
        lark_encrypt_key: None,
        lark_verification_token: None,
        allowed_users,
        private_card: false,
        allowed_chat_groups: Vec::new(),
        chat_grants: std::collections::HashMap::new(),
        global_grants: Vec::new(),
        oncall_chats: Vec::new(),
        restrict_grant_commands: false,
        message_quota: None,
        quota_state: std::collections::HashMap::new(),
    })
}

async fn cmd_setup(paths: &BeamPaths) -> Result<()> {
    let root = paths.root();
    std::fs::create_dir_all(root)?;
    for dir in [
        paths.logs_dir(),
        paths.run_dir(),
        paths.sessions_dir(),
        paths.workflows_dir(),
        paths.workflow_runs_dir(),
        paths.state_dir(),
        paths.cli_pid_markers_dir(),
        paths.observed_bots_dir(),
        paths.schedules_output_dir(),
    ] {
        std::fs::create_dir_all(&dir)?;
    }

    let cfg = paths.config_toml();
    if !cfg.exists() {
        let backend = ask_line("默认后端类型 [tmux/pty/zellij] [tmux]: ")?;
        let backend_type = backend.trim().to_lowercase();
        let backend_type = if backend_type.is_empty() || backend_type == "tmux" {
            "tmux"
        } else if backend_type == "zellij" {
            "zellij"
        } else if backend_type == "pty" {
            "pty"
        } else {
            "tmux"
        };
        let defaults = format!(
            "[daemon]\nbackend_type = \"{}\"\nworking_dirs = [\"~\"]\n\n\
             [web]\nhost = \"0.0.0.0\"\nproxy_base_port = 8800\n\n\
             [lark]\nevent_mode = \"http\"\n",
            backend_type,
        );
        std::fs::write(&cfg, &defaults)?;
        println!("Wrote {}", cfg.display());
    } else {
        println!("Config exists: {}", cfg.display());
    }

    let bots = paths.bots_json();
    if !bots.exists() {
        std::fs::write(&bots, "[]\n")?;
        println!("Wrote {}", bots.display());
    } else {
        println!("Bots config exists: {}", bots.display());
    }

    println!("Setup complete. Data root: {}", root.display());
    println!("现有 bots 数量: {}", load_bots(paths)?.len());
    println!();

    let existing = load_bots(paths)?;
    let action = if existing.is_empty() {
        "replace".to_string()
    } else {
        println!("已检测到现有机器人配置：");
        for (i, bot) in existing.iter().enumerate() {
            println!(
                "  {}. {} ({})",
                i + 1,
                bot.name.clone().unwrap_or_else(|| bot.cli_id.clone()),
                bot.lark_app_id
            );
        }
        ask_line("操作 [replace/add/skip] [replace]: ")?
    };

    let action = action.trim().to_lowercase();
    let action = if action.is_empty() {
        "replace".to_string()
    } else {
        action
    };
    if action == "skip" {
        println!("已跳过 setup 写盘。");
        return Ok(());
    }

    let mut next_bots = if action == "add" {
        existing.clone()
    } else {
        Vec::new()
    };
    let next_bot = prompt_setup_bot().await?;
    validate_setup_credentials(&next_bot.lark_app_id, &next_bot.lark_app_secret).await?;
    next_bots.push(next_bot);

    if bots.exists() {
        if let Some(backup) = setup_backup_file(&bots)? {
            println!("旧配置已备份: {}", backup.display());
        }
    }
    std::fs::write(&bots, serde_json::to_string_pretty(&next_bots)? + "\n")?;
    println!("✅ 已写入 {}", bots.display());

    if let Err(err) = hook_setup::install_hooks() {
        eprintln!("hook install skipped: {}", err);
    } else {
        println!("Installed hook config for Claude/OpenCode.");
    }

    println!("提示：先运行 `beam start`，再用 `beam autostart enable` 注册自启。");
    Ok(())
}

fn print_lang_status() {
    let cfg = global_config::read_global_config();
    let global_lang = cfg.lang.map(|loc| loc.as_str().to_string());
    let effective = global_lang.clone().unwrap_or_else(|| "zh".to_string());
    println!(
        "Global lang: {}",
        global_lang.as_deref().unwrap_or("(unset, defaults to zh)")
    );
    println!("Effective for CLI:    {}", effective);
    println!(
        "Config file:          {}",
        global_config::global_config_path().display()
    );
}

fn cmd_lang(args: Vec<String>) -> Result<()> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("");
    if sub.is_empty() {
        print_lang_status();
        return Ok(());
    }
    if sub == "--unset" {
        global_config::set_global_locale(None)?;
        println!("✅ Cleared global lang (will default to zh).");
        println!("Run `beam restart` for changes to take effect.");
        return Ok(());
    }
    match sub {
        "zh" | "en" => {
            let locale = beam_core::i18n::Locale::from_str(sub);
            global_config::set_global_locale(Some(locale))?;
            println!("✅ Set global lang → {}.", sub);
            println!("Run `beam restart` for changes to take effect.");
            Ok(())
        }
        _ => {
            eprintln!("Unknown locale \"{}\". Supported: zh, en.", sub);
            eprintln!("Usage: beam lang [zh|en|--unset]");
            std::process::exit(1);
        }
    }
}

fn mask_secret(s: Option<&str>) -> String {
    match s {
        Some(value) if !value.is_empty() => {
            let prefix: String = value.chars().take(4).collect();
            format!("{}***", prefix)
        }
        _ => "(未设)".to_string(),
    }
}

fn ask_line(prompt: &str) -> Result<String> {
    use std::io::{self, Write};
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

fn cmd_voice(args: Vec<String>) -> Result<()> {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("");
    if sub == "status" {
        let cfg = global_config::read_global_config().voice;
        if let Some(v) = cfg {
            println!("当前语音配置（全局 ~/.beam/config.json）:");
            println!(
                "  引擎: {}",
                match v.engine {
                    Some(global_config::VoiceEngine::Openai) => "openai",
                    _ => "sami",
                }
            );
            println!("  音色: {}", v.speaker.as_deref().unwrap_or("(默认)"));
            if let Some(rate) = v.rate {
                println!("  语速: {}", rate);
            }
            if let Some(sami) = v.sami {
                println!(
                    "  SAMI: accessKey={} secretKey={} appkey={}{}{}",
                    mask_secret(sami.access_key.as_deref()),
                    mask_secret(sami.secret_key.as_deref()),
                    sami.appkey.as_deref().unwrap_or("(未设)"),
                    sami.token_url
                        .as_deref()
                        .map(|v| format!(" tokenUrl={}", v))
                        .unwrap_or_default(),
                    sami.ws_url
                        .as_deref()
                        .map(|v| format!(" wsUrl={}", v))
                        .unwrap_or_default(),
                );
            }
            if let Some(openai) = v.openai {
                println!(
                    "  OpenAI: baseUrl={} model={} apiKey={}",
                    openai.base_url.as_deref().unwrap_or("(未设)"),
                    openai.model.as_deref().unwrap_or("(未设)"),
                    mask_secret(openai.api_key.as_deref())
                );
            }
        } else {
            println!("语音功能未配置。运行 `beam voice` 配置。");
        }
        return Ok(());
    }

    if sub == "disable" || sub == "off" {
        global_config::set_global_voice(None)?;
        println!(
            "✅ 已移除全局语音配置（回复卡片不再显示「🔊 语音总结」按钮）。重启 daemon 生效。"
        );
        return Ok(());
    }

    if !sub.is_empty() && sub != "setup" {
        eprintln!("用法: beam voice [status|disable]（无参 = 交互式配置）");
        std::process::exit(1);
    }

    println!("🔊 配置语音总结（高级功能）。写入全局 ~/.beam/config.json，重启后生效。\n");
    let engine = ask_line(
        "选择 TTS 引擎  [1] SAMI（需 AK/SK/appkey）  [2] OpenAI 兼容（自带 baseUrl/key）: ",
    )?;
    let mut voice = global_config::VoiceConfig::default();
    if engine == "2" || engine.to_lowercase().contains("openai") {
        voice.engine = Some(global_config::VoiceEngine::Openai);
        let base_url = ask_line(
            "baseUrl（如 https://api.openai.com/v1，自托管如 http://127.0.0.1:8880/v1）: ",
        )?;
        let api_key = ask_line("apiKey（无则留空）: ")?;
        let model = ask_line("model（如 tts-1 / kokoro）: ")?;
        if base_url.is_empty() || model.is_empty() {
            eprintln!("❌ baseUrl 和 model 必填，未写入。");
            return Ok(());
        }
        voice.openai = Some(global_config::VoiceOpenAIConfig {
            base_url: Some(base_url),
            api_key: if api_key.is_empty() {
                None
            } else {
                Some(api_key)
            },
            model: Some(model),
        });
        let sp = ask_line("音色 voice（留空=默认 alloy）: ")?;
        if !sp.is_empty() {
            voice.speaker = Some(sp);
        }
    } else {
        voice.engine = Some(global_config::VoiceEngine::Sami);
        let access_key = ask_line("SAMI accessKey: ")?;
        let secret_key = ask_line("SAMI secretKey: ")?;
        let appkey = ask_line("SAMI appkey: ")?;
        if access_key.is_empty() || secret_key.is_empty() || appkey.is_empty() {
            eprintln!("❌ accessKey/secretKey/appkey 都必填，未写入。");
            return Ok(());
        }
        let mut sami = global_config::VoiceSamiCreds {
            access_key: Some(access_key),
            secret_key: Some(secret_key),
            appkey: Some(appkey),
            token_url: None,
            ws_url: None,
        };
        let sp = ask_line("音色 speaker（留空=默认灿灿）: ")?;
        if !sp.is_empty() {
            voice.speaker = Some(sp);
        }
        let adv = ask_line("自定义 SAMI 端点？一般不用，回车跳过 (y/N): ")?;
        if adv.to_lowercase() == "y" {
            let token_url = ask_line("tokenUrl（留空用默认）: ")?;
            let ws_url = ask_line("wsUrl（留空用默认）: ")?;
            if !token_url.is_empty() {
                sami.token_url = Some(token_url);
            }
            if !ws_url.is_empty() {
                sami.ws_url = Some(ws_url);
            }
        }
        voice.sami = Some(sami);
    }

    let rate = ask_line("语速倍率（留空=1.1）: ")?;
    if !rate.is_empty() {
        if let Ok(parsed) = rate.parse::<f64>() {
            voice.rate = Some(parsed);
        }
    }

    global_config::set_global_voice(Some(voice))?;
    println!(
        "\n✅ 已写入 voice 配置。`beam restart` 后，配了语音的机器人回复卡片底部会出现「🔊 语音总结」按钮。"
    );
    println!("   查看：`beam voice status`  关闭：`beam voice disable`");
    Ok(())
}

async fn cmd_hook(cli_id: Option<String>, _paths: &BeamPaths) -> Result<()> {
    if cli_id.as_deref().unwrap_or("").is_empty() {
        eprintln!("Usage: beam hook <cliId>");
        std::process::exit(1);
    }

    use std::io::Read;
    let mut sink = String::new();
    let _ = std::io::stdin().read_to_string(&mut sink);
    let payload: serde_json::Value = match serde_json::from_str(&sink) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let Some(cli_id) = cli_id else {
        return Ok(());
    };
    let Some(parsed) = ask_hook::parse_questions(&cli_id, &payload) else {
        return Ok(());
    };
    let session_id = match discover_session_id(&_paths) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let chat_id = match std::env::var("BEAM_CHAT_ID") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };
    let lark_app_id = match std::env::var("BEAM_LARK_APP_ID") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };
    let root_message_id = std::env::var("BEAM_ROOT_MESSAGE_ID")
        .ok()
        .and_then(|value| {
            let value = value.trim().to_string();
            if value.is_empty() { None } else { Some(value) }
        });
    let body = serde_json::json!({
        "sessionId": session_id,
        "chatId": chat_id,
        "larkAppId": lark_app_id,
        "rootMessageId": root_message_id,
        "questions": parsed.questions.iter().map(|q| serde_json::json!({
            "prompt": q.prompt,
            "options": q.options,
            "multiSelect": q.multi_select,
        })).collect::<Vec<_>>(),
        "timeoutMs": 3_600_000u64,
        "approvers": [],
    });
    let result = match post_ask(_paths, &body).await {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let ask_result: beam_core::AskResult = match serde_json::from_value(result) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    match ask_result {
        beam_core::AskResult::Answered { answers, .. } => {
            let directive = ask_hook::format_answer(&cli_id, &answers, &parsed)?;
            if !directive.is_empty() {
                println!("{directive}");
            }
        }
        _ => {
            let directive = ask_hook::passthrough(&cli_id, &payload)?;
            if !directive.is_empty() {
                println!("{directive}");
            }
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct MigrateFlags {
    dry_run: bool,
    force: bool,
}

fn parse_migrate_flags(args: &[String]) -> Result<MigrateFlags> {
    let mut flags = MigrateFlags::default();
    for arg in args {
        match arg.as_str() {
            "--dry-run" => flags.dry_run = true,
            "--force" | "-f" => flags.force = true,
            "--backup" => {}
            other => bail!("未知 migrate 参数: {}", other),
        }
    }
    Ok(flags)
}

async fn cmd_migrate(paths: &BeamPaths, args: Vec<String>) -> Result<()> {
    let flags = parse_migrate_flags(&args)?;
    let home = std::env::var("HOME").context("HOME env var not set")?;
    let ts_root = std::path::PathBuf::from(home).join(".beam");
    let ts_bots = ts_root.join("bots.json");

    if !ts_bots.exists() {
        println!("No TS bots.json found at {}", ts_bots.display());
        println!("Nothing to migrate.");
        return Ok(());
    }

    let raw = std::fs::read_to_string(&ts_bots)
        .with_context(|| format!("failed to read {}", ts_bots.display()))?;
    let bots: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("invalid JSON in {}", ts_bots.display()))?;

    let rs_bots = paths.bots_json();
    let mut conflict_report = Vec::new();
    let existing_bots = if rs_bots.exists() {
        let existing = std::fs::read_to_string(&rs_bots)?;
        if existing.trim().len() > 2 {
            conflict_report.push(format!("bots: {}", rs_bots.display()));
            Some(existing)
        } else {
            None
        }
    } else {
        None
    };

    let ts_sessions = ts_root.join("sessions.json");
    let sessions: Vec<serde_json::Value> = if ts_sessions.exists() {
        let raw = std::fs::read_to_string(&ts_sessions)
            .with_context(|| format!("failed to read {}", ts_sessions.display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("invalid JSON in {}", ts_sessions.display()))?
    } else {
        Vec::new()
    };

    let session_plan: Vec<(String, std::path::PathBuf, serde_json::Value)> = sessions
        .iter()
        .map(|session| {
            let session_id = session
                .get("sessionId")
                .or_else(|| session.get("session_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let out_path = paths.sessions_dir().join(format!("{}.json", session_id));
            (session_id, out_path, session.clone())
        })
        .collect();

    let session_conflicts: Vec<String> = session_plan
        .iter()
        .filter(|(_, path, _)| path.exists())
        .map(|(id, path, _)| format!("session {} -> {}", id, path.display()))
        .collect();
    conflict_report.extend(session_conflicts.iter().cloned());

    if !conflict_report.is_empty() {
        println!("迁移冲突报告:");
        for item in &conflict_report {
            println!("  - {}", item);
        }
        if !flags.force {
            println!("使用 --force 可覆盖这些冲突目标；使用 --dry-run 仅查看报告。");
            if flags.dry_run {
                return Ok(());
            }
            bail!("迁移目标已有内容，已停止。");
        }
    }

    if flags.dry_run {
        println!("Dry run:");
        println!(
            "  TS bots: {}",
            bots.as_array().map(|a| a.len()).unwrap_or(0)
        );
        println!("  TS sessions: {}", session_plan.len());
        println!("  Rust bots target: {}", rs_bots.display());
        for (_, path, _) in &session_plan {
            println!("  session target: {}", path.display());
        }
        return Ok(());
    }

    std::fs::create_dir_all(paths.root())?;
    if existing_bots.is_some() {
        let backup = rs_bots.with_extension("json.bak");
        std::fs::copy(&rs_bots, &backup)?;
        println!("旧 bots 备份: {}", backup.display());
    }
    std::fs::write(&rs_bots, serde_json::to_string_pretty(&bots)?)?;
    println!("Migrated {} -> {}", ts_bots.display(), rs_bots.display());
    println!(
        "{} bot(s) migrated.",
        bots.as_array().map(|a| a.len()).unwrap_or(0)
    );

    if !session_plan.is_empty() {
        std::fs::create_dir_all(paths.sessions_dir())?;
        let mut migrated = 0usize;
        for (session_id, out_path, session) in session_plan {
            if out_path.exists() {
                let backup = out_path.with_extension("json.bak");
                std::fs::copy(&out_path, &backup)?;
                println!("旧 session 备份: {}", backup.display());
            }
            let migrated_session = serde_json::json!({
                "session_id": session_id,
                "title": session.get("title").unwrap_or(&serde_json::json!("migrated")),
                "chat_id": session.get("chatId").or_else(|| session.get("chat_id")).unwrap_or(&serde_json::json!("")),
                "root_message_id": session.get("rootMessageId").or_else(|| session.get("root_message_id")).unwrap_or(&serde_json::json!("")),
                "scope": session.get("scope").unwrap_or(&serde_json::json!("thread")),
                "status": "closed",
                "created_at": chrono::Utc::now().to_rfc3339(),
                "lark_app_id": session.get("larkAppId").or_else(|| session.get("lark_app_id")).unwrap_or(&serde_json::json!("unknown")),
                "owner_open_id": session.get("ownerOpenId").or_else(|| session.get("owner_open_id")),
                "cli_id": session.get("cliId").or_else(|| session.get("cli_id")),
                "cli_bin": session.get("cliBin").or_else(|| session.get("cli_bin")),
                "working_dir": session.get("workingDir").or_else(|| session.get("working_dir")),
                "backend_type": session.get("backendType").or_else(|| session.get("backend_type")).unwrap_or(&serde_json::json!("tmux")),
            });
            tokio::fs::write(
                &out_path,
                serde_json::to_string_pretty(&migrated_session)? + "\n",
            )
            .await?;
            migrated += 1;
        }
        println!("Migrated {} session(s).", migrated);
    }

    Ok(())
}

fn cmd_autostart(paths: &BeamPaths, args: Vec<String>) -> Result<()> {
    let action = autostart::parse_action(&args);
    let opts = autostart::AutostartOpts {
        exe: std::env::current_exe()?,
        paths: paths.clone(),
    };
    match action {
        autostart::AutostartAction::Enable => autostart::enable_autostart(&opts),
        autostart::AutostartAction::Disable => autostart::disable_autostart(),
        autostart::AutostartAction::Status => autostart::autostart_status(),
        autostart::AutostartAction::Refresh => {
            if autostart::refresh_autostart(&opts)? {
                println!("✅ autostart 已刷新");
            } else {
                println!("ℹ️  autostart 无需更新");
            }
            Ok(())
        }
    }
}
