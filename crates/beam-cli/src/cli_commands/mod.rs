use crate::*;
use anyhow::{Result, bail};

mod autostart_command;
mod bot_registry;
mod daemon;
mod hook;
mod migration;
mod preferences;
mod schedule;
mod send;
mod setup;

pub(crate) use autostart_command::cmd_autostart;
pub(crate) use bot_registry::{cmd_bots, daemon_log_path, load_bots};
pub(crate) use daemon::{
    cmd_attach, current_exe, fetch_sessions, print_sessions, spawn_background_daemon,
    wait_for_health,
};
pub(crate) use hook::{
    api_client, cmd_hook, discover_session_id, find_runtime, read_send_content,
    resolve_cli_session_id,
};
pub(crate) use migration::cmd_migrate;
pub(crate) use preferences::{ask_line, cmd_lang, cmd_voice};
pub(crate) use schedule::cmd_schedule;
pub(crate) use send::build_send_request;
pub(crate) use setup::cmd_setup;

#[cfg(test)]
pub(crate) use bot_registry::{BotInfoEntry, format_bot_info_entries_for_cli};
#[cfg(test)]
pub(crate) use daemon::{active_sessions, format_duration};
#[cfg(test)]
pub(crate) use hook::discover_session_id_from_pid;
#[cfg(test)]
pub(crate) use migration::parse_migrate_flags;
#[cfg(test)]
pub(crate) use send::parse_mention;
#[cfg(test)]
pub(crate) use setup::{
    bin_candidates_for_cli_id, default_cli_args_for_cli_id, parse_cli_args_input,
    resolve_allowed_users, setup_backup_file,
};

pub(crate) async fn run(command: Command) -> Result<()> {
    match command {
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
                Command::Send(args) => {
                    let req = build_send_request(args)?;
                    let session_id = discover_session_id(&paths)?;
                    let (client, base) = api_client(&paths).await?;
                    let resp = client
                        .post(format!("{}/sessions/{}/final-output", base, session_id))
                        .json(&req)
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        bail!("{}", resp.text().await.unwrap_or_default());
                    }
                    println!("final output accepted");
                }
                Command::History(args) => {
                    let session_id = resolve_cli_session_id(&paths, args.session_id)?;
                    let (client, base) = api_client(&paths).await?;
                    let resp = client
                        .get(format!("{}/sessions/{}/history", base, session_id))
                        .query(&[
                            ("limit", args.limit.to_string()),
                            ("scope", args.scope.clone()),
                        ])
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        bail!("{}", resp.text().await.unwrap_or_default());
                    }
                    let out: serde_json::Value = resp.json().await?;
                    println!("{}", serde_json::to_string_pretty(&out)?);
                }
                Command::Quoted(args) => {
                    let session_id = resolve_cli_session_id(&paths, args.session_id)?;
                    let (client, base) = api_client(&paths).await?;
                    let resp = client
                        .get(format!(
                            "{}/sessions/{}/quoted/{}",
                            base, session_id, args.message_id
                        ))
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        bail!("{}", resp.text().await.unwrap_or_default());
                    }
                    let out: serde_json::Value = resp.json().await?;
                    println!("{}", serde_json::to_string_pretty(&out)?);
                }
                Command::Bots { args } => cmd_bots(args, &paths)?,
                Command::Session { command } => {
                    let (client, base) = api_client(&paths).await?;
                    match command {
                        SessionCommand::Create(args) => {
                            let resp = client
                                .post(format!("{}/sessions", base))
                                .json(&CreateSessionRequest {
                                    title: args.title,
                                    cli_id: args.cli_id,
                                    cli_bin: args.cli_bin,
                                    cli_args: args.cli_args,
                                    working_dir: args.working_dir,
                                    prompt: args.prompt,
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
                        SessionCommand::Adopt(args) => {
                            // Parse target as "session:pane_id" or "session"
                            let (zellij_session, zellij_pane_id) = match args.target.split_once(':')
                            {
                                Some((session, pane)) => (session.to_string(), pane.to_string()),
                                None => (args.target.clone(), "terminal_0".to_string()),
                            };
                            let resp = client
                                .post(format!("{}/adopt/zellij", base))
                                .json(&serde_json::json!({
                                    "zellij_session": zellij_session,
                                    "zellij_pane_id": zellij_pane_id,
                                    "cli_id": args.cli_id,
                                    "cli_bin": args.cli_bin,
                                    "title": args.title,
                                    "cwd": "",
                                }))
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let session = resp.json::<SessionSummary>().await?;
                            println!("{}", serde_json::to_string_pretty(&session)?);
                        }
                        SessionCommand::Discover => {
                            let resp = client.get(format!("{}/adopt/zellij", base)).send().await?;
                            if !resp.status().is_success() {
                                bail!("{}", resp.text().await.unwrap_or_default());
                            }
                            let items = resp.json::<Vec<serde_json::Value>>().await?;
                            for item in &items {
                                let session = item
                                    .get("zellij_session")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("-");
                                let pane_id = item
                                    .get("zellij_pane_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("-");
                                let pid =
                                    item.get("cli_pid").and_then(|v| v.as_i64()).unwrap_or(-1);
                                let cwd = item.get("cwd").and_then(|v| v.as_str()).unwrap_or("-");
                                let title =
                                    item.get("title").and_then(|v| v.as_str()).unwrap_or("-");
                                println!(
                                    "{}:{}  pid={}  cwd={}  {}",
                                    session, pane_id, pid, cwd, title
                                );
                            }
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
