use super::{ApiClient, daemon_log_path, find_runtime};
use crate::*;
use anyhow::{Context, Result, bail};

pub(crate) fn current_exe() -> Result<PathBuf> {
    Ok(std::env::current_exe().context("failed to locate current executable")?)
}

pub(crate) fn daemon_state_is_live(paths: &BeamPaths) -> bool {
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

pub(crate) fn spawn_background_daemon(exe: &Path, paths: &BeamPaths) -> Result<()> {
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

pub(crate) async fn wait_for_health(paths: &BeamPaths) -> Result<ApiHealth> {
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

pub(crate) fn format_duration(ms: i64) -> String {
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

pub(crate) fn active_sessions(items: &[SessionSummary]) -> Vec<SessionSummary> {
    let mut v: Vec<SessionSummary> = items
        .iter()
        .filter(|s| s.status == SessionStatus::Active)
        .cloned()
        .collect();
    v.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    v
}

pub(crate) fn truncate(s: &str, max: usize) -> &str {
    if s.chars().count() <= max {
        s
    } else {
        &s[..s.floor_char_boundary(max)]
    }
}

pub(crate) fn shorten_home(path: &str) -> String {
    if let Ok(home) = std::env::var("HOME") {
        if path.starts_with(&home) {
            return path.replacen(&home, "~", 1);
        }
    }
    path.to_string()
}

pub(crate) fn print_sessions(items: &[SessionSummary]) {
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
            SessionStatus::Active if item.worker_unresponsive => "无响应",
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

pub(crate) async fn fetch_sessions(api: &ApiClient) -> Result<Vec<SessionSummary>> {
    let resp = api.get(format!("{}/sessions", api.base())).send().await?;
    if !resp.status().is_success() {
        bail!("{}", resp.text().await.unwrap_or_default());
    }
    Ok(resp.json::<Vec<SessionSummary>>().await?)
}

pub(crate) fn session_attach_target(session: &SessionSummary) -> String {
    let fallback = format!(
        "beam-{}",
        &session.session_id[..8.min(session.session_id.len())]
    );
    session
        .adopted_from
        .as_ref()
        .and_then(|adopted| adopted.zellij_session.clone())
        .unwrap_or(fallback)
}

pub(crate) fn resolve_session_prefix(
    items: &[SessionSummary],
    prefix: &str,
) -> Result<SessionSummary> {
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

pub(crate) fn attach_session(session: &SessionSummary) -> Result<()> {
    let target = session_attach_target(session);
    let status = StdCommand::new("zellij")
        .args(["attach", &target])
        .status()
        .context("failed to run zellij attach")?;
    if !status.success() {
        bail!("zellij attach failed for {}", target);
    }
    Ok(())
}

pub(crate) async fn cmd_attach(api: &ApiClient, session_id: &str) -> Result<()> {
    let items = fetch_sessions(api).await?;
    let session = resolve_session_prefix(&items, session_id)?;
    attach_session(&session)
}
