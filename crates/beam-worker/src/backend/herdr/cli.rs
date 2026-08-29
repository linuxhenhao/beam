//! Bounded `herdr` CLI wrapper for the worker control plane.
//!
//! The official automation advice is CLI-first; the raw socket is reserved
//! for long subscriptions (see `observe.rs`). Every call here is bounded by
//! [`HERDR_ACTION_TIMEOUT`] (create/run may use [`HERDR_SPAWN_TIMEOUT`]) so a
//! wedged herdr server cannot block a tokio thread forever.
//!
//! Environment hygiene: worker herdr invocations must not inherit
//! `HERDR_PANE_ID` / `HERDR_TAB_ID` / `HERDR_WORKSPACE_ID` from a daemon that
//! was itself started inside a Herdr TUI, or `--current` style resolution
//! would target the wrong pane.

use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tracing::warn;

use super::ids::{HerdrIds, WorkspaceEntry, parse_create_ids, parse_workspace_list};

/// Upper bound for a single `herdr` control call (create/run may take longer;
/// they use [`HERDR_SPAWN_TIMEOUT`]).
pub(crate) const HERDR_ACTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
/// Upper bound for `workspace create` / `pane run` (CLI startup can be slow).
pub(crate) const HERDR_SPAWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Upper bound for waiting on a shell prompt before `pane run`.
pub(crate) const HERDR_SHELL_READY_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);
/// Upper bound for polling `herdr status server` after starting a server.
pub(crate) const HERDR_SERVER_START_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

fn clean_env(cmd: &mut Command) {
    cmd.env_remove("HERDR_PANE_ID")
        .env_remove("HERDR_TAB_ID")
        .env_remove("HERDR_WORKSPACE_ID");
}

fn base_command() -> Command {
    let mut cmd = Command::new("herdr");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    clean_env(&mut cmd);
    cmd
}

async fn run_json(cmd: &mut Command, timeout: std::time::Duration, what: &str) -> Result<String> {
    let out = tokio::time::timeout(timeout, cmd.output())
        .await
        .with_context(|| format!("{what} timed out after {}s", timeout.as_secs()))?
        .with_context(|| format!("failed to spawn {what}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!(
            "{what} failed ({}): {}",
            out.status,
            stderr.trim().trim_start_matches("Error: ").trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `herdr status server` — true when the server socket answers.
pub(crate) async fn status_server() -> Result<bool> {
    let mut cmd = base_command();
    cmd.args(["status", "server"]);
    let out = tokio::time::timeout(HERDR_ACTION_TIMEOUT, cmd.output())
        .await
        .context("herdr status server timed out")?
        .context("failed to spawn herdr status server")?;
    Ok(out.status.success())
}

/// `herdr server` — start a headless server detached from this worker.
///
/// `herdr server` runs in the foreground until the server exits, so we spawn
/// it without waiting (and without `kill_on_drop`, which would kill the
/// freshly started server), redirect output away from the worker's stdout
/// (reserved for JSON IPC), then poll `status server` until it answers.
pub(crate) async fn start_server() -> Result<()> {
    let paths = beam_core::BeamPaths::discover()?;
    let log_path = paths.logs_dir().join("herdr-server.log");
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create log dir {}", parent.display()))?;
    }
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("failed to open herdr server log {}", log_path.display()))?;
    let mut cmd = std::process::Command::new("herdr");
    cmd.arg("server")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .env_remove("HERDR_PANE_ID")
        .env_remove("HERDR_TAB_ID")
        .env_remove("HERDR_WORKSPACE_ID");
    // Detach: the child keeps running after this worker exits. The daemon
    // shares one server across sessions, so a worker must not kill it.
    let _ = cmd.spawn().context("failed to spawn herdr server")?;
    let deadline = tokio::time::Instant::now() + HERDR_SERVER_START_TIMEOUT;
    loop {
        if status_server().await.unwrap_or(false) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "herdr server did not answer within {}s (log: {})",
                HERDR_SERVER_START_TIMEOUT.as_secs(),
                log_path.display()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}

/// `herdr workspace list` — all workspaces in the session.
pub(crate) async fn workspace_list() -> Result<Vec<WorkspaceEntry>> {
    let mut cmd = base_command();
    cmd.args(["workspace", "list"]);
    let payload = run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr workspace list").await?;
    parse_workspace_list(&payload)
}

/// `herdr workspace create --cwd <dir> --label <label> --no-focus`.
pub(crate) async fn workspace_create(cwd: &str, label: &str) -> Result<HerdrIds> {
    let mut cmd = base_command();
    cmd.args([
        "workspace",
        "create",
        "--cwd",
        cwd,
        "--label",
        label,
        "--no-focus",
    ]);
    let payload = run_json(&mut cmd, HERDR_SPAWN_TIMEOUT, "herdr workspace create").await?;
    parse_create_ids(&payload)
}

/// `herdr workspace get <id>` — `Ok(Some(payload))` when the workspace exists,
/// `Ok(None)` when herdr confirms it is gone (exit nonzero / not_found error),
/// `Err` only for probe failures (timeout, spawn failure, non-JSON).
pub(crate) async fn workspace_get(workspace_id: &str) -> Result<Option<String>> {
    let mut cmd = base_command();
    cmd.args(["workspace", "get", workspace_id]);
    let out = tokio::time::timeout(HERDR_ACTION_TIMEOUT, cmd.output())
        .await
        .with_context(|| format!("herdr workspace get {workspace_id} timed out"))?
        .with_context(|| format!("failed to spawn herdr workspace get {workspace_id}"))?;
    if out.status.success() {
        return Ok(Some(String::from_utf8_lossy(&out.stdout).into_owned()));
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    if stderr.contains("workspace_not_found") || stderr.contains("not found") {
        return Ok(None);
    }
    bail!(
        "herdr workspace get {workspace_id} failed ({}): {}",
        out.status,
        stderr.trim()
    )
}

/// Read the workspace + pane ids back from a `workspace get` payload when it
/// embeds them (reuse path needs a pane id without creating). Tolerant of
/// several plausible shapes; `None` when the payload does not carry a pane.
pub(crate) fn workspace_get_ids(payload: &str) -> Result<Option<HerdrIds>> {
    let value: serde_json::Value =
        serde_json::from_str(payload).context("workspace get payload is not JSON")?;
    let workspace_id = value
        .pointer("/result/workspace/workspace_id")
        .or_else(|| value.pointer("/workspace/workspace_id"))
        .or_else(|| value.get("workspace_id"))
        .and_then(serde_json::Value::as_str);
    let pane_id = value
        .pointer("/result/workspace/root_pane/pane_id")
        .or_else(|| value.pointer("/result/root_pane/pane_id"))
        .or_else(|| value.pointer("/result/pane/pane_id"))
        .or_else(|| value.pointer("/result/pane_id"))
        .or_else(|| value.get("root_pane").and_then(|p| p.get("pane_id")))
        .or_else(|| value.get("pane_id"))
        .and_then(serde_json::Value::as_str);
    let Some(workspace_id) = workspace_id else {
        return Ok(None);
    };
    let Some(pane_id) = pane_id else {
        return Ok(None);
    };
    Ok(Some(HerdrIds {
        workspace_id: workspace_id.to_string(),
        pane_id: pane_id.to_string(),
    }))
}

/// `herdr workspace close <id>` — close a managed workspace. herdr 0.8.2
/// closes immediately (no `--force` flag exists); if a future version answers
/// `confirmation_required`, retry with `--force`.
pub(crate) async fn workspace_close(workspace_id: &str) -> Result<()> {
    match run_close(&["workspace", "close", workspace_id]).await {
        Ok(()) => return Ok(()),
        Err(err) => {
            if !err.to_string().contains("confirmation_required") {
                return Err(err);
            }
        }
    }
    warn!(
        workspace_id,
        "herdr workspace close asked for confirmation; retrying with --force"
    );
    run_close(&["workspace", "close", "--force", workspace_id]).await
}

async fn run_close(args: &[&str]) -> Result<()> {
    let mut cmd = base_command();
    cmd.args(args);
    run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr workspace close").await?;
    Ok(())
}

/// `herdr pane run <pane_id> <command-string>` — run one shell command in the
/// pane. This is the managed spawn path: the launch spec (env + `cli_bin`) is
/// quoted into a single string.
pub(crate) async fn pane_run(pane_id: &str, command: &str) -> Result<()> {
    let mut cmd = base_command();
    cmd.args(["pane", "run", pane_id, command]);
    run_json(&mut cmd, HERDR_SPAWN_TIMEOUT, "herdr pane run").await?;
    Ok(())
}

/// `herdr pane wait-output --regex <regex> <pane>` — wait for a shell prompt.
/// `--regex` (not `--match`, which is literal) so the prompt pattern actually
/// matches. Returns false on timeout; the caller proceeds to `pane run`
/// anyway (this only lowers the race probability).
pub(crate) async fn pane_wait_output(pane_id: &str, regex: &str) -> Result<bool> {
    let mut cmd = base_command();
    cmd.args(["pane", "wait-output", "--regex", regex, pane_id]);
    let out = tokio::time::timeout(HERDR_SHELL_READY_TIMEOUT, cmd.output())
        .await
        .context("herdr pane wait-output timed out")?
        .context("failed to spawn herdr pane wait-output")?;
    Ok(out.status.success())
}

/// `herdr pane list` — every pane with its workspace/tab ids. Used to recover
/// the root pane id of an existing workspace (workspace get does not embed
/// pane ids).
pub(crate) async fn pane_list() -> Result<Vec<PaneEntry>> {
    let mut cmd = base_command();
    cmd.args(["pane", "list"]);
    let payload = run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr pane list").await?;
    parse_pane_list(&payload)
}

/// A pane entry from `herdr pane list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PaneEntry {
    pub(crate) workspace_id: String,
    pub(crate) pane_id: String,
}

/// Parse `pane list` JSON: `.result.panes[]` entries carry `workspace_id` and
/// `pane_id`; a flat array is also accepted.
pub(crate) fn parse_pane_list(payload: &str) -> Result<Vec<PaneEntry>> {
    let value: serde_json::Value =
        serde_json::from_str(payload).context("pane list payload is not JSON")?;
    let items = value
        .pointer("/result/panes")
        .or_else(|| value.pointer("/panes"))
        .or_else(|| value.as_array().map(|_| &value))
        .context("pane list JSON has no pane array")?;
    let Some(items) = items.as_array() else {
        bail!("pane list JSON array expected");
    };
    let mut out = Vec::new();
    for item in items {
        let Some(workspace_id) = item.get("workspace_id").and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Some(pane_id) = item.get("pane_id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        out.push(PaneEntry {
            workspace_id: workspace_id.to_string(),
            pane_id: pane_id.to_string(),
        });
    }
    Ok(out)
}

/// `herdr pane send-text <pane_id> <text>` — low-level input, no submit.
pub(crate) async fn pane_send_text(pane_id: &str, text: &str) -> Result<()> {
    let mut cmd = base_command();
    cmd.args(["pane", "send-text", pane_id, text]);
    run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr pane send-text").await?;
    Ok(())
}

/// `herdr pane send-keys <pane_id> <key>…` — special keys (enter, esc, ctrl+c).
pub(crate) async fn pane_send_keys(pane_id: &str, keys: &[&str]) -> Result<()> {
    let mut cmd = base_command();
    cmd.arg("pane").arg("send-keys").arg(pane_id).args(keys);
    run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr pane send-keys").await?;
    Ok(())
}

/// `herdr pane read <pane_id> --source visible --format ansi` — the
/// authoritative full visible screen.
pub(crate) async fn pane_read_visible(pane_id: &str) -> Result<String> {
    let mut cmd = base_command();
    cmd.args([
        "pane", "read", pane_id, "--source", "visible", "--format", "ansi",
    ]);
    run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr pane read").await
}

/// Foreground process info from `pane process_info`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProcessInfo {
    pub(crate) pid: Option<i32>,
    pub(crate) argv: Option<String>,
    pub(crate) cwd: Option<String>,
}

/// `herdr pane process-info --pane <pane_id>` — foreground pid/argv/cwd.
pub(crate) async fn pane_process_info(pane_id: &str) -> Result<ProcessInfo> {
    let mut cmd = base_command();
    cmd.args(["pane", "process-info", "--pane", pane_id]);
    let payload = run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr pane process-info").await?;
    parse_process_info(&payload)
}

/// `herdr agent get <pane_id>` (or the documented alias `agent get`) — one
/// agent state snapshot. Used as the polling fallback when the socket
/// `events.subscribe` long connection is unavailable.
pub(crate) async fn agent_get(pane_id: &str) -> Result<String> {
    let mut cmd = base_command();
    cmd.args(["agent", "get", pane_id]);
    run_json(&mut cmd, HERDR_ACTION_TIMEOUT, "herdr agent get").await
}

/// Extract the agent state name from an `agent get` payload. Returns `None`
/// for unknown/unparseable shapes (callers treat that as no signal).
pub(crate) fn parse_agent_state(payload: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    let agent = value
        .pointer("/result/agent")
        .or_else(|| value.get("agent"));
    let state = agent
        .and_then(|a| a.get("agent_status").and_then(serde_json::Value::as_str))
        .or_else(|| agent.and_then(|a| a.get("state").and_then(serde_json::Value::as_str)))
        .or_else(|| {
            value
                .pointer("/result/agent_status")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            value
                .get("agent_status")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            value
                .pointer("/result/state")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| value.get("state").and_then(serde_json::Value::as_str))?;
    Some(state.to_string())
}

/// Parse a `process-info` JSON payload into [`ProcessInfo`]. Pure so hermetic
/// tests can pin the contract without a real herdr binary. The real payload
/// nests the foreground process under `.result.process_info`:
/// `foreground_processes[0].{pid,argv,cwd}` with `shell_pid` as fallback.
pub(crate) fn parse_process_info(payload: &str) -> Result<ProcessInfo> {
    let value: serde_json::Value =
        serde_json::from_str(payload).context("process-info payload is not JSON")?;
    let process_info = value
        .pointer("/result/process_info")
        .or_else(|| value.pointer("/process_info"))
        .or_else(|| value.get("result"));
    let foreground = process_info
        .and_then(|p| p.get("foreground_processes"))
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first());
    let get_text = |obj: Option<&serde_json::Value>, keys: &[&str]| {
        let obj = obj?;
        keys.iter().find_map(|k| {
            let v = obj.get(*k)?;
            v.as_str()
                .map(ToOwned::to_owned)
                .or_else(|| _argv_from_array(v))
        })
    };
    let pid = foreground
        .and_then(|f| {
            ["pid", "process_id", "foreground_pid"]
                .iter()
                .find_map(|k| {
                    f.get(*k).and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                })
        })
        .or_else(|| {
            // No foreground process: fall back to the shell pid (a fresh pane
            // sitting at a prompt still counts as alive for `is_alive`).
            process_info
                .and_then(|p| p.get("shell_pid"))
                .and_then(serde_json::Value::as_i64)
        });
    let argv = get_text(foreground, &["argv", "command", "cmdline"])
        .or_else(|| get_text(process_info, &["cmdline", "argv"]));
    let cwd = get_text(foreground, &["cwd", "working_dir"])
        .or_else(|| get_text(process_info, &["cwd", "working_dir"]));
    if pid.is_none() && argv.is_none() && cwd.is_none() {
        // Flat fallback (older/foreign payloads and the empty-foreground
        // marker `{"pid":null,"argv":"","cwd":null}`).
        let get_str = |keys: &[&str]| {
            keys.iter()
                .filter_map(|k| value.get(*k).and_then(serde_json::Value::as_str))
                .next()
        };
        let flat_pid = ["pid", "process_id", "foreground_pid"]
            .iter()
            .find_map(|k| {
                value.get(*k).and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
            });
        return Ok(ProcessInfo {
            pid: flat_pid.and_then(|v| i32::try_from(v).ok()),
            argv: get_str(&["argv", "command", "cmdline"]).map(ToOwned::to_owned),
            cwd: get_str(&["cwd", "working_dir"]).map(ToOwned::to_owned),
        });
    }
    Ok(ProcessInfo {
        pid: pid.and_then(|v| i32::try_from(v).ok()),
        argv,
        cwd,
    })
}

/// `argv` may be a JSON array (`["sleep","300"]`) on some payloads; keep the
/// top-level read tolerant of both shapes.
fn _argv_from_array(value: &serde_json::Value) -> Option<String> {
    value.as_array().map(|items| {
        items
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>()
            .join(" ")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_info_parses_pid_argv_cwd() {
        let payload = r#"{"result":{"process_info":{"foreground_processes":[{"argv":["claude","--resume"],"pid":1234,"cwd":"/repo"}],"shell_pid":99}}}"#;
        let info = parse_process_info(payload).expect("process info");
        assert_eq!(info.pid, Some(1234));
        assert_eq!(info.argv.as_deref(), Some("claude --resume"));
        assert_eq!(info.cwd.as_deref(), Some("/repo"));
    }

    #[test]
    fn process_info_empty_pid_is_none() {
        let payload = r#"{"result":{"process_info":{"foreground_processes":[{"argv":[],"pid":null,"cwd":null}],"shell_pid":null}}}"#;
        let info = parse_process_info(payload).expect("process info");
        assert_eq!(info.pid, None);
        // Empty foreground + no shell pid is the dead-pane contract.
        assert!(info.argv.as_deref().unwrap_or("").is_empty());
    }

    #[test]
    fn process_info_shell_pid_fallback() {
        let payload =
            r#"{"result":{"process_info":{"foreground_processes":[],"shell_pid":2749406}}}"#;
        let info = parse_process_info(payload).expect("process info");
        assert_eq!(info.pid, Some(2749406));
    }

    #[test]
    fn process_info_flat_shape_still_parses() {
        let payload = r#"{"pid":1234,"argv":"claude --resume","cwd":"/repo"}"#;
        let info = parse_process_info(payload).expect("process info");
        assert_eq!(info.pid, Some(1234));
        assert_eq!(info.argv.as_deref(), Some("claude --resume"));
    }

    #[test]
    fn agent_state_reads_agent_status() {
        let payload = r#"{"result":{"agent":{"agent":"codex","agent_status":"blocked"}}}"#;
        assert_eq!(parse_agent_state(payload).as_deref(), Some("blocked"));
        let flat = r#"{"agent_status":"idle"}"#;
        assert_eq!(parse_agent_state(flat).as_deref(), Some("idle"));
        let legacy = r#"{"result":{"state":"working"}}"#;
        assert_eq!(parse_agent_state(legacy).as_deref(), Some("working"));
        assert_eq!(parse_agent_state("not json"), None);
    }

    #[test]
    fn pane_list_parses_result_panes() {
        let payload = r#"{"result":{"panes":[{"pane_id":"w2:p1","workspace_id":"w2"},{"pane_id":"w3:p1","workspace_id":"w3"}]}}"#;
        let panes = parse_pane_list(payload).expect("pane list");
        assert_eq!(panes.len(), 2);
        assert_eq!(panes[1].workspace_id, "w3");
        assert_eq!(panes[1].pane_id, "w3:p1");
    }
}
