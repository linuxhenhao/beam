use std::borrow::ToOwned;

use super::*;

pub(crate) fn zellij_has_session(target: &str) -> bool {
    std::process::Command::new("zellij")
        .args(["list-sessions", "--no-formatting"])
        .output()
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .any(|line| line.contains(target) && !line.contains("EXITED"))
        })
        .unwrap_or(false)
}

pub(crate) fn zellij_live_sessions() -> Vec<String> {
    std::process::Command::new("zellij")
        .args(["list-sessions", "--no-formatting"])
        .output()
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.contains("EXITED"))
        .filter_map(|line| line.split_whitespace().next().map(ToOwned::to_owned))
        .collect()
}

pub(crate) fn zellij_find_server_pid(session: &str) -> Option<i32> {
    let out = std::process::Command::new("ps")
        .args(["-eo", "pid=,args="])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let expected = format!("/{session}");
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let trimmed = line.trim();
        let (pid_str, args) = trimmed.split_once(char::is_whitespace)?;
        let pid = pid_str.trim().parse::<i32>().ok()?;
        let argv = args.trim();
        if argv.contains("zellij") && argv.contains("--server") && argv.ends_with(&expected) {
            return Some(pid);
        }
    }
    None
}

pub(crate) fn zellij_pane_children(server_pid: i32) -> Vec<i32> {
    let out = std::process::Command::new("ps")
        .args(["-eo", "pid=,ppid=,comm="])
        .output();
    let Ok(out) = out else {
        return Vec::new();
    };
    if !out.status.success() {
        return Vec::new();
    }
    let mut children = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let trimmed = line.trim();
        let mut parts = trimmed.split_whitespace();
        let Some(pid_str) = parts.next() else {
            continue;
        };
        let Some(ppid_str) = parts.next() else {
            continue;
        };
        let comm = parts.next().unwrap_or_default();
        let Ok(pid) = pid_str.parse::<i32>() else {
            continue;
        };
        let Ok(ppid) = ppid_str.parse::<i32>() else {
            continue;
        };
        if ppid == server_pid && comm != "zellij" && comm != "ps" && comm != "sh-from-ps" {
            children.push(pid);
        }
    }
    children.sort_unstable();
    children
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ZellijPaneProbe {
    pub(crate) id: u64,
    pub(crate) is_plugin: bool,
    pub(crate) is_floating: bool,
    pub(crate) title: Option<String>,
    pub(crate) pane_content_columns: Option<u64>,
    pub(crate) pane_content_rows: Option<u64>,
    pub(crate) pane_columns: Option<u64>,
    pub(crate) pane_rows: Option<u64>,
}

pub(crate) fn zellij_list_panes(session: &str) -> Vec<ZellijPaneProbe> {
    let out = std::process::Command::new("zellij")
        .args(["--session", session, "action", "list-panes", "--json"])
        .output();
    let Ok(out) = out else {
        return Vec::new();
    };
    if !out.status.success() {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_slice::<Value>(&out.stdout) else {
        return Vec::new();
    };
    let Some(array) = value.as_array() else {
        return Vec::new();
    };
    array
        .iter()
        .filter_map(|pane| {
            let id = pane.get("id").and_then(Value::as_u64)?;
            Some(ZellijPaneProbe {
                id,
                is_plugin: pane
                    .get("is_plugin")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                is_floating: pane
                    .get("is_floating")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                title: pane
                    .get("title")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                pane_content_columns: pane.get("pane_content_columns").and_then(Value::as_u64),
                pane_content_rows: pane.get("pane_content_rows").and_then(Value::as_u64),
                pane_columns: pane.get("pane_columns").and_then(Value::as_u64),
                pane_rows: pane.get("pane_rows").and_then(Value::as_u64),
            })
        })
        .collect()
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ZellijLayoutPane {
    pub(crate) command: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) args: Vec<String>,
}

pub(crate) fn zellij_dump_layout_panes(session: &str) -> Vec<ZellijLayoutPane> {
    let out = std::process::Command::new("zellij")
        .args(["--session", session, "action", "dump-layout"])
        .output();
    let Ok(out) = out else {
        return Vec::new();
    };
    if !out.status.success() {
        return Vec::new();
    }
    let raw = String::from_utf8_lossy(&out.stdout);
    let mut body = raw.as_ref();
    for marker in [
        "new_tab_template",
        "swap_tiled_layout",
        "swap_floating_layout",
    ] {
        if let Some(idx) = body.find(marker) {
            body = &body[..idx];
        }
    }
    parse_dump_layout_body(body)
}

/// Parse the body of a `zellij action dump-layout` output (after stripping
/// `new_tab_template` etc.) into [`ZellijLayoutPane`] leaves.
///
/// This handles both KDL attribute (`cwd="..."`) and node parameter (`cwd "..."`)
/// forms for cwd, and inherits a layout-level cwd into panes that don't declare
/// their own.
fn parse_dump_layout_body(body: &str) -> Vec<ZellijLayoutPane> {
    #[derive(Clone)]
    struct Frame {
        is_pane: bool,
        is_floating: bool,
        command: Option<String>,
        cwd: Option<String>,
        args: Vec<String>,
        has_plugin: bool,
        has_child_pane: bool,
    }

    let mut stack: Vec<Frame> = Vec::new();
    let mut leaves = Vec::new();
    // Track the nearest ancestor cwd (from layout-level `cwd "..."` node parameter).
    let mut layout_cwd: Option<String> = None;

    let attr = |line: &str, name: &str| -> Option<String> {
        let needle = format!(r#"{}=""#, name);
        let idx = line.find(&needle)? + needle.len();
        let rest = &line[idx..];
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    };
    let emit = |frame: Frame, leaves: &mut Vec<ZellijLayoutPane>| {
        if frame.is_floating {
            return;
        }
        leaves.push(ZellijLayoutPane {
            command: frame.command,
            cwd: frame.cwd,
            args: frame.args,
        });
    };

    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "}" {
            if let Some(frame) = stack.pop()
                && frame.is_pane
                && !frame.has_plugin
                && !frame.has_child_pane
            {
                emit(frame, &mut leaves);
            }
            continue;
        }
        let opens = line.ends_with('{');
        if line.starts_with("pane") {
            if let Some(last) = stack.last_mut()
                && last.is_pane
            {
                last.has_child_pane = true;
            }
            let frame = Frame {
                is_pane: true,
                is_floating: false,
                command: attr(line, "command"),
                // Pane's own cwd attribute takes priority; otherwise inherit
                // the nearest ancestor (layout-level) cwd.
                cwd: attr(line, "cwd").or_else(|| layout_cwd.clone()),
                args: Vec::new(),
                has_plugin: false,
                has_child_pane: false,
            };
            if opens {
                stack.push(frame);
            } else {
                emit(frame, &mut leaves);
            }
            continue;
        }
        if line.starts_with("plugin") {
            if let Some(last) = stack.last_mut()
                && last.is_pane
            {
                last.has_plugin = true;
            }
            if opens {
                stack.push(Frame {
                    is_pane: false,
                    is_floating: false,
                    command: None,
                    cwd: None,
                    args: Vec::new(),
                    has_plugin: false,
                    has_child_pane: false,
                });
            }
            continue;
        }
        if opens {
            stack.push(Frame {
                is_pane: false,
                is_floating: line.starts_with("floating_panes"),
                command: None,
                cwd: None,
                args: Vec::new(),
                has_plugin: false,
                has_child_pane: false,
            });
            continue;
        }
        if line.starts_with("args")
            && let Some(last) = stack.last_mut()
            && last.is_pane
        {
            last.args = line
                .match_indices('"')
                .collect::<Vec<_>>()
                .chunks(2)
                .filter_map(|chunk| match chunk {
                    [(start, _), (end, _)] if *end > *start => {
                        Some(line[*start + 1..*end].to_string())
                    }
                    _ => None,
                })
                .collect();
            continue;
        }
        // Handle KDL node parameter form: `cwd "/path/to/dir"`
        // This appears as a child node inside `layout { ... }` (and potentially
        // inside `pane { ... }`).
        if line.starts_with("cwd ") {
            if let Some(rest) = line.strip_prefix("cwd ") {
                let rest = rest.trim();
                if rest.starts_with('"') {
                    let inner = &rest[1..];
                    if let Some(end) = inner.find('"') {
                        let cwd_value = inner[..end].to_string();
                        if let Some(last) = stack.last_mut()
                            && last.is_pane
                        {
                            // cwd node inside a pane block overrides inherited/attribute cwd.
                            last.cwd = Some(cwd_value);
                        } else {
                            // Otherwise it's an ancestor-level cwd (e.g. layout).
                            layout_cwd = Some(cwd_value);
                        }
                    }
                }
            }
            continue;
        }
    }
    leaves
}

pub(crate) fn discover_zellij_adopt_candidates() -> Vec<ZellijAdoptCandidate> {
    let mut out = Vec::new();
    for session in zellij_live_sessions() {
        if session.starts_with("beam-") {
            continue;
        }
        let panes = zellij_list_panes(&session);
        let layouts = zellij_dump_layout_panes(&session);
        if panes.is_empty() || layouts.is_empty() {
            continue;
        }
        let mut candidates = join_zellij_adopt_candidates(&session, layouts, panes);
        if let Some(server_pid) = zellij_find_server_pid(&session) {
            let child_pids = zellij_pane_children(server_pid);
            for (candidate, pid) in candidates.iter_mut().zip(child_pids.into_iter()) {
                candidate.cli_pid = Some(pid);
            }
        }
        out.extend(candidates);
    }
    out
}

pub(crate) fn cli_id_from_zellij_command(command: &str) -> String {
    let command = command.rsplit('/').next().unwrap_or(command).to_lowercase();
    for spec in beam_core::cli_specs::CLI_SPECS {
        if spec
            .adopt_command_patterns
            .iter()
            .any(|pattern| command.contains(pattern))
        {
            return spec.cli_id.to_string();
        }
    }
    command
}

pub(crate) fn join_zellij_adopt_candidates(
    session: &str,
    layouts: Vec<ZellijLayoutPane>,
    panes: Vec<ZellijPaneProbe>,
) -> Vec<ZellijAdoptCandidate> {
    let terminals = panes
        .into_iter()
        .filter(|pane| !pane.is_plugin && !pane.is_floating)
        .collect::<Vec<_>>();
    layouts
        .into_iter()
        .zip(terminals)
        .map(|(layout, pane)| {
            let pane_id = format!("terminal_{}", pane.id);
            let command = layout.command.clone().unwrap_or_default();
            let cli_id = cli_id_from_zellij_command(&command);
            ZellijAdoptCandidate {
                zellij_session: session.to_string(),
                zellij_pane_id: pane_id,
                title: pane.title.clone().unwrap_or_else(|| {
                    format!("{} {}", command, layout.args.join(" "))
                        .trim()
                        .to_string()
                }),
                cwd: layout.cwd.unwrap_or_default(),
                cli_id,
                cli_pid: None,
                pane_cols: pane
                    .pane_content_columns
                    .or(pane.pane_columns)
                    .and_then(|v| u16::try_from(v).ok()),
                pane_rows: pane
                    .pane_content_rows
                    .or(pane.pane_rows)
                    .and_then(|v| u16::try_from(v).ok()),
            }
        })
        .collect()
}

pub(crate) fn should_auto_fork_on_restore(quiet_restart: bool) -> bool {
    !quiet_restart
}

pub(crate) fn session_zellij_target(session: &Session) -> String {
    session
        .adopted_from
        .as_ref()
        .and_then(|adopted| adopted.zellij_session.clone())
        .unwrap_or_else(|| {
            format!(
                "beam-{}",
                &session.session_id[..8.min(session.session_id.len())]
            )
        })
}

pub(crate) fn reconcile_restored_sessions_with<FZ>(
    sessions: &mut HashMap<String, Session>,
    quiet_restart: bool,
    has_zellij_session: FZ,
) -> Vec<Session>
where
    FZ: Fn(&str) -> bool,
{
    let mut restore_candidates = Vec::new();
    for session in sessions.values_mut() {
        if session.status != SessionStatus::Active {
            continue;
        }
        let is_live = has_zellij_session(&session_zellij_target(session));

        if is_live {
            session.worker_pid = None;
            if should_auto_fork_on_restore(quiet_restart) {
                restore_candidates.push(session.clone());
            } else {
                session.terminal_url = None;
            }
        } else {
            session.status = SessionStatus::Closed;
            session.closed_at = Some(Utc::now());
            session.worker_pid = None;
            session.terminal_url = None;
        }
    }
    restore_candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn should_auto_fork_on_restore_matches_quiet_restart_gate() {
        assert!(should_auto_fork_on_restore(false));
        assert!(!should_auto_fork_on_restore(true));
    }

    #[test]
    fn reconcile_restored_sessions_closes_missing_zellij_sessions() {
        let mut missing_zellij = make_session("zellij-missing");
        missing_zellij.status = SessionStatus::Active;
        missing_zellij.closed_at = None;
        missing_zellij.worker_pid = Some(12);
        missing_zellij.terminal_url = Some("http://127.0.0.1:4".to_string());

        let mut sessions = HashMap::from([(missing_zellij.session_id.clone(), missing_zellij)]);
        let restore = reconcile_restored_sessions_with(&mut sessions, false, |_target| false);

        assert!(restore.is_empty());
        assert_eq!(sessions["zellij-missing"].status, SessionStatus::Closed);
        assert!(sessions["zellij-missing"].closed_at.is_some());
        assert_eq!(sessions["zellij-missing"].worker_pid, None);
        assert_eq!(sessions["zellij-missing"].terminal_url, None);
    }

    #[test]
    fn reconcile_restored_sessions_respects_quiet_restart() {
        let mut zellij_session = make_session("zellij-live");
        zellij_session.status = SessionStatus::Active;
        zellij_session.closed_at = None;
        zellij_session.worker_pid = Some(23);
        zellij_session.terminal_url = Some("http://127.0.0.1:4".to_string());

        let mut eager_sessions =
            HashMap::from([(zellij_session.session_id.clone(), zellij_session.clone())]);
        let eager_restore =
            reconcile_restored_sessions_with(&mut eager_sessions, false, |_target| true);
        assert_eq!(eager_restore.len(), 1);
        assert!(
            eager_restore
                .iter()
                .any(|session| session.session_id == "zellij-live")
        );
        assert_eq!(eager_sessions["zellij-live"].status, SessionStatus::Active);
        assert_eq!(eager_sessions["zellij-live"].worker_pid, None);
        assert_eq!(
            eager_sessions["zellij-live"].terminal_url,
            Some("http://127.0.0.1:4".to_string())
        );

        let mut quiet_sessions =
            HashMap::from([(zellij_session.session_id.clone(), zellij_session)]);
        let quiet_restore =
            reconcile_restored_sessions_with(&mut quiet_sessions, true, |_target| true);
        assert!(quiet_restore.is_empty());
        assert_eq!(quiet_sessions["zellij-live"].status, SessionStatus::Active);
        assert_eq!(quiet_sessions["zellij-live"].worker_pid, None);
        assert_eq!(quiet_sessions["zellij-live"].terminal_url, None);
    }

    #[test]
    fn zellij_cli_id_detection_is_command_based() {
        assert_eq!(cli_id_from_zellij_command("/usr/bin/codex"), "codex");
        assert_eq!(cli_id_from_zellij_command("/usr/bin/traex"), "traex");
        assert_eq!(cli_id_from_zellij_command("claude"), "claude-code");
        assert_eq!(
            cli_id_from_zellij_command("/home/u/.kimi-code/bin/kimi"),
            "kimi"
        );
        assert_eq!(cli_id_from_zellij_command("custom-tool"), "custom-tool");
    }

    #[test]
    fn zellij_adopt_candidates_join_layout_and_panes_by_order() {
        let layouts = vec![
            ZellijLayoutPane {
                command: Some("codex".to_string()),
                cwd: Some("/repo".to_string()),
                args: vec!["--foo".to_string()],
            },
            ZellijLayoutPane {
                command: Some("hermes".to_string()),
                cwd: Some("/repo/other".to_string()),
                args: vec![],
            },
        ];
        let panes = vec![
            ZellijPaneProbe {
                id: 1,
                is_plugin: false,
                is_floating: false,
                title: Some("codex pane".to_string()),
                pane_content_columns: Some(120),
                pane_content_rows: Some(40),
                pane_columns: None,
                pane_rows: None,
            },
            ZellijPaneProbe {
                id: 2,
                is_plugin: false,
                is_floating: false,
                title: None,
                pane_content_columns: None,
                pane_content_rows: None,
                pane_columns: Some(100),
                pane_rows: Some(30),
            },
        ];
        let candidates = join_zellij_adopt_candidates("my-session", layouts, panes);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].zellij_session, "my-session");
        assert_eq!(candidates[0].zellij_pane_id, "terminal_1");
        assert_eq!(candidates[0].cli_id, "codex");
        assert_eq!(candidates[0].cwd, "/repo");
        assert_eq!(candidates[1].zellij_pane_id, "terminal_2");
        assert_eq!(candidates[1].cli_id, "hermes");
        assert_eq!(candidates[1].cwd, "/repo/other");
    }

    // ── cwd parsing & inheritance tests ──

    #[test]
    fn parse_body_pane_inherits_layout_cwd_node_param() {
        let body = r#"layout {
    cwd "/repo"
    tab name="Tab #1" {
        pane command="codex" {
        }
    }
}"#;
        let panes = parse_dump_layout_body(body);
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].cwd.as_deref(), Some("/repo"));
        assert_eq!(panes[0].command.as_deref(), Some("codex"));
    }

    #[test]
    fn parse_body_pane_cwd_attr_overrides_layout_cwd() {
        let body = r#"layout {
    cwd "/repo"
    tab name="Tab #1" {
        pane cwd="/project" command="codex" {
        }
    }
}"#;
        let panes = parse_dump_layout_body(body);
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].cwd.as_deref(), Some("/project"));
    }

    #[test]
    fn parse_body_pane_cwd_child_node_overrides_layout_cwd() {
        let body = r#"layout {
    cwd "/repo"
    tab name="Tab #1" {
        pane command="codex" {
            cwd "/override"
        }
    }
}"#;
        let panes = parse_dump_layout_body(body);
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].cwd.as_deref(), Some("/override"));
    }

    #[test]
    fn parse_body_no_cwd_anywhere_is_none() {
        let body = r#"layout {
    tab name="Tab #1" {
        pane command="codex" {
        }
    }
}"#;
        let panes = parse_dump_layout_body(body);
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].cwd, None);
    }

    #[test]
    fn parse_body_multiple_panes_inherit_same_layout_cwd() {
        let body = r#"layout {
    cwd "/shared"
    tab name="Tab #1" {
        pane command="codex" {
        }
        pane command="hermes" {
        }
    }
}"#;
        let panes = parse_dump_layout_body(body);
        assert_eq!(panes.len(), 2);
        assert_eq!(panes[0].cwd.as_deref(), Some("/shared"));
        assert_eq!(panes[1].cwd.as_deref(), Some("/shared"));
    }

    #[test]
    fn parse_body_pane_no_own_cwd_no_layout_cwd_is_none() {
        let body = r#"layout {
    tab name="Tab #1" {
        pane command="codex" {
        }
    }
}"#;
        let panes = parse_dump_layout_body(body);
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].cwd, None);
        assert_eq!(panes[0].command.as_deref(), Some("codex"));
    }
}
