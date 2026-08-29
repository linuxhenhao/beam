//! Herdr public id parsing and POSIX quoting helpers.
//!
//! Herdr returns JSON with `.result.workspace.workspace_id` /
//! `.result.tab.tab_id` / `.result.root_pane.pane_id` on create, and
//! workspace entries with `workspace_id` + `label` on list. These helpers are
//! pure so the JSON contract can be pinned in hermetic tests with fixtures
//! before a real `herdr` binary is available.

use anyhow::{Context, Result, bail};
use serde_json::Value;

/// The three ids that identify a managed Herdr terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdrIds {
    pub workspace_id: String,
    pub pane_id: String,
}

impl HerdrIds {
    pub fn workspace_pane(&self) -> (String, String) {
        (self.workspace_id.clone(), self.pane_id.clone())
    }
}

/// Read the ids from a `workspace create` JSON payload:
/// `.result.workspace.workspace_id` and `.result.root_pane.pane_id`.
pub(crate) fn parse_create_ids(payload: &str) -> Result<HerdrIds> {
    let value: Value = serde_json::from_str(payload).context("create payload is not JSON")?;
    let workspace_id = value
        .pointer("/result/workspace/workspace_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .context("missing .result.workspace.workspace_id")?;
    let pane_id = value
        .pointer("/result/root_pane/pane_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .context("missing .result.root_pane.pane_id")?;
    Ok(HerdrIds {
        workspace_id: workspace_id.to_string(),
        pane_id: pane_id.to_string(),
    })
}

/// A workspace entry from `workspace list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceEntry {
    pub(crate) workspace_id: String,
    pub(crate) label: Option<String>,
}

/// Parse `workspace list` JSON into entries. The payload layout is not pinned
/// across herdr versions, so both a top-level array and a `.result` wrapper
/// are accepted; entries are objects carrying `workspace_id` and `label`.
pub(crate) fn parse_workspace_list(payload: &str) -> Result<Vec<WorkspaceEntry>> {
    let value: Value =
        serde_json::from_str(payload).context("workspace list payload is not JSON")?;
    let items = value
        .pointer("/result/workspaces")
        .or_else(|| value.pointer("/workspaces"))
        .or_else(|| value.as_array().map(|_| &value))
        .context("workspace list JSON has no workspace array")?;
    let Some(items) = items.as_array() else {
        bail!("workspace list JSON array expected");
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(workspace_id) = item
            .get("workspace_id")
            .or_else(|| item.get("id"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        out.push(WorkspaceEntry {
            workspace_id: workspace_id.to_string(),
            label: item
                .get("label")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        });
    }
    Ok(out)
}

/// Find the workspace with the given label (e.g. `beam-{sid8}`).
pub(crate) fn workspace_by_label<'a>(
    entries: &'a [WorkspaceEntry],
    label: &str,
) -> Option<&'a WorkspaceEntry> {
    entries
        .iter()
        .find(|entry| entry.label.as_deref() == Some(label))
}

/// POSIX single-quote a string so it can be embedded in a `pane run` command
/// string. `'` becomes `'\''`; everything else is passed through.
pub(crate) fn posix_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "_-./:=+,".contains(c))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Join a `bin + args` launch spec into a single POSIX command string for
/// `herdr pane run`. The env authority stays in the launch-spec argv
/// (`/usr/bin/env KEY=VAL …` or `systemd-run …`), so no extra shell
/// interpolation happens here.
pub(crate) fn command_string(bin: &str, args: &[String]) -> String {
    let mut parts = vec![posix_quote(bin)];
    parts.extend(args.iter().map(|a| posix_quote(a)));
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_ids_reads_result_pointers() {
        let payload = r#"{"result":{"workspace":{"workspace_id":"w1"},"tab":{"tab_id":"t1"},"root_pane":{"pane_id":"w1:p1"}}}"#;
        let ids = parse_create_ids(payload).expect("create ids");
        assert_eq!(ids.workspace_id, "w1");
        assert_eq!(ids.pane_id, "w1:p1");
    }

    #[test]
    fn create_ids_missing_pointer_is_error() {
        assert!(parse_create_ids(r#"{"result":{}}"#).is_err());
        assert!(parse_create_ids("not json").is_err());
    }

    #[test]
    fn workspace_list_accepts_wrapped_and_flat_arrays() {
        let wrapped = r#"{"result":{"workspaces":[{"workspace_id":"w1","label":"beam-abc"},{"workspace_id":"w2","label":"mine"}]}}"#;
        let flat = r#"[{"workspace_id":"w1","label":"beam-abc"}]"#;
        let entries = parse_workspace_list(wrapped).expect("wrapped");
        assert_eq!(entries.len(), 2);
        assert_eq!(
            workspace_by_label(&entries, "beam-abc").map(|e| e.workspace_id.as_str()),
            Some("w1")
        );
        assert_eq!(
            workspace_by_label(&entries, "missing").map(|e| e.workspace_id.as_str()),
            None
        );
        let flat_entries = parse_workspace_list(flat).expect("flat");
        assert_eq!(flat_entries.len(), 1);
    }

    #[test]
    fn posix_quote_handles_spaces_quotes_and_empties() {
        assert_eq!(posix_quote(""), "''");
        assert_eq!(posix_quote("claude"), "claude");
        assert_eq!(
            posix_quote("--dangerously-skip-permissions"),
            "--dangerously-skip-permissions"
        );
        assert_eq!(posix_quote("two words"), "'two words'");
        assert_eq!(posix_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn command_string_joins_bin_and_args() {
        let args = vec![
            "--always-approve".to_string(),
            "--prompt".to_string(),
            "hello world".to_string(),
        ];
        let cmd = command_string("claude", &args);
        assert_eq!(cmd, "claude --always-approve --prompt 'hello world'");
    }
}
