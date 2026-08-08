use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    Adapter, PendingTurnKind, PollResult, ResolveOutcome, SpawnSpec, SubmitResult,
    TranscriptCursor, confirm_submit_loop, file_size, normalize_history_text, realpath_cwd,
};
use crate::backend::SessionBackend;

const CLAUDE_SUBMIT_MARKERS: [&str; 2] = [
    "\"role\":\"user\",\"content\":\"",
    "\"operation\":\"enqueue\"",
];

#[derive(Debug, Clone)]
pub(crate) struct ClaudeState {
    data_dir: PathBuf,
    session_jsonl: PathBuf,
    cli_pid: Option<u32>,
    cli_cwd: String,
    cli_session_id: Option<String>,
    cursor: TranscriptCursor,
    pending_final_text: Option<String>,
    pending_final_since: Option<Instant>,
    adopt_mode: bool,
    adopt_restored_from_metadata: bool,
    adopt_preamble_emitted: bool,
    pending_remote_user_inputs: VecDeque<String>,
    active_turn: Option<PendingTurnKind>,
}

fn state_from_init(init: &InitConfig) -> ClaudeState {
    let data_dir = PathBuf::from(
        std::env::var("CLAUDE_CONFIG_DIR")
            .unwrap_or_else(|_| format!("{}/.claude", std::env::var("HOME").unwrap_or_default())),
    );
    let session_jsonl = claude_jsonl_path_for_session(
        init.cli_session_id.as_deref().unwrap_or(&init.session_id),
        &init.working_dir,
        &data_dir,
    );
    ClaudeState {
        data_dir,
        session_jsonl,
        cli_pid: None,
        cli_cwd: init.working_dir.clone(),
        cli_session_id: init.cli_session_id.clone(),
        cursor: TranscriptCursor::new(),
        pending_final_text: None,
        pending_final_since: None,
        adopt_mode: init.adopted_from.is_some(),
        adopt_restored_from_metadata: init.adopt_restored_from_metadata,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    }
}

pub fn create(init: &InitConfig) -> Box<dyn Adapter> {
    Box::new(state_from_init(init))
}

#[async_trait]
impl Adapter for ClaudeState {
    fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
        let mut args = Vec::new();
        if init.resume {
            args.push("--resume".to_string());
            args.push(
                init.cli_session_id
                    .clone()
                    .unwrap_or_else(|| init.session_id.clone()),
            );
        } else {
            args.push("--session-id".to_string());
            args.push(init.session_id.clone());
        }
        if !init.disable_cli_bypass {
            args.push("--dangerously-skip-permissions".to_string());
        }
        args.push("--settings".to_string());
        args.push(
            serde_json::json!({
                "skipDangerousModePermissionPrompt": true,
                "permissions": { "defaultMode": "bypassPermissions" },
            })
            .to_string(),
        );
        args.push("--disallowed-tools".to_string());
        args.push("EnterPlanMode,ExitPlanMode".to_string());
        args.extend(init.cli_args.clone());
        SpawnSpec {
            bin: init.cli_bin.clone(),
            args,
        }
    }

    async fn write_input(
        &mut self,
        backend: &dyn SessionBackend,
        content: &str,
    ) -> Result<SubmitResult> {
        if self.adopt_mode {
            self.pending_remote_user_inputs
                .push_back(normalize_history_text(content));
        }
        refresh_claude_pid_state(self);
        let base_byte = file_size(&self.session_jsonl);
        let lines: Vec<&str> = content.split('\n').collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.is_empty() {
                backend.send_text(line).await?;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            if index < lines.len() - 1 {
                backend.send_text("\\").await?;
                tokio::time::sleep(Duration::from_millis(30)).await;
                backend.send_enter().await?;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        backend.send_enter().await?;
        let confirmed = confirm_submit_loop(backend, || {
            claude_submit_seen(&self.session_jsonl, base_byte)
        })
        .await?;
        if confirmed {
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: self.cli_session_id.clone(),
                ..Default::default()
            });
        }
        Ok(SubmitResult {
            submitted: false,
            cli_session_id: self.cli_session_id.clone(),
            failure_reason: Some("Claude transcript did not confirm submit".to_string()),
        })
    }

    fn poll(&mut self) -> Result<PollResult> {
        refresh_claude_pid_state(self);
        if self.adopt_mode && !self.adopt_preamble_emitted {
            let baseline = if self.adopt_restored_from_metadata {
                None
            } else {
                baseline_claude_adopt_preamble(&self.session_jsonl)?
            };
            self.cursor.skip_to(file_size(&self.session_jsonl));
            self.pending_final_text = None;
            self.pending_final_since = None;
            self.adopt_preamble_emitted = true;
            return Ok(PollResult {
                cli_session_id: self.cli_session_id.clone(),
                final_output: None,
                final_output_kind: None,
                final_output_user_text: None,
                adopt_preamble: baseline,
                prompt_ready: false,
            });
        }
        let path = self.session_jsonl.clone();
        let lines = self.cursor.drain(&path)?;
        for line in &lines {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if let Some(role) = value.pointer("/message/role").and_then(Value::as_str) {
                match role {
                    "user" if self.adopt_mode => {
                        let text = extract_claude_message_text(&value);
                        if !text.trim().is_empty() {
                            let normalized = normalize_history_text(&text);
                            let kind = if self
                                .pending_remote_user_inputs
                                .front()
                                .map(|expected| *expected == normalized)
                                .unwrap_or(false)
                            {
                                let _ = self.pending_remote_user_inputs.pop_front();
                                PendingTurnKind::Remote
                            } else {
                                PendingTurnKind::Local { user_text: text }
                            };
                            self.active_turn = Some(kind);
                        }
                    }
                    "assistant" => {
                        let text = extract_claude_assistant_text(&value);
                        if !text.is_empty() {
                            if self.adopt_mode && self.active_turn.is_none() {
                                self.active_turn = Some(PendingTurnKind::LocalHeadless);
                            }
                            self.pending_final_text = Some(text);
                            self.pending_final_since = Some(Instant::now());
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut result = PollResult {
            cli_session_id: self.cli_session_id.clone(),
            final_output: None,
            final_output_kind: None,
            final_output_user_text: None,
            adopt_preamble: None,
            prompt_ready: false,
        };
        if let (Some(text), Some(since)) = (&self.pending_final_text, self.pending_final_since) {
            if since.elapsed() >= Duration::from_millis(1200) {
                if let Some(emitted) = self.cursor.emit_if_new(text) {
                    let kind = self.active_turn.take();
                    result.final_output = Some(emitted);
                    match kind {
                        Some(PendingTurnKind::Local { user_text }) => {
                            result.final_output_kind = Some(FinalOutputKind::LocalTurn);
                            result.final_output_user_text = Some(user_text);
                        }
                        Some(PendingTurnKind::LocalHeadless) => {
                            result.final_output_kind = Some(FinalOutputKind::LocalTurnHeadless);
                        }
                        _ => {}
                    }
                    result.prompt_ready = true;
                }
            }
        }
        Ok(result)
    }

    fn on_spawned(&mut self, child_pid: Option<u32>) {
        self.cli_pid = child_pid;
    }
}

fn claude_jsonl_path_for_session(session_id: &str, cwd: &str, data_dir: &Path) -> PathBuf {
    let project_hash = realpath_cwd(cwd)
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    data_dir
        .join("projects")
        .join(project_hash)
        .join(format!("{}.jsonl", session_id))
}

fn refresh_claude_pid_state(state: &mut ClaudeState) {
    let Some(pid) = state.cli_pid else {
        return;
    };
    if let ResolveOutcome::Found((session_id, cwd, session_jsonl)) =
        resolve_claude_session_via_pid(pid, &state.data_dir)
    {
        state.cli_session_id = Some(session_id);
        state.session_jsonl = session_jsonl;
        state.cli_cwd = cwd;
    }
}

/// Resolve a Claude session's metadata via its PID.
///
/// Reads `<data_dir>/sessions/<pid>.json` and returns the
/// `(session_id, cwd, session_jsonl_path)` triple, or
/// [`ResolveOutcome::NotFound`] with a reason string.
fn resolve_claude_session_via_pid(
    pid: u32,
    data_dir: &Path,
) -> ResolveOutcome<(String, String, PathBuf)> {
    let path = data_dir.join("sessions").join(format!("{}.json", pid));
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) => r,
        Err(e) => {
            return ResolveOutcome::NotFound {
                reason: format!("cannot read pid session file {}: {}", path.display(), e),
            };
        }
    };
    let value = match serde_json::from_str::<Value>(&raw) {
        Ok(v) => v,
        Err(e) => {
            return ResolveOutcome::NotFound {
                reason: format!("invalid pid session json: {}", e),
            };
        }
    };
    let session_id = match value.get("sessionId").and_then(Value::as_str) {
        Some(s) => s.to_string(),
        None => {
            return ResolveOutcome::NotFound {
                reason: "sessionId not found in pid session file".to_string(),
            };
        }
    };
    let cwd = match value.get("cwd").and_then(Value::as_str) {
        Some(c) => c.to_string(),
        None => {
            return ResolveOutcome::NotFound {
                reason: "cwd not found in pid session file".to_string(),
            };
        }
    };
    let session_jsonl = claude_jsonl_path_for_session(&session_id, &cwd, data_dir);
    ResolveOutcome::Found((session_id, cwd, session_jsonl))
}

fn claude_submit_seen(path: &Path, from_byte: u64) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let size = file_size(path);
    if size <= from_byte {
        return Ok(false);
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(from_byte))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(CLAUDE_SUBMIT_MARKERS
        .iter()
        .any(|marker| text.contains(marker)))
}

fn extract_claude_assistant_text(value: &Value) -> String {
    let Some(content) = value.pointer("/message/content") else {
        return String::new();
    };
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    let Some(items) = content.as_array() else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|block| {
            if block.get("type").and_then(Value::as_str) == Some("text") {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn extract_claude_message_text(value: &Value) -> String {
    let Some(content) = value.pointer("/message/content") else {
        return String::new();
    };
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    let Some(items) = content.as_array() else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|block| {
            block
                .get("text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn baseline_claude_adopt_preamble(path: &Path) -> Result<Option<(String, String)>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)?;
    let mut pending_user: Option<String> = None;
    let mut latest_pair: Option<(String, String)> = None;
    for line in raw.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(role) = value.pointer("/message/role").and_then(Value::as_str) else {
            continue;
        };
        let text = extract_claude_message_text(&value);
        if text.trim().is_empty() {
            continue;
        }
        match role {
            "user" => pending_user = Some(text),
            "assistant" => {
                if let Some(user_text) = pending_user.take() {
                    latest_pair = Some((user_text, text));
                }
            }
            _ => {}
        }
    }
    Ok(latest_pair)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use beam_core::FinalOutputKind;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("beam-claude-{}-{}", name, uuid::Uuid::new_v4()))
    }

    #[test]
    fn extracts_claude_assistant_text_blocks() {
        let value = serde_json::json!({
            "message": {
                "content": [
                    { "type": "text", "text": "first" },
                    { "type": "tool_use", "name": "ignored" },
                    { "type": "text", "text": "second" }
                ]
            }
        });
        assert_eq!(extract_claude_assistant_text(&value), "first\n\nsecond");
    }

    #[test]
    fn emits_stable_assistant_text() {
        let path = temp_path("claude.jsonl");
        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"ready\"}]}}\n",
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = state.poll().unwrap();
        assert!(first.final_output.is_none());
        assert!(!first.prompt_ready);
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = state.poll().unwrap();
        assert_eq!(second.final_output.as_deref(), Some("ready"));
        assert!(second.prompt_ready);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_preamble_once_and_absorbs_history() {
        let path = temp_path("claude-adopt.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = state.poll().unwrap();
        assert_eq!(
            first.adopt_preamble,
            Some(("ask".to_string(), "answer".to_string()))
        );
        assert!(first.final_output.is_none());
        let second = state.poll().unwrap();
        assert!(second.adopt_preamble.is_none());
        assert!(second.final_output.is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_local_turn_when_user_text_is_not_from_daemon() {
        let path = temp_path("claude-adopt-local.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"local ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"local answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = state.poll().unwrap();
        assert!(first.final_output.is_none());
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = state.poll().unwrap();
        assert_eq!(second.final_output.as_deref(), Some("local answer"));
        assert_eq!(second.final_output_kind, Some(FinalOutputKind::LocalTurn));
        assert_eq!(second.final_output_user_text.as_deref(), Some("local ask"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_emits_headless_local_turn_when_assistant_arrives_first() {
        let path = temp_path("claude-adopt-headless.jsonl");
        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"headless answer\"}]}}\n",
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = state.poll().unwrap();
        assert!(first.final_output.is_none());
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = state.poll().unwrap();
        assert_eq!(second.final_output.as_deref(), Some("headless answer"));
        assert_eq!(
            second.final_output_kind,
            Some(FinalOutputKind::LocalTurnHeadless)
        );
        assert_eq!(second.final_output_user_text, None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_keeps_remote_turn_as_bridge_output() {
        let path = temp_path("claude-adopt-remote.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"remote ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"remote answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: true,
            pending_remote_user_inputs: VecDeque::from([crate::adapter::normalize_history_text(
                "remote ask",
            )]),
            active_turn: None,
        };
        let first = state.poll().unwrap();
        assert!(first.final_output.is_none());
        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let second = state.poll().unwrap();
        assert_eq!(second.final_output.as_deref(), Some("remote answer"));
        assert_eq!(second.final_output_kind, None);
        assert_eq!(second.final_output_user_text, None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adopt_restored_absorbs_history_without_preamble() {
        let path = temp_path("claude-adopt-restored.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"ask\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: true,
            adopt_restored_from_metadata: true,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let first = state.poll().unwrap();
        assert!(first.adopt_preamble.is_none());
        assert!(first.final_output.is_none());
        let second = state.poll().unwrap();
        assert!(second.adopt_preamble.is_none());
        assert!(second.final_output.is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bridge_turn_queue_complete_cycle() {
        let path = temp_path("claude-bridge-cycle.jsonl");
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: path.clone(),
            cli_pid: None,
            cli_cwd: ".".to_string(),
            cli_session_id: Some("sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };

        let round1 = state.poll().unwrap();
        assert!(round1.final_output.is_none());

        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 ask\"}]}}\n",
        )
        .unwrap();

        let round2 = state.poll().unwrap();
        assert!(round2.final_output.is_none());

        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 ask\"}]}}\n\
             {\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 answer\"}]}}\n",
        )
        .unwrap();

        let round3 = state.poll().unwrap();
        assert!(round3.final_output.is_none());

        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let round4 = state.poll().unwrap();
        assert_eq!(round4.final_output.as_deref(), Some("turn1 answer"));
        assert!(round4.prompt_ready);

        state.pending_final_text = None;
        state.pending_final_since = None;

        std::fs::write(
            &path,
            "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 ask\"}]}}\n\
             {\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"turn1 answer\"}]}}\n\
             {\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"turn2 ask\"}]}}\n\
             {\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"turn2 answer\"}]}}\n",
        )
        .unwrap();

        let round5 = state.poll().unwrap();
        assert!(round5.final_output.is_none());

        state.pending_final_since = Some(Instant::now() - Duration::from_millis(1300));
        let round6 = state.poll().unwrap();
        assert_eq!(round6.final_output.as_deref(), Some("turn2 answer"));

        let _ = std::fs::remove_file(path);
    }

    // ── pid-based strong anchoring ──────────────────────────────────

    fn make_project_hash(cwd: &str) -> String {
        use crate::adapter::realpath_cwd;
        realpath_cwd(cwd)
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect::<String>()
    }

    #[test]
    fn pid_anchoring_resolves_session_jsonl_from_pid() {
        // Set up mock data_dir:
        //   sessions/<pid>.json → { sessionId, cwd }
        //   projects/<hash>/<sessionId>.jsonl → transcript
        let cwd_dir = temp_path("claude-cwd");
        std::fs::create_dir_all(&cwd_dir).unwrap();
        let cwd_canonical = std::fs::canonicalize(&cwd_dir).unwrap();
        let cwd_str = cwd_canonical.display().to_string();

        let data_dir = temp_path("claude-data");
        let sessions_dir = data_dir.join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();
        let session_content = serde_json::json!({
            "sessionId": "real-sid-pid-anchor",
            "cwd": &cwd_str
        });
        std::fs::write(sessions_dir.join("99999.json"), session_content.to_string()).unwrap();

        let project_hash = make_project_hash(&cwd_str);
        let projects_dir = data_dir.join("projects").join(&project_hash);
        std::fs::create_dir_all(&projects_dir).unwrap();
        let real_jsonl = projects_dir.join("real-sid-pid-anchor.jsonl");
        std::fs::write(
            &real_jsonl,
            "{\"message\":{\"role\":\"user\",\"content\":\"hi from pid\"}}\n",
        )
        .unwrap();

        // State: wrong session_jsonl initially, but pid is set
        let mut state = ClaudeState {
            data_dir: data_dir.clone(),
            session_jsonl: data_dir
                .join("projects")
                .join("wrong-hash")
                .join("wrong-sid.jsonl"),
            cli_pid: Some(99999),
            cli_cwd: cwd_str.clone(),
            cli_session_id: None,
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };

        // poll calls refresh_claude_pid_state internally
        let _result = state.poll().unwrap();

        // After poll, the state should be corrected from pid
        assert_eq!(
            state.cli_session_id.as_deref(),
            Some("real-sid-pid-anchor"),
            "cli_session_id should be resolved from pid session file"
        );
        assert_eq!(
            state.session_jsonl, real_jsonl,
            "session_jsonl should point to the pid-resolved transcript"
        );
        assert_eq!(
            state.cli_cwd, cwd_str,
            "cli_cwd should be resolved from pid session file"
        );

        let _ = std::fs::remove_dir_all(&cwd_dir);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn pid_anchoring_is_noop_when_pid_is_none() {
        // When cli_pid is None, refresh_claude_pid_state should be a no-op
        // and the state should remain unchanged.
        let mut state = ClaudeState {
            data_dir: PathBuf::new(),
            session_jsonl: PathBuf::from("/original/path.jsonl"),
            cli_pid: None,
            cli_cwd: "/orig/cwd".to_string(),
            cli_session_id: Some("orig-sid".to_string()),
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: false,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };
        let session_jsonl_before = state.session_jsonl.clone();
        let cli_session_id_before = state.cli_session_id.clone();
        let cli_cwd_before = state.cli_cwd.clone();

        let _result = state.poll().unwrap();

        assert_eq!(
            state.session_jsonl, session_jsonl_before,
            "session_jsonl should not change when pid is None"
        );
        assert_eq!(
            state.cli_session_id, cli_session_id_before,
            "cli_session_id should not change when pid is None"
        );
        assert_eq!(
            state.cli_cwd, cli_cwd_before,
            "cli_cwd should not change when pid is None"
        );
    }

    #[test]
    fn adopt_poll_with_pid_uses_corrected_transcript_for_preamble() {
        let cwd_dir = temp_path("claude-preamble-cwd");
        std::fs::create_dir_all(&cwd_dir).unwrap();
        let cwd_canonical = std::fs::canonicalize(&cwd_dir).unwrap();
        let cwd_str = cwd_canonical.display().to_string();

        let data_dir = temp_path("claude-preamble-data");
        let sessions_dir = data_dir.join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();
        let session_content = serde_json::json!({
            "sessionId": "pid-session-preamble",
            "cwd": &cwd_str
        });
        std::fs::write(sessions_dir.join("11111.json"), session_content.to_string()).unwrap();

        let project_hash = make_project_hash(&cwd_str);
        let projects_dir = data_dir.join("projects").join(&project_hash);
        std::fs::create_dir_all(&projects_dir).unwrap();
        let real_jsonl = projects_dir.join("pid-session-preamble.jsonl");
        std::fs::write(
            &real_jsonl,
            concat!(
                "{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"adopt question\"}]}}\n",
                "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"adopt response\"}]}}\n"
            ),
        )
        .unwrap();

        let mut state = ClaudeState {
            data_dir: data_dir.clone(),
            // Intentionally wrong path — should be corrected by pid refresh
            session_jsonl: data_dir
                .join("projects")
                .join("deadbeef")
                .join("nope.jsonl"),
            cli_pid: Some(11111),
            cli_cwd: cwd_str.clone(),
            cli_session_id: None,
            cursor: TranscriptCursor::new(),
            pending_final_text: None,
            pending_final_since: None,
            adopt_mode: true,
            adopt_restored_from_metadata: false,
            adopt_preamble_emitted: false,
            pending_remote_user_inputs: VecDeque::new(),
            active_turn: None,
        };

        let first = state.poll().unwrap();
        // Preamble should come from the pid-corrected transcript
        assert_eq!(
            first.adopt_preamble,
            Some(("adopt question".to_string(), "adopt response".to_string())),
            "preamble should be from pid-resolved transcript"
        );
        assert!(first.final_output.is_none());

        // Second poll should absorb history (preamble already emitted)
        let second = state.poll().unwrap();
        assert!(second.adopt_preamble.is_none());
        assert!(second.final_output.is_none());

        let _ = std::fs::remove_dir_all(&cwd_dir);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
