use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use anyhow::{Context, Result};
use beam_core::{FinalOutputKind, InitConfig};

use crate::backend::SessionBackend;

#[derive(Debug, Clone, Default)]
pub struct SubmitResult {
    pub submitted: bool,
    pub cli_session_id: Option<String>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PollResult {
    pub cli_session_id: Option<String>,
    pub final_output: Option<String>,
    pub final_output_kind: Option<FinalOutputKind>,
    pub final_output_user_text: Option<String>,
    pub adopt_preamble: Option<(String, String)>,
    pub prompt_ready: bool,
}

#[derive(Debug, Clone)]
pub enum PendingTurnKind {
    Remote,
    Local { user_text: String },
    LocalHeadless,
}

#[derive(Debug, Clone)]
pub struct SpawnSpec {
    pub bin: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum AdapterKind {
    Claude(ClaudeState),
    Codex(CodexState),
    OpenCode(OpenCodeState),
    Gemini(GeminiState),
    CoCo(CoCoState),
    Hermes(HermesState),
    Antigravity(AntigravityState),
}

#[derive(Debug, Clone)]
pub struct CliAdapter {
    pub kind: AdapterKind,
}

#[derive(Debug, Clone)]
pub struct ClaudeState {
    pub data_dir: PathBuf,
    pub session_jsonl: PathBuf,
    pub cli_pid: Option<u32>,
    pub cli_cwd: String,
    pub cli_session_id: Option<String>,
    pub transcript_offset: u64,
    pub pending_tail: String,
    pub pending_final_text: Option<String>,
    pub pending_final_since: Option<Instant>,
    pub emitted_final_text: Option<String>,
    pub adopt_mode: bool,
    pub adopt_restored_from_metadata: bool,
    pub adopt_preamble_emitted: bool,
    pub pending_remote_user_inputs: VecDeque<String>,
    pub active_turn: Option<PendingTurnKind>,
}

#[derive(Debug, Clone)]
pub struct CodexState {
    pub home_dir: PathBuf,
    pub history_path: PathBuf,
    pub rollout_path: Option<PathBuf>,
    pub cli_pid: Option<u32>,
    pub cli_session_id: Option<String>,
    pub transcript_offset: u64,
    pub pending_tail: String,
    pub emitted_final_text: Option<String>,
    pub adopt_mode: bool,
    pub adopt_restored_from_metadata: bool,
    pub adopt_preamble_emitted: bool,
    pub pending_remote_user_inputs: VecDeque<String>,
    pub active_turn: Option<PendingTurnKind>,
}

#[derive(Debug, Clone, Default)]
pub struct OpenCodeState {
    pub data_dir: PathBuf,
    pub expected_session_id: Option<String>,
    pub working_dir: String,
    pub cli_session_id: Option<String>,
    pub transcript_offset: u64,
    pub emitted_final_text: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct GeminiState;

#[derive(Debug, Clone, Default)]
pub struct CoCoState {
    pub history_path: PathBuf,
    pub cli_session_id: Option<String>,
    pub transcript_offset: u64,
    pub pending_tail: String,
    pub emitted_final_text: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct HermesState;

#[derive(Debug, Clone, Default)]
pub struct AntigravityState {
    pub history_path: PathBuf,
    pub cli_session_id: Option<String>,
    pub transcript_offset: u64,
    pub pending_tail: String,
    pub emitted_final_text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JsonlDrain {
    pub lines: Vec<String>,
    pub new_offset: u64,
    pub pending_tail: String,
}

impl CliAdapter {
    pub fn from_init(init: &InitConfig) -> Result<Self> {
        crate::adapters::create_adapter(init)
    }

    pub fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
        crate::adapters::build_spawn_spec(self, init)
    }

    pub fn on_spawned(&mut self, child_pid: Option<u32>) {
        crate::adapters::on_spawned(self, child_pid);
    }

    pub async fn write_input(
        &mut self,
        backend: &dyn SessionBackend,
        content: &str,
    ) -> Result<SubmitResult> {
        crate::adapters::write_input(self, backend, content).await
    }

    pub fn poll(&mut self) -> Result<PollResult> {
        crate::adapters::poll(self)
    }
}

pub fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

pub fn drain_jsonl(path: &Path, from_offset: u64, pending_tail: &str) -> Result<JsonlDrain> {
    if !path.exists() {
        return Ok(JsonlDrain {
            lines: Vec::new(),
            new_offset: 0,
            pending_tail: pending_tail.to_string(),
        });
    }
    let size = file_size(path);
    let start = if size < from_offset { 0 } else { from_offset };
    if size == start {
        return Ok(JsonlDrain {
            lines: Vec::new(),
            new_offset: start,
            pending_tail: pending_tail.to_string(),
        });
    }
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    file.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let text = format!("{}{}", pending_tail, String::from_utf8_lossy(&buf));
    let Some(last_nl) = text.rfind('\n') else {
        return Ok(JsonlDrain {
            lines: Vec::new(),
            new_offset: start,
            pending_tail: text,
        });
    };
    let complete = &text[..last_nl];
    let tail = text[last_nl + 1..].to_string();
    let lines = complete
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    Ok(JsonlDrain {
        lines,
        new_offset: size - tail.len() as u64,
        pending_tail: tail,
    })
}

pub fn normalize_history_text(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

#[cfg(test)]
pub(crate) fn home_test_lock() -> &'static Mutex<()> {
    static HOME_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    HOME_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

pub fn realpath_cwd(cwd: &str) -> String {
    std::fs::canonicalize(cwd)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| cwd.to_string())
}

pub fn is_uuid_like(value: &str) -> bool {
    let parts = value.split('-').collect::<Vec<_>>();
    matches!(parts.as_slice(), [a, b, c, d, e] if a.len() == 8 && b.len() == 4 && c.len() == 4 && d.len() == 4 && e.len() == 12)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "beam-common-{}-{}",
            name,
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn drain_jsonl_preserves_partial_tail() {
        let path = temp_path("drain.jsonl");
        fs::write(&path, b"{\"a\":1}\n{\"b\":2}").unwrap();
        let drain = drain_jsonl(&path, 0, "").unwrap();
        assert_eq!(drain.lines, vec!["{\"a\":1}".to_string()]);
        assert_eq!(drain.pending_tail, "{\"b\":2}".to_string());
        let _ = fs::remove_file(path);
    }
}
