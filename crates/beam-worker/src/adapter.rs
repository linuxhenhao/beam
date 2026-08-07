use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};

use crate::backend::SessionBackend;

/// Unified resolver outcome for adopt/source resolution.
///
/// Expresses the three canonical resolution states used across adapters:
/// - [`ResolveOutcome::Found`]: exactly one matching resource.
/// - [`ResolveOutcome::NotFound`]: no matching resource.
/// - [`ResolveOutcome::Ambiguous`]: multiple candidates that cannot be
///   automatically disambiguated.
#[derive(Debug, Clone)]
pub enum ResolveOutcome<T> {
    Found(T),
    NotFound { reason: String },
    Ambiguous { candidates: Vec<T>, reason: String },
}

impl<T> ResolveOutcome<T> {
    /// Map the payload(s) through `f`, preserving the resolution state.
    pub fn map<U>(self, f: impl Fn(T) -> U) -> ResolveOutcome<U> {
        match self {
            ResolveOutcome::Found(value) => ResolveOutcome::Found(f(value)),
            ResolveOutcome::NotFound { reason } => ResolveOutcome::NotFound { reason },
            ResolveOutcome::Ambiguous { candidates, reason } => ResolveOutcome::Ambiguous {
                candidates: candidates.into_iter().map(f).collect(),
                reason,
            },
        }
    }
}

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

/// A transcript source candidate reported by
/// [`Adapter::resolve_transcript_source`] (e.g. an opencode session row).
#[derive(Debug, Clone)]
pub struct TranscriptSourceCandidate {
    pub session_id: String,
    pub db_path: PathBuf,
}

/// One AI coding CLI integration.
///
/// Implementations live in `crate::adapters::<name>` next to their state
/// struct and are registered in `crate::adapters::REGISTRY`; cross-crate CLI
/// metadata (setup label, bin candidates, resume support, ...) lives in
/// `beam_core::cli_specs::CLI_SPECS`. Only `build_spawn_spec`, `write_input`
/// and `poll` are required — capability hooks default to no-ops.
#[async_trait]
pub trait Adapter: Send + std::fmt::Debug {
    /// Build the binary + args used to spawn the CLI.
    fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec;

    /// Type `content` into the CLI and confirm the submit landed in the
    /// transcript. Implementations should reuse [`confirm_submit_loop`] for
    /// the confirm/retry policy and must not claim success without
    /// transcript confirmation.
    async fn write_input(
        &mut self,
        backend: &dyn SessionBackend,
        content: &str,
    ) -> Result<SubmitResult>;

    /// Drain the CLI transcript and report newly completed final output.
    fn poll(&mut self) -> Result<PollResult>;

    /// Called after the CLI process is spawned. Default: no-op.
    fn on_spawned(&mut self, _child_pid: Option<u32>) {}

    /// Optional capability: resolve the transcript source at init/adopt time
    /// (opencode). Returning `None` means the adapter has no resolvable
    /// transcript source; `Some(Err)` means resolution was attempted and
    /// failed (the runtime degrades it to `NotFound`).
    async fn resolve_transcript_source(
        &mut self,
        _backend: &dyn SessionBackend,
    ) -> Option<Result<ResolveOutcome<TranscriptSourceCandidate>>> {
        None
    }

    /// Optional capability: apply a user-picked transcript source after an
    /// ambiguous resolution. Returns true when the adapter applied it.
    fn set_transcript_source(&mut self, _cli_session_id: &str) -> bool {
        false
    }
}

/// Type-erased adapter handle used by the worker runtime.
#[derive(Debug)]
pub struct CliAdapter {
    inner: Box<dyn Adapter>,
}

impl CliAdapter {
    pub(crate) fn new(inner: Box<dyn Adapter>) -> Self {
        Self { inner }
    }

    pub fn from_init(init: &InitConfig) -> Result<Self> {
        crate::adapters::create_adapter(init)
    }

    pub fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
        self.inner.build_spawn_spec(init)
    }

    pub async fn write_input(
        &mut self,
        backend: &dyn SessionBackend,
        content: &str,
    ) -> Result<SubmitResult> {
        self.inner.write_input(backend, content).await
    }

    pub fn poll(&mut self) -> Result<PollResult> {
        self.inner.poll()
    }

    pub fn on_spawned(&mut self, child_pid: Option<u32>) {
        self.inner.on_spawned(child_pid);
    }

    pub async fn resolve_transcript_source(
        &mut self,
        backend: &dyn SessionBackend,
    ) -> Option<Result<ResolveOutcome<TranscriptSourceCandidate>>> {
        self.inner.resolve_transcript_source(backend).await
    }

    pub fn set_transcript_source(&mut self, cli_session_id: &str) -> bool {
        self.inner.set_transcript_source(cli_session_id)
    }
}

/// Incremental-read state for JSONL transcript files: byte offset, partial
/// line tail, and last emitted final text for same-text dedupe.
///
/// Replaces the `transcript_offset` / `pending_tail` / `emitted_final_text`
/// field trio that adapters used to carry (and mishandle) individually.
#[derive(Debug, Clone, Default)]
pub struct TranscriptCursor {
    offset: u64,
    pending_tail: String,
    emitted_final_text: Option<String>,
}

impl TranscriptCursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read complete lines appended since the last call. Resets offset, tail
    /// and dedupe state automatically when the file was truncated.
    pub fn drain(&mut self, path: &Path) -> Result<Vec<String>> {
        if file_size(path) < self.offset {
            self.offset = 0;
            self.pending_tail.clear();
            self.emitted_final_text = None;
        }
        let drain = drain_jsonl(path, self.offset, &self.pending_tail)?;
        self.offset = drain.new_offset;
        self.pending_tail = drain.pending_tail;
        Ok(drain.lines)
    }

    /// Jump to an absolute byte offset and drop any partial tail (used when
    /// baselining an adopted session to skip history).
    pub fn skip_to(&mut self, offset: u64) {
        self.offset = offset;
        self.pending_tail.clear();
    }

    /// Return `text` for emission unless it is empty or identical to the
    /// last emitted text.
    pub fn emit_if_new(&mut self, text: &str) -> Option<String> {
        if text.is_empty() || self.emitted_final_text.as_deref() == Some(text) {
            return None;
        }
        self.emitted_final_text = Some(text.to_string());
        Some(text.to_string())
    }

    /// Forget the last emitted text so an identical reply can be emitted
    /// again (call when a new user turn starts).
    pub fn reset_dedupe(&mut self) {
        self.emitted_final_text = None;
    }
}

/// Standard submit-confirmation policy shared by adapters.
///
/// Call after the adapter has typed the content and pressed Enter once:
/// polls `confirm` up to 4 times at 800ms intervals, re-sending Enter
/// between attempts. Returns `Ok(true)` as soon as `confirm` observes the
/// submitted text in the transcript; `Ok(false)` means the CLI never
/// confirmed — the caller must surface a failure reason instead of
/// claiming success.
pub async fn confirm_submit_loop(
    backend: &dyn SessionBackend,
    confirm: impl FnMut() -> Result<bool>,
) -> Result<bool> {
    confirm_submit_loop_with_interval(backend, confirm, Duration::from_millis(800)).await
}

/// [`confirm_submit_loop`] with a tunable poll interval (tests).
pub(crate) async fn confirm_submit_loop_with_interval(
    backend: &dyn SessionBackend,
    mut confirm: impl FnMut() -> Result<bool>,
    interval: Duration,
) -> Result<bool> {
    for attempt in 0..4 {
        tokio::time::sleep(interval).await;
        if confirm()? {
            return Ok(true);
        }
        if attempt < 3 {
            backend.send_enter().await?;
        }
    }
    Ok(false)
}

#[derive(Debug, Clone)]
pub struct JsonlDrain {
    pub lines: Vec<String>,
    pub new_offset: u64,
    pub pending_tail: String,
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

/// Shared test scaffolding for adapter unit tests: HOME isolation and a
/// ready-made [`InitConfig`] builder. Use instead of re-declaring
/// `HomeGuard` / `temp_home` / `test_init` per adapter.
#[cfg(test)]
pub(crate) mod test_support {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    use beam_core::{InitConfig, ScreenAnalyzerConfig};

    pub(crate) use super::home_test_lock;

    /// An [`InitConfig`] with placeholder values for `cli_id`; override
    /// fields with struct-update syntax (`..test_support::test_init("kimi")`).
    pub(crate) fn test_init(cli_id: &str) -> InitConfig {
        InitConfig {
            session_id: "session-test".to_string(),
            title: "title".to_string(),
            chat_id: "chat".to_string(),
            root_message_id: "root".to_string(),
            working_dir: "/tmp".to_string(),
            cli_id: cli_id.to_string(),
            cli_bin: cli_id.to_string(),
            cli_args: vec![],
            prompt: String::new(),
            resume: false,
            cli_session_id: None,
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            prompt_turn_id: None,
            owner_open_id: None,
            adopted_from: None,
            adopt_restored_from_metadata: false,
            screen_analyzer: ScreenAnalyzerConfig::default(),
            initial_prompt: None,
            model: None,
            locale: None,
            bot_name: None,
            bot_open_id: None,
            resume_session_id: None,
            disable_cli_bypass: false,
        }
    }

    pub(crate) fn temp_home(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}-{}", prefix, uuid::Uuid::new_v4()))
    }

    /// Restores the previous HOME on drop; pair with [`home_test_lock`].
    pub(crate) struct HomeGuard {
        old_home: Option<OsString>,
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            match &self.old_home {
                Some(home) => unsafe {
                    std::env::set_var("HOME", home);
                },
                None => unsafe {
                    std::env::remove_var("HOME");
                },
            }
        }
    }

    pub(crate) fn set_home(home: &Path) -> HomeGuard {
        let old_home = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", home);
        }
        HomeGuard { old_home }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("beam-common-{}-{}", name, uuid::Uuid::new_v4()))
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

    #[test]
    fn cursor_drains_new_lines_and_resets_on_truncation() {
        let path = temp_path("cursor.jsonl");
        fs::write(&path, "{\"a\":1}\n{\"b\":2}\n").unwrap();
        let mut cursor = TranscriptCursor::new();
        assert_eq!(cursor.drain(&path).unwrap().len(), 2);
        assert!(cursor.drain(&path).unwrap().is_empty());

        // Truncation: offset is past EOF, cursor restarts from 0.
        fs::write(&path, "{\"c\":3}\n").unwrap();
        assert_eq!(cursor.drain(&path).unwrap(), vec!["{\"c\":3}".to_string()]);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn cursor_emit_if_new_dedupes_until_reset() {
        let mut cursor = TranscriptCursor::new();
        assert_eq!(cursor.emit_if_new("hello"), Some("hello".to_string()));
        assert_eq!(cursor.emit_if_new("hello"), None);
        assert_eq!(cursor.emit_if_new(""), None);
        cursor.reset_dedupe();
        assert_eq!(cursor.emit_if_new("hello"), Some("hello".to_string()));
    }

    #[test]
    fn cursor_skip_to_drops_tail() {
        let path = temp_path("cursor-skip.jsonl");
        fs::write(&path, "{\"a\":1}\n{\"b\":2}").unwrap();
        let mut cursor = TranscriptCursor::new();
        assert_eq!(cursor.drain(&path).unwrap().len(), 1);
        cursor.skip_to(0);
        assert_eq!(cursor.drain(&path).unwrap().len(), 1);
        let _ = fs::remove_file(&path);
    }

    mod confirm_loop {
        use super::super::confirm_submit_loop_with_interval;
        use crate::backend::{SessionBackend, SpawnOpts};
        use anyhow::Result;
        use async_trait::async_trait;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        use tokio::sync::broadcast;

        struct EnterCounter {
            enters: AtomicUsize,
        }

        #[async_trait]
        impl SessionBackend for EnterCounter {
            async fn spawn(&self, _bin: &str, _args: &[String], _opts: SpawnOpts) -> Result<()> {
                unimplemented!()
            }
            async fn send_text(&self, _text: &str) -> Result<()> {
                unimplemented!()
            }
            async fn send_enter(&self) -> Result<()> {
                self.enters.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn send_special_keys(&self, _keys: &[String]) -> Result<()> {
                unimplemented!()
            }
            async fn paste_text(&self, _text: &str) -> Result<()> {
                unimplemented!()
            }
            async fn write_raw(&self, _text: &str) -> Result<()> {
                unimplemented!()
            }
            async fn raw_input(&self, _text: &str) -> Result<()> {
                unimplemented!()
            }
            async fn capture_viewport(&self) -> Result<String> {
                unimplemented!()
            }
            async fn capture_current_screen(&self) -> Result<String> {
                unimplemented!()
            }
            async fn is_alive(&self) -> Result<bool> {
                unimplemented!()
            }
            async fn child_pid(&self) -> Result<Option<u32>> {
                unimplemented!()
            }
            async fn kill(&self) -> Result<()> {
                unimplemented!()
            }
            async fn destroy_session(&self) -> Result<()> {
                unimplemented!()
            }
            async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
                unimplemented!()
            }
            fn subscribe(&self) -> broadcast::Receiver<String> {
                unimplemented!()
            }
        }

        #[tokio::test]
        async fn succeeds_on_first_confirm_without_extra_enter() {
            let backend = EnterCounter { enters: AtomicUsize::new(0) };
            let ok = confirm_submit_loop_with_interval(&backend, || Ok(true), Duration::from_millis(1))
                .await
                .unwrap();
            assert!(ok);
            assert_eq!(backend.enters.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn resends_enter_between_attempts() {
            let backend = EnterCounter { enters: AtomicUsize::new(0) };
            let calls = AtomicUsize::new(0);
            let ok = confirm_submit_loop_with_interval(
                &backend,
                || Ok(calls.fetch_add(1, Ordering::SeqCst) >= 2),
                Duration::from_millis(1),
            )
            .await
            .unwrap();
            assert!(ok);
            // failed attempts 0 and 1 each trigger one Enter resend
            assert_eq!(backend.enters.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn returns_false_after_all_attempts() {
            let backend = EnterCounter { enters: AtomicUsize::new(0) };
            let ok = confirm_submit_loop_with_interval(&backend, || Ok(false), Duration::from_millis(1))
                .await
                .unwrap();
            assert!(!ok);
            // 4 attempts, Enter resent after the first 3
            assert_eq!(backend.enters.load(Ordering::SeqCst), 3);
        }
    }
}
