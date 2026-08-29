use super::*;
use crate::adapter::test_support::{home_test_lock, set_home, temp_home, test_init};
use async_trait::async_trait;
use beam_core::{AdoptedFrom, BackendKind};
use std::fs::{self, create_dir_all};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const SAMPLE_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

struct GrokHomeGuard {
    previous: Option<String>,
}

impl GrokHomeGuard {
    fn isolate() -> Self {
        let previous = std::env::var("GROK_HOME").ok();
        unsafe { std::env::remove_var("GROK_HOME") };
        Self { previous }
    }
}

impl Drop for GrokHomeGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe { std::env::set_var("GROK_HOME", value) },
            None => unsafe { std::env::remove_var("GROK_HOME") },
        }
    }
}

fn isolate_home(prefix: &str) -> (PathBuf, impl Drop, impl Drop, impl Drop) {
    let lock = home_test_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let home = temp_home(prefix);
    let home_guard = set_home(&home);
    let grok_home_guard = GrokHomeGuard::isolate();
    (home, lock, home_guard, grok_home_guard)
}

fn init_for(working_dir: &str) -> InitConfig {
    InitConfig {
        session_id: SAMPLE_SESSION.to_string(),
        working_dir: working_dir.to_string(),
        cli_bin: "grok".to_string(),
        ..test_init("grok")
    }
}

fn updates_path(home: &Path, working_dir: &str, session_id: &str) -> PathBuf {
    home.join(".grok")
        .join("sessions")
        .join(encode_cwd(working_dir))
        .join(session_id)
        .join(UPDATES_FILE)
}

fn append_updates(path: &Path, lines: &[String]) {
    if let Some(parent) = path.parent() {
        create_dir_all(parent).unwrap();
    }
    let mut content = fs::read_to_string(path).unwrap_or_default();
    for line in lines {
        content.push_str(line);
        content.push('\n');
    }
    fs::write(path, content).unwrap();
}

fn event_line(session_id: &str, update: Value) -> String {
    serde_json::json!({
        "timestamp": 1,
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": update,
        }
    })
    .to_string()
}

fn user_chunk(session_id: &str, text: &str) -> String {
    event_line(
        session_id,
        serde_json::json!({
            "sessionUpdate": "user_message_chunk",
            "content": {"type": "text", "text": text},
        }),
    )
}

fn agent_chunk(session_id: &str, text: &str) -> String {
    event_line(
        session_id,
        serde_json::json!({
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": text},
        }),
    )
}

fn turn_completed(session_id: &str, reason: &str) -> String {
    serde_json::json!({
        "timestamp": 1,
        "method": "_x.ai/session/update",
        "params": {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "turn_completed",
                "stop_reason": reason,
            }
        }
    })
    .to_string()
}

#[derive(Clone, Default)]
struct RecordingBackend {
    updates_path: PathBuf,
    buffer: Arc<Mutex<String>>,
    append_on_enter: bool,
    calls: Arc<Mutex<Vec<String>>>,
}

impl RecordingBackend {
    fn new(updates_path: PathBuf, append_on_enter: bool) -> Self {
        Self {
            updates_path,
            buffer: Arc::new(Mutex::new(String::new())),
            append_on_enter,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl SessionBackend for RecordingBackend {
    async fn spawn(
        &self,
        _bin: &str,
        _args: &[String],
        _opts: crate::backend::SpawnOpts,
    ) -> Result<()> {
        Ok(())
    }

    async fn send_text(&self, text: &str) -> Result<()> {
        self.calls.lock().unwrap().push(format!("text:{text}"));
        self.buffer.lock().unwrap().push_str(text);
        Ok(())
    }

    async fn send_enter(&self) -> Result<()> {
        self.calls.lock().unwrap().push("enter".to_string());
        if self.append_on_enter {
            let content = {
                let mut buffer = self.buffer.lock().unwrap();
                let content = buffer.clone();
                buffer.clear();
                content
            };
            if !content.is_empty() {
                append_updates(&self.updates_path, &[user_chunk(SAMPLE_SESSION, &content)]);
            }
        }
        Ok(())
    }

    async fn send_special_keys(&self, keys: &[String]) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("keys:{}", keys.join(",")));
        if keys.iter().any(|key| key == "M-Enter") {
            self.buffer.lock().unwrap().push('\n');
        }
        Ok(())
    }

    async fn paste_text(&self, text: &str) -> Result<()> {
        self.send_text(text).await
    }
    async fn write_raw(&self, _text: &str) -> Result<()> {
        Ok(())
    }
    async fn raw_input(&self, _text: &str) -> Result<()> {
        Ok(())
    }
    async fn capture_viewport(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn capture_current_screen(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn is_alive(&self) -> Result<bool> {
        Ok(true)
    }
    async fn child_pid(&self) -> Result<Option<u32>> {
        Ok(None)
    }
    async fn kill(&self) -> Result<()> {
        Ok(())
    }
    async fn destroy_session(&self) -> Result<()> {
        Ok(())
    }
    async fn cursor_position(&self) -> Result<Option<(u16, u16)>> {
        Ok(None)
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        drop(tx);
        rx
    }
}

#[test]
fn encode_cwd_matches_grok_session_group_names() {
    assert_eq!(encode_cwd("/home/huangyu"), "%2Fhome%2Fhuangyu");
    assert_eq!(decode_cwd("%2Fhome%2Fhuangyu"), "/home/huangyu");
    assert_eq!(encode_cwd("/tmp/beam grok"), "%2Ftmp%2Fbeam%20grok");
}

#[test]
fn build_spawn_spec_adds_session_id_and_does_not_inject_static_flags() {
    let init = init_for("/tmp");
    let spec = GrokState::default().build_spawn_spec(&init);
    assert_eq!(spec.bin, "grok");
    assert!(!spec.args.iter().any(|arg| arg == "--always-approve"));
    assert!(!spec.args.iter().any(|arg| arg == "--no-alt-screen"));
    let session_pos = spec
        .args
        .iter()
        .position(|arg| arg == "--session-id")
        .unwrap();
    assert_eq!(spec.args[session_pos + 1], SAMPLE_SESSION);
    assert!(!spec.args.iter().any(|arg| arg == "--resume"));
}

#[test]
fn build_spawn_spec_passes_cli_args_through() {
    let init = InitConfig {
        cli_args: vec![
            "--always-approve".to_string(),
            "--no-alt-screen".to_string(),
        ],
        ..init_for("/tmp")
    };
    let spec = GrokState::default().build_spawn_spec(&init);
    assert_eq!(
        spec.args,
        vec![
            "--session-id",
            SAMPLE_SESSION,
            "--always-approve",
            "--no-alt-screen",
        ]
    );
}

#[test]
fn build_spawn_spec_respects_model_and_resume() {
    let init = InitConfig {
        resume: true,
        cli_session_id: Some("01aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee".to_string()),
        model: Some("grok-4".to_string()),
        cli_args: vec!["--no-alt-screen".to_string()],
        ..init_for("/tmp")
    };
    let spec = GrokState::default().build_spawn_spec(&init);
    assert!(!spec.args.iter().any(|arg| arg == "--always-approve"));
    assert!(!spec.args.iter().any(|arg| arg == "--session-id"));
    let resume_pos = spec.args.iter().position(|arg| arg == "--resume").unwrap();
    assert_eq!(
        spec.args[resume_pos + 1],
        "01aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee"
    );
    let model_pos = spec.args.iter().position(|arg| arg == "--model").unwrap();
    assert_eq!(spec.args[model_pos + 1], "grok-4");
    assert!(spec.args.iter().any(|arg| arg == "--no-alt-screen"));
}

#[test]
fn adopt_does_not_treat_beam_session_id_as_grok_id() {
    let init = InitConfig {
        adopted_from: Some(AdoptedFrom {
            backend_kind: BackendKind::Zellij,
            tmux_target: None,
            zellij_session: Some("sess".to_string()),
            zellij_pane_id: Some("pane".to_string()),
            herdr_workspace_id: None,
            herdr_pane_id: None,
            original_cli_pid: 1,
            session_id: None,
            cli_id: Some("grok".to_string()),
            cwd: "/tmp".to_string(),
            pane_cols: None,
            pane_rows: None,
        }),
        cli_session_id: None,
        ..init_for("/tmp")
    };
    let state = state_from_init(&init);
    assert!(state.cli_session_id.is_none());
}

#[test]
fn poll_emits_final_text_after_end_turn_and_dedupes() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-poll");
    let working_dir = "/tmp/beam-grok-work";
    let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
    append_updates(
        &updates,
        &[
            user_chunk(SAMPLE_SESSION, "hello grok"),
            agent_chunk(SAMPLE_SESSION, "Grok final reply"),
            turn_completed(SAMPLE_SESSION, "end_turn"),
        ],
    );
    let mut state = state_from_init(&init_for(working_dir));

    let first = state.poll().expect("poll");
    assert_eq!(first.final_output.as_deref(), Some("Grok final reply"));
    assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
    assert!(first.prompt_ready);
    assert_eq!(first.cli_session_id.as_deref(), Some(SAMPLE_SESSION));

    let second = state.poll().expect("poll again");
    assert!(second.final_output.is_none());
    assert!(!second.prompt_ready);
}

#[test]
fn poll_ignores_intermediate_agent_text_until_turn_completes() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-midturn");
    let working_dir = "/tmp/beam-grok-work";
    let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
    append_updates(
        &updates,
        &[
            user_chunk(SAMPLE_SESSION, "question"),
            agent_chunk(SAMPLE_SESSION, "working on it"),
        ],
    );
    let mut state = state_from_init(&init_for(working_dir));

    let result = state.poll().expect("poll");
    assert!(result.final_output.is_none());
    assert!(!result.prompt_ready);
}

#[test]
fn poll_emits_only_last_agent_chunk_on_end_turn() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-join");
    let working_dir = "/tmp/beam-grok-work";
    let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
    append_updates(
        &updates,
        &[
            agent_chunk(SAMPLE_SESSION, "working on it"),
            agent_chunk(SAMPLE_SESSION, "done"),
            turn_completed(SAMPLE_SESSION, "max_tokens"),
        ],
    );
    let mut state = state_from_init(&init_for(working_dir));
    let skipped = state.poll().expect("poll skipped");
    assert!(skipped.final_output.is_none());

    append_updates(
        &updates,
        &[
            agent_chunk(SAMPLE_SESSION, "working on it"),
            agent_chunk(SAMPLE_SESSION, "done"),
            turn_completed(SAMPLE_SESSION, "end_turn"),
        ],
    );
    let emitted = state.poll().expect("poll last chunk");
    assert_eq!(emitted.final_output.as_deref(), Some("done"));
}

#[test]
fn poll_recovers_after_truncation_and_re_emits() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-truncate");
    let working_dir = "/tmp/beam-grok-work";
    let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
    append_updates(
        &updates,
        &[
            user_chunk(SAMPLE_SESSION, "question"),
            agent_chunk(SAMPLE_SESSION, "first reply"),
            turn_completed(SAMPLE_SESSION, "end_turn"),
        ],
    );
    let mut state = state_from_init(&init_for(working_dir));
    let first = state.poll().expect("poll");
    assert_eq!(first.final_output.as_deref(), Some("first reply"));

    fs::write(&updates, "").unwrap();
    append_updates(
        &updates,
        &[
            agent_chunk(SAMPLE_SESSION, "first reply"),
            turn_completed(SAMPLE_SESSION, "end_turn"),
        ],
    );
    let second = state.poll().expect("poll after truncation");
    assert_eq!(second.final_output.as_deref(), Some("first reply"));
}

#[test]
fn poll_picks_latest_session_matching_work_dir() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-resolve");
    let working_dir = "/tmp/beam-grok-work";
    let other_dir = "/tmp/beam-grok-other";
    let old_id = "11111111-1111-1111-1111-111111111111";
    let other_id = "22222222-2222-2222-2222-222222222222";
    let old = updates_path(&home, working_dir, old_id);
    append_updates(
        &old,
        &[
            agent_chunk(old_id, "old"),
            turn_completed(old_id, "end_turn"),
        ],
    );
    let other = updates_path(&home, other_dir, other_id);
    append_updates(
        &other,
        &[
            agent_chunk(other_id, "foreign"),
            turn_completed(other_id, "end_turn"),
        ],
    );
    let mut state = state_from_init(&InitConfig {
        session_id: "session-test".to_string(),
        cli_session_id: None,
        ..init_for(working_dir)
    });

    let result = state.poll().expect("poll");
    assert_eq!(result.final_output.as_deref(), Some("old"));
    assert_eq!(result.cli_session_id.as_deref(), Some(old_id));
}

#[test]
fn poll_does_not_latch_older_session_when_assigned_id_missing() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-nolatch");
    let working_dir = "/tmp/beam-grok-work";
    let old_id = "11111111-1111-1111-1111-111111111111";
    append_updates(
        &updates_path(&home, working_dir, old_id),
        &[
            agent_chunk(old_id, "old reply"),
            turn_completed(old_id, "end_turn"),
        ],
    );
    let mut state = state_from_init(&init_for(working_dir));
    assert_eq!(state.cli_session_id.as_deref(), Some(SAMPLE_SESSION));

    let result = state.poll().expect("poll");
    assert!(result.final_output.is_none());
    assert_eq!(result.cli_session_id.as_deref(), Some(SAMPLE_SESSION));
    assert!(state.transcript_path.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn write_input_confirms_submit_when_prompt_recorded() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-submit");
    let working_dir = "/tmp/beam-grok-work";
    let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
    append_updates(&updates, &[]);
    let mut state = state_from_init(&init_for(working_dir));
    let backend = RecordingBackend::new(updates.clone(), true);

    let result = state
        .write_input(&backend, "hello grok")
        .await
        .expect("write input");
    assert!(result.submitted);
    assert_eq!(result.failure_reason, None);
    assert_eq!(result.cli_session_id.as_deref(), Some(SAMPLE_SESSION));
    assert!(backend.calls().iter().any(|call| call == "enter"));
}

#[tokio::test(flavor = "current_thread")]
async fn write_input_fails_when_transcript_does_not_confirm() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-nosubmit");
    let working_dir = "/tmp/beam-grok-work";
    let updates = updates_path(&home, working_dir, SAMPLE_SESSION);
    append_updates(&updates, &[]);
    let mut state = state_from_init(&init_for(working_dir));
    let backend = RecordingBackend::new(updates.clone(), false);

    let result = state
        .write_input(&backend, "hello grok")
        .await
        .expect("write input");
    assert!(!result.submitted);
    assert!(
        result
            .failure_reason
            .as_deref()
            .unwrap_or("")
            .contains("did not accept")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn write_input_does_not_confirm_against_foreign_session() {
    let (home, _lock, _home_guard, _grok_home) = isolate_home("beam-grok-foreign");
    let working_dir = "/tmp/beam-grok-work";
    let old_id = "11111111-1111-1111-1111-111111111111";
    append_updates(
        &updates_path(&home, working_dir, old_id),
        &[user_chunk(old_id, "hello grok")],
    );
    let mut state = state_from_init(&init_for(working_dir));
    let backend = RecordingBackend::new(updates_path(&home, working_dir, old_id), false);

    let result = state
        .write_input(&backend, "hello grok")
        .await
        .expect("write input");
    assert!(!result.submitted);
    assert_eq!(result.cli_session_id.as_deref(), Some(SAMPLE_SESSION));
}

#[test]
fn user_text_matches_plain_and_wrapped_prompts() {
    assert!(user_text_matches("hello grok", "hello grok"));
    assert!(user_text_matches(
        "<user_message>\nhello grok\n</user_message>\n\n<session_id>x</session_id>",
        "hello grok"
    ));
    assert!(!user_text_matches("hello grok", "other"));
}

fn has_command(name: &str) -> bool {
    std::process::Command::new("which")
        .arg(name)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

struct LiveGuard {
    zellij_session: String,
    working_dir: PathBuf,
    grok_session_root: Option<PathBuf>,
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("zellij")
            .args(["delete-session", &self.zellij_session, "-f"])
            .output();
        let _ = fs::remove_dir_all(&self.working_dir);
        if let Some(root) = &self.grok_session_root {
            let _ = fs::remove_dir_all(root);
        }
    }
}

// cargo test -p beam-worker live_grok -- --ignored --nocapture
#[tokio::test(flavor = "current_thread")]
#[ignore = "live test: requires locally installed and authenticated `grok` and `zellij`"]
async fn live_grok_submit_and_poll_final_output() {
    if !has_command("grok") || !has_command("zellij") {
        eprintln!("skipping live test: `grok` or `zellij` not found in PATH");
        return;
    }

    let short = &Uuid::new_v4().to_string()[..8];
    let working_dir = std::env::temp_dir().join(format!("beam-grok-live-{short}"));
    create_dir_all(&working_dir).expect("create live working dir");
    let zellij_session = format!("beam-grok-live-{short}");
    let mut guard = LiveGuard {
        zellij_session: zellij_session.clone(),
        working_dir: working_dir.clone(),
        grok_session_root: None,
    };

    let session_id = Uuid::new_v4().to_string();
    let init = InitConfig {
        session_id: session_id.clone(),
        working_dir: working_dir.display().to_string(),
        prompt: String::new(),
        ..init_for(&working_dir.display().to_string())
    };
    let mut state = state_from_init(&init);
    let spec = state.build_spawn_spec(&init);
    assert!(spec.args.iter().any(|arg| arg == "--session-id"));

    let backend = crate::backend::ZellijBackend::new(zellij_session);
    backend
        .spawn(
            &spec.bin,
            &spec.args,
            crate::backend::SpawnOpts {
                cwd: init.working_dir.clone(),
                cols: 120,
                rows: 40,
                env: vec![],
            },
        )
        .await
        .expect("spawn grok through zellij backend");

    let mut ready = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let viewport = backend.capture_viewport().await.unwrap_or_default();
        if viewport.to_ascii_lowercase().contains("grok") {
            ready = true;
            break;
        }
    }
    assert!(ready, "grok TUI did not show Grok chrome within 60s");

    let submit = state
        .write_input(&backend, "reply with exactly: BEAM_GROK_OK")
        .await
        .expect("write input to live grok");
    assert!(
        submit.submitted,
        "live submit was not confirmed: {:?}",
        submit.failure_reason
    );
    assert!(submit.cli_session_id.is_some());

    let mut final_output = None;
    for _ in 0..90 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let result = state.poll().expect("poll live grok");
        if result.final_output.is_some() {
            final_output = result.final_output;
            break;
        }
    }
    let final_output = final_output.expect("grok did not produce a final output within 90s");
    assert!(
        final_output.contains("BEAM_GROK_OK"),
        "unexpected final output: {final_output}"
    );

    if let Some(path) = &state.transcript_path {
        guard.grok_session_root = path.parent().and_then(Path::parent).map(Path::to_path_buf);
    }
}
