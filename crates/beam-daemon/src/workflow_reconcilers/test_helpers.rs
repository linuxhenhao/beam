//! Test helpers shared across workflow_reconcilers test modules.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use beam_core::BeamPaths;

use crate::AppState;

/// Create a temporary BeamPaths rooted in a temp directory for test isolation.
pub(super) fn temp_paths(label: &str) -> BeamPaths {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    BeamPaths::from_root(std::env::temp_dir().join(format!(
        "beam-reconciler-{label}-{nanos}-{}",
        std::process::id()
    )))
}

/// Build a minimal AppState for testing, backed by `paths`.
#[allow(clippy::let_and_return)]
pub(super) fn make_state(paths: &BeamPaths) -> AppState {
    let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
    AppState {
        paths: paths.clone(),
        started_at: chrono::Utc::now(),
        sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        workers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        attempt_resumes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        shutdown: Arc::new(tokio::sync::Mutex::new(Some(_shutdown_tx))),
        options: crate::RunOptions {
            worker_exe: PathBuf::from("/bin/true"),
        },
        http: reqwest::Client::new(),
        config: beam_core::Config::default(),
        bots: Arc::new(HashMap::new()),
        lark_tokens: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        chat_mode_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        recent_lark_events: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        inflight_final_output_turns: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashSet::new(),
        )),
        workflow_progress_cards: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        ask_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        grant_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        pending_creates: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        dashboard_token: Arc::new(tokio::sync::Mutex::new(None)),
        api_token: std::sync::Arc::new(tokio::sync::RwLock::new(crate::ApiTokenState::for_test())),
        external_host: std::sync::Arc::new(tokio::sync::RwLock::new("localhost".to_string())),
    }
}
