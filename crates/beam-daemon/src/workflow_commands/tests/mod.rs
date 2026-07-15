//! Shared test fixtures and submodule declarations for workflow_commands tests.

use super::*;
use crate::tests::test_helpers::*;
use beam_core::{
    BeamPaths, BootstrapWorkflowRunInput, EventLog, WaitResolution, bootstrap_workflow_run,
    read_run_snapshot,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Create a temporary paths root unique to the test label.
fn temp_paths(label: &str) -> BeamPaths {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    BeamPaths::from_root(std::env::temp_dir().join(format!(
        "beam-wf-cmds-{label}-{nanos}-{}",
        std::process::id()
    )))
}

/// Build a minimal AppState for test use.
fn make_state(paths: &BeamPaths) -> crate::AppState {
    let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
    crate::AppState {
        paths: paths.clone(),
        started_at: chrono::Utc::now(),
        sessions: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        workers: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        attempt_resumes: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        shutdown: std::sync::Arc::new(tokio::sync::Mutex::new(Some(_shutdown_tx))),
        options: crate::RunOptions {
            worker_exe: std::path::PathBuf::from("/bin/true"),
        },
        http: reqwest::Client::new(),
        config: beam_core::Config::default(),
        bots: std::sync::Arc::new(std::collections::HashMap::new()),
        lark_tokens: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        chat_mode_cache: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        recent_lark_events: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        inflight_final_output_turns: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashSet::new(),
        )),
        workflow_progress_cards: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        ask_pending: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        grant_pending: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        pending_creates: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        dashboard_token: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        external_host: std::sync::Arc::new(tokio::sync::RwLock::new("localhost".to_string())),
    }
}

// Submodules grouped by cohesive behavior scenario
mod approval;
mod approval_card;
mod cancel;
mod dashboard;
mod retry_task;
mod text_command;
