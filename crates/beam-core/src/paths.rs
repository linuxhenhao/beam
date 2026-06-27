use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct BeamPaths {
    root: PathBuf,
}

impl BeamPaths {
    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn discover() -> Result<Self> {
        if let Ok(root) = env::var("BEAM_HOME") {
            return Ok(Self::from_root(root));
        }

        let home = env::var("HOME").context("HOME is not set and BEAM_HOME was not provided")?;
        Ok(Self::from_root(Path::new(&home).join(".beam")))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn run_dir(&self) -> PathBuf {
        self.root.join("run")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    pub fn sessions_dir(&self) -> PathBuf {
        self.root.join("sessions")
    }

    pub fn workflows_dir(&self) -> PathBuf {
        self.root.join("workflows")
    }

    pub fn workflow_runs_dir(&self) -> PathBuf {
        self.workflows_dir().join("runs")
    }

    pub fn workflow_run_dir(&self, run_id: &str) -> PathBuf {
        self.workflow_runs_dir().join(run_id)
    }

    pub fn config_toml(&self) -> PathBuf {
        self.root.join("config.toml")
    }

    pub fn bots_json(&self) -> PathBuf {
        self.root.join("bots.json")
    }

    pub fn connectors_json(&self) -> PathBuf {
        self.root.join("connectors.json")
    }

    pub fn webhook_master_key(&self) -> PathBuf {
        self.root.join("webhook-master.key")
    }

    pub fn webhook_secrets_json(&self) -> PathBuf {
        self.root.join("webhook-secrets.json")
    }

    pub fn trigger_logs_jsonl(&self) -> PathBuf {
        self.root.join("trigger-logs.jsonl")
    }

    pub fn schedules_json(&self) -> PathBuf {
        self.root.join("schedules.json")
    }

    pub fn observed_bots_dir(&self) -> PathBuf {
        self.root.join("observed-bots")
    }

    pub fn webhook_triggers_json(&self) -> PathBuf {
        self.root.join("webhook-triggers.json")
    }

    pub fn webhook_lifecycle_json(&self) -> PathBuf {
        self.root.join("webhook-lifecycle.json")
    }

    pub fn schedules_output_dir(&self) -> PathBuf {
        self.root.join("schedules-output")
    }

    pub fn runtime_state_json(&self) -> PathBuf {
        self.run_dir().join("daemon.json")
    }

    pub fn daemon_log(&self) -> PathBuf {
        self.logs_dir().join("daemon.log")
    }

    pub fn session_store_json(&self) -> PathBuf {
        self.sessions_dir().join("sessions.json")
    }

    pub fn frozen_cards_dir(&self) -> PathBuf {
        self.sessions_dir().join("frozen-cards")
    }

    pub fn frozen_cards_json(&self, session_id: &str) -> PathBuf {
        self.frozen_cards_dir().join(format!("{}.json", session_id))
    }

    pub fn pending_response_patches_dir(&self) -> PathBuf {
        self.sessions_dir().join("pending-response-patches")
    }

    pub fn pending_response_patch_json(&self, session_id: &str) -> PathBuf {
        self.pending_response_patches_dir()
            .join(format!("{}.json", session_id))
    }

    pub fn workflow_approval_cards_dir(&self) -> PathBuf {
        self.sessions_dir().join("workflow-approval-cards")
    }

    pub fn workflow_approval_cards_json(&self, run_id: &str) -> PathBuf {
        self.workflow_approval_cards_dir()
            .join(format!("{}.json", run_id))
    }

    pub fn attempt_resume_dir(&self, run_id: &str, activity_id: &str, attempt_id: &str) -> PathBuf {
        self.workflow_run_dir(run_id)
            .join("attempts")
            .join(activity_id)
            .join(attempt_id)
            .join("resumes")
    }

    pub fn attempt_resume_json(
        &self,
        run_id: &str,
        activity_id: &str,
        attempt_id: &str,
        resume_id: &str,
    ) -> PathBuf {
        self.attempt_resume_dir(run_id, activity_id, attempt_id)
            .join(resume_id)
            .join("resume.json")
    }

    pub fn worker_init_json(&self, session_id: &str) -> PathBuf {
        self.run_dir()
            .join(format!("worker-init-{}.json", session_id))
    }

    pub fn worker_wrapper_sh(&self, session_id: &str) -> PathBuf {
        self.run_dir()
            .join(format!("worker-wrapper-{}.sh", session_id))
    }

    pub fn cli_pid_markers_dir(&self) -> PathBuf {
        self.state_dir().join(".beam-cli-pids")
    }

    pub fn zellij_web_tokens_json(&self) -> PathBuf {
        self.state_dir().join("zellij-web-tokens.json")
    }

    pub fn workflow_progress_cards_json(&self) -> PathBuf {
        self.state_dir().join("workflow-progress-cards.json")
    }

    pub fn used_tickets_json(&self) -> PathBuf {
        self.state_dir().join("used-tickets.json")
    }

    pub fn ask_pending_json(&self) -> PathBuf {
        self.state_dir().join("ask-pending.json")
    }

    pub fn grant_pending_json(&self) -> PathBuf {
        self.state_dir().join("grant-pending.json")
    }

    pub fn pending_creates_json(&self) -> PathBuf {
        self.state_dir().join("pending-creates.json")
    }

    pub fn replay_nonces_json(&self) -> PathBuf {
        self.state_dir().join("replay-nonces.json")
    }

    pub fn rate_buckets_json(&self) -> PathBuf {
        self.state_dir().join("rate-buckets.json")
    }

    pub fn recent_lark_events_json(&self) -> PathBuf {
        self.state_dir().join("recent-lark-events.json")
    }

    pub fn final_output_retries_json(&self) -> PathBuf {
        self.state_dir().join("final-output-retries.json")
    }

    pub fn recent_dirs_json(&self) -> PathBuf {
        self.state_dir().join("recent-dirs.json")
    }
}
