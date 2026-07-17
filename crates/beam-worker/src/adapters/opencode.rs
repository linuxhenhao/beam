//! Adapter glue for the opencode CLI.
//!
//! This file is the public entry point for the `opencode` adapter module.
//! Implementation details are split into submodules under `opencode/`:
//! - `types` — data structures and constants
//! - `transcript` — SQLite queries and drain
//! - `source_resolution` — session discovery via PID, logs, directory
//! - `disambiguation` — screen vs transcript scoring

// Module declarations (submodule files live in opencode/).
mod disambiguation;
mod source_resolution;
mod transcript;
mod types;

#[cfg(test)]
#[path = "opencode/tests.rs"]
mod tests;

// Imports used by the adapter glue below.
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};

use crate::adapter::{
    Adapter, PollResult, ResolveOutcome, SpawnSpec, SubmitResult, TranscriptSourceCandidate,
    confirm_submit_loop,
};
use crate::backend::SessionBackend;

use self::disambiguation::disambiguate_by_screen;
use self::source_resolution::{current_source, wait_for_source};
use self::transcript::{
    current_opencode_session_offset, drain_opencode_session, opencode_submit_confirmed,
};
use self::types::OpenCodeSourceResolution;

// Re-exports for tests (only compiled when testing).
#[cfg(test)]
pub(crate) use self::disambiguation::*;
#[cfg(test)]
pub(crate) use self::source_resolution::*;
#[cfg(test)]
pub(crate) use self::transcript::*;

#[derive(Debug, Clone, Default)]
pub(crate) struct OpenCodeState {
    pub data_dir: PathBuf,
    pub expected_session_id: Option<String>,
    pub working_dir: String,
    pub cli_session_id: Option<String>,
    pub transcript_offset: u64,
    pub emitted_final_text: Option<String>,
    /// PID of the adopted CLI process, if any.
    /// When set and alive, directory-based candidate resolution
    /// picks the most recent session instead of raising Ambiguous.
    pub adopted_pid: Option<u32>,
}

fn state_from_init(init: &InitConfig) -> OpenCodeState {
    let home = std::env::var("HOME").unwrap_or_default();
    let data_dir = PathBuf::from(format!("{}/.local/share/opencode", home));
    let expected_session_id = init.cli_session_id.clone();
    let adopted_pid = init
        .adopted_from
        .as_ref()
        .and_then(|a| u32::try_from(a.original_cli_pid).ok());
    OpenCodeState {
        data_dir,
        expected_session_id,
        working_dir: init.working_dir.clone(),
        cli_session_id: init.cli_session_id.clone(),
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid,
    }
}

pub fn create(init: &InitConfig) -> Box<dyn Adapter> {
    Box::new(state_from_init(init))
}

#[async_trait]
impl Adapter for OpenCodeState {
    fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
        let mut args = Vec::new();
        if let Some(model) = &init.model {
            if !model.is_empty() {
                args.push("--model".to_string());
                args.push(model.clone());
            }
        }
        if let Some(prompt) = &init.initial_prompt {
            args.push("--prompt".to_string());
            args.push(prompt.clone());
        }
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
        let source = match wait_for_source(self).await {
            ResolveOutcome::Found(source) => source,
            ResolveOutcome::Ambiguous { candidates, reason } => {
                // Try screen vs transcript disambiguation before giving up.
                match disambiguate_by_screen(self, backend, &candidates).await {
                    Ok(Some(source)) => source,
                    Ok(None) | Err(_) => {
                        return Ok(SubmitResult {
                            submitted: false,
                            cli_session_id: self.cli_session_id.clone(),
                            failure_reason: Some(reason),
                        });
                    }
                }
            }
            ResolveOutcome::NotFound { reason } => {
                return Ok(SubmitResult {
                    submitted: false,
                    cli_session_id: self.cli_session_id.clone(),
                    failure_reason: Some(reason),
                });
            }
        };
        let base_offset = current_opencode_session_offset(&source)?;
        self.cli_session_id = Some(source.session_id.clone());
        self.expected_session_id = Some(source.session_id.clone());

        backend.send_text(content).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        backend.send_enter().await?;
        let confirmed =
            confirm_submit_loop(backend, || opencode_submit_confirmed(&source, base_offset, content))
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
            failure_reason: Some("OpenCode transcript did not confirm submit".to_string()),
        })
    }

    fn poll(&mut self) -> Result<PollResult> {
        let source = match current_source(self) {
            ResolveOutcome::Found(source) => source,
            ResolveOutcome::Ambiguous { .. } | ResolveOutcome::NotFound { .. } => {
                // Do not auto-bind cli_session_id on ambiguous / not-found.
                return Ok(PollResult {
                    cli_session_id: self.cli_session_id.clone(),
                    ..Default::default()
                });
            }
        };

        let drain = drain_opencode_session(&source, self.transcript_offset)?;
        self.transcript_offset = drain.new_offset;
        if self.cli_session_id.is_none() {
            self.cli_session_id = Some(source.session_id.clone());
        }

        let mut result = PollResult {
            cli_session_id: self.cli_session_id.clone(),
            ..Default::default()
        };

        for event in drain.events {
            if event.kind != "assistant_final" {
                continue;
            }
            if !event.text.is_empty() && self.emitted_final_text.as_deref() != Some(&event.text) {
                self.emitted_final_text = Some(event.text.clone());
                result.final_output = Some(event.text);
                result.final_output_kind = Some(FinalOutputKind::Bridge);
                result.prompt_ready = true;
            }
        }

        Ok(result)
    }

    async fn resolve_transcript_source(
        &mut self,
        backend: &dyn SessionBackend,
    ) -> Option<Result<ResolveOutcome<TranscriptSourceCandidate>>> {
        if self.cli_session_id.is_some() {
            return None;
        }
        Some(
            resolve_transcript_source(self, backend)
                .await
                .map(|resolution| {
                    resolution.map(|source| TranscriptSourceCandidate {
                        session_id: source.session_id,
                        db_path: source.db_path,
                    })
                }),
        )
    }

    fn set_transcript_source(&mut self, cli_session_id: &str) -> bool {
        self.expected_session_id = Some(cli_session_id.to_string());
        self.cli_session_id = Some(cli_session_id.to_string());
        true
    }
}

// ---- init-time transcript resolution ----

/// Resolve transcript source at init/adopt time, with screen disambiguation.
/// Returns candidates for user selection when automatic resolution fails.
pub(crate) async fn resolve_transcript_source(
    state: &OpenCodeState,
    backend: &dyn SessionBackend,
) -> Result<OpenCodeSourceResolution> {
    let resolution = current_source(state);
    match resolution {
        ResolveOutcome::Ambiguous { candidates, reason } => {
            match disambiguate_by_screen(state, backend, &candidates).await {
                Ok(Some(source)) => Ok(ResolveOutcome::Found(source)),
                _ => Ok(ResolveOutcome::Ambiguous { candidates, reason }),
            }
        }
        other => Ok(other),
    }
}
