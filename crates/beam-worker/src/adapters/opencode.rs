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
use beam_core::{FinalOutputKind, InitConfig};

use crate::adapter::{OpenCodeState, PollResult, ResolveOutcome, SpawnSpec, SubmitResult};
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

// ---- state creation ----

pub fn create_state(init: &InitConfig) -> OpenCodeState {
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

// ---- spawn spec ----

pub fn build_spawn_spec(_state: &OpenCodeState, init: &InitConfig) -> SpawnSpec {
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

// ---- write input ----

pub async fn write_input(
    state: &mut OpenCodeState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    let source = match wait_for_source(state).await {
        ResolveOutcome::Found(source) => source,
        ResolveOutcome::Ambiguous { candidates, reason } => {
            // Try screen vs transcript disambiguation before giving up.
            match disambiguate_by_screen(state, backend, &candidates).await {
                Ok(Some(source)) => source,
                Ok(None) | Err(_) => {
                    return Ok(SubmitResult {
                        submitted: false,
                        cli_session_id: state.cli_session_id.clone(),
                        failure_reason: Some(reason),
                    });
                }
            }
        }
        ResolveOutcome::NotFound { reason } => {
            return Ok(SubmitResult {
                submitted: false,
                cli_session_id: state.cli_session_id.clone(),
                failure_reason: Some(reason),
            });
        }
    };
    let base_offset = current_opencode_session_offset(&source)?;
    state.cli_session_id = Some(source.session_id.clone());
    state.expected_session_id = Some(source.session_id.clone());

    backend.send_text(content).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.send_enter().await?;
    for attempt in 0..4 {
        tokio::time::sleep(Duration::from_millis(800)).await;
        if opencode_submit_confirmed(&source, base_offset, content)? {
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: state.cli_session_id.clone(),
                ..Default::default()
            });
        }
        if attempt < 3 {
            backend.send_enter().await?;
        }
    }
    Ok(SubmitResult {
        submitted: false,
        cli_session_id: state.cli_session_id.clone(),
        failure_reason: Some("OpenCode transcript did not confirm submit".to_string()),
    })
}

// ---- poll ----

pub fn poll(state: &mut OpenCodeState) -> Result<PollResult> {
    let source = match current_source(state) {
        ResolveOutcome::Found(source) => source,
        ResolveOutcome::Ambiguous { .. } | ResolveOutcome::NotFound { .. } => {
            // Do not auto-bind cli_session_id on ambiguous / not-found.
            return Ok(PollResult {
                cli_session_id: state.cli_session_id.clone(),
                ..Default::default()
            });
        }
    };

    let drain = drain_opencode_session(&source, state.transcript_offset)?;
    state.transcript_offset = drain.new_offset;
    if state.cli_session_id.is_none() {
        state.cli_session_id = Some(source.session_id.clone());
    }

    let mut result = PollResult {
        cli_session_id: state.cli_session_id.clone(),
        ..Default::default()
    };

    for event in drain.events {
        if event.kind != "assistant_final" {
            continue;
        }
        if !event.text.is_empty() && state.emitted_final_text.as_deref() != Some(&event.text) {
            state.emitted_final_text = Some(event.text.clone());
            result.final_output = Some(event.text);
            result.final_output_kind = Some(FinalOutputKind::Bridge);
            result.prompt_ready = true;
        }
    }

    Ok(result)
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
