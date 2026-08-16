pub(crate) use std::sync::Arc;
pub(crate) use std::sync::LazyLock;
pub(crate) use std::sync::Mutex as StdMutex;
pub(crate) use std::time::Duration;
pub(crate) use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use ab_glyph::{Font, FontVec, PxScale, ScaleFont, point};
pub(crate) use anyhow::{Context, Result};
pub(crate) use beam_core::{
    BeamPaths, CliUsageLimitKind, CliUsageLimitState, DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS,
    DaemonToWorker, DisplayMode, InitConfig, ScreenAnalyzerConfig, ScreenStatus, TermActionKey,
    TranscriptChoice, TuiPromptOption, WorkerToDaemon,
};
pub(crate) use image::{ColorType, ImageBuffer, ImageEncoder, Rgba, codecs::png::PngEncoder};
pub(crate) use reqwest::multipart::{Form, Part};
pub(crate) use reqwest::{Client, header::HeaderMap};
pub(crate) use serde::Deserialize;
pub(crate) use sha2::{Digest, Sha256};
pub(crate) use tokio::io::AsyncWriteExt;
pub(crate) use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
pub(crate) use tracing::{debug, info, warn};
pub(crate) use unicode_width::UnicodeWidthChar;
pub(crate) use uuid::Uuid;

pub(crate) use crate::adapter::CliAdapter;
pub(crate) use crate::adapter::ResolveOutcome;
pub(crate) use crate::adapter::{TUI_READY_TIMEOUT, wait_for_tui_ready};
pub(crate) use crate::backend::{SessionBackend, SpawnOpts, ZellijBackend, ZellijObserveBackend};

mod analyzer;
mod coordinator;
mod coordinator_runtime;
mod grok_prompts;
mod launch;
#[cfg(test)]
mod launch_live;
mod run_loop;
mod screenshot;
mod tui;

pub(crate) use analyzer::*;
pub(crate) use coordinator::*;
pub(crate) use coordinator_runtime::coordinator_loop;
#[cfg(test)]
pub(crate) use run_loop::maybe_inject_term;
pub use run_loop::run;
pub(crate) use run_loop::send_message;
pub(crate) use screenshot::*;
pub(crate) use tui::*;
