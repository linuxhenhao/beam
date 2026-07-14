//! Data types for the opencode adapter module.

use std::path::PathBuf;

use serde::Deserialize;
use serde_json::Value;

use crate::adapter::ResolveOutcome;

// ---- disambiguation constants ----

/// Maximum chars from the screen tail used for scoring.
pub(crate) const SCREEN_TAIL_CHARS: usize = 500;
/// Maximum chars from the transcript tail used for scoring.
pub(crate) const TRANSCRIPT_TAIL_CHARS: usize = 1000;
/// How many recent text parts to read from a candidate transcript.
pub(crate) const TRANSCRIPT_TAIL_PARTS: usize = 20;
/// Minimum combined similarity score required to auto-select a candidate.
pub(crate) const MIN_DISAMBIGUATION_SCORE: f64 = 0.12;
/// Top score must be ≥ this ratio × second score to avoid near-ties.
pub(crate) const MIN_SCORE_LEAD_RATIO: f64 = 1.4;
/// Directory fallback should stay small and focus on active root sessions.
pub(crate) const OPENCODE_DIRECTORY_FALLBACK_LIMIT: usize = 10;

// ---- SQL query constants ----

/// Lookback window for cursor-based transcript queries (milliseconds).
pub(crate) const OPENCODE_CURSOR_LOOKBACK_MS: u64 = 5_000;

// ---- transcript source types ----

#[derive(Debug, Clone, Deserialize)]
pub struct OpenCodeTranscriptSource {
    pub db_path: PathBuf,
    pub session_id: String,
}

/// Resolution of a transcript source lookup for opencode sessions.
pub type OpenCodeSourceResolution = ResolveOutcome<OpenCodeTranscriptSource>;

// ---- internal row types from the opencode SQLite schema ----

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct OpenCodeSessionRow {
    pub(crate) id: String,
    pub(crate) directory: String,
    pub(crate) time_archived: Option<u64>,
    pub(crate) parent_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct OpenCodeMessageRow {
    pub(crate) message_id: String,
    pub(crate) session_id: String,
    pub(crate) message_time_created: Option<u64>,
    pub(crate) message_time_updated: Option<u64>,
    pub(crate) message_data: String,
    pub(crate) part_id: Option<String>,
    pub(crate) part_time_updated: Option<u64>,
    pub(crate) part_data: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupedMessage {
    pub(crate) id: String,
    pub(crate) session_id: String,
    pub(crate) time_created: u64,
    pub(crate) time_updated: u64,
    pub(crate) data: Value,
    pub(crate) parts: Vec<GroupedPart>,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupedPart {
    pub(crate) time_updated: u64,
    pub(crate) data: Value,
}

// ---- bridge event types ----

#[derive(Debug, Clone)]
pub struct OpenCodeBridgeEvent {
    #[allow(dead_code)]
    pub uuid: String,
    #[allow(dead_code)]
    pub timestamp_ms: u64,
    pub kind: String,
    pub text: String,
    #[allow(dead_code)]
    pub source_session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpenCodeDrainResult {
    pub events: Vec<OpenCodeBridgeEvent>,
    pub new_offset: u64,
}
