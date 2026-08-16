//! Directory selection card for new Feishu sessions.
//!
//! When a new Feishu message would create a new agent session, instead of
//! immediately starting the worker, we present a directory selection card.
//! The user must pick a working directory under the bot's root working dir
//! before the session starts.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use beam_core::SessionScope;

pub mod card;
pub mod recent;
pub mod scan;
pub mod validation;

// Re-export public API so existing callers via `dir_select::` continue to work.
pub use card::{build_dir_select_card, build_dir_session_starting_card};
pub(crate) use recent::load_pending_creates;
pub use recent::{
    build_recent_dir_key, get_recent_dirs, load_recent_dirs, prune_expired_pending_creates,
    record_recent_dir, save_recent_dirs,
};
pub use scan::{
    determine_root_working_dir, filter_dirs, find_best_match, match_dirs, resolve_dir,
    scan_candidate_dirs, tokenize_keywords,
};
pub use validation::is_valid_candidate;
// Types are defined above; they are automatically re-exported as pub items
// of this module.

// --- Constants ---

pub(crate) const MAX_SCAN_DEPTH: usize = 3;
pub(crate) const MAX_SCAN_CANDIDATES: usize = 500;
pub(crate) const MAX_RECENT_DIRS: usize = 10;
pub(crate) const MAX_RECOMMENDED_DIRS: usize = 8;
/// Maximum number of directory buttons rendered in the card.
/// Beyond this, the user should use the select_static dropdown.
pub(crate) const MAX_BUTTON_DIRS: usize = 40;
/// Maximum number of options in the select_static dropdown.
pub(crate) const MAX_SELECT_DIRS: usize = 150;
/// TTL for pending create entries (30 minutes in milliseconds).
/// Entries older than this are pruned and the user must send a new message.
pub const PENDING_CREATE_TTL_MS: i64 = 30 * 60 * 1000;

pub(crate) const SKIP_DIR_NAMES: &[&str] = &[
    ".git",
    ".beam",
    "target",
    "node_modules",
    ".venv",
    "__pycache__",
    ".DS_Store",
    "dist",
    "build",
    "vendor",
    "bin",
    "obj",
    ".svn",
    ".hg",
    ".idea",
    ".vscode",
    ".cache",
    ".npm",
    ".yarn",
    ".next",
    ".nuxt",
    "coverage",
    ".tox",
    ".eggs",
    ".mypy_cache",
    ".pytest_cache",
];

// --- Data Structures ---

/// Pending session creation context, stored in memory until the user picks a working dir.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingCreateSession {
    pub pending_id: String,
    pub lark_app_id: String,
    pub chat_id: String,
    pub chat_type: Option<String>,
    pub message_id: String,
    pub anchor: String,
    pub scope: SessionScope,
    /// Feishu thread_id (omt_*), stable topic identifier for session matching.
    #[serde(default)]
    pub thread_id: Option<String>,
    /// Root message ID (from Feishu root_id field) used for reply and quote hint.
    /// Falls back to message_id when not available.
    #[serde(default)]
    pub root_id: Option<String>,
    pub title: String,
    pub text: String,
    pub sender_open_id: Option<String>,
    pub sender_type: Option<String>,
    #[serde(default)]
    pub locale: Option<String>,
    pub parent_id: Option<String>,
    /// Serialized Vec<LarkEventMention>
    #[serde(default)]
    pub mentions_json: String,
    pub quota_key: Option<String>,
    pub created_at: i64,
    // Bot info for session creation
    pub cli_id: String,
    pub cli_bin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_slice: Option<String>,
    #[serde(default)]
    pub cli_args: Vec<String>,
    pub root_working_dir: String,
    /// All scanned candidate dirs (relative paths from root)
    pub candidate_dirs: Vec<String>,
    /// The card's message_id so we can update it later
    #[serde(default)]
    pub card_message_id: Option<String>,
}

/// A single entry in the recent directories store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentDirEntry {
    pub dir: String,
    pub used_at: String,
}

/// Persistent store of recently used directories per key.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecentDirsStore {
    pub entries: HashMap<String, Vec<RecentDirEntry>>,
}
