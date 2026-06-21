//! Directory selection card for new Feishu sessions.
//!
//! When a new Feishu message would create a new agent session, instead of
//! immediately starting the worker, we present a directory selection card.
//! The user must pick a working directory under the bot's root working dir
//! before the session starts.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use beam_core::{BackendType, SessionScope};

// --- Constants ---

const MAX_SCAN_DEPTH: usize = 3;
const MAX_SCAN_CANDIDATES: usize = 500;
const MAX_RECENT_DIRS: usize = 10;
const MAX_RECOMMENDED_DIRS: usize = 8;
const MAX_SHOWN_DIRS: usize = 150; // cap for rendered directory choices
/// TTL for pending create entries (30 minutes in milliseconds).
/// Entries older than this are pruned and the user must send a new message.
pub const PENDING_CREATE_TTL_MS: i64 = 30 * 60 * 1000;

const SKIP_DIR_NAMES: &[&str] = &[
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
    pub title: String,
    pub text: String,
    pub sender_open_id: Option<String>,
    pub sender_type: Option<String>,
    pub parent_id: Option<String>,
    /// Serialized Vec<LarkEventMention>
    #[serde(default)]
    pub mentions_json: String,
    pub quota_key: Option<String>,
    pub created_at: i64,
    // Bot info for session creation
    pub cli_id: String,
    pub cli_bin: String,
    pub backend_type: BackendType,
    // Working directory info
    pub root_working_dir: String,
    /// All scanned candidate dirs (relative paths from root)
    pub candidate_dirs: Vec<String>,
    /// The card's message_id so we can update it later
    #[serde(default)]
    pub card_message_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentDirEntry {
    pub dir: String,
    pub used_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecentDirsStore {
    pub entries: HashMap<String, Vec<RecentDirEntry>>,
}

// --- Root Working Dir ---

/// Determine the root working directory for a bot.
/// Priority: bot.workingDir > daemon.working_dirs[0] > "."
pub fn determine_root_working_dir(
    bot_working_dir: Option<&str>,
    daemon_working_dirs: &[String],
) -> String {
    let raw = bot_working_dir
        .map(|s| s.to_string())
        .or_else(|| daemon_working_dirs.first().cloned())
        .unwrap_or_else(|| ".".to_string());
    expand_tilde(&raw)
}

fn expand_tilde(path: &str) -> String {
    if path.starts_with("~/") || path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            if path == "~" {
                return home;
            }
            return format!("{}/{}", home, &path[2..]);
        }
    }
    path.to_string()
}

// --- Directory Scanning ---

/// Scan the root directory for candidate subdirectories.
/// Returns relative paths (from root), including "." for root itself.
/// Skips common noise directories and limits depth/quantity.
pub fn scan_candidate_dirs(root: &Path) -> Vec<String> {
    let mut dirs: Vec<String> = Vec::new();
    // Include root itself
    dirs.push(".".to_string());
    scan_dirs_recursive(root, root, 1, &mut dirs);
    dirs
}

fn scan_dirs_recursive(base: &Path, current: &Path, depth: usize, dirs: &mut Vec<String>) {
    if depth > MAX_SCAN_DEPTH || dirs.len() >= MAX_SCAN_CANDIDATES {
        return;
    }

    let entries = match std::fs::read_dir(current) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        if dirs.len() >= MAX_SCAN_CANDIDATES {
            return;
        }
        let path = entry.path();
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Check file_type first: skip symlinks to prevent escaping root.
        // path.is_dir() follows symlinks, which could point outside root.
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_symlink() {
            continue;
        }
        if !ft.is_dir() || should_skip_dir(file_name) {
            continue;
        }
        // Compute relative path from base
        if let Ok(rel) = path.strip_prefix(base) {
            dirs.push(rel.to_string_lossy().to_string());
        }
        scan_dirs_recursive(base, &path, depth + 1, dirs);
    }
}

fn should_skip_dir(name: &str) -> bool {
    SKIP_DIR_NAMES.contains(&name) || name.starts_with('.')
}

// --- Directory Filtering & Matching ---

/// Tokenize a keyword string into individual words (split by whitespace).
pub fn tokenize_keywords(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .map(|s| s.to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Filter directories by keywords using AND matching (case-insensitive).
/// Returns dirs that match ALL keywords in the path.
pub fn match_dirs(dirs: &[String], keywords: &[&str]) -> Vec<String> {
    if keywords.is_empty() {
        return dirs.to_vec();
    }
    let lower_keywords: Vec<String> = keywords.iter().map(|k| k.to_lowercase()).collect();
    dirs.iter()
        .filter(|dir| {
            let lower_dir = dir.to_lowercase();
            lower_keywords.iter().all(|kw| lower_dir.contains(kw))
        })
        .cloned()
        .collect()
}

/// Filter directories by a single keyword search string (multi-word AND).
/// If the search string is empty, returns all dirs.
pub fn filter_dirs(dirs: &[String], search: &str) -> Vec<String> {
    let keywords = tokenize_keywords(search);
    let kw_refs: Vec<&str> = keywords.iter().map(|s| s.as_str()).collect();
    match_dirs(dirs, &kw_refs)
}

/// Find the best match from a list of directories given keywords.
/// Returns Some only when there is exactly ONE match (excluding "." root).
/// Multiple matches or zero matches → None (let user pick manually).
pub fn find_best_match(dirs: &[String], search: &str) -> Option<String> {
    let keywords = tokenize_keywords(search);
    if keywords.is_empty() {
        return None;
    }
    let kw_refs: Vec<&str> = keywords.iter().map(|s| s.as_str()).collect();
    let matched = match_dirs(dirs, &kw_refs);
    // Exclude root from match consideration
    let non_root: Vec<&String> = matched.iter().filter(|d| d.as_str() != ".").collect();
    if non_root.len() == 1 {
        Some(non_root[0].clone())
    } else {
        None
    }
}

// --- Security Validation ---

/// Check if a directory path (absolute or relative) is under the given root.
/// Uses pure path manipulation; does NOT require the paths to exist on disk.
/// Handles boundary cases like `/tmp/rootX` NOT being under `/tmp/root`,
/// and relative roots like `"."`.
pub fn is_dir_under_root(dir: &str, root: &str) -> bool {
    let root_path = Path::new(root);
    let dir_path = Path::new(dir);

    // Reject paths that attempt to escape via ".."
    if dir_path
        .components()
        .any(|c| c == std::path::Component::ParentDir)
    {
        return false;
    }

    // Reject absolute dir when root is relative (can't be under it)
    if dir_path.is_absolute() && !root_path.is_absolute() {
        return false;
    }

    // Normalize both paths (resolve ".", "..", etc.)
    let normalized_root = normalize_path(root_path);
    let normalized_dir = if dir_path.is_absolute() {
        normalize_path(dir_path)
    } else {
        normalize_path(&root_path.join(dir_path))
    };

    if normalized_dir == normalized_root {
        return true;
    }

    // If root is empty/current-dir (e.g., "."), accept any non-absolute,
    // non-escape relative path (already verified above).
    let root_str = normalized_root.to_string_lossy();
    if root_str.is_empty() {
        return true;
    }

    // Check that dir starts with root + separator
    let dir_str = normalized_dir.to_string_lossy();
    if dir_str.len() > root_str.len() {
        let remainder = &dir_str[root_str.len()..];
        remainder.starts_with(std::path::MAIN_SEPARATOR)
    } else {
        false
    }
}

/// Normalize a path by resolving components where possible.
/// For non-existing paths, this does a best-effort normalization
/// by collapsing ".." and "." components.
fn normalize_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                result.pop();
            }
            std::path::Component::CurDir => {}
            c => {
                result.push(c.as_os_str());
            }
        }
    }
    result
}

/// Check if a directory is a valid candidate (exists in candidate list and under root).
pub fn is_valid_candidate(dir: &str, root: &str, candidates: &[String]) -> bool {
    if !is_dir_under_root(dir, root) {
        return false;
    }
    // dir should be in the candidate list (or be root itself)
    candidates.contains(&dir.to_string()) || dir == "."
}

/// Resolve a relative dir (from candidate list) to an absolute path.
pub fn resolve_dir(root: &str, rel: &str) -> String {
    if rel == "." {
        root.to_string()
    } else {
        Path::new(root).join(rel).to_string_lossy().to_string()
    }
}

// --- Recent Directories ---

/// Build a key for the recent dirs map.
/// Format: {app_id}:{chat_id}:{operator}
pub fn build_recent_dir_key(app_id: &str, chat_id: &str, operator: Option<&str>) -> String {
    match operator {
        Some(op) if !op.is_empty() => format!("{}:{}:{}", app_id, chat_id, op),
        _ => format!("{}:{}", app_id, chat_id),
    }
}

/// Get recent directories for a key, filtered to those under root.
pub fn get_recent_dirs(store: &RecentDirsStore, key: &str, root: &str) -> Vec<String> {
    let entries = match store.entries.get(key) {
        Some(entries) => entries,
        None => return Vec::new(),
    };
    entries
        .iter()
        .map(|e| e.dir.clone())
        .filter(|d| d == "." || is_dir_under_root(d, root))
        .take(MAX_RECOMMENDED_DIRS)
        .collect()
}

/// Record a directory selection as recent.
pub fn record_recent_dir(store: &mut RecentDirsStore, key: &str, dir: &str) {
    let entries = store.entries.entry(key.to_string()).or_default();
    // Remove existing entry for the same dir
    entries.retain(|e| e.dir != dir);
    // Insert at front
    entries.insert(
        0,
        RecentDirEntry {
            dir: dir.to_string(),
            used_at: chrono::Utc::now().to_rfc3339(),
        },
    );
    // Trim
    entries.truncate(MAX_RECENT_DIRS);
}

/// Load recent dirs from disk.
pub async fn load_recent_dirs(path: &Path) -> Result<RecentDirsStore> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => Ok(serde_json::from_str(&content).unwrap_or_default()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(RecentDirsStore::default()),
        Err(e) => Err(e.into()),
    }
}

/// Save recent dirs to disk.
pub async fn save_recent_dirs(path: &Path, store: &RecentDirsStore) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(store)?;
    tokio::fs::write(&tmp, &payload).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

// --- Pending Create Pruning ---

/// Remove expired pending create entries.
/// Returns the number of entries pruned.
pub fn prune_expired_pending_creates(
    map: &mut HashMap<String, PendingCreateSession>,
    now_ms: i64,
) -> usize {
    let before = map.len();
    map.retain(|_, pending| now_ms - pending.created_at < PENDING_CREATE_TTL_MS);
    before - map.len()
}

// --- Card Building ---

/// Build the directory selection card JSON string.
///
/// Parameters:
/// - pending_id: unique ID for this pending session creation
/// - root_dir: the root working directory (displayed to user)
/// - title: the session title (user message summary)
/// - recommended_dirs: list of recommended directories (relative paths from root)
/// - all_candidates: all candidate directories (for the button list)
/// - filter_result: optional filtered subset to show as current results
/// - search_keyword: current search keyword (for restoring input field value)
/// - message: optional info/warning message to display
pub fn build_dir_select_card(
    pending_id: &str,
    root_dir: &str,
    title: &str,
    recommended_dirs: &[String],
    _all_candidates: &[String],
    filter_result: Option<&[String]>,
    search_keyword: Option<&str>,
    message: Option<&str>,
) -> String {
    let mut elements: Vec<Value> = Vec::new();

    // Header: root dir display
    let display_root = truncate_str_tail(root_dir, 60);
    elements.push(serde_json::json!({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": format!("📁 **根目录：** {}", display_root)
        }
    }));

    // Message summary
    let display_title = truncate_str_head(title, 60);
    elements.push(serde_json::json!({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": format!("💬 **消息：** {}", display_title)
        }
    }));

    // Optional message
    if let Some(msg) = message {
        elements.push(serde_json::json!({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": msg
            }
        }));
    }

    // Recommended directories (capped to avoid blowing up the card)
    let dirs_to_show_full = filter_result.unwrap_or(recommended_dirs);
    let total_count = dirs_to_show_full.len();
    let dirs_to_show = if dirs_to_show_full.len() > MAX_SHOWN_DIRS {
        &dirs_to_show_full[..MAX_SHOWN_DIRS]
    } else {
        dirs_to_show_full
    };
    if !dirs_to_show.is_empty() {
        let section_label = if filter_result.is_some() {
            if total_count > MAX_SHOWN_DIRS {
                format!(
                    "📋 **当前结果（共 {} 个，显示前 {} 个）：**",
                    total_count, MAX_SHOWN_DIRS
                )
            } else {
                format!("📋 **当前结果（{} 个）：**", total_count)
            }
        } else {
            "📋 **推荐目录：**".to_string()
        };
        elements.push(serde_json::json!({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": section_label
            }
        }));

        if filter_result.is_some() {
            // Detect short-name conflicts within the filtered results.
            // When multiple dirs share the same short display name, show the
            // relative path instead so the user can distinguish them.
            // The recommended-dir section (filter_result.is_none()) stays
            // with short names regardless of conflicts.
            let short_names: Vec<String> = dirs_to_show
                .iter()
                .map(|dir| {
                    if dir == "." {
                        root_dir_basename(root_dir)
                    } else {
                        dir_display_name(dir)
                    }
                })
                .collect();
            let mut name_count: HashMap<String, usize> = HashMap::new();
            for sn in &short_names {
                *name_count.entry(sn.clone()).or_insert(0) += 1;
            }

            for (i, dir) in dirs_to_show.iter().enumerate() {
                let short = &short_names[i];
                let conflict = name_count.get(short).copied().unwrap_or(1) > 1;

                let display = if dir == "." {
                    format!("📁 {}", root_dir_basename(root_dir))
                } else if conflict {
                    format!("📁 {}", dir)
                } else {
                    format!("📁 {}", short)
                };

                let truncated = if !conflict || dir == "." {
                    truncate_str(&display, 22)
                } else {
                    truncate_str_tail(&display, 22)
                };

                let pick_value = serde_json::json!({
                    "action": "dir_select_pick",
                    "pending_id": pending_id,
                    "working_dir": dir
                });
                elements.push(serde_json::json!({
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": truncated
                            },
                            "type": if dir == "." { "primary" } else { "default" },
                            "value": pick_value
                        }
                    ]
                }));
            }
        } else {
            // Directory buttons (split into rows to avoid too-wide action groups).
            let max_per_row = 4;
            for chunk in dirs_to_show.chunks(max_per_row) {
                if chunk.is_empty() {
                    continue;
                }
                let actions: Vec<Value> = chunk
                    .iter()
                    .map(|dir| {
                        let display = if dir == "." {
                            format!("📁 {}", root_dir_basename(root_dir))
                        } else {
                            format!("📁 {}", dir_display_name(dir))
                        };
                        serde_json::json!({
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": truncate_str(&display, 22)
                            },
                            "type": if dir == "." { "primary" } else { "default" },
                            "value": {
                                "action": "dir_select_pick",
                                "pending_id": pending_id,
                                "working_dir": dir
                            }
                        })
                    })
                    .collect();
                elements.push(serde_json::json!({
                    "tag": "action",
                    "actions": actions
                }));
            }
        }
    } else {
        elements.push(serde_json::json!({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "⚠️ 没有匹配的目录，请尝试其他关键词。"
            }
        }));
    }

    // Separator before search section
    elements.push(serde_json::json!({ "tag": "hr" }));

    // Search hint: must be a standalone div outside the form.
    // Feishu card forms only accept input + button; div is not allowed inside form.
    elements.push(serde_json::json!({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": "🔍 **搜索目录：** 输入关键词后点击「筛选」"
        }
    }));

    // Form container: input + two form_submit buttons.
    // Must be a single "tag": "form" so that the input value is submitted
    // as /action/form_value/dir_search_keyword when either button is clicked.
    let mut form_elements: Vec<Value> = Vec::new();

    form_elements.push(serde_json::json!({
        "tag": "input",
        "name": "dir_search_keyword",
        "placeholder": {
            "tag": "plain_text",
            "content": "输入关键词筛选目录..."
        },
        "default_value": search_keyword.unwrap_or("")
    }));

    form_elements.push(serde_json::json!({
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": "🔍 筛选"
        },
        "type": "primary",
        "action_type": "form_submit",
        "name": "dir_select_filter_btn",
        "value": {
            "action": "dir_select_filter",
            "pending_id": pending_id
        }
    }));

    form_elements.push(serde_json::json!({
        "tag": "button",
        "text": {
            "tag": "plain_text",
            "content": "🚀 使用最优匹配启动"
        },
        "type": "default",
        "action_type": "form_submit",
        "name": "dir_select_best_btn",
        "value": {
            "action": "dir_select_best",
            "pending_id": pending_id
        }
    }));

    elements.push(serde_json::json!({
        "tag": "form",
        "name": "dir_search_form",
        "elements": form_elements
    }));

    // Build card with config
    let card = serde_json::json!({
        "config": {
            "wide_screen_mode": true
        },
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "请选择工作目录"
            },
            "template": "blue"
        },
        "elements": elements
    });

    serde_json::to_string(&card).unwrap_or_default()
}

/// Build a simple "session starting" card to replace the dir select card.
pub fn build_dir_session_starting_card(working_dir: &str, title: &str) -> String {
    let card = serde_json::json!({
        "config": {
            "wide_screen_mode": true
        },
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "正在启动会话"
            },
            "template": "blue"
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": format!("✅ 已选择工作目录：{}\n\n正在启动会话：_{}_\n\n等待终端就绪...", working_dir, title)
                }
            }
        ]
    });
    serde_json::to_string(&card).unwrap_or_default()
}

// --- Helpers ---

fn root_dir_basename(root_dir: &str) -> String {
    Path::new(root_dir)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(root_dir)
        .to_string()
}

fn dir_display_name(rel_path: &str) -> String {
    if rel_path == "." {
        return ".".to_string();
    }
    // Show the last component
    Path::new(rel_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(rel_path)
        .to_string()
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.chars().count() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len.saturating_sub(2)).collect();
        format!("{}..", truncated)
    }
}

/// Char-safe truncation: keep the first `max_chars` characters.
fn truncate_str_head(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = chars
            .into_iter()
            .take(max_chars.saturating_sub(1))
            .collect();
        format!("{}…", truncated)
    }
}

/// Char-safe truncation: keep the last `max_chars` characters, prefix with "…".
fn truncate_str_tail(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = chars
            .into_iter()
            .rev()
            .take(max_chars.saturating_sub(1))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("…{}", truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_tilde_home() {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        let result = expand_tilde("~/projects");
        assert_eq!(result, format!("{}/projects", home));
    }

    #[test]
    fn test_expand_tilde_alone() {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        let result = expand_tilde("~");
        assert_eq!(result, home);
    }

    #[test]
    fn test_expand_tilde_no_tilde() {
        let result = expand_tilde("/abs/path");
        assert_eq!(result, "/abs/path");
    }

    #[test]
    fn test_tokenize_keywords() {
        let tokens = tokenize_keywords("beam daemon");
        assert_eq!(tokens, vec!["beam", "daemon"]);
    }

    #[test]
    fn test_tokenize_keywords_extra_spaces() {
        let tokens = tokenize_keywords("  beam   daemon  ");
        assert_eq!(tokens, vec!["beam", "daemon"]);
    }

    #[test]
    fn test_tokenize_keywords_empty() {
        let tokens = tokenize_keywords("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_match_dirs_and() {
        let dirs = vec![
            ".".to_string(),
            "beam-daemon".to_string(),
            "beam-core".to_string(),
            "beam-cli".to_string(),
            "docs/design/beam.md".to_string(),
            "README.md".to_string(),
        ];
        let matched = match_dirs(&dirs, &["beam", "daemon"]);
        assert_eq!(matched, vec!["beam-daemon".to_string()]);
    }

    #[test]
    fn test_match_dirs_case_insensitive() {
        let dirs = vec!["MyProject".to_string(), "myproject".to_string()];
        let matched = match_dirs(&dirs, &["myproject"]);
        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn test_match_dirs_empty_keywords() {
        let dirs = vec!["a".to_string(), "b".to_string()];
        let matched = match_dirs(&dirs, &[]);
        assert_eq!(matched, dirs);
    }

    #[test]
    fn test_filter_dirs() {
        let dirs = vec![
            ".".to_string(),
            "crates/beam-daemon".to_string(),
            "crates/beam-core".to_string(),
            "docs".to_string(),
        ];
        let result = filter_dirs(&dirs, "crates beam");
        assert_eq!(
            result,
            vec![
                "crates/beam-daemon".to_string(),
                "crates/beam-core".to_string(),
            ]
        );
    }

    #[test]
    fn test_find_best_match_unique() {
        let dirs = vec![
            ".".to_string(),
            "projects/foo".to_string(),
            "projects/bar".to_string(),
            "projects".to_string(),
        ];
        let best = find_best_match(&dirs, "foo");
        assert_eq!(best, Some("projects/foo".to_string()));
    }

    #[test]
    fn test_find_best_match_ambiguous() {
        let dirs = vec![
            ".".to_string(),
            "foo/bar".to_string(),
            "foo/baz".to_string(),
        ];
        let best = find_best_match(&dirs, "foo");
        assert_eq!(best, None);
    }

    #[test]
    fn test_find_best_match_multiple_different_lengths_returns_none() {
        // Even though "foo" is shorter than "foo/bar/baz", there are 2 matches
        // and the new conservative logic requires exactly 1 match.
        let dirs = vec![
            ".".to_string(),
            "foo".to_string(),
            "foo/bar/baz".to_string(),
        ];
        let best = find_best_match(&dirs, "foo");
        assert_eq!(
            best, None,
            "2 matches (different lengths) should return None"
        );
    }

    #[test]
    fn test_find_best_match_no_match() {
        let dirs = vec![".".to_string(), "a".to_string(), "b".to_string()];
        let best = find_best_match(&dirs, "xyz");
        assert_eq!(best, None);
    }

    #[test]
    fn test_find_best_match_empty_search() {
        let dirs = vec![".".to_string(), "a".to_string()];
        let best = find_best_match(&dirs, "");
        assert_eq!(best, None);
    }

    #[test]
    fn test_is_dir_under_root_absolute() {
        let result = is_dir_under_root("/tmp/root/sub", "/tmp/root");
        assert!(result);
    }

    #[test]
    fn test_is_dir_under_root_not_under() {
        let result = is_dir_under_root("/etc/passwd", "/tmp/root");
        assert!(!result);
    }

    #[test]
    fn test_is_dir_under_root_equal() {
        let result = is_dir_under_root("/tmp/root", "/tmp/root");
        assert!(result);
    }

    #[test]
    fn test_is_dir_under_root_relative() {
        let result = is_dir_under_root("sub/dir", "/tmp/root");
        assert!(result);
    }

    #[test]
    fn test_is_dir_under_root_dot_root_accepts_relative() {
        // root="." should accept relative paths like "crates"
        assert!(is_dir_under_root("crates", "."));
        assert!(is_dir_under_root("crates/beam-daemon", "."));
    }

    #[test]
    fn test_is_dir_under_root_dot_root_rejects_escape() {
        assert!(!is_dir_under_root("../x", "."), ".. should be rejected");
        assert!(
            !is_dir_under_root("crates/../../etc", "."),
            ".. should be rejected"
        );
    }

    #[test]
    fn test_is_dir_under_root_dot_root_rejects_absolute() {
        assert!(
            !is_dir_under_root("/tmp/x", "."),
            "absolute path should be rejected when root is '.'"
        );
    }

    #[test]
    fn test_is_valid_candidate_dot_root() {
        let candidates = vec![
            "crates".to_string(),
            "crates/beam-daemon".to_string(),
            "src".to_string(),
        ];
        assert!(is_valid_candidate("crates", ".", &candidates));
        assert!(is_valid_candidate("src", ".", &candidates));
        assert!(!is_valid_candidate("nonexistent", ".", &candidates));
        assert!(!is_valid_candidate("../x", ".", &candidates));
        assert!(!is_valid_candidate("/tmp/x", ".", &candidates));
    }

    #[test]
    fn test_resolve_dir() {
        assert_eq!(resolve_dir("/root", "."), "/root");
        assert_eq!(resolve_dir("/root", "sub"), "/root/sub");
        assert_eq!(resolve_dir("/root", "sub/deep"), "/root/sub/deep");
    }

    #[test]
    fn test_record_recent_dir() {
        let mut store = RecentDirsStore::default();
        let key = "app:chat:user";
        record_recent_dir(&mut store, key, "project-a");
        record_recent_dir(&mut store, key, "project-b");
        record_recent_dir(&mut store, key, "project-a"); // should move to front
        let entries = &store.entries[key];
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].dir, "project-a");
        assert_eq!(entries[1].dir, "project-b");
    }

    #[test]
    fn test_record_recent_dir_trims() {
        let mut store = RecentDirsStore::default();
        let key = "app:chat:user";
        for i in 0..MAX_RECENT_DIRS + 5 {
            record_recent_dir(&mut store, key, &format!("dir-{}", i));
        }
        assert_eq!(store.entries[key].len(), MAX_RECENT_DIRS);
        // Most recent first
        assert_eq!(
            store.entries[key][0].dir,
            format!("dir-{}", MAX_RECENT_DIRS + 4)
        );
    }

    #[test]
    fn test_build_recent_dir_key() {
        assert_eq!(
            build_recent_dir_key("app1", "chat1", Some("user1")),
            "app1:chat1:user1"
        );
        assert_eq!(build_recent_dir_key("app1", "chat1", None), "app1:chat1");
        assert_eq!(
            build_recent_dir_key("app1", "chat1", Some("")),
            "app1:chat1"
        );
    }

    #[test]
    fn test_determine_root_working_dir() {
        let result = determine_root_working_dir(Some("/my/project"), &[]);
        assert_eq!(result, "/my/project");

        let result = determine_root_working_dir(None, &["/daemon/dir".to_string()]);
        assert_eq!(result, "/daemon/dir");

        let result = determine_root_working_dir(None, &[]);
        // Fallback is ".", expand_tilde(".") = "." (no tilde to expand)
        assert_eq!(result, ".");
    }

    #[test]
    fn test_scan_candidate_dirs_includes_root() {
        let tmp = std::env::temp_dir().join("beam_dir_select_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::create_dir_all(tmp.join("sub_a")).unwrap();
        std::fs::create_dir_all(tmp.join("sub_b")).unwrap();
        std::fs::create_dir_all(tmp.join(".hidden_dir")).unwrap();
        std::fs::create_dir_all(tmp.join("__pycache__")).unwrap();
        std::fs::create_dir_all(tmp.join(".git")).unwrap();

        let dirs = scan_candidate_dirs(&tmp);
        assert!(dirs.contains(&".".to_string()));
        assert!(dirs.contains(&"sub_a".to_string()));
        assert!(dirs.contains(&"sub_b".to_string()));
        // Hidden and skipped dirs should not be included
        assert!(!dirs.iter().any(|d| d.contains(".hidden_dir")));
        assert!(!dirs.iter().any(|d| d.contains("__pycache__")));
        assert!(!dirs.iter().any(|d| d.contains(".git")));

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_build_dir_select_card_contains_required_elements() {
        let recommended = vec![".".to_string(), "project-a".to_string()];
        let all = recommended.clone();
        let card = build_dir_select_card(
            "pending-1",
            "/home/user/projects",
            "帮我修复这个 bug",
            &recommended,
            &all,
            None,
            None,
            None,
        );
        // Card should be valid JSON
        let _v: Value = serde_json::from_str(&card).expect("card should be valid JSON");
        assert!(card.contains("请选择工作目录"));
        assert!(card.contains("/home/user/projects"));
        assert!(card.contains("帮我修复这个 bug"));
        assert!(card.contains("dir_select_pick"));
        assert!(card.contains("dir_select_filter"));
        assert!(card.contains("dir_select_best"));
        assert!(card.contains("pending-1"));
        assert!(card.contains("dir_search_keyword"));
        // Verify directory button structure
        let v: Value = serde_json::from_str(&card).expect("card should be valid JSON");
        let elements = v["elements"]
            .as_array()
            .expect("elements should be an array");

        let action_groups: Vec<&Value> = elements
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .collect();
        assert!(
            !action_groups.is_empty(),
            "card should contain directory action groups"
        );
        let first_button = action_groups[0]["actions"]
            .as_array()
            .and_then(|actions| actions.first())
            .expect("action group should contain directory buttons");
        assert_eq!(
            first_button
                .pointer("/value/action")
                .and_then(Value::as_str),
            Some("dir_select_pick")
        );
        assert_eq!(
            first_button
                .pointer("/value/pending_id")
                .and_then(Value::as_str),
            Some("pending-1")
        );

        // Verify form container structure
        let form = elements
            .iter()
            .find(|e| e["tag"].as_str() == Some("form"))
            .expect("card should contain a form element");
        assert_eq!(form["name"].as_str(), Some("dir_search_form"));

        let form_els = form["elements"]
            .as_array()
            .expect("form should have elements");

        // form elements must only contain input + buttons (no div)
        let tags: Vec<&str> = form_els
            .iter()
            .map(|e| e["tag"].as_str().unwrap_or(""))
            .collect();
        assert!(
            !tags.contains(&"div"),
            "form should NOT contain div elements, got: {:?}",
            tags
        );
        assert!(
            tags.contains(&"input"),
            "form should contain an input, got: {:?}",
            tags
        );
        assert!(
            tags.contains(&"button"),
            "form should contain buttons, got: {:?}",
            tags
        );

        // input must have default_value (not value dict)
        let input = form_els
            .iter()
            .find(|e| e["tag"].as_str() == Some("input"))
            .expect("form should contain an input");
        assert!(
            input["default_value"].is_string() || input["default_value"].is_null(),
            "input must have default_value, got value: {:?}",
            input.get("value")
        );

        // all buttons must have action_type=form_submit
        for btn in form_els
            .iter()
            .filter(|e| e["tag"].as_str() == Some("button"))
        {
            assert_eq!(
                btn["action_type"].as_str(),
                Some("form_submit"),
                "all form buttons must be form_submit"
            );
        }
    }

    #[test]
    fn test_build_dir_select_card_with_message() {
        let card = build_dir_select_card(
            "p1",
            "/root",
            "test",
            &[],
            &[],
            None,
            None,
            Some("请先选择目录"),
        );
        assert!(card.contains("请先选择目录"));
    }

    #[test]
    fn test_build_dir_session_starting_card() {
        let card = build_dir_session_starting_card("/home/user/projects", "my title");
        assert!(card.contains("正在启动会话"));
        assert!(card.contains("/home/user/projects"));
        assert!(card.contains("my title"));
    }

    #[test]
    fn test_truncate_str() {
        assert_eq!(truncate_str("hello", 10), "hello");
        assert_eq!(truncate_str("hello world", 8), "hello ..");
        assert_eq!(truncate_str("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_str_head_chinese() {
        // Chinese chars (3 bytes each) — byte slicing would panic
        let s = "你好世界这是一个很长的标题需要截断测试";
        let result = truncate_str_head(s, 10);
        assert!(result.chars().count() <= 10);
        assert!(result.ends_with('…'));
        // Should not panic
    }

    #[test]
    fn test_truncate_str_tail_emoji() {
        let s = "/home/user/很长的路径/包含中文/and/emoji/🌟/test";
        let result = truncate_str_tail(s, 20);
        assert!(result.chars().count() <= 20);
        assert!(result.starts_with('…'));
        // Should not panic
    }

    #[test]
    fn test_build_dir_select_card_utf8_safe() {
        // Chinese title + long root path must not panic
        let recommended = vec![".".to_string()];
        let all = recommended.clone();
        let long_root =
            "/home/user/这是一个很长的路径用来测试截断功能/包含中文字符/abc/def/ghi/jkl/mno";
        let chinese_title = "帮我修复这个生产环境的紧急bug非常着急请尽快处理谢谢";
        let card = build_dir_select_card(
            "p-utf8",
            long_root,
            chinese_title,
            &recommended,
            &all,
            None,
            None,
            None,
        );
        // Should be valid JSON
        let _v: Value = serde_json::from_str(&card).expect("card should be valid JSON");
        assert!(card.contains("请选择工作目录"));
    }

    #[test]
    fn test_build_dir_select_card_truncates_excess_options() {
        // When there are more directories than MAX_SHOWN_DIRS (150),
        // the directory buttons are capped.
        let many_dirs: Vec<String> = (0..200).map(|i| format!("project-{:03}", i)).collect();
        let card = build_dir_select_card(
            "pid", "/root", "test", &many_dirs, &many_dirs, None, None, None,
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");

        let pick_button_count: usize = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .flat_map(|e| e["actions"].as_array().into_iter().flatten())
            .filter(|button| {
                button.pointer("/value/action").and_then(Value::as_str) == Some("dir_select_pick")
            })
            .count();
        assert_eq!(
            pick_button_count, 150,
            "directory buttons should be capped at MAX_SHOWN_DIRS"
        );
    }

    #[test]
    fn test_build_dir_select_card_filtered_truncation_shows_count_in_label() {
        // When filtering produces more results than MAX_SHOWN_DIRS,
        // the section label should indicate the total count and shown count.
        let many_dirs: Vec<String> = (0..200).map(|i| format!("project-{:03}", i)).collect();
        let card = build_dir_select_card(
            "pid",
            "/root",
            "test",
            &[],
            &many_dirs,
            Some(&many_dirs),
            Some("proj"),
            None,
        );
        // Section label should mention total count and shown count
        assert!(card.contains("共 200"), "label should show total count");
        assert!(
            card.contains("显示前 150"),
            "label should show truncation limit"
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");
        let result_row_count = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .count();
        assert_eq!(
            result_row_count, 150,
            "result rows capped at MAX_SHOWN_DIRS"
        );
    }

    #[test]
    fn test_build_dir_select_card_no_truncation_when_under_limit() {
        // When there are fewer directories than MAX_SHOWN_DIRS, no truncation.
        let few_dirs: Vec<String> = (0..10).map(|i| format!("project-{:02}", i)).collect();
        let card = build_dir_select_card(
            "pid",
            "/root",
            "test",
            &few_dirs,
            &few_dirs,
            Some(&few_dirs),
            Some("proj"),
            None,
        );
        // No "显示前" message when under limit
        assert!(
            !card.contains("显示前"),
            "no truncation label when under limit"
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");
        let result_row_count = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .count();
        assert_eq!(result_row_count, 10, "all 10 result rows should be present");
    }

    #[test]
    fn test_build_dir_select_card_filtered_unique_short_names_single_button() {
        // Filtered results with unique short names should show a single button
        // per directory displaying the short name.
        let dirs = vec!["project-a".to_string(), "project-b".to_string()];
        let card = build_dir_select_card(
            "pid",
            "/home/user/workspace",
            "test",
            &[],
            &dirs,
            Some(&dirs),
            Some("proj"),
            None,
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");
        let actions: Vec<&Value> = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .collect();
        // Each dir → exactly 1 action row
        assert_eq!(actions.len(), 2, "should have 2 action rows");

        let mut working_dirs: Vec<String> = Vec::new();
        for action in &actions {
            let buttons = action["actions"]
                .as_array()
                .expect("action row should have buttons");
            assert_eq!(
                buttons.len(),
                1,
                "filtered row should have exactly 1 button (no extra full-path button)"
            );
            let content = buttons[0]
                .pointer("/text/content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // Short name should be shown (not relative path like "project-a")
            assert!(
                content.contains("project-a") || content.contains("project-b"),
                "button should display short name, got: {}",
                content
            );
            // No full resolved path in button text
            assert!(
                !content.contains("/home/user/workspace"),
                "button text should NOT contain full resolved path"
            );
            // Value must have correct action, pending_id, and working_dir
            assert_eq!(
                buttons[0].pointer("/value/action").and_then(Value::as_str),
                Some("dir_select_pick"),
                "button action should be dir_select_pick"
            );
            assert_eq!(
                buttons[0]
                    .pointer("/value/pending_id")
                    .and_then(Value::as_str),
                Some("pid"),
                "button pending_id should be pid"
            );
            let wd = buttons[0]
                .pointer("/value/working_dir")
                .and_then(Value::as_str)
                .expect("button should have working_dir");
            working_dirs.push(wd.to_string());
        }
        working_dirs.sort();
        let mut expected_dirs = dirs.clone();
        expected_dirs.sort();
        assert_eq!(
            working_dirs, expected_dirs,
            "collected working_dirs should match input dirs"
        );
    }

    #[test]
    fn test_build_dir_select_card_filtered_duplicate_short_names_shows_relative_path() {
        // When filtered results contain dirs with the same short name
        // (e.g. a/foo and b/foo both resolve to "foo"), conflicting entries
        // should display the relative path to distinguish them.
        let dirs = vec![
            "group-a/project".to_string(),
            "group-b/project".to_string(),
            "group-a/unique".to_string(),
        ];
        let card = build_dir_select_card(
            "pid",
            "/root",
            "test",
            &[],
            &dirs,
            Some(&dirs),
            Some("proj"),
            None,
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");
        let actions: Vec<&Value> = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .collect();
        assert_eq!(actions.len(), 3, "should have 3 action rows");

        for action in &actions {
            let buttons = action["actions"]
                .as_array()
                .expect("action row should have buttons");
            assert_eq!(buttons.len(), 1, "each row must have exactly 1 button");

            let working_dir = buttons[0]
                .pointer("/value/working_dir")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let content = buttons[0]
                .pointer("/text/content")
                .and_then(Value::as_str)
                .unwrap_or_default();

            match working_dir {
                "group-a/project" | "group-b/project" => {
                    // Conflicting short name "project" → must show relative path
                    assert!(
                        content.contains("group"),
                        "conflicting dir '{}' should show relative path, got: {}",
                        working_dir,
                        content
                    );
                    assert!(
                        !content.ends_with("project") || content.contains("group"),
                        "conflicting dir '{}' should NOT show bare short name, got: {}",
                        working_dir,
                        content
                    );
                }
                "group-a/unique" => {
                    // Unique short name "unique" → short name is fine
                    assert!(
                        content.contains("unique"),
                        "unique dir should show short name, got: {}",
                        content
                    );
                }
                _ => panic!("unexpected working_dir: {}", working_dir),
            }
        }
    }

    #[test]
    fn test_build_dir_select_card_recommended_duplicate_short_names_stays_short() {
        // Even when recommended dirs have duplicate short names, the
        // recommended section should keep showing short names.
        let dirs = vec!["group-a/project".to_string(), "group-b/project".to_string()];
        let card = build_dir_select_card(
            "pid", "/root", "test", &dirs, &dirs, None, // recommended section
            None, None,
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");
        let all_buttons: Vec<&Value> = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .flat_map(|e| e["actions"].as_array().into_iter().flatten())
            .collect();

        // Both buttons should show "project" (short name), not the full rel path
        for button in &all_buttons {
            let content = button
                .pointer("/text/content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            assert!(
                content.contains("project"),
                "recommended section should show short name, got: {}",
                content
            );
            assert!(
                !content.contains("group"),
                "recommended section should NOT show full relative path, got: {}",
                content
            );
        }
    }

    #[test]
    fn test_build_dir_select_card_filtered_working_dir_value_correct() {
        // The button value must always carry the real working_dir (relative path),
        // regardless of what is displayed in the button text.
        let dirs = vec![
            "deep/nested/path/api".to_string(),
            "another/deep/nested/path/api".to_string(),
        ];
        let card = build_dir_select_card(
            "pid",
            "/home/user/workspace",
            "test",
            &[],
            &dirs,
            Some(&dirs),
            Some("api"),
            None,
        );
        let v: Value = serde_json::from_str(&card).expect("valid card JSON");
        let actions: Vec<&Value> = v["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["tag"].as_str() == Some("action"))
            .collect();

        let mut found_dirs: Vec<String> = Vec::new();
        for action in &actions {
            let buttons = action["actions"]
                .as_array()
                .expect("action row should have buttons");
            assert_eq!(buttons.len(), 1, "each row must have exactly 1 button");
            let wd = buttons[0]
                .pointer("/value/working_dir")
                .and_then(Value::as_str)
                .unwrap()
                .to_string();
            let action_val = buttons[0]
                .pointer("/value/action")
                .and_then(Value::as_str)
                .unwrap();
            assert_eq!(action_val, "dir_select_pick");
            let pending = buttons[0]
                .pointer("/value/pending_id")
                .and_then(Value::as_str)
                .unwrap();
            assert_eq!(pending, "pid");
            found_dirs.push(wd);
        }
        found_dirs.sort();
        let mut expected: Vec<String> = dirs.clone();
        expected.sort();
        assert_eq!(
            found_dirs, expected,
            "button values must contain the correct relative working_dir"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_scan_candidate_dirs_skips_symlinks() {
        use std::os::unix::fs as unix_fs;

        let tmp = std::env::temp_dir().join("beam_dir_select_symlink_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        // real subdirectory
        std::fs::create_dir_all(tmp.join("real_subdir")).unwrap();
        // symlink pointing outside root
        let external = std::env::temp_dir().join("beam_dir_select_external_target");
        std::fs::create_dir_all(&external).unwrap();
        unix_fs::symlink(&external, tmp.join("symlink_out")).unwrap();
        // symlink pointing inside root
        unix_fs::symlink(tmp.join("real_subdir"), tmp.join("symlink_in")).unwrap();

        let dirs = scan_candidate_dirs(&tmp);
        // Root "." must be present
        assert!(dirs.contains(&".".to_string()), "root must be included");
        // Real directory must be present
        assert!(
            dirs.contains(&"real_subdir".to_string()),
            "real_subdir must be included, got: {:?}",
            dirs
        );
        // Symlink to external must NOT be present
        assert!(
            !dirs
                .iter()
                .any(|d| d == "symlink_out" || d.contains("symlink_out")),
            "symlink to external must be excluded, got: {:?}",
            dirs
        );
        // Symlink to internal must NOT be present (all symlinks skipped)
        assert!(
            !dirs
                .iter()
                .any(|d| d == "symlink_in" || d.contains("symlink_in")),
            "symlink to internal must be excluded, got: {:?}",
            dirs
        );

        let _ = std::fs::remove_dir_all(&tmp);
        let _ = std::fs::remove_dir_all(&external);
    }

    #[test]
    fn test_prune_expired_pending_creates_removes_expired() {
        use std::collections::HashMap;

        let mut map: HashMap<String, PendingCreateSession> = HashMap::new();

        // Helper to make a minimal pending with given age
        let make_pending = |id: &str, created_at_ms: i64| -> PendingCreateSession {
            PendingCreateSession {
                pending_id: id.to_string(),
                lark_app_id: "app".to_string(),
                chat_id: "chat".to_string(),
                chat_type: None,
                message_id: "msg".to_string(),
                anchor: "anchor".to_string(),
                scope: SessionScope::Chat,
                title: "t".to_string(),
                text: "".to_string(),
                sender_open_id: None,
                sender_type: None,
                parent_id: None,
                mentions_json: "[]".to_string(),
                quota_key: None,
                created_at: created_at_ms,
                cli_id: "codex".to_string(),
                cli_bin: "codex".to_string(),
                backend_type: BackendType::Tmux,
                root_working_dir: "/tmp".to_string(),
                candidate_dirs: vec![".".to_string()],
                card_message_id: None,
            }
        };

        let now: i64 = 1_700_000_000_000; // some fixed timestamp in ms

        // fresh: created 5 min ago
        map.insert(
            "fresh".to_string(),
            make_pending("fresh", now - 5 * 60 * 1000),
        );
        // borderline: created 29 min ago (within TTL)
        map.insert(
            "borderline".to_string(),
            make_pending("borderline", now - 29 * 60 * 1000),
        );
        // expired: created 31 min ago
        map.insert(
            "expired".to_string(),
            make_pending("expired", now - 31 * 60 * 1000),
        );
        // very old: created 2 hours ago
        map.insert(
            "old".to_string(),
            make_pending("old", now - 2 * 60 * 60 * 1000),
        );

        assert_eq!(map.len(), 4);

        let pruned = prune_expired_pending_creates(&mut map, now);
        assert_eq!(pruned, 2, "should prune 2 expired entries");
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("fresh"));
        assert!(map.contains_key("borderline"));
        assert!(!map.contains_key("expired"));
        assert!(!map.contains_key("old"));
    }

    #[test]
    fn test_prune_expired_pending_creates_empty_map() {
        let mut map: HashMap<String, PendingCreateSession> = HashMap::new();
        let pruned = prune_expired_pending_creates(&mut map, 1_700_000_000_000);
        assert_eq!(pruned, 0);
        assert!(map.is_empty());
    }
}
