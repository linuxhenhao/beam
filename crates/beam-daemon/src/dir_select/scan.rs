//! Directory scanning, path resolution, and keyword filtering.

use std::path::Path;

use super::{MAX_SCAN_CANDIDATES, MAX_SCAN_DEPTH, SKIP_DIR_NAMES};

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

/// Expand ~ to the user's home directory.
pub(crate) fn expand_tilde(path: &str) -> String {
    if (path.starts_with("~/") || path == "~")
        && let Ok(home) = std::env::var("HOME")
    {
        if path == "~" {
            return home;
        }
        return format!("{}/{}", home, &path[2..]);
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

// --- Path Resolution ---

/// Resolve a relative dir (from candidate list) to an absolute path.
pub fn resolve_dir(root: &str, rel: &str) -> String {
    if rel == "." {
        root.to_string()
    } else {
        Path::new(root).join(rel).to_string_lossy().to_string()
    }
}

#[cfg(test)]
#[path = "tests/scan.rs"]
mod tests;
