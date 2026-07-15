//! Recent directories store and pending session creation management.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use super::validation::is_dir_under_root;
use super::{
    MAX_RECENT_DIRS, MAX_RECOMMENDED_DIRS, PENDING_CREATE_TTL_MS, RecentDirEntry, RecentDirsStore,
};

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
    map: &mut HashMap<String, super::PendingCreateSession>,
    now_ms: i64,
) -> usize {
    let before = map.len();
    map.retain(|_, pending| now_ms - pending.created_at < PENDING_CREATE_TTL_MS);
    before - map.len()
}

/// Load pending creates from disk, pruning expired entries.
pub(crate) async fn load_pending_creates(
    paths: &beam_core::BeamPaths,
) -> HashMap<String, super::PendingCreateSession> {
    let path = paths.pending_creates_json();
    let entries: Vec<super::PendingCreateSession> = match beam_core::persist::read_json(&path) {
        Ok(Some(entries)) => entries,
        _ => return Default::default(),
    };
    let total_loaded = entries.len();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut map = HashMap::new();
    let mut retained = Vec::new();
    for entry in &entries {
        if now_ms - entry.created_at > PENDING_CREATE_TTL_MS {
            continue;
        }
        retained.push(entry.clone());
        map.insert(entry.pending_id.clone(), entry.clone());
    }
    // Prune expired entries
    if retained.len() < total_loaded {
        if retained.is_empty() {
            let _ = tokio::fs::remove_file(&path).await;
        } else {
            let path_clone = path.clone();
            let _ = tokio::task::spawn_blocking(move || {
                beam_core::persist::atomic_write_json(&path_clone, &retained)
            })
            .await;
        }
    }
    map
}

/// Save pending creates to disk.
#[allow(dead_code)]
pub(crate) async fn save_pending_creates(
    paths: &beam_core::BeamPaths,
    map: &HashMap<String, super::PendingCreateSession>,
) {
    let entries: Vec<super::PendingCreateSession> = map.values().cloned().collect();
    let path = paths.pending_creates_json();
    if entries.is_empty() {
        let _ = tokio::fs::remove_file(&path).await;
        return;
    }
    let path_clone = path.clone();
    let _ = tokio::task::spawn_blocking(move || {
        beam_core::persist::atomic_write_json(&path_clone, &entries)
    })
    .await;
}

#[cfg(test)]
#[path = "tests/recent.rs"]
mod tests;
