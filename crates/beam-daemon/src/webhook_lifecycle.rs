use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{Result, bail};
use beam_core::BeamPaths;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WebhookLifecycleRecord {
    pub lifecycle_id: String,
    pub connector_id: String,
    pub dedup_key: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator_lark_app_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_resolved: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creating_expires_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WebhookLifecycleStoreFile {
    pub version: u8,
    pub records: Vec<WebhookLifecycleRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginLifecycleFiringResult {
    Create(WebhookLifecycleRecord),
    Reuse(WebhookLifecycleRecord),
    Creating(WebhookLifecycleRecord),
}

fn store_path(paths: &BeamPaths) -> PathBuf {
    paths.root().join("webhook-lifecycle.json")
}

fn lock_path(paths: &BeamPaths) -> PathBuf {
    store_path(paths).with_extension("lock")
}

struct FileLock {
    path: PathBuf,
}

impl FileLock {
    fn acquire(paths: &BeamPaths) -> Result<Self> {
        let path = lock_path(paths);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        for _ in 0..300 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(_) => return Ok(Self { path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => return Err(err.into()),
            }
        }
        bail!(
            "timed out acquiring webhook lifecycle file lock: {}",
            path.display()
        );
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn empty_store() -> WebhookLifecycleStoreFile {
    WebhookLifecycleStoreFile {
        version: 1,
        records: Vec::new(),
    }
}

fn read_store(paths: &BeamPaths) -> Result<WebhookLifecycleStoreFile> {
    let fp = store_path(paths);
    let raw = match fs::read_to_string(&fp) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(empty_store()),
        Err(err) => return Err(err.into()),
    };
    let parsed = serde_json::from_str::<WebhookLifecycleStoreFile>(&raw).ok();
    Ok(parsed
        .filter(|s| s.version == 1)
        .unwrap_or_else(empty_store))
}

fn write_store(paths: &BeamPaths, store: &WebhookLifecycleStoreFile) -> Result<()> {
    let fp = store_path(paths);
    if let Some(parent) = fp.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = fp.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&tmp, serde_json::to_string_pretty(store)? + "\n")?;
    fs::rename(&tmp, &fp)?;
    Ok(())
}

fn key_of(connector_id: &str, dedup_key: &str) -> String {
    format!("{}\0{}", connector_id, dedup_key)
}

fn find_index(
    store: &WebhookLifecycleStoreFile,
    connector_id: &str,
    dedup_key: &str,
) -> Option<usize> {
    let key = key_of(connector_id, dedup_key);
    store
        .records
        .iter()
        .position(|r| key_of(&r.connector_id, &r.dedup_key) == key)
}

const CREATING_TTL_MS: i64 = 10 * 60 * 1000;

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn creating_expired(record: &WebhookLifecycleRecord, now_ms: i64) -> bool {
    let created_ms = chrono::DateTime::parse_from_rfc3339(&record.created_at)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(now_ms);
    let expires_ms = record
        .creating_expires_at
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(created_ms + CREATING_TTL_MS);
    expires_ms <= now_ms
}

#[cfg(test)]
pub fn list_webhook_lifecycle_records(
    paths: &BeamPaths,
    connector_id: Option<&str>,
    status: Option<&str>,
) -> Result<Vec<WebhookLifecycleRecord>> {
    Ok(read_store(paths)?
        .records
        .into_iter()
        .filter(|r| connector_id.map(|id| r.connector_id == id).unwrap_or(true))
        .filter(|r| status.map(|s| r.status == s).unwrap_or(true))
        .collect())
}

pub fn begin_webhook_lifecycle_firing(
    paths: &BeamPaths,
    connector_id: &str,
    dedup_key: &str,
) -> Result<BeginLifecycleFiringResult> {
    let _lock = FileLock::acquire(paths)?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut store = read_store(paths)?;
    let idx = find_index(&store, connector_id, dedup_key);
    let existing = idx.and_then(|idx| store.records.get(idx).cloned());
    if let Some(existing) = existing.clone() {
        if existing.status == "active" && existing.chat_id.is_some() {
            return Ok(BeginLifecycleFiringResult::Reuse(existing));
        }
        if existing.status == "creating" && !creating_expired(&existing, now_ms) {
            return Ok(BeginLifecycleFiringResult::Creating(existing));
        }
    }

    let now = now_iso();
    let record = WebhookLifecycleRecord {
        lifecycle_id: Uuid::new_v4().to_string(),
        connector_id: connector_id.to_string(),
        dedup_key: dedup_key.to_string(),
        status: "creating".to_string(),
        chat_id: None,
        creator_lark_app_id: None,
        pending_resolved: None,
        creating_expires_at: Some(
            (chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339(),
        ),
        created_at: now.clone(),
        updated_at: now,
        resolved_at: None,
    };
    if let Some(idx) = idx {
        store.records[idx] = record.clone();
    } else {
        store.records.push(record.clone());
    }
    write_store(paths, &store)?;
    Ok(BeginLifecycleFiringResult::Create(record))
}

#[cfg(test)]
pub fn activate_webhook_lifecycle_group(
    paths: &BeamPaths,
    connector_id: &str,
    dedup_key: &str,
    lifecycle_id: &str,
    chat_id: &str,
    creator_lark_app_id: Option<&str>,
) -> Result<Option<WebhookLifecycleRecord>> {
    let _lock = FileLock::acquire(paths)?;
    let mut store = read_store(paths)?;
    let Some(idx) = find_index(&store, connector_id, dedup_key) else {
        return Ok(None);
    };
    let Some(existing) = store.records.get(idx).cloned() else {
        return Ok(None);
    };
    if existing.lifecycle_id != lifecycle_id || existing.status != "creating" {
        return Ok(None);
    }
    let now = now_iso();
    let next = if existing.pending_resolved.unwrap_or(false) {
        WebhookLifecycleRecord {
            status: "resolved".to_string(),
            chat_id: Some(chat_id.to_string()),
            creator_lark_app_id: creator_lark_app_id.map(|s| s.to_string()),
            pending_resolved: Some(false),
            creating_expires_at: None,
            updated_at: now.clone(),
            resolved_at: Some(now.clone()),
            ..existing
        }
    } else {
        WebhookLifecycleRecord {
            status: "active".to_string(),
            chat_id: Some(chat_id.to_string()),
            creator_lark_app_id: creator_lark_app_id.map(|s| s.to_string()),
            pending_resolved: None,
            creating_expires_at: None,
            updated_at: now,
            resolved_at: None,
            ..existing
        }
    };
    store.records[idx] = next.clone();
    write_store(paths, &store)?;
    Ok(Some(next))
}

#[allow(dead_code)]
pub fn fail_webhook_lifecycle_group(
    paths: &BeamPaths,
    connector_id: &str,
    dedup_key: &str,
    lifecycle_id: &str,
) -> Result<()> {
    let _lock = FileLock::acquire(paths)?;
    let mut store = read_store(paths)?;
    if let Some(idx) = find_index(&store, connector_id, dedup_key) {
        if store.records[idx].lifecycle_id == lifecycle_id
            && store.records[idx].status == "creating"
        {
            store.records.remove(idx);
            write_store(paths, &store)?;
        }
    }
    Ok(())
}

pub fn resolve_webhook_lifecycle_group(
    paths: &BeamPaths,
    connector_id: &str,
    dedup_key: &str,
) -> Result<Option<WebhookLifecycleRecord>> {
    let _lock = FileLock::acquire(paths)?;
    let mut store = read_store(paths)?;
    let Some(idx) = find_index(&store, connector_id, dedup_key) else {
        return Ok(None);
    };
    let Some(existing) = store.records.get(idx).cloned() else {
        return Ok(None);
    };
    if existing.status == "resolved" {
        return Ok(Some(existing));
    }
    let now = now_iso();
    let next = if existing.status == "creating" {
        WebhookLifecycleRecord {
            pending_resolved: Some(true),
            updated_at: now,
            ..existing
        }
    } else {
        WebhookLifecycleRecord {
            status: "resolved".to_string(),
            updated_at: now.clone(),
            resolved_at: Some(now),
            ..existing
        }
    };
    store.records[idx] = next.clone();
    write_store(paths, &store)?;
    Ok(Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-webhook-lifecycle-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[test]
    fn lifecycle_begin_reuse_resolve_and_activate() {
        let paths = temp_paths("roundtrip");
        let _ = std::fs::remove_dir_all(paths.root());
        let begun = begin_webhook_lifecycle_firing(&paths, "conn_1", "dedup_1").expect("begin");
        let record = match begun {
            BeginLifecycleFiringResult::Create(record) => record,
            _ => panic!("expected create"),
        };
        let reused = begin_webhook_lifecycle_firing(&paths, "conn_1", "dedup_1").expect("reuse");
        assert!(matches!(reused, BeginLifecycleFiringResult::Creating(_)));
        let activated = activate_webhook_lifecycle_group(
            &paths,
            "conn_1",
            "dedup_1",
            &record.lifecycle_id,
            "chat_1",
            Some("app_1"),
        )
        .expect("activate");
        assert_eq!(
            activated.as_ref().map(|record| record.status.as_str()),
            Some("active")
        );
        let resolved =
            resolve_webhook_lifecycle_group(&paths, "conn_1", "dedup_1").expect("resolve");
        assert_eq!(
            resolved.as_ref().map(|record| record.status.as_str()),
            Some("resolved")
        );
        let listed =
            list_webhook_lifecycle_records(&paths, Some("conn_1"), Some("resolved")).expect("list");
        assert_eq!(listed.len(), 1);
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
