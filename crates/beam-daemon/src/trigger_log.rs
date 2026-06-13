use std::fs;
#[cfg(test)]
use std::fs::OpenOptions;
#[cfg(test)]
use std::io::Write;

use anyhow::Result;
use beam_core::BeamPaths;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TriggerLogEntry {
    pub trigger_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connector_id: Option<String>,
    pub action: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TriggerLogStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connector_id: Option<String>,
    pub total: u64,
    pub ok: u64,
    pub error: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub actions: std::collections::BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub error_codes: std::collections::BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_triggered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_ok_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TriggerLogPruneResult {
    pub before: usize,
    pub after: usize,
    pub deleted: usize,
}

fn path(paths: &BeamPaths) -> std::path::PathBuf {
    paths.trigger_logs_jsonl()
}

#[cfg(test)]
fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn new_trigger_id() -> String {
    format!("trg_{}", Uuid::new_v4())
}

#[cfg(test)]
pub fn append_trigger_log(
    paths: &BeamPaths,
    entry: TriggerLogEntry,
) -> Result<TriggerLogEntry> {
    let fp = path(paths);
    if let Some(parent) = fp.parent() {
        fs::create_dir_all(parent)?;
    }
    let full = TriggerLogEntry {
        created_at: if entry.created_at.trim().is_empty() {
            now_iso()
        } else {
            entry.created_at
        },
        ..entry
    };
    let mut file = OpenOptions::new().create(true).append(true).open(&fp)?;
    writeln!(file, "{}", serde_json::to_string(&full)?)?;
    Ok(full)
}

fn read_entries(paths: &BeamPaths) -> Result<Vec<TriggerLogEntry>> {
    let fp = path(paths);
    let raw = match fs::read_to_string(&fp) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut out = Vec::new();
    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<TriggerLogEntry>(line) {
            out.push(entry);
        }
    }
    Ok(out)
}

pub fn list_trigger_logs(
    paths: &BeamPaths,
    limit: usize,
    connector_id: Option<&str>,
    status: Option<&str>,
    error_code: Option<&str>,
    since: Option<&str>,
) -> Result<Vec<TriggerLogEntry>> {
    let entries = read_entries(paths)?;
    let since_ms = since
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis());
    let mut out = Vec::new();
    for entry in entries.into_iter().rev() {
        if let Some(connector_id) = connector_id {
            if entry.connector_id.as_deref() != Some(connector_id) {
                continue;
            }
        }
        if let Some(status) = status {
            if entry.status != status {
                continue;
            }
        }
        if let Some(error_code) = error_code {
            if entry.error_code.as_deref() != Some(error_code) {
                continue;
            }
        }
        if let Some(since_ms) = since_ms {
            let created_ms = chrono::DateTime::parse_from_rfc3339(&entry.created_at)
                .ok()
                .map(|dt| dt.timestamp_millis());
            if created_ms.map(|ms| ms < since_ms).unwrap_or(false) {
                continue;
            }
        }
        out.push(entry);
        if out.len() >= limit {
            break;
        }
    }
    Ok(out)
}

pub fn summarize_trigger_logs(
    paths: &BeamPaths,
    connector_id: Option<&str>,
    since: Option<&str>,
) -> Result<Vec<TriggerLogStats>> {
    let entries = read_entries(paths)?;
    let since_ms = since
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis());
    let mut groups: std::collections::BTreeMap<String, TriggerLogStats> = Default::default();
    for entry in entries {
        if let Some(connector_id) = connector_id {
            if entry.connector_id.as_deref() != Some(connector_id) {
                continue;
            }
        }
        if let Some(since_ms) = since_ms {
            let created_ms = chrono::DateTime::parse_from_rfc3339(&entry.created_at)
                .ok()
                .map(|dt| dt.timestamp_millis());
            if created_ms.map(|ms| ms < since_ms).unwrap_or(false) {
                continue;
            }
        }
        let key = entry.connector_id.clone().unwrap_or_default();
        let stat = groups
            .entry(key.clone())
            .or_insert_with(|| TriggerLogStats {
                connector_id: if key.is_empty() {
                    None
                } else {
                    Some(key.clone())
                },
                ..Default::default()
            });
        stat.total += 1;
        if entry.status == "ok" {
            stat.ok += 1;
            stat.last_ok_at = Some(entry.created_at.clone());
        } else {
            stat.error += 1;
            stat.last_error_at = Some(entry.created_at.clone());
            stat.last_error = entry.error.clone();
            if let Some(code) = entry.error_code.clone() {
                stat.last_error_code = Some(code.clone());
                *stat.error_codes.entry(code).or_insert(0) += 1;
            }
        }
        *stat.actions.entry(entry.action.clone()).or_insert(0) += 1;
        stat.last_triggered_at = Some(entry.created_at.clone());
    }
    Ok(groups.into_values().collect())
}

pub fn prune_trigger_logs(
    paths: &BeamPaths,
    retention_days: Option<u64>,
    max_entries: Option<usize>,
) -> Result<TriggerLogPruneResult> {
    let entries = read_entries(paths)?;
    let before = entries.len();
    let mut kept = entries;
    if let Some(days) = retention_days {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(days as i64);
        kept.retain(|entry| {
            chrono::DateTime::parse_from_rfc3339(&entry.created_at)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc) >= cutoff)
                .unwrap_or(true)
        });
    }
    if let Some(max_entries) = max_entries {
        if kept.len() > max_entries {
            kept = kept[kept.len() - max_entries..].to_vec();
        }
    }
    let fp = path(paths);
    if let Some(parent) = fp.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = fp.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut content = String::new();
    for entry in &kept {
        content.push_str(&serde_json::to_string(entry)?);
        content.push('\n');
    }
    fs::write(&tmp, content)?;
    fs::rename(&tmp, &fp)?;
    Ok(TriggerLogPruneResult {
        before,
        after: kept.len(),
        deleted: before.saturating_sub(kept.len()),
    })
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
            "beam-trigger-log-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[test]
    fn trigger_logs_round_trip_and_prune() {
        let paths = temp_paths("roundtrip");
        let _ = std::fs::remove_dir_all(paths.root());
        append_trigger_log(
            &paths,
            TriggerLogEntry {
                trigger_id: "trg_1".to_string(),
                connector_id: Some("conn_1".to_string()),
                action: "queued".to_string(),
                status: "ok".to_string(),
                error: None,
                error_code: None,
                created_at: "2026-06-08T00:00:00Z".to_string(),
            },
        )
        .expect("append");
        append_trigger_log(
            &paths,
            TriggerLogEntry {
                trigger_id: "trg_2".to_string(),
                connector_id: Some("conn_1".to_string()),
                action: "failed".to_string(),
                status: "error".to_string(),
                error: Some("boom".to_string()),
                error_code: Some("trigger_failed".to_string()),
                created_at: "2026-06-08T00:01:00Z".to_string(),
            },
        )
        .expect("append");
        let listed = list_trigger_logs(&paths, 10, Some("conn_1"), None, None, None).expect("list");
        assert_eq!(listed.len(), 2);
        let stats = summarize_trigger_logs(&paths, Some("conn_1"), None).expect("stats");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].total, 2);
        let prune = prune_trigger_logs(&paths, None, Some(1)).expect("prune");
        assert_eq!(prune.before, 2);
        assert_eq!(prune.after, 1);
        let listed = list_trigger_logs(&paths, 10, None, None, None, None).expect("list");
        assert_eq!(listed.len(), 1);
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
