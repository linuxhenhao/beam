use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use beam_core::BeamPaths;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorVerify {
    #[serde(rename = "type")]
    pub verify_type: String,
    pub secret_ref: String,
    pub signature_header: String,
    pub timestamp_header: String,
    pub nonce_header: String,
    pub tolerance_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorTarget {
    pub mode: String,
    pub kind: String,
    pub bot_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bot_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_chats: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorPromptEnvelope {
    pub source_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub header_allowlist: Vec<String>,
    pub include_raw_text: bool,
    pub max_body_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorLoggingPolicy {
    pub store_payload: bool,
    pub store_headers: bool,
    pub retention_days: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorLifecycleExtractors {
    pub dedup_key: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub status_map: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorDefinition {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub verify: ConnectorVerify,
    pub target: ConnectorTarget,
    pub prompt_envelope: ConnectorPromptEnvelope,
    pub logging_policy: ConnectorLoggingPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_extractors: Option<ConnectorLifecycleExtractors>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<ConnectorRateLimit>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorRateLimit {
    pub window_seconds: u64,
    pub max_requests: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorStoreFile {
    pub version: u8,
    pub connectors: Vec<ConnectorDefinition>,
}

fn store_path(paths: &BeamPaths) -> PathBuf {
    paths.connectors_json()
}

fn empty_store() -> ConnectorStoreFile {
    ConnectorStoreFile {
        version: 1,
        connectors: Vec::new(),
    }
}

fn normalize_store(raw: Option<ConnectorStoreFile>) -> ConnectorStoreFile {
    raw.filter(|store| store.version == 1)
        .unwrap_or_else(empty_store)
}

fn read_store(paths: &BeamPaths) -> Result<ConnectorStoreFile> {
    let fp = store_path(paths);
    let raw = match fs::read_to_string(&fp) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(empty_store()),
        Err(err) => return Err(err.into()),
    };
    let parsed = serde_json::from_str::<ConnectorStoreFile>(&raw).ok();
    Ok(normalize_store(parsed))
}

fn write_store(paths: &BeamPaths, store: &ConnectorStoreFile) -> Result<()> {
    let fp = store_path(paths);
    if let Some(parent) = fp.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = fp.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&tmp, serde_json::to_string_pretty(store)? + "\n")?;
    fs::rename(&tmp, &fp)
        .with_context(|| format!("failed to atomically write {}", fp.display()))?;
    Ok(())
}

pub fn list_connectors(paths: &BeamPaths) -> Result<Vec<ConnectorDefinition>> {
    Ok(read_store(paths)?.connectors)
}

pub fn get_connector(paths: &BeamPaths, id: &str) -> Result<Option<ConnectorDefinition>> {
    Ok(read_store(paths)?
        .connectors
        .into_iter()
        .find(|c| c.id == id))
}

pub fn upsert_connector(
    paths: &BeamPaths,
    connector: ConnectorDefinition,
) -> Result<ConnectorDefinition> {
    let mut store = read_store(paths)?;
    let now = connector.updated_at.clone();
    let idx = store.connectors.iter().position(|c| c.id == connector.id);
    let next = if let Some(idx) = idx {
        let created_at = store.connectors[idx].created_at.clone();
        let mut next = connector;
        next.created_at = created_at;
        next.updated_at = now;
        store.connectors[idx] = next.clone();
        next
    } else {
        store.connectors.push(connector.clone());
        connector
    };
    write_store(paths, &store)?;
    Ok(next)
}

pub fn delete_connector(paths: &BeamPaths, id: &str) -> Result<bool> {
    let mut store = read_store(paths)?;
    let before = store.connectors.len();
    store.connectors.retain(|c| c.id != id);
    if store.connectors.len() != before {
        write_store(paths, &store)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn new_connector_id() -> String {
    format!("conn_{}", Uuid::new_v4())
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
            "beam-connector-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[test]
    fn connector_store_round_trips() {
        let paths = temp_paths("roundtrip");
        let _ = std::fs::remove_dir_all(paths.root());
        let connector = ConnectorDefinition {
            id: "conn_1".to_string(),
            name: "alerts".to_string(),
            enabled: true,
            verify: ConnectorVerify {
                verify_type: "hmac-sha256".to_string(),
                secret_ref: "whsec_1".to_string(),
                signature_header: "x-sig".to_string(),
                timestamp_header: "x-ts".to_string(),
                nonce_header: "x-nonce".to_string(),
                tolerance_seconds: 300,
            },
            target: ConnectorTarget {
                mode: "dynamic".to_string(),
                kind: "turn".to_string(),
                bot_id: "bot_1".to_string(),
                bot_ids: vec!["bot_1".to_string()],
                chat_id: None,
                allow_chats: vec![],
                workflow_id: None,
            },
            prompt_envelope: ConnectorPromptEnvelope {
                source_name: "alerts".to_string(),
                header_allowlist: vec!["x-request-id".to_string()],
                include_raw_text: false,
                max_body_bytes: 1024,
            },
            logging_policy: ConnectorLoggingPolicy {
                store_payload: true,
                store_headers: true,
                retention_days: 7,
            },
            lifecycle_extractors: None,
            rate_limit: Some(ConnectorRateLimit {
                window_seconds: 60,
                max_requests: 10,
            }),
            created_at: "2026-06-08T00:00:00Z".to_string(),
            updated_at: "2026-06-08T00:00:00Z".to_string(),
        };
        upsert_connector(&paths, connector.clone()).expect("upsert");
        assert_eq!(
            get_connector(&paths, "conn_1").expect("get"),
            Some(connector.clone())
        );
        assert_eq!(list_connectors(&paths).expect("list").len(), 1);
        assert!(delete_connector(&paths, "conn_1").expect("delete"));
        assert!(list_connectors(&paths).expect("list").is_empty());
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
