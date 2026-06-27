use std::fs;
use std::path::PathBuf;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, bail};
use base64::Engine;
use beam_core::BeamPaths;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WebhookSecretRecord {
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub alg: String,
    pub iv: String,
    pub tag: String,
    pub ciphertext: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct SecretStoreFile {
    version: u8,
    secrets: std::collections::BTreeMap<String, WebhookSecretRecord>,
}

fn master_key_path(paths: &BeamPaths) -> PathBuf {
    paths.webhook_master_key()
}

fn secret_store_path(paths: &BeamPaths) -> PathBuf {
    paths.webhook_secrets_json()
}

fn read_or_create_master_key(paths: &BeamPaths) -> Result<[u8; 32]> {
    let fp = master_key_path(paths);
    if let Ok(raw) = fs::read_to_string(&fp) {
        let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw.trim())
            .context("failed to decode webhook master key")?;
        if key.len() != 32 {
            bail!("invalid webhook master key length at {}", fp.display());
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&key);
        return Ok(out);
    }
    if let Some(parent) = fp.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    fs::write(
        &fp,
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key) + "\n",
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fp, fs::Permissions::from_mode(0o600))?;
    }
    Ok(key)
}

fn read_store(paths: &BeamPaths) -> Result<SecretStoreFile> {
    let fp = secret_store_path(paths);
    let raw = match fs::read_to_string(&fp) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SecretStoreFile {
                version: 1,
                secrets: Default::default(),
            });
        }
        Err(err) => return Err(err.into()),
    };
    let parsed = serde_json::from_str::<SecretStoreFile>(&raw).ok();
    Ok(parsed
        .filter(|s| s.version == 1)
        .unwrap_or(SecretStoreFile {
            version: 1,
            secrets: Default::default(),
        }))
}

fn write_store(paths: &BeamPaths, store: &SecretStoreFile) -> Result<()> {
    let fp = secret_store_path(paths);
    if let Some(parent) = fp.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = fp.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&tmp, serde_json::to_string_pretty(store)? + "\n")?;
    fs::rename(&tmp, &fp)
        .with_context(|| format!("failed to atomically write {}", fp.display()))?;
    Ok(())
}

fn encrypt_secret(plaintext: &str, key: &[u8; 32]) -> Result<(String, String, String)> {
    let cipher = Aes256Gcm::new_from_slice(key).context("failed to initialize webhook cipher")?;
    let mut iv = [0u8; 12];
    iv.copy_from_slice(&Uuid::new_v4().as_bytes()[..12]);
    let nonce = Nonce::from_slice(&iv);
    let mut buf = plaintext.as_bytes().to_vec();
    let tag = cipher
        .encrypt_in_place_detached(nonce, b"", &mut buf)
        .map_err(|_| anyhow::anyhow!("failed to encrypt webhook secret"))?;
    Ok((
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(iv),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(tag),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf),
    ))
}

fn decrypt_secret(record: &WebhookSecretRecord, key: &[u8; 32]) -> Result<String> {
    if record.alg != "aes-256-gcm" {
        bail!("unsupported webhook secret alg: {}", record.alg);
    }
    let cipher = Aes256Gcm::new_from_slice(key).context("failed to initialize webhook cipher")?;
    let iv = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(record.iv.as_bytes())
        .context("failed to decode webhook iv")?;
    let tag = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(record.tag.as_bytes())
        .context("failed to decode webhook tag")?;
    let mut buf = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(record.ciphertext.as_bytes())
        .context("failed to decode webhook ciphertext")?;
    let nonce = Nonce::from_slice(&iv);
    cipher
        .decrypt_in_place_detached(nonce, b"", &mut buf, aes_gcm::Tag::from_slice(&tag))
        .map_err(|_| anyhow::anyhow!("failed to decrypt webhook secret"))?;
    String::from_utf8(buf).context("webhook secret is not utf-8")
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn create_webhook_secret(paths: &BeamPaths, plaintext: &str) -> Result<WebhookSecretRecord> {
    let ref_id = format!("whsec_{}", Uuid::new_v4().simple());
    set_webhook_secret(paths, &ref_id, plaintext)
}

pub fn generate_webhook_secret_plaintext() -> String {
    Uuid::new_v4().simple().to_string() + &Uuid::new_v4().simple().to_string()
}

pub fn set_webhook_secret(
    paths: &BeamPaths,
    ref_id: &str,
    plaintext: &str,
) -> Result<WebhookSecretRecord> {
    if ref_id.trim().is_empty() {
        bail!("secret ref is required");
    }
    if plaintext.trim().is_empty() {
        bail!("secret plaintext is required");
    }
    let key = read_or_create_master_key(paths)?;
    let mut store = read_store(paths)?;
    let now = now_iso();
    let prior = store.secrets.get(ref_id);
    let (iv, tag, ciphertext) = encrypt_secret(plaintext, &key)?;
    let record = WebhookSecretRecord {
        ref_name: ref_id.to_string(),
        alg: "aes-256-gcm".to_string(),
        iv,
        tag,
        ciphertext,
        created_at: prior
            .map(|r| r.created_at.clone())
            .unwrap_or_else(|| now.clone()),
        updated_at: now,
    };
    store.secrets.insert(ref_id.to_string(), record.clone());
    write_store(paths, &store)?;
    Ok(record)
}

pub fn get_webhook_secret(paths: &BeamPaths, ref_id: &str) -> Result<Option<String>> {
    if ref_id.trim().is_empty() {
        return Ok(None);
    }
    let key = read_or_create_master_key(paths)?;
    let store = read_store(paths)?;
    let Some(record) = store.secrets.get(ref_id) else {
        return Ok(None);
    };
    decrypt_secret(record, &key).map(Some)
}

pub fn delete_webhook_secret(paths: &BeamPaths, ref_id: &str) -> Result<bool> {
    let mut store = read_store(paths)?;
    if store.secrets.remove(ref_id).is_none() {
        return Ok(false);
    }
    write_store(paths, &store)?;
    Ok(true)
}

pub fn list_webhook_secret_refs(paths: &BeamPaths) -> Result<Vec<serde_json::Value>> {
    let store = read_store(paths)?;
    Ok(store
        .secrets
        .into_values()
        .map(|record| {
            serde_json::json!({
                "ref": record.ref_name,
                "alg": record.alg,
                "createdAt": record.created_at,
                "updatedAt": record.updated_at,
            })
        })
        .collect())
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
            "beam-webhook-secret-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[test]
    fn webhook_secret_round_trips_and_hides_ciphertext() {
        let paths = temp_paths("roundtrip");
        let _ = std::fs::remove_dir_all(paths.root());
        let record = create_webhook_secret(&paths, "super-secret").expect("create");
        assert_eq!(
            get_webhook_secret(&paths, &record.ref_name).expect("get"),
            Some("super-secret".to_string())
        );
        let refs = list_webhook_secret_refs(&paths).expect("refs");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0]["ref"], record.ref_name);
        assert!(refs[0].get("ciphertext").is_none());
        assert!(delete_webhook_secret(&paths, &record.ref_name).expect("delete"));
        assert!(
            get_webhook_secret(&paths, &record.ref_name)
                .expect("get")
                .is_none()
        );
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
