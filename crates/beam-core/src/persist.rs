//! Common persistence helpers: atomic JSON file writes with tmp + rename.
//! Sensitive files can opt-in to 0600 permissions on Unix.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use uuid::Uuid;

/// Atomically write a serializable value to `path` using tmp + rename.
/// Creates parent directories if needed.
pub fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    atomic_write_json_with_perms(path, value, false)
}

/// Atomically write a serializable value to `path` using tmp + rename.
/// If `restrict_perms` is true, sets file permissions to 0o600 on Unix.
pub fn atomic_write_json_with_perms(
    path: &Path,
    value: &impl Serialize,
    restrict_perms: bool,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let payload = serde_json::to_vec_pretty(value)?;
    fs::write(&tmp, &payload)?;
    if restrict_perms {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = fs::Permissions::from_mode(0o600);
            let _ = fs::set_permissions(&tmp, perms);
        }
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to atomically write {}", path.display()))?;
    Ok(())
}

/// Read and deserialize JSON from `path`. Returns `Ok(None)` if the file does not exist.
pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read_to_string(path) {
        Ok(raw) => {
            if raw.trim().is_empty() {
                return Ok(None);
            }
            let value = serde_json::from_str(&raw)?;
            Ok(Some(value))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Remove a file if it exists. No error if not found.
pub fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Helper: build a tmp path for a given file path.
pub fn tmp_path_for(path: &Path) -> PathBuf {
    path.with_extension(format!("{}.tmp", Uuid::new_v4()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestData {
        key: String,
        value: i32,
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "beam-persist-test-{}-{}-{}",
            label,
            nanos,
            std::process::id()
        ))
    }

    #[test]
    fn atomic_write_and_read_roundtrip() {
        let path = temp_path("roundtrip");
        let data = TestData {
            key: "hello".to_string(),
            value: 42,
        };
        atomic_write_json(&path, &data).unwrap();
        let loaded: Option<TestData> = read_json(&path).unwrap();
        assert_eq!(loaded, Some(data));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn read_json_returns_none_for_missing_file() {
        let path = temp_path("missing");
        let result: Option<TestData> = read_json(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn atomic_write_with_restrict_perms() {
        let path = temp_path("perms");
        let data = TestData {
            key: "secret".to_string(),
            value: 99,
        };
        atomic_write_json_with_perms(&path, &data, true).unwrap();
        let loaded: Option<TestData> = read_json(&path).unwrap();
        assert_eq!(loaded, Some(data));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(&path).unwrap();
            let mode = metadata.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "expected 0600 permissions, got {:o}", mode);
        }
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn remove_file_if_exists_ok() {
        let path = temp_path("rm");
        fs::write(&path, "test").unwrap();
        remove_file_if_exists(&path).unwrap();
        assert!(!path.exists());
        // Removing again is fine
        remove_file_if_exists(&path).unwrap();
    }
}
