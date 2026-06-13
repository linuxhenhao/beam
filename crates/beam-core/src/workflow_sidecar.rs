use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::Value;

use crate::EventLog;

pub async fn write_effect_input_sidecar(
    log: &EventLog,
    activity_id: &str,
    attempt_id: &str,
    input: &Value,
) -> Result<PathBuf> {
    let dir = effect_input_dir(log, activity_id, attempt_id);
    fs::create_dir_all(&dir)?;
    let path = dir.join("effect-input.json");
    fs::write(&path, serde_json::to_vec_pretty(input)?)?;
    Ok(path)
}

pub async fn load_effect_input_sidecar(
    run_dir: &Path,
    activity_id: &str,
    attempt_id: &str,
) -> Result<Option<Value>> {
    let path = run_dir
        .join("attempts")
        .join(activity_id)
        .join(attempt_id)
        .join("effect-input.json");
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let parsed = serde_json::from_str::<Value>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(parsed))
}

fn effect_input_dir(log: &EventLog, activity_id: &str, attempt_id: &str) -> PathBuf {
    log.run_dir
        .join("attempts")
        .join(activity_id)
        .join(attempt_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BeamPaths;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-workflow-sidecar-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[tokio::test]
    async fn effect_input_sidecar_round_trips() {
        let paths = temp_paths("roundtrip");
        let log = EventLog::new("run-1", paths.workflow_runs_dir()).unwrap();
        let input = serde_json::json!({"chatId":"chat-1","content":"hello"});
        let path = write_effect_input_sidecar(&log, "act-1", "act-1::att-1", &input)
            .await
            .expect("write");
        assert!(path.exists());
        let loaded = load_effect_input_sidecar(&log.run_dir, "act-1", "act-1::att-1")
            .await
            .expect("load")
            .expect("some");
        assert_eq!(loaded, input);
        let _ = std::fs::remove_dir_all(paths.root());
    }
}
