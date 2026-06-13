use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    BeamPaths, RunChatBinding, WorkflowDefinition, read_run_snapshot,
    workflow_snapshot::{RunSnapshotDTO, RunStatus},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdWorkflowRun {
    pub run_id: String,
    pub def: WorkflowDefinition,
    pub snapshot: RunSnapshotDTO,
    pub binding: RunChatBinding,
}

#[derive(Debug, Clone, Default)]
pub struct ColdScanStats {
    pub discovered: usize,
    pub skipped: Vec<String>,
}

pub async fn scan_cold_workflow_runs(
    paths: &BeamPaths,
    owner_lark_app_id: &str,
) -> Result<(Vec<ColdWorkflowRun>, ColdScanStats)> {
    let runs_dir = paths.workflow_runs_dir();
    if !runs_dir.exists() {
        return Ok((vec![], ColdScanStats::default()));
    }

    let mut runs = Vec::new();
    let mut stats = ColdScanStats::default();
    let dir_entries = std::fs::read_dir(&runs_dir).context("failed to read workflow runs dir")?;

    for entry in dir_entries {
        let entry = entry.context("failed to read dir entry")?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let run_id = entry.file_name().to_string_lossy().to_string();
        let run_dir = runs_dir.join(&run_id);

        match scan_single_run(&run_dir, &run_id, owner_lark_app_id).await {
            Ok(Some(run)) => {
                stats.discovered += 1;
                runs.push(run);
            }
            Ok(None) => {}
            Err(reason) => {
                stats.skipped.push(format!("{}: {}", run_id, reason));
            }
        }
    }

    Ok((runs, stats))
}

async fn scan_single_run(
    run_dir: &Path,
    run_id: &str,
    owner_lark_app_id: &str,
) -> Result<Option<ColdWorkflowRun>> {
    let binding_path = run_dir.join("chat-binding.json");
    if !binding_path.exists() {
        return Ok(None);
    }

    let binding: RunChatBinding = serde_json::from_str(
        &std::fs::read_to_string(&binding_path)
            .with_context(|| "failed to read chat-binding.json")?,
    )
    .with_context(|| "invalid chat-binding.json")?;

    if binding.lark_app_id != owner_lark_app_id {
        return Ok(None);
    }

    let def_path = run_dir.join("workflow.json");
    if !def_path.exists() {
        anyhow::bail!("missing workflow.json");
    }

    let raw_def =
        std::fs::read_to_string(&def_path).with_context(|| "failed to read workflow.json")?;
    let def: WorkflowDefinition =
        serde_json::from_str(&raw_def).with_context(|| "invalid workflow.json")?;

    let Some(snapshot) = read_run_snapshot(run_dir)
        .await
        .with_context(|| "failed to read snapshot")?
    else {
        anyhow::bail!("empty event log");
    };

    let terminal = matches!(
        snapshot.run.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
    );
    if terminal {
        return Ok(None);
    }

    Ok(Some(ColdWorkflowRun {
        run_id: run_id.to_string(),
        def,
        snapshot,
        binding,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BeamPaths;
    use std::{fs, path::PathBuf};

    fn temp_data_root() -> PathBuf {
        std::env::temp_dir().join(format!("beam-coldscan-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn scan_empty_runs_dir_returns_nothing() {
        let root = temp_data_root();
        let paths = BeamPaths::from_root(&root);
        fs::create_dir_all(paths.workflow_runs_dir()).unwrap();

        let (runs, stats) = scan_cold_workflow_runs(&paths, "app-1").await.unwrap();
        assert!(runs.is_empty());
        assert_eq!(stats.discovered, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn scan_silently_skips_runs_without_chat_binding() {
        let root = temp_data_root();
        let paths = BeamPaths::from_root(&root);
        let run_dir = paths.workflow_run_dir("run-1");
        fs::create_dir_all(&run_dir).unwrap();

        let (runs, _stats) = scan_cold_workflow_runs(&paths, "app-1").await.unwrap();
        assert!(runs.is_empty());

        let _ = fs::remove_dir_all(root);
    }
}
