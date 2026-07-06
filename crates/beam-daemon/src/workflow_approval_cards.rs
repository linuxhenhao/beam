use super::*;

pub(crate) async fn load_workflow_approval_cards(
    paths: &BeamPaths,
    run_id: &str,
) -> Result<HashMap<String, FrozenCard>> {
    let path = paths.workflow_approval_cards_json(run_id);
    match tokio::fs::read_to_string(&path).await {
        Ok(raw) => {
            let parsed = serde_json::from_str::<HashMap<String, FrozenCard>>(&raw)
                .context("failed to parse workflow approval cards")?;
            Ok(parsed)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn save_workflow_approval_cards(
    paths: &BeamPaths,
    run_id: &str,
    cards: &HashMap<String, FrozenCard>,
) -> Result<()> {
    let dir = paths.workflow_approval_cards_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let path = paths.workflow_approval_cards_json(run_id);
    if cards.is_empty() {
        let _ = tokio::fs::remove_file(&path).await;
        return Ok(());
    }
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_string_pretty(cards)?;
    tokio::fs::write(&tmp, body).await?;
    tokio::fs::rename(&tmp, &path).await?;
    Ok(())
}
