use super::*;

pub(crate) fn load_config(paths: &BeamPaths) -> Result<Config> {
    match std::fs::read_to_string(paths.config_toml()) {
        Ok(raw) => Ok(toml::from_str(&raw)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn load_bot_configs(paths: &BeamPaths) -> Result<HashMap<String, BotConfig>> {
    match std::fs::read_to_string(paths.bots_json()) {
        Ok(raw) => {
            let items = serde_json::from_str::<Vec<BotConfig>>(&raw)?;
            Ok(items
                .into_iter()
                .map(|cfg| (cfg.lark_app_id.clone(), cfg))
                .collect())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn persist_sessions(
    paths: &BeamPaths,
    sessions: &HashMap<String, Session>,
) -> Result<()> {
    tokio::fs::create_dir_all(paths.sessions_dir()).await?;
    let tmp = paths.session_store_json().with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(sessions)?;
    tokio::fs::write(&tmp, payload).await?;
    tokio::fs::rename(tmp, paths.session_store_json()).await?;
    Ok(())
}

pub(crate) async fn persist_runtime_state(
    paths: &BeamPaths,
    state: &DaemonRuntimeState,
) -> Result<()> {
    tokio::fs::create_dir_all(paths.run_dir()).await?;
    let tmp = paths.runtime_state_json().with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(state)?;
    tokio::fs::write(&tmp, payload).await?;
    tokio::fs::rename(tmp, paths.runtime_state_json()).await?;
    Ok(())
}

pub(crate) async fn load_sessions(paths: &BeamPaths) -> Result<HashMap<String, Session>> {
    match tokio::fs::read(paths.session_store_json()).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn load_recent_lark_events(
    paths: &BeamPaths,
) -> HashMap<String, std::time::Instant> {
    let path = paths.recent_lark_events_json();
    let entries: Vec<(String, u64)> = match beam_core::persist::read_json(&path) {
        Ok(Some(entries)) => entries,
        _ => return HashMap::new(),
    };
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    let ttl_ms = 300_000u64;
    let mut map = HashMap::new();
    for (key, ts_ms) in entries {
        if now_ms.saturating_sub(ts_ms) < ttl_ms {
            let elapsed = now_ms.saturating_sub(ts_ms);
            map.insert(
                key,
                std::time::Instant::now() - std::time::Duration::from_millis(elapsed),
            );
        }
    }
    map
}

pub(crate) async fn save_recent_lark_events(
    paths: &BeamPaths,
    events: &HashMap<String, std::time::Instant>,
) {
    let now = std::time::Instant::now();
    let now_epoch_ms = chrono::Utc::now().timestamp_millis() as u64;
    let ttl_ms = 300_000u64;
    let entries: Vec<(String, u64)> = events
        .iter()
        .filter_map(|(key, instant)| {
            let elapsed = now.duration_since(*instant);
            if elapsed.as_millis() as u64 > ttl_ms {
                return None;
            }
            let seen_at_ms = now_epoch_ms.saturating_sub(elapsed.as_millis() as u64);
            Some((key.clone(), seen_at_ms))
        })
        .collect();
    let path = paths.recent_lark_events_json();
    if entries.is_empty() {
        let _ = tokio::fs::remove_file(&path).await;
        return;
    }
    let _ =
        tokio::task::spawn_blocking(move || beam_core::persist::atomic_write_json(&path, &entries))
            .await;
}
