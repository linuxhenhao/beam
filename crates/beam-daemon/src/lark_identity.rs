use super::*;

pub(crate) fn load_known_bot_open_ids_for_app(
    paths: &BeamPaths,
    lark_app_id: &str,
) -> HashSet<String> {
    let mut out = HashSet::new();
    let cross_ref_path = paths
        .root()
        .join(format!("bot-openids-{}.json", lark_app_id));
    if let Ok(payload) = std::fs::read_to_string(cross_ref_path)
        && let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&payload)
    {
        for value in map.values() {
            if let Some(open_id) = value.as_str() {
                out.insert(open_id.to_string());
            }
        }
    }

    let bots_info_path = paths.root().join("bots-info.json");
    if let Ok(payload) = std::fs::read_to_string(bots_info_path)
        && let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(&payload)
    {
        for entry in entries {
            if entry.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id)
                && let Some(open_id) = entry.get("botOpenId").and_then(Value::as_str)
            {
                out.insert(open_id.to_string());
            }
        }
    }
    out
}

pub(crate) fn load_self_bot_open_id_for_app(
    paths: &BeamPaths,
    lark_app_id: &str,
) -> Option<String> {
    let bots_info_path = paths.root().join("bots-info.json");
    let payload = std::fs::read_to_string(bots_info_path).ok()?;
    let Value::Array(entries) = serde_json::from_str::<Value>(&payload).ok()? else {
        return None;
    };
    entries.into_iter().find_map(|entry| {
        (entry.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
            .then(|| {
                entry
                    .get("botOpenId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .flatten()
    })
}

pub(crate) fn load_bot_identity(
    paths: &BeamPaths,
    lark_app_id: &str,
) -> (Option<String>, Option<String>) {
    let bots_info_path = paths.root().join("bots-info.json");
    let Ok(payload) = std::fs::read_to_string(bots_info_path) else {
        return (None, None);
    };
    let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(&payload) else {
        return (None, None);
    };
    for entry in entries {
        if entry.get("larkAppId").and_then(Value::as_str) != Some(lark_app_id) {
            continue;
        }
        let name = entry
            .get("botName")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let open_id = entry
            .get("botOpenId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        return (name, open_id);
    }
    (None, None)
}

pub(crate) async fn probe_and_persist_bot_info(paths: &BeamPaths, bot: &BotConfig) {
    let result = async {
        let http = Client::new();
        let resp = http
            .post(format!(
                "{}/auth/v3/tenant_access_token/internal",
                lark_base_url()
            ))
            .json(&serde_json::json!({
                "app_id": bot.lark_app_id,
                "app_secret": bot.lark_app_secret,
            }))
            .send()
            .await?;
        let body = resp.json::<LarkTokenResponse>().await?;
        if body.code != 0 {
            anyhow::bail!("token failed: {}", body.msg.unwrap_or_default());
        }
        let token = body
            .tenant_access_token
            .context("missing tenant_access_token")?;

        let resp = http
            .get(format!("{}/bot/v3/info/", lark_base_url()))
            .bearer_auth(token)
            .send()
            .await?;
        let info: Value = resp.json().await?;
        if info.get("code").and_then(Value::as_i64).unwrap_or(-1) != 0 {
            anyhow::bail!(
                "bot info failed: {}",
                info.get("msg").and_then(Value::as_str).unwrap_or("unknown")
            );
        }
        let open_id = info
            .pointer("/bot/open_id")
            .and_then(Value::as_str)
            .map(String::from);
        let bot_name = info
            .pointer("/bot/app_name")
            .and_then(Value::as_str)
            .map(String::from);
        anyhow::Ok((open_id, bot_name))
    }
    .await;

    let (open_id, name) = match result {
        Ok((Some(open_id), name)) => (open_id, name),
        Ok((None, _)) => {
            warn!("[{}] bot info had no open_id field", bot.lark_app_id);
            return;
        }
        Err(err) => {
            warn!("[{}] probe bot info failed: {}", bot.lark_app_id, err);
            return;
        }
    };

    let open_id_trunc = open_id[..8.min(open_id.len())].to_string();

    let path = paths.root().join("bots-info.json");
    let mut entries: Vec<Value> = match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_default(),
        Err(_) => Vec::new(),
    };

    let lark_app_id = &bot.lark_app_id;
    let cli_id = bot.cli_id.clone();
    if let Some(entry) = entries
        .iter_mut()
        .find(|e| e.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
    {
        entry["botOpenId"] = Value::String(open_id);
        if let Some(name) = name {
            entry["botName"] = Value::String(name);
        }
        if entry.get("cliId").is_none() {
            entry["cliId"] = Value::String(cli_id);
        }
    } else {
        entries.push(serde_json::json!({
            "larkAppId": lark_app_id,
            "botOpenId": open_id,
            "botName": name,
            "cliId": cli_id,
        }));
    }

    let payload = match serde_json::to_string_pretty(&entries) {
        Ok(p) => p,
        Err(err) => {
            warn!(
                "[{}] failed to serialize bots-info.json: {}",
                lark_app_id, err
            );
            return;
        }
    };

    if let Err(err) = std::fs::write(&path, payload + "\n") {
        warn!(
            "[{}] failed to persist bots-info.json: {}",
            lark_app_id, err
        );
    } else {
        tracing::info!(
            "[{}] persisted bot info (open_id={})",
            lark_app_id,
            open_id_trunc,
        );
    }
}

pub(crate) fn peer_bot_open_ids_for_app(paths: &BeamPaths, lark_app_id: &str) -> Vec<String> {
    let mut ids = load_known_bot_open_ids_for_app(paths, lark_app_id)
        .into_iter()
        .chain(load_observed_bot_open_ids_for_app(paths, lark_app_id))
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupStats {
    pub(crate) user_count: u32,
    pub(crate) bot_count: u32,
}

pub(crate) async fn lark_group_stats(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
) -> Result<GroupStats> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .get(format!("{}/im/v1/chats/{}", lark_base_url(), chat_id))
        .bearer_auth(token)
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark chat info failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    Ok(GroupStats {
        user_count: parse_group_count(value.pointer("/data/user_count")),
        bot_count: parse_group_count(value.pointer("/data/bot_count")),
    })
}

/// Feishu returns `user_count`/`bot_count` as strings (e.g. "2"), so accept
/// both numbers and numeric strings. Missing/invalid values default to 0,
/// which keeps the multi-bot gate fail-closed (0 users + 0 bots still passes
/// the single-user exemption, but a real count will now be honored).
fn parse_group_count(value: Option<&Value>) -> u32 {
    match value {
        Some(v) => v
            .as_u64()
            .map(|n| n as u32)
            .or_else(|| v.as_str().and_then(|s| s.parse::<u32>().ok()))
            .unwrap_or(0),
        None => 0,
    }
}

const CHAT_MODE_TTL_SECS: u64 = 5 * 60;

pub(crate) fn parse_chat_info_mode(chat_mode: &str, group_message_type: &str) -> ChatMode {
    if chat_mode.eq_ignore_ascii_case("p2p") {
        ChatMode::P2p
    } else if chat_mode.eq_ignore_ascii_case("topic")
        || group_message_type.eq_ignore_ascii_case("thread")
    {
        ChatMode::Topic
    } else {
        ChatMode::Group
    }
}

pub(crate) async fn get_lark_chat_mode(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    force_refresh: bool,
) -> Result<ChatMode> {
    let cache_key = format!("{}::{}", bot.lark_app_id, chat_id);
    if !force_refresh {
        let cache = state.chat_mode_cache.lock().await;
        if let Some(entry) = cache.get(&cache_key)
            && entry.cached_at.elapsed().as_secs() < CHAT_MODE_TTL_SECS
        {
            return Ok(entry.mode);
        }
    }
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .get(format!("{}/im/v1/chats/{}", lark_base_url(), chat_id))
        .bearer_auth(token)
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark chat info failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    let chat_mode_raw = value
        .pointer("/data/chat_mode")
        .and_then(Value::as_str)
        .unwrap_or("");
    let group_message_type = value
        .pointer("/data/group_message_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mode = parse_chat_info_mode(chat_mode_raw, group_message_type);
    debug!(
        app_id = %bot.lark_app_id,
        chat_id = %chat_id,
        chat_mode = %chat_mode_raw,
        group_message_type = %group_message_type,
        resolved_mode = ?mode,
        "lark chat info parsed"
    );
    {
        let mut cache = state.chat_mode_cache.lock().await;
        cache.insert(
            cache_key,
            CachedChatMode {
                mode,
                cached_at: Instant::now(),
            },
        );
    }
    Ok(mode)
}

pub(crate) fn current_bot_is_mentioned(
    paths: &BeamPaths,
    app_id: &str,
    parsed: &ParsedLarkInboundMessage,
) -> bool {
    let Some(bot_open_id) = load_self_bot_open_id_for_app(paths, app_id) else {
        return false;
    };
    parsed
        .mentions
        .iter()
        .any(|mention| mention.key == bot_open_id)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decide_multibot_inbound_gate(
    sender_type: Option<&str>,
    sender_open_id: Option<&str>,
    self_bot_open_id: Option<&str>,
    mentioned_self_bot: bool,
    custom_trigger_hit: bool,
    chat_type: Option<&str>,
    scope: SessionScope,
    is_oncall_chat: bool,
    owns_session: bool,
    is_known_peer_bot: bool,
    has_chat_grant: bool,
    has_global_grant: bool,
    group_stats: Option<GroupStats>,
    text: &str,
) -> bool {
    let is_bot_sender = matches!(sender_type, Some("bot") | Some("app"));
    if is_bot_sender {
        if let (Some(sender_open_id), Some(self_bot_open_id)) = (sender_open_id, self_bot_open_id)
            && sender_open_id == self_bot_open_id
        {
            return text.trim() == "/close";
        }
        if !mentioned_self_bot {
            return false;
        }
        if scope == SessionScope::Chat
            && !is_oncall_chat
            && !owns_session
            && !is_known_peer_bot
            && !has_chat_grant
            && !has_global_grant
        {
            return false;
        }
        return true;
    }

    if chat_type == Some("group") {
        if mentioned_self_bot || custom_trigger_hit {
            return true;
        }
        let Some(stats) = group_stats else {
            return false;
        };
        return stats.user_count <= 1 && stats.bot_count <= 1;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;
    use serde_json::Value;

    #[test]
    fn parse_group_count_accepts_string_and_number_forms() {
        assert_eq!(
            parse_group_count(Some(&serde_json::json!("2"))),
            2,
            "Feishu returns counts as strings"
        );
        assert_eq!(
            parse_group_count(Some(&serde_json::json!(1))),
            1,
            "numeric form still works"
        );
        assert_eq!(parse_group_count(Some(&serde_json::json!("abc"))), 0);
        assert_eq!(parse_group_count(None), 0);
    }

    #[test]
    fn string_counts_keep_multi_bot_group_gated() {
        // Feishu reports user_count/bot_count as strings (e.g. "2"). A group
        // with one user and two bots must still deny a plain, non-mentioned
        // message; otherwise the multi-bot gate fails open and the bot replies
        // without any trigger.
        let stats = GroupStats {
            user_count: parse_group_count(Some(&serde_json::json!("1"))),
            bot_count: parse_group_count(Some(&serde_json::json!("2"))),
        };
        assert!(!decide_multibot_inbound_gate(
            Some("user"),
            Some("ou_user"),
            Some("ou_self"),
            false,
            false,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            Some(stats),
            "明天深圳天气怎么样",
        ));
    }

    #[test]
    fn peer_bot_open_ids_load_from_known_sources() {
        let paths = temp_paths("peer-bot-openids");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");
        std::fs::write(
            paths.root().join("bot-openids-app-1.json"),
            r#"{"peerA":"ou_peer_a"}"#,
        )
        .expect("write cross-ref");
        std::fs::write(
            paths.root().join("bots-info.json"),
            r#"[{"larkAppId":"app-1","botOpenId":"ou_peer_b"}]"#,
        )
        .expect("write bots info");

        let ids = peer_bot_open_ids_for_app(&paths, "app-1");
        assert_eq!(ids, vec!["ou_peer_a".to_string(), "ou_peer_b".to_string()]);
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn record_observed_bots_round_trips_into_peer_lookup() {
        let paths = temp_paths("observed-bots");
        maybe_remove_dir(&paths.root().to_path_buf());
        std::fs::create_dir_all(paths.root()).expect("mkdir root");
        record_observed_bots(
            &paths,
            "app-1",
            "chat-1",
            &[(String::from("ou_peer_c"), String::from("ou_peer_c"))],
            "grant",
        )
        .expect("record observed bots");
        let raw = std::fs::read_to_string(
            paths
                .observed_bots_dir()
                .join("observed-bots-app-1-chat-1.json"),
        )
        .expect("observed store file");
        let value: Value = serde_json::from_str(&raw).expect("observed json");
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(
            peer_bot_open_ids_for_app(&paths, "app-1"),
            vec!["ou_peer_c".to_string()]
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
