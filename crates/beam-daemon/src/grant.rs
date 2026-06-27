use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// TTL for grant pending entries: 10 minutes in milliseconds.
pub const GRANT_PENDING_TTL_MS: i64 = 10 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct GrantContext {
    #[allow(dead_code)]
    pub lark_app_id: String,
    #[allow(dead_code)]
    pub chat_id: String,
    #[allow(dead_code)]
    pub sender_open_id: String,
    pub resolved_allowed_users: Vec<String>,
    #[allow(dead_code)]
    pub peer_bot_open_ids: Vec<String>,
}

pub struct GrantCommand {
    pub action: GrantAction,
    pub targets: Vec<GrantTarget>,
    pub quota: Option<u32>,
}

pub struct GrantTarget {
    pub open_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantAction {
    Grant,
    Revoke,
    GrantAll,
}

pub fn parse_grant_command(
    text: &str,
    bot_mention: Option<&str>,
    _ctx: &GrantContext,
) -> Option<GrantCommand> {
    let stripped = strip_bot_mention(text, bot_mention);
    let trimmed = stripped.trim();

    if !trimmed.starts_with("/grant") && !trimmed.starts_with("/revoke") {
        return None;
    }

    let action = if trimmed.starts_with("/revoke") {
        GrantAction::Revoke
    } else {
        GrantAction::Grant
    };

    let after_cmd = if trimmed.starts_with("/revoke") {
        trimmed[7..].trim()
    } else {
        trimmed[6..].trim()
    };

    if after_cmd.is_empty() || after_cmd == "all" {
        return Some(GrantCommand {
            action: GrantAction::GrantAll,
            targets: vec![],
            quota: None,
        });
    }

    let (target_text, quota) = parse_quota(after_cmd);
    let targets = parse_targets(target_text);

    if targets.is_empty() && action == GrantAction::Revoke {
        return None;
    }

    Some(GrantCommand {
        action,
        targets,
        quota,
    })
}

fn strip_bot_mention<'a>(text: &'a str, _bot_mention: Option<&str>) -> &'a str {
    text.trim_start_matches('@').trim_start()
}

fn parse_quota(text: &str) -> (&str, Option<u32>) {
    let parts: Vec<&str> = text.rsplitn(2, ' ').collect();
    if parts.len() == 2 {
        if let Ok(n) = parts[0].parse::<u32>() {
            if n > 0 {
                return (parts[1].trim(), Some(n));
            }
        }
    }
    (text, None)
}

fn parse_targets(text: &str) -> Vec<GrantTarget> {
    text.split(' ')
        .filter(|s| s.starts_with('@'))
        .map(|s| GrantTarget {
            open_id: s.trim_start_matches('@').to_string(),
        })
        .collect()
}

pub fn add_chat_grant(
    config: &mut Value,
    lark_app_id: &str,
    chat_id: &str,
    target_open_id: &str,
    quota: Option<u32>,
) -> Result<()> {
    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    if bot.get("chatGrants").is_none() {
        bot["chatGrants"] = serde_json::json!({});
    }
    if bot["chatGrants"].get(chat_id).is_none() {
        bot["chatGrants"][chat_id] = serde_json::json!([]);
    }
    let entry = &mut bot["chatGrants"][chat_id];

    if let Some(arr) = entry.as_array_mut() {
        if !arr.iter().any(|v| v.as_str() == Some(target_open_id)) {
            arr.push(serde_json::json!(target_open_id));
        }
    }

    if let Some(q) = quota {
        if q > 0 {
            set_quota_entry(
                config,
                lark_app_id,
                &format!("chat:{}:{}", chat_id, target_open_id),
                q,
            )?;
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub fn add_global_grant(
    config: &mut Value,
    lark_app_id: &str,
    target_open_id: &str,
    quota: Option<u32>,
) -> Result<()> {
    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    if bot.get("globalGrants").is_none() {
        bot["globalGrants"] = serde_json::json!([]);
    }
    if let Some(arr) = bot["globalGrants"].as_array_mut() {
        if !arr.iter().any(|v| v.as_str() == Some(target_open_id)) {
            arr.push(serde_json::json!(target_open_id));
        }
    }

    if let Some(q) = quota {
        if q > 0 {
            set_quota_entry(
                config,
                lark_app_id,
                &format!("global:{}", target_open_id),
                q,
            )?;
        }
    }

    Ok(())
}

pub fn add_allowed_chat_group(config: &mut Value, lark_app_id: &str, chat_id: &str) -> Result<()> {
    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    if bot.get("allowedChatGroups").is_none() {
        bot["allowedChatGroups"] = serde_json::json!([]);
    }
    if let Some(arr) = bot["allowedChatGroups"].as_array_mut() {
        if !arr.iter().any(|v| v.as_str() == Some(chat_id)) {
            arr.push(serde_json::json!(chat_id));
        }
    }

    Ok(())
}

pub fn revoke_grant(
    config: &mut Value,
    lark_app_id: &str,
    chat_id: &str,
    target_open_id: &str,
    resolved_allowed_users: &[String],
) -> Result<()> {
    let owner_open_id = resolved_allowed_users.first().cloned();
    if owner_open_id.as_deref() == Some(target_open_id) {
        bail!("cannot revoke owner permissions");
    }

    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    if let Some(chat_grants) = bot.get_mut("chatGrants") {
        if let Some(arr) = chat_grants.get_mut(chat_id).and_then(Value::as_array_mut) {
            arr.retain(|v| v.as_str() != Some(target_open_id));
        }
    }

    if let Some(allowed_users) = bot.get_mut("allowedUsers") {
        if let Some(arr) = allowed_users.as_array_mut() {
            arr.retain(|v| v.as_str() != Some(target_open_id));
        }
    }

    if let Some(global_grants) = bot.get_mut("globalGrants") {
        if let Some(arr) = global_grants.as_array_mut() {
            arr.retain(|v| v.as_str() != Some(target_open_id));
        }
    }

    remove_quota_entries(config, lark_app_id, target_open_id, chat_id)?;

    Ok(())
}

fn set_quota_entry(
    config: &mut Value,
    lark_app_id: &str,
    quota_key: &str,
    limit: u32,
) -> Result<()> {
    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    if bot.get("quotaState").is_none() {
        bot["quotaState"] = serde_json::json!({});
    }
    let quota_state = &mut bot["quotaState"];

    quota_state[quota_key] = serde_json::json!({ "limit": limit, "used": 0 });

    Ok(())
}

fn remove_quota_entries(
    config: &mut Value,
    lark_app_id: &str,
    target_open_id: &str,
    chat_id: &str,
) -> Result<()> {
    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    if let Some(quota_state) = bot.get_mut("quotaState") {
        if let Some(obj) = quota_state.as_object_mut() {
            obj.retain(|k, _| {
                !k.starts_with(&format!("chat:{}:{}", chat_id, target_open_id))
                    && !k.starts_with(&format!("global:{}", target_open_id))
            });
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub fn consume_quota(
    config: &mut Value,
    lark_app_id: &str,
    quota_key: &str,
) -> Result<QuotaResult> {
    let bots = config.as_array_mut().context("bots.json is not an array")?;
    let bot = bots
        .iter_mut()
        .find(|b| b.get("larkAppId").and_then(Value::as_str) == Some(lark_app_id))
        .with_context(|| format!("bot {} not found", lark_app_id))?;

    let Some(quota_state) = bot.get_mut("quotaState") else {
        return Ok(QuotaResult {
            allowed: true,
            exhausted: false,
        });
    };

    let Some(entry) = quota_state.get_mut(quota_key) else {
        return Ok(QuotaResult {
            allowed: true,
            exhausted: false,
        });
    };

    let limit = entry.get("limit").and_then(Value::as_u64).unwrap_or(0) as u32;
    let mut used = entry.get("used").and_then(Value::as_u64).unwrap_or(0) as u32;

    if used >= limit {
        return Ok(QuotaResult {
            allowed: false,
            exhausted: true,
        });
    }

    used += 1;
    entry["used"] = serde_json::json!(used);

    Ok(QuotaResult {
        allowed: true,
        exhausted: used >= limit,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(dead_code)]
pub enum GrantPendingState {
    Pending,
    Denied { denied_at: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct GrantPendingEntry {
    pub nonce: String,
    pub targets: Vec<String>,
    pub quota: Option<u32>,
    pub ts: u64,
    pub state: GrantPendingState,
}

impl GrantPendingEntry {
    pub fn is_pending(&self) -> bool {
        matches!(self.state, GrantPendingState::Pending)
    }

    #[allow(dead_code)]
    pub fn is_throttled(&self, now_ms: u64) -> bool {
        match self.state {
            GrantPendingState::Pending => true,
            GrantPendingState::Denied { denied_at } => {
                now_ms.saturating_sub(denied_at) < 10 * 60 * 1000
            }
        }
    }

    pub fn mark_denied(&mut self, denied_at: u64) {
        self.state = GrantPendingState::Denied { denied_at };
    }
}

pub fn build_grant_card(
    targets: &[String],
    nonce: &str,
    chat_id: &str,
    quota: Option<u32>,
) -> serde_json::Value {
    let target_display = targets.join(", ");
    let quota_text = quota
        .map(|q| format!("\nQuota: {} messages", q))
        .unwrap_or_default();
    let title = format!("Grant access to @{}?{}", target_display, quota_text);

    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "header": {
            "template": "blue",
            "title": { "tag": "plain_text", "content": "Permission Grant" },
        },
        "elements": [
            {
                "tag": "markdown",
                "content": title,
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": { "tag": "lark_md", "content": "仅本群授权 (Chat Only)" },
                        "type": "primary",
                        "value": serde_json::json!({
                            "action": "grant_chat",
                            "nonce": nonce,
                            "targets": targets,
                            "chatId": chat_id,
                            "quota": quota,
                        }).to_string(),
                    },
                    {
                        "tag": "button",
                        "text": { "tag": "lark_md", "content": "全局授权 (Global)" },
                        "type": "default",
                        "value": serde_json::json!({
                            "action": "grant_global",
                            "nonce": nonce,
                            "targets": targets,
                            "chatId": chat_id,
                            "quota": quota,
                        }).to_string(),
                    },
                    {
                        "tag": "button",
                        "text": { "tag": "lark_md", "content": "拒绝 (Deny)" },
                        "type": "danger",
                        "value": serde_json::json!({
                            "action": "grant_deny",
                            "nonce": nonce,
                            "targets": targets,
                            "chatId": chat_id,
                        }).to_string(),
                    },
                ],
            },
        ],
    })
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QuotaResult {
    pub allowed: bool,
    pub exhausted: bool,
}

/// Serializable grant pending entry with its map key.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GrantPendingPersisted {
    key: String,
    nonce: String,
    targets: Vec<String>,
    quota: Option<u32>,
    ts: u64,
    state: GrantPendingState,
}

/// Load grant pending entries from disk, pruning expired ones.
pub(crate) fn load_grant_pending(
    paths: &beam_core::BeamPaths,
) -> std::collections::HashMap<String, GrantPendingEntry> {
    let path = paths.grant_pending_json();
    let entries: Vec<GrantPendingPersisted> = match beam_core::persist::read_json(&path) {
        Ok(Some(entries)) => entries,
        _ => return Default::default(),
    };
    let total_loaded = entries.len();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut map = std::collections::HashMap::new();
    let mut retained = Vec::new();
    for entry in &entries {
        if now_ms - entry.ts as i64 > GRANT_PENDING_TTL_MS {
            continue;
        }
        retained.push(entry.clone());
        map.insert(
            entry.key.clone(),
            GrantPendingEntry {
                nonce: entry.nonce.clone(),
                targets: entry.targets.clone(),
                quota: entry.quota,
                ts: entry.ts,
                state: entry.state.clone(),
            },
        );
    }
    // Prune expired entries
    if retained.len() < total_loaded {
        if retained.is_empty() {
            let _ = std::fs::remove_file(&path);
        } else {
            let _ = beam_core::persist::atomic_write_json(&path, &retained);
        }
    }
    map
}

/// Save grant pending entries to disk with their map keys preserved.
pub(crate) fn save_grant_pending(
    paths: &beam_core::BeamPaths,
    map: &std::collections::HashMap<String, GrantPendingEntry>,
) {
    let entries: Vec<GrantPendingPersisted> = map
        .iter()
        .map(|(key, entry)| GrantPendingPersisted {
            key: key.clone(),
            nonce: entry.nonce.clone(),
            targets: entry.targets.clone(),
            quota: entry.quota,
            ts: entry.ts,
            state: entry.state.clone(),
        })
        .collect();
    let path = paths.grant_pending_json();
    if entries.is_empty() {
        let _ = std::fs::remove_file(&path);
        return;
    }
    let _ = beam_core::persist::atomic_write_json(&path, &entries);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_grant_user() {
        let ctx = GrantContext {
            lark_app_id: "app".to_string(),
            chat_id: "chat".to_string(),
            sender_open_id: "owner".to_string(),
            resolved_allowed_users: vec!["owner".to_string()],
            peer_bot_open_ids: vec![],
        };
        let cmd = parse_grant_command("/grant @user1", None, &ctx).unwrap();
        assert!(matches!(cmd.action, GrantAction::Grant));
        assert_eq!(cmd.targets.len(), 1);
        assert_eq!(cmd.targets[0].open_id, "user1");
    }

    #[test]
    fn parse_grant_all() {
        let ctx = GrantContext {
            lark_app_id: "app".to_string(),
            chat_id: "chat".to_string(),
            sender_open_id: "owner".to_string(),
            resolved_allowed_users: vec!["owner".to_string()],
            peer_bot_open_ids: vec![],
        };
        let cmd = parse_grant_command("/grant all", None, &ctx).unwrap();
        assert!(matches!(cmd.action, GrantAction::GrantAll));
    }

    #[test]
    fn parse_revoke_user() {
        let ctx = GrantContext {
            lark_app_id: "app".to_string(),
            chat_id: "chat".to_string(),
            sender_open_id: "owner".to_string(),
            resolved_allowed_users: vec!["owner".to_string()],
            peer_bot_open_ids: vec![],
        };
        let cmd = parse_grant_command("/revoke @user1", None, &ctx).unwrap();
        assert!(matches!(cmd.action, GrantAction::Revoke));
        assert_eq!(cmd.targets.len(), 1);
    }

    #[test]
    fn grant_pending_entry_tracks_denied_cooldown() {
        let mut entry = GrantPendingEntry {
            nonce: "nonce".to_string(),
            targets: vec!["target".to_string()],
            quota: Some(3),
            ts: 100,
            state: GrantPendingState::Pending,
        };
        assert!(entry.is_pending());
        assert!(entry.is_throttled(101));

        entry.mark_denied(1_000);
        assert!(!entry.is_pending());
        assert!(entry.is_throttled(1_000 + 1));
        assert!(!entry.is_throttled(1_000 + 10 * 60 * 1000));
    }

    #[test]
    fn grant_pending_persistence_preserves_key() {
        let tmp = std::env::temp_dir().join(format!(
            "beam-grant-pending-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);
        // We need grant_pending_json to return our temp file path.
        // Since we can't override BeamPaths easily, let's test directly.
        let mut map = std::collections::HashMap::new();
        let key = "app1:chat1:user1".to_string();
        map.insert(
            key.clone(),
            GrantPendingEntry {
                nonce: "n1".to_string(),
                targets: vec!["user1".to_string()],
                quota: Some(5),
                ts: chrono::Utc::now().timestamp_millis() as u64,
                state: GrantPendingState::Pending,
            },
        );
        let _ = beam_core::persist::atomic_write_json(
            &tmp,
            &map.iter()
                .map(|(k, v)| GrantPendingPersisted {
                    key: k.clone(),
                    nonce: v.nonce.clone(),
                    targets: v.targets.clone(),
                    quota: v.quota,
                    ts: v.ts,
                    state: v.state.clone(),
                })
                .collect::<Vec<_>>(),
        );
        // Load through the standard load function (simulate)
        let loaded: Option<Vec<GrantPendingPersisted>> =
            beam_core::persist::read_json(&tmp).unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].key, "app1:chat1:user1");
        assert_eq!(loaded[0].nonce, "n1");
        let _ = std::fs::remove_file(&tmp);
    }
}
