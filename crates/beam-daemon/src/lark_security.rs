use super::*;

pub(crate) fn lark_base_url() -> String {
    std::env::var("BEAM_LARK_BASE_URL")
        .unwrap_or_else(|_| "https://open.feishu.cn/open-apis".to_string())
        .trim_end_matches('/')
        .to_string()
}

pub(crate) fn header_string(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn lark_encrypt_key(state: &AppState, bot: &BotConfig) -> Option<String> {
    bot.lark_encrypt_key
        .clone()
        .or_else(|| state.config.lark.encrypt_key.clone())
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn lark_verification_token(state: &AppState, bot: &BotConfig) -> Option<String> {
    bot.lark_verification_token
        .clone()
        .or_else(|| state.config.lark.verification_token.clone())
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn compute_lark_signature(
    timestamp: &str,
    nonce: &str,
    encrypt_key: &str,
    body: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(timestamp.as_bytes());
    hasher.update(nonce.as_bytes());
    hasher.update(encrypt_key.as_bytes());
    hasher.update(body);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", byte);
    }
    out
}

pub(crate) fn verify_lark_signature(
    state: &AppState,
    bot: &BotConfig,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<()> {
    let Some(encrypt_key) = lark_encrypt_key(state, bot) else {
        return Ok(());
    };
    let timestamp = header_string(headers, "x-lark-request-timestamp")
        .context("missing x-lark-request-timestamp")?;
    let nonce =
        header_string(headers, "x-lark-request-nonce").context("missing x-lark-request-nonce")?;
    let signature =
        header_string(headers, "x-lark-signature").context("missing x-lark-signature")?;
    let expected = compute_lark_signature(&timestamp, &nonce, &encrypt_key, body);
    if expected != signature {
        anyhow::bail!("invalid lark signature");
    }
    Ok(())
}

pub(crate) fn verify_lark_token(state: &AppState, bot: &BotConfig, payload: &Value) -> Result<()> {
    let Some(expected) = lark_verification_token(state, bot) else {
        return Ok(());
    };
    let actual = payload
        .get("token")
        .and_then(Value::as_str)
        .context("missing lark verification token")?;
    if actual != expected {
        anyhow::bail!("invalid lark verification token");
    }
    Ok(())
}

pub(crate) async fn dedupe_lark_event(state: &AppState, event_key: &str) -> bool {
    let ttl = Duration::from_secs(300);
    let cutoff = Instant::now() - ttl;
    let mut events = state.recent_lark_events.lock().await;
    events.retain(|_, seen_at| *seen_at >= cutoff);
    if events.contains_key(event_key) {
        return true;
    }
    events.insert(event_key.to_string(), Instant::now());
    false
}

pub(crate) async fn consume_inbound_quota(
    state: &AppState,
    lark_app_id: &str,
    quota_key: &str,
) -> Result<grant::QuotaResult, (StatusCode, String)> {
    let bots_path = state.paths.bots_json();
    let raw = tokio::fs::read_to_string(&bots_path)
        .await
        .unwrap_or_else(|_| "[]".to_string());
    let mut config: serde_json::Value = serde_json::from_str(&raw).unwrap_or(serde_json::json!([]));
    let result =
        grant::consume_quota(&mut config, lark_app_id, quota_key).map_err(internal_error)?;
    if result.allowed {
        tokio::fs::write(
            &bots_path,
            serde_json::to_string_pretty(&config).unwrap_or_default(),
        )
        .await
        .map_err(internal_error)?;
    }
    Ok(result)
}

#[cfg(test)]
pub(crate) fn can_operate_bot(bot: &BotConfig, sender_open_id: Option<&str>) -> bool {
    let Some(sender) = sender_open_id else {
        return bot.allowed_users.is_empty();
    };
    can_operate(bot, sender, &bot.allowed_users, &[])
}

pub(crate) fn can_operate_bot_with_state(
    state: &AppState,
    bot: &BotConfig,
    sender_open_id: Option<&str>,
) -> bool {
    let Some(sender) = sender_open_id else {
        return bot.allowed_users.is_empty();
    };
    let peer_bot_open_ids = peer_bot_open_ids_for_app(&state.paths, &bot.lark_app_id);
    can_operate(bot, sender, &bot.allowed_users, &peer_bot_open_ids)
}

pub(crate) fn card_action_requires_operate(action: &str) -> bool {
    matches!(
        action,
        "restart"
            | "close"
            | "resume"
            | "skip_repo"
            | "retry_last_task"
            | "get_write_link"
            | "term_action"
            | "takeover"
            | "disconnect"
            | "tui_keys"
            | "tui_text_input"
            | "wf_approve"
            | "wf_reject"
            | "wf_cancel"
            | "dir_select_pick"
            | "dir_select_filter"
            | "dir_select_best"
    )
}

#[cfg(test)]
pub(crate) fn evaluate_talk_for_bot(
    bot: &BotConfig,
    chat_id: &str,
    sender_open_id: &str,
) -> TalkEvaluation {
    evaluate_talk(bot, chat_id, sender_open_id, &bot.allowed_users, &[])
}

pub(crate) fn evaluate_talk_for_bot_with_state(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    sender_open_id: &str,
) -> TalkEvaluation {
    let peer_bot_open_ids = peer_bot_open_ids_for_app(&state.paths, &bot.lark_app_id);
    evaluate_talk(
        bot,
        chat_id,
        sender_open_id,
        &bot.allowed_users,
        &peer_bot_open_ids,
    )
}

const LARK_CODE_MESSAGE_WITHDRAWN: i64 = 230011;

pub(crate) fn is_lark_message_withdrawn_payload(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|value| value.get("code").and_then(Value::as_i64))
        == Some(LARK_CODE_MESSAGE_WITHDRAWN)
        || payload.contains("230011")
        || payload.to_ascii_lowercase().contains("withdrawn")
}

pub(crate) fn is_lark_message_withdrawn_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| is_lark_message_withdrawn_payload(&cause.to_string()))
}

pub(crate) fn is_operate_command(text: &str) -> bool {
    matches!(
        text,
        "/close" | "/restart" | "/card" | "/adopt" | "/adopt list"
    ) || text.starts_with("/adopt ")
}

pub(crate) async fn lark_tenant_token(state: &AppState, bot: &BotConfig) -> Result<String> {
    if let Some(cached) = state
        .lark_tokens
        .lock()
        .await
        .get(&bot.lark_app_id)
        .cloned()
        && cached.expires_at > Instant::now() + Duration::from_secs(30)
    {
        return Ok(cached.token);
    }

    let resp = state
        .http
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
        anyhow::bail!(
            "lark tenant_access_token failed: {}",
            body.msg.unwrap_or_else(|| "unknown error".to_string())
        );
    }
    let token = body
        .tenant_access_token
        .context("lark tenant_access_token missing")?;
    let ttl = body.expire.unwrap_or(7200);
    state.lark_tokens.lock().await.insert(
        bot.lark_app_id.clone(),
        CachedLarkToken {
            token: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(ttl.saturating_sub(60)),
        },
    );
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lark_signature_matches_known_digest() {
        let body = br#"{"event":"demo"}"#;
        let actual = compute_lark_signature("1710000000", "nonce", "secret", body);
        assert_eq!(
            actual,
            "aa99ff23621bc571ba6ad9ec2989f4b336458b51b5ff88297eca00d55af04740"
        );
    }

    #[test]
    fn operate_permission_defaults_open_without_allowlist() {
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: Vec::new(),
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: Vec::new(),
        };
        assert!(can_operate_bot(&bot, None));
        assert!(can_operate_bot(&bot, Some("ou_123")));
    }

    #[test]
    fn operate_permission_respects_allowlist() {
        let bot = BotConfig {
            name: None,
            backend: None,
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cgroup_slice: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_owner".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
            custom_triggers: Vec::new(),
        };
        assert!(can_operate_bot(&bot, Some("ou_owner")));
        assert!(!can_operate_bot(&bot, Some("ou_other")));
        assert!(!can_operate_bot(&bot, None));
    }
}
