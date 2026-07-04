use super::*;

/// Lark reply with a text message (defaults to non-thread).
pub(crate) async fn lark_reply_message(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    text: &str,
) -> Result<String> {
    lark_reply_message_with_opts(state, bot, message_id, text, false).await
}

pub(crate) async fn lark_reply_message_with_opts(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    text: &str,
    reply_in_thread: bool,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let mut body = serde_json::json!({
        "content": serde_json::json!({ "text": text }).to_string(),
        "msg_type": "text",
    });
    if reply_in_thread {
        body.as_object_mut()
            .unwrap()
            .insert("reply_in_thread".to_string(), serde_json::Value::Bool(true));
    }
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages/{}/reply",
            lark_base_url(),
            message_id
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark reply failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark reply missing message_id")
}

pub(crate) async fn lark_send_chat_message(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    text: &str,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages?receive_id_type=chat_id",
            lark_base_url()
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "receive_id": chat_id,
            "content": serde_json::json!({ "text": text }).to_string(),
            "msg_type": "text",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark send failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark send missing message_id")
}

pub(crate) async fn lark_send_post_message(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    content: &str,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages?receive_id_type=chat_id",
            lark_base_url()
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "receive_id": chat_id,
            "content": content,
            "msg_type": "post",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark send post failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark post send missing message_id")
}

pub(crate) async fn lark_reply_post_message(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    content: &str,
) -> Result<String> {
    lark_reply_post_message_with_opts(state, bot, message_id, content, false).await
}

pub(crate) async fn lark_reply_post_message_with_opts(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    content: &str,
    reply_in_thread: bool,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let mut body = serde_json::json!({
        "content": content,
        "msg_type": "post",
    });
    if reply_in_thread {
        body.as_object_mut()
            .unwrap()
            .insert("reply_in_thread".to_string(), serde_json::Value::Bool(true));
    }
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages/{}/reply",
            lark_base_url(),
            message_id
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark reply post failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark reply post missing message_id")
}

pub(crate) fn build_report_post_content(session: &Session, content: &str) -> String {
    let mut paragraphs: Vec<Vec<Value>> = Vec::new();
    let mut lines = content.lines();
    if let Some(first) = lines.next() {
        let mut head = Vec::new();
        if let Some(owner) = session
            .owner_open_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            head.push(serde_json::json!({ "tag": "at", "user_id": owner }));
            head.push(serde_json::json!({ "tag": "text", "text": " " }));
        }
        head.push(serde_json::json!({ "tag": "text", "text": first }));
        paragraphs.push(head);
    }
    for line in lines {
        paragraphs.push(vec![serde_json::json!({ "tag": "text", "text": line })]);
    }
    serde_json::json!({
        "zh_cn": { "title": "", "content": paragraphs },
    })
    .to_string()
}

pub(crate) async fn send_lark_card_in_chat(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    card_json: &str,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages?receive_id_type=chat_id",
            lark_base_url()
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "receive_id": chat_id,
            "content": card_json,
            "msg_type": "interactive",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark card send failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark card send missing message_id")
}

pub(crate) const DEFAULT_BRAND_LABEL: &str = "[beam](https://github.com/deepcoldy/beam)";
pub(crate) const COMPLETED_REACTION_EMOJI_TYPE: &str = "DONE";

pub(crate) async fn lark_add_reaction(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    emoji_type: &str,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages/{}/reactions",
            lark_base_url(),
            message_id
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "reaction_type": {
                "emoji_type": emoji_type,
            }
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark add reaction failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/reaction_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark reaction missing reaction_id")
}
