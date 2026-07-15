//! File/image upload and card building for final output.

use std::path::Path;

use crate::*;

// ---------------------------------------------------------------------------
// card building
// ---------------------------------------------------------------------------

pub(crate) fn build_final_output_footer(recipient_open_id: Option<&str>) -> Option<String> {
    let mut parts = vec![DEFAULT_BRAND_LABEL.to_string()];
    if let Some(open_id) = recipient_open_id.filter(|open_id| !open_id.trim().is_empty()) {
        parts.push(format!("发送给：<at id={}></at>", open_id));
    }
    if parts.is_empty() {
        None
    } else {
        Some(format!("<font color='grey'>{}</font>", parts.join(" · ")))
    }
}

pub(crate) fn build_final_output_card_with_images(
    content: &str,
    recipient_open_id: Option<&str>,
    kind: Option<FinalOutputKind>,
    user_text: Option<&str>,
    cli_label: Option<&str>,
    image_keys: &[String],
) -> String {
    let mut elements = Vec::new();
    match kind.unwrap_or(FinalOutputKind::Bridge) {
        FinalOutputKind::Bridge => {
            elements.push(serde_json::json!({
                "tag": "markdown",
                "content": content,
                "i18n_content": {
                    "zh_cn": content,
                    "en_us": content,
                },
            }));
        }
        FinalOutputKind::LocalTurn => {
            return build_contextual_reply_card(
                "🖥️ 终端本地对话（在 adopted pane 中直接输入，已同步至飞书）",
                "🖥️ Local terminal conversation (type directly in the adopted pane; synced to Feishu)",
                user_text,
                content,
                cli_label.unwrap_or("助手"),
                cli_label.unwrap_or("Assistant"),
                recipient_open_id,
            );
        }
        FinalOutputKind::LocalTurnHeadless => {
            return build_contextual_reply_card(
                "🖥️ 终端本地对话续传（daemon 重启时模型正在输出）",
                "🖥️ Local terminal conversation resumed (model was still streaming when daemon restarted)",
                None,
                content,
                cli_label.unwrap_or("助手"),
                cli_label.unwrap_or("Assistant"),
                recipient_open_id,
            );
        }
    }

    // ---- inline images (before footer) ----
    for key in image_keys {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        elements.push(serde_json::json!({
            "tag": "img",
            "img_key": key,
            "alt": { "tag": "plain_text", "content": "" },
            "mode": "fit_horizontal",
            "preview": true,
        }));
    }

    if let Some(footer) = build_final_output_footer(recipient_open_id) {
        let footer_text = footer.clone();
        elements.push(serde_json::json!({ "tag": "hr" }));
        elements.push(serde_json::json!({
            "tag": "markdown",
            "text_size": "notation_small_v2",
            "content": footer_text,
            "i18n_content": {
                "zh_cn": footer.clone(),
                "en_us": footer,
            },
        }));
    }
    serde_json::json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
        },
        "body": {
            "direction": "vertical",
            "elements": elements,
        }
    })
    .to_string()
}

pub(crate) fn build_final_output_card(
    content: &str,
    recipient_open_id: Option<&str>,
    kind: Option<FinalOutputKind>,
    user_text: Option<&str>,
    cli_label: Option<&str>,
) -> String {
    build_final_output_card_with_images(content, recipient_open_id, kind, user_text, cli_label, &[])
}

// ---------------------------------------------------------------------------
// MIME type guess
// ---------------------------------------------------------------------------

/// Simple MIME type guess from file extension. Falls back to application/octet-stream.
fn guess_mime_type(path: &str) -> &'static str {
    let lower = path.to_lowercase();
    if lower.ends_with(".png") {
        "image/png"
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        "image/jpeg"
    } else if lower.ends_with(".gif") {
        "image/gif"
    } else if lower.ends_with(".webp") {
        "image/webp"
    } else if lower.ends_with(".bmp") {
        "image/bmp"
    } else if lower.ends_with(".svg") {
        "image/svg+xml"
    } else if lower.ends_with(".pdf") {
        "application/pdf"
    } else if lower.ends_with(".txt") || lower.ends_with(".log") {
        "text/plain"
    } else if lower.ends_with(".html") || lower.ends_with(".htm") {
        "text/html"
    } else if lower.ends_with(".json") {
        "application/json"
    } else if lower.ends_with(".csv") {
        "text/csv"
    } else if lower.ends_with(".md") {
        "text/markdown"
    } else if lower.ends_with(".zip") {
        "application/zip"
    } else {
        "application/octet-stream"
    }
}

// ---------------------------------------------------------------------------
// Lark upload helpers
// ---------------------------------------------------------------------------

/// Upload a local image to Lark and return its `image_key` for inlining in cards.
///
/// Does NOT send an independent image message. Callers should embed the returned
/// `image_key` into an interactive card's `img` element.
pub(crate) async fn upload_lark_image(
    state: &AppState,
    bot: &BotConfig,
    image_path: &str,
) -> Result<String> {
    let data = tokio::fs::read(image_path)
        .await
        .with_context(|| format!("failed to read image: {}", image_path))?;
    let mime = guess_mime_type(image_path);

    let token = lark_tenant_token(state, bot).await?;
    let part = reqwest::multipart::Part::bytes(data)
        .file_name("image")
        .mime_str(mime)
        .with_context(|| format!("invalid MIME type for image: {}", image_path))?;

    let form = reqwest::multipart::Form::new()
        .text("image_type", "message")
        .part("image", part);

    let upload_resp = state
        .http
        .post(format!("{}/im/v1/images", lark_base_url()))
        .bearer_auth(&token)
        .multipart(form)
        .send()
        .await?;
    let status = upload_resp.status();
    let upload_body = upload_resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark image upload failed: {}", upload_body);
    }
    let upload_value: Value =
        serde_json::from_str(&upload_body).context("parse lark image upload response")?;
    let image_key = upload_value
        .pointer("/data/image_key")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .context("lark upload missing image_key")?;
    Ok(image_key)
}

/// Upload a local file to Lark and send it as a file message in the given chat.
pub(super) async fn send_lark_file_message(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    file_path: &str,
) -> Result<()> {
    let data = tokio::fs::read(file_path)
        .await
        .with_context(|| format!("failed to read file: {}", file_path))?;
    let file_name = Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file");
    let mime = guess_mime_type(file_path);

    let token = lark_tenant_token(state, bot).await?;
    let part = reqwest::multipart::Part::bytes(data)
        .file_name(file_name.to_string())
        .mime_str(mime)
        .with_context(|| format!("invalid MIME type for file: {}", file_path))?;

    let form = reqwest::multipart::Form::new()
        .text("file_type", "stream")
        .text("file_name", file_name.to_string())
        .part("file", part);

    let upload_resp = state
        .http
        .post(format!("{}/im/v1/files", lark_base_url()))
        .bearer_auth(&token)
        .multipart(form)
        .send()
        .await?;
    let status = upload_resp.status();
    let upload_body = upload_resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark file upload failed: {}", upload_body);
    }
    let upload_value: Value =
        serde_json::from_str(&upload_body).context("parse lark upload response")?;
    let file_key = upload_value
        .pointer("/data/file_key")
        .and_then(Value::as_str)
        .context("lark upload missing file_key")?;

    // Send as file message
    let msg_body = serde_json::json!({ "file_key": file_key }).to_string();
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages?receive_id_type=chat_id",
            lark_base_url()
        ))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "receive_id": chat_id,
            "content": msg_body,
            "msg_type": "file",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("lark file send failed: {}", payload);
    }
    Ok(())
}
