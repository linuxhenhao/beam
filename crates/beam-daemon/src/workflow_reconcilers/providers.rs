//! Concrete ProviderReconciler implementations for built-in providers.

use anyhow::{Context, Result};
use async_trait::async_trait;
use beam_core::{BeamPaths, get_task};
use serde_json::Value;

use super::registry::ProviderReconciler;
use crate::AppState;

// ---------------------------------------------------------------------------
// BeamScheduleReconciler
// ---------------------------------------------------------------------------

/// Reconciler for `beam-schedule`: looks up the scheduled task by idempotency key.
///
/// Capabilities: `readOnlyLookup`.
/// Does NOT need the effect-input sidecar (uses idempotency key for lookup).
pub struct BeamScheduleReconciler;

#[async_trait]
impl ProviderReconciler for BeamScheduleReconciler {
    fn provider_name(&self) -> &str {
        "beam-schedule"
    }

    fn requires_effect_input(&self) -> bool {
        false
    }

    fn canonical_input(&self, raw_input: &Value) -> Result<Value> {
        // For read-only lookups we don't need canonical input, but provide
        // the raw input as-is for cases where the caller wants a representation.
        Ok(raw_input.clone())
    }

    async fn read_only_lookup(
        &self,
        _state: &AppState,
        paths: &BeamPaths,
        idempotency_key: &str,
    ) -> Result<Option<Value>> {
        match get_task(paths, idempotency_key)? {
            Some(task) => {
                let evidence = serde_json::json!({
                    "source": "getTask",
                    "externalRefs": { "taskId": task.id },
                });
                Ok(Some(evidence))
            }
            None => Ok(None),
        }
    }

    async fn idempotent_submit(
        &self,
        _state: &AppState,
        _canonical_input: &Value,
    ) -> Result<Value> {
        // beam-schedule uses readOnlyLookup; idempotentSubmit is not applicable.
        // If the task doesn't exist, the caller should issue a freshRetry.
        anyhow::bail!(
            "beam-schedule does not support idempotentSubmit; use readOnlyLookup + freshRetry"
        )
    }

    fn is_retryable_error(&self, _err: &anyhow::Error) -> bool {
        // File system / local store errors are not retryable in the provider sense
        false
    }

    fn supports_read_only_lookup(&self) -> bool {
        true
    }

    fn supports_idempotent_submit(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// FeishuImReconciler
// ---------------------------------------------------------------------------

/// Reconciler for `feishu-im`: re-sends a chat message as idempotent submit.
///
/// Capabilities: `idempotentSubmit`.
/// Requires the effect-input sidecar (needs `larkAppId`, `chatId`/`rootMessageId`, `content`).
pub struct FeishuImReconciler;

impl FeishuImReconciler {
    /// Parse the raw sidecar input into a structured form.
    fn parse_raw_input(raw_input: &Value) -> Result<crate::FeishuResumeInput> {
        serde_json::from_value::<crate::FeishuResumeInput>(raw_input.clone())
            .context("invalid feishu-im effect input")
    }
}

#[async_trait]
impl ProviderReconciler for FeishuImReconciler {
    fn provider_name(&self) -> &str {
        "feishu-im"
    }

    fn requires_effect_input(&self) -> bool {
        true
    }

    fn canonical_input(&self, raw_input: &Value) -> Result<Value> {
        let parsed = Self::parse_raw_input(raw_input)?;
        let mut canonical = serde_json::json!({
            "larkAppId": parsed.lark_app_id,
            "content": parsed.content,
        });
        if let Some(chat_id) = &parsed.chat_id {
            canonical["chatId"] = serde_json::Value::String(chat_id.clone());
        }
        if let Some(root_message_id) = &parsed.root_message_id {
            canonical["rootMessageId"] = serde_json::Value::String(root_message_id.clone());
        }
        Ok(canonical)
    }

    async fn read_only_lookup(
        &self,
        _state: &AppState,
        _paths: &BeamPaths,
        _idempotency_key: &str,
    ) -> Result<Option<Value>> {
        // feishu-im does not support a read-only lookup (no "get message by idempotency key" API)
        Ok(None)
    }

    async fn idempotent_submit(&self, state: &AppState, canonical_input: &Value) -> Result<Value> {
        let parsed = Self::parse_raw_input(canonical_input)?;

        let bot = state
            .bots
            .get(&parsed.lark_app_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("bot '{}' is not registered.", parsed.lark_app_id))?;

        let (submit_kind, message_id) = if let Some(chat_id) = parsed.chat_id.as_deref() {
            let mid = crate::lark_send_chat_message(state, &bot, chat_id, &parsed.content).await?;
            ("send", mid)
        } else if let Some(root_message_id) = parsed.root_message_id.as_deref() {
            let mid =
                crate::lark_reply_message(state, &bot, root_message_id, &parsed.content).await?;
            ("reply", mid)
        } else {
            anyhow::bail!("feishu-im effect input missing both chatId and rootMessageId");
        };

        let evidence = serde_json::json!({
            "source": "lark",
            "submitKind": submit_kind,
            "messageId": &message_id,
            "externalRefs": { "messageId": &message_id },
        });
        Ok(evidence)
    }

    fn is_retryable_error(&self, err: &anyhow::Error) -> bool {
        crate::is_retryable_feishu_resume_error(err)
    }

    fn supports_read_only_lookup(&self) -> bool {
        false
    }

    fn supports_idempotent_submit(&self) -> bool {
        true
    }
}
