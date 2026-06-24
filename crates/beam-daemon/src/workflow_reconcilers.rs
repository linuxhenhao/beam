//! ProviderReconciler trait and registry for workflow effect reconciliation.
//!
//! ## Error handling convention
//!
//! - Missing reconciler → `manual` recovery (provider is unknown).
//! - Missing effect input when required → `manual` failure.
//! - Input hash mismatch → `manual` failure (no provider call).
//! - Retryable provider errors → transient failure (effect stays dangling).
//! - Non-retryable provider errors → `manual` failure.
//!
//! ## Implementation status
//!
//! - **Task 3.1** (done): trait, registry, reconciler implementations.
//! - **Task 3.2** (done): `reconcile_activity` / `reconcile_provider_dangling_effects`
//!   are the primary recovery path for all registered providers, replacing the
//!   legacy provider-specific `resume_schedule_dangling_effects` /
//!   `resume_feishu_im_dangling_effects` in the daemon resume handler.
//! - `handle_missing_provider_dangling_effects` catches unregistered providers
//!   after registered-provider reconciliation completes.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use async_trait::async_trait;
use beam_core::{BeamPaths, EventDraft, EventLog, WorkflowActor, WorkflowOutputRef, get_task};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::AppState;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A reconciler that recovers dangling effects for a specific provider.
///
/// All trait methods are now exercised through the unified
/// `reconcile_activity` / `reconcile_provider_dangling_effects` path
/// (Task 3.2 merged resume decision tree).
#[async_trait]
pub trait ProviderReconciler: Send + Sync {
    /// The provider name this reconciler handles (e.g. `"beam-schedule"`, `"feishu-im"`).
    fn provider_name(&self) -> &str;

    /// Whether this reconciler needs the effect-input sidecar file
    /// (written before the original `effectAttempted`) to perform reconciliation.
    fn requires_effect_input(&self) -> bool;

    /// Convert raw sidecar input into a canonical (deterministic) representation
    /// suitable for idempotent re-submission.
    ///
    /// The default implementation returns the raw input unchanged.
    fn canonical_input(&self, raw_input: &Value) -> Result<Value> {
        Ok(raw_input.clone())
    }

    /// Read-only lookup: check whether the effect already exists on the
    /// provider side.
    ///
    /// Returns `Some(evidence)` if the effect was already completed,
    /// `None` if there is no record of it.
    ///
    /// The default implementation returns `None` (read-only lookup not supported).
    #[allow(unused_variables)]
    async fn read_only_lookup(
        &self,
        state: &AppState,
        paths: &BeamPaths,
        idempotency_key: &str,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    /// Idempotent submit: re-submit the effect to the provider using the
    /// canonical input.
    ///
    /// Returns `Ok(evidence)` on success (e.g. `{"messageId":"…"}`).
    ///
    /// The default implementation returns an error (idempotent submit not supported).
    #[allow(unused_variables)]
    async fn idempotent_submit(&self, state: &AppState, canonical_input: &Value) -> Result<Value> {
        anyhow::bail!(
            "idempotentSubmit is not supported for provider '{}'",
            self.provider_name()
        )
    }

    /// Whether an error from this provider is retryable (transient).
    ///
    /// Retryable errors cause the effect to remain dangling so it can be
    /// retried on the next resume cycle. Non-retryable errors result in a
    /// `manual` failure.
    fn is_retryable_error(&self, err: &anyhow::Error) -> bool;

    /// Whether this reconciler supports `readOnlyLookup`.
    ///
    /// If `readOnlyLookup` is supported and returns `None`, and
    /// `supports_idempotent_submit()` is false, the reconciler will issue a
    /// `freshRetry` (instead of falling through to idempotent submit which
    /// would fail).
    fn supports_read_only_lookup(&self) -> bool {
        false
    }

    /// Whether this reconciler supports `idempotentSubmit`.
    fn supports_idempotent_submit(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Registry of provider reconcilers, keyed by provider name.
pub struct ProviderReconcilerRegistry {
    reconcilers: HashMap<String, Box<dyn ProviderReconciler>>,
}

impl ProviderReconcilerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            reconcilers: HashMap::new(),
        }
    }

    /// Register a reconciler.
    pub fn register(&mut self, reconciler: Box<dyn ProviderReconciler>) {
        let name = reconciler.provider_name().to_string();
        self.reconcilers.insert(name, reconciler);
    }

    /// Look up a reconciler by provider name.
    pub fn get(&self, provider: &str) -> Option<&dyn ProviderReconciler> {
        self.reconcilers.get(provider).map(|b| b.as_ref())
    }

    /// Returns an iterator over all registered provider names.
    #[allow(dead_code)]
    pub fn providers(&self) -> impl Iterator<Item = &str> {
        self.reconcilers.keys().map(|k| k.as_str())
    }
}

impl Default for ProviderReconcilerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Concrete reconcilers
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

// ---------------------------------------------------------------------------
// Reconciliation helpers
// ---------------------------------------------------------------------------

/// Outcome of reconciling a single dangling effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileActivityOutcome {
    /// Effect was successfully reconciled (terminal event written).
    Reconciled {
        activity_id: String,
        attempt_id: String,
        decision: String,
    },
    /// Effect should be retried from scratch.
    FreshRetry {
        activity_id: String,
        attempt_id: String,
    },
    /// Provider error is transient – effect remains dangling.
    TransientFailure {
        activity_id: String,
        attempt_id: String,
        provider: String,
        idempotency_key: String,
        error_code: String,
        error_message: String,
    },
    /// Provider not registered – manual failure written.
    ManualRecovery {
        activity_id: String,
        attempt_id: String,
        reason: String,
    },
    /// This activity was skipped (not applicable to this reconciler).
    #[allow(dead_code)]
    Skipped { activity_id: String, reason: String },
}

/// Run reconciliation for a single dangling activity using the given reconciler.
///
/// This function:
/// 1. Checks for prior `reconcileResult` recovery (covers crash-recovery of a previous reconcile cycle).
/// 2. If the reconciler supports `readOnlyLookup`, tries that first.
/// 3. If the reconciler supports `idempotentSubmit`, tries that next.
/// 4. On success, writes `reconcileResult` + `activitySucceeded`.
/// 5. On failure, writes `reconcileResult` + `activityFailed` (manual recovery) or returns transient failure.
///
/// If `expected_input_hash` is provided and the reconciler requires effect input,
/// the canonical input hash is validated against the expected hash from the
/// original `effectAttempted` event before calling `idempotentSubmit`.
/// A mismatch results in `manual` recovery without contacting the provider.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_activity(
    reconciler: &dyn ProviderReconciler,
    state: &AppState,
    log: &mut EventLog,
    run_dir: &std::path::Path,
    activity_id: &str,
    attempt_id: &str,
    idempotency_key: &str,
    sidecar_input: Option<&Value>,
    expected_input_hash: Option<&str>,
) -> Result<Vec<ReconcileActivityOutcome>> {
    let mut outcomes = Vec::new();

    // --- Step 1: Check prior reconcileResult recovery ---
    let snapshot = beam_core::read_run_snapshot(run_dir)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing run snapshot for activity {}", activity_id))?;
    let activity = snapshot
        .activities
        .iter()
        .find(|a| a.activity_id == activity_id);
    if let Some(latest) = activity.and_then(|a| a.attempts.last()) {
        if let Some(recovery) =
            beam_core::recover_prior_reconcile_result(log, activity_id, latest).await?
        {
            match recovery {
                beam_core::PriorReconcileRecoveryOutcome::Recovered {
                    activity_id,
                    attempt_id,
                    decision,
                } => {
                    outcomes.push(ReconcileActivityOutcome::Reconciled {
                        activity_id,
                        attempt_id,
                        decision,
                    });
                    return Ok(outcomes);
                }
                beam_core::PriorReconcileRecoveryOutcome::FreshRetry {
                    activity_id,
                    attempt_id,
                } => {
                    outcomes.push(ReconcileActivityOutcome::FreshRetry {
                        activity_id,
                        attempt_id,
                    });
                    return Ok(outcomes);
                }
            }
        }
    }

    // --- Step 2: Try readOnlyLookup (only if the reconciler declares support) ---
    if reconciler.supports_read_only_lookup() {
        match reconciler
            .read_only_lookup(state, &state.paths, idempotency_key)
            .await
        {
            Ok(Some(evidence)) => {
                let external_refs = evidence
                    .get("externalRefs")
                    .cloned()
                    .and_then(|v| v.as_object().cloned().map(Value::Object))
                    .unwrap_or_else(|| evidence.clone());
                let output_ref = write_json_blob(log, external_refs.clone())?;
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "readOnlyLookup",
                        "decision": "completedByIdempotentSubmit",
                        "evidence": evidence,
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                let _ = log.append(EventDraft {
                    event_type: "activitySucceeded".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "outputRef": output_ref,
                        "externalRefs": { "taskId": external_refs.get("taskId") },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::Reconciled {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    decision: "completedByIdempotentSubmit".to_string(),
                });
                return Ok(outcomes);
            }
            Ok(None) => {
                // readOnlyLookup found nothing.
                // If this reconciler does NOT support idempotentSubmit, issue
                // freshRetry so the caller can recreate the effect from scratch.
                if !reconciler.supports_idempotent_submit() {
                    let _ = log.append(EventDraft {
                        event_type: "reconcileResult".to_string(),
                        actor: WorkflowActor::System,
                        payload: serde_json::json!({
                            "activityId": activity_id,
                            "idempotencyKey": idempotency_key,
                            "capability": "readOnlyLookup",
                            "decision": "freshRetry",
                            "evidence": {
                                "source": "getTask",
                                "returned": "undefined",
                            },
                        }),
                        timestamp: None,
                        payload_hash: None,
                    })?;
                    outcomes.push(ReconcileActivityOutcome::FreshRetry {
                        activity_id: activity_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                    });
                    return Ok(outcomes);
                }
                // Otherwise fall through to idempotentSubmit
            }
            Err(err) => {
                // readOnlyLookup failed – treat as transient unless we have idempotentSubmit fallback
                if reconciler.is_retryable_error(&err) {
                    outcomes.push(ReconcileActivityOutcome::TransientFailure {
                        activity_id: activity_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                        provider: reconciler.provider_name().to_string(),
                        idempotency_key: idempotency_key.to_string(),
                        error_code: "ReconcilerReadOnlyLookupError".to_string(),
                        error_message: format!("{:#}", err),
                    });
                    return Ok(outcomes);
                }
                // Non-retryable read-only error: fall through to try idempotentSubmit
            }
        }
    }

    // --- Step 3: Try idempotentSubmit ---
    let canonical_input = if let Some(raw) = sidecar_input {
        match reconciler.canonical_input(raw) {
            Ok(ci) => {
                // --- Validate input hash against the original effectAttempted.inputHash ---
                if let Some(expected) = expected_input_hash {
                    if !expected.is_empty() {
                        let actual_bytes = serde_json::to_vec(&ci)?;
                        let actual_hash = sha256_hex(&actual_bytes);
                        if actual_hash != expected {
                            let _ = log.append(EventDraft {
                                event_type: "reconcileResult".to_string(),
                                actor: WorkflowActor::System,
                                payload: serde_json::json!({
                                    "activityId": activity_id,
                                    "attemptId": attempt_id,
                                    "idempotencyKey": idempotency_key,
                                    "capability": "idempotentSubmit",
                                    "decision": "manual",
                                    "evidence": {
                                        "source": "effectInputSidecar",
                                        "returned": "hashMismatch",
                                        "expectedHash": expected,
                                        "actualHash": actual_hash,
                                    },
                                }),
                                timestamp: None,
                                payload_hash: None,
                            })?;
                            let _ = log.append(EventDraft {
                                event_type: "activityFailed".to_string(),
                                actor: WorkflowActor::System,
                                payload: serde_json::json!({
                                    "activityId": activity_id,
                                    "attemptId": attempt_id,
                                    "error": {
                                        "errorCode": "EffectInputHashMismatch",
                                        "errorClass": "manual",
                                        "errorMessage": format!(
                                            "effect input hash mismatch: expected {expected}, got {actual_hash}"
                                        ),
                                    }
                                }),
                                timestamp: None,
                                payload_hash: None,
                            })?;
                            outcomes.push(ReconcileActivityOutcome::ManualRecovery {
                                activity_id: activity_id.to_string(),
                                attempt_id: attempt_id.to_string(),
                                reason: format!(
                                    "effect input hash mismatch: expected {expected}, got {actual_hash}"
                                ),
                            });
                            return Ok(outcomes);
                        }
                    }
                }
                Some(ci)
            }
            Err(err) => {
                // Invalid input – manual failure
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "manual",
                        "evidence": {
                            "source": "effectInputSidecar",
                            "returned": "invalid",
                        },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                let _ = log.append(EventDraft {
                    event_type: "activityFailed".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "error": {
                            "errorCode": "InvalidEffectInput",
                            "errorClass": "manual",
                            "errorMessage": format!("{:#}", err),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::ManualRecovery {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    reason: format!("invalid effect input: {:#}", err),
                });
                return Ok(outcomes);
            }
        }
    } else if reconciler.requires_effect_input() {
        // Sidecar missing but required – manual failure
        let _ = log.append(EventDraft {
            event_type: "reconcileResult".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": attempt_id,
                "idempotencyKey": idempotency_key,
                "capability": "idempotentSubmit",
                "decision": "manual",
                "evidence": {
                    "source": "effectInputSidecar",
                    "returned": "missing",
                },
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        let _ = log.append(EventDraft {
            event_type: "activityFailed".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": attempt_id,
                "error": {
                    "errorCode": "MissingEffectInputSidecar",
                    "errorClass": "manual",
                    "errorMessage": "effect input sidecar is missing".to_string(),
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        outcomes.push(ReconcileActivityOutcome::ManualRecovery {
            activity_id: activity_id.to_string(),
            attempt_id: attempt_id.to_string(),
            reason: "missing effect input sidecar".to_string(),
        });
        return Ok(outcomes);
    } else {
        None
    };

    match reconciler
        .idempotent_submit(state, canonical_input.as_ref().unwrap_or(&Value::Null))
        .await
    {
        Ok(evidence) => {
            let external_refs = evidence
                .get("externalRefs")
                .cloned()
                .unwrap_or_else(|| evidence.clone());
            let output_ref = write_json_blob(log, external_refs.clone())?;
            let _ = log.append(EventDraft {
                event_type: "reconcileResult".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "idempotencyKey": idempotency_key,
                    "capability": "idempotentSubmit",
                    "decision": "completedByIdempotentSubmit",
                    "evidence": evidence,
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            let _ = log.append(EventDraft {
                event_type: "activitySucceeded".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": attempt_id,
                    "outputRef": output_ref,
                    "externalRefs": external_refs,
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            outcomes.push(ReconcileActivityOutcome::Reconciled {
                activity_id: activity_id.to_string(),
                attempt_id: attempt_id.to_string(),
                decision: "completedByIdempotentSubmit".to_string(),
            });
        }
        Err(err) => {
            if crate::is_lark_message_withdrawn_error(&err) {
                // Lark message was withdrawn – manual failure (can't resend)
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "manual",
                        "evidence": {
                            "source": "lark",
                            "submitKind": "sendOrReply",
                        },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                let _ = log.append(EventDraft {
                    event_type: "activityFailed".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "error": {
                            "errorCode": "MessageWithdrawnError",
                            "errorClass": "manual",
                            "errorMessage": format!("{:#}", err),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::Reconciled {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    decision: "manual".to_string(),
                });
            } else if reconciler.is_retryable_error(&err) {
                outcomes.push(ReconcileActivityOutcome::TransientFailure {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    provider: reconciler.provider_name().to_string(),
                    idempotency_key: idempotency_key.to_string(),
                    error_code: "FeishuSubmitRetryable".to_string(),
                    error_message: format!("{:#}", err),
                });
            } else {
                // Non-retryable error – manual failure
                let _ = log.append(EventDraft {
                    event_type: "reconcileResult".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "idempotencyKey": idempotency_key,
                        "capability": "idempotentSubmit",
                        "decision": "manual",
                        "evidence": {
                            "source": "lark",
                            "submitKind": "sendOrReply",
                        },
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                let _ = log.append(EventDraft {
                    event_type: "activityFailed".to_string(),
                    actor: WorkflowActor::System,
                    payload: serde_json::json!({
                        "activityId": activity_id,
                        "attemptId": attempt_id,
                        "error": {
                            "errorCode": "FeishuSubmitFailed",
                            "errorClass": "manual",
                            "errorMessage": format!("{:#}", err),
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })?;
                outcomes.push(ReconcileActivityOutcome::Reconciled {
                    activity_id: activity_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    decision: "manual".to_string(),
                });
            }
        }
    }

    Ok(outcomes)
}

/// Run reconciliation for all dangling effects of a single provider.
///
/// This function:
/// 1. Looks up the reconciler for the given provider.
/// 2. If no reconciler is found, writes manual failures for all matching dangling effects.
/// 3. Otherwise, delegates to `reconcile_activity` for each matching dangling activity.
pub async fn reconcile_provider_dangling_effects(
    registry: &ProviderReconcilerRegistry,
    state: &AppState,
    log: &mut EventLog,
    run_dir: &std::path::Path,
    provider: &str,
    snapshot: &beam_core::RunSnapshotDTO,
) -> Result<ProviderResumeResult> {
    let reconciler = registry.get(provider);

    let mut reconciled = Vec::new();
    let mut fresh_retry = Vec::new();
    let mut transient_failures = Vec::new();
    let mut skipped = Vec::new();

    for activity_id in &snapshot.dangling.effect_attempted {
        let Some(activity) = snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
        else {
            skipped.push(activity_id.clone());
            continue;
        };
        let Some(latest) = activity.attempts.last() else {
            skipped.push(activity_id.clone());
            continue;
        };
        let Some(effect_attempted) = latest.effect_attempted.as_ref() else {
            skipped.push(activity_id.clone());
            continue;
        };
        if effect_attempted.provider != provider {
            skipped.push(activity_id.clone());
            continue;
        }

        let Some(reconciler) = reconciler else {
            // No reconciler registered for this provider → manual recovery
            let _ = log.append(EventDraft {
                event_type: "reconcileResult".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": &latest.attempt_id,
                    "idempotencyKey": &effect_attempted.idempotency_key,
                    "capability": "manual",
                    "decision": "manual",
                    "evidence": {
                        "source": "reconcilerRegistry",
                        "returned": "missing",
                        "message": format!("no reconciler registered for provider '{}'", provider),
                    },
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            let _ = log.append(EventDraft {
                event_type: "activityFailed".to_string(),
                actor: WorkflowActor::System,
                payload: serde_json::json!({
                    "activityId": activity_id,
                    "attemptId": &latest.attempt_id,
                    "error": {
                        "errorCode": "UnknownProviderError",
                        "errorClass": "manual",
                        "errorMessage": format!("no reconciler registered for provider '{}'", provider),
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })?;
            reconciled.push(ProviderResumeOutcome {
                activity_id: activity_id.clone(),
                attempt_id: latest.attempt_id.clone(),
                decision: "manual".to_string(),
            });
            continue;
        };

        // Load sidecar if needed
        let sidecar = if reconciler.requires_effect_input() {
            beam_core::load_effect_input_sidecar(run_dir, activity_id, &latest.attempt_id).await?
        } else {
            None
        };

        let outcomes = reconcile_activity(
            reconciler,
            state,
            log,
            run_dir,
            activity_id,
            &latest.attempt_id,
            &effect_attempted.idempotency_key,
            sidecar.as_ref(),
            Some(&effect_attempted.input_hash),
        )
        .await?;

        for outcome in outcomes {
            match outcome {
                ReconcileActivityOutcome::Reconciled {
                    activity_id,
                    attempt_id,
                    decision,
                } => {
                    reconciled.push(ProviderResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision,
                    });
                }
                ReconcileActivityOutcome::FreshRetry {
                    activity_id,
                    attempt_id,
                } => {
                    fresh_retry.push(ProviderResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision: "freshRetry".to_string(),
                    });
                }
                ReconcileActivityOutcome::TransientFailure {
                    activity_id,
                    attempt_id,
                    provider: p,
                    idempotency_key,
                    error_code,
                    error_message,
                } => {
                    transient_failures.push(ProviderTransientFailure {
                        activity_id,
                        attempt_id,
                        provider: p,
                        idempotency_key,
                        error_code,
                        error_message,
                    });
                }
                ReconcileActivityOutcome::ManualRecovery {
                    activity_id,
                    attempt_id,
                    reason: _,
                } => {
                    reconciled.push(ProviderResumeOutcome {
                        activity_id,
                        attempt_id,
                        decision: "manual".to_string(),
                    });
                }
                ReconcileActivityOutcome::Skipped { .. } => {
                    // Already counted in skipped
                }
            }
        }
    }

    Ok(ProviderResumeResult {
        reconciled,
        fresh_retry,
        transient_failures,
        skipped,
    })
}

// ---------------------------------------------------------------------------
// Result types (mirror the existing ScheduleResumeResult / FeishuResumeResult shapes)
// ---------------------------------------------------------------------------

/// Summary of the reconciler registry check performed during resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerRegistryCheckResult {
    /// Providers for which a reconciler exists (and provider-specific recovery was used).
    pub covered_providers: Vec<String>,
    /// Providers for which no reconciler exists (manual recovery was written).
    pub missing_providers: Vec<String>,
}

/// Outcome for a single activity within a provider's resume result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResumeOutcome {
    pub activity_id: String,
    pub attempt_id: String,
    pub decision: String,
}

/// Transient failure that should be retried on a future resume cycle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTransientFailure {
    pub activity_id: String,
    pub attempt_id: String,
    pub provider: String,
    pub idempotency_key: String,
    pub error_code: String,
    pub error_message: String,
}

/// Full result of reconciling dangling effects for a single provider.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderResumeResult {
    pub reconciled: Vec<ProviderResumeOutcome>,
    pub fresh_retry: Vec<ProviderResumeOutcome>,
    pub transient_failures: Vec<ProviderTransientFailure>,
    pub skipped: Vec<String>,
}

// ---------------------------------------------------------------------------
// Registry factory
// ---------------------------------------------------------------------------

/// Build a registry pre-populated with all built-in provider reconcilers.
pub fn default_reconciler_registry() -> ProviderReconcilerRegistry {
    let mut reg = ProviderReconcilerRegistry::new();
    reg.register(Box::new(BeamScheduleReconciler));
    reg.register(Box::new(FeishuImReconciler));
    reg
}

/// Return a reference to a process-wide default reconciler registry (lazily initialized).
pub fn global_reconciler_registry() -> &'static ProviderReconcilerRegistry {
    static REGISTRY: std::sync::OnceLock<ProviderReconcilerRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(default_reconciler_registry)
}

// ---------------------------------------------------------------------------
// Helpers (re-exports / thin wrappers for use within this module)
// ---------------------------------------------------------------------------

fn write_json_blob(log: &mut EventLog, value: Value) -> Result<WorkflowOutputRef> {
    let bytes = serde_json::to_vec(&value)?;
    let hash = sha256_hex(&bytes);
    let path = PathBuf::from(&log.blob_dir).join(&hash);
    std::fs::write(&path, &bytes)?;
    Ok(WorkflowOutputRef {
        output_hash: format!("sha256:{hash}"),
        output_path: path.display().to_string(),
        output_bytes: bytes.len(),
        output_schema_version: 1,
        content_type: Some("application/json".to_string()),
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

// ---------------------------------------------------------------------------
// Helpers (re-exports / thin wrappers for use within this module)
// ---------------------------------------------------------------------------

/// Scan all dangling `effectAttempted` activities and write `manual` recovery
/// events for any provider that has **no reconciler registered**.
///
/// Returns the list of providers for which a reconciler **was found** (so that
/// caller can continue with provider-specific recovery for those).
pub fn handle_missing_provider_dangling_effects(
    registry: &ProviderReconcilerRegistry,
    log: &mut EventLog,
    snapshot: &beam_core::RunSnapshotDTO,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut covered_providers = Vec::new();
    let mut missing_providers = Vec::new();

    for activity_id in &snapshot.dangling.effect_attempted {
        let Some(activity) = snapshot
            .activities
            .iter()
            .find(|a| &a.activity_id == activity_id)
        else {
            continue;
        };
        let Some(latest) = activity.attempts.last() else {
            continue;
        };
        let Some(effect_attempted) = latest.effect_attempted.as_ref() else {
            continue;
        };
        let provider = &effect_attempted.provider;

        if registry.get(provider).is_some() {
            if !covered_providers.contains(provider) {
                covered_providers.push(provider.clone());
            }
            continue;
        }

        // No reconciler → manual recovery
        if !missing_providers.contains(provider) {
            missing_providers.push(provider.clone());
        }

        let _ = log.append(EventDraft {
            event_type: "reconcileResult".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": &latest.attempt_id,
                "idempotencyKey": &effect_attempted.idempotency_key,
                "capability": "manual",
                "decision": "manual",
                "evidence": {
                    "source": "reconcilerRegistry",
                    "returned": "missing",
                    "message": format!("no reconciler registered for provider '{}'", provider),
                },
            }),
            timestamp: None,
            payload_hash: None,
        })?;
        let _ = log.append(EventDraft {
            event_type: "activityFailed".to_string(),
            actor: WorkflowActor::System,
            payload: serde_json::json!({
                "activityId": activity_id,
                "attemptId": &latest.attempt_id,
                "error": {
                    "errorCode": "UnknownProviderError",
                    "errorClass": "manual",
                    "errorMessage": format!("no reconciler registered for provider '{}'", provider),
                }
            }),
            timestamp: None,
            payload_hash: None,
        })?;
    }

    Ok((covered_providers, missing_providers))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::{
        BootstrapWorkflowRunInput, CreateTaskInput, ParsedSchedule, ParsedScheduleKind,
        RunChatBinding, bootstrap_workflow_run, create_task,
    };
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-reconciler-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    fn make_state(paths: &BeamPaths) -> AppState {
        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        AppState {
            paths: paths.clone(),
            started_at: chrono::Utc::now(),
            sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            workers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            shutdown: Arc::new(tokio::sync::Mutex::new(Some(_shutdown_tx))),
            options: crate::RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: reqwest::Client::new(),
            config: beam_core::Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            workflow_progress_cards: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            ask_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            grant_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            pending_creates: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(tokio::sync::Mutex::new(None)),
            external_host: "localhost".to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // Registry tests
    // -----------------------------------------------------------------------

    #[test]
    fn registry_contains_beam_schedule_and_feishu_im() {
        let reg = default_reconciler_registry();
        let schedule = reg.get("beam-schedule");
        assert!(
            schedule.is_some(),
            "registry should contain beam-schedule reconciler"
        );
        let feishu = reg.get("feishu-im");
        assert!(
            feishu.is_some(),
            "registry should contain feishu-im reconciler"
        );
    }

    #[test]
    fn registry_unknown_provider_returns_none() {
        let reg = default_reconciler_registry();
        assert!(reg.get("nonexistent-provider").is_none());
    }

    #[test]
    fn global_registry_is_singleton() {
        let r1 = global_reconciler_registry();
        let r2 = global_reconciler_registry();
        let p1 = r1 as *const _;
        let p2 = r2 as *const _;
        assert_eq!(
            p1, p2,
            "global reconciler registry should be the same instance"
        );
    }

    // -----------------------------------------------------------------------
    // Trait implementation tests
    // -----------------------------------------------------------------------

    #[test]
    fn beam_schedule_reconciler_metadata() {
        let r = BeamScheduleReconciler;
        assert_eq!(r.provider_name(), "beam-schedule");
        assert!(!r.requires_effect_input());
    }

    #[test]
    fn feishu_im_reconciler_metadata() {
        let r = FeishuImReconciler;
        assert_eq!(r.provider_name(), "feishu-im");
        assert!(r.requires_effect_input());
    }

    #[test]
    fn beam_schedule_is_not_retryable() {
        let r = BeamScheduleReconciler;
        assert!(!r.is_retryable_error(&anyhow::anyhow!("file not found")));
    }

    #[test]
    fn feishu_im_is_retryable_detects_timeout() {
        let r = FeishuImReconciler;
        // Use an anyhow error containing retryable keywords rather than
        // constructing a private reqwest::ErrorKind directly.
        let timeout_err = anyhow::anyhow!("request timeout: timed out after 30s");
        assert!(r.is_retryable_error(&timeout_err));
    }

    #[test]
    fn feishu_im_is_retryable_detects_rate_limit() {
        let r = FeishuImReconciler;
        assert!(r.is_retryable_error(&anyhow::anyhow!("HTTP 429: too many requests")));
    }

    #[test]
    fn feishu_im_is_retryable_rejects_generic_error() {
        let r = FeishuImReconciler;
        assert!(!r.is_retryable_error(&anyhow::anyhow!("permission denied")));
    }

    #[test]
    fn feishu_im_canonical_input_parses_chat_id_variant() {
        let r = FeishuImReconciler;
        let raw = serde_json::json!({
            "larkAppId": "app-1",
            "chatId": "chat-1",
            "content": "hello"
        });
        let canonical = r.canonical_input(&raw).expect("canonical input parse");
        assert_eq!(canonical["larkAppId"], "app-1");
        assert_eq!(canonical["chatId"], "chat-1");
        assert_eq!(canonical["content"], "hello");
        // canonical should NOT contain msgType
        assert!(canonical.get("msgType").is_none());
        assert!(canonical.get("rootMessageId").is_none());
    }

    #[test]
    fn feishu_im_canonical_input_parses_reply_variant() {
        let r = FeishuImReconciler;
        let raw = serde_json::json!({
            "larkAppId": "app-1",
            "rootMessageId": "msg-1",
            "content": "reply"
        });
        let canonical = r.canonical_input(&raw).expect("canonical input parse");
        assert_eq!(canonical["larkAppId"], "app-1");
        assert_eq!(canonical["rootMessageId"], "msg-1");
        assert_eq!(canonical["content"], "reply");
        assert!(canonical.get("chatId").is_none());
    }

    #[test]
    fn feishu_im_canonical_input_missing_target_both_still_succeeds() {
        // canonical_input does NOT validate that at least one target is present;
        // that check is deferred to idempotent_submit.
        let r = FeishuImReconciler;
        let raw = serde_json::json!({
            "larkAppId": "app-1",
            "content": "no target"
        });
        let canonical = r
            .canonical_input(&raw)
            .expect("canonical input should parse");
        assert_eq!(canonical["larkAppId"], "app-1");
        assert_eq!(canonical["content"], "no target");
        assert!(canonical.get("chatId").is_none());
        assert!(canonical.get("rootMessageId").is_none());
    }

    #[test]
    fn beam_schedule_canonical_input_is_passthrough() {
        let r = BeamScheduleReconciler;
        let raw = serde_json::json!({"name": "test"});
        let canonical = r.canonical_input(&raw).unwrap();
        assert_eq!(canonical, raw);
    }

    // -----------------------------------------------------------------------
    // read_only_lookup tests (schedule)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn beam_schedule_read_only_lookup_finds_existing_task() {
        let paths = temp_paths("schedule-lookup-found");
        let _ = std::fs::remove_dir_all(paths.root());
        let run_id = "run-sched-found";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();
        // Create a task with a known idempotency key
        create_task(
            &paths,
            CreateTaskInput {
                id: Some("test-key".to_string()),
                name: "demo".to_string(),
                schedule: "0 9 * * *".to_string(),
                parsed: ParsedSchedule {
                    kind: ParsedScheduleKind::Cron,
                    run_at: None,
                    minutes: None,
                    expr: Some("0 9 * * *".to_string()),
                    display: "0 9 * * *".to_string(),
                },
                prompt: "demo".to_string(),
                working_dir: "/tmp".to_string(),
                chat_id: "oc_".to_string(),
                root_message_id: None,
                scope: Some("thread".to_string()),
                chat_type: None,
                lark_app_id: None,
                creator_chat_id: None,
                creator_root_message_id: None,
                creator_lark_app_id: None,
                next_run_at: None,
                repeat: None,
                deliver: None,
            },
        )
        .unwrap();

        let state = make_state(&paths);
        let r = BeamScheduleReconciler;
        let result = r
            .read_only_lookup(&state, &paths, "test-key")
            .await
            .expect("read_only_lookup");
        assert!(
            result.is_some(),
            "should find existing task by idempotency key"
        );
        let evidence = result.unwrap();
        assert_eq!(evidence["source"], "getTask");
        assert_eq!(evidence["externalRefs"]["taskId"], "test-key");

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn beam_schedule_read_only_lookup_returns_none_for_missing_task() {
        let paths = temp_paths("schedule-lookup-missing");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let r = BeamScheduleReconciler;
        let result = r
            .read_only_lookup(&state, &paths, "nonexistent-key")
            .await
            .expect("read_only_lookup");
        assert!(result.is_none(), "should return None for non-existent task");
        let _ = std::fs::remove_dir_all(paths.root());
    }

    // -----------------------------------------------------------------------
    // Missing reconciler → manual recovery
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn missing_reconciler_produces_manual_recovery() {
        let paths = temp_paths("missing-reconciler");
        let _ = std::fs::remove_dir_all(paths.root());
        let run_id = "run-missing";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"unknown-provider","input":{"x":1}}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        // Write effectAttempted for a provider that has no reconciler registered
        {
            let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "unknown-key",
                        "inputHash": "sha256:1",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "unknown-provider",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .expect("snapshot");
        let state = make_state(&paths);
        let registry = default_reconciler_registry();
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();

        let result = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &paths.workflow_run_dir(run_id),
            "unknown-provider",
            &snapshot,
        )
        .await
        .expect("reconcile_provider_dangling_effects");

        // Should have produced manual recovery (not skipped)
        assert!(
            !result.reconciled.is_empty(),
            "should produce manual recovery"
        );
        assert_eq!(result.reconciled[0].decision, "manual");

        // Verify the EventLog has the expected manual recovery events
        let events = log.read_all().unwrap();
        let reconcile_result = events
            .iter()
            .find(|e| e.event_type == "reconcileResult")
            .expect("should have reconcileResult");
        assert_eq!(
            reconcile_result.payload["decision"], "manual",
            "decision should be manual"
        );
        assert!(
            reconcile_result.payload["evidence"]["message"]
                .as_str()
                .unwrap()
                .contains("no reconciler registered"),
            "evidence should mention missing reconciler"
        );
        let activity_failed = events
            .iter()
            .find(|e| e.event_type == "activityFailed")
            .expect("should have activityFailed");
        assert_eq!(
            activity_failed.payload["error"]["errorCode"],
            "UnknownProviderError"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    // -----------------------------------------------------------------------
    // End-to-end: schedule reconciliation via registry
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn reconcile_schedule_dangling_via_registry_finds_task() {
        let paths = temp_paths("registry-schedule-found");
        let _ = std::fs::remove_dir_all(paths.root());

        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
        let run_id = "run-reg-sched";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Write events and create the task
        {
            let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "wf-key-xyz",
                        "inputHash": "sha256:1",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "beam-schedule",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            create_task(
                &paths,
                CreateTaskInput {
                    id: Some("wf-key-xyz".to_string()),
                    name: "schedule-demo".to_string(),
                    schedule: "0 9 * * *".to_string(),
                    parsed: ParsedSchedule {
                        kind: ParsedScheduleKind::Cron,
                        run_at: None,
                        minutes: None,
                        expr: Some("0 9 * * *".to_string()),
                        display: "0 9 * * *".to_string(),
                    },
                    prompt: "demo".to_string(),
                    working_dir: "/tmp".to_string(),
                    chat_id: "oc_".to_string(),
                    root_message_id: None,
                    scope: Some("thread".to_string()),
                    chat_type: None,
                    lark_app_id: None,
                    creator_chat_id: None,
                    creator_root_message_id: None,
                    creator_lark_app_id: None,
                    next_run_at: None,
                    repeat: None,
                    deliver: None,
                },
            )
            .unwrap();
        }

        let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .expect("snapshot");
        let state = make_state(&paths);
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let registry = default_reconciler_registry();

        let result = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &paths.workflow_run_dir(run_id),
            "beam-schedule",
            &snapshot,
        )
        .await
        .expect("reconcile");

        assert_eq!(result.reconciled.len(), 1);
        assert_eq!(result.reconciled[0].decision, "completedByIdempotentSubmit");

        let events = log.read_all().unwrap();
        assert!(
            events.iter().any(|e| e.event_type == "reconcileResult"),
            "should have reconcileResult"
        );
        assert!(
            events.iter().any(|e| e.event_type == "activitySucceeded"),
            "should have activitySucceeded"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn reconcile_schedule_dangling_via_registry_issues_fresh_retry_when_task_missing() {
        let paths = temp_paths("registry-schedule-freshretry");
        let _ = std::fs::remove_dir_all(paths.root());

        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
        let run_id = "run-reg-sched-fr";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Write events but DO NOT create the task – simulate missing effect
        {
            let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "wf-key-nonexistent",
                        "inputHash": "sha256:1",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "beam-schedule",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let snapshot = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .expect("snapshot");
        let state = make_state(&paths);
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let registry = default_reconciler_registry();

        let result = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &paths.workflow_run_dir(run_id),
            "beam-schedule",
            &snapshot,
        )
        .await
        .expect("reconcile");

        // Should produce freshRetry (not manual and not reconciled)
        assert_eq!(
            result.fresh_retry.len(),
            1,
            "should have one freshRetry when task doesn't exist"
        );
        assert_eq!(
            result.fresh_retry[0].decision, "freshRetry",
            "decision should be freshRetry"
        );
        assert!(result.reconciled.is_empty(), "no reconciled expected");
        assert!(
            result.transient_failures.is_empty(),
            "no transient failures expected"
        );

        let events = log.read_all().unwrap();
        let reconcile_result = events
            .iter()
            .find(|e| e.event_type == "reconcileResult")
            .expect("should have reconcileResult");
        assert_eq!(
            reconcile_result.payload["decision"], "freshRetry",
            "reconcileResult decision should be freshRetry"
        );
        assert_eq!(
            reconcile_result.payload["capability"], "readOnlyLookup",
            "should use readOnlyLookup capability"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn beam_schedule_supports_read_only_lookup_only() {
        let r = BeamScheduleReconciler;
        assert!(r.supports_read_only_lookup());
        assert!(!r.supports_idempotent_submit());
    }

    #[test]
    fn feishu_im_supports_idempotent_submit_only() {
        let r = FeishuImReconciler;
        assert!(!r.supports_read_only_lookup());
        assert!(r.supports_idempotent_submit());
    }

    // -----------------------------------------------------------------------
    // Feishu reconciler trait behaviour
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn feishu_im_read_only_lookup_always_returns_none() {
        let paths = temp_paths("feishu-readonly");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let r = FeishuImReconciler;
        let result = r
            .read_only_lookup(&state, &paths, "any-key")
            .await
            .expect("read_only_lookup");
        assert!(
            result.is_none(),
            "feishu-im should not support readOnlyLookup"
        );
        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[test]
    fn feishu_im_canonical_input_rejects_missing_lark_app_id() {
        let r = FeishuImReconciler;
        let raw = serde_json::json!({
            "chatId": "chat-1",
            "content": "hello"
        });
        let err = r.canonical_input(&raw).unwrap_err();
        assert!(
            format!("{err:#}").contains("larkAppId") || format!("{err}").contains("larkAppId"),
            "should mention missing larkAppId"
        );
    }

    // -----------------------------------------------------------------------
    // Transient failure pathway
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn feishu_idempotent_submit_missing_bot_returns_error() {
        let paths = temp_paths("feishu-missing-bot");
        let _ = std::fs::remove_dir_all(paths.root());
        let state = make_state(&paths);
        let r = FeishuImReconciler;
        let canonical = serde_json::json!({
            "larkAppId": "nonexistent-app",
            "chatId": "chat-1",
            "content": "hello"
        });
        let err = r.idempotent_submit(&state, &canonical).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("not registered"),
            "should mention bot not registered"
        );
        // Missing bot is NOT retryable
        assert!(!r.is_retryable_error(&err));
        let _ = std::fs::remove_dir_all(paths.root());
    }

    // -----------------------------------------------------------------------
    // Hash mismatch → manual failure (no provider call)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn feishu_im_hash_mismatch_produces_manual_failure_without_provider_call() {
        let paths = temp_paths("feishu-hash-mismatch");
        let _ = std::fs::remove_dir_all(paths.root());
        let run_id = "run-hash-mismatch";

        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"larkAppId":"app-1","chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let run_dir = paths.workflow_run_dir(run_id);

        // Write attemptCreated + effectAttempted with a deliberately wrong inputHash
        {
            let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-feishu-1",
                        "attemptId": "act-feishu-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-feishu-1",
                        "attemptId": "act-feishu-1::att-1",
                        "idempotencyKey": "wf-key-feishu",
                        "inputHash": "deadbeef_wrong_hash_123",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "feishu-im",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Write a sidecar with valid content (different from what the wrong hash represents)
        let sidecar_dir = run_dir
            .join("attempts")
            .join("act-feishu-1")
            .join("act-feishu-1::att-1");
        std::fs::create_dir_all(&sidecar_dir).unwrap();
        let sidecar_content = serde_json::json!({
            "larkAppId": "app-1",
            "chatId": "chat-1",
            "content": "hello"
        });
        std::fs::write(
            sidecar_dir.join("effect-input.json"),
            serde_json::to_vec_pretty(&sidecar_content).unwrap(),
        )
        .unwrap();

        let snapshot = beam_core::read_run_snapshot(&run_dir)
            .await
            .unwrap()
            .expect("snapshot");
        let state = make_state(&paths);
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let registry = default_reconciler_registry();

        let result = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &run_dir,
            "feishu-im",
            &snapshot,
        )
        .await
        .expect("reconcile_provider_dangling_effects");

        // Should produce manual recovery — NOT call the provider
        assert!(
            !result.reconciled.is_empty(),
            "should produce manual recovery"
        );
        let manual = result.reconciled.iter().find(|o| o.decision == "manual");
        assert!(
            manual.is_some(),
            "should have manual decision due to hash mismatch"
        );

        // Verify the EventLog has reconcileResult with hashMismatch evidence
        let events = log.read_all().unwrap();
        let reconcile_result = events
            .iter()
            .find(|e| e.event_type == "reconcileResult")
            .expect("should have reconcileResult");
        assert_eq!(
            reconcile_result.payload["decision"], "manual",
            "decision should be manual"
        );
        assert_eq!(
            reconcile_result.payload["evidence"]["source"], "effectInputSidecar",
            "evidence source should be effectInputSidecar"
        );
        assert_eq!(
            reconcile_result.payload["evidence"]["returned"], "hashMismatch",
            "evidence should indicate hashMismatch"
        );
        assert!(
            reconcile_result.payload["evidence"]["expectedHash"]
                .as_str()
                .unwrap()
                .contains("deadbeef"),
            "expectedHash should be the wrong hash from effectAttempted"
        );

        let activity_failed = events
            .iter()
            .find(|e| e.event_type == "activityFailed")
            .expect("should have activityFailed");
        assert_eq!(
            activity_failed.payload["error"]["errorCode"],
            "EffectInputHashMismatch"
        );
        assert_eq!(
            activity_failed.payload["error"]["errorClass"], "manual",
            "errorClass should be manual"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn feishu_im_hash_match_falls_through_to_idempotent_submit_not_hash_mismatch() {
        // Verify that when the hash MATCHES, the code falls through to
        // idempotentSubmit (which fails because bot is missing — but the error
        // should be "bot not registered", NOT "hash mismatch").
        let paths = temp_paths("feishu-hash-match");
        let _ = std::fs::remove_dir_all(paths.root());
        let run_id = "run-hash-match";

        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"nodes":{"a":{"type":"hostExecutor","executor":"feishu-send","input":{"larkAppId":"app-nonexistent","chatId":"chat-1","content":"hello"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &BTreeMap::<String, Value>::new(),
                initiator: "cli",
                chat_binding: None,
            },
        )
        .unwrap();

        let run_dir = paths.workflow_run_dir(run_id);

        // The canonical input for feishu-im sidecar {"larkAppId":"app-nonexistent","chatId":"chat-1","content":"hello"}
        // Compute the matching hash so the hash check passes.
        let sidecar_content = serde_json::json!({
            "larkAppId": "app-nonexistent",
            "chatId": "chat-1",
            "content": "hello"
        });
        let r = FeishuImReconciler;
        let canonical = r
            .canonical_input(&sidecar_content)
            .expect("canonical_input");
        let canonical_bytes = serde_json::to_vec(&canonical).unwrap();
        let correct_hash = sha256_hex(&canonical_bytes);

        // Write attemptCreated + effectAttempted with the CORRECT hash
        {
            let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-feishu-2",
                        "attemptId": "act-feishu-2::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-feishu-2",
                        "attemptId": "act-feishu-2::att-1",
                        "idempotencyKey": "wf-key-feishu-2",
                        "inputHash": &correct_hash,
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "feishu-im",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        // Write the sidecar
        let sidecar_dir = run_dir
            .join("attempts")
            .join("act-feishu-2")
            .join("act-feishu-2::att-1");
        std::fs::create_dir_all(&sidecar_dir).unwrap();
        std::fs::write(
            sidecar_dir.join("effect-input.json"),
            serde_json::to_vec_pretty(&sidecar_content).unwrap(),
        )
        .unwrap();

        let snapshot = beam_core::read_run_snapshot(&run_dir)
            .await
            .unwrap()
            .expect("snapshot");
        let state = make_state(&paths);
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let registry = default_reconciler_registry();

        let result = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &run_dir,
            "feishu-im",
            &snapshot,
        )
        .await
        .expect("reconcile_provider_dangling_effects");

        // Should produce manual recovery because bot is missing, NOT because of hash mismatch
        let manual = result.reconciled.iter().find(|o| o.decision == "manual");
        assert!(
            manual.is_some(),
            "should have manual decision (bot missing, not hash mismatch)"
        );

        // Verify the events do NOT contain hashMismatch
        let events = log.read_all().unwrap();
        let has_hash_mismatch = events.iter().any(|e| {
            e.event_type == "reconcileResult"
                && e.payload
                    .get("evidence")
                    .and_then(|v| v.get("returned"))
                    .and_then(|v| v.as_str())
                    == Some("hashMismatch")
        });
        assert!(
            !has_hash_mismatch,
            "should NOT have hashMismatch when hash matches"
        );

        // Verify activityFailed has bot-related error (not EffectInputHashMismatch)
        let activity_failed = events
            .iter()
            .find(|e| e.event_type == "activityFailed")
            .expect("should have activityFailed");
        let error_code = activity_failed.payload["error"]["errorCode"]
            .as_str()
            .unwrap_or("");
        assert!(
            error_code != "EffectInputHashMismatch",
            "error should NOT be EffectInputHashMismatch, got: {error_code}"
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }

    #[tokio::test]
    async fn prior_fresh_retry_does_not_write_new_events_on_second_reconciliation() {
        let paths = temp_paths("prior-freshretry-noprogress");
        let _ = std::fs::remove_dir_all(paths.root());

        let params: BTreeMap<String, Value> =
            BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
        let run_id = "run-prior-fr";
        bootstrap_workflow_run(
            &paths,
            BootstrapWorkflowRunInput {
                run_id,
                workflow_json: r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"schedule-demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"demo","workingDir":"/tmp","chatId":"oc_","scope":"thread"},"unsafeAllowUngated":true}}}"#,
                expected_workflow_id: Some("flow-a"),
                params: &params,
                initiator: "cli",
                chat_binding: Some(RunChatBinding {
                    chat_id: "chat-1".to_string(),
                    lark_app_id: "app-1".to_string(),
                }),
            },
        )
        .unwrap();

        // Write dangling effectAttempted (task does NOT exist → freshRetry).
        {
            let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "attemptCreated".to_string(),
                    actor: WorkflowActor::Scheduler,
                    payload: serde_json::json!({
                        "nodeId": "a",
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "attemptNumber": 1,
                        "inputRef": {
                            "outputHash": "sha256:dummy",
                            "outputPath": "dummy",
                            "outputBytes": 1,
                            "outputSchemaVersion": 1,
                            "contentType": "application/json",
                        }
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
            let _ = log
                .append(EventDraft {
                    event_type: "effectAttempted".to_string(),
                    actor: WorkflowActor::HostExecutor,
                    payload: serde_json::json!({
                        "activityId": "act-1",
                        "attemptId": "act-1::att-1",
                        "idempotencyKey": "wf-key-nonexistent",
                        "inputHash": "sha256:1",
                        "idempotencyTtlMs": 9999999u64,
                        "provider": "beam-schedule",
                    }),
                    timestamp: None,
                    payload_hash: None,
                })
                .unwrap();
        }

        let state = make_state(&paths);
        let mut log = EventLog::new(run_id.to_string(), paths.workflow_runs_dir()).unwrap();
        let registry = default_reconciler_registry();

        // --- First reconciliation: should write reconcileResult{decision=freshRetry} ---
        let snap1 = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .expect("snapshot 1");
        let result1 = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &paths.workflow_run_dir(run_id),
            "beam-schedule",
            &snap1,
        )
        .await
        .expect("first reconcile");

        assert_eq!(
            result1.fresh_retry.len(),
            1,
            "first call: should have freshRetry"
        );
        let events_after_first = log.read_all().unwrap();
        let count_after_first = events_after_first.len();
        let has_reconcile_result = events_after_first
            .iter()
            .any(|e| e.event_type == "reconcileResult");
        assert!(
            has_reconcile_result,
            "first call should write reconcileResult"
        );

        // --- Second reconciliation: prior freshRetry exists, must NOT write new events ---
        let snap2 = beam_core::read_run_snapshot(&paths.workflow_run_dir(run_id))
            .await
            .unwrap()
            .expect("snapshot 2");
        let _result2 = reconcile_provider_dangling_effects(
            &registry,
            &state,
            &mut log,
            &paths.workflow_run_dir(run_id),
            "beam-schedule",
            &snap2,
        )
        .await
        .expect("second reconcile");

        // The result may still reference the prior freshRetry outcome,
        // but NO new events should have been appended.
        let events_after_second = log.read_all().unwrap();
        assert_eq!(
            events_after_second.len(),
            count_after_first,
            "second reconciliation must NOT write new events when prior freshRetry exists; \
             before={count_after_first} after={}",
            events_after_second.len()
        );

        let _ = std::fs::remove_dir_all(paths.root());
    }
}
