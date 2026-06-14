//! HostExecutor trait and registry for workflow side-effect dispatch.
//!
//! Each registered executor carries out a specific external provider call
//! (e.g. send a Feishu message, create a beam schedule task).
//! The registry enables the daemon to look up the correct executor by name
//! and provides a single point for Phase 2.2 effectAttempted integration.

use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use beam_core::{
    CreateTaskInput, HostExecutorNode, ScheduleStoreError, WorkflowDispatchOutcome,
    WorkflowDispatchRun, WorkflowDispatchSession, create_task,
};
use chrono::Utc;
use serde_json::Value;

use crate::AppState;

// Input structs are defined in lib.rs and re-exported via `pub(crate)`.
use crate::{WorkflowFeishuReplyInput, WorkflowFeishuSendInput};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A host executor that carries out an external side-effect for a workflow node.
#[async_trait]
#[allow(dead_code)]
pub trait HostExecutor: Send + Sync {
    /// The executor name used in workflow JSON (e.g. `"feishu-send"`).
    fn name(&self) -> &str;

    /// The provider identifier for effect tracking (e.g. `"feishu-im"`).
    fn provider(&self) -> &str;

    /// How long this executor's effect should be considered idempotent, in milliseconds.
    fn idempotency_ttl_ms(&self) -> u64;

    /// Parse and validate resolved input into a typed structured form.
    ///
    /// Returns an error if the input shape is invalid (missing fields, wrong types).
    fn parse_input(&self, resolved_input: &Value) -> Result<Value>;

    /// Produce a canonical (deterministic, sorted-key) JSON representation
    /// of the parsed input suitable for idempotency hashing.
    fn canonical_input(&self, parsed: &Value) -> Result<Value>;

    /// Invoke the external provider and return a dispatch outcome.
    ///
    /// The `parsed_input` is the value returned by `parse_input`.
    async fn invoke(
        &self,
        state: &AppState,
        ctx: WorkflowDispatchRun<'_>,
        node: &HostExecutorNode,
        parsed_input: &Value,
    ) -> Result<WorkflowDispatchOutcome>;

    /// Classify an error from this executor into a `Failed` outcome.
    ///
    /// The default implementation produces `UnknownProviderError / manual`.
    fn classify_error(
        &self,
        error: &anyhow::Error,
        _ctx: &WorkflowDispatchRun<'_>,
    ) -> WorkflowDispatchOutcome {
        WorkflowDispatchOutcome::Failed {
            error_code: "UnknownProviderError".to_string(),
            error_class: "manual".to_string(),
            error_message: format!("{} executor failed: {:#}", self.name(), error),
            session: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Registry of host executors, keyed by executor name.
pub struct HostExecutorRegistry {
    executors: HashMap<String, Box<dyn HostExecutor>>,
}

impl HostExecutorRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            executors: HashMap::new(),
        }
    }

    /// Register an executor.
    pub fn register(&mut self, executor: Box<dyn HostExecutor>) {
        let name = executor.name().to_string();
        self.executors.insert(name, executor);
    }

    /// Look up an executor by name.
    pub fn get(&self, name: &str) -> Option<&dyn HostExecutor> {
        self.executors.get(name).map(|b| b.as_ref())
    }

    /// Resolve an executor by name.
    ///
    /// Returns `Ok(&dyn HostExecutor)` if found, or a `Failed` outcome
    /// with `UnknownProviderError / manual` if not registered.
    #[allow(dead_code)]
    pub fn resolve(&self, name: &str) -> Result<&dyn HostExecutor, WorkflowDispatchOutcome> {
        self.get(name).ok_or_else(|| WorkflowDispatchOutcome::Failed {
            error_code: "UnknownProviderError".to_string(),
            error_class: "manual".to_string(),
            error_message: format!("hostExecutor '{}' is not registered.", name),
            session: None,
        })
    }

    /// Returns an iterator over all registered executor names.
    #[allow(dead_code)]
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.executors.keys().map(|k| k.as_str())
    }
}

impl Default for HostExecutorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Concrete executors
// ---------------------------------------------------------------------------

/// Executor for `feishu-send`: sends a chat message via Lark API.
pub struct FeishuSendExecutor;

#[async_trait]
impl HostExecutor for FeishuSendExecutor {
    fn name(&self) -> &str {
        "feishu-send"
    }

    fn provider(&self) -> &str {
        "feishu-im"
    }

    fn idempotency_ttl_ms(&self) -> u64 {
        60_000 // 1 minute
    }

    fn parse_input(&self, resolved_input: &Value) -> Result<Value> {
        let _parsed: WorkflowFeishuSendInput = serde_json::from_value(resolved_input.clone())
            .context("invalid feishu-send input")?;
        serde_json::to_value(&_parsed).context("serialize feishu-send input")
    }

    fn canonical_input(&self, parsed: &Value) -> Result<Value> {
        let input: WorkflowFeishuSendInput = serde_json::from_value(parsed.clone())
            .context("re-parse feishu-send input for canonical form")?;
        let canonical = serde_json::json!({
            "larkAppId": input.lark_app_id,
            "chatId": input.chat_id,
            "content": input.content,
        });
        Ok(sort_json_keys(canonical))
    }

    async fn invoke(
        &self,
        state: &AppState,
        ctx: WorkflowDispatchRun<'_>,
        node: &HostExecutorNode,
        parsed_input: &Value,
    ) -> Result<WorkflowDispatchOutcome> {
        let input: WorkflowFeishuSendInput = serde_json::from_value(parsed_input.clone())
            .map_err(|err| anyhow::anyhow!("invalid feishu-send input: {}", err))?;
        let Some(bot) = state.bots.get(&input.lark_app_id).cloned() else {
            return Ok(WorkflowDispatchOutcome::Failed {
                error_code: "UnknownProviderError".to_string(),
                error_class: "manual".to_string(),
                error_message: format!("bot '{}' is not registered.", input.lark_app_id),
                session: None,
            });
        };
        let message_id =
            crate::lark_send_chat_message(state, &bot, &input.chat_id, &input.content).await?;
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: serde_json::json!({ "messageId": message_id }),
            session: Some(WorkflowDispatchSession {
                session_id: format!("host-{}-{}", ctx.activity_id, ctx.attempt_id),
                bot_name: node.executor.clone(),
                started_at: Utc::now().timestamp_millis().max(0) as u64,
                ended_at: Some(Utc::now().timestamp_millis().max(0) as u64),
                cli_session_id: None,
                lark_app_id: Some(input.lark_app_id),
                cli_id: Some(bot.cli_id.clone()),
                working_dir: None,
                web_port: None,
                log_path: None,
            }),
        })
    }
}

/// Executor for `feishu-reply`: replies to a message via Lark API.
pub struct FeishuReplyExecutor;

#[async_trait]
impl HostExecutor for FeishuReplyExecutor {
    fn name(&self) -> &str {
        "feishu-reply"
    }

    fn provider(&self) -> &str {
        "feishu-im"
    }

    fn idempotency_ttl_ms(&self) -> u64 {
        60_000 // 1 minute
    }

    fn parse_input(&self, resolved_input: &Value) -> Result<Value> {
        let _parsed: WorkflowFeishuReplyInput = serde_json::from_value(resolved_input.clone())
            .context("invalid feishu-reply input")?;
        serde_json::to_value(&_parsed).context("serialize feishu-reply input")
    }

    fn canonical_input(&self, parsed: &Value) -> Result<Value> {
        let input: WorkflowFeishuReplyInput = serde_json::from_value(parsed.clone())
            .context("re-parse feishu-reply input for canonical form")?;
        let canonical = serde_json::json!({
            "larkAppId": input.lark_app_id,
            "rootMessageId": input.root_message_id,
            "content": input.content,
        });
        Ok(sort_json_keys(canonical))
    }

    async fn invoke(
        &self,
        state: &AppState,
        ctx: WorkflowDispatchRun<'_>,
        node: &HostExecutorNode,
        parsed_input: &Value,
    ) -> Result<WorkflowDispatchOutcome> {
        let input: WorkflowFeishuReplyInput = serde_json::from_value(parsed_input.clone())
            .map_err(|err| anyhow::anyhow!("invalid feishu-reply input: {}", err))?;
        let Some(bot) = state.bots.get(&input.lark_app_id).cloned() else {
            return Ok(WorkflowDispatchOutcome::Failed {
                error_code: "UnknownProviderError".to_string(),
                error_class: "manual".to_string(),
                error_message: format!("bot '{}' is not registered.", input.lark_app_id),
                session: None,
            });
        };
        let reply_id =
            crate::lark_reply_message(state, &bot, &input.root_message_id, &input.content).await?;
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: serde_json::json!({ "messageId": reply_id }),
            session: Some(WorkflowDispatchSession {
                session_id: format!("host-{}-{}", ctx.activity_id, ctx.attempt_id),
                bot_name: node.executor.clone(),
                started_at: Utc::now().timestamp_millis().max(0) as u64,
                ended_at: Some(Utc::now().timestamp_millis().max(0) as u64),
                cli_session_id: None,
                lark_app_id: Some(input.lark_app_id),
                cli_id: Some(bot.cli_id.clone()),
                working_dir: None,
                web_port: None,
                log_path: None,
            }),
        })
    }
}

/// Executor for `beam-schedule`: creates a scheduled task.
pub struct BeamScheduleExecutor;

#[async_trait]
impl HostExecutor for BeamScheduleExecutor {
    fn name(&self) -> &str {
        "beam-schedule"
    }

    fn provider(&self) -> &str {
        "beam-schedule"
    }

    fn idempotency_ttl_ms(&self) -> u64 {
        86_400_000 // 24 hours
    }

    fn parse_input(&self, resolved_input: &Value) -> Result<Value> {
        let _parsed: CreateTaskInput = serde_json::from_value(resolved_input.clone())
            .context("invalid beam-schedule input")?;
        serde_json::to_value(&_parsed).context("serialize beam-schedule input")
    }

    fn canonical_input(&self, parsed: &Value) -> Result<Value> {
        let input: CreateTaskInput = serde_json::from_value(parsed.clone())
            .context("re-parse beam-schedule input for canonical form")?;
        // Only include scheduling-relevant fields for idempotency.
        let canonical = serde_json::json!({
            "name": input.name,
            "schedule": input.schedule,
            "parsed": {
                "kind": input.parsed.kind,
                "expr": input.parsed.expr,
            },
            "prompt": input.prompt,
            "workingDir": input.working_dir,
            "chatId": input.chat_id,
            "scope": input.scope,
        });
        Ok(sort_json_keys(canonical))
    }

    async fn invoke(
        &self,
        state: &AppState,
        ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        parsed_input: &Value,
    ) -> Result<WorkflowDispatchOutcome> {
        let idempotency_key = crate::derive_workflow_idempotency_key(
            ctx.workflow_id,
            ctx.revision_id,
            ctx.run_id,
            ctx.node_id,
            ctx.attempt_id,
        );
        let mut input: CreateTaskInput = serde_json::from_value(parsed_input.clone())
            .map_err(|err| anyhow::anyhow!("invalid beam-schedule input: {}", err))?;
        input.id = Some(idempotency_key.clone());
        match create_task(&state.paths, input) {
            Ok(task) => Ok(WorkflowDispatchOutcome::Succeeded {
                output: serde_json::json!({ "taskId": task.id.clone() }),
                session: None,
            }),
            Err(ScheduleStoreError::IdempotencyConflict {
                task_id,
                existing_input_hash,
                incoming_input_hash,
            }) => Ok(WorkflowDispatchOutcome::Failed {
                error_code: "IdempotencyConflict".to_string(),
                error_class: "fatal".to_string(),
                error_message: format!(
                    "IdempotencyConflict: schedule task {task_id} exists with different canonical input \
                     (existing={existing_input_hash}…, incoming={incoming_input_hash}…)",
                ),
                session: None,
            }),
            Err(err) => Ok(WorkflowDispatchOutcome::Failed {
                error_code: "UnknownProviderError".to_string(),
                error_class: "manual".to_string(),
                error_message: format!("beam-schedule createTask failed: {err}"),
                session: None,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Registry factory
// ---------------------------------------------------------------------------

/// Build a registry pre-populated with all built-in host executors.
pub fn default_host_executor_registry() -> HostExecutorRegistry {
    let mut reg = HostExecutorRegistry::new();
    reg.register(Box::new(FeishuSendExecutor));
    reg.register(Box::new(FeishuReplyExecutor));
    reg.register(Box::new(BeamScheduleExecutor));
    reg
}

/// Return a reference to a process-wide default registry (lazily initialized).
pub fn global_host_executor_registry() -> &'static HostExecutorRegistry {
    static REGISTRY: std::sync::OnceLock<HostExecutorRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(default_host_executor_registry)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Recursively sort all object keys in a JSON value so that serialization is
/// deterministic across runs (used for canonical-input hashing).
#[allow(dead_code)]
fn sort_json_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map
                .into_iter()
                .map(|(k, v)| (k, sort_json_keys(v)))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(entries.into_iter().collect())
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_json_keys).collect()),
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use beam_core::{BeamPaths, Config};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[allow(dead_code)]
    fn temp_paths(label: &str) -> BeamPaths {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        BeamPaths::from_root(std::env::temp_dir().join(format!(
            "beam-executor-{label}-{nanos}-{}",
            std::process::id()
        )))
    }

    #[allow(dead_code)]
    fn maybe_remove_dir(path: &std::path::PathBuf) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[allow(dead_code)]
    fn make_state(paths: &BeamPaths) -> AppState {
        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
        AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            workers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            shutdown: Arc::new(tokio::sync::Mutex::new(Some(_shutdown_tx))),
            options: crate::RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: reqwest::Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new(
            ))),
            workflow_progress_cards: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            ask_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            grant_pending: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(tokio::sync::Mutex::new(None)),
            external_host: "localhost".to_string(),
        }
    }

    fn test_ctx() -> WorkflowDispatchRun<'static> {
        WorkflowDispatchRun {
            run_id: "run-test",
            workflow_id: "flow-test",
            revision_id: "rev-test",
            activity_id: "activity-test",
            attempt_id: "attempt-test",
            node_id: "node-test",
        }
    }

    #[allow(dead_code)]
    fn test_host_executor_node(executor: &str) -> HostExecutorNode {
        HostExecutorNode {
            base: beam_core::workflow_definition::NodeBase {
                description: None,
                depends: None,
                human_gate: None,
                retry_policy: None,
                timeout_ms: None,
                max_output_bytes: None,
                output_schema: None,
                unsafe_allow_ungated: None,
            },
            executor: executor.to_string(),
            input: serde_json::Value::Null,
        }
    }

    // -----------------------------------------------------------------------
    // Registry tests
    // -----------------------------------------------------------------------

    #[test]
    fn registry_finds_registered_executors() {
        let reg = default_host_executor_registry();
        assert!(reg.get("feishu-send").is_some());
        assert!(reg.get("feishu-reply").is_some());
        assert!(reg.get("beam-schedule").is_some());
    }

    #[test]
    fn registry_resolve_unknown_executor_returns_manual_error() {
        let reg = default_host_executor_registry();
        let result = reg.resolve("nonexistent");
        assert!(result.is_err(), "expected Err for unknown executor");
        match result {
            Err(WorkflowDispatchOutcome::Failed {
                error_code,
                error_class,
                error_message,
                ..
            }) => {
                assert_eq!(error_code, "UnknownProviderError");
                assert_eq!(error_class, "manual");
                assert!(
                    error_message.contains("not registered"),
                    "expected 'not registered' in message, got: {error_message}"
                );
            }
            other => panic!("expected Err(Failed), got unexpected: {:?}", other.err()),
        }
    }

    #[test]
    fn registry_names_iterates_all_executors() {
        let reg = default_host_executor_registry();
        let names: Vec<&str> = reg.names().collect();
        assert!(names.contains(&"feishu-send"));
        assert!(names.contains(&"feishu-reply"));
        assert!(names.contains(&"beam-schedule"));
        assert_eq!(names.len(), 3);
    }

    #[test]
    fn global_registry_is_same_across_calls() {
        let r1 = global_host_executor_registry();
        let r2 = global_host_executor_registry();
        let p1 = r1 as *const _;
        let p2 = r2 as *const _;
        assert_eq!(p1, p2, "global registry should be the same instance");
    }

    // -----------------------------------------------------------------------
    // Executor behaviour tests
    // -----------------------------------------------------------------------

    #[test]
    fn feishu_send_executor_parse_input_valid() {
        let executor = FeishuSendExecutor;
        let input = serde_json::json!({
            "larkAppId": "app-1",
            "chatId": "chat-1",
            "content": "hello"
        });
        let parsed = executor.parse_input(&input).expect("parse valid input");
        assert_eq!(parsed["larkAppId"], "app-1");
        assert_eq!(parsed["chatId"], "chat-1");
        assert_eq!(parsed["content"], "hello");
    }

    #[test]
    fn feishu_send_executor_parse_input_missing_field_fails() {
        let executor = FeishuSendExecutor;
        let input = serde_json::json!({
            "chatId": "chat-1",
            "content": "hello"
        });
        let err = executor.parse_input(&input).unwrap_err();
        assert!(
            format!("{err:#}").contains("larkAppId") || format!("{err}").contains("larkAppId"),
            "error should mention missing larkAppId: {err}"
        );
    }

    #[test]
    fn feishu_send_executor_canonical_input_is_deterministic() {
        let executor = FeishuSendExecutor;
        let parsed = serde_json::json!({
            "larkAppId": "app-1",
            "chatId": "chat-1",
            "content": "hello",
            "msgType": null
        });
        let c1 = executor.canonical_input(&parsed).expect("canonical 1");
        let c2 = executor.canonical_input(&parsed).expect("canonical 2");
        assert_eq!(c1, c2);
        // canonical form should NOT contain msgType
        assert!(c1.get("msgType").is_none());
    }

    #[test]
    fn beam_schedule_executor_parse_input_valid() {
        let executor = BeamScheduleExecutor;
        let input = serde_json::json!({
            "name": "daily report",
            "schedule": "0 9 * * *",
            "parsed": {
                "kind": "cron",
                "expr": "0 9 * * *",
                "display": "0 9 * * *"
            },
            "prompt": "Run report",
            "workingDir": "/tmp/report",
            "chatId": "chat-1"
        });
        let parsed = executor.parse_input(&input).expect("parse valid input");
        assert_eq!(parsed["name"], "daily report");
        assert_eq!(parsed["workingDir"], "/tmp/report");
    }

    #[test]
    fn beam_schedule_executor_canonical_input_is_deterministic() {
        let executor = BeamScheduleExecutor;
        let parsed = serde_json::json!({
            "name": "daily report",
            "schedule": "0 9 * * *",
            "parsed": {
                "kind": "cron",
                "expr": "0 9 * * *",
                "display": "0 9 * * *"
            },
            "prompt": "Run report",
            "workingDir": "/tmp/report",
            "chatId": "chat-1",
            "scope": "thread"
        });
        let c1 = executor.canonical_input(&parsed).expect("canonical 1");
        let c2 = executor.canonical_input(&parsed).expect("canonical 2");
        assert_eq!(c1, c2);
        // should only contain canonical fields
        let obj = c1.as_object().expect("should be object");
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("schedule"));
        assert!(obj.contains_key("prompt"));
    }

    #[test]
    fn classify_error_default_returns_unknown_provider_manual() {
        let executor = FeishuSendExecutor;
        let error = anyhow::anyhow!("network timeout");
        let ctx = test_ctx();
        let outcome = executor.classify_error(&error, &ctx);
        match outcome {
            WorkflowDispatchOutcome::Failed {
                error_code,
                error_class,
                ..
            } => {
                assert_eq!(error_code, "UnknownProviderError");
                assert_eq!(error_class, "manual");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // sort_json_keys tests
    // -----------------------------------------------------------------------

    #[test]
    fn sort_json_keys_sorts_object_keys() {
        let input = serde_json::json!({"z": 1, "a": 2, "m": 3});
        let sorted = sort_json_keys(input);
        let keys: Vec<&str> = sorted
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn sort_json_keys_recurses_nested_objects() {
        let input = serde_json::json!({"z": {"b": 1, "a": 2}});
        let sorted = sort_json_keys(input);
        let inner_keys: Vec<&str> = sorted["z"]
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(inner_keys, vec!["a", "b"]);
    }

    #[test]
    fn sort_json_keys_recurses_arrays_of_objects() {
        let input = serde_json::json!([{"z": 1, "a": 2}, {"c": 3, "b": 4}]);
        let sorted = sort_json_keys(input);
        let arr = sorted.as_array().unwrap();
        let k0: Vec<&str> = arr[0].as_object().unwrap().keys().map(|k| k.as_str()).collect();
        let k1: Vec<&str> = arr[1].as_object().unwrap().keys().map(|k| k.as_str()).collect();
        assert_eq!(k0, vec!["a", "z"]);
        assert_eq!(k1, vec!["b", "c"]);
    }
}
