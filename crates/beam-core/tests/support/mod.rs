// Shared test fixtures for workflow integration tests.
// Extracted from workflow_regression.rs (see plan register line 23).
//
// This module is `mod support;`-included by multiple standalone integration test
// targets (workflow_run, workflow_loop, workflow_recovery).  Each target only
// references a subset of the exported fixtures, so dead_code warnings across
// targets are expected and benign.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use beam_core::{
    HostExecutorPrepareResult, WorkflowDispatchOutcome, WorkflowDispatchRun,
    WorkflowExecutionHooks,
    workflow_definition::{HostExecutorNode, SubagentNode},
};
use serde_json::Value;

/// Create a unique temporary directory for a test run.
pub fn temp_run_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "beam-regression-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

/// A no-op hook that immediately succeeds subagent and host-executor calls.
#[derive(Clone)]
pub struct FakeHooks;

#[async_trait]
impl WorkflowExecutionHooks for FakeHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        resolved_input: Value,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: resolved_input,
            session: None,
        })
    }
}

/// Hook that records whether `prepare_host_executor` and `execute_host_executor`
/// were called, so we can verify ordering.
#[derive(Clone)]
pub struct SpyHooks {
    pub prepare_called: Arc<Mutex<bool>>,
    pub execute_called: Arc<Mutex<bool>>,
}

impl SpyHooks {
    pub fn new() -> Self {
        Self {
            prepare_called: Arc::new(Mutex::new(false)),
            execute_called: Arc::new(Mutex::new(false)),
        }
    }
}

#[async_trait]
impl WorkflowExecutionHooks for SpyHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        parsed_input: Value,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        *self.execute_called.lock().unwrap() = true;
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: parsed_input,
            session: None,
        })
    }

    fn prepare_host_executor(
        &self,
        _executor_name: &str,
        resolved_input: &Value,
    ) -> anyhow::Result<HostExecutorPrepareResult> {
        *self.prepare_called.lock().unwrap() = true;
        Ok(HostExecutorPrepareResult {
            parsed_input: resolved_input.clone(),
            canonical_input: resolved_input.clone(),
            provider: "test-provider".to_string(),
            idempotency_ttl_ms: 42_000,
        })
    }
}

/// Hook whose `prepare_host_executor` always returns an error.
/// Used to verify that a failing prepare aborts the dispatch **before** writing
/// `effectAttempted` or calling `execute_host_executor`.
#[derive(Clone)]
pub struct FailingPrepareHooks {
    pub execute_called: Arc<Mutex<bool>>,
}

impl FailingPrepareHooks {
    pub fn new() -> Self {
        Self {
            execute_called: Arc::new(Mutex::new(false)),
        }
    }
}

#[async_trait]
impl WorkflowExecutionHooks for FailingPrepareHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        parsed_input: Value,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        *self.execute_called.lock().unwrap() = true;
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: parsed_input,
            session: None,
        })
    }

    fn prepare_host_executor(
        &self,
        _executor_name: &str,
        _resolved_input: &Value,
    ) -> anyhow::Result<HostExecutorPrepareResult> {
        anyhow::bail!("prepare_host_executor forced failure")
    }
}

/// Hook that returns an error in `execute_host_executor` to simulate an external
/// provider call failure.  The runtime must write `effectAttempted` before
/// calling the hook, so it remains in the event log despite the failure.
#[derive(Clone)]
pub struct PanicHooks;

#[async_trait]
impl WorkflowExecutionHooks for PanicHooks {
    async fn execute_subagent(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &SubagentNode,
        resolved_prompt: String,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        Ok(WorkflowDispatchOutcome::Succeeded {
            output: Value::String(resolved_prompt),
            session: None,
        })
    }

    async fn execute_host_executor(
        &mut self,
        _ctx: WorkflowDispatchRun<'_>,
        _node: &HostExecutorNode,
        _resolved_input: Value,
    ) -> anyhow::Result<WorkflowDispatchOutcome> {
        anyhow::bail!("simulated executor failure")
    }
}
