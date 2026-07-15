//! Tests for [`crate::parse_workflow_text_command`] which handles
//! `/workflow run` and `/workflow cancel` text commands from Lark.

use crate::{WorkflowTextCommand, parse_workflow_text_command};

#[test]
fn parse_workflow_text_command_handles_run_and_cancel() {
    // ── basic run with unquoted params ─────────────────────────
    match parse_workflow_text_command("/workflow run demo.flow foo=bar baz=qux") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "demo.flow");
            assert_eq!(raw_params.get("foo").map(String::as_str), Some("bar"));
            assert_eq!(raw_params.get("baz").map(String::as_str), Some("qux"));
        }
        other => panic!("unexpected parse result: {:?}", other),
    }

    // ── double-quoted value with spaces ───────────────────────
    match parse_workflow_text_command("/workflow run flow task=\"review and deploy PR #42\"") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(
                raw_params.get("task").map(String::as_str),
                Some("review and deploy PR #42")
            );
        }
        other => panic!("unexpected: {:?}", other),
    }

    // ── single-quoted value with spaces ───────────────────────
    match parse_workflow_text_command("/workflow run flow task='review and deploy PR #42'") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(
                raw_params.get("task").map(String::as_str),
                Some("review and deploy PR #42")
            );
        }
        other => panic!("unexpected: {:?}", other),
    }

    // ── escaped double-quote inside double-quoted value ───────
    match parse_workflow_text_command("/workflow run flow task=\"say \\\"hello\\\"\"") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(
                raw_params.get("task").map(String::as_str),
                Some("say \"hello\"")
            );
        }
        other => panic!("unexpected: {:?}", other),
    }

    // ── empty value ────────────────────────────────────────────
    match parse_workflow_text_command("/workflow run flow foo=") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(raw_params.get("foo").map(String::as_str), Some(""));
        }
        other => panic!("unexpected: {:?}", other),
    }

    // ── mixed quoted and unquoted params ──────────────────────
    match parse_workflow_text_command("/workflow run flow task=\"do stuff\" verbose=true count=10")
    {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(raw_params.get("task").map(String::as_str), Some("do stuff"));
            assert_eq!(raw_params.get("verbose").map(String::as_str), Some("true"));
            assert_eq!(raw_params.get("count").map(String::as_str), Some("10"));
        }
        other => panic!("unexpected: {:?}", other),
    }

    // ── JSON payload in single-quoted value ───────────────────
    match parse_workflow_text_command("/workflow run flow payload='{\"a\":1}'") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(
                raw_params.get("payload").map(String::as_str),
                Some("{\"a\":1}")
            );
        }
        other => panic!("unexpected: {:?}", other),
    }

    // ── basic cancel ──────────────────────────────────────────
    match parse_workflow_text_command("/workflow cancel run-123") {
        Some(WorkflowTextCommand::Cancel { run_id }) => {
            assert_eq!(run_id, "run-123");
        }
        other => panic!("unexpected parse result: {:?}", other),
    }

    // ── missing workflow id ────────────────────────────────────
    match parse_workflow_text_command("/workflow run") {
        Some(WorkflowTextCommand::Invalid { error, usage }) => {
            assert_eq!(error, "缺少 workflow id");
            assert!(usage.contains("/workflow run"));
        }
        other => panic!("unexpected parse result: {:?}", other),
    }

    // ── unclosed double quote ──────────────────────────────────
    match parse_workflow_text_command("/workflow run flow task=\"unclosed") {
        Some(WorkflowTextCommand::Invalid { error, .. }) => {
            assert!(error.contains("参数引号不匹配"), "got: {error}");
            assert!(error.contains("missing closing quote"), "got: {error}");
        }
        other => panic!("expected Invalid, got: {:?}", other),
    }

    // ── unclosed single quote ──────────────────────────────────
    match parse_workflow_text_command("/workflow run flow task='unclosed") {
        Some(WorkflowTextCommand::Invalid { error, .. }) => {
            assert!(error.contains("参数引号不匹配"), "got: {error}");
            assert!(error.contains("missing closing quote"), "got: {error}");
        }
        other => panic!("expected Invalid, got: {:?}", other),
    }

    // ── token without = ────────────────────────────────────────
    match parse_workflow_text_command("/workflow run flow foo=bar baz") {
        Some(WorkflowTextCommand::Invalid { error, .. }) => {
            assert!(error.contains("key=value"), "got: {error}");
            assert!(error.contains("baz"), "got: {error}");
        }
        other => panic!("expected Invalid, got: {:?}", other),
    }

    // ── empty key (=value) ─────────────────────────────────────
    match parse_workflow_text_command("/workflow run flow =value") {
        Some(WorkflowTextCommand::Invalid { error, .. }) => {
            assert!(error.contains("参数名不能为空"), "got: {error}");
        }
        other => panic!("expected Invalid, got: {:?}", other),
    }

    // ── duplicate key ──────────────────────────────────────────
    match parse_workflow_text_command("/workflow run flow foo=bar foo=qux") {
        Some(WorkflowTextCommand::Invalid { error, .. }) => {
            assert!(error.contains("重复参数"), "got: {error}");
            assert!(error.contains("foo"), "got: {error}");
        }
        other => panic!("expected Invalid, got: {:?}", other),
    }

    // ── adjacent quoted/unquoted concatenation (shell-like) ──
    // In shell word parsing, `"done"extra` concatenates to `doneextra`.
    match parse_workflow_text_command("/workflow run flow task=\"done\"extra") {
        Some(WorkflowTextCommand::Run {
            workflow_id,
            raw_params,
        }) => {
            assert_eq!(workflow_id, "flow");
            assert_eq!(
                raw_params.get("task").map(String::as_str),
                Some("doneextra")
            );
        }
        other => panic!("expected Run with concatenated value, got: {:?}", other),
    }
}
