//! Tests for the opencode adapter module.
//!
//! All tests use temporary directories and in-process Python SQLite to avoid
//! real daemon / opencode binary / tailscale dependencies.

#[path = "test_helpers.rs"]
mod test_helpers;

use super::*;
use crate::adapter::{OpenCodeState, ResolveOutcome};
use beam_core::FinalOutputKind;
use std::fs;
use std::process::Command;
use test_helpers::*;

// ---------------------------------------------------------------------------
// Reader / poll tests
// ---------------------------------------------------------------------------

#[test]
fn opencode_reader_finds_sessions_and_final_output() {
    let root = temp_dir("poll");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_test_db(&db_path);

    let candidates = opencode_db_candidates(&data_dir);
    assert_eq!(candidates, vec![db_path.clone()]);

    let source = find_opencode_session_by_id(Some("sess-1"), &candidates).expect("session lookup");
    assert_eq!(source.db_path, db_path);
    assert_eq!(source.session_id, "sess-1");

    let all = find_all_opencode_sessions_by_directory(Some("/repo/opencode"), &candidates);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].session_id, "sess-1");

    let drain = drain_opencode_session(&source, 0).expect("drain");
    assert_eq!(drain.events.len(), 2);
    assert_eq!(drain.events[0].kind, "user");
    assert_eq!(drain.events[0].text, "hello");
    assert_eq!(drain.events[1].kind, "assistant_final");
    assert_eq!(drain.events[1].text, "hi there");

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-1".to_string()),
        working_dir: "/repo/opencode".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let first = poll(&mut state).expect("first poll");
    assert_eq!(first.final_output.as_deref(), Some("hi there"));
    assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
    assert!(first.prompt_ready);
    assert_eq!(state.transcript_offset, 1500);
    let second = poll(&mut state).expect("second poll");
    assert!(second.final_output.is_none());
    assert!(second.prompt_ready == false);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opencode_reader_dedupes_repeat_final_output_and_recovers_offset() {
    let root = temp_dir("dedupe");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_test_db(&db_path);

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-1".to_string()),
        working_dir: "/repo/opencode".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let first = poll(&mut state).expect("first poll");
    assert_eq!(first.final_output.as_deref(), Some("hi there"));
    assert_eq!(state.transcript_offset, 1500);

    append_user_submit(&db_path, "sess-1", "hello opencode", 1600, 1601);
    let second = poll(&mut state).expect("second poll");
    assert!(second.final_output.is_none());

    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
DELETE FROM part;
DELETE FROM message;
""")
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ("msg-user-2", "sess-1", 2000, 2001, '{"role":"user","id":"msg-user-2"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-user-2", "msg-user-2", "sess-1", 2002, 2002, '{"type":"text","text":"fresh"}'),
)
conn.execute(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ("msg-asst-2", "sess-1", 2100, 2200, '{"role":"assistant","id":"msg-asst-2"}'),
)
conn.execute(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ("part-asst-2", "msg-asst-2", "sess-1", 2190, 2190, '{"type":"text","text":"after truncate"}'),
)
conn.commit()
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    let status = Command::new("python3")
        .args(["-c", &script])
        .status()
        .expect("python3 available");
    assert!(status.success(), "failed to rewrite sqlite db");

    let third = poll(&mut state).expect("third poll");
    assert_eq!(third.final_output.as_deref(), Some("after truncate"));
    assert_eq!(third.final_output_kind, Some(FinalOutputKind::Bridge));
    assert_eq!(state.transcript_offset, 2200);
    let _ = fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// write_input tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn opencode_write_input_verifies_transcript_before_reporting_success() {
    let root = temp_dir("submit");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER,
  time_archived INTEGER,
  parent_id TEXT
);
CREATE TABLE message (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
CREATE TABLE part (
  id TEXT PRIMARY KEY,
  message_id TEXT,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
""")
conn.execute(
    "INSERT INTO session (id, directory, time_updated) VALUES (?, ?, ?)",
    ("sess-1", "/repo/opencode", 1000),
)
conn.commit()
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    let status = Command::new("python3")
        .args(["-c", &script])
        .status()
        .expect("python3 available");
    assert!(status.success(), "failed to create sqlite db");

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-1".to_string()),
        working_dir: "/repo/opencode".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let backend = RecordingBackend::new(db_path.clone(), true, 1000);
    let result = write_input(&mut state, &backend, "hello opencode")
        .await
        .expect("write input");
    assert!(result.submitted);
    assert_eq!(result.cli_session_id.as_deref(), Some("sess-1"));
    assert!(backend.calls().iter().any(|call| call == "enter"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn opencode_write_input_reports_failure_when_transcript_does_not_confirm() {
    let root = temp_dir("submit-fail");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    let mut script = String::from(
        r#"
import sqlite3
conn = sqlite3.connect(__DB_PATH__)
conn.executescript("""
CREATE TABLE session (
  id TEXT PRIMARY KEY,
  directory TEXT,
  time_updated INTEGER,
  time_archived INTEGER,
  parent_id TEXT
);
CREATE TABLE message (
  id TEXT PRIMARY KEY,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
CREATE TABLE part (
  id TEXT PRIMARY KEY,
  message_id TEXT,
  session_id TEXT,
  time_created INTEGER,
  time_updated INTEGER,
  data TEXT
);
""")
conn.execute(
    "INSERT INTO session (id, directory, time_updated) VALUES (?, ?, ?)",
    ("sess-1", "/repo/opencode", 1000),
)
conn.commit()
"#,
    );
    script = script.replace("__DB_PATH__", &json_string(&db_path.display().to_string()));
    let status = Command::new("python3")
        .args(["-c", &script])
        .status()
        .expect("python3 available");
    assert!(status.success(), "failed to create sqlite db");

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-1".to_string()),
        working_dir: "/repo/opencode".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let backend = RecordingBackend::new(db_path.clone(), false, 1000);
    let result = write_input(&mut state, &backend, "hello opencode")
        .await
        .expect("write input");
    assert!(!result.submitted);
    assert!(
        result
            .failure_reason
            .as_deref()
            .unwrap_or("")
            .contains("did not confirm")
    );
    let _ = fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// Source resolution tests
// ---------------------------------------------------------------------------

#[test]
fn opencode_source_resolution_no_candidates_returns_not_found() {
    let root = temp_dir("res-no-candidates");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/nonexistent/dir".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let resolution = current_source(&state);
    assert!(
        matches!(resolution, ResolveOutcome::NotFound { .. }),
        "expected NotFound, got {:?}",
        resolution
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opencode_source_resolution_single_candidate_returns_found_with_session_backfill() {
    let root = temp_dir("res-single-candidate");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(&db_path, &[("sess-abc", "/repo/single-project", 2000)]);

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/single-project".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    // poll should backfill cli_session_id when exactly one candidate
    let result = poll(&mut state).expect("poll");
    assert!(result.final_output.is_none());
    assert_eq!(state.cli_session_id.as_deref(), Some("sess-abc"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opencode_source_resolution_multiple_candidates_returns_ambiguous_no_auto_bind() {
    let root = temp_dir("res-multi-candidates");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-old", "/repo/shared", 1000),
            ("sess-new", "/repo/shared", 2000),
        ],
    );

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/shared".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    // poll should NOT auto-bind when ambiguous
    let result = poll(&mut state).expect("poll");
    assert!(result.cli_session_id.is_none());
    assert_eq!(state.cli_session_id, None);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opencode_source_resolution_expected_session_id_exact_match_overrides_ambiguity() {
    let root = temp_dir("res-exact-match");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-one", "/repo/shared-2", 1000),
            ("sess-two", "/repo/shared-2", 2000),
            ("sess-three", "/repo/shared-2", 3000),
        ],
    );

    // Without expected_session_id → ambiguous (3 candidates)
    let state_no_expect = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/shared-2".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let resolution = current_source(&state_no_expect);
    assert!(
        matches!(resolution, ResolveOutcome::Ambiguous { ref candidates, .. } if candidates.len() == 3),
        "expected Ambiguous with 3, got {:?}",
        resolution
    );

    // With expected_session_id pointing to sess-one → Found (exact match)
    let state_exact = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-one".to_string()),
        working_dir: "/repo/shared-2".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let resolution = current_source(&state_exact);
    match resolution {
        ResolveOutcome::Found(source) => assert_eq!(source.session_id, "sess-one"),
        other => panic!("expected Found(sess-one), got {:?}", other),
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opencode_source_resolution_directory_fallback_filters_and_caps_results() {
    let root = temp_dir("res-dir-cap");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_session_rows(
        &db_path,
        &[
            ("sess-01", "/repo/capped", 1001, None, None),
            ("sess-02", "/repo/capped", 1002, None, None),
            ("sess-03", "/repo/capped", 1003, None, None),
            ("sess-04", "/repo/capped", 1004, None, None),
            ("sess-05", "/repo/capped", 1005, None, None),
            ("sess-06", "/repo/capped", 1006, None, None),
            ("sess-07", "/repo/capped", 1007, None, None),
            ("sess-08", "/repo/capped", 1008, None, None),
            ("sess-09", "/repo/capped", 1009, None, None),
            ("sess-10", "/repo/capped", 1010, None, None),
            ("sess-11", "/repo/capped", 1011, None, None),
            ("sess-12", "/repo/capped", 1012, None, None),
            ("sess-archived", "/repo/capped", 9999, Some(10000), None),
            ("sess-child", "/repo/capped", 9998, None, Some("sess-11")),
        ],
    );

    let candidates = opencode_db_candidates(&data_dir);
    let all = find_all_opencode_sessions_by_directory(Some("/repo/capped"), &candidates);
    let session_ids: Vec<String> = all.iter().map(|source| source.session_id.clone()).collect();
    assert_eq!(
        session_ids,
        vec![
            "sess-12".to_string(),
            "sess-11".to_string(),
            "sess-10".to_string(),
            "sess-09".to_string(),
            "sess-08".to_string(),
            "sess-07".to_string(),
            "sess-06".to_string(),
            "sess-05".to_string(),
            "sess-04".to_string(),
            "sess-03".to_string(),
        ]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opencode_source_resolution_logs_prefer_recent_matching_session() {
    let root = temp_dir("res-log");
    let data_dir = root.join("share").join("opencode");
    let log_dir = data_dir.join("log");
    fs::create_dir_all(&log_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_session_rows(
        &db_path,
        &[
            ("sess-old", "/repo/logged", 1000, None, None),
            ("sess-new", "/repo/logged", 2000, None, None),
        ],
    );
    fs::write(
        log_dir.join("worker.log"),
        "2026-07-05T12:00:00Z session.id=sess-old\n2026-07-05T12:01:00Z session.id=sess-new\n",
    )
    .unwrap();

    let state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/logged".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let resolution = current_source(&state);
    match resolution {
        ResolveOutcome::Found(source) => assert_eq!(source.session_id, "sess-new"),
        other => panic!("expected Found(sess-new), got {:?}", other),
    }
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn opencode_write_input_returns_ambiguous_failure_for_multiple_candidates() {
    let root = temp_dir("submit-ambiguous");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-a", "/repo/ambiguous-project", 1000),
            ("sess-b", "/repo/ambiguous-project", 2000),
        ],
    );

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/ambiguous-project".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    let backend = RecordingBackend::new(db_path.clone(), false, 1000);
    let result = write_input(&mut state, &backend, "hello")
        .await
        .expect("write input");
    assert!(!result.submitted);
    let reason = result.failure_reason.as_deref().unwrap_or("");
    assert!(
        reason.contains("ambiguous"),
        "expected 'ambiguous' in: {}",
        reason
    );
    assert!(
        reason.contains("2 sessions"),
        "expected '2 sessions' in: {}",
        reason
    );
    let _ = fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// Screen vs transcript disambiguation tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disambiguation_screen_matches_one_candidate_clearly_selects_it() {
    let root = temp_dir("disambig-clear");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    // Two sessions in same dir with distinct transcript content.
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-a", "/repo/disambig", 1000),
            ("sess-b", "/repo/disambig", 2000),
        ],
    );
    // Session A: assistant says "The tests all passed, 42 assertions succeeded."
    insert_message_with_text(
        &db_path,
        "sess-a",
        "msg-a1",
        "user",
        "run tests please",
        100,
        101,
    );
    insert_message_with_text(
        &db_path,
        "sess-a",
        "msg-a2",
        "assistant",
        "The tests all passed, 42 assertions succeeded.",
        200,
        201,
    );
    // Session B: assistant says "Deployment completed to production successfully."
    insert_message_with_text(&db_path, "sess-b", "msg-b1", "user", "deploy app", 100, 101);
    insert_message_with_text(
        &db_path,
        "sess-b",
        "msg-b2",
        "assistant",
        "Deployment completed to production successfully.",
        200,
        201,
    );

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/disambig".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    // screen contains text unique to sess-a
    let backend = RecordingBackend::new(db_path.clone(), true, 2001)
        .with_target_session("sess-a")
        .with_screen("The tests all passed, 42 assertions succeeded.".to_string());
    let result = write_input(&mut state, &backend, "next command")
        .await
        .expect("write input");
    assert!(result.submitted, "should auto-select sess-a and submit");
    assert_eq!(
        result.cli_session_id.as_deref(),
        Some("sess-a"),
        "should bind to sess-a"
    );
    assert_eq!(
        state.expected_session_id.as_deref(),
        Some("sess-a"),
        "poll should reuse the disambiguated source"
    );
    insert_message_with_text(
        &db_path,
        "sess-a",
        "msg-a3",
        "assistant",
        "selected session answer",
        3000,
        3001,
    );
    let poll = poll(&mut state).expect("poll");
    assert_eq!(
        poll.final_output.as_deref(),
        Some("selected session answer")
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn disambiguation_screen_does_not_match_weakly_stays_ambiguous() {
    let root = temp_dir("disambig-weak");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-x", "/repo/disambig2", 1000),
            ("sess-y", "/repo/disambig2", 2000),
        ],
    );
    insert_message_with_text(&db_path, "sess-x", "msg-x1", "user", "run tests", 100, 101);
    insert_message_with_text(
        &db_path,
        "sess-x",
        "msg-x2",
        "assistant",
        "All tests passed.",
        200,
        201,
    );
    insert_message_with_text(
        &db_path,
        "sess-y",
        "msg-y1",
        "user",
        "build project",
        100,
        101,
    );
    insert_message_with_text(
        &db_path,
        "sess-y",
        "msg-y2",
        "assistant",
        "Build completed successfully.",
        200,
        201,
    );

    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/disambig2".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    // screen contains unrelated text – neither candidate will score well.
    let backend = RecordingBackend::new(db_path.clone(), false, 2001)
        .with_screen("some totally unrelated content here".to_string());
    let result = write_input(&mut state, &backend, "cmd")
        .await
        .expect("write input");
    assert!(!result.submitted, "should stay ambiguous");
    let reason = result.failure_reason.as_deref().unwrap_or("");
    assert!(
        reason.contains("ambiguous"),
        "expected ambiguous, got: {}",
        reason
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn disambiguation_expected_session_id_still_takes_priority() {
    let root = temp_dir("disambig-exact");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-one", "/repo/disambig3", 1000),
            ("sess-two", "/repo/disambig3", 2000),
        ],
    );
    insert_message_with_text(
        &db_path,
        "sess-one",
        "msg-o1",
        "user",
        "hello one",
        100,
        101,
    );
    insert_message_with_text(
        &db_path,
        "sess-one",
        "msg-o2",
        "assistant",
        "response one",
        200,
        201,
    );
    insert_message_with_text(
        &db_path,
        "sess-two",
        "msg-t1",
        "user",
        "hello two",
        100,
        101,
    );
    insert_message_with_text(
        &db_path,
        "sess-two",
        "msg-t2",
        "assistant",
        "detailed response from session two about deployment",
        200,
        201,
    );

    // state has expected_session_id pointing to sess-one
    let mut state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-one".to_string()),
        working_dir: "/repo/disambig3".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: None,
    };
    // screen matches sess-two much better, but expected_session_id should win
    let backend = RecordingBackend::new(db_path.clone(), true, 2001)
        .with_target_session("sess-one")
        .with_screen("detailed response from session two about deployment".to_string());
    let result = write_input(&mut state, &backend, "cmd")
        .await
        .expect("write input");
    assert!(result.submitted, "should submit via expected_session_id");
    assert_eq!(
        result.cli_session_id.as_deref(),
        Some("sess-one"),
        "expected_session_id exact match should take priority"
    );
    let _ = fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// normalize_for_scoring char-safety tests
// ---------------------------------------------------------------------------

#[test]
fn normalize_chinese_text_does_not_panic_and_preserves_tail() {
    // Chinese chars are 3 bytes each in UTF-8. The old byte-index
    // slicing would likely panic on a non-char boundary.
    let text = "你好世界这是一段很长的中文文本用于测试截断功能是否正常工作";
    let result = normalize_for_scoring(text, 10);
    assert!(!result.is_empty(), "should return non-empty tail");
    assert!(
        result.chars().count() <= 10,
        "tail should be at most 10 chars (or slightly more if word-boundary snap moved start)"
    );
    let _ = result.chars().collect::<Vec<_>>();
}

#[test]
fn normalize_emoji_text_does_not_panic() {
    let mut long_emoji = String::new();
    for _ in 0..200 {
        long_emoji.push_str("🦀🌟🔥");
    }
    let result = normalize_for_scoring(&long_emoji, 100);
    assert!(
        result.chars().count() <= 100,
        "tail should be ≤ 100 chars, got {}",
        result.chars().count()
    );
}

#[test]
fn normalize_mixed_ascii_chinese_does_not_panic() {
    let text = "English followed by 中文内容 mixed together 混合内容 ".repeat(20);
    let result = normalize_for_scoring(&text, 50);
    assert!(!result.is_empty());
    assert!(result.chars().count() <= 50);
    assert!(result.contains("混合") || result.contains("together"));
}

#[test]
fn normalize_shorter_than_tail_returns_unchanged() {
    let text = "short text";
    let result = normalize_for_scoring(text, 500);
    assert_eq!(result, "short text");
}

#[test]
fn normalize_tail_with_no_space_returns_full_tail() {
    let text = "X".repeat(1000);
    let result = normalize_for_scoring(&text, 20);
    assert_eq!(result.chars().count(), 20);
    assert!(result.chars().all(|c| c == 'X'));
}

// ---------------------------------------------------------------------------
// adopted_pid filtering tests
// ---------------------------------------------------------------------------

#[test]
fn parse_cmdline_session_extracts_id_after_flag() {
    let raw = b"opencode\0--session\0abc-123\0";
    assert_eq!(parse_session_from_cmdline(raw), Some("abc-123".to_string()));
}

#[test]
fn parse_cmdline_session_flag_with_trailing_args() {
    let raw = b"opencode\0--model\0gpt-4\0--session\0sess-x\0--prompt\0hello\0";
    assert_eq!(parse_session_from_cmdline(raw), Some("sess-x".to_string()));
}

#[test]
fn parse_cmdline_session_flag_absent_returns_none() {
    let raw = b"opencode\0--model\0gpt-4\0";
    assert_eq!(parse_session_from_cmdline(raw), None);
}

#[test]
fn parse_cmdline_session_empty_input_returns_none() {
    assert_eq!(parse_session_from_cmdline(b""), None);
}

mod adopt_tests;
