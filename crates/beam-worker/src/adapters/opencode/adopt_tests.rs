//! Linux-only adopted PID tests for the opencode adapter.

use super::super::*;
use super::test_helpers::*;
use super::OpenCodeState;
use crate::adapter::ResolveOutcome;
use std::fs;

#[cfg(target_os = "linux")]
#[test]
fn adopted_pid_alive_without_session_mapping_returns_ambiguous() {
    let root = temp_dir("adopt-alive-ambig");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-historic", "/repo/adopt", 1000),
            ("sess-middle", "/repo/adopt", 2000),
            ("sess-current", "/repo/adopt", 3000),
        ],
    );

    let state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/adopt".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: Some(std::process::id()),
    };
    let resolution = current_source(&state);
    assert!(
        matches!(
            resolution,
            ResolveOutcome::Ambiguous { ref candidates, .. }
            if candidates.len() == 3
        ),
        "alive pid without --session mapping should remain Ambiguous (3 candidates), got {:?}",
        resolution
    );
    let _ = fs::remove_dir_all(root);
}

#[cfg(target_os = "linux")]
#[test]
fn adopted_pid_dead_returns_not_found() {
    let root = temp_dir("adopt-dead");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-a", "/repo/dead", 1000),
            ("sess-b", "/repo/dead", 2000),
        ],
    );

    let dead_pid: u32 = u32::MAX;
    assert!(
        !is_process_alive(dead_pid),
        "u32::MAX should not be a live PID"
    );

    let state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/dead".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: Some(dead_pid),
    };
    let resolution = current_source(&state);
    assert!(
        matches!(resolution, ResolveOutcome::NotFound { .. }),
        "dead adopted pid should yield NotFound, got {:?}",
        resolution
    );
    let _ = fs::remove_dir_all(root);
}

#[cfg(target_os = "linux")]
#[test]
fn expected_session_id_overrides_adopted_pid_filter() {
    let root = temp_dir("adopt-override");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-old", "/repo/override", 1000),
            ("sess-new", "/repo/override", 2000),
        ],
    );

    let state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: Some("sess-old".to_string()),
        working_dir: "/repo/override".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: Some(std::process::id()),
    };
    let resolution = current_source(&state);
    match resolution {
        ResolveOutcome::Found(source) => {
            assert_eq!(
                source.session_id, "sess-old",
                "expected_session_id exact match must take priority"
            );
        }
        other => panic!("expected Found(sess-old), got {:?}", other),
    }
    let _ = fs::remove_dir_all(root);
}

#[cfg(target_os = "linux")]
#[test]
fn adopted_pid_alive_no_candidates_returns_not_found() {
    let root = temp_dir("adopt-empty");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();

    let state = OpenCodeState {
        data_dir: data_dir.clone(),
        expected_session_id: None,
        working_dir: "/repo/empty".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: Some(std::process::id()),
    };
    let resolution = current_source(&state);
    assert!(
        matches!(resolution, ResolveOutcome::NotFound { .. }),
        "alive pid with no sessions should yield NotFound, got {:?}",
        resolution
    );
    let _ = fs::remove_dir_all(root);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn adopted_pid_alive_screen_disambiguation_still_works_in_write_input() {
    let root = temp_dir("adopt-screen-disambig");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-a", "/repo/adopt-disambig", 1000),
            ("sess-b", "/repo/adopt-disambig", 2000),
        ],
    );
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
        working_dir: "/repo/adopt-disambig".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: Some(std::process::id()),
    };
    let backend = RecordingBackend::new(db_path.clone(), true, 2001)
        .with_target_session("sess-a")
        .with_screen("The tests all passed, 42 assertions succeeded.".to_string());
    let result = state.write_input(&backend, "next command")
        .await
        .expect("write input");
    assert!(
        result.submitted,
        "screen disambiguation should auto-select sess-a"
    );
    assert_eq!(
        result.cli_session_id.as_deref(),
        Some("sess-a"),
        "should bind to sess-a via screen disambiguation"
    );
    let _ = fs::remove_dir_all(root);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn adopted_pid_alive_weak_screen_match_stays_ambiguous() {
    let root = temp_dir("adopt-screen-weak");
    let data_dir = root.join("share").join("opencode");
    fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("opencode.db");
    create_db_with_sessions(
        &db_path,
        &[
            ("sess-x", "/repo/adopt-weak", 1000),
            ("sess-y", "/repo/adopt-weak", 2000),
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
        working_dir: "/repo/adopt-weak".to_string(),
        cli_session_id: None,
        transcript_offset: 0,
        emitted_final_text: None,
        adopted_pid: Some(std::process::id()),
    };
    let backend = RecordingBackend::new(db_path.clone(), false, 2001)
        .with_screen("some totally unrelated content here".to_string());
    let result = state.write_input(&backend, "cmd")
        .await
        .expect("write input");
    assert!(!result.submitted, "weak screen match should stay ambiguous");
    let reason = result.failure_reason.as_deref().unwrap_or("");
    assert!(
        reason.contains("ambiguous"),
        "expected ambiguous, got: {}",
        reason
    );
    let _ = fs::remove_dir_all(root);
}
