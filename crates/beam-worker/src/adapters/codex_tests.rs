#[cfg(test)]
use super::*;
use std::collections::VecDeque;
use std::path::PathBuf;

use beam_core::FinalOutputKind;

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("beam-codex-{}-{}", name, uuid::Uuid::new_v4()))
}

#[test]
fn rollout_path_extracts_session_id() {
    let path = "/tmp/rollout-20260603-019c6e27-e55b-73d1-87d8-4e01f1f75043.jsonl";
    assert_eq!(
        codex_session_id_from_rollout_path(path).as_deref(),
        Some("019c6e27-e55b-73d1-87d8-4e01f1f75043")
    );
}

#[test]
fn history_match_finds_submitted_text() {
    let path = temp_path("history.jsonl");
    std::fs::write(
        &path,
        concat!(
            "{\"text\":\"older\",\"session_id\":\"s0\"}\n",
            "{\"text\":\"hello\\nworld\",\"session_id\":\"s1\"}\n"
        ),
    )
    .unwrap();
    let found = codex_history_match(&path, &HistoryBoundary::Byte(0), "hello\r\nworld").unwrap();
    assert_eq!(found.as_deref(), Some("s1"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn history_match_supports_json_document() {
    let path = temp_path("history.json");
    std::fs::write(&path, r#"[{"sessionId":"s0","message":"hello\nworld"}]"#).unwrap();
    let boundary = capture_history_boundary(&path).unwrap();
    std::fs::write(
            &path,
            r#"[{"sessionId":"s0","message":"hello\nworld"},{"sessionId":"s1","message":"hello\nworld"}]"#,
        )
        .unwrap();
    let found = codex_history_match(&path, &boundary, "hello\r\nworld").unwrap();
    assert_eq!(found.as_deref(), Some("s1"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn history_match_ignores_stale_json_document_entry() {
    let path = temp_path("history.json");
    std::fs::write(&path, r#"[{"sessionId":"s0","message":"continue"}]"#).unwrap();
    let boundary = capture_history_boundary(&path).unwrap();
    let found = codex_history_match(&path, &boundary, "continue").unwrap();
    assert_eq!(found, None);
    let _ = std::fs::remove_file(path);
}

#[test]
fn missing_json_history_uses_an_empty_document_boundary() {
    let path = temp_path("history").with_extension("json");
    let boundary = capture_history_boundary(&path).unwrap();
    std::fs::write(&path, r#"[{"sessionId":"s1","message":"hello"}]"#).unwrap();
    let found = codex_history_match(&path, &boundary, "hello").unwrap();
    assert_eq!(found.as_deref(), Some("s1"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn traex_uses_its_rollout_home() {
    let (history_path, home_dir) = traex_paths(Path::new("/home/tester"));
    assert_eq!(
        history_path,
        PathBuf::from("/home/tester/.trae/cli/history.json")
    );
    assert_eq!(home_dir, PathBuf::from("/home/tester/.traex/cli"));
}

#[test]
fn latest_session_uses_beam_session_marker() {
    let path = temp_path("latest-history.jsonl");
    std::fs::write(
        &path,
        concat!(
            "{\"text\":\"no marker\",\"session_id\":\"s0\"}\n",
            "{\"text\":\"session beam-123 marker\",\"session_id\":\"s1\"}\n"
        ),
    )
    .unwrap();
    let found = latest_codex_session_for_beam_session(&path, "beam-123");
    assert_eq!(found.as_deref(), Some("s1"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn latest_session_supports_json_document_history() {
    let path = temp_path("latest-history.json");
    std::fs::write(
            &path,
            r#"{"entries":[{"sessionId":"s0","message":"no marker"},{"sessionId":"s1","message":"session beam-123 marker"}]}"#,
        )
        .unwrap();
    let found = latest_codex_session_for_beam_session(&path, "beam-123");
    assert_eq!(found.as_deref(), Some("s1"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn emits_final_output_from_rollout() {
    let path = temp_path("rollout.jsonl");
    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"ignore\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}}\n"
            ),
        )
        .unwrap();
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: None,
        cli_session_id: Some("sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: false,
        adopt_restored_from_metadata: false,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    };
    let result = state.poll().unwrap();
    assert_eq!(result.final_output.as_deref(), Some("done"));
    assert!(result.prompt_ready);
    let _ = std::fs::remove_file(path);
}

#[test]
fn adopt_emits_preamble_once_and_absorbs_history() {
    let path = temp_path("codex-adopt-rollout.jsonl");
    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: None,
        cli_session_id: Some("sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: true,
        adopt_restored_from_metadata: false,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    };
    let first = state.poll().unwrap();
    assert_eq!(
        first.adopt_preamble,
        Some(("ask".to_string(), "answer".to_string()))
    );
    assert!(first.final_output.is_none());
    let second = state.poll().unwrap();
    assert!(second.adopt_preamble.is_none());
    assert!(second.final_output.is_none());
    let _ = std::fs::remove_file(path);
}

#[test]
fn adopt_emits_local_turn_when_user_text_is_not_from_daemon() {
    let path = temp_path("codex-adopt-local-rollout.jsonl");
    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"local ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"local answer\"}]}}\n"
            ),
        )
        .unwrap();
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: None,
        cli_session_id: Some("sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: true,
        adopt_restored_from_metadata: false,
        adopt_preamble_emitted: true,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    };
    let result = state.poll().unwrap();
    assert_eq!(result.final_output.as_deref(), Some("local answer"));
    assert_eq!(result.final_output_kind, Some(FinalOutputKind::LocalTurn));
    assert_eq!(result.final_output_user_text.as_deref(), Some("local ask"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn adopt_keeps_remote_turn_as_bridge_output() {
    let path = temp_path("codex-adopt-remote-rollout.jsonl");
    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"remote ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"remote answer\"}]}}\n"
            ),
        )
        .unwrap();
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: None,
        cli_session_id: Some("sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: true,
        adopt_restored_from_metadata: false,
        adopt_preamble_emitted: true,
        pending_remote_user_inputs: VecDeque::from([crate::adapter::normalize_history_text(
            "remote ask",
        )]),
        active_turn: None,
    };
    let result = state.poll().unwrap();
    assert_eq!(result.final_output.as_deref(), Some("remote answer"));
    assert_eq!(result.final_output_kind, None);
    assert_eq!(result.final_output_user_text, None);
    let _ = std::fs::remove_file(path);
}

#[test]
fn adopt_restored_absorbs_history_without_preamble() {
    let path = temp_path("codex-adopt-restored-rollout.jsonl");
    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n"
            ),
        )
        .unwrap();
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: None,
        cli_session_id: Some("sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: true,
        adopt_restored_from_metadata: true,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    };
    let first = state.poll().unwrap();
    assert!(first.adopt_preamble.is_none());
    assert!(first.final_output.is_none());
    let second = state.poll().unwrap();
    assert!(second.adopt_preamble.is_none());
    assert!(second.final_output.is_none());
    let _ = std::fs::remove_file(path);
}

#[test]
fn bridge_queue_final_answer_detection_across_turns() {
    let path = temp_path("codex-bridge-queue.jsonl");
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: None,
        cli_session_id: Some("sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: false,
        adopt_restored_from_metadata: false,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    };

    let empty = state.poll().unwrap();
    assert!(empty.final_output.is_none());

    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn1\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"thinking\"}]}}\n",
            ),
        )
        .unwrap();

    let partial = state.poll().unwrap();
    assert!(partial.final_output.is_none());

    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn1\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"thinking\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"turn1 result\"}]}}\n",
            ),
        )
        .unwrap();

    let done = state.poll().unwrap();
    assert_eq!(done.final_output.as_deref(), Some("turn1 result"));
    assert!(done.prompt_ready);

    let _ = state.cursor.emit_if_new("turn1 result");

    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn1\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"analysis\",\"content\":[{\"type\":\"output_text\",\"text\":\"thinking\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"turn1 result\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"turn2\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"turn2 result\"}]}}\n",
            ),
        )
        .unwrap();

    let second_turn = state.poll().unwrap();
    assert_eq!(second_turn.final_output.as_deref(), Some("turn2 result"));
    assert!(second_turn.prompt_ready);

    let _ = std::fs::remove_file(path);
}

// ── pid-based strong anchoring ──────────────────────────────────

#[test]
#[cfg(unix)]
fn pid_anchoring_resolves_rollout_and_session_id_from_fd_dir() {
    let tmp = temp_path("pid-anchor");
    // Create mock ~/.codex/sessions/xxx/rollout-...-uuid.jsonl
    let sessions_dir = tmp.join(".codex").join("sessions").join("abc123");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let rollout_path =
        sessions_dir.join("rollout-20260603-019c6e27-e55b-73d1-87d8-4e01f1f75043.jsonl");
    std::fs::write(&rollout_path, "{}").unwrap();

    // Create mock fd dir with a symlink pointing to the rollout
    let fd_dir = tmp.join("fake-fd");
    std::fs::create_dir_all(&fd_dir).unwrap();
    std::os::unix::fs::symlink(&rollout_path, fd_dir.join("3")).unwrap();

    let result = find_codex_rollout_by_fd_dir(&fd_dir, &tmp.join(".codex").join("sessions"));
    assert!(
        matches!(result, ResolveOutcome::Found(_)),
        "pid anchoring should find the rollout"
    );
    let (_found_path, session_id) = match result {
        ResolveOutcome::Found(v) => v,
        other => panic!("expected Found, got {:?}", other),
    };
    assert_eq!(
        session_id, "019c6e27-e55b-73d1-87d8-4e01f1f75043",
        "session_id should be extracted from rollout filename"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
#[cfg(unix)]
fn pid_anchoring_supports_traex_sessions_root() {
    let tmp = temp_path("traex-pid-anchor");
    let sessions_dir = tmp
        .join(".traex")
        .join("cli")
        .join("sessions")
        .join("2026")
        .join("06")
        .join("11");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let rollout_path =
        sessions_dir.join("rollout-2026-06-11T10-00-00-8db7d911-96f3-4764-a310-e42ae4cb626f.jsonl");
    std::fs::write(&rollout_path, "{}").unwrap();

    let fd_dir = tmp.join("fake-fd");
    std::fs::create_dir_all(&fd_dir).unwrap();
    std::os::unix::fs::symlink(&rollout_path, fd_dir.join("3")).unwrap();

    let result =
        find_codex_rollout_by_fd_dir(&fd_dir, &tmp.join(".traex").join("cli").join("sessions"));
    let (_found_path, session_id) = match result {
        ResolveOutcome::Found(v) => v,
        other => panic!("expected Found, got {:?}", other),
    };
    assert_eq!(session_id, "8db7d911-96f3-4764-a310-e42ae4cb626f");

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn poll_with_rollout_resolved_by_pid_emits_preamble_without_cwd_fallback() {
    // Simulates what happens AFTER find_codex_rollout_by_pid resolves
    // the path: state has rollout_path and cli_session_id set from pid,
    // and adopt mode works without any cwd-based fallback.
    let path = temp_path("codex-pid-resolved.jsonl");
    std::fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"adopted ask\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"adopted answer\"}]}}\n"
            ),
        )
        .unwrap();
    let mut state = CodexState {
        home_dir: PathBuf::new(),
        history_path: PathBuf::new(),
        rollout_path: Some(path.clone()),
        cli_pid: Some(12345),
        // cli_session_id resolved from pid — NOT from cwd/latest fallback
        cli_session_id: Some("pid-resolved-sid".to_string()),
        cursor: TranscriptCursor::new(),
        adopt_mode: true,
        adopt_restored_from_metadata: false,
        adopt_preamble_emitted: false,
        pending_remote_user_inputs: VecDeque::new(),
        active_turn: None,
    };
    let result = state.poll().unwrap();
    assert_eq!(
        result.adopt_preamble,
        Some(("adopted ask".to_string(), "adopted answer".to_string())),
        "preamble should be emitted from pid-resolved rollout"
    );
    assert_eq!(
        result.cli_session_id.as_deref(),
        Some("pid-resolved-sid"),
        "cli_session_id should come from pid resolution"
    );
    assert!(result.final_output.is_none());
    let _ = std::fs::remove_file(path);
}

#[test]
fn pid_anchoring_session_id_from_rollout_path() {
    // Verify codex_session_id_from_rollout_path works with
    // the filename pattern that find_codex_rollout_by_pid discovers
    let path = "/home/user/.codex/sessions/proj/rollout-20260603-019c6e27-e55b-73d1-87d8-4e01f1f75043.jsonl";
    assert_eq!(
        codex_session_id_from_rollout_path(path).as_deref(),
        Some("019c6e27-e55b-73d1-87d8-4e01f1f75043")
    );
}
