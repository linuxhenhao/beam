use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{CoCoState, PollResult, SpawnSpec, SubmitResult, drain_jsonl, file_size};
use crate::backend::SessionBackend;

const HISTORY_LOOKBACK: u64 = 65536;

pub fn create_state(init: &InitConfig) -> CoCoState {
    let home = std::env::var("HOME").unwrap_or_default();
    let history_path = PathBuf::from(format!("{}/.cache/coco/history.jsonl", home));
    CoCoState {
        history_path,
        cli_session_id: init.cli_session_id.clone(),
        transcript_offset: 0,
        pending_tail: String::new(),
        emitted_final_text: None,
    }
}

pub fn build_spawn_spec(_state: &CoCoState, init: &InitConfig) -> SpawnSpec {
    let mut args = Vec::new();
    if init.resume {
        args.push("--resume".to_string());
        args.push(
            init.resume_session_id
                .clone()
                .unwrap_or_else(|| init.session_id.clone()),
        );
    } else {
        args.push("--session-id".to_string());
        args.push(init.session_id.clone());
    }
    if !init.disable_cli_bypass {
        args.push("--yolo".to_string());
    }
    if let Some(model) = &init.model {
        if !model.is_empty() {
            args.push("--config".to_string());
            args.push(format!("model.name={}", model));
        }
    }
    args.push("--disallowed-tool".to_string());
    args.push("EnterPlanMode".to_string());
    args.push("--disallowed-tool".to_string());
    args.push("ExitPlanMode".to_string());
    args.extend(init.cli_args.clone());
    SpawnSpec {
        bin: init.cli_bin.clone(),
        args,
    }
}

pub async fn write_input(
    state: &mut CoCoState,
    backend: &dyn SessionBackend,
    content: &str,
) -> Result<SubmitResult> {
    let base_byte = file_size(&state.history_path);

    backend.paste_text(content).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    backend.send_enter().await?;

    for attempt in 0..4 {
        tokio::time::sleep(Duration::from_millis(800)).await;
        if let Some(session_id) = coco_history_match(&state.history_path, base_byte, content)? {
            state.cli_session_id = Some(session_id);
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: state.cli_session_id.clone(),
                ..Default::default()
            });
        }
        if attempt < 3 {
            backend.send_enter().await?;
        }
    }
    if let Some(session_id) = coco_history_match(&state.history_path, base_byte, content)? {
        state.cli_session_id = Some(session_id);
        return Ok(SubmitResult {
            submitted: true,
            cli_session_id: state.cli_session_id.clone(),
            ..Default::default()
        });
    }
    Ok(SubmitResult {
        submitted: false,
        cli_session_id: state.cli_session_id.clone(),
        failure_reason: Some("CoCo history did not confirm submit".to_string()),
    })
}

pub fn poll(state: &mut CoCoState) -> Result<PollResult> {
    let path = state.history_path.clone();
    let current_size = file_size(&path);
    if current_size < state.transcript_offset {
        state.transcript_offset = 0;
        state.pending_tail.clear();
        state.emitted_final_text = None;
    }
    let drain = drain_jsonl(&path, state.transcript_offset, &state.pending_tail)?;
    state.transcript_offset = drain.new_offset;
    state.pending_tail = drain.pending_tail;

    let mut result = PollResult {
        cli_session_id: state.cli_session_id.clone(),
        ..Default::default()
    };

    for line in &drain.lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(mode) = value.get("mode").and_then(Value::as_str) else {
            continue;
        };
        match mode {
            "assistant" => {
                if value
                    .get("message")
                    .and_then(|v| v.get("message"))
                    .and_then(|v| v.get("response_meta"))
                    .and_then(|v| v.get("finish_reason"))
                    .and_then(Value::as_str)
                    != Some("stop")
                {
                    continue;
                }
                if let Some(text) = value.get("content").and_then(Value::as_str) {
                    let text = text.to_string();
                    if !text.is_empty() && state.emitted_final_text.as_deref() != Some(&text) {
                        state.emitted_final_text = Some(text.clone());
                        result.final_output = Some(text);
                        result.final_output_kind = Some(FinalOutputKind::Bridge);
                        result.prompt_ready = true;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(result)
}

fn coco_history_match(
    history_path: &Path,
    from_byte: u64,
    expected_text: &str,
) -> Result<Option<String>> {
    if !history_path.exists() {
        return Ok(None);
    }
    let size = file_size(history_path);
    if size <= from_byte {
        return Ok(None);
    }
    let start = if from_byte > HISTORY_LOOKBACK {
        from_byte - HISTORY_LOOKBACK
    } else {
        0
    };
    let mut file = File::open(history_path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let prefix = &expected_text.chars().take(40).collect::<String>();
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("mode").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let Some(actual) = value.get("content").and_then(Value::as_str) else {
            continue;
        };
        if actual.starts_with(prefix) {
            return Ok(value
                .get("session_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::home_test_lock;
    use beam_core::{BackendType, InitConfig, ScreenAnalyzerConfig};
    use std::fs::{self, create_dir_all};
    use std::path::PathBuf;
    use uuid::Uuid;

    fn test_init() -> InitConfig {
        InitConfig {
            session_id: "session-coco".to_string(),
            title: "title".to_string(),
            chat_id: "chat".to_string(),
            root_message_id: "root".to_string(),
            working_dir: "/tmp".to_string(),
            cli_id: "coco".to_string(),
            cli_bin: "/bin/coco".to_string(),
            cli_args: vec![],
            backend_type: BackendType::Tmux,
            prompt: "prompt".to_string(),
            resume: false,
            cli_session_id: Some("cli-session".to_string()),
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            prompt_turn_id: None,
            web_port: None,
            owner_open_id: None,
            adopted_from: None,
            adopt_restored_from_metadata: false,
            screen_analyzer: ScreenAnalyzerConfig::default(),
            initial_prompt: None,
            model: None,
            locale: None,
            bot_name: None,
            bot_open_id: None,
            resume_session_id: None,
            disable_cli_bypass: false,
        }
    }

    fn temp_home(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}-{}", prefix, Uuid::new_v4()))
    }

    struct HomeGuard {
        old_home: Option<std::ffi::OsString>,
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            match &self.old_home {
                Some(home) => unsafe {
                    std::env::set_var("HOME", home);
                },
                None => unsafe {
                    std::env::remove_var("HOME");
                },
            }
        }
    }

    fn set_home(home: &PathBuf) -> HomeGuard {
        let old_home = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", home);
        }
        HomeGuard { old_home }
    }

    fn write_history(path: &Path, lines: &[&str]) {
        if let Some(parent) = path.parent() {
            create_dir_all(parent).unwrap();
        }
        fs::write(path, lines.join("\n") + "\n").unwrap();
    }

    #[test]
    fn poll_emits_only_stop_final_output_and_dedupes_repeats() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-coco-test");
        let _guard = set_home(&home);
        let init = test_init();
        let mut state = create_state(&init);
        write_history(
            &state.history_path,
            &[
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"length"}}},"content":"ignore"}"#,
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"stop"}}},"content":"done"}"#,
            ],
        );

        let first = poll(&mut state).unwrap();
        assert_eq!(first.final_output.as_deref(), Some("done"));
        assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
        assert!(first.prompt_ready);

        let second = poll(&mut state).unwrap();
        assert!(second.final_output.is_none());
        assert!(!second.prompt_ready);
    }

    #[test]
    fn poll_recovers_after_truncation_and_re_emits_final_output() {
        let _lock = home_test_lock()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let home = temp_home("beam-coco-truncate-test");
        let _guard = set_home(&home);
        let init = test_init();
        let mut state = create_state(&init);
        write_history(
            &state.history_path,
            &[
                r#"{"mode":"user","content":"noise"}"#,
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"stop"}}},"content":"first"}"#,
            ],
        );

        let first = poll(&mut state).unwrap();
        assert_eq!(first.final_output.as_deref(), Some("first"));

        write_history(
            &state.history_path,
            &[
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"stop"}}},"content":"first"}"#,
            ],
        );
        let second = poll(&mut state).unwrap();
        assert_eq!(second.final_output.as_deref(), Some("first"));
    }
}
