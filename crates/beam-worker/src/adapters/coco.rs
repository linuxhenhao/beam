use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use beam_core::{FinalOutputKind, InitConfig};
use serde_json::Value;

use crate::adapter::{
    Adapter, PollResult, SpawnSpec, SubmitResult, TranscriptCursor, confirm_submit_loop, file_size,
};
use crate::backend::SessionBackend;

const HISTORY_LOOKBACK: u64 = 65536;

#[derive(Debug, Clone, Default)]
pub(crate) struct CoCoState {
    history_path: PathBuf,
    cli_session_id: Option<String>,
    cursor: TranscriptCursor,
}

fn state_from_init(init: &InitConfig) -> CoCoState {
    let home = std::env::var("HOME").unwrap_or_default();
    let history_path = PathBuf::from(format!("{}/.cache/coco/history.jsonl", home));
    CoCoState {
        history_path,
        cli_session_id: init.cli_session_id.clone(),
        cursor: TranscriptCursor::new(),
    }
}

pub fn create(init: &InitConfig) -> Box<dyn Adapter> {
    Box::new(state_from_init(init))
}

#[async_trait]
impl Adapter for CoCoState {
    fn build_spawn_spec(&self, init: &InitConfig) -> SpawnSpec {
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
        if let Some(model) = &init.model
            && !model.is_empty()
        {
            args.push("--config".to_string());
            args.push(format!("model.name={}", model));
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

    async fn write_input(
        &mut self,
        backend: &dyn SessionBackend,
        content: &str,
    ) -> Result<SubmitResult> {
        let base_byte = file_size(&self.history_path);

        backend.paste_text(content).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        backend.send_enter().await?;

        let mut confirm = || -> Result<bool> {
            match coco_history_match(&self.history_path, base_byte, content)? {
                Some(session_id) => {
                    self.cli_session_id = Some(session_id);
                    Ok(true)
                }
                None => Ok(false),
            }
        };
        let mut confirmed = confirm_submit_loop(backend, &mut confirm).await?;
        if !confirmed {
            confirmed = confirm()?;
        }
        if confirmed {
            return Ok(SubmitResult {
                submitted: true,
                cli_session_id: self.cli_session_id.clone(),
                ..Default::default()
            });
        }
        Ok(SubmitResult {
            submitted: false,
            cli_session_id: self.cli_session_id.clone(),
            failure_reason: Some("CoCo history did not confirm submit".to_string()),
        })
    }

    fn poll(&mut self) -> Result<PollResult> {
        let path = self.history_path.clone();
        let lines = self.cursor.drain(&path)?;

        let mut result = PollResult {
            cli_session_id: self.cli_session_id.clone(),
            ..Default::default()
        };

        for line in &lines {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Some(mode) = value.get("mode").and_then(Value::as_str) else {
                continue;
            };
            if mode == "assistant" {
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
                if let Some(text) = value.get("content").and_then(Value::as_str)
                    && let Some(emitted) = self.cursor.emit_if_new(text)
                {
                    result.final_output = Some(emitted);
                    result.final_output_kind = Some(FinalOutputKind::Bridge);
                    result.prompt_ready = true;
                }
            }
        }

        Ok(result)
    }
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
    let start = from_byte.saturating_sub(HISTORY_LOOKBACK);
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
    use crate::adapter::test_support::{home_test_lock, set_home, temp_home, test_init};
    use std::fs::{self, create_dir_all};

    fn coco_init() -> InitConfig {
        InitConfig {
            session_id: "session-coco".to_string(),
            cli_bin: "/bin/coco".to_string(),
            prompt: "prompt".to_string(),
            cli_session_id: Some("cli-session".to_string()),
            ..test_init("coco")
        }
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
        let init = coco_init();
        let mut state = state_from_init(&init);
        write_history(
            &state.history_path,
            &[
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"length"}}},"content":"ignore"}"#,
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"stop"}}},"content":"done"}"#,
            ],
        );

        let first = state.poll().unwrap();
        assert_eq!(first.final_output.as_deref(), Some("done"));
        assert_eq!(first.final_output_kind, Some(FinalOutputKind::Bridge));
        assert!(first.prompt_ready);

        let second = state.poll().unwrap();
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
        let init = coco_init();
        let mut state = state_from_init(&init);
        write_history(
            &state.history_path,
            &[
                r#"{"mode":"user","content":"noise"}"#,
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"stop"}}},"content":"first"}"#,
            ],
        );

        let first = state.poll().unwrap();
        assert_eq!(first.final_output.as_deref(), Some("first"));

        write_history(
            &state.history_path,
            &[
                r#"{"mode":"assistant","message":{"message":{"response_meta":{"finish_reason":"stop"}}},"content":"first"}"#,
            ],
        );
        let second = state.poll().unwrap();
        assert_eq!(second.final_output.as_deref(), Some("first"));
    }
}
