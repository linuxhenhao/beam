use anyhow::Result;
use serde_json::Value;

use beam_core::{AskOption, AskQuestion};

#[derive(Debug, Clone)]
pub struct ParsedAsk {
    pub questions: Vec<AskQuestion>,
    pub raw: Value,
}

fn parse_options(raw: &Value) -> Vec<AskOption> {
    raw.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|opt| {
                    let label = opt
                        .get("label")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if label.is_empty() {
                        return None;
                    }
                    let key = opt
                        .get("key")
                        .and_then(Value::as_str)
                        .filter(|v| !v.trim().is_empty())
                        .unwrap_or(&label)
                        .to_string();
                    Some(AskOption { key, label })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_questions(cli_id: &str, payload: &Value) -> Option<ParsedAsk> {
    match cli_id {
        "claude-code" => parse_claude(payload),
        "opencode" => parse_opencode(payload),
        "codex" => None,
        _ => None,
    }
}

fn parse_claude(payload: &Value) -> Option<ParsedAsk> {
    let event = payload.get("hook_event_name").and_then(Value::as_str)?;
    if event != "PreToolUse" && event != "PermissionRequest" {
        return None;
    }
    if payload.get("tool_name").and_then(Value::as_str)? != "AskUserQuestion" {
        return None;
    }
    let raw_questions = payload
        .get("tool_input")
        .and_then(Value::as_object)?
        .get("questions")
        .and_then(Value::as_array)?;
    if raw_questions.is_empty() {
        return None;
    }
    let questions = raw_questions
        .iter()
        .filter_map(|q| {
            let prompt = q
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if prompt.trim().is_empty() {
                return None;
            }
            let options = parse_options(q.get("options").unwrap_or(&Value::Null));
            Some(AskQuestion {
                prompt,
                options,
                multi_select: q
                    .get("multiSelect")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect::<Vec<_>>();
    if questions.is_empty() {
        return None;
    }
    Some(ParsedAsk {
        questions,
        raw: payload.clone(),
    })
}

fn parse_opencode(payload: &Value) -> Option<ParsedAsk> {
    if payload.get("hook_event_name").and_then(Value::as_str)? != "QuestionAsked" {
        return None;
    }
    let raw_questions = payload
        .get("tool_input")
        .and_then(Value::as_object)?
        .get("questions")
        .and_then(Value::as_array)?;
    if raw_questions.is_empty() {
        return None;
    }
    let questions = raw_questions
        .iter()
        .filter_map(|q| {
            let prompt = q
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if prompt.trim().is_empty() {
                return None;
            }
            let options = parse_options(q.get("options").unwrap_or(&Value::Null));
            Some(AskQuestion {
                prompt,
                options,
                multi_select: q.get("multiple").and_then(Value::as_bool).unwrap_or(false),
            })
        })
        .collect::<Vec<_>>();
    if questions.is_empty() {
        return None;
    }
    Some(ParsedAsk {
        questions,
        raw: payload.clone(),
    })
}

pub fn format_answer(cli_id: &str, answers: &[Vec<String>], parsed: &ParsedAsk) -> Result<String> {
    let _ = parsed;
    match cli_id {
        "claude-code" => {
            let mut answers_map = serde_json::Map::new();
            for (idx, question) in parsed.questions.iter().enumerate() {
                let selected = answers.get(idx).cloned().unwrap_or_default();
                if selected.is_empty() {
                    continue;
                }
                let labels = selected
                    .iter()
                    .map(|key| {
                        question
                            .options
                            .iter()
                            .find(|opt| opt.key == *key)
                            .map(|opt| opt.label.clone())
                            .unwrap_or_else(|| key.clone())
                    })
                    .collect::<Vec<_>>();
                answers_map.insert(question.prompt.clone(), Value::String(labels.join(", ")));
            }
            let event_name = parsed
                .raw
                .get("hook_event_name")
                .and_then(Value::as_str)
                .unwrap_or("PreToolUse");
            let directive = if event_name == "PermissionRequest" {
                serde_json::json!({
                    "hookSpecificOutput": {
                        "hookEventName": "PermissionRequest",
                        "decision": {
                            "behavior": "allow",
                            "updatedInput": {
                                "questions": parsed.raw.pointer("/tool_input/questions").cloned().unwrap_or(Value::Null),
                                "answers": answers_map,
                            }
                        }
                    }
                })
            } else {
                serde_json::json!({
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "updatedInput": {
                            "questions": parsed.raw.pointer("/tool_input/questions").cloned().unwrap_or(Value::Null),
                            "answers": answers_map,
                        }
                    }
                })
            };
            Ok(serde_json::to_string(&directive)?)
        }
        "opencode" => {
            let directive = serde_json::json!({
                "type": "answer",
                "answers": answers,
            });
            Ok(serde_json::to_string(&directive)?)
        }
        "codex" => Ok(String::new()),
        _ => Ok(String::new()),
    }
}

pub fn passthrough(cli_id: &str, payload: &Value) -> Result<String> {
    match cli_id {
        "codex" => Ok(String::new()),
        "claude-code" | "opencode" => {
            let _ = payload;
            Ok(String::new())
        }
        _ => Ok(String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_format_claude_question() {
        let payload = serde_json::json!({
            "hook_event_name": "PreToolUse",
            "tool_name": "AskUserQuestion",
            "tool_input": {
                "questions": [
                    {
                        "question": "pick one",
                        "multiSelect": false,
                        "options": [
                            { "label": "yes" },
                            { "label": "no" }
                        ]
                    }
                ]
            }
        });
        let parsed = parse_questions("claude-code", &payload).expect("parsed");
        let directive =
            format_answer("claude-code", &[vec!["yes".to_string()]], &parsed).expect("directive");
        assert!(directive.contains("\"hookSpecificOutput\""));
        assert!(directive.contains("pick one"));
    }

    #[test]
    fn parse_and_format_opencode_question() {
        let payload = serde_json::json!({
            "hook_event_name": "QuestionAsked",
            "tool_input": {
                "questions": [
                    {
                        "question": "pick many",
                        "multiple": true,
                        "options": [
                            { "label": "a" },
                            { "label": "b" }
                        ]
                    }
                ]
            }
        });
        let parsed = parse_questions("opencode", &payload).expect("parsed");
        let directive = format_answer(
            "opencode",
            &[vec!["a".to_string(), "b".to_string()]],
            &parsed,
        )
        .expect("directive");
        assert!(directive.contains("\"type\":\"answer\""));
        assert!(directive.contains("\"a\""));
    }

    #[test]
    fn codex_passthrough_is_empty() {
        let payload = serde_json::json!({});
        assert_eq!(passthrough("codex", &payload).unwrap(), "");
    }
}
