use anyhow::Result;
use serde_json::Value;

use beam_core::{AskOption, AskQuestion};

#[derive(Debug, Clone)]
pub struct ParsedAsk {
    pub kind: AskKind,
    pub questions: Vec<AskQuestion>,
    pub raw: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskKind {
    Question,
    Permission,
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
        kind: AskKind::Question,
        questions,
        raw: payload.clone(),
    })
}

fn parse_opencode(payload: &Value) -> Option<ParsedAsk> {
    let event = payload.get("hook_event_name").and_then(Value::as_str)?;
    match event {
        "question.asked" => {
            let raw_questions = payload
                .get("tool_input")
                .and_then(Value::as_object)
                .and_then(|tool_input| tool_input.get("questions"))
                .or_else(|| payload.get("questions"))
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
                kind: AskKind::Question,
                questions,
                raw: payload.clone(),
            })
        }
        "permission.asked" => {
            let permission = payload
                .get("permission")
                .and_then(Value::as_str)
                .unwrap_or("permission request");
            let mut details = Vec::new();
            if let Some(patterns) = payload.get("patterns").and_then(Value::as_array) {
                let rendered = patterns
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|s| !s.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join(", ");
                if !rendered.is_empty() {
                    details.push(format!("patterns: {}", rendered));
                }
            }
            if let Some(tool) = payload.get("tool").and_then(Value::as_object) {
                if let Some(message_id) = tool.get("messageID").and_then(Value::as_str) {
                    details.push(format!("message: {}", message_id));
                }
                if let Some(call_id) = tool.get("callID").and_then(Value::as_str) {
                    details.push(format!("call: {}", call_id));
                }
            }
            if let Some(metadata) = payload.get("metadata").and_then(Value::as_object) {
                if let Some(description) = metadata.get("description").and_then(Value::as_str) {
                    details.push(description.to_string());
                }
            }
            let prompt = if details.is_empty() {
                permission.to_string()
            } else {
                format!("{}\n{}", permission, details.join("\n"))
            };
            Some(ParsedAsk {
                kind: AskKind::Permission,
                questions: vec![AskQuestion {
                    prompt,
                    options: vec![
                        AskOption {
                            key: "once".to_string(),
                            label: "Allow once".to_string(),
                        },
                        AskOption {
                            key: "always".to_string(),
                            label: "Allow always".to_string(),
                        },
                        AskOption {
                            key: "reject".to_string(),
                            label: "Reject".to_string(),
                        },
                    ],
                    multi_select: false,
                }],
                raw: payload.clone(),
            })
        }
        _ => None,
    }
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
            let directive = if parsed.kind == AskKind::Permission {
                let reply = answers
                    .first()
                    .and_then(|row| row.first())
                    .map(String::as_str)
                    .unwrap_or("");
                if reply.is_empty() {
                    return Ok(String::new());
                }
                serde_json::json!({
                    "type": "permission",
                    "request_id": parsed
                        .raw
                        .get("id")
                        .and_then(Value::as_str)
                        .or_else(|| parsed.raw.get("requestID").and_then(Value::as_str))
                        .unwrap_or(""),
                    "reply": reply,
                })
            } else {
                serde_json::json!({
                    "type": "answer",
                    "answers": answers,
                })
            };
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
            "hook_event_name": "question.asked",
            "id": "q_123",
            "sessionID": "s_123",
            "questions": [
                {
                    "question": "pick many",
                    "multiple": true,
                    "options": [
                        { "label": "a", "description": "A" },
                        { "label": "b", "description": "B" }
                    ]
                }
            ]
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
    fn parse_and_format_opencode_permission_request() {
        let payload = serde_json::json!({
            "hook_event_name": "permission.asked",
            "id": "perm_123",
            "permission": "bash",
            "patterns": ["git status", "git diff"],
            "metadata": { "description": "needs approval" },
            "tool": { "messageID": "msg_1", "callID": "call_1" }
        });
        let parsed = parse_questions("opencode", &payload).expect("parsed");
        assert_eq!(parsed.kind, AskKind::Permission);
        let directive =
            format_answer("opencode", &[vec!["once".to_string()]], &parsed).expect("directive");
        assert!(directive.contains("\"type\":\"permission\""));
        assert!(directive.contains("\"reply\":\"once\""));
        assert!(directive.contains("perm_123"));
    }

    #[test]
    fn parse_opencode_permission_ask_event() {
        let payload = serde_json::json!({
            "hook_event_name": "permission.asked",
            "id": "perm_ask",
            "permission": "external_directory"
        });
        let parsed = parse_questions("opencode", &payload).expect("parsed");
        assert_eq!(parsed.kind, AskKind::Permission);
        assert_eq!(parsed.questions[0].prompt, "external_directory");
    }

    #[test]
    fn codex_passthrough_is_empty() {
        let payload = serde_json::json!({});
        assert_eq!(passthrough("codex", &payload).unwrap(), "");
    }
}
