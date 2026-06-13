use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskOption {
    pub key: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskQuestion {
    pub prompt: String,
    pub options: Vec<AskOption>,
    #[serde(rename = "multiSelect")]
    pub multi_select: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum AskResult {
    Answered {
        answers: Vec<Vec<String>>,
        by: String,
        comment: Option<String>,
        #[serde(default)]
        timed_out: bool,
    },
    TimedOut {
        selected: Option<String>,
        by: Option<String>,
        comment: Option<String>,
        #[serde(default)]
        timed_out: bool,
    },
    Invalidated {
        reason: String,
        selected: Option<String>,
        by: Option<String>,
        comment: Option<String>,
        #[serde(default)]
        timed_out: bool,
    },
}

impl AskResult {
    pub fn answered(answers: Vec<Vec<String>>, by: impl Into<String>) -> Self {
        Self::Answered {
            answers,
            by: by.into(),
            comment: None,
            timed_out: false,
        }
    }
}

pub fn legacy_selected(result: &AskResult) -> Option<String> {
    match result {
        AskResult::Answered { answers, .. } if answers.len() == 1 && answers[0].len() == 1 => {
            answers[0].first().cloned()
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct AskRequest {
    pub session_id: String,
    pub chat_id: String,
    pub lark_app_id: String,
    pub root_message_id: Option<String>,
    pub questions: Vec<AskQuestion>,
    pub timeout_ms: u64,
    pub approvers: HashSet<String>,
}
