use super::*;

// ---- Types ----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LarkEventMention {
    pub(crate) key: String,
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LarkTextAction {
    Close,
    Restart,
    Card,
    AdoptZellij(String),
    AdoptHerdr(String),
    AdoptList,
    PassthroughInput(String),
    ReuseSessionInput,
    CreateSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedLarkCardAction {
    pub(crate) action: String,
    pub(crate) session_id: Option<String>,
    pub(crate) root_id: Option<String>,
    pub(crate) clicked_message_id: Option<String>,
    pub(crate) operator_open_id: Option<String>,
    pub(crate) term_key: Option<TermActionKey>,
    pub(crate) visibility: Option<String>,
    pub(crate) card_nonce: Option<String>,
    pub(crate) special_keys: Option<Vec<String>>,
    pub(crate) selected_text: Option<String>,
    pub(crate) input_keys: Option<Vec<String>>,
    pub(crate) input_text: Option<String>,
    pub(crate) option_type: Option<String>,
    pub(crate) selected_index: Option<usize>,
    pub(crate) is_final: bool,
    pub(crate) workflow_run_id: Option<String>,
    pub(crate) workflow_id: Option<String>,
    pub(crate) workflow_revision_id: Option<String>,
    pub(crate) workflow_node_id: Option<String>,
    pub(crate) workflow_activity_id: Option<String>,
    pub(crate) workflow_attempt_id: Option<String>,
    pub(crate) workflow_comment: Option<String>,
    pub(crate) raw_value: Option<String>,
    pub(crate) ask_id: Option<String>,
    pub(crate) ask_nonce: Option<String>,
    pub(crate) ask_question_index: Option<usize>,
    pub(crate) ask_key: Option<String>,
    pub(crate) ask_submit: bool,
    pub(crate) pending_id: Option<String>,
    pub(crate) working_dir: Option<String>,
    pub(crate) dir_search_keyword: Option<String>,
    pub(crate) cli_session_id: Option<String>,
}

// ---- Message parsing ----

pub(crate) fn resolve_lark_mentions(text: &str, mentions: &[LarkEventMention]) -> String {
    if mentions.is_empty() {
        return text.to_string();
    }
    let mut resolved = text.to_string();
    let mut sorted = mentions.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|mention| std::cmp::Reverse(mention.key.len()));
    for mention in sorted {
        resolved = resolved.replace(&mention.key, &format!("@{}", mention.name));
    }
    resolved
}

pub(crate) fn strip_leading_mentions(text: &str, mentions: &[LarkEventMention]) -> String {
    let mut s = text.trim_start().to_string();
    if !mentions.is_empty() {
        let mut sorted = mentions.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|mention| std::cmp::Reverse(mention.name.len()));
        loop {
            let mut changed = false;
            for mention in &sorted {
                let tag = format!("@{}", mention.name);
                if s.starts_with(&tag) {
                    s = s[tag.len()..].trim_start().to_string();
                    changed = true;
                    break;
                }
            }
            if !changed {
                break;
            }
        }
        return s;
    }

    while let Some(stripped) = s.strip_prefix('@') {
        let end = stripped.find(char::is_whitespace).unwrap_or(stripped.len());
        s = stripped[end..].trim_start().to_string();
    }
    s
}

pub(crate) fn parse_force_topic_invocation(text: &str) -> Option<String> {
    let trimmed = text.trim_start();
    if let Some(rest) = trimmed.strip_prefix("/topic") {
        if rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace()) {
            return Some(rest.trim_start().to_string());
        }
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("/t") {
        if rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace()) {
            return Some(rest.trim_start().to_string());
        }
        return None;
    }
    None
}

pub(crate) fn classify_lark_text_action(text: &str, has_existing_session: bool) -> LarkTextAction {
    if text == "/close" {
        return LarkTextAction::Close;
    }
    if text == "/restart" {
        return LarkTextAction::Restart;
    }
    if text == "/card" {
        return LarkTextAction::Card;
    }
    if let Some(rest) = text.strip_prefix("/adopt ") {
        // Tolerate multi-line copies from the /adopt list reply: only the
        // first line carries the "<session>:<pane_id>" target.
        let rest = rest.lines().next().unwrap_or("").trim();
        if rest.is_empty() || rest == "list" {
            return LarkTextAction::AdoptList;
        }
        // Herdr public pane ids collide with the zellij `session:pane` grammar,
        // so `/adopt herdr:<pane_id>` disambiguates. Case-insensitive prefix;
        // the remainder is passed through and the actual pane lookup validates
        // it (avoids re-implementing herdr's base-32 id encoding).
        if let Some(pane_id) = rest
            .get(.."herdr:".len())
            .filter(|prefix| prefix.eq_ignore_ascii_case("herdr:"))
            .and_then(|_| rest.get("herdr:".len()..))
        {
            return LarkTextAction::AdoptHerdr(pane_id.trim().to_string());
        }
        return LarkTextAction::AdoptZellij(rest.to_string());
    }
    if text == "/adopt" {
        return LarkTextAction::AdoptList;
    }
    if text.starts_with('/') {
        return LarkTextAction::PassthroughInput(text.to_string());
    }
    if has_existing_session {
        LarkTextAction::ReuseSessionInput
    } else {
        LarkTextAction::CreateSession
    }
}

// ---- Card action parsing ----

pub(crate) fn parse_special_keys(value: &Value) -> Option<Vec<String>> {
    if let Some(keys) = value.as_array() {
        let items = keys
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        return (!items.is_empty()).then_some(items);
    }
    let raw = value.as_str()?;
    serde_json::from_str::<Vec<String>>(raw)
        .ok()
        .filter(|keys| !keys.is_empty())
}

pub(crate) fn try_parse_select_option(
    option_str: &str,
) -> Option<(String, Option<String>, Option<String>)> {
    let v: Value = serde_json::from_str(option_str).ok()?;
    let action = v.get("action").and_then(Value::as_str)?.to_string();
    let pending_id = v
        .get("pending_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let working_dir = v
        .get("working_dir")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some((action, pending_id, working_dir))
}

pub(crate) fn parse_lark_card_action(
    payload: &Value,
) -> Result<ParsedLarkCardAction, (StatusCode, String)> {
    let action_from_value = payload
        .pointer("/action/value/action")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    // Fallback: /action/option/ for select_static dropdown events.
    // The option value is a JSON-encoded string containing {action, pending_id, working_dir}.
    let option_parsed = if action_from_value.is_none() {
        payload
            .pointer("/action/option")
            .and_then(Value::as_str)
            .and_then(try_parse_select_option)
    } else {
        None
    };

    let (action_str, opt_pending_id, opt_working_dir) = match (action_from_value, option_parsed) {
        (Some(action), _) => (action, None, None),
        (None, Some((action, pending_id, working_dir))) => (action, pending_id, working_dir),
        (None, None) => {
            return Err((StatusCode::BAD_REQUEST, "missing card action".to_string()));
        }
    };

    Ok(ParsedLarkCardAction {
        action: action_str,
        session_id: payload
            .pointer("/action/value/session_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        root_id: payload
            .pointer("/action/value/root_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        clicked_message_id: payload
            .pointer("/context/open_message_id")
            .and_then(Value::as_str)
            .or_else(|| payload.pointer("/open_message_id").and_then(Value::as_str))
            .map(ToOwned::to_owned),
        operator_open_id: payload
            .pointer("/operator/open_id")
            .and_then(Value::as_str)
            .or_else(|| {
                payload
                    .pointer("/operator_id/open_id")
                    .and_then(Value::as_str)
            })
            .map(ToOwned::to_owned),
        term_key: payload
            .pointer("/action/value/key")
            .and_then(Value::as_str)
            .and_then(parse_term_action_key),
        visibility: payload
            .pointer("/action/value/visibility")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        card_nonce: payload
            .pointer("/action/value/card_nonce")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        special_keys: payload
            .pointer("/action/value/keys")
            .and_then(parse_special_keys),
        selected_text: payload
            .pointer("/action/value/selected_text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        input_keys: payload
            .pointer("/action/value/input_keys")
            .and_then(parse_special_keys),
        input_text: payload
            .pointer("/action/form_value/tui_custom_input")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        option_type: payload
            .pointer("/action/value/option_type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        selected_index: payload
            .pointer("/action/value/selected_index")
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        is_final: payload
            .pointer("/action/value/is_final")
            .and_then(Value::as_str)
            .map(|value| value == "1")
            .unwrap_or(false),
        workflow_run_id: payload
            .pointer("/action/value/run_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workflow_id: payload
            .pointer("/action/value/workflow_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workflow_revision_id: payload
            .pointer("/action/value/revision_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workflow_node_id: payload
            .pointer("/action/value/node_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workflow_activity_id: payload
            .pointer("/action/value/activity_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workflow_attempt_id: payload
            .pointer("/action/value/attempt_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        workflow_comment: payload
            .pointer("/action/form_value/wf_comment")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        raw_value: payload.pointer("/action/value").and_then(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| serde_json::to_string(value).ok())
        }),
        ask_id: payload
            .pointer("/action/value/ask_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        ask_nonce: payload
            .pointer("/action/value/nonce")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        ask_question_index: payload
            .pointer("/action/value/question_index")
            .and_then(Value::as_u64)
            .map(|v| v as usize),
        ask_key: payload
            .pointer("/action/value/key")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        ask_submit: payload
            .pointer("/action/value/action")
            .and_then(Value::as_str)
            .map(|v| v == "ask_submit")
            .unwrap_or(false),
        pending_id: payload
            .pointer("/action/value/pending_id")
            .and_then(Value::as_str)
            .or(opt_pending_id.as_deref())
            .map(ToOwned::to_owned),
        working_dir: payload
            .pointer("/action/value/working_dir")
            .and_then(Value::as_str)
            .or(opt_working_dir.as_deref())
            .map(ToOwned::to_owned),
        dir_search_keyword: payload
            .pointer("/action/form_value/dir_search_keyword")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        cli_session_id: payload
            .pointer("/action/value/cli_session_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(crate) fn parse_term_action_key(raw: &str) -> Option<TermActionKey> {
    match raw {
        "esc" => Some(TermActionKey::Esc),
        "ctrlc" => Some(TermActionKey::CtrlC),
        "tab" => Some(TermActionKey::Tab),
        "enter" => Some(TermActionKey::Enter),
        "space" => Some(TermActionKey::Space),
        "up" => Some(TermActionKey::Up),
        "down" => Some(TermActionKey::Down),
        "left" => Some(TermActionKey::Left),
        "right" => Some(TermActionKey::Right),
        "half_page_up" => Some(TermActionKey::HalfPageUp),
        "half_page_down" => Some(TermActionKey::HalfPageDown),
        _ => None,
    }
}

// ---- Message content parsing ----

pub(crate) fn parse_lark_inbound_message(
    payload: &Value,
) -> Result<ParsedLarkInboundMessage, (StatusCode, String)> {
    let event = payload
        .get("event")
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing event payload".to_string()))?;
    let message = event.get("message").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "missing message payload".to_string(),
        )
    })?;
    let mentions = message
        .get("mentions")
        .cloned()
        .map(serde_json::from_value::<Vec<LarkEventMention>>)
        .transpose()
        .map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid mentions payload: {}", err),
            )
        })?
        .unwrap_or_default();
    let sender_open_id = event
        .pointer("/sender/sender_id/open_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let sender_type = event
        .pointer("/sender/sender_type")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let message_id = message
        .get("message_id")
        .and_then(Value::as_str)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing message_id".to_string()))?;
    let event_id = payload
        .pointer("/header/event_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| message_id.to_string());
    let chat_id = message
        .get("chat_id")
        .and_then(Value::as_str)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing chat_id".to_string()))?;
    let root_id = message
        .get("root_id")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty());
    let thread_id = message
        .get("thread_id")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty());
    let root_id_owned = root_id.map(ToOwned::to_owned);
    let thread_id_owned = thread_id.map(ToOwned::to_owned);
    let parent_id = message
        .get("parent_id")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned);
    let chat_type = message.get("chat_type").and_then(Value::as_str);
    let locale = extract_lark_message_locale(payload);
    let (scope, anchor) = decide_lark_routing(message_id, chat_id, chat_type, root_id, thread_id);
    let content_raw = message
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing content".to_string()))?;
    let content_json: Value = serde_json::from_str(content_raw).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid content json: {}", err),
        )
    })?;
    let raw_text = content_json
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let text = strip_leading_mentions(&resolve_lark_mentions(&raw_text, &mentions), &mentions);
    Ok(ParsedLarkInboundMessage {
        event_id,
        message_id: message_id.to_string(),
        chat_id: chat_id.to_string(),
        chat_type: chat_type.map(ToOwned::to_owned),
        sender_type,
        scope,
        anchor: anchor.to_string(),
        text,
        sender_open_id,
        mentions,
        parent_id,
        root_id: root_id_owned,
        thread_id: thread_id_owned,
        locale,
    })
}

pub(crate) fn normalize_lark_locale(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    if normalized == "zh" || normalized.starts_with("zh_") {
        Some("zh".to_string())
    } else if normalized == "en" || normalized.starts_with("en_") {
        Some("en".to_string())
    } else {
        None
    }
}

pub(crate) fn extract_lark_message_locale(payload: &Value) -> Option<String> {
    [
        "/event/message/locale",
        "/event/message/language",
        "/event/message/i18n_locale",
        "/event/message/message_locale",
        "/event/locale",
        "/header/locale",
    ]
    .iter()
    .filter_map(|pointer| payload.pointer(pointer).and_then(Value::as_str))
    .find_map(normalize_lark_locale)
}

pub(crate) fn lark_locale_or_english(locale: Option<&str>) -> &'static str {
    match locale.and_then(normalize_lark_locale).as_deref() {
        Some("zh") => "zh",
        _ => "en",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn classify_lark_text_action_routes_commands_and_session_reuse() {
        assert_eq!(
            classify_lark_text_action("/close", false),
            LarkTextAction::Close
        );
        assert_eq!(
            classify_lark_text_action("/restart", true),
            LarkTextAction::Restart
        );
        assert_eq!(
            classify_lark_text_action("/card", true),
            LarkTextAction::Card
        );
        assert_eq!(
            classify_lark_text_action("/adopt zellij  0:1.0  ", false),
            LarkTextAction::AdoptZellij("zellij  0:1.0".to_string())
        );
        assert_eq!(
            classify_lark_text_action("/adopt mysession:0.1", false),
            LarkTextAction::AdoptZellij("mysession:0.1".to_string())
        );
        assert_eq!(
            classify_lark_text_action("/adopt mysession", false),
            LarkTextAction::AdoptZellij("mysession".to_string())
        );
        assert_eq!(
            classify_lark_text_action(
                "/adopt mysession:terminal_0\n  claude  /home/user/proj",
                false
            ),
            LarkTextAction::AdoptZellij("mysession:terminal_0".to_string())
        );
        assert_eq!(
            classify_lark_text_action("/adopt", false),
            LarkTextAction::AdoptList
        );
        assert_eq!(
            classify_lark_text_action("/adopt list", false),
            LarkTextAction::AdoptList
        );
        assert_eq!(
            classify_lark_text_action("continue please", true),
            LarkTextAction::ReuseSessionInput
        );
        assert_eq!(
            classify_lark_text_action("new topic", false),
            LarkTextAction::CreateSession
        );
    }

    #[test]
    fn classify_adopt_herdr_disambiguates_public_pane_ids() {
        // `herdr:` prefix with a valid public id → AdoptHerdr.
        assert_eq!(
            classify_lark_text_action("/adopt herdr:w1:p1", false),
            LarkTextAction::AdoptHerdr("w1:p1".to_string())
        );
        assert_eq!(
            classify_lark_text_action("/adopt HERDR:w2:p3", false),
            LarkTextAction::AdoptHerdr("w2:p3".to_string())
        );
        // Herdr 0.8.x workspace ids are base-26 style (`wN`, `wP`).
        assert_eq!(
            classify_lark_text_action("/adopt herdr:wN:p1", false),
            LarkTextAction::AdoptHerdr("wN:p1".to_string())
        );
        // Bare `w1:p1` is a zellij target (session w1 / pane p1), NOT herdr.
        assert_eq!(
            classify_lark_text_action("/adopt w1:p1", false),
            LarkTextAction::AdoptZellij("w1:p1".to_string())
        );
        assert_eq!(
            classify_lark_text_action("/adopt my-session:terminal_0", false),
            LarkTextAction::AdoptZellij("my-session:terminal_0".to_string())
        );
        // Any `herdr:` target goes to the herdr adopt path; the pane lookup
        // validates it. Never falls through to a zellij session named "herdr".
        assert_eq!(
            classify_lark_text_action("/adopt herdr:not-a-pane", false),
            LarkTextAction::AdoptHerdr("not-a-pane".to_string())
        );
    }

    #[test]
    fn parse_lark_card_action_extracts_resume_payload() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": {
                "value": {
                    "action": "resume",
                    "root_id": "om_root",
                    "session_id": "sess-1"
                }
            }
        });
        assert_eq!(
            parse_lark_card_action(&payload).expect("parsed"),
            ParsedLarkCardAction {
                action: "resume".to_string(),
                session_id: Some("sess-1".to_string()),
                root_id: Some("om_root".to_string()),
                clicked_message_id: None,
                operator_open_id: Some("ou_user".to_string()),
                term_key: None,
                visibility: None,
                card_nonce: None,
                special_keys: None,
                selected_text: None,
                input_keys: None,
                input_text: None,
                option_type: None,
                selected_index: None,
                is_final: false,
                workflow_run_id: None,
                workflow_id: None,
                workflow_revision_id: None,
                workflow_node_id: None,
                workflow_activity_id: None,
                workflow_attempt_id: None,
                workflow_comment: None,
                raw_value: Some(
                    serde_json::json!({
                        "action": "resume",
                        "root_id": "om_root",
                        "session_id": "sess-1"
                    })
                    .to_string(),
                ),
                ask_id: None,
                ask_nonce: None,
                ask_question_index: None,
                ask_key: None,
                ask_submit: false,
                pending_id: None,
                working_dir: None,
                dir_search_keyword: None,
                cli_session_id: None,
            }
        );
    }

    #[test]
    fn parse_lark_card_action_accepts_operator_id_open_id() {
        let payload = serde_json::json!({
            "operator_id": { "open_id": "ou_owner" },
            "action": {
                "value": {
                    "action": "close",
                    "session_id": "sess-1"
                }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.operator_open_id.as_deref(), Some("ou_owner"));
    }

    #[test]
    fn parse_lark_card_action_extracts_visibility() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "context": { "open_message_id": "om_card_clicked" },
            "action": {
                "value": {
                    "action": "close",
                    "session_id": "sess-1",
                    "visibility": "private"
                }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.visibility.as_deref(), Some("private"));
        assert_eq!(
            parsed.clicked_message_id.as_deref(),
            Some("om_card_clicked")
        );
    }

    #[test]
    fn parse_lark_card_action_extracts_workflow_payload() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "context": { "open_message_id": "om_card_clicked" },
            "action": {
                "value": {
                    "action": "wf_approve",
                    "run_id": "run-1",
                    "workflow_id": "flow-a",
                    "revision_id": "rev-9",
                    "node_id": "node-1",
                    "activity_id": "act-1",
                    "attempt_id": "att-1",
                    "card_nonce": "nonce-1"
                },
                "form_value": { "wf_comment": "looks good" }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.action, "wf_approve");
        assert_eq!(parsed.workflow_run_id.as_deref(), Some("run-1"));
        assert_eq!(parsed.workflow_id.as_deref(), Some("flow-a"));
        assert_eq!(parsed.workflow_revision_id.as_deref(), Some("rev-9"));
        assert_eq!(parsed.workflow_node_id.as_deref(), Some("node-1"));
        assert_eq!(parsed.workflow_activity_id.as_deref(), Some("act-1"));
        assert_eq!(parsed.workflow_attempt_id.as_deref(), Some("att-1"));
        assert_eq!(parsed.workflow_comment.as_deref(), Some("looks good"));
    }

    #[test]
    fn parse_lark_card_action_extracts_dir_search_keyword_from_form_value() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "context": { "open_message_id": "om_card_clicked" },
            "action": {
                "value": {
                    "action": "dir_select_filter",
                    "pending_id": "pending-abc"
                },
                "form_value": { "dir_search_keyword": "src/crates" }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.action, "dir_select_filter");
        assert_eq!(parsed.pending_id.as_deref(), Some("pending-abc"));
        assert_eq!(
            parsed.dir_search_keyword.as_deref(),
            Some("src/crates"),
            "dir_search_keyword should be extracted from /action/form_value/dir_search_keyword"
        );
    }

    #[test]
    fn parse_lark_card_action_dir_search_keyword_none_when_no_form_value() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": {
                "value": {
                    "action": "dir_select_pick",
                    "pending_id": "pending-abc",
                    "working_dir": "src"
                }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.action, "dir_select_pick");
        assert_eq!(parsed.dir_search_keyword.as_deref(), None);
    }

    #[test]
    fn parse_lark_card_action_rejects_missing_action() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": { "value": { "session_id": "sess-1" } }
        });
        let err = parse_lark_card_action(&payload).expect_err("missing action should fail");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert_eq!(err.1, "missing card action");
    }

    #[test]
    fn parse_lark_card_action_serializes_object_value_for_raw_payload() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": {
                "value": {
                    "action": "grant_chat",
                    "nonce": "n-1",
                    "targets": ["ou_1"],
                    "chatId": "oc_1"
                }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("action should parse");
        let raw = parsed.raw_value.expect("raw payload should be preserved");
        assert!(raw.contains("\"grant_chat\""));
        assert!(raw.contains("\"targets\""));
    }

    #[test]
    fn parse_lark_card_action_extracts_select_static_option() {
        // Simulates a select_static dropdown selection event.
        // The selected option value is a JSON-encoded string with
        // action, pending_id, and working_dir.
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "context": { "open_message_id": "om_card" },
            "action": {
                "tag": "select_static",
                "option": "{\"action\":\"dir_select_pick\",\"pending_id\":\"pid-1\",\"working_dir\":\"project-a\"}"
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed select_static action");
        assert_eq!(parsed.action, "dir_select_pick");
        assert_eq!(parsed.pending_id.as_deref(), Some("pid-1"));
        assert_eq!(parsed.working_dir.as_deref(), Some("project-a"));
    }

    #[test]
    fn parse_lark_card_action_select_static_option_falls_back_to_value() {
        // When both /action/value/action and /action/option/ exist,
        // /action/value/action takes priority (button click with option field).
        // This tests that select_static option parsing doesn't interfere.
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": {
                "value": {
                    "action": "dir_select_filter",
                    "pending_id": "pid-v"
                },
                "tag": "button",
                "option": "should-be-ignored"
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.action, "dir_select_filter");
        assert_eq!(parsed.pending_id.as_deref(), Some("pid-v"));
        // option is only used when /action/value/action is absent
    }

    #[test]
    fn parse_lark_card_action_rejects_malformed_select_static_option() {
        // If /action/option/ is not valid JSON, it should still fail
        // with "missing card action" since no /action/value/action exists.
        let payload = serde_json::json!({
            "action": {
                "option": "not-valid-json"
            }
        });
        let err = parse_lark_card_action(&payload).expect_err("should fail");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert_eq!(err.1, "missing card action");
    }

    #[test]
    fn parse_lark_card_action_extracts_tui_prompt_fields() {
        let payload = serde_json::json!({
            "operator": { "open_id": "ou_user" },
            "action": {
                "form_value": { "tui_custom_input": "hello world" },
                "value": {
                    "action": "tui_text_input",
                    "session_id": "sess-1",
                    "input_keys": ["Down", "Enter"],
                    "option_type": "input",
                    "selected_index": 3,
                    "is_final": "1",
                    "selected_text": "Type something"
                }
            }
        });
        let parsed = parse_lark_card_action(&payload).expect("parsed");
        assert_eq!(parsed.input_text.as_deref(), Some("hello world"));
        assert_eq!(
            parsed.input_keys,
            Some(vec!["Down".to_string(), "Enter".to_string()])
        );
        assert_eq!(parsed.option_type.as_deref(), Some("input"));
        assert_eq!(parsed.selected_index, Some(3));
        assert!(parsed.is_final);
    }

    #[test]
    fn parse_special_keys_accepts_array_and_stringified_json() {
        assert_eq!(
            parse_special_keys(&serde_json::json!(["Down", "Enter"])),
            Some(vec!["Down".to_string(), "Enter".to_string()])
        );
        assert_eq!(
            parse_special_keys(&serde_json::json!("[\"Space\",\"Up\"]")),
            Some(vec!["Space".to_string(), "Up".to_string()])
        );
    }

    #[test]
    fn parse_term_action_key_maps_supported_values() {
        assert_eq!(parse_term_action_key("esc"), Some(TermActionKey::Esc));
        assert_eq!(parse_term_action_key("ctrlc"), Some(TermActionKey::CtrlC));
        assert_eq!(
            parse_term_action_key("half_page_up"),
            Some(TermActionKey::HalfPageUp)
        );
        assert_eq!(parse_term_action_key("unknown"), None);
    }

    #[test]
    fn classify_lark_text_action_identifies_all_commands() {
        assert_eq!(
            classify_lark_text_action("/close", false),
            LarkTextAction::Close
        );
        assert_eq!(
            classify_lark_text_action("/restart", false),
            LarkTextAction::Restart
        );
        assert_eq!(
            classify_lark_text_action("/card", false),
            LarkTextAction::Card
        );
        assert_eq!(
            classify_lark_text_action("/adopt", false),
            LarkTextAction::AdoptList
        );
        assert_eq!(
            classify_lark_text_action("/adopt list", false),
            LarkTextAction::AdoptList
        );
        assert_eq!(
            classify_lark_text_action("/adopt zellij mysession:0.1", false),
            LarkTextAction::AdoptZellij("zellij mysession:0.1".into())
        );
        assert_eq!(
            classify_lark_text_action("/adopt mysession:0.1", false),
            LarkTextAction::AdoptZellij("mysession:0.1".into())
        );
        assert_eq!(
            classify_lark_text_action("/adopt mysession", false),
            LarkTextAction::AdoptZellij("mysession".into())
        );
        assert_eq!(
            classify_lark_text_action("hello world", false),
            LarkTextAction::CreateSession
        );
        assert_eq!(
            classify_lark_text_action("hello world", true),
            LarkTextAction::ReuseSessionInput
        );
    }
}
