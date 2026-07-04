use super::*;

pub(crate) fn build_lark_card_action_toast(kind: &str, content: &str) -> Value {
    serde_json::json!({
        "toast": {
            "type": kind,
            "content": content,
        }
    })
}

pub(crate) fn build_tui_prompt_card(
    root_id: &str,
    session_id: &str,
    description: &str,
    options: &[TuiPromptOption],
    multi_select: bool,
    toggled_indices: &[usize],
    locale: Option<&str>,
) -> String {
    let toggled = toggled_indices
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let has_input_option = options
        .iter()
        .any(|option| option.option_type.as_deref() == Some("input"));
    let option_lines = options
        .iter()
        .enumerate()
        .filter(|(_, option)| option.option_type.as_deref() != Some("confirm"))
        .map(|(i, option)| {
            let label = option.label.clone().unwrap_or_else(|| (i + 1).to_string());
            match option.option_type.as_deref() {
                Some("toggle") => {
                    let check = if toggled.contains(&i) { "☑" } else { "☐" };
                    format!("{} {}. {}", check, label, option.text)
                }
                _ if option.selected => format!("**{}. {}**", label, option.text),
                _ => format!("{}. {}", label, option.text),
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let actions = options
        .iter()
        .enumerate()
        .filter(|(_, option)| option.option_type.as_deref() != Some("input"))
        .map(|(i, option)| {
            let option_type = option.option_type.clone().unwrap_or_else(|| "select".to_string());
            let label = if option_type == "confirm" {
                format!("✅ {}", option.text)
            } else {
                option.label.clone().unwrap_or_else(|| (i + 1).to_string())
            };
            serde_json::json!({
                "tag": "button",
                "text": { "tag": "plain_text", "content": label },
                "type": if option_type == "confirm" || option.selected { "primary" } else { "default" },
                "value": {
                    "action": "tui_keys",
                    "root_id": root_id,
                    "session_id": session_id,
                    "keys": option.keys,
                    "selected_text": option.text,
                    "multi_select": if multi_select { "1" } else { "0" },
                    "selected_index": i,
                    "option_type": option_type,
                    "is_final": if option_type == "select" || option_type == "confirm" { "1" } else { "0" },
                }
            })
        })
        .collect::<Vec<_>>();

    let mut elements = vec![
        serde_json::json!({
            "tag": "markdown",
            "content": option_lines,
            "i18n_content": {
                "zh_cn": option_lines.clone(),
                "en_us": option_lines,
            }
        }),
        serde_json::json!({ "tag": "hr" }),
        serde_json::json!({ "tag": "action", "actions": actions }),
    ];

    if has_input_option {
        let input_keys = options
            .iter()
            .find(|option| option.option_type.as_deref() == Some("input"))
            .map(|option| option.keys.clone())
            .unwrap_or_default();
        elements.push(serde_json::json!({ "tag": "hr" }));
        elements.push(serde_json::json!({
            "tag": "form",
            "name": "tui_input_form",
            "elements": [
                {
                    "tag": "input",
                    "name": "tui_custom_input",
                    "placeholder": card_i18n::plain_text(locale, "输入内容", "Type something")
                },
                {
                    "tag": "button",
                    "text": card_i18n::plain_text(locale, "发送自定义文本", "Send custom text"),
                    "type": "primary",
                    "name": "tui_input_submit",
                    "action_type": "form_submit",
                    "value": {
                        "action": "tui_text_input",
                        "root_id": root_id,
                        "session_id": session_id,
                        "input_keys": input_keys,
                    }
                }
            ]
        }));
    }

    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "title": card_i18n::plain_text(locale, description, description),
            "template": "orange"
        },
        "elements": elements
    })
    .to_string()
}

pub(crate) fn build_tui_prompt_processing_card(
    selected_text: Option<&str>,
    locale: Option<&str>,
) -> String {
    let content_zh = selected_text
        .filter(|text| !text.trim().is_empty())
        .map(|text| format!("{}: `{}`", "正在处理选择", text))
        .unwrap_or_else(|| "正在处理选择".to_string());
    let content_en = selected_text
        .filter(|text| !text.trim().is_empty())
        .map(|text| format!("{}: `{}`", "processing selection", text))
        .unwrap_or_else(|| "processing selection".to_string());
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "title": card_i18n::plain_text(locale, "处理中", "processing"),
            "template": "blue"
        },
        "elements": [
            { "tag": "markdown", "content": content_en, "i18n_content": { "zh_cn": content_zh, "en_us": content_en } }
        ]
    })
    .to_string()
}

pub(crate) fn build_tui_prompt_resolved_card(
    selected_text: Option<&str>,
    locale: Option<&str>,
) -> String {
    let content_zh = selected_text
        .filter(|text| !text.trim().is_empty())
        .map(|text| format!("{}: `{}`", "已应用选择", text))
        .unwrap_or_else(|| "提示已完成".to_string());
    let content_en = selected_text
        .filter(|text| !text.trim().is_empty())
        .map(|text| format!("{}: `{}`", "selection applied", text))
        .unwrap_or_else(|| "prompt resolved".to_string());
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "title": card_i18n::plain_text(locale, "已完成", "resolved"),
            "template": "green"
        },
        "elements": [
            { "tag": "markdown", "content": content_en, "i18n_content": { "zh_cn": content_zh, "en_us": content_en } }
        ]
    })
    .to_string()
}

pub(crate) fn build_workflow_approval_resolved_card(
    action: &str,
    run_id: &str,
    workflow_id: Option<&str>,
    revision_id: Option<&str>,
    node_id: &str,
    activity_id: &str,
    attempt_id: &str,
    operator_open_id: &str,
    comment: Option<&str>,
) -> String {
    let (title_zh, title_en, template, label_zh, label_en) = match action {
        "wf_approve" => ("已通过", "Approved", "green", "✅ 已通过", "✅ Approved"),
        "wf_reject" => ("已拒绝", "Rejected", "red", "❌ 已拒绝", "❌ Rejected"),
        "wf_cancel" => ("已取消", "Cancelled", "grey", "🛑 已取消", "🛑 Cancelled"),
        _ => ("workflow", "workflow", "blue", "Workflow", "Workflow"),
    };
    let workflow = workflow_id
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("{} @ {}", value, revision_id.unwrap_or("unknown")))
        .unwrap_or_else(|| format!("unknown @ {}", revision_id.unwrap_or("unknown")));
    let mut content_zh = vec![
        format!("**{}**", label_zh),
        format!("**Workflow**\n{}", workflow),
        format!("**Run**\n{}", run_id),
        format!("**Step**\n{}", node_id),
        format!("**Activity**\n{}", activity_id),
        format!("**Attempt**\n{}", attempt_id),
        format!("**操作人**\n{}", operator_open_id),
    ];
    let mut content_en = vec![
        format!("**{}**", label_en),
        format!("**Workflow**\n{}", workflow),
        format!("**Run**\n{}", run_id),
        format!("**Step**\n{}", node_id),
        format!("**Activity**\n{}", activity_id),
        format!("**Attempt**\n{}", attempt_id),
        format!("**Operator**\n{}", operator_open_id),
    ];
    if let Some(comment) = comment.filter(|value| !value.trim().is_empty()) {
        content_zh.push(format!("**备注**\n{}", comment));
        content_en.push(format!("**Comment**\n{}", comment));
    }
    serde_json::json!({
        "config": { "wide_screen_mode": true },
        "locales": card_i18n::card_locales(),
        "header": {
            "template": template,
            "title": card_i18n::plain_text(None, format!("{}：{}", title_zh, node_id), format!("{}: {}", title_en, node_id))
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": content_en.join("\n\n"),
                    "i18n_content": {
                        "zh_cn": content_zh.join("\n\n"),
                        "en_us": content_en.join("\n\n"),
                    }
                }
            }
        ]
    })
    .to_string()
}

pub(crate) fn workflow_approval_target_message_id(action: &ParsedLarkCardAction) -> Option<String> {
    action
        .clicked_message_id
        .as_ref()
        .or(action.root_id.as_ref())
        .cloned()
}

pub(crate) fn resolve_tui_prompt_final_text(
    session: &Session,
    selected_text: Option<&str>,
) -> String {
    if !session.tui_toggled_indices.is_empty() && !session.tui_prompt_options.is_empty() {
        let mut sorted = session.tui_toggled_indices.clone();
        sorted.sort_unstable();
        let toggled = sorted
            .into_iter()
            .filter_map(|index| session.tui_prompt_options.get(index))
            .map(|option| option.text.clone())
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join(", ");
        if !toggled.trim().is_empty() {
            return toggled;
        }
    }
    selected_text
        .filter(|text| !text.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "selection".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_final_output_card_uses_markdown_footer_shape() {
        let card: Value = serde_json::from_str(&build_final_output_card(
            "done",
            Some("ou_owner"),
            None,
            None,
            None,
        ))
        .expect("valid card json");
        assert_eq!(card.pointer("/schema").and_then(Value::as_str), Some("2.0"));
        assert_eq!(
            card.pointer("/body/elements/0/content")
                .and_then(Value::as_str),
            Some("done")
        );
        assert_eq!(
            card.pointer("/body/elements/2/content")
                .and_then(Value::as_str),
            Some(
                "<font color='grey'>[beam](https://github.com/deepcoldy/beam) · 发送给：<at id=ou_owner></at></font>"
            )
        );
    }

    #[test]
    fn build_final_output_card_supports_local_turn_variants() {
        let local_turn: Value = serde_json::from_str(&build_final_output_card(
            "assistant body",
            Some("ou_owner"),
            Some(FinalOutputKind::LocalTurn),
            Some("user prompt"),
            Some("Claude"),
        ))
        .expect("local turn card");
        assert_eq!(
            local_turn
                .pointer("/body/elements/0/content")
                .and_then(Value::as_str),
            Some(
                "🖥️ Local terminal conversation (type directly in the adopted pane; synced to Feishu)"
            )
        );
        assert_eq!(
            local_turn
                .pointer("/body/elements/0/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("🖥️ 终端本地对话（在 adopted pane 中直接输入，已同步至飞书）")
        );
        assert_eq!(
            local_turn
                .pointer("/body/elements/1/content")
                .and_then(Value::as_str),
            Some("**👤 You**\n\n> user prompt")
        );
        assert_eq!(
            local_turn
                .pointer("/body/elements/1/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("**👤 你**\n\n> user prompt")
        );
        assert_eq!(
            local_turn
                .pointer("/body/elements/3/content")
                .and_then(Value::as_str),
            Some("**🤖 Claude**")
        );

        let headless: Value = serde_json::from_str(&build_final_output_card(
            "assistant body",
            None,
            Some(FinalOutputKind::LocalTurnHeadless),
            None,
            Some("Codex"),
        ))
        .expect("headless card");
        assert_eq!(
            headless
                .pointer("/body/elements/0/content")
                .and_then(Value::as_str),
            Some(
                "🖥️ Local terminal conversation resumed (model was still streaming when daemon restarted)"
            )
        );
        assert_eq!(
            headless
                .pointer("/body/elements/0/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("🖥️ 终端本地对话续传（daemon 重启时模型正在输出）")
        );
        assert_eq!(
            headless
                .pointer("/body/elements/2/content")
                .and_then(Value::as_str),
            Some("**🤖 Codex**")
        );
    }

    #[test]
    fn build_contextual_reply_card_supports_adopt_preamble_shape() {
        let card: Value = serde_json::from_str(&build_contextual_reply_card(
            "📜 /adopt 前最后一轮",
            "📜 Last turn before /adopt",
            Some("previous user"),
            "previous assistant",
            "Claude",
            "Claude",
            Some("ou_owner"),
        ))
        .expect("contextual card");
        assert_eq!(
            card.pointer("/body/elements/0/content")
                .and_then(Value::as_str),
            Some("📜 Last turn before /adopt")
        );
        assert_eq!(
            card.pointer("/body/elements/0/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("📜 /adopt 前最后一轮")
        );
        assert_eq!(
            card.pointer("/body/elements/1/content")
                .and_then(Value::as_str),
            Some("**👤 You**\n\n> previous user")
        );
        assert_eq!(
            card.pointer("/body/elements/1/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("**👤 你**\n\n> previous user")
        );
        assert_eq!(
            card.pointer("/body/elements/3/content")
                .and_then(Value::as_str),
            Some("**🤖 Claude**")
        );
    }

    #[test]
    fn build_workflow_approval_resolved_card_includes_resolution_banner() {
        let card: Value = serde_json::from_str(&build_workflow_approval_resolved_card(
            "wf_reject",
            "run-1",
            Some("flow-a"),
            Some("rev-9"),
            "node-1",
            "act-1",
            "att-1",
            "ou_user",
            Some("not ready"),
        ))
        .expect("valid workflow card json");
        assert_eq!(
            card.pointer("/header/title/content")
                .and_then(Value::as_str),
            Some("Rejected: node-1")
        );
        assert_eq!(
            card.pointer("/header/title/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some("已拒绝：node-1")
        );
        assert_eq!(
            card.pointer("/elements/0/text/content")
                .and_then(Value::as_str),
            Some(
                "**❌ Rejected**\n\n**Workflow**\nflow-a @ rev-9\n\n**Run**\nrun-1\n\n**Step**\nnode-1\n\n**Activity**\nact-1\n\n**Attempt**\natt-1\n\n**Operator**\nou_user\n\n**Comment**\nnot ready"
            )
        );
        assert_eq!(
            card.pointer("/elements/0/text/i18n_content/zh_cn")
                .and_then(Value::as_str),
            Some(
                "**❌ 已拒绝**\n\n**Workflow**\nflow-a @ rev-9\n\n**Run**\nrun-1\n\n**Step**\nnode-1\n\n**Activity**\nact-1\n\n**Attempt**\natt-1\n\n**操作人**\nou_user\n\n**备注**\nnot ready"
            )
        );
    }

    #[test]
    fn build_lark_card_action_toast_shapes_expected_payload() {
        let toast = build_lark_card_action_toast("success", "session resumed");
        assert_eq!(
            toast.pointer("/toast/type").and_then(Value::as_str),
            Some("success")
        );
        assert_eq!(
            toast.pointer("/toast/content").and_then(Value::as_str),
            Some("session resumed")
        );
    }

    #[test]
    fn build_tui_prompt_card_embeds_tui_keys_actions() {
        let card: Value = serde_json::from_str(&build_tui_prompt_card(
            "root",
            "session",
            "pick one",
            &[TuiPromptOption {
                label: Some("1".to_string()),
                text: "alpha".to_string(),
                selected: false,
                option_type: Some("select".to_string()),
                keys: vec!["Enter".to_string()],
            }],
            false,
            &[],
            None,
        ))
        .expect("valid card json");
        assert_eq!(
            card.pointer("/header/title/content")
                .and_then(Value::as_str),
            Some("pick one")
        );
        assert_eq!(
            card.pointer("/elements/2/actions/0/value/action")
                .and_then(Value::as_str),
            Some("tui_keys")
        );
        assert_eq!(
            card.pointer("/elements/2/actions/0/value/keys/0")
                .and_then(Value::as_str),
            Some("Enter")
        );
        assert_eq!(
            card.pointer("/elements/2/actions/0/value/is_final")
                .and_then(Value::as_str),
            Some("1")
        );
    }

    #[test]
    fn build_tui_prompt_card_includes_text_input_form_when_input_option_present() {
        let card: Value = serde_json::from_str(&build_tui_prompt_card(
            "root",
            "session",
            "type something",
            &[TuiPromptOption {
                label: Some("I".to_string()),
                text: "Type something".to_string(),
                selected: false,
                option_type: Some("input".to_string()),
                keys: vec!["Down".to_string(), "Enter".to_string()],
            }],
            false,
            &[],
            None,
        ))
        .expect("valid card json");
        assert_eq!(
            card.pointer("/elements/4/elements/1/value/action")
                .and_then(Value::as_str),
            Some("tui_text_input")
        );
        assert_eq!(
            card.pointer("/elements/4/elements/1/value/input_keys/1")
                .and_then(Value::as_str),
            Some("Enter")
        );
    }
}
