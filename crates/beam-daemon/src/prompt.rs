use crate::LarkEventMention;
use beam_core::SessionScope;

pub struct ObservedBot {
    pub open_id: String,
    pub name: String,
}

pub fn is_zh_locale(locale: Option<&str>) -> bool {
    locale
        .map(|value| {
            let normalized = value.to_ascii_lowercase().replace('-', "_");
            normalized == "zh" || normalized.starts_with("zh_")
        })
        .unwrap_or(false)
}

pub fn build_beam_shell_hints(locale: Option<&str>) -> Vec<String> {
    if is_zh_locale(locale) {
        return vec![
            "你正在飞书话题中通过 Beam 与用户协作；用户消息会转发给你，但终端输出不会自动发给用户。".to_string(),
            "回复用户必须执行 `beam send`，并明确选择 `--mention-back`、`--mention <open_id>` 或 `--no-mention`。".to_string(),
            "常用回复格式：\n```sh\nbeam send --mention-back <<'EOF'\n<回复内容>\nEOF\n```".to_string(),
            "得出结论、完成修改、需要确认、遇到阻塞时，主动发送消息。".to_string(),
            "辅助命令：`beam history`、`beam quoted <id>`、`beam bots list`、`beam send --file <path>`。".to_string(),
        ];
    }
    vec![
        "You are collaborating with the user in a Feishu/Lark thread through Beam. User messages are forwarded to you, but terminal output is not sent to the user automatically.".to_string(),
        "To reply, run `beam send` and explicitly choose `--mention-back`, `--mention <open_id>`, or `--no-mention`.".to_string(),
        "Common reply format:\n```sh\nbeam send --mention-back <<'EOF'\n<reply content>\nEOF\n```".to_string(),
        "Send a message when you reach a conclusion, finish changes, need confirmation, or hit a blocker.".to_string(),
        "Helpful commands: `beam history`, `beam quoted <id>`, `beam bots list`, `beam send --file <path>`.".to_string(),
    ]
}

pub struct InitialPromptOptions<'a> {
    pub user_message: &'a str,
    pub session_id: &'a str,
    pub sender_open_id: Option<&'a str>,
    pub sender_type: Option<&'a str>,
    pub mentions: &'a [LarkEventMention],
    pub bot_name: Option<&'a str>,
    pub bot_open_id: Option<&'a str>,
    pub observed_bots: &'a [ObservedBot],
    pub follow_ups: &'a [String],
    pub locale: Option<&'a str>,
}

pub fn build_initial_prompt(opts: &InitialPromptOptions) -> String {
    let mut blocks = Vec::new();

    let merged = if !opts.follow_ups.is_empty() {
        format!("{}\n\n{}", opts.user_message, opts.follow_ups.join("\n\n"))
    } else {
        opts.user_message.to_string()
    };
    blocks.push(format!("<user_message>\n{}\n</user_message>", merged));

    if let Some(open_id) = opts.sender_open_id {
        let stype = opts.sender_type.unwrap_or("user");
        blocks.push(format!(
            r#"<sender type="{}" open_id="{}" />"#,
            xml_escape(stype),
            xml_escape(open_id)
        ));
    }

    blocks.push(format!(
        "<session_id>{}</session_id>",
        xml_escape(opts.session_id)
    ));

    let hints = build_beam_shell_hints(opts.locale);
    blocks.push(format!(
        "<beam_routing>\n{}\n</beam_routing>",
        hints.join("\n")
    ));

    if let (Some(name), Some(open_id)) = (opts.bot_name, opts.bot_open_id) {
        blocks.push(format!(
            "<identity>\n  <name>{}</name>\n  <open_id>{}</open_id>\n  <routing_rules>beam send --mention 必须指定目标用户</routing_rules>\n</identity>",
            xml_escape(name),
            xml_escape(open_id)
        ));
    } else if let Some(name) = opts.bot_name {
        blocks.push(format!(
            "<identity>\n  <name>{}</name>\n  <routing_rules>beam send --mention 必须指定目标用户</routing_rules>\n</identity>",
            xml_escape(name)
        ));
    }

    if !opts.mentions.is_empty() {
        let mention_tags: Vec<String> = opts
            .mentions
            .iter()
            .map(|m| {
                format!(
                    r#"<mention name="{}" open_id="{}" />"#,
                    xml_escape(&m.name),
                    xml_escape(&m.key)
                )
            })
            .collect();
        blocks.push(format!(
            "<mentions>\n{}\n</mentions>",
            mention_tags.join("\n")
        ));
    }

    if !opts.observed_bots.is_empty() {
        let mentioned_ids: std::collections::HashSet<&str> =
            opts.mentions.iter().map(|m| m.key.as_str()).collect();
        let unmentioned: Vec<String> = opts
            .observed_bots
            .iter()
            .filter(|b| !mentioned_ids.contains(b.open_id.as_str()))
            .map(|b| {
                format!(
                    r#"<bot name="{}" open_id="{}" />"#,
                    xml_escape(&b.name),
                    xml_escape(&b.open_id)
                )
            })
            .collect();
        if !unmentioned.is_empty() {
            let hint = if is_zh_locale(opts.locale) {
                "你可以用 beam send --mention <bot_id> 让群里其他 bot 帮你"
            } else {
                "Use beam send --mention <bot_id> to ask another bot in the chat for help"
            };
            blocks.push(format!(
                "<available_bots hint=\"{}\">\n{}\n</available_bots>",
                hint,
                unmentioned.join("\n")
            ));
        }
    }

    blocks.join("\n\n")
}

pub fn build_quote_hint(
    parent_id: Option<&str>,
    message_id: &str,
    scope: SessionScope,
    anchor: &str,
) -> String {
    let Some(quoted_id) = parent_id else {
        return String::new();
    };
    if quoted_id.is_empty() {
        return String::new();
    }
    if quoted_id == message_id {
        return String::new();
    }
    if scope == SessionScope::Thread && quoted_id == anchor {
        return String::new();
    }
    format!("[用户引用了消息 用 beam quoted {} 查看]\n", quoted_id)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub struct FollowUpContentOptions<'a> {
    pub session_id: &'a str,
    pub sender_open_id: Option<&'a str>,
    pub sender_type: Option<&'a str>,
    pub mentions: &'a [LarkEventMention],
    pub cli_id: &'a str,
    pub locale: Option<&'a str>,
}

pub fn build_follow_up_content(content: &str, opts: &FollowUpContentOptions) -> String {
    let mut blocks = Vec::new();

    blocks.push(format!("<user_message>\n{}\n</user_message>", content));

    if let Some(open_id) = opts.sender_open_id {
        let stype = opts.sender_type.unwrap_or("user");
        blocks.push(format!(
            r#"<sender type="{}" open_id="{}" />"#,
            xml_escape(stype),
            xml_escape(open_id)
        ));
    }

    blocks.push(format!(
        "<session_id>{}</session_id>",
        xml_escape(opts.session_id)
    ));

    if !opts.mentions.is_empty() {
        let mention_tags: Vec<String> = opts
            .mentions
            .iter()
            .map(|m| {
                format!(
                    r#"<mention name="{}" open_id="{}" />"#,
                    xml_escape(&m.name),
                    xml_escape(&m.key)
                )
            })
            .collect();
        blocks.push(format!(
            "<mentions>\n{}\n</mentions>",
            mention_tags.join("\n")
        ));
    }

    if opts.cli_id != "mira" {
        let reminder = if is_zh_locale(opts.locale) {
            "如果这条消息改变了计划，请继续处理；如果需要回复用户，请使用 beam send。"
        } else {
            "If this message changes the plan, continue working. If you need to reply to the user, use beam send."
        };
        blocks.push(format!("<beam_reminder>{}</beam_reminder>", reminder));
    }

    blocks.join("\n\n")
}
