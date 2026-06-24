use crate::LarkEventMention;
use beam_core::SessionScope;

pub struct ObservedBot {
    pub open_id: String,
    pub name: String,
}

pub fn build_beam_shell_hints() -> Vec<String> {
    vec![
        "你运行在飞书话题群中。发出的消息会被 beam 转发给用户，用户回复也会回传给你。".to_string(),
        "beam 是一条 SHELL 命令，不是 MCP 工具。不要假设你能调用 MCP beam，直接执行 shell 命令即可。".to_string(),
        "使用 `beam send <消息>` 回复用户。回显文本和 JSON 只对你自己可见，必须用 beam send 才能发到群里。".to_string(),
        "多行消息请用 heredoc：\n```sh\nbeam send <<'EOF'\n<多行内容>\nEOF\n```".to_string(),
        "示例：\n```sh\nbeam send <<'EOF'\n好的，这个问题我这样解决：\n1. 先检查配置...\n2. 再修改代码...\nEOF\n```".to_string(),
        "辅助命令：\n- `beam history` 查看对话历史\n- `beam quoted <id>` 查看被引用的消息\n- `beam bots list` 查看群内可用 bot\n- `beam send --file <path>` 发送文件".to_string(),
        "以下情况请主动发送消息：得出结论或方案时、完成代码修改时、需要用户确认或选择时、遇到需要用户介入的问题时。".to_string(),
        "beam send 必须带以下参数之一：--mention 提及发送者、--mention-back 引用并提及、--no-mention 静默发送。明确选择一个，不要省略。".to_string(),
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

    let hints = build_beam_shell_hints();
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
            blocks.push(format!(
                "<available_bots hint=\"你可以用 beam send --mention <bot_id> 让群里其他 bot 帮你\">\n{}\n</available_bots>",
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
        blocks
            .push("<beam_reminder>你可以继续对话，或使用 Ctrl+C 返回</beam_reminder>".to_string());
    }

    blocks.join("\n\n")
}
