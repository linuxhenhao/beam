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

pub fn infer_prompt_locale(text: &str) -> &'static str {
    let mut cjk_count = 0usize;
    let mut significant_count = 0usize;

    for ch in text.chars() {
        if ch.is_whitespace() || ch.is_control() {
            continue;
        }
        significant_count += 1;
        if matches!(
            ch,
            '\u{3400}'..='\u{4dbf}'
                | '\u{4e00}'..='\u{9fff}'
                | '\u{f900}'..='\u{faff}'
                | '\u{3000}'..='\u{303f}'
        ) {
            cjk_count += 1;
        }
    }

    if cjk_count >= 2 || (significant_count > 0 && cjk_count * 5 >= significant_count) {
        "zh"
    } else {
        "en"
    }
}

fn localized(locale: Option<&str>, zh: &str, en: &str) -> String {
    if is_zh_locale(locale) {
        zh.to_string()
    } else {
        en.to_string()
    }
}

fn beam_shell_must_rules(locale: Option<&str>) -> Vec<String> {
    vec![
        localized(
            locale,
            "你正在飞书中与用户对话。普通终端输出、echo、JSON 只对你自己可见；需要让用户看到的内容必须用 `beam send` 发回飞书。",
            "You are talking with the user in Feishu. Normal terminal output, echo, and JSON are only visible to you; anything the user should see must be sent back with `beam send`.",
        ),
        localized(
            locale,
            "beam 是一条 SHELL 命令，不是 MCP 工具。不要假设你能调用 MCP beam，直接执行 shell 命令即可。",
            "Beam is a SHELL command, not an MCP tool. Do not assume you can call an MCP beam tool; run the shell command directly.",
        ),
        localized(
            locale,
            "回复用户时使用 `beam send`，并在每条回复里显式选择 @ 策略：`--mention-back`、`--mention <open_id[:name]>` 或 `--no-mention`。",
            "When replying to the user, use `beam send` and explicitly choose an attention policy on every reply: `--mention-back`, `--mention <open_id[:name]>`, or `--no-mention`.",
        ),
        localized(
            locale,
            "`--no-mention` 不能和 `--mention` / `--mention-back` 同用；如果 `--mention-back` 无法使用，改用 `--mention <open_id[:name]>` 或 `--no-mention`。",
            "`--no-mention` cannot be combined with `--mention` / `--mention-back`; if `--mention-back` is unavailable, use `--mention <open_id[:name]>` or `--no-mention`.",
        ),
    ]
}

fn beam_shell_usage_rules(locale: Option<&str>) -> Vec<String> {
    vec![
        localized(
            locale,
            "发送策略：有实质结论、完成修改、需要用户确认/决策、遇到阻塞时，用 `--mention-back`；纯记录、低优先级进度、无需立即查看时，用 `--no-mention`；点名某人或其他 bot 时，用 `--mention <open_id[:name]>`。",
            "Send policy: use `--mention-back` for substantive conclusions, completed changes, requests for confirmation/decision, or blockers; use `--no-mention` for low-priority progress or notes; use `--mention <open_id[:name]>` to address a specific person or bot.",
        ),
        localized(
            locale,
            "多行回复请用 heredoc，并把 @ 策略放在 `beam send` 后面：\n```sh\nbeam send --mention-back <<'EOF'\n<多行回复内容>\nEOF\n```",
            "For multiline replies, use heredoc and put the attention policy after `beam send`:\n```sh\nbeam send --mention-back <<'EOF'\n<multiline reply>\nEOF\n```",
        ),
        localized(
            locale,
            "示例：\n```sh\nbeam send --mention-back <<'EOF'\n我已经定位到问题并完成修改：\n1. 修复了发送路径...\n2. 验证了相关测试...\nEOF\n```",
            "Example:\n```sh\nbeam send --mention-back <<'EOF'\nI found the issue and completed the change:\n1. Fixed the send path...\n2. Verified the relevant tests...\nEOF\n```",
        ),
    ]
}

fn beam_shell_support_rules(locale: Option<&str>) -> Vec<String> {
    vec![
        localized(
            locale,
            "常用命令：\n- `beam history` 查看对话历史\n- `beam quoted <id>` 查看被引用的消息\n- `beam bots list` 查看飞书内可用 bot\n- `beam send --content-file <path> --mention-back` 从文件读取正文\n- `beam send --files <path> --mention-back` 发送附件\n- `beam send --images <path> --mention-back` 发送图片",
            "Helpful commands:\n- `beam history` to inspect conversation history\n- `beam quoted <id>` to inspect the quoted message\n- `beam bots list` to list available bots in Feishu\n- `beam send --content-file <path> --mention-back` to read the message body from a file\n- `beam send --files <path> --mention-back` to send attachments\n- `beam send --images <path> --mention-back` to send images",
        ),
        localized(
            locale,
            "需要发到其他位置时可用：`--top-level` 发顶层消息，`--chat-id <oc_xxx>` 指定群，`--into <message_id>` 发进指定话题，`--quote <message_id>` / `--no-quote` 控制普通群引用。",
            "For alternate destinations, use `--top-level` for a top-level message, `--chat-id <oc_xxx>` for a target chat, `--into <message_id>` for a specific topic, and `--quote <message_id>` / `--no-quote` to control chat-scope quoting.",
        ),
    ]
}

pub fn build_beam_shell_hints(locale: Option<&str>) -> Vec<String> {
    let mut hints = Vec::new();
    hints.extend(beam_shell_must_rules(locale));
    hints.extend(beam_shell_usage_rules(locale));
    hints.extend(beam_shell_support_rules(locale));
    hints
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
    let locale = opts
        .locale
        .unwrap_or_else(|| infer_prompt_locale(opts.user_message));

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

    let hints = build_beam_shell_hints(Some(locale));
    blocks.push(format!(
        "<beam_routing>\n{}\n</beam_routing>",
        hints.join("\n")
    ));

    let identity_routing_rules = localized(
        Some(locale),
        "`beam send --mention` 必须指定 `open_id[:name]`；回复当前触发者优先用 `--mention-back`",
        "`beam send --mention` requires `open_id[:name]`; prefer `--mention-back` when replying to the current sender",
    );
    if let (Some(name), Some(open_id)) = (opts.bot_name, opts.bot_open_id) {
        blocks.push(format!(
            "<identity>\n  <name>{}</name>\n  <open_id>{}</open_id>\n  <routing_rules>{}</routing_rules>\n</identity>",
            xml_escape(name),
            xml_escape(open_id),
            xml_escape(&identity_routing_rules)
        ));
    } else if let Some(name) = opts.bot_name {
        blocks.push(format!(
            "<identity>\n  <name>{}</name>\n  <routing_rules>{}</routing_rules>\n</identity>",
            xml_escape(name),
            xml_escape(&identity_routing_rules)
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
            let hint = localized(
                Some(locale),
                "你可以用 `beam send --mention <bot_open_id[:name]>` 点名飞书里的其他 bot",
                "Use `beam send --mention <bot_open_id[:name]>` to address another bot in Feishu",
            );
            blocks.push(format!(
                "<available_bots hint=\"{}\">\n{}\n</available_bots>",
                hint,
                unmentioned.join("\n")
            ));
        }
    }

    blocks.join("\n\n")
}

pub struct AdoptContextOptions<'a> {
    pub bot_name: Option<&'a str>,
    pub bot_open_id: Option<&'a str>,
    pub observed_bots: &'a [ObservedBot],
    pub locale: Option<&'a str>,
}

/// Build beam context to prepend to the first message of an adopted session.
/// This gives the LLM the same routing / identity hints that a normal new
/// session would receive, but without a <user_message> block (the follow-up
/// wrapper already provides one).
pub fn build_adopt_context(opts: &AdoptContextOptions) -> String {
    let mut blocks = Vec::new();
    let locale = opts.locale.unwrap_or("en");

    let hints = build_beam_shell_hints(Some(locale));
    blocks.push(format!(
        "<beam_routing>\n{}\n</beam_routing>",
        hints.join("\n")
    ));

    let identity_routing_rules = localized(
        Some(locale),
        "`beam send --mention` 必须指定 `open_id[:name]`；回复当前触发者优先用 `--mention-back`",
        "`beam send --mention` requires `open_id[:name]`; prefer `--mention-back` when replying to the current sender",
    );
    if let (Some(name), Some(open_id)) = (opts.bot_name, opts.bot_open_id) {
        blocks.push(format!(
            "<identity>\n  <name>{}</name>\n  <open_id>{}</open_id>\n  <routing_rules>{}</routing_rules>\n</identity>",
            xml_escape(name),
            xml_escape(open_id),
            xml_escape(&identity_routing_rules)
        ));
    } else if let Some(name) = opts.bot_name {
        blocks.push(format!(
            "<identity>\n  <name>{}</name>\n  <routing_rules>{}</routing_rules>\n</identity>",
            xml_escape(name),
            xml_escape(&identity_routing_rules)
        ));
    }

    if !opts.observed_bots.is_empty() {
        let tags: Vec<String> = opts
            .observed_bots
            .iter()
            .map(|b| {
                format!(
                    r#"<bot name="{}" open_id="{}" />"#,
                    xml_escape(&b.name),
                    xml_escape(&b.open_id)
                )
            })
            .collect();
        let hint = localized(
            Some(locale),
            "你可以用 `beam send --mention <bot_open_id[:name]>` 点名飞书里的其他 bot",
            "Use `beam send --mention <bot_open_id[:name]>` to address another bot in Feishu",
        );
        blocks.push(format!(
            "<available_bots hint=\"{}\">\n{}\n</available_bots>",
            hint,
            tags.join("\n")
        ));
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

const FOLLOW_UP_CONTRACTS_ZH: [&str; 10] = [
    "这条飞书消息需要用户可见回应时，最终必须执行 `beam send --mention-back`；终端输出不算回复。低优先级记录才用 `--no-mention`，点名他人用 `--mention <open_id[:name]>`。",
    "不要把普通终端输出当作回复。只要要让飞书用户看到结论、进度、问题或阻塞，就用 `beam send --mention-back`；无需打扰时才改 `--no-mention`，定向通知用 `--mention <open_id[:name]>`。",
    "本轮如果产生任何给用户看的内容，结束前必须用 shell 命令 `beam send --mention-back` 发回飞书。不要只在终端写最终答案；安静记录用 `--no-mention`，点名用 `--mention <open_id[:name]>`。",
    "用户看不到你的终端最终回答。需要回复、确认、汇报完成或说明阻塞时，必须执行 `beam send --mention-back`；仅普通记录用 `--no-mention`，需要找别人用 `--mention <open_id[:name]>`。",
    "处理完这条飞书输入后，凡是用户应该看到的信息都必须通过 `beam send --mention-back` 发出。不要依赖自然语言输出；低优先级更新用 `--no-mention`，指定对象用 `--mention <open_id[:name]>`。",
    "请把 `beam send` 当作唯一对飞书用户可见的回复通道。需要回应时默认 `beam send --mention-back`；安静日志才用 `--no-mention`，点名人或 bot 时用 `--mention <open_id[:name]>`。",
    "如果本轮需要告诉用户任何结果、问题、计划变化或等待事项，必须用 `beam send --mention-back`。终端文本不会送达飞书；不需提醒才用 `--no-mention`，定向提醒用 `--mention <open_id[:name]>`。",
    "不要只输出最终答复给自己看。给飞书用户的可见回复必须走 `beam send --mention-back`；纯记录可选 `--no-mention`，需要让特定人或 bot 收到提醒时用 `--mention <open_id[:name]>`。",
    "这是一条飞书会话输入：有用户可见结论时，最后必须执行 `beam send --mention-back`。终端 echo/JSON/自然语言都不是送达；安静更新用 `--no-mention`，点名用 `--mention <open_id[:name]>`。",
    "本轮回复的交付标准是飞书收到消息。默认用 `beam send --mention-back`；不要把终端输出当交付。低优先级记录用 `--no-mention`，要通知指定对象用 `--mention <open_id[:name]>`。",
];

const FOLLOW_UP_CONTRACTS_EN: [&str; 10] = [
    "If this Feishu turn needs a user-visible response, you must finish by running `beam send --mention-back`. Terminal output is not a reply. Use `--no-mention` only for quiet notes, and `--mention <open_id[:name]>` for a specific target.",
    "Do not treat normal terminal output as the answer. Any conclusion, progress, question, or blocker the user should see must go through `beam send --mention-back`; use `--no-mention` for low-priority notes or `--mention <open_id[:name]>` for a target.",
    "Before ending this turn, send any user-facing content with the shell command `beam send --mention-back`. A plain final answer in the terminal will not reach Feishu; quiet updates use `--no-mention`, targeted updates use `--mention <open_id[:name]>`.",
    "The Feishu user cannot see your terminal-only final text. When replying, confirming, reporting completion, or explaining a blocker, run `beam send --mention-back`; use `--no-mention` only for quiet notes and `--mention <open_id[:name]>` for a target.",
    "For this Feishu input, anything the user should see must be delivered with `beam send --mention-back`. Do not rely on natural-language terminal output; use `--no-mention` for quiet records and `--mention <open_id[:name]>` for a specific person or bot.",
    "Treat `beam send` as the only user-visible Feishu reply channel. Default to `beam send --mention-back`; choose `--no-mention` only for quiet logs, or `--mention <open_id[:name]>` when addressing a person or bot.",
    "If this turn produces a result, question, plan change, or wait state for the user, you must run `beam send --mention-back`. Terminal text is not delivered; use `--no-mention` only when no alert is needed, or `--mention <open_id[:name]>` for a target.",
    "Do not leave the final response only in the terminal. A visible Feishu reply must use `beam send --mention-back`; use `--no-mention` for quiet records, and `--mention <open_id[:name]>` when a specific person or bot should be notified.",
    "This is a Feishu session input: when there is a user-visible conclusion, run `beam send --mention-back` before stopping. Echo, JSON, or plain terminal prose is not delivery; quiet updates use `--no-mention`, targeted ones use `--mention <open_id[:name]>`.",
    "This turn is not complete until Feishu receives the user-facing message. Default to `beam send --mention-back`; terminal output is not delivery. Use `--no-mention` for low-priority notes and `--mention <open_id[:name]>` for a specific target.",
];

fn follow_up_contract_templates(locale: Option<&str>) -> &'static [&'static str] {
    if is_zh_locale(locale) {
        &FOLLOW_UP_CONTRACTS_ZH
    } else {
        &FOLLOW_UP_CONTRACTS_EN
    }
}

fn random_follow_up_contract(locale: Option<&str>) -> &'static str {
    let templates = follow_up_contract_templates(locale);
    let index = usize::from(uuid::Uuid::new_v4().as_bytes()[0]) % templates.len();
    templates[index]
}

pub fn build_follow_up_content(content: &str, opts: &FollowUpContentOptions) -> String {
    let mut blocks = Vec::new();
    let locale = opts.locale.unwrap_or_else(|| infer_prompt_locale(content));

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
        blocks.push(format!(
            "<beam_response_contract>{}</beam_response_contract>",
            random_follow_up_contract(Some(locale))
        ));
    }

    blocks.join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_prompt_locale_detects_chinese_text() {
        assert_eq!(infer_prompt_locale("请帮我检查这个改动"), "zh");
    }

    #[test]
    fn infer_prompt_locale_defaults_to_english_for_ascii_text() {
        assert_eq!(infer_prompt_locale("Please review this change"), "en");
    }

    #[test]
    fn infer_prompt_locale_defaults_to_english_for_sparse_mixed_text() {
        assert_eq!(infer_prompt_locale("Please fix 这 bug"), "en");
    }

    #[test]
    fn build_beam_shell_hints_restore_explicit_send_constraints() {
        let hints = build_beam_shell_hints(Some("en"));
        assert!(
            hints
                .iter()
                .any(|line| line.contains("Beam is a SHELL command"))
        );
        assert!(
            hints.iter().any(|line| line.contains("--mention-back"))
                && hints.iter().any(|line| line.contains("--no-mention"))
        );
        assert!(
            hints
                .first()
                .unwrap()
                .contains("You are talking with the user in Feishu")
        );
        assert!(hints.iter().any(|line| line.contains("--content-file")));
        assert!(hints.iter().any(|line| line.contains("--top-level")));
    }

    #[test]
    fn build_initial_prompt_infers_locale_from_first_message() {
        let prompt = build_initial_prompt(&InitialPromptOptions {
            user_message: "请帮我检查这个改动",
            session_id: "session-1",
            sender_open_id: None,
            sender_type: None,
            mentions: &[],
            bot_name: None,
            bot_open_id: None,
            observed_bots: &[],
            follow_ups: &[],
            locale: None,
        });
        assert!(prompt.contains("发送策略：有实质结论"));
        assert!(prompt.contains("beam send --mention-back <<'EOF'"));
    }

    #[test]
    fn build_follow_up_content_infers_locale_from_content() {
        let prompt = build_follow_up_content(
            "请继续处理这个任务",
            &FollowUpContentOptions {
                session_id: "session-1",
                sender_open_id: None,
                sender_type: None,
                mentions: &[],
                cli_id: "codex",
                locale: None,
            },
        );
        assert!(prompt.contains("<beam_response_contract>"));
        assert!(prompt.contains("</beam_response_contract>"));
        assert!(prompt.contains("beam send --mention-back"));
        assert!(prompt.contains("--no-mention"));
    }

    #[test]
    fn follow_up_contract_templates_stay_short_and_complete() {
        for templates in [
            FOLLOW_UP_CONTRACTS_ZH.as_slice(),
            FOLLOW_UP_CONTRACTS_EN.as_slice(),
        ] {
            assert_eq!(templates.len(), 10);
            for template in templates {
                assert!(
                    template.chars().count() <= 260,
                    "template too long: {template}"
                );
                assert!(template.contains("beam send --mention-back"));
                assert!(template.contains("--no-mention"));
                assert!(template.contains("--mention <open_id[:name]>"));
            }
        }
    }

    #[test]
    fn build_initial_prompt_uses_english_identity_rules_for_english_locale() {
        let prompt = build_initial_prompt(&InitialPromptOptions {
            user_message: "Please review this change",
            session_id: "session-1",
            sender_open_id: None,
            sender_type: None,
            mentions: &[],
            bot_name: Some("Beam"),
            bot_open_id: Some("ou_123"),
            observed_bots: &[],
            follow_ups: &[],
            locale: None,
        });
        assert!(prompt.contains("beam send --mention` requires `open_id[:name]"));
    }
}
