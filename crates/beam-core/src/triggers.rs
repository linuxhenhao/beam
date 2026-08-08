use crate::config::CustomTrigger;

/// Returns the first configured trigger whose keyword starts the trimmed
/// text, respecting a word boundary after the keyword.
pub fn resolve_custom_trigger<'a>(
    text: &str,
    triggers: &'a [CustomTrigger],
) -> Option<&'a CustomTrigger> {
    let text = text.trim_start();
    triggers.iter().find(|entry| {
        let keyword = entry.trigger.trim();
        if keyword.is_empty() {
            return false;
        }
        text.strip_prefix(keyword)
            .map(is_trigger_boundary)
            .unwrap_or(false)
    })
}

/// Text that follows the trigger keyword, with leading separators removed.
/// Returns `None` when the message is exactly the keyword.
pub fn custom_trigger_rest<'a>(text: &'a str, trigger: &str) -> Option<&'a str> {
    let text = text.trim_start();
    let keyword = trigger.trim();
    if keyword.is_empty() {
        return None;
    }
    let rest = text.strip_prefix(keyword)?;
    let rest = rest.trim_start_matches(|c: char| {
        c.is_whitespace() || "，。：:；;、,．.！!？?）)]}>》」』】".contains(c)
    });
    if rest.is_empty() { None } else { Some(rest) }
}

/// The effective initial message for a session created by a trigger:
/// the configured prompt plus any trailing user text, or the raw text
/// when the trigger has no prompt.
pub fn resolve_trigger_message(text: &str, trigger: &CustomTrigger) -> String {
    match trigger.prompt.as_deref() {
        None => text.to_string(),
        Some(prompt) => match custom_trigger_rest(text, &trigger.trigger) {
            Some(rest) => format!("{}\n\n{}", prompt, rest),
            None => prompt.to_string(),
        },
    }
}

fn is_trigger_boundary(rest: &str) -> bool {
    match rest.chars().next() {
        None => true,
        Some(c) => !(c.is_alphanumeric() || is_cjk_ideograph(c)),
    }
}

fn is_cjk_ideograph(c: char) -> bool {
    matches!(c as u32,
        0x3400..=0x4DBF
        | 0x4E00..=0x9FFF
        | 0xF900..=0xFAFF
        | 0x20000..=0x2A6DF
        | 0x2A700..=0x2B73F
        | 0x2B740..=0x2B81F
        | 0x2B820..=0x2CEAF
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trigger(keyword: &str, prompt: Option<&str>) -> CustomTrigger {
        CustomTrigger {
            trigger: keyword.to_string(),
            prompt: prompt.map(ToOwned::to_owned),
            skip_dir_select: false,
            working_dir: None,
            ack_message: None,
        }
    }

    #[test]
    fn exact_keyword_matches() {
        let triggers = vec![trigger("日报", Some("生成今日日报"))];
        let hit = resolve_custom_trigger("日报", &triggers).expect("exact match");
        assert_eq!(hit.trigger, "日报");
    }

    #[test]
    fn keyword_with_trailing_text_matches() {
        let triggers = vec![trigger("日报", Some("生成今日日报"))];
        assert!(resolve_custom_trigger("日报 今天修了三个 bug", &triggers).is_some());
        assert!(resolve_custom_trigger("日报：今天修了三个 bug", &triggers).is_some());
    }

    #[test]
    fn trailing_punctuation_is_stripped_from_rest() {
        let triggers = vec![trigger("日报", Some("请以日报模板输出"))];
        let hit = resolve_custom_trigger("日报：今天修了 bug", &triggers).expect("match");
        assert_eq!(
            resolve_trigger_message("日报：今天修了 bug", hit),
            "请以日报模板输出\n\n今天修了 bug"
        );
    }

    #[test]
    fn keyword_inside_longer_word_does_not_match() {
        let triggers = vec![trigger("日报", Some("生成今日日报"))];
        assert!(resolve_custom_trigger("日报表", &triggers).is_none());
        assert!(resolve_custom_trigger("今日日报", &triggers).is_none());
    }

    #[test]
    fn trigger_without_prompt_keeps_raw_text() {
        let triggers = vec![trigger("开会", None)];
        let hit = resolve_custom_trigger("开会 讲讲方案", &triggers).expect("match");
        assert_eq!(
            resolve_trigger_message("开会 讲讲方案", hit),
            "开会 讲讲方案"
        );
    }

    #[test]
    fn trigger_with_prompt_builds_initial_message() {
        let triggers = vec![trigger("日报", Some("请以日报模板输出"))];
        let hit = resolve_custom_trigger("日报", &triggers).expect("exact match");
        assert_eq!(resolve_trigger_message("日报", hit), "请以日报模板输出");

        let hit = resolve_custom_trigger("日报 今天修了 bug", &triggers).expect("match");
        assert_eq!(
            resolve_trigger_message("日报 今天修了 bug", hit),
            "请以日报模板输出\n\n今天修了 bug"
        );
    }

    #[test]
    fn first_matching_trigger_wins() {
        let triggers = vec![trigger("hello", None), trigger("hello world", Some("hi"))];
        let hit = resolve_custom_trigger("hello world", &triggers).expect("match");
        assert_eq!(hit.trigger, "hello");
    }

    #[test]
    fn empty_or_whitespace_text_never_matches() {
        let triggers = vec![trigger("日报", None)];
        assert!(resolve_custom_trigger("", &triggers).is_none());
        assert!(resolve_custom_trigger("   ", &triggers).is_none());
    }
}
