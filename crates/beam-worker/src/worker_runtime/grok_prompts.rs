use beam_core::TuiPromptOption;

/// Detect Grok's plan-approval overlay and map Feishu card buttons to the
/// TUI shortcuts (`a` approve, `s` request changes, `q` quit).
///
/// Do not match the idle footer badge `always-approve` — that is a permission
/// mode, not the plan approval surface.
pub(crate) fn detect_grok_plan_approval(screen: &str) -> Option<Vec<TuiPromptOption>> {
    let lower = screen.to_ascii_lowercase();
    let is_plan_surface = lower.contains("request changes")
        || lower.contains("quit plan")
        || lower.contains("approve w/ comments")
        || lower.contains("no plan written yet");
    if !is_plan_surface {
        return None;
    }
    Some(vec![
        plan_option("Approve", "a"),
        plan_option("Request changes", "s"),
        plan_option("Quit plan", "q"),
    ])
}

fn plan_option(label: &str, key: &str) -> TuiPromptOption {
    TuiPromptOption {
        label: Some(label.to_string()),
        text: label.to_string(),
        selected: false,
        option_type: Some("select".to_string()),
        keys: vec![key.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_always_approve_footer() {
        let screen = "Grok 4.6 (high) · always-approve\n❯ ";
        assert!(detect_grok_plan_approval(screen).is_none());
    }

    #[test]
    fn detects_plan_approval_action_bar() {
        let screen = "Review the plan\na Approve   s Request changes   q Quit plan";
        let options = detect_grok_plan_approval(screen).expect("detected");
        assert_eq!(options.len(), 3);
        assert_eq!(options[0].keys, vec!["a".to_string()]);
        assert_eq!(options[1].keys, vec!["s".to_string()]);
        assert_eq!(options[2].keys, vec!["q".to_string()]);
    }

    #[test]
    fn detects_empty_plan_approval() {
        let screen = "No plan written yet\nApprove   Request changes";
        assert!(detect_grok_plan_approval(screen).is_some());
    }
}
