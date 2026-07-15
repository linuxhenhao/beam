use super::*;

use serde_json::Value;

/// Helper: find a select_static element nested inside an action.actions array.
/// Since select_static must be wrapped in an action component to render
/// in Feishu cards, callers should not search for bare select_static at
/// the top level of card elements.
fn find_select_static_in_elements(elements: &[Value]) -> Option<&Value> {
    elements
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .find_map(|e| {
            e["actions"].as_array().and_then(|actions| {
                actions
                    .iter()
                    .find(|a| a["tag"].as_str() == Some("select_static"))
            })
        })
}

#[test]
fn test_build_dir_select_card_contains_required_elements() {
    let recommended = vec![".".to_string(), "project-a".to_string()];
    let all = recommended.clone();
    let card = build_dir_select_card(
        "pending-1",
        "/home/user/projects",
        "帮我修复这个 bug",
        &recommended,
        &all,
        None,
        None,
        None,
        Some("zh"),
    );
    // Card should be valid JSON
    let _v: Value = serde_json::from_str(&card).expect("card should be valid JSON");
    assert!(card.contains("请选择工作目录"));
    assert!(card.contains("/home/user/projects"));
    assert!(card.contains("帮我修复这个 bug"));
    assert!(card.contains("dir_select_pick"));
    assert!(card.contains("dir_select_filter"));
    assert!(card.contains("dir_select_best"));
    assert!(card.contains("pending-1"));
    assert!(card.contains("dir_search_keyword"));
    // Verify directory button structure
    let v: Value = serde_json::from_str(&card).expect("card should be valid JSON");
    let elements = v["elements"]
        .as_array()
        .expect("elements should be an array");

    let action_groups: Vec<&Value> = elements
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .collect();
    assert!(
        !action_groups.is_empty(),
        "card should contain directory action groups"
    );
    let first_button = action_groups[0]["actions"]
        .as_array()
        .and_then(|actions| actions.first())
        .expect("action group should contain directory buttons");
    assert_eq!(
        first_button
            .pointer("/value/action")
            .and_then(Value::as_str),
        Some("dir_select_pick")
    );
    assert_eq!(
        first_button
            .pointer("/value/pending_id")
            .and_then(Value::as_str),
        Some("pending-1")
    );

    // Card should contain select_static dropdown inside an action.actions array.
    // A bare select_static as a top-level card element is not valid in the
    // Feishu card schema.
    let select_action = elements
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .find(|e| {
            e["actions"]
                .as_array()
                .map(|actions| {
                    actions
                        .iter()
                        .any(|a| a["tag"].as_str() == Some("select_static"))
                })
                .unwrap_or(false)
        })
        .expect("card should contain an action wrapping select_static dropdown");
    let select_static = select_action["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["tag"].as_str() == Some("select_static"))
        .expect("action.actions should contain select_static");
    // Verify no bare select_static as a top-level element
    let top_level_select = elements
        .iter()
        .find(|e| e["tag"].as_str() == Some("select_static"));
    assert!(
        top_level_select.is_none(),
        "select_static must NOT be a bare top-level element; it must be inside action.actions"
    );
    let options = select_static["options"]
        .as_array()
        .expect("select_static should have options");
    assert!(!options.is_empty(), "select_static should have options");
    // Verify first option value is valid JSON with action/pending_id/working_dir
    let first_opt_val = options[0]["value"]
        .as_str()
        .expect("option value should be a string");
    let opt_parsed: Value =
        serde_json::from_str(first_opt_val).expect("option value should be valid JSON");
    assert_eq!(
        opt_parsed["action"].as_str(),
        Some("dir_select_pick"),
        "select option should have action=dir_select_pick"
    );
    assert_eq!(
        opt_parsed["pending_id"].as_str(),
        Some("pending-1"),
        "select option should have pending_id"
    );
    assert!(
        opt_parsed["working_dir"].as_str().is_some(),
        "select option should have working_dir"
    );

    // Verify form container structure
    let form = elements
        .iter()
        .find(|e| e["tag"].as_str() == Some("form"))
        .expect("card should contain a form element");
    assert_eq!(form["name"].as_str(), Some("dir_search_form"));

    let form_els = form["elements"]
        .as_array()
        .expect("form should have elements");

    // form elements must only contain input + buttons (no div)
    let tags: Vec<&str> = form_els
        .iter()
        .map(|e| e["tag"].as_str().unwrap_or(""))
        .collect();
    assert!(
        !tags.contains(&"div"),
        "form should NOT contain div elements, got: {:?}",
        tags
    );
    assert!(
        tags.contains(&"input"),
        "form should contain an input, got: {:?}",
        tags
    );
    assert!(
        tags.contains(&"button"),
        "form should contain buttons, got: {:?}",
        tags
    );

    // input must have default_value (not value dict)
    let input = form_els
        .iter()
        .find(|e| e["tag"].as_str() == Some("input"))
        .expect("form should contain an input");
    assert!(
        input["default_value"].is_string() || input["default_value"].is_null(),
        "input must have default_value, got value: {:?}",
        input.get("value")
    );

    // all buttons must have action_type=form_submit
    for btn in form_els
        .iter()
        .filter(|e| e["tag"].as_str() == Some("button"))
    {
        assert_eq!(
            btn["action_type"].as_str(),
            Some("form_submit"),
            "all form buttons must be form_submit"
        );
    }
}

#[test]
fn test_build_dir_select_card_uses_english_for_en_locale() {
    let recommended = vec![".".to_string(), "project-a".to_string()];
    let all = recommended.clone();
    let card: Value = serde_json::from_str(&build_dir_select_card(
        "pending-1",
        "/home/user/projects",
        "你好",
        &recommended,
        &all,
        None,
        None,
        None,
        Some("en"),
    ))
    .expect("valid dir select card");
    assert_eq!(
        card.pointer("/elements/0/text/content")
            .and_then(Value::as_str),
        Some("📁 **Root:** /home/user/projects")
    );
    assert_eq!(
        card.pointer("/elements/0/text/i18n_content/zh_cn")
            .and_then(Value::as_str),
        Some("📁 **根目录:** /home/user/projects")
    );
    assert_eq!(
        card.pointer("/elements/1/text/content")
            .and_then(Value::as_str),
        Some("💬 **Message:** 你好")
    );
    assert_eq!(
        card.pointer("/elements/1/text/i18n_content/zh_cn")
            .and_then(Value::as_str),
        Some("💬 **消息:** 你好")
    );
    assert_eq!(
        card.pointer("/elements/2/text/content")
            .and_then(Value::as_str),
        Some("📋 **Recommended directories:**")
    );
    assert_eq!(
        card.pointer("/elements/2/text/i18n_content/zh_cn")
            .and_then(Value::as_str),
        Some("📋 **推荐目录：**")
    );
}

#[test]
fn test_build_dir_select_card_with_message() {
    let card = build_dir_select_card(
        "p1",
        "/root",
        "test",
        &[],
        &[],
        None,
        None,
        Some("请先选择目录"),
        Some("zh"),
    );
    assert!(card.contains("请先选择目录"));
}

#[test]
fn test_build_dir_session_starting_card() {
    let card = build_dir_session_starting_card("/home/user/projects", "my title", Some("zh"));
    assert!(card.contains("工作目录已选择"));
    assert!(card.contains("/home/user/projects"));
    assert!(!card.contains("my title"));
    assert!(!card.contains("\"tag\":\"action\""));
}

#[test]
fn test_truncate_str() {
    assert_eq!(truncate_str("hello", 10), "hello");
    assert_eq!(truncate_str("hello world", 8), "hello ..");
    assert_eq!(truncate_str("hello", 5), "hello");
}

#[test]
fn test_truncate_str_head_chinese() {
    // Chinese chars (3 bytes each) — byte slicing would panic
    let s = "你好世界这是一个很长的标题需要截断测试";
    let result = truncate_str_head(s, 10);
    assert!(result.chars().count() <= 10);
    assert!(result.ends_with('…'));
    // Should not panic
}

#[test]
fn test_truncate_str_tail_emoji() {
    let s = "/home/user/很长的路径/包含中文/and/emoji/🌟/test";
    let result = truncate_str_tail(s, 20);
    assert!(result.chars().count() <= 20);
    assert!(result.starts_with('…'));
    // Should not panic
}

#[test]
fn test_build_dir_select_card_utf8_safe() {
    // Chinese title + long root path must not panic
    let recommended = vec![".".to_string()];
    let all = recommended.clone();
    let long_root =
        "/home/user/这是一个很长的路径用来测试截断功能/包含中文字符/abc/def/ghi/jkl/mno";
    let chinese_title = "帮我修复这个生产环境的紧急bug非常着急请尽快处理谢谢";
    let card = build_dir_select_card(
        "p-utf8",
        long_root,
        chinese_title,
        &recommended,
        &all,
        None,
        None,
        None,
        Some("zh"),
    );
    // Should be valid JSON
    let _v: Value = serde_json::from_str(&card).expect("card should be valid JSON");
    assert!(card.contains("请选择工作目录"));
}

#[test]
fn test_build_dir_select_card_truncates_excess_options() {
    // When there are more directories than MAX_BUTTON_DIRS (40),
    // the directory buttons are capped at 40, while the select_static
    // dropdown shows up to MAX_SELECT_DIRS (150).
    let many_dirs: Vec<String> = (0..200).map(|i| format!("project-{:03}", i)).collect();
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &many_dirs,
        &many_dirs,
        None,
        None,
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");

    let pick_button_count: usize = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .flat_map(|e| e["actions"].as_array().into_iter().flatten())
        .filter(|button| {
            button.pointer("/value/action").and_then(Value::as_str) == Some("dir_select_pick")
        })
        .count();
    assert_eq!(
        pick_button_count, 40,
        "directory buttons should be capped at MAX_BUTTON_DIRS"
    );
    // select_static should be inside action.actions and contain up to MAX_SELECT_DIRS options
    let select_static = find_select_static_in_elements(v["elements"].as_array().unwrap())
        .expect("card should have select_static inside action.actions");
    let option_count = select_static["options"].as_array().unwrap().len();
    assert_eq!(
        option_count, 150,
        "select_static options should be capped at MAX_SELECT_DIRS"
    );
}

#[test]
fn test_build_dir_select_card_filtered_truncation_shows_count_in_label() {
    // When filtering produces more results than MAX_BUTTON_DIRS (40),
    // the section label should indicate the total count and button cap.
    let many_dirs: Vec<String> = (0..200).map(|i| format!("project-{:03}", i)).collect();
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &[],
        &many_dirs,
        Some(&many_dirs),
        Some("proj"),
        None,
        Some("zh"),
    );
    // Section label should mention total count and button cap
    assert!(card.contains("共 200"), "label should show total count");
    assert!(
        card.contains("显示前 40"),
        "label should show button truncation limit"
    );
    // select_static label should also mention the total
    assert!(
        card.contains("更多匹配"),
        "should have select_static more-matches section"
    );
    assert!(
        card.contains("显示前 150"),
        "select_static label should show dropdown limit"
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    // Count only button action rows (not the select_static action wrapper)
    let button_row_count = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .filter(|e| {
            e["actions"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|first| first["tag"].as_str())
                == Some("button")
        })
        .count();
    assert_eq!(
        button_row_count, 40,
        "filtered result button rows capped at MAX_BUTTON_DIRS"
    );
    let select_opts = find_select_static_in_elements(v["elements"].as_array().unwrap())
        .expect("should have select_static inside action.actions")["options"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(
        select_opts, 150,
        "select_static options capped at MAX_SELECT_DIRS"
    );
}

#[test]
fn test_build_dir_select_card_no_truncation_when_under_limit() {
    // When there are fewer directories than MAX_BUTTON_DIRS (40),
    // all fit in buttons and no truncation label.
    let few_dirs: Vec<String> = (0..10).map(|i| format!("project-{:02}", i)).collect();
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &few_dirs,
        &few_dirs,
        Some(&few_dirs),
        Some("proj"),
        None,
        Some("zh"),
    );
    // No "显示前" message when under limit (10 <= 40)
    assert!(
        !card.contains("显示前"),
        "no truncation label when under button limit"
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    // Count only button action rows (not the select_static action wrapper)
    let result_row_count = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .filter(|e| {
            e["actions"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|first| first["tag"].as_str())
                == Some("button")
        })
        .count();
    assert_eq!(result_row_count, 10, "all 10 result rows should be present");
    // select_static should exist inside action.actions (showing all results as dropdown)
    assert!(
        find_select_static_in_elements(v["elements"].as_array().unwrap()).is_some(),
        "select_static should be present even with few results"
    );
}

#[test]
fn test_build_dir_select_card_filtered_unique_short_names_single_button() {
    // Filtered results with unique short names should show a single button
    // per directory displaying the short name.
    let dirs = vec!["project-a".to_string(), "project-b".to_string()];
    let card = build_dir_select_card(
        "pid",
        "/home/user/workspace",
        "test",
        &[],
        &dirs,
        Some(&dirs),
        Some("proj"),
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    // Filter only action elements that contain buttons (exclude select_static wrapper)
    let actions: Vec<&Value> = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| {
            e["tag"].as_str() == Some("action")
                && e["actions"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|first| first["tag"].as_str())
                    == Some("button")
        })
        .collect();
    // Each dir → exactly 1 action row
    assert_eq!(actions.len(), 2, "should have 2 action rows");

    let mut working_dirs: Vec<String> = Vec::new();
    for action in &actions {
        let buttons = action["actions"]
            .as_array()
            .expect("action row should have buttons");
        assert_eq!(
            buttons.len(),
            1,
            "filtered row should have exactly 1 button (no extra full-path button)"
        );
        let content = buttons[0]
            .pointer("/text/content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // Short name should be shown (not relative path like "project-a")
        assert!(
            content.contains("project-a") || content.contains("project-b"),
            "button should display short name, got: {}",
            content
        );
        // No full resolved path in button text
        assert!(
            !content.contains("/home/user/workspace"),
            "button text should NOT contain full resolved path"
        );
        // Value must have correct action, pending_id, and working_dir
        assert_eq!(
            buttons[0].pointer("/value/action").and_then(Value::as_str),
            Some("dir_select_pick"),
            "button action should be dir_select_pick"
        );
        assert_eq!(
            buttons[0]
                .pointer("/value/pending_id")
                .and_then(Value::as_str),
            Some("pid"),
            "button pending_id should be pid"
        );
        let wd = buttons[0]
            .pointer("/value/working_dir")
            .and_then(Value::as_str)
            .expect("button should have working_dir");
        working_dirs.push(wd.to_string());
    }
    working_dirs.sort();
    let mut expected_dirs = dirs.clone();
    expected_dirs.sort();
    assert_eq!(
        working_dirs, expected_dirs,
        "collected working_dirs should match input dirs"
    );
}

#[test]
fn test_build_dir_select_card_filtered_duplicate_short_names_shows_relative_path() {
    // When filtered results contain dirs with the same short name
    // (e.g. a/foo and b/foo both resolve to "foo"), conflicting entries
    // should display the relative path to distinguish them.
    let dirs = vec![
        "group-a/project".to_string(),
        "group-b/project".to_string(),
        "group-a/unique".to_string(),
    ];
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &[],
        &dirs,
        Some(&dirs),
        Some("proj"),
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    // Filter only action elements that contain buttons (exclude select_static wrapper)
    let actions: Vec<&Value> = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| {
            e["tag"].as_str() == Some("action")
                && e["actions"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|first| first["tag"].as_str())
                    == Some("button")
        })
        .collect();
    assert_eq!(actions.len(), 3, "should have 3 action rows");

    for action in &actions {
        let buttons = action["actions"]
            .as_array()
            .expect("action row should have buttons");
        assert_eq!(buttons.len(), 1, "each row must have exactly 1 button");

        let working_dir = buttons[0]
            .pointer("/value/working_dir")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let content = buttons[0]
            .pointer("/text/content")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match working_dir {
            "group-a/project" | "group-b/project" => {
                // Conflicting short name "project" → must show relative path
                assert!(
                    content.contains("group"),
                    "conflicting dir '{}' should show relative path, got: {}",
                    working_dir,
                    content
                );
                assert!(
                    !content.ends_with("project") || content.contains("group"),
                    "conflicting dir '{}' should NOT show bare short name, got: {}",
                    working_dir,
                    content
                );
            }
            "group-a/unique" => {
                // Unique short name "unique" → short name is fine
                assert!(
                    content.contains("unique"),
                    "unique dir should show short name, got: {}",
                    content
                );
            }
            _ => panic!("unexpected working_dir: {}", working_dir),
        }
    }
}

#[test]
fn test_build_dir_select_card_recommended_duplicate_short_names_stays_short() {
    // Even when recommended dirs have duplicate short names, the
    // recommended section should keep showing short names.
    let dirs = vec!["group-a/project".to_string(), "group-b/project".to_string()];
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &dirs,
        &dirs,
        None, // recommended section
        None,
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    let all_buttons: Vec<&Value> = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .flat_map(|e| e["actions"].as_array().into_iter().flatten())
        .filter(|child| child["tag"].as_str() == Some("button"))
        .collect();

    // Both buttons should show "project" (short name), not the full rel path
    for button in &all_buttons {
        let content = button
            .pointer("/text/content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            content.contains("project"),
            "recommended section should show short name, got: {}",
            content
        );
        assert!(
            !content.contains("group"),
            "recommended section should NOT show full relative path, got: {}",
            content
        );
    }
}

#[test]
fn test_build_dir_select_card_filtered_working_dir_value_correct() {
    // The button value must always carry the real working_dir (relative path),
    // regardless of what is displayed in the button text.
    let dirs = vec![
        "deep/nested/path/api".to_string(),
        "another/deep/nested/path/api".to_string(),
    ];
    let card = build_dir_select_card(
        "pid",
        "/home/user/workspace",
        "test",
        &[],
        &dirs,
        Some(&dirs),
        Some("api"),
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    // Filter only action elements that contain buttons (exclude select_static wrapper)
    let actions: Vec<&Value> = v["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| {
            e["tag"].as_str() == Some("action")
                && e["actions"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|first| first["tag"].as_str())
                    == Some("button")
        })
        .collect();

    let mut found_dirs: Vec<String> = Vec::new();
    for action in &actions {
        let buttons = action["actions"]
            .as_array()
            .expect("action row should have buttons");
        assert_eq!(buttons.len(), 1, "each row must have exactly 1 button");
        let wd = buttons[0]
            .pointer("/value/working_dir")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        let action_val = buttons[0]
            .pointer("/value/action")
            .and_then(Value::as_str)
            .unwrap();
        assert_eq!(action_val, "dir_select_pick");
        let pending = buttons[0]
            .pointer("/value/pending_id")
            .and_then(Value::as_str)
            .unwrap();
        assert_eq!(pending, "pid");
        found_dirs.push(wd);
    }
    found_dirs.sort();
    let mut expected: Vec<String> = dirs.clone();
    expected.sort();
    assert_eq!(
        found_dirs, expected,
        "button values must contain the correct relative working_dir"
    );
}

#[test]
fn test_select_static_is_inside_action_actions_not_top_level() {
    // Verify that the "more directories" select_static dropdown is placed
    // inside an action.actions array, NOT as a bare top-level element.
    // A bare select_static would not render in Feishu cards.
    let dirs: Vec<String> = (0..10).map(|i| format!("project-{:03}", i)).collect();
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &dirs,
        &dirs,
        None,
        None,
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    let elements = v["elements"].as_array().unwrap();

    // Must NOT have select_static as a bare top-level element
    let bare_select = elements
        .iter()
        .find(|e| e["tag"].as_str() == Some("select_static"));
    assert!(
        bare_select.is_none(),
        "select_static must NOT be a bare top-level card element"
    );

    // Must have select_static inside action.actions
    let found = find_select_static_in_elements(elements);
    assert!(
        found.is_some(),
        "select_static must be wrapped inside action.actions"
    );

    // Verify the select_static has the correct content
    let select = found.unwrap();
    let placeholder = select["placeholder"]["content"].as_str().unwrap_or("");
    assert!(
        placeholder.contains("选择"),
        "select_static should have a meaningful placeholder"
    );
    let options = select["options"].as_array().unwrap();
    assert!(!options.is_empty(), "select_static must have options");

    // Each option value must be valid JSON with action/pending_id/working_dir
    for opt in options {
        let val_str = opt["value"]
            .as_str()
            .expect("option value must be a string");
        let parsed: Value = serde_json::from_str(val_str).expect("option value must be valid JSON");
        assert_eq!(
            parsed["action"].as_str(),
            Some("dir_select_pick"),
            "each select option must have action=dir_select_pick"
        );
        assert_eq!(
            parsed["pending_id"].as_str(),
            Some("pid"),
            "each select option must have pending_id"
        );
        assert!(
            parsed["working_dir"].as_str().is_some(),
            "each select option must have working_dir"
        );
    }
}

#[test]
fn test_search_area_has_interactive_input_and_button() {
    // Verify the search area is interactive: it has a form with an input
    // and at least one button. The standalone div before the form is purely
    // instructional text — it should NOT look like a clickable title.
    let dirs = vec!["project-a".to_string(), "project-b".to_string()];
    let card = build_dir_select_card(
        "pid",
        "/root",
        "test",
        &dirs,
        &dirs,
        None,
        Some("test"),
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card).expect("valid card JSON");
    let elements = v["elements"].as_array().unwrap();

    // Find the form element (must exist and be interactive)
    let form = elements
        .iter()
        .find(|e| e["tag"].as_str() == Some("form"))
        .expect("card must contain a form element for search");
    assert_eq!(form["name"].as_str(), Some("dir_search_form"));

    let form_els = form["elements"]
        .as_array()
        .expect("form must have elements");

    // Must have an input element
    let input = form_els
        .iter()
        .find(|e| e["tag"].as_str() == Some("input"))
        .expect("search form must have an input element");
    assert_eq!(
        input["name"].as_str(),
        Some("dir_search_keyword"),
        "input name should be dir_search_keyword"
    );
    // Input should have a default_value reflecting the search keyword
    assert_eq!(
        input["default_value"].as_str(),
        Some("test"),
        "input default_value should reflect the search keyword"
    );
    // Placeholder should be instructive
    let placeholder = input["placeholder"]["content"].as_str().unwrap_or("");
    assert!(
        placeholder.contains("关键词"),
        "input placeholder should mention keywords"
    );

    // Must have form_submit buttons
    let buttons: Vec<&Value> = form_els
        .iter()
        .filter(|e| e["tag"].as_str() == Some("button"))
        .collect();
    assert!(!buttons.is_empty(), "form must have at least one button");

    // One button must be the filter button with action=dir_select_filter
    let filter_btn = buttons
        .iter()
        .find(|b| b.pointer("/value/action").and_then(Value::as_str) == Some("dir_select_filter"))
        .expect("must have a dir_select_filter button");
    assert_eq!(
        filter_btn["action_type"].as_str(),
        Some("form_submit"),
        "filter button must be form_submit"
    );

    // One button must be the best-match button with action=dir_select_best
    let best_btn = buttons
        .iter()
        .find(|b| b.pointer("/value/action").and_then(Value::as_str) == Some("dir_select_best"))
        .expect("must have a dir_select_best button");
    assert_eq!(
        best_btn["action_type"].as_str(),
        Some("form_submit"),
        "best-match button must be form_submit"
    );

    // The instructional div before the form should be plain text,
    // not a bold title that looks clickable
    let instructional_div = elements
        .iter()
        .filter(|e| e["tag"].as_str() == Some("div"))
        .find(|e| {
            e.pointer("/text/content")
                .and_then(Value::as_str)
                .map(|s| s.contains("下方输入") && s.contains("筛选"))
                .unwrap_or(false)
        })
        .expect("card must have instructional text for the search area");
    let div_content = instructional_div
        .pointer("/text/content")
        .and_then(Value::as_str)
        .unwrap_or("");
    // The text should be instructional, not a bold clickable-looking title
    assert!(
        !div_content.contains("**搜索目录：**"),
        "instructional div should NOT use bold title '搜索目录：' that looks clickable"
    );
    assert!(
        div_content.contains("输入关键词") || div_content.contains("筛选"),
        "instructional div should explain how to search"
    );
}
