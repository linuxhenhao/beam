use super::*;

#[test]
fn dir_select_filter_response_includes_card_and_toast() {
    // Verify that a filter action response returns both toast and card fields,
    // so the Feishu client updates the card inline instead of just showing a toast.
    let card_json = dir_select::build_dir_select_card(
        "pending-1",
        "/home/user/projects",
        "test message",
        &[".".to_string(), "project-a".to_string()],
        &[
            ".".to_string(),
            "project-a".to_string(),
            "project-b".to_string(),
        ],
        Some(&["project-a".to_string()]),
        Some("project"),
        None,
        Some("zh"),
    );
    let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
    let toast_msg = "已筛选 \"project\"";
    let response = serde_json::json!({
        "toast": { "type": "success", "content": toast_msg },
        "card": { "type": "raw", "data": card_data }
    });

    // Response must contain both toast and card fields
    assert!(
        response.get("toast").is_some(),
        "response must have toast field"
    );
    assert!(
        response.get("card").is_some(),
        "response must have card field"
    );
    assert_eq!(
        response.pointer("/toast/content").and_then(Value::as_str),
        Some(toast_msg)
    );
    assert_eq!(
        response.pointer("/card/type").and_then(Value::as_str),
        Some("raw")
    );
    // The card data should contain the filtered directory button
    let card_str = response.pointer("/card/data").unwrap().to_string();
    assert!(
        card_str.contains("project-a"),
        "filtered card must show project-a"
    );
    assert!(
        card_str.contains("dir_select_pick"),
        "filtered card must retain pickable buttons"
    );
}

#[test]
fn dir_select_filter_response_card_contains_filtered_dirs_only() {
    // When filtering with a keyword, the response card should show only matching dirs.
    let all_dirs: Vec<String> = vec![
        ".".to_string(),
        "project-a".to_string(),
        "project-b".to_string(),
        "other".to_string(),
    ];
    let filtered: Vec<String> = vec!["project-a".to_string(), "project-b".to_string()];

    let card_json = dir_select::build_dir_select_card(
        "pending-2",
        "/root",
        "test",
        &[],
        &all_dirs,
        Some(&filtered),
        Some("project"),
        None,
        Some("zh"),
    );
    let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
    let response = serde_json::json!({
        "toast": { "type": "success", "content": "已筛选 \"project\"" },
        "card": { "type": "raw", "data": card_data }
    });

    let card_str = response.pointer("/card/data").unwrap().to_string();
    // Must contain the matching dirs
    assert!(
        card_str.contains("project-a"),
        "card must contain project-a"
    );
    assert!(
        card_str.contains("project-b"),
        "card must contain project-b"
    );
    // Should NOT contain the non-matching dir "other"
    assert!(
        !card_str.contains("\"working_dir\":\"other\""),
        "card must NOT contain non-matching dir 'other'"
    );
    // Must still have pickable buttons
    assert!(
        card_str.contains("dir_select_pick"),
        "filtered card must retain dir_select_pick buttons"
    );
}

#[test]
fn dir_select_filter_response_empty_keyword_shows_all_dirs() {
    // Empty keyword should show all candidates (clear filter / show all).
    let all_dirs: Vec<String> = vec![
        ".".to_string(),
        "project-a".to_string(),
        "project-b".to_string(),
    ];

    let card_json = dir_select::build_dir_select_card(
        "pending-3",
        "/root",
        "test",
        &[],
        &all_dirs,
        Some(&all_dirs),
        None,
        None,
        Some("zh"),
    );
    let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
    let response = serde_json::json!({
        "toast": { "type": "success", "content": "已显示全部目录" },
        "card": { "type": "raw", "data": card_data }
    });

    // Response must have both fields
    assert!(response.get("toast").is_some());
    assert!(response.get("card").is_some());

    let card_str = response.pointer("/card/data").unwrap().to_string();
    // All dirs should be present as buttons
    assert!(card_str.contains("dir_select_pick"));
    // The search keyword field should be empty (cleared)
    let v: Value = serde_json::from_str(&card_str).expect("valid card JSON");
    let elements = v["elements"].as_array().unwrap();
    let form = elements
        .iter()
        .find(|e| e["tag"].as_str() == Some("form"))
        .unwrap();
    let form_els = form["elements"].as_array().unwrap();
    let input = form_els
        .iter()
        .find(|e| e["tag"].as_str() == Some("input"))
        .unwrap();
    assert_eq!(
        input["default_value"].as_str(),
        Some(""),
        "empty keyword should clear the input field"
    );
}

#[test]
fn dir_select_filter_response_empty_result_shows_warning() {
    // When no directories match, the card should show a warning message.
    let all_dirs: Vec<String> = vec![".".to_string(), "project-a".to_string()];
    let filtered: Vec<String> = vec![];

    let card_json = dir_select::build_dir_select_card(
        "pending-4",
        "/root",
        "test",
        &[],
        &all_dirs,
        Some(&filtered),
        Some("nonexistent"),
        Some("⚠️ 没有目录匹配关键词 \"nonexistent\"，请尝试其他关键词。"),
        Some("zh"),
    );
    let card_data: Value = serde_json::from_str(&card_json).expect("card should be valid JSON");
    let response = serde_json::json!({
        "toast": { "type": "success", "content": "已筛选 \"nonexistent\"" },
        "card": { "type": "raw", "data": card_data }
    });

    let card_str = response.pointer("/card/data").unwrap().to_string();
    // Must show the warning message
    assert!(
        card_str.contains("没有目录匹配"),
        "empty result card must show warning message"
    );
    assert!(
        card_str.contains("请尝试其他关键词"),
        "empty result card must suggest trying other keywords"
    );
    // Must still have the search form to allow retry
    assert!(
        card_str.contains("dir_search_keyword"),
        "empty result card must retain search input"
    );
}

#[test]
fn dir_select_card_uses_action_buttons() {
    // Verify that the card exposes directory choices as clickable buttons
    // AND a select_static dropdown as an alternative entry point.
    let all_dirs: Vec<String> = (0..10).map(|i| format!("project-{}", i)).collect();
    let card_json = dir_select::build_dir_select_card(
        "pid",
        "/root",
        "test",
        &all_dirs,
        &all_dirs,
        None,
        None,
        None,
        Some("zh"),
    );
    let v: Value = serde_json::from_str(&card_json).expect("valid card JSON");
    let elements = v["elements"].as_array().unwrap();

    let action_elements: Vec<&Value> = elements
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .collect();
    assert!(
        !action_elements.is_empty(),
        "card must contain directory action groups"
    );

    let buttons: Vec<&Value> = action_elements
        .iter()
        .flat_map(|e| e["actions"].as_array().into_iter().flatten())
        .filter(|child| child["tag"].as_str() == Some("button"))
        .collect();
    // 10 dirs ≤ MAX_BUTTON_DIRS(40), so all show as buttons
    assert_eq!(buttons.len(), 10, "should have one button per directory");
    for (i, button) in buttons.iter().enumerate() {
        assert_eq!(
            button.pointer("/value/action").and_then(Value::as_str),
            Some("dir_select_pick")
        );
        assert_eq!(
            button.pointer("/value/pending_id").and_then(Value::as_str),
            Some("pid")
        );
        assert_eq!(
            button.pointer("/value/working_dir").and_then(Value::as_str),
            Some(format!("project-{}", i).as_str())
        );
    }
    // Card now includes select_static inside action.actions as alternative entry point.
    // A bare select_static at the top level is not valid in Feishu card schema.
    let select_static = elements
        .iter()
        .filter(|e| e["tag"].as_str() == Some("action"))
        .find_map(|e| {
            e["actions"].as_array().and_then(|actions| {
                actions
                    .iter()
                    .find(|a| a["tag"].as_str() == Some("select_static"))
            })
        })
        .expect("card should contain select_static dropdown inside action.actions");
    // Verify no bare select_static at the top level
    assert!(
        elements
            .iter()
            .find(|e| e["tag"].as_str() == Some("select_static"))
            .is_none(),
        "select_static must not be a bare top-level element"
    );
    let options = select_static["options"].as_array().unwrap();
    assert_eq!(
        options.len(),
        10,
        "select_static should have all 10 options"
    );
    // Verify first option value is valid JSON with correct fields
    let first_opt_val = options[0]["value"].as_str().unwrap();
    let opt_parsed: Value = serde_json::from_str(first_opt_val).unwrap();
    assert_eq!(opt_parsed["action"].as_str(), Some("dir_select_pick"));
    assert_eq!(opt_parsed["pending_id"].as_str(), Some("pid"));
    assert!(opt_parsed["working_dir"].as_str().is_some());
}

#[test]
fn grant_add_chat_grant_includes_quota_key() {
    let mut config = serde_json::json!([{
        "larkAppId": "app-1",
        "larkAppSecret": "s",
        "cliId": "codex",
        "allowedUsers": ["ou_owner"]
    }]);
    grant::add_chat_grant(&mut config, "app-1", "chat-1", "ou_user", Some(5)).unwrap();
    let bot = &config.as_array().unwrap()[0];
    let grants = bot["chatGrants"]["chat-1"].as_array().unwrap();
    assert!(grants.iter().any(|v| v.as_str() == Some("ou_user")));
    let quota = &bot["quotaState"]["chat:chat-1:ou_user"];
    assert_eq!(quota["limit"].as_u64().unwrap(), 5);
    assert_eq!(quota["used"].as_u64().unwrap(), 0);
}

#[test]
fn grant_revoke_removes_from_all_lists() {
    let mut config = serde_json::json!([{
        "larkAppId": "app-1",
        "larkAppSecret": "s",
        "cliId": "codex",
        "allowedUsers": ["ou_owner", "ou_user"],
        "chatGrants": {"chat-1": ["ou_user"]},
        "globalGrants": ["ou_user"],
        "quotaState": {"chat:chat-1:ou_user": {"limit": 5, "used": 3}}
    }]);
    grant::revoke_grant(
        &mut config,
        "app-1",
        "chat-1",
        "ou_user",
        &["ou_owner".to_string()],
    )
    .unwrap();
    let bot = &config.as_array().unwrap()[0];
    assert!(
        !bot["allowedUsers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str() == Some("ou_user"))
    );
    assert!(
        bot["chatGrants"]["chat-1"]
            .as_array()
            .unwrap_or(&vec![])
            .is_empty()
    );
    assert!(bot["globalGrants"].as_array().unwrap_or(&vec![]).is_empty());
    assert!(bot["quotaState"].as_object().unwrap().is_empty());
}

#[test]
fn grant_cannot_revoke_owner() {
    let mut config = serde_json::json!([{
        "larkAppId": "app-1",
        "larkAppSecret": "s",
        "cliId": "codex",
        "allowedUsers": ["ou_owner"]
    }]);
    let result = grant::revoke_grant(
        &mut config,
        "app-1",
        "chat-1",
        "ou_owner",
        &["ou_owner".to_string()],
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn inbound_quota_consumes_and_exhausts() {
    let paths = temp_paths("inbound-quota");
    maybe_remove_dir(&paths.root().to_path_buf());
    std::fs::create_dir_all(paths.root()).unwrap();
    let bot = BotConfig {
        name: None,
        lark_app_id: "app-1".to_string(),
        lark_app_secret: "secret".to_string(),
        cli_id: "codex".to_string(),
        cli_bin: None,
        cgroup_slice: None,
        cli_args: Vec::new(),
        skip_working_dir_prompt: false,
        model: None,
        working_dir: None,
        lark_encrypt_key: None,
        lark_verification_token: None,
        allowed_users: vec!["ou_owner".to_string()],
        private_card: false,
        allowed_chat_groups: Vec::new(),
        chat_grants: std::collections::HashMap::from([(
            "chat-1".to_string(),
            vec!["ou_user".to_string()],
        )]),
        global_grants: Vec::new(),
        oncall_chats: Vec::new(),
        restrict_grant_commands: false,
        message_quota: None,
        quota_state: std::collections::HashMap::from([(
            "chat:chat-1:ou_user".to_string(),
            beam_core::QuotaEntry { limit: 2, used: 1 },
        )]),
        custom_triggers: Vec::new(),
    };
    let app_id = bot.lark_app_id.clone();
    std::fs::write(
        paths.bots_json(),
        serde_json::to_string_pretty(&serde_json::json!([{
            "larkAppId": "app-1",
            "larkAppSecret": "secret",
            "cliId": "codex",
            "allowedUsers": ["ou_owner"],
            "chatGrants": {"chat-1": ["ou_user"]},
            "globalGrants": [],
            "oncallChats": [],
            "restrictGrantCommands": false,
            "quotaState": {
                "chat:chat-1:ou_user": { "limit": 2, "used": 1 }
            }
        }]))
        .unwrap(),
    )
    .unwrap();
    let state = make_state(paths.clone(), HashMap::from([(app_id, bot)]));
    let before = consume_inbound_quota(&state, "app-1", "chat:chat-1:ou_user")
        .await
        .expect("quota consume");
    assert!(before.allowed);
    assert!(before.exhausted);
    let raw = std::fs::read_to_string(paths.bots_json()).unwrap();
    let value: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        value[0]["quotaState"]["chat:chat-1:ou_user"]["used"]
            .as_u64()
            .unwrap(),
        2
    );

    let after = consume_inbound_quota(&state, "app-1", "chat:chat-1:ou_user")
        .await
        .expect("quota consume");
    assert!(!after.allowed);
    assert!(after.exhausted);
    let raw = std::fs::read_to_string(paths.bots_json()).unwrap();
    let value: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        value[0]["quotaState"]["chat:chat-1:ou_user"]["used"]
            .as_u64()
            .unwrap(),
        2
    );
    maybe_remove_dir(&paths.root().to_path_buf());
}
