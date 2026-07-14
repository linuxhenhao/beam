use super::*;
use crate::tests::test_helpers::*;

#[test]
fn parse_feishu_resume_input_routes_send_and_reply_variants() {
    let send = serde_json::json!({
        "larkAppId": "app-1",
        "chatId": "chat-1",
        "content": "hello",
    });
    let send_input = parse_feishu_resume_input(&send).expect("send input");
    assert_eq!(send_input.lark_app_id, "app-1");
    assert_eq!(send_input.chat_id.as_deref(), Some("chat-1"));
    assert_eq!(send_input.root_message_id, None);
    assert_eq!(send_input.content, "hello");

    let reply = serde_json::json!({
        "larkAppId": "app-1",
        "rootMessageId": "msg-1",
        "content": "world",
    });
    let reply_input = parse_feishu_resume_input(&reply).expect("reply input");
    assert_eq!(reply_input.chat_id, None);
    assert_eq!(reply_input.root_message_id.as_deref(), Some("msg-1"));
    assert_eq!(reply_input.content, "world");
}

#[test]
fn lark_event_dedupe_key_skips_empty_ids() {
    assert_eq!(
        lark_event_dedupe_key("app-1", "evt-1").as_deref(),
        Some("app-1:evt-1")
    );
    assert_eq!(lark_event_dedupe_key("app-1", ""), None);
    assert_eq!(lark_event_dedupe_key("app-1", "   "), None);
}

#[test]
fn ws_card_action_handler_routes_toggle_display() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let app_id = "app-toggle-ws";
        let bot = BotConfig {
            name: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: Vec::new(),
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        let state = make_state(temp_paths("toggle-ws"), HashMap::from([(app_id.to_string(), bot)]));
        let mut session = make_session("sess-toggle-ws");
        session.lark_app_id = app_id.to_string();
        session.closed_at = None;
        session.status = SessionStatus::Active;
        session.display_mode = Some(DisplayMode::Hidden);
        session.current_image_key = Some("img-2".to_string());
        session.stream_card_nonce = Some("nonce-toggle-ws".to_string());
        {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session.session_id.clone(), session.clone());
        }

        let handler = LarkWsCardActionEventHandler {
            state: state.clone(),
            app_id: app_id.to_string(),
            event_type: "card.action.trigger",
        };
        let event = mock_card_action_event(serde_json::json!({
            "open_id": "ou_user",
            "open_message_id": session.stream_card_id.clone().unwrap_or_else(|| "om-card".to_string()),
            "action": {
                "value": {
                    "action": "toggle_display",
                    "root_id": session.root_message_id,
                    "session_id": session.session_id,
                    "cli_id": session.cli_id.clone().unwrap_or_else(|| "codex".to_string())
                }
            }
        }));

        let resp = handler.handle(event).await.expect("event handler").expect("event resp");
        let body: Value = serde_json::from_slice(&resp.body).expect("body json");
        assert_eq!(body.pointer("/toast/type").and_then(Value::as_str), Some("success"));
        let stored = state.sessions.lock().await.get(&session.session_id).cloned().expect("stored session");
        assert_eq!(stored.display_mode, Some(DisplayMode::Screenshot));
    });
}

#[test]
fn ws_card_action_handler_routes_ask_toggle_and_submit() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
        let base_url = start_mock_lark_server().await;
        let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

        let app_id = "app-ask-ws";
        let bot = BotConfig {
            name: None,
            lark_app_id: app_id.to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "opencode".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_approver".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        let paths = temp_paths("ask-ws");
        let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

        let ask_body = serde_json::json!({
            "sessionId": "sess-ask-ws",
            "chatId": "chat-1",
            "larkAppId": app_id,
            "rootMessageId": null,
            "timeoutMs": 10_000,
            "approvers": ["ou_approver"],
            "questions": [{
                "prompt": "Approve OpenCode permission?",
                "multiSelect": false,
                "options": [
                    { "key": "always", "label": "Always allow" },
                    { "key": "reject", "label": "Reject" }
                ]
            }]
        });

        let create_state = state.clone();
        let create_task =
            tokio::spawn(
                async move { ask::create_ask(State(create_state), Json(ask_body)).await },
            );

        let snapshot = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if let Ok(Some(snaps)) = beam_core::persist::read_json::<
                    Vec<ask::AskPendingSnapshot>,
                >(&paths.ask_pending_json())
                {
                    if let Some(snap) = snaps.into_iter().next() {
                        break snap;
                    }
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "ask pending snapshot was not persisted"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };

        let handler = LarkWsCardActionEventHandler {
            state: state.clone(),
            app_id: app_id.to_string(),
            event_type: "card.action.trigger",
        };

        let toggle_event = mock_card_action_event(serde_json::json!({
            "operator": { "open_id": "ou_approver" },
            "context": { "open_message_id": "om_ask_card" },
            "action": {
                "value": {
                    "action": "ask_toggle",
                    "ask_id": snapshot.ask_id,
                    "nonce": snapshot.nonce,
                    "question_index": 0,
                    "key": "always"
                }
            }
        }));
        let toggle_resp = handler
            .handle(toggle_event)
            .await
            .expect("toggle event handler")
            .expect("toggle event response");
        let toggle_body: Value =
            serde_json::from_slice(&toggle_resp.body).expect("toggle body");
        assert_eq!(
            toggle_body.pointer("/toast/type").and_then(Value::as_str),
            Some("success")
        );

        let submit_event = mock_card_action_event(serde_json::json!({
            "operator": { "open_id": "ou_approver" },
            "context": { "open_message_id": "om_ask_card" },
            "action": {
                "value": {
                    "action": "ask_submit",
                    "ask_id": snapshot.ask_id,
                    "nonce": snapshot.nonce
                }
            }
        }));
        let submit_resp = handler
            .handle(submit_event)
            .await
            .expect("submit event handler")
            .expect("submit event response");
        let submit_body: Value =
            serde_json::from_slice(&submit_resp.body).expect("submit body");
        assert_eq!(
            submit_body
                .pointer("/toast/content")
                .and_then(Value::as_str),
            Some("ask submitted")
        );

        let create_response = create_task
            .await
            .expect("create task join")
            .expect("create ask");
        assert_eq!(
            create_response.0.pointer("/kind").and_then(Value::as_str),
            Some("answered")
        );
        assert_eq!(
            create_response
                .0
                .pointer("/answers/0/0")
                .and_then(Value::as_str),
            Some("always")
        );
        assert_eq!(
            create_response.0.pointer("/by").and_then(Value::as_str),
            Some("ou_approver")
        );
    });
}

#[test]
fn lark_message_withdrawn_helpers_recognize_code_230011() {
    let payload = r#"{"code":230011,"msg":"message withdrawn"}"#;
    assert!(is_lark_message_withdrawn_payload(payload));
    assert_eq!("DONE", "DONE");

    let err = anyhow::anyhow!("lark message withdrawn: {}", payload);
    assert!(is_lark_message_withdrawn_error(&err));

    let other = anyhow::anyhow!("lark reply failed: {{\"code\":999}}");
    assert!(!is_lark_message_withdrawn_error(&other));
}

#[test]
fn normalize_lark_ws_card_action_preserves_operator_context_and_value() {
    let action = CardAction {
        open_id: Some("ou_owner".to_string()),
        open_message_id: Some("om_card".to_string()),
        action: Some(feishu_sdk::card::CardActionValue {
            value: Some(serde_json::json!({
                "action": "toggle_display",
                "session_id": "sess-1",
                "card_nonce": "nonce-1",
            })),
            tag: Some("button".to_string()),
            option: None,
            timezone: None,
        }),
        ..Default::default()
    };

    let payload = normalize_lark_ws_card_action(action);
    assert_eq!(
        payload.pointer("/operator/open_id").and_then(Value::as_str),
        Some("ou_owner")
    );
    assert_eq!(
        payload
            .pointer("/context/open_message_id")
            .and_then(Value::as_str),
        Some("om_card")
    );
    assert_eq!(
        payload
            .pointer("/action/value/action")
            .and_then(Value::as_str),
        Some("toggle_display")
    );
    assert_eq!(
        payload
            .pointer("/action/value/card_nonce")
            .and_then(Value::as_str),
        Some("nonce-1")
    );
}

#[test]
fn normalize_lark_ws_card_action_preserves_form_value_for_form_submit() {
    let raw = serde_json::json!({
        "open_id": "ou_owner",
        "open_message_id": "om_card",
        "action": {
            "value": {
                "action": "dir_select_filter",
                "pending_id": "pending-xyz"
            },
            "tag": "button",
            "form_value": {
                "dir_search_keyword": "home/test"
            }
        }
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

    assert_eq!(
        payload.pointer("/operator/open_id").and_then(Value::as_str),
        Some("ou_owner")
    );
    assert_eq!(
        payload
            .pointer("/action/value/action")
            .and_then(Value::as_str),
        Some("dir_select_filter")
    );
    assert_eq!(
        payload
            .pointer("/action/value/pending_id")
            .and_then(Value::as_str),
        Some("pending-xyz")
    );
    assert_eq!(
        payload
            .pointer("/action/form_value/dir_search_keyword")
            .and_then(Value::as_str),
        Some("home/test"),
        "form_value must be preserved through the CardAction deserialization round-trip"
    );

    let parsed = parse_lark_card_action(&payload).expect("parse normalized payload");
    assert_eq!(parsed.action, "dir_select_filter");
    assert_eq!(parsed.dir_search_keyword.as_deref(), Some("home/test"));
}

#[test]
fn normalize_lark_ws_card_action_restores_operator_context_from_raw() {
    let raw = serde_json::json!({
        "operator": {
            "open_id": "ou_ac4d3f69f6c8b13349ba3f51c7b7c2cc",
            "tenant_key": "t_xxx"
        },
        "context": {
            "open_message_id": "om_abc123"
        },
        "action": {
            "value": {
                "action": "get_write_link",
                "session_id": "sess-1"
            },
            "tag": "button"
        },
        "token": "x-token"
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

    assert_eq!(
        payload.pointer("/operator/open_id").and_then(Value::as_str),
        Some("ou_ac4d3f69f6c8b13349ba3f51c7b7c2cc")
    );
    assert_eq!(
        payload
            .pointer("/context/open_message_id")
            .and_then(Value::as_str),
        Some("om_abc123")
    );
    assert_eq!(
        payload
            .pointer("/action/value/action")
            .and_then(Value::as_str),
        Some("get_write_link")
    );

    let parsed = parse_lark_card_action(&payload).expect("parse normalized payload");
    assert_eq!(parsed.action, "get_write_link");
    assert_eq!(
        parsed.operator_open_id.as_deref(),
        Some("ou_ac4d3f69f6c8b13349ba3f51c7b7c2cc"),
        "operator_open_id must be extracted from restored /operator/open_id"
    );
    assert_eq!(
        parsed.clicked_message_id.as_deref(),
        Some("om_abc123"),
        "clicked_message_id must be extracted from restored /context/open_message_id"
    );
}

#[test]
fn normalize_lark_ws_card_action_preserves_choose_read_only_terminal_link_action() {
    let raw = serde_json::json!({
        "operator": {
            "open_id": "ou_choose"
        },
        "context": {
            "open_message_id": "om_choose"
        },
        "action": {
            "value": {
                "action": "choose_read_only_terminal_link",
                "session_id": "sess-choose"
            },
            "tag": "button"
        }
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");
    let parsed = parse_lark_card_action(&payload).expect("parse normalized payload");
    assert_eq!(parsed.action, "choose_read_only_terminal_link");
    assert_eq!(parsed.operator_open_id.as_deref(), Some("ou_choose"),);
    assert_eq!(parsed.clicked_message_id.as_deref(), Some("om_choose"),);
}

#[test]
fn normalize_lark_ws_card_action_restores_operator_context_with_operator_id_fallback() {
    let raw = serde_json::json!({
        "operator_id": {
            "open_id": "ou_from_operator_id"
        },
        "context": {
            "open_message_id": "om_from_context"
        },
        "action": {
            "value": {
                "action": "close",
                "session_id": "sess-1"
            }
        }
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

    let parsed = parse_lark_card_action(&payload).expect("parse");
    assert_eq!(
        parsed.operator_open_id.as_deref(),
        Some("ou_from_operator_id"),
        "operator_open_id should fall back to /operator_id/open_id"
    );
    assert_eq!(
        parsed.clicked_message_id.as_deref(),
        Some("om_from_context")
    );
}

#[test]
fn normalize_lark_ws_card_action_raw_operator_overrides_cardaction_open_id() {
    let raw = serde_json::json!({
        "open_id": "ou_from_top_level",
        "open_message_id": "om_from_top_level",
        "operator": {
            "open_id": "ou_from_operator",
            "tenant_key": "t_xxx"
        },
        "context": {
            "open_message_id": "om_from_context"
        },
        "action": {
            "value": {
                "action": "restart",
                "session_id": "sess-1"
            },
            "tag": "button"
        }
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");

    let parsed = parse_lark_card_action(&payload).expect("parse");
    assert_eq!(parsed.action, "restart");
    assert_eq!(
        parsed.operator_open_id.as_deref(),
        Some("ou_from_operator"),
        "raw /operator/open_id should take precedence"
    );
    assert_eq!(
        parsed.clicked_message_id.as_deref(),
        Some("om_from_context"),
        "raw /context/open_message_id should take precedence"
    );
}

#[test]
fn normalize_lark_ws_card_action_from_raw_uses_operator_id_when_operator_absent() {
    let raw = serde_json::json!({
        "operator_id": {
            "open_id": "ou_from_operator_id"
        },
        "context": {
            "open_message_id": "om_from_context"
        },
        "action": {
            "value": {
                "action": "close",
                "session_id": "sess-1"
            }
        }
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");
    let parsed = parse_lark_card_action(&payload).expect("parse");

    assert_eq!(
        parsed.operator_open_id.as_deref(),
        Some("ou_from_operator_id"),
        "operator_open_id must be extracted from /operator_id/open_id"
    );
    assert_eq!(
        parsed.clicked_message_id.as_deref(),
        Some("om_from_context")
    );
}

#[test]
fn normalize_lark_ws_card_action_from_raw_operator_wins_over_operator_id() {
    let raw = serde_json::json!({
        "operator": {
            "open_id": "ou_from_operator"
        },
        "operator_id": {
            "open_id": "ou_from_operator_id"
        },
        "context": {
            "open_message_id": "om_from_context"
        },
        "action": {
            "value": {
                "action": "restart",
                "session_id": "sess-1"
            }
        }
    });

    let payload = normalize_lark_ws_card_action_from_raw(raw).expect("normalize from raw");
    let parsed = parse_lark_card_action(&payload).expect("parse");

    assert_eq!(
        parsed.operator_open_id.as_deref(),
        Some("ou_from_operator"),
        "/operator must win over /operator_id"
    );
    assert_eq!(
        parsed.clicked_message_id.as_deref(),
        Some("om_from_context")
    );
}

#[test]
fn parse_lark_inbound_message_normalizes_topic_and_mentions() {
    let payload = serde_json::json!({
        "header": { "event_id": "evt-1" },
        "event": {
            "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
            "message": {
                "message_id": "msg-1",
                "root_id": "root-1",
                "thread_id": "omt-1",
                "chat_id": "chat-1",
                "chat_type": "group",
                "content": "{\"text\":\"@_bot_a /close\"}",
                "mentions": [
                    { "key": "@_bot_a", "name": "BotA" }
                ]
            }
        }
    });
    let parsed = parse_lark_inbound_message(&payload).expect("parsed message");
    assert_eq!(parsed.event_id, "evt-1");
    assert_eq!(parsed.message_id, "msg-1");
    assert_eq!(parsed.chat_id, "chat-1");
    assert_eq!(parsed.scope, SessionScope::Thread);
    assert_eq!(parsed.anchor, "omt-1");
    assert_eq!(parsed.text, "/close");
    assert_eq!(parsed.sender_open_id.as_deref(), Some("ou_user"));
    assert_eq!(parsed.sender_type.as_deref(), Some("user"));
    assert_eq!(parsed.mentions.len(), 1);
}

#[test]
fn parse_lark_inbound_message_handles_quote_bubble_group_as_chat_scope() {
    let payload = serde_json::json!({
        "event": {
            "sender": { "sender_id": { "open_id": "ou_user" } },
            "message": {
                "message_id": "msg-2",
                "root_id": "root-quirk",
                "chat_id": "chat-2",
                "chat_type": "group",
                "content": "{\"text\":\"continue please\"}"
            }
        }
    });
    let parsed = parse_lark_inbound_message(&payload).expect("parsed message");
    assert_eq!(parsed.event_id, "msg-2");
    assert_eq!(parsed.scope, SessionScope::Chat);
    assert_eq!(parsed.anchor, "chat-2");
    assert_eq!(parsed.text, "continue please");
}

#[test]
fn parse_lark_inbound_message_rejects_missing_or_invalid_payload_bits() {
    let missing_message_id = serde_json::json!({
        "event": {
            "message": {
                "chat_id": "chat-1",
                "content": "{\"text\":\"hi\"}"
            }
        }
    });
    let err = parse_lark_inbound_message(&missing_message_id).expect_err("missing message_id");
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert_eq!(err.1, "missing message_id");

    let invalid_content = serde_json::json!({
        "event": {
            "message": {
                "message_id": "msg-3",
                "chat_id": "chat-3",
                "content": "{oops"
            }
        }
    });
    let err = parse_lark_inbound_message(&invalid_content).expect_err("invalid content");
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert!(err.1.starts_with("invalid content json: "));
}

#[test]
fn resolve_and_strip_leading_mentions_supports_lark_placeholder_keys() {
    let mentions = vec![LarkEventMention {
        key: "@_bot_a".to_string(),
        name: "BotA".to_string(),
    }];
    let resolved = resolve_lark_mentions("@_bot_a /close", &mentions);
    assert_eq!(resolved, "@BotA /close");
    assert_eq!(strip_leading_mentions(&resolved, &mentions), "/close");
}

#[test]
fn strip_leading_mentions_prefers_longer_names_in_multi_bot_chains() {
    let mentions = vec![
        LarkEventMention {
            key: "@_claude".to_string(),
            name: "Claude".to_string(),
        },
        LarkEventMention {
            key: "@_claude_clone".to_string(),
            name: "Claude分身".to_string(),
        },
        LarkEventMention {
            key: "@_coco".to_string(),
            name: "CoCo".to_string(),
        },
    ];
    let resolved = resolve_lark_mentions("@_claude @_claude_clone @_coco /close", &mentions);
    assert_eq!(strip_leading_mentions(&resolved, &mentions), "/close");
}

#[test]
fn strip_leading_mentions_leaves_non_prefix_mentions_in_place() {
    let mentions = vec![LarkEventMention {
        key: "@_bot_a".to_string(),
        name: "BotA".to_string(),
    }];
    let resolved = resolve_lark_mentions("hello @BotA how are you", &mentions);
    assert_eq!(
        strip_leading_mentions(&resolved, &mentions),
        "hello @BotA how are you"
    );
}

#[test]
fn parse_chat_info_mode_p2p_from_chat_mode() {
    assert_eq!(parse_chat_info_mode("p2p", ""), ChatMode::P2p);
    assert_eq!(parse_chat_info_mode("P2P", ""), ChatMode::P2p);
}

#[test]
fn parse_chat_info_mode_topic_from_chat_mode() {
    assert_eq!(parse_chat_info_mode("topic", ""), ChatMode::Topic);
    assert_eq!(parse_chat_info_mode("topic", "chat"), ChatMode::Topic);
}

#[test]
fn parse_chat_info_mode_topic_from_group_message_type() {
    assert_eq!(parse_chat_info_mode("group", "thread"), ChatMode::Topic);
    assert_eq!(
        parse_chat_info_mode("someUnknown", "thread"),
        ChatMode::Topic
    );
}

#[test]
fn parse_chat_info_mode_group_when_neither() {
    assert_eq!(parse_chat_info_mode("group", "chat"), ChatMode::Group);
    assert_eq!(parse_chat_info_mode("", ""), ChatMode::Group);
    assert_eq!(parse_chat_info_mode("group", ""), ChatMode::Group);
}

#[test]
fn parse_lark_inbound_message_uses_locale_field_not_text() {
    let payload = serde_json::json!({
        "header": { "event_id": "evt-locale" },
        "event": {
            "sender": {
                "sender_type": "user",
                "sender_id": { "open_id": "ou_user" }
            },
            "message": {
                "message_id": "msg-locale",
                "chat_id": "chat-locale",
                "chat_type": "group",
                "locale": "zh-CN",
                "content": "{\"text\":\"please investigate this\"}"
            }
        }
    });
    let parsed = parse_lark_inbound_message(&payload).expect("valid lark message");
    assert_eq!(parsed.text, "please investigate this");
    assert_eq!(parsed.locale.as_deref(), Some("zh"));
}

#[test]
fn parse_force_topic_invocation_t_only() {
    assert_eq!(parse_force_topic_invocation("/t"), Some(String::new()));
}

#[test]
fn parse_force_topic_invocation_t_with_content() {
    assert_eq!(
        parse_force_topic_invocation("/t hello world"),
        Some("hello world".to_string())
    );
}

#[test]
fn parse_force_topic_invocation_topic_only() {
    assert_eq!(parse_force_topic_invocation("/topic"), Some(String::new()));
}

#[test]
fn parse_force_topic_invocation_topic_with_content() {
    assert_eq!(
        parse_force_topic_invocation("/topic some question"),
        Some("some question".to_string())
    );
}

#[test]
fn parse_force_topic_invocation_no_match() {
    assert_eq!(parse_force_topic_invocation("hello"), None);
    assert_eq!(parse_force_topic_invocation("/slash not topic"), None);
    assert_eq!(parse_force_topic_invocation("/tsomething"), None);
}

#[test]
fn parse_force_topic_invocation_leading_whitespace() {
    assert_eq!(
        parse_force_topic_invocation("  /t hello"),
        Some("hello".to_string())
    );
}

#[test]
fn is_operate_command_recognizes_adopt_variants() {
    assert!(is_operate_command("/close"));
    assert!(is_operate_command("/restart"));
    assert!(is_operate_command("/card"));
    assert!(is_operate_command("/adopt"));
    assert!(is_operate_command("/adopt list"));
    assert!(is_operate_command("/adopt foo:bar"));
    assert!(is_operate_command("/adopt mysession"));
    assert!(is_operate_command("/adopt mysession:0.1"));
    assert!(is_operate_command("/adopt zellij foo:bar"));
    assert!(!is_operate_command("/adoption"));
    assert!(!is_operate_command("/adoptz"));
    assert!(!is_operate_command("hello"));
    assert!(!is_operate_command("/workflow run x"));
}

#[test]
fn chat_mode_from_str_maps_correctly() {
    assert_eq!(ChatMode::from("p2p"), ChatMode::P2p);
    assert_eq!(ChatMode::from("P2P"), ChatMode::P2p);
    assert_eq!(ChatMode::from("topic"), ChatMode::Topic);
    assert_eq!(ChatMode::from("group"), ChatMode::Group);
    assert_eq!(ChatMode::from(""), ChatMode::Group);
    assert_eq!(ChatMode::from("unknown"), ChatMode::Group);
}

#[tokio::test]
async fn send_input_keeps_live_card_when_turn_card_begin_fails() {
    let paths = temp_paths("send-input-turn-begin-fail");
    maybe_remove_dir(&paths.root().to_path_buf());

    let state = make_state(paths.clone(), HashMap::new());
    let mut session = make_session("sess-send-input");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.lark_app_id = "app-no-bot".to_string();
    session.stream_card_id = Some("om_live_old".to_string());
    session.stream_card_nonce = Some("nonce_live_old".to_string());
    session.current_screen = Some("old output".to_string());
    session.current_image_key = Some("img_live_old".to_string());
    session.last_screen_status = Some(ScreenStatus::Working);
    session.last_final_output_turn_id = Some("turn-old".to_string());
    session.last_cli_input = Some("previous input".to_string());
    {
        let mut sessions = state.sessions.lock().await;
        sessions.insert(session.session_id.clone(), session.clone());
    }

    let mut child = tokio::process::Command::new("/bin/cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn worker");
    let stdin = child.stdin.take().expect("worker stdin");
    {
        let mut workers = state.workers.lock().await;
        workers.insert(
            session.session_id.clone(),
            WorkerHandle {
                child,
                stdin: Arc::new(Mutex::new(stdin)),
            },
        );
    }

    let response = send_input(
        State(state.clone()),
        AxumPath(session.session_id.clone()),
        Json(SessionInputRequest {
            content: "hello".to_string(),
            raw: false,
        }),
    )
    .await;
    assert_eq!(response, Ok(StatusCode::ACCEPTED));

    let stored = {
        let sessions = state.sessions.lock().await;
        sessions
            .get(&session.session_id)
            .cloned()
            .expect("stored session")
    };
    assert_eq!(stored.last_cli_input.as_deref(), Some("hello"));
    assert_eq!(stored.stream_card_id.as_deref(), Some("om_live_old"));
    assert_eq!(stored.stream_card_nonce.as_deref(), Some("nonce_live_old"));
    assert_eq!(stored.current_screen.as_deref(), Some("old output"));
    assert_eq!(stored.current_image_key.as_deref(), Some("img_live_old"));
    assert_eq!(stored.last_screen_status, Some(ScreenStatus::Working));
    assert_eq!(
        stored.last_final_output_turn_id.as_deref(),
        Some("turn-old")
    );

    let mut worker = {
        let mut workers = state.workers.lock().await;
        workers.remove(&session.session_id).expect("worker handle")
    };
    let _ = worker.child.kill().await;
    let _ = worker.child.wait().await;

    maybe_remove_dir(&paths.root().to_path_buf());
}
