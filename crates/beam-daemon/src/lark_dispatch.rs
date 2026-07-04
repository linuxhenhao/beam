use super::*;

pub(crate) fn resolve_lark_card_action_session_id(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    action: &ParsedLarkCardAction,
) -> Option<String> {
    if let Some(session_id) = action.session_id.as_ref() {
        return Some(session_id.clone());
    }
    let root_id = action.root_id.as_ref()?;
    sessions
        .values()
        .find(|session| {
            session.lark_app_id == lark_app_id
                && session.status == SessionStatus::Active
                && session.root_message_id == *root_id
        })
        .map(|session| session.session_id.clone())
}

pub(crate) fn decide_lark_event_outcome(
    action: LarkTextAction,
    existing: Option<&Session>,
) -> LarkEventOutcome {
    let has_existing_session = existing.is_some();
    match action {
        LarkTextAction::Close => LarkEventOutcome::CloseSession {
            reply: if has_existing_session {
                "session closed"
            } else {
                "no active session"
            }
            .to_string(),
        },
        LarkTextAction::Restart => LarkEventOutcome::RestartSession {
            reply: if has_existing_session {
                "session restarted"
            } else {
                "no active session"
            }
            .to_string(),
        },
        LarkTextAction::Card => LarkEventOutcome::ShowCard {
            reply: if has_existing_session {
                "session card"
            } else {
                "no active session"
            }
            .to_string(),
        },
        LarkTextAction::AdoptZellij(target) => {
            if let Some(session) = existing.filter(|session| session.adopted_from.is_some()) {
                LarkEventOutcome::ReplyOnly {
                    reply: build_adopt_already_attached_reply(session),
                }
            } else {
                LarkEventOutcome::AdoptZellij { target }
            }
        }
        LarkTextAction::AdoptList => {
            if let Some(session) = existing.filter(|session| session.adopted_from.is_some()) {
                LarkEventOutcome::ReplyOnly {
                    reply: build_adopt_already_attached_reply(session),
                }
            } else {
                LarkEventOutcome::AdoptList
            }
        }
        LarkTextAction::PassthroughInput(text) => {
            if has_existing_session {
                LarkEventOutcome::PassthroughInput { text }
            } else {
                LarkEventOutcome::ReplyOnly {
                    reply: "this command requires an active CLI session".to_string(),
                }
            }
        }
        LarkTextAction::ReuseSessionInput => LarkEventOutcome::ReuseSession,
        LarkTextAction::CreateSession => LarkEventOutcome::CreateSession,
    }
}

pub(crate) fn lark_event_dedupe_key(app_id: &str, event_id: &str) -> Option<String> {
    let event_id = event_id.trim();
    if event_id.is_empty() {
        None
    } else {
        Some(format!("{}:{}", app_id, event_id))
    }
}

pub(crate) fn evaluate_lark_preflight(
    state: &AppState,
    bot: &BotConfig,
    text: &str,
    chat_id: &str,
    sender_open_id: Option<&str>,
    deduped: bool,
) -> LarkPreflight {
    if deduped {
        return LarkPreflight::Deduped;
    }
    if text.is_empty() {
        return LarkPreflight::IgnoredEmptyText;
    }

    let Some(sender) = sender_open_id else {
        if is_operate_command(text) {
            return LarkPreflight::Denied {
                reply: "permission denied: unknown sender",
            };
        }
        return LarkPreflight::Continue;
    };

    if is_operate_command(text) && !can_operate_bot_with_state(state, bot, Some(sender)) {
        return LarkPreflight::Denied {
            reply: "permission denied",
        };
    }

    let talk = evaluate_talk_for_bot_with_state(state, bot, chat_id, sender);
    if !talk.allowed {
        return LarkPreflight::Denied {
            reply: "permission denied: you are not authorized to talk to this bot",
        };
    }

    if grant_restricted(&talk, bot.restrict_grant_commands) && (text.starts_with('/')) {
        return LarkPreflight::Denied {
            reply: "slash commands are restricted for grant-authorized users",
        };
    }

    LarkPreflight::Continue
}

pub(crate) async fn handle_introduce_command(
    state: &AppState,
    app_id: &str,
    chat_id: &str,
    message_id: &str,
    parsed: &ParsedLarkInboundMessage,
) -> Result<bool, (StatusCode, String)> {
    if !parsed.text.trim_start().starts_with("/introduce") {
        return Ok(false);
    }
    let entries = parsed
        .mentions
        .iter()
        .filter_map(|mention| {
            let open_id = mention.key.trim();
            let name = mention.name.trim();
            if open_id.is_empty() || name.is_empty() {
                None
            } else {
                Some((open_id.to_string(), name.to_string()))
            }
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Ok(true);
    }
    record_observed_bots(&state.paths, app_id, chat_id, &entries, "introduce")
        .map_err(internal_error)?;
    let summary = entries
        .iter()
        .map(|(_, name)| format!("@{}", name))
        .collect::<Vec<_>>()
        .join(" ");
    let reply = if summary.is_empty() {
        "✅ 已认识本群伙伴".to_string()
    } else {
        format!("✅ 已认识本群 {}", summary)
    };
    let bot = state
        .bots
        .get(app_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "bot config not found".to_string()))?;
    let _ = lark_reply_message(state, &bot, message_id, &reply).await;
    Ok(true)
}

pub(crate) fn update_session_from_lark_message(
    session: &mut Session,
    parsed: &ParsedLarkInboundMessage,
) {
    session.quote_target_id = Some(parsed.message_id.clone());
    if session.thread_id.is_none() {
        if let Some(ref tid) = parsed.thread_id {
            session.thread_id = Some(tid.clone());
        }
    }
    if session.locale.is_none() {
        session.locale = Some(lark_locale_or_english(parsed.locale.as_deref()).to_string());
    }
}

pub(crate) fn resolve_existing_lark_session(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    parsed: &ParsedLarkInboundMessage,
) -> Option<Session> {
    sessions
        .values()
        .find(|session| {
            session.scope == parsed.scope
                && session_anchor_matches(session, lark_app_id, &parsed.chat_id, &parsed.anchor)
        })
        .cloned()
}

pub(crate) fn decide_lark_dispatch(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    parsed: &ParsedLarkInboundMessage,
) -> (Option<Session>, LarkEventOutcome) {
    let existing = resolve_existing_lark_session(sessions, lark_app_id, parsed);
    let action = classify_lark_text_action(&parsed.text, existing.is_some());
    let outcome = decide_lark_event_outcome(action, existing.as_ref());
    (existing, outcome)
}

#[cfg(test)]
pub(crate) fn session_for_lark_anchor(
    sessions: &HashMap<String, Session>,
    lark_app_id: &str,
    chat_id: &str,
    root_message_id: &str,
) -> Option<Session> {
    sessions
        .values()
        .find(|session| session_anchor_matches(session, lark_app_id, chat_id, root_message_id))
        .cloned()
}

pub(crate) fn active_anchor_owner(
    sessions: &HashMap<String, Session>,
    candidate: &Session,
) -> Option<String> {
    let anchor = match candidate.scope {
        SessionScope::Thread => candidate.thread_id.as_deref()?,
        SessionScope::Chat => &candidate.chat_id,
    };
    sessions
        .values()
        .find(|session| {
            session.session_id != candidate.session_id
                && session.scope == candidate.scope
                && session_anchor_matches(
                    session,
                    &candidate.lark_app_id,
                    &candidate.chat_id,
                    anchor,
                )
        })
        .map(|session| session.session_id.clone())
}

pub(crate) fn validate_resume_target(
    sessions: &HashMap<String, Session>,
    session_id: &str,
) -> Result<Session, (StatusCode, String)> {
    let session = sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "session not found".to_string()))?;
    if session.status != SessionStatus::Closed {
        return Err((StatusCode::CONFLICT, "session is not closed".to_string()));
    }
    if session.adopted_from.is_some() {
        return Err((
            StatusCode::CONFLICT,
            "adopted sessions cannot be resumed yet".to_string(),
        ));
    }
    if let Some(owner) = active_anchor_owner(sessions, &session) {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "session anchor is already owned by active session {}",
                owner
            ),
        ));
    }
    Ok(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn validate_resume_target_accepts_closed_non_adopted_session() {
        let candidate = make_session("closed-1");
        let sessions = HashMap::from([(candidate.session_id.clone(), candidate.clone())]);
        let resumed =
            validate_resume_target(&sessions, &candidate.session_id).expect("resume target");
        assert_eq!(resumed.session_id, candidate.session_id);
    }

    #[test]
    fn validate_resume_target_rejects_active_session() {
        let mut candidate = make_session("active-1");
        candidate.status = SessionStatus::Active;
        candidate.closed_at = None;
        let sessions = HashMap::from([(candidate.session_id.clone(), candidate)]);
        let err =
            validate_resume_target(&sessions, "active-1").expect_err("active session should fail");
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert_eq!(err.1, "session is not closed");
    }

    #[test]
    fn validate_resume_target_rejects_adopted_session() {
        let mut candidate = make_session("adopted-1");
        candidate.adopted_from = Some(AdoptedFrom {
            tmux_target: Some("0:1.0".to_string()),
            zellij_session: None,
            zellij_pane_id: None,
            original_cli_pid: 123,
            session_id: None,
            cli_id: Some("codex".to_string()),
            cwd: "/tmp/project".to_string(),
            pane_cols: Some(120),
            pane_rows: Some(40),
        });
        let sessions = HashMap::from([(candidate.session_id.clone(), candidate)]);
        let err = validate_resume_target(&sessions, "adopted-1")
            .expect_err("adopted session should fail");
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert_eq!(err.1, "adopted sessions cannot be resumed yet");
    }

    #[test]
    fn validate_resume_target_rejects_anchor_conflict() {
        let mut candidate = make_session("closed-1");
        candidate.thread_id = Some("thread-1".to_string());
        let mut owner = make_session("active-1");
        owner.status = SessionStatus::Active;
        owner.closed_at = None;
        owner.thread_id = Some("thread-1".to_string());

        let sessions = HashMap::from([
            (candidate.session_id.clone(), candidate),
            (owner.session_id.clone(), owner),
        ]);
        let err = validate_resume_target(&sessions, "closed-1").expect_err("conflict expected");
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert_eq!(
            err.1,
            "session anchor is already owned by active session active-1"
        );
    }

    #[test]
    fn validate_resume_target_ignores_other_scope_or_anchor() {
        let candidate = make_session("closed-1");
        let mut sibling = make_session("active-1");
        sibling.status = SessionStatus::Active;
        sibling.closed_at = None;
        sibling.scope = SessionScope::Chat;
        sibling.root_message_id = "other-root".to_string();

        let sessions = HashMap::from([
            (candidate.session_id.clone(), candidate.clone()),
            (sibling.session_id.clone(), sibling),
        ]);
        let resumed =
            validate_resume_target(&sessions, &candidate.session_id).expect("no conflict");
        assert_eq!(resumed.session_id, candidate.session_id);
    }

    #[test]
    fn session_for_lark_anchor_matches_thread_scope_by_thread_id() {
        // Thread-scoped sessions now match on thread_id, not root_message_id.
        let mut thread = make_session("thread-1");
        thread.status = SessionStatus::Active;
        thread.closed_at = None;
        thread.scope = SessionScope::Thread;
        thread.chat_id = "chat-a".to_string();
        thread.root_message_id = "root-a".to_string();
        thread.thread_id = Some("anchor-a".to_string());

        let sessions = HashMap::from([(thread.session_id.clone(), thread.clone())]);
        let found = session_for_lark_anchor(&sessions, "app-1", "chat-a", "anchor-a")
            .expect("thread session should match on thread_id");
        assert_eq!(found.session_id, thread.session_id);
        assert!(session_for_lark_anchor(&sessions, "app-1", "chat-a", "anchor-b").is_none());
    }

    #[test]
    fn decide_lark_event_outcome_reflects_existing_session_state() {
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::Close, Some(&make_session("sess-1"))),
            LarkEventOutcome::CloseSession {
                reply: "session closed".to_string()
            }
        );
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::Close, None),
            LarkEventOutcome::CloseSession {
                reply: "no active session".to_string()
            }
        );
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::Restart, Some(&make_session("sess-1"))),
            LarkEventOutcome::RestartSession {
                reply: "session restarted".to_string()
            }
        );
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::Restart, None),
            LarkEventOutcome::RestartSession {
                reply: "no active session".to_string()
            }
        );
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::Card, None),
            LarkEventOutcome::ShowCard {
                reply: "no active session".to_string()
            }
        );
        assert_eq!(
            decide_lark_event_outcome(
                LarkTextAction::ReuseSessionInput,
                Some(&make_session("sess-1"))
            ),
            LarkEventOutcome::ReuseSession
        );
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::CreateSession, None),
            LarkEventOutcome::CreateSession
        );
    }

    #[test]
    fn decide_lark_event_outcome_blocks_re_adopt_when_session_already_adopted() {
        let mut session = make_session("sess-1");
        session.adopted_from = Some(AdoptedFrom {
            tmux_target: Some("mysession:0.0".to_string()),
            zellij_session: None,
            zellij_pane_id: None,
            original_cli_pid: 12345,
            session_id: None,
            cli_id: Some("coco".to_string()),
            cwd: "/repo/project".to_string(),
            pane_cols: Some(120),
            pane_rows: Some(40),
        });
        assert_eq!(
            decide_lark_event_outcome(LarkTextAction::AdoptList, Some(&session)),
            LarkEventOutcome::ReplyOnly {
                reply: "session already adopted from coco (mysession:0.0)\ndisconnect it before running /adopt again".to_string()
            }
        );
        assert_eq!(
            decide_lark_event_outcome(
                LarkTextAction::AdoptZellij("0:2.0".to_string()),
                Some(&session)
            ),
            LarkEventOutcome::ReplyOnly {
                reply: "session already adopted from coco (mysession:0.0)\ndisconnect it before running /adopt again".to_string()
            }
        );
    }

    #[test]
    fn evaluate_lark_preflight_handles_dedupe_empty_and_permission_gate() {
        let paths = temp_paths("preflight");
        maybe_remove_dir(&paths.root().to_path_buf());
        let bot = BotConfig {
            name: None,
            lark_app_id: "app-1".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_owner".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        let state = make_state(
            paths.clone(),
            HashMap::from([(bot.lark_app_id.clone(), bot.clone())]),
        );
        assert_eq!(
            evaluate_lark_preflight(&state, &bot, "hello", "chat-1", Some("ou_owner"), true),
            LarkPreflight::Deduped
        );
        assert_eq!(
            evaluate_lark_preflight(&state, &bot, "", "chat-1", Some("ou_owner"), false),
            LarkPreflight::IgnoredEmptyText
        );
        assert_eq!(
            evaluate_lark_preflight(&state, &bot, "/close", "chat-1", Some("ou_other"), false),
            LarkPreflight::Denied {
                reply: "permission denied"
            }
        );
        assert_eq!(
            evaluate_lark_preflight(&state, &bot, "hello", "chat-1", Some("ou_other"), false),
            LarkPreflight::Denied {
                reply: "permission denied: you are not authorized to talk to this bot"
            }
        );
        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn resolve_lark_card_action_session_id_prefers_explicit_id_and_falls_back_to_root() {
        let mut session = make_session("sess-1");
        session.lark_app_id = "app-1".to_string();
        session.root_message_id = "om-root".to_string();
        session.status = SessionStatus::Active;
        session.closed_at = None;
        let sessions = HashMap::from([(session.session_id.clone(), session.clone())]);

        let direct = ParsedLarkCardAction {
            action: "restart".to_string(),
            session_id: Some("sess-explicit".to_string()),
            root_id: Some("om-root".to_string()),
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
            raw_value: None,
            ask_id: None,
            ask_nonce: None,
            ask_question_index: None,
            ask_key: None,
            ask_submit: false,
            pending_id: None,
            working_dir: None,
            dir_search_keyword: None,
        };
        assert_eq!(
            resolve_lark_card_action_session_id(&sessions, "app-1", &direct).as_deref(),
            Some("sess-explicit")
        );

        let fallback = ParsedLarkCardAction {
            action: "restart".to_string(),
            session_id: None,
            root_id: Some("om-root".to_string()),
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
            raw_value: None,
            ask_id: None,
            ask_nonce: None,
            ask_question_index: None,
            ask_key: None,
            ask_submit: false,
            pending_id: None,
            working_dir: None,
            dir_search_keyword: None,
        };
        assert_eq!(
            resolve_lark_card_action_session_id(&sessions, "app-1", &fallback).as_deref(),
            Some("sess-1")
        );
    }

    #[test]
    fn resolve_lark_card_action_session_id_ignores_other_apps_and_closed_sessions() {
        let mut active_other_app = make_session("sess-other");
        active_other_app.lark_app_id = "app-2".to_string();
        active_other_app.root_message_id = "om-root".to_string();

        let mut closed_same_app = make_session("sess-closed");
        closed_same_app.lark_app_id = "app-1".to_string();
        closed_same_app.root_message_id = "om-root".to_string();
        closed_same_app.status = SessionStatus::Closed;

        let sessions = HashMap::from([
            (active_other_app.session_id.clone(), active_other_app),
            (closed_same_app.session_id.clone(), closed_same_app),
        ]);
        let action = ParsedLarkCardAction {
            action: "close".to_string(),
            session_id: None,
            root_id: Some("om-root".to_string()),
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
            raw_value: None,
            ask_id: None,
            ask_nonce: None,
            ask_question_index: None,
            ask_key: None,
            ask_submit: false,
            pending_id: None,
            working_dir: None,
            dir_search_keyword: None,
        };
        assert_eq!(
            resolve_lark_card_action_session_id(&sessions, "app-1", &action),
            None
        );
    }

    #[test]
    fn decide_multibot_inbound_gate_requires_mention_for_foreign_bots() {
        assert!(!decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            false,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            None,
            "hello",
        ));
        assert!(decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            true,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            None,
            "hello",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_allows_single_user_group_without_mention() {
        assert!(decide_multibot_inbound_gate(
            Some("user"),
            Some("ou_user"),
            Some("ou_self"),
            false,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            Some(GroupStats {
                user_count: 1,
                bot_count: 1,
            }),
            "continue please",
        ));
        assert!(!decide_multibot_inbound_gate(
            Some("user"),
            Some("ou_user"),
            Some("ou_self"),
            false,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            Some(GroupStats {
                user_count: 3,
                bot_count: 2,
            }),
            "continue please",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_keeps_self_close_only() {
        assert!(decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_self"),
            Some("ou_self"),
            false,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            None,
            "/close",
        ));
        assert!(!decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_self"),
            Some("ou_self"),
            false,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            None,
            "status",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_allows_thread_scope_foreign_bot_with_mention() {
        assert!(decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            true,
            Some("group"),
            SessionScope::Thread,
            false,
            false,
            false,
            false,
            false,
            None,
            "hello",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_blocks_chat_scope_foreign_bot_without_grant() {
        assert!(!decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            true,
            Some("group"),
            SessionScope::Chat,
            false,
            false,
            false,
            false,
            false,
            None,
            "hello",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_allows_chat_scope_foreign_bot_with_chat_grant() {
        assert!(decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            true,
            Some("group"),
            SessionScope::Chat,
            false,
            false,
            false,
            true,
            false,
            None,
            "hello",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_allows_chat_scope_foreign_bot_if_owns_session() {
        assert!(decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            true,
            Some("group"),
            SessionScope::Chat,
            false,
            true,
            false,
            false,
            false,
            None,
            "hello",
        ));
    }

    #[test]
    fn decide_multibot_inbound_gate_allows_chat_scope_oncall_without_grant() {
        assert!(decide_multibot_inbound_gate(
            Some("bot"),
            Some("ou_peer"),
            Some("ou_self"),
            true,
            Some("group"),
            SessionScope::Chat,
            true,
            false,
            false,
            false,
            false,
            None,
            "hello",
        ));
    }

    #[test]
    fn decide_lark_dispatch_reuses_chat_scope_session_for_quote_bubble_messages() {
        let mut session = make_session("chat-session");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.scope = SessionScope::Chat;
        session.chat_id = "chat-2".to_string();
        session.root_message_id = "seed-root".to_string();

        let sessions = HashMap::from([(session.session_id.clone(), session.clone())]);
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-2".to_string(),
            message_id: "msg-2".to_string(),
            chat_id: "chat-2".to_string(),
            chat_type: Some("group".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Chat,
            anchor: "chat-2".to_string(),
            text: "continue please".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: None,
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert_eq!(
            existing.map(|session| session.session_id),
            Some("chat-session".to_string())
        );
        assert_eq!(outcome, LarkEventOutcome::ReuseSession);
    }

    #[test]
    fn decide_lark_dispatch_creates_new_thread_when_only_chat_scope_session_exists() {
        let mut chat_session = make_session("chat-session");
        chat_session.status = SessionStatus::Active;
        chat_session.closed_at = None;
        chat_session.scope = SessionScope::Chat;
        chat_session.chat_id = "chat-3".to_string();
        chat_session.root_message_id = "chat-3".to_string();

        let sessions = HashMap::from([(chat_session.session_id.clone(), chat_session)]);
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-3".to_string(),
            message_id: "msg-3".to_string(),
            chat_id: "chat-3".to_string(),
            chat_type: Some("group".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "real-topic-root".to_string(),
            text: "new topic please".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: None,
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(existing.is_none());
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_dispatch_reuses_topic_session_by_chat_id_without_thread_metadata() {
        // Without thread_id, Thread-scoped sessions no longer match
        // via chat_id fallback (the fallback was removed).  A new
        // session must be created.
        let mut topic_session = make_session("topic-session");
        topic_session.status = SessionStatus::Active;
        topic_session.closed_at = None;
        topic_session.scope = SessionScope::Thread;
        topic_session.chat_id = "topic-chat-1".to_string();
        topic_session.chat_type = Some("topic".to_string());
        topic_session.root_message_id = "first-topic-message".to_string();

        let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session)]);
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-topic-2".to_string(),
            message_id: "second-topic-message".to_string(),
            chat_id: "topic-chat-1".to_string(),
            chat_type: Some("topic".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "second-topic-message".to_string(),
            text: "same topic follow-up".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: None,
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(
            existing.is_none(),
            "without thread_id on either side, no match is expected"
        );
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_dispatch_does_not_reuse_group_forced_topic_by_chat_id() {
        let mut topic_session = make_session("topic-session");
        topic_session.status = SessionStatus::Active;
        topic_session.closed_at = None;
        topic_session.scope = SessionScope::Thread;
        topic_session.chat_id = "group-chat-1".to_string();
        topic_session.root_message_id = "first-forced-topic".to_string();

        let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session)]);
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-topic-2".to_string(),
            message_id: "second-forced-topic".to_string(),
            chat_id: "group-chat-1".to_string(),
            chat_type: Some("group".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "second-forced-topic".to_string(),
            text: "new forced topic".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: None,
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(existing.is_none());
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_dispatch_creates_new_session_when_thread_id_missing() {
        // When a topic session exists but has no thread_id, and a new
        // message arrives with root_id but no thread_id, the session
        // does NOT match (thread_id on session is None, anchor is a
        // message_id).  A new session is created.
        let mut topic_session = make_session("topic-session-id");
        topic_session.status = SessionStatus::Active;
        topic_session.closed_at = None;
        topic_session.scope = SessionScope::Thread;
        topic_session.chat_id = "topic-chat-reuse".to_string();
        topic_session.chat_type = Some("topic".to_string());
        topic_session.root_message_id = "first-topic-msg".to_string();

        let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-topic-2".to_string(),
            message_id: "second-topic-msg".to_string(),
            chat_id: "topic-chat-reuse".to_string(),
            chat_type: Some("topic".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "first-topic-msg".to_string(),
            text: "follow-up in same topic".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: Some("first-topic-msg".to_string()),
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(existing.is_none());
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_dispatch_reuses_topic_session_with_root_id_and_thread_id() {
        // Thread-scoped session with thread_id="omt_thread".  A new
        // message with the same thread_id should match.
        let mut topic_session = make_session("topic-session-full");
        topic_session.status = SessionStatus::Active;
        topic_session.closed_at = None;
        topic_session.scope = SessionScope::Thread;
        topic_session.chat_id = "topic-full-chat".to_string();
        topic_session.chat_type = Some("topic".to_string());
        topic_session.root_message_id = "topic-root-msg".to_string();
        topic_session.thread_id = Some("omt_thread".to_string());

        let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-full".to_string(),
            message_id: "later-msg".to_string(),
            chat_id: "topic-full-chat".to_string(),
            chat_type: Some("topic".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "omt_thread".to_string(),
            text: "later message".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: Some("topic-root-msg".to_string()),
            thread_id: Some("omt_thread".to_string()),
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert_eq!(
            existing.map(|session| session.session_id),
            Some("topic-session-full".to_string())
        );
        assert_eq!(outcome, LarkEventOutcome::ReuseSession);
    }

    #[test]
    fn decide_lark_dispatch_no_fallback_creates_session_when_anchor_mismatches() {
        // The chat_type-based fallback was removed.  Without thread_id
        // on the session, a new message cannot match even if it's in
        // the same chat.  A new session is created.
        let mut topic_session = make_session("topic-fb-session");
        topic_session.status = SessionStatus::Active;
        topic_session.closed_at = None;
        topic_session.scope = SessionScope::Thread;
        topic_session.chat_id = "topic-fb-chat".to_string();
        topic_session.chat_type = Some("topic".to_string());
        topic_session.root_message_id = "first-msg".to_string();

        let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-fb".to_string(),
            message_id: "second-msg".to_string(),
            chat_id: "topic-fb-chat".to_string(),
            chat_type: Some("topic".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "second-msg".to_string(),
            text: "another message".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: None,
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(
            existing.is_none(),
            "no fallback: without thread_id, new session is created"
        );
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_dispatch_creates_new_session_for_different_root_id_in_topic_chat() {
        // When a topic session exists with root_message_id "topic-a-root"
        // and a new message arrives in the SAME topic-mode chat but with
        // a DIFFERENT root_id ("topic-b-root"), the exact anchor match
        // fails.  Because root_id IS present (not None), the fallback by
        // chat_id should NOT trigger.  A new session should be created
        // so the different topic gets its own independent session and
        // directory selection.
        let mut topic_session = make_session("topic-existing");
        topic_session.status = SessionStatus::Active;
        topic_session.closed_at = None;
        topic_session.scope = SessionScope::Thread;
        topic_session.chat_id = "topic-multi".to_string();
        topic_session.chat_type = Some("topic".to_string());
        topic_session.root_message_id = "topic-a-root".to_string();

        let sessions = HashMap::from([(topic_session.session_id.clone(), topic_session.clone())]);

        // Message with different root_id — should NOT match the existing session.
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-diff-topic".to_string(),
            message_id: "topic-b-msg".to_string(),
            chat_id: "topic-multi".to_string(),
            chat_type: Some("topic".to_string()),
            sender_type: Some("user".to_string()),
            // anchor = root_id = "topic-b-root" (set by decide_lark_routing fix)
            scope: SessionScope::Thread,
            anchor: "topic-b-root".to_string(),
            text: "different topic message".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: Some("topic-b-root".to_string()),
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        // Exact match fails (root_message_id mismatch); root_id is Some
        // so fallback does NOT trigger.  Must create a new session.
        assert!(
            existing.is_none(),
            "different root_id should NOT reuse existing topic session"
        );
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_routing_uses_thread_id_as_authoritative_topic_signal() {
        // Non-p2p messages with thread_id use thread_id as anchor (stable
        // topic identifier), NOT root_id (which is for reply semantics).
        assert_eq!(
            decide_lark_routing(
                "msg-1",
                "chat-a",
                Some("group"),
                Some("real-topic-root"),
                Some("omt_topic")
            ),
            (SessionScope::Thread, "omt_topic")
        );
        // Group without thread_id stays Chat-scoped, even with root_id
        // (root_id alone is a quote reply, not a topic signal).
        assert_eq!(
            decide_lark_routing(
                "msg-1",
                "chat-a",
                Some("group"),
                Some("quote-bubble-root"),
                None
            ),
            (SessionScope::Chat, "chat-a")
        );
    }

    #[test]
    fn decide_lark_routing_keeps_p2p_and_topic_chats_thread_scoped() {
        // p2p always Thread-scoped with message_id anchor
        assert_eq!(
            decide_lark_routing("msg-dm", "chat-dm", Some("p2p"), None, None),
            (SessionScope::Thread, "msg-dm")
        );
        // chat_type="topic" is NOT a real Feishu receive_v1 field.
        // Without thread_id, it stays Chat-scoped (topic detection
        // happens later via get_lark_chat_mode()).
        assert_eq!(
            decide_lark_routing("msg-topic", "chat-topic", Some("topic"), None, None),
            (SessionScope::Chat, "chat-topic")
        );
    }

    #[test]
    fn session_for_lark_anchor_matches_chat_scope_without_root_message_match() {
        let mut chat = make_session("chat-1");
        chat.status = SessionStatus::Active;
        chat.closed_at = None;
        chat.scope = SessionScope::Chat;
        chat.chat_id = "chat-a".to_string();
        chat.root_message_id = "original-root".to_string();

        let sessions = HashMap::from([(chat.session_id.clone(), chat.clone())]);
        let found = session_for_lark_anchor(&sessions, "app-1", "chat-a", "different-root")
            .expect("chat session should match by chat only");
        assert_eq!(found.session_id, chat.session_id);
        assert!(session_for_lark_anchor(&sessions, "app-1", "chat-b", "different-root").is_none());
    }

    #[test]
    fn validate_resume_target_detects_chat_scope_anchor_conflict_by_chat_id() {
        let mut candidate = make_session("closed-chat");
        candidate.scope = SessionScope::Chat;
        candidate.chat_id = "chat-a".to_string();
        candidate.root_message_id = "closed-root".to_string();

        let mut owner = make_session("active-chat");
        owner.status = SessionStatus::Active;
        owner.closed_at = None;
        owner.scope = SessionScope::Chat;
        owner.chat_id = "chat-a".to_string();
        owner.root_message_id = "other-root".to_string();

        let sessions = HashMap::from([
            (candidate.session_id.clone(), candidate),
            (owner.session_id.clone(), owner),
        ]);
        let err = validate_resume_target(&sessions, "closed-chat")
            .expect_err("chat scope conflict expected");
        assert_eq!(err.0, StatusCode::CONFLICT);
        assert_eq!(
            err.1,
            "session anchor is already owned by active session active-chat"
        );
    }

    #[test]
    fn session_anchor_matches_thread_vs_chat_scope() {
        let mut session = make_session("sess-t1");
        session.lark_app_id = "app-1".to_string();
        session.status = SessionStatus::Active;
        session.scope = SessionScope::Thread;
        session.chat_id = "chat-1".to_string();
        session.root_message_id = "root-1".to_string();
        session.thread_id = Some("thread-1".to_string());
        // Thread scope matches on thread_id
        assert!(session_anchor_matches(
            &session, "app-1", "chat-1", "thread-1"
        ));
        assert!(!session_anchor_matches(
            &session, "app-1", "chat-1", "thread-9"
        ));

        session.scope = SessionScope::Chat;
        // Chat scope matches on chat_id only
        assert!(session_anchor_matches(
            &session,
            "app-1",
            "chat-1",
            "any-anchor"
        ));
        assert!(!session_anchor_matches(
            &session,
            "app-1",
            "chat-9",
            "any-anchor"
        ));
    }

    #[test]
    fn session_anchor_matches_p2p_falls_back_to_root_message_id() {
        // p2p first message session: Thread scope, thread_id=None,
        // root_message_id=message_id.  A follow-up p2p message with
        // root_id=message_id should match via the root_message_id fallback.
        let mut session = make_session("p2p-sess");
        session.lark_app_id = "app-1".to_string();
        session.status = SessionStatus::Active;
        session.scope = SessionScope::Thread;
        session.chat_id = "dm-chat".to_string();
        session.chat_type = Some("p2p".to_string());
        session.root_message_id = "first-msg".to_string();
        session.thread_id = None;

        // Follow-up with root_id=first-msg matches via root_message_id fallback.
        assert!(session_anchor_matches(
            &session,
            "app-1",
            "dm-chat",
            "first-msg"
        ));
        // Different root_id does NOT match.
        assert!(!session_anchor_matches(
            &session,
            "app-1",
            "dm-chat",
            "other-msg"
        ));
        // Different chat_id does NOT match.
        assert!(!session_anchor_matches(
            &session,
            "app-1",
            "other-chat",
            "first-msg"
        ));

        // After thread_id is backfilled, root_message_id fallback STILL works.
        // This is critical: p2p routing prefers root_id over thread_id, so
        // follow-ups that carry both root_id + thread_id will have anchor=root_id,
        // which must match root_message_id even though thread_id is now Some.
        session.thread_id = Some("omt_thread".to_string());
        assert!(session_anchor_matches(
            &session,
            "app-1",
            "dm-chat",
            "first-msg"
        ));
        // thread_id matching also works.
        assert!(session_anchor_matches(
            &session,
            "app-1",
            "dm-chat",
            "omt_thread"
        ));
        // A bogus anchor matches neither.
        assert!(!session_anchor_matches(
            &session, "app-1", "dm-chat", "bogus"
        ));

        // Non-p2p session with thread_id=None should NOT fall back to
        // root_message_id (only p2p sessions get the fallback).
        session.chat_type = Some("group".to_string());
        assert!(!session_anchor_matches(
            &session,
            "app-1",
            "dm-chat",
            "first-msg"
        ));
    }

    #[test]
    fn evaluate_talk_denies_unknown_sender_with_strict_bot() {
        let bot = BotConfig {
            name: None,
            lark_app_id: "app-1".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_owner".to_string()],
            private_card: false,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        let talk = evaluate_talk_for_bot(&bot, "chat-1", "ou_other");
        assert!(!talk.allowed);

        let owner_talk = evaluate_talk_for_bot(&bot, "chat-1", "ou_owner");
        assert!(owner_talk.allowed);
    }

    #[test]
    fn decide_lark_routing_topic_group_should_be_thread_scoped_with_message_id() {
        assert_eq!(
            decide_lark_routing("msg-1", "chat-a", Some("group"), None, None),
            (SessionScope::Chat, "chat-a")
        );
    }

    #[test]
    fn decide_lark_routing_p2p_always_thread_scoped() {
        assert_eq!(
            decide_lark_routing("msg-1", "chat-dm", Some("p2p"), None, None),
            (SessionScope::Thread, "msg-1")
        );
    }

    #[test]
    fn decide_lark_routing_with_thread_id_overrides_chat_type() {
        // Non-p2p with thread_id → Thread scope, anchor = thread_id
        assert_eq!(
            decide_lark_routing(
                "msg-1",
                "chat-a",
                Some("group"),
                Some("root-1"),
                Some("thread-1")
            ),
            (SessionScope::Thread, "thread-1")
        );
    }

    #[test]
    fn decide_lark_routing_p2p_uses_root_id_as_anchor_for_follow_ups() {
        // p2p with root_id && thread_id: root_id takes priority so the
        // follow-up can match the first message's session via root_message_id.
        // When root_id == message_id (self-root), the result is the same
        // as using message_id.
        assert_eq!(
            decide_lark_routing(
                "msg-1",
                "chat-dm",
                Some("p2p"),
                Some("msg-1"),
                Some("omt_thread")
            ),
            (SessionScope::Thread, "msg-1")
        );
        // When root_id != message_id (true reply/thread follow-up), use
        // root_id so it can match the first message's root_message_id.
        assert_eq!(
            decide_lark_routing(
                "msg-2",
                "chat-dm",
                Some("p2p"),
                Some("first-msg"),
                Some("omt_thread")
            ),
            (SessionScope::Thread, "first-msg")
        );
        // p2p with root_id but no thread_id: still use root_id as anchor.
        assert_eq!(
            decide_lark_routing("msg-3", "chat-dm", Some("p2p"), Some("first-msg"), None),
            (SessionScope::Thread, "first-msg")
        );
    }

    #[test]
    fn decide_lark_routing_p2p_with_thread_id_no_root_id_uses_thread_id() {
        // p2p message with thread_id but no root_id: use thread_id so events
        // after thread_id backfill can still match.
        assert_eq!(
            decide_lark_routing("msg-4", "chat-dm", Some("p2p"), None, Some("omt_thread")),
            (SessionScope::Thread, "omt_thread")
        );
    }

    #[test]
    fn decide_lark_routing_group_with_thread_id_uses_thread_id_as_anchor() {
        // Non-p2p with thread_id uses thread_id as anchor, not root_id.
        // The thread_id (omt_*) is the stable topic identifier.
        assert_eq!(
            decide_lark_routing(
                "msg-2",
                "chat-a",
                Some("group"),
                Some("topic-root"),
                Some("omt_topic")
            ),
            (SessionScope::Thread, "omt_topic")
        );
    }

    #[test]
    fn decide_lark_routing_topic_chat_type_without_thread_id_stays_chat_scoped() {
        // chat_type="topic" is NOT a real Feishu receive_v1 field.
        // Without thread_id, root_id alone is not a topic signal.
        // Topic detection happens later via get_lark_chat_mode().
        // The initial routing stays Chat-scoped.
        assert_eq!(
            decide_lark_routing(
                "msg-2",
                "topic-chat-1",
                Some("topic"),
                Some("first-topic-msg"),
                None
            ),
            (SessionScope::Chat, "topic-chat-1")
        );
    }

    #[test]
    fn decide_lark_routing_topic_chat_type_without_metadata_stays_chat_scoped() {
        // chat_type="topic" is NOT a real Feishu receive_v1 field.
        // Without thread_id or root_id, stays Chat-scoped.
        // Topic detection happens later via get_lark_chat_mode().
        assert_eq!(
            decide_lark_routing("msg-1", "topic-chat-2", Some("topic"), None, None),
            (SessionScope::Chat, "topic-chat-2")
        );
    }

    #[test]
    fn decide_lark_dispatch_creates_new_session_for_different_thread_id() {
        // Two messages in the same chat but with different thread_ids
        // must create separate sessions.
        let mut session_a = make_session("topic-a");
        session_a.status = SessionStatus::Active;
        session_a.closed_at = None;
        session_a.scope = SessionScope::Thread;
        session_a.chat_id = "multi-topic-chat".to_string();
        session_a.thread_id = Some("omt_topic_a".to_string());

        let sessions = HashMap::from([(session_a.session_id.clone(), session_a)]);

        // Message for a DIFFERENT topic thread in the same chat
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-diff-thread".to_string(),
            message_id: "msg-topic-b".to_string(),
            chat_id: "multi-topic-chat".to_string(),
            chat_type: Some("topic".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "omt_topic_b".to_string(),
            text: "different topic".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: Some("topic-b-root".to_string()),
            thread_id: Some("omt_topic_b".to_string()),
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(
            existing.is_none(),
            "different thread_id must create a new session"
        );
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_dispatch_p2p_follow_up_reuses_session_by_root_id() {
        // First p2p message: creates a session with root_message_id="msg-p2p-1",
        // thread_id=None, chat_type="p2p".
        let mut p2p_session = make_session("p2p-first");
        p2p_session.status = SessionStatus::Active;
        p2p_session.closed_at = None;
        p2p_session.scope = SessionScope::Thread;
        p2p_session.chat_id = "dm-chat".to_string();
        p2p_session.chat_type = Some("p2p".to_string());
        p2p_session.root_message_id = "msg-p2p-1".to_string();
        p2p_session.thread_id = None;

        let sessions = HashMap::from([(p2p_session.session_id.clone(), p2p_session)]);

        // Follow-up p2p message in the same thread: carries root_id pointing to
        // the first message.  Routing uses root_id as anchor, and
        // session_anchor_matches falls back to root_message_id because
        // thread_id is None.
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-p2p-2".to_string(),
            message_id: "msg-p2p-2".to_string(),
            chat_id: "dm-chat".to_string(),
            chat_type: Some("p2p".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "msg-p2p-1".to_string(), // root_id from routing
            text: "follow-up message".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: Some("msg-p2p-1".to_string()),
            root_id: Some("msg-p2p-1".to_string()),
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert_eq!(
            existing.as_ref().map(|s| s.session_id.as_str()),
            Some("p2p-first"),
            "p2p follow-up with root_id should reuse the existing session"
        );
        assert_eq!(outcome, LarkEventOutcome::ReuseSession);
    }

    #[test]
    fn decide_lark_dispatch_p2p_after_thread_id_backfill_still_reuses_by_root_id() {
        // After thread_id has been backfilled, a follow-up with root_id
        // (which routes to anchor=root_id) must still match via the
        // root_message_id fallback, NOT get blocked by thread_id mismatch.
        let mut p2p_session = make_session("p2p-backfilled");
        p2p_session.status = SessionStatus::Active;
        p2p_session.closed_at = None;
        p2p_session.scope = SessionScope::Thread;
        p2p_session.chat_id = "dm-chat".to_string();
        p2p_session.chat_type = Some("p2p".to_string());
        p2p_session.root_message_id = "first-msg".to_string();
        // thread_id was backfilled from a previous follow-up
        p2p_session.thread_id = Some("omt_thread".to_string());

        let sessions = HashMap::from([(p2p_session.session_id.clone(), p2p_session)]);

        // Another follow-up in the same thread: carries both root_id and
        // thread_id.  Routing prefers root_id → anchor="first-msg".
        // Must match via root_message_id fallback even though thread_id is
        // already Some (the old thread_id.is_none() guard would block this).
        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-p2p-3".to_string(),
            message_id: "msg-p2p-3".to_string(),
            chat_id: "dm-chat".to_string(),
            chat_type: Some("p2p".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "first-msg".to_string(), // root_id from routing
            text: "another follow-up".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: Some("msg-p2p-2".to_string()),
            root_id: Some("first-msg".to_string()),
            thread_id: Some("omt_thread".to_string()),
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert_eq!(
            existing.as_ref().map(|s| s.session_id.as_str()),
            Some("p2p-backfilled"),
            "p2p follow-up with root_id must reuse session even after thread_id backfill"
        );
        assert_eq!(outcome, LarkEventOutcome::ReuseSession);
    }

    #[test]
    fn decide_lark_dispatch_p2p_new_message_does_not_reuse_session() {
        // A fresh p2p message (no root_id/thread_id) must not reuse an
        // existing p2p session, even if it's in the same p2p chat.
        let mut p2p_session = make_session("p2p-existing");
        p2p_session.status = SessionStatus::Active;
        p2p_session.closed_at = None;
        p2p_session.scope = SessionScope::Thread;
        p2p_session.chat_id = "dm-chat".to_string();
        p2p_session.chat_type = Some("p2p".to_string());
        p2p_session.root_message_id = "old-msg".to_string();
        p2p_session.thread_id = None;

        let sessions = HashMap::from([(p2p_session.session_id.clone(), p2p_session)]);

        let parsed = ParsedLarkInboundMessage {
            event_id: "evt-p2p-new".to_string(),
            message_id: "new-msg".to_string(),
            chat_id: "dm-chat".to_string(),
            chat_type: Some("p2p".to_string()),
            sender_type: Some("user".to_string()),
            scope: SessionScope::Thread,
            anchor: "new-msg".to_string(), // message_id as anchor (no root_id)
            text: "brand new message".to_string(),
            sender_open_id: Some("ou_user".to_string()),
            mentions: Vec::new(),
            parent_id: None,
            root_id: None,
            thread_id: None,
            locale: None,
        };

        let (existing, outcome) = decide_lark_dispatch(&sessions, "app-1", &parsed);
        assert!(
            existing.is_none(),
            "p2p new message without root_id/thread_id must not reuse old session"
        );
        assert_eq!(outcome, LarkEventOutcome::CreateSession);
    }

    #[test]
    fn decide_lark_routing_group_with_root_id_but_no_thread_id_stays_chat_scoped() {
        // For group chats, root_id without thread_id is a quote-bubble
        // reply, not a topic message.  Must stay Chat-scoped so the
        // topic routing is not accidentally applied.
        assert_eq!(
            decide_lark_routing(
                "msg-1",
                "group-chat-1",
                Some("group"),
                Some("some-root"),
                None
            ),
            (SessionScope::Chat, "group-chat-1")
        );
    }

    #[test]
    fn handle_lark_event_uses_api_to_detect_topic_group() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let _env_lock = lark_base_url_env_lock().lock().expect("lark env lock");
            let base_url = start_mock_lark_server().await;
            let _env_guard = LarkBaseUrlEnvGuard::set(&base_url);

            let paths = temp_paths("detect-topic");
            maybe_remove_dir(&paths.root().to_path_buf());

            let app_id = "app-topic";
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
            let state = make_state(paths.clone(), HashMap::from([(app_id.to_string(), bot)]));

            let payload = serde_json::json!({
                "header": { "event_type": "im.message.receive_v1", "event_id": "evt-topic-1" },
                "event": {
                    "sender": { "sender_id": { "open_id": "ou_user" }, "sender_type": "user" },
                    "message": {
                        "message_id": "msg-topic-1",
                        "chat_id": "chat-topic-1",
                        "chat_type": "group",
                        "content": "{\"text\":\"hello\"}",
                        "mentions": []
                    }
                }
            });

            let result =
                handle_lark_event_payload(state.clone(), app_id.to_string(), payload, None).await;
            assert!(result.is_ok());

            // With directory selection, new sessions are NOT created immediately.
            // Instead a dir-select card is sent. Verify the pending entry was stored
            // with the correct Thread scope.
            let pending_creates = state.pending_creates.lock().await;
            assert!(
                !pending_creates.is_empty(),
                "pending create entry should be stored when no active session exists"
            );
            let pending = pending_creates.values().next().unwrap();
            assert_eq!(
                pending.scope,
                SessionScope::Thread,
                "pending create should have Thread scope when API detects topic group"
            );

            maybe_remove_dir(&paths.root().to_path_buf());
        });
    }
}
