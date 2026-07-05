use super::*;

pub(crate) async fn load_frozen_cards(
    paths: &BeamPaths,
    session_id: &str,
) -> Result<HashMap<String, FrozenCard>> {
    match tokio::fs::read(paths.frozen_cards_json(session_id)).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn save_frozen_cards(
    paths: &BeamPaths,
    session_id: &str,
    cards: &HashMap<String, FrozenCard>,
) -> Result<()> {
    tokio::fs::create_dir_all(paths.frozen_cards_dir()).await?;
    let path = paths.frozen_cards_json(session_id);
    if cards.is_empty() {
        match tokio::fs::remove_file(&path).await {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }
    let tmp = path.with_extension("json.tmp");
    let payload = serde_json::to_vec_pretty(cards)?;
    tokio::fs::write(&tmp, payload).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

pub(crate) async fn delete_frozen_cards(paths: &BeamPaths, session_id: &str) -> Result<()> {
    let path = paths.frozen_cards_json(session_id);
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) async fn remove_frozen_card(
    paths: &BeamPaths,
    session_id: &str,
    nonce: &str,
) -> Result<()> {
    let mut cards = load_frozen_cards(paths, session_id).await?;
    if cards.remove(nonce).is_some() {
        save_frozen_cards(paths, session_id, &cards).await?;
    }
    Ok(())
}

pub(crate) async fn lark_reply_card(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    card_json: &str,
) -> Result<String> {
    lark_reply_card_with_opts(state, bot, message_id, card_json, false).await
}

pub(crate) async fn lark_reply_card_with_opts(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    card_json: &str,
    reply_in_thread: bool,
) -> Result<String> {
    let cleaned_json = card_json.to_string();
    let token = lark_tenant_token(state, bot).await?;
    let mut body = serde_json::json!({
        "content": cleaned_json,
        "msg_type": "interactive",
    });
    if reply_in_thread {
        body.as_object_mut()
            .unwrap()
            .insert("reply_in_thread".to_string(), serde_json::Value::Bool(true));
    }
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages/{}/reply",
            lark_base_url(),
            message_id
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark reply card failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark reply card missing message_id")
}

pub(crate) async fn lark_update_card(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
    card_json: &str,
) -> Result<()> {
    let cleaned_json = card_json.to_string();

    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .patch(format!("{}/im/v1/messages/{}", lark_base_url(), message_id))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "content": cleaned_json,
            "msg_type": "interactive",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        if is_lark_message_withdrawn_payload(&payload) {
            anyhow::bail!("lark message withdrawn: {}", payload);
        }
        anyhow::bail!("lark update card failed: {}", payload);
    }
    Ok(())
}

pub(crate) async fn lark_send_open_id_card(
    state: &AppState,
    bot: &BotConfig,
    open_id: &str,
    card_json: &str,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .post(format!(
            "{}/im/v1/messages?receive_id_type=open_id",
            lark_base_url()
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "receive_id": open_id,
            "content": card_json,
            "msg_type": "interactive",
        }))
        .send()
        .await?;
    let status = resp.status();
    let payload = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("lark open_id card failed: {}", payload);
    }
    let value: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
    value
        .pointer("/data/message_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("lark open_id card missing message_id")
}

pub(crate) async fn lark_send_ephemeral_card(
    state: &AppState,
    bot: &BotConfig,
    chat_id: &str,
    open_id: &str,
    card_json: &str,
) -> Result<String> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .post(format!("{}/ephemeral/v1/send", lark_base_url()))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "open_id": open_id,
            "msg_type": "interactive",
            "card": card_json,
        }))
        .send()
        .await?;
    let status = resp.status();
    let body = resp.json::<LarkMessageResponse>().await?;
    if !status.is_success() || body.code.unwrap_or(0) != 0 {
        anyhow::bail!(
            "lark ephemeral card failed: {}",
            body.msg.unwrap_or_else(|| "unknown error".to_string())
        );
    }
    body.data
        .and_then(|data| data.message_id)
        .context("lark ephemeral card missing message_id")
}

pub(crate) async fn lark_delete_message(
    state: &AppState,
    bot: &BotConfig,
    message_id: &str,
) -> Result<bool> {
    let token = lark_tenant_token(state, bot).await?;
    let resp = state
        .http
        .delete(format!("{}/im/v1/messages/{}", lark_base_url(), message_id))
        .bearer_auth(token)
        .send()
        .await?;
    let body = resp.json::<LarkMessageResponse>().await?;
    Ok(body.code.unwrap_or(0) == 0)
}

pub(crate) fn private_card_delivery(chat_type: Option<&str>) -> PrivateCardDelivery {
    match chat_type.unwrap_or("group") {
        "group" => PrivateCardDelivery::Ephemeral,
        _ => PrivateCardDelivery::DirectMessage,
    }
}

pub(crate) fn resolve_private_card_audience(session: &Session, bot: &BotConfig) -> Vec<String> {
    let mut audience = Vec::new();
    if let Some(owner_open_id) = session.owner_open_id.as_ref() {
        audience.push(owner_open_id.clone());
    }
    for allowed in &bot.allowed_users {
        if !audience.iter().any(|existing| existing == allowed) {
            audience.push(allowed.clone());
        }
    }
    audience
}

pub(crate) fn ensure_stream_card_nonce(session: &mut Session) {
    if session.stream_card_nonce.is_none() {
        session.stream_card_nonce = Some(Uuid::new_v4().simple().to_string());
    }
}

pub(crate) async fn load_clicked_frozen_card(
    paths: &BeamPaths,
    session: &Session,
    clicked_nonce: Option<&str>,
) -> Result<Option<FrozenCard>> {
    let Some(clicked_nonce) = clicked_nonce else {
        return Ok(None);
    };
    if session.stream_card_nonce.as_deref() == Some(clicked_nonce) {
        return Ok(None);
    }
    let frozen_cards = load_frozen_cards(paths, &session.session_id).await?;
    Ok(frozen_cards.get(clicked_nonce).cloned())
}

pub(crate) async fn park_stream_card(paths: &BeamPaths, session: &Session) -> Result<()> {
    let Some(message_id) = session.stream_card_id.as_ref() else {
        return Ok(());
    };
    let Some(card_nonce) = session.stream_card_nonce.as_ref() else {
        return Ok(());
    };
    let mut frozen_cards = load_frozen_cards(paths, &session.session_id).await?;
    frozen_cards.insert(
        card_nonce.clone(),
        FrozenCard {
            message_id: message_id.clone(),
            content: session.current_screen.clone().unwrap_or_default(),
            title: session.title.clone(),
            display_mode: session.display_mode,
            image_key: session.current_image_key.clone(),
        },
    );
    save_frozen_cards(paths, &session.session_id, &frozen_cards).await
}

pub(crate) fn partition_frozen_cards_for_recall(
    frozen_cards: HashMap<String, FrozenCard>,
    active_id: Option<&str>,
) -> (HashMap<String, FrozenCard>, Vec<String>, bool) {
    let mut changed = false;
    let mut retained = HashMap::new();
    let mut to_delete = Vec::new();
    for (nonce, frozen) in frozen_cards {
        if active_id == Some(frozen.message_id.as_str()) {
            retained.insert(nonce, frozen);
            continue;
        }
        changed = true;
        to_delete.push(frozen.message_id);
    }
    (retained, to_delete, changed)
}

pub(crate) async fn recall_frozen_cards(state: &AppState, session: &Session) -> Result<()> {
    let frozen_cards = load_frozen_cards(&state.paths, &session.session_id).await?;
    if frozen_cards.is_empty() {
        return Ok(());
    }
    let active_id = session.stream_card_id.as_deref();
    let Some(bot) = state.bots.get(&session.lark_app_id) else {
        return Ok(());
    };
    let (retained, to_delete, changed) = partition_frozen_cards_for_recall(frozen_cards, active_id);
    for message_id in &to_delete {
        if let Err(err) = lark_delete_message(state, bot, message_id).await {
            warn!("failed to recall frozen card {}: {}", message_id, err);
        }
    }
    if changed {
        save_frozen_cards(&state.paths, &session.session_id, &retained).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;

    #[test]
    fn private_card_delivery_uses_ephemeral_for_group_only() {
        assert_eq!(
            private_card_delivery(Some("group")),
            PrivateCardDelivery::Ephemeral
        );
        assert_eq!(
            private_card_delivery(Some("p2p")),
            PrivateCardDelivery::DirectMessage
        );
        assert_eq!(
            private_card_delivery(Some("topic")),
            PrivateCardDelivery::DirectMessage
        );
        assert_eq!(private_card_delivery(None), PrivateCardDelivery::Ephemeral);
    }

    #[test]
    fn resolve_private_card_audience_prefers_owner_and_dedupes_allowed_users() {
        let mut session = make_session("sess-private");
        session.owner_open_id = Some("ou_owner".to_string());
        let bot = BotConfig {
            name: None,
            lark_app_id: "app".to_string(),
            lark_app_secret: "secret".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: None,
            cli_args: Vec::new(),
            skip_working_dir_prompt: false,
            model: None,
            working_dir: None,
            lark_encrypt_key: None,
            lark_verification_token: None,
            allowed_users: vec!["ou_owner".to_string(), "ou_peer".to_string()],
            private_card: true,
            allowed_chat_groups: Vec::new(),
            chat_grants: std::collections::HashMap::new(),
            global_grants: Vec::new(),
            oncall_chats: Vec::new(),
            restrict_grant_commands: false,
            message_quota: None,
            quota_state: std::collections::HashMap::new(),
        };
        assert_eq!(
            resolve_private_card_audience(&session, &bot),
            vec!["ou_owner".to_string(), "ou_peer".to_string()]
        );
    }

    #[test]
    fn is_stale_stream_card_action_rejects_mismatched_nonce_only_for_live_card_actions() {
        let mut session = make_session("sess-stale");
        session.stream_card_nonce = Some("nonce-current".to_string());

        let stale_toggle = ParsedLarkCardAction {
            action: "toggle_display".to_string(),
            session_id: Some("sess-stale".to_string()),
            root_id: Some("root-1".to_string()),
            clicked_message_id: None,
            operator_open_id: Some("ou_user".to_string()),
            term_key: None,
            visibility: None,
            card_nonce: Some("nonce-old".to_string()),
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
        assert!(is_stale_stream_card_action(&stale_toggle, &session));

        let compat_toggle = ParsedLarkCardAction {
            card_nonce: None,
            ..stale_toggle.clone()
        };
        assert!(!is_stale_stream_card_action(&compat_toggle, &session));

        let resume = ParsedLarkCardAction {
            action: "resume".to_string(),
            card_nonce: Some("nonce-old".to_string()),
            ..stale_toggle
        };
        assert!(!is_stale_stream_card_action(&resume, &session));
    }

    #[test]
    fn stale_stream_card_action_self_heal_is_toggle_only() {
        assert!(stale_stream_card_action_self_heals_live_session(
            "toggle_display"
        ));
        assert!(stale_stream_card_action_self_heals_live_session(
            "toggle_stream"
        ));
        assert!(!stale_stream_card_action_self_heals_live_session(
            "refresh_screenshot"
        ));
        assert!(!stale_stream_card_action_self_heals_live_session(
            "export_text"
        ));
    }

    #[test]
    fn resolve_card_render_target_patches_clicked_legacy_card_only() {
        let mut session = make_session("sess-render");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.stream_card_id = Some("om_live".to_string());

        let legacy_click = ParsedLarkCardAction {
            action: "toggle_display".to_string(),
            session_id: Some("sess-render".to_string()),
            root_id: Some("root-1".to_string()),
            clicked_message_id: Some("om_legacy".to_string()),
            operator_open_id: Some("ou_user".to_string()),
            term_key: None,
            visibility: None,
            card_nonce: Some("nonce-old".to_string()),
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
            resolve_card_render_target(&legacy_click, &session),
            CardRenderTarget::PatchMessage("om_legacy".to_string())
        );

        let current_click = ParsedLarkCardAction {
            clicked_message_id: Some("om_live".to_string()),
            ..legacy_click.clone()
        };
        assert_eq!(
            resolve_card_render_target(&current_click, &session),
            CardRenderTarget::CallbackRaw
        );

        let no_context = ParsedLarkCardAction {
            clicked_message_id: None,
            ..legacy_click
        };
        assert_eq!(
            resolve_card_render_target(&no_context, &session),
            CardRenderTarget::CallbackRaw
        );
    }

    #[test]
    fn decide_lark_card_delivery_distinguishes_not_ready_post_and_patch() {
        let mut session = make_session("sess-1");
        assert_eq!(
            decide_lark_card_delivery(&session),
            LarkCardDeliveryPlan::NotReady
        );
        assert_eq!(build_card_not_ready_reply(), "session card not ready");

        session.lark_app_id = "app-1".to_string();
        session.root_message_id = "root-1".to_string();
        session.terminal_url = Some("http://127.0.0.1:9000".to_string());
        assert_eq!(
            decide_lark_card_delivery(&session),
            LarkCardDeliveryPlan::PostNew
        );

        session.stream_card_id = Some("om_card_1".to_string());
        assert_eq!(
            decide_lark_card_delivery(&session),
            LarkCardDeliveryPlan::PatchExisting
        );
    }

    #[tokio::test]
    async fn park_stream_card_persists_frozen_snapshot() {
        let paths = temp_paths("park-frozen");
        maybe_remove_dir(&paths.root().to_path_buf());

        let mut session = make_session("sess-frozen");
        session.status = SessionStatus::Active;
        session.closed_at = None;
        session.stream_card_id = Some("om_card_old".to_string());
        session.stream_card_nonce = Some("nonce_old".to_string());
        session.current_screen = Some("old output".to_string());
        session.current_image_key = Some("img_old".to_string());
        session.display_mode = Some(DisplayMode::Screenshot);

        park_stream_card(&paths, &session)
            .await
            .expect("park succeeds");
        let frozen_cards = load_frozen_cards(&paths, &session.session_id)
            .await
            .expect("load succeeds");
        let frozen = frozen_cards.get("nonce_old").expect("frozen snapshot");
        assert_eq!(frozen.message_id, "om_card_old");
        assert_eq!(frozen.content, "old output");
        assert_eq!(frozen.image_key.as_deref(), Some("img_old"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn partition_frozen_cards_for_recall_deletes_all_without_active_card() {
        let mut cards = HashMap::new();
        cards.insert(
            "n1".to_string(),
            FrozenCard {
                message_id: "om_a".to_string(),
                content: String::new(),
                title: String::new(),
                display_mode: None,
                image_key: None,
            },
        );
        cards.insert(
            "n2".to_string(),
            FrozenCard {
                message_id: "om_b".to_string(),
                content: String::new(),
                title: String::new(),
                display_mode: None,
                image_key: None,
            },
        );

        let (retained, to_delete, changed) = partition_frozen_cards_for_recall(cards, None);
        assert!(changed);
        assert!(retained.is_empty());
        let mut ids = to_delete;
        ids.sort();
        assert_eq!(ids, vec!["om_a".to_string(), "om_b".to_string()]);
    }

    #[test]
    fn partition_frozen_cards_for_recall_preserves_active_entry() {
        let mut cards = HashMap::new();
        cards.insert(
            "nonce_active".to_string(),
            FrozenCard {
                message_id: "om_active".to_string(),
                content: String::new(),
                title: String::new(),
                display_mode: None,
                image_key: None,
            },
        );
        cards.insert(
            "nonce_old".to_string(),
            FrozenCard {
                message_id: "om_old".to_string(),
                content: String::new(),
                title: String::new(),
                display_mode: None,
                image_key: None,
            },
        );

        let (retained, to_delete, changed) =
            partition_frozen_cards_for_recall(cards, Some("om_active"));
        assert!(changed);
        assert_eq!(retained.len(), 1);
        assert!(retained.contains_key("nonce_active"));
        assert_eq!(to_delete, vec!["om_old".to_string()]);
    }

    #[test]
    fn partition_frozen_cards_for_recall_is_noop_when_only_active_entry_exists() {
        let mut cards = HashMap::new();
        cards.insert(
            "nonce_active".to_string(),
            FrozenCard {
                message_id: "om_active".to_string(),
                content: String::new(),
                title: String::new(),
                display_mode: None,
                image_key: None,
            },
        );

        let (retained, to_delete, changed) =
            partition_frozen_cards_for_recall(cards, Some("om_active"));
        assert!(!changed);
        assert_eq!(retained.len(), 1);
        assert!(to_delete.is_empty());
    }

    #[test]
    fn pending_response_state_tracks_open_and_patched_cards() {
        let mut session = make_session("sess-pending");
        session.status = SessionStatus::Active;
        session.closed_at = None;

        start_pending_response_turn(&mut session, "om_processing".to_string());
        assert_eq!(
            session.pending_response_card_id.as_deref(),
            Some("om_processing")
        );
        assert_eq!(
            session.pending_response_card_state,
            Some(PendingResponseCardState::Open)
        );
        assert!(is_pending_response_card_open(&session));
        assert_eq!(
            claim_pending_response_card(&session).as_deref(),
            Some("om_processing")
        );

        assert!(mark_pending_response_card_patched_if_current(
            &mut session,
            "om_processing"
        ));
        assert_eq!(session.pending_response_card_id, None);
        assert_eq!(
            session.pending_response_card_state,
            Some(PendingResponseCardState::Patched)
        );
        assert_eq!(
            session.last_patched_response_card_id.as_deref(),
            Some("om_processing")
        );
        assert!(!is_pending_response_card_open(&session));
    }

    #[test]
    fn pending_response_patch_guard_does_not_close_newer_card() {
        let mut session = make_session("sess-pending-guard");
        session.status = SessionStatus::Active;
        session.closed_at = None;

        start_pending_response_turn(&mut session, "om_new".to_string());
        assert!(!mark_pending_response_card_patched_if_current(
            &mut session,
            "om_old"
        ));
        assert_eq!(session.pending_response_card_id.as_deref(), Some("om_new"));
        assert_eq!(
            session.pending_response_card_state,
            Some(PendingResponseCardState::Open)
        );
        assert_eq!(session.last_patched_response_card_id, None);
    }

    #[test]
    fn claim_pending_response_card_requires_open_state() {
        let mut session = make_session("sess-claim");
        session.pending_response_card_id = Some("om_pending".to_string());
        session.pending_response_card_state = Some(PendingResponseCardState::Patched);
        assert_eq!(claim_pending_response_card(&session), None);

        session.pending_response_card_state = Some(PendingResponseCardState::Open);
        assert_eq!(
            claim_pending_response_card(&session).as_deref(),
            Some("om_pending")
        );
    }

    #[tokio::test]
    async fn pending_response_patch_marker_round_trips_and_clears() {
        let paths = temp_paths("pending-marker");
        maybe_remove_dir(&paths.root().to_path_buf());

        write_pending_response_patch_marker(&paths, "sess-1", "om_card")
            .await
            .expect("write marker");
        let marker = read_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("read marker")
            .expect("marker exists");
        assert_eq!(marker.session_id, "sess-1");
        assert_eq!(marker.card_id, "om_card");
        assert_eq!(marker.state, "patching");

        mark_pending_response_patch_marker_patched(&paths, "sess-1")
            .await
            .expect("promote marker");
        let patched = read_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("read patched")
            .expect("patched marker exists");
        assert_eq!(patched.state, "patched");
        assert!(patched.patched_at.is_some());

        clear_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("clear marker");
        let cleared = read_pending_response_patch_marker(&paths, "sess-1")
            .await
            .expect("read cleared");
        assert!(cleared.is_none());

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn pending_response_patch_marker_only_matches_same_card_when_patched() {
        let marker = PendingResponsePatchMarker {
            session_id: "sess-1".to_string(),
            card_id: "om_card".to_string(),
            state: "patched".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            patched_at: Some("2026-01-01T00:00:01Z".to_string()),
        };
        assert!(should_treat_pending_card_as_patched_by_marker(
            Some("om_card"),
            Some(&marker)
        ));
        assert!(!should_treat_pending_card_as_patched_by_marker(
            Some("om_other"),
            Some(&marker)
        ));

        let patching = PendingResponsePatchMarker {
            state: "patching".to_string(),
            ..marker
        };
        assert!(!should_treat_pending_card_as_patched_by_marker(
            Some("om_card"),
            Some(&patching)
        ));
        assert!(!should_treat_pending_card_as_patched_by_marker(
            None,
            Some(&patching)
        ));
    }

    #[test]
    fn clear_pending_response_tracking_resets_all_pending_fields() {
        let mut session = make_session("sess-clear-pending");
        session.pending_response_card_id = Some("om_pending".to_string());
        session.pending_response_card_state = Some(PendingResponseCardState::Open);
        session.last_patched_response_card_id = Some("om_done".to_string());

        clear_pending_response_tracking(&mut session);

        assert_eq!(session.pending_response_card_id, None);
        assert_eq!(session.pending_response_card_state, None);
        assert_eq!(session.last_patched_response_card_id, None);
    }
}
