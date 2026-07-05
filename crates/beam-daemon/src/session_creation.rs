use super::*;

pub(crate) struct SessionCreateSpec {
    pub(crate) title: String,
    pub(crate) chat_id: String,
    pub(crate) chat_type: Option<String>,
    pub(crate) root_message_id: String,
    pub(crate) quote_target_id: Option<String>,
    pub(crate) scope: SessionScope,
    pub(crate) thread_id: Option<String>,
    pub(crate) working_dir: String,
    pub(crate) cli_id: String,
    pub(crate) cli_bin: String,
    pub(crate) cli_args: Vec<String>,
    pub(crate) prompt: String,
    pub(crate) lark_app_id: String,
    pub(crate) owner_open_id: Option<String>,
    pub(crate) locale: Option<String>,
    pub(crate) adopted_from: Option<AdoptedFrom>,
}

pub(crate) fn next_session_turn_id() -> String {
    Uuid::new_v4().simple().to_string()
}

pub(crate) fn build_session_create_spec_from_bot(
    bot: &BotConfig,
    title: String,
    chat_id: String,
    chat_type: Option<String>,
    root_message_id: String,
    quote_target_id: Option<String>,
    scope: SessionScope,
    thread_id: Option<String>,
    working_dir: String,
    prompt: String,
    lark_app_id: String,
    owner_open_id: Option<String>,
    locale: Option<String>,
    adopted_from: Option<AdoptedFrom>,
) -> SessionCreateSpec {
    SessionCreateSpec {
        title,
        chat_id,
        chat_type,
        root_message_id,
        quote_target_id,
        scope,
        thread_id,
        working_dir,
        cli_id: bot.cli_id.clone(),
        cli_bin: bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
        cli_args: bot.cli_args.clone(),
        prompt,
        lark_app_id,
        owner_open_id,
        locale,
        adopted_from,
    }
}

pub(crate) fn build_session_create_spec_from_pending(
    pending: &dir_select::PendingCreateSession,
    title: String,
    chat_id: String,
    chat_type: Option<String>,
    root_message_id: String,
    quote_target_id: Option<String>,
    scope: SessionScope,
    thread_id: Option<String>,
    working_dir: String,
    prompt: String,
    lark_app_id: String,
    owner_open_id: Option<String>,
    locale: Option<String>,
    adopted_from: Option<AdoptedFrom>,
) -> SessionCreateSpec {
    SessionCreateSpec {
        title,
        chat_id,
        chat_type,
        root_message_id,
        quote_target_id,
        scope,
        thread_id,
        working_dir,
        cli_id: pending.cli_id.clone(),
        cli_bin: pending.cli_bin.clone(),
        cli_args: pending.cli_args.clone(),
        prompt,
        lark_app_id,
        owner_open_id,
        locale,
        adopted_from,
    }
}

fn resolve_direct_create_working_dir(bot: &BotConfig, daemon_working_dirs: &[String]) -> String {
    dir_select::determine_root_working_dir(bot.working_dir.as_deref(), daemon_working_dirs)
}

pub(crate) fn build_direct_create_session_spec_from_bot(
    bot: &BotConfig,
    daemon_working_dirs: &[String],
    title: String,
    chat_id: String,
    chat_type: Option<String>,
    root_message_id: String,
    quote_target_id: Option<String>,
    scope: SessionScope,
    thread_id: Option<String>,
    prompt: String,
    lark_app_id: String,
    owner_open_id: Option<String>,
    locale: Option<String>,
    adopted_from: Option<AdoptedFrom>,
) -> SessionCreateSpec {
    let working_dir = resolve_direct_create_working_dir(bot, daemon_working_dirs);
    SessionCreateSpec {
        title,
        chat_id,
        chat_type,
        root_message_id,
        quote_target_id,
        scope,
        thread_id,
        working_dir,
        cli_id: bot.cli_id.clone(),
        cli_bin: bot.cli_bin.clone().unwrap_or_else(|| bot.cli_id.clone()),
        cli_args: bot.cli_args.clone(),
        prompt,
        lark_app_id,
        owner_open_id,
        locale,
        adopted_from,
    }
}

pub(crate) async fn create_session_internal(
    state: &AppState,
    spec: SessionCreateSpec,
) -> Result<SessionSummary> {
    let session_id = Uuid::new_v4().to_string();
    let prompt_turn_id = (!spec.prompt.is_empty()).then(next_session_turn_id);

    // Resolve bot identity for this session (P2-9 off-topic hint support).
    // Skip for "local" sessions (no Lark backend).
    let (bot_name, bot_open_id) = if spec.lark_app_id == "local" {
        (None, None)
    } else {
        load_bot_identity(&state.paths, &spec.lark_app_id)
    };

    let session = Session {
        session_id: session_id.clone(),
        title: spec.title.clone(),
        chat_id: spec.chat_id.clone(),
        chat_type: spec.chat_type.clone(),
        root_message_id: spec.root_message_id.clone(),
        quote_target_id: spec.quote_target_id.clone(),
        scope: spec.scope,
        thread_id: spec.thread_id.clone(),
        status: SessionStatus::Active,
        created_at: Utc::now(),
        closed_at: None,
        working_dir: Some(spec.working_dir.clone()),
        lark_app_id: spec.lark_app_id.clone(),
        owner_open_id: spec.owner_open_id.clone(),
        quote_target_sender_open_id: spec.owner_open_id.clone(),
        worker_pid: None,
        cli_id: Some(spec.cli_id.clone()),
        cli_bin: Some(spec.cli_bin.clone()),
        cli_args: spec.cli_args.clone(),
        cli_session_id: None,
        last_cli_input: None,
        stream_card_id: None,
        stream_card_nonce: None,
        display_mode: None,
        current_screen: None,
        last_screen_status: None,
        usage_limit: None,
        current_image_key: None,
        tui_prompt_card_id: None,
        tui_prompt_options: Vec::new(),
        tui_prompt_multi_select: None,
        tui_toggled_indices: Vec::new(),
        pending_response_card_id: None,
        pending_response_card_state: None,
        last_patched_response_card_id: None,
        terminal_url: None,
        last_final_output_turn_id: None,
        last_final_output: None,
        last_explicit_send_at: None,
        adopted_from: spec.adopted_from.clone(),
        bot_name: bot_name.clone(),
        bot_open_id: bot_open_id.clone(),
        disable_cli_bypass: false,
        initial_prompt: None,
        model: None,
        locale: spec.locale.clone(),
        resume_session_id: None,
        agent_attention: None,
    };
    {
        let snapshot = {
            let mut sessions = state.sessions.lock().await;
            sessions.insert(session_id.clone(), session.clone());
            sessions.clone()
        };
        persist_sessions(&state.paths, &snapshot).await?;
    }
    let init = InitConfig {
        session_id,
        title: spec.title,
        chat_id: spec.chat_id,
        root_message_id: spec.root_message_id,
        working_dir: spec.working_dir,
        cli_id: spec.cli_id,
        cli_bin: spec.cli_bin,
        cli_args: spec.cli_args,
        prompt: spec.prompt.clone(),
        resume: false,
        cli_session_id: None,
        lark_app_secret: state
            .bots
            .get(&spec.lark_app_id)
            .map(|b| b.lark_app_secret.clone())
            .unwrap_or_default(),
        lark_app_id: spec.lark_app_id,
        prompt_turn_id,
        owner_open_id: spec.owner_open_id,
        adopted_from: spec.adopted_from,
        adopt_restored_from_metadata: false,
        screen_analyzer: state.config.screen_analyzer.clone(),
        bot_name: bot_name.clone(),
        bot_open_id: bot_open_id.clone(),
        disable_cli_bypass: false,
        initial_prompt: (!spec.prompt.is_empty()).then_some(spec.prompt),
        model: None,
        locale: spec.locale,
        resume_session_id: None,
    };
    spawn_worker(state.clone(), session.clone(), init).await?;
    Ok(SessionSummary::from(&session))
}

pub(crate) async fn await_session_final_output(
    state: &AppState,
    session_id: &str,
    timeout: Duration,
    cancel_token: Option<&tokio_util::sync::CancellationToken>,
) -> Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let snapshot = {
            let sessions = state.sessions.lock().await;
            sessions.get(session_id).cloned()
        };
        let Some(session) = snapshot else {
            anyhow::bail!("workflow session not found: {}", session_id);
        };
        if let Some(output) = session.last_final_output.clone() {
            return Ok(output);
        }
        if session.status == SessionStatus::Closed {
            anyhow::bail!(
                "workflow session closed before final output: {}",
                session_id
            );
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "workflow session timed out waiting for final output: {}",
                session_id
            );
        }
        let sleep = tokio::time::sleep(Duration::from_millis(200));
        if let Some(token) = cancel_token {
            tokio::select! {
                _ = token.cancelled() => {
                    anyhow::bail!("workflow activity cancelled");
                }
                _ = sleep => {}
            }
        } else {
            sleep.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::*;
    use beam_core::{
        ActivityState,
        workflow_snapshot::{ActivityStatus, CancelRequestState},
    };

    #[test]
    fn build_direct_create_session_spec_from_bot_prefers_bot_working_dir_and_cli_args() {
        let bot = BotConfig {
            cli_id: "traex".to_string(),
            cli_bin: Some("traex".to_string()),
            cli_args: vec!["-y".to_string()],
            skip_working_dir_prompt: true,
            working_dir: Some("/bot/work".to_string()),
            ..make_bot("app-spec")
        };
        let daemon_working_dirs = vec!["/daemon/work".to_string()];
        let spec = build_direct_create_session_spec_from_bot(
            &bot,
            &daemon_working_dirs,
            "title".to_string(),
            "chat".to_string(),
            Some("group".to_string()),
            "root".to_string(),
            Some("quote".to_string()),
            SessionScope::Thread,
            Some("omt_1".to_string()),
            "prompt".to_string(),
            "app-spec".to_string(),
            Some("ou_owner".to_string()),
            None,
            None,
        );
        assert_eq!(spec.cli_id, "traex");
        assert_eq!(spec.cli_bin, "traex");
        assert_eq!(spec.cli_args, vec!["-y".to_string()]);
        assert_eq!(spec.working_dir, "/bot/work");
    }

    #[test]
    fn build_direct_create_session_spec_from_bot_falls_back_to_daemon_working_dir() {
        let bot = BotConfig {
            cli_id: "codex".to_string(),
            cli_bin: Some("codex".to_string()),
            cli_args: vec![],
            skip_working_dir_prompt: true,
            working_dir: None,
            ..make_bot("app-spec-fallback")
        };
        let daemon_working_dirs = vec!["/daemon/work".to_string()];
        let spec = build_direct_create_session_spec_from_bot(
            &bot,
            &daemon_working_dirs,
            "title".to_string(),
            "chat".to_string(),
            Some("group".to_string()),
            "root".to_string(),
            None,
            SessionScope::Thread,
            Some("omt_1".to_string()),
            "prompt".to_string(),
            "app-spec-fallback".to_string(),
            None,
            None,
            None,
        );
        assert_eq!(spec.working_dir, "/daemon/work");
        assert!(spec.cli_args.is_empty());
    }

    #[test]
    fn effective_history_scope_defaults_from_session_scope() {
        let mut session = make_session("scope-thread");
        session.scope = SessionScope::Thread;
        assert_eq!(effective_history_scope(&session, None).unwrap(), "thread");
        assert_eq!(
            effective_history_scope(&session, Some("ambient")).unwrap(),
            "ambient"
        );

        session.scope = SessionScope::Chat;
        assert_eq!(effective_history_scope(&session, None).unwrap(), "chat");
        assert!(effective_history_scope(&session, Some("thread")).is_err());
        assert!(effective_history_scope(&session, Some("bad")).is_err());
    }

    #[test]
    fn append_resume_wait_recovery_uses_decision_node_success_path() {
        let paths = temp_paths("wait-recovery");
        let mut log = EventLog::new("run-wait", paths.workflow_runs_dir()).expect("log");
        let workflow_def = test_decision_workflow();
        let activity = ActivityState {
            activity_id: "activity-1".to_string(),
            attempts: vec![test_attempt(
                "attempt-1",
                ActivityStatus::Waiting,
                Some(beam_core::WaitState {
                    wait_kind: "human-gate".to_string(),
                    deadline_at: None,
                    prompt: Some("approve?".to_string()),
                    prompt_ref: None,
                    prompt_preview: None,
                    approvers: None,
                    on_timeout: Some("fail".to_string()),
                    resolution: Some(beam_core::WaitResolutionState {
                        kind: "resolved".to_string(),
                        resolution: Some("rejected".to_string()),
                        by: Some("alice".to_string()),
                        comment: Some("ok".to_string()),
                        event_id: Some("resume-1".to_string()),
                        deadline_at: None,
                        exceeded_at_ms: None,
                    }),
                }),
                None,
                None,
                None,
            )],
            status: ActivityStatus::Waiting,
            current_attempt_id: Some("attempt-1".to_string()),
            owner_node_id: Some("decision".to_string()),
        };

        let outcome = append_resume_wait_recovery(&mut log, &workflow_def, &activity)
            .expect("recovery")
            .expect("some");
        assert_eq!(outcome["kind"], "succeeded");
        assert_eq!(outcome["source"], "resolved");
        let events = log.read_all().expect("events");
        assert_eq!(events[0].event_type, "activitySucceeded");
    }

    #[test]
    fn append_resume_cancel_and_worker_crashed_recovery_write_terminals() {
        let paths = temp_paths("cancel-worker");
        let mut log = EventLog::new("run-cancel", paths.workflow_runs_dir()).expect("log");
        let cancel_activity = ActivityState {
            activity_id: "activity-cancel".to_string(),
            attempts: vec![test_attempt(
                "attempt-cancel",
                ActivityStatus::Running,
                None,
                None,
                Some(CancelRequestState {
                    cancel_origin_event_id: "cancel-1".to_string(),
                    requested_by: "alice".to_string(),
                    reason: "stop".to_string(),
                    delivered: false,
                }),
                None,
            )],
            status: ActivityStatus::Running,
            current_attempt_id: Some("attempt-cancel".to_string()),
            owner_node_id: None,
        };
        let worker_activity = ActivityState {
            activity_id: "activity-worker".to_string(),
            attempts: vec![test_attempt(
                "attempt-worker",
                ActivityStatus::Running,
                None,
                None,
                None,
                None,
            )],
            status: ActivityStatus::Running,
            current_attempt_id: Some("attempt-worker".to_string()),
            owner_node_id: None,
        };
        let event_index = HashMap::new();
        let cancel_outcome =
            append_resume_cancel_recovery(&mut log, &event_index, &cancel_activity)
                .expect("cancel")
                .expect("some");
        let worker_outcome = append_resume_worker_crashed(&mut log, &worker_activity)
            .expect("worker")
            .expect("some");
        assert_eq!(cancel_outcome["kind"], "cancelled");
        assert_eq!(worker_outcome["terminalEvent"]["type"], "activityFailed");
        let events = log.read_all().expect("events");
        assert_eq!(events[0].event_type, "activityCanceled");
        assert_eq!(events[1].event_type, "activityFailed");
    }

    #[tokio::test]
    async fn attempt_resume_sidecar_tracks_ready_state_and_key_is_stable() {
        let paths = temp_paths("attempt-resume-sidecar");
        maybe_remove_dir(&paths.root().to_path_buf());

        let key = attempt_resume_key("run-1", "activity-1", "attempt-1");
        assert_eq!(key, "run-1\nactivity-1\nattempt-1");

        let entry = AttemptResumeEntry {
            resume_id: "resume-1".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "activity-1".to_string(),
            attempt_id: "attempt-1".to_string(),
            session_id: "session-1".to_string(),
            original_session_id: "orig-session-1".to_string(),
            cli_session_id: Some("cli-session-1".to_string()),
            lark_app_id: "cli-app".to_string(),
            bot_name: Some("Bot".to_string()),
            cli_id: "claude-code".to_string(),
            working_dir: "/tmp".to_string(),
            log_path: paths
                .workflow_run_dir("run-1")
                .join("attempts/activity-1/attempt-1/resumes/resume-1/terminal.log")
                .display()
                .to_string(),
            sidecar_path: paths
                .attempt_resume_json("run-1", "activity-1", "attempt-1", "resume-1")
                .display()
                .to_string(),
            started_at: 123,
            updated_at: 123,
            web_port: Some(9123),
            write_token: Some("token-1".to_string()),
            close_reason: None,
        };
        write_attempt_resume_sidecar(&paths, &entry, "live")
            .await
            .expect("write sidecar");

        let sidecar_path =
            paths.attempt_resume_json("run-1", "activity-1", "attempt-1", "resume-1");
        let sidecar: AttemptResumeSidecar = serde_json::from_str(
            &tokio::fs::read_to_string(&sidecar_path)
                .await
                .expect("read sidecar"),
        )
        .expect("parse sidecar");
        assert_eq!(sidecar.status, "live");
        assert_eq!(sidecar.web_port, Some(9123));
        assert_eq!(sidecar.write_token.as_deref(), Some("token-1"));

        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::from([(key.clone(), entry.clone())]))),
            shutdown: Arc::new(Mutex::new(None)),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        };
        match wait_for_attempt_resume_ready(&state, &key, &entry.sidecar_path).await {
            AttemptResumeWaitOutcome::Ready(ready) => {
                assert_eq!(ready.web_port, Some(9123));
                assert_eq!(ready.write_token.as_deref(), Some("token-1"));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn attempt_resume_waits_for_inflight_entry_to_become_ready() {
        let paths = temp_paths("attempt-resume-ready-wait");
        maybe_remove_dir(&paths.root().to_path_buf());
        let key = attempt_resume_key("run-2", "activity-2", "attempt-2");
        let entry = AttemptResumeEntry {
            resume_id: "resume-2".to_string(),
            run_id: "run-2".to_string(),
            activity_id: "activity-2".to_string(),
            attempt_id: "attempt-2".to_string(),
            session_id: "session-2".to_string(),
            original_session_id: "orig-session-2".to_string(),
            cli_session_id: None,
            lark_app_id: "cli-app".to_string(),
            bot_name: None,
            cli_id: "claude-code".to_string(),
            working_dir: "/tmp".to_string(),
            log_path: paths
                .workflow_run_dir("run-2")
                .join("attempts/activity-2/attempt-2/resumes/resume-2/terminal.log")
                .display()
                .to_string(),
            sidecar_path: paths
                .attempt_resume_json("run-2", "activity-2", "attempt-2", "resume-2")
                .display()
                .to_string(),
            started_at: 123,
            updated_at: 123,
            web_port: None,
            write_token: None,
            close_reason: None,
        };
        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::from([(key.clone(), entry.clone())]))),
            shutdown: Arc::new(Mutex::new(None)),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        };
        let state_for_update = state.clone();
        let key_for_update = key.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut resumes = state_for_update.attempt_resumes.lock().await;
            if let Some(existing) = resumes.get_mut(&key_for_update) {
                existing.web_port = Some(9124);
                existing.write_token = Some("token-2".to_string());
            }
        });

        match wait_for_attempt_resume_ready(&state, &key, &entry.sidecar_path).await {
            AttemptResumeWaitOutcome::Ready(ready) => {
                assert_eq!(ready.web_port, Some(9124));
                assert_eq!(ready.write_token.as_deref(), Some("token-2"));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn attempt_resume_sidecar_falls_back_to_terminal_bot_name() {
        let paths = temp_paths("attempt-resume-bot-name");
        maybe_remove_dir(&paths.root().to_path_buf());

        let entry = AttemptResumeEntry {
            resume_id: "resume-3".to_string(),
            run_id: "run-3".to_string(),
            activity_id: "activity-3".to_string(),
            attempt_id: "attempt-3".to_string(),
            session_id: "session-3".to_string(),
            original_session_id: "orig-session-3".to_string(),
            cli_session_id: None,
            lark_app_id: "cli-app".to_string(),
            bot_name: Some("Terminal Bot".to_string()),
            cli_id: "claude-code".to_string(),
            working_dir: "/tmp".to_string(),
            log_path: paths
                .workflow_run_dir("run-3")
                .join("attempts/activity-3/attempt-3/resumes/resume-3/terminal.log")
                .display()
                .to_string(),
            sidecar_path: paths
                .attempt_resume_json("run-3", "activity-3", "attempt-3", "resume-3")
                .display()
                .to_string(),
            started_at: 123,
            updated_at: 123,
            web_port: Some(9125),
            write_token: Some("token-3".to_string()),
            close_reason: None,
        };
        write_attempt_resume_sidecar(&paths, &entry, "live")
            .await
            .expect("write sidecar");
        let sidecar_path =
            paths.attempt_resume_json("run-3", "activity-3", "attempt-3", "resume-3");
        let sidecar: AttemptResumeSidecar = serde_json::from_str(
            &tokio::fs::read_to_string(&sidecar_path)
                .await
                .expect("read sidecar"),
        )
        .expect("parse sidecar");
        assert_eq!(sidecar.bot_name.as_deref(), Some("Terminal Bot"));

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn attempt_resume_wait_reports_closed_sidecar_failure() {
        let paths = temp_paths("attempt-resume-closed");
        maybe_remove_dir(&paths.root().to_path_buf());
        let key = attempt_resume_key("run-4", "activity-4", "attempt-4");
        let sidecar_path =
            paths.attempt_resume_json("run-4", "activity-4", "attempt-4", "resume-4");
        let entry = AttemptResumeEntry {
            resume_id: "resume-4".to_string(),
            run_id: "run-4".to_string(),
            activity_id: "activity-4".to_string(),
            attempt_id: "attempt-4".to_string(),
            session_id: "session-4".to_string(),
            original_session_id: "orig-session-4".to_string(),
            cli_session_id: None,
            lark_app_id: "cli-app".to_string(),
            bot_name: None,
            cli_id: "claude-code".to_string(),
            working_dir: "/tmp".to_string(),
            log_path: sidecar_path.display().to_string(),
            sidecar_path: sidecar_path.display().to_string(),
            started_at: 123,
            updated_at: 123,
            web_port: None,
            write_token: None,
            close_reason: Some("worker_error".to_string()),
        };
        write_attempt_resume_sidecar(&paths, &entry, "closed")
            .await
            .expect("write closed sidecar");
        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(Mutex::new(None)),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        };
        match wait_for_attempt_resume_ready(&state, &key, &entry.sidecar_path).await {
            AttemptResumeWaitOutcome::Failed { error, .. } => {
                assert_eq!(error, "worker_error");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[tokio::test]
    async fn attempt_resume_wait_defaults_closed_sidecar_without_reason_to_exit_before_ready() {
        let paths = temp_paths("attempt-resume-closed-default");
        maybe_remove_dir(&paths.root().to_path_buf());
        let key = attempt_resume_key("run-5", "activity-5", "attempt-5");
        let sidecar_path =
            paths.attempt_resume_json("run-5", "activity-5", "attempt-5", "resume-5");
        let entry = AttemptResumeEntry {
            resume_id: "resume-5".to_string(),
            run_id: "run-5".to_string(),
            activity_id: "activity-5".to_string(),
            attempt_id: "attempt-5".to_string(),
            session_id: "session-5".to_string(),
            original_session_id: "orig-session-5".to_string(),
            cli_session_id: None,
            lark_app_id: "cli-app".to_string(),
            bot_name: None,
            cli_id: "claude-code".to_string(),
            working_dir: "/tmp".to_string(),
            log_path: sidecar_path.display().to_string(),
            sidecar_path: sidecar_path.display().to_string(),
            started_at: 123,
            updated_at: 123,
            web_port: None,
            write_token: None,
            close_reason: None,
        };
        write_attempt_resume_sidecar(&paths, &entry, "closed")
            .await
            .expect("write closed sidecar");
        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(Mutex::new(None)),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::new()),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        };
        match wait_for_attempt_resume_ready(&state, &key, &entry.sidecar_path).await {
            AttemptResumeWaitOutcome::Failed { error, .. } => {
                assert_eq!(error, "worker_exited_before_ready");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        maybe_remove_dir(&paths.root().to_path_buf());
    }

    #[test]
    fn parse_attempt_resume_request_body_accepts_empty_and_rejects_bad_json() {
        let empty = parse_attempt_resume_request_body(b"").expect("empty body");
        assert!(empty.reason.is_none());

        let bad = parse_attempt_resume_request_body(b"{not-json");
        assert!(matches!(bad, Err((StatusCode::BAD_REQUEST, ref err)) if err == "bad_json"));
    }

    /// Verify that `create_session_internal` populates `bot_name` / `bot_open_id`
    /// from `bots-info.json` when creating a Lark session. This is needed for the
    /// P2-9 off-topic sub-bot hint to work at runtime.
    #[tokio::test]
    async fn create_session_internal_populates_bot_identity() {
        let paths = temp_paths("bot-identity");
        maybe_remove_dir(&paths.root().to_path_buf());

        // Write bots-info.json with known bot identity
        let bots_info = serde_json::json!([{
            "larkAppId": "app-bot-id-test",
            "botName": "TestBot",
            "botOpenId": "ou_test_bot_789",
        }]);
        std::fs::create_dir_all(paths.root()).expect("create temp root");
        std::fs::write(
            paths.root().join("bots-info.json"),
            serde_json::to_string_pretty(&bots_info).unwrap(),
        )
        .expect("write bots-info.json");

        let state = AppState {
            paths: paths.clone(),
            started_at: Utc::now(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(Mutex::new(None)),
            options: RunOptions {
                worker_exe: PathBuf::from("/bin/true"),
            },
            http: Client::new(),
            config: Config::default(),
            bots: Arc::new(HashMap::from([(
                "app-bot-id-test".to_string(),
                make_bot("app-bot-id-test"),
            )])),
            lark_tokens: Arc::new(Mutex::new(HashMap::new())),
            chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
            recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
            inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
            workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
            ask_pending: Arc::new(Mutex::new(HashMap::new())),
            grant_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_creates: Arc::new(Mutex::new(HashMap::new())),
            dashboard_token: Arc::new(Mutex::new(None)),
            external_host: "localhost".to_string(),
        };

        let spec = SessionCreateSpec {
            title: "Test Session".to_string(),
            chat_id: "chat-test".to_string(),
            chat_type: None,
            root_message_id: "root-test".to_string(),
            quote_target_id: None,
            scope: SessionScope::Thread,
            thread_id: None,
            working_dir: "/tmp/test-bot-identity".to_string(),
            cli_id: "codex".to_string(),
            cli_bin: "codex".to_string(),
            cli_args: vec![],
            prompt: "hello".to_string(),
            lark_app_id: "app-bot-id-test".to_string(),
            owner_open_id: None,
            locale: None,
            adopted_from: None,
        };

        let summary = create_session_internal(&state, spec)
            .await
            .expect("create_session_internal should succeed");

        // Verify session persisted with bot identity
        let sessions = state.sessions.lock().await;
        let session = sessions
            .get(&summary.session_id)
            .expect("session should exist in state");
        assert_eq!(
            session.bot_name.as_deref(),
            Some("TestBot"),
            "bot_name should be populated from bots-info.json"
        );
        assert_eq!(
            session.bot_open_id.as_deref(),
            Some("ou_test_bot_789"),
            "bot_open_id should be populated from bots-info.json"
        );

        maybe_remove_dir(&paths.root().to_path_buf());
    }
}
