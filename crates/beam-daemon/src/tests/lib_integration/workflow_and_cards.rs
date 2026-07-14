use super::*;

#[tokio::test]
async fn list_workflow_definitions_prefers_first_search_path_and_hashes_canonically() {
    let paths = temp_paths("workflow-defs");
    maybe_remove_dir(&paths.root().to_path_buf());
    let dir_a = paths.root().join("workflows-a");
    let dir_b = paths.root().join("workflows-b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();
    let def_a = r#"{"workflowId":"flow-a","version":1,"params":{"name":{"type":"string","required":true}},"nodes":{"root":{"type":"hostExecutor","executor":"beam-schedule","input":{"name":"demo","schedule":"0 9 * * *","parsed":{"kind":"cron","expr":"0 9 * * *","display":"0 9 * * *"},"prompt":"Schedule demo","workingDir":"/tmp/demo","chatId":"oc_demo","scope":"thread"},"unsafeAllowUngated":true}}}"#;
    let def_b = r#"{"workflowId":"flow-a","version":2,"nodes":{"alt":{"type":"subagent","bot":"bot-a","prompt":"hi"}}}"#;
    tokio::fs::write(dir_a.join("flow-a.workflow.json"), def_a)
        .await
        .unwrap();
    tokio::fs::write(dir_b.join("flow-a.workflow.json"), def_b)
        .await
        .unwrap();

    let defs = list_workflow_definitions_in(vec![dir_a.clone(), dir_b.clone()])
        .await
        .expect("defs");
    assert_eq!(defs.len(), 1);
    let def = &defs[0];
    assert_eq!(def.workflow_id, "flow-a");
    assert_eq!(def.version, 1);
    assert_eq!(
        def.path,
        dir_a.join("flow-a.workflow.json").display().to_string()
    );
    assert_eq!(def.param_count, 1);
    assert_eq!(def.required_param_count, 1);
    assert_eq!(def.node_count, 1);
    assert_eq!(def.revision_id.len(), 64);
    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn list_workflow_runs_respects_terminal_filter_and_status_filters() {
    let paths = temp_paths("workflow-runs");
    maybe_remove_dir(&paths.root().to_path_buf());
    let params: BTreeMap<String, Value> =
        BTreeMap::from([(String::from("name"), Value::String("beam".to_string()))]);
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id: "run-active",
            workflow_json: r#"{"workflowId":"flow-active","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello"}}}"#,
            expected_workflow_id: Some("flow-active"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-1".to_string(),
                lark_app_id: "app-1".to_string(),
            }),
        },
    )
    .unwrap();
    bootstrap_workflow_run(
        &paths,
        BootstrapWorkflowRunInput {
            run_id: "run-done",
            workflow_json: r#"{"workflowId":"flow-done","version":1,"params":{"name":{"type":"string"}},"nodes":{"a":{"type":"subagent","bot":"bot-a","prompt":"hello"}}}"#,
            expected_workflow_id: Some("flow-done"),
            params: &params,
            initiator: "cli",
            chat_binding: Some(RunChatBinding {
                chat_id: "chat-2".to_string(),
                lark_app_id: "app-2".to_string(),
            }),
        },
    )
    .unwrap();
    {
        let mut log = EventLog::new("run-done", paths.workflow_runs_dir()).unwrap();
        let _ = log
            .append(EventDraft {
                event_type: "runSucceeded".to_string(),
                actor: WorkflowActor::Scheduler,
                payload: serde_json::json!({
                    "outputRef": {
                        "outputHash": "sha256:done",
                        "outputPath": paths.workflow_run_dir("run-done").join("blobs").join("done").display().to_string(),
                        "outputBytes": 1,
                        "outputSchemaVersion": 1,
                        "contentType": "application/json",
                    }
                }),
                timestamp: None,
                payload_hash: None,
            })
            .unwrap();
    }

    let default_rows = list_workflow_runs(&paths, false, None).await.expect("runs");
    assert_eq!(default_rows.len(), 1);
    assert_eq!(default_rows[0].run_id, "run-active");
    assert_eq!(default_rows[0].chat_id.as_deref(), Some("chat-1"));

    let all_rows = list_workflow_runs(&paths, true, None)
        .await
        .expect("all runs");
    assert_eq!(all_rows.len(), 2);

    let filtered_rows = list_workflow_runs(
        &paths,
        true,
        Some(HashSet::from([String::from("succeeded")])),
    )
    .await
    .expect("filtered");
    assert_eq!(filtered_rows.len(), 1);
    assert_eq!(filtered_rows[0].run_id, "run-done");
    assert_eq!(filtered_rows[0].status, "succeeded");

    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn load_workflow_catalog_definition_in_hashes_canonically() {
    let paths = temp_paths("workflow-catalog-canonical");
    maybe_remove_dir(&paths.root().to_path_buf());
    let dir_a = paths.root().join("catalog-a");
    let dir_b = paths.root().join("catalog-b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();
    let raw_a = r#"{"workflowId":"flow-catalog","version":1,"nodes":{"root":{"type":"subagent","bot":"bot-a","prompt":"hi","workingDir":"/tmp/demo"}}}"#;
    let raw_b = r#"
    {
        "nodes": {
            "root": {
                "workingDir": "/tmp/demo",
                "prompt": "hi",
                "bot": "bot-a",
                "type": "subagent"
            }
        },
        "version": 1,
        "workflowId": "flow-catalog"
    }
    "#;
    tokio::fs::write(dir_a.join("flow-catalog.workflow.json"), raw_a)
        .await
        .unwrap();
    tokio::fs::write(dir_b.join("flow-catalog.workflow.json"), raw_b)
        .await
        .unwrap();

    let def_a = load_workflow_catalog_definition_in(
        "flow-catalog",
        vec![dir_a.join("flow-catalog.workflow.json")],
    )
    .await
    .expect("catalog a")
    .expect("catalog a present");
    let def_b = load_workflow_catalog_definition_in(
        "flow-catalog",
        vec![dir_b.join("flow-catalog.workflow.json")],
    )
    .await
    .expect("catalog b")
    .expect("catalog b present");

    assert_eq!(def_a.revision_id, def_b.revision_id);
    assert_eq!(
        def_a.path,
        dir_a
            .join("flow-catalog.workflow.json")
            .display()
            .to_string()
    );
    assert_eq!(
        def_b.path,
        dir_b
            .join("flow-catalog.workflow.json")
            .display()
            .to_string()
    );
    let _ = std::fs::remove_dir_all(paths.root());
}

#[tokio::test]
async fn webhook_trigger_records_round_trip_and_list_api_shape() {
    let paths = temp_paths("webhook-triggers");
    maybe_remove_dir(&paths.root().to_path_buf());
    let records = vec![WebhookTriggerRecord {
        workflow_id: "flow-a".to_string(),
        created_at: "2026-06-07T00:00:00Z".to_string(),
        secret_valid: true,
        request_body: serde_json::json!({"hello":"world"}),
        run_id: Some("run-1".to_string()),
        workflow_run_id: Some("run-1".to_string()),
        status: "accepted".to_string(),
    }];
    write_webhook_trigger_records(&paths, &records).expect("write records");
    let loaded = read_webhook_trigger_records(&paths)
        .await
        .expect("read records");
    assert_eq!(loaded, records);
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn dashboard_auth_helpers_support_header_and_cookie_tokens() {
    let paths = temp_paths("dashboard-auth");
    maybe_remove_dir(&paths.root().to_path_buf());
    let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
    let state = AppState {
        paths: paths.clone(),
        started_at: Utc::now(),
        sessions: Arc::new(Mutex::new(HashMap::new())),
        workers: Arc::new(Mutex::new(HashMap::new())),
        attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
        shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
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
        external_host: std::sync::Arc::new(tokio::sync::RwLock::new("localhost".to_string())),
    };

    let token = mint_dashboard_token();
    {
        let mut guard = state.dashboard_token.lock().await;
        *guard = Some(DashboardAuthToken {
            token: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(30),
        });
    }
    let header_token = extract_dashboard_token(
        &HeaderMap::from_iter([(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        )]),
        None,
    )
    .expect("bearer token");
    assert_eq!(header_token, token);

    let cookie_token = extract_dashboard_token(
        &HeaderMap::from_iter([(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_str(&format!("beam-dashboard-token={}; foo=bar", token))
                .unwrap(),
        )]),
        None,
    )
    .expect("cookie token");
    assert_eq!(cookie_token, token);

    assert!(dashboard_token_is_valid(&state, &token).await);
    let mut expired = state.dashboard_token.lock().await;
    *expired = Some(DashboardAuthToken {
        token: token.clone(),
        expires_at: Instant::now() - Duration::from_secs(1),
    });
    drop(expired);
    assert!(!dashboard_token_is_valid(&state, &token).await);
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn beam_schedule_host_executor_creates_task_and_returns_task_id() {
    let paths = temp_paths("schedule-host");
    maybe_remove_dir(&paths.root().to_path_buf());
    let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
    let state = AppState {
        paths: paths.clone(),
        started_at: Utc::now(),
        sessions: Arc::new(Mutex::new(HashMap::new())),
        workers: Arc::new(Mutex::new(HashMap::new())),
        shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
        options: RunOptions {
            worker_exe: PathBuf::from("/bin/true"),
        },
        http: Client::new(),
        config: Config::default(),
        bots: Arc::new(HashMap::new()),
        lark_tokens: Arc::new(Mutex::new(HashMap::new())),
        chat_mode_cache: Arc::new(Mutex::new(HashMap::new())),
        recent_lark_events: Arc::new(Mutex::new(HashMap::new())),
        attempt_resumes: Arc::new(Mutex::new(HashMap::new())),
        inflight_final_output_turns: Arc::new(Mutex::new(HashSet::new())),
        workflow_progress_cards: Arc::new(Mutex::new(HashMap::new())),
        ask_pending: Arc::new(Mutex::new(HashMap::new())),
        grant_pending: Arc::new(Mutex::new(HashMap::new())),
        pending_creates: Arc::new(Mutex::new(HashMap::new())),
        dashboard_token: Arc::new(Mutex::new(None)),
        external_host: std::sync::Arc::new(tokio::sync::RwLock::new("localhost".to_string())),
    };
    let node = beam_core::HostExecutorNode {
        base: beam_core::workflow_definition::NodeBase {
            description: None,
            depends: None,
            human_gate: None,
            retry_policy: None,
            timeout_ms: None,
            max_output_bytes: None,
            output_schema: None,
            unsafe_allow_ungated: None,
        },
        executor: "beam-schedule".to_string(),
        input: serde_json::json!({
            "name": "schedule-demo daily 9am",
            "schedule": "0 9 * * *",
            "parsed": {
                "kind": "cron",
                "expr": "0 9 * * *",
                "display": "0 9 * * *"
            },
            "prompt": "Schedule demo: run workflow self-check.",
            "workingDir": "/tmp/beam-schedule-demo",
            "chatId": "oc_workflow_demo",
            "scope": "thread"
        }),
    };
    let outcome = run_workflow_host_executor(
        &state,
        WorkflowDispatchRun {
            run_id: "run-1",
            workflow_id: "flow-a",
            revision_id: "rev-1",
            activity_id: "activity-1",
            attempt_id: "attempt-1",
            node_id: "node-1",
        },
        &node,
        node.input.clone(),
        None,
    )
    .await
    .expect("host executor");
    match outcome {
        WorkflowDispatchOutcome::Succeeded { output, session } => {
            assert_eq!(
                output["taskId"],
                derive_workflow_idempotency_key("flow-a", "rev-1", "run-1", "node-1", "attempt-1")
            );
            assert!(session.is_none());
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert!(paths.schedules_json().exists());
    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn clear_pending_response_patch_marker_is_idempotent() {
    let paths = temp_paths("pending-marker-clear");
    maybe_remove_dir(&paths.root().to_path_buf());

    write_pending_response_patch_marker(&paths, "sess-1", "om_card")
        .await
        .expect("write marker");
    clear_pending_response_patch_marker(&paths, "sess-1")
        .await
        .expect("clear once");
    clear_pending_response_patch_marker(&paths, "sess-1")
        .await
        .expect("clear twice");
    let marker = read_pending_response_patch_marker(&paths, "sess-1")
        .await
        .expect("read marker");
    assert!(marker.is_none());

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn park_stream_card_merges_with_existing_on_disk_entries() {
    let paths = temp_paths("park-merge");
    maybe_remove_dir(&paths.root().to_path_buf());

    let mut existing = HashMap::new();
    existing.insert(
        "persisted_a".to_string(),
        FrozenCard {
            message_id: "om_disk_a".to_string(),
            content: "old".to_string(),
            title: "older".to_string(),
            display_mode: Some(DisplayMode::Hidden),
            image_key: None,
        },
    );
    save_frozen_cards(&paths, "sess-merge", &existing)
        .await
        .expect("save existing");

    let mut session = make_session("sess-merge");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.stream_card_id = Some("om_live".to_string());
    session.stream_card_nonce = Some("nonce_live".to_string());

    park_stream_card(&paths, &session)
        .await
        .expect("park succeeds");
    let frozen_cards = load_frozen_cards(&paths, &session.session_id)
        .await
        .expect("load merged");
    assert_eq!(frozen_cards.len(), 2);
    assert!(frozen_cards.contains_key("persisted_a"));
    assert!(frozen_cards.contains_key("nonce_live"));

    maybe_remove_dir(&paths.root().to_path_buf());
}

#[tokio::test]
async fn load_clicked_frozen_card_only_returns_stale_snapshot() {
    let paths = temp_paths("load-frozen");
    maybe_remove_dir(&paths.root().to_path_buf());

    let mut cards = HashMap::new();
    cards.insert(
        "nonce_old".to_string(),
        FrozenCard {
            message_id: "om_old".to_string(),
            content: "frozen output".to_string(),
            title: "old turn".to_string(),
            display_mode: Some(DisplayMode::Screenshot),
            image_key: None,
        },
    );
    save_frozen_cards(&paths, "sess-load", &cards)
        .await
        .expect("save succeeds");

    let mut session = make_session("sess-load");
    session.status = SessionStatus::Active;
    session.closed_at = None;
    session.stream_card_nonce = Some("nonce_live".to_string());

    let stale = load_clicked_frozen_card(&paths, &session, Some("nonce_old"))
        .await
        .expect("load stale");
    assert_eq!(
        stale.as_ref().map(|card| card.content.as_str()),
        Some("frozen output")
    );

    let live = load_clicked_frozen_card(&paths, &session, Some("nonce_live"))
        .await
        .expect("load live");
    assert!(live.is_none());

    session.stream_card_nonce = None;
    let after_turn_reset = load_clicked_frozen_card(&paths, &session, Some("nonce_old"))
        .await
        .expect("load after reset");
    assert_eq!(
        after_turn_reset.as_ref().map(|card| card.content.as_str()),
        Some("frozen output")
    );

    maybe_remove_dir(&paths.root().to_path_buf());
}
