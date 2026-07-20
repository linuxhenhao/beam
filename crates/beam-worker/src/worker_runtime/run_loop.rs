use super::*;

/// Returns `Some(("TERM", "xterm-256color"))` when the CLI's spec sets
/// `inject_term_xterm` (codex/traex) and the inherited `TERM` is missing,
/// empty, or `"dumb"`.
///
/// For any other CLI, or when `TERM` is already set to a non-empty value other
/// than `"dumb"`, returns `None` — the environment is never overwritten.
///
/// This is a pure function so it can be tested deterministically without
/// touching the real process environment.
pub(crate) fn maybe_inject_term(
    cli_id: &str,
    current_term: Option<&str>,
) -> Option<(String, String)> {
    let inject = beam_core::cli_specs::cli_spec(cli_id)
        .map(|spec| spec.inject_term_xterm)
        .unwrap_or(false);
    if !inject {
        return None;
    }
    match current_term {
        None | Some("") | Some("dumb") => Some(("TERM".to_string(), "xterm-256color".to_string())),
        _ => None,
    }
}

/// Whether to generate the env-injecting wrapper script for the CLI spawn.
///
/// The wrapper pins `BEAM_SESSION_ID` / `BEAM_HOME` / `BEAM_BIN` for the CLI
/// process. Without it the CLI inherits whatever ambient env the daemon (and
/// in turn the worker and zellij server) was started with — which may carry a
/// *different* session's `BEAM_SESSION_ID` (e.g. when the daemon was started
/// from inside another session via `beam restart`), misrouting `beam send`
/// deliveries to that session's (possibly closed) topic.
///
/// Adopted sessions attach to an already-running external CLI, so there is no
/// spawn to wrap. Every other session — including resumed ones — must get the
/// wrapper.
pub(crate) fn should_prepare_wrapper(init: &InitConfig) -> bool {
    init.adopted_from.is_none()
}

pub(crate) async fn prepare_wrapper(
    init: &InitConfig,
    paths: &BeamPaths,
) -> Result<std::path::PathBuf> {
    tokio::fs::create_dir_all(paths.run_dir()).await?;
    let wrapper = paths.worker_wrapper_sh(&init.session_id);
    let exe_path = std::env::current_exe()
        .ok()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "beam".to_string());
    let content = format!(
        "#!/bin/sh\ncd {cwd}\nexport BEAM_SESSION_ID={sid}\nexport BEAM_HOME={home}\nexport BEAM_BIN={exe}\nif [ -n \"$PATH\" ]; then\n  export PATH={bindir}:$PATH\nelse\n  export PATH={bindir}\nfi\nexec \"$@\"\n",
        cwd = shell_quote(&init.working_dir),
        sid = shell_quote(&init.session_id),
        home = shell_quote(&paths.root().display().to_string()),
        exe = shell_quote(&exe_path),
        bindir = shell_quote(
            &std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|v| v.display().to_string()))
                .unwrap_or_default()
        ),
    );
    tokio::fs::write(&wrapper, content).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o755);
        tokio::fs::set_permissions(&wrapper, perms).await?;
    }
    Ok(wrapper)
}

pub(crate) async fn send_message(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    msg: &WorkerToDaemon,
) -> Result<()> {
    let mut out = stdout.lock().await;
    out.write_all(serde_json::to_string(msg)?.as_bytes())
        .await?;
    out.write_all(b"\n").await?;
    out.flush().await?;
    Ok(())
}

pub async fn run(init: InitConfig) -> Result<()> {
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let session_name = format!("beam-{}", &init.session_id[..8.min(init.session_id.len())]);
    let paths = BeamPaths::discover()?;
    let adapter = Arc::new(Mutex::new(CliAdapter::from_init(&init)?));
    let wrapper = if should_prepare_wrapper(&init) {
        Some(prepare_wrapper(&init, &paths).await?)
    } else {
        None
    };
    let (mut backend_impl, attach_context): (Box<dyn SessionBackend>, &'static str) =
        if let Some(adopted) = init.adopted_from.as_ref() {
            if let Some(pane_id) = adopted.zellij_pane_id.clone() {
                let session = adopted.zellij_session.clone().unwrap_or_else(|| {
                    format!("beam-{}", &init.session_id[..8.min(init.session_id.len())])
                });
                let observe = ZellijObserveBackend::new(
                    session,
                    pane_id,
                    u32::try_from(adopted.original_cli_pid).ok(),
                );
                (Box::new(observe), "observe")
            } else {
                let zellij = ZellijBackend::new(session_name.clone());
                (Box::new(zellij), "spawn")
            }
        } else {
            let zellij = ZellijBackend::new(session_name.clone());
            (Box::new(zellij), "spawn")
        };
    let spawn_spec = adapter.lock().await.build_spawn_spec(&init);
    let args = if let Some(wrapper) = wrapper {
        let mut args = Vec::with_capacity(2 + init.cli_args.len());
        args.push(wrapper.display().to_string());
        args.push(spawn_spec.bin.clone());
        args.extend(spawn_spec.args.clone());
        ("/bin/sh".to_string(), args)
    } else {
        (spawn_spec.bin, spawn_spec.args)
    };
    let mut env = Vec::new();
    if let Some((k, v)) = maybe_inject_term(&init.cli_id, std::env::var("TERM").ok().as_deref()) {
        env.push((k, v));
    }
    let spawn_opts = SpawnOpts {
        cwd: init.working_dir.clone(),
        cols: DEFAULT_TERMINAL_COLS,
        rows: DEFAULT_TERMINAL_ROWS,
        env,
    };
    backend_impl
        .spawn(&args.0, &args.1, spawn_opts)
        .await
        .with_context(|| format!("failed to {} session {}", attach_context, init.session_id))?;
    let backend: Arc<Mutex<Box<dyn SessionBackend>>> = Arc::new(Mutex::new(backend_impl));
    let mut cli_pid_marker = None;
    let child_pid = backend.lock().await.child_pid().await?;
    adapter.lock().await.on_spawned(child_pid);
    if let Some(pid) = child_pid {
        tokio::fs::create_dir_all(paths.cli_pid_markers_dir()).await?;
        let marker = paths.cli_pid_markers_dir().join(pid.to_string());
        tokio::fs::write(&marker, init.session_id.as_bytes()).await?;
        cli_pid_marker = Some(marker);
    }
    let latest_screen = Arc::new(RwLock::new(String::new()));
    let latest_raw_screen = Arc::new(RwLock::new(String::new()));
    let display_mode = Arc::new(RwLock::new(DisplayMode::Hidden));
    let analyzer_runtime = Arc::new(RwLock::new(AnalyzerRuntime::default()));
    let usage_limit_tracker = Arc::new(Mutex::new(UsageLimitTracker::default()));
    let current_turn_id = Arc::new(RwLock::new(String::new()));
    let (updates, _) = broadcast::channel::<String>(256);

    send_message(
        &stdout,
        &WorkerToDaemon::Ready {
            zellij_session: session_name.clone(),
        },
    )
    .await?;

    let sample_backend = backend.clone();
    let sample_screen = latest_screen.clone();
    let sample_raw_screen = latest_raw_screen.clone();
    let sample_updates = updates.clone();
    let sample_stdout = stdout.clone();
    let sample_adapter = adapter.clone();
    let sample_display_mode = display_mode.clone();
    let sample_usage_limit_tracker = usage_limit_tracker.clone();
    let sample_current_turn_id = current_turn_id.clone();
    let last_uploaded_hash = Arc::new(Mutex::new(None::<String>));
    let last_broadcast_hash: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sample_last_broadcast_hash = last_broadcast_hash.clone();
    let sample_analyzer_runtime = analyzer_runtime.clone();
    // Channel for trigger events (TurnStarted, PaneUpdate, …) to the screenshot coordinator.
    let (trigger_tx, trigger_rx) = mpsc::channel::<Trigger>(16);
    let screen_capture_task = tokio::spawn(async move {
        let mut last_emitted_status = ScreenStatus::Starting;
        let mut last_emitted_usage_limit: Option<CliUsageLimitState> = None;
        loop {
            let (screen, alive) = {
                let guard = sample_backend.lock().await;
                let screen = guard.capture_viewport().await.unwrap_or_default();
                let alive = guard.is_alive().await.unwrap_or(false);
                (screen, alive)
            };

            let hash_changed;

            {
                *sample_raw_screen.write().await = screen.clone();
                let mode = *sample_display_mode.read().await;
                let rendered = render_screen_for_display_mode(&screen, mode);
                let now_ms = now_ms();
                let analyzing = sample_analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing {
                    ScreenStatus::Analyzing
                } else {
                    ScreenStatus::Working
                };
                let (status, usage_limit) =
                    sample_usage_limit_tracker
                        .lock()
                        .await
                        .classify(&screen, base_status, now_ms);
                let rendered_hash = lower_hex(&Sha256::digest(rendered.as_bytes()));
                {
                    let guard = sample_last_broadcast_hash.lock().await;
                    hash_changed = guard.as_deref() != Some(&rendered_hash);
                }

                if hash_changed
                    || last_emitted_status != status
                    || last_emitted_usage_limit != usage_limit
                {
                    *sample_last_broadcast_hash.lock().await = Some(rendered_hash.clone());
                    let mut current = sample_screen.write().await;
                    *current = rendered.clone();
                    let _ = sample_updates.send(rendered.clone());
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::ScreenUpdate {
                            content: rendered.clone(),
                            status,
                            usage_limit: usage_limit.clone(),
                        },
                    )
                    .await;
                    last_emitted_status = status;
                    last_emitted_usage_limit = usage_limit.clone();
                }
            }

            if let Ok(poll) = sample_adapter.lock().await.poll() {
                if let Some(cli_session_id) = poll.cli_session_id {
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::CliSessionId { cli_session_id },
                    )
                    .await;
                }
                if let Some((user_text, assistant_text)) = poll.adopt_preamble {
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::AdoptPreamble {
                            user_text,
                            assistant_text,
                        },
                    )
                    .await;
                }
                if let Some(content) = poll.final_output {
                    let turn_id = sample_current_turn_id.read().await.clone();
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::FinalOutput {
                            content,
                            turn_id,
                            kind: poll.final_output_kind,
                            user_text: poll.final_output_user_text,
                        },
                    )
                    .await;
                }
                if poll.prompt_ready {
                    let _ = send_message(&sample_stdout, &WorkerToDaemon::PromptReady).await;
                    let rendered = sample_screen.read().await.clone();
                    let raw = sample_raw_screen.read().await.clone();
                    let now_ms = now_ms();
                    let analyzing = sample_analyzer_runtime.read().await.is_analyzing;
                    let base_status = if analyzing {
                        ScreenStatus::Analyzing
                    } else {
                        ScreenStatus::Idle
                    };
                    let (status, usage_limit) =
                        sample_usage_limit_tracker
                            .lock()
                            .await
                            .classify(&raw, base_status, now_ms);
                    let _ = send_message(
                        &sample_stdout,
                        &WorkerToDaemon::ScreenUpdate {
                            content: rendered,
                            status,
                            usage_limit,
                        },
                    )
                    .await;
                }
            }

            if !alive {
                let _ = send_message(
                    &sample_stdout,
                    &WorkerToDaemon::CliExit {
                        code: Some(0),
                        signal: None,
                    },
                )
                .await;
                break;
            }

            tokio::time::sleep(Duration::from_millis(5000)).await;
        }
    });
    let mut worker_joins = tokio::task::JoinSet::new();
    worker_joins.spawn(async move {
        let _ = screen_capture_task.await;
    });

    if screen_analyzer_enabled(&init.screen_analyzer) {
        let analyzer_cfg = init.screen_analyzer.clone();
        let analyzer_raw_screen = latest_raw_screen.clone();
        let analyzer_runtime_state = analyzer_runtime.clone();
        let analyzer_stdout = stdout.clone();
        let analyzer_task = tokio::spawn(async move {
            let client = Client::new();
            loop {
                tokio::time::sleep(Duration::from_millis(analyzer_cfg.interval_ms)).await;
                let snapshot = analyzer_raw_screen.read().await.clone();
                if snapshot.is_empty() {
                    continue;
                }
                let truncated = if snapshot.len() > analyzer_cfg.snapshot_max_chars {
                    snapshot[snapshot.len() - analyzer_cfg.snapshot_max_chars..].to_string()
                } else {
                    snapshot
                };
                let now = now_ms();
                {
                    let mut runtime = analyzer_runtime_state.write().await;
                    if truncated == runtime.last_snapshot {
                        runtime.stable_count = runtime.stable_count.saturating_add(1);
                    } else {
                        runtime.stable_count = 1;
                        runtime.last_snapshot = truncated.clone();
                        if runtime.waiting_for_content_change {
                            runtime.waiting_for_content_change = false;
                        }
                    }
                    if runtime.stable_count < analyzer_cfg.stable_count {
                        continue;
                    }
                    if runtime.waiting_for_content_change
                        && truncated == runtime.last_analyzed_snapshot
                    {
                        continue;
                    }
                    if runtime.cooldown_until_ms > now {
                        continue;
                    }
                    runtime.is_analyzing = true;
                    runtime.last_analyzed_snapshot = truncated.clone();
                }

                let result = call_screen_analyzer(&client, &analyzer_cfg, &truncated).await;

                let mut runtime = analyzer_runtime_state.write().await;
                runtime.is_analyzing = false;
                match result {
                    Ok(analysis) => {
                        apply_screen_analyzer_result(
                            &mut runtime,
                            &analysis.check_again_when,
                            now_ms(),
                        );
                        if analysis.needs_interaction && !analysis.options.is_empty() {
                            if !runtime.prompt_active {
                                runtime.prompt_active = true;
                                let _ = send_message(
                                    &analyzer_stdout,
                                    &WorkerToDaemon::TuiPrompt {
                                        description: analysis.description.clone().unwrap_or_else(
                                            || "CLI needs your selection".to_string(),
                                        ),
                                        options: analysis.options.clone(),
                                        multi_select: analysis.multi_select,
                                    },
                                )
                                .await;
                            }
                        } else if runtime.prompt_active {
                            runtime.prompt_active = false;
                            let _ = send_message(
                                &analyzer_stdout,
                                &WorkerToDaemon::TuiPromptResolved {
                                    selected_text: None,
                                },
                            )
                            .await;
                        }
                    }
                    Err(_) => {
                        runtime.waiting_for_content_change = true;
                        runtime.cooldown_until_ms = 0;
                    }
                }
            }
        });
        worker_joins.spawn(async move {
            let _ = analyzer_task.await;
        });
    }

    // Coordinator task — owns the trigger receiver and the 5-second fallback.
    {
        let coord_backend = backend.clone();
        let coord_stdout = stdout.clone();
        let coord_session_id = init.session_id.clone();
        let coord_app_id = init.lark_app_id.clone();
        let coord_app_secret = init.lark_app_secret.clone();
        let coord_display_mode = display_mode.clone();
        let coord_analyzer_runtime = analyzer_runtime.clone();
        let coord_usage_limit_tracker = usage_limit_tracker.clone();
        let coord_last_uploaded_hash = last_uploaded_hash.clone();
        let coord_latest_raw_screen = latest_raw_screen.clone();
        let coord_rx = trigger_rx;
        worker_joins.spawn(async move {
            coordinator_loop(
                coord_backend,
                coord_stdout,
                coord_session_id,
                coord_app_id,
                coord_app_secret,
                coord_display_mode,
                coord_analyzer_runtime,
                coord_usage_limit_tracker,
                coord_last_uploaded_hash,
                coord_latest_raw_screen,
                coord_rx,
            )
            .await;
        });
    }
    // Subscribe task: forward backend pane-update notifications to the screenshot coordinator.
    // Also caches the full viewport ANSI chunk so the coordinator can capture
    // without waiting for the backend Mutex held by write_input().
    {
        let sub_backend = backend.clone();
        let sub_trigger_tx = trigger_tx.clone();
        let sub_latest_raw_screen = latest_raw_screen.clone();
        worker_joins.spawn(async move {
            let mut rx = sub_backend.lock().await.subscribe();
            loop {
                match rx.recv().await {
                    Ok(chunk) => {
                        // latest wins: the chunk is the full viewport (not incremental)
                        *sub_latest_raw_screen.write().await = chunk;
                        match sub_trigger_tx.try_send(Trigger::PaneUpdate) {
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                            _ => {} // Ok or Full → discard, keep listening
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }
    if !init.prompt.is_empty() && !crate::adapters::passes_initial_prompt_via_args(&init.cli_id) {
        usage_limit_tracker.lock().await.begin_turn(
            "",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        *current_turn_id.write().await = init
            .prompt_turn_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        *last_uploaded_hash.lock().await = None;
        let guard = backend.lock().await;
        let submit = adapter
            .lock()
            .await
            .write_input(guard.as_ref(), &init.prompt)
            .await?;
        if let Some(cli_session_id) = submit.cli_session_id {
            send_message(&stdout, &WorkerToDaemon::CliSessionId { cli_session_id }).await?;
        }
    }

    // Init-time transcript source resolution for adapters with resolvable
    // sources (opencode): resolve cli_session_id before the message loop.
    {
        let resolution = {
            let mut adapter_guard = adapter.lock().await;
            let backend_guard = backend.lock().await;
            let resolution = adapter_guard
                .resolve_transcript_source(backend_guard.as_ref())
                .await;
            drop(backend_guard);
            drop(adapter_guard);
            resolution
        };
        if let Some(resolution) = resolution {
            let outcome = resolution.unwrap_or_else(|err| {
                warn!(session = %init.session_id, adapter = %init.cli_id, "resolve_transcript_source error: {:?}", err);
                ResolveOutcome::NotFound {
                    reason: format!("transcript source resolution failed: {}", err),
                }
            });
            match outcome {
                ResolveOutcome::Found(source) => {
                    send_message(
                        &stdout,
                        &WorkerToDaemon::CliSessionId {
                            cli_session_id: source.session_id.clone(),
                        },
                    )
                    .await?;
                    // Also set in adapter state for subsequent poll/write_input.
                    adapter
                        .lock()
                        .await
                        .set_transcript_source(&source.session_id);
                    info!(
                        session = %init.session_id, adapter = %init.cli_id,
                        transcript_session = %source.session_id,
                        "transcript source resolved automatically"
                    );
                }
                ResolveOutcome::Ambiguous { candidates, .. } => {
                    info!(
                        session = %init.session_id, adapter = %init.cli_id,
                        candidate_count = candidates.len(),
                        "transcript source ambiguous, requesting user choice"
                    );
                    let turn_id = Uuid::new_v4().to_string();
                    let choices: Vec<TranscriptChoice> = candidates
                        .iter()
                        .map(|c| TranscriptChoice {
                            session_id: c.session_id.clone(),
                            label: format!("{} ({})", c.session_id, c.db_path.display()),
                        })
                        .collect();
                    send_message(
                        &stdout,
                        &WorkerToDaemon::TranscriptChoices {
                            candidates: choices,
                            turn_id: turn_id.clone(),
                        },
                    )
                    .await?;
                }
                ResolveOutcome::NotFound { reason } => {
                    info!(session = %init.session_id, adapter = %init.cli_id, "transcript source not found: {}", reason);
                    send_message(&stdout, &WorkerToDaemon::UserNotify { message: reason })
                        .await?;
                }
            }
        }
    }

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: DaemonToWorker = serde_json::from_str(&line)?;
        match msg {
            DaemonToWorker::Message { content, turn_id } => {
                info!(session = %init.session_id, %turn_id, "Message received, sending TurnStarted to coordinator");
                handle_tui_prompt_override(&stdout, &analyzer_runtime).await;
                let snapshot = latest_raw_screen.read().await.clone();
                usage_limit_tracker.lock().await.begin_turn(
                    &snapshot,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                *current_turn_id.write().await = turn_id.clone();
                *last_uploaded_hash.lock().await = None;
                // Notify coordinator: a new turn has started (best-effort).
                if trigger_tx
                    .send(Trigger::TurnStarted { turn_id })
                    .await
                    .is_err()
                {
                    warn!("coordinator channel closed, TurnStarted not sent");
                }
                let guard = backend.lock().await;
                let submit = adapter
                    .lock()
                    .await
                    .write_input(guard.as_ref(), &content)
                    .await?;
                if let Some(cli_session_id) = submit.cli_session_id {
                    send_message(&stdout, &WorkerToDaemon::CliSessionId { cli_session_id }).await?;
                }
                if !submit.submitted {
                    let message = submit
                        .failure_reason
                        .unwrap_or_else(|| "CLI submit could not be confirmed".to_string());
                    send_message(&stdout, &WorkerToDaemon::UserNotify { message }).await?;
                }
            }
            DaemonToWorker::RawInput { content, turn_id } => {
                info!(session = %init.session_id, %turn_id, "RawInput received, sending TurnStarted to coordinator");
                handle_tui_prompt_override(&stdout, &analyzer_runtime).await;
                let snapshot = latest_raw_screen.read().await.clone();
                usage_limit_tracker.lock().await.begin_turn(
                    &snapshot,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                *current_turn_id.write().await = turn_id.clone();
                *last_uploaded_hash.lock().await = None;
                // Notify coordinator: a new turn has started (best-effort).
                if trigger_tx
                    .send(Trigger::TurnStarted { turn_id })
                    .await
                    .is_err()
                {
                    warn!("coordinator channel closed, TurnStarted not sent");
                }
                let guard = backend.lock().await;
                guard.raw_input(&content).await?;
            }
            DaemonToWorker::Close => {
                let mut guard = backend.lock().await;
                guard.destroy_session().await?;
                break;
            }
            DaemonToWorker::Restart => {
                let mut guard = backend.lock().await;
                guard.destroy_session().await?;
                break;
            }
            DaemonToWorker::RefreshScreen => {
                let guard = backend.lock().await;
                let screen = guard.capture_viewport().await?;
                *latest_raw_screen.write().await = screen.clone();
                let mode = *display_mode.read().await;
                let rendered = render_screen_for_display_mode(&screen, mode);
                let now_ms = now_ms();
                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing {
                    ScreenStatus::Analyzing
                } else {
                    ScreenStatus::Working
                };
                let (status, usage_limit) =
                    usage_limit_tracker
                        .lock()
                        .await
                        .classify(&screen, base_status, now_ms);
                *latest_screen.write().await = rendered.clone();
                let _ = updates.send(rendered.clone());
                send_message(
                    &stdout,
                    &WorkerToDaemon::ScreenUpdate {
                        content: rendered.clone(),
                        status,
                        usage_limit: usage_limit.clone(),
                    },
                )
                .await?;
                let rendered_hash = lower_hex(&Sha256::digest(rendered.as_bytes()));
                *last_broadcast_hash.lock().await = Some(rendered_hash);
                // Notify coordinator: refresh request (best-effort).
                if trigger_tx.send(Trigger::Refresh).await.is_err() {
                    warn!("coordinator channel closed, Refresh not sent");
                }
            }
            DaemonToWorker::SetDisplayMode { mode } => {
                *display_mode.write().await = mode;
                let raw = latest_raw_screen.read().await.clone();
                let rendered = render_screen_for_display_mode(&raw, mode);
                let now_ms = now_ms();
                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing {
                    ScreenStatus::Analyzing
                } else {
                    ScreenStatus::Working
                };
                let (status, usage_limit) =
                    usage_limit_tracker
                        .lock()
                        .await
                        .classify(&raw, base_status, now_ms);
                *latest_screen.write().await = rendered.clone();
                let _ = updates.send(rendered.clone());
                send_message(
                    &stdout,
                    &WorkerToDaemon::ScreenUpdate {
                        content: rendered,
                        status,
                        usage_limit: usage_limit.clone(),
                    },
                )
                .await?;
                // Notify coordinator: display mode changed (best-effort).
                if trigger_tx
                    .send(Trigger::SetDisplayMode(mode))
                    .await
                    .is_err()
                {
                    warn!("coordinator channel closed, SetDisplayMode not sent");
                }
            }
            DaemonToWorker::TermAction { key } => {
                let keys = term_action_keys(key);
                let guard = backend.lock().await;
                guard.send_special_keys(&keys).await?;
            }
            DaemonToWorker::SpecialKeys { keys } => {
                let guard = backend.lock().await;
                guard.send_special_keys(&keys).await?;
            }
            DaemonToWorker::TuiKeys { keys, is_final } => {
                handle_tui_keys(&backend, &analyzer_runtime, &keys, is_final).await?;
            }
            DaemonToWorker::TuiTextInput { keys, text } => {
                handle_tui_text_input(&backend, &adapter, &analyzer_runtime, &keys, &text).await?;
            }
            DaemonToWorker::SetTranscriptSource { cli_session_id } => {
                let applied = adapter.lock().await.set_transcript_source(&cli_session_id);
                if applied {
                    info!(session = %init.session_id, adapter = %init.cli_id, transcript_session = %cli_session_id, "transcript source set by user");
                    send_message(&stdout, &WorkerToDaemon::CliSessionId { cli_session_id }).await?;
                }
            }
            DaemonToWorker::Init(_) => {}
        }
    }

    worker_joins.abort_all();
    while worker_joins.join_next().await.is_some() {}
    {
        let mut guard = backend.lock().await;
        let _ = guard.kill().await;
    }
    if let Some(marker) = cli_pid_marker {
        let _ = tokio::fs::remove_file(marker).await;
    }
    info!("worker exiting");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::test_support::test_init;

    /// Regression: resumed sessions must still get the env-injecting wrapper.
    /// Otherwise the CLI inherits the daemon's ambient `BEAM_SESSION_ID`
    /// (which may belong to a different, possibly closed session) and
    /// `beam send` deliveries are misrouted to that session's topic.
    #[test]
    fn should_prepare_wrapper_covers_new_and_resumed_sessions() {
        let mut init = test_init("kimi");
        assert!(should_prepare_wrapper(&init));
        init.resume = true;
        assert!(should_prepare_wrapper(&init));
    }

    #[test]
    fn should_prepare_wrapper_skips_adopted_sessions() {
        let mut init = test_init("kimi");
        init.adopted_from = Some(beam_core::AdoptedFrom {
            tmux_target: None,
            zellij_session: Some("ext".to_string()),
            zellij_pane_id: Some("terminal_0".to_string()),
            original_cli_pid: 1234,
            session_id: None,
            cli_id: Some("kimi".to_string()),
            cwd: "/tmp".to_string(),
            pane_cols: None,
            pane_rows: None,
        });
        assert!(!should_prepare_wrapper(&init));
    }
}
