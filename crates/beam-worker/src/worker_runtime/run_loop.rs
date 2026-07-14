use super::*;

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
    let wrapper = if init.resume || init.adopted_from.is_some() {
        None
    } else {
        Some(prepare_wrapper(&init, &paths).await?)
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
    let spawn_opts = SpawnOpts {
        cwd: init.working_dir.clone(),
        cols: DEFAULT_TERMINAL_COLS,
        rows: DEFAULT_TERMINAL_ROWS,
        env: Vec::new(),
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
    let screenshot_app_id = init.lark_app_id.clone();
    let screenshot_app_secret = init.lark_app_secret.clone();
    let last_uploaded_hash = Arc::new(Mutex::new(None::<String>));
    let sample_last_uploaded_hash = last_uploaded_hash.clone();
    let last_broadcast_hash: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sample_last_broadcast_hash = last_broadcast_hash.clone();
    let sample_analyzer_runtime = analyzer_runtime.clone();
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

                if mode == DisplayMode::Screenshot
                    && screenshot_app_id != "local"
                    && !screenshot_app_secret.is_empty()
                {
                    maybe_send_screenshot_upload(
                        &sample_stdout,
                        &screenshot_app_id,
                        &screenshot_app_secret,
                        &screen,
                        status,
                        usage_limit.clone(),
                        &sample_last_uploaded_hash,
                    )
                    .await;
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

    // Init-time transcript source resolution for OpenCode adapter.
    // When cli_session_id is not yet known (typical for adopted sessions),
    // try to resolve it before entering the main message loop.
    {
        let adapter_guard = adapter.lock().await;
        if let AdapterKind::OpenCode(ref opencode_state) = adapter_guard.kind {
            if opencode_state.cli_session_id.is_none() {
                let backend_guard = backend.lock().await;
                let resolution = crate::adapters::opencode::resolve_transcript_source(
                    opencode_state,
                    backend_guard.as_ref(),
                )
                .await
                .unwrap_or_else(|err| {
                    warn!("resolve_transcript_source error: {:?}", err);
                    ResolveOutcome::NotFound {
                        reason: format!("OpenCode transcript source resolution failed: {}", err),
                    }
                });
                drop(backend_guard);
                match resolution {
                    ResolveOutcome::Found(source) => {
                        send_message(
                            &stdout,
                            &WorkerToDaemon::CliSessionId {
                                cli_session_id: source.session_id.clone(),
                            },
                        )
                        .await?;
                        // Also set in adapter state for subsequent poll/write_input.
                        drop(adapter_guard);
                        let mut adapter_mut = adapter.lock().await;
                        if let AdapterKind::OpenCode(ref mut state) = adapter_mut.kind {
                            state.expected_session_id = Some(source.session_id.clone());
                            state.cli_session_id = Some(source.session_id.clone());
                        }
                        info!(
                            "transcript source resolved automatically: session={}",
                            source.session_id
                        );
                    }
                    ResolveOutcome::Ambiguous { candidates, .. } => {
                        warn!(
                            "transcript source ambiguous ({} candidates), requesting user choice",
                            candidates.len()
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
                        drop(adapter_guard);
                    }
                    ResolveOutcome::NotFound { reason } => {
                        warn!("transcript source not found: {}", reason);
                        send_message(&stdout, &WorkerToDaemon::UserNotify { message: reason })
                            .await?;
                    }
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
                handle_tui_prompt_override(&stdout, &analyzer_runtime).await;
                let snapshot = latest_raw_screen.read().await.clone();
                usage_limit_tracker.lock().await.begin_turn(
                    &snapshot,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                *current_turn_id.write().await = turn_id;
                *last_uploaded_hash.lock().await = None;
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
                handle_tui_prompt_override(&stdout, &analyzer_runtime).await;
                let snapshot = latest_raw_screen.read().await.clone();
                usage_limit_tracker.lock().await.begin_turn(
                    &snapshot,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                );
                *current_turn_id.write().await = turn_id;
                *last_uploaded_hash.lock().await = None;
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
                if mode == DisplayMode::Screenshot {
                    maybe_send_screenshot_upload(
                        &stdout,
                        &init.lark_app_id,
                        &init.lark_app_secret,
                        &screen,
                        status,
                        usage_limit,
                        &last_uploaded_hash,
                    )
                    .await;
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
                if mode == DisplayMode::Screenshot {
                    maybe_send_screenshot_upload(
                        &stdout,
                        &init.lark_app_id,
                        &init.lark_app_secret,
                        &raw,
                        status,
                        usage_limit,
                        &last_uploaded_hash,
                    )
                    .await;
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
                if let AdapterKind::OpenCode(ref mut state) = adapter.lock().await.kind {
                    state.expected_session_id = Some(cli_session_id.clone());
                    state.cli_session_id = Some(cli_session_id.clone());
                }
                info!("transcript source set by user: session={}", cli_session_id);
                send_message(&stdout, &WorkerToDaemon::CliSessionId { cli_session_id }).await?;
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
