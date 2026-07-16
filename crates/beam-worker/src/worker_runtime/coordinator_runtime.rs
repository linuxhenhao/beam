//! Runtime integration for the screenshot coordinator state machine.
//!
//! This module contains the async `coordinator_loop` that drives the pure
//! [`super::coordinator`] state machine with timers, screen captures, and
//! Feishu uploads.  The pure state machine stays in [`super::coordinator`].
//!
//! ## Upload decoupling
//!
//! Network uploads are spawned as independent `tokio` tasks so that slow
//! uploads never block `TurnStarted`, grace or debounce timers inside the
//! `select!` loop.  Results flow back via an `mpsc::unbounded_channel`.
//!
//! - At most one upload is in flight at a time (`upload_in_flight`).
//! - When in flight, new capture requests are merged into a single
//!   `pending_capture` (latest wins).  `TurnStarted` discards the old
//!   pending so it can never pollute a new turn.
//! - Timer arms that fire while in flight store a pending and return
//!   immediately.  When the upload completes the pending is executed
//!   without waiting for the next 5 s tick.
//! - The coordinator updates `last_uploaded_hash` (shared) **only** on
//!   confirmed success; failed uploads are retried by the next fallback
//!   tick and do not poison the dedup state.

use super::*;

/// Pure cache-first selection: returns `Some(cached)` when the cache is
/// non-empty, `None` otherwise.
///
/// The caller falls back to `backend.capture_viewport()` only when this
/// returns `None`, keeping graceful degradation when no subscription
/// viewport has been received yet.
///
/// This is extracted for deterministic unit testing without needing a real
/// backend or async runtime.
fn cached_screen_or_none(cached: &str) -> Option<&str> {
    if cached.is_empty() {
        None
    } else {
        Some(cached)
    }
}

/// Event-driven screen capture with cache-first semantics.
///
/// 1. Reads `latest_raw_screen` (populated by the subscribe task).
/// 2. If non-empty, returns the cached ANSI viewport snapshot immediately
///    — **without** locking the backend.  This avoids being blocked behind
///    the main message handler's backend Mutex held across
///    `adapter.write_input().await`.
/// 3. Only when the cache is empty (subscribe hasn't delivered yet) does
///    it fall back to `backend.lock().await.capture_viewport()`.
///
/// This function does NOT hold the backend lock across upload/render; it
/// copies the screen string and drops the lock before returning.
async fn capture_screen_cached(
    latest_raw_screen: &Arc<RwLock<String>>,
    backend: &Arc<Mutex<Box<dyn SessionBackend>>>,
) -> String {
    let cached = latest_raw_screen.read().await.clone();
    if let Some(screen) = cached_screen_or_none(&cached) {
        return screen.to_string();
    }
    // Fallback: cache is empty, use backend directly.
    let guard = backend.lock().await;
    guard.capture_viewport().await.unwrap_or_default()
}

/// Reap the completed upload task handle deterministically.
///
/// Uses [`JoinSet::join_next`]`.await` instead of `try_join_next` to avoid a
/// race where the task has sent `UploadCompleted` but the [`JoinSet`] hasn't
/// observed the task completion yet (which would silently leak the handle).
///
/// Because we maintain at most one in-flight upload, there is always exactly
/// 0 or 1 task in the set when this is called.  `join_next().await` resolves
/// immediately when the task is already done.
///
/// Upload-task panics are logged but **not** propagated — a single
/// panicking upload must not crash the coordinator.
async fn drain_upload_joins(upload_joins: &mut tokio::task::JoinSet<()>) {
    while let Some(result) = upload_joins.join_next().await {
        match result {
            Ok(()) => {}
            Err(e) => {
                if e.is_panic() {
                    warn!("upload task panicked: {:?}", e);
                }
                // Do NOT propagate — coordinator must survive.
            }
        }
    }
}

/// Spawn a screenshot upload as an independent task that reports back via
/// `upload_tx`.
///
/// The caller **must** set `state.upload_in_flight = true` before calling
/// this function, so that concurrent triggers see the in-flight guard.
fn spawn_upload(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    session_id: &str,
    app_id: &str,
    app_secret: &str,
    screen: String,
    hash: String,
    status: ScreenStatus,
    usage_limit: Option<CliUsageLimitState>,
    trigger_source: &str,
    turn_id: String,
    upload_tx: &mpsc::UnboundedSender<UploadCompleted>,
    upload_joins: &mut tokio::task::JoinSet<()>,
) {
    let stdout = stdout.clone();
    let session_id = session_id.to_string();
    let app_id = app_id.to_string();
    let app_secret = app_secret.to_string();
    let trigger_source = trigger_source.to_string();
    let hash_clone = hash.clone();
    let turn_id_clone = turn_id.clone();
    let tx = upload_tx.clone();

    upload_joins.spawn(async move {
        let ok = do_screenshot_upload(
            &stdout,
            &session_id,
            &trigger_source,
            &app_id,
            &app_secret,
            &screen,
            status,
            usage_limit,
            &hash_clone,
            if turn_id_clone.is_empty() {
                None
            } else {
                Some(turn_id_clone.clone())
            },
        )
        .await;
        let _ = tx.send(UploadCompleted {
            success: ok,
            hash: hash_clone,
            turn_id: turn_id_clone,
        });
    });
}

/// Runtime loop for the screenshot coordinator.
///
/// Runs a 5-second interval fallback tick and listens for trigger events
/// via an `mpsc::Receiver<Trigger>` provided by [`super::run_loop::run()`].
///
/// Trigger handling:
/// - `TurnStarted` — resets turn state, cancels pending pane-debounce,
///   and starts a 500 ms message-grace timer.
/// - `PaneUpdate` — schedules a one-shot 250 ms pane-debounce timer on the
///   first update per turn; subsequent updates do NOT reset it.  Does NOT
///   cancel the message-grace timer.
/// - `GraceTimeout` — the 500 ms grace expired; the state machine atomically
///   claims first-screenshot eligibility.  If claimed, the runtime cancels
///   any pending pane-debounce and captures.
/// - `Pane-debounce timeout` — the 250 ms timer expired; the runtime
///   atomically claims first-screenshot eligibility via
///   [`claim_first_screenshot`].  If claimed, cancels grace and captures
///   with hash CAS (only one first screenshot per turn).
/// - `FallbackTick` — periodic 5 s check that uploads only when the screen
///   hash has changed since the last upload in the current turn.
///
/// The grace and debounce timers are implemented as pinned `Sleep` futures
/// inside the `select!` loop so they are fully cancellable.
///
/// ## Upload decoupling
///
/// Uploads are spawned via [`tokio::spawn`] (tracked by a local `JoinSet`).
/// On loop exit all in-flight uploads are aborted.
pub(crate) async fn coordinator_loop(
    backend: Arc<Mutex<Box<dyn SessionBackend>>>,
    stdout: Arc<Mutex<tokio::io::Stdout>>,
    session_id: String,
    app_id: String,
    app_secret: String,
    display_mode: Arc<RwLock<DisplayMode>>,
    analyzer_runtime: Arc<RwLock<AnalyzerRuntime>>,
    usage_limit_tracker: Arc<Mutex<UsageLimitTracker>>,
    last_uploaded_hash: Arc<Mutex<Option<String>>>,
    latest_raw_screen: Arc<RwLock<String>>,
    mut rx: mpsc::Receiver<Trigger>,
) {
    let mut state = CoordinatorState::default();
    state.current_turn_id = String::new();
    let mut interval = tokio::time::interval(Duration::from_millis(5000));
    interval.tick().await;

    let grace = tokio::time::sleep(Duration::from_secs(100_000_000));
    tokio::pin!(grace);

    let debounce = tokio::time::sleep(Duration::from_secs(100_000_000));
    tokio::pin!(debounce);

    // One-shot debounce per turn; prevents high-frequency resets.
    let mut debounce_scheduled = false;

    let mut rx_open = true;

    // Channel for upload results from spawned tasks.
    let (upload_tx, mut upload_rx) = mpsc::unbounded_channel::<UploadCompleted>();
    // Track spawned upload tasks so they are cleaned up on loop exit.
    let mut upload_joins = tokio::task::JoinSet::new();

    loop {
        // Biased select ensures deterministic priority when multiple
        // branches are ready simultaneously:
        //
        //   1. UploadCompleted — release in-flight guard immediately so
        //      pending captures can flow.
        //   2. Grace / Debounce timers — expired timers MUST beat
        //      continuous PaneUpdate flood; without biased ordering a
        //      full rx channel can starve them for seconds.
        //   3. rx.recv() — TurnStarted resets timers, PaneUpdate is
        //      merged/skipped (lowest priority among ready branches).
        //   4. interval.tick() — periodic 5 s fallback.
        //
        // A race between an old-turn timer and a newly-arrived
        // TurnStarted is resolved in favour of the timer.  The old-turn
        // capture is harmless: TurnStarted clears the eligibility flag
        // afterward, and the UploadCompleted branch discards misplaced
        // turn_ids via the `upload_result.turn_id ==
        // state.current_turn_id` guard.
        tokio::select! {
            biased;

            // ── Upload completed (highest priority) ───────────────────
            Some(upload_result) = upload_rx.recv() => {
                state.upload_in_flight = false;

                // Reap the completed upload's JoinHandle to prevent
                // unbounded accumulation.  Panics are logged, never
                // propagated.
                drain_upload_joins(&mut upload_joins).await;

                // Only record on success AND when the turn hasn't changed.
                // A new TurnStarted clears `state.current_turn_id` to the new
                // turn, so old-turn results are silently dropped.
                if upload_result.success
                    && !upload_result.turn_id.is_empty()
                    && upload_result.turn_id == state.current_turn_id
                {
                    // Update shared hash so the sampler (and any other
                    // task) can see the latest upload.
                    {
                        let mut shared = last_uploaded_hash.lock().await;
                        *shared = Some(upload_result.hash.clone());
                    }
                    record_upload(&mut state, &upload_result.turn_id, &upload_result.hash);
                } else if !upload_result.success {
                    info!(session = %session_id, hash = %upload_result.hash, "coordinator: upload failed, will retry via fallback tick");
                }
                // else: turn mismatch — old upload finished on a new turn,
                // discard silently.

                // ── Process pending capture for the current turn ───────
                if let Some(pending) = state.pending_capture.take() {
                    if pending.turn_id != state.current_turn_id {
                        // Old-turn pending — discard.
                        continue;
                    }

                    let mode = *display_mode.read().await;
                    if mode != DisplayMode::Screenshot
                        || app_id == "local"
                        || app_secret.is_empty()
                    {
                        continue;
                    }

                    // Sync external hash into local state.
                    {
                        let shared = last_uploaded_hash.lock().await.clone();
                        if shared != state.last_uploaded_hash {
                            state.last_uploaded_hash = shared.clone();
                            if shared.is_none() {
                                state.last_uploaded_turn_id = None;
                            }
                        }
                    }

                    // Grace/Debounce pending use cache-first capture to
                    // avoid blocking on the backend Mutex held by
                    // write_input().  Refresh/SetDisplayMode/Fallback
                    // pending keep the direct backend path.
                    let screen =
                        if matches!(pending.source, PendingSource::Grace | PendingSource::Debounce) {
                            capture_screen_cached(&latest_raw_screen, &backend).await
                        } else {
                            let guard = backend.lock().await;
                            guard.capture_viewport().await.unwrap_or_default()
                        };

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
                            .classify(&screen, base_status, now_ms());
                    // Dedup hash is based on rendered visual content (PNG
                    // bytes) so that invisible control characters (e.g.
                    // \r, bare ESC) don't cause false-positive uploads.
                    let hash = match screenshot_visual_hash(&screen) {
                        Ok(h) => h,
                        Err(e) => {
                            warn!(session = %session_id, "coordinator: screenshot_visual_hash failed: {e}");
                            continue;
                        }
                    };

                    // Grace, Refresh, SetDisplayMode always upload (no dedup).
                    // Debounce and Fallback re-check should_upload.
                    let check_hash_dedup = matches!(
                        pending.source,
                        PendingSource::Debounce | PendingSource::Fallback
                    );
                    if check_hash_dedup
                        && !should_upload(&state.current_turn_id, &hash, &state)
                    {
                        continue;
                    }

                    let trigger_source = match pending.source {
                        PendingSource::Debounce => "pending-debounce",
                        PendingSource::Grace => "pending-grace",
                        PendingSource::Fallback => "pending-fallback",
                        PendingSource::Refresh => "pending-refresh",
                        PendingSource::SetDisplayMode => "pending-display-mode",
                    };

                    let upload_turn_id = state.current_turn_id.clone();
                    state.upload_in_flight = true;
                    spawn_upload(
                        &stdout, &session_id, &app_id, &app_secret,
                        screen, hash, status, usage_limit, trigger_source,
                        upload_turn_id, &upload_tx, &mut upload_joins,
                    );
                }
            }

            // ── Message-grace timer expired (~500 ms after TurnStarted) ─
            _ = &mut grace => {
                info!(session = %session_id, "coordinator: grace timer fired");
                grace.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(100_000_000));

                // Atomically claim first-screenshot eligibility before any
                // awaits.  If the debounce already consumed it, skip.
                if !claim_first_screenshot(&mut state) {
                    continue;
                }

                // Eligibility consumed by the state machine.  Cancel the
                // pane-debounce timer to prevent a late duplicate capture.
                debounce_scheduled = false;
                debounce.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(100_000_000));

                let mode = *display_mode.read().await;
                if mode != DisplayMode::Screenshot || app_id == "local" || app_secret.is_empty() {
                    continue;
                }

                if state.upload_in_flight {
                    state.pending_capture = Some(PendingCapture {
                        turn_id: state.current_turn_id.clone(),
                        source: PendingSource::Grace,
                    });
                    continue;
                }

                {
                    let shared = last_uploaded_hash.lock().await.clone();
                    if shared != state.last_uploaded_hash {
                        state.last_uploaded_hash = shared.clone();
                        if shared.is_none() { state.last_uploaded_turn_id = None; }
                    }
                }

                let screen =
                    capture_screen_cached(&latest_raw_screen, &backend).await;

                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing { ScreenStatus::Analyzing } else { ScreenStatus::Working };
                let (status, usage_limit) = usage_limit_tracker.lock().await.classify(&screen, base_status, now_ms());
                // Dedup hash is based on rendered visual content (PNG
                // bytes) — invisible control chars are ignored.
                let hash = match screenshot_visual_hash(&screen) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(session = %session_id, "coordinator: screenshot_visual_hash failed: {e}");
                        continue;
                    }
                };

                let upload_turn_id = state.current_turn_id.clone();
                state.upload_in_flight = true;
                spawn_upload(
                    &stdout, &session_id, &app_id, &app_secret,
                    screen, hash, status, usage_limit, "message-grace",
                    upload_turn_id, &upload_tx, &mut upload_joins,
                );
            }

            // ── Pane update debounce timer expired (250 ms after first PaneUpdate) ─
            _ = &mut debounce => {
                debounce.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(100_000_000));

                // Atomically claim first-screenshot eligibility before any
                // awaits.  If the grace timer already consumed it, release
                // and skip.
                if !claim_first_screenshot(&mut state) {
                    debounce_scheduled = false;
                    continue;
                }

                debounce_scheduled = false;
                // Cancel grace to prevent a duplicate first screenshot.
                grace.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(100_000_000));

                let mode = *display_mode.read().await;
                if mode != DisplayMode::Screenshot || app_id == "local" || app_secret.is_empty() {
                    continue;
                }

                if state.upload_in_flight {
                    state.pending_capture = Some(PendingCapture {
                        turn_id: state.current_turn_id.clone(),
                        source: PendingSource::Debounce,
                    });
                    continue;
                }

                {
                    let shared = last_uploaded_hash.lock().await.clone();
                    if shared != state.last_uploaded_hash {
                        state.last_uploaded_hash = shared.clone();
                        if shared.is_none() { state.last_uploaded_turn_id = None; }
                    }
                }

                let screen =
                    capture_screen_cached(&latest_raw_screen, &backend).await;

                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing { ScreenStatus::Analyzing } else { ScreenStatus::Working };
                let (status, usage_limit) = usage_limit_tracker.lock().await.classify(&screen, base_status, now_ms());
                // Dedup hash is based on rendered visual content (PNG
                // bytes) — invisible control chars are ignored.
                let hash = match screenshot_visual_hash(&screen) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(session = %session_id, "coordinator: screenshot_visual_hash failed: {e}");
                        continue;
                    }
                };

                // Use hash-based dedup: skip if the screen hasn't changed
                // since the last upload within this turn.
                if !should_upload(&state.current_turn_id, &hash, &state) {
                    continue;
                }

                let upload_turn_id = state.current_turn_id.clone();
                state.upload_in_flight = true;
                spawn_upload(
                    &stdout, &session_id, &app_id, &app_secret,
                    screen, hash, status, usage_limit, "pane-debounce",
                    upload_turn_id, &upload_tx, &mut upload_joins,
                );
            }

            // ── Trigger channel (TurnStarted > PaneUpdate > others) ─────
            trigger = rx.recv(), if rx_open => {
                match trigger {
                    Some(Trigger::TurnStarted { turn_id }) => {
                        info!(session = %session_id, %turn_id, "coordinator: TurnStarted received, grace timer reset to 500ms");
                        handle_trigger(&mut state, Trigger::TurnStarted { turn_id }, None);
                        debounce_scheduled = false;
                        debounce.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(100_000_000));
                        grace.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(500));
                    }
                    Some(Trigger::PaneUpdate) => {
                        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
                        if matches!(action, Action::Debounce { .. }) && !debounce_scheduled {
                            debounce_scheduled = true;
                            debounce.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(250));
                            info!(session = %session_id, "coordinator: PaneUpdate debounce scheduled (250ms)");
                        }
                    }
                    Some(other) => {
                        let kind = match &other {
                            Trigger::Refresh => "refresh",
                            Trigger::SetDisplayMode(_) => "display-mode",
                            _ => "",
                        };
                        let action = handle_trigger(&mut state, other, None);
                        if action == Action::Capture && !kind.is_empty() {
                            let mode = *display_mode.read().await;
                            if mode != DisplayMode::Screenshot
                                || app_id == "local"
                                || app_secret.is_empty()
                            {
                                continue;
                            }
                            {
                                let shared = last_uploaded_hash.lock().await.clone();
                                if shared != state.last_uploaded_hash {
                                    state.last_uploaded_hash = shared.clone();
                                    if shared.is_none() {
                                        state.last_uploaded_turn_id = None;
                                    }
                                }
                            }

                            if state.upload_in_flight {
                                state.pending_capture = Some(PendingCapture {
                                    turn_id: state.current_turn_id.clone(),
                                    source: if kind == "refresh" {
                                        PendingSource::Refresh
                                    } else {
                                        PendingSource::SetDisplayMode
                                    },
                                });
                                continue;
                            }

                            let screen = {
                                let guard = backend.lock().await;
                                guard.capture_viewport().await.unwrap_or_default()
                            };
                            let analyzing = analyzer_runtime.read().await.is_analyzing;
                            let base_status = if analyzing {
                                ScreenStatus::Analyzing
                            } else {
                                ScreenStatus::Working
                            };
                            let (status, usage_limit) = usage_limit_tracker
                                .lock()
                                .await
                                .classify(&screen, base_status, now_ms());
                            // Dedup hash is based on rendered visual
                            // content (PNG bytes) — invisible control
                            // chars are ignored.
                            let hash = match screenshot_visual_hash(&screen) {
                                Ok(h) => h,
                                Err(e) => {
                                    warn!(session = %session_id, "coordinator: screenshot_visual_hash failed: {e}");
                                    continue;
                                }
                            };
                            let upload_turn_id = state.current_turn_id.clone();
                            state.upload_in_flight = true;
                            spawn_upload(
                                &stdout, &session_id, &app_id, &app_secret,
                                screen, hash, status, usage_limit, kind,
                                upload_turn_id, &upload_tx, &mut upload_joins,
                            );
                        }
                    }
                    None => {
                        warn!("coordinator_loop: trigger channel closed");
                        rx_open = false;
                    }
                }
                continue;
            }

            // ── 5-second fallback tick (lowest priority) ─────────────────
            _ = interval.tick() => {
                let mode = *display_mode.read().await;
                if mode != DisplayMode::Screenshot || app_id == "local" || app_secret.is_empty() {
                    continue;
                }

                {
                    let shared = last_uploaded_hash.lock().await.clone();
                    if shared != state.last_uploaded_hash {
                        state.last_uploaded_hash = shared.clone();
                        if shared.is_none() { state.last_uploaded_turn_id = None; }
                    }
                }

                let screen = {
                    let guard = backend.lock().await;
                    guard.capture_viewport().await.unwrap_or_default()
                };

                let analyzing = analyzer_runtime.read().await.is_analyzing;
                let base_status = if analyzing { ScreenStatus::Analyzing } else { ScreenStatus::Working };
                let (status, usage_limit) = usage_limit_tracker.lock().await.classify(&screen, base_status, now_ms());
                // Dedup hash is based on rendered visual content (PNG
                // bytes) — invisible control chars are ignored.
                let hash = match screenshot_visual_hash(&screen) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(session = %session_id, "coordinator: screenshot_visual_hash failed: {e}");
                        continue;
                    }
                };

                if handle_trigger(&mut state, Trigger::FallbackTick, Some(&hash)) != Action::Capture {
                    continue;
                }

                if state.upload_in_flight {
                    state.pending_capture = Some(PendingCapture {
                        turn_id: state.current_turn_id.clone(),
                        source: PendingSource::Fallback,
                    });
                    continue;
                }

                let upload_turn_id = state.current_turn_id.clone();
                state.upload_in_flight = true;
                spawn_upload(
                    &stdout, &session_id, &app_id, &app_secret,
                    screen, hash, status, usage_limit, "sampler",
                    upload_turn_id, &upload_tx, &mut upload_joins,
                );
            }
        }
    }
    // Loop runs forever; spawned upload tasks are cleaned up by
    // `upload_joins`'s `Drop` impl when this task is aborted.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `drain_upload_joins` on an empty set returns immediately (None from
    /// `join_next()`), without crashing.
    #[tokio::test]
    async fn drain_empty_join_set_does_not_crash() {
        let mut js = tokio::task::JoinSet::<()>::new();
        drain_upload_joins(&mut js).await;
    }

    /// A normal (non-panicking) task is drained cleanly.
    #[tokio::test]
    async fn drain_handles_completed_task() {
        let mut js = tokio::task::JoinSet::<()>::new();
        js.spawn(async {});
        drain_upload_joins(&mut js).await;
        assert!(js.is_empty());
    }

    /// A panicking upload task's panic must NOT propagate — the drain call
    /// itself returns normally.
    #[tokio::test]
    async fn drain_handles_panicking_task_without_crashing() {
        let mut js = tokio::task::JoinSet::<()>::new();
        js.spawn(async {
            panic!("simulated upload panic");
        });
        drain_upload_joins(&mut js).await;
        assert!(js.is_empty());
    }

    /// The core race: the task signals "result ready" via a oneshot
    /// **before** its future actually returns.  `try_join_next` could
    /// miss the still-in-progress task and leak the handle.
    /// `join_next().await` deterministically waits for true completion.
    #[tokio::test]
    async fn drain_awaits_task_that_signals_before_completing() {
        let mut js = tokio::task::JoinSet::<()>::new();
        let (tx, rx) = tokio::sync::oneshot::channel();

        js.spawn(async move {
            // Signal "result sent" before the task fully returns — this
            // mirrors the upload task sending UploadCompleted and then
            // returning from the async block.
            tx.send(()).unwrap();
            // One more yield to ensure the task doesn't finish inside
            // the same poll cycle.
            tokio::task::yield_now().await;
        });

        // Wait for the signal — the task has "sent its result" but the
        // JoinSet may not have observed its completion yet.
        rx.await.unwrap();

        // join_next().await must wait for true completion, then return.
        drain_upload_joins(&mut js).await;
        assert!(js.is_empty());
    }

    // ── cached_screen_or_none ────────────────────────────────────────────

    /// Non-empty cache returns `Some` — caller should use the cached value.
    #[test]
    fn cached_screen_or_none_returns_some_for_non_empty() {
        let screen = "ansi viewport content";
        let result = cached_screen_or_none(screen);
        assert_eq!(result, Some(screen));
    }

    /// Empty cache returns `None` — caller falls back to backend capture.
    #[test]
    fn cached_screen_or_none_returns_none_for_empty() {
        let result = cached_screen_or_none("");
        assert_eq!(result, None);
    }

    /// The pure function does not observe whitespace-only strings
    /// as "non-empty" — "   " is treated as a present (non-empty) cache.
    #[test]
    fn cached_screen_or_none_whitespace_only_is_non_empty() {
        let result = cached_screen_or_none("   ");
        assert_eq!(result, Some("   "));
    }
}
