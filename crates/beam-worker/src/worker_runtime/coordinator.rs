//! Screenshot coordinator — pure state machine for deciding when to capture
//! and upload screenshots.
//!
//! This module contains only deterministic, synchronous logic. No async,
//! tokio, backend, or HTTP dependencies. The runtime integration that
//! drives this state machine lives in [`super::run_loop`].

use super::*;

/// Coordinator state for screenshot capture/upload decisions.
#[derive(Debug, Clone)]
pub(crate) struct CoordinatorState {
    /// Current turn from the last `Message` / `RawInput`.
    pub current_turn_id: String,
    /// Hash of the last uploaded screen within the current turn.
    pub last_uploaded_hash: Option<String>,
    /// The turn_id when `last_uploaded_hash` was set.
    pub last_uploaded_turn_id: Option<String>,
    /// `true` when an upload is in progress.
    pub upload_in_flight: bool,
    /// Pending capture to execute after the in-flight upload completes.
    ///
    /// Written by the runtime when a timer fires while an upload is already
    /// in flight.  Read (and cleared) by the runtime when the upload
    /// completes.  Only the latest pending is retained (latest wins).
    /// Cleared on `TurnStarted` to prevent old-turn data from polluting a
    /// new turn.
    pub pending_capture: Option<PendingCapture>,
    /// Current display mode.
    pub display_mode: DisplayMode,
    /// Whether the current turn is eligible for its first event-driven
    /// screenshot (pane-debounce or message-grace).  Set to `true` by
    /// `TurnStarted`, consumed to `false` by the runtime when either the
    /// pane-debounce or message-grace path performs the first capture.
    pub first_screenshot_eligible: bool,
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            current_turn_id: String::new(),
            last_uploaded_hash: None,
            last_uploaded_turn_id: None,
            upload_in_flight: false,
            pending_capture: None,
            display_mode: DisplayMode::Hidden,
            first_screenshot_eligible: false,
        }
    }
}

/// Describes a deferred capture that should run once the in-flight upload
/// completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingCapture {
    /// The turn that was active when this pending was created.
    pub turn_id: String,
    /// Which trigger path created this pending (affects hash-dedup behaviour).
    pub source: PendingSource,
}

/// Identifies the trigger that produced a [`PendingCapture`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingSource {
    /// Pane-debounce timer expired.
    Debounce,
    /// Message-grace timer expired.
    Grace,
    /// Periodic 5-second fallback tick.
    Fallback,
    /// Explicit refresh request.
    Refresh,
    /// Display mode changed to Screenshot.
    SetDisplayMode,
}

/// Result delivered back to the coordinator loop after an async upload task
/// completes.
#[derive(Debug, Clone)]
pub(crate) struct UploadCompleted {
    /// Whether the full pipeline (render + Feishu + IPC) succeeded.
    pub success: bool,
    /// The screen hash that was (or would have been) uploaded.
    pub hash: String,
    /// The turn_id that was active when the upload was spawned.
    pub turn_id: String,
}

/// Trigger events for the screenshot coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Trigger {
    /// A new turn has started (new `Message` / `RawInput` arrived).
    TurnStarted { turn_id: String },
    /// Terminal pane content has changed (from backend subscribe).
    PaneUpdate,
    /// Explicit user/daemon refresh request.
    Refresh,
    /// Display mode changed.
    SetDisplayMode(DisplayMode),
    /// Message-grace timer expired (500 ms after TurnStarted).
    ///
    /// The runtime now calls [`claim_first_screenshot`] directly instead of
    /// constructing this variant.  It is retained for the pure state-machine
    /// API and tests.
    #[allow(dead_code)]
    GraceTimeout,
    /// Periodic fallback tick (every 5 seconds).
    FallbackTick,
}

/// Action to take after processing a trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action {
    /// Do nothing.
    Skip,
    /// Capture and upload immediately.
    Capture,
    /// Debounce: wait for the given delay (ms), then capture (and upload if
    /// the hash has changed).
    Debounce { delay_ms: u64 },
}

// ---------------------------------------------------------------------------
// Pure transition functions
// ---------------------------------------------------------------------------

/// Decide whether a screen with the given hash needs to be uploaded in the
/// current turn.
///
/// Returns `true` when:
/// - The turn has changed since the last upload (different turn → same hash
///   must **not** be silently dropped).
/// - The screen hash differs from the last uploaded hash *within the same
///   turn*.
pub(crate) fn should_upload(
    current_turn_id: &str,
    screen_hash: &str,
    state: &CoordinatorState,
) -> bool {
    if state.last_uploaded_turn_id.as_deref() != Some(current_turn_id) {
        return true;
    }
    state.last_uploaded_hash.as_deref() != Some(screen_hash)
}

/// Record that a screen with the given hash has been uploaded in the given
/// turn.
pub(crate) fn record_upload(
    state: &mut CoordinatorState,
    current_turn_id: &str,
    screen_hash: &str,
) {
    state.last_uploaded_hash = Some(screen_hash.to_string());
    state.last_uploaded_turn_id = Some(current_turn_id.to_string());
}

/// Process a trigger and decide what action to take.
///
/// * `state` — mutable coordinator state (updated in place).
/// * `trigger` — the event that occurred.
/// * `current_screen_hash` — optional hash of the current viewport screen.
///   Used by triggers that want to check [`should_upload`] before deciding to
///   capture (e.g. `FallbackTick`). Triggers that always capture (e.g.
///   `Refresh`, `GraceTimeout`, `SetDisplayMode(Screenshot)`) ignore this
///   parameter.
pub(crate) fn handle_trigger(
    state: &mut CoordinatorState,
    trigger: Trigger,
    current_screen_hash: Option<&str>,
) -> Action {
    match trigger {
        Trigger::TurnStarted { turn_id } => {
            // Reset turn-specific state so that even a screen with the same
            // hash as the previous turn will be uploaded.
            state.current_turn_id = turn_id;
            state.last_uploaded_turn_id = None;
            // Pending capture from the old turn is no longer relevant.
            state.pending_capture = None;
            // Keep last_uploaded_hash — it is now harmless because
            // last_uploaded_turn_id was cleared.
            // Enable the first event-driven screenshot for this turn.
            state.first_screenshot_eligible = true;
            Action::Skip
        }
        Trigger::PaneUpdate => {
            if state.display_mode != DisplayMode::Screenshot {
                // Hidden mode: do not schedule captures.
                return Action::Skip;
            }
            // Idle guard: no active turn — no screenshots needed.
            if state.current_turn_id.is_empty() {
                return Action::Skip;
            }
            // Only the first PaneUpdate per turn may schedule a debounce.
            if !state.first_screenshot_eligible {
                return Action::Skip;
            }
            Action::Debounce { delay_ms: 250 }
        }
        Trigger::Refresh => Action::Capture,
        Trigger::SetDisplayMode(mode) => {
            state.display_mode = mode;
            if mode == DisplayMode::Screenshot {
                Action::Capture
            } else {
                Action::Skip
            }
        }
        Trigger::GraceTimeout => {
            // Atomically claim first-screenshot eligibility.
            // If the debounce already consumed it, return Skip.
            if !claim_first_screenshot(state) {
                return Action::Skip;
            }
            Action::Capture
        }
        Trigger::FallbackTick => {
            let needs_capture = current_screen_hash
                .is_some_and(|hash| should_upload(&state.current_turn_id, hash, state));
            if needs_capture {
                Action::Capture
            } else {
                Action::Skip
            }
        }
    }
}

/// Atomically check and consume first-screenshot eligibility.
///
/// Returns `true` if the current turn was eligible and eligibility was
/// successfully claimed (consumed to `false`).  Returns `false` if the
/// first screenshot has already been captured or scheduled by another path
/// (e.g. grace already consumed it, or no active turn).
///
/// Callers (the async runtime in [`super::coordinator_runtime`]) use this
/// before any `.await` to prevent the trigger arm from scheduling a second
/// debounce for the same turn.
pub(crate) fn claim_first_screenshot(state: &mut CoordinatorState) -> bool {
    if state.first_screenshot_eligible {
        state.first_screenshot_eligible = false;
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── should_upload ─────────────────────────────────────────────────

    #[test]
    fn different_turn_same_hash_must_upload() {
        let state = CoordinatorState {
            current_turn_id: "turn2".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        assert!(should_upload("turn2", "abc", &state));
    }

    #[test]
    fn same_turn_different_hash_must_upload() {
        let state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        assert!(should_upload("turn1", "xyz", &state));
    }

    #[test]
    fn same_turn_same_hash_skips() {
        let state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        assert!(!should_upload("turn1", "abc", &state));
    }

    #[test]
    fn first_upload_always_passes() {
        let state = CoordinatorState::default();
        assert!(should_upload("turn1", "abc", &state));
    }

    #[test]
    fn different_turn_different_hash_must_upload() {
        let state = CoordinatorState {
            current_turn_id: "turn2".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        assert!(should_upload("turn2", "xyz", &state));
    }

    // ── record_upload ─────────────────────────────────────────────────

    #[test]
    fn record_upload_sets_hash_and_turn() {
        let mut state = CoordinatorState::default();
        record_upload(&mut state, "turn1", "abc");
        assert_eq!(state.last_uploaded_hash.as_deref(), Some("abc"));
        assert_eq!(state.last_uploaded_turn_id.as_deref(), Some("turn1"));
    }

    #[test]
    fn record_upload_overwrites_previous() {
        let mut state = CoordinatorState {
            last_uploaded_hash: Some("old".into()),
            last_uploaded_turn_id: Some("turn0".into()),
            ..Default::default()
        };
        record_upload(&mut state, "turn1", "new");
        assert_eq!(state.last_uploaded_hash.as_deref(), Some("new"));
        assert_eq!(state.last_uploaded_turn_id.as_deref(), Some("turn1"));
    }

    // ── handle_trigger: TurnStarted ───────────────────────────────────

    #[test]
    fn turn_started_resets_turn_state() {
        let mut state = CoordinatorState {
            current_turn_id: "old_turn".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("old_turn".into()),
            pending_capture: Some(PendingCapture {
                turn_id: "old_turn".into(),
                source: PendingSource::Debounce,
            }),
            upload_in_flight: true,
            first_screenshot_eligible: false,
            ..Default::default()
        };
        let action = handle_trigger(
            &mut state,
            Trigger::TurnStarted {
                turn_id: "new_turn".into(),
            },
            None,
        );
        assert_eq!(action, Action::Skip);
        assert_eq!(state.current_turn_id, "new_turn");
        // Turn-specific fields are cleared
        assert_eq!(state.last_uploaded_turn_id, None);
        assert_eq!(state.pending_capture, None);
        // last_uploaded_hash is preserved (now harmless)
        assert_eq!(state.last_uploaded_hash.as_deref(), Some("abc"));
        // upload_in_flight is NOT reset — the runtime manages that
        assert!(state.upload_in_flight);
        // First screenshot eligibility is enabled for the new turn.
        assert!(state.first_screenshot_eligible);
    }

    // ── handle_trigger: PaneUpdate ────────────────────────────────────

    #[test]
    fn pane_update_screenshot_returns_debounce() {
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Screenshot,
            current_turn_id: "turn1".into(),
            first_screenshot_eligible: true,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Debounce { delay_ms: 250 });
    }

    #[test]
    fn pane_update_hidden_skips() {
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Hidden,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, Some("hash1"));
        assert_eq!(action, Action::Skip);
    }

    #[test]
    fn pane_update_during_inflight_still_returns_debounce() {
        // The pure state machine no longer checks upload_in_flight for
        // PaneUpdate — the runtime handles that.  PaneUpdate should still
        // return Debounce so the timer is scheduled.
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Screenshot,
            current_turn_id: "turn1".into(),
            first_screenshot_eligible: true,
            upload_in_flight: true,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Debounce { delay_ms: 250 });
    }

    // ── handle_trigger: Refresh ───────────────────────────────────────

    #[test]
    fn refresh_always_captures() {
        let mut state = CoordinatorState::default();
        let action = handle_trigger(&mut state, Trigger::Refresh, None);
        assert_eq!(action, Action::Capture);
    }

    // ── handle_trigger: GraceTimeout ──────────────────────────────────

    #[test]
    fn grace_timeout_always_captures() {
        let mut state = CoordinatorState {
            first_screenshot_eligible: true,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::GraceTimeout, None);
        assert_eq!(action, Action::Capture);
        // GraceTimeout consumes eligibility at the state-machine level.
        assert!(!state.first_screenshot_eligible);
    }

    // ── handle_trigger: SetDisplayMode ────────────────────────────────

    #[test]
    fn set_display_mode_screenshot_captures() {
        let mut state = CoordinatorState::default();
        let action = handle_trigger(
            &mut state,
            Trigger::SetDisplayMode(DisplayMode::Screenshot),
            None,
        );
        assert_eq!(action, Action::Capture);
        assert_eq!(state.display_mode, DisplayMode::Screenshot);
    }

    #[test]
    fn set_display_mode_hidden_skips() {
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Screenshot,
            ..Default::default()
        };
        let action = handle_trigger(
            &mut state,
            Trigger::SetDisplayMode(DisplayMode::Hidden),
            None,
        );
        assert_eq!(action, Action::Skip);
        assert_eq!(state.display_mode, DisplayMode::Hidden);
    }

    #[test]
    fn hidden_then_set_display_mode_screenshot_captures() {
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Hidden,
            ..Default::default()
        };
        // PaneUpdate in Hidden mode should skip without scheduling
        assert_eq!(
            handle_trigger(&mut state, Trigger::PaneUpdate, Some("ignore_me")),
            Action::Skip
        );

        // Switching to Screenshot triggers a capture
        let action = handle_trigger(
            &mut state,
            Trigger::SetDisplayMode(DisplayMode::Screenshot),
            None,
        );
        assert_eq!(action, Action::Capture);
        assert_eq!(state.display_mode, DisplayMode::Screenshot);
    }

    // ── handle_trigger: FallbackTick ──────────────────────────────────

    #[test]
    fn fallback_tick_new_hash_captures() {
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("old".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::FallbackTick, Some("new_hash"));
        assert_eq!(action, Action::Capture);
    }

    #[test]
    fn fallback_tick_same_hash_skips() {
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::FallbackTick, Some("abc"));
        assert_eq!(action, Action::Skip);
    }

    #[test]
    fn fallback_tick_no_hash_skips() {
        let mut state = CoordinatorState::default();
        let action = handle_trigger(&mut state, Trigger::FallbackTick, None);
        assert_eq!(action, Action::Skip);
    }

    #[test]
    fn fallback_tick_new_turn_same_hash_captures() {
        let mut state = CoordinatorState {
            current_turn_id: "turn2".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        // Different turn, same hash — must not be silently dropped
        let action = handle_trigger(&mut state, Trigger::FallbackTick, Some("abc"));
        assert_eq!(action, Action::Capture);
    }

    // ── Fallback failure-recovery ───────────────────────────────────────

    #[test]
    fn fallback_after_failure_retries_same_hash() {
        // Simulate: first capture gets "new_hash", upload fails
        // (record_upload is NOT called). Next tick with same "new_hash"
        // should still Capture (not permanently suppressed).
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("old_hash".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };

        // First attempt — different hash, triggers capture
        let action = handle_trigger(&mut state, Trigger::FallbackTick, Some("new_hash"));
        assert_eq!(action, Action::Capture);

        // Upload failed — record_upload NOT called, state unchanged.

        // Second tick with same hash — should still capture (retry).
        let action = handle_trigger(&mut state, Trigger::FallbackTick, Some("new_hash"));
        assert_eq!(action, Action::Capture);
    }

    #[test]
    fn fallback_after_successful_upload_skips_same_hash() {
        // Simulate: upload succeeded, record_upload was called.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            ..Default::default()
        };
        record_upload(&mut state, "turn1", "abc123");

        // Next tick with same hash should skip.
        let action = handle_trigger(&mut state, Trigger::FallbackTick, Some("abc123"));
        assert_eq!(action, Action::Skip);
    }

    // ── PendingCapture state-machine behaviour (deterministic) ────────

    #[test]
    fn pending_capture_stored_and_cleared_by_runtime() {
        // Simulate: debounce fires during inflight → runtime stores pending.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            upload_in_flight: true,
            ..Default::default()
        };
        state.pending_capture = Some(PendingCapture {
            turn_id: "turn1".into(),
            source: PendingSource::Debounce,
        });
        assert_eq!(state.pending_capture.as_ref().unwrap().turn_id, "turn1");
        assert_eq!(
            state.pending_capture.as_ref().unwrap().source,
            PendingSource::Debounce
        );

        // Upload completes → runtime clears pending and processes it.
        let pending = state.pending_capture.take();
        assert!(pending.is_some());
        assert!(state.pending_capture.is_none());

        state.upload_in_flight = false;
        record_upload(&mut state, "turn1", "hash_from_pending");
        assert_eq!(
            state.last_uploaded_hash.as_deref(),
            Some("hash_from_pending")
        );
        assert_eq!(state.last_uploaded_turn_id.as_deref(), Some("turn1"));
    }

    #[test]
    fn pending_capture_from_old_turn_is_discarded_after_turn_started() {
        // Old turn's pending must never be processed after a new turn starts.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            upload_in_flight: true,
            pending_capture: Some(PendingCapture {
                turn_id: "turn1".into(),
                source: PendingSource::Fallback,
            }),
            ..Default::default()
        };

        // New turn starts — TurnStarted clears the old pending.
        handle_trigger(
            &mut state,
            Trigger::TurnStarted {
                turn_id: "turn2".into(),
            },
            None,
        );
        assert_eq!(state.current_turn_id, "turn2");
        assert!(state.pending_capture.is_none());

        // Upload completes → no pending to process for the new turn.
        state.upload_in_flight = false;
        let pending = state.pending_capture.take();
        assert!(pending.is_none());
    }

    #[test]
    fn pending_capture_latest_wins() {
        // When multiple captures fire while in flight, only the latest is kept.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            upload_in_flight: true,
            ..Default::default()
        };

        // First pending: debounce
        state.pending_capture = Some(PendingCapture {
            turn_id: "turn1".into(),
            source: PendingSource::Debounce,
        });

        // Second pending: fallback (overwrites)
        state.pending_capture = Some(PendingCapture {
            turn_id: "turn1".into(),
            source: PendingSource::Fallback,
        });

        assert_eq!(
            state.pending_capture.as_ref().unwrap().source,
            PendingSource::Fallback
        );
    }

    #[test]
    fn failed_upload_does_not_prevent_pending_processing() {
        // An upload that fails must still allow the pending to be processed.
        // The state machine itself doesn't know about upload success/failure —
        // the runtime sets upload_in_flight = false regardless and then checks
        // pending.  The important thing is that failed upload does NOT
        // record_upload, so the pending will NOT hit a hash-dedup block.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("old_hash".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            upload_in_flight: true,
            pending_capture: Some(PendingCapture {
                turn_id: "turn1".into(),
                source: PendingSource::Fallback,
            }),
            ..Default::default()
        };

        // Upload failed → runtime sets upload_in_flight = false
        // record_upload is NOT called (hash stays "old_hash").
        state.upload_in_flight = false;

        // Runtime takes pending — should_upload("turn1", "new_hash", state)
        // will return true because last_uploaded_hash still = "old_hash".
        let pending = state.pending_capture.take();
        assert!(pending.is_some());
        assert!(should_upload("turn1", "new_hash", &state));
    }

    #[test]
    fn successful_upload_then_pending_with_same_hash_skips_via_dedup() {
        // After a successful upload with hash "abc", a pending Fallback with
        // the same hash should be skipped by should_upload.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            upload_in_flight: true,
            pending_capture: Some(PendingCapture {
                turn_id: "turn1".into(),
                source: PendingSource::Fallback,
            }),
            ..Default::default()
        };

        // Upload succeeds, record takes effect.
        state.upload_in_flight = false;
        record_upload(&mut state, "turn1", "abc");

        // Runtime takes pending & re-captures → same hash "abc".
        let pending = state.pending_capture.take();
        assert!(pending.is_some());
        assert!(!should_upload("turn1", "abc", &state));

        // Grace pending always uploads (skips dedup check).
        state.pending_capture = Some(PendingCapture {
            turn_id: "turn1".into(),
            source: PendingSource::Grace,
        });
        let pending = state.pending_capture.take();
        assert!(pending.is_some());
        // Grace does NOT check should_upload — handled by runtime.
    }

    // ── External hash sync ──────────────────────────────────────────────

    #[test]
    fn state_sync_from_external_hash_overwrites_local() {
        // The coordinator runtime syncs from the shared last_uploaded_hash
        // before each tick.  When the external hash differs, the local
        // coordinator state is overwritten.
        let mut state = CoordinatorState {
            current_turn_id: "turn1".into(),
            last_uploaded_hash: Some("coord_hash".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        let external_hash = Some("ext_hash".into());

        // Simulate sync
        if external_hash != state.last_uploaded_hash {
            state.last_uploaded_hash = external_hash.clone();
            if external_hash.is_none() {
                state.last_uploaded_turn_id = None;
            }
        }

        assert_eq!(state.last_uploaded_hash.as_deref(), Some("ext_hash"));
        // last_uploaded_turn_id is preserved because external_hash is Some
        assert_eq!(state.last_uploaded_turn_id.as_deref(), Some("turn1"));
    }

    #[test]
    fn state_sync_from_external_none_clears_turn_id() {
        // When the external hash is None (new turn cleared it), the
        // coordinator state must also clear last_uploaded_turn_id so that
        // the next fallback tick treats it as a new turn.
        let mut state = CoordinatorState {
            current_turn_id: "turn2".into(),
            last_uploaded_hash: Some("abc".into()),
            last_uploaded_turn_id: Some("turn1".into()),
            ..Default::default()
        };
        let external_hash: Option<String> = None;

        // Simulate sync
        if external_hash != state.last_uploaded_hash {
            state.last_uploaded_hash = external_hash.clone();
            if external_hash.is_none() {
                state.last_uploaded_turn_id = None;
            }
        }

        assert_eq!(state.last_uploaded_hash, None);
        assert_eq!(state.last_uploaded_turn_id, None);
    }

    // ── First-screenshot eligibility ─────────────────────────────────────

    #[test]
    fn pane_update_idle_skips() {
        // During idle (no active turn), PaneUpdate must not schedule a
        // debounce — it returns Skip.
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Screenshot,
            current_turn_id: String::new(),
            first_screenshot_eligible: true,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Skip);
    }

    #[test]
    fn pane_update_not_eligible_skips() {
        // After the first screenshot has been captured (eligibility
        // consumed), PaneUpdate returns Skip even with an active turn.
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Screenshot,
            current_turn_id: "turn1".into(),
            first_screenshot_eligible: false,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Skip);
    }

    // ── claim_first_screenshot ──────────────────────────────────────────

    #[test]
    fn claim_first_screenshot_when_eligible() {
        let mut state = CoordinatorState {
            first_screenshot_eligible: true,
            ..Default::default()
        };
        assert!(claim_first_screenshot(&mut state));
        assert!(!state.first_screenshot_eligible);
    }

    #[test]
    fn claim_first_screenshot_when_not_eligible() {
        let mut state = CoordinatorState {
            first_screenshot_eligible: false,
            ..Default::default()
        };
        assert!(!claim_first_screenshot(&mut state));
    }

    #[test]
    fn grace_timeout_consumes_eligibility_via_claim() {
        // GraceTimeout must atomically check and consume eligibility.
        let mut state = CoordinatorState {
            first_screenshot_eligible: true,
            ..Default::default()
        };
        let action = handle_trigger(&mut state, Trigger::GraceTimeout, None);
        assert_eq!(action, Action::Capture);
        assert!(!state.first_screenshot_eligible);

        // Second GraceTimeout returns Skip (already consumed).
        let action = handle_trigger(&mut state, Trigger::GraceTimeout, None);
        assert_eq!(action, Action::Skip);
    }

    #[test]
    fn turn_started_reenables_eligibility_after_capture() {
        // Simulate: TurnStarted → PaneUpdate schedules debounce →
        // debounce wins via claim_first_screenshot → eligibility consumed.
        // Then a new TurnStarted → eligibility should be re-enabled.
        let mut state = CoordinatorState {
            display_mode: DisplayMode::Screenshot,
            ..Default::default()
        };

        // First turn.
        handle_trigger(
            &mut state,
            Trigger::TurnStarted {
                turn_id: "turn1".into(),
            },
            None,
        );
        assert!(state.first_screenshot_eligible);

        // PaneUpdate returns Debounce (does not consume eligibility — the
        // runtime consumes it atomically before capture).
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Debounce { delay_ms: 250 });

        // Simulate debounce capture: runtime claims eligibility atomically.
        assert!(claim_first_screenshot(&mut state));

        // PaneUpdate after capture → Skip.
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Skip);

        // New turn re-enables eligibility.
        handle_trigger(
            &mut state,
            Trigger::TurnStarted {
                turn_id: "turn2".into(),
            },
            None,
        );
        assert!(state.first_screenshot_eligible);
        let action = handle_trigger(&mut state, Trigger::PaneUpdate, None);
        assert_eq!(action, Action::Debounce { delay_ms: 250 });
    }
}
