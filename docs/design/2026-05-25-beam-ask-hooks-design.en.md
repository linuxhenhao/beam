# beam askUserQuestion: Move From Skill Triggering to Hook Triggering

Chinese: [2026-05-25-beam-ask-hooks-design.md](2026-05-25-beam-ask-hooks-design.md)

- Date: 2026-05-25
- Scope: replace the old skill-triggered `askUserQuestion` path with a hook-driven flow.
- Status: implemented in Rust. This document keeps the design background and records the current implementation paths below.

## Problem

`askUserQuestion` should be a runtime capability, not a skill convention. The worker needs a reliable way to pause a running task, ask the user a structured question, and resume once the answer is available. Encoding this as a skill-level instruction makes the behavior fragile: the model must remember when to call it, the daemon cannot audit the wait cleanly, and recovery after restart is difficult.

## Target Model

The worker emits an ask hook event when it needs human input. The daemon records the wait in the workflow/session state, renders a Feishu card or message, and later sends the selected answer back to the worker. The worker resumes from the hook result instead of treating the question as ordinary chat text.

Core responsibilities:

- Worker: detects the need for user input, emits a structured hook request, and blocks/resumes around the hook result.
- Daemon: persists the wait, renders the interaction, deduplicates callbacks, validates the selected answer, and sends the result back.
- Feishu card: presents choices and optional free-form input without exposing internal runtime details.
- Event log: records the request, decision, timeout/cancel path, and final resumed state for audit and recovery.

## Runtime Flow

1. Worker reaches a point where human input is required.
2. Worker sends an `askUserQuestion` hook request with question text, options, selection mode, and timeout policy.
3. Daemon appends a durable wait event and renders the Feishu card.
4. User selects an option or submits text.
5. Daemon validates that the callback still targets the active wait.
6. Daemon writes the resolution event and sends the answer to the worker.
7. Worker resumes execution with the structured answer.

Timeouts and cancellation must write explicit terminal events so recovery can distinguish an unresolved wait from a completed one.

## Design Constraints

- The hook request must be structured and versionable.
- Card callbacks must be idempotent. Duplicate clicks should either return the already stored result or a clear stale-card response.
- The worker must not depend on a particular Feishu card layout.
- Recovery after daemon restart should inspect persisted events before deciding whether to re-render, resume, or fail a wait.
- Hook execution must not bypass existing session ownership, chat, or bot routing checks.

Current OpenCode hook coverage includes both `QuestionAsked` and `permission.asked` events. The latter is what carries permission-requirement prompts in the current CLI flow.

## Implementation Notes

The Rust hook client lives in `crates/beam-cli/src/ask_hook.rs`. `beam hook <cliId>` parses the CLI hook payload, posts the ask request to the daemon, waits for the Feishu card answer, and formats the CLI-specific reply.

Hook installation lives in `crates/beam-cli/src/hook_setup.rs`. Claude hooks are written to `~/.claude/settings.json`. OpenCode uses a standalone JavaScript template at `crates/beam-cli/assets/opencode/beam-ask.js`; the Rust binary embeds it with `include_str!` and installs it to `~/.config/opencode/plugins/beam-ask.js`.

OpenCode permission replies keep the asynchronous `permission.asked` event. The plugin replies through the injected v1 OpenCode client with `postSessionIdPermissionsPermissionId({ path: { id: sessionID, permissionID: requestID }, body: { response } })`. It does not use `serverUrl` to reconstruct an HTTP client, because the OpenCode runtime may use in-process transport and may not expose a reachable local listener. It also does not use the v2 SDK unless the plugin runtime explicitly injects a v2 client.

The hook path should use the same daemon-to-worker channel as other worker control messages. Avoid inventing a second side channel. State transitions should be visible in the same event log used by workflow execution, so that cold attach and recovery can reason from a single source of truth.

The card layer should be treated as a view over the wait state. If a card is stale, the callback handler should re-read current state and either self-heal the view or reject the action with a short explanation.

## Validation

Minimum validation should cover:

- A basic ask/resume round trip.
- Duplicate callback handling.
- Stale card callback handling.
- Timeout or cancellation materialization.
- Recovery when the daemon restarts after rendering the question but before receiving an answer.
