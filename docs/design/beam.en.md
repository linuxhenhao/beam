# beam: Rust Core Backend Design

Chinese: [beam.md](beam.md)

- Date: 2026-06-02

## Product Frame

Beam is a local-first bridge from Feishu conversations to existing agent CLIs. It keeps the agent running in a real local terminal session and exposes that session through cards, daemon APIs, and web terminal links. Beam should not become a separate agent platform that hides or replaces the CLI.

The core idea is one conversation thread mapped to one local session. Users interact from Feishu, while the actual CLI keeps its normal terminal context, files, credentials, and working directory.

## Architecture Overview

Beam is a Rust workspace:

- `beam-core`: shared config, IPC, session models, workflow/event-log types, and API types.
- `beam-daemon`: long-running process for Feishu integration, HTTP APIs, terminal proxy, session persistence, worker supervision, and workflow orchestration.
- `beam-worker`: per-session process that owns the CLI adapter and terminal backend.
- `beam-cli`: command-line entrypoint for starting/stopping the daemon and sending commands.

The daemon and worker communicate through structured IPC messages. The worker never directly owns Feishu card state; it reports updates and the daemon renders/persists them.

## Session Model

A session binds:

- Beam session id.
- Feishu chat/thread/root message.
- Bot and CLI identity.
- Working directory.
- Terminal backend/session metadata.
- Current screen and screenshot image key.
- Display mode.
- Lifecycle status.
- Optional adopted-session metadata.

The daemon persists sessions so that cards and runtime state can be reconstructed after restart.

## Worker Runtime

For each active session, the daemon starts a worker. The worker:

1. Builds a CLI spawn spec through the selected adapter.
2. Starts or attaches to the terminal backend.
3. Sends `Ready` to the daemon.
4. Samples the terminal screen.
5. Applies display-mode rendering.
6. Sends `ScreenUpdate` and optional `ScreenshotUploaded` events.
7. Accepts input, special keys, refresh, close, restart, and display-mode commands.

Worker logic should stay CLI/backend focused. Daemon logic should own user-facing side effects.

## Terminal and Screenshot Model

Beam separates three viewports:

- Terminal viewport: the real web viewer's interactive size.
- Card viewport: the Feishu screenshot display size.
- Fallback viewport: the temporary size used when no real viewer has reported size.

Current defaults:

- `fallback_cols = 120`
- `fallback_rows = 36`
- `card_cols = 120`
- `card_rows = 36`

The web terminal may resize the real pane through zellij web. Card screenshots are cropped to the card viewport and should not force the real terminal size.

## Feishu Card Lifecycle

Streaming cards represent session state. The daemon should create and refresh terminal cards through the established card paths and must not mark the streaming card as the pending final-response target. Final assistant output should not patch over the terminal card.

Card actions include display toggle, screenshot refresh, read-only terminal open, write-link request, close/restart, export text, and workflow actions. Handlers must validate session identity and stale-card state.

## CLI Passthrough

Daemon slash-command classification should only intercept Beam daemon commands. Unknown slash commands are forwarded to the CLI through `raw_input`, preserving the user's ability to use CLI-native commands.

## Workflow Runtime

Workflows add an event-log-driven DAG layer on top of sessions. Activities can call agents, wait for humans, or run side effects. The event log is the recovery source of truth. Wait resolution, cancellation, worker crash, and side-effect completion must all materialize durable events.

## Recovery Rules

After restart, Beam should:

- Reload config, bots, sessions, and workflow state.
- Restart or reattach workers where appropriate.
- Recover zellij web/proxy dependencies.
- Re-render cards from persisted state.
- Avoid duplicate final-output or side-effect delivery.

## Design Principles

- Keep behavior anchored in real Rust code, not stale docs.
- Preserve local terminal ownership and CLI semantics.
- Keep read-only and writable terminal access separate.
- Use structured IPC/events instead of parsing UI text when possible.
- Make external callbacks idempotent.
- Keep Chinese and English system design docs in sync.
