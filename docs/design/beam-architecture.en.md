# beam Architecture

Chinese: [beam-architecture.md](beam-architecture.md)

## Core Entities

Beam is a local-first orchestration layer around agent CLIs. It does not invent a new agent runtime. Instead, it keeps one local terminal-backed session per conversation and connects that session to Feishu cards, daemon APIs, worker processes, and optional workflow execution.

Key entities:

- Session: the durable unit that binds a chat/thread, bot, CLI adapter, terminal backend, current screen, display mode, and lifecycle state.
- Daemon: the long-running local process that owns HTTP routes, Feishu callbacks, session persistence, worker supervision, terminal proxying, and workflow coordination.
- Worker: a per-session process that owns the CLI adapter and terminal backend, samples screen state, sends updates, and accepts control messages.
- Backend: the terminal/session implementation, currently centered on zellij with support for managed and adopted sessions.
- Adapter: CLI-specific logic for spawn arguments, transcript polling, final output extraction, usage-limit detection, and session id discovery.
- Card: the Feishu-facing view of a session or workflow wait.
- Event log: the append-only record used by workflow execution and recovery.

## Process Layout

The daemon starts first and loads configuration, bots, sessions, and runtime state. For active sessions it spawns workers as needed. Each worker launches or attaches to a terminal backend, runs the requested CLI, and reports screen/status events back to the daemon.

The daemon persists session state after worker events. Feishu cards are patched from the persisted session snapshot, not from transient worker memory alone. This keeps card rendering recoverable after restarts.

## Session Lifecycle

Typical managed session flow:

1. A user creates or resumes a session from Feishu or the CLI.
2. Daemon creates a session record and starts a worker.
3. Worker spawns the CLI inside the backend.
4. Worker sends `Ready`, then periodic `ScreenUpdate` and optional `ScreenshotUploaded` messages.
5. Daemon persists the updates and patches the streaming card.
6. User actions such as send, close, restart, display toggle, refresh screenshot, or write-link request are handled by daemon routes/callbacks.
7. Daemon sends control messages to the worker.
8. Worker exits or the daemon marks the session closed.

Adopted sessions differ only in backend attachment and metadata. The worker observes and drives an existing zellij pane instead of owning a newly created session.

## Terminal Model

Beam has two terminal-facing surfaces:

- Read-only terminal: opened from the public card link. It goes through the daemon terminal proxy and zellij web read-only token path.
- Writable terminal: intentionally separate and issued through private write-link flows.

The read-only and write boundaries are part of the safety model. Do not make the normal card terminal link writable unless the product requirement explicitly changes.

Screen updates are snapshots sampled by the worker. The card screenshot is a display artifact and should not be treated as the authoritative terminal state. For web terminal details, see `terminal-proxy.md` / `terminal-proxy.en.md`.

## Daemon Responsibilities

The daemon owns:

- Configuration and path discovery.
- Bot registry and per-bot credentials.
- Session persistence.
- Worker process lifecycle.
- Feishu webhook/callback handling.
- Card rendering and patching.
- Terminal proxy auth bridge.
- Workflow run APIs and recovery.
- Public/open daemon APIs needed by CLI commands.

Routes called by CLI commands must be placed in open routes when they cannot carry dashboard tokens.

## Worker Responsibilities

The worker owns:

- CLI spawn spec construction through adapters.
- Terminal backend spawn/attach.
- Raw input and special-key delivery.
- Screen capture and display-mode rendering.
- Screenshot rendering/upload for Feishu cards.
- Adapter polling for transcript, final output, usage limits, and CLI session ids.
- Controlled shutdown and backend cleanup.

The worker should not directly patch Feishu cards. It reports structured messages to the daemon, and the daemon owns external side effects.

## Workflow Layer

Workflows are DAG-based orchestration over agent sessions, human gates, and side effects. The workflow driver writes event-log entries for activity scheduling, completion, wait creation, wait resolution, cancellation, and recovery. Recovery should derive state from the event log instead of trusting in-memory tasks.

## Persistence and Recovery

Session and workflow state must survive daemon restarts. Recovery paths should:

- Reload persisted sessions and bots.
- Reconnect or rescan active terminal backends when possible.
- Re-render cards from stored state.
- Abort or materialize incomplete workflow waits according to the event log.
- Avoid double-applying external side effects.

## Design Rules

- Verify design-doc claims against Rust code before relying on them.
- Keep daemon, worker, backend, adapter, and card responsibilities separated.
- Preserve the read-only vs writable terminal split.
- Prefer structured events and messages over parsing display text.
- Make callback handling idempotent.
- Keep recovery behavior explicit and testable.
