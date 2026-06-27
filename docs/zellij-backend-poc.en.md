# zellij Backend PoC (`BACKEND_TYPE=zellij`)

Chinese: [zellij-backend-poc.md](zellij-backend-poc.md)

## Goal

Use zellij as a session backend in addition to existing terminal backends, and verify whether Beam can preserve the full product experience: managed sessions, adopted sessions, screen capture, input delivery, liveness detection, web terminal access, and Feishu card screenshots.

## Why zellij

Zellij gives Beam a modern multiplexer with web terminal support and structured commands such as `list-panes`, `dump-screen`, `dump-layout`, `subscribe`, and targeted pane actions. It can support both Beam-managed sessions and adoption of user-created panes.

## Managed Backend

For managed sessions, Beam creates a zellij session and launches the CLI in a pane. The worker sends text, enter, special keys, paste, and raw input through zellij actions. Screen capture uses `dump-screen` or subscription updates.

Managed sessions are owned by Beam. When the session is destroyed, Beam may delete the zellij session.

## Observe/Adopt Backend

For adopted sessions, Beam attaches to an existing zellij session and pane. It should observe and drive the selected pane without destroying user-owned state.

Adoption requires reliable pane discovery:

- Enumerate zellij panes.
- Inspect layout/process metadata.
- Match CLI identity and working directory.
- Reject ambiguous matches.
- Store adopted session metadata for resume.

## Screen Capture

The backend can capture screen state through:

- `zellij action dump-screen --pane-id ...`
- `zellij subscribe --pane-id ... --ansi --format json`

Subscription updates are converted into ANSI chunks for downstream consumers. Dump-screen remains useful for refresh and screenshot paths.

## Input Delivery

Input should target a specific pane id:

- Text: `write-chars`.
- Enter: `send-keys Enter`.
- Paste: `paste`.
- Special keys: translated escape sequences or zellij key names.

Do not rely on whichever pane is currently focused when Beam has a known pane id.

## Liveness

Managed and adopted sessions need different cleanup semantics:

- Managed: Beam owns the session and can delete it on destroy.
- Adopted: Beam should stop observation but must not delete the user's session.

Liveness checks should detect missing sessions/panes and report worker exit without destructive cleanup for adopted panes.

## Web Terminal

Daemon-side zellij web exposure is handled by the terminal proxy design. The backend provides the zellij session/pane state; the daemon terminal proxy handles auth, cookie bridging, read-only anchor behavior, and session-scoped URLs.

## Risks

- `dump-layout` parsing can drift with zellij versions.
- Pane matching can be ambiguous in multi-tab/multi-pane sessions.
- Read-only web clients require anchor behavior in zellij web 0.44.x.
- Screen dimensions can drift between web terminal, dump-screen, and Feishu screenshots if viewport rules are not explicit.
- Adopted-session cleanup must remain non-destructive.

## Validation

The PoC should validate:

- Managed session launch.
- Adopted session discovery and attachment.
- Targeted input delivery.
- Screen capture and subscription updates.
- Liveness and pane-closed behavior.
- Non-destructive adopted-session cleanup.
- Interaction with terminal proxy and card screenshot paths.
