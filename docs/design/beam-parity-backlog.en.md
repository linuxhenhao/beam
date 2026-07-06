# beam Parity Backlog

Chinese: [beam-parity-backlog.md](beam-parity-backlog.md)

- Source: `docs/design/beam-parity-plan.md`
- Purpose: track task-level gaps between the Rust implementation and the historical TypeScript behavior.

## How to Use This Backlog

This file is a working queue, not a product spec. Before implementing an item, verify the current Rust code and the current parity plan. If an item is completed, update the backlog, the parity plan, and any parity manifest or targeted tests that define the same behavior.

## Backlog Categories

### Terminal and Backend

- Keep zellij managed sessions and adopted sessions aligned with the expected terminal lifecycle.
- Preserve the read-only card terminal link and the private writable link split.
- Ensure screen capture, screenshot rendering, and web terminal behavior use consistent viewport rules.
- Validate that special keys, paste, raw input, and enter handling work across supported adapters.
- Keep backend liveness detection and cleanup non-destructive for adopted sessions.

### Feishu Cards and Callbacks

- Ensure streaming cards are created and refreshed through the same lifecycle.
- Prevent final assistant output from overwriting terminal cards.
- Keep stale-card callbacks idempotent and self-healing where possible.
- Preserve screenshot display mode and refresh behavior across worker restarts.
- Validate screenshot upload credentials are resolved from bot state when sessions are created internally.

### CLI Adapter Parity

- Track per-CLI spawn args, environment, session id discovery, final output extraction, usage-limit detection, and resume support.
- Keep adapter-specific polling from leaking into generic daemon logic.
- Add targeted tests whenever adapter parsing behavior is changed.

### Workflow Runtime

- Keep workflow scheduling, wait resolution, cancellation, and recovery behavior event-log driven.
- Ensure human gates, ask hooks, and external side effects write terminal events.
- Avoid duplicate side-effect execution during retry or cold recovery.

### Rust Send Parity

- **Status:** ✅ Completed — key semantic gaps filled; known limitations below. Implemented: CLI flags, mention hard gates, target/quote rules, voice error, partial attachment failure, multilingual prompt, mention-back sender accuracy, attention state reporting/clearing and constraints, images inline in card, bot-to-bot auto mention, minimal human-first footer subset, send marker dedup, quote target withdrawn fallback, off-topic sub-bot hint. See "本轮实现完成" in `docs/plans/2026-07-05-beam-send-parity-plan.md` for details.
- **Plan:** `docs/plans/2026-07-05-beam-send-parity-plan.md`
- **Known limitations:** footer is a minimal human-first subset (full oncall/roster is future enhancement); off-topic hint is delivered via daemon warn log, not inline Lark message hint.
- **CLI flags:** `--mention` / `--mention-back` / `--no-mention` / `--content-file` / `--files` / `--images` / `--top-level` / `--chat-id` / `--into` / `--quote` / `--no-quote` / `--card` / `--text` / `--anyway` / `--attention` / `--voice`
- **Daemon delivery:** session/bot resolution, file/image upload, partial attachment failure handling (does not retry main message).
- **Target/quote rules:** chat scope quote chain, thread scope default reply, `--top-level` forces top-level, `--no-quote` disables quoting.
- **Voice:** explicitly rejected with a clear error when TTS is unavailable.
- **Backward compatibility:** old `{ "content": "..." }` requests are delegated to the legacy delivery path.

### Release and Operations

- Keep release workflow semantics aligned with `release-plz` and package-prefixed tags.
- Do not hand-edit crate versions or lockfile versions.
- Maintain daemon restart/build instructions in `AGENTS.md` when runtime behavior changes.

## Completion Criteria

An item should be considered complete only when:

- The Rust behavior has been checked against the intended baseline.
- Targeted tests or smoke validation cover the changed path.
- Related design docs are updated in Chinese and English.
- The backlog and parity plan no longer list the item as an open gap.
