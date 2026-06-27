# Codex Type-ahead Steer-aware Attribution: Design

Chinese: [2026-05-28-codex-type-ahead-steer-design.md](2026-05-28-codex-type-ahead-steer-design.md)

- Date: 2026-05-28
- Branch context: `worktree-codex-type-ahead`

## Goal

When a user types ahead while Codex is still working, Beam must attribute the text correctly. Some messages are normal user input for the next turn; others are steering instructions that should affect the currently running turn. The design goal is to preserve responsiveness without losing the causal boundary between user messages, model turns, and terminal state.

## Problem

Terminal sessions can receive user input while a CLI is still streaming or waiting. If all incoming text is appended as ordinary chat, the running CLI may ignore it or the daemon may record it under the wrong turn. If all text is pushed into the terminal immediately, normal follow-up messages can unexpectedly alter the active run.

Beam needs a clear attribution rule for type-ahead input:

- Is this input steering the active task?
- Is it a new turn that should wait for the current task to settle?
- Is it terminal raw input intended for the CLI/TUI?

## Model

The runtime separates three channels:

- Steering input: short instructions meant to influence the active model turn.
- Next-turn input: ordinary user messages queued for a later turn.
- Terminal input: raw keystrokes or text sent to the terminal backend.

Attribution is decided from current session state, active turn id, input source, and explicit user action. The daemon should preserve the original user message and the decision it made, rather than only storing the derived terminal command.

## Runtime Behavior

1. User sends input while the session has an active turn.
2. Daemon classifies the input as steering, queued next-turn input, or raw terminal input.
3. Steering input is attached to the active turn and forwarded through the supported CLI control path.
4. Next-turn input is stored until the current turn reaches a safe boundary.
5. Raw terminal input is sent directly to the backend only from an explicit terminal-control surface.

The classification must be conservative. If the daemon cannot confidently treat a message as steering, it should keep it as next-turn input instead of mutating a running task.

## State and Audit

Every attribution decision should be persisted with:

- Session id.
- Active turn id, if any.
- Original text.
- Chosen attribution.
- Delivery result or failure reason.

This allows later debugging of "why did the agent react to this message?" without reconstructing behavior from terminal logs.

## Boundaries

- Do not treat arbitrary chat messages as raw terminal input.
- Do not let type-ahead bypass permissions or session ownership.
- Do not collapse steering and next-turn queues into one untyped buffer.
- Do not rely only on UI timing; the server-side session state is authoritative.

## Validation

Required tests should cover active-turn steering, queued next-turn input, raw terminal input from the terminal surface, duplicate or reordered delivery, and session restart recovery.
