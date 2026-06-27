# beam: TypeScript Parity Plan

Chinese: [beam-parity-plan.md](beam-parity-plan.md)

- Initial draft: 2026-06-03
- Updated by code review: 2026-06-08

## Goal

The Rust implementation should preserve the product behavior that mattered in the TypeScript version while using Rust-native architecture. Parity does not mean copying TypeScript structure. It means users should see the same session lifecycle, Feishu card behavior, terminal access model, adapter behavior, and workflow semantics unless a deliberate product change is documented.

## Baseline Areas

### Session Lifecycle

Rust must support session creation, worker startup, CLI launch, screen updates, close/restart, resume, and durable session state. The daemon remains the authority for session records and card rendering. Workers own terminal and adapter execution.

### Terminal Behavior

The terminal surface must preserve:

- Read-only card links.
- Private writable links.
- Screen snapshot updates.
- Special key delivery.
- Screenshot display mode.
- zellij managed sessions and adopted sessions.

The terminal proxy and card screenshot paths may have different display sizes, but those sizes must be explicitly modeled.

### Feishu Integration

Rust must preserve card creation, card patching, action callbacks, stale-card handling, screenshot image upload, and private write-link delivery. Callback handlers should be idempotent and should validate session/card identity before applying actions.

### CLI Adapters

Each supported CLI needs adapter-specific behavior for:

- Spawn command and arguments.
- Working directory.
- Resume or session id restoration.
- Transcript/final output extraction.
- Usage/rate-limit detection.
- Prompt and raw input delivery.

Same-named commands are not proof of parity; behavior must be tested on the actual runtime path.

### Workflow Runtime

Workflow parity includes DAG execution, human gates, ask hooks, activity terminalization, event-log persistence, recovery after daemon/worker crash, and side-effect idempotency.

## Known Drift Policy

When Rust intentionally differs from TypeScript, record the decision in the parity plan or backlog. Do not silently let Rust-only behavior become the baseline. For bug fixes, state whether the change is restoring parity or intentionally changing the product behavior.

## Validation Strategy

Use layered validation:

- Unit tests for parsers and callback normalization.
- Targeted daemon/worker tests for control messages and lifecycle paths.
- Adapter tests for CLI-specific transcript/session logic.
- Smoke tests for zellij terminal behavior.
- Workflow recovery tests for event-log behavior.

## Documentation Rules

When parity work lands, update:

- The Rust code and tests.
- The parity plan.
- The parity backlog.
- Any manifest or smoke instructions that define the same behavior.
- Chinese and English design docs together.

## Current Priorities

The highest-risk parity areas are terminal/card lifecycle, adapter-specific resume/final-output behavior, and workflow recovery. These paths cross daemon, worker, backend, and external Feishu state, so changes should be narrow and validated against the actual runtime chain.
