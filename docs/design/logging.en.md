# Beam Logging Specification

Chinese: [logging.md](logging.md)

- Date: 2026-07-16
- Status: Actively maintained system design document

This document defines the `tracing` logging contract for all Rust crates in the beam workspace, including level classification, field conventions, sanitization rules, and troubleshooting workflows. All module implementations must treat this document as authoritative.

## 1. Output Targets

- `tracing` output from `beam-cli`, `beam-daemon`, and `beam-worker` processes **must be written to stderr** and must not go to stdout.
- `beam-worker` **stdout is line-delimited JSON IPC**, consumed exclusively by the daemon. Worker tracing must go to stderr; the two must not be mixed.
- When started via `beam start` / `beam restart`, the background daemon appends both stdout and stderr to `logs/daemon.log`. For local troubleshooting you can also tail that file directly, or use the `beam logs` command.

## 2. Level Classification

The five `tracing` levels follow the contract below. Implementations must not introduce additional levels or alter the semantics described here.

| Level | When to use | Examples | Forbidden |
|---|---|---|---|
| `ERROR` | The operation failed after retries/fallback exhausted, and the call boundary cannot fulfill its contract | Critical session persistence failure; worker fails to start after final attempt; unrecoverable workflow execution failure | Re-logging expected HTTP 4xx, recoverable states, or errors already logged by lower layers |
| `WARN` | The service continues to run but is degraded, anomalous, or requires operator attention | Upstream persistently unreachable; retries exhausted; first or state-change screenshot failure streak; IPC protocol violations | Normal timeout semantics, stale CAS results, auth probes, per-round repeated failures |
| `INFO` | Default-visible business/lifecycle results with low frequency and operational value | Daemon/worker start and stop; session/turn creation or completion; config fallback; recovery actions; periodic health summaries | Every HTTP request, auth success, every screen capture, raw input/output |
| `DEBUG` | Per-operation detail needed for troubleshooting; higher frequency is acceptable | Terminal proxy request/auth decisions; screenshot render/upload per-stage timing; CAS discard reasons; adapter candidates and retries | Secrets, user message bodies, raw terminal content, full third-party responses |
| `TRACE` | Extremely high-frequency internal state and protocol decisions; enabled only briefly and selectively | Poll ticks, dedup hits, state-machine branches, raw event length/summary | All secrets and reconstructable user content |

### 2.1 Supplementary Rules

- **High-frequency failures**: For failures in unbounded loops or high-frequency callbacks, use the "first occurrence + state change → `WARN`, remainder → `DEBUG` with rate limiting" strategy. Rate limiting affects only log output; it must not block or alter business retry logic, state updates, or recovery semantics. Rate-limit state must be bounded and reset per session.
- **At most one `ERROR` per error chain per request boundary**, to avoid duplicated upper/lower layer logging.
- **Expected races, stale CAS results, and normal auth probes are normal behavior** and must not be logged as `WARN` (assign to `DEBUG` or `INFO` per the table above).

## 3. Structured Fields

Each structured log event should use the following stable fields wherever applicable. Not every field is required on every event — pick what is relevant.

| Field | Type | Description |
|---|---|---|
| `component` | `&str` | Module or component producing the log, e.g. `terminal_proxy`, `screenshot`, `worker_lifecycle` |
| `operation` | `&str` | Current operation name, e.g. `render`, `upload`, `cas_accept`, `token_create` |
| `outcome` | `&str` | Operation result, e.g. `success`, `failure`, `retry_exhausted`, `skipped` |
| `session_id` | `&str` | Associated session ID |
| `turn_id` | `Option<&str> / Option<String>` | Associated turn ID (if applicable) |
| `bot` | `&str` | Bot name |
| `worker_pid` | `u32` | Worker process PID |
| `trigger` | `&str` | Triggering event, e.g. `event_driven`, `5s_fallback`, `session_start` |
| `status` | `&str` or `u16` | HTTP status code or protocol status |
| `elapsed_ms` | `u64` | Operation duration in milliseconds |
| `retry_count` | `u32` | Current retry count |
| `error` | `%err` | Error description (use `%err` display format; do not embed full response/body) |

### 3.1 Field Usage Guidelines

- `session_id` and `turn_id` are for correlating events within the same flow. Do not log unrelated chat/message/open IDs by default.
- Use `error = %err` for errors. The log message describes the outcome; never concatenate full response/body text.
- When sizing information is needed for diagnostics, record `content_len`, `candidate_count`, `png_bytes`, or an irreversible digest. Never log the content itself.

## 4. Forbidden Content

The following **must never appear** in any log field or message text at any level:

- Tokens, cookies, tickets, app secrets, passwords, Authorization headers
- Credential parameters in URL query strings
- Full user input text (including Lark message bodies)
- Full CLI/terminal screen content
- Raw Lark request/response bodies
- Any partial fragment reconstructable to the above

**Allowed** (as diagnostic substitutes):

- Command exit status codes
- stdout/stderr lengths (`stdout_len`, `stderr_len`)
- Irreversible content digests (e.g. hash prefixes)
- Strategy names (e.g. `named`, `bare`)
- Read-only flags and permission types

## 5. Operations and Troubleshooting

### 5.1 Default Level

All entrypoints default to **`INFO`**. Day-to-day operation must not show per-request, per-screenshot, or per-poll hot-path detail.

### 5.2 Raising Levels for Troubleshooting

Troubleshooting uses restart with the standard `RUST_LOG` environment variable:

```bash
RUST_LOG='beam_daemon=debug,beam_worker=debug,beam_cli=info' target/debug/beam restart
```

To scope down to a specific module:

```bash
RUST_LOG='beam_daemon::terminal_proxy=trace' target/debug/beam restart
```

**`TRACE` must only be enabled briefly.** Collect the needed logs and then restore the default `INFO` level as soon as possible.

### 5.3 Common Troubleshooting Commands

```bash
beam logs                            # View recent logs
beam status                          # Check daemon/worker status
RUST_LOG='beam_daemon=debug' target/debug/beam restart   # Enable daemon debug logging
```

## 6. Compatibility

- This specification depends on Rust `tracing` ecosystem `EnvFilter` directive syntax. Invalid `RUST_LOG` values must not cause process startup failure (guaranteed by `from_env_lossy()`).
- All existing standard `RUST_LOG` directive syntax must remain compatible.
- This specification does not change how `beam logs` reads logs, the on-disk log paths, or the daemon/worker process model.

## References

- Logging overhaul plan: [docs/plans/2026-07-16-logging-levels-plan.md](../plans/2026-07-16-logging-levels-plan.md)
