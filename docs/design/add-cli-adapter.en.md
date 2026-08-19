# Adding a New CLI Adapter

> Reflects the trait + registry architecture introduced in the 2026-07 refactor.
> Chinese source: [add-cli-adapter.md](add-cli-adapter.md).

## 0. Architecture at a glance

- `crates/beam-worker/src/adapter.rs` defines the `Adapter` trait (async_trait), shared helpers (`TranscriptCursor`, `confirm_submit_loop`, `drain_jsonl`, `file_size`, `normalize_history_text`, `realpath_cwd`), and the `test_support` module (`home_test_lock` / `set_home` / `temp_home` / `test_init`).
- Each adapter is **a single file** `crates/beam-worker/src/adapters/<name>.rs`: its own state struct + `impl Adapter` + `pub fn create(init: &InitConfig) -> Box<dyn Adapter>`.
- `REGISTRY` in `crates/beam-worker/src/adapters/mod.rs` maps cli_id → factory; a test guarantees REGISTRY and `CLI_SPECS` stay in sync.
- `CLI_SPECS` in `crates/beam-core/src/cli_specs.rs` is the **single source of truth for cross-crate CLI metadata**: the setup wizard (label, bin probing, default args), zellij adopt recognition, the workflow resume allowlist, TERM injection, and initial-prompt-via-args all read it. beam-cli, beam-daemon, and beam-worker consume this table directly.

## 1. Investigation first (the most important step)

### 1.1 Session storage (transcript bridge)

beam derives the final output by reading the CLI's on-disk session record, not by scraping the terminal screen. Locate the target CLI's transcript first, e.g.:

- `claude`: `~/.claude/**/*.jsonl`
- `hermes`: `~/.hermes/state.db` (SQLite)
- `kimi`: `~/.kimi-code/session_index.jsonl` pointing at `sessions/<wd_*>/session_*/agents/main/wire.jsonl`

You must answer three questions:

1. **How to locate the current session's transcript by working directory.** kimi's approach: read `session_index.jsonl`, filter by `workDir` (also compare the `realpath_cwd`-normalized form), pick the `wire.jsonl` with the newest mtime; when the index is missing, fall back to scanning `state.json` files.
2. **How to confirm a user input was submitted.** kimi: a `turn.prompt` event with matching text appears in the transcript.
3. **How to detect turn completion and extract the final reply.** kimi: accumulate `content.part` events (`part.type == "text"`) within one step of `context.append_loop_event`, emit when a `step.end` with `finishReason == "end_turn"` arrives; text from intermediate steps (`finishReason == "tool_use"`) is progress narration and must not be emitted.

### 1.2 Spawn arguments

- Static launch flags (auto-approve, `--no-alt-screen`, disallowed-tools, and similar) belong only in `CLI_SPECS.default_cli_args` and are written to `bots.json` `cliArgs` by setup. Adapters must not inject those flags; an empty `cliArgs` means the CLI starts with no static flags. Session-scoped args (`--session-id` / `--resume` / `--model` / initial prompt) are still generated from `init`.
- Model: `init.model` → kimi `--model <model>`, gemini `--model <model>`.
- Resume: when `init.resume` is set, use `init.cli_session_id` (falling back to `resume_session_id` / `session_id`); kimi maps to `--session <id>`.
- Initial prompt: only CLIs that support "interactive mode with an initial prompt passed via argv" may set `passes_initial_prompt_via_args: true` in `CLI_SPECS` (gemini `-i`, opencode). kimi's `-p` is a one-shot non-interactive mode and does **not** qualify — its initial prompt is typed into the TUI by the worker via `write_input`.
- TUI-ready gate: TUI CLIs drop keystrokes typed before their input UI is initialized. After spawn and before signaling `Ready`, the worker polls the viewport until the case-insensitive substring in `CLI_SPECS.tui_ready_marker` appears, then lets the first input (initial prompt / first stdin message) through. Use the exact welcome text when known (kimi `"Welcome to Kimi Code"`, grok `"Grok"`, Codex/Traex `"›"`), the generic `"Welcome"` otherwise; `None` disables the wait (gemini/opencode pass the initial prompt via argv). Adopted sessions attach to an already-running CLI, so the wait is skipped. Codex `write_input` also refuses to type if `›` is still missing.

## 2. Change checklist (3 code touch points)

### 2.1 `crates/beam-core/src/cli_specs.rs`: add one `CliSpec` row

```rust
CliSpec {
    cli_id: "mynewcli",
    label: "MyNewCli",                    // setup wizard display name
    bin_candidates: &["mynewcli"],        // PATH candidates probed by setup
    default_cli_args: &[],                // default launch args suggested by setup
    adopt_command_patterns: &["mynewcli"],// zellij adopt substring match; empty = never auto-recognized
    supports_resume: true,                // only when the adapter implements init.resume
    passes_initial_prompt_via_args: false,// see §1.2
    tui_ready_marker: Some("Welcome"),    // TUI ready marker (case-insensitive); None = no wait
    inject_term_xterm: false,             // only when the CLI requires xterm-256color
},
```

This single row drives the setup wizard, bin probing, `default_cli_args_for_cli_id`, zellij adopt recognition, the workflow resume allowlist, and the worker's TERM injection — **no match arms to add anywhere else**. The table's unit tests lock the field semantics; `adapters/mod.rs` has a test ensuring every `CLI_SPECS` entry has a factory.

### 2.2 `crates/beam-worker/src/adapters/<name>.rs`: implement `Adapter`

One file holds everything: the state struct, a `state_from_init` constructor, `pub fn create(init: &InitConfig) -> Box<dyn Adapter>`, and `#[async_trait] impl Adapter`.

- Template: `antigravity.rs` for a single-file JSONL transcript; `kimi.rs` for workDir-based transcript resolution; `hermes.rs` / `opencode.rs` for DB-backed or complex resolution.
- **Required** methods: `build_spawn_spec` / `write_input` / `poll`.
- **Do not hand-roll boilerplate** — use the shared pieces in `crate::adapter`:
  - `TranscriptCursor` (for JSONL transcripts): `drain(path)` handles truncation resets and offset/tail bookkeeping; `emit_if_new(text)` dedupes same-text finals; `reset_dedupe()` when a new user turn starts; `skip_to(size)` to baseline an adopted session past its history. Do not carry `transcript_offset` / `pending_tail` / `emitted_final_text` fields in your state.
  - Queue-style TUIs (grok / kimi / codex) use `composer::confirm_typed_submit`: accepted means the CLI took the input. Sample the composer's real-input color before submit; after submit, an empty box or a uniformly colored payload that is not that color is a placeholder / queued follow-up — do not press the submit key again. Resubmit only if the draft is still there. Transcript is a side signal for idle sends. Other CLIs may still use `confirm_submit_loop` (4×800ms, extra Enter). On final failure return `failure_reason` — never report `submitted` falsely.
- `poll` contract: set `final_output`, `final_output_kind = FinalOutputKind::Bridge`, and `prompt_ready = true` together; never emit intermediate-step text.
- **Optional capability hooks** (default no-op; most adapters don't need them):
  - `on_spawned(child_pid)`: when you need the CLI process PID (claude, codex).
  - `resolve_transcript_source` / `set_transcript_source`: resolve the transcript source at init/adopt time and let the user pick on ambiguity (see `opencode.rs`; returning `None` means "no such capability" and the run loop skips it automatically).

### 2.3 `crates/beam-worker/src/adapters/mod.rs`: `pub mod` + one REGISTRY row

```rust
pub mod mynewcli;
// in REGISTRY:
("mynewcli", mynewcli::create),
```

### 2.4 Optional registration points (most adapters don't touch these)

| Location | When needed |
| --- | --- |
| `parse_questions` / `format_answer` / `passthrough` in `beam-cli/src/ask_hook.rs` | Only when the CLI has a question/permission hook protocol (see claude/opencode) |
| `install_hooks_at` in `beam-cli/src/hook_setup.rs` | Only when hook config must be written into the CLI's config directory |

### 2.5 Docs

- CLI lists in `README.md` / `README.en.md` (mermaid diagram and prerequisites).
- Adapter list in `docs/design/beam.md`, bridge-type diagram in `docs/design/beam-architecture.md`.
- The zh/en pairing rule applies to `docs/design/*.md`: changing one side requires syncing the other; if the English mirror never had the corresponding section, no addition is needed there.

## 3. Tests

### 3.1 Unit tests (`#[cfg(test)]` inside `adapters/<name>.rs`)

- Use `crate::adapter::test_support`: `test_init(cli_id)` builds the 25-field `InitConfig` (override fields with struct-update syntax, `..test_init("...")`); `temp_home` + `set_home` (`HomeGuard`) + `home_test_lock` serialize HOME-dependent tests. **Do not** re-declare these four per adapter.
- Write a `RecordingBackend` mocking `SessionBackend`: on `send_enter`, flush the buffered input into the fake transcript to simulate the CLI recording user input.
- Cover at least:
  - Spawn args: pass `cliArgs` through unchanged (no implicit static flags); cover dynamic model / resume / session-id.
  - `poll` emitting the final output + same-text dedup; intermediate-step text not emitted.
  - Recovery after file truncation (re-emit works).
  - `write_input` submit-confirmed and not-confirmed paths.
  - Transcript resolution only matches sessions with the right workDir.

### 3.2 Live test

- Name it `live_*` or place it under `tests/live_*.rs`, mark `#[ignore]`, and document requirements (real CLI installed and authenticated, `zellij`) plus the run command.
- Keeping it inside the adapter file's `#[cfg(test)]` module gives access to the crate-private `ZellijBackend`: `ZellijBackend::new(name)` → `spawn` the real CLI → wait for the TUI to be ready (kimi: viewport contains "Welcome to Kimi Code") → `write_input` a prompt → poll `poll` for `final_output`.
  - The "wait for TUI ready" marker in live tests is the same `CLI_SPECS.tui_ready_marker`: the runtime waits for it automatically before the first input (see §1.2); the live test keeps the explicit wait to prove the marker actually matches the real CLI.
- Always clean up: `zellij delete-session -f`, the temporary working directory, and the session data the CLI created for it (for kimi also drop the matching lines from `session_index.jsonl`).

### 3.3 Full verification

```bash
cargo test --workspace --no-fail-fast
scripts/check-rust-line-count.sh   # hard limit: 1000 lines per file
```

## 4. Commit

Commit messages follow `type(scope): 中文描述`; a new adapter is `feat(beam-worker): ...`, which triggers a minor version bump (managed by release-plz).
