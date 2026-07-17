# Adding a New CLI Adapter

> Based on the `kimi` (Kimi Code CLI) integration landed in 2026-07. This is the complete checklist for adding support for a new AI coding CLI.
> Chinese source: [add-cli-adapter.md](add-cli-adapter.md).

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

- Auto-approve flag: kimi `--yolo`, claude `--dangerously-skip-permissions`, codex `--dangerously-bypass-approvals-and-sandbox`. Uniformly gated by `init.disable_cli_bypass` (omit the flag when true).
- Model: `init.model` → kimi `-m <model>`, gemini `--model <model>`.
- Resume: when `init.resume` is set, use `init.cli_session_id` (falling back to `resume_session_id` / `session_id`); kimi maps to `--session <id>`.
- Initial prompt: only CLIs that support "interactive mode with an initial prompt passed via argv" may be added to `passes_initial_prompt_via_args` (gemini `-i`, opencode). kimi's `-p` is a one-shot non-interactive mode and does **not** qualify — its initial prompt is typed into the TUI by the worker via `write_input`.

## 2. Change checklist

### 2.1 `crates/beam-worker/src/adapter.rs`

- Add a variant to the `AdapterKind` enum.
- Add the `XxxState` struct. Typical fields: `data_dir`, `working_dir`, `transcript_path`, `transcript_offset`, `pending_tail`, `emitted_final_text`, `cli_session_id`.

### 2.2 `crates/beam-worker/src/adapters/<name>.rs`

Implement the four functions: `create_state` / `build_spawn_spec` / `write_input` / `poll`.

- Template: `antigravity.rs` for a single-file transcript; `hermes.rs` or `opencode/` for DB-backed or complex resolution.
- Reuse the shared helpers from `crate::adapter`: `drain_jsonl`, `file_size`, `normalize_history_text`, `realpath_cwd`.
- `poll` contract:
  - Set `final_output`, `final_output_kind = FinalOutputKind::Bridge`, and `prompt_ready = true` together.
  - Deduplicate with `emitted_final_text`; reset the dedup state when a new user turn starts.
  - On truncation (`size < transcript_offset`), reset `transcript_offset` / `pending_tail` / `emitted_final_text`.
- `write_input` contract: `send_text` → 200ms → `send_enter`, then confirm the submit against the transcript with 4×800ms retries, re-sending Enter in between; on final failure return `failure_reason` — never report `submitted` falsely.

### 2.3 `crates/beam-worker/src/adapters/mod.rs`

- `pub mod <name>;`
- Add one arm to each of the 5 dispatch points: `create_adapter` / `build_spawn_spec` / `write_input` / `poll` / `on_spawned`.

### 2.4 Registration points in other crates

| File | When needed |
| --- | --- |
| `CLI_CHOICES` in `beam-cli/src/cli_commands/setup.rs` | Always, otherwise the setup wizard cannot see the CLI |
| `default_cli_args_for_cli_id` in `beam-cli/src/cli_commands/setup.rs` | Only when default args (e.g. bypass flags) should not live inside the adapter |
| `cli_id_from_zellij_command` in `beam-daemon/src/zellij_adopt.rs` | When zellij adopt should recognize the CLI |
| Resume allowlist in `beam-daemon/src/lark_ingress/workflow_actions.rs` | Only when the adapter implements `init.resume` |
| `maybe_inject_term` in `beam-worker/src/worker_runtime/run_loop.rs` | Only when the CLI requires a specific TERM (e.g. codex/traex) |

### 2.5 Docs

- CLI lists in `README.md` / `README.en.md` (mermaid diagram and prerequisites).
- Adapter list in `docs/design/beam.md`, bridge-type diagram in `docs/design/beam-architecture.md`.
- The zh/en pairing rule applies to `docs/design/*.md`: changing one side requires syncing the other; if the English mirror never had the corresponding section, no addition is needed there.

## 3. Tests

### 3.1 Unit tests (`#[cfg(test)]` inside `adapters/<name>.rs`)

- Use a temp HOME plus `crate::adapter::home_test_lock()` to serialize HOME-dependent tests (see `HomeGuard` in `antigravity.rs`).
- Write a `RecordingBackend` mocking `SessionBackend`: on `send_enter`, flush the buffered input into the fake transcript to simulate the CLI recording user input.
- Cover at least:
  - Spawn args: default bypass flag, `disable_cli_bypass`, model, resume.
  - `poll` emitting the final output + same-text dedup; intermediate-step text not emitted.
  - Recovery after file truncation (re-emit works).
  - `write_input` submit-confirmed and not-confirmed paths.
  - Transcript resolution only matches sessions with the right workDir.

### 3.2 Live test

- Name it `live_*` or place it under `tests/live_*.rs`, mark `#[ignore]`, and document requirements (real CLI installed and authenticated, `zellij`) plus the run command.
- Keeping it inside the adapter file's `#[cfg(test)]` module gives access to the crate-private `ZellijBackend`: `ZellijBackend::new(name)` → `spawn` the real CLI → wait for the TUI to be ready (kimi: viewport contains "Welcome to Kimi Code") → `write_input` a prompt → poll `poll` for `final_output`.
- Always clean up: `zellij delete-session -f`, the temporary working directory, and the session data the CLI created for it (for kimi also drop the matching lines from `session_index.jsonl`).

### 3.3 Full verification

```bash
cargo test --workspace --no-fail-fast
scripts/check-rust-line-count.sh   # hard limit: 1000 lines per file
```

## 4. Commit

Commit messages follow `type(scope): 中文描述`; a new adapter is `feat(beam-worker): ...`, which triggers a minor version bump (managed by release-plz).
