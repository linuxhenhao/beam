# beam

- Pure Rust workspace. Crates under `crates/`: `beam-cli`, `beam-core`, `beam-daemon`, `beam-worker`.
- Use progressive disclosure for repo context. Start with `AGENTS.md` only, then read the minimum relevant design docs instead of loading all `docs/` up front.
- Design-doc routing:
  - Core runtime/session/card/worker/daemon changes: read `docs/design/beam.md` first, then `docs/design/beam-architecture.md`.
  - Current parity status / known drift / remaining gaps: read `docs/design/beam-parity-plan.md`, then `docs/design/beam-parity-backlog.md` if you need task-level status.
  - Platform, team roster, multi-bot collaboration: read `docs/platform-design.md`.
  - Cross-deployment federation: read `docs/federation-design.md`.
  - Ask hook flow: read `docs/design/2026-05-25-beam-ask-hooks-design.md` and `docs/plans/2026-05-25-beam-ask-hooks.md`.
  - Zellij backend or adopt work: read `docs/zellij-backend-poc.md`.
- Do not treat design docs as automatically authoritative. Verify critical behavior against the Rust code, and if you rely on a doc that has drifted from code, update the doc in the same change.
- Build daemon: `cargo build -p beam-cli`, binary at `target/debug/beam`.
- After daemon/runtime changes, rebuild with `cargo build -p beam-cli` then restart with `beam restart`.
- Lifecycle commands: `beam start` (background daemon), `beam stop`, `beam restart`, `beam logs`, `beam status`.
- Run tests: `cargo test --workspace --no-fail-fast`; narrower: `cargo test -p <crate> <filter>`.
- There is no repo `lint` or `format` script; do not assume one exists.
- Commit messages use `type(scope): 中文描述` (conventional commits).
  - `feat:` → minor version bump (0.x.0)
  - `fix:` → patch version bump (0.0.x)
  - `BREAKING CHANGE` footer or `!` after type (e.g. `feat!:` ) → major version bump (x.0.0)
  - Other types (`docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`, `revert`) appear in changelog but do NOT trigger version bumps.
- **Do not hand-edit `Cargo.toml` / `Cargo.lock` versions.**
  - Version bumps, internal path-dependency version updates, and changelogs are automatically managed by `release-plz`.
- Release flow (3 stages, automated via GitHub Actions):
  1. **Release PR** — On every push to `master`, `.github/workflows/release-plz.yml` runs `release-plz PR` to create or update a single Release PR containing version bumps, dependency updates, and the generated changelog. Nothing is published yet.
  2. **Git Tag + GitHub Release** — When the Release PR is merged, the same workflow runs `release-plz release`, which creates a git tag and a GitHub Release with the changelog. The `v*` tag push also triggers `.github/workflows/release.yml` to build the `beam-cli` binary and upload it as a release asset. **No crates.io publish occurs at this stage.**
  3. **crates.io Publish** — Any `v*` tag push or manual `workflow_dispatch` triggers `.github/workflows/publish.yml`. A `validate-tag` job checks that the tag is a **stable semver** (`vX.Y.Z` only; no prerelease suffix); prerelease tags are skipped without triggering the production environment. Only stable tags proceed to the `publish` job, which is gated by the `production` GitHub Environment (can be configured with required reviewers). Crates are published in topological order: `beam-core` → `beam-daemon` → `beam-worker` → `beam-cli`, each with dry-run retry first, then publish retry (handles crates.io index delay).
  - **Prerelease tags** (e.g. `vX.Y.Z-beta.1`, `vX.Y.Z-canary.1`) trigger the binary build & GitHub Release upload, but are **NOT** published to crates.io (crates.io has no dist-tag / non-latest concept).
- Do not add TypeScript code to this repo. The TS daemon has been removed.
- Rust daemon CLI passthrough: `classify_lark_text_action` in `crates/beam-daemon/src/lib.rs` passes through any `/slash` command that is not a beam daemon command (`/close`, `/restart`, `/card`, `/adopt`, `/workflow`). Unknown `/` commands are forwarded verbatim via `raw_input` to the CLI.
- Card lifecycle: `ensure_lark_streaming_card` (main new-card path) and `post_or_refresh_lark_session_card` (show-card/"Refresh" path) both create streaming cards. DO NOT call `start_pending_response_turn` on the streaming card — it marks the streaming card as the pending response target, causing `deliver_final_output_once` to PATCH-overwrite the terminal card with reply content.
- When creating sessions via `create_session_internal`, resolve `lark_app_secret` from `state.bots` (like `build_init_from_session` does). An empty secret blocks screenshot uploads.
- Place daemon API routes that the CLI `send` command calls (`/sessions/{id}/final-output`) in `open_routes`, not `protected_dashboard` (which requires a dashboard token).

## Required GitHub repo settings

These must be configured manually (one-time setup) for the release pipelines to work:

### Secrets (repo → Settings → Secrets and variables → Actions)
| Secret | Purpose |
|--------|---------|
| `CRATES_IO_TOKEN` | crates.io API token (scope: `publish-update`). Used by `.github/workflows/publish.yml`. |

### Environments (repo → Settings → Environments)
| Environment | Configuration |
|-------------|---------------|
| `production` | Create this environment. Optionally add **required reviewers** (up to 6) to gate crates.io publish. The `publish.yml` workflow references `environment: production`. |

### Branch protection (optional but recommended)
- Protect `master`: require status checks (`Parity Gate` / `rust-tests`) to pass before merging.
- Require a pull request before merging (the Release PR workflow depends on PR merges to trigger Stage 2).
