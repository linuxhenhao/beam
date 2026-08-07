# 新增 CLI Adapter 指南

> 本文对应 2026-07 重构后的 trait + 注册表架构。英文镜像：[add-cli-adapter.en.md](add-cli-adapter.en.md)。

## 0. 架构速览

- `crates/beam-worker/src/adapter.rs` 定义 `trait Adapter`（async_trait）、共享 helper（`TranscriptCursor`、`confirm_submit_loop`、`drain_jsonl`、`file_size`、`normalize_history_text`、`realpath_cwd`）以及测试支持 `test_support`（`home_test_lock` / `set_home` / `temp_home` / `test_init`）。
- 每个 adapter 就是 `crates/beam-worker/src/adapters/<name>.rs` **一个文件**：自己的 state 结构体 + `impl Adapter` + `pub fn create(init: &InitConfig) -> Box<dyn Adapter>`。
- `crates/beam-worker/src/adapters/mod.rs` 里的 `REGISTRY` 把 cli_id 映射到工厂函数，并有测试保证 REGISTRY 与 `CLI_SPECS` 一致。
- `crates/beam-core/src/cli_specs.rs` 的 `CLI_SPECS` 是**跨 crate 元数据的唯一来源**：setup 向导（label、bin 探测、默认参数）、zellij adopt 识别、workflow resume 允许列表、TERM 注入、initial-prompt-via-args 全部由它驱动。beam-cli / beam-daemon / beam-worker 都直接读这张表。

## 1. 动手前调研（最关键的一步）

### 1.1 会话存储（transcript bridge）

beam 的最终输出依赖「读 CLI 落盘的会话记录」，而不是解析终端屏幕。先找到目标 CLI 的 transcript 位置与格式，例如：

- `claude`：`~/.claude/**/*.jsonl`
- `hermes`：`~/.hermes/state.db`（SQLite）
- `kimi`：`~/.kimi-code/session_index.jsonl` 定位 `sessions/<wd_*>/session_*/agents/main/wire.jsonl`

必须确认三件事：

1. **如何按 working_dir 定位当前会话的 transcript**。kimi 的做法：读 `session_index.jsonl`，按 `workDir` 过滤（同时比较 `realpath_cwd` 归一化后的形式），取 `wire.jsonl` mtime 最新者；index 缺失时退化为扫描 `state.json`。
2. **如何确认用户输入已提交**。kimi：transcript 里出现文本匹配的 `turn.prompt` 事件。
3. **如何界定一轮结束并提取最终回复**。kimi：一个 step 内累积 `context.append_loop_event` 的 `content.part`（`part.type == "text"`），在 `step.end` 且 `finishReason == "end_turn"` 时产出；中间步骤（`finishReason == "tool_use"`）的文本只是进度旁白，不产出。

### 1.2 启动参数

- 自动批准 flag：kimi `--yolo`、claude `--dangerously-skip-permissions`、codex `--dangerously-bypass-approvals-and-sandbox`。统一受 `init.disable_cli_bypass` 控制（true 时不加）。
- model：`init.model` → kimi `--model <model>`、gemini `--model <model>`。
- resume：`init.resume` 时用 `init.cli_session_id`（fallback `resume_session_id` / `session_id`），kimi 对应 `--session <id>`。
- 初始 prompt：只有支持「交互模式 + 命令行传入初始 prompt」的 CLI 才能在 `CLI_SPECS` 里设 `passes_initial_prompt_via_args: true`（gemini `-i`、opencode）。kimi 的 `-p` 是一次性非交互模式，**不能**算——它的初始 prompt 由 worker 经 `write_input` 在 TUI 里输入。
- TUI ready gate：TUI 类 CLI 在输入 UI 就绪前会丢弃键入的按键。worker 在 spawn 后、发送 `Ready` 前轮询 viewport，等待 `CLI_SPECS` 里 `tui_ready_marker` 指定的子串（大小写不敏感）出现，再放行第一条输入（初始 prompt / 首条 stdin 消息）。已知 marker 用精确欢迎语（kimi `"Welcome to Kimi Code"`），未知的用通用 `"Welcome"`；`None` 表示不等待（codex/traex 在 `write_input` 内部自守，gemini/opencode 初始 prompt 走命令行参数）。adopt 会话连的是已运行 CLI，跳过等待。

## 2. 改动清单（3 个代码触点）

### 2.1 `crates/beam-core/src/cli_specs.rs`：加一行 `CliSpec`

```rust
CliSpec {
    cli_id: "mynewcli",
    label: "MyNewCli",                    // setup 向导显示名
    bin_candidates: &["mynewcli"],        // setup 探测 PATH 用的候选 bin 名
    default_cli_args: &[],                // setup 向导默认启动参数（如必须）
    adopt_command_patterns: &["mynewcli"],// zellij adopt 子串识别；空数组 = 不自动识别
    supports_resume: true,                // 实现了 init.resume 才为 true
    passes_initial_prompt_via_args: false,// 见 §1.2
    tui_ready_marker: Some("Welcome"),    // TUI 就绪标记（大小写不敏感）；None = 不等待
    inject_term_xterm: false,             // 仅当 CLI 要求 xterm-256color
},
```

这一行同时驱动 setup 向导、bin 探测、`default_cli_args_for_cli_id`、zellij adopt 识别、workflow resume 允许列表和 worker 的 TERM 注入——**不再需要在其它 crate 里加任何 match 臂**。表自带单测锁住字段语义；`adapters/mod.rs` 有测试保证每个 `CLI_SPECS` 条目都有对应工厂。

### 2.2 `crates/beam-worker/src/adapters/<name>.rs`：实现 `Adapter`

一个文件装下全部：state 结构体、`state_from_init` 构造函数、`pub fn create(init: &InitConfig) -> Box<dyn Adapter>`、`#[async_trait] impl Adapter`。

- 模板选择：单文件 JSONL transcript 参考 `antigravity.rs`；按 workDir 定位 transcript 参考 `kimi.rs`；DB / 复杂定位参考 `hermes.rs`、`opencode.rs`。
- **必须实现**三个方法：`build_spawn_spec` / `write_input` / `poll`。
- **不要手写样板**，直接用 `crate::adapter` 的共享件：
  - `TranscriptCursor`（JSONL transcript 专用）：`drain(path)` 内含截断重置与 offset/tail 维护；`emit_if_new(text)` 同文去重；`reset_dedupe()` 新用户 turn 时调用；`skip_to(size)` adopt 基线跳过历史。state 里不要再带 `transcript_offset` / `pending_tail` / `emitted_final_text` 字段。
  - `confirm_submit_loop(backend, || ...)`：实现「4×800ms 用 transcript 确认提交，未确认时补 Enter」的统一策略。发送阶段（`send_text`、换行方式、首个 Enter 前的 sleep）各 adapter 不同，自行保留；确认阶段一律用这个 helper。最终失败必须返回 `failure_reason`，不要谎报 `submitted`。
- `poll` 约定：`final_output`、`final_output_kind = FinalOutputKind::Bridge`、`prompt_ready = true` 三者一起设置；中间步骤文本不产出。
- **可选能力钩子**（默认 no-op，多数 adapter 不需要）：
  - `on_spawned(child_pid)`：需要跟踪 CLI 进程 PID 时（claude、codex）。
  - `resolve_transcript_source` / `set_transcript_source`：init/adopt 时解析 transcript 源、歧义时交给用户选择（参考 `opencode.rs`；返回 `None` 表示无此能力，run_loop 自动跳过）。

### 2.3 `crates/beam-worker/src/adapters/mod.rs`：`pub mod` + 一行 REGISTRY

```rust
pub mod mynewcli;
// REGISTRY 里加：
("mynewcli", mynewcli::create),
```

### 2.4 可选注册点（多数 adapter 不用动）

| 位置 | 何时需要 |
| --- | --- |
| `beam-cli/src/ask_hook.rs` 的 `parse_questions` / `format_answer` / `passthrough` | 仅当 CLI 有提问/权限确认 hook 协议（参考 claude/opencode） |
| `beam-cli/src/hook_setup.rs` 的 `install_hooks_at` | 仅当需要向 CLI 的安装目录写 hook 配置 |

### 2.5 文档

- `README.md` / `README.en.md` 的 CLI 清单（mermaid 图与前置要求）。
- `docs/design/beam.md` 的 adapter 清单、`docs/design/beam-architecture.md` 的 bridge 类型图。
- 中英配对规则适用于 `docs/design/*.md`：改动一边必须同步另一边；若英文镜像本来就没有对应段落，则无需补。

## 3. 测试

### 3.1 单元测试（`adapters/<name>.rs` 的 `#[cfg(test)]`）

- 用 `crate::adapter::test_support`：`test_init(cli_id)` 构造 24 字段 `InitConfig`（需要覆盖字段时用结构体更新语法 `..test_init("...")`）；`temp_home` + `set_home`（`HomeGuard`）+ `home_test_lock` 串行化对 HOME 的依赖。**不要**再在测试里自定义这四件套。
- 写一个 `RecordingBackend` mock `SessionBackend`：`send_enter` 时把缓冲的输入写进假 transcript，模拟 CLI 记录用户输入。
- 至少覆盖：
  - spawn 参数：默认 bypass flag、`disable_cli_bypass`、model、resume。
  - `poll` 产出 final output + 同文去重；中间步骤文本不产出。
  - 文件截断后能恢复并重新产出。
  - `write_input` 确认提交 / 未确认返回失败两条路径。
  - transcript 定位只认匹配 workDir 的会话。

### 3.2 live 测试

- 命名 `live_*` 或放 `tests/live_*.rs`，标 `#[ignore]`，注释写明依赖（真实 CLI 已安装并登录、`zellij`）与运行命令。
- 放在 adapter 文件内的 `#[cfg(test)]` 里可以直接用 crate 私有的 `ZellijBackend`：`ZellijBackend::new(name)` → `spawn` 真跑 CLI → 等 TUI ready（kimi 等 viewport 出现 "Welcome to Kimi Code"）→ `write_input` 提交 → 轮询 `poll` 拿 `final_output`。
  - live 测试里「等 TUI ready」的 marker 就是 `CLI_SPECS` 的 `tui_ready_marker`：运行时会在第一条输入前自动等待它（见 §1.2），live 测试保持显式等待是为了验证该 marker 对真实 CLI 有效。
- 必须清理：`zellij delete-session -f`、临时工作目录、CLI 为该目录生成的 session 数据（kimi 还需清掉 `session_index.jsonl` 里的对应行）。

### 3.3 全量验证

```bash
cargo test --workspace --no-fail-fast
scripts/check-rust-line-count.sh   # 单文件 1000 行硬限制
```

## 4. 提交

commit message 遵循 `type(scope): 中文描述`，新 adapter 属于 `feat(beam-worker): ...`，会触发 minor 版本号（release-plz 管理）。
