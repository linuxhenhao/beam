# 新增 CLI Adapter 指南

> 以 `kimi`（Kimi Code CLI，2026-07 接入）为例，总结新增一个 AI coding CLI adapter 的完整流程。
> 英文镜像：[add-cli-adapter.en.md](add-cli-adapter.en.md)。

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
- model：`init.model` → kimi `-m <model>`、gemini `--model <model>`。
- resume：`init.resume` 时用 `init.cli_session_id`（fallback `resume_session_id` / `session_id`），kimi 对应 `--session <id>`。
- 初始 prompt：只有支持「交互模式 + 命令行传入初始 prompt」的 CLI 才能加入 `passes_initial_prompt_via_args`（gemini `-i`、opencode）。kimi 的 `-p` 是一次性非交互模式，**不能**算——它的初始 prompt 由 worker 经 `write_input` 在 TUI 里输入。

## 2. 改动清单

### 2.1 `crates/beam-worker/src/adapter.rs`

- `AdapterKind` 枚举加变体。
- 新增 `XxxState` 结构体。常规字段：`data_dir`、`working_dir`、`transcript_path`、`transcript_offset`、`pending_tail`、`emitted_final_text`、`cli_session_id`。

### 2.2 `crates/beam-worker/src/adapters/<name>.rs`

实现四件套：`create_state` / `build_spawn_spec` / `write_input` / `poll`。

- 模板选择：单文件 transcript 参考 `antigravity.rs`；DB / 复杂定位参考 `hermes.rs`、`opencode/`。
- 公共 helper 直接复用 `crate::adapter`：`drain_jsonl`、`file_size`、`normalize_history_text`、`realpath_cwd`。
- `poll` 约定：
  - `final_output`、`final_output_kind = FinalOutputKind::Bridge`、`prompt_ready = true` 三者一起设置。
  - 用 `emitted_final_text` 对同文去重；检测到新用户 turn 开始时重置去重状态。
  - 文件截断（`size < transcript_offset`）时重置 `transcript_offset` / `pending_tail` / `emitted_final_text`。
- `write_input` 约定：`send_text` → 200ms → `send_enter`，随后 4×800ms 用 transcript 确认提交，未确认时补 Enter；最终失败返回 `failure_reason`，不要谎报 `submitted`。

### 2.3 `crates/beam-worker/src/adapters/mod.rs`

- `pub mod <name>;`
- 5 处分发各加一臂：`create_adapter` / `build_spawn_spec` / `write_input` / `poll` / `on_spawned`。

### 2.4 其他 crate 注册点

| 文件 | 何时需要 |
| --- | --- |
| `beam-cli/src/cli_commands/setup.rs` 的 `CLI_CHOICES` | 必须，否则 setup 向导看不到 |
| `beam-cli/src/cli_commands/setup.rs` 的 `default_cli_args_for_cli_id` | 仅当 bypass 等默认参数不适合写死在 adapter 里 |
| `beam-daemon/src/zellij_adopt.rs` 的 `cli_id_from_zellij_command` | 需要 zellij adopt 识别时 |
| `beam-daemon/src/lark_ingress/workflow_actions.rs` 的 resume 允许列表 | 仅当 adapter 实现了 `init.resume` |
| `beam-worker/src/worker_runtime/run_loop.rs` 的 `maybe_inject_term` | 仅当 CLI 要求特定 TERM（如 codex/traex） |

### 2.5 文档

- `README.md` / `README.en.md` 的 CLI 清单（mermaid 图与前置要求）。
- `docs/design/beam.md` 的 adapter 清单、`docs/design/beam-architecture.md` 的 bridge 类型图。
- 中英配对规则适用于 `docs/design/*.md`：改动一边必须同步另一边；若英文镜像本来就没有对应段落，则无需补。

## 3. 测试

### 3.1 单元测试（`adapters/<name>.rs` 的 `#[cfg(test)]`）

- 用临时 HOME + `crate::adapter::home_test_lock()` 串行化对 HOME 的依赖（参考 `antigravity.rs` 的 `HomeGuard`）。
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
- 必须清理：`zellij delete-session -f`、临时工作目录、CLI 为该目录生成的 session 数据（kimi 还需清掉 `session_index.jsonl` 里的对应行）。

### 3.3 全量验证

```bash
cargo test --workspace --no-fail-fast
scripts/check-rust-line-count.sh   # 单文件 1000 行硬限制
```

## 4. 提交

commit message 遵循 `type(scope): 中文描述`，新 adapter 属于 `feat(beam-worker): ...`，会触发 minor 版本号（release-plz 管理）。
