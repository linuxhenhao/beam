# beam：Rust 版核心后端设计

- 日期：2026-06-02
- 状态：持续维护中的设计总览（2026-06-13 已按当前 Rust 实现校对）

## 0. 阅读方式

本文是 Rust `beam` 的核心运行时总览，重点解释 daemon / worker / session / backend / Lark bridge 的主链路。

渐进式阅读建议：

- 先读本文，建立运行时主模型。
- 需要更细的实体、IPC、卡片生命周期和时序图时，再读 `docs/design/beam-architecture.md`。
- 需要看“当前已经做到哪里、哪些仍未完全对标 TS”时，读 `docs/design/beam-parity-plan.md`。
- 需要看 team/platform/federation 设计时，分别读 `docs/platform-design.md`、`docs/federation-design.md`。

## 1. 背景

当前 beam 已经形成一条可用的生产路径：

```
Feishu/Lark message
  -> daemon session router
  -> per-session worker
  -> CLI adapter
  -> tmux pipe backend
  -> AI CLI
```

核心价值不在于“又做一个聊天机器人”，而是把 IM 线程、终端 TUI、CLI transcript、tmux 持久会话和 web terminal 接到同一条会话生命周期里。Rust 版不应从 UI 或 workflow 开始，而应先复刻这条核心链路。

`beam` 的目标是成为一个更小、更稳定、更低资源的核心 runtime。它先承载飞书到本地 AI CLI 的主链路，再逐步迁移周边能力。

## 2. 目标

核心目标：

- 支持飞书消息驱动本地 AI CLI。
- 支持 tmux 持久 session，worker/daemon 重启后可重新接回。
- 支持 `beam send` 从 CLI 内回发飞书。
- 支持 `/close`、`/restart`、`/adopt`、`/card` 的核心语义。
- 支持 read/write web terminal。
- 支持 Claude Code、Codex，以及当前仓库已接入的其他 CLI adapter。
- 支持 `tmux`、`pty`、`zellij` 三种 backend，其中 `tmux` 仍是默认生产后端。
- 支持 workflow、schedule、grant、connector/webhook、dashboard API 等已落地能力，但这些能力的详细设计不在本文展开。

Rust 版应尽量保持与现有 beam 的会话语义一致：

- 一个飞书会话对应一个 beam session。
- CLI 运行在本机。
- session 持久化落盘。
- CLI 子进程可以调用本地 `beam send`。
- 显式 `/close` 才销毁底层 tmux session；daemon/worker 退出只 detach。

## 3. 非目标

本文不展开或不作为默认主路径的内容：

- team/platform/federation 的完整产品设计。
- 与 TS 全量对标的审计细节和剩余 gap。
- 所有 dashboard 前端交互细节。
- voice、完整 i18n、组织级开放平台自动化。
- 对每个 CLI adapter 的行为边界做逐项说明。

这些主题在仓库中部分已经实现或另有专门文档；这里仍以 runtime 主链路为中心。

## 4. 设计原则

1. **tmux-first**：当前仓库最稳定的生产路径是 `TmuxPipeBackend`。Rust 版第一阶段继续以 tmux 为默认后端。
2. **保留进程隔离**：沿用 daemon + worker，而不是单进程全 async。CLI/TUI/PTY 崩溃和挂死必须隔离。
3. **协议优先于实现细节**：先定义 daemon-worker IPC、CLI context marker、session store 形状，再填实现。
4. **少复制周边能力**：先让消息、输入、输出、恢复、send、adopt 跑通。
5. **与 TS 版可并存**：Rust 版初期不要抢占所有 dataDir；提供独立 data root 或明确迁移工具。

## 5. 总体架构

```
beam
├─ daemon
│  ├─ Lark event receiver
│  ├─ session router
│  ├─ command handler
│  ├─ worker supervisor
│  ├─ local IPC API
│  └─ session/card state persistence
│
├─ worker
│  ├─ CLI adapter
│  ├─ session backend
│  ├─ terminal web server
│  ├─ screen sampler
│  ├─ idle detector
│  └─ transcript bridge
│
├─ cli
│  ├─ start / stop / restart / logs
│  ├─ send
│  ├─ bots list
│  ├─ thread messages
│  └─ setup
│
└─ shared
   ├─ config
   ├─ bot registry
   ├─ session store
   ├─ Lark client
   ├─ terminal protocol
   └─ adapters
```

## 6. 进程模型

### 6.1 daemon

daemon 是长期运行的控制进程，职责：

- 接收飞书事件。
- 解析消息和 slash commands。
- 查找或创建 session。
- fork worker。
- 向 worker 发送用户消息、raw input、restart、close 等 IPC。
- 接收 worker 的 `ready`、`screen_update`、`final_output`、`cli_exit`。
- 维护飞书 streaming card / static reply。
- 提供本地 IPC，供 `beam send` 调用。

### 6.2 worker

worker 是每个 session 一个进程，职责：

- 启动并持有 CLI backend。
- 维护 CLI 输入队列。
- 读取终端 ANSI 输出。
- 定时生成 screen snapshot。
- 运行 idle detector。
- 监听 CLI transcript fallback。
- 维护 CLI PID marker。

### 6.3 为什么不做单进程多 task

Rust 单进程多 task 更省资源，但不适合作为第一版：

- AI CLI 和 terminal backend 的挂死/崩溃会污染整个 daemon。
- 不同 session 的日志、生命周期、restart 边界更难清晰隔离。
- 当前 TS 版已经验证 per-session worker 可恢复、易排查。
- 后续可以在 worker 内部优化 async，但不应先取消 worker 进程边界。

## 7. Backend 设计

### 7.1 trait

```rust
#[async_trait]
pub trait SessionBackend: Send {
    async fn spawn(&mut self, bin: &str, args: &[String], opts: SpawnOpts) -> anyhow::Result<()>;
    async fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()>;
    async fn send_text(&mut self, text: &str) -> anyhow::Result<()>;
    async fn send_keys(&mut self, keys: &[TermKey]) -> anyhow::Result<()>;
    async fn paste_text(&mut self, text: &str) -> anyhow::Result<()>;
    async fn resize(&mut self, cols: u16, rows: u16) -> anyhow::Result<()>;
    async fn kill(&mut self) -> anyhow::Result<()>;
    async fn destroy_session(&mut self) -> anyhow::Result<()>;
    fn child_pid(&self) -> Option<u32>;
}
```

可观测后端额外提供：

```rust
#[async_trait]
pub trait ObserveBackend: SessionBackend {
    async fn capture_current_screen(&self) -> anyhow::Result<String>;
    async fn capture_viewport(&self) -> anyhow::Result<String>;
    async fn pane_size(&self) -> anyhow::Result<Option<TermSize>>;
    async fn is_pane_alive(&self) -> anyhow::Result<bool>;
}
```

### 7.2 tmux-pipe backend

Rust 版的第一生产后端应复刻当前 `TmuxPipeBackend`：

新 session：

```
tmux new-session -d -s bmx-<sid8> -x <cols> -y <rows> -- <shell> ... <cli-bin> <args>
```

观察输出：

```
mkfifo /tmp/beam-pipe-xxxx
tmux pipe-pane -O -t bmx-<sid8> 'cat > <fifo>'
```

输入：

```
tmux send-keys -t <pane> -l -- <text>
tmux send-keys -t <pane> Enter
tmux load-buffer -
tmux paste-buffer -t <pane> -d -p
```

快照：

```
tmux capture-pane -e -p -t <pane>
tmux capture-pane -e -p -t <pane> -S -
```

生命周期：

- `kill()`：取消 `pipe-pane`，关闭 FIFO，不 kill tmux session。
- `destroy_session()`：先 `kill()`，再 `tmux kill-session`。
- `resize()`：owned session 用 `tmux resize-window`。
- liveness：定时 `tmux display-message -p -t <pane> '#{pane_id}'`。

### 7.3 pty fallback

pty fallback 用于 tmux 不可用时：

- Rust crate 候选：`portable-pty` 或 `tokio-pty-process`。
- 不支持 daemon 重启后 CLI 保活。
- web terminal 通过 worker relay。
- `/adopt` 不支持。

### 7.4 zellij

当前仓库已经支持 `zellij` backend，并补上了 managed / adopt / observe 的主路径：

- managed：worker 可直接启动或重连 zellij session。
- adopt：daemon 可发现 zellij pane 并创建 adopted session。
- observe：通过 pane 级 screen dump 和 action relay 保持只读观测与定向输入。

`tmux` 仍是默认后端；`zellij` 适合明确需要它的场景。更细的实现细节见 `docs/zellij-backend-poc.md`。

## 8. CLI Adapter 设计

### 8.1 trait

```rust
#[async_trait]
pub trait CliAdapter: Send + Sync {
    fn id(&self) -> &'static str;
    fn display_name(&self) -> &'static str;
    fn resolved_bin(&self) -> PathBuf;
    fn build_args(&self, opts: BuildArgs) -> Vec<String>;
    async fn write_input(
        &self,
        backend: &mut dyn SessionBackend,
        content: &str,
    ) -> anyhow::Result<SubmitResult>;
    fn ready_pattern(&self) -> Option<Regex>;
    fn completion_pattern(&self) -> Option<Regex>;
    fn supports_typeahead(&self) -> bool;
    fn transcript_bridge(&self) -> Option<TranscriptBridgeKind>;
}
```

### 8.2 当前 adapters

当前仓库已接入的 adapter 至少包括：

- `claude`
- `codex`
- `coco`
- `gemini`
- `opencode`
- `hermes`
- `antigravity`
- `generic`

其余 adapter 的真实对标状态与测试覆盖，统一以 `docs/design/beam-parity-plan.md` 为准。

### 8.3 输入提交

不要把所有 CLI 简化成 `paste + Enter`。当前 TS 版已经证明不同 CLI 对 bracketed paste、Enter retry、history confirmation 的行为不同。

Rust 版 adapter 需要保留：

- 大块输入优先 `paste_text()`。
- slash command 使用 `send_text()` + 延迟 + `Enter`，避免被当成 pasted prompt。
- Claude/Codex 优先通过 transcript/history 验证 submit 是否落盘。
- adapter 可返回 `SubmitResult { submitted: false, recheck }`，worker 延迟复查后再提醒用户。

## 9. Daemon-worker IPC

建议第一版使用 length-delimited JSON over stdio 或 Unix domain socket。为了简单，fork worker 后父子进程之间用 stdin/stdout framed JSON；worker 日志走 stderr。

Daemon -> Worker：

```rust
enum DaemonToWorker {
    Init(InitConfig),
    Message { content: String },
    RawInput { content: String },
    Close,
    Restart,
    SetDisplayMode { mode: DisplayMode },
    TermAction { key: TermActionKey },
    RefreshScreen,
}
```

Worker -> Daemon：

```rust
enum WorkerToDaemon {
    Ready { port: u16, token: String },
    PromptReady,
    ScreenUpdate { content: String, status: ScreenStatus },
    CliSessionId { cli_session_id: String },
    CliExit { code: Option<i32>, signal: Option<String> },
    FinalOutput { content: String, turn_id: String },
    UserNotify { message: String },
    Error { message: String },
}
```

IPC 类型应版本化：

```rust
struct Envelope<T> {
    version: u32,
    session_id: String,
    payload: T,
}
```

## 10. 飞书/Lark Bridge

第一阶段实现最小飞书能力：

- 事件接收：优先 HTTP callback；event websocket 后补。
- 发送普通文本消息。
- 发送/更新 interactive card。
- 获取 thread messages。
- 下载用户上传附件可以延后。

daemon 侧内建命令：

- `/close`
- `/restart`
- `/adopt`
- `/card`
- `/workflow`

其他未知 `/slash` 命令不在 daemon 侧消费，而是按原文透传给底层 CLI。

`beam send` 通过本地 HTTP API 调 daemon，关键回发入口是：

- `POST /sessions/{session_id}/final-output`

session_id 获取：

- worker 启 CLI 后写 marker：`{data_dir}/.beam-cli-pids/{pid}`。
- `beam send` 从当前进程向上遍历 ppid，找到 marker。
- fallback 读取 `BEAM_SESSION_ID` 环境变量。

## 11. Web Terminal

daemon 启动本地 `zellij web` 和统一 terminal proxy。proxy 是唯一对外入口，浏览器不直接访问 zellij web，也不直接持有 zellij cookie。

- daemon 启动 `zellij web` 于 `proxy_base_port + 1`，并创建/持久化 read-only / write zellij web token。
- daemon 对外暴露 terminal proxy 于 `proxy_base_port`。
- 飞书卡片和 dashboard 生成 `/s/{session_id}?beam_terminal_ticket=...` 链接，而不是暴露 zellij token。
- proxy 验证 Beam ticket 后调用 zellij web `/command/login`，捕获上游 `Set-Cookie`，保存在服务端 cookie jar。
- proxy 只给浏览器设置 `beam_terminal_session` cookie；后续 HTTP/WS 请求由 proxy 将 Beam cookie 映射为后台 zellij cookie 并注入上游。
- 上游 zellij `Set-Cookie` 响应头会被剥离，避免 zellij cookie 泄露给外部浏览器。
- session 页面和资源走 `/s/{session_id}` / `/s/{session_id}/{path}`，WebSocket 走 `/s/{session_id}/ws` 和 `/s/{session_id}/ws/{*rest}`。

read-only 和 write 的入口使用不同 zellij token 登录，但当前 zellij web 返回的 cookie 可能是全局 session cookie；proxy 记录权限用于审计和后续拦截，不能把它视为已经在 zellij web 协议层强制限制输入。

细节见 `docs/design/terminal-proxy.md`。

tmux-pipe 模式：

- 新连接优先 `capture_current_screen()` seed。
- 后续 live 输出来自 pipe-pane stream fan-out。
- 写入通过 backend `write()` 或 adapter path。

当前实现除 terminal web view 外，还支持：

- 流式卡片更新。
- screenshot 上传与刷新。
- usage-limit 状态展示。

## 12. Screen Update 与 Idle Detector

当前 screen update 主路径：

- tmux 后端每 1s `capture_viewport()`。
- 转为纯文本后 hash 去重。
- 根据 idle detector 状态发 `ScreenUpdate`。

idle detector：

- 维护最近 ANSI/text buffer。
- CLI adapter 提供 `ready_pattern`。
- ready pattern 命中且输出静默一段时间后标记 `PromptReady`。

当前仓库也已经有更完整的能力接线，包括：

- usage limit classifier / retry-ready 状态。
- screen analyzer 配置。
- screenshot upload。

## 13. Transcript Bridge

Rust 版保留 transcript fallback，当前仓库已覆盖多类来源：

- Claude JSONL。
- Codex history / rollout。
- OpenCode / Gemini transcript store。
- Hermes / MTR 等各自存储后端（具体完成度见 parity plan）。

能力：

- adapter submit confirmation。
- 如果 CLI 最终没调用 `beam send`，daemon 可从 transcript 发 final output。

第二阶段：

- CoCo events。
- MTR SQLite。
- Hermes store。

## 14. Store 设计

建议 data root：

```
~/.beam/
├─ config.toml
├─ bots.json
├─ sessions/
│  └─ <lark_app_id>.json
├─ workers/
├─ logs/
├─ .beam-cli-pids/
└─ state/
```

Session：

```rust
struct Session {
    session_id: String,
    chat_id: String,
    root_message_id: String,
    scope: SessionScope,
    title: String,
    status: SessionStatus,
    created_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
    working_dir: Option<PathBuf>,
    web_port: Option<u16>,
    lark_app_id: String,
    owner_open_id: Option<String>,
    cli_id: Option<String>,
    cli_session_id: Option<String>,
    last_cli_input: Option<String>,
    stream_card_id: Option<String>,
    display_mode: DisplayMode,
    adopted_from: Option<AdoptedFrom>,
}
```

Store 写入要求：

- 原子写：写 temp file 后 rename。
- 尽量保留未知字段，方便和 TS 版迁移。
- session update 要避免每秒 screen update 都落盘。

## 15. Adopt 设计

adopt 不再只限 tmux。当前仓库同时支持：

- tmux adopt
- zellij adopt

发现：

- `tmux list-panes -a` 拿 pane target、pid、cwd、command。
- 进程树识别 Claude/Codex CLI。
- 找到候选后发飞书 card 让用户选择。

zellij adopt 的 pane 发现、观测与驱动细节见 `docs/zellij-backend-poc.md`。

启动 adopt worker：

- 不创建 tmux session。
- `TmuxPipeBackend` target 使用真实 pane address。
- `owns_session=false`。
- `kill()` 只取消 pipe，不影响用户 pane。
- `destroy_session()` 对 adopt 也只 detach，不 kill 用户 tmux。

输入：

- 普通消息走 adapter `write_input()`。
- raw slash command 走 `send_text()` + `Enter`。

安全：

- adopt 前再次验证 pane 仍在，pid 仍匹配。
- 原 CLI pid 退出后关闭 beam session，避免把后续输入打进用户 shell。

## 16. 配置

`config.toml`：

```toml
[daemon]
backend_type = "tmux"
working_dirs = ["~/workspace"]
quiet_restart = false

[web]
host = "0.0.0.0"
proxy_base_port = 8800

[lark]
event_mode = "http"
```

`bots.json`：

```json
[
  {
    "larkAppId": "cli_xxx",
    "larkAppSecret": "...",
    "cliId": "claude-code",
    "model": null,
    "backendType": "tmux",
    "allowedUsers": []
  }
]
```

## 17. Rust crate 建议

- async runtime：`tokio`
- HTTP/WebSocket：`axum`
- HTTP client：`reqwest`
- serialization：`serde`, `serde_json`, `toml`
- CLI：`clap`
- logging：`tracing`, `tracing-subscriber`
- error：`anyhow`, `thiserror`
- regex：`regex`
- process：`tokio::process`
- pty：`portable-pty`（先 spike）
- file lock：`fs2` 或 `fd-lock`
- time：`chrono` 或 `time`

## 18. MVP 里程碑

### M0：仓库骨架

- `crates/beam-cli`
- `crates/beam-daemon`
- `crates/beam-worker`
- `crates/beam-core`

出口：`beam --help`、配置读取、日志可用。

### M1：tmux backend spike

- 创建 tmux session。
- pipe-pane 读取输出。
- send-keys/paste-buffer 写输入。
- capture-pane snapshot。
- kill/destroy 语义正确。

出口：本地 terminal harness 通过。

### M2：worker

- worker init。
- spawn Claude/Codex。
- web terminal。
- pending message queue。
- idle detection。

出口：本地无飞书情况下可通过 HTTP/WS 控制 CLI。

### M3：daemon + Lark

- 接收飞书消息。
- 创建 session。
- fork worker。
- 发送 streaming card 或文本状态。
- `/close`、`/restart`。

出口：飞书里能驱动 Claude/Codex。

### M4：`beam send`

- 本地 IPC。
- PID marker。
- CLI 内回发飞书。
- final fallback 抑制。

出口：agent 可用 `beam send` 给用户正式回复。

### M5：tmux adopt

- 发现 tmux pane。
- 飞书选择卡片。
- adopt worker。
- pane liveness。

出口：用户手动 tmux 里的 Claude/Codex 可被飞书接管。

## 19. 迁移策略

不要一次性替换 TS 版。

推荐路径：

1. `beam` 使用独立 dataDir，先 dogfood 单 bot。
2. 与 TS 版共存，选择一个测试飞书 bot 指向 Rust daemon。
3. 跑通 Claude/Codex + tmux + send + close/restart。
4. 再加入 `/adopt`。
5. 稳定后写 migration tool，把 TS sessions/bots 迁移到 Rust store。
6. 最后评估是否迁移 scheduler/dashboard/workflow。

## 20. 风险

### 20.1 Rust PTY 生态不如 Node

tmux-pipe 路径主要依赖外部 tmux 命令和 FIFO，PTY 只用于 fallback。MVP 应避免把关键路径压在 Rust PTY crate 上。

### 20.2 飞书卡片复杂度

当前 TS 版卡片能力很多。Rust MVP 只做最小 streaming card，避免第一阶段复制所有按钮和状态。

### 20.3 Transcript bridge 漂移

Claude/Codex transcript 格式会变化。Rust 版应把 bridge 做成 adapter 内部模块，测试用真实 fixture。

### 20.4 与 TS 版 dataDir 互相污染

初期必须使用独立 data root，避免两个 daemon 同时修改同一 session store。

### 20.5 tmux 命令超时/失败

所有 tmux command 必须：

- 清理 `TMUX` / `TMUX_PANE` env。
- 设置 timeout。
- 捕获 stderr。
- classify pane gone vs transient failure。

## 21. 开放问题

1. Rust 版是否使用同一个二进制多子命令，还是 daemon/worker 分离二进制？
2. daemon-worker IPC 用 stdio framed JSON，还是 localhost HTTP/UDS？
3. MVP streaming card 是发 interactive card，还是先发纯文本状态？
4. `beam send` 是否保持和 TS 版 `beam send` 参数完全兼容？
5. Rust 版是否需要读取 TS 版 `bots.json`，还是定义新 schema？
6. web terminal 是否沿用当前 HTML/xterm.js 静态页面？
7. transcript fallback 在 MVP 是否必须先覆盖 Codex steer/type-ahead 归因？

## 22. 初始建议

第一轮实现应只接受以下验收：

- tmux session 可创建、可重连、可销毁。
- 飞书一条消息能进入 Claude/Codex。
- CLI 输出能在飞书 card 或文本中更新。
- CLI 内 `beam send` 能回发。
- daemon restart 后 tmux session 仍可接回。
- `/close` 后 tmux session 确实被删除。

这组能力跑稳后，`beam` 才值得继续扩展成完整 beam 替代品。
