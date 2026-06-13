# beam：TS 版功能对标计划

- 日期：2026-06-03（初稿），2026-06-08（代码审查更新）
- 状态：计划草案 + TODO 跟踪
- 关联文档：`docs/design/beam.md`
- 最近审计：2026-06-08 — 对照当前 TS/Rust 实现与测试重新验证

> 审查结论：**Rust 版尚未真正对标 TS 版**。核心 tmux + Lark + Claude/Codex
> 主链路已具备，`codex-app/cursor/mtr` 也已从静默 generic fallback 改为显式 adapter，
> 但全量对标仍不成立。以下状态只认实际调用链和行为测试，不以模块、路由或子命令
> 名称是否存在作为完成依据。

## 1. 目标

Rust 版最终需要完整对标当前 TS 版 beam，而不是只复刻核心消息链路。

对标目标分两层：

1. Rust 版先成为可替换生产核心 runtime，覆盖 `Feishu/Lark -> daemon -> worker -> tmux -> AI CLI -> reply/card` 主链路。
2. Rust 版再逐步覆盖 TS 版周边产品能力，包括 dashboard、workflow、scheduler、setup、权限、多人协作、connector、federation 和测试矩阵。

判断是否对标完成，应以行为和测试为准，而不是以代码模块数量为准。

## 2. 逐模块审计结论（2026-06-08）

每个条目基于实际代码行验证，而非函数存活性判断。

### 2.1 核心 Runtime / Daemon / Worker — ⚠️ tmux 主链路基本完整，Zellij 已补 discover / adopt / observe

| 能力 | 状态 | 证据 |
|------|------|------|
| Lark webhook 事件接收（签名验证、challenge、dedup） | ✅ | `handle_lark_event` 完整实现 |
| 消息解析（text/chat_id/sender/mentions/thread/scope） | ✅ | `parse_lark_inbound_message` 全字段提取 |
| Session 创建/spawn worker/持久化 | ✅ | `create_session_internal` + `spawn_worker` |
| Session 恢复（daemon 重启后 tmux reattach） | ✅ | `reconcile_restored_sessions_with` |
| Session 关闭（`/close` 销毁 tmux session） | ✅ | `close_session` 完整实现 |
| Session resume | ✅ | `validate_resume_target` + spawn |
| Worker IPC（全部 DaemonToWorker/WorkerToDaemon 消息处理） | ✅ | daemon 端 10+ 种消息，worker 端 11 种 |
| Lark token 管理（tenant_access_token 缓存+过期） | ✅ | `lark_tenant_token` 带 30s buffer |
| Lark 消息发送（text/card/patch/delete/ephemeral） | ✅ | 5 个发送函数，真实 HTTP 调用 |
| Lark card lifecycle（streaming/frozen/closed/pending/stale） | ✅ | 全部卡片类型均有真实 JSON 构建 |
| Lark card action handler（13 种 action 全部实现） | ✅ | close/restart/toggle/refresh/term/export/retry/resume/write_link/wf_approve/wf_reject/wf_cancel/tui_keys/tui_input |
| `beam send` PID marker 发现 + 进程树回溯 | ✅ | `discover_session_id` → `/proc/{pid}/stat` 遍历 |
| CLI daemon 管理（start/stop/restart/status/logs） | ✅ | fork + setsid 后台进程 |
| CLI setup（创建 config/bots/目录） | ✅ | `cmd_setup` |
| CLI migrate（TS→Rust bots.json 迁移） | ✅ | `cmd_migrate` |

**缺口**：
- Zellij 已补 discover / adopt / observe 路由与恢复路径，但实现仍偏 best-effort：
  - adopt 候选通过 `list-panes` / `dump-layout` / `ps` 做探测式合并。
  - observe backend 通过 pane 级 `dump-screen` / action relay 工作，仍需 live dogfood 继续打磨边界。
- DataDir migrate 已覆盖 `bots.json` 和 legacy `sessions.json`，并补了 dry-run、backup 与冲突报告。
- Setup 向导已补交互式机器人配置与凭证校验，后续主要是继续收敛更多 operator experience。

### 2.2 CLI Adapter — ⚠️ 两极分化

| Adapter | buildArgs | writeInput | submit verification | poll (turn 分类+final output) | 整体 |
|---------|-----------|-----------|---------------------|-------------------------------|------|
| Claude | ✅ | ✅ send_text+Enter+5 次重试 | ✅ jsonl submit markers 验证 | ✅ drain jsonl→user/assistant 分类→LocalTurn/Headless/Remote | ✅ 完整 |
| Codex | ✅ | ✅ pasteText+Enter+5 次重试 | ✅ history.jsonl 文本匹配 | ✅ rollout jsonl→final_answer 检测→turn 分类 | ✅ 完整 |
| CoCo | ✅ | ✅ pasteText+Enter+重试 | ✅ history.jsonl prefix 匹配 | ✅ 基础 assistant final output | ✅ turn 分类、cursor 恢复、去重 fixture 已补 |
| Antigravity | ✅ | ✅ sendText+M-Enter+Enter+重试 | ✅ history.jsonl marker 匹配 | ✅ 基础 model final output | ✅ turn 分类、cursor 恢复、去重 fixture 已补 |
| OpenCode | ✅ | ✅ sendText+Enter | ✅ transcript submit confirmation | ✅ transcript poll + final output dedupe | ✅ 已接入 |
| Gemini | ✅ | ✅ sendText+Enter | ✅ transcript submit confirmation | ✅ transcript poll + final output dedupe | ✅ 已接入 |
| Hermes | ✅ | ✅ sendText+Enter | ✅ transcript submit confirmation | ✅ state.db poll + final output dedupe | ✅ 已接入 |
| Aiden | ✅ | ✅ sendText+Enter | ❌ 同上 | ❌ `PollResult::default()` 空桩 | ⚠️ 旧待办，已从 backlog 移除 |
| Mira | ✅ | ✅ base64 `::beam-mira:` 编码 | ❌ 同上 | ❌ `PollResult::default()` 空桩 | ⚠️ 旧待办，已从 backlog 移除 |
| Seed | ✅ | ✅ 复用 Claude 数据根和参数语义 | ✅ | ✅ | 仍需单独的 TS fixture 证明无回归 |
| Codex App | ⚠️ | ✅ runner + app-server 参数 | ✅ | ❌ 仍无 transcript poll | 仍缺最终输出/恢复等价测试 |
| Cursor | ⚠️ | ✅ force/model/resume 语义 | ✅ | ❌ 仍无 transcript poll | 仍缺最终输出/恢复等价测试 |
| MTR | ⚠️ | ✅ native session id 映射 + resume command | ✅ | ✅ transcript poll 已补齐 | 仍缺 TS fixture 证明无回归 |

**详细缺口说明**：

1. **CoCo/Antigravity**：已补 turn 分类、cursor 恢复和重复抑制 fixture；仍需后续 live dogfood 继续观察真实 CLI 行为边界。
2. **Codex App/Cursor 仍缺 transcript parity fixture，MTR 已补 transcript poll**：Rust 不再静默落入 Generic；MTR 现在能读 SQLite session store 并产出 bridge events，但仍需要 TS fixture 证明回归边界。
3. **Seed 已复用 Claude special-case**：仍需要单独 fixture 证明 `.claude-runtime`、`CLAUDE_CONFIG_DIR` 和 resume 语义没有回归。

### 2.3 权限/授权 — ✅ grant 已接线，quota、peer/observed bot 读取与 multi-bot 路由门禁已补

| 能力 | 状态 | 详情 |
|------|------|------|
| BotConfig 权限字段（allowedChatGroups/chatGrants/globalGrants/oncallChats/messageQuota/quotaState/restrictGrantCommands） | ✅ | config.rs 已扩展 |
| evaluate_talk / can_operate / is_owner 核心逻辑 | ✅ | permissions.rs 含 11 个单测 |
| evaluate_lark_preflight（talk+operate+grant 拦截） | ✅ | 已在 `handle_lark_event` 调用 |
| grant 命令文本解析（/grant @user /revoke @user） | ✅ | grant.rs `parse_grant_command` 含 3 单测 |
| grant store 操作（add_chat_grant/add_global_grant/revoke_grant/consume_quota） | ✅ | grant.rs 完整实现 |
| grant 命令接入 event dispatcher | ✅ | `try_handle_grant_command()` 在普通 session 路由前拦截 |
| grant card 交互 | ✅ | 已有 grant_chat/grant_global/grant_deny、nonce、pending/denied 状态与边界测试 |
| **observe-bots 注册** | ✅ | Rust 已能读取 `bots-info.json` / `bot-openids-<app>.json` / `observed-bots-<app>-<chat>.json` 作为 peer bot 来源，并在 `/introduce` 与 grant 成功后写入 observed store |
| **multi-bot session** | ✅ | Rust 已补 foreign-bot @mention gate、self-bot /close 特判、单人/单 bot group 无 @ 放行与会话选择门禁 |
| **send policy + quota dedup** | ✅ | inbound 消息已接入 `consume_quota`，并补了 multi-bot 路由门禁与去重入口 |

### 2.4 Dashboard API — ⚠️ 核心端点已有，完整面未齐

| 端点 | 状态 |
|------|------|
| GET /health | ✅ |
| POST /shutdown | ✅ |
| GET /sessions, POST /sessions | ✅ |
| GET /sessions/{id} | ✅ |
| POST /sessions/{id}/input, /close, /restart, /resume, /final-output, /refresh | ✅ |
| POST /adopt/tmux, GET /adopt/tmux | ✅ |
| GET /api/bots, GET /api/bots/{id} | ✅ |
| GET /api/overview | ✅ |
| GET /api/sessions/groups | ✅ |
| GET /api/sessions/{id}/locate | ✅ |
| GET /api/workflows/definitions, /{id}, /{id}/run | ✅ |
| GET /api/workflows/runs, /{id}/snapshot, /events | ✅ |
| POST /api/workflows/runs/{id}/approve, /reject, /cancel, /resume | ✅ |
| Dashboard 静态资源 serve | ⚠️ 通过相对路径 `src/dashboard/web` 暴露 TS 资源；从非 repo cwd 启动仍可能受 cwd 影响，且不是独立 Rust dashboard 构建产物 |
| **GET /api/preferences** | ✅ |
| **GET /api/connectors** | ✅ connector store + CRUD |
| **GET /api/auth** | ✅ 一次性登录 token + dashboard cookie |
| **Terminal proxy WebSocket relay** | ✅ |

### 2.5 Workflow — ⚠️ 状态机主体存在，执行语义未对齐

| 能力 | 状态 |
|------|------|
| definition parser + validation | ✅ |
| event log (append/replay/idempotency) | ✅ |
| snapshot projection (RunSnapshotDTO + replay_events) | ✅ |
| binding ($ref resolution) | ✅ |
| orchestrator (decide_next_actions + topological_order) | ✅ |
| runtime loop (run_tick/run_loop) | ✅ |
| subagent execution（spawn worker + await output + parse） | ✅ |
| host executor（feishu-send/feishu-reply/beam-schedule） | ✅ |
| cold-scan / cold-attach（daemon 启动恢复） | ✅ |
| attempt resume | ✅ |
| approve/reject（human gate） | ✅ |
| schedule store（CRUD + output log） | ✅ |
| workflow resume CLI（--wait/--verbose） | ✅ |
| workflow progress card | ✅ 已接入 run 前、事件变化和终态 PATCH |
| **runLoop 并发** | ⚠️ `max_concurrency` 已按 bot 进行槽位调度；仍缺更完整的 TS 派发/恢复语义 |
| **runLoop cancel awareness** | ⚠️ 每 tick 处理 cancel event，并在运行中尽快中止未完成动作；仍缺更完整的 AbortController 级恢复语义 |
| **Dashboard binding resolution** | ⚠️ 部分实现 |
| webhook trigger | ⚠️ 已有 secret 校验、connector store + workflow 启动路由和持久 trigger log，但 new-group lifecycle 仍以 best-effort 处理 |
| **lifecycle store** | ✅ |

### 2.6 CLI / Setup / 运维 — ⚠️ 核心命令可用，周边待补齐

| 命令/能力 | 状态 |
|-----------|------|
| start / stop / restart / status / logs | ✅ |
| send（PID marker 发现） | ✅ |
| bots list | ✅ |
| session create/list/input/close/restart/resume/adopt/info | ✅ |
| workflow run/resume/cancel/validate/ls/tail/show | ✅ |
| setup（config + 目录创建） | ✅ 已补 Device Flow 扫码创建应用、手动凭证 fallback、凭证校验、hook 安装与目录初始化 |
| migrate（TS→Rust bots/session） | ✅ 已补 dry-run / backup / 冲突报告与 legacy `sessions.json` 转换 |
| **交互式 setup 向导**（飞书扫码创建 / 手动凭证） | ✅ |
| session store 迁移（TS→Rust sessions） | ✅ 已有 legacy `sessions.json` 转换并可备份冲突目标 |
| dashboard | ⚠️ 已改为动态 api_addr 打开，补了 preferences/connectors/auth 登录 cookie、webhook 列表和 session terminal proxy，但 dashboard 完整 token/写权限与 terminal relay 体验仍可继续收尾 |
| schedule | ⚠️ 已补 list/add/remove/pause/resume/run/logs 的本地管理入口，但仍没有 TS 的自然语言解析和 daemon 级调度语义 |
| report | ⚠️ 已接入 daemon `/sessions/{id}/report`，但仍是简化版编排回报，不是 TS 的完整 ask/report 流程 |
| ask | ⚠️ Rust 已有 `/api/asks` 与长轮询闭环，但卡片/审批/多问多选还只覆盖基础路径 |
| hook | ⚠️ Rust 已有 `beam hook <cliId>` 的 payload 解析和 `/api/asks` 回填，但还没补齐所有 CLI 形状 |
| lang | ✅ 已持久化 `~/.beam/config.json` 的全局 locale |
| voice | ✅ 已支持 `status` / `disable` / 交互式 `setup` 并持久化 `~/.beam/config.json` 的 voice block |
| dispatch | ❌ 命令不存在 |
| **open platform automation** | ❌ |
| autostart | ✅ 已支持 Linux user systemd + macOS launchd 的 enable / disable / status / refresh |

### 2.7 测试 — ⚠️ 单测覆盖好，集成/e2e 不足

| 层级 | 测试数 | 状态 |
|------|--------|------|
| beam-core | 40（含 integration） | ✅ |
| beam-daemon | 118 | ✅ session/lark/card/workflow 单元测试 |
| beam-worker | 24 | ✅ adapter(15) + 各种 worker 逻辑测试 |
| beam-cli | 8 | ✅ workflow CLI 测试 |
| **parity gate**（TS fixture 复用） | 0 | ❌ 未开始 |
| **e2e gate**（Feishu browser 等价测试） | 0 | ❌ 未开始 |

2026-06-07 验证：`cargo test --workspace` 为 **211 passed, 0 failed**。该结果只证明
Rust 自身测试通过，不证明与 TS 等价；目前没有任何 cross-runtime parity test。

---

## 3. 合并 TODO（按优先级）

### P0：剩余功能任务总表

详细任务清单已单独拆到 `docs/design/beam-parity-backlog.md`，该文件是后续
deepseek_coder 的执行输入。当前这里只保留优先级概览：

- P0 核心缺口：secondary adapters 剩余 adapter 轮询与验证、parity gate
- P1 产品缺口：workflow 并发/取消、connector/lifecycle
- P2 运维缺口：setup 向导、migrate dry-run/backup、autostart 已收口
- P3 测试缺口：Rust parity gate、双栈 E2E gate

---

## 4. Phase 设计（原始规划，供参考）

### Phase 0：建立对标基线 ✅
- `test/parity-manifest.json` 已建立，TS 测试已做第一轮分类

### Phase 1：核心链路 ✅
- Lark 消息接收 → session routing → worker spawn → tmux → CLI → reply/card 全链路
- streaming card、frozen/stale card、closed-session card
- `beam send`、web terminal
- daemon 重启恢复 tmux session

### Phase 2：CLI Adapter
- Claude Code ✅ | Codex ✅ | CoCo ⚠️ | Antigravity ⚠️
- OpenCode ✅ | Gemini ✅ | Hermes ✅
- Seed ⚠️ | Codex App ❌ | Cursor ❌ | MTR ❌
- 见上方详细 gap 清单

### Phase 3：权限/授权
- BotConfig 字段 ✅ | evaluate_talk/can_operate ✅ | preflight 拦截 ✅
- grant 命令解析 ✅ | grant store ✅
- grant 命令接入 daemon ✅ | grant card 交互 ✅ | observe-bots ✅ | multi-bot ✅ | quota 接入 ✅

### Phase 4：Dashboard
- 核心 session/workflow API ✅ | bots/groups/locate/overview ✅
- 静态资源相对路径 serve ⚠️ | preferences/connectors/auth ✅ | terminal proxy ✅

### Phase 5：Workflow
- 核心引擎 ✅ | cold-scan/attach ✅ | 4 个 host executor ✅
- progress card ✅ | runLoop 并发 ⚠️（已按 bot 做槽位调度并改善取消响应） | webhook 基础入口 ⚠️ | connector/lifecycle store ❌

### Phase 6：CLI/Setup
- 核心 daemon 管理 ✅ | session 管理 ✅ | send ✅ | setup ✅ | migrate ✅
- 交互式向导 ✅ | 周边命令多为占位 ❌ | autostart ✅

### Phase 7：测试
- Core gate：211 tests ✅
- Parity gate ❌ | E2E gate ❌

---

## 5. 风险与约束

- 不应把 dashboard、workflow、federation 提前塞进 Rust MVP，否则核心链路会长期不可替换。
- 不应只迁移代码路径而忽略 TS 测试里沉淀的边界行为。
- Lark card/action 权限语义复杂，应先做 store 和纯函数测试，再接真实事件。
- workflow 状态机边界多，建议保持一段 TS/Rust 双栈兼容期。
- dataDir 迁移已具 dry-run + backup + 冲突报告，仍需保持与 TS 的兼容边界一致。

## 6. 完成定义

Rust 版完全对标 TS 版，至少满足：

- 主链路真实 Lark dogfood 稳定运行。
- Claude Code 和 Codex 达到 TS 版行为等价。
- 主要 adapter 输入和 final output 行为等价。
- 权限、grant、multi-bot、card lifecycle 行为等价。
- dashboard 主要页面以 Rust daemon 为后端可用。
- workflow/scheduler/connector 主要测试等价通过。
- CLI/setup 运维体验不低于 TS 版。
- TS 版默认入口可以安全切换到 Rust，并保留明确 fallback。
