# Beam 日志分级与排障可观测性改造计划

## 1. 目标与范围

本计划为后续 coder 提供可独立交接、独立验收的实施顺序。目标是在不引入外部日志平台、不改变 daemon/worker IPC 协议的前提下，建立一致、低噪声且可按需展开的 Rust `tracing` 日志体系。

交付目标：

1. 日常运行的默认 `INFO` 日志只保留生命周期、状态变化、恢复动作和聚合后的健康结果；不出现每请求、每轮询、每张截图等热路径明细。
2. `WARN` 只表示服务仍能运行但需关注的异常或降级；预期竞态、过期结果、正常认证探测不能污染 warning。
3. `ERROR` 只在某项关键操作最终失败且当前边界无法继续时记录一次；避免在下层和上层重复记录同一错误。
4. 排障人员可在重启时使用 `RUST_LOG` 精确提升某个 crate 或模块到 `DEBUG` / `TRACE`，而不需要修改配置文件。
5. 日志绝不输出 token、cookie、ticket、app secret、Authorization 头、完整上游响应体、用户消息正文或终端原文。

不在本期范围：动态调整日志级别、接入 OpenTelemetry/远端日志平台、JSON 日志格式、修改 `beam logs` 的读取方式、为所有历史日志调用一次性重写。

## 2. 已确认的现状与约束

- `beam-cli`、`beam-daemon`、`beam-worker` 入口各自创建 `tracing_subscriber::fmt()`，默认级别为 `INFO`，通过 `EnvFilter::from_env_lossy()` 支持 `RUST_LOG`。入口分别在：
  - `crates/beam-cli/src/main.rs`
  - `crates/beam-daemon/src/main.rs`
  - `crates/beam-worker/src/main.rs`
- `beam start` / `beam restart` 后台 daemon 将 stdout、stderr 追加写到 `logs/daemon.log`。worker 的 stdout 是 line-delimited JSON IPC，**worker 的 tracing 必须继续写 stderr**，不得改为 stdout。
- 已存在 workspace 级 `tracing` 和 `tracing-subscriber` 依赖；实现不应再引入另一套日志门面。
- 当前高噪声区域主要是 terminal proxy、截图上传/轮询、worker 生命周期竞态；当前最紧急风险是 zellij token 创建失败时可能打印原始 stdout/stderr。
- 现有 `docs/plans/2026-07-15-event-driven-screenshot-refresh.md` 指出截图分段耗时日志可用于诊断；本计划保留这些字段，但将逐次成功日志调整至 `DEBUG`，不再默认输出。

## 3. 日志契约

### 3.1 级别判定表

| 级别 | 使用条件 | 示例 | 禁止事项 |
|---|---|---|---|
| `ERROR` | 当前操作在完成重试/回退后仍失败，调用边界无法兑现契约 | session 关键持久化失败；最终无法启动 worker；不可恢复的 workflow 执行失败 | 把预期 HTTP 4xx、可恢复状态或下层已记录的错误再记一次 |
| `WARN` | 服务继续运行，但发生降级、异常或需要人工关注的状态 | 上游持续不可用；重试耗尽；首次/状态变化的截图连续失败；IPC 协议违例 | 正常超时语义、过期 CAS 结果、认证探测、每轮重复失败 |
| `INFO` | 默认可见的业务/生命周期结果，频率低且有运维价值 | daemon/worker 启停；session/turn 创建或完成；配置回退；恢复动作；周期健康摘要 | 每个 HTTP 请求、认证成功、每个 screen capture、原始输入输出 |
| `DEBUG` | 排障所需的单次操作细节，允许较高频 | terminal proxy 请求/认证判定；截图 render/upload 分段耗时；CAS 丢弃理由；adapter 候选与重试 | 秘密、用户正文、终端原文、完整第三方响应 |
| `TRACE` | 极高频内部状态与协议决策，仅短时定向开启 | poll tick、去重命中、状态机分支、原始事件长度/摘要 | 一切秘密与可还原用户内容 |

### 3.2 字段与脱敏规则

每条新增或改造的结构化日志按适用性使用以下稳定字段：

`component`、`operation`、`outcome`、`session_id`、`turn_id`、`bot`、`worker_pid`、`trigger`、`status`、`elapsed_ms`、`retry_count`、`error`。

- `session_id`、`turn_id` 可用于关联同一流程；无关联价值的 chat/message/open ID 不默认记录。
- 错误使用 `error = %err`；日志消息描述结果，不拼接完整 response/body。
- 禁止记录：token、cookie、ticket、secret、密码、授权头、URL query 中的凭据、用户输入全文、CLI/terminal 画面全文、Lark 原始请求/响应。
- 只需诊断输入规模时记录 `content_len`、`candidate_count`、`png_bytes` 或不可逆摘要，不能记录内容本身。
- 对无限循环或高频回调的失败，采用“首次 + 状态变化为 `WARN`，其余 `DEBUG` 并限频”的策略；限频不得阻塞或改变业务重试、状态更新和恢复语义。

### 3.3 运行方式

默认保持 `INFO`。排障通过重启生效：

```bash
RUST_LOG='beam_daemon=debug,beam_worker=debug,beam_cli=info' target/debug/beam restart
```

只排查特定模块时应进一步缩小 directive，例如 `beam_daemon::terminal_proxy=trace`。`TRACE` 只能短时启用并在收集完成后恢复默认。实现必须保留现有标准 `RUST_LOG` directive 兼容性。

## 4. 实施任务（按顺序交接）

每个任务完成后必须经 reviewer 验收才可开始下一项。任务不得顺带重构未列出的业务逻辑。

### 任务 1：新增日志规范文档

**目标**：将本计划第 3 节提炼为长期维护的日志规范，明确分级、字段、脱敏和运行命令。

**允许修改**：

- 新增 `docs/design/logging.md`
- 新增 `docs/design/logging.en.md`
- `README.md`、`README.en.md` 中与 `beam logs`/排障命令直接相关的小节（仅在确有合适入口时）

**不得改变**：运行时代码、现有命令语义、日志格式。

**实现要点**：

1. 中文和英文设计文档必须同义同步，互相链接。
2. 文档应列出级别判定表、允许字段、禁止字段、默认模式与 `RUST_LOG` 示例。
3. 明确 worker stdout 是 IPC，诊断输出必须走 stderr。
4. 不要在文档中写入任何真实环境 ID 或凭据。

**验收**：双语内容完整对应；链接有效；没有敏感样例；`git diff --check` 通过。

---

### 任务 2：统一三个二进制入口的 subscriber 策略

**目标**：消除 CLI、daemon、worker 的重复 subscriber 初始化，固定默认级别、输出目标与 target 显示策略，且保持 `RUST_LOG` 行为。

**允许修改**：

- `crates/beam-core/Cargo.toml`
- `crates/beam-core/src/lib.rs`
- 新增 `crates/beam-core/src/logging.rs`
- `crates/beam-cli/src/main.rs`
- `crates/beam-daemon/src/main.rs`
- `crates/beam-worker/src/main.rs`
- 仅为本任务所需的相邻测试文件/模块

**不得改变**：CLI 参数、daemon 启动/重启方式、worker stdout IPC、默认日志落点（stderr）。不得手改 workspace/crate 版本。

**实现要点**：

1. 在 `beam-core` 暴露一个仅初始化一次的公共函数，封装当前 `EnvFilter` 默认 `INFO` 与 `from_env_lossy()` 行为。
2. compact、人读格式保留；为可用的模块 directive 打开 target 显示（或在规范中说明等价的稳定来源字段）。三入口必须完全一致。
3. 函数不读取或输出业务秘密；不得引入全局可变 reload 句柄。
4. worker 入口显式保持 `stderr` writer；三个入口不得向 stdout 写 tracing。

**验证命令**：

```bash
cargo test -p beam-core logging
cargo build -p beam-cli
scripts/check-rust-line-count.sh
```

**验收**：三个入口不再各自构造 subscriber；无 `RUST_LOG` 时默认 `INFO`；设置无效 directive 不导致进程启动失败；设置 crate directive 可生效；worker stdout IPC 测试仍通过。

---

### 任务 3：消除 zellij token 路径的敏感日志泄露

**目标**：zellij token 创建、解析和回退失败时保留可诊断性，但任何级别都不记录原始 stdout/stderr 或 token 值。

**允许修改**：

- `crates/beam-daemon/src/zellij_web/tokens.rs`
- 该模块的现有测试模块，或新建最小相邻测试文件

**不得改变**：token 创建策略顺序、zellij 命令参数、成功/失败返回语义、token 解析逻辑。

**实现要点**：

1. 处理 `tokens.rs` 中 token 创建成功但无法解析、named strategy 失败、bare strategy 失败的所有日志与错误文本。
2. 可记录 strategy、read-only 标志、命令退出状态、stdout/stderr 长度；不得记录内容。
3. 成功但不可解析、最终创建失败保持错误语义；中间 named 策略回退仍按现有逻辑执行。
4. 优先使用结构化字段，避免 `format!` 后把不可信输出嵌入日志。

**验证命令**：

```bash
cargo test -p beam-daemon zellij_web::tokens
cargo build -p beam-cli
```

**验收**：新增测试传入包含模拟 token/secret 的 stdout/stderr，断言生成的诊断文本不含该值；原有 strategy fallback 测试继续通过。

---

### 任务 4：治理 terminal proxy 的访问、认证与 WebSocket 日志

**目标**：将正常流量和预期鉴权结果从默认日志移除，同时保留真正的上游/安全异常。

**允许修改**：

- `crates/beam-daemon/src/terminal_proxy/http_forward.rs`
- `crates/beam-daemon/src/terminal_proxy/auth.rs`
- `crates/beam-daemon/src/terminal_proxy/ws_relay.rs`
- `crates/beam-daemon/src/terminal_proxy/anchor.rs`（仅涉及同类日志时）
- 对应单元测试

**不得改变**：HTTP 状态码、cookie/ticket 验证逻辑、proxy 转发行为、read-only/write 权限行为。

**实现要点**：

1. 单个 HTTP 请求、认证成功、正常 WS 建连改为 `DEBUG`。
2. 缺 cookie、过期/无效 ticket、未认证访问等会正常发生的拒绝改为 `DEBUG`；不得记录 ticket、cookie 或完整 URI query。
3. 上游不可达、上游协议异常、cookie bridge 状态不一致、无法恢复的 WS relay 异常保留 `WARN` 或按第 3 节升级 `ERROR`。
4. 补充 `session_id`、`operation`、`status`/`outcome` 等字段，不记录用户可识别内容。

**验证命令**：

```bash
cargo test -p beam-daemon terminal_proxy
cargo build -p beam-cli
```

**验收**：现有 HTTP/WS 行为测试不变；测试或审查可确认凭据不进入日志；默认 `INFO` 无每请求成功记录。

---

### 任务 5：治理 worker 截图热路径与重复失败告警

**目标**：保留截图上传失败可见性和耗时诊断，消除默认日志中的每次成功与循环性 warning 洪泛。

**允许修改**：

- `crates/beam-worker/src/worker_runtime/screenshot.rs`
- `crates/beam-worker/src/worker_runtime/coordinator_runtime.rs`
- 与截图日志状态直接相关的相邻 coordinator 模块
- 对应单元/集成测试

**不得改变**：截图 render、Feishu upload、IPC 载荷、hash 去重、5 秒 fallback、事件驱动刷新和失败重试语义。

**实现要点**：

1. `screenshot_upload start`、success 与每次 render/upload 耗时改为 `DEBUG`，保留 `session_id`、trigger、`render_ms`、`upload_ms`、`png_bytes`。
2. render/upload/IPC 失败首次以及从成功变失败时为 `WARN`；相同原因的连续失败按固定时窗限频或降为 `DEBUG`。
3. 多条相同 hash 失败路径收敛到一个可测试的日志决策 helper，字段至少含 session、trigger、stage、screen 长度（如需要）和失败计数。
4. 限频状态只影响日志，不得跳过 upload、改变去重结果或引入无界内存增长；session 重启/成功后应重置失败状态。

**验证命令**：

```bash
cargo test -p beam-worker screenshot
cargo test -p beam-worker coordinator
cargo build -p beam-cli
```

**验收**：模拟连续相同失败时，业务调用次数与改造前一致，而 warning 次数被限制；成功上传不会在默认 `INFO` 产生逐次日志；失败仍具有足够关联字段。

---

### 任务 6：将 worker 生命周期中的预期 CAS 结果降噪

**目标**：正确区分 IPC 协议问题与截图异步结果的正常过期，避免 session close/restart 时产生误导性 warning。

**允许修改**：

- `crates/beam-daemon/src/worker_lifecycle.rs`
- 现有 daemon 测试模块或紧邻集成测试

**不得改变**：`ScreenshotUploaded` 的 CAS 接受/丢弃条件、session 更新、card patch、旧 worker `turn_id: None` 兼容语义。

**实现要点**：

1. 已不存在 session、非 Active session、turn ID 不匹配均为生命周期预期结果，改为 `DEBUG`。
2. 日志必须以结构化字段表达收到/当前 turn 与 session 状态，不能输出截图内容或 image key。
3. 真正无法解析的 IPC、违反载荷约束等协议问题才保留 `WARN`。
4. 若现有测试文本断言 warning，更新为断言状态行为而不是具体日志语句。

**验证命令**：

```bash
cargo test -p beam-daemon screenshot_cas
cargo test -p beam-daemon worker_lifecycle
cargo build -p beam-cli
```

**验收**：活跃且匹配的 screenshot 仍更新；关闭/重启/旧 turn 的上传仍丢弃；旧 worker 兼容路径不变；默认日志不将这些预期丢弃标成 warning。

---

### 任务 7a：整理 ask 超时日志

**目标**：将 ask 的协议允许超时与真正的持久化/发送失败分开记录。

**允许修改**：

- `crates/beam-daemon/src/ask.rs`
- 对应测试

**不得改变**：ask 超时对用户的响应和用户可见文案。

**实现要点**：

1. ask 超时是业务结果，使用 `INFO`，带 session、chat、app 标识和 timeout 字段（仅记录安全标识）。
2. 持久化或发送卡片失败仍按实际影响使用 `WARN`/`ERROR`。

**验证命令**：

```bash
cargo test -p beam-daemon ask
cargo build -p beam-cli
```

**验收**：业务返回和用户可见结果未变；超时不再默认 warning；无正文或秘密输出。

---

### 任务 7b：整理 adapter transcript 业务边界日志

**目标**：将可恢复的 transcript 未命中/歧义与解析/执行异常分级记录。

**允许修改**：

- `crates/beam-worker/src/worker_runtime/run_loop.rs`
- `crates/beam-worker/src/adapters/opencode/disambiguation.rs`
- 对应测试

**不得改变**：adapter transcript 选择规则、回退行为和用户可见文案。

**实现要点**：未命中/歧义使用 `INFO` 或高频时 `DEBUG`，带 session、adapter、candidate_count；解析/执行异常保持 `WARN`；不记录 transcript 正文。

**验证命令**：

```bash
cargo test -p beam-worker opencode
cargo build -p beam-cli
```

**验收**：adapter 选择与用户结果不变；可恢复状态不再默认 warning；无 transcript 正文泄露。

---

### 任务 7c：整理 workflow 错误边界日志

**目标**：使 workflow 同一 error chain 在同一请求边界仅记录一次，并按可恢复性分级。

**允许修改**：

- `crates/beam-daemon/src/workflow_commands/mod.rs`
- 与该入口直接相关的 workflow handler 模块（仅为移动记录边界）
- 对应测试

**不得改变**：workflow API 返回值、HTTP 状态和业务执行语义。

**实现要点**：通用错误转换 helper 只转换错误、不记录；在 command/request 执行边界记录一次。可预期冲突、幂等和不存在结果为 `DEBUG`/`INFO`；不可恢复执行/存储失败为 `ERROR`。

**验证命令**：

```bash
cargo test -p beam-daemon workflow
cargo build -p beam-cli
```

**验收**：业务返回不变；可恢复状态不默认 warning；不可恢复 workflow 失败有一次带上下文的 error。

---

### 任务 8：全量回归、人工排障演练与文档收尾

**目标**：验证各模块变更组合后的默认可读性、按模块展开能力和运行时约束。

**允许修改**：

- 仅补充本计划直接相关的测试、文档和必要的微小修正

**不得改变**：已验收任务的分级契约；不得借本任务重构无关模块。

**验证步骤**：

```bash
cargo test --workspace --no-fail-fast
scripts/check-rust-line-count.sh
cargo build -p beam-cli
RUST_LOG='beam_daemon=debug,beam_worker=debug,beam_cli=info' target/debug/beam restart
target/debug/beam status
```

人工演练仅在具备本地 daemon/Lark/zellij 环境时执行：启动一个测试 session，访问一次 terminal proxy，触发一次截图刷新与一次关闭；确认默认日志没有请求/截图成功洪泛，`DEBUG` 中可以按 session/operation 定位流程，且日志不含秘密。

**验收**：所有命令通过；工作区无未预期格式或行数问题；若演练环境可用，记录命令和结果；若不可用，明确说明未执行的 live 验证及原因。

## 5. 风险、兼容与回滚

| 风险 | 缓解措施 |
|---|---|
| 将现有 `WARN` 下调会掩盖真实故障 | 每次下调都以“该结果是否需要人工处理”为判断标准，并以状态测试锁定业务语义；上游不可用、重试耗尽仍保持 `WARN`。 |
| 限频改变截图行为或积累状态 | 日志限频与业务执行分离；使用有界、按 session 重置的状态；测试验证调用次数不变。 |
| 修改 subscriber 影响 IPC | worker 明确指定 stderr；通过 worker IPC 测试和 build 验证；禁止 stdout writer。 |
| 显示 target 影响既有人工检索 | 保持 compact 格式和主要消息文本；在文档中提供新的 `RUST_LOG` 过滤示例。 |
| 日志仍可能泄露凭据 | 任务 3 为优先阻断项；每个任务审查 forbidden fields；测试使用哨兵 secret 检查诊断文本。 |

如需回滚，优先回滚单个模块的 level/限频改动，不回滚 IPC、session 或外部协议逻辑；subscriber 改动应作为独立提交，确保可单独回退。

## 6. 统一交付要求

- 每项任务单独提交，提交信息遵循 `type(scope): 中文描述`。
- 不手改 `Cargo.toml` / `Cargo.lock` 的版本字段；仅在任务 2 必要时新增 `tracing` 依赖声明。
- 修改设计文档必须同步中文和英文版本。
- 所有新增 Rust 文件遵守仓库行数约束；纯逻辑测试放实现模块旁，集成测试使用 hermetic fixture。
- daemon/runtime 实际变更完成后，必须先 `cargo build -p beam-cli`，再使用 `target/debug/beam restart` 验证，不得依赖 PATH 中的旧 binary。
