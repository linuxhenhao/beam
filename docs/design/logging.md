# Beam 日志规范

English: [logging.en.md](logging.en.md)

- 日期：2026-07-16
- 状态：持续维护中的系统设计文档

本文定义 beam workspace 中所有 Rust crate 的 `tracing` 日志契约，包括级别判定、字段约定、脱敏规则和排障操作方式。所有模块的实现必须以本文为权威参考。

## 1. 输出目标

- `beam-cli`、`beam-daemon`、`beam-worker` 三个进程的 `tracing` 日志**必须写入 stderr**，不得写入 stdout。
- `beam-worker` 的 **stdout 是 line-delimited JSON IPC**，仅供 daemon 消费。worker 的 tracing 输出必须走 stderr，两者不可混淆。
- `beam start` / `beam restart` 启动的后台 daemon 将 stdout 与 stderr 追加写入 `logs/daemon.log`。本地排障时也可以直接查看该文件，或使用 `beam logs` 命令。

## 2. 级别判定

五个 `tracing` 级别按如下契约使用。实现不得自行引入额外级别或改变下文语义。

| 级别 | 使用条件 | 示例 | 禁止事项 |
|---|---|---|---|
| `ERROR` | 当前操作在完成重试/回退后仍失败，调用边界无法兑现契约 | session 关键持久化失败；最终无法启动 worker；不可恢复的 workflow 执行失败 | 把预期 HTTP 4xx、可恢复状态或下层已记录的错误再记一次 |
| `WARN` | 服务继续运行，但发生降级、异常或需要人工关注的状态 | 上游持续不可用；重试耗尽；首次/状态变化的截图连续失败；IPC 协议违例 | 正常超时语义、过期 CAS 结果、认证探测、每轮重复失败 |
| `INFO` | 默认可见的业务/生命周期结果，频率低且有运维价值 | daemon/worker 启停；session/turn 创建或完成；配置回退；恢复动作；周期健康摘要 | 每个 HTTP 请求、认证成功、每个 screen capture、原始输入输出 |
| `DEBUG` | 排障所需的单次操作细节，允许较高频 | terminal proxy 请求/认证判定；截图 render/upload 分段耗时；CAS 丢弃理由；adapter 候选与重试 | 秘密、用户正文、终端原文、完整第三方响应 |
| `TRACE` | 极高频内部状态与协议决策，仅短时定向开启 | poll tick、去重命中、状态机分支、原始事件长度/摘要 | 一切秘密与可还原用户内容 |

### 2.1 补充规则

- **高频失败策略**：对无限循环或高频回调中的失败，采用"首次 + 状态变化时为 `WARN`，其余 `DEBUG` 并限频"的策略。限频只影响日志输出，不得阻塞或改变业务重试、状态更新和恢复语义。限频状态应有界并按 session 重置。
- **同一错误链在同一个请求边界内最多有一条 `ERROR`**，避免下层与上层重复记录。
- **预期竞态、过期 CAS 结果、正常认证探测均为正常行为**，不得记录为 `WARN`（按上表分别归入 `DEBUG` 或 `INFO`）。

## 3. 结构化字段

每条结构化日志应尽可能使用以下稳定字段。字段按适用性选择，不要求每条日志包含全部字段。

| 字段 | 类型 | 说明 |
|---|---|---|
| `component` | `&str` | 产生日志的模块或组件名，如 `terminal_proxy`、`screenshot`、`worker_lifecycle` |
| `operation` | `&str` | 当前操作名，如 `render`、`upload`、`cas_accept`、`token_create` |
| `outcome` | `&str` | 操作结果，如 `success`、`failure`、`retry_exhausted`、`skipped` |
| `session_id` | `&str` | 关联的会话 ID |
| `turn_id` | `Option<&str> / Option<String>` | 关联的 turn ID（如适用） |
| `bot` | `&str` | bot 名称 |
| `worker_pid` | `u32` | worker 进程 PID |
| `trigger` | `&str` | 触发事件，如 `event_driven`、`5s_fallback`、`session_start` |
| `status` | `&str` 或 `u16` | HTTP 状态码或协议状态 |
| `elapsed_ms` | `u64` | 操作耗时（毫秒） |
| `retry_count` | `u32` | 当前重试次数 |
| `error` | `%err` | 错误描述（使用 `%err` display 格式，不拼接完整 response/body） |

### 3.1 字段使用原则

- `session_id`、`turn_id` 用于关联同一流程。无关联价值的 chat/message/open ID 不默认记录。
- 错误使用 `error = %err`，日志消息描述结果，不拼接完整 response/body。
- 附加诊断规模信息时使用 `content_len`、`candidate_count`、`png_bytes` 或不可逆摘要，不能记录内容本身。

## 4. 禁止记录的内容

以下内容**任何级别均禁止记录**为日志字段或消息文本：

- token、cookie、ticket、app secret、密码、Authorization 头
- URL query 中的凭据参数
- 用户输入全文（包括 Lark 消息正文）
- CLI/terminal 画面全文
- Lark 原始请求/响应体
- 任何可还原为上述内容的部分片段

**允许记录**（作为替代诊断信息）：

- 命令退出状态码
- stdout/stderr 的长度（`stdout_len`、`stderr_len`）
- 内容不可逆摘要（如 hash 前缀）
- 策略名（如 `named`、`bare`）
- 只读标志、权限类型

## 5. 运行与排障

### 5.1 默认级别

所有入口的默认 `tracing` 级别为 **`INFO`**。日常运维不应看到每请求、每截图或每轮询的热路径明细。

### 5.2 排障时提升级别

排障通过重启生效，使用标准 `RUST_LOG` 环境变量：

```bash
RUST_LOG='beam_daemon=debug,beam_worker=debug,beam_cli=info' target/debug/beam restart
```

排查特定模块时应进一步缩小 directive：

```bash
RUST_LOG='beam_daemon::terminal_proxy=trace' target/debug/beam restart
```

**`TRACE` 只能短时启用**，收集到所需日志后应尽快恢复为默认 `INFO` 级别。

### 5.3 常用排障命令

```bash
beam logs                          # 查看最近日志
beam status                        # 检查 daemon/worker 状态
RUST_LOG='beam_daemon=debug' target/debug/beam restart   # 开启 daemon 调试
```

## 6. 兼容性

- 本规范依赖于 Rust `tracing` 生态的 `EnvFilter` directive 语法。设置无效的 `RUST_LOG` 值不得导致进程启动失败（由 `from_env_lossy()` 保证）。
- 现有所有 `RUST_LOG` 标准 directive 语法必须保持兼容。
- 本规范不改变 `beam logs` 的读取方式、日志落盘路径或 daemon/worker 进程模型。

## 参考

- 日志体系改造计划：[docs/plans/2026-07-16-logging-levels-plan.md](../plans/2026-07-16-logging-levels-plan.md)
