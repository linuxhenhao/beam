# Beam Workflow 使用指南

workflow 是 beam 的编排层，允许你把 subagent 调用、人工审批、外部副作用串联成可审计的有向无环图（DAG）。

**核心原则**：EventLog 是唯一事实来源。所有状态变化都通过写入事件实现，Dashboard、飞书卡片、CLI 只写 command/event，不直接改状态。

## 快速开始

### 触发 workflow

在 Lark 话题中发送：

```
/workflow <workflowId> [key=value ...]
```

例如：
```
/workflow subagent-approval-feishu-send task="review and deploy the PR #42"
```

### 目录结构

所有 `.workflow.json` 文件按以下顺序搜索：

1. `<workspace>/workflows/<workflowId>.workflow.json` — 仓库内定义
2. `<data>/workflows/<workflowId>.workflow.json` — 用户级定义

## Node 类型

### `subagent`

将 prompt 交给 AI coding CLI 执行，输出 JSON。

```json
{
  "type": "subagent",
  "bot": "cli_canary_PLACEHOLDER",
  "prompt": "Write a friendly greeting. Return JSON: { \"message\": string }",
  "outputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["message"],
    "properties": { "message": { "type": "string" } }
  }
}
```

关键字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `bot` | string | Lark App ID（`cli_*`）或占位符；实际 bot 必须存在于 `beam bots list` |
| `prompt` | Value | 发送给 CLI 的 prompt，支持 `${node.output.field}` 模板插值 |
| `outputSchema` | JSON Schema | 可选的输出 schema，用于验证 subagent 返回的 JSON |
| `workingDir` | string | 可选的 CLI 工作目录 |
| `modelOverrides` | Value | 可选的模型覆盖（provider/model/temperature） |
| `toolPolicy` | Value | 可选的工具策略 |

### `hostExecutor`

调用外部 API（飞书发送/回复、定时任务），不做 AI 推理。

```json
{
  "type": "hostExecutor",
  "executor": "feishu-send",
  "input": {
    "larkAppId": "cli_demo_PLACEHOLDER",
    "chatId": "oc_demo_PLACEHOLDER",
    "content": "Hello from workflow!",
    "msgType": "text"
  }
}
```

**内置 executor**：

| executor | 用途 |
|----------|------|
| `feishu-send` | 向指定群发送新消息 |
| `feishu-reply` | 回复指定消息（需要 `rootMessageId`） |
| `beam-schedule` | 注册定时任务（通过 cron 表达式） |

### `humanGate`

不是独立的 node 类型，而是附加在任何 node 上的审批门控。两种 stage：

- **`stage: "before"`**（推荐）：节点执行前等待人工审批。prompt 会显示在审批卡片上。审批通过后节点执行；拒绝则节点失败。
- **`stage: "after"`**：节点执行完成后等待审批。目前较少使用。

```json
{
  "humanGate": {
    "stage": "before",
    "prompt": { "$ref": "draft.output.preview" },
    "approvers": ["ou_user_open_id_1", "ou_user_open_id_2"],
    "deadlineMs": 600000,
    "onTimeout": "fail"
  }
}
```

关键字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `stage` | string | `"before"` 或 `"after"` |
| `prompt` | Value | 审批提示，支持 `$ref` 引用上游节点输出 |
| `approvers` | string[] | 可选白名单，Lark 路径校验审批人身份 |
| `deadlineMs` | u64 | 可选超时（毫秒） |
| `onTimeout` | string | 超时行为：`"fail"`（默认）或 `"success"` |

### `loop`

将一组 body 节点循环执行，由 `Decision` 节点终止。

```json
{
  "type": "loop",
  "maxIterations": 3,
  "body": ["implement", "review", "reviewDecision"],
  "terminate": { "node": "reviewDecision", "via": "humanGate" },
  "output": { "from": "implement" }
}
```

关键字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `maxIterations` | u64 | 最大迭代次数（安全上限） |
| `body` | string[] | body 内的节点 ID 列表，在每轮迭代中按拓扑顺序执行 |
| `terminate.node` | string | 终止节点，必须是 `Decision` 类型且在 body 中 |
| `terminate.via` | string | 终止方式，目前必须是 `"humanGate"` |
| `output.from` | string | 可选，指定 loop 输出取自哪个 body 节点的产出（sink loop 必须声明） |

**loop 运行时行为**（已完全实现）：

1. loop 的外部依赖就绪后，写入 `loopStarted`
2. 开始 iteration 1，body 节点按拓扑顺序执行
3. 到达 `terminate.node`（Decision）时，显示审批卡片
4. **approve** → loop 成功（写入 `loopFinished`，resolution=`"approved"`），run 继续下游节点
5. **reject** → 进入下一轮迭代（`loopIterationStarted(N+1)`）。`${reviewDecision.previous.comment}` 可在 body 节点 prompt 中引用上一轮的拒绝理由
6. **超时**（`onTimeout: "fail"`）→ 等同于 reject，触发下一轮或 loop 失败
7. **达到 maxIterations** → loop 失败（写入 `loopFinished`，resolution=`"failed"`，errorCode: `MaxIterationsReached`）
8. **body 节点失败**（subagent/hostExecutor 失败，或 body 内 humanGate 被拒）→ loop 失败（写入 `loopFinished`，resolution=`"failed"`，errorCode: `BodyNodeFailed`）

**loop activity ID 格式**：

- work: `<runId>::loop::<loopId>.<N>::work::<bodyNodeId>`
- gate: `<runId>::loop::<loopId>.<N>::gate::<bodyNodeId>`（当 body 节点有 humanGate 时）

**loop 约束**（definition validation 拒绝非法定义）：

- body 节点必须存在
- body 不能包含另一个 loop（不支持嵌套）
- `terminate.node` 必须是 Decision 类型且在 body 中
- 所有 Decision 必须被 loop 使用（不允许 standalone Decision）
- body 中最多一个 Decision（即 terminate.node）
- body 节点的外部依赖必须在 loop 自身 `depends` 中声明
- 外部节点不得 depends body 节点，应 depends loop 节点
- sink loop（无外部节点依赖）必须声明 `output.from`

### `Decision`

仅作为 loop 的终止节点使用。自身不能独立作为节点存在。

```json
{
  "type": "decision",
  "humanGate": {
    "stage": "before",
    "prompt": { "$ref": "review.output.preview" },
    "deadlineMs": 1800000,
    "onTimeout": "fail"
  }
}
```

Decision 必须被至少一个 loop 的 `terminate.node` 引用。它自身只包含 `humanGate`（不含 prompt/outputSchema）。

## Side-effect hostExecutor 门控

以下 executor 会产生外部副作用：

- `feishu-send`
- `feishu-reply`
- `beam-schedule`

**默认规则**：这些 executor 必须有 `humanGate`（stage: `"before"`），否则 parse 阶段拒绝。

**例外**：设置 `unsafeAllowUngated: true` 可以显式跳过门控检查。

```json
// ✅ 合法：有 humanGate
{ "type": "hostExecutor", "executor": "feishu-send", "humanGate": {...}, "input": {...} }

// ✅ 合法：显式跳过
{ "type": "hostExecutor", "executor": "feishu-send", "unsafeAllowUngated": true, "input": {...} }

// ❌ 非法：side-effect executor 无门控
{ "type": "hostExecutor", "executor": "feishu-send", "input": {...} }
```

**风险提示**：`unsafeAllowUngated: true` 意味着 workflow 无需人工确认即可发送消息或注册定时任务。仅在以下场景才应使用：
- 上游节点已经通过 humanGate 审批（如 `canary-multistep` 中 `send` 依赖已由 `confirm` 节点的 humanGate 审批过的内容）
- workflow 触发方式本身已包含授权（如仅受信用户可触发）

## Approval 行为

### 审批卡片

当 workflow 进入 humanGate（包含 loop 中的 Decision）时，系统会自动发送可点击的审批卡片到 Lark 话题，包含：

- **Approve** 按钮：写入 `waitResolved{resolution: approved}`
- **Reject** 按钮：写入 `waitResolved{resolution: rejected}`
- **Cancel** 按钮：写入 `cancelRequested`（不直接写 `runCanceled`）
- **Comment** 输入框：可选审批意见，记录为 `waitResolved.comment`
- **Dashboard** 链接：跳转到 Dashboard 查看完整状态

卡片发送幂等：同一 `{activityId}::{attemptId}` 不会重复发送。

### Approve / Reject

- 点击 Approve/Reject → 写入 `waitResolved` 事件，由 runtime 推进
- Lark 卡片路径：先通过 `activity_id`/`attempt_id` 精确匹配，再做 approver allowlist 校验
- Dashboard 路径：自动选择唯一的 open human-gate wait
- 重复点击幂等：不会重复写入 `waitResolved`

### Cancel

- 点击 Cancel → 写入 `cancelRequested`（不直接写 `runCanceled`）
- `cancelRequested` 写入后立即取消 active dispatch tokens（通过 `WorkflowCancellationRegistry`）
- Runtime 在下一轮 `run_loop` tick 中传播 cancel：写入 `activityCanceled`（如有 running activity）→ 写入 `runCanceled`
- 对于 subagent dispatch：发送 SIGINT → 等待 worker 退出 → SIGKILL（如 5 秒后仍存活）
- EventLog 顺序：`cancelRequested` → `activityCanceled`(s) → `runCanceled`

## Recovery 行为

Beam workflow 的 recovery 基于 EventLog 重放，daemon 重启后非 terminal workflow 自动恢复。

### EventLog 是事实来源

- 所有状态都由 EventLog 事件序列重放（replay）得到
- `run_loop` 是 workflow 唯一推进入口：正常运行、冷恢复（cold attach）、Dashboard resume 都走同一套语义
- 不会绕过 EventLog 直接修改 snapshot 状态

### effectAttempted 协议

hostExecutor 每次调用外部 provider 之前，EventLog 中必须先写入 `effectAttempted`：

```
attemptCreated → resolve bindings → parse input →
write effect-input.json → append effectAttempted →
call executor hook → append terminal event
```

`effectAttempted` payload 包含 `activityId`、`attemptId`、`idempotencyKey`、`inputHash`、`idempotencyTtlMs`、`provider`。

即使 executor invoke panic/失败，EventLog 仍保留 `effectAttempted`，cold attach 时可以恢复。

### Dangling Effect 恢复

daemon 重启后，非 terminal workflow 的 `run_loop` 在每轮 tick 前执行 recovery 阶段：

1. 读取 snapshot，检查 `dangling.effect_attempted` 非空
2. 通过 `ProviderReconciler` 恢复：
   - `beam-schedule`：`readOnlyLookup`（查询已存在的定时任务）
   - `feishu-im`：`idempotentSubmit`（重发未完成的飞书消息）
   - 无 reconciler 的 provider → 写入 `manual` recovery 事件
3. 如果 recovery 写入了新事件 → replay 并 continue（不消耗 tick）
4. 如果 recovery 无法推进 → fall through 到 `run_tick`

### Dangling Wait Resolution

`waitResolved`/`waitDeadlineExceeded` 已写入但对应的 terminal event（`activitySucceeded`/`activityFailed`）未写入时：

- `run_loop` recovery 阶段确定性恢复：
  - approved → 写入 `activitySucceeded`
  - rejected → 写入 `activityFailed`（InputValidationFailed）
  - deadlineExceeded（onTimeout=success）→ 写入 `activitySucceeded`
  - deadlineExceeded（onTimeout=fail）→ 写入 `activityFailed`（WaitDeadlineExceeded）
- Open wait 保持 `AwaitingWait`，不会被错误 terminalize

### Cold Attach

daemon 启动时自动扫描所有非 terminal workflow run 目录，通过统一的 `workflow_runtime_driver::run` 调用 `run_loop`，使用与正常触发完全一致的 recovery + 推进逻辑。

## Node 公共字段

所有 node 类型共享的字段（通过 `NodeBase`）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `description` | string | 节点描述 |
| `depends` | string[] | 节点依赖列表（DAG 边） |
| `humanGate` | HumanGate | 审批门控 |
| `retryPolicy` | RetryPolicy | 重试策略（`maxAttempts`/`backoff`/`baseMs`/`factor`/`jitter`） |
| `timeoutMs` | u64 | 超时（毫秒） |
| `maxOutputBytes` | u64 | 输出最大字节数 |
| `outputSchema` | JSON Schema | 输出 JSON Schema |
| `unsafeAllowUngated` | bool | 仅 hostExecutor：跳过 side-effect gate 检查 |

## 完整示例

以下示例可在仓库 `workflows/` 目录中找到：

| 文件 | 说明 |
|------|------|
| `hello.workflow.json` | subagent → humanGate-approved subagent（最小 approval 示例） |
| `canary-multistep.workflow.json` | subagent draft → subagent confirm (humanGate) → feishu-send（三节点 dag） |
| `feishu-send-demo.workflow.json` | ungated feishu-send（最小副作用示例） |
| `feishu-reply-demo.workflow.json` | ungated feishu-reply |
| `schedule-demo.workflow.json` | ungated beam-schedule（定时任务注册） |
| `code-review-loop.workflow.json` | implement → review → reviewDecision (Decision) loop（完整 loop 示例） |
| `subagent-approval-feishu-send.workflow.json` | draft subagent → send hostExecutor（含 humanGate 审批后发送）（最小 dag + 审批 + 副作用示例） |
