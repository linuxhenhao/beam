# Beam Workflow Implementation Plan

本文档把 workflow 重构设计拆成可交给初级工程师执行的任务。目标不是把 beam 做成 agent framework，而是把 beam workflow 收敛成可靠的 IM ↔ 既有 CLI agent 桥接运行时。

核心原则：

- EventLog 是唯一事实来源。
- Dashboard、飞书卡片、CLI 只写 command/event，不直接改状态。
- hostExecutor 的外部副作用必须先写 `effectAttempted`，再调用外部 provider。
- `run_loop` 是 workflow 唯一推进入口，正常运行、冷恢复、dashboard resume 都应走同一套语义。
- loop 要么完整实现，要么 parse 阶段拒绝，不能“定义存在但运行时跳过”。

## Phase 0: 基线保护

### Task 0.1: 补充 workflow 现状回归测试

涉及文件：

- `crates/beam-core/tests/`
- `crates/beam-core/src/workflow_definition.rs`
- `crates/beam-core/src/workflow_runtime.rs`
- `crates/beam-daemon/src/lib.rs`

任务：

- 添加最小 DAG workflow 成功测试。
- 添加 humanGate approve 后继续执行测试。
- 添加 hostExecutor 执行会产生 terminal event 的测试。
- 添加 run cancel 后不再 dispatch 新 action 的测试。

验收标准：

- `cargo test -p beam-core workflow` 通过。
- 测试能清晰表达当前行为，后续重构失败时能定位回归。

注意事项：

- 这阶段不要改行为，只补测试。
- 如果现有行为不理想，测试名称要写清楚是 current behavior，不要伪装成目标行为。

## Phase 1: Definition Validation

### Task 1.1: 加强 node id 校验

涉及文件：

- `crates/beam-core/src/workflow_definition.rs`

任务：

- 增加 node id 正则校验：`^[A-Za-z0-9_.-]+$`。
- 保留现有 `.`、`..`、包含 `..` 的拒绝逻辑。
- 给非法 id 添加单元测试。

验收标准：

- `node/a`、`node a`、`..`、`a..b` 都被拒绝。
- `node-a`、`node_a`、`node.a` 被接受。

### Task 1.2: 增加 side-effect executor gate 校验

涉及文件：

- `crates/beam-core/src/workflow_definition.rs`

任务：

- 添加 side-effect executor 列表：`feishu-send`、`feishu-reply`、`beam-schedule`。
- 如果 `hostExecutor.executor` 在该列表中，且没有 `humanGate`，且 `unsafeAllowUngated != true`，parse 阶段拒绝。
- 添加对应测试。

验收标准：

- ungated `feishu-send` 被拒绝。
- 带 `humanGate` 的 `feishu-send` 被接受。
- 设置 `unsafeAllowUngated: true` 的 `feishu-send` 被接受。

### Task 1.3: 暂时拒绝未实现 loop，或补齐 loop validation

涉及文件：

- `crates/beam-core/src/workflow_definition.rs`
- `crates/beam-core/src/workflow_orchestrator.rs`

任务：

- 如果 Phase 8 之前不实现 loop，则在 parse 阶段拒绝 `WorkflowNode::Loop` 和 standalone `Decision`。
- 错误信息必须明确：`loop runtime is not implemented yet`。
- 如果团队决定直接实现 loop，则跳过此任务，做 Phase 8。

验收标准：

- `workflows/code-review-loop.workflow.json` 在 loop 未实现前不能 parse 成功。
- 普通 DAG workflow 不受影响。

## Phase 2: HostExecutor Protocol

### Task 2.1: 定义 HostExecutor trait 和 registry

建议新增文件：

- `crates/beam-daemon/src/workflow_host_executors.rs`

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- `crates/beam-core/src/workflow_runtime.rs`

任务：

- 定义 `HostExecutor` trait。
- 定义 `HostExecutorRegistry`。
- trait 最少包含：`name`、`provider`、`idempotency_ttl_ms`、`parse_input`、`canonical_input`、`invoke`、`classify_error`。
- 先注册 `feishu-send`、`feishu-reply`、`beam-schedule`。

验收标准：

- daemon 可通过 registry 找到 executor。
- 未注册 executor 返回 `UnknownProviderError/manual`。
- 现有 hostExecutor demo workflow 仍可运行。

### Task 2.2: 在 core runtime 写入 effectAttempted

涉及文件：

- `crates/beam-core/src/workflow_runtime.rs`
- `crates/beam-core/src/workflow_sidecar.rs`
- `crates/beam-core/src/workflow_snapshot.rs`

任务：

- hostExecutor dispatch 流程改成：
  - `attemptCreated`
  - resolve bindings
  - parse input
  - write `effect-input.json`
  - append `effectAttempted`
  - call executor hook
  - append terminal event
- `effectAttempted` payload 包含：`activityId`、`attemptId`、`idempotencyKey`、`inputHash`、`idempotencyTtlMs`、`provider`。
- idempotency key 使用已有 `derive_workflow_idempotency_key` 逻辑，但应移动到 core 或共享模块。

验收标准：

- hostExecutor 每次调用外部 provider 前，EventLog 中已有 `effectAttempted`。
- 如果 executor invoke panic/失败，EventLog 仍保留 `effectAttempted`。
- snapshot 能投影 `dangling.effect_attempted`。

注意事项：

- 不要在 daemon executor 分支里分别写 `effectAttempted`。
- `effectAttempted` 必须早于外部 API 调用。

### Task 2.3: 迁移 daemon host executor match 分支

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- `crates/beam-daemon/src/workflow_host_executors.rs`

任务：

- 把 `run_workflow_host_executor` 里的 `feishu-send`、`feishu-reply`、`beam-schedule` 逻辑迁移到 executor 实现。
- `run_workflow_host_executor` 只负责 registry lookup 和调用统一 executor。

验收标准：

- `run_workflow_host_executor` 不再包含大型 `match node.executor.as_str()`。
- 每个 executor 有独立单元测试或集成测试。

## Phase 3: Reconciler Registry

### Task 3.1: 定义 ProviderReconciler trait

建议新增文件：

- `crates/beam-daemon/src/workflow_reconcilers.rs`

涉及文件：

- `crates/beam-core/src/workflow_resume.rs`
- `crates/beam-daemon/src/lib.rs`

任务：

- 定义 `ProviderReconciler` trait。
- 支持能力：`read_only_lookup`、`idempotent_submit`。
- 支持 `requires_effect_input` 和 `canonical_input`。
- 注册 `beam-schedule` reconciler 和 `feishu-im` reconciler。

验收标准：

- 不再需要 `resume_schedule_dangling_effects` 和 `resume_feishu_im_dangling_effects` 两套独立入口。
- provider 缺失时进入 manual recovery。

### Task 3.2: 合并 resume 决策树

涉及文件：

- `crates/beam-core/src/workflow_resume.rs`
- `crates/beam-core/src/workflow_snapshot.rs`

任务：

- 实现通用 `resume_dangling_effects`。
- 逻辑包含：
  - prior `reconcileResult` recovery
  - missing reconciler -> manual failure
  - missing effect input -> manual failure
  - input hash mismatch -> manual failure
  - readOnlyLookup success -> `activitySucceeded`
  - idempotentSubmit success -> `activitySucceeded`
  - retryable failure -> 保持 dangling，返回 transient failure

验收标准：

- schedule 和 feishu 都通过同一个 resume 函数恢复。
- 现有 `workflow_resume.rs` 测试通过并新增 feishu/schedule registry 测试。

## Phase 4: run_loop 内置 Recovery

### Task 4.1: 在 run_loop 前置 recovery 阶段

涉及文件：

- `crates/beam-core/src/workflow_runtime.rs`
- `crates/beam-core/src/workflow_resume.rs`
- `crates/beam-daemon/src/lib.rs`

任务：

- 修改 `run_loop`：每轮 `decide_next_actions` 前先处理 dangling 状态。
- 如果 recovery 写入了事件，重新 replay 并进入下一轮。
- 如果 recovery 无法推进，返回 `NoProgress`。

验收标准：

- cold attach 非 terminal workflow 时会自动尝试 recover dangling effect。
- dashboard `/resume` 不再包含大量 provider-specific 恢复逻辑，只是调用统一 run loop 或 recovery API。

### Task 4.2: 增加 dangling wait resolution 投影

涉及文件：

- `crates/beam-core/src/workflow_snapshot.rs`
- `crates/beam-core/src/workflow_actions.rs`
- `crates/beam-core/src/workflow_resume.rs`

任务：

- `DanglingSnapshot` 增加 `wait_resolutions`。
- replay 中区分：
  - open wait：`waitCreated` 后无 resolution
  - resolved wait dangling：`waitResolved`/`waitDeadlineExceeded` 已写，但 terminal 未写
- recovery 只 materialize resolved wait terminal，不处理 open wait。

验收标准：

- open wait 让 run_loop 返回 `AwaitingWait`。
- resolved-but-no-terminal wait 会被 resume 写入 terminal event。

## Phase 5: Approval Card 闭环

### Task 5.1: 抽出 workflow approval command handler

建议新增文件：

- `crates/beam-daemon/src/workflow_commands.rs`

涉及文件：

- `crates/beam-daemon/src/lib.rs`

任务：

- 实现统一 command：
  - approve wait
  - reject wait
  - cancel run
- Dashboard API 和 Lark card action 都调用这个 command handler。

验收标准：

- dashboard approve/reject 行为不变。
- Lark card action 也能写 `waitResolved` 并推进 workflow。

### Task 5.2: 修复 Lark wf_approve/wf_reject/wf_cancel 行为

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- `crates/beam-daemon/src/workflow_commands.rs`

任务：

- `wf_approve` 调用 `resolve_wait(Approved)`。
- `wf_reject` 调用 `resolve_wait(Rejected)`。
- `wf_cancel` 调用 `request_cancel(run)`，不要直接 `runCanceled`。
- 写入事件后调用 `run_workflow_runtime_once` 或统一 driver。
- 保留 frozen card 幂等能力。

验收标准：

- 点击飞书 approval card 后，EventLog 出现 `waitResolved`。
- workflow 继续执行或进入 terminal。
- 重复点击不会重复写 `waitResolved`。

### Task 5.3: 自动发送 approval card

建议新增文件：

- `crates/beam-daemon/src/workflow_event_fanout.rs`

任务：

- 监听 workflow `events.ndjson`。
- 发现新 `waitCreated` 且 `waitKind == human-gate` 时，读取 `chat-binding.json` 并发送 approval card。
- card 包含 approve/reject/cancel 按钮、comment input、dashboard link。

验收标准：

- workflow 进入 humanGate 后自动发送可点击 approval card。
- 不依赖 dashboard 手动 approve。

## Phase 6: Cancel Propagation

### Task 6.1: 修正 dashboard cancel 行为

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- `crates/beam-core/src/workflow_runtime.rs`

任务：

- dashboard cancel 只写 `cancelRequested(run)`。
- 不再直接写 `runCanceled`。
- 调用 run loop 让 runtime 完成 activity/node/run cancel propagation。

验收标准：

- cancel 后 EventLog 顺序不再是 `cancelRequested -> runCanceled` 直接结束。
- 对已有 running activity，最终能看到 `activityCanceled`。

### Task 6.2: 引入 workflow active cancellation registry

建议新增文件：

- `crates/beam-daemon/src/workflow_cancellation.rs`

任务：

- 使用 `tokio_util::sync::CancellationToken`。
- runtime dispatch work 时注册 activity token。
- cancelRequested(run/node/activity) 后 cancel 对应 token。

验收标准：

- 单测能证明 cancelRequested 后 active dispatch 收到 token。

### Task 6.3: worker cancel 接入真实 session

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- worker/session 管理相关代码

任务：

- subagent workflow session 记录 session id 和 worker pid。
- cancel token 触发时：
  - close/interrupt session
  - 发送 SIGINT
  - grace 后 SIGKILL
  - worker 退出后返回 `WorkflowDispatchOutcome::Cancelled`

验收标准：

- 长任务 workflow cancel 后，worker 进程被终止。
- `activityCanceled` 在 worker 确认退出后写入。

## Phase 7: Runtime Driver 收敛

### Task 7.1: 抽出 workflow runtime driver

建议新增文件：

- `crates/beam-daemon/src/workflow_runtime_driver.rs`

任务：

- 把 `run_workflow_runtime_once` 从 `lib.rs` 移出。
- driver 负责：
  - load definition
  - create runtime context
  - attach event fanout
  - send/update progress card
  - call `run_loop`

验收标准：

- `lib.rs` 中 workflow runtime 相关大段逻辑减少。
- trigger、approval、cancel、cold attach 都调用同一个 driver。

### Task 7.2: cold attach 使用统一 recovery run loop

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- `crates/beam-core/src/workflow_cold_scan.rs`

任务：

- cold attach 后调用统一 driver。
- 不写特殊恢复逻辑。

验收标准：

- daemon restart 后 non-terminal workflow 可以继续等待、恢复或推进。

## Phase 8: Loop Runtime

### Task 8.1: 扩展 OrchestratorAction 支持 loop lifecycle

涉及文件：

- `crates/beam-core/src/workflow_orchestrator.rs`
- `crates/beam-core/src/workflow_runtime.rs`
- `crates/beam-core/src/workflow_snapshot.rs`

任务：

- 增加 action：
  - `StartLoop`
  - `StartLoopIteration`
  - `FinishLoopIteration`
  - `FinishLoop`
- runtime settle 阶段写入：
  - `loopStarted`
  - `loopIterationStarted`
  - `loopIterationFinished`
  - `loopFinished`

验收标准：

- action 能序列化为对应事件。
- replay 后 `snapshot.loops` 正确更新。

### Task 8.2: 实现 loop dispatch pass

涉及文件：

- `crates/beam-core/src/workflow_orchestrator.rs`

任务：

- 对齐 botmux loop 语义：
  - loop depends 成功后写 `loopStarted`
  - iteration 0 后写 `loopIterationStarted(1)`
  - body 节点按拓扑顺序执行
  - decision approve -> finish loop success
  - decision reject -> 下一轮
  - maxIterations -> loop failed
  - body failure -> loop failed
- activity id 格式：
  - `<runId>::loop::<loopId>.<N>::work::<bodyNodeId>`
  - `<runId>::loop::<loopId>.<N>::gate::<bodyNodeId>`

验收标准：

- `workflows/code-review-loop.workflow.json` 可以执行到 approval wait。
- reject 后进入下一轮。
- approve 后 loop succeeded，run 可以 succeeded。

### Task 8.3: 补齐 loop definition validation

涉及文件：

- `crates/beam-core/src/workflow_definition.rs`

任务：

- body node 必须存在。
- body node 不能是 loop。
- decision 必须在 loop body 中，且作为 terminate.node。
- 每个 loop body 只能有一个 decision。
- body external deps 必须显式出现在 loop.depends。
- external node 不能依赖 loop body，只能依赖 loop block。
- sink loop 必须声明 `output.from`。

验收标准：

- 错误 loop 定义在 parse 阶段失败。
- 合法 code-review-loop 定义通过。

## Phase 9: 清理与文档

### Task 9.1: 拆分 daemon lib.rs 中的 workflow 代码

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- 新增 workflow 相关模块

任务：

- 将 host executors、reconcilers、runtime driver、approval card、workflow commands 分模块。
- `lib.rs` 只保留路由 glue 和 app state wiring。

验收标准：

- `lib.rs` workflow 相关逻辑明显减少。
- 每个模块有清晰职责。

### Task 9.2: 更新 workflow 文档和示例

涉及文件：

- `README.md`
- `workflows/*.workflow.json`
- 可新增 `docs/workflow.md`

任务：

- 说明支持的 node 类型。
- 说明 side-effect executor 默认需要 humanGate。
- 说明 approval/cancel/recovery 行为。
- 修正或标注 loop 示例。

验收标准：

- 用户按文档能创建一个 subagent -> approval -> feishu-send workflow。
- loop 示例在 loop runtime 完成前不会误导用户。

## 推荐执行顺序

1. Phase 0: 基线测试。
2. Phase 1: definition validation，先堵住坏输入。
3. Phase 2: hostExecutor side-effect protocol，这是最核心可靠性缺口。
4. Phase 3: reconciler registry。
5. Phase 4: run_loop 内置 recovery。
6. Phase 5: approval card 闭环。
7. Phase 6: cancel propagation。
8. Phase 7: runtime driver 收敛。
9. Phase 8: loop runtime。
10. Phase 9: 清理和文档。

## 每个 PR 的通用验收要求

- 必须包含单元测试或集成测试。
- 必须运行 `cargo test` 或至少相关 crate 的测试。
- 不要在同一个 PR 同时做大规模移动和行为修改，除非必要。
- 不要绕过 EventLog 直接修改 snapshot 状态。
- 不要在 provider 调用后才写 `effectAttempted`。
- 不要让 parse 成功但 runtime 永久 `NoProgress` 的 workflow 进入系统。
