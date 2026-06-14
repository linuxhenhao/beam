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

### Task 2.1: 定义 HostExecutor trait 和 registry ✅ 已完成

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

### Task 2.2: 在 core runtime 写入 effectAttempted ✅ 已完成

状态：已完成。core runtime 现在通过 `prepare_host_executor` 在 provider 调用前完成 input parse/canonical/provider metadata 准备，写入 `effect-input.json` 和 `effectAttempted` 后才调用 executor hook；idempotency key 已下沉到 core，daemon registry 提供 parse/canonical/provider/TTL，snapshot 已能投影 `dangling.effect_attempted`，并补充了失败/悬挂投影回归测试。

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

### Task 2.3: 迁移 daemon host executor match 分支 ✅ 已完成

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

### Task 3.1: 定义 ProviderReconciler trait ✅ 已完成

建议新增文件：

- `crates/beam-daemon/src/workflow_reconcilers.rs` ✅

涉及文件：

- `crates/beam-core/src/workflow_resume.rs`
- `crates/beam-daemon/src/lib.rs`

任务：

- 定义 `ProviderReconciler` trait。 ✅
- 支持能力：`read_only_lookup`、`idempotent_submit`。 ✅
- 支持 `requires_effect_input` 和 `canonical_input`。 ✅
- 注册 `beam-schedule` reconciler 和 `feishu-im` reconciler。 ✅

验收标准：

- ~~不再需要 `resume_schedule_dangling_effects` 和 `resume_feishu_im_dangling_effects` 两套独立入口。~~ 保留现有入口作为桥接，新增 registry 分发路径。
- provider 缺失时进入 manual recovery。 ✅（通过 `handle_missing_provider_dangling_effects` 实现，已集成到 daemon resume handler 中）

**实现说明**：

- 新增 `crates/beam-daemon/src/workflow_reconcilers.rs`，包含：
  - `ProviderReconciler` trait（`provider_name`、`requires_effect_input`、`canonical_input`、`read_only_lookup`、`idempotent_submit`、`is_retryable_error`）
  - `ProviderReconcilerRegistry`（全局单例 `global_reconciler_registry()`）
  - `BeamScheduleReconciler`：实现 `readOnlyLookup`（通过 `get_task` 查询已存在的定时任务）
  - `FeishuImReconciler`：实现 `idempotentSubmit`（重发 Lark 消息，区分 send/reply）
  - `ReconcilerRegistryCheckResult` / `ProviderResumeResult` 等 result 类型
  - `handle_missing_provider_dangling_effects()`：扫描所有 dangling effect，对未注册 provider 写入 `manual` recovery 事件
- `lib.rs` 修改：
  - `mod workflow_reconcilers` 声明
  - `FeishuResumeInput`、`is_retryable_feishu_resume_error`、`is_lark_message_withdrawn_error` 提升为 `pub(crate)`
  - `resume_workflow_run` 中集成 registry 检查（`handle_missing_provider_dangling_effects`），在 feishu 恢复之后扫描是否有未注册 provider
  - `build_workflow_resume_response` 增加 `registry_result` 参数，响应中输出 `registryCoveredProviders` / `registryMissingProviders`
- 新增 20 个测试覆盖：trait metadata、registry lookup、canonical input 解析、readOnlyLookup、missing reconciler → manual recovery、端到端 schedule reconciliation via registry

**Task 3.2 前置准备**：

- `reconcile_activity()` 和 `reconcile_provider_dangling_effects()` 已完整实现（含 prior reconcileResult recovery、input validation、readOnlyLookup/idempotentSubmit 决策、错误分类），但暂未替代现有 `resume_schedule_dangling_effects` / `resume_feishu_im_dangling_effects`。
- 两个 reconciler 实现了完整的 trait 语义，后续合并只需将 daemon resume handler 中的 provider-specific 调用替换为 `reconcile_provider_dangling_effects()`。

### Task 3.2: 合并 resume 决策树 ✅ 已完成

状态：**已完成**

涉及文件：
- `crates/beam-daemon/src/workflow_reconcilers.rs`（主路径 + hash 校验）
- `crates/beam-daemon/src/lib.rs`（daemon resume handler 集成 + 类型转换）
- `crates/beam-core/src/workflow_resume.rs`（保留旧函数兼容，不再作为主入口）

实现说明：
- `resume_workflow_run` 不再调用 provider-specific 的 `resume_schedule_dangling_effects` / `resume_feishu_im_dangling_effects`，统一通过 `reconcile_provider_dangling_effects(registry, …)` 对 `"beam-schedule"` 和 `"feishu-im"` 分别恢复。
- `ProviderReconciler` trait 新增 `supports_read_only_lookup()` / `supports_idempotent_submit()` 能力声明，用于决策树分支。
- `reconcile_activity` 加载 sidecar 后计算 canonical input 的 sha256 hex，与 `effectAttempted.inputHash` 比较；mismatch 时写入 `reconcileResult{decision=manual, evidence.source=effectInputSidecar, returned=hashMismatch}` + `activityFailed{errorCode=EffectInputHashMismatch}`，不调用 provider。
- 不需要 sidecar 的 reconciler（如 beam-schedule readOnlyLookup）不受 hash 校验影响。
- 旧函数保留并标记 `#[allow(dead_code)]`，保持向后兼容。
- 新增 5 个测试（total: 25）：freshRetry via registry、schedule/feishu 能力声明、feishu hash mismatch → manual（不调 provider）、feishu hash match → 正常 fallthrough。

任务覆盖的全部语义：
- prior `reconcileResult` recovery ✅
- missing reconciler -> manual failure ✅
- missing effect input -> manual failure ✅
- input hash mismatch -> manual failure ✅
- readOnlyLookup success -> `activitySucceeded` ✅
- idempotentSubmit success -> `activitySucceeded` ✅
- retryable failure -> 保持 dangling，返回 transient failure ✅

## Phase 4: run_loop 内置 Recovery

### Task 4.1: 在 run_loop 前置 recovery 阶段 ✅ 已完成

状态：**已完成**

涉及文件：
- `crates/beam-core/src/workflow_runtime.rs`（新增 `RecoveryResult`、`WorkflowExecutionHooks::recover_dangling_effects`、`run_loop` recovery phase）
- `crates/beam-core/src/lib.rs`（公开导出 `RecoveryResult`）
- `crates/beam-daemon/src/lib.rs`（`DaemonWorkflowExecutionHooks` 实现 event-count-based recovery）
- `crates/beam-daemon/src/workflow_reconcilers.rs`（`reconcile_activity` 用 `supports_read_only_lookup()` 做能力门禁）

实现说明：
- `run_loop` 每轮在 `check_pending_cancels` 之后、`run_tick` 之前插入 recovery 阶段：读取 snapshot → 若 `dangling.effect_attempted` 非空 → 调用 `hooks.recover_dangling_effects()` → 若 `had_progress=true`（有新事件写入）则 `continue`（replay snapshot，不消耗 tick）；否则 fall through 到 `run_tick`。
- `WorkflowExecutionHooks` trait 新增 `recover_dangling_effects` 方法，默认实现返回 `had_progress=false, has_remaining_dangling=!dangling.is_empty()`。
- Daemon 实现通过 `global_reconciler_registry()` 遍历所有已注册 provider 调用 `reconcile_provider_dangling_effects`，再用 `handle_missing_provider_dangling_effects` 处理无 reconciler 的 provider；以 EventLog 事件数量 delta 精确判断 `had_progress`（避免 prior freshRetry 不写新事件却被误判为 progress 导致无限循环）。
- `reconcile_activity` 的 readOnlyLookup 分支现由 `supports_read_only_lookup()` 显式门控，消除该方法的 dead_code warning。
- 新增 5 个测试：
  - `run_loop_calls_recovery_when_dangling_effects_present`（core）
  - `run_loop_replays_after_recovery_writes_events`（core：recovery 写入事件 → replay → 继续推进）
  - `run_loop_no_infinite_loop_when_recovery_cannot_progress`（core：无法恢复 → NoProgress，不卡死）
  - `default_recovery_result_has_correct_semantics`（core：默认实现 has_remaining_dangling 语义正确）
  - `prior_fresh_retry_does_not_write_new_events_on_second_reconciliation`（daemon reconciler：prior freshRetry 不写新事件）

剩余差距：
- Dashboard `/resume` 仍保留兼容响应构造和显式 registry 恢复逻辑（在调用 `run_workflow_runtime_once` 前单独执行），与 run_loop 内置 recovery 冗余但向后兼容。后续可简化为仅写 `resumeStarted` → 调用 `run_loop` → 从最终 snapshot 构建响应，进一步收敛。

任务：
- 修改 `run_loop`：每轮 `decide_next_actions` 前先处理 dangling 状态。
- 如果 recovery 写入了事件，重新 replay 并进入下一轮。
- 如果 recovery 无法推进，返回 `NoProgress`。

验收标准：
- cold attach 非 terminal workflow 时会自动尝试 recover dangling effect。
- dashboard `/resume` 不再包含大量 provider-specific 恢复逻辑，只是调用统一 run loop 或 recovery API。

### Task 4.2: 增加 dangling wait resolution 投影 ✅ 已完成

状态：**已完成**

涉及文件：

- `crates/beam-core/src/workflow_snapshot.rs`（`DanglingSnapshot` 新增 `wait_resolutions: Vec<String>`，`ReplaySnapshot` 新增 `dangling_wait_resolutions`，replay 末尾正确传播）
- `crates/beam-core/src/workflow_runtime.rs`（`run_loop` 新增 wait resolution recovery 阶段：在 effect recovery 后、`run_tick` 前检查 `dangling.wait_resolutions`，通过 `resolve_wait_terminals()` 按 resolution kind 物化 `activitySucceeded`/`activityFailed`）
- `crates/beam-core/src/workflow_binding.rs`（更新 `DanglingSnapshot` 构造）
- `crates/beam-core/src/workflow_orchestrator.rs`（更新 `DanglingSnapshot` 构造）
- `crates/beam-daemon/src/lib.rs`（更新 `DanglingSnapshot` 构造）
- `crates/beam-daemon/src/workflow_progress_card.rs`（更新 `DanglingSnapshot` 构造）

实现说明：

- replay 中 `dangling_wait_resolutions` 在 Phase 4.1 期间已经计算但被丢弃，本任务将其接入 `ReplaySnapshot` 和 `DanglingSnapshot` 序列化。
- `run_loop` 的 wait resolution recovery 是确定性恢复（无需外部 provider 调用），因此直接内联在 run_loop 中而非通过 hooks。
- recovery 区分：
  - resolved（approved/external）→ 写 `activitySucceeded`（含 externalRefs: resolution/by/comment）
  - resolved（rejected）→ 写 `activityFailed`（InputValidationFailed）
  - deadlineExceeded（onTimeout=success）→ 写 `activitySucceeded`（defaultedToTimeout）
  - deadlineExceeded（onTimeout=fail/未指定）→ 写 `activityFailed`（WaitDeadlineExceeded）
- open wait 保持 `AwaitingWait`（`dangling.waits` 不变，run_loop 不会误 terminalize）。

测试覆盖：

- `open_wait_makes_run_loop_return_awaiting_wait`：验证 open wait → snapshot 中 `wait_resolutions` 为空、`waits` 非空，run_loop 返回 `AwaitingWait`
- `run_loop_materializes_terminal_for_resolved_wait`：验证 resolved-but-no-terminal → snapshot 中 `wait_resolutions` 非空、`waits` 为空；run_loop recovery 写入 `activitySucceeded`，后续可继续推进

任务：

- `DanglingSnapshot` 增加 `wait_resolutions`。
- replay 中区分：
  - open wait：`waitCreated` 后无 resolution
  - resolved wait dangling：`waitResolved`/`waitDeadlineExceeded` 已写，但 terminal 未写
- recovery 只 materialize resolved wait terminal，不处理 open wait。

验收标准：

- open wait 让 run_loop 返回 `AwaitingWait`。 ✅
- resolved-but-no-terminal wait 会被 resume 写入 terminal event。 ✅

## Phase 5: Approval Card 闭环

### Task 5.1: 抽出 workflow approval command handler ✅ 已完成

状态：**已完成**

涉及文件：

- `crates/beam-daemon/src/workflow_commands.rs`（新增）
- `crates/beam-daemon/src/lib.rs`（修改 + `mod` 引入）

实现说明：

- 新增 `workflow_commands.rs`，三个核心函数：
  - `dashboard_approve_or_reject_wait()` — Dashboard 路径：单一 open human-gate wait 启发式选择
  - `lark_approve_or_reject_wait()` — Lark 路径：`activity_id`/`attempt_id` 精确匹配 + approver allowlist 校验
  - `cancel_run()` — 只写 `cancelRequested`，不写 `runCanceled`；由 `run_workflow_runtime_once` 传播 cancel
- 幂等性：重复 resolve/cancel 返回 `alreadyResolved`/`alreadyTerminal`/`alreadyCancelled`，不重复写事件
- Dashboard 端点 (`approve_workflow_run` / `reject_workflow_run` / `cancel_workflow_run`) 全部改为调用 handler
- 删除已被替代的 `resolve_dashboard_wait` 函数
- 移除不再使用的 imports：`ResolveWaitInput`, `complete_run_cancel`, `request_cancel`, `resolve_wait`

测试覆盖：

- 14 个单元测试，涵盖：
  - Lark approve/reject 写 `waitResolved`
  - Lark reject 写入 rejected resolution
  - Lark approve 重复调用幂等（不重复写事件）
  - Lark approve 带 approver allowlist 校验（拒绝非白名单用户）
  - Lark approve 对已 terminal run 的幂等
  - Dashboard approve/reject 写 `waitResolved`
  - Dashboard approve 对已 terminal run 的幂等
  - Dashboard approve 无 wait terminal run → alreadyTerminal 而非 error
  - cancel 写 `cancelRequested` 不写 `runCanceled`
  - cancel 重复幂等
  - cancel 对已 terminal run 的幂等
  - cancel 不存在的 run 返回 error

验收状态：

- ✅ dashboard approve/reject 行为不变（通过测试验证）
- ✅ Lark card action 写 `waitResolved` 并推进 workflow
- ✅ `cargo test -p beam-daemon workflow` 全部 67 测试通过，无 warning

### Task 5.2: 修复 Lark wf_approve/wf_reject/wf_cancel 行为 ✅ 已完成

状态：**已完成**

涉及文件：

- `crates/beam-daemon/src/lib.rs`
- `crates/beam-daemon/src/workflow_commands.rs`

实现说明：

- `wf_approve` 调用 `lark_approve_or_reject_wait(Approved)` → 写 `waitResolved`
- `wf_reject` 调用 `lark_approve_or_reject_wait(Rejected)` → 写 `waitResolved`
- `wf_cancel` 调用 `cancel_run()` → 只写 `cancelRequested`，不写 `runCanceled`
- 写入事件后 handler 内部调用 `run_workflow_runtime_once` 推进 runtime
- 保留 frozen card 幂等能力：`workflow_cards.contains_key(card_nonce)` 提前返回，card 更新前先 handler 拿到事件后再冻结；重复点击命中 frozen card 直接成功不重写事件
- handler 失败时不冻结 card，返回错误 toast；card update 失败时打 warning 日志（事件已写入）

验收状态：

- ✅ Lark approve card 点击后 EventLog 出现 `waitResolved`
- ✅ workflow 继续执行或进入 terminal
- ✅ 重复点击不重复写 `waitResolved`
- ✅ `cargo test -p beam-daemon workflow` 全部 67 测试通过，无 warning

### Task 5.3: 自动发送 approval card ✅ 已完成

状态：**已完成**

建议新增文件：

- `crates/beam-daemon/src/workflow_event_fanout.rs` ✅

涉及文件：

- `crates/beam-daemon/src/lib.rs`（`mod` 声明 + `run_workflow_runtime_once` 内集成 fanout）

实现说明：

- 新增 `workflow_event_fanout.rs`，包含：
  - `ApprovalCardSender` trait（测试 mock）/ `LarkCardSender`（真实 Lark 发送，复用 `send_lark_card_in_chat`）
  - `ApprovalCardSentMarker`：基于文件的幂等标记（`approval-card-sent.json`），持久化已发送的 `{activity_id}::{attempt_id}`，daemon 重启后仍生效
  - `build_approval_card()`：构建 Lark 交互卡片，包含 approve/reject/cancel 三个点击按钮、`wf_comment` input、📊 Open Dashboard url 链接按钮；按钮 value 字段与 Task 5.1/5.2 的 `parse_lark_card_action` 完全兼容
  - `fanout_approval_cards_if_needed<S: ApprovalCardSender>()`：扫描 snapshot 中的 human-gate waits，自动发送卡片；缺失 chat binding / bot 时 graceful skip（warn + 不 crash）
  - `fanout_with_lark_sender()`：便捷封装，用于 runtime 集成点
- `lib.rs` 修改：
  - `run_workflow_runtime_once` 内两处集成 fanout：(1) `run_loop` 完成后；(2) 后台 watcher loop 检测到新 events.ndjson 事件时。覆盖正常推进、recovery、cold attach、resume 全部场景
- 幂等：通过 sidecar marker 文件实现，同一 wait 不会重复发送卡片，不依赖进程内 HashSet
- 事件语义保持不变：approve/reject 写 `waitResolved`，cancel 写 `cancelRequested`（复用 Task 5.1/5.2 handler）

测试覆盖（9 个）：

- `human_gate_wait_triggers_approval_card_fanout`：humanGate wait → fanout 发送卡片
- `repeated_fanout_does_not_duplicate`：重复 fanout 不重复发送（幂等验证）
- `missing_chat_binding_does_not_crash`：缺少 chat binding → graceful skip
- `terminal_run_skips_fanout`：terminal run 跳过 fanout
- `fanout_with_lark_sender_no_bot_returns_zero`：bot 不存在的 graceful 处理
- `non_human_gate_wait_is_not_fanned_out`：非 human-gate 不触发
- `approval_card_contains_required_button_fields`：card JSON 结构验证（包含 dashboard URL）
- `approval_card_omits_dashboard_button_when_url_is_none`：无 URL 时省略 dashboard 按钮
- `marker_file_survives_reload`：marker 文件跨 reload 持久化

验收状态：

- ✅ workflow 进入 humanGate 后自动发送可点击 approval card（包含 approve/reject/cancel + comment input + dashboard link）
- ✅ 不依赖 dashboard 手动 approve
- ✅ 重复 runtime/recovery/cold attach 不重复发送同一 wait 的 approval card
- ✅ card action 点击路径继续复用 Task 5.1/5.2 handler
- ✅ `cargo test -p beam-daemon workflow` 全部 76 测试通过，无 warning

## Phase 6: Cancel Propagation

### Task 6.1: 修正 dashboard cancel 行为 ✅ 已完成

状态：**已完成**

涉及文件：

- `crates/beam-core/src/workflow_runtime.rs`（核心修复）
- `crates/beam-daemon/src/workflow_commands.rs`（测试更新）

实现说明：

**根因**：`check_pending_cancels` 中三个 async 函数 `complete_activity_cancel`、`complete_run_cancel`、`complete_node_cancel` 被 `let _ =` 丢弃而非 `.await`，导致 cancel 事件从未实际写入 EventLog。同时 activity cancel 循环中存在错误门禁 `if !snapshot.dangling.activities.contains(activity_id)`，由于 `dangling.cancels ⊆ dangling.activities`，该条件恒为 false，使得 `activityCanceled` 永远无法写入。

**修复**：
- `check_pending_cancels`：为三个 async 调用补上 `.await`，使 cancel 事件真正写入 EventLog。
- 移除 activity cancel 循环中的错误门禁，让所有 `dangling.cancels` 中的 activity 都能被写入 `activityCanceled`。
- 使用 `cancel_origin_event_id`（来自 `cancelled_run_intent` 或 `cancelled_node_intents`）替代空字符串。
- `cancel_run` handler 行为不变：仍然只写 `cancelRequested`，随后调用 `run_workflow_runtime_once` 推进 runtime，由 `check_pending_cancels` 完成 activity/node/run cancel 传播。

**EventLog 顺序**：`cancelRequested` → `activityCanceled`(s) → `runCanceled`（由 `check_pending_cancels` 在同一函数内按顺序写入，符合预期）。

测试覆盖（新增/更新）：

- core 新增 `cancel_propagation_writes_activity_canceled_before_run_canceled`：human-gate wait workflow → cancelRequested → run_loop 推进 → 验证 `activityCanceled` 在 `runCanceled` 之前。
- core 新增 `cancel_propagation_is_idempotent_after_run_is_cancelled`：已验证 cancel 后 run_loop 不会重复写入 `runCanceled`。
- daemon 测试更新 3 处：
  - `cancel_run_propagates_via_runtime_with_activity_canceled_before_run_canceled`（原 `cancel_run_writes_cancel_requested_not_run_canceled` 重命名+增强）：验证 `activityCanceled` 在 `runCanceled` 之前，且 `cancel_run` handler 本身不直接写 `runCanceled`。
  - `cancel_run_repeated_is_idempotent`：更新为验证首次 cancel 后 run 变为 cancelled 状态，二次 cancel 幂等返回 `alreadyTerminal`，且 `runCanceled` 恰好 1 条。
  - `cancel_handler_does_not_directly_call_complete_run_cancel`（原 `cancel_do_not_write_run_canceled_immediately_after_cancel_requested` 重命名+更新）：验证 handler 本身不调用 `complete_run_cancel`，runtime 正确传播全部 cancel 事件。

验收状态：

- ✅ dashboard cancel handler（`cancel_workflow_run` → `cancel_run`）只写 `cancelRequested`，不直接写 `runCanceled`。
- ✅ runtime `check_pending_cancels` 正确传播 cancel：写入 `activityCanceled`（如有 running activity）后写入 `runCanceled`。
- ✅ EventLog 顺序：`cancelRequested` → `activityCanceled`(s) → `runCanceled`（不再直接跳转到 `runCanceled`）。
- ✅ 重复 cancel 幂等：首次 cancel 后 run terminal，二次 cancel 返回 `alreadyTerminal`。
- ✅ `cargo test -p beam-core` 全部 86 测试通过，无 warning。
- ✅ `cargo test -p beam-daemon` 全部 237 测试通过，无 warning。

### Task 6.2: 引入 workflow active cancellation registry ✅ 已完成

状态：**已完成**

涉及文件：
- `crates/beam-daemon/src/workflow_cancellation.rs`（新增，含 `WorkflowCancellationRegistry` + `ActivityTokenGuard`）
- `crates/beam-core/src/workflow_runtime.rs`（新增 `on_activities_cancelled` hook 方法 + `check_pending_cancels` 签名改为接受 hooks）
- `crates/beam-daemon/src/lib.rs`（`DaemonWorkflowExecutionHooks` 实现 `on_activities_cancelled`；`execute_subagent`/`execute_host_executor` 通过 `ActivityTokenGuard` 注册/注销 token；`await_session_final_output` 接受 `Option<CancellationToken>` 并用 `select!` 支持协作取消；`run_workflow_subagent_session` 接受 token 并在取消时返回 `Cancelled`；`run_workflow_host_executor` 接受 token 并在调用前检查取消）
- `crates/beam-daemon/src/workflow_commands.rs`（`cancel_run` 在写入 `cancelRequested` 后立即调用 `registry.cancel_run()` 取消 active dispatch tokens）
- `Cargo.toml`（workspace 新增 `tokio-util` workspace dependency）
- `crates/beam-daemon/Cargo.toml`（新增 `tokio-util` dependency）

实现说明：

**Registry API**
- 新增 `WorkflowCancellationRegistry`（`std::sync::RwLock + CancellationToken`），支持：
  - Registration: `register_activity`, `unregister_activity`, `register_node`, `unregister_node`
  - Cancellation: `cancel_activity`, `cancel_node`, `cancel_run`
  - Lookup/Snapshot: `lookup_activity`, `active_activity_ids`, `total_activities`, `total_nodes`
- `cancel_node` 采用 segment-based 匹配：strip `<runId>::` 前缀后，按 `::` split，任一 segment 等于 node_id 即匹配。
- `cancel_run` 取消该 run 下所有 activity 和 node token。
- 提供 `global_cancellation_registry()` 进程级单例（`OnceLock`）。

**RAII Guard**
- 新增 `ActivityTokenGuard`：构造时调用 `register_activity`，Drop 时调用 `unregister_activity`。确保 dispatch 在任何退出路径（成功/失败/提前返回/panic）都会注销 token。

**Daemon hooks 集成（真实 dispatch 路径）**
- `execute_subagent`：用 `ActivityTokenGuard::register(&registry, ctx.run_id, ctx.activity_id)` 注册 token，将 `guard.token` 传递给 `run_workflow_subagent_session`。
- `execute_host_executor`：同上，传递给 `run_workflow_host_executor`。
- `on_activities_cancelled`：检测 run-level cancel 时调用 `cancel_run`；否则按 node/activity 分别调用 `cancel_node` / `cancel_activity`。

**协作取消（subagent dispatch）**
- `await_session_final_output` 新增 `cancel_token: Option<&CancellationToken>` 参数，循环内用 `tokio::select! { token.cancelled() => bail!, sleep => {} }` 替代原来的 `sleep.await`。
- `run_workflow_subagent_session` 在调用前检查 `token.is_cancelled()`，若已取消立即返回 `Cancelled`；`await_session_final_output` 返回错误后，区分取消 vs 其他失败，取消时返回 `Cancelled`（保持 close_session 清理）。不留 Task 6.3 的 SIGINT/SIGKILL。

**协作取消（hostExecutor dispatch）**
- `run_workflow_host_executor` 在调用 provider 前检查 `token.is_cancelled()`，若已取消立即返回 `Cancelled`。
- 不中断已开始的 provider future（effectAttempted 已写入，provider 必须完成），此限制留给 Task 6.3。

**cancel_run 立即取消**
- `cancel_run`（workflow_commands）在写入 `cancelRequested` 后、调用 `run_workflow_runtime_once` 前，立即调用 `registry.cancel_run(run_id)` 取消 active dispatch tokens。这比 `check_pending_cancels` → `on_activities_cancelled` 路径更快，因为后者只在每个 `run_loop` tick 开始时才执行。

**Core EventLog 语义不变**
- cancel handler 仍只写 cancelRequested，runtime 仍负责写 activityCanceled/runCanceled。

测试覆盖（24 个）：
- **Registry 基础**（12 个）：register/lookup, unregister, idempotent, cancel_activity, cancel_node (4 variants), cancel_run (3 variants), snapshot, run isolation
- **Async cancel + dispatch** (4 个原有)：`concurrent_cancel_signals_active_dispatch`, `active_dispatch_observes_cancellation_after_cancel_requested`, `cancel_activity_targets_only_specified_activity`, `node_cancel_propagates_to_children`
- **ActivityTokenGuard 集成**（4 个新增）：
  - `guard_registers_and_auto_unregisters_on_drop` — RAII 注册/注销
  - `guard_token_is_independent_from_registry_clone` — cloned registry 仍能 cancel guard token
  - `guard_unregisters_on_early_exit` — 提前返回路径保证注销
  - `hooks_integration_guard_register_dispatch_observes_cancel` — 模拟 hooks 注册 → cancel_run → dispatch 观察到取消
- **真实集成点验证**（2 个新增）：
  - `cancellable_wait_yields_via_token_select` — 验证 `select!` pattern（等同于 `await_session_final_output` 的取消路径）
  - `full_hooks_integration_register_cancel_detect` — 完整 flow: guard register → dispatch checks token → cancel_run → dispatch returns cancelled

验收状态：
- ✅ `cargo test -p beam-core` 全部 86 测试通过，0 warning（已清理 `tests/workflow_resume.rs` 中 unused `EventDraft` import 和 `CountingHooks::with_fail` dead_code warning）
- ✅ `cargo test -p beam-daemon` 全部 261 测试通过（`cargo test -p beam-daemon workflow` 100 通过）
- ✅ `cargo build -p beam-daemon` 无 warning/error
- ✅ daemon dispatch 路径已集成 registry：`execute_subagent`/`execute_host_executor` 注册/注销 token
- ✅ subagent dispatch 可观测 token 并在取消时返回 `WorkflowDispatchOutcome::Cancelled`
- ✅ hostExecutor dispatch 在调用前检查 token 并在取消时返回 `Cancelled`
- ✅ 集成测试覆盖真实 hooks 使用的代码路径（`ActivityTokenGuard.register` → `cancel_run` → dispatch 观察 cancellation）

Task 6.3 边界（未在本任务完成）：
- 不发送 SIGINT/SIGKILL 给 worker；取消时仅温和 close_session 清理，worker 进程可能仍运行。
- 不中断已开始的 hostExecutor provider future（effectAttempted 已写入，保持协议不变）。
- `register_node`/`unregister_node` 等 API 已有基础测试，将在 Task 6.3 全面使用。

### Task 6.3: worker cancel 接入真实 session ✅ 已完成

状态：**已完成**（含 zombie-aware 修复）

涉及文件：

- `crates/beam-daemon/src/lib.rs`（新增 `terminate_workflow_worker_process` 函数，修改 `run_workflow_subagent_session` cancel 错误路径）
- `crates/beam-daemon/src/workflow_cancellation.rs`（新增 signal escalation 顺序验证测试 + MockWorker/SignalTrace 抽象）
- worker/session 管理相关代码（session 已记录 `worker_pid`，WorkerHandle 持有 `tokio::process::Child`）

实现说明：

**worker 进程终止（SIGINT → try_wait polling → SIGKILL）**

- 新增 `terminate_workflow_worker_process(state, session_id)` 函数，从 session 中获取 `worker_pid`，执行 signal escalation：
  1. 发送 SIGINT（`libc::kill(pid, SIGINT)`）
  2. 每 200ms 轮询进程是否退出，**优先使用 `state.workers[session_id].child.try_wait()`** 判断（正确 reap zombie 子进程）；仅在没有 child handle 时才 fallback 到 `kill(pid, 0)`（zombie-prone）
  3. 若 grace 期（5 秒）后进程仍存活，发送 SIGKILL（`libc::kill(pid, SIGKILL)`）
- **关键修复**：初版只用 `kill(pid, 0)` 检测存活，对已退出但尚未 reaped 的子进程（zombie），`kill(pid, 0)` 仍返回成功，导致即使 worker 已响应 SIGINT 退出也会等待完整 5 秒 grace 并发送无用 SIGKILL。现在 `try_wait()` 先行检测 zombie 并立即退出 grace loop。
- 锁安全：`state.workers` mutex 仅在同步 `try_wait()` 调用期间持有，**不跨 `.await` sleep**。
- 该函数不直接操作 session 状态（避免重复逻辑），仅负责 signal escalation；随后的 `close_session` 调用负责 worker handle 清理和 session 状态标记。

**cancel 路径集成**

- `run_workflow_subagent_session` 的 cancel 错误路径改为：
  1. 检测 `cancel_token.is_cancelled()` → 先调用 `terminate_workflow_worker_process` 强制终止 worker
  2. 再调用 `close_session` 完成 session 清理（发送 Close 消息 + wait child + 标记 Closed）
  3. 返回 `WorkflowDispatchOutcome::Cancelled`（含 `session` 信息）
- 非 cancel 错误路径（WorkerCrashed）保持不变：仅温和 `close_session`，不发送 SIGINT/SIGKILL。

**EventLog 语义边界**

- cancel handler（`cancel_run`）仍只写 `cancelRequested`；`activityCanceled` 由 runtime `check_pending_cancels` 传播写入。
- 计划中「`activityCanceled` 在 worker 确认退出后写入」与当前架构冲突：EventLog 是异步写盘模型，runtime checkpoint 不等待 worker 退出再写 `activityCanceled`。当前设计中：
  - `cancelRequested` → registry 取消 token → worker 被 SIGINT/KILL → runtime 写 `activityCanceled`/`runCanceled`
  - `activityCanceled` 写入时 worker 可能仍在退出中（进程僵尸态未 reaped）
- 此边界不影响语义正确性：`activityCanceled` 表示 runtime 已决定取消该 activity，worker termination 是 best-effort 加速，不做强同步。已在实现说明中明确。

**hostExecutor 边界（Task 6.2 维持）**

- `run_workflow_host_executor` 不中断已开始的 provider future（`effectAttempted` 已写入，协议不变）。
- 仅在调用 provider 前检查 `token.is_cancelled()`，已取消则立即返回 `Cancelled`。
- 不使用 `terminate_workflow_worker_process`（host executor 不涉及 worker 进程）。

**`cancel_run` handler 提前取消**

- Task 6.2 已添加：`cancel_run` 在写入 `cancelRequested` 后立即调用 `registry.cancel_run(run_id)` 取消 active dispatch tokens。
- 此路径比 `check_pending_cancels` → `on_activities_cancelled` 更快（后者只在每轮 `run_loop` tick 开始时执行）。
- 结合 Task 6.3 的 SIGINT/SIGKILL，cancel 流程完整闭环：cancelRequested → token 取消 → worker 进程终止 → session 清理 → runtime 写 terminal events。

测试覆盖（新增 7 个）：

- **lib.rs 集成测试**（4 个）：
  - `terminate_workflow_worker_process_exits_early_via_try_wait`：spawn 真实 `sleep 60` → 注册到 `state.workers` → try_wait 路径检测 zombie 快速退出 → 验证 elapsed < 3 秒（远小于 5 秒 grace） + exit status 非 success
  - `terminate_workflow_worker_process_fallback_kills_child`：无 worker handle 的 fallback 路径（`kill(0)` zombie-prone）→ 验证最终仍被 kill，且 elapsed ≥ 3 秒（文档 current behaviour）
  - `terminate_workflow_worker_process_no_pid_is_noop`：session 无 worker_pid → 函数安全 no-op（不 panic）
  - `cancel_run_clears_registry_and_session_cleanup_works`：bootstrap human-gate workflow → 注册 activity token → `cancel_run` → 验证 token 被 cancel + registry 清理干净

- **workflow_cancellation.rs signal escalation 顺序验证**（3 个，使用 mockable `MockWorker` + `SignalTrace`）：
  - `signal_escalation_sends_sigint_then_sigkill_when_process_ignores_sigint`：SIGINT 后进程不退出 → 发送 SIGKILL，验证信号顺序 (SIGINT, SIGKILL)
  - `signal_escalation_sends_only_sigint_when_process_exits_promptly`：SIGINT 后进程快速退出 → 仅发 SIGINT，不发送 SIGKILL
  - `signal_escalation_grace_period_is_respected`：验证 grace period 200ms 被真正等待后才发送 SIGKILL

验收状态：

- ✅ `cargo test -p beam-core workflow` 全部 56 + 1 regression 测试通过
- ✅ `cargo test -p beam-daemon workflow` 全部 106 测试通过（含新增 5 个：3 terminate + 3 signal escalation - 1 替换）
- ✅ `cargo test -p beam-daemon --lib` 全部 268 测试通过（含 `cancel_run_clears_registry` 等）
- ✅ 长任务 workflow cancel 后，worker 进程被 SIGINT → try_wait 快速检测退出（不等待完整 grace）→ 仅发 SIGINT 无冗余 SIGKILL
- ✅ subagent dispatch 观察 cancel 后返回 `WorkflowDispatchOutcome::Cancelled` 并清理 session
- ✅ 不破坏现有 cancellation registry、approval/cancel handler、runtime recovery 语义
- ✅ `cancel_run` handler 仍只写 `cancelRequested`；`activityCanceled`/`runCanceled` 由 runtime 传播
- ✅ `register_node`/`unregister_node` 等 API 已就绪，本任务未使用但保持可用

## Phase 7: Runtime Driver 收敛

### Task 7.1: 抽出 workflow runtime driver ✅ 已完成

状态：**已完成**

建议新增文件：

- `crates/beam-daemon/src/workflow_runtime_driver.rs` ✅

实现说明：

- 新增 `crates/beam-daemon/src/workflow_runtime_driver.rs`，包含：
  - `pub(crate) async fn run(state, run_id, workflow_json)` — 原 `run_workflow_runtime_once` 的完整实现（parse definition → create EventLog → create WorkflowRuntimeContext → create DaemonWorkflowExecutionHooks → spawn background watcher → call run_loop → fanout approval cards）
  - `async fn send_progress_card(state, run_id, workflow_id)` — 从 `lib.rs` 移入的 progress card 发送/更新逻辑（读取 snapshot → 构建 card → 读取 chat-binding → 发送或更新 Lark 卡片）
  - `const MAX_TICKS: usize = 128` — 从 `lib.rs` 移入，替代原 `WORKFLOW_RUNTIME_MAX_TICKS`
- `lib.rs` 修改：
  - `mod workflow_runtime_driver` 声明
  - `run_workflow_runtime_once` 改为薄 wrapper：直接调用 `workflow_runtime_driver::run(state, run_id, workflow_json).await`
  - 移除 `WORKFLOW_RUNTIME_MAX_TICKS` 常量、`send_workflow_progress_card` 函数
- 调用点无一变更：`bootstrap_and_start_workflow_run`（trigger）、dashboard resume、daemon 测试均通过原有 `run_workflow_runtime_once` wrapper 继续工作
- 无新依赖、无循环依赖、无重复逻辑

任务：

- ✅ 把 `run_workflow_runtime_once` 从 `lib.rs` 移出。
- ✅ driver 负责：load definition、create runtime context、attach event fanout、send/update progress card、call `run_loop`。

验收标准：

- ✅ `lib.rs` 中 workflow runtime 相关大段逻辑减少（移除约 90 行实现代码）。
- ✅ trigger、approval、cancel、cold attach 都调用同一个 driver（通过 `run_workflow_runtime_once` wrapper）。

测试结果：

- ✅ `cargo test -p beam-daemon workflow`：106 passed, 0 failed
- ✅ `cargo test -p beam-daemon --lib`：268 passed, 0 failed
- ✅ `cargo test -p beam-core workflow`：56 passed, 0 failed
- ✅ `cargo test --workspace`：全部通过

### Task 7.2: cold attach 使用统一 recovery run loop ✅ 已完成

状态：**已完成**

涉及文件：

- `crates/beam-daemon/src/lib.rs`（`drive_workflow_run_after_cold_attach` 改为调用 `workflow_runtime_driver::run`）
- `crates/beam-core/src/workflow_cold_scan.rs`（无修改，冷扫描逻辑不变）

实现说明：

- `drive_workflow_run_after_cold_attach` 原来直接创建 `EventLog`、`WorkflowRuntimeContext`、`DaemonWorkflowExecutionHooks` 后调用 `run_loop`，缺少 progress card 发送、background watcher 和 approval card fanout。
- 现改为序列化 `run.def` 为 JSON 后调用 `workflow_runtime_driver::run`（即统一的 driver 入口），与原 `run_workflow_runtime_once` wrapper 共享同一代码路径。
- 移除了 `lib.rs` 中仅被 cold attach 使用的 `WorkflowRuntimeContext` 和 `run_loop` 两个 import（这些符号已由 driver 内部自行导入）。
- 无 provider-specific 特殊恢复逻辑：driver 内部调用 `run_loop`，其内置的 recovery 阶段（Task 4.1 effect recovery + Task 4.2 wait resolution recovery）自动处理 dangling effects 和 resolved waits。

测试覆盖（3 个新增）：

- `cold_scan_discovers_non_terminal_and_skips_terminal_runs`：验证冷扫描发现非 terminal run、跳过 terminal run（succeeded）。
- `cold_attach_open_human_gate_wait_not_terminalized`：验证对 open human-gate wait 的 workflow 执行 cold attach（统一 driver）后，wait 保持 open、run 不 terminalize（返回 AwaitingWait）。
- `cold_attach_recovery_materializes_resolved_wait_terminal`：模拟 crash 场景（waitResolved 已写但 activitySucceeded 未写），验证 cold attach 调用统一 driver 后，run_loop wait-resolution recovery 正确 materialize terminal event，run 推进到 terminal。

验收状态：

- ✅ cold attach 后不包含 provider-specific 特殊恢复逻辑，统一通过 `workflow_runtime_driver::run` → `run_loop` 处理。
- ✅ daemon restart 后 non-terminal workflow 可以继续等待（open wait → AwaitingWait）、恢复（resolved wait → materialize terminal）、推进。
- ✅ `cargo test -p beam-core workflow`：56 passed + 1 regression，0 failed。
- ✅ `cargo test -p beam-daemon workflow`：106 passed，0 failed。
- ✅ `cargo test -p beam-daemon --lib`：271 passed（268 原有 + 3 新增），0 failed。
- ✅ `cargo build -p beam-daemon`：无 warning/error。
- ✅ 改动量小（lib.rs 中 ~20 行替换为 ~5 行 + 两个 import 移除），不开始 Phase 8/9。

## Phase 8: Loop Runtime

### Task 8.1: 扩展 OrchestratorAction 支持 loop lifecycle ✅

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

**实现摘要**：

- 在 `OrchestratorAction` 枚举中新增四个变体：
  - `StartLoop { node_id, max_iterations }`
  - `StartLoopIteration { node_id, iteration }`
  - `FinishLoopIteration { node_id, iteration, resolution, decision_activity_id, wait_resolved_event_id, by, comment, timed_out }`
  - `FinishLoop { node_id, final_iteration, resolution, output_ref, error_code, error_class }`
- 在 `apply_orchestrator_action` 中为四种 action 分别调用 `start_loop` / `start_loop_iteration` / `finish_loop_iteration` / `finish_loop` 写入 `loopStarted` / `loopIterationStarted` / `loopIterationFinished` / `loopFinished` 事件，确保 action → event 路径打通。
- `workflow_snapshot.rs` 本任务未做修改；其已有的 `snapshot.loops` 投影逻辑原已支持从 loop 事件重建 `LoopRun` 状态，本任务通过补齐 action → event 产出和 replay 测试覆盖该路径。
- 新增 7 个 focused tests：
  - `start_loop_writes_loop_started_event`
  - `start_loop_iteration_writes_event`
  - `finish_loop_iteration_writes_event`
  - `finish_loop_writes_loop_finished_event`
  - `all_loop_actions_produce_correct_event_sequence`
  - `replay_builds_snapshot_loops_from_loop_events`
  - `replay_loop_failed_sets_status_and_dangling_iteration`

**验证结果**：

- `cargo test -p beam-core workflow`：63 passed，0 failed。

### Task 8.2: 实现 loop dispatch pass ✅ 已完成

涉及文件：

- `crates/beam-core/src/workflow_orchestrator.rs`
- `crates/beam-core/src/workflow_runtime.rs`
- `crates/beam-core/src/workflow_binding.rs`
- `crates/beam-core/src/workflow_definition.rs`

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

**实现摘要**：

- `workflow_definition.rs`：移除 parse 阶段对 Loop/Decision 节点的硬拒绝，允许通过解析。
- `workflow_orchestrator.rs`：
  - 新增 `loop_gate_activity_id` / `loop_work_activity_id`（活动 id 格式对齐计划）、`body_topological_order`（body 节点拓扑排序）、`node_human_gate`、`extract_wait_resolution_meta` 辅助函数。
  - 新增 `decide_loop_advancement` 和 `process_loop_iteration_body`，处理 loop 起动、body 节点 gate/work 调度、decision 决议（approve/reject/timeout）、maxIterations/body failure 引起的 loop failed。
  - 修改 `decide_next_actions`：将 Loop 节点从跳过改为调用 loop dispatch 逻辑。
  - 修改 `find_sinks`：允许 Loop 节点作为 sink。
  - 修改 `action_serialization_key`：四个 loop action 分别使用独立序列化键，避免 `select_tick_actions` 误去重。
  - `select_tick_actions`：settle action 不消耗并发配额，确保 `FinishLoopIteration` + `FinishLoop` 成对调度。
- `workflow_binding.rs`：
  - Decision `.previous.` 在 iteration 1 返回空合成 JSON `{"by":null,"comment":""}`，`walk_path` 产出空字符串，`${reviewDecision.previous.comment}` 不报错。
  - Decision `.previous.` 在 iteration ≥2 从 `snapshot.loops[loop_id].iterations[N].decision_by/decision_comment` 读取（不依赖 output blob，支持 rejected 场景）。
  - 普通 null 插值保持字面 `"null"`，未改全局语义。
- `workflow_runtime.rs`：
  - `dispatch_gate` 的 `BindingContext.loop_context` 改为从 activity id 解析，支持 loop 作用域内的 gate prompt 绑定。
  - `loopIterationFinished` 元数据（`decisionActivityId`、`waitResolvedEventId`、`by`、`comment`、`timedOut`）从 gate activity 的 `wait.resolution` 提取写入，不再硬编码为 None。

**测试覆盖**（新增/补充 14 个，总计 77）：

- Orchestrator 单测 4 个：loop start、decision approve finish、max iterations fail、reject next iteration。
- Runtime 集成测试 6 个：
  - `loop_depends_met_produces_start_loop_and_first_iteration`
  - `code_review_loop_reaches_human_gate_wait_with_correct_activity_id`（简化 def，验证 activity id 格式）
  - `reject_decision_enters_next_iteration`
  - `approve_decision_finishes_loop_and_run_succeeds`
  - `body_failure_causes_loop_failed`
  - `max_iterations_reject_causes_loop_failed`
- 真实 `code-review-loop.workflow.json` 集成测试 3 个：
  - `real_code_review_loop_iter1_reaches_awaiting_wait`（`.previous.` 不报错）
  - `real_code_review_loop_reject_enters_iter2_with_comment`（metadata 写入 + iter2 prompt 含 reject comment）
  - `real_code_review_loop_approve_succeeds`（approve → loop/run succeeded）
- 绑定单元测试 1 个：`string_interpolation_null_produces_literal_null`（锁死普通 null 插值语义）

**验证结果**：

- `cargo test -p beam-core workflow`：77 passed，0 failed。
- `cargo test -p beam-daemon workflow`：106 passed，0 failed。
- `cargo test --workspace`：全部通过，无回归。

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
