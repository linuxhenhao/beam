# lib.rs 拆分重构工作总结

## 概要

将 `crates/beam-daemon/src/lib.rs` 从 **21,145 行** 的单体文件拆分为 **48 个模块文件**，非测试代码从 ~13,400 行缩减到 ~968 行（减少 93%）。后续又将堆积在 lib.rs 中的 **196 个测试**按被测函数归位到 13 个模块文件，lib.rs 最终缩减至 **2,663 行**（减少 87%）。

## 最终关键数字

| 指标 | 重构前 | 模块拆分后 | 测试拆分后 |
|------|:-----:|:-----:|:-----:|
| lib.rs 总行数 | 21,145 | 8,696 | **2,663** |
| lib.rs 非测试行数 | ~13,400 | ~968 | ~970 |
| lib.rs 测试行数 | ~7,723 | ~7,723 | ~1,693 |
| lib.rs 测试函数数 | 215 | 215 | **33** |
| 模块源文件数 | 29 | 48 | **49** |
| 编译 warning | 10 | 0 | **0** |
| 测试通过数 | 766 | 766 | **766** |

## 变更文件清单

### 新增文件（24 个，全部位于 `crates/beam-daemon/src/`）

| 文件 | 行数 | 说明 |
|------|:---:|------|
| `lark_ingress.rs` | 4,802 | Lark 事件入口、卡片 action 处理、工作流/会话操作 |
| `lark_parse.rs` | 885 | 消息解析、卡片 action 解析、类型定义 |
| `lark_dispatch.rs` | 1,777 | 事件路由、多 bot 门控、调度决策 |
| `lark_card_builders.rs` | 548 | 卡片构建（toast、TUI prompt、审批卡） |
| `lark_delivery.rs` | 794 | 卡片发送/更新/删除、frozen card 生命周期 |
| `lark_history.rs` | 473 | Lark 聊天历史拉取与解析 |
| `lark_identity.rs` | 430 | Bot 身份识别、多 bot 协作 |
| `lark_replies.rs` | 370 | 回复文案构建 |
| `lark_security.rs` | 330 | Lark 签名验证、token 校验、去重 |
| `lark_session_cards.rs` | 287 | Session 卡片创建/刷新、路由决策 |
| `lark_api_helpers.rs` | 244 | Lark API 辅助函数（发送消息、卡片） |
| `connector_runtime.rs` | 682 | Webhook connector 运行时 |
| `dashboard_support.rs` | 328 | Dashboard 鉴权、bot 观察记录 |
| `final_output.rs` | 915 | Final output 卡片、pending response 管理 |
| `session_cards.rs` | 1,369 | Session 可写/只读卡片、display mode |
| `session_creation.rs` | 814 | Session 创建逻辑 |
| `daemon_types.rs` | 388 | 所有类型定义（38 个 struct/enum/const） |
| `worker_lifecycle.rs` | 590 | Worker 进程生命周期管理 |
| `persistence.rs` | 95 | 配置/session/事件持久化 |
| `utils.rs` | 78 | 工具函数（sha256、expand_tilde 等） |
| `route_handlers.rs` | 1,067 | API 路由处理器 |
| `workflow_approval_cards.rs` | 36 | Workflow 审批卡片持久化 |
| `zellij_adopt.rs` | 544 | Zellij session 发现与 adopt |
| `tests/test_helpers.rs` | ~400 | 公共测试基础设施（temp_paths、mock server 等） |

### 修改文件（16 个）

| 文件 | 变更 |
|------|------|
| `lib.rs` | -18,482 行：最终仅保留 ~970 行代码 + ~1,693 行跨模块集成测试（33 个） |
| `card_i18n.rs` | 删除未使用的 `markdown()` 函数 |
| `connector_store.rs` | 删除未使用的 `new_connector_id()` 函数 |
| `lark_parse.rs` | +393 行：追加 `#[cfg(test)] mod tests`（16 个测试） |
| `lark_dispatch.rs` | +1,488 行：追加 `#[cfg(test)] mod tests`（48 个测试） |
| `lark_ingress.rs` | +832 行：追加 `#[cfg(test)] mod tests`（31 个测试） |
| `session_cards.rs` | +759 行：追加 `#[cfg(test)] mod tests`（28 个测试） |
| `lark_card_builders.rs` | +265 行：追加 `#[cfg(test)] mod tests`（7 个测试） |
| `lark_delivery.rs` | +479 行：追加 `#[cfg(test)] mod tests`（16 个测试） |
| `final_output.rs` | +283 行：追加 `#[cfg(test)] mod tests`（9 个测试） |
| `session_creation.rs` | +537 行：追加 `#[cfg(test)] mod tests`（11 个测试） |
| `workflow_commands.rs` | +245 行：追加 `#[cfg(test)] mod tests`（3 个测试） |
| `lark_identity.rs` | +56 行：追加 `#[cfg(test)] mod tests`（2 个测试） |
| `lark_security.rs` | +72 行：追加 `#[cfg(test)] mod tests`（3 个测试） |
| `lark_replies.rs` | +218 行：追加 `#[cfg(test)] mod tests`（3 个测试） |
| `zellij_adopt.rs` | +120 行：追加 `#[cfg(test)] mod tests`（5 个测试） |

## 执行阶段

### 阶段 0（本 session 之前）：初始拆分
- 将 lib.rs 拆出 15 个模块：`connector_runtime`、`dashboard_support`、`final_output`、`lark_delivery`、`lark_history`、`lark_identity`、`lark_ingress`、`lark_replies`、`lark_security`、`lark_session_cards`、`route_handlers`、`session_cards`、`session_creation`、`workflow_approval_cards`、`zellij_adopt`
- 添加模块声明和 `pub(crate) use` 重导出
- lib.rs 从 21,145 → 10,239 行

### 阶段 1（本 session）：清理重叠代码 + 修复回归

**修复 bug：**
- `lark_ingress.rs` 中 `parse_lark_card_action` 的 select_static option 路径从 `/action/value` 修正为 `/action/option`

**清理操作：**
- 删除 lib.rs 中 `now_iso`、`COMPLETED_REACTION_EMOJI_TYPE` 等死代码
- 将 frozen card 持久化函数从 lib.rs 移入 `lark_delivery.rs`
- 将 pending_response_patch_marker 函数从 lib.rs 移入 `final_output.rs`
- 消除 lib.rs 与模块之间的代码重复

### 阶段 2：拆分 `lark_ingress.rs`

将 5,022 行的 `lark_ingress.rs` 拆为 3 个模块：
- **`lark_parse.rs`**：纯解析层（类型 + 消息解析 + 卡片 action 解析）
- **`lark_dispatch.rs`**：路由调度层（事件路由、多 bot 门控）
- **`lark_card_builders.rs`**：卡片构建层（toast、TUI prompt、审批卡）

### 阶段 3：拆分 lib.rs 剩余代码

创建 5 个新模块：
- **`daemon_types.rs`**：38 个 struct/enum/const 类型定义
- **`worker_lifecycle.rs`**：Worker 进程生命周期管理
- **`persistence.rs`**：配置/session/事件持久化
- **`utils.rs`**：工具函数
- **`lark_api_helpers.rs`**：Lark API 辅助函数

### 阶段 4：清理 warning

删除 4 个未使用函数，添加 `#[allow(dead_code)]` 标记。最终 0 warning。

### 阶段 5：测试拆分（2026-07-04）

**创建公共测试基础设施：**
- 新建 `crates/beam-daemon/src/tests/test_helpers.rs`，提取 12 个公共辅助函数（`temp_paths`、`make_bot`、`make_session`、`make_state`、`start_mock_lark_server` 等）

**按分组搬迁测试到 13 个模块：**

| 分组 | 目标模块 | 测试数 | 被测函数 |
|------|---------|:-----:|---------|
| 1 | `lark_parse.rs` | 16 | `parse_lark_card_action`、`classify_lark_text_action`、`parse_term_action_key`、`parse_special_keys` |
| 2 | `lark_dispatch.rs` | 48 | `decide_lark_dispatch`、`decide_lark_routing`、`decide_multibot_inbound_gate`、`validate_resume_target` 等 |
| 3 | `lark_ingress.rs` | 31 | `parse_lark_inbound_message`、`parse_force_topic_invocation`、`normalize_lark_ws_card_action` 等 |
| 4 | `session_cards.rs` | 28 | `build_streaming_card_*`、`build_writable_session_card_*`、`toggle_display_*` 等 |
| 4B | `lark_card_builders.rs` | 7 | `build_tui_prompt_card_*`、`build_lark_card_action_toast_*` 等 |
| 5 | `lark_delivery.rs` | 16 | `decide_lark_card_delivery`、`partition_frozen_cards_for_recall`、`pending_response_*` 等 |
| 6 | `final_output.rs` | 9 | `final_output_delivery_*`、`final_output_retry_*`、`resolve_tui_prompt_final_text` 等 |
| 7 | `session_creation.rs` | 11 | `build_direct_create_session_spec_from_bot_*`、`attempt_resume_*` 等 |
| 8 | `workflow_commands.rs` | 3 | `parse_workflow_text_command_*`、`prepare_retry_last_task_*` |
| 9 | `lark_identity.rs` + `lark_security.rs` | 5 | `peer_bot_open_ids_*`、`operate_permission_*`、`lark_signature_*` |
| 10 | `lark_replies.rs` + `lark_ingress.rs` | 5 | `lark_reply_message_*`、`user_notify_*`、`chat_mode_from_str_*` |
| 11 | `zellij_adopt.rs` | 5 | `zellij_adopt_candidates_*`、`reconcile_restored_sessions_*` 等 |
| **留存** | `lib.rs` | **33** | 跨模块集成测试（Lark history API、dir_select、grant、webhook、cold scan、workflow catalog 等） |

**最终状态：0 warning，766 tests passed（beam-daemon: 462），0 failed。**

## 当前 lib.rs 结构（2,663 行）

```
lines 1-52:   mod 声明（48 个模块 + test_helpers）
lines 54-85:  pub(crate) use 重导出
lines 87-155: use 导入（供 run() 使用）
lines 156-972: pub async fn run()  ← daemon 主入口函数
lines 973-2663: pub(crate) mod tests { ... } ← 33 个跨模块集成测试
```

## 已完成待办

- [x] **测试拆分**：将 lib.rs 中的 196 个测试按被测函数归位到各模块文件末尾
- 详见：`docs/plans/2026-07-04-lib-rs-test-split-plan.md`

---

*生成时间：2026-07-04*
*最后更新：2026-07-04（阶段 5 测试拆分完成）*
