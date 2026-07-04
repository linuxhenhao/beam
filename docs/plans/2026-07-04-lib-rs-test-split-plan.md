# lib.rs 测试拆分计划

## 背景

`crates/beam-daemon/src/lib.rs` 经过之前拆分重构，非测试代码已从 ~13,400 行缩减到 ~968 行（仅剩 imports + `pub async fn run()`）。但 `mod tests { ... }` 仍有 **7,723 行、184 个测试函数**，全部堆积在 lib.rs 末尾。

这些测试对应的是已被拆分到各个模块里的函数（如 `lark_parse.rs`、`lark_dispatch.rs`、`session_cards.rs` 等），按 Rust 惯例应该归位到各自模块的 `#[cfg(test)] mod tests { ... }` 中。

## 目标

1. 将 lib.rs 中的 184 个测试函数按其所测函数所在的模块，移动到对应模块文件末尾
2. lib.rs 的 `mod tests` 最终只保留**跨模块工具类/辅助类测试**（如 `temp_paths`、mock server、通用 helpers）
3. lib.rs 最终缩减到 ~2,000 行左右（968 行代码 + ~1,000 行剩余测试）

## 当前已完成状态

- ✅ 编译: 0 warning, 0 error
- ✅ 测试: 766 passed, 0 failed
- ✅ 模块拆分: 48 个源文件，结构清晰
- ✅ lib.rs 当前 8,647 行（968 代码 + 7,679 测试行号 974-EOF）

## 测试 → 模块归位映射

### 测试辅助函数（留在 lib.rs tests 中）

这些是所有测试共享的基础设施，需要保留或复制：

```
temp_paths(label) -> BeamPaths          ← 路径工具，所有测试用
maybe_remove_dir(path)                  ← 路径清理
lark_base_url_env_lock()                ← Lark 测试环境
LarkBaseUrlEnvGuard                     ← Lark 测试环境
start_mock_lark_server() -> String      ← mock 服务器
make_test_bot_config()                  ← 测试用 bot 配置
make_test_session(...) -> Session       ← 测试用 session 构造
make_test_app_state(...) -> AppState    ← 测试用 AppState 构造
```

处理方式：
- `temp_paths`、`maybe_remove_dir` → 复制到每个需要它们的模块（或提取到 test_helpers.rs）
- `lark_base_url_env_lock`、`LarkBaseUrlEnvGuard` → 复制到 Lark 相关测试文件
- mock server 函数 → 复制到需要 mock 的测试文件

**最终方案**：创建公共测试辅助文件，避免复制粘贴：
- 在 lib.rs 的 `mod tests` 中添加 `mod test_helpers;`
- 创建 `crates/beam-daemon/src/test_helpers.rs`（`#[cfg(test)]` 条件编译）

### 分组 1 → `lark_parse.rs`（卡片+消息解析测试，~15 tests）

这些测试的核心被测函数现在位于 `lark_parse.rs`：

| 测试函数 | 被测函数 |
|----------|---------|
| `parse_lark_card_action_extracts_resume_payload` | `parse_lark_card_action` |
| `parse_lark_card_action_accepts_operator_id_open_id` | `parse_lark_card_action` |
| `parse_lark_card_action_extracts_visibility` | `parse_lark_card_action` |
| `parse_lark_card_action_extracts_workflow_payload` | `parse_lark_card_action` |
| `parse_lark_card_action_extracts_dir_search_keyword_from_form_value` | `parse_lark_card_action` |
| `parse_lark_card_action_dir_search_keyword_none_when_no_form_value` | `parse_lark_card_action` |
| `parse_lark_card_action_rejects_missing_action` | `parse_lark_card_action` |
| `parse_lark_card_action_serializes_object_value_for_raw_payload` | `parse_lark_card_action` |
| `parse_lark_card_action_extracts_select_static_option` | `parse_lark_card_action` |
| `parse_lark_card_action_select_static_option_falls_back_to_value` | `parse_lark_card_action` |
| `parse_lark_card_action_rejects_malformed_select_static_option` | `parse_lark_card_action` |
| `parse_lark_card_action_extracts_tui_prompt_fields` | `parse_lark_card_action` |
| `parse_special_keys_accepts_array_and_stringified_json` | `parse_special_keys` |
| `parse_term_action_key_maps_supported_values` | `parse_term_action_key` |
| `classify_lark_text_action_identifies_all_commands` | `classify_lark_text_action` |
| `classify_lark_text_action_routes_commands_and_session_reuse` | `classify_lark_text_action` |

### 分组 2 → `lark_dispatch.rs`（路由+调度测试，~31 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `decide_lark_event_outcome_blocks_re_adopt_when_session_already_adopted` | `decide_lark_event_outcome` |
| `decide_lark_event_outcome_reflects_existing_session_state` | `decide_lark_event_outcome` |
| `decide_lark_dispatch_*` 全部 13 个 | `decide_lark_dispatch` |
| `decide_lark_routing_*` 全部 11 个 | `decide_lark_routing` |
| `decide_multibot_inbound_gate_*` 全部 8 个 | `decide_multibot_inbound_gate` |
| `evaluate_lark_preflight_handles_dedupe_empty_and_permission_gate` | `evaluate_lark_preflight` |
| `evaluate_talk_denies_unknown_sender_with_strict_bot` | `evaluate_talk_for_bot` |
| `handle_lark_event_uses_api_to_detect_topic_group` | `handle_lark_event` |
| `validate_resume_target_*` 全部 6 个 | `validate_resume_target` |
| `resolve_lark_card_action_session_id_*` 2 个 | `resolve_lark_card_action_session_id` |
| `session_for_lark_anchor_*` 2 个 | `session_for_lark_anchor` |
| `session_anchor_matches_*` 2 个 | `session_anchor_matches` |

### 分组 3 → `lark_ingress.rs`（消息解析测试，~15 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `parse_lark_inbound_message_*` 4 个 | `parse_lark_inbound_message` |
| `parse_force_topic_invocation_*` 6 个 | `parse_force_topic_invocation` |
| `parse_chat_info_mode_*` 4 个 | `parse_chat_info_mode` |
| `parse_feishu_resume_input_routes_send_and_reply_variants` | （路由解析） |
| `resolve_and_strip_leading_mentions_supports_lark_placeholder_keys` | `resolve_lark_mentions` |
| `strip_leading_mentions_*` 2 个 | `strip_leading_mentions` |
| `normalize_lark_ws_card_action_*` 7 个 | `normalize_lark_ws_card_action` |
| `ws_card_action_handler_*` 2 个 | card action handler |
| `lark_event_dedupe_key_skips_empty_ids` | `lark_event_dedupe_key` |
| `lark_message_withdrawn_helpers_recognize_code_230011` | `is_lark_message_withdrawn_payload` |

### 分组 4 → `session_cards.rs` + `lark_card_builders.rs`（卡片构建测试，~30 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `build_streaming_card_*` 10 个 | `build_writable_session_card`/`build_readonly_link_card` |
| `build_writable_session_card_*` 2 个 | `build_writable_session_card` |
| `build_readonly_link_card_*` 1 个 | `build_readonly_link_card` |
| `build_tui_prompt_card_*` 2 个 | `build_tui_prompt_card` |
| `build_workflow_approval_resolved_card_*` 1 个 | `build_workflow_approval_resolved_card` |
| `build_closed_session_card_contains_resume_button_and_command` | `build_closed_session_card` |
| `build_lark_card_action_toast_shapes_expected_payload` | `build_lark_card_action_toast` |
| `build_contextual_reply_card_supports_adopt_preamble_shape` | `build_contextual_reply_card` |
| `build_final_output_card_*` 2 个 | `build_final_output_card` |
| `build_follow_up_content_*` 3 个 | （follow-up 内容） |
| `build_quote_hint_*` 3 个 | （quote hint） |
| `build_report_post_content_mentions_owner_and_preserves_line_breaks` | `build_report_post_content` |
| `build_adopt_helpers_render_stable_replies` | （adopt replies） |
| `build_export_text_reply_handles_empty_and_truncates_long_output` | （export text） |
| `streaming_card_template_matches_expected_status_colors` | `streaming_card_template` |
| `screen_status_card_label_matches_worker_statuses` | `screen_status_card_label` |

### 分组 5 → `lark_delivery.rs`（卡片投递+生命周期测试，~20 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `decide_lark_card_delivery_distinguishes_not_ready_post_and_patch` | `decide_lark_card_delivery` |
| `private_card_delivery_uses_ephemeral_for_group_only` | `private_card_delivery` |
| `resolve_card_render_target_patches_clicked_legacy_card_only` | `resolve_card_render_target` |
| `resolve_private_card_audience_prefers_owner_and_dedupes_allowed_users` | `resolve_private_card_audience` |
| `is_stale_stream_card_action_rejects_mismatched_nonce_only_for_live_card_actions` | `is_stale_stream_card_action` |
| `stale_stream_card_action_self_heal_is_toggle_only` | `stale_stream_card_action_*` |
| `pending_response_state_tracks_open_and_patched_cards` | pending response |
| `pending_response_patch_guard_does_not_close_newer_card` | pending response |
| `pending_response_patch_marker_only_matches_same_card_when_patched` | pending response |
| `clear_pending_response_tracking_resets_all_pending_fields` | `clear_pending_response_tracking` |
| `claim_pending_response_card_requires_open_state` | `claim_pending_response_card` |
| `partition_frozen_cards_for_recall_*` 3 个 | `partition_frozen_cards_for_recall` |
| `load_clicked_frozen_card_only_returns_stale_snapshot` | `load_clicked_frozen_card` |
| `park_stream_card_*` 2 个 | `park_stream_card` |

### 分组 6 → `final_output.rs`（final output 测试，~8 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `final_output_delivery_aborts_for_closed_or_missing_session` | `final_output` |
| `final_output_retry_delay_matches_three_attempt_backoff` | `next_final_output_retry_delay_ms` |
| `worker_final_output_dedupes_by_turn_id_instead_of_content` | final output dedup |
| `resolve_tui_prompt_final_text_prefers_toggled_option_texts` | `resolve_tui_prompt_final_text` |
| `build_feishu_transient_failure_marks_retryable_result` | `FeishuTransientFailure` |
| `build_workflow_resume_response_includes_transient_failures` | workflow resume response |
| `retryable_feishu_resume_error_detects_timeout_and_rate_limit` | `is_retryable_feishu_resume_error` |
| `next_display_mode_toggles_hidden_and_screenshot` | `next_display_mode` |

### 分组 7 → `session_creation.rs`（会话创建/恢复测试，~20 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `session_*` 6 个 | 各种 session 函数 |
| `build_direct_create_session_spec_*` 2 个 | `build_direct_create_session_spec_from_bot` |
| `attempt_resume_*` 多方 5 个 | `attempt_resume_sidecar`/`attempt_resume_wait` |
| `parse_attempt_resume_request_body_accepts_empty_and_rejects_bad_json` | `parse_attempt_resume_request_body` |
| `append_resume_*` 2 个 | resume 相关 |
| `effective_history_scope_defaults_from_session_scope` | `effective_history_scope` |

### 分组 8 → `workflow_commands.rs`（工作流测试，~10 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `parse_workflow_text_command_handles_run_and_cancel` | `parse_workflow_text_command` |
| `workflow_approval_target_message_id_prefers_clicked_message` | `workflow_approval_target_message_id` |
| `prepare_retry_last_task_clears_limit_and_marks_working` | workflow retry |
| ... 其余 workflow 相关测试 | |

### 分组 9 → `lark_identity.rs` + `lark_security.rs`（身份+安全测试，~8 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `peer_bot_open_ids_load_from_known_sources` | `peer_bot_open_ids_for_app` |
| `record_observed_bots_round_trips_into_peer_lookup` | `record_observed_bots` |
| `lark_signature_matches_known_digest` | signature 验证 |
| `operate_permission_*` 2 个 | `can_operate_bot` |
| `chat_mode_from_str_maps_correctly` | chat mode |
| `is_operate_command_recognizes_adopt_variants` | `is_operate_command` |

### 分组 10 → `lark_replies.rs` + `lark_api_helpers.rs`（回复+API 测试，~6 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `lark_reply_message_includes_reply_in_thread_when_true` | `lark_reply_message` |
| `user_notify_*` 2 个 | user notify |
| `build_final_output_card_uses_markdown_footer_shape` | `build_final_output_footer` |

### 分组 11 → `zellij_adopt.rs`（Zellij 测试，~5 tests）

| 测试函数 | 被测函数 |
|----------|---------|
| `zellij_adopt_candidates_join_layout_and_panes_by_order` | `join_zellij_adopt_candidates` |
| `zellij_cli_id_detection_is_command_based` | `cli_id_from_zellij_command` |
| `reconcile_restored_sessions_*` 2 个 | `reconcile_restored_sessions_with` |
| `should_auto_fork_on_restore_matches_quiet_restart_gate` | `should_auto_fork_on_restore` |

### 分组 12 → 留在 `lib.rs` 的测试（跨模块集成+工具测试，~15 tests）

| 测试函数 | 原因 |
|----------|------|
| `dir_select_*` 5 个 | dir_select 已有自己的单元测试模块，这些是集成测试 |
| `grant_*` 3 个 | grant 已有自己的单元测试模块 |
| `schedule_create_appends_to_file` | 跨模块调度测试 |
| `terminate_workflow_worker_process_*` | 跨模块 worker 测试 |
| `cold_attach_*`, `cold_scan_*` 多个 | cold scan 集成测试 |
| `webhook_trigger_records_*` | webhook 集成测试 |
| `quoted_message_api_*` 2 个 | Lark API 集成测试 |
| `list_chat_history_*`, `list_thread_history_*`, `session_history_*` | Lark 历史 API 集成测试 |
| `toggle_display_returns_a_screenshot_card_response` | 跨模块 display 测试 |
| `render_streaming_card_body_hides_content_in_hidden_mode` | 跨模块 render 测试 |
| `refresh_screenshot_in_hidden_mode_returns_info_toast` | 跨模块 refresh 测试 |
| `worker_ready_display_mode_command_only_resends_screenshot_mode` | 跨模块 worker 测试 |
| `lark_base_url_env_lock` + `LarkBaseUrlEnvGuard` | 测试基础设施 |
| mock server 相关函数 | 测试基础设施 |

## 公共测试基础设施方案

由于多个模块的测试需要 `temp_paths`、mock server 等基础设施，有两种方案：

**方案 A（推荐）**：提取到 `test_helpers.rs`
- 创建 `crates/beam-daemon/src/test_helpers.rs`，用 `#[cfg(test)]` 包裹
- 放入 `temp_paths`、`maybe_remove_dir`、`LarkBaseUrlEnvGuard`、mock server、`make_test_bot_config`、`make_test_session` 等
- 在 lib.rs 的 `mod tests` 中：`#[cfg(test)] mod test_helpers;`
- 各模块通过 `use crate::test_helpers::*;` 引用

**方案 B**：复制到各模块
- 简单但有代码重复

本次采用**方案 A**。

## 执行步骤

### Step 1: 创建 `test_helpers.rs`

从 lib.rs 的 `mod tests` 中提取测试辅助函数到 `crates/beam-daemon/src/test_helpers.rs`：
- 文件以 `#![allow(dead_code)]` 和 `use super::*;` 开头
- 所有辅助函数加 `pub(crate)` 或 `pub(super)` 可见性
- 内容：`temp_paths`、`maybe_remove_dir`、`lark_base_url_env_lock`、`LarkBaseUrlEnvGuard`、mock server 函数、`make_test_bot_config`、`make_test_session`、`make_test_app_state`

### Step 2: 按分组逐个搬迁测试到目标模块

每个分组一个 coder task：

2.1. 解析测试 → `lark_parse.rs`（分组 1）
2.2. 路由测试 → `lark_dispatch.rs`（分组 2）
2.3. 消息测试 → `lark_ingress.rs`（分组 3）
2.4. 卡片构建测试 → `session_cards.rs` + `lark_card_builders.rs`（分组 4）
2.5. 投递/生命周期测试 → `lark_delivery.rs`（分组 5）
2.6. final output 测试 → `final_output.rs`（分组 6）
2.7. 会话测试 → `session_creation.rs`（分组 7）
2.8. 工作流测试 → `workflow_commands.rs`（分组 8）
2.9. 身份/安全测试 → `lark_identity.rs` + `lark_security.rs`（分组 9）
2.10. 回复/API 测试 → `lark_replies.rs`（分组 10）
2.11. Zellij 测试 → `zellij_adopt.rs`（分组 11）
2.12. 清理 lib.rs 剩余测试（分组 12）

每个 step 的通用操作：
1. Read 目标模块文件，确认末尾结构
2. 在目标模块末尾添加 `#[cfg(test)] mod tests { ... }`（如果不存在），追加对应测试函数
3. 从 lib.rs 的 `mod tests` 中删除已搬迁的测试函数
4. 运行 `cargo test -p beam-daemon --lib` 验证
5. 运行 `cargo build -p beam-cli` 确认编译

### Step 3: 验证

```bash
cargo build -p beam-cli 2>&1    # 0 warning, 0 error
cargo test --workspace --no-fail-fast 2>&1  # 766 passed, 0 failed
```

### Step 4: 清理

- 确认 lib.rs 中 `mod tests` 只保留分组 12 的内容和 test_helpers 引用
- 确认所有编译 warning 归零

## 风险与注意事项

1. **测试依赖**：某些测试可能依赖其他测试的副作用（如共享 mock server 状态），移动后可能需要调整
2. **use 语句**：移动测试后可能需要添加对应的 `use` 导入
3. **`use super::*`**：目标模块的 `#[cfg(test)] mod tests` 中需要 `use super::*` 来访问被测函数和类型
4. **helper 函数可见性**：移到 `test_helpers.rs` 后需确保 `pub(crate)` 可见
5. **不修改测试逻辑**：本次纯搬迁，不修改任何测试断言或逻辑
6. **每次搬迁后立即测试**：避免累积错误

## 完成后状态

- `lib.rs`: ~8,647 行 → ~2,000 行（~970 行代码 + ~1,000 行剩余测试）
- 各模块文件末尾新增 50-600 行测试代码
- 测试通过数不变：766 passed, 0 failed
- 编译 warning: 0
