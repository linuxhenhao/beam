# 飞书消息 turn 卡片交接优化计划

## 背景

当前飞书输入进入 `send_input` 后，会先把旧 streaming card 通过 `park_stream_card` 记录成 frozen card，然后清空 `session.stream_card_id`、`session.stream_card_nonce`、`current_screen` 等字段，再把输入发送给 worker。

这导致新卡片的创建依赖后续 worker 状态/截图刷新路径触发 `ensure_lark_streaming_card`，旧卡片撤回又依赖新卡片创建之后的 `recall_frozen_cards`。用户可见效果是：新消息发出后旧卡片会残留一段时间，新卡片出现也不够及时。

目标是把“收到飞书消息后的 turn 切换”变成入口路径上的明确交接流程，而不是依赖后续定时或 worker 刷新。

## 目标

收到飞书消息后，daemon 应立即完成当前 session 的 live card 交接：

1. 旧 live card 被记录为 frozen snapshot，用于旧按钮兼容和撤回失败兜底。
2. 新 turn 的 live streaming card 立即发送，显示已收到/处理中状态。
3. `session.stream_card_id` 和 `session.stream_card_nonce` 只在新卡片发送成功后切换到新卡片。
4. 旧 card 在新 card 成功持久化后立即异步撤回，不等待 worker 后续 tick。
5. worker 后续状态更新继续 patch 新 card。

## 非目标

- 不调整飞书 API 轮询间隔或新增全局定时 worker。
- 不改变 final output/pending response card 的语义。
- 不把 streaming card 作为 pending final-output card。
- 不删除 frozen card 兼容逻辑；只是不让它成为正常用户体验路径的等待机制。
- 不做卡片文案/i18n 重构。

## 现状代码入口

- `crates/beam-daemon/src/lark_ingress.rs`
  - `send_input` 是 CLI/API/飞书消息最终发送 worker 输入的入口。
  - 当前在 `send_input` 中调用 `park_stream_card`，随后清空 `stream_card_id` 和 `stream_card_nonce`。
- `crates/beam-daemon/src/lark_session_cards.rs`
  - `ensure_lark_streaming_card` 负责没有 live card 时发新卡片。
  - `patch_lark_streaming_card` 有卡片则 patch，没有则 fallback 到 `ensure_lark_streaming_card`。
  - `post_or_refresh_lark_session_card` 用于 `/card` 或显式展示卡片。
- `crates/beam-daemon/src/lark_delivery.rs`
  - `park_stream_card` 保存旧卡片 snapshot。
  - `recall_frozen_cards` 删除 frozen card 对应的旧飞书消息。
  - `lark_delete_message` 封装飞书删除消息 API。

## 推荐实现

### 1. 新增 turn card helper

在 `crates/beam-daemon/src/lark_session_cards.rs` 新增 helper，例如：

```rust
pub(crate) async fn begin_lark_turn_card(
    state: &AppState,
    session_id: &str,
    status: &str,
) -> Result<()>
```

职责：

1. 读取 session snapshot。
2. 本地 session 不处理：
   - `session.lark_app_id == "local"` 直接 `Ok(())`。
3. 不可发卡片时不处理：
   - `root_message_id` 为空或 bot 不存在时直接 `Ok(())`。
   - 如果当前还没有 `terminal_url`，仍然允许发基础 processing card 只有在 `build_streaming_card` 能安全处理时才这么做；否则沿用现有 `decide_lark_card_delivery` 的 NotReady 规则。优先选择最小风险：需要 `terminal_url` 才发卡片。
4. 调用 `park_stream_card(&state.paths, &old_session)` 保存旧 card snapshot。
5. 准备 new session snapshot：
   - 生成新的 `stream_card_nonce`。
   - 清空 `stream_card_id`，但这一步先只在内存局部 snapshot 中做，不要先持久化覆盖真实 session。
   - 清空 `current_image_key`、`current_screen`、`last_screen_status`。
6. 用新 snapshot 构造 `build_streaming_card(&session_for_new_card, status)`，通过 `lark_reply_card_with_opts` 发送新卡片。
7. 只有发送成功后，才在真实 session 中写入：
   - `stream_card_id = Some(new_card_id)`
   - `stream_card_nonce = Some(new_nonce)`
   - `current_image_key = None`
   - `current_screen = None`
   - `last_screen_status = None`
   - `last_final_output_turn_id = None`
8. 持久化 sessions。
9. 用 `tokio::spawn` 异步撤回旧 frozen cards：
   - 可以复用 `recall_frozen_cards(state, &updated_session)`。
   - 如果删除失败，只记录 warn，不影响主路径。

注意：不要调用 `start_pending_response_turn`。AGENTS.md 已明确 streaming card 不能成为 pending response target。

### 2. 调整 `send_input`

在 `crates/beam-daemon/src/lark_ingress.rs::send_input`：

1. 保留 `ensure_worker_for_session`。
2. 在发送 worker message 前调用 `begin_lark_turn_card(&state, &session_id, "starting")` 或更准确的状态字符串。
3. 从 `send_input` 中移除直接调用 `park_stream_card` 和直接清空 `stream_card_id/nonce` 的逻辑，避免旧逻辑提前制造空窗。
4. `last_cli_input` 仍在 `send_input` 中更新；如果 `begin_lark_turn_card` 已经负责清空 screen 相关字段，避免重复改同一批字段。
5. 如果 `begin_lark_turn_card` 失败：
   - 不应阻断 worker 输入发送，除非错误说明 session 不存在。
   - 记录 warn，继续 `send_worker_message`。
   - 保持旧 card 指针不变，后续 worker patch 仍有 fallback。

### 3. 保留现有 fallback

保留以下路径作为兜底：

- `ensure_lark_streaming_card` 仍处理缺失 card 的情况。
- `post_or_refresh_lark_session_card` 仍用于 `/card` 显式展示。
- 新卡片成功后的 `recall_frozen_cards` 仍可被这些路径调用。

如有必要，可以把“发送新卡片 + 持久化 + recall”里的公共部分抽成内部 helper，避免 `ensure_lark_streaming_card`、`post_or_refresh_lark_session_card` 和 `begin_lark_turn_card` 产生明显重复。

### 4. 并发和竞态要求

实现必须满足：

- 新卡片发送失败时，不清空旧 `stream_card_id`。
- 旧卡片撤回失败时，不回滚新 `stream_card_id`。
- 两条输入非常接近时，不能让较早 turn 的新卡片覆盖较晚 turn 的 `stream_card_id`。
  - 简化做法：发送新卡片前记录旧 `stream_card_id` 和旧 `last_cli_input`/turn nonce，持久化时确认当前 session 仍处于同一次输入切换预期。
  - 若当前代码没有 turn-level CAS，至少避免在锁外发送卡片后无条件覆盖明显更新过的 `stream_card_id`。
- 不在持有 `state.sessions` lock 时发 HTTP 请求。

### 5. 测试要求

优先新增/调整 `crates/beam-daemon/src/lark_session_cards.rs` 或已有测试辅助里的单元测试。需要覆盖：

1. `begin_lark_turn_card` 新卡片发送成功后：
   - 旧 card 被写入 frozen cards。
   - session 指向新 `stream_card_id`。
   - `stream_card_nonce` 变化。
   - screen/image/status 字段被清空。
2. 新卡片发送失败时：
   - 旧 `stream_card_id` 和旧 nonce 保留。
   - frozen snapshot 可以保留作为兜底，但不能造成 session 指针空窗。
3. `send_input` 不再直接清空 `stream_card_id`。
4. 不调用 `start_pending_response_turn`，可通过 pending response 字段保持不变来验证。

如果 mock Lark server 已有测试基础设施，复用现有 `start_mock_lark_server` / `make_test_app_state` 之类 helper；不要引入 live Feishu 依赖。

## 验证命令

至少运行：

```bash
rustfmt --edition 2024 crates/beam-daemon/src/lark_session_cards.rs crates/beam-daemon/src/lark_ingress.rs crates/beam-daemon/src/lark_delivery.rs
cargo test -p beam-daemon lark_session_cards -- --nocapture
cargo test -p beam-daemon lark_delivery -- --nocapture
cargo build -p beam-cli
```

如果测试名不匹配，运行对应的窄范围 `cargo test -p beam-daemon <new_test_filter> -- --nocapture`。

## 验收标准

- `send_input` 的主路径不再依赖后续 worker tick 才创建新 streaming card。
- 旧 card 撤回在新 card 成功创建并持久化后立即触发。
- 新 card 创建失败时，旧 card 仍然是当前 session live card。
- final output card 和 streaming card 仍然分离。
- 目标测试和 `cargo build -p beam-cli` 通过。
