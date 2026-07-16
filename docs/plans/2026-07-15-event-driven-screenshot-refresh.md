# 事件驱动截图刷新优化计划

## 1. 背景

当前 screenshot 截图/上传的多条路径：

- **Sampler 轮询**：`screen_capture_task` 每 5 秒 capture + render + 上传（若 `DisplayMode::Screenshot`）。
- **RefreshScreen**：worker 收到 `DaemonToWorker::RefreshScreen` 立即 capture + 上传。
- **SetDisplayMode**：worker 收到 `DaemonToWorker::SetDisplayMode { mode: Screenshot }` 立即用缓存画面（`latest_raw_screen`）render 并尝试上传（但此时 `last_uploaded_hash` 仍持有旧 turn 的 hash）。

新消息的 turn 切换流程：

1. daemon `send_input` → `begin_lark_turn_card`（清空 `current_image_key`，发新 card）→ `send_worker_message(Message { content, turn_id })`。
2. `begin_lark_turn_card` 尾部发送 `SetDisplayMode(Screenshot)`（刷新模式以让新 card 显示截图）。
3. worker 端：`SetDisplayMode` 先到达 → 更新 `display_mode` → 调 `maybe_send_screenshot_upload` → 但 hash 去重检查发现 `last_uploaded_hash` 仍为旧 turn 的 hash → **跳过**。
4. worker 端：`Message` 后到达 → 清空 `last_uploaded_hash` → 写入 CLI → 但不再触发上传。
5. 新的截图上传只能等 sampler 下一次循环（0~5 秒内）才真正触发。

实测一条消息：daemon 收到 12:56:12.442，sampler 在 12:56:16.957 开始，等待 4.515 秒；PNG render 152ms，Feishu token+image upload 861ms，daemon PATCH 595ms，总约 6.35 秒。

此外，Zellij backend 的 `subscribe()` 通过 `data_tx` broadcast 持续推送 `pane_update` 事件，但 worker run loop 从未消费它。该信号可在 terminal 内容实际变化后即时驱动截图。

## 2. 目标

1. **首图延迟降低**：消息到达 worker 后，触发截图上传的等待时间从 0~5 秒降低到 ≤500ms（不含 render + upload + PATCH 网络耗时）。
2. **消除重复上传**：同一 turn 内相同 screen hash 不重复上传；不同 turn 的相同 screen hash 不被静默吞掉。
3. **保持 fallback**：5 秒 sampler 作为兜底，在事件驱动失效时仍能刷新。
4. **保留已加入的分段日志**（diagnostic INFO/WARN 耗时日志），作为上线后的指标来源。

## 3. 非目标

- 不改变 screenshot PNG render 逻辑、Feishu upload API、card PATCH 流程本身。
- 不改变 `begin_lark_turn_card`、`SetDisplayMode` 的语义和调用时机。
- 不重构 `send_input`、`final_output`、`worker_lifecycle` 等外围调用链。
- 不新增外部依赖（如消息队列）。
- 不改变 Analyzer / TUI prompt / UsageLimitTracker 的行为。

## 4. 当前代码入口与竞态说明

### worker 端

- `crates/beam-worker/src/worker_runtime/run_loop.rs`：
  - `screen_capture_task`（第 167~315 行）：每 5 秒 `capture_viewport` → render → 广播 hash/status → 若 `DisplayMode::Screenshot` 则调 `maybe_send_screenshot_upload`。
  - `DaemonToWorker::Message`（第 526 行）：清空 `last_uploaded_hash`，写输入到 CLI。
  - `DaemonToWorker::SetDisplayMode`（第 625 行）：更新 mode，用缓存 `latest_raw_screen` render 并广播，若新 mode=Screenshot 则立即调 `maybe_send_screenshot_upload`（但旧 hash 可能仍存在）。
  - `DaemonToWorker::RefreshScreen`（第 579 行）：立即 capture + upload。
  - `backend.subscribe()` 返回的 `broadcast::Receiver<String>`：目前从未在 run loop 中被消费。
- `crates/beam-worker/src/worker_runtime/screenshot.rs`：
  - `maybe_send_screenshot_upload`（第 507 行）：hash 去重（`last_uploaded_hash`），render PNG，upload 到 Feishu，发送 `WorkerToDaemon::ScreenshotUploaded`。
- `crates/beam-worker/src/backend/zellij.rs`（及 observe.rs）：
  - `subscribe()` 返回 `data_tx.subscribe()`，收到 `pane_update` 事件时向 channel 推 ANSI chunk。

### daemon 端

- `crates/beam-daemon/src/lark_ingress/session_actions.rs::send_input`：调用 `begin_lark_turn_card` → 发送 `DaemonToWorker::Message`。
- `crates/beam-daemon/src/lark_session_cards.rs::begin_lark_turn_card`：park 旧 card，发新 card，清空 screen/image 字段，若 `display_mode == Screenshot` 则重发 `SetDisplayMode(Screenshot)`。
- `crates/beam-daemon/src/worker_lifecycle.rs`：处理 `ScreenshotUploaded` → 无条件更新 `current_image_key` → `patch_lark_streaming_card`。

### 已知竞态

1. **首图被 hash 去重吞没**：`SetDisplayMode` 调用 `maybe_send_screenshot_upload` 时 `last_uploaded_hash` 仍是旧 turn hash，上传被跳过；`Message` 清空 hash 但不再触发上传。新卡首图退回 sampler。
2. **多条消息快速连续提交**：较早 turn 的 `ScreenshotUploaded` 可能覆盖较晚 turn 的 session `current_image_key`。当前 card 切换通过 CAS（`last_cli_input` + `stream_card_id`）保护，但截图上传无 turn 关联保护。
3. **pane update 高频触发**：终端每帧变化都触发 `pane_update`，无 debounce 会大量 render+upload。
4. **上传中又触发新上传**：`maybe_send_screenshot_upload` 无 inflight 跟踪，可能并发执行多个 upload。

## 5. 推荐架构

引入单一的 **Capture/Upload Coordinator**（以下简称 coordinator），作为 worker 内一个独立 tokio task 运行。coordinator 通过容量为 16 的有界 channel 接收触发信号，独占维护截图状态。

Channel 发送策略：
- `TurnStarted`、`Refresh`、`SetDisplayMode`、`GraceTimeout`、`FallbackTick`：使用 `send().await`（阻塞发送方直到 channel 有空位；这些事件本身低频，不会死锁）。
- `PaneUpdate`：使用 `try_send()`，满时静默丢弃——debounce + fallback 兜底，丢失一个 pane_update 不会导致丢失截图。

```
输入源:
  A. TurnStarted     —— DaemonToWorker::Message/RawInput 到达时立即发送（切换 revision，取消旧 debounce/dirty）
  B. PaneUpdate      —— backend.subscribe() 的 pane_update 事件（250ms debounce）
  C. ManualRefresh   —— DaemonToWorker::RefreshScreen
  D. DisplayModeChange  —— DaemonToWorker::SetDisplayMode
  E. FallbackTick    —— 5 秒 fallback 定时器
  F. MessageGrace    —— TurnStarted 后启动 500ms 定时器，若 pane_update 未到则 capture 一次
```

只有 coordinator 读写以下状态：

```rust
struct CoordinatorState {
    /// Current turn from the last `Message` / `RawInput`.
    current_turn_id: String,
    /// Hash of the last uploaded screen within the current turn.
    last_uploaded_hash: Option<String>,
    /// The turn_id when last_uploaded_hash was set.
    last_uploaded_turn_id: Option<String>,
    /// True when an upload is in progress.
    upload_in_flight: bool,
    /// Dirty hash to upload after inflight completes.
    pending_dirty_hash: Option<String>,
}
```

### Trigger 枚举

```rust
enum Trigger {
    TurnStarted { turn_id: String },
    PaneUpdate,           // debounce 已处理，仅通知 coordinator 检查
    Refresh,
    SetDisplayMode(DisplayMode),
    GraceTimeout,         // 500ms message-grace fallback
    FallbackTick,
}
```

## 6. 状态与去重设计

用 `(turn_id, screen_hash)` 元组做去重。新 turn 即使相同 screen_hash 也不被去重；同 turn 内相同 screen_hash 去重。

```rust
fn should_upload(current_turn_id: &str, screen_hash: &str, state: &CoordinatorState) -> bool {
    if state.last_uploaded_turn_id.as_deref() != Some(current_turn_id) {
        return true;  // 新 turn：即使 hash 相同也要上传
    }
    state.last_uploaded_hash.as_deref() != Some(screen_hash)
}
```

上传完成后更新：

```rust
state.last_uploaded_hash = Some(screen_hash);
state.last_uploaded_turn_id = Some(current_turn_id);
```

### 上传中防堆积

- 当 `upload_in_flight == true` 时新触发只记录 `pending_dirty_hash = Some(latest_hash)`，不做排队。
- 上传完成后检查 `pending_dirty_hash`，若有值则发起下一次上传。
- 保证最多一个 upload 在执行，且不会丢失最新的 dirty。

## 7. 事件顺序与并发细节

### 7.1 TurnStarted（消息到达触发）

`DaemonToWorker::Message { content, turn_id }` 或 `RawInput` handler 中：

1. **立即**（在 `write_input` / `raw_input` 调用前）通过 channel 发送 `Trigger::TurnStarted { turn_id }`。
2. Coordinator 收到后：
   - 设置 `current_turn_id = turn_id`。
   - 清空 `last_uploaded_turn_id`（使下一次 upload 不受旧 hash 去重限制）。
   - 取消已有 pane_update debounce timer、message-grace timer。
   - 启动 500ms **message-grace timer**。
3. 不等待 `adapter.write_input` 返回确认。handler 继续正常执行 `write_input`，可能会触发 CLI 产生 pane_update。

**为什么不在 write_input 完成后触发**：`adapter.write_input` 涉及 CLI 交互（如 OpenCode transcript resolve），可能耗时不确定；消息到达时就知道新 turn_id，应立即切换 coordinator 状态，避免旧 turn 的 debounce/dirty 干扰。

**为什么需要 message-grace 500ms**：pane_update 是整个终端变化后的通知。在写入后、CLI 回复到达 terminal 之前，有一个短暂窗口。如果 pane_update 在 500ms 内到达，grace 被取消，由 debounce 驱动。如果 500ms 内无 pane_update（例如 CLI 无回显或 subscribe 延迟），grace timer 到期后 capture 一次。这次 capture 可能拿到写入前的画面（hash 与旧 turn 相同），但新 turn 的 `last_uploaded_turn_id` 已被清空，`should_upload` 返回 true，因此会触发上传。后续 pane_update 会以新 hash 覆盖。

### 7.2 Pane Update 触发（Debounce）

- 单独 tokio task 消费 `backend.subscribe()` 的 `broadcast::Receiver<String>`。
- 每次收到 pane_update 事件，通过 `coord_tx.try_send(Trigger::PaneUpdate)` 发送。channel 满时静默丢弃——debounce + fallback 兜底，丢失单个 pane_update 不影响正确性。
- Coordinator 内部实现 debounce：取消已有 debounce JoinHandle（如果有），启动新的 tokio::spawn sleep 250ms，到期后执行 hash 去重检查。
- 如果已有 upload inflight，只更新 `pending_dirty_hash`。
- 如果当前 `display_mode != Screenshot`，跳过 upload，**不记录** `pending_dirty_hash`。当用户或 daemon 切回 Screenshot 时，`SetDisplayMode(Screenshot)` trigger 会 capture 当前 viewport 并按 `(turn_id, hash)` 判断是否上传——此时即使同 turn 同 hash 也不会因隐藏期间的画面被漏掉。此策略最小化 coordinator 内存/状态且与现有隐藏行为一致（隐藏时不截图）。

### 7.3 Capture 与 Upload 的执行

- Capture 通过 `backend.lock().await.capture_viewport().await` 获取 viewport。只在 capture 期间持有锁，释放后才做 render 和 upload。
- Render（`render_text_screenshot_png`）是 CPU 密集型，在 `tokio::task::spawn_blocking` 中执行。
- Upload 使用独立 HTTP client，不阻塞主循环。
- 上传完成后通过 `send_message` 发送 `WorkerToDaemon::ScreenshotUploaded { image_key, status, usage_limit, turn_id }`（turn_id 见 §7.5）。

### 7.4 关闭/重启时取消

- Coordinator 作为 `worker_joins` 中的 tokio task，在 `worker_joins.abort_all()` 时被取消。已有取消机制即可，不需要额外引入 `CancellationToken`。
- `Close`/`Restart` handler 设 `stop_flag`（或直接 `break` 出主循环），coordinator 停止接收新 trigger，放弃 pending dirty。
- 正在执行的 HTTP upload 可能无法被 tokio task abort 强杀（reqwest 不响应 tokio 的 coop/abort），但其产生的 `ScreenshotUploaded` IPC 消息将在 daemon 端被 CAS（§7.5）拒绝，因为 daemon 侧在收到 Close/Restart 时已将 session 标记为 `Closed` 或清空 `current_turn_id`，使得 `status == Active` 条件不满足或 `current_turn_id` 不匹配。
- 实现时评估 workspace 是否已依赖 `tokio-util`；如未依赖，不新增该依赖，取消机制沿用 `worker_joins.abort_all()` + IPC 丢弃策略。

### 7.5 IPC 扩展

1. `WorkerToDaemon::ScreenshotUploaded` 增加 `turn_id: Option<String>`：
   ```rust
   ScreenshotUploaded {
       image_key: String,
       status: ScreenStatus,
       usage_limit: Option<CliUsageLimitState>,
       turn_id: Option<String>,   // 新增，serde default
   }
   ```
   旧 worker 不含此字段，反序列化为 `None`（向后兼容）。

2. `Session` 持久化新增 `current_turn_id: Option<String>`（`#[serde(default)]`）：
   ```rust
   // crates/beam-core/src/session.rs
   pub current_turn_id: Option<String>,
   ```

3. `send_input`（`crates/beam-daemon/src/lark_ingress/session_actions.rs`）在生成 `turn_id` 后同时写入 `session.current_turn_id`：
   ```rust
   let turn_id = next_session_turn_id();
   {
       let mut sessions = state.sessions.lock().await;
       if let Some(entry) = sessions.get_mut(&session_id) {
           entry.current_turn_id = Some(turn_id.clone());
       }
   }
   ```

4. daemon `worker_lifecycle.rs` 中 `ScreenshotUploaded` handler（分两步：先锁内 clone snapshot，锁外判断）：
   ```rust
   // 第一步：从锁范围内读取 (status, current_turn_id) snapshot
   let (session_status, current_turn_id) = {
       let sessions = state.sessions.lock().await;
       sessions.get(&session_id_for_task)
           .map(|s| (s.status, s.current_turn_id.clone()))
           .unwrap_or((SessionStatus::Closed, None))
   };
   // 第二步：锁外判断 — 仅当 session 活跃且 turn_id 匹配才接受
   let stale = match (&turn_id, &current_turn_id) {
       (Some(msg_tid), Some(cur_tid)) => {
           session_status != SessionStatus::Active || msg_tid != cur_tid
       }
       (Some(_), None) => {
           // session 已关闭或尚未写入 current_turn_id — 拒绝
           true
       }
       (None, _) => {
           // 旧 worker 无 turn_id 字段 — 兼容接受（但仍受现有基本保护，如 session 存在性检查）
           false
       }
   };
   if stale {
       warn!(
           "stale screenshot for session {}: msg_turn={:?} current={:?} status={:?}, discarding",
           session_id_for_task, turn_id, current_turn_id, session_status,
       );
       // continue 或 return：不更新 session，不 patch card
   }
   // turn_id 为 None（旧 worker）或匹配且活跃时，正常更新
   ```

5. Worker coordinator 发送 `ScreenshotUploaded` 时填入 `current_turn_id` 的当前值。如果上传在 coordinator 被 cancel 后仍完成（HTTP in-flight），发送的 turn_id 是关闭前的值，daemon 端会因不匹配而丢弃。

此方案在 IPC 和 Session 持久化两个层面均通过 `#[serde(default)]` 保证向后兼容。

### 7.6 IPC send 失败处理

- `send_message` 失败（stdout write error）意味着 worker 进程即将退出。coordinator 记录 warn 并停止后续上传。
- 一次 send 失败不影响已记录的 `pending_dirty_hash`——但后继触发最终也会遇到同一错误，因此直接停止是合理的。

## 8. 实施任务拆分

以下任务按顺序执行，每次 coder 交接一个。每个任务保持改动小、易审查。

---

### 任务 A：Coordinator 纯状态机与确定性测试

**目标**：创建 `ScreenshotCoordinator` 的结构体、状态去重逻辑、trigger 分类，用纯 Rust 单元覆盖；**不**改动 `run_loop.rs` 的现有 `screen_capture_task`。

**允许改动文件**：

- `crates/beam-worker/src/worker_runtime/mod.rs`（声明 `pub(crate) mod coordinator;`）
- `crates/beam-worker/src/worker_runtime/coordinator.rs`（新建文件）

**实现要点**：

1. 定义 `CoordinatorState`（如 §5 所示）和 `Trigger` 枚举。
2. 定义纯函数 `should_upload(current_turn_id, screen_hash, state) -> bool`，以及 `record_upload(state, current_turn_id, screen_hash)`。
3. 定义 `handle_trigger(state, trigger, current_screen_hash) -> Action`：
   - `Action::Skip` | `Action::Capture` | `Action::Debounce { delay_ms }`。
4. 纯状态转换逻辑，不涉及任何 async/tokio/spawn。

**测试**：

- 在 `coordinator.rs` 的 `#[cfg(test)]` 中覆盖全部去重与状态组合：
  - 同一 turn 相同 hash → Skip
  - 同一 turn 不同 hash → Capture
  - 不同 turn 相同 hash → Capture（不被吞）
  - upload_in_flight + 新 trigger → 设置 pending_dirty_hash
  - inflight 完成后检查 pending_dirty → 触发 Capture
  - GraceTimeout / PaneUpdate / Refresh / SetDisplayMode(Screenshot) → 都走向 `handle_trigger`
- 不需要 mock backend 或 HTTP server，纯函数测试。

**验收**：

- `cargo test -p beam-worker coordinator::tests` 通过。
- `cargo build -p beam-cli` 通过。

---

### 任务 B：Fallback Tick 集成与截图上传所有权交接

**目标**：
- 在 `coordinator.rs` 中实现 `run()` 主循环，接受 `Trigger::FallbackTick` 并执行 capture+upload。
- 将截图上传的唯一所有权从旧 `screen_capture_task` 移交给 coordinator。
- 旧 task **继续保留** 5 秒的 `ScreenUpdate` 广播、status/poll/analyzer 等非上传职责，**只删除**其中唯一的 `maybe_send_screenshot_upload` 调用。

**允许改动文件**：

- `crates/beam-worker/src/worker_runtime/coordinator.rs`（新增 `run()` 方法、FallbackTick 处理和 upload 流程）
- `crates/beam-worker/src/worker_runtime/run_loop.rs`（注入 coordinator task；从旧 screen_capture_task 中删除 `maybe_send_screenshot_upload` 调用）
- `crates/beam-worker/src/worker_runtime/screenshot.rs`（如果 coordinator 需要复用 `maybe_send_screenshot_upload` 内部的 PNG render 和 HTTP upload 逻辑，将其拆为 public helper 函数，供 coordinator 调用；否则不动）

**实现要点**：

1. `coordinator.rs` 新增 `run()`：
   ```rust
   pub async fn run(
       mut rx: mpsc::Receiver<Trigger>,
       backend: Arc<Mutex<Box<dyn SessionBackend>>>,
       stdout: Arc<Mutex<tokio::io::Stdout>>,
       session_id: String,
       app_id: String,
       app_secret: String,
   ) {
       let mut interval = tokio::time::interval(Duration::from_secs(5));
       let mut state = CoordinatorState { ... };
       loop {
           tokio::select! {
               biased;
               trigger = rx.recv() => {
                   let trigger = match trigger { Some(t) => t, None => break };
                   // 驱动状态机，必要时 capture+upload
               }
               _ = interval.tick() => {
                   // 发送 Trigger::FallbackTick 到自身状态机
                   // 或用独立分支直接处理
               }
           }
       }
   }
   ```
   - 收到 `Trigger::FallbackTick` 时调用 `should_upload` → 通过则 capture+upload。
   - Capture/upload 复用 `screenshot.rs` 中的 render + upload helper，或直接调用 `maybe_send_screenshot_upload`（从 coordinator 传递 stdout/state）。

2. `run_loop.rs`：
   - 创建容量 16 的 `mpsc::channel(16)`，sender 存入 `Arc` 以便各 handler 克隆。
   - 将 sender 传给 coordinator task（`tokio::spawn`）并加入 `worker_joins`。
   - **旧 `screen_capture_task` 中**：在 `if mode == DisplayMode::Screenshot` 分支下**删除** `maybe_send_screenshot_upload` 调用（第 223~239 行）。保留其余所有代码（`capture_viewport`、render、`ScreenUpdate` 广播、hash/broadcast/status 追踪、poll、alive 检查、sleep）。
   - `run_loop.rs` 中 `Message`/`RawInput` handler 的清空 `last_uploaded_hash` 可保留（未来由 coordinator 的 `TurnStarted` 取代），但必须先保留以免旧 task 的 hash 去重影响其他代码。

3. 截图上传的 render + HTTP upload helper：
   - 如果 `maybe_send_screenshot_upload` 还用于其他地方（确认只有协调前的 `SetDisplayMode`/`RefreshScreen` 和旧 task），可以在 `screenshot.rs` 中拆出内部函数：
     ```rust
     pub(crate) async fn render_and_upload(
         stdout: &..., session_id: &str, app_id: &str, app_secret: &str,
         screen: &str, status: ScreenStatus, usage_limit: ..., turn_id: Option<String>,
     ) -> bool { ... }
     ```
   - Coordinator 和旧 task 的清理后代码均可调用此函数，但最终旧 task 不再调用它。

**不出现双上传**：旧 task 中只有唯一的 `maybe_send_screenshot_upload` 入口，将其删除后旧 task 不再执行任何上传。coordinator 是唯一执行上传的地方。

**测试**：

- 集成测试：mock backend + mock Feishu server，等待 5 秒验证 coordinator 触发 upload（通过 `Trigger::FallbackTick`）。
- 验证旧 `screen_capture_task` 仍定期发送 `ScreenUpdate` 但不触发截图上传。
- 验证 coordinator 的 upload 通过 `send_message` 发送 `ScreenshotUploaded`。

**验收**：

- `cargo test -p beam-worker coordinator_fallback` 通过。
- `cargo build -p beam-cli` 通过。
- 确认旧 `screen_capture_task` 中 `maybe_send_screenshot_upload` 已被删除。

---

### 任务 C：TurnStarted 与 Message-Grace 定时器

**目标**：`DaemonToWorker::Message`/`RawInput` 到达时立即发送 `TurnStarted` 给 coordinator，切换 turn 状态并启动 500ms grace timer。

**允许改动文件**：

- `crates/beam-worker/src/worker_runtime/run_loop.rs`
- `crates/beam-worker/src/worker_runtime/coordinator.rs`

**实现要点**：

1. `run_loop.rs` 中 `DaemonToWorker::Message` handler：
   - 在清空 `last_uploaded_hash` 和设置 `*current_turn_id` 的同一位置（第 536~537 行），发送 `Trigger::TurnStarted { turn_id: turn_id.clone() }` 给 coordinator。
   - **不需要**在 `write_input` 之后做任何额外触发。
2. `Run_loop.rs` 中 `RawInput` handler 同理（第 564~565 行）。
3. `Coordinator` 的 `handle_trigger` 对 `TurnStarted` 响应：
   - 设置 `current_turn_id`，清空 `last_uploaded_turn_id`，取消已有 debounce/grace 计时器。
   - 启动 500ms `tokio::spawn`（grace timer），到期后发 `Trigger::GraceTimeout`。
4. 对 `GraceTimeout`：执行一次 capture+upload（可能拿到写入前画面，但 `should_upload` 因 turn 切换返回 true）。

**注意**：

- `TurnStarted` 在 `write_input` 之前发送是刻意的——切换状态无需等 CLI 确认。
- 旧 `last_uploaded_hash` 的清空由 coordinator 的 `TurnStarted` 接管（清 `last_uploaded_turn_id` 达到同效果）。`run_loop.rs` 中的 `*last_uploaded_hash.lock().await = None` 可以移除或保留为 no-op（由 coordinator 状态机决定去重）。

**测试**：

- 发送 `TurnStarted` → 验证 `last_uploaded_turn_id` 被清空。
- 发送 `TurnStarted` → 500ms 内收到 `PaneUpdate` → grace timer 被取消 → 无单独 capture。
- 发送 `TurnStarted` → 500ms 无 `PaneUpdate` → `GraceTimeout` 触发 capture。

**验收**：

- `cargo test -p beam-worker turn_started` 通过。
- `cargo build -p beam-cli` 通过。

---

### 任务 D：Backend Subscribe Debounce

**目标**：消费 `backend.subscribe()` 的 pane_update 事件，向 coordinator 发 `PaneUpdate` 触发，由 coordinator 做 250ms debounce 后 capture+upload。

**允许改动文件**：

- `crates/beam-worker/src/worker_runtime/run_loop.rs`
- `crates/beam-worker/src/worker_runtime/coordinator.rs`

**实现要点**：

1. `run_loop.rs` 中创建新的 tokio task（或在 coordinator task 内）：
   ```rust
   let mut pane_rx = backend.lock().await.subscribe();
   tokio::spawn(async move {
       while let Ok(_chunk) = pane_rx.recv().await {
           match coord_tx.try_send(Trigger::PaneUpdate) {
               Ok(()) => {}         // 正常入队
               Err(TrySendError::Full(_)) => {} // channel 满，丢弃本次 event；debounce+fallback 兜底
               Err(TrySendError::Closed(_)) => break, // coordinator 已退出
           }
       }
   });
   ```
2. Coordinator 对 `PaneUpdate`：
   - 若已有 debounce task（`JoinHandle`），调用 `.abort()`。
   - 启动新的 250ms sleep，到期后检查 `should_upload`，通过则 capture+upload。
   - 若 `display_mode != Screenshot`，跳过 upload（但仍允许 debounce 重置）。
3. 确保 observe backend 的 `subscribe()` 也返回有效 receiver（确认 `ZellijObserveBackend::subscribe` 已在 `observe.rs` 实现）。

**注意**：

- `subscribe()` 返回的 receiver 永远收不到旧事件，只在订阅后产生的事件才能收到——这是 broadcast channel 的语义，符合需求。

**测试**：

- Mock backend 实现 `subscribe()` 返回可控 broadcast。
- 模拟 10 条 pane_update 在 100ms 内到达，验证 debounce 后只在 250ms 处触发一次 capture。
- 验证 debounce 期间新 event 重置 timer。
- **channel 满不阻塞**：用慢 coordinator（模拟 `rx.recv()` 延迟），持续发 pane_update，验证 pane subscriber task 的 `try_send` 不阻塞且不 panic；停止慢 coordinator 后 fallback tick 仍能触发上传。

**验收**：

- `cargo test -p beam-worker pane_debounce` 通过。
- `cargo build -p beam-cli` 通过。

---

### 任务 E：RefreshScreen / SetDisplayMode 迁移

**目标**：将 `RefreshScreen` 和 `SetDisplayMode` 的截图触发改为 coordinator 处理，移除 run_loop handler 中的直接 `maybe_send_screenshot_upload` 调用。

**允许改动文件**：

- `crates/beam-worker/src/worker_runtime/run_loop.rs`
- `crates/beam-worker/src/worker_runtime/coordinator.rs`

**实现要点**：

1. `DaemonToWorker::RefreshScreen` handler：
   - 只保留 `capture_viewport` + `render` + `broadcast ScreenUpdate`（这些用于 dashboard/CLI，不是截图专用）。
   - 移除直接调 `maybe_send_screenshot_upload`，改为发送 `Trigger::Refresh`。
2. `DaemonToWorker::SetDisplayMode { mode }` handler：
   - 保留 `*display_mode` 更新、render、broadcast。
   - 移除直接调 `maybe_send_screenshot_upload`，改为发送 `Trigger::SetDisplayMode(mode)`。
3. Coordinator 对 `Trigger::Refresh`：无视 debounce，立即 capture+upload（与现有 `RefreshScreen` 行为一致）。
4. Coordinator 对 `Trigger::SetDisplayMode(mode)`：
   - 更新内部 `display_mode` 缓存。
   - 若 `mode == Screenshot` 且 `should_upload` 通过，立即 capture+upload。
   - 若 `mode == Hidden`，不触发 upload。

**注意**：

- `RefreshScreen` 的 `ScreenUpdate` 广播仍由 run_loop handler 直接完成（不涉及截图上传）。分离清晰。
- `SetDisplayMode` handler 仍更新 `*display_mode.write().await`（其他代码依赖此值），coordinator 只需读一次该值或通过消息同步。

**测试**：

- 发送 `Trigger::Refresh` → 验证立即 capture。
- 发送 `Trigger::SetDisplayMode(Screenshot)` → 验证 capture。
- 发送 `Trigger::SetDisplayMode(Hidden)` → 不 capture。

**验收**：

- `cargo test -p beam-worker refresh_display` 通过。
- `cargo build -p beam-cli` 通过。

---

### 任务 F：IPC/Session CAS 扩展

**目标**：在 `ScreenshotUploaded` 增加 `turn_id`，在 `Session` 增加 `current_turn_id`，daemon 端校验 turn 一致性并丢弃过期上传。

**允许改动文件**：

- `crates/beam-core/src/ipc.rs`（`ScreenshotUploaded` 扩展）
- `crates/beam-core/src/session.rs`（`current_turn_id` 字段）
- `crates/beam-daemon/src/lark_ingress/session_actions.rs`（`send_input` 写入 `current_turn_id`）
- `crates/beam-daemon/src/worker_lifecycle.rs`（校验 turn_id）
- `crates/beam-worker/src/worker_runtime/coordinator.rs`（发送时填入 turn_id）
- `crates/beam-worker/src/worker_runtime/screenshot.rs`（如果仍保留 `maybe_send_screenshot_upload`，或直接由 coordinator 发送）

**实现要点**：

1. `ipc.rs`：
   ```rust
   ScreenshotUploaded {
       image_key: String,
       status: ScreenStatus,
       usage_limit: Option<CliUsageLimitState>,
       #[serde(default)]
       turn_id: Option<String>,
   }
   ```
2. `session.rs`：
   ```rust
   #[serde(default)]
   pub current_turn_id: Option<String>,
   ```
3. `session_actions.rs::send_input`：
   ```rust
   let turn_id = next_session_turn_id();
   {
       let mut sessions = state.sessions.lock().await;
       if let Some(entry) = sessions.get_mut(&session_id) {
           entry.current_turn_id = Some(turn_id.clone());
       }
   }
   ```
4. `worker_lifecycle.rs`（分两步：先锁内 clone snapshot，锁外判断）：
   ```rust
   // 第一步：从锁范围内读取 (status, current_turn_id) snapshot
   let (session_status, current_turn_id) = {
       let sessions = state.sessions.lock().await;
       sessions.get(&session_id_for_task)
           .map(|s| (s.status, s.current_turn_id.clone()))
           .unwrap_or((SessionStatus::Closed, None))
   };
   // 第二步：锁外判断 — 仅当 session 活跃且 turn_id 匹配才接受
   let stale = match (&turn_id, &current_turn_id) {
       (Some(msg_tid), Some(cur_tid)) => {
           session_status != SessionStatus::Active || msg_tid != cur_tid
       }
       (Some(_), None) => true,  // 关闭或尚未写入 turn — 拒绝
       (None, _) => false,       // 旧 worker 无 turn_id — 兼容接受
   };
   if stale {
       warn!(
           "stale screenshot for session {}: msg_turn={:?} current={:?} status={:?}, discarding",
           session_id_for_task, turn_id, current_turn_id, session_status,
       );
       // continue 或 return，取决于外层循环结构
   }
   // turn_id 为 None（旧 worker）或匹配且活跃时，正常更新
   ```
5. Coordinator 在发送 `WorkerToDaemon::ScreenshotUploaded` 时填入 `current_turn_id` 快照值（上传开始时记录，非发送时）。

**测试**：

| 场景 | session.status | msg_turn_id | session.current_turn_id | 期望行为 |
|------|---------------|-------------|------------------------|----------|
| 活跃+匹配 | `Active` | `Some("t1")` | `Some("t1")` | 正常更新 + patch |
| 活跃+不匹配 | `Active` | `Some("t0")` | `Some("t1")` | warn + 丢弃 |
| 已关闭+匹配 | `Closed` | `Some("t1")` | `Some("t1")` | warn + 丢弃 |
| 已关闭+无 turn | `Closed` | `Some("t1")` | `None` | warn + 丢弃 |
| session 不存在 | N/A | `Some("t1")` | N/A | warn + 丢弃（unwrap_or Closed） |
| 刚创建尚未有 turn | `Active` | `Some("t1")` | `None` | warn + 丢弃 |
| 旧 worker 无 turn_id | `Active` | `None` | `Some("t1")` | 兼容：正常更新 |
| 旧 worker + 旧 daemon | `Active` | `None` | `None` | 兼容：正常更新 |

- 反序列化测试：旧格式（`ScreenshotUploaded` 不含 turn_id）反序列化为 `turn_id: None`。
- Daemon 单元测试用 mock worker channel 发送上述各组合，验证 session 是否被更新以及 warn 日志。

**验收**：

- `cargo test -p beam-daemon screenshot_cas` 通过。
- `cargo test -p beam-core ipc` 通过（序列化向后兼容）。
- `cargo build -p beam-cli` 通过。

---

### 任务 G：集成测试与 Live Test

**目标**：任务 A~F 完成后，端到端验证事件驱动截图流程。

**测试文件**：

- `crates/beam-worker/tests/coordinator_integration.rs`（或已有测试文件）
- `crates/beam-daemon/tests/live_screenshot.rs`（ignored live test）

**测试矩阵**：

| # | 场景 | 验证点 | 覆盖任务 |
|---|------|--------|----------|
| G1 | 新 turn Message → coordinator 触发 upload | upload 发生，dedup 不吞 | C |
| G2 | 同 turn 相同 hash 不重复上传 | 第二次触发 Skip | A |
| G3 | 不同 turn 相同 hash 上传 | 两次 upload | A |
| G4 | upload inflight 时新 trigger 只记 dirty，完成后上传最新 | 一次 upload 后紧接第二次 | A |
| G5 | 10 pane_update 在 100ms 内 → 250ms debounce 后一次 upload | 一次 capture/upload | D |
| G6 | TurnStarted → 500ms 无 pane_update → GraceTimeout capture | 一次 upload | C |
| G7 | RefreshScreen → 立即 capture | 一次 upload | E |
| G8 | SetDisplayMode(Screenshot) → capture | 一次 upload | E |
| G9 | SetDisplayMode(Hidden) → 不 capture | 无 upload | E |
| G10 | Fallback tick 在无事件时触发 upload | 一次 upload | B |
| G11 | ScreenshotUploaded turn_id 不匹配 → daemon 丢弃 + warn | session 不更新 | F |
| G12 | Close/Restart → in-flight upload 的结果 IPC 被 CAS 丢弃 | daemon warn + 不 patch | F |

**Live test**（`#[ignore]`，需本地环境）：

- 文件名：`crates/beam-daemon/tests/live_screenshot.rs`（新建）
- 测试名：`live_screenshot_refresh_latency`
- 前提：本地运行 beam daemon + zellij，提供 `BEAM_LIVE_TEST_LARK_APP_ID`、`BEAM_LIVE_TEST_LARK_APP_SECRET` 环境变量，bot 需有发消息和上传图片权限。
- 步骤：启动 session → 发送消息 → 轮询 card 内容直到出现 image_key → 记录总耗时。
- 期望：P50 ≤ 2 秒，P95 ≤ 3 秒（从消息提交到 card 出现截图，含 Feishu 网络往返）。

**验收**：

- `cargo test -p beam-worker coordinator_integration -- --nocapture` 通过。
- `cargo test -p beam-daemon -- --nocapture`（live 测试除外）通过。
- live 测试手动验证：`cargo test -p beam-daemon --test live_screenshot -- --ignored --nocapture`。

---

## 9. 风险与兼容策略

| 风险 | 影响 | 缓解 |
|------|------|------|
| Message 到达时立即发 TurnStarted 但 write_input 可能失败 | coordinator 已切换 turn，但实际未写入 | write_input 失败后 adapter 返回 submit.failure_reason，worker 发 UserNotify；turn 状态残留不影响后续正确 turn（新 turn 到达后重置），只是凭空多一次 upload（通过 500ms grace 可能拿旧画面） |
| 旧 worker 二进制（无 turn_id）连接新 daemon | daemon 收到 `turn_id: None`，不走 CAS 校验，行为与当前一致（全量接受） | 向后兼容 |
| 新 worker 连接旧 daemon（无 current_turn_id） | daemon 侧 `current_turn_id == None`，CAS 将 `(Some(_), None)` 视为 stale 而丢弃所有上传 | 升级顺序须先部署新 daemon 再部署新 worker，否则新 worker 的所有截图上传被丢弃；旧 daemon 兼容路径不存在（旧 daemon 无此 CAS 代码） |
| 新 daemon 连接旧 worker（无 turn_id） | daemon 收到 `turn_id: None`，不走校验，全量接受 | 向后兼容，可先升级 daemon |
| HTTP upload 在 worker 关闭后仍在运行 | 结果 IPC 发送到已关闭的 stdout，write error → 被忽略；或 daemon 收到后因 turn_id 不匹配被丢弃 | 依赖 CAS 丢弃；无数据损坏 |
| pane_update 在 terminal 无变化时仍可能触发 | 浪费 render | 250ms debounce 已降低频率；hash 去重二次过滤；5 秒 fallback 在事件驱动正常时被抑制 |
| tokio-util 未依赖 | worker_joins.abort_all() 即可，无需新增依赖 | 不新增依赖，HTTP in-flight 结果由 CAS 丢弃 |

**回滚策略**：

- 每个任务均可独立 revert。任务 B 前旧 task 完整保留。任务 B 后旧 task 仍运行（`capture_viewport`、render、`ScreenUpdate` 广播、poll、alive），只是 `maybe_send_screenshot_upload` 调用被删除——revert 只需补回该调用。
- 若新 coordinator 出现严重问题，最快的回滚是：run_loop.rs 中移除 coordinator task，恢复旧的 `screen_capture_task` 为唯一上传路径。只需 revert 任务 B~E 的 run_loop.rs 修改。

**兼容性**：

- IPC 与 Session 持久化所有新增字段标记 `#[serde(default)]`，新旧版本互相通信时对应字段为 `None`，走兼容路径。
- 部署顺序：先升级 daemon（识别新/旧 worker），再升级 worker。回滚顺序相反。

## 10. 验证命令

```bash
# 任务 A：纯状态机测试
cargo test -p beam-worker coordinator::tests -- --nocapture

# 全量 coordinator 集成测试
cargo test -p beam-worker coordinator -- --nocapture

# IPC 序列化向后兼容
cargo test -p beam-core ipc -- --nocapture

# Daemon 端 ScreenshotUploaded handler（含 CAS 兼容）
cargo test -p beam-daemon screenshot_cas -- --nocapture

# 全量不中断
cargo test --workspace --no-fail-fast

# 编译通过
cargo build -p beam-cli

# Live test（手动，需飞书环境变量）
BEAM_LIVE_TEST_LARK_APP_ID=xxx BEAM_LIVE_TEST_LARK_APP_SECRET=yyy \
  cargo test -p beam-daemon --test live_screenshot -- --ignored --nocapture
```

## 11. 量化验收指标

| 指标 | 当前（实测） | 目标 |
|------|-------------|------|
| 消息到达 worker 到首次截图上传触发 | ~4.5 秒（sampler 等待） | ≤500ms（不含 render+upload+PATCH 网络耗时） |
| 同 turn 内相同 screen hash 重复上传 | 有（下游去重） | 零（coordinator 决策阶段去重） |
| 不同 turn 相同 screen hash 被吞掉 | 有 | 零（turn_id 参与去重键） |
| Fallback tick 在无事件时仍工作 | 是 | 是（不变） |
| 上传中触发新传被阻塞 | 是（hash 不变则不触发） | 不阻塞：记录 dirty，inflight 完成后立即上传最新 |
| 端到端首图延迟（消息提交到 card 显示截图） | ~6.35 秒 | P50 ≤2s, P95 ≤3s（依赖飞书 API 网络延迟） |
| 同一 turn 内 pane_update drive 不产生额外 upload | N/A（无事件驱动） | 250ms debounce + hash 去重确保 ≤1 upload / terminal change |

## 12. 非功能需求

- 所有 async 测试使用 `tokio::test`，超时控制在 5 秒内（纯逻辑测试 ≤100ms）。
- `last_uploaded_hash` 只由 coordinator 通过去重逻辑决定写入，其余代码不再直接修改。
- Render（`render_text_screenshot_png`）不得在 tokio 异步上下文中阻塞；必须在 `spawn_blocking` 中执行。
- 实现时评估 workspace 是否已依赖 `tokio-util`；如未依赖，不新增该依赖，取消机制沿用 `worker_joins.abort_all()`。
