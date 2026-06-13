# beam parity backlog

- 来源：`docs/design/beam-parity-plan.md`
- 目标：把 TS 版还未对齐的能力拆成可以逐个交付、逐个验收的 Rust 任务
- 原则：
  - 不新增 TS 没有的语义
  - 代码改动必须配套测试
  - 先补行为，再补文档状态
  - 每个任务必须能独立复核是否对标 TS

## 已完成

- 任务 1: OpenCode adapter parity
- 任务 2: Gemini / Hermes adapter parity
- 任务 3: CoCo / Antigravity parity fixtures
- 任务 4: Zellij discover / adopt / observe
- 任务 5: Workflow 真实并发与取消
- 任务 6: Connector / trigger / lifecycle store
- 任务 7: Multi-bot session + send policy
- 任务 8: Grant card 与权限边界
- 任务 9: Setup / migrate / autostart

## 任务 1: OpenCode adapter parity

- 状态：已完成，Rust 已补 transcript poll、submit 验证（最多四次重试+DB 比对）、final output 去重，带 4 个单测覆盖 transcript 解析、重复轮询去重、submit 成功/失败两条路径

## 任务 3: CoCo / Antigravity parity fixtures

- 状态：已完成，Rust 已补 turn 分类、cursor 恢复和 final output 去重测试
- 目标：把现有基础 poll 提升为 TS 等价的 turn 分类和重复抑制
- 必做项：
  - 补 turn 分类测试
  - 补 cursor 恢复测试
  - 补 final output 去重测试
- 验收标准：
  - Rust 测试可明确证明 CoCo / Antigravity 的行为边界

## 任务 4: Zellij discover / adopt / observe

- 状态：已完成，Rust 已接入 discover / adopt / observe 路由和恢复路径
- 目标：补齐 Zellij managed backend 的 TS 路由语义
- 目标文件：
  - `crates/beam-worker/src/backend.rs`
  - `crates/beam-daemon/src/lib.rs`
  - `crates/beam-core/src/session.rs`（如需要）
- 必做项：
  - discover：可列出已有 Zellij session
  - adopt：daemon 重启后可重新接管既有 session
  - observe：能持续观察并恢复 terminal / session 状态
  - 绑定 PID 的逻辑不能再依赖全系统第一个匹配项
- 验收标准：
  - 至少一个 daemon restart 恢复测试
  - 至少一个 adopt/observe 集成测试

## 任务 5: Workflow 真实并发与取消

- 目标：把现有 `max_concurrency` 与 best-effort cancel 提升为更接近 TS 的执行语义
- 目标文件：
  - `crates/beam-core/src/workflow_runtime.rs`
  - `crates/beam-daemon/src/lib.rs`
  - `crates/beam-worker/src/workflow.rs`（如需要）
- 必做项：
  - 并发调度保持确定性
  - 同一 bot 的动作不能乱序并发
  - cancel 到达后尽快阻断后续 action
  - 运行中任务要能被中断并落盘
- 验收标准：
  - 有并发、取消、恢复三类测试
  - workflow progress card 与 runtime snapshot 一致

## 任务 6: Connector / trigger / lifecycle store

- 状态：已完成，Rust 已补 connector store、webhook secret、trigger log、lifecycle store 和 `/api/trigger` / `/api/connectors` 路由
- 目标：补齐 webhook connector store 和 lifecycle 记录
- 目标文件：
  - `crates/beam-daemon/src/lib.rs`
  - `crates/beam-core/src/paths.rs`
  - `crates/beam-core/src/workflow_*`
- 必做项：
  - connector 持久化
  - trigger envelope 持久化
  - trigger log / lifecycle store 的读写闭环
  - webhook 返回错误语义与 TS 对齐
- 验收标准：
  - connector / trigger / lifecycle 各至少一组回归测试

## 任务 7: Multi-bot session + send policy

- 状态：已完成，Rust 已补 foreign-bot @mention gate、单人/单 bot group 放行、self-bot /close 特判，以及 observe-bots / grant 的 peer store 写回闭环
- 目标：把多 bot 群聊路由从“能识别 peer”补成“能按 TS 策略路由”
- 必做项：
  - session 选择规则
  - send policy
  - quota 去重
  - observe-bots / grant 的 peer store 写回闭环
- 验收标准：
  - multi-bot group/session 测试通过
  - quota 重放不重复扣减

## 任务 9: Setup / migrate / autostart

- 状态：已完成，Rust 已补 interactive setup wizard、凭证校验、migrate dry-run / backup / 冲突报告，以及 Linux/macOS autostart enable / disable / status / refresh
- 目标：把 CLI 运维能力补成可真实替换 TS
- 必做项：
  - interactive setup wizard
  - Lark 凭证引导与权限检查
  - migrate dry-run / backup / 冲突报告
  - systemd / launchd 生命周期语义
- 验收标准：
  - setup / migrate / autostart 均有可复核测试

## 任务 10: Parity gate 与 E2E gate

- 目标：把“看起来像对标”变成“测试上证明对标”
- 必做项：
  - manifest 驱动的 Rust/TS 对照门禁
  - 关键 adapter / workflow / dashboard 的 parity fixture
  - 双栈 browser E2E gate
- 验收标准：
  - 未映射的 must-port 项会阻断 CI
  - Rust 与 TS 的同名场景能跑出可比较结果

## 执行顺序

1. 任务 2 到 4：先补 adapter 和 Zellij 这两个最容易出现“表面可用、实际不对标”的断层
2. 任务 6 到 7：再补 connector 和 multi-bot 路由
3. 任务 8 到 10：最后补运维、门禁和测试矩阵
