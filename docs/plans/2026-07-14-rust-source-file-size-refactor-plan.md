# Rust 源文件大小治理与模块化重构计划

- 日期：2026-07-14
- 状态：待实施
- 受众：初级工程师（每个任务均可独立提交、独立验收）
- 范围：仅重构 Rust 源代码的模块边界和测试归属；**不改变** HTTP/IPC/Lark/workflow 对外行为、持久化格式、配置字段或命令行兼容性。

## 1. 目标与硬性约束

仓库约定是：源文件一般保持在 800 行左右；超过 1,000 行必须明确评估职责；超过 1,500 行默认拆分。2026-07-14 的盘点表明当前有 24 个生产源文件超过 1,000 行，其中 16 个超过 1,500 行。因此不能只继续拆 `beam-daemon/src/lib.rs`，需要按领域完成一轮全仓库治理。

本计划完成时必须满足：

1. `crates/*/src/**/*.rs` 中没有超过 1,000 行的文件；新文件优先控制在 800 行以内。
2. 一个文件只承担一个清晰职责；不得为了降低行数机械地把同一职责切成 `*_part1.rs`、`*_part2.rs`。
3. 测试跟随被测模块，但不得把大量测试继续堆在实现文件末尾。实现文件需要测试时，使用同名目录下的外置测试子模块，例如：

   ```rust
   // workflow_runtime.rs
   #[cfg(test)]
   #[path = "workflow_runtime/tests.rs"]
   mod tests;
   ```

4. 不修改 `pub` API 的语义。新模块内部优先使用 `pub(super)` / `pub(crate)`，不要为了跨文件调用把内部实现误提升为公开 crate API。
5. 每次提交只拆一个有界领域，必须能编译、通过该领域测试；禁止在同一提交混入功能修复、格式化全仓库或依赖升级。
6. 所有移动后的 Rust 文件执行 `rustfmt --edition 2024`。

### 完成度量

在每个阶段开始和结束时执行，并把输出贴到 PR 描述：

```bash
rg --files crates -g '*.rs' | xargs wc -l | sort -rn | head -40
find crates -path '*/src/*.rs' -o -path '*/src/**/*.rs' | sort
cargo fmt --check
```

最终门禁（可以在最后一个任务中加入 CI；在此之前先本地执行）为：

```bash
! rg --files crates -g '*.rs' | xargs wc -l | awk '$2 != "total" && $1 > 1000 { found=1; print } END { exit found }'
cargo test --workspace --no-fail-fast
cargo build -p beam-cli
```

> `crates/*/tests/*.rs` 也纳入 1,000 行限制。大型集成测试应按行为场景拆分，不应成为绕过源文件约束的容器。

## 2. 基线清单与优先级

以下行数为 `wc -l` 的物理行数。`测试起始行` 用于判断应先搬测试还是先拆生产代码；`—` 表示文件内未找到 `#[cfg(test)]`。

| 优先级 | 文件 | 当前行数 | 测试起始行 | 主要混合职责 | 目标模块 |
|---|---|---:|---:|---|---|
| P0 | `beam-core/workflow_runtime.rs` | 5465 | 1410 | run loop、dispatch、完成事件、loop、测试 | `workflow_runtime/{loop,dispatch,completion,helpers,tests}.rs` |
| P0 | `beam-daemon/lark_ingress.rs` | 4963 | 4030 | Lark webhook、card action、workflow/session 操作、WS | `lark_ingress/{webhook,card_actions,workflow_actions,session_actions,ws,tests}.rs` |
| P0 | `beam-cli/main.rs` | 3051 | 1050 | CLI 解析、daemon 生命周期、setup、schedule、send | `cli_commands/{daemon,send,schedule,sessions,setup,misc,tests}.rs`，`main.rs` 仅入口/分派 |
| P0 | `beam-daemon/final_output.rs` | 2989 | 1639 | final-output 交付、attention、附件、重试、测试 | `final_output/{delivery,attention,attachments,retry,pending,tests}.rs` |
| P0 | `beam-worker/adapters/opencode.rs` | 2600 | 1057 | adapter、SQLite transcript、来源解析、屏幕消歧、测试 | `adapters/opencode/{transcript,source_resolution,disambiguation,types,tests}.rs` |
| P0 | `beam-daemon/dir_select.rs` | 2427 | 1076 | 目录扫描/校验、recent store、卡片渲染、测试 | `dir_select/{scan,validation,recent,card,tests}.rs` |
| P0 | `beam-daemon/workflow_reconcilers.rs` | 2303 | 1144 | trait/registry、provider 实现、reconcile、测试 | `workflow_reconcilers/{registry,providers,reconcile,missing_provider,tests}.rs` |
| P0 | `beam-core/workflow_run.rs` | 2250 | 672 | run bootstrap、参数归一化/校验、文件读取、测试 | `workflow_run/{bootstrap,params,validation,tests}.rs` |
| P0 | `beam-core/workflow_snapshot.rs` | 2043 | 1815 | DTO、event replay、dashboard preview/binding、测试 | `workflow_snapshot/{model,replay,preview,tests}.rs` |
| P0 | `beam-daemon/terminal_proxy.rs` | 1903 | 1464 | ticket/cookie auth、HTTP 转发、WS relay、readonly anchor、测试 | `terminal_proxy/{auth,http,websocket,anchor,tests}.rs` |
| P1 | `beam-daemon/lark_dispatch.rs` | 1822 | 234 | preflight、路由决策、session 更新、大量测试 | `lark_dispatch/{preflight,routing,session,tests}.rs` |
| P1 | `beam-worker/lib.rs` | 1817 | 1673 | worker 主循环、screen analyzer、截图/上传、TUI | `worker_runtime/{analyzer,screenshot,tui,run_loop,tests}.rs` |
| P1 | `beam-daemon/workflow_commands.rs` | 1798 | 711 | approve/reject、cancel、runtime driver、测试 | `workflow_commands/{approval,cancel,runtime,tests}.rs` |
| P1 | `beam-daemon/session_cards.rs` | 1586 | 102 | terminal link、streaming card、display/retry、测试 | `session_cards/{terminal_links,streaming,actions,tests}.rs` |
| P1 | `beam-core/workflow_orchestrator.rs` | 1534 | 999 | DAG 推进、loop 推进、拓扑辅助、测试 | `workflow_orchestrator/{dag,loops,topology,tests}.rs` |
| P1 | `beam-daemon/zellij_web.rs` | 1349 | 907 | zellij web 生命周期、token、watchdog、测试 | `zellij_web/{lifecycle,tokens,watchdog,tests}.rs` |
| P1 | `beam-core/workflow_definition.rs` | 1117 | 583 | workflow schema/parse/validation、测试 | `workflow_definition/{schema,validation,tests}.rs` |
| P1 | `beam-daemon/workflow_resume.rs` | 1104 | — | resume 请求/响应与 provider 恢复编排 | `workflow_resume/{request,response,recovery}.rs` |
| P1 | `beam-daemon/route_handlers.rs` | 1076 | — | API handler 包装器 | `route_handlers/{sessions,workflows,connectors}.rs` |
| P1 | `beam-worker/backend.rs` | 1063 | 742 | backend trait、zellij backend、observe/subscribe、测试 | `backend/{zellij,observe,subscribe,tests}.rs` |
| P1 | `beam-daemon/workflow_cancellation.rs` | 1029 | 329 | 取消编排、状态计算、测试 | `workflow_cancellation/{logic,delivery,tests}.rs` |
| P2 | `beam-core/tests/workflow_regression.rs` | 1060 | — | 多种 workflow 回归场景混杂 | 按 run/loop/recovery/card 场景拆为多个集成测试文件 |

`beam-daemon/src/lib.rs` 当前 2672 行，但生产启动编排约到第 981 行，剩余为跨模块测试；它在 Phase 0.1 单独处理，不能因为此前已拆过就豁免。

## 3. 通用实施方法（每个任务严格照做）

1. **建立行为护栏**：先列出待移动的函数、其调用点和现有测试。若没有覆盖关键分支，先补最小确定性测试；不要在移动同时修改断言语义。
2. **选模块边界**：按上表的职责切分。模块可以共用父级私有类型；只有需要由父模块调用的项才标记 `pub(super)`。
3. **先搬生产实现**：使用 `#[path]` 或目录 `mod.rs` 建立新模块，移动完整函数/关联类型，修复 import。不要复制后保留旧实现。
4. **再搬测试**：将原 `mod tests` 内容移动到 `领域/tests.rs`；保留原有测试名、断言与 helper。跨模块测试放在父模块的 `tests.rs`，不属于任何一个模块的测试才放 crate 的 `tests/` 目录。
5. **缩小入口文件**：`lib.rs`/`main.rs` 只保留 module declarations、依赖组合、顶层 `run/main` 和少量真正跨领域的 types。入口文件的测试也必须外置。
6. **验证与提交**：按任务卡执行命令。先 `cargo test -p <crate> <filter>`，再 `cargo build -p beam-cli`；仅在阶段收尾跑 workspace 全量测试。提交信息使用 `refactor(<crate>): 拆分<领域>模块`。

### 禁止事项

- 不改变 event 名称、JSON 字段、workflow event 顺序、数据库查询语义、HTTP 路由路径或 Lark 卡片 action value。
- 不把 daemon 的 `AppState` 克隆成新状态对象；子模块始终接收现有 `&AppState` / `Arc<AppState>`。
- 不把 core 层依赖倒灌到 daemon/worker/cli。
- 不在纯重构任务中启用真实 Lark、zellij 或外网测试；真实系统回归测试必须继续 `#[ignore]`。

## 4. 执行批次与任务卡

### Phase 0：先移测试，建立可控的入口

#### Task 0.1：清空 `beam-daemon/src/lib.rs` 的内联测试

**修改：** `crates/beam-daemon/src/lib.rs`，新增 `crates/beam-daemon/src/tests/lib_integration.rs`；必要时扩展现有 `src/tests/test_helpers.rs`。

**步骤：**

1. 确认第 982 行后的每个测试确为跨模块集成测试；可归属到具体模块的测试应直接移到对应模块的 `tests.rs`，不要移入新的总测试文件。
2. 在 `lib.rs` 用 `#[cfg(test)] #[path = "tests/lib_integration.rs"] mod tests;` 替换内联测试模块。
3. 保持测试 helper 的 crate 可见性和临时目录清理行为不变。

**验收：** `lib.rs` 小于 1,000 行；`cargo test -p beam-daemon` 通过。

#### Task 0.2：抽取 CLI 命令实现

**修改：** `crates/beam-cli/src/main.rs`，新增 `crates/beam-cli/src/cli_commands/`。

**步骤：**

1. 先迁移第 1050 行以后的测试到 `cli_commands/tests.rs`。
2. 按表中六类移动命令实现；`Cli`、`Command`、clap 参数类型可暂留 `main.rs`，但每个命令函数不得再留在入口。
3. 新建 `cli_commands/mod.rs` 作为命令函数再导出层，`main()` 仅解析 `Cli` 后 `match Command` 分派。
4. 重点保留 `send` 的 stdin/content 优先级、`setup` 的备份逻辑、daemon health 等待和 exit code。

**验收：** `main.rs` 小于 800 行；`cargo test -p beam-cli`、`cargo build -p beam-cli` 通过。

#### Task 0.3：抽取 worker 主循环的辅助领域

**修改：** `crates/beam-worker/src/lib.rs`，新增 `crates/beam-worker/src/worker_runtime/`。

**步骤：** 按 analyzer、screenshot、tui、run_loop 拆出；`run()` 只构造运行时依赖并调用 `worker_runtime::run_loop`；将第 1673 行后的测试移到 `worker_runtime/tests.rs`。

**验收：** `lib.rs` 小于 500 行；`cargo test -p beam-worker` 通过。

### Phase 1：Core workflow（按依赖顺序）

#### Task 1.1：`workflow_run`

**唯一范围：** `crates/beam-core/src/workflow_run.rs`。不得在此任务改动 `workflow_definition.rs`、`workflow_runtime.rs` 或 event schema。

**交付结构：** 保留 `workflow_run.rs` 作为稳定 API 再导出入口；新增 `workflow_run/bootstrap.rs`（run bootstrap、持久化、definition read/hash）、`workflow_run/validation.rs`（参数 normalize、type/format validation、coercion）及按 bootstrap / params / format / coercion 分开的外置测试文件。任何一个测试文件超过 800 行时继续按测试场景拆分。

**不变量：** `bootstrap_workflow_run`、`normalize_workflow_params`、`mint_workflow_run_id` 和 `read_workflow_definition_from_path` 的签名、错误文案、hash 输入与参数 coercion 规则逐字保持。

**验收：** 所有相关文件小于 800 行；`cargo test -p beam-core workflow_run` 与 `cargo build -p beam-cli` 通过。

#### Task 1.2：`workflow_definition`

**唯一范围：** `crates/beam-core/src/workflow_definition.rs`。新增 `workflow_definition/schema.rs`（serde DTO/enum）和 `workflow_definition/validation.rs`（parse 与校验）；测试外置到 `workflow_definition/tests.rs`。严禁改变错误文案，尤其是定义校验失败的错误类别。

**验收：** 原入口和每个新增文件小于 800 行；`cargo test -p beam-core workflow_definition` 与 `cargo build -p beam-cli` 通过。

#### Task 1.3：`workflow_snapshot`

将纯 DTO 放 `model.rs`，EventLog → snapshot 状态机放 `replay.rs`，sidecar/preview/dashboard binding 放 `preview.rs`。`replay.rs` 是唯一修改 `ReplaySnapshot` 的位置，避免在 preview 中二次解释 event。

**验收：** 对 run 成功、失败、cancel、dangling effect、loop 的现有 snapshot 测试全部通过；原文件小于 800 行。

#### Task 1.4：`workflow_orchestrator`

`dag.rs` 只处理普通节点推进，`loops.rs` 只处理 loop iteration，`topology.rs` 放排序、依赖和 activity-id helper。不要改变 `OrchestratorAction` 的类型或 action 顺序。

**验收：** `cargo test -p beam-core workflow_orchestrator` 通过；原文件小于 800 行。

#### Task 1.5：`workflow_runtime`

这是最高风险任务，必须最后拆。顺序为：先外置测试；再抽 `helpers`（hash/blob/prompt/id）、`completion`、`dispatch`、`loop`；最后保留 `run_tick`、`run_loop` 和调度协调层。不得在本任务调整并发、重试、cancel 检查或 effectAttempted 写入顺序。

**验收：**

```bash
cargo test -p beam-core workflow_runtime
cargo test -p beam-core workflow
cargo test -p beam-core --test workflow_regression
```

所有相关文件小于 1,000 行，原 `workflow_runtime.rs` 小于 800 行。

### Phase 2：Daemon Lark 与卡片域

按下列顺序拆分，每个条目单独提交：

1. `lark_dispatch`：先外置测试，再拆 preflight/routing/session。保持 dedupe key 和多 bot gate 不变。
2. `dir_select`：拆 scan/validation/recent/card；目录边界检查必须使用同一 canonical/normalize 语义。
3. `session_cards`：拆 terminal link、streaming card、action/display；保持卡片 action 名称和 ticket URL 不变。
4. `final_output`：拆 pending marker、attention、attachments、retry、delivery。`deliver_final_output_once` 的单次交付和 streaming card 不能被 PATCH 覆盖的约束必须有回归测试。
5. `lark_ingress`：最后拆 webhook、card actions、workflow actions、session actions、WS。入口 `handle_lark_event_payload` 与 `handle_lark_card_action_payload` 可保留为薄协调层；所有 action value 路由保持原样。

**每项验收：** `cargo test -p beam-daemon <模块关键词>`，然后 `cargo build -p beam-cli`。Phase 2 结束时执行 `cargo test -p beam-daemon`。

### Phase 3：Daemon workflow、HTTP 与终端代理

1. `workflow_reconcilers`：registry、具体 providers、reconcile decision、missing-provider 分开。`effectAttempted` sidecar hash 校验和 manual recovery 断言必须保留。
2. `workflow_commands`：approval、cancel、runtime 启动拆开；共享的 run-id/状态检查置于私有 helper。
3. `workflow_resume`：request 解析、response 组装、recovery 调用拆开；不改变 provider 恢复顺序。
4. `workflow_cancellation`：纯状态/决策和外部 delivery 拆开；取消后不调度新 action 的测试必须继续存在。
5. `route_handlers`：按 sessions/workflows/connectors 拆成薄 handler；路由注册仍集中在原有 router composition，`/sessions/{id}/final-output` 必须继续位于 `open_routes`。
6. `terminal_proxy`：auth（ticket/cookie）、HTTP forwarding、WS relay、readonly anchor 拆开；不得改变 cookie 隔离和 response header stripping。
7. `zellij_web`：lifecycle、tokens、watchdog 拆开；配置端口 readiness 是启动边界，不能因为其他端口 listener 判定成功。

**Phase 验收：** `cargo test -p beam-daemon`、`cargo build -p beam-cli`。涉及 runtime 的提交额外执行 `target/debug/beam restart`，且只能在已获得本机运行验证授权时执行。

### Phase 4：Worker adapter/backend 与剩余测试文件

1. `adapters/opencode`：保留 adapter trait glue 在 `opencode.rs`；把 SQLite 查询/分组放 transcript，CLI/log 发现放 source_resolution，screen-vs-transcript 打分放 disambiguation，数据结构放 types。adopted PID 的过滤必须有原样测试。
2. `backend`：trait 与 common types 留 `backend.rs`；Zellij 实现、observe、subscribe 分开。不得改变 pane id 解析、viewport ANSI 拼接或 process 生命周期。
3. 将 `workflow_regression.rs` 按 `run_regression.rs`、`loop_regression.rs`、`recovery_regression.rs` 等行为命名拆分；测试共享 fixture 放 `crates/beam-core/tests/support/`。
4. 复查所有 801–1,000 行文件；若文件内只有一个内聚职责，可在文档中记录原因并暂留；若同时有生产代码和大段测试，按通用方法继续外置测试，使物理文件不超过 1,000 行。

## 5. 合并与审查清单

每个 PR/提交由审查者逐项确认：

- [ ] 只移动/重组代码，或功能修复有单独提交和明确说明。
- [ ] 原函数没有留下重复实现；`rg '<旧函数名>'` 的定义只有一个。
- [ ] 新模块名称描述职责，不使用 `misc`、`common2`、`partN`。
- [ ] 可见性没有不必要地从私有提升到 `pub`。
- [ ] 测试名字、断言和 `#[ignore]` 状态保持，新增测试覆盖拆分边界。
- [ ] `cargo fmt --check`、目标 crate 测试、`cargo build -p beam-cli` 通过。
- [ ] 已更新文件行数基线，没有文件超过 1,000 行。
- [ ] 不含 Cargo 版本改动、lockfile 噪音、自动格式化无关文件。

## 6. 建议提交序列

按下列顺序，每项一个提交；若中途出现行为回归，立刻停止后续拆分并回退到上一个可编译提交定位。

1. `refactor(daemon): 外置lib跨模块测试`
2. `refactor(cli): 拆分命令实现`
3. `refactor(worker): 拆分运行时辅助模块`
4. `refactor(core): 拆分workflow定义与运行初始化`
5. `refactor(core): 拆分workflow快照与编排`
6. `refactor(core): 拆分workflow运行时`
7. `refactor(daemon): 拆分Lark与会话卡片模块`
8. `refactor(daemon): 拆分workflow和终端代理模块`
9. `refactor(worker): 拆分OpenCode适配器和Zellij后端`
10. `test(core): 拆分workflow回归场景`
11. `ci: 增加Rust源文件行数门禁`（仅在前十项全部完成后）

最后一次提交前运行完整验证：

```bash
cargo fmt --check
cargo test --workspace --no-fail-fast
cargo build -p beam-cli
rg --files crates -g '*.rs' | xargs wc -l | sort -rn | head -40
```

若最后一条仍显示任意文件超过 1,000 行，任务不得标记完成；必须新增一个有界拆分任务，而不是以“历史文件”为由豁免。

## 7. 原子任务登记册（派发给初级工程师的唯一顺序）

本表是实施时唯一的任务粒度。**一次只派发一行**；工程师完成后不得自行领取下一行。审查者必须依次完成“diff 边界检查 → 行数检查 → 该行指定测试/构建”才可勾选完成。除最后一行外，每行都必须保持可独立回滚。

| 序号 | 状态 | 唯一改动边界 | 完成定义 |
|---:|---|---|---|
| 01 | 已完成 | `beam-daemon/src/lib.rs` 内联跨模块测试 | 外置为按场景测试子模块；`lib.rs` 与每个新测试文件均 <1,000 行；`cargo test -p beam-daemon` |
| 02 | 已完成 | `beam-cli/src/main.rs` | 仅抽 CLI commands 与其测试；入口只保留 clap 类型和分派；`cargo test -p beam-cli`、`cargo build -p beam-cli` |
| 03 | 已完成 | `beam-worker/src/lib.rs` | 仅抽 analyzer/screenshot/TUI/run-loop 与其测试；保持 IPC；`cargo test -p beam-worker`、`cargo build -p beam-cli` |
| 04 | 已完成 | `beam-core/src/workflow_run.rs` | 仅抽 bootstrap、validation/coercion 与测试；不改 definition/runtime；`cargo test -p beam-core workflow_run` |
| 05 | 已完成 | `beam-core/src/workflow_definition.rs` | 仅抽 schema、validation 与测试；保持 parse 错误文案；`cargo test -p beam-core workflow_definition` |
| 06 | 已完成 | `beam-core/src/workflow_snapshot.rs` | 仅抽 model、event replay、preview/binding 与测试；`cargo test -p beam-core workflow_snapshot` |
| 07 | 已完成 | `beam-core/src/workflow_orchestrator.rs` | 仅抽 DAG、loop、topology 与测试；action 次序不变；`cargo test -p beam-core workflow_orchestrator` |
| 08 | 已完成 | `beam-core/src/workflow_runtime.rs` | 仅抽 helpers、completion、dispatch、loop 与测试；不改 effect/cancel 顺序；`cargo test -p beam-core workflow_runtime` 和 `workflow_regression` |
| 09 | 已完成 | `beam-daemon/src/lark_dispatch.rs` | 仅抽 preflight、routing、session 更新与测试；dedupe/multibot gate 不变；`cargo test -p beam-daemon lark_dispatch` |
| 10 | 已完成 | `beam-daemon/src/dir_select.rs` | 仅抽 scan、validation、recent、card 与测试；路径边界语义不变；`cargo test -p beam-daemon dir_select` |
| 11 | 已完成 | `beam-daemon/src/session_cards.rs` | 仅抽 terminal links、streaming render、actions 与测试；action/ticket URL 不变；`cargo test -p beam-daemon session_cards` |
| 12 | 待开始 | `beam-daemon/src/final_output.rs` | 仅抽 pending、attention、attachments、retry、delivery 与测试；streaming/final 卡片隔离不变；`cargo test -p beam-daemon final_output` |
| 13 | 待开始 | `beam-daemon/src/lark_ingress.rs` | 仅抽 webhook、card/workflow/session actions、WS 与测试；payload/action value 不变；`cargo test -p beam-daemon lark_ingress` |
| 14 | 待开始 | `beam-daemon/src/workflow_reconcilers.rs` | 仅抽 registry、providers、reconcile、missing-provider 与测试；sidecar hash/manual recovery 不变；`cargo test -p beam-daemon workflow_reconcilers` |
| 15 | 待开始 | `beam-daemon/src/workflow_commands.rs` | 仅抽 approval、cancel、runtime driver 与测试；run-id/status 语义不变；`cargo test -p beam-daemon workflow_commands` |
| 16 | 待开始 | `beam-daemon/src/workflow_resume.rs` | 仅抽 request、response、recovery；provider 恢复次序不变；`cargo test -p beam-daemon workflow_resume` |
| 17 | 待开始 | `beam-daemon/src/workflow_cancellation.rs` | 仅抽 pure logic、外部 delivery 与测试；cancel 后不 dispatch 新 action；`cargo test -p beam-daemon workflow_cancellation` |
| 18 | 待开始 | `beam-daemon/src/route_handlers.rs` | 仅按 sessions/workflows/connectors 拆 handler；router 组合与 open routes 不变；`cargo test -p beam-daemon`、`cargo build -p beam-cli` |
| 19 | 待开始 | `beam-daemon/src/terminal_proxy.rs` | 仅抽 auth、HTTP、WS、anchor 与测试；cookie/header/readonly 边界不变；`cargo test -p beam-daemon terminal_proxy` |
| 20 | 待开始 | `beam-daemon/src/zellij_web.rs` | 仅抽 lifecycle、tokens、watchdog 与测试；configured-port readiness 不变；`cargo test -p beam-daemon zellij_web` |
| 21 | 待开始 | `beam-worker/src/adapters/opencode.rs` | 仅抽 transcript、source resolution、disambiguation、types 与测试；adopted PID 行为不变；`cargo test -p beam-worker opencode` |
| 22 | 待开始 | `beam-worker/src/backend.rs` | 仅抽 zellij、observe、subscribe 与测试；pane/ANSI/lifecycle 语义不变；`cargo test -p beam-worker backend` |
| 23 | 待开始 | `beam-core/tests/workflow_regression.rs` | 仅按 run/loop/recovery 场景拆集成测试和 `tests/support` fixture；`cargo test -p beam-core --test <拆分后的每个测试目标>` |
| 24 | 待开始 | 基线中 801–1,000 行的八个文件：`workflow_event_fanout`、`session_creation`、`lark_parse`、`terminal_auth`、`lark_session_cards`、`workflow_cli`、`adapters/codex`、`lark_delivery` | 逐文件写“单一职责保留理由”或另开一个只含该文件的拆分任务；不能批量移动代码；所有文件仍 <1,000 行 |
| 25 | 待开始 | CI 与最终质量门禁 | 只新增行数检查并跑 `cargo fmt --check`、workspace tests、`cargo build -p beam-cli`；不得与业务重构混合提交 |

### 每次派发时必须附上的固定指令

向初级工程师派发上表任一行时，任务消息必须包含以下内容，避免其自行推断范围：

1. “只执行第 NN 行；不要处理下一行，也不要顺手整理其他文件。”
2. “不得修改 Cargo 文件、依赖、公开 API、持久化/HTTP/IPC 字段或无关格式。”
3. “所有新增或修改的 Rust 文件必须 <1,000 行，优先 <800；测试外置后也受此限制。”
4. “完成前运行 `rustfmt --edition 2024`、`git diff --check`、该行指定测试和 `cargo build -p beam-cli`。”
5. “汇报改动文件、各文件行数、测试/构建输出、`git diff --stat`；不提交。”

审查者若发现任何一项未满足，只能退回同一行返工；不得通过“下一任务顺手修复”的方式放行。
