# Beam Send Parity 与初始化 Prompt 优化计划

## Summary

补齐 Rust `beam send` 到 botmux `send` 的完整语义，并同步优化初始化 prompt。目标是让 prompt 中要求的 `--mention` / `--mention-back` / `--no-mention` 等发送约束全部变成 Rust CLI 的真实可执行能力，而不是只停留在提示词里。

计划文档落点：新增 `docs/plans/2026-07-05-beam-send-parity-plan.md`，并同步更新 `docs/design/beam-parity-backlog.md` / `.en.md`，把该项登记为 Rust send parity 任务。

## Key Changes

- 优化 `crates/beam-daemon/src/prompt.rs` 初始化 prompt：
  - 第一层说明：agent 正在飞书中和用户交流，只有 `beam send` 发出的内容用户可见。
  - 明确 mention policy：实质结论/需要用户看见或决策用 `--mention-back`，低优先级记录用 `--no-mention`，点名特定对象用 `--mention <open_id[:name]>`。
  - 示例全部改成真实 Rust CLI 将支持的格式，例如 `beam send --mention-back <<'EOF' ... EOF`。
  - 辅助命令改用主接口名：`--files`、`--images`、`--content-file`，不再主推 `--file`。

- 扩展 Rust CLI `beam send`：
  - 支持 `--mention <open_id[:name]>` 可重复、`--mention-back`、`--no-mention`。
  - 支持 `--content-file <path>`、`--files/--file <path>` 可重复、`--images/--image <path>` 可重复。
  - 支持 botmux send 的目标与引用 flags：`--top-level`、`--chat-id <oc_xxx>`、`--into <message_id>`、`--quote <message_id>`、`--no-quote`。
  - 支持兼容 flags：`--card`、`--text` 作为 no-op；`--anyway` 先解析透传，供 off-topic bot guard 使用。
  - 支持 `--attention[=kind]`，kind 限定为 `authz|decision|blocked|help`，默认 `blocked`。
  - 支持 `--voice`，按 botmux 语义发送语音气泡；如果 Rust 端缺少 TTS 运行能力，命令必须清晰报错，不得静默退化成文本。

- 扩展 API 与 daemon 发送路径：
  - 将 `FinalOutputRequest` 从 `{ content }` 扩展为结构化请求，包含 content、mentions、mention_back、no_mention、files、images、target、quote、voice、attention 等字段；保持旧 `{ content }` 兼容。
  - daemon 端负责读取 session、bot secret、chat/root/quote 信息，并统一执行 Lark 发送；CLI 不直接接触 Lark token。
  - mention policy 在 daemon 端做硬校验：`--no-mention` 不能和 `--mention` / `--mention-back` 同用；没有任何 mention decision 时拒发；`--mention-back` 找不到可回提及对象时拒发。
  - `--no-mention` 必须禁止正文自动 @ 注入和 footer 收件人 @。
  - 文件和图片由 daemon 上传并发送；主消息已发后附件失败时只返回部分失败信息，不把整体 send 判为失败，避免模型重试造成重复消息。
  - `--top-level` / `--chat-id` / `--into` 决定发送目标；chat scope 默认发普通群消息，thread scope 默认 reply 到 root message。
  - `--quote` / `--no-quote` 只作用于 chat scope；thread scope 和 `--top-level` 不走 quote chain。

- 文档更新：
  - 新增 `docs/plans/2026-07-05-beam-send-parity-plan.md`，内容即本计划。
  - 在 parity backlog 中新增 “Rust send parity” 任务，注明覆盖 botmux send flags、mention hard gate、media attachments、targeting、quote、voice、attention。
  - prompt 文档或设计说明中记录：初始化 prompt 可以要求这些 flags，因为 Rust CLI 已对齐支持。

## Test Plan

- CLI parse tests：
  - `beam send --mention-back "done"`、`--no-mention`、多个 `--mention`、`--content-file`、`--files/--file`、`--images/--image`、`--top-level`、`--chat-id`、`--into`、`--quote`、`--no-quote`、`--attention=decision`、`--voice`。
  - 冲突参数报错：`--no-mention --mention`、`--no-mention --mention-back`、非法 attention kind。

- daemon send policy tests：
  - 未选择 mention decision 时拒发。
  - `--mention-back` 使用 session 中的触发消息 sender open_id。
  - `--no-mention` 不产生任何 `<at>`。
  - chat scope quote 默认引用，`--no-quote` 取消引用，`--quote` 覆盖引用目标。
  - thread scope 默认 reply in thread，`--top-level` 改为 chat message。

- media tests：
  - 图片上传后内联进 interactive card。
  - 文件作为独立 file 消息发送。
  - 主消息成功、附件失败时返回成功加附件失败列表，不触发重复发送。

- prompt tests：
  - 中文首条消息生成中文 beam routing。
  - 英文首条消息生成英文 beam routing。
  - prompt 包含飞书场景、`beam send` 可见性、三选一 mention policy、真实 flags 示例。

## Assumptions

- 本次按“完整 send parity”实现，不只补当前 prompt 用到的三个 mention flags。
- `--file` 作为 `--files` 兼容别名保留，但文档和 prompt 主推 `--files`。
- `--card` / `--text` 保留为兼容 no-op，不恢复纯文本发送路径。
- CLI 与 daemon 运行在同一机器，`--files` / `--images` 传本地路径给 daemon 读取上传。
- 如果 Rust 端当前没有可用 TTS synthesis，实现 `--voice` 时必须明确失败并提示先补语音运行时；不能假装发送成功。

## 本轮实现完成

✅ **本轮已补齐原差距清单中所有 P0/P1/P2 语义缺口。** 各项均已完成实现并通过 focused tests/build 及当前飞书 session 的 structured `beam send` E2E 验证，未跑全 workspace 测试。backlog（`docs/design/beam-parity-backlog.md` / `.en.md`）中"任务 10: Rust send parity"已同步更新为"已完成，语义缺口已补齐"。

以下保留原差距清单作为实现记录，每项标注本轮补齐方式与已知局限性。

## 原差距清单 / Implemented Gaps

以下为原上轮验收时验证的 botmux 语义缺失项，本轮已逐一补齐，保留作为实现记录。

### P0 — 必须对齐（当前行为明显或静默错误）

1. ✅ 本轮已补齐 — **`--mention-back` 回提及目标修正**
   - 原现状：使用 `session.owner_open_id` 作为 @ 对象。
   - 原问题：`owner_open_id` 是 session 创建者（通常是授权人），而 botmux 的 `quoteTargetSenderOpenId` 是触发/引用消息的实际发送者。在多人群聊中，如果授权人和触发消息发送者不同，`--mention-back` 会 @ 错人。
   - 修复：session 中持久化 `quote_target_sender_open_id`（对齐 botmux 的 `quoteTargetSenderOpenId`），`--mention-back` 优先使用该字段；旧 session 无此字段时 fallback 到 `owner_open_id`，两者均缺失才返回清晰错误。

2. ✅ 本轮已补齐 — **`--attention` 状态上报/清除能力**
   - 原现状：CLI 把 `--attention` kind 传到 daemon，daemon 只校验 kind 是否为 `authz|decision|blocked|help`，之后不做任何动作。
   - 原问题：botmux 有 `/api/attention` 风格的状态上报与清除能力，能让 dashboard/工作流看到 needs-you 信号。当前 beam 的 `--attention` 只是"允许通过"的参数，没有真正改变任何系统状态。
   - 修复：实现 attention 状态的写/清除路径（沿用已有 store 层），使 dashboard 和其他 bot 可查询 needs-attention 信号。

3. ✅ 本轮已补齐 — **`--images` 内联到同一 interactive card**
   - 原现状：图片上传后通过 `im/v1/messages` 以 `msg_type: "image"` 作为独立消息发送，与正文 card 是两条独立消息。
   - 原问题：botmux 是上传拿 `image_key` 后内联到同一 interactive card 的 `img` 元素中。当前独立发送的方式不符合 botmux 语义——图片与正文分离，易被消息刷屏打断。
   - 修复：`build_final_output_card` 接受 `image_keys` 列表并将图片作为 `img` 元素写入 card json；发送时只发一条 interactive card 消息。

### P1 — 高优先级（可能导致静默错误或不符合契约）

4. ✅ 本轮已补齐 — **`--attention` 使用约束**
   - 原现状：`--attention` 可以和任何其他 flag 组合使用。
   - 原问题：botmux 中 `--attention` 应拒绝与 `--top-level`、`--chat-id`、`--into`、`--voice` 等组合（attention 是针对当前对话上下文的，不应跨 chat/thread/top-level）。当前无约束可能导致 attention 信号发到错误上下文。
   - 修复：在 CLI parse 层和 daemon 层增加冲突检测，组合使用时清晰报错。

5. ✅ 本轮已补齐 — **文本 `@BotName` 自动转真实 Lark `<at>` 的 bot-to-bot mention 注入**
   - 原现状：正文中的 `@BotName` 只是纯文本，不会自动转为 `<at user_id="...">`。
   - 原问题：botmux 会检测正文中 `@BotName`（本群内已知 bot 名称）并自动注入真实 Lark `<at>`，实现 bot-to-bot mention。缺少此能力时被 @ 的 bot 收不到真正的 group mention 事件。
   - 修复：在发送前扫描正文中的 `@BotName` 模式，匹配已知 bot 的 open_id 后转为 `<at user_id="...">`。

### P2 — 中等优先级（用户体验差距，非破坏性）

6. ✅ 本轮已补齐（最小子集） — **footer 收件人逻辑**
   - 原现状：`final_output_footer_recipient_open_id` 只返回 `owner_open_id`（且过滤已知 bot id）。
   - 原问题：botmux 的 footer 收件人逻辑有 oncall 概念、人类优先、bot 过滤、去重等完整语义。当前只有一个简单的"owner 非 bot 即收件人"判断。
   - 修复：已实现最小 human-first 子集（人类优先筛选 + 去重 + bot 过滤），确保基础场景正确。完整 oncall/roster 仍是未来增强（非本次范围）。

7. ✅ 本轮已补齐 — **send marker / bridge 去重机制**
   - 原现状：没有发送标记机制来防止最终输出重复发送。
   - 原问题：botmux 有 send marker 和 bridge 抑制逻辑，确保同一条最终输出不会因为重试/重连而被重复发送到飞书。当前 Rust 侧缺少此机制，存在重复发送风险。
   - 修复：在 daemon 发送路径中引入基于 recent explicit-send timestamp + normalized content match + 10 分钟窗口的去重机制，同内容同 session 在窗口期内不重复发送。

8. ✅ 本轮已补齐 — **quote target 被撤回时的 fallback plain send**
   - 原现状：quote target 被撤回时 patch/reply 操作可能失败，但没有 fallback。
   - 原问题：botmux 在 quote target 不可用时（如消息被撤回）会降级为普通 chat 消息发送（plain send），保证消息仍能送达。当前 Rust 缺少此 fallback。
   - 修复：在 `lark_reply_card_with_opts` 报错时增加 fallback 为 `lark_send_chat_message`。

9. ✅ 本轮已补齐 — **off-topic sub-bot 提示逻辑**
   - 原现状：当 `--mention` 指向某个 sub-bot 时，Beam 不做任何提示，直接发送。
   - 原问题：botmux 会检测 `--mention` 的目标：若目标 sub-bot 正活跃在另一个 topic（不同 chat/thread）中，botmux 给出 informational 提示，建议发送者改用 `--into` 将消息发到对应 topic 下，而不是当前聊天。这不是强制 gate——消息仍然发出，只是附带提示。Beam 当前完全缺失此提示。
   - 修复：在 daemon 发送前检查 `--mention` 目标 bot 的活跃 session 上下文，若上下文与当前聊天不同，通过 daemon warn 日志输出 informational 提示（建议 `--into`），不阻断发送。

> **关于 structured send 的 pending card patch 行为**：如前轮分析确认，当前 beam 在有 `target_message_id` 时已经尝试 claim `pending_response_card_id` 并 patch。此行为不是明确缺失项，但在以下场景需要额外测试确认：`--top-level` 是否错误触发了 patch pending card、`--chat-id` 外发是否影响了卡片覆盖。建议列为补测试项而非功能缺失。

## 测试补充建议

以下说明本轮测试覆盖情况与仍可增强项（本轮仅跑了 focused tests/build 和当前 session E2E send，未跑全 workspace 测试）：

| 覆盖项 | 状态 | 说明 |
|--------|------|------|
| `--mention-back` sender 准确性 | ✅ focused test 已覆盖 | 构造trigger sender ≠ owner 场景验证 @ 目标为 trigger sender |
| `--attention` 状态流转 | ✅ focused test 已覆盖 | 验证 attention 写入/查询/清除闭环 |
| `--attention` 与 `--top-level`/`--chat-id`/`--into`/`--voice` 冲突 | ✅ focused test 已覆盖 | 验证组合使用被拒绝并返回清晰错误 |
| `--images` 内联到 card | ✅ focused test 已覆盖 | 验证图片作为 `img` 元素出现在 interactive card json 中 |
| send marker / 去重 | ✅ focused test 已覆盖 | 验证同内容不产生重复消息 |
| quote target 撤回 fallback | ✅ focused test 已覆盖 | 模拟 quote message_id 已不可用，验证降级为 chat message |
| bot-to-bot auto mention | ✅ focused test 已覆盖 | 验证 `@BotName` 正文被转换为 `<at user_id="...">` |
| footer 收件人逻辑 | ✅ focused test 已覆盖 | 验证多 recipient 场景下的去重、人类优先、bot 过滤 |
| route handler E2E（完整 daemon 路径） | 🔶 仍可增强 | mock image upload 等完整 HTTP 层 E2E 可后续补充 |
| 多 bot 群聊并发 E2E | 🔶 仍可增强 | 构造真实多 bot 群聊 session，验证并发 send 一致性 |
