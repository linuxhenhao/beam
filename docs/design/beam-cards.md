# beam 卡片与多语言支持

English: [beam-cards.en.md](beam-cards.en.md)

- 日期：2026-07-04
- 状态：与当前 Rust 实现对齐

## 0. 范围

本文记录仓库中所有由 `beam-daemon` 生成的 Feishu 卡片，以及当前卡片多语言支持的实现方式。

不包含：

- 仅用于接口返回的 toast JSON。
- 普通消息文本、日志、告警字符串。
- 前端 UI，不包括 dashboard 页面。

## 1. 多语言支持方式

当前实现采用 Feishu 卡片原生多语言字段，不依赖“收到消息时的 locale”去猜测用户语言。

### 1.1 约定

每张卡片都遵循同一组约定：

- 根节点加入 `"locales": ["zh_cn", "en_us"]`。
- 所有可见字段都尽量带 `"i18n_content"`。
- `content` 是当前渲染兜底内容，通常按调用时传入的 locale 选择；如果调用方没有 locale，就默认英文。
- `i18n_content.zh_cn` 和 `i18n_content.en_us` 同时保留。
- `plain_text`、`lark_md`、`markdown` 三类内容都走同样的模式。

### 1.2 辅助函数

实现位于 [crates/beam-daemon/src/card_i18n.rs](../../crates/beam-daemon/src/card_i18n.rs)。

- `card_locales()`：返回 `["zh_cn", "en_us"]`。
- `plain_text(locale, zh, en)`：生成 `plain_text` 节点。
- `lark_md(locale, zh, en)`：生成 `lark_md` 节点。
- `markdown(locale, zh, en)`：生成 `markdown` 节点。

使用建议：

- `plain_text` 用于标题、按钮、placeholder。
- `lark_md` 用于带富文本语法的正文。
- `markdown` 用于显式 `tag: markdown` 的内容。

### 1.3 选择语言的规则

- 如果调用方知道 session locale，就传入它。
- 如果调用方拿不到 locale，就传 `None`，让 `content` 使用英文兜底，但仍保留双语 `i18n_content`。
- 不要把消息事件里看到的文本语言当成卡片语言来源。

## 2. 卡片清单

下面是当前仓库里实际存在的卡片构造点。路径后面是主要用途。

| 文件 / 函数 | 作用 | 语言方式 |
| --- | --- | --- |
| `crates/beam-daemon/src/workflow_event_fanout.rs` `build_approval_card` | human-gate 审批卡，含 approve / reject / cancel / dashboard 按钮 | 标题、正文、按钮、note 都有双语 |
| `crates/beam-daemon/src/grant.rs` `build_grant_card` | `/grant` 权限申请卡 | 标题、正文、按钮双语 |
| `crates/beam-daemon/src/ask.rs` `build_ask_card` | ask 问题卡和已提交/已完成状态卡 | 标题、问题正文、按钮、settled banner 双语 |
| `crates/beam-daemon/src/dir_select.rs` `build_dir_select_card` | 工作目录选择卡 | header、列表标题、按钮、输入框、提示文案双语 |
| `crates/beam-daemon/src/dir_select.rs` `build_dir_session_starting_card` | 目录已选中后，过渡到“正在启动会话” | 标题和正文双语 |
| `crates/beam-daemon/src/workflow_progress_card.rs` `build_workflow_progress_card` | 工作流运行进度卡 | header、进度、运行中/等待中、活动列表双语 |
| `crates/beam-daemon/src/lib.rs` `build_contextual_reply_card` | 通用上下文回复卡，供 local turn / adopt preamble 等复用 | 标题、用户消息、assistant 标题、正文、footer 双语 |
| `crates/beam-daemon/src/lib.rs` `build_final_output_card` | 最终输出卡，桥接到 `build_contextual_reply_card` 或纯输出卡 | 标题、正文、footer 双语 |
| `crates/beam-daemon/src/lib.rs` `build_writable_session_card` | 可写终端卡，含 open / restart / close / screenshot / export | 标题、正文、按钮双语 |
| `crates/beam-daemon/src/lib.rs` `build_readonly_link_card` | 只读终端卡 | 标题、正文、按钮双语 |
| `crates/beam-daemon/src/lib.rs` `build_streaming_card` | 活动会话卡，展示状态、截图、读写入口、重试、导出等 | 整体卡片双语 |
| `crates/beam-daemon/src/lib.rs` `build_closed_session_card` | 会话关闭后的恢复卡 | 标题、正文、按钮双语 |
| `crates/beam-daemon/src/lib.rs` `build_tui_prompt_card` | 选中文字 / 自定义文本的 TUI 提示卡 | 标题、表单、按钮双语 |
| `crates/beam-daemon/src/lib.rs` `build_tui_prompt_processing_card` | TUI 提示处理中卡 | 标题、正文双语 |
| `crates/beam-daemon/src/lib.rs` `build_tui_prompt_resolved_card` | TUI 提示已完成卡 | 标题、正文双语 |
| `crates/beam-daemon/src/lib.rs` `build_workflow_approval_resolved_card` | 审批动作完成后的结果卡 | 标题、正文双语 |

## 3. 各类卡片的结构模式

### 3.1 审批类

包括：

- `build_approval_card`
- `build_grant_card`
- `build_ask_card`
- `build_workflow_approval_resolved_card`

特点：

- 主要是 `header + body + actions`。
- 按钮通常使用 `plain_text` 或 `lark_md`。
- 表单字段和结果说明都要带 `i18n_content`。

### 3.2 会话类

包括：

- `build_streaming_card`
- `build_writable_session_card`
- `build_readonly_link_card`
- `build_closed_session_card`
- `build_dir_session_starting_card`

特点：

- 重点是入口按钮和状态说明。
- `build_streaming_card` 是最常更新的卡片，必须保持 locale 一致。
- 关闭 / 恢复 / 只读 / 可写这类入口不要混成一张卡的单个按钮文案逻辑，分别保留双语字段。

### 3.3 选择类

包括：

- `build_dir_select_card`
- `build_tui_prompt_card`

特点：

- 输入框 placeholder 也要双语。
- 列表区、按钮区、提示区都属于可见字段，不能只改 header。
- `select_static` 的分组标题也要带 `i18n_content`。

### 3.4 工作流类

包括：

- `build_workflow_progress_card`
- `build_approval_card`
- `build_workflow_approval_resolved_card`

特点：

- 进度卡和审批卡都属于状态驱动卡片，内容会随事件更新。
- 这类卡片更适合把静态模板和动态状态拆开，再分别给中英内容。

## 4. 实现备注

- `build_lark_card_action_toast` 产生的是 toast，不算本文的“卡片”范围。
- `card_i18n::markdown` 目前在仓库里未大规模使用，但作为 API 保留，便于以后把 `tag: markdown` 的内容统一收口。
- 文档和代码的真实来源都在 `beam-daemon`；如果后续新增卡片，优先在这里补双语字段，再更新本文清单。

