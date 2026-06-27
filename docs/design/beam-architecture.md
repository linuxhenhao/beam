# beam 架构

## 核心实体

### 进程架构

```
┌─────────────────────────────────────────────────────┐
│                   beam-cli                      │
│   start / stop / restart / send / bots / workflow    │
│   logs / status / attach / schedule / dashboard      │
└─────────┬───────────────────────────────┬────────────┘
          │ 管理生命周期                    │ send 透传
          ▼                                ▼
┌─────────────────────────────────────────────────────┐
│                beam-daemon                      │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐ │
│  │Lark WS   │  │HTTP API  │  │Session & Card     │ │
│  │receiver  │  │server    │  │Lifecycle Manager  │ │
│  └──────────┘  └──────────┘  └───────────────────┘ │
│       │              │                   │           │
│       ▼              ▼                   ▼           │
│  ┌──────────────────────────────────────────────┐   │
│  │              AppState                         │   │
│  │  sessions  │  workers  │  bots  │  config     │   │
│  │  paths     │  http     │  schedules          │   │
│  └──────────────────────────────────────────────┘   │
└─────────┬───────────────────────────────────────────┘
          │ stdin/stdout JSON IPC
          ▼
┌─────────────────────────────────────────────────────┐
│                beam-worker                       │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │CLI Adapter   │  │Screen Capture│  │Backend I/O │ │
│  │(opencode/    │  │+ Screenshot  │  │Control     │ │
│  │ claude/codex)│  │Upload Loop   │  │            │ │
│  └──────┬───────┘  └──────┬───────┘  └────────────┘ │
│         │                 │                          │
│         ▼                 ▼                          │
│  ┌──────────────────────────────────────────────┐   │
│  │            SessionBackend                     │   │
│  │   TmuxPipeBackend / ZellijBackend / PtyBackend│   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### 终端代理（Terminal Proxy）

daemon 启动一个本地 `zellij web` server，并在它前面放置统一的 **Terminal Proxy**（`crates/beam-daemon/src/terminal_proxy.rs`）。proxy 是唯一对外入口，负责 Beam ticket 登录、Beam cookie 与后台 zellij cookie 的转换、HTTP/WS 路由和响应头过滤。worker 不再提供独立的 xterm.js HTTP/WebSocket terminal server。

```
外部浏览器                    Daemon Terminal Proxy            zellij web
┌──────────┐    HTTP/WS     ┌────────────────────────┐ HTTP/WS ┌──────────────┐
│ browser  │ ─────────────> │ /s/{session_id}        │ ───────>│ 127.0.0.1:   │
│          │ <───────────── │ /s/{session_id}/ws     │ <───────│ proxy+1      │
│ 飞书卡片  │                │ /s/{session_id}/{path} │         │              │
│ terminal │                │                        │         │ /command/    │
│ button   │                │ Beam cookie            │         │ login        │
│          │                │ -> zellij cookie       │         │ /ws/...      │
└──────────┘                └────────────────────────┘         └──────────────┘
```

- **zellij web**: 本地上游服务，端口为 `web.proxy_base_port + 1`。daemon 启动时确保服务在线，创建 read-only / write token，并启动 watchdog 掉线重启。
- **Daemon Terminal Proxy**: 唯一对外入口，`<host>:<web.proxy_base_port>`。它暴露 `/s/{session_id}`、`/s/{session_id}/ws`、`/s/{session_id}/ws/{*rest}` 和 `/s/{session_id}/{*path}`，把 Beam session 映射到实际 zellij session 名。
- **Cookie bridge**: 外部浏览器只持有 `beam_terminal_session`。proxy 在服务端保存 `Beam cookie -> zellij cookie` 映射，转发上游请求时丢弃浏览器 Cookie 并注入后台 zellij Cookie。zellij 的 `Set-Cookie` 不会透传给浏览器。
- **Terminal ticket**: 终端链接使用 `beam_terminal_ticket`。ticket HMAC 签名且单次使用；write ticket 有 5 分钟 TTL，read-only ticket 用于卡片入口，不按创建时间过期。
- **Read-only anchor**: read-only 登录后，proxy 可在 daemon 内部用 zellij write token 建立一个隐藏普通 web client，丢弃 frames，仅用于让 zellij read-only watcher 有普通 client 可 follow。外部浏览器仍只持有 Beam read-only cookie。
- **权限边界**: read-only/write ticket 会选择不同 zellij token 登录，但 zellij web cookie 可能是全局 cookie；当前实现只记录权限，尚未在 proxy 层强制阻断 read-only 输入。
- 详细路由和安全约束见 `docs/design/terminal-proxy.md`。

### 配置

```
Config
├── DaemonConfig      # 守护进程全局配置
│   ├── bind_host / bind_port
│   ├── backend_type: Tmux | Zellij | Pty
│   ├── working_dirs
│   └── screen_analyzer
├── BotConfig         # 每个机器人的配置
│   ├── lark_app_id / lark_app_secret
│   ├── cli_id / cli_bin / cli_args
│   ├── backend_type
│   ├── allowed_users / allowed_chat_groups
│   └── chat_grants / global_grants
└── BeamPaths         # 文件系统路径布局
    ├── root / bots.json / config.toml
    ├── run/          # 运行时临时文件
    ├── sessions/     # 会话持久化
    └── schedules/
```

### Session

```
Session
├── session_id / title
├── chat_id / chat_type (p2p|group|topic)
├── root_message_id / quote_target_id
├── scope: Thread | Chat
├── status: Active | Closed
├── lark_app_id / owner_open_id
├── cli_id / cli_bin / cli_args
├── backend_type
├── display_mode: Hidden | Screenshot
├── stream_card_id / stream_card_nonce   ← 飞书流式卡片
├── current_image_key                    ← 最新截图的 image_key
├── current_screen / last_screen_status
├── worker_pid / web_port / worker_token
├── adopted_from
├── pending_response_card_id / pending_response_card_state
├── last_final_output
├── frozen_cards: HashMap<nonce, FrozenCard>
└── last_cli_input / bot_name / bot_open_id
```

### IPC（Daemon ↔ Worker 通信）

```
InitConfig              # 启动 Worker 时的初始化参数
├── session_id / title / chat_id / root_message_id
├── working_dir / cli_id / cli_bin / cli_args
├── backend_type / prompt / initial_prompt
├── lark_app_id / lark_app_secret
├── model / locale
└── resume / resume_session_id / adopted_from

DaemonToWorker          # Daemon → Worker
├── Init(InitConfig)
├── Message { content, turn_id }      ← 用户消息（包 XML 标签）
├── RawInput { content, turn_id }     ← 透传命令（原样）
├── Close / Restart
├── SetDisplayMode { mode }
├── TermAction { key }
├── RefreshScreen
└── SpecialKeys / TuiKeys / TuiTextInput

WorkerToDaemon          # Worker → Daemon
├── Ready { port, token }
├── ScreenUpdate { content, status, usage_limit }
├── ScreenshotUploaded { image_key, status, usage_limit }
├── PromptReady
├── FinalOutput { content, turn_id, kind, user_text }
├── CliExit { code, signal }
├── CliSessionId { cli_session_id }
├── AdoptPreamble { user_text, assistant_text }
└── Error { message }
```

### CLI 适配器

```
Adapter trait
├── create_state → 创建 CLI 特定状态
├── build_spawn_spec → 构建启动参数
├── write_input → 写入用户输入
├── poll → 轮询输出（bridge/transcript 检查）
└── 适配器：
    ├── opencode    → SQLite transcript bridge
    ├── claude      → JSONL bridge
    ├── codex       → structured bridge
    ├── coco        → structured bridge
    ├── gemini      → structured bridge
    ├── hermes      → structured bridge
    ├── antigravity → structured bridge
    └── generic     → passthrough
```

### 飞书卡片的两种形态

```
流式卡片 (Streaming Card)           │   回复卡片 (Final Output Card)
────────────────────────────────────┼───────────────────────────────────
创建：ensure_lark_streaming_card    │   创建：deliver_final_output_once
      → POST 到 thread 根消息        │         → fallback_reply
生命周期：整个 turn 持续更新          │         → lark_reply_card_with_opts
      同 turn PATCH 更新终端内容      │   生命周期：一次性，不再更新
      新 turn park → delete → 重建   │
内容：终端截图 + 状态 + 操作按钮      │   内容：Markdown 文本 + 脚注
操作：显示/隐藏截图、刷新、终端、     │   操作：无（纯展示）
      重启、关闭会话                   │
```

---

## 完整流程

### 1. 消息接收 → 会话创建

```mermaid
sequenceDiagram
    participant F as 飞书用户
    participant L as Lark Webhook
    participant D as Daemon
    participant S as Session Store
    participant W as Worker
    participant C as CLI (opencode)

    F->>L: 发送消息 "你好"
    L->>D: POST /lark/events/{app_id}
    D->>D: 权限检查 (can_operate / can_talk)
    D->>D: classify_lark_text_action
    alt 新话题 / 无 active session
        D->>D: build_initial_prompt (opencode 专用)
        D->>S: 创建 Session
        D->>W: spawn_worker + InitConfig
        W->>C: spawn CLI process
        C-->>W: stdout/stderr
        W-->>D: Ready { port, token }
    else 已有 session
        D->>S: send_input → park_stream_card
        D->>W: Message { content: XML包装 }
        W->>C: raw_input / write_input
    end
```

### 2. 终端输出 → 飞书卡片

```mermaid
sequenceDiagram
    participant C as CLI
    participant W as Worker
    participant D as Daemon
    participant L as Lark API
    participant F as 飞书用户

    loop 每 5 秒（内容有变化时）
        W->>W: backend.capture_viewport()
        W->>W: hash = SHA256(screen)
        alt hash 变化
            W-->>D: ScreenUpdate { content, status }
            D->>D: patch_lark_streaming_card
            alt stream_card_id 不存在
                D->>L: POST reply card (创建新卡片)
                D->>D: recall_frozen_cards (删除旧卡片)
            else 已存在
                D->>L: PATCH 更新卡片
            end
            alt display_mode == Screenshot
                W->>W: render_text_screenshot_png
                W->>L: POST /im/v1/images (上传截图)
                W-->>D: ScreenshotUploaded { image_key }
                D->>L: PATCH 卡片 (更新截图)
            end
        end
    end
    F->>F: 看到实时终端卡片
```

### 3. CLI 回复 → 卡片处理

```mermaid
sequenceDiagram
    participant C as CLI (opencode)
    participant W as Worker
    participant D as Daemon
    participant L as Lark API
    participant F as 飞书用户

    C->>W: 完成处理 (assistant_final)
    W->>D: FinalOutput { content, kind }
    D->>D: claim_pending_response_card → None
    D->>L: lark_reply_card_with_opts
    Note over D,L: fallback_reply: 创建独立回复卡片
    L-->>F: 显示回复卡片
    
    Note over C,D: CLI 通过 beam send 主动回复
    C->>D: POST /sessions/{id}/final-output
    D->>D: deliver_final_output_once
    D->>L: lark_reply_card_with_opts
    Note over D,L: 同 fallback_reply 路径
    
    F->>F: 看到回复卡片 + 流式卡片 (两者独立)
```

### 4. 第二轮消息 → 卡片轮替

```mermaid
sequenceDiagram
    participant F as 飞书用户
    participant D as Daemon
    participant L as Lark API
    
    F->>D: 新消息
    D->>D: send_input
    D->>D: park_stream_card (保存旧卡片到 frozen-cards)
    D->>D: stream_card_id = None (清除引用)
    Note over D: 旧卡片仍存在于飞书
    
    D->>D: 收到第一个 ScreenUpdate
    D->>D: ensure_lark_streaming_card
    D->>L: POST 新流式卡片
    D->>D: recall_frozen_cards
    D->>L: DELETE 旧卡片
    Note over F,L: 旧卡片被删除，新卡片显示
```

### 5. 透传命令 (`/review`, `/plan`, 等)

```mermaid
sequenceDiagram
    participant F as 飞书用户
    participant D as Daemon
    participant W as Worker
    participant C as CLI

    F->>D: "/plan"
    D->>D: classify_lark_text_action
    D->>D: text.starts_with('/') → PassthroughInput
    D->>W: RawInput { content: "/plan" }
    W->>C: write text "/plan" + send Enter
    Note over C: CLI 自己的 /slash 处理器处理
```

### 6. 终端代理

```mermaid
sequenceDiagram
    participant U as 飞书用户
    participant C as 飞书卡片
    participant P as Daemon Proxy
    participant Z as zellij web
    participant T as Zellij Session

    U->>C: 点击「Open terminal」
    C->>P: GET /s/{session_id}?beam_terminal_ticket=...
    P->>P: 验证 ticket，选择 read-only/write zellij token
    P->>Z: POST /command/login
    Z-->>P: Set-Cookie: zellij_session=...
    P->>P: 保存 Beam cookie -> zellij cookie
    P-->>C: 302 /s/{session_id} + Set-Cookie: beam_terminal_session=...
    C->>P: GET /s/{session_id}
    P->>Z: GET /{zellij_session} (注入 zellij cookie)
    Z-->>P: HTML/assets (剥离 zellij Set-Cookie)
    P-->>C: HTML
    C->>P: WS /s/{session_id}/ws/{*rest} + Beam cookie
    P->>P: Beam cookie -> zellij cookie
    P->>Z: WS /ws/... (注入 zellij cookie)
    loop 终端同步
        T-->>Z: zellij web terminal state
        Z-->>P: WS message
        P-->>C: WS message
        C->>P: WS input/control message
        P->>Z: WS message
    end
```

---

## 关键设计决定

| 决策 | 原因 |
|------|------|
| 流式卡片 ≠ 回复卡片 | 回复走独立卡片，不 PATCH 覆盖流式卡片 |
| 所有非 beam 的 `/` 命令一律透传 | 不维护白名单，新 CLI 命令自动支持 |
| 截图固定 5s 间隔 | 自适应间隔过于频繁，5s + hash 去重避免无效请求 |
| `/final-output` 在 open_routes | CLI `send` 无需 dashboard token |
| `lark_app_secret` 从 `state.bots` 查 | `create_session_internal` 不能传空 secret |
| Worker 终端 + Daemon 代理双层架构 | Worker 内网端口不暴露，Daemon 统一入口路由 |
```
