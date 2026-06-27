# Terminal Proxy

English: [terminal-proxy.en.md](terminal-proxy.en.md)

本文记录当前 Rust daemon 的 web terminal 实现。代码入口：

- `crates/beam-daemon/src/lib.rs`: daemon 启动时接线 zellij web 和 terminal proxy。
- `crates/beam-daemon/src/zellij_web.rs`: 管理本地 `zellij web`、token 创建和 watchdog。
- `crates/beam-daemon/src/terminal_auth.rs`: Beam ticket、Beam cookie、服务端 cookie jar。
- `crates/beam-daemon/src/terminal_proxy.rs`: HTTP/WS proxy、路径重写、上游 cookie 注入。

## 目标模型

web terminal 当前不是 worker 内置 xterm.js server。daemon 启动本地 `zellij web`，再用 Beam 自己的 terminal proxy 对外暴露 session-scoped URL。

外部浏览器和后台 zellij web 使用两组 cookie：

- 外部浏览器只持有 `beam_terminal_session`。
- 后台 zellij cookie 只保存在 daemon 进程内的 `TerminalAuthState`。
- proxy 转发到 zellij web 时丢弃浏览器 Cookie，并注入服务端保存的 zellij cookie。
- proxy 返回浏览器时剥离 zellij web 的 `Set-Cookie`，避免后台 cookie 泄露。

这意味着 `beam_terminal_session` 是 Beam proxy cookie，不是 zellij cookie。

## 启动流程

daemon `run()` 中：

1. `zellij_web_port = web.proxy_base_port + 1`。
2. `ensure_zellij_web(zellij_web_port)` 确保本地 zellij web 在线。
3. `ensure_zellij_web_tokens(...)` 创建或加载 read-only / write token，持久化在 Beam state 目录下的 zellij web tokens JSON。
4. `spawn_zellij_web_watchdog(zellij_web_port)` 每 30 秒检查 zellij web，离线时尝试重启。
5. `terminal_proxy::start_proxy(...)` 在 `web.host:web.proxy_base_port` 启动对外 proxy。

## 登录和 Cookie Bridge

终端入口链接使用 Beam ticket：

```text
/s/{session_id}?beam_terminal_ticket=...
```

proxy 收到 ticket 后：

1. 验证 HMAC 签名、session id 和一次性 nonce。
2. 按 ticket 权限选择 zellij web token：
   - `ReadOnly` -> `read_only_token`
   - `Write` -> `write_token`
3. 调 zellij web：

```text
POST http://127.0.0.1:{zellij_web_port}/command/login
```

4. 捕获 zellij web 返回的 `Set-Cookie`，只取 `name=value`，存进服务端 cookie jar。
5. 生成随机 Beam cookie value，保存映射：

```text
beam_terminal_session value -> { zellij_cookie, session_id, permission, created_at }
```

6. 返回浏览器：

```text
302 /s/{session_id}
Set-Cookie: beam_terminal_session=...; HttpOnly; SameSite=Strict; Path=/s/; Max-Age=86400
```

后续请求只用 Beam cookie 认证。proxy 查到映射后，把对应 zellij cookie 注入上游请求。

### Read-only render anchor

zellij web 的 read-only client 在 0.44.x 中走 watcher client 路径。实际浏览器前端先打开 terminal WS，收到首帧后才打开 control WS；如果这个 watcher 没有普通 client 可 follow，首屏可能没有任何 terminal frame，页面表现为黑屏。

Beam 不 patch zellij 的 JS/assets，也不把 write token 或 zellij write cookie 发给外部浏览器。proxy 在 read-only 登录成功后，为同一个 zellij session 在 daemon 内部建立一个隐藏普通 web client：

1. 使用 zellij `write_token` 调 `/command/login`，cookie 仅保存在 daemon 进程内。
2. 调 zellij root `/session` 创建普通 `web_client_id`。
3. 连接 `/ws/control`，发送固定的 `TerminalResize` 和 `TerminalMetrics`。
4. 连接 `/ws/terminal/{zellij_session}?web_client_id=...`，丢弃收到的 terminal frames。

这个 anchor 只用于让 zellij read-only watcher 有普通 client 可 follow，不转发外部输入，也不会把内部 cookie/token 泄露给浏览器。anchor 按 zellij session 维度复用；如果 anchor 失败，proxy 只记录 warning，read-only 请求继续按正常路径代理。

### Viewport model

Beam 把 terminal viewport、card viewport 和 fallback viewport 分开处理：

- terminal viewport 是真实 web viewer 的交互尺寸。zellij web 收到浏览器 control WS 的 resize 后驱动 pane 尺寸；Beam proxy 只透传这条路径。
- card viewport 是飞书卡片截图展示尺寸。worker 上传截图前按 `120x36` 裁剪，避免卡片截图和卡片文本截断尺度不一致。
- fallback viewport 是没有真实 viewer 可用时的临时尺寸。worker managed session 默认用 `120x36`，read-only anchor 也发送 `120x36` 的初始 resize/metrics。

如果只有 read-only viewer，优先让 zellij web 使用该 viewer 上报的实际尺寸；只有 read-only anchor 先行建立、真实 viewer 尚未完成 resize/control WS 时，才使用 `120x36` fallback。未来如果飞书卡片模板支持更大图片，应只调整 card viewport，不反向影响真实 terminal viewport。

## Ticket 生命周期

ticket 是 HMAC-SHA256 签名的 URL-safe 字符串，payload 包含：

```text
session_id:permission:created_at:nonce
```

当前规则：

- ticket 单次使用，nonce 会记录在 daemon 内存中。
- write ticket 有 5 分钟 TTL。
- read-only ticket 不按创建时间过期，因为 streaming card 会长期展示 read-only 入口。
- ticket secret 持久化到 Beam state 目录的 `ticket-secret`，daemon 重启后已签发 ticket 仍可验证；如果磁盘读写失败则退化为进程内 secret。

## 路由

session-scoped 路由需要有效 Beam cookie，除首次 ticket 登录外不接受裸 zellij cookie。

| Proxy route | Upstream | 说明 |
| --- | --- | --- |
| `GET /s/{session_id}` | `/{zellij_session}` | 终端页面。首次可带 `beam_terminal_ticket` 登录；已登录时注入 zellij cookie 并代理 HTML。 |
| `/s/{session_id}/ws` | `/{zellij_session}/ws` | session-scoped WS。必须使用 Beam cookie。 |
| `/s/{session_id}/ws/{*rest}` | `/ws/...` | zellij root WS，例如 `/ws/terminal`、`/ws/control`。要求 Beam cookie。 |
| `/s/{session_id}/{*path}` | root 或 session path | root API/static/WS 相关 path 代理到 zellij root；其他 path 代理到 `/{zellij_session}/{path}`。 |

非 `/s/{session_id}...` 的路径不代理到 zellij web，返回 404。proxy 不再提供 `/_zellij/...`、全局 `/ws` 或裸 fallback proxy。

Beam session 到 zellij session 的映射：

- adopt session 优先使用 `session.adopted_from.zellij_session`。
- managed session 使用 `beam-{session_id 前 8 位}`。

## 路径重写

zellij web 页面和静态资源可能包含绝对路径。proxy 对 text-like 响应做轻量重写：

- `<base href="/">` 改为 `<base href="/s/{session_id}/">`。
- 绝对 `href="/..."`、`src="/..."`、`url("/..."` 改为 `/s/{session_id}/...`。
- zellij JS 由此把 API 和 WS 调用发回 session-scoped proxy 路径。

## Header 处理

请求转发：

- 跳过 hop-by-hop headers，例如 `connection`、`upgrade`、`host`。
- 跳过 WebSocket handshake headers，HTTP proxy path 收到 WS upgrade 会返回 `426 Upgrade Required`。
- 跳过浏览器 Cookie。
- 如果已有服务端 zellij cookie，则注入为上游 `Cookie`。

响应转发：

- 跳过 hop-by-hop headers 和 `content-length`。
- 始终剥离上游 `set-cookie`。

WS 转发：

- 用 `ClientRequestBuilder` 构造上游 WS handshake。
- 如认证成功，在上游 WS handshake 中注入 zellij cookie。
- client 和 zellij web 之间只做 message relay。
- read-only 登录会额外确保 daemon 内部 anchor client 在线；anchor 的 WS 不接收浏览器输入，只丢弃 zellij frames。

## 不支持的入口

proxy 不再支持旧的 raw token 或全局透传入口：

- 不支持 `?token=...`。
- 不支持 `/_zellij/...`。
- 不支持全局 `/ws`。
- 不支持把任意 fallback path 透传给 zellij web。

新的链接必须使用 `beam_terminal_ticket`，不要暴露 raw zellij token。

## 已知限制

- read-only/write ticket 目前只决定使用哪个 zellij token 登录，并记录在 Beam cookie entry 中。zellij web 返回的 cookie 可能是全局 session cookie，因此当前实现不能保证在 zellij web 协议层强制 read-only 输入限制。
- `TerminalAuthState` 是进程内状态。daemon 重启后，浏览器已有 Beam cookie 不再能映射到 zellij cookie，需要重新通过 ticket 登录。
- read-only ticket 不按创建时间过期，依赖一次性 nonce 和 ticket secret。长期可见卡片能重新生成新 ticket，但已被消费的旧 ticket 不能复用。
- read-only anchor 依赖 zellij web 的 HTTP/WS 协议入口（`/command/login`、`/session`、`/ws/control`、`/ws/terminal/...`），不依赖 zellij 前端 JS 的具体实现文本。
