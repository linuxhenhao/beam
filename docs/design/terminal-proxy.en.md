# Terminal Proxy

Chinese: [terminal-proxy.md](terminal-proxy.md)

This document records the current web terminal implementation in the Rust daemon. Code entry points:

- `crates/beam-daemon/src/lib.rs`: wires zellij web and the terminal proxy during daemon startup.
- `crates/beam-daemon/src/zellij_web.rs`: manages local `zellij web`, token creation, and the watchdog.
- `crates/beam-daemon/src/terminal_auth.rs`: Beam tickets, Beam cookies, and the server-side cookie jar.
- `crates/beam-daemon/src/terminal_proxy.rs`: HTTP/WS proxying, path rewriting, and upstream cookie injection.

## Target Model

The web terminal is not an xterm.js server embedded in the worker. The daemon starts local `zellij web`, then exposes session-scoped URLs through Beam's own terminal proxy.

External browsers and background zellij web use two cookie sets:

- External browsers only hold `beam_terminal_session`.
- Background zellij cookies are stored only inside the daemon process, in `TerminalAuthState`.
- When proxying to zellij web, the proxy drops the browser Cookie header and injects the server-side zellij cookie.
- When proxying responses back to the browser, the proxy strips zellij web `Set-Cookie` headers to avoid leaking background cookies.

This means `beam_terminal_session` is a Beam proxy cookie, not a zellij cookie.

## Startup Flow

In daemon `run()`:

1. `zellij_web_port = web.proxy_base_port + 1`.
2. `ensure_zellij_web(zellij_web_port)` ensures local zellij web is online.
3. `ensure_zellij_web_tokens(...)` creates or loads read-only / write tokens and persists the zellij web tokens JSON under the Beam state directory.
4. `spawn_zellij_web_watchdog(zellij_web_port)` checks zellij web every 30 seconds and attempts restart when it is offline.
5. `terminal_proxy::start_proxy(...)` starts the external proxy on `web.host:web.proxy_base_port`.

## Login and Cookie Bridge

Terminal entry links use Beam tickets:

```text
/s/{session_id}?beam_terminal_ticket=...
```

After receiving a ticket, the proxy:

1. Verifies the HMAC signature, session id, and one-time nonce.
2. Selects the zellij web token by ticket permission:
   - `ReadOnly` -> `read_only_token`
   - `Write` -> `write_token`
3. Calls zellij web:

```text
POST http://127.0.0.1:{zellij_web_port}/command/login
```

4. Captures the `Set-Cookie` returned by zellij web, keeps only `name=value`, and stores it in the server-side cookie jar.
5. Generates a random Beam cookie value and stores the mapping:

```text
beam_terminal_session value -> { zellij_cookie, session_id, permission, created_at }
```

6. Returns the browser response:

```text
302 /s/{session_id}
Set-Cookie: beam_terminal_session=...; HttpOnly; SameSite=Strict; Path=/s/; Max-Age=86400
```

Later requests authenticate only with the Beam cookie. After the proxy finds the mapping, it injects the corresponding zellij cookie into upstream requests.

### Read-only Render Anchor

In zellij web 0.44.x, read-only clients use the watcher client path. The browser frontend opens the terminal WS first, then opens the control WS after receiving the first frame. If this watcher has no regular client to follow, the first screen may have no terminal frames and the page can appear black.

Beam does not patch zellij JS/assets and does not send the write token or zellij write cookie to the external browser. After a successful read-only login, the proxy creates one hidden regular web client inside the daemon for the same zellij session:

1. Calls `/command/login` with the zellij `write_token`; the cookie is stored only inside the daemon process.
2. Calls zellij root `/session` to create a regular `web_client_id`.
3. Connects to `/ws/control` and keeps it open, but does not send `TerminalResize` or `TerminalMetrics`.
4. Connects to `/ws/terminal/{zellij_session}?web_client_id=...` and discards received terminal frames.

This anchor only gives the zellij read-only watcher a regular client to follow. It does not forward external input, does not send resize/metrics, and does not leak internal cookies/tokens to the browser. Anchors are reused per zellij session. If anchor startup fails, the proxy only logs a warning and continues proxying the read-only request normally.

### Viewport Model

Beam treats terminal viewport and card viewport separately:

- The terminal viewport is the interaction size of the real web viewer. After zellij web receives browser control WS resize events, it drives the pane size; the Beam proxy only relays that path, without intercepting or filtering. Resize/metrics from both read-only and write viewers are handled normally by zellij/web; Beam does not intervene.
- Card text and screenshot sampling is done by the worker using `dump-screen` (without `--full`) to capture the current visible viewport. Beam does not apply additional cropping or truncation. If the Feishu platform itself has display limits, those are platform limits; Beam does not silently crop.

## Ticket Lifecycle

Tickets are URL-safe HMAC-SHA256 strings. The payload contains:

```text
session_id:permission:created_at:nonce
```

Current rules:

- Tickets are single-use; the nonce is recorded in daemon memory.
- Write tickets have a 5-minute TTL.
- Read-only tickets do not expire by creation time because the streaming card shows the read-only entry point for a long time.
- The ticket secret is persisted to `ticket-secret` under the Beam state directory. Tickets signed before a daemon restart remain verifiable. If disk I/O fails, the daemon falls back to a process-local secret.

## Routes

Session-scoped routes require a valid Beam cookie, except for the initial ticket login. Raw zellij cookies are not accepted.

| Proxy route | Upstream | Description |
| --- | --- | --- |
| `GET /s/{session_id}` | `/{zellij_session}` | Terminal page. The first request may include `beam_terminal_ticket`; authenticated requests inject the zellij cookie and proxy HTML. |
| `/s/{session_id}/ws` | `/{zellij_session}/ws` | Session-scoped WS. Requires a Beam cookie. |
| `/s/{session_id}/ws/{*rest}` | `/ws/...` | Zellij root WS, such as `/ws/terminal` and `/ws/control`. Requires a Beam cookie. |
| `/s/{session_id}/{*path}` | root or session path | Root API/static/WS-related paths proxy to zellij root; other paths proxy to `/{zellij_session}/{path}`. |

Paths outside `/s/{session_id}...` are not proxied to zellij web and return 404. The proxy no longer provides `/_zellij/...`, global `/ws`, or raw fallback proxying.

Beam session to zellij session mapping:

- Adopted sessions prefer `session.adopted_from.zellij_session`.
- Managed sessions use `beam-{first 8 chars of session_id}`.

## Path Rewriting

Zellij web pages and static assets may contain absolute paths. The proxy lightly rewrites text-like responses:

- `<base href="/">` becomes `<base href="/s/{session_id}/">`.
- Absolute `href="/..."`, `src="/..."`, and `url("/..."` become `/s/{session_id}/...`.
- This makes zellij JS send API and WS calls back through the session-scoped proxy path.

## Header Handling

Request forwarding:

- Skip hop-by-hop headers such as `connection`, `upgrade`, and `host`.
- Skip WebSocket handshake headers. If the HTTP proxy path receives a WS upgrade, it returns `426 Upgrade Required`.
- Skip browser Cookie.
- If a server-side zellij cookie exists, inject it as the upstream `Cookie`.

Response forwarding:

- Skip hop-by-hop headers and `content-length`.
- Always strip upstream `set-cookie`.

WS forwarding:

- Use `ClientRequestBuilder` to build the upstream WS handshake.
- If authenticated, inject the zellij cookie into the upstream WS handshake.
- Pure message relay between the client and zellij web — no message filtering.
- Read-only login additionally ensures that the daemon-internal anchor client is online. The anchor WS does not receive browser input and only discards zellij frames.

## Unsupported Entrypoints

The proxy no longer supports old raw-token or global passthrough entrypoints:

- No `?token=...`.
- No `/_zellij/...`.
- No global `/ws`.
- No arbitrary fallback path passthrough to zellij web.

New links must use `beam_terminal_ticket`; do not expose raw zellij tokens.

## Known Limitations

- Read-only/write tickets currently only decide which zellij token is used for login and are recorded in the Beam cookie entry. The cookie returned by zellij web may be a global session cookie, so the current implementation cannot guarantee read-only input enforcement at the zellij web protocol layer.
- `TerminalAuthState` is process-local. After daemon restart, existing browser Beam cookies no longer map to zellij cookies and the browser must log in through a ticket again.
- Read-only tickets do not expire by creation time. They rely on one-time nonce and ticket secret. Long-lived visible cards can generate new tickets, but already consumed old tickets cannot be reused.
- The read-only anchor depends on zellij web HTTP/WS protocol entrypoints (`/command/login`, `/session`, `/ws/control`, `/ws/terminal/...`), not on exact zellij frontend JS implementation text.
