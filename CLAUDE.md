# beam

飞书话题群 ↔ AI 编程 CLI 桥接。Daemon 监听飞书消息，每个新话题自动 spawn 一个独立 CLI 进程。

## 构建 & 运行

```bash
cargo build -p beam-cli     # 编译
beam restart                 # 重启 daemon（自动恢复 active sessions）
beam logs                    # 查看日志
```

## 模块结构 (`crates/`)

- `beam-cli` — CLI 入口（start/stop/restart/send/bots/setup/workflow）
- `beam-core` — 共享类型：Session、IPC、Config、Permissions、Workflow
- `beam-daemon` — 守护进程：Lark WS 事件、Session 生命周期、卡片管理、Terminal Proxy
- `beam-worker` — Worker：CLI 适配器、屏幕截图、WebSocket 终端服务

## 添加新 CLI 适配器

1. `crates/beam-worker/src/adapters/` 下创建新文件，实现 `Adapter` trait（`create_state` / `build_spawn_spec` / `write_input` / `poll`）
2. `crates/beam-worker/src/adapters/mod.rs` 注册适配器
3. `crates/beam-worker/src/lib.rs` 的 `CLI_DISPLAY_NAMES` 添加显示名
4. `crates/beam-daemon/src/prompt.rs` 添加 prompt 构建逻辑（如需要专用 initial prompt）
5. `crates/beam-cli/src/main.rs` 的 setup 交互菜单添加选项
