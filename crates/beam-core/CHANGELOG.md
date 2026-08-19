# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.2](https://github.com/linuxhenhao/beam/compare/beam-core-v0.10.1...beam-core-v0.10.2) - 2026-08-19

### Added

- *(worker)* 按输入框颜色确认 TUI 提交，截图保留 ANSI 着色

## [0.10.1](https://github.com/linuxhenhao/beam/compare/beam-core-v0.10.0...beam-core-v0.10.1) - 2026-08-16

### Added

- *(worker)* 静态启动参数改为 cliArgs 显式默认项

## [0.10.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.9.0...beam-core-v0.10.0) - 2026-08-16

### Added

- *(beam-worker)* 用 env 与 cgroupSlice 替换 wrapper 启动

### Other

- *(main)* 合入 grok adapter，保留 cgroupSlice 启动模块

## [0.9.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.8.0...beam-core-v0.9.0) - 2026-08-08

### Added

- group-chat custom triggers with per-trigger prompt, dir and ack

### Other

- Merge pull request #53 from linuxhenhao/feat/custom-triggers
- 全仓清理 clippy 警告至清零
- 全仓 rustfmt 格式化，消除历史格式遗留
- 修复 custom-triggers 分支引入的 rustfmt 格式问题

## [0.8.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.7.4...beam-core-v0.8.0) - 2026-08-07

### Fixed

- *(worker)* 等待 TUI 就绪后再发送首条输入，避免被启动中的 CLI 丢弃

### Other

- Merge branch 'debug-kimi-tui-init-order' into fix/worker-self-heal

## [0.7.2](https://github.com/linuxhenhao/beam/compare/beam-core-v0.7.1...beam-core-v0.7.2) - 2026-07-20

### Added

- *(daemon)* 本地 API token 每日轮换与 HMAC 请求签名

### Other

- Merge pull request #45 from linuxhenhao/fix/session-env-isolation

## [0.7.1](https://github.com/linuxhenhao/beam/compare/beam-core-v0.7.0...beam-core-v0.7.1) - 2026-07-17

### Added

- *(worker)* 新增 kimi-code CLI 适配器

### Other

- *(beam-worker)* CLI adapter 改为 trait + 注册表架构

## [0.7.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.6.4...beam-core-v0.7.0) - 2026-07-16

### Added

- *(logging)* 优化运行与排障日志分级
- *(screenshot)* 优化事件驱动截图刷新

### Other

- Merge pull request #40 from linuxhenhao/feat/event-driven-screenshot-refresh

## [0.6.3](https://github.com/linuxhenhao/beam/compare/beam-core-v0.6.2...beam-core-v0.6.3) - 2026-07-15

### Other

- *(workspace)* 本地测试覆盖Rust文件行数检查
- *(core)* 拆分workflow回归场景
- *(workspace)* 拆分首批 Rust 源文件模块

## [0.6.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.5.1...beam-core-v0.6.0) - 2026-07-13

### Added

- *(daemon)* 支持外部终端入口候选

## [0.5.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.4.0...beam-core-v0.5.0) - 2026-07-06

### Added

- *(adopt)* 支持 OpenCode adopt 解析与首条消息 beam context 注入
- *(send)* 对齐 Rust send 语义
- *(lark)* 支持中英文提示和卡片文案

### Other

- *(deps)* 更新 Rust 依赖

## [0.4.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.3.3...beam-core-v0.4.0) - 2026-07-01

### Added

- *(beam)* 添加 Traex CLI 支持，支持 cliArgs 和跳过工作目录选择

## [0.3.3](https://github.com/linuxhenhao/beam/compare/beam-core-v0.3.2...beam-core-v0.3.3) - 2026-06-30

### Fixed

- *(terminal-proxy)* anchor 发送 TermnalResize 设默认 160×50，viewer 断开后 debounce 复位

## [0.3.0](https://github.com/linuxhenhao/beam/compare/beam-core-v0.2.3...beam-core-v0.3.0) - 2026-06-27

### Added

- *(core)* 增加持久化子系统与状态恢复机制
- *(terminal)* 切换到 zellij web terminal

### Other

- Merge pull request #15 from linuxhenhao/feat/lark-workdir-select
- 格式化代码

## [0.2.2](https://github.com/linuxhenhao/beam/compare/beam-core-v0.2.0...beam-core-v0.2.2) - 2026-06-22

### Fixed

- *(daemon)* 修复飞书话题会话匹配
