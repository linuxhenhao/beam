# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.9.0...beam-worker-v0.10.0) - 2026-08-16

### Added

- *(beam-worker)* 用 env 与 cgroupSlice 替换 wrapper 启动

### Fixed

- *(beam-worker)* 收拢 live launch 测试中的嵌套 if

### Other

- *(main)* 合入 grok adapter，保留 cgroupSlice 启动模块

## [0.9.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.8.0...beam-worker-v0.9.0) - 2026-08-08

### Other

- Merge pull request #53 from linuxhenhao/feat/custom-triggers
- 全仓清理 clippy 警告至清零
- 全仓 rustfmt 格式化，消除历史格式遗留

## [0.8.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.7.4...beam-worker-v0.8.0) - 2026-08-07

### Fixed

- *(worker)* 适配 &self backend 风格，修正合并后的编译问题
- *(worker)* 等待 TUI 就绪后再发送首条输入，避免被启动中的 CLI 丢弃

### Other

- Merge branch 'debug-kimi-tui-init-order' into fix/worker-self-heal

## [0.7.2](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.7.1...beam-worker-v0.7.2) - 2026-07-20

### Fixed

- *(worker)* resume 路径补齐 env wrapper，杜绝 BEAM_SESSION_ID 跨会话污染

### Other

- Merge pull request #45 from linuxhenhao/fix/session-env-isolation

## [0.7.1](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.7.0...beam-worker-v0.7.1) - 2026-07-17

### Added

- *(worker)* 新增 kimi-code CLI 适配器

### Other

- *(beam-worker)* CLI adapter 改为 trait + 注册表架构

## [0.7.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.6.4...beam-worker-v0.7.0) - 2026-07-16

### Added

- *(logging)* 优化运行与排障日志分级
- *(screenshot)* 优化事件驱动截图刷新

### Other

- Merge pull request #40 from linuxhenhao/feat/event-driven-screenshot-refresh

## [0.6.4](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.6.3...beam-worker-v0.6.4) - 2026-07-15

### Fixed

- *(worker)* 修复Codex和Traex终端环境

### Other

- Merge pull request #38 from linuxhenhao/fix/screenshot_delay_and_traex_codex_start

## [0.6.3](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.6.2...beam-worker-v0.6.3) - 2026-07-15

### Fixed

- *(worker)* 修复Traex会话记录发现
- screenshot_delay/cli startup args/dir choose card fixes

### Other

- *(workspace)* 本地测试覆盖Rust文件行数检查
- *(worker)* 拆分Zellij后端模块
- *(worker)* 拆分OpenCode适配器模块
- *(workspace)* 拆分首批 Rust 源文件模块

## [0.6.1](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.6.0...beam-worker-v0.6.1) - 2026-07-14

### Fixed

- *(zellij)* 重启陈旧 web 服务并等待 pane 就绪

## [0.6.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.5.1...beam-worker-v0.6.0) - 2026-07-13

### Added

- *(daemon)* 支持外部终端入口候选

## [0.5.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.4.0...beam-worker-v0.5.0) - 2026-07-06

### Added

- *(adopt)* 支持 OpenCode adopt 解析与首条消息 beam context 注入
- *(lark)* 支持中英文提示和卡片文案

### Other

- *(deps)* 更新 Rust 依赖

## [0.4.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.3.3...beam-worker-v0.4.0) - 2026-07-01

### Added

- *(beam)* 添加 Traex CLI 支持，支持 cliArgs 和跳过工作目录选择

## [0.3.3](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.3.2...beam-worker-v0.3.3) - 2026-06-30

### Fixed

- *(opencode)* 修复权限确认回填
- *(runtime)* 修复 hook 输出和 slash 透传执行
- *(terminal-proxy)* anchor 发送 TermnalResize 设默认 160×50，viewer 断开后 debounce 复位

### Other

- Merge pull request #22 from linuxhenhao/fix/dump_screen

## [0.3.2](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.3.1...beam-worker-v0.3.2) - 2026-06-27

### Fixed

- *(terminal-proxy)* 仅采样可见终端区域，移除 full dump
- *(terminal-proxy)* 使用 dump-screen --full 替代 viewport capture，移除 card viewport 裁剪逻辑
- *(terminal-proxy)* 移除 anchor 多余 resize/metrics，截图不再裁剪到 card viewport

### Other

- Merge pull request #21 from linuxhenhao/fix/dump_screen

## [0.3.1](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.3.0...beam-worker-v0.3.1) - 2026-06-27

### Fixed

- *(beam)* 使用 beam 前缀命名托管会话

## [0.3.0](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.2.3...beam-worker-v0.3.0) - 2026-06-27

### Added

- *(terminal)* 持久化 ticket 密钥、只读 ticket 无过期、默认日志级别 INFO、支持 zellij 0.44 WS 路径
- *(terminal)* 切换到 zellij web terminal
- *(terminal)* 接入 xterm 并支持实时终端流

### Fixed

- *(beam)* 对齐 terminal viewport 与卡片截图尺寸
- *(daemon)* 修复只读终端黑屏

### Other

- Merge pull request #15 from linuxhenhao/feat/lark-workdir-select
- 格式化代码

## [0.2.3](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.2.2...beam-worker-v0.2.3) - 2026-06-22

### Other

- update Cargo.lock dependencies

## [0.2.2](https://github.com/linuxhenhao/beam/compare/beam-worker-v0.2.1...beam-worker-v0.2.2) - 2026-06-22

### Other

- update Cargo.lock dependencies
