# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
