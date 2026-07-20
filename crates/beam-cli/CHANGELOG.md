# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.2](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.7.1...beam-cli-v0.7.2) - 2026-07-20

### Added

- *(daemon)* 本地 API token 每日轮换与 HMAC 请求签名

### Other

- Merge pull request #45 from linuxhenhao/fix/session-env-isolation

## [0.7.1](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.7.0...beam-cli-v0.7.1) - 2026-07-17

### Added

- *(worker)* 新增 kimi-code CLI 适配器

### Other

- *(beam-worker)* CLI adapter 改为 trait + 注册表架构

## [0.7.0](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.6.4...beam-cli-v0.7.0) - 2026-07-16

### Added

- *(logging)* 优化运行与排障日志分级
- *(screenshot)* 优化事件驱动截图刷新

### Other

- Merge pull request #40 from linuxhenhao/feat/event-driven-screenshot-refresh

## [0.6.3](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.6.2...beam-cli-v0.6.3) - 2026-07-15

### Fixed

- screenshot_delay/cli startup args/dir choose card fixes

### Other

- *(workspace)* 拆分首批 Rust 源文件模块

## [0.6.0](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.5.1...beam-cli-v0.6.0) - 2026-07-13

### Added

- *(daemon)* 支持外部终端入口候选

## [0.5.0](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.4.0...beam-cli-v0.5.0) - 2026-07-06

### Added

- *(send)* 对齐 Rust send 语义

### Other

- *(beam-daemon)* 拆分 lib.rs 为模块化结构并归位测试

## [0.4.0](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.3.3...beam-cli-v0.4.0) - 2026-07-01

### Added

- *(cli)* setup 阶段自动探测 agent 二进制名、优化 allowedUsers 提示并补充单测
- *(beam)* 添加 Traex CLI 支持，支持 cliArgs 和跳过工作目录选择

## [0.3.3](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.3.2...beam-cli-v0.3.3) - 2026-06-30

### Fixed

- *(opencode)* 修复权限确认回填
- *(runtime)* 修复 hook 输出和 slash 透传执行
- *(ask)* 更新 opencode 插件模板
- *(terminal-proxy)* anchor 发送 TermnalResize 设默认 160×50，viewer 断开后 debounce 复位

### Other

- Merge pull request #22 from linuxhenhao/fix/dump_screen

## [0.3.1](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.3.0...beam-cli-v0.3.1) - 2026-06-27

### Fixed

- *(beam)* 使用 beam 前缀命名托管会话

## [0.3.0](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.2.3...beam-cli-v0.3.0) - 2026-06-27

### Added

- *(beam)* 支持飞书历史消息读取
- *(terminal)* 持久化 ticket 密钥、只读 ticket 无过期、默认日志级别 INFO、支持 zellij 0.44 WS 路径
- *(terminal)* 切换到 zellij web terminal

### Other

- Merge pull request #15 from linuxhenhao/feat/lark-workdir-select
- 格式化代码

## [0.2.3](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.2.2...beam-cli-v0.2.3) - 2026-06-22

### Other

- update Cargo.lock dependencies

## [0.2.2](https://github.com/linuxhenhao/beam/compare/beam-cli-v0.2.1...beam-cli-v0.2.2) - 2026-06-22

### Other

- update Cargo.lock dependencies
