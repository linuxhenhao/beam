# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.3.0...beam-daemon-v0.3.1) - 2026-06-27

### Fixed

- *(beam)* 使用 beam 前缀命名托管会话

## [0.3.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.2.3...beam-daemon-v0.3.0) - 2026-06-27

### Added

- *(beam)* 支持飞书历史消息读取
- *(core)* 增加持久化子系统与状态恢复机制
- *(terminal)* 持久化 ticket 密钥、只读 ticket 无过期、默认日志级别 INFO、支持 zellij 0.44 WS 路径
- *(daemon)* 支持通用 /adopt 命令和飞书上下文透传
- *(terminal)* 支持 web terminal 免输入认证
- *(terminal)* 切换到 zellij web terminal
- *(terminal)* 接入 xterm 并支持实时终端流

### Fixed

- *(beam)* 对齐 terminal viewport 与卡片截图尺寸
- *(daemon)* 修复只读终端黑屏
- *(terminal)* 区分 ticket 读写 token

### Other

- Merge pull request #15 from linuxhenhao/feat/lark-workdir-select
- 格式化代码

## [0.2.3](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.2.2...beam-daemon-v0.2.3) - 2026-06-22

### Other

- update Cargo.toml dependencies

## [0.2.2](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.2.0...beam-daemon-v0.2.2) - 2026-06-22

### Added

- *(daemon)* 优化飞书新会话目录选择
- *(daemon)* 飞书新会话支持选择工作目录

### Fixed

- *(daemon)* 修复飞书话题会话匹配
- *(daemon)* 修复飞书目录选择交互

## [0.2.1](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.2.0...beam-daemon-v0.2.1) - 2026-06-16

### Other

- update Cargo.toml dependencies
