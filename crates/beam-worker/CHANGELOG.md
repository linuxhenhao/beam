# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
