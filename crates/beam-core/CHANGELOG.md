# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
