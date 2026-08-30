# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.1](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.11.0...beam-daemon-v0.11.1) - 2026-08-30

### Fixed

- *(daemon)* 修复 herdr adopt 卡片段落结构与 pane id 校验

## [0.11.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.10.2...beam-daemon-v0.11.0) - 2026-08-29

### Added

- *(daemon)* 实现 herdr web 终端（内置 xterm.js 页 + observe/control WS 桥）

### Fixed

- *(daemon)* 修复 Herdr 会话 worker 就绪看门狗误报启动超时

### Other

- *(readme)* 更新 README 后端支持并致谢 botmux
- 实现了herdr支持

## [0.10.2](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.10.1...beam-daemon-v0.10.2) - 2026-08-19

### Fixed

- *(worker)* 修正 CLI 探活误判，CliExit 不再关闭会话

## [0.10.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.9.0...beam-daemon-v0.10.0) - 2026-08-16

### Added

- *(beam-worker)* 用 env 与 cgroupSlice 替换 wrapper 启动

### Other

- *(main)* 合入 grok adapter，保留 cgroupSlice 启动模块

## [0.9.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.8.0...beam-daemon-v0.9.0) - 2026-08-08

### Added

- group-chat custom triggers with per-trigger prompt, dir and ack

### Fixed

- *(daemon)* 触发词按会话锚点激活，话题群新话题可独立触发
- *(daemon)* 活跃会话存在时触发词不再注入 prompt/ack
- *(daemon)* 清理拆分测试文件后的 unused import 警告
- *(daemon)* 拆分超长源文件，通过行数上限 harness 检查
- *(daemon)* parse string-form group member counts in multi-bot gate

### Other

- Merge pull request #55 from linuxhenhao/feat/custom-triggers
- 全仓清理 clippy 警告至清零
- 全仓 rustfmt 格式化，消除历史格式遗留
- 修复 custom-triggers 分支引入的 rustfmt 格式问题

## [0.8.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.7.4...beam-daemon-v0.8.0) - 2026-08-07

### Fixed

- *(daemon)* worker 假死自愈、zellij 调用超时与可观测性补齐

## [0.7.4](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.7.3...beam-daemon-v0.7.4) - 2026-07-28

### Fixed

- *(daemon)* beam send 标记当前 turn，修复飞书重复回复卡片

## [0.7.3](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.7.2...beam-daemon-v0.7.3) - 2026-07-28

### Added

- *(daemon)* adopt list 改用富文本代码块回复并容错多行复制

### Other

- Merge pull request #47 from linuxhenhao/feat/adopt-list-rich-text

## [0.7.2](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.7.1...beam-daemon-v0.7.2) - 2026-07-20

### Added

- *(daemon)* 本地 API token 每日轮换与 HMAC 请求签名

### Fixed

- *(worker)* resume 路径补齐 env wrapper，杜绝 BEAM_SESSION_ID 跨会话污染

### Other

- Merge pull request #45 from linuxhenhao/fix/session-env-isolation

## [0.7.1](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.7.0...beam-daemon-v0.7.1) - 2026-07-17

### Added

- *(worker)* 新增 kimi-code CLI 适配器

### Other

- *(beam-worker)* CLI adapter 改为 trait + 注册表架构

## [0.7.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.6.4...beam-daemon-v0.7.0) - 2026-07-16

### Added

- *(logging)* 优化运行与排障日志分级
- *(screenshot)* 优化事件驱动截图刷新

### Other

- Merge pull request #40 from linuxhenhao/feat/event-driven-screenshot-refresh

## [0.6.3](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.6.2...beam-daemon-v0.6.3) - 2026-07-15

### Fixed

- screenshot_delay/cli startup args/dir choose card fixes

### Other

- *(daemon)* 格式化拆分模块
- *(daemon)* 拆分Zellij Web模块
- *(daemon)* 拆分终端代理模块
- *(daemon)* 拆分路由处理模块
- *(daemon)* 拆分工作流取消模块
- *(daemon)* 拆分工作流恢复模块
- *(daemon)* 拆分工作流命令模块
- *(daemon)* 拆分工作流协调模块
- *(daemon)* 拆分Lark入口模块
- *(daemon)* 拆分最终输出模块
- *(workspace)* 拆分首批 Rust 源文件模块

## [0.6.2](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.6.1...beam-daemon-v0.6.2) - 2026-07-14

### Fixed

- *(daemon)* 优化飞书回复提醒随机化

## [0.6.1](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.6.0...beam-daemon-v0.6.1) - 2026-07-14

### Fixed

- *(lark)* 优化消息 turn 卡片交接
- *(zellij)* 重启陈旧 web 服务并等待 pane 就绪

## [0.6.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.5.1...beam-daemon-v0.6.0) - 2026-07-13

### Added

- *(daemon)* 支持外部终端入口候选

## [0.5.1](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.5.0...beam-daemon-v0.5.1) - 2026-07-08

### Fixed

- *(terminal)* 修复 zellij web 就绪检测

## [0.5.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.4.0...beam-daemon-v0.5.0) - 2026-07-06

### Added

- *(adopt)* 支持 OpenCode adopt 解析与首条消息 beam context 注入
- *(send)* 对齐 Rust send 语义
- *(lark)* 支持中英文提示和卡片文案

### Fixed

- *(beam-daemon)* 为卡片补充多语言支持

### Other

- *(beam-daemon)* 拆分 lib.rs 为模块化结构并归位测试
- *(deps)* 更新 Rust 依赖

## [0.4.0](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.3.3...beam-daemon-v0.4.0) - 2026-07-01

### Added

- *(beam)* 添加 Traex CLI 支持，支持 cliArgs 和跳过工作目录选择

## [0.3.3](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.3.2...beam-daemon-v0.3.3) - 2026-06-30

### Fixed

- *(opencode)* 修复权限确认回填
- *(terminal-proxy)* anchor 发送 TermnalResize 设默认 160×50，viewer 断开后 debounce 复位

### Other

- Merge pull request #22 from linuxhenhao/fix/dump_screen

## [0.3.2](https://github.com/linuxhenhao/beam/compare/beam-daemon-v0.3.1...beam-daemon-v0.3.2) - 2026-06-27

### Fixed

- *(terminal-proxy)* 使用 dump-screen --full 替代 viewport capture，移除 card viewport 裁剪逻辑
- *(daemon)* 修复目录选择下拉渲染
- *(terminal-proxy)* 移除 anchor 多余 resize/metrics，截图不再裁剪到 card viewport

### Other

- Merge pull request #21 from linuxhenhao/fix/dump_screen

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
