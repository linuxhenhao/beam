# 群内授权（`/grant` · `/revoke` · 授权卡片）设计

日期：2026-05-20
状态：v3 — 已纳入 Codex review round-1（8 项）+ round-2（5 项）全部边界（见文末处置表）

## 背景与动机

beam 的使用权限由每个 bot 的 `allowedUsers`（`bots.json` 中的字符串数组）控制：
启动时把 email 前缀解析成 `open_id` 写入内存 `resolvedAllowedUsers`，`canTalk` /
`canOperate` 两个闸门都查它。

**痛点**：要给新成员开权限，必须在启动前拿到对方的 `open_id`，而 `open_id` 无法
从 email 直接查到。于是「给别人加权限」非常不便。

**核心洞察**：飞书消息里只要出现 `@某人`，那条 mention 就**自带对方的 `open_id`**
（`message.mentions[].id.open_id`）。因此「在群里 @ 一下就能授权」天然绕开 email→open_id
的查询，这是本方案成立的根基。

## 目标

1. 新增 `/grant`（授权）、`/revoke`（撤销）两个群内命令，**仅 owner 可用**。
2. 支持两种授权范围：**授权本群**、**全局授权**。
3. 一张「授权卡片」，两种入口都弹它：
   - **入口 A（自助申请）**：无权限者 @机器人 时，不再静默/回「无操作权限」，而是自动弹卡片并 @owner。
   - **入口 B（owner 主动）**：owner 发 `/grant @张三`，弹同一张卡片，owner 直接点范围按钮。
4. 变更即时生效（同步内存），并持久化到 `bots.json`（重启保留）。

## 非目标

- 不做基于角色/分组的细粒度 RBAC，只有「本群」「全局」两档。
- 不引入 per-chat owner 列表；owner 始终等于 bot 的 `resolvedAllowedUsers[0]`（首个 `ou_`）。
- 不改动 oncall 的 chat 级开放语义；本方案是 per-user 授权，与 oncall 正交叠加。

## 术语：谁是 owner

owner = bot 的首个已授权用户，即 `resolvedAllowedUsers.find(u => u.startsWith('ou_'))`，
与现有「缺权限警告私信对象」（`bot-registry.ts:120`）同一口径。新增 `getOwnerOpenId(larkAppId)`
封装这一查询，全程复用。

**开放模式特例**：当 `allowedUsers` 为空时，现有语义是「所有人可用」。此时没有 owner，
也无需授权——`/grant` / `/revoke` 直接回一句「当前未设置 allowedUsers，所有人可用，无需授权」，
入口 A 的卡片也不触发。

## 数据模型

### 全局授权
复用现有机制：把 `open_id` 追加进 `bots.json` 对应 bot 条目的 `allowedUsers`（去重），
并同步追加到内存 `resolvedAllowedUsers`。

### 本群授权（新增）
`BotConfig` 新增字段：

```ts
/** Per-chat per-user grants: chat_id → 被授权的 open_id 列表。
 *  与全局 allowedUsers 正交：命中任一即放行。 */
chatGrants?: { [chatId: string]: string[] };
```

`BotState` 不新增字段——`chatGrants` 直接读 `bot.config.chatGrants`（与 oncall 的
`oncallChats` 一样走 in-memory config）。

**⚠️ 必须进 `parseBotConfigFile` 白名单**：`bot-registry.ts:278-292` 用字段白名单重建
`BotConfig`，未列出的字段重启后被丢弃。所以除了加 interface 字段，还要在解析处显式读取、
**校验过滤**（只保留 `chatId: string` → `string[]`，逐项 `typeof === 'string'`）并回填
`chatGrants`，否则 grant-store 写入 `bots.json` 后重启不进内存。

### 授权范围语义（Codex review #2 — 关键澄清）
**「授权本群」= 仅放行「与机器人对话/喂 prompt」，不授予 daemon 管理命令权。**

理由：`chatGrants` 若同时进 `canOperate`，本群授权用户就能跑 `/cd`、`/restart`、`/repo`、
`/schedule`，尤其 `/oncall bind`（把整个群对所有人开放）、`/adopt`、`get_write_link`（写终端
链接）——这比「让某人在本群用机器人」大一档，是权限膨胀。用户原话是「加使用权限/授权人」，
语义就是「能用」，管理权应保留给 owner / 全局 allowedUsers。

因此：
- `chatGrants` **只进 `canTalk`，绝不进 `canOperate`**。
- 卡片敏感动作已走 `canOperate`（`card-handler.ts:152`），故自动不受 chatGrant 影响——
  只要不把 chatGrant 加进 canOperate 即可。
- **但 daemon 命令路径在非 oncall 群没查 canOperate**（`daemon.ts:455-464` / `760-768` 仅在
  `isChatOncallBoundForAnyBot` 时才查）。chat-granted 用户能过 `canTalk` 到达命令分支，存在缺口。
  **修复**：把这两处的 oncall 前置条件去掉，改为**所有群**的 daemon 命令都要求 `canOperate`
  （对现有 allowedUsers 用户是 no-op，因为他们本就过 canOperate；只挡住 chat-granted 用户）。

### 闸门改动
仅 `canTalk` 增加一条放行规则（`canOperate` **不动**）：

```ts
function hasChatGrant(larkAppId, chatId, openId): boolean {
  return !!chatId && !!openId &&
    !!getBot(larkAppId).config.chatGrants?.[chatId]?.includes(openId);
}
```

- `canTalk`：oncall 放行 → known peer bot 放行 → `allowedUsers` 命中 → **`chatGrants` 命中** → 否则拒。
- `canOperate`：维持现状（`allowedUsers` 命中），**不加 chatGrants**。
- daemon 命令路径：去掉 oncall 前置，统一要求 `canOperate`（见上）。

注意：开放模式（`allowedUsers` 为空）下闸门本就返回 `true`，`chatGrants` 不影响。

## 持久化层：`grant-store.ts`

镜像 `oncall-store.ts` 的并发安全写法（`withFileLock` + 原子 rename + 内存同步）。

**先做一个小重构**：把 `oncall-store.ts` 里私有的 `rmwBotEntry` / `readRawConfig` /
`writeRawConfigAtomic` / `findEntryIndex` / `requireConfigPath` 抽到共享模块
`src/services/config-store.ts`，`oncall-store.ts` 与新 `grant-store.ts` 都从它 import。
（纯提取，不改行为；让两个 store 共享同一把跨进程文件锁。）

**⚠️ 文件权限（Codex review #5）**：`bots.json` 含 `appSecret`，setup 写它用 `0o600`
（`bots-store.ts:9-13`），但 oncall-store 现有 `writeRawConfigAtomic` 的临时文件没指定 mode
（`oncall-store.ts:29-32`），rename 后会把 `bots.json` 落成 umask 默认权限（可能 0644）。
抽取时**顺手修掉**：temp 文件以 `{ mode: 0o600 }` 写入，并在 rename 前 `fchmod`/写入即正确，
保证最终文件保持 `0o600`。（这是抽取附带的安全修复，不是行为回归。）

`grant-store.ts` 暴露：

```ts
// 全局
addGlobalGrant(larkAppId, openId): Promise<{ok:true; created:boolean} | {ok:false; reason}>
// 本群
addChatGrant(larkAppId, chatId, openId): Promise<{ok:true; created:boolean} | {ok:false; reason}>
// 撤销（原子，见 round-2 #4）
revokeGrant(larkAppId, chatId, openId): Promise<{ok:true; removed:{chat:boolean; global:boolean}} | {ok:false; reason}>
```

写函数：`rmwBotEntry` 改 `bots.json` → 成功后同步内存（`resolvedAllowedUsers` 或
`config.chatGrants`）→ `logger.info`。

**⚠️ email allowedUsers 撤销不彻底（round-2 #2 — High）**：启动时 `allowedUsers` 里的 email
只在内存解析成 `resolvedAllowedUsers`，**`bots.json` 里仍是 email 字符串**
（`daemon.ts:1128-1137`，`client.ts` 的 `resolveAllowedUsers`）。`/revoke @user` 拿到的是
open_id，直接从 raw `allowedUsers` 删 open_id **删不掉 email 条目**，重启后权限回来。
**修复**：启动解析时维护一张 `rawAllowedUserResolution: Map<rawEntry, resolvedOpenId>`
（挂在 `BotState`）；撤销全局时按「resolved open_id == target」反查并删除对应 raw 条目（无论它
是 email 还是 open_id）。这样彻底撤销对 email-配置 的全局用户同样成立。

**⚠️ 防误删致开放模式（round-1 #1 + round-2 #3）**：开放模式（`allowedUsers` 空 → 全员可用）
与「无人授权」共用空数组，语义相反。守卫**必须按运行时 `resolvedAllowedUsers` 判断，不能只看
raw `allowedUsers.length`**——因为 raw 里可能剩的是解析失败的 email，运行时 resolved 仍会变空 →
被现有 `canTalk/canOperate` 当成开放。
**守卫条件**：移除目标后，下一版 `resolvedAllowedUsers` 仍至少有一个非目标 open_id（owner/global
user）；否则拒绝并返回 `reason:'would_open_bot'`。同样禁止撤销当前 owner 的全局授权。
（将来若要支持「彻底锁死无人可用」，需引入显式 `restricted:true` 状态，本期不做。）

**⚠️ revoke 必须原子（round-2 #4）**：不要顺序调用「删 chat + 删 global」两次 RMW（两次拿锁，
中途崩溃/并发会留半撤销态）。改为单个 `revokeGrant`，在**同一个 `rmwBotEntry` critical section**
里同时删 chat grant 与 global（含上面 email 反查与 would_open_bot 守卫），写完一次性同步内存。

**revoke 语义**：`/revoke @user` = 原子「彻底撤销」本群 + 全局，回执说明实际移除范围
（本群/全局/无/被守卫拒绝 would_open_bot）。受守卫约束：不会把全局清空。

**⚠️ revoke 不清理历史副作用（Codex review #8）**：被撤销用户此前用 `/schedule` 建的定时任务
不会被 revoke 停掉（schedule task 当前无 creator open_id、无运行时权限复查，
`schedule-store.ts:126-168`）。本期**明确不处理**，仅在回执/文档说明；如需联动需另加 creator
字段与撤销时禁用策略（独立任务）。

## 命令层：`im/lark/grant-command.ts`

`/grant`、`/revoke` 是**元命令**，必须在 dispatcher 路由/spawn 之前拦截，否则会被当成 prompt
喂给 CLI 会话。但**不能照搬 `/introduce` 的「无条件拦截」**（Codex review #3）：`/introduce`
（`event-dispatcher.ts:779-783`）有意让每个被 @ 的 bot 各自记录 mentions，所以无条件；而 `/grant`
若裸发，在多 bot 群里可能被多个 daemon 重复处理，或（若飞书只推 @bot 消息）根本收不到。

**修复**：
- 入口 B 固定为 **`@bot /grant @user`**——拦截时先 `isBotMentioned(larkAppId, message, senderOpenId)`
  确认本 bot 被 @，否则不处理（p2p / 单 bot 群可放宽）。
- 解析 target mention 时**排除 bot 自己的 open_id**（不能直接取 `message.mentions[0]`，否则会把
  被 @ 的机器人自己当成授权对象）。取第一个非本 bot 的人类 mention。

新增 `tryHandleGrantCommand(larkAppId, message, senderOpenId, chatId, ...)`，在 introduce
拦截之后调用；命中且本 bot 被 @ 时处理并返回 `true`（短路）。

### `/grant`
- 解析文本（容忍 `@_user_N` 占位符 → 从 `message.mentions` 取 `open_id`，与 message-parser 同款解析）。
- **owner 闸门**：`senderOpenId !== getOwnerOpenId(larkAppId)` → 回「仅 owner 可授权」。
- 无 mention（`/grant` 单发）→ 回用法提示。
- 有 mention（`/grant @张三`）→ 弹**授权卡片**（owner 发起态），owner 点范围按钮完成。
- （可选增强，先不做）`/grant @张三 here` / `/grant @张三 global` 直接授权跳过卡片。

### `/revoke`
- 同样 owner 闸门 + mention 解析。
- `/revoke @张三` → 调用彻底撤销，回执说明移除范围。直接执行，不弹卡片。
- 同时把该用户从入口 A 的「pending 节流表」里清掉（见下）。

### 命令注册
- `DAEMON_COMMANDS`（command-handler.ts:29）**不加** `/grant` `/revoke`——它们走 dispatcher 拦截，不进 command-handler 的 session 分支。
- 但需确保 dispatcher 的 `/grant` `/revoke` 拦截在「命中 daemon 命令」判断之前，避免误入 CLI。

## 授权卡片

复用 `card-builder.ts` 的卡片构造风格，新增 `buildGrantCard(...)`：

- 文案：「用户 @<申请人> 申请使用我，请 @<owner> 选择授权范围」（卡片正文 mention owner，
  保证 owner 收到红点）。
- 按钮三枚，`value` 各带 action + 上下文 + **nonce**：
  - `[ 授权本群 ]` → `{ action: 'grant_chat', target_open_id, chat_id, nonce }`
  - `[ 全局授权 ]` → `{ action: 'grant_global', target_open_id, chat_id, nonce }`
  - `[ 拒绝 ]` → `{ action: 'grant_deny', target_open_id, chat_id, nonce }`
- 入口 A 与入口 B 用同一张卡，仅文案前缀略不同（「申请使用」vs「请选择对 @X 的授权范围」）。

**⚠️ nonce 防旧卡重放（Codex review #6 — 关键）**：发卡时生成随机 `nonce`，写进 pending 表
（key=`bot:chat:target`，value 含 nonce）。card-handler 处理 grant action 前先校验
**pending 仍存在且 nonce 匹配**；否则只 toast「该授权请求已失效」。这样 `/revoke` 清 pending、
或 daemon 重启清空内存表后，**旧卡片点击一律失效**——owner 误点过期卡不会重新授权。内存表
重启重置在这里反而是安全特性。

### 卡片点击处理（card-handler.ts）

在 `handleCardAction` **靠前**处理这三个 action（在现有 session 解析逻辑之前），
因为它们不绑定 DaemonSession（无 `root_id`/`ds`）：

1. **owner 闸门（强）**：必须用**当前 app** 的 `operator.open_id === getOwnerOpenId(larkAppId)`
   → 否则 toast「仅 owner 可操作」，不改任何状态。比现有 `isSensitive` 的 `canOperate` 更严。
2. **nonce 校验**：pending 表里该 `(bot,chat,target)` 仍存在且 nonce 匹配 → 继续；否则 toast
   「该授权请求已失效」（旧卡 / revoke 后 / 重启后）。
3. `grant_chat` → `addChatGrant`；`grant_global` → `addGlobalGrant`；`grant_deny` → 不授权。
4. 三种都更新卡片为终态（「✅ 已授权本群 / ✅ 已全局授权 / 🚫 已拒绝」），按钮置灰/移除，避免重复点击。
5. 清理该 `(bot,chat,target)` 的 pending 记录。

## repo 选择卡的权限边界（round-2 #1 — High）

v2 把 daemon 命令统一收到 `canOperate`，于是 `/repo` 命令、`skip_repo` 卡片动作（已在敏感列表，
`card-handler.ts:130`）都只剩 owner/global 可用。但 **repo 下拉选择本身当前不在敏感动作里**
（`card-handler.ts:707-759` 完全没有权限闸门），任何点卡人都能选 repo、甚至 mid-session 切 repo。
这会造成两个问题：
1. **一致性漏洞**：管理级的「切 repo」命令被挡，但等效的下拉却人人可点。
2. **新坑**：chat-granted 用户开新话题、CLI 尚未 spawn 时会先收到 repo 选择卡（pendingRepo，
   `daemon.ts:524-615`）。若把下拉也按 `canOperate` 收死，被授权人**连自己的首次会话都启动不了**。

**修复——把 repo 选择拆成「会话本地使用权」vs「管理级切换」**：
- **pendingRepo 阶段**（首次选 repo 才能 spawn）：放行 `operator === session.ownerOpenId || canOperate`。
  让发起话题的 chat-granted 用户能完成自己的首次使用；`skip_repo` 维持敏感（保持现状）。
- **非 pending 的 mid-session repo 切换**：要求 `canOperate`（等同 `/repo` 的管理语义）。

测试补三条：pending 下拉（session owner 可选）、pending skip（仍需 canOperate）、mid-session 切换（需 canOperate）。

## 入口 A：无权限者自助申请

改 `event-dispatcher.ts:884` 的 `access === 'not_allowed'` 分支：原本回「⚠️ 无操作权限」，改为：

- 若**开放模式**（无 owner）→ 维持原逻辑（理论上开放模式不会进 not_allowed，但兜底保留）。
- 否则：发**授权卡片**（@owner，申请人 = `senderOpenId`），代替「无操作权限」文本。

**⚠️ 覆盖 ownsSession 场景（Codex review #7）**：现有逻辑在 `ownsSession === true` 时连
「无操作权限」都不回（`event-dispatcher.ts:884-888`），会漏掉「普通群已有 chat-scope session、
无权限者来 @ 申请」的场景。目标是「无权限者 @机器人就弹申请卡」，所以 `access === 'not_allowed'`
**无论 ownsSession 真假都走节流+卡片**；只是**绝不把该消息送进已有 session**（不喂 prompt）。

### 节流（必须）
避免无权限者每发一句就刷一张卡。用**内存** Map（与 nonce pending 表合一）：

```ts
key = `${larkAppId}:${chatId}:${requesterOpenId}`
value = { state: 'pending' | 'denied', nonce, ts }
```

- `state==='pending'`（卡已发、owner 未处置）或 `state==='denied'` 冷却窗口内（10 分钟）→ 静默不再发。
- **deny 不清记录，而是转 `denied` 冷却态（round-2 #5）**：否则恶意用户可在 owner 点「拒绝」后
  立刻再 @bot 刷新卡，绕过节流。冷却期满才允许再次申请。
- **授权成功 / `/revoke`** → 清除该 key（授权后无需再申请；revoke 后允许重新走流程）。
- 仅内存（daemon 重启后重置可接受——重启后旧卡 nonce 失效见上，第一条会重新弹卡，符合直觉）。

## 模块清单

| 文件 | 改动 |
| --- | --- |
| `src/services/config-store.ts` | **新增**：从 oncall-store 提取的共享 rmw/锁/IO helper；temp 写入保 `0o600`（R1#5） |
| `src/services/oncall-store.ts` | 改为 import 共享 helper（纯重构） |
| `src/services/grant-store.ts` | **新增**：`addGlobalGrant`/`addChatGrant`/**原子 `revokeGrant`**（R2#4）；防清空守卫按 `resolvedAllowedUsers` 判定（R1#1+R2#3）；email 条目反查删除（R2#2） |
| `src/bot-registry.ts` | `BotConfig.chatGrants`；`getOwnerOpenId()`；**`parseBotConfigFile` 白名单解析+过滤 `chatGrants`**（R1#4）；**`rawAllowedUserResolution` map**（R2#2） |
| `src/im/lark/event-dispatcher.ts` | `canTalk` 加 chatGrants 放行（**`canOperate` 不动**, R1#2）；not_allowed 分支改弹卡片+节流，覆盖 ownsSession（R1#7）；grant-command 拦截（要求 isBotMentioned, R1#3） |
| `src/daemon.ts` | 去掉 daemon 命令路径（`455-464`/`760-768`）的 oncall 前置，**所有群** daemon 命令统一要求 `canOperate`（R1#2） |
| `src/im/lark/grant-command.ts` | **新增**：`tryHandleGrantCommand`（/grant、/revoke）；isBotMentioned 守卫 + 排除 bot 自身 mention（R1#3） |
| `src/im/lark/card-builder.ts` | **新增**：`buildGrantCard`（按钮带 nonce, R1#6） |
| `src/im/lark/card-handler.ts` | 处理 `grant_*`：owner 强闸门 + nonce 校验（R1#6），在 session 解析之前；**repo 下拉拆使用权/管理权**：pendingRepo 放行 `owner||canOperate`、mid-session 切换要 `canOperate`（R2#1） |
| `src/im/lark/grant-pending.ts` | **新增**：内存 pending/denied 表（key=`bot:chat:target`→{state,nonce,ts}），nonce 生成/校验/清除 + deny 冷却（R1#6, R2#5） |
| `src/i18n/zh.ts` `en.ts` | 命令回执、卡片、toast、失效提示文案 |
| `src/core/command-handler.ts` `/help` | 文档里补 `/grant` `/revoke` 说明 |

## 测试要点

- `grant-store`：add/remove 全局与本群，去重、幂等、内存与 `bots.json` 同步、并发锁（与 oncall 同款）。
- **R1#1**：删到只剩最后一个 / owner 时被守卫拒绝（`would_open_bot`），bot 不变开放。
- **R1#2**：`chatGrants` 命中只过 `canTalk` 不过 `canOperate`；chat-granted 用户在非 oncall 群跑 `/cd`/`/oncall bind` 被 `canOperate` 挡；现有 allowedUsers 用户不受影响（回归）。
- **R1#3**：裸 `/grant @x`（未 @bot）不被处理；`@bot /grant @x` 生效；mention 解析排除 bot 自身。
- **R1#4**：写入 `chatGrants` → 重启 → `parseBotConfigFile` 正确回填进内存。
- **R1#5**：写 `bots.json` 后文件权限仍是 `0o600`。
- **R1#6**：旧卡 / revoke 后 / 重启后点击授权 → nonce 不匹配 → toast 失效，不重新授权。
- **R1#7**：not_allowed 在 ownsSession=true 时也弹卡，且消息不进 session。
- **R2#1**：pendingRepo 下拉 session owner 可选并 spawn；pending skip 仍需 canOperate；mid-session 切 repo 需 canOperate。
- **R2#2**：用 email 配在 allowedUsers 的用户 `/revoke` 后**重启权限不回来**（raw 条目被反查删除）。
- **R2#3**：raw 里残留解析失败 email、resolved 仅剩目标时，撤销被 `would_open_bot` 拒。
- **R2#4**：`revokeGrant` 单次 RMW 同删 chat+global（原子，无半撤销态）。
- **R2#5**：owner 点拒绝后立刻再 @bot → 命中 denied 冷却，不刷新卡；冷却期满才再弹。
- 闸门跨 chat 不串；开放模式不受影响。
- 命令解析：`/grant @x`、`/revoke @x`、无 mention、非 owner 调用被拒。
- 卡片点击：非 owner（非 owner 本人）点击被拦（toast）；三种 action 终态正确；pending 清除。
- 入口 A：not_allowed → 弹卡（@owner）；同人重复发不刷屏；revoke 后可再次申请。

## 待评审决策点（已与用户确认）

1. 命令名：`/grant` ✔
2. 谁能批准卡片：**仅 owner**（当前 app 的 `operator.open_id === getOwnerOpenId`）✔
3. 撤销：`/revoke @xx`（彻底撤销本群+全局，但受 #1 守卫不清空全局）✔

## Codex review round-1 处置（基于 a2cb248，全部采纳）

| # | 级别 | 处置 |
| --- | --- | --- |
| 1 | Critical | removeGlobalGrant 守卫：不允许删到空 / 删 owner，避免「空=开放」反转 |
| 2 | High | chatGrant 只进 canTalk；daemon 命令统一要求 canOperate（去掉 oncall 前置） |
| 3 | High | /grant 拦截要求 isBotMentioned；mention 解析排除 bot 自身 |
| 4 | Medium | chatGrants 进 parseBotConfigFile 白名单解析+过滤 |
| 5 | Medium | config-store temp 写入保 0o600 |
| 6 | Medium | 授权卡带 nonce，card-handler 校验 pending+nonce，旧卡失效 |
| 7 | Med/Low | not_allowed 覆盖 ownsSession 场景，但不喂 session |
| 8 | Low | 明确记录：revoke 不停历史 schedule（本期不联动） |

**唯一产品决策（R1#2 语义）**：「授权本群」= 仅对话使用，不含 daemon 管理命令权——管理权保留
owner / 全局 allowedUsers。符合用户「加使用权限」的原意。

## Codex review round-2 处置（基于 v2 spec，全部采纳）

| # | 级别 | 处置 |
| --- | --- | --- |
| 1 | High | repo 下拉拆「会话本地使用权 / 管理权」：pendingRepo 放行 `owner‖canOperate`，mid-session 切换要 canOperate |
| 2 | High | email 配的全局用户撤销不彻底 → BotState 维护 resolution map，按 resolved open_id 反查删 raw 条目 |
| 3 | Medium | would_open_bot 守卫按运行时 `resolvedAllowedUsers` 判定，不只看 raw 长度（防解析失败 email 残留） |
| 4 | Medium | revoke 原子化：单个 `revokeGrant` 在同一 rmwBotEntry 内同删 chat+global |
| 5 | Low/Med | deny 不清记录，转 denied 冷却态（10min），防 owner 拒绝后被立刻刷卡绕过节流 |
