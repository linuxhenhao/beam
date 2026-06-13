# 群内授权（/grant · /revoke · 授权卡片）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 owner 在飞书群里通过 `@bot /grant @某人` 或无权限者自助申请卡片，给人添加「本群使用 / 全局」两档权限，并支持 `/revoke @某人` 彻底撤销；全程绕开 email→open_id 查询。

**Architecture:** 复用现有 `allowedUsers`（全局）+ 新增 `chatGrants`（per-chat per-user，仅放行 `canTalk`，不给管理命令权）。持久化镜像 `oncall-store` 的 `withFileLock` + 原子写。授权卡片带 nonce、owner 强闸门、内存 pending/denied 表防重放与刷屏。命令与卡片在 dispatcher 层拦截，不进 CLI 会话。

**Tech Stack:** TypeScript (ESM, NodeNext)、Vitest、飞书 `@larksuiteoapi/node-sdk` 交互卡片。

**设计依据:** `docs/superpowers/specs/2026-05-20-grant-permission-design.md`（v3，含 Codex review round-1/2 全部处置）。

**通用约定:**
- 每个 task 跑 `pnpm build`（tsc 严格）确认无类型错误；测试 `pnpm vitest run <file>`。
- commit message: `type(scope): 中文描述`（见 CLAUDE.md）。
- 测试文件放 `test/` 下，命名 `<topic>.test.ts`，沿用现有 vitest 习惯（参考 `test/` 现有用例）。

---

## Task 1: 抽取共享 `config-store`，并修临时文件权限 0o600（R1#5）

**Files:**
- Create: `src/services/config-store.ts`
- Modify: `src/services/oncall-store.ts`（改为 import 共享 helper）
- Test: `test/config-store.test.ts`

把 `oncall-store.ts` 里私有的 `readRawConfig` / `writeRawConfigAtomic` / `findEntryIndex` / `requireConfigPath` / `rmwBotEntry` 原样搬到 `config-store.ts` 并 export；写临时文件时加 `{ mode: 0o600 }`（`bots.json` 含 appSecret）。`oncall-store.ts` 删掉本地实现改 import。纯重构 + 一处安全修复，行为不变。

- [ ] **Step 1: 写失败测试**

```ts
// test/config-store.test.ts
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, writeFileSync, statSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readRawConfig, writeRawConfigAtomic, findEntryIndex } from '../src/services/config-store.js';

let dir: string; let cfg: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'cfgstore-'));
  cfg = join(dir, 'bots.json');
  writeFileSync(cfg, JSON.stringify([{ larkAppId: 'a1', allowedUsers: ['ou_x'] }], null, 2), { mode: 0o600 });
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

it('writeRawConfigAtomic keeps file 0o600', async () => {
  const raw = await readRawConfig(cfg);
  raw[0].allowedUsers.push('ou_y');
  await writeRawConfigAtomic(cfg, raw);
  expect(statSync(cfg).mode & 0o777).toBe(0o600);
  expect((await readRawConfig(cfg))[0].allowedUsers).toEqual(['ou_x', 'ou_y']);
});

it('findEntryIndex matches by larkAppId', async () => {
  expect(findEntryIndex(await readRawConfig(cfg), 'a1')).toBe(0);
  expect(findEntryIndex(await readRawConfig(cfg), 'nope')).toBe(-1);
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/config-store.test.ts`
Expected: FAIL — `Cannot find module '../src/services/config-store.js'`

- [ ] **Step 3: 创建 `config-store.ts`**

```ts
// src/services/config-store.ts
/**
 * 共享的 bots.json 读改写原语：跨进程文件锁 + 原子 rename。
 * oncall-store 与 grant-store 共用，保证对同一 bots.json 的并发写不丢更新。
 */
import { promises as fsp } from 'node:fs';
import { getLoadedConfigPath } from '../bot-registry.js';
import { withFileLock } from '../utils/file-lock.js';

export async function readRawConfig(path: string): Promise<any[]> {
  const raw = JSON.parse(await fsp.readFile(path, 'utf-8'));
  if (!Array.isArray(raw)) throw new Error(`Config file is not a JSON array: ${path}`);
  return raw;
}

export async function writeRawConfigAtomic(path: string, raw: any[]): Promise<void> {
  const tmp = path + '.tmp.' + process.pid;
  // bots.json 含 appSecret —— 临时文件即以 0o600 写入，rename 后保持私有权限。
  await fsp.writeFile(tmp, JSON.stringify(raw, null, 2) + '\n', { encoding: 'utf-8', mode: 0o600 });
  await fsp.rename(tmp, path);
}

export function findEntryIndex(raw: any[], larkAppId: string): number {
  return raw.findIndex((e: any) => e?.larkAppId === larkAppId);
}

export function requireConfigPath(): string {
  const p = getLoadedConfigPath();
  if (!p) throw new Error('Bot config path unknown — cannot persist config changes');
  return p;
}

/**
 * 在跨进程锁内对某个 bot 条目做 read-modify-write。`mutate` 拿到最新磁盘快照决定
 * 写什么；返回 `{ write:false }` 表示不写。沿用 oncall-store 原有语义。
 */
export async function rmwBotEntry<T>(
  larkAppId: string,
  mutate: (entry: any, raw: any[]) => { write: boolean; result: T } | T,
): Promise<{ ok: true; result: T } | { ok: false; reason: string }> {
  const path = requireConfigPath();
  return withFileLock(path, async () => {
    const raw = await readRawConfig(path);
    const idx = findEntryIndex(raw, larkAppId);
    if (idx < 0) return { ok: false, reason: 'bot_not_in_config' };
    const entry = raw[idx];
    const out = mutate(entry, raw);
    if (out && typeof out === 'object' && 'write' in (out as any)) {
      const wrap = out as { write: boolean; result: T };
      if (wrap.write) await writeRawConfigAtomic(path, raw);
      return { ok: true, result: wrap.result };
    }
    await writeRawConfigAtomic(path, raw);
    return { ok: true, result: out as T };
  });
}
```

- [ ] **Step 4: `oncall-store.ts` 改为 import 共享 helper**

删除 `oncall-store.ts` 中 `readRawConfig`/`writeRawConfigAtomic`/`findEntryIndex`/`requireConfigPath`/`rmwBotEntry` 的本地定义（约 `23-69` 行）及不再需要的 `fsp`/`withFileLock` import，替换为：

```ts
import { readRawConfig, writeRawConfigAtomic, rmwBotEntry } from './config-store.js';
```

保留 `readFileSync` import（`_readRawConfigSyncForTesting` 仍用）。其余函数体不动。

- [ ] **Step 5: 跑测试 + 全量回归确认无回归**

Run: `pnpm vitest run test/config-store.test.ts && pnpm vitest run test/oncall-store.test.ts`
Expected: PASS（若无 oncall-store 测试文件则只跑前者）；再 `pnpm build` 应无类型错误。

- [ ] **Step 6: Commit**

```bash
git add src/services/config-store.ts src/services/oncall-store.ts test/config-store.test.ts
git commit -m "refactor(services): 抽 config-store 共享 rmw/锁,临时文件保 0o600"
```

---

## Task 2: `bot-registry` —— chatGrants 字段 + 解析白名单 + getOwnerOpenId + 解析映射（R1#4, R2#2）

**Files:**
- Modify: `src/bot-registry.ts`
- Test: `test/bot-registry-grant.test.ts`

新增 `BotConfig.chatGrants`、`BotState.rawAllowedUserResolution`（raw 条目 → resolved open_id 的映射，供 revoke 反查 email 条目）、`getOwnerOpenId()`；并把 `chatGrants` 加进 `parseBotConfigFile` 白名单（带过滤），否则重启丢失。

- [ ] **Step 1: 写失败测试**

```ts
// test/bot-registry-grant.test.ts
import { describe, it, expect } from 'vitest';
import { parseBotConfigFile, getOwnerOpenId, registerBot, getBot } from '../src/bot-registry.js';

it('parseBotConfigFile preserves & filters chatGrants', () => {
  const cfgs = parseBotConfigFile(JSON.stringify([{
    larkAppId: 'a1', larkAppSecret: 's',
    chatGrants: { oc_1: ['ou_a', 'ou_b', 123], oc_2: 'bad', oc_3: ['ou_c'] },
  }]));
  expect(cfgs[0].chatGrants).toEqual({ oc_1: ['ou_a', 'ou_b'], oc_3: ['ou_c'] });
});

it('getOwnerOpenId returns first ou_ in resolvedAllowedUsers', () => {
  registerBot({ larkAppId: 'a2', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['x@y.com', 'ou_owner', 'ou_2'] });
  // resolvedAllowedUsers seeded from allowedUsers at registerBot time
  expect(getOwnerOpenId('a2')).toBe('ou_owner');
});
```

> 注：`parseBotConfigFile` 是当前 `loadBotConfigs` 内联解析逻辑（`bot-registry.ts:194-296`）。本 task 先把那段「单条 entry → BotConfig」的解析抽成 **导出的** `parseBotConfigFile(json: string): BotConfig[]`，供测试直接调用；`loadBotConfigs` 读文件后调用它。

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/bot-registry-grant.test.ts`
Expected: FAIL — `parseBotConfigFile`/`getOwnerOpenId` 未导出。

- [ ] **Step 3: 实现**

3a. `BotConfig` 接口加字段（`bot-registry.ts` 接口区）：

```ts
  /** Per-chat per-user grants: chat_id → 被授权的 open_id 列表。仅放行 canTalk，不给管理命令权。 */
  chatGrants?: { [chatId: string]: string[] };
```

3b. `BotState` 加字段：

```ts
  /** raw allowedUsers 条目 → 解析后的 open_id。供 /revoke 反查并删除 email 形式的 raw 条目。 */
  rawAllowedUserResolution: Map<string, string>;
```

`registerBot` 里初始化 `rawAllowedUserResolution: new Map()`（解析在 daemon 启动 resolveAllowedUsers 后回填，见 Task 7 集成；本 task 仅建字段 + 空 Map）。

3c. 新增 `getOwnerOpenId`：

```ts
/** Owner = bot 首个已授权 open_id，与「缺权限警告私信对象」同口径（bot-registry.ts:120）。 */
export function getOwnerOpenId(larkAppId: string): string | undefined {
  return bots.get(larkAppId)?.resolvedAllowedUsers.find(u => u.startsWith('ou_'));
}
```

3d. 把 `loadBotConfigs` 内联的 entry→BotConfig 解析抽成导出函数，并补 `chatGrants` 解析：

```ts
export function parseBotConfigFile(jsonText: string): BotConfig[] {
  const parsed = JSON.parse(jsonText);
  const entries: any[] = Array.isArray(parsed) ? parsed : [parsed];
  const configs: BotConfig[] = [];
  for (const entry of entries) {
    // ... 保留现有 workingDirs / oncallChats / defaultOncall / defaultOncallAutoboundChats 解析 ...

    // chatGrants：只保留 { [chatId:string]: string[] }，逐项 typeof === 'string'
    let chatGrants: { [chatId: string]: string[] } | undefined;
    if (entry.chatGrants && typeof entry.chatGrants === 'object' && !Array.isArray(entry.chatGrants)) {
      const out: { [chatId: string]: string[] } = {};
      for (const [cid, arr] of Object.entries(entry.chatGrants)) {
        if (!Array.isArray(arr)) continue;
        const ids = (arr as any[]).filter((x): x is string => typeof x === 'string');
        if (ids.length > 0) out[cid] = ids;
      }
      if (Object.keys(out).length > 0) chatGrants = out;
    }

    configs.push({
      // ... 现有所有字段 ...
      chatGrants,
    });
  }
  return configs;
}
```

`loadBotConfigs` 改为读文件文本后 `return parseBotConfigFile(text)`（保留原有路径解析/默认值逻辑）。

- [ ] **Step 4: 跑测试确认通过**

Run: `pnpm vitest run test/bot-registry-grant.test.ts && pnpm build`
Expected: PASS + 无类型错误。

- [ ] **Step 5: Commit**

```bash
git add src/bot-registry.ts test/bot-registry-grant.test.ts
git commit -m "feat(bot-registry): chatGrants 字段+白名单解析,getOwnerOpenId,解析映射"
```

---

## Task 3: `grant-store` —— add/revoke + 防开放守卫 + email 反查 + 原子撤销（R1#1, R2#2/#3/#4）

**Files:**
- Create: `src/services/grant-store.ts`
- Test: `test/grant-store.test.ts`

- [ ] **Step 1: 写失败测试**

```ts
// test/grant-store.test.ts
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// 用真实 bots.json + registerBot，依赖 getLoadedConfigPath 指向临时文件。
import * as registry from '../src/bot-registry.js';
import { addGlobalGrant, addChatGrant, revokeGrant } from '../src/services/grant-store.js';

let dir: string; let cfg: string;
function seed(entry: any) {
  writeFileSync(cfg, JSON.stringify([entry], null, 2), { mode: 0o600 });
}
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'grantstore-'));
  cfg = join(dir, 'bots.json');
  vi.spyOn(registry, 'getLoadedConfigPath').mockReturnValue(cfg);
});
afterEach(() => { vi.restoreAllMocks(); rmSync(dir, { recursive: true, force: true }); });

it('addChatGrant persists & syncs in-memory; only affects given chat', async () => {
  seed({ larkAppId: 'a1', larkAppSecret: 's', allowedUsers: ['ou_owner'] });
  registry.registerBot({ larkAppId: 'a1', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['ou_owner'] });
  const r = await addChatGrant('a1', 'oc_1', 'ou_guest');
  expect(r.ok).toBe(true);
  expect(JSON.parse(readFileSync(cfg, 'utf-8'))[0].chatGrants).toEqual({ oc_1: ['ou_guest'] });
  expect(registry.getBot('a1').config.chatGrants).toEqual({ oc_1: ['ou_guest'] });
});

it('revokeGrant refuses to empty resolvedAllowedUsers (would_open_bot)', async () => {
  seed({ larkAppId: 'a2', larkAppSecret: 's', allowedUsers: ['ou_owner'] });
  const bot = registry.registerBot({ larkAppId: 'a2', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['ou_owner'] });
  bot.resolvedAllowedUsers = ['ou_owner'];
  const r = await revokeGrant('a2', 'oc_1', 'ou_owner');
  expect(r).toEqual({ ok: false, reason: 'would_open_bot' });
});

it('revokeGrant atomically removes chat+global for a normal user', async () => {
  seed({ larkAppId: 'a3', larkAppSecret: 's', allowedUsers: ['ou_owner', 'ou_guest'], chatGrants: { oc_1: ['ou_guest'] } });
  const bot = registry.registerBot({ larkAppId: 'a3', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['ou_owner', 'ou_guest'] });
  bot.resolvedAllowedUsers = ['ou_owner', 'ou_guest'];
  bot.config.chatGrants = { oc_1: ['ou_guest'] };
  const r = await revokeGrant('a3', 'oc_1', 'ou_guest');
  expect(r).toEqual({ ok: true, removed: { chat: true, global: true } });
  const disk = JSON.parse(readFileSync(cfg, 'utf-8'))[0];
  expect(disk.allowedUsers).toEqual(['ou_owner']);
  expect(disk.chatGrants).toEqual({});
  expect(bot.resolvedAllowedUsers).toEqual(['ou_owner']);
});

it('revokeGrant deletes email raw entry by resolution map', async () => {
  seed({ larkAppId: 'a4', larkAppSecret: 's', allowedUsers: ['owner@x.com', 'guest@x.com'] });
  const bot = registry.registerBot({ larkAppId: 'a4', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['owner@x.com', 'guest@x.com'] });
  bot.resolvedAllowedUsers = ['ou_owner', 'ou_guest'];
  bot.rawAllowedUserResolution = new Map([['owner@x.com', 'ou_owner'], ['guest@x.com', 'ou_guest']]);
  const r = await revokeGrant('a4', 'oc_1', 'ou_guest');
  expect(r.ok).toBe(true);
  expect(JSON.parse(readFileSync(cfg, 'utf-8'))[0].allowedUsers).toEqual(['owner@x.com']);
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/grant-store.test.ts`
Expected: FAIL — `grant-store.js` 不存在。

- [ ] **Step 3: 实现 `grant-store.ts`**

```ts
// src/services/grant-store.ts
/**
 * 群内授权持久化：全局 allowedUsers（含 email 形式条目）+ per-chat chatGrants。
 * 写路径走 config-store 的跨进程锁；撤销在单个 RMW 内同删 chat+global（原子）。
 */
import { getBot } from '../bot-registry.js';
import { rmwBotEntry } from './config-store.js';
import { logger } from '../utils/logger.js';

type Fail = { ok: false; reason: string };

/** 把 allowedUsers raw 条目映射到 resolved open_id：优先用解析映射，open_id 自身原样。 */
function rawEntryForOpenId(larkAppId: string, openId: string): string | undefined {
  const bot = getBot(larkAppId);
  for (const [raw, resolved] of bot.rawAllowedUserResolution.entries()) {
    if (resolved === openId) return raw;
  }
  // 没有映射时：若 raw 里直接就是该 open_id，返回它自身。
  return bot.config.allowedUsers?.includes(openId) ? openId : undefined;
}

/** 模拟移除目标后运行时 open_id 集合是否仍非空（R2#3：按 resolved 判，不看 raw 长度）。 */
function resolvedAfterRemoval(larkAppId: string, openId: string): string[] {
  return getBot(larkAppId).resolvedAllowedUsers.filter(u => u !== openId);
}

export async function addGlobalGrant(
  larkAppId: string, openId: string,
): Promise<{ ok: true; created: boolean } | Fail> {
  let bot; try { bot = getBot(larkAppId); } catch { return { ok: false, reason: 'bot_not_registered' }; }
  const r = await rmwBotEntry<{ created: boolean }>(larkAppId, (entry) => {
    const cur: string[] = Array.isArray(entry.allowedUsers) ? entry.allowedUsers : [];
    const created = !cur.includes(openId);
    if (created) cur.push(openId);
    entry.allowedUsers = cur;
    return { write: created, result: { created } };
  });
  if (!r.ok) return r;
  if (r.result.created) {
    bot.config.allowedUsers = [...(bot.config.allowedUsers ?? []), openId];
    if (!bot.resolvedAllowedUsers.includes(openId)) bot.resolvedAllowedUsers.push(openId);
    bot.rawAllowedUserResolution.set(openId, openId);
    logger.info(`[grant:${larkAppId}] +global ${openId}`);
  }
  return { ok: true, created: r.result.created };
}

export async function addChatGrant(
  larkAppId: string, chatId: string, openId: string,
): Promise<{ ok: true; created: boolean } | Fail> {
  let bot; try { bot = getBot(larkAppId); } catch { return { ok: false, reason: 'bot_not_registered' }; }
  const r = await rmwBotEntry<{ created: boolean }>(larkAppId, (entry) => {
    const map = (entry.chatGrants && typeof entry.chatGrants === 'object') ? entry.chatGrants : {};
    const cur: string[] = Array.isArray(map[chatId]) ? map[chatId] : [];
    const created = !cur.includes(openId);
    if (created) cur.push(openId);
    map[chatId] = cur;
    entry.chatGrants = map;
    return { write: created, result: { created } };
  });
  if (!r.ok) return r;
  if (r.result.created) {
    const map = (bot.config.chatGrants ??= {});
    map[chatId] = [...(map[chatId] ?? []), openId];
    logger.info(`[grant:${larkAppId}] +chat ${chatId} ${openId}`);
  }
  return { ok: true, created: r.result.created };
}

/**
 * 原子彻底撤销：同一 RMW 内删 chatGrants[chatId] 与全局 allowedUsers（email 反查）。
 * 守卫：移除全局后运行时 resolvedAllowedUsers 不能变空，否则 would_open_bot。
 */
export async function revokeGrant(
  larkAppId: string, chatId: string, openId: string,
): Promise<{ ok: true; removed: { chat: boolean; global: boolean } } | Fail> {
  let bot; try { bot = getBot(larkAppId); } catch { return { ok: false, reason: 'bot_not_registered' }; }

  const rawEntry = rawEntryForOpenId(larkAppId, openId); // email 或 open_id 或 undefined
  const willRemoveGlobal = !!rawEntry;
  if (willRemoveGlobal && resolvedAfterRemoval(larkAppId, openId).length === 0) {
    return { ok: false, reason: 'would_open_bot' };
  }

  const r = await rmwBotEntry<{ chat: boolean; global: boolean }>(larkAppId, (entry) => {
    let chat = false, global = false;
    const map = (entry.chatGrants && typeof entry.chatGrants === 'object') ? entry.chatGrants : {};
    if (Array.isArray(map[chatId]) && map[chatId].includes(openId)) {
      map[chatId] = map[chatId].filter((u: string) => u !== openId);
      if (map[chatId].length === 0) delete map[chatId];
      chat = true;
    }
    entry.chatGrants = map;
    if (rawEntry && Array.isArray(entry.allowedUsers) && entry.allowedUsers.includes(rawEntry)) {
      entry.allowedUsers = entry.allowedUsers.filter((u: string) => u !== rawEntry);
      global = true;
    }
    return { write: chat || global, result: { chat, global } };
  });
  if (!r.ok) return r;

  // 同步内存
  if (r.result.chat && bot.config.chatGrants?.[chatId]) {
    bot.config.chatGrants[chatId] = bot.config.chatGrants[chatId].filter(u => u !== openId);
    if (bot.config.chatGrants[chatId].length === 0) delete bot.config.chatGrants[chatId];
  }
  if (r.result.global) {
    if (rawEntry) {
      bot.config.allowedUsers = (bot.config.allowedUsers ?? []).filter(u => u !== rawEntry);
      bot.rawAllowedUserResolution.delete(rawEntry);
    }
    bot.resolvedAllowedUsers = bot.resolvedAllowedUsers.filter(u => u !== openId);
  }
  logger.info(`[grant:${larkAppId}] revoke chat=${chatId} ${openId} removed=${JSON.stringify(r.result)}`);
  return { ok: true, removed: r.result };
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `pnpm vitest run test/grant-store.test.ts && pnpm build`
Expected: PASS + 无类型错误。

- [ ] **Step 5: Commit**

```bash
git add src/services/grant-store.ts test/grant-store.test.ts
git commit -m "feat(services): grant-store —— add/revoke,防开放守卫,email 反查,原子撤销"
```

---

## Task 4: 闸门接入 —— canTalk 认 chatGrants；daemon 命令统一要 canOperate（R1#2）

**Files:**
- Modify: `src/im/lark/event-dispatcher.ts:486-498`（canTalk）
- Modify: `src/daemon.ts:461`、`src/daemon.ts:765`（去掉 oncall 前置）
- Test: `test/grant-gates.test.ts`

- [ ] **Step 1: 写失败测试**

```ts
// test/grant-gates.test.ts
import { describe, it, expect } from 'vitest';
import { registerBot, getBot } from '../src/bot-registry.js';
import { canTalk, canOperate } from '../src/im/lark/event-dispatcher.js';

it('chatGrant grants canTalk but NOT canOperate', () => {
  const bot = registerBot({ larkAppId: 'g1', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['ou_owner'] });
  bot.resolvedAllowedUsers = ['ou_owner'];
  bot.config.chatGrants = { oc_1: ['ou_guest'] };
  expect(canTalk('g1', 'oc_1', 'ou_guest')).toBe(true);   // 本群可对话
  expect(canTalk('g1', 'oc_2', 'ou_guest')).toBe(false);  // 跨群不串
  expect(canOperate('g1', 'oc_1', 'ou_guest')).toBe(false); // 不给管理权
  expect(canOperate('g1', 'oc_1', 'ou_owner')).toBe(true);
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/grant-gates.test.ts`
Expected: FAIL — `canTalk('g1','oc_1','ou_guest')` 返回 false（chatGrant 未接入）。

- [ ] **Step 3: 实现**

3a. `event-dispatcher.ts` 加 helper 并改 `canTalk`：

```ts
function hasChatGrant(larkAppId: string, chatId: string | undefined, openId: string | undefined): boolean {
  return !!chatId && !!openId && !!getBot(larkAppId).config.chatGrants?.[chatId]?.includes(openId);
}

export function canTalk(larkAppId: string, chatId: string | undefined, senderOpenId: string | undefined): boolean {
  if (chatId && isChatOncallBoundForAnyBot(chatId)) return true;
  if (isKnownPeerBot(config.session.dataDir, larkAppId, senderOpenId)) return true;
  if (hasChatGrant(larkAppId, chatId, senderOpenId)) return true;   // ← 新增
  const allowedUsers = getBot(larkAppId).resolvedAllowedUsers;
  if (allowedUsers.length === 0) return true;
  return !!senderOpenId && allowedUsers.includes(senderOpenId);
}
```

`canOperate` **保持不动**（不加 chatGrant）。

3b. `daemon.ts:461` 去掉 oncall 前置——daemon 命令对所有群都要 canOperate：

```ts
// 原: if (isChatOncallBoundForAnyBot(chatId) && !canOperate(larkAppId, chatId, senderOpenId)) {
if (!canOperate(larkAppId, chatId, senderOpenId)) {
  await sessionReply(anchor, tr('daemon.cmd_allowed_users_only', { cmd }, localeForBot(larkAppId)), 'text', larkAppId);
  return;
}
```

3c. `daemon.ts:765` 同样去掉 `threadChatId && isChatOncallBoundForAnyBot(threadChatId) &&` 前缀：

```ts
if (!canOperate(larkAppId, threadChatId, threadSenderOpenId)) {
  sessionReply(anchor, tr('daemon.cmd_allowed_users_only', { cmd }, localeForBot(larkAppId)), 'text', larkAppId);
  return;
}
```

> 注意：3b/3c 对现有 allowedUsers 用户是 no-op（他们本就过 canOperate）；只新增挡住 chat-granted 用户跑 daemon 命令。非 oncall 群里原本到这一步的只有 allowedUsers 用户（被 canTalk 前置过滤过），所以不改变既有行为；oncall 群行为也与原先一致。

- [ ] **Step 4: 跑测试 + 回归**

Run: `pnpm vitest run test/grant-gates.test.ts && pnpm build`
Expected: PASS + 无类型错误。若存在 dispatcher/daemon 相关测试一并跑过。

- [ ] **Step 5: Commit**

```bash
git add src/im/lark/event-dispatcher.ts src/daemon.ts test/grant-gates.test.ts
git commit -m "feat(im/lark): canTalk 认 chatGrants;daemon 命令统一要 canOperate"
```

---

## Task 5: `grant-pending` —— 内存 pending/denied 表 + nonce（R1#6, R2#5）

**Files:**
- Create: `src/im/lark/grant-pending.ts`
- Test: `test/grant-pending.test.ts`

- [ ] **Step 1: 写失败测试**

```ts
// test/grant-pending.test.ts
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { openPending, checkNonce, clearPending, markDenied, isThrottled, _resetForTest } from '../src/im/lark/grant-pending.js';

beforeEach(() => { _resetForTest(); vi.useFakeTimers(); });
afterEach(() => vi.useRealTimers());

it('openPending issues nonce, throttles repeats, checkNonce validates', () => {
  const n = openPending('a1', 'oc_1', 'ou_g');
  expect(typeof n).toBe('string');
  expect(isThrottled('a1', 'oc_1', 'ou_g')).toBe(true);   // pending → 静默
  expect(checkNonce('a1', 'oc_1', 'ou_g', n)).toBe(true);
  expect(checkNonce('a1', 'oc_1', 'ou_g', 'wrong')).toBe(false);
});

it('clearPending lifts throttle; markDenied keeps 10min cooldown', () => {
  openPending('a1', 'oc_1', 'ou_g');
  clearPending('a1', 'oc_1', 'ou_g');
  expect(isThrottled('a1', 'oc_1', 'ou_g')).toBe(false);

  const n = openPending('a1', 'oc_1', 'ou_g');
  markDenied('a1', 'oc_1', 'ou_g');
  expect(checkNonce('a1', 'oc_1', 'ou_g', n)).toBe(false);  // 拒绝后旧 nonce 失效
  expect(isThrottled('a1', 'oc_1', 'ou_g')).toBe(true);     // 冷却中
  vi.advanceTimersByTime(10 * 60 * 1000 + 1);
  expect(isThrottled('a1', 'oc_1', 'ou_g')).toBe(false);    // 冷却期满
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/grant-pending.test.ts`
Expected: FAIL — 模块不存在。

- [ ] **Step 3: 实现**

```ts
// src/im/lark/grant-pending.ts
/**
 * 授权申请的内存状态表（per bot:chat:target）。两个职责合一：
 *  - nonce 防旧卡重放：每次发卡生成 nonce，卡片按钮带它；处置时校验仍匹配。
 *  - 节流：pending 期间 / denied 冷却期间，不重复弹卡。
 * 纯内存，daemon 重启清空（重启后旧卡 nonce 自然失效，符合预期）。
 */
import { randomUUID } from 'node:crypto';

const DENY_COOLDOWN_MS = 10 * 60 * 1000;

type Entry = { state: 'pending' | 'denied'; nonce?: string; ts: number };
const table = new Map<string, Entry>();

const key = (a: string, c: string, t: string) => `${a}:${c}:${t}`;

/** 开一张待处置的卡，返回 nonce。 */
export function openPending(larkAppId: string, chatId: string, target: string): string {
  const nonce = randomUUID();
  table.set(key(larkAppId, chatId, target), { state: 'pending', nonce, ts: Date.now() });
  return nonce;
}

/** 卡片处置前校验：必须仍 pending 且 nonce 匹配。 */
export function checkNonce(larkAppId: string, chatId: string, target: string, nonce: string): boolean {
  const e = table.get(key(larkAppId, chatId, target));
  return !!e && e.state === 'pending' && e.nonce === nonce;
}

/** 授权成功 / revoke → 清除，允许将来重新申请。 */
export function clearPending(larkAppId: string, chatId: string, target: string): void {
  table.delete(key(larkAppId, chatId, target));
}

/** 拒绝 → 转 denied 冷却态（不清除），旧 nonce 失效，冷却期内不再弹卡。 */
export function markDenied(larkAppId: string, chatId: string, target: string): void {
  table.set(key(larkAppId, chatId, target), { state: 'denied', ts: Date.now() });
}

/** 入口 A 节流判断：pending 中、或 denied 冷却未过 → true（静默不发卡）。 */
export function isThrottled(larkAppId: string, chatId: string, target: string): boolean {
  const e = table.get(key(larkAppId, chatId, target));
  if (!e) return false;
  if (e.state === 'pending') return true;
  return Date.now() - e.ts < DENY_COOLDOWN_MS;
}

export function _resetForTest(): void { table.clear(); }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `pnpm vitest run test/grant-pending.test.ts`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/im/lark/grant-pending.ts test/grant-pending.test.ts
git commit -m "feat(im/lark): grant-pending —— nonce 防重放 + deny 冷却节流表"
```

---

## Task 6: `buildGrantCard` —— 授权卡片（含 @owner + nonce 按钮，R1#6）

**Files:**
- Modify: `src/im/lark/card-builder.ts`（新增 export）
- Test: `test/grant-card.test.ts`

- [ ] **Step 1: 写失败测试**

```ts
// test/grant-card.test.ts
import { describe, it, expect } from 'vitest';
import { buildGrantCard } from '../src/im/lark/card-builder.js';

it('buildGrantCard embeds @owner, requester, and nonce-bearing actions', () => {
  const json = buildGrantCard({ ownerOpenId: 'ou_owner', requesterOpenId: 'ou_g', requesterName: '张三', chatId: 'oc_1', nonce: 'n1', mode: 'request' }, 'zh');
  const card = JSON.parse(json);
  const flat = JSON.stringify(card);
  expect(flat).toContain('<at id=ou_owner></at>');
  expect(flat).toContain('张三');
  const actions = card.elements.find((e: any) => e.tag === 'action').actions;
  const byAction = Object.fromEntries(actions.map((a: any) => [a.value.action, a.value]));
  expect(byAction.grant_chat).toMatchObject({ target_open_id: 'ou_g', chat_id: 'oc_1', nonce: 'n1' });
  expect(byAction.grant_global).toMatchObject({ target_open_id: 'ou_g', nonce: 'n1' });
  expect(byAction.grant_deny).toMatchObject({ target_open_id: 'ou_g', nonce: 'n1' });
});
```

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/grant-card.test.ts`
Expected: FAIL — `buildGrantCard` 未导出。

- [ ] **Step 3: 实现（追加到 card-builder.ts）**

```ts
export interface GrantCardOpts {
  ownerOpenId: string;
  requesterOpenId: string;
  requesterName: string;
  chatId: string;
  nonce: string;
  /** 'request' = 无权限者自助申请；'owner' = owner 主动 /grant。文案略不同。 */
  mode: 'request' | 'owner';
}

export function buildGrantCard(o: GrantCardOpts, locale?: Locale): string {
  const body = o.mode === 'request'
    ? t('card.grant.body_request', { name: escapeMd(o.requesterName), owner: o.ownerOpenId }, locale)
    : t('card.grant.body_owner', { name: escapeMd(o.requesterName), owner: o.ownerOpenId }, locale);
  const v = { target_open_id: o.requesterOpenId, chat_id: o.chatId, nonce: o.nonce };
  const card = {
    config: { wide_screen_mode: true },
    header: { template: 'orange', title: { tag: 'plain_text', content: t('card.grant.title', undefined, locale) } },
    elements: [
      { tag: 'div', text: { tag: 'lark_md', content: body } },
      { tag: 'hr' },
      { tag: 'action', actions: [
        { tag: 'button', type: 'primary', text: { tag: 'plain_text', content: t('card.grant.btn_chat', undefined, locale) }, value: { action: 'grant_chat', ...v } },
        { tag: 'button', type: 'default', text: { tag: 'plain_text', content: t('card.grant.btn_global', undefined, locale) }, value: { action: 'grant_global', ...v } },
        { tag: 'button', type: 'danger', text: { tag: 'plain_text', content: t('card.grant.btn_deny', undefined, locale) }, value: { action: 'grant_deny', ...v } },
      ] },
      { tag: 'note', elements: [{ tag: 'lark_md', content: t('card.grant.note', undefined, locale) }] },
    ],
  };
  return JSON.stringify(card);
}
```

> `card.grant.body_request` 文案里用 `{owner}` 拼成 `<at id=ou_xxx></at>`，见 Task 9 的 i18n。`escapeMd` 已在 card-builder 内（line 371 调用处）。

- [ ] **Step 4: 跑测试确认通过**

Run: `pnpm vitest run test/grant-card.test.ts && pnpm build`
Expected: PASS + 无类型错误（i18n key 在 Task 9 补；为通过本 task 编译，可先在 zh/en 补占位文案，或先把文案 inline 后于 Task 9 改 t()——推荐先 inline 中文，Task 9 替换为 t()）。

> 实操建议：本 task 先 inline 中文文案让测试/编译通过，Task 9 统一替换成 `t('card.grant.*')`。测试断言只校验结构与占位值，不校验具体文案字面量，故替换不破测试。

- [ ] **Step 5: Commit**

```bash
git add src/im/lark/card-builder.ts test/grant-card.test.ts
git commit -m "feat(im/lark): buildGrantCard 授权卡片(含 @owner 与 nonce 按钮)"
```

---

## Task 7: card-handler —— grant 动作（owner 强闸门 + nonce）与 repo 卡权限边界（R1#6, R2#1）

**Files:**
- Modify: `src/im/lark/card-handler.ts`
- Test: `test/card-handler-grant.test.ts`

分两部分：
1. 在 `handleCardAction` 最前面处理 `grant_chat/grant_global/grant_deny`（不依赖 session）。
2. repo 下拉（`repo_switch` option 与 `skip_repo` 已是 sensitive）：pendingRepo 阶段放行 `operator === session.ownerOpenId || canOperate`；mid-session 切换要 `canOperate`。

- [ ] **Step 1: 写失败测试**

```ts
// test/card-handler-grant.test.ts
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { registerBot, getBot } from '../src/bot-registry.js';
import * as grantStore from '../src/services/grant-store.js';
import * as pending from '../src/im/lark/grant-pending.js';
import { handleCardAction } from '../src/im/lark/card-handler.js';

const deps = { activeSessions: new Map(), sessionReply: vi.fn(), /* 其余按真实 CardHandlerDeps 形态最小填充 */ } as any;

beforeEach(() => {
  pending._resetForTest();
  const bot = registerBot({ larkAppId: 'h1', larkAppSecret: 's', cliId: 'claude-code', allowedUsers: ['ou_owner'] });
  bot.resolvedAllowedUsers = ['ou_owner'];
});
afterEach(() => vi.restoreAllMocks());

it('non-owner click is rejected (no grant)', async () => {
  const spy = vi.spyOn(grantStore, 'addChatGrant');
  const nonce = pending.openPending('h1', 'oc_1', 'ou_g');
  await handleCardAction({ operator: { open_id: 'ou_not_owner' }, action: { value: { action: 'grant_chat', target_open_id: 'ou_g', chat_id: 'oc_1', nonce } } } as any, deps, 'h1');
  expect(spy).not.toHaveBeenCalled();
});

it('owner grant_chat with valid nonce applies and clears pending', async () => {
  const spy = vi.spyOn(grantStore, 'addChatGrant').mockResolvedValue({ ok: true, created: true });
  const nonce = pending.openPending('h1', 'oc_1', 'ou_g');
  await handleCardAction({ operator: { open_id: 'ou_owner' }, action: { value: { action: 'grant_chat', target_open_id: 'ou_g', chat_id: 'oc_1', nonce } } } as any, deps, 'h1');
  expect(spy).toHaveBeenCalledWith('h1', 'oc_1', 'ou_g');
  expect(pending.checkNonce('h1', 'oc_1', 'ou_g', nonce)).toBe(false); // cleared
});

it('stale nonce → no grant', async () => {
  const spy = vi.spyOn(grantStore, 'addGlobalGrant');
  await handleCardAction({ operator: { open_id: 'ou_owner' }, action: { value: { action: 'grant_global', target_open_id: 'ou_g', chat_id: 'oc_1', nonce: 'stale' } } } as any, deps, 'h1');
  expect(spy).not.toHaveBeenCalled();
});
```

> 注：`CardHandlerDeps` 真实形态见 `card-handler.ts:26`；测试里按需最小填充，grant 分支不触达 session 字段。若 import 期产生副作用，按现有其它 card-handler 测试的 mock 方式对齐。

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/card-handler-grant.test.ts`
Expected: FAIL — grant 分支未实现，addChatGrant 未被调用。

- [ ] **Step 3: 实现**

3a. `handleCardAction` 顶部（取出 `value`/`operatorOpenId` 之后、sensitive 判断之前）插入 grant 分支：

```ts
import { getOwnerOpenId } from '../../bot-registry.js';
import { addChatGrant, addGlobalGrant, revokeGrant } from '../../services/grant-store.js';
import { checkNonce, clearPending, markDenied } from './grant-pending.js';
import { updateMessage } from './client.js';

const GRANT_ACTIONS = new Set(['grant_chat', 'grant_global', 'grant_deny']);
if (value?.action && GRANT_ACTIONS.has(value.action) && larkAppId) {
  const owner = getOwnerOpenId(larkAppId);
  // owner 强闸门：必须是当前 app 的 owner 本人
  if (!operatorOpenId || operatorOpenId !== owner) {
    return { toast: { type: 'error', content: t('card.grant.toast_owner_only', undefined, localeForBot(larkAppId)) } };
  }
  const { target_open_id: target, chat_id: chatId, nonce } = value;
  if (!target || !chatId || !nonce || !checkNonce(larkAppId, chatId, target, nonce)) {
    return { toast: { type: 'error', content: t('card.grant.toast_expired', undefined, localeForBot(larkAppId)) } };
  }
  const cardMsgId = data?.open_message_id ?? (data as any)?.message_id; // 用于 updateMessage 置终态
  if (value.action === 'grant_deny') {
    markDenied(larkAppId, chatId, target);
    if (cardMsgId) await updateMessage(larkAppId, cardMsgId, buildGrantResultCard('deny', localeForBot(larkAppId)));
    return;
  }
  const res = value.action === 'grant_chat'
    ? await addChatGrant(larkAppId, chatId, target)
    : await addGlobalGrant(larkAppId, target);
  clearPending(larkAppId, chatId, target);
  if (cardMsgId) await updateMessage(larkAppId, cardMsgId, buildGrantResultCard(value.action === 'grant_chat' ? 'chat' : 'global', localeForBot(larkAppId)));
  return;
}
```

> `buildGrantResultCard(kind, locale)`：在 card-builder.ts 加一个极简终态卡（标题 + 「✅ 已授权本群 / ✅ 已全局授权 / 🚫 已拒绝」一行 div、无按钮）。`open_message_id` 字段名以飞书回调实际 payload 为准（见现有 card-handler 里取 cardMessageId 的写法，如 `data?.open_message_id`）；对齐现有用法。

3b. repo 卡权限边界——把 `repo_switch` option 选择纳入闸门。在 repo select 处理处（`card-handler.ts:707` 起）插入：

```ts
// targetDs 已解析（行 717）。pendingRepo 阶段允许会话发起人或 canOperate；mid-session 切换要 canOperate。
const isOwnerOfSession = operatorOpenId && operatorOpenId === targetDs.session.ownerOpenId;
const allowRepo = targetDs.pendingRepo
  ? (isOwnerOfSession || canOperate(targetDs.larkAppId, targetDs.chatId, operatorOpenId))
  : canOperate(targetDs.larkAppId, targetDs.chatId, operatorOpenId);
if (!allowRepo) {
  logger.info(`Repo card action blocked for ${operatorOpenId} (pending=${targetDs.pendingRepo})`);
  return { toast: { type: 'error', content: t('card.grant.toast_no_repo_perm', undefined, localeForBot(targetDs.larkAppId)) } };
}
```

> `canOperate` 从 `event-dispatcher.js` import（card-handler 已 import 过 canOperate 用于 sensitive 闸门，复用）。`session.ownerOpenId` 字段见 `core/types.ts:60`。

- [ ] **Step 4: 跑测试确认通过**

Run: `pnpm vitest run test/card-handler-grant.test.ts && pnpm build`
Expected: PASS + 无类型错误。

- [ ] **Step 5: Commit**

```bash
git add src/im/lark/card-handler.ts src/im/lark/card-builder.ts test/card-handler-grant.test.ts
git commit -m "feat(im/lark): card-handler 处理 grant 动作(owner+nonce)与 repo 卡权限边界"
```

---

## Task 8: `grant-command` —— /grant、/revoke（isBotMentioned + 排除自身 mention，R1#3）

**Files:**
- Create: `src/im/lark/grant-command.ts`
- Test: `test/grant-command.test.ts`

- [ ] **Step 1: 写失败测试**

```ts
// test/grant-command.test.ts
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { registerBot, getBot } from '../src/bot-registry.js';
import * as pending from '../src/im/lark/grant-pending.js';
import { parseGrantTarget } from '../src/im/lark/grant-command.js';

it('parseGrantTarget extracts first non-bot human mention', () => {
  const msg = { mentions: [
    { key: '@_user_1', id: { open_id: 'ou_bot' }, name: 'Claude' },
    { key: '@_user_2', id: { open_id: 'ou_g' }, name: '张三' },
  ] };
  expect(parseGrantTarget(msg, 'ou_bot')).toEqual({ openId: 'ou_g', name: '张三' });
  expect(parseGrantTarget({ mentions: [{ id: { open_id: 'ou_bot' }, name: 'Claude' }] }, 'ou_bot')).toBeUndefined();
});
```

> 命令分发（/grant /revoke 全流程含发卡/owner 闸门）依赖飞书 API 副作用，单测聚焦纯函数 `parseGrantTarget`；端到端行为靠 Task 9 集成后人工验证 + 既有 dispatcher 测试覆盖拦截路径。

- [ ] **Step 2: 跑测试确认失败**

Run: `pnpm vitest run test/grant-command.test.ts`
Expected: FAIL — 模块/函数不存在。

- [ ] **Step 3: 实现**

```ts
// src/im/lark/grant-command.ts
/**
 * 群内授权元命令：`@bot /grant @user`、`@bot /revoke @user`。
 * 在 dispatcher 路由/spawn 之前拦截，仅 owner 可用。
 * 与 /introduce 不同：必须确认本 bot 被 @（多 bot 群防重复处理），且解析 target 时排除 bot 自身。
 */
import { getBotOpenId, getOwnerOpenId, getBot } from '../bot-registry.js';
import { isBotMentioned, extractMessageTextForRouting } from './event-dispatcher.js';
import { buildGrantCard } from './card-builder.js';
import { openPending } from './grant-pending.js';
import { revokeGrant } from '../../services/grant-store.js';
import { replyMessage } from './client.js';
import { localeForBot, t } from '../../i18n/index.js';
import { logger } from '../../utils/logger.js';

export function parseGrantTarget(message: any, botOpenId: string | undefined): { openId: string; name: string } | undefined {
  const m = (message?.mentions ?? []).find((x: any) => x?.id?.open_id && x.id.open_id !== botOpenId);
  return m ? { openId: m.id.open_id, name: m.name ?? m.id.open_id } : undefined;
}

/** 返回 true 表示已拦截（不再进入路由/spawn）。 */
export async function tryHandleGrantCommand(
  larkAppId: string, message: any, senderOpenId: string | undefined,
): Promise<boolean> {
  const text = (extractMessageTextForRouting(message) ?? '').trim();
  const isGrant = /^\/grant(\s|$)/i.test(text);
  const isRevoke = /^\/revoke(\s|$)/i.test(text);
  if (!isGrant && !isRevoke) return false;

  // 多 bot 群：必须明确 @ 当前 bot 才由本 daemon 处理
  if (!isBotMentioned(larkAppId, message, senderOpenId)) return true; // 命中命令但非本 bot —— 静默吞掉(不喂 CLI)

  const loc = localeForBot(larkAppId);
  const messageId = message.message_id;
  const chatId = message.chat_id;

  // owner 强闸门
  if (!senderOpenId || senderOpenId !== getOwnerOpenId(larkAppId)) {
    await replyMessage(larkAppId, messageId, JSON.stringify({ text: t('cmd.grant.owner_only', undefined, loc) }));
    return true;
  }
  const botOpenId = getBotOpenId(larkAppId);
  const target = parseGrantTarget(message, botOpenId);
  if (!target) {
    await replyMessage(larkAppId, messageId, JSON.stringify({ text: t(isGrant ? 'cmd.grant.usage' : 'cmd.revoke.usage', undefined, loc) }));
    return true;
  }

  if (isRevoke) {
    const r = await revokeGrant(larkAppId, chatId, target.openId);
    const txt = !r.ok
      ? (r.reason === 'would_open_bot' ? t('cmd.revoke.would_open', undefined, loc) : t('cmd.revoke.failed', { reason: r.reason }, loc))
      : t('cmd.revoke.done', { name: target.name, scope: `${r.removed.chat ? '本群 ' : ''}${r.removed.global ? '全局' : ''}`.trim() || '无' }, loc);
    await replyMessage(larkAppId, messageId, JSON.stringify({ text: txt }));
    return true;
  }

  // /grant → 弹卡（owner 主动态）
  const owner = getOwnerOpenId(larkAppId)!;
  const nonce = openPending(larkAppId, chatId, target.openId);
  const card = buildGrantCard({ ownerOpenId: owner, requesterOpenId: target.openId, requesterName: target.name, chatId, nonce, mode: 'owner' }, loc);
  await replyMessage(larkAppId, messageId, card, 'interactive');
  logger.info(`[grant:${larkAppId}] owner /grant card for ${target.openId} in ${chatId}`);
  return true;
}
```

> 需在 `bot-registry.ts` 导出 `getBotOpenId(larkAppId): string | undefined`（返回 `bots.get(id)?.botOpenId`）——若已有等价导出则复用。`extractMessageTextForRouting` 已在 event-dispatcher 导出（line 585）。

- [ ] **Step 4: 跑测试确认通过**

Run: `pnpm vitest run test/grant-command.test.ts && pnpm build`
Expected: PASS + 无类型错误。

- [ ] **Step 5: Commit**

```bash
git add src/im/lark/grant-command.ts src/bot-registry.ts test/grant-command.test.ts
git commit -m "feat(im/lark): grant-command —— /grant /revoke(owner 闸门+排除自身 mention)"
```

---

## Task 9: dispatcher 集成 + 入口 A 弹卡 + i18n + 启动期解析映射回填（R1#3, R1#7, R2#2）

**Files:**
- Modify: `src/im/lark/event-dispatcher.ts`（grant-command 拦截 + not_allowed 弹卡）
- Modify: `src/daemon.ts`（启动 resolveAllowedUsers 后回填 `rawAllowedUserResolution`）
- Modify: `src/i18n/zh.ts`、`src/i18n/en.ts`
- Modify: `src/core/command-handler.ts` 的 `/help` 文案
- Test: 手动验证为主（飞书侧）；i18n key 完整性靠 build。

- [ ] **Step 1: grant-command 拦截接入 dispatcher**

在 `event-dispatcher.ts` 的 `/introduce` 拦截之后（`tryHandleIntroduceCommand` 调用之后、`logger.debug('Received message:'...)` 之前）插入：

```ts
if (await tryHandleGrantCommand(larkAppId, message, senderOpenId)) {
  return;
}
```

import：`import { tryHandleGrantCommand } from './grant-command.js';`（注意：grant-command 也 import event-dispatcher 的 `isBotMentioned`/`extractMessageTextForRouting`，存在循环 import；TS/ESM 下函数级循环 import 可行，但若初始化期报错，则把 `isBotMentioned`/`extractMessageTextForRouting` 抽到一个无副作用的小模块 `lark-message-util.ts` 供两边 import。优先尝试直接 import，编译/运行报错再抽离。）

- [ ] **Step 2: 入口 A —— not_allowed 弹授权卡（覆盖 ownsSession，R1#7）**

把 `event-dispatcher.ts:884-890` 的 `not_allowed` 分支改为：

```ts
if (access === 'not_allowed') {
  await maybeSendGrantRequestCard(larkAppId, message, chatId, senderOpenId);
  logger.debug(`not_allowed from ${senderOpenId} → grant request card path`);
  return; // 无论 ownsSession 与否都不喂 session
}
```

新增本地函数：

```ts
import { getOwnerOpenId, getBot } from '../../bot-registry.js';
import { buildGrantCard } from './card-builder.js';
import { openPending, isThrottled } from './grant-pending.js';

async function maybeSendGrantRequestCard(
  larkAppId: string, message: any, chatId: string, requesterOpenId: string | undefined,
): Promise<void> {
  const owner = getOwnerOpenId(larkAppId);
  if (!owner || !requesterOpenId) return;            // 开放模式无 owner / 无发信人 → 兜底不弹
  if (isThrottled(larkAppId, chatId, requesterOpenId)) return;
  const name = (message?.mentions ?? []).find((m: any) => m?.id?.open_id === requesterOpenId)?.name
    ?? message?.sender?.sender_id?.open_id ?? requesterOpenId;
  const nonce = openPending(larkAppId, chatId, requesterOpenId);
  const card = buildGrantCard({ ownerOpenId: owner, requesterOpenId, requesterName: String(name), chatId, nonce, mode: 'request' }, localeForBot(larkAppId));
  await replyMessage(larkAppId, message.message_id, card, 'interactive')
    .catch(err => logger.debug(`grant request card send failed: ${err}`));
}
```

> 发信人姓名：not_allowed 路径不一定有 mention 含 requester；取不到就回落 open_id。可接受（卡片主要靠 @owner 触达 owner）。

- [ ] **Step 3: 启动期回填 resolution map（R2#2 落地）**

`daemon.ts:1128-1137` resolve 之后，记录 raw→resolved 映射。把：

```ts
bot.resolvedAllowedUsers = await resolveAllowedUsers(cfg.larkAppId, bot.resolvedAllowedUsers);
```

扩展为同时建映射（`resolveAllowedUsers` 返回的顺序需与输入对齐；若其实现不保序，改为逐条解析或返回 pair）：

```ts
const rawList = [...bot.resolvedAllowedUsers]; // resolve 前 = raw（含 email）
const resolved = await resolveAllowedUsers(cfg.larkAppId, rawList);
bot.resolvedAllowedUsers = resolved;
bot.rawAllowedUserResolution = new Map();
rawList.forEach((raw, i) => { if (resolved[i]) bot.rawAllowedUserResolution!.set(raw, resolved[i]); });
```

> 校对 `resolveAllowedUsers`（`im/lark/client.ts`）是否保持输入顺序、是否丢弃解析失败项。若会丢项导致错位，改成：让 `resolveAllowedUsers` 返回 `{ raw, openId }[]`，或在此逐个 email 单独解析。实现时以该函数真实契约为准，保证映射不错位（这是 R2#2 正确性的关键，需读源码确认）。
> 对未解析成功的 email：不进映射；revoke 时 `rawEntryForOpenId` 找不到对应 raw → 不删全局，仅删本群（与「彻底撤销」尽力而为一致，且不会误删错 entry）。

- [ ] **Step 4: i18n 文案（zh.ts / en.ts 同步加 key）**

zh.ts 追加：

```ts
  'card.grant.title': '使用授权',
  'card.grant.body_request': '用户 **{name}** 申请在本群使用我。<at id={owner}></at> 请选择授权范围：',
  'card.grant.body_owner': '请选择对 **{name}** 的授权范围（<at id={owner}></at>）：',
  'card.grant.btn_chat': '授权本群',
  'card.grant.btn_global': '全局授权',
  'card.grant.btn_deny': '拒绝',
  'card.grant.note': '「授权本群」仅允许其在本群与我对话；管理命令仍仅限 owner。',
  'card.grant.toast_owner_only': '仅 owner 可操作',
  'card.grant.toast_expired': '该授权请求已失效',
  'card.grant.toast_no_repo_perm': '无权限切换仓库',
  'card.grant.result_chat': '✅ 已授权本群',
  'card.grant.result_global': '✅ 已全局授权',
  'card.grant.result_deny': '🚫 已拒绝',
  'cmd.grant.owner_only': '仅 owner 可使用 /grant。',
  'cmd.grant.usage': '用法：@机器人 /grant @某人 —— 弹出授权卡片，由 owner 选择授权范围。',
  'cmd.revoke.owner_only': '仅 owner 可使用 /revoke。',
  'cmd.revoke.usage': '用法：@机器人 /revoke @某人 —— 撤销该用户的本群与全局授权。',
  'cmd.revoke.done': '已撤销 {name} 的权限（范围：{scope}）。',
  'cmd.revoke.would_open': '⚠️ 撤销失败：该用户是最后一个全局授权用户/owner，撤销会让机器人对所有人开放。已阻止。',
  'cmd.revoke.failed': '⚠️ 撤销失败：{reason}',
```

en.ts 追加等价英文 key（同 key 名）。`buildGrantResultCard(kind)` 用 `card.grant.result_*`。

把 Task 6 中 inline 的中文替换为对应 `t('card.grant.*')`。

- [ ] **Step 5: `/help` 文案补 /grant /revoke**

在 `command-handler.ts` 的 `/help` 输出（及对应 i18n key）追加：

```
/grant @某人   — (owner) 弹授权卡片，授权本群使用或全局
/revoke @某人  — (owner) 撤销某人的本群+全局授权
```

- [ ] **Step 6: 构建 + 全量回归**

Run: `pnpm build && pnpm test`
Expected: build 无类型错误；现有测试全绿（本功能新增测试也在内）。

- [ ] **Step 7: Commit**

```bash
git add src/im/lark/event-dispatcher.ts src/daemon.ts src/i18n/zh.ts src/i18n/en.ts src/core/command-handler.ts src/im/lark/card-builder.ts
git commit -m "feat(im/lark): dispatcher 接入 /grant 拦截与入口A弹卡;i18n;启动回填解析映射"
```

---

## Task 10: 端到端人工验证（飞书侧）

**Files:** 无（验证 + 记录）

- [ ] **Step 1: 构建并重启 daemon**

```bash
pnpm build && pnpm daemon:restart && pnpm daemon:logs
```

- [ ] **Step 2: 走查清单（对照 spec 测试要点）**

- [ ] 无权限者 @bot → 弹授权卡且 @owner；同人再发不刷屏（节流）。
- [ ] owner 点「授权本群」→ 该用户能在本群 @bot 对话；换个群仍无权限。
- [ ] 被授权人尝试 `/cd` `/oncall bind` → 被「仅 allowedUsers」挡（canOperate）。
- [ ] owner `@bot /grant @某人` → 弹卡；点「全局授权」→ 该用户全局可用。
- [ ] owner 点「拒绝」→ 申请人 10 分钟内再 @bot 不再弹卡。
- [ ] 重启 daemon → 旧授权卡按钮点击提示「已失效」（nonce 重启失效）。
- [ ] `/revoke @某人` → 回执说明范围；该用户权限消失；重启后不恢复（含 email 配置场景需另配验证）。
- [ ] `/revoke` 最后一个全局用户 → 被 would_open_bot 阻止。
- [ ] 非 owner 点授权卡按钮 → toast「仅 owner 可操作」，不生效。
- [ ] pendingRepo 阶段被授权人能选 repo 起会话；mid-session 切 repo 需 canOperate。

- [ ] **Step 3: 发现问题回到对应 Task 修复；全部通过后准备收尾（finishing-a-development-branch）。**

---

## 自检对照（plan ↔ spec）

- R1#1 防开放守卫 → Task 3（revokeGrant 守卫，按 resolved 判 = R2#3）
- R1#2 chatGrant 仅 canTalk + daemon 命令 canOperate → Task 4
- R1#3 /grant isBotMentioned + 排除自身 → Task 8 + Task 9 Step1
- R1#4 chatGrants 进白名单解析 → Task 2
- R1#5 0o600 → Task 1
- R1#6 nonce + owner 强闸门 + 卡片 → Task 5/6/7
- R1#7 not_allowed 覆盖 ownsSession 不喂 session → Task 9 Step2
- R1#8 revoke 不停历史 schedule → 文档已记（无代码）
- R2#1 repo 卡权限边界 → Task 7 (3b)
- R2#2 email 撤销/解析映射 → Task 3 + Task 9 Step3
- R2#3 守卫按 resolvedAllowedUsers → Task 3
- R2#4 revoke 原子 → Task 3（单 RMW）
- R2#5 deny 冷却 → Task 5 + Task 7
