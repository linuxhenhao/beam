# beam Multi-user Multi-bot Collaboration Platform Design

Chinese: [platform-design.md](platform-design.md)

## Problem

Beam needs to support teams where multiple users collaborate with multiple bots from Feishu. The system must answer: who can see which bots, who can invite a bot into a group, which local deployment owns a bot, and how cards/actions remain auditable when several people interact with the same session.

The platform layer should not replace the local-first session model. It adds roster, permission, and collaboration semantics around local daemon sessions.

## Goals

- Let users discover available bots and their capabilities.
- Let a group chat invite one or more bots into a collaboration thread.
- Preserve per-bot credentials and ownership boundaries.
- Keep session/card actions attributable to the Feishu user who clicked or sent them.
- Support future federation across independent deployments.

## Core Concepts

- User: a Feishu user identified by open id/user id and optional display metadata.
- Bot: a configured CLI profile owned by one Beam deployment.
- Team roster: the set of bots/users visible to a workspace or deployment.
- Group binding: a Feishu group chat's relationship to one or more bots.
- Session: a concrete conversation between a chat/thread and a bot/CLI.
- Permission grant: a durable rule allowing a user or group to use a bot or action.

## Collaboration Flow

1. A user or group discovers available bots.
2. The user selects a bot and working directory/context.
3. Beam creates a session and posts a streaming card.
4. Other group members can view the card and use allowed actions.
5. Write-sensitive actions are gated by private write-link or permission checks.
6. Session state remains owned by the deployment that owns the bot.

## Permission Model

Permissions should be explicit and auditable:

- Group-level grants decide which bots can be used in a group.
- User-level grants decide who can perform sensitive actions.
- Bot ownership decides which deployment stores credentials and runs the CLI.
- Card callbacks must re-check permissions server-side; UI visibility is not enough.

## Bot Roster

The roster should expose stable bot identity, display name, CLI kind, description, and capability metadata. It should not expose secrets, local paths, or token material. For federation, roster entries can include deployment identity and routing metadata.

## Session Ownership

A session is executed by exactly one owning deployment. Even when several users interact from Feishu, the owning daemon remains responsible for:

- Worker lifecycle.
- Terminal backend.
- Credential access.
- Card patching.
- Event persistence.

Cross-deployment collaboration should route to the owner instead of copying runtime state.

## Card and Action Rules

Cards are shared views, not security boundaries. Every callback must include enough identity to validate:

- Which card was clicked.
- Which session it targets.
- Which user clicked it.
- Whether the card is stale.
- Whether the action is allowed.

Stale cards should self-heal when possible and reject dangerous actions when not.

## Non-goals

- Do not centralize all agent execution into one remote service.
- Do not make local bot secrets visible to other deployments.
- Do not rely on Feishu card visibility alone for authorization.
- Do not let multi-user collaboration erase the read-only/write terminal split.

## Open Questions

- How much roster metadata should be synchronized for federated deployments?
- What is the minimum admin UX for granting group access?
- Which actions require user-level approval beyond group-level bot access?
- How should audit logs be surfaced to admins?
