# beam Federation Design

Chinese: [federation-design.md](federation-design.md)

## Goal

Allow multiple independent Beam deployments to form a shared team. The first milestone is making bots from different deployments visible to each other. The next milestone is allowing bots owned by different deployments to collaborate in the same Feishu group.

Federation should preserve local ownership. A deployment that owns a bot continues to run that bot locally with its own credentials, paths, and daemon state.

## Principles

- Deployments are peers, not workers of a central control plane.
- Bot secrets never leave the owning deployment.
- Runtime session ownership stays with the deployment that owns the bot.
- Federation metadata must be explicit, signed or authenticated, and revocable.
- Cross-deployment calls should be routed through stable deployment identity.

## Core Objects

- Deployment: one Beam installation with its own daemon, bots, secrets, and state.
- Federation peer: a trusted remote deployment.
- Federated bot: a roster entry advertised by a peer.
- Remote session: a session created on another deployment but visible from the local collaboration context.
- Routing record: metadata that tells Beam where to send actions for a remote bot/session.

## Discovery Phase

In the first phase, each deployment shares a safe roster:

- Deployment id and display name.
- Bot id, name, CLI kind, and description.
- Capability metadata.
- Routing endpoint.
- Optional availability/status.

The roster must not include credentials, local paths, terminal tokens, or user-private session data.

## Collaboration Phase

When a Feishu group invokes a remote bot:

1. Local deployment resolves the federated bot to its owning deployment.
2. Local deployment sends a session creation request to the owner.
3. Owner creates the session, starts the worker, and returns card/session routing metadata.
4. Cards shown in the group route callbacks back to the owner.
5. Owner validates permissions and applies actions.

The local deployment may proxy or relay user-facing messages, but it should not pretend to own remote runtime state.

## Authentication and Trust

Federation requires a trust relationship between deployments. At minimum:

- Peer identity must be stable.
- Requests must be authenticated.
- Replay-sensitive requests should include nonce/timestamp or equivalent protection.
- Grants should be revocable.
- Audit logs should include both local user identity and remote deployment identity.

## Permission Model

Permissions are layered:

- Local group policy decides whether a group may use federated bots.
- Remote owner policy decides whether the requesting deployment/group/user may use a specific bot.
- Sensitive actions remain checked by the owner.

UI affordances should never replace server-side checks.

## Failure Handling

Remote deployments can be offline or slow. The local UX should show clear unavailable/retry state and avoid creating duplicate sessions. Idempotency keys should be used for create-session and action requests that may be retried.

If a federated card becomes stale, the callback path should query the owner or reject the action with a clear message.

## Non-goals

- No shared global database in the initial design.
- No transfer of bot credentials between deployments.
- No remote takeover of local terminal sessions.
- No assumption that every deployment runs the same version at the same time.

## Open Questions

- What transport should federation use initially: HTTP webhook, Feishu-mediated relay, or direct daemon endpoint?
- How should deployment identity and key rotation be managed?
- How much card rendering should happen locally versus on the owning deployment?
- What is the migration path from discovery-only federation to full collaborative sessions?
