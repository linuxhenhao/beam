// beam ask hook for OpenCode
import { spawn } from "child_process";

const BEAM_BIN = "beam";
const BEAM_CLI_ID = "opencode";
const seenPermissionIds = new Map();
const SEEN_PERMISSION_TTL_MS = 5 * 60 * 1000;

function pruneSeenPermissionIds() {
  const cutoff = Date.now() - SEEN_PERMISSION_TTL_MS;
  for (const [requestID, seenAt] of seenPermissionIds.entries()) {
    if (seenAt < cutoff) {
      seenPermissionIds.delete(requestID);
    }
  }
}

function runBeamHook(payload) {
  return new Promise((resolve) => {
    let stdout = "";
    let settled = false;
    let child;
    const finish = (directive) => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(directive);
    };
    try {
      child = spawn(BEAM_BIN, ["hook", BEAM_CLI_ID], {
        stdio: ["pipe", "pipe", "pipe"],
      });
    } catch {
      finish(undefined);
      return;
    }

    const timeout = setTimeout(() => {
      try {
        child.kill("SIGKILL");
      } catch {}
      finish(undefined);
    }, 86400000);
    timeout.unref?.();

    child.stdout.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", () => {});
    child.on("error", () => {
      clearTimeout(timeout);
      finish(undefined);
    });
    child.on("close", (status) => {
      clearTimeout(timeout);
      if (status === 0 && stdout && stdout.trim()) {
        try {
          finish(JSON.parse(stdout.trim()));
          return;
        } catch {}
      }
      finish(undefined);
    });

    try {
      child.stdin.end(JSON.stringify(payload));
    } catch {
      clearTimeout(timeout);
      try {
        child.kill("SIGKILL");
      } catch {}
      finish(undefined);
    }
  });
}

function normalizePermissionPayload(input) {
  const patterns = Array.isArray(input?.patterns)
    ? input.patterns
    : input?.patterns
      ? [input.patterns]
      : Array.isArray(input?.pattern)
        ? input.pattern
        : input?.pattern
          ? [input.pattern]
          : [];
  return {
    hook_event_name: "permission.asked",
    id: input?.id ?? "",
    sessionID: input?.sessionID ?? "",
    permission: input?.permission ?? input?.type ?? input?.title ?? "permission request",
    patterns,
    metadata: input?.metadata ?? {},
    tool: {
      messageID: input?.messageID ?? "",
      callID: input?.callID ?? "",
    },
  };
}

async function handlePermission(input, output) {
  const directive = await runBeamHook(normalizePermissionPayload(input));
  if (!directive || directive.type !== "permission") return;
  if (output) {
    output.status = directive.reply === "reject" ? "deny" : "allow";
  }
  return directive;
}

async function replyPermission(client, sessionID, requestID, reply) {
  if (!sessionID || !requestID || !reply) return;
  if (client?.postSessionIdPermissionsPermissionId) {
    try {
      await client.postSessionIdPermissionsPermissionId({
        path: { id: sessionID, permissionID: requestID },
        body: { response: reply },
      });
    } catch {}
  }
}

async function handleQuestion(event, client) {
  const directive = await runBeamHook({
    hook_event_name: event.type,
    ...(event.properties ?? {}),
  });
  if (!directive || directive.type !== "answer") return;
  const requestID = event.properties?.id;
  const answers = directive.answers ?? [];
  if (!requestID) return;
  if (client?.question?.reply) {
    try {
      await client.question.reply({ requestID, answers });
    } catch {}
  }
}

function trackBackground(task) {
  task.catch(() => {});
}

export const BeamAskPlugin = async ({ client } = {}) => ({
  event: async ({ event }) => {
    pruneSeenPermissionIds();
    if (event?.type === "question.asked") {
      trackBackground(handleQuestion(event, client));
      return;
    }
    if (event?.type === "permission.asked") {
      const requestID = event.properties?.requestID ?? event.properties?.id;
      const sessionID = event.properties?.sessionID;
      if (requestID && seenPermissionIds.has(requestID)) {
        return;
      }
      if (requestID) {
        seenPermissionIds.set(requestID, Date.now());
      }
      trackBackground((async () => {
        const directive = await handlePermission(event.properties, undefined);
        if (directive?.type === "permission") {
          await replyPermission(client, sessionID, requestID, directive.reply);
        }
      })());
      return;
    }
    if (event?.type === "permission.replied") {
      const requestID = event.properties?.requestID ?? event.properties?.id;
      if (requestID) {
        seenPermissionIds.delete(requestID);
      }
    }
  },
});
