const container = document.getElementById("terminal-container");
const modeBadge = document.getElementById("mode-badge");
const sessionLabel = document.getElementById("session-label");
const overlay = document.getElementById("overlay");
const overlayTitle = document.getElementById("overlay-title");
const overlayDetail = document.getElementById("overlay-detail");
const reconnectBtn = document.getElementById("reconnect");

const sessionId = location.pathname.split("/").filter(Boolean)[1] || "";
sessionLabel.textContent = sessionId;

const term = new Terminal({
  cursorBlink: true,
  fontFamily: 'Menlo, Consolas, "Courier New", monospace',
  fontSize: 14,
  lineHeight: 1.2,
  scrollback: 4000,
  convertEol: false,
});
const fitAddon = new FitAddon.FitAddon();
term.loadAddon(fitAddon);
term.loadAddon(new WebLinksAddon.WebLinksAddon());
term.open(container);
fitAddon.fit();

let socket = null;
let mode = "readonly";
let reconnectTimer = null;

function setMode(next) {
  mode = next;
  modeBadge.className = "badge " + next;
  modeBadge.textContent = next;
  term.options.disableStdin = next !== "write";
}

function showOverlay(title, detail, canReconnect) {
  overlayTitle.textContent = title;
  overlayDetail.textContent = detail;
  reconnectBtn.classList.toggle("hidden", !canReconnect);
  overlay.classList.remove("hidden");
}

function hideOverlay() {
  overlay.classList.add("hidden");
}

function baseUrl() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/s/${sessionId}/ws/herdr`;
}

function connect() {
  hideOverlay();
  setMode("connecting");
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  socket = new WebSocket(baseUrl());

  socket.addEventListener("open", () => {
    if (mode === "write" || mode === "readonly") {
      return;
    }
    resizeTerminal();
  });

  socket.addEventListener("message", (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }
    if (msg.type === "hello") {
      setMode(msg.mode);
      term.reset();
      if (msg.mode === "readonly") {
        term.resize(msg.cols || 160, msg.rows || 50);
      } else {
        resizeTerminal();
      }
    } else if (msg.type === "frame") {
      const bytes = atob(msg.bytes);
      const out = new Uint8Array(bytes.length);
      for (let i = 0; i < bytes.length; i++) {
        out[i] = bytes.charCodeAt(i);
      }
      term.write(out);
    } else if (msg.type === "closed") {
      setMode("closed");
      showOverlay("Terminal closed", "The pane has been closed. Reconnect to reattach.", true);
    } else if (msg.type === "error") {
      setMode("readonly");
      showOverlay("Connection error", msg.message || "unknown error", true);
    }
  });

  socket.addEventListener("close", (event) => {
    socket = null;
    if (event.code === 1001 || event.code === 4001) {
      setMode("readonly");
      const detail =
        event.code === 4001
          ? "A writable session is already active. This view is read-only."
          : "Disconnected from the terminal.";
      showOverlay("Disconnected", detail, true);
    } else if (event.code !== 1000) {
      scheduleReconnect();
    }
  });

  socket.addEventListener("error", () => {
    socket = null;
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }
  showOverlay("Disconnected", "Reconnecting in 3 seconds...", false);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 3000);
}

function send(obj) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(obj));
  }
}

function resizeTerminal() {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  const { cols, rows } = fitAddon.proposeDimensions();
  if (!cols || !rows) {
    return;
  }
  term.resize(cols, rows);
  send({ type: "resize", cols, rows });
}

term.onData((data) => {
  if (mode === "write") {
    send({ type: "input", text: data });
  }
});

term.onResize(() => {
  if (mode === "write") {
    resizeTerminal();
  }
});

window.addEventListener("resize", () => {
  if (mode === "write") {
    resizeTerminal();
  }
});

reconnectBtn.addEventListener("click", connect);
connect();
