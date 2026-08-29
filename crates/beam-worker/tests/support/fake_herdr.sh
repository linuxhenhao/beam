#!/usr/bin/env bash
# Fake herdr CLI for hermetic tests (design: "Fake herdr shim 合同").
# State lives in $FAKE_HERDR_STATE. Never executes a real PTY.
set -u

STATE="${FAKE_HERDR_STATE:?fake herdr needs FAKE_HERDR_STATE}"
mkdir -p "$STATE/workspaces" "$STATE/input"

next_workspace_id() {
  local n
  n=1
  while [ -f "$STATE/workspaces/w$n" ]; do
    n=$((n + 1))
  done
  echo "w$n"
}

workspace_store() {
  # label -> workspace id
  echo "$1" > "$STATE/workspace_label"
}

case "${1:-}" in
  --version)
    echo "herdr 0.8.2"
    exit 0
    ;;
  api)
    if [ "${2:-}" = "schema" ] && [ "${3:-}" = "--json" ]; then
      cat "$(dirname "$0")/../fixtures/herdr/api-schema-0.8.2.json"
      exit 0
    fi
    echo "usage error" >&2
    exit 2
    ;;
  status)
    if [ "${2:-}" = "server" ]; then
      if [ -f "$STATE/server_up" ]; then
        echo '{"status":"ok"}'
        exit 0
      fi
      echo "server not running" >&2
      exit 1
    fi
    exit 2
    ;;
  server)
    touch "$STATE/server_up"
    exit 0
    ;;
  workspace)
    case "${2:-}" in
      list)
        if [ -f "$STATE/workspace_label" ]; then
          wid=$(cat "$STATE/workspace_id" 2>/dev/null || echo "w1")
          printf '{"result":{"workspaces":[{"workspace_id":"%s","label":"%s"}]}}\n' "$wid" "$(cat "$STATE/workspace_label")"
        else
          echo '{"result":{"workspaces":[]}}'
        fi
        exit 0
        ;;
      create)
        label=""
        prev=""
        for arg in "$@"; do
          if [ "$prev" = "--label" ]; then
            label="$arg"
            prev=""
          elif [ "$arg" = "--label" ]; then
            prev="--label"
          fi
        done
        if [ -f "$STATE/workspace_label" ] && [ "$(cat "$STATE/workspace_label")" = "$label" ]; then
          existing=$(cat "$STATE/workspace_id" 2>/dev/null || echo "w1")
          touch "$STATE/workspaces/$existing"
          printf '{"result":{"workspace":{"workspace_id":"%s"},"tab":{"tab_id":"t1"},"root_pane":{"pane_id":"%s:p1"}}}\n' "$existing" "$existing"
          exit 0
        fi
        wid=$(next_workspace_id)
        touch "$STATE/workspaces/$wid"
        echo "$label" > "$STATE/workspace_label"
        echo "$wid" > "$STATE/workspace_id"
        printf '{"result":{"workspace":{"workspace_id":"%s"},"tab":{"tab_id":"t1"},"root_pane":{"pane_id":"%s:p1"}}}\n' "$wid" "$wid"
        exit 0
        ;;
      get)
        id="${3:-}"
        if [ -f "$STATE/workspaces/$id" ]; then
          printf '{"result":{"workspace":{"workspace_id":"%s","root_pane":{"pane_id":"%s:p1"}}}}\n' "$id" "$id"
          exit 0
        fi
        echo '{"error":"not_found"}'
        exit 1
        ;;
      close)
        id="${3:-}"
        force=0
        case "$3" in
          --force) force=1; id="${4:-}" ;;
        esac
        if [ "$force" = "0" ]; then
          echo '{"error":{"code":"confirmation_required","message":"confirm"}}' >&2
          exit 1
        fi
        rm -f "$STATE/workspaces/$id"
        rm -f "$STATE/workspace_label" "$STATE/workspace_id"
        echo '{"ok":true}'
        exit 0
        ;;
      *)
        exit 2
        ;;
    esac
    ;;
  pane)
    case "${2:-}" in
      run)
        echo "$3 $4" >> "$STATE/input/run.log"
        if [ -f "$STATE/fail_pane_run_once" ]; then
          rm -f "$STATE/fail_pane_run_once"
          echo '{"error":"shell not ready"}' >&2
          exit 1
        fi
        echo '{"ok":true}'
        exit 0
        ;;
      wait-output)
        if [ -f "$STATE/wait_output_timeout" ]; then
          sleep 1
          exit 1
        fi
        exit 0
        ;;
      send-text)
        echo "${4:-}" >> "$STATE/input/send_text.log"
        echo '{"ok":true}'
        exit 0
        ;;
      send-keys)
        echo "$3 ${4:-}" >> "$STATE/input/send_keys.log"
        echo '{"ok":true}'
        exit 0
        ;;
      read)
        printf 'hello\r\nworld\r\n'
        exit 0
        ;;
      process-info)
        if [ -f "$STATE/empty_foreground" ]; then
          echo '{"pid":null,"argv":"","cwd":null}'
          exit 0
        fi
        cat "$(dirname "$0")/../fixtures/herdr/process-info.json"
        exit 0
        ;;
      *)
        exit 2
        ;;
    esac
    ;;
  agent)
    case "${2:-}" in
      list)
        cat "$(dirname "$0")/../fixtures/herdr/agent-get.json"
        exit 0
        ;;
      get)
        if [ -f "$STATE/agent_idle" ]; then
          echo '{"result":{"state":"idle"}}'
          exit 0
        fi
        cat "$(dirname "$0")/../fixtures/herdr/agent-get.json"
        exit 0
        ;;
      *)
        exit 2
        ;;
    esac
    ;;
  terminal)
    if [ "${2:-}" = "session" ] && [ "${3:-}" = "observe" ]; then
      pane="${4:-}"
      b64=$(printf '\033[H\033[2Jfake frame' | base64 | tr -d '\n')
      printf '{"type":"frame","data":"%s"}\n' "$b64"
      # Block until SIGTERM, then report closed.
      trap 'echo "{\"type\":\"terminal.closed\"}"; exit 0' TERM INT
      while :; do sleep 1; done
    fi
    exit 2
    ;;
  *)
    echo "unknown subcommand" >&2
    exit 2
    ;;
esac
