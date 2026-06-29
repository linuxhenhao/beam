#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for pkg in beam-cli beam-daemon beam-worker; do
  cargo install --path "$root/crates/$pkg" --force
done
