#!/usr/bin/env bash
# Enforce rustfmt across the workspace (local and CI).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo fmt --check
