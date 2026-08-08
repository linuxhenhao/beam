#!/usr/bin/env bash
# Run the full workspace test suite.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo test --workspace --no-fail-fast
