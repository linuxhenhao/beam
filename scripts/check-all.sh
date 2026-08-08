#!/usr/bin/env bash
# Unified harness entry point: runs every workspace check in order.
# Each check is an independent script so they can also be run individually.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> [1/4] rust source line count"
"$repo_root/scripts/check-rust-line-count.sh"

echo "==> [2/4] clippy (zero warnings)"
"$repo_root/scripts/check-clippy.sh"

echo "==> [3/4] build"
"$repo_root/scripts/check-build.sh"

echo "==> [4/4] tests"
"$repo_root/scripts/check-tests.sh"

echo "All harness checks passed."
