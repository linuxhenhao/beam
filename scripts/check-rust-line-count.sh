#!/usr/bin/env bash
# Enforce the Rust source-file size limit used by local tests and CI.
set -euo pipefail

readonly max_lines=1000
readonly repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

failed=0
while IFS= read -r -d '' file; do
  lines=$(wc -l < "$file")
  if (( lines > max_lines )); then
    printf 'FAIL: %s has %s lines (limit: %s)\n' \
      "${file#"$repo_root"/}" "$lines" "$max_lines" >&2
    failed=1
  fi
done < <(find "$repo_root/crates" -type f -name '*.rs' -print0)

exit "$failed"
