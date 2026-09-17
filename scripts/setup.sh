#!/usr/bin/env sh
set -eu

artifact_root="${IMPOSSIBLE_INFERENCES_ARTIFACT_ROOT:-runtime-artifacts}"
if [ "${1:-}" = "--offline" ]; then
  exec cargo run --locked -p impossible-inferences-server -- setup --artifact-root "$artifact_root" --offline
fi
if [ "$#" -ne 0 ]; then
  echo "usage: scripts/setup.sh [--offline]" >&2
  exit 2
fi
exec cargo run --locked -p impossible-inferences-server -- setup --artifact-root "$artifact_root"
