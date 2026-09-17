#!/usr/bin/env sh
set -eu

artifact_root="${IMPOSSIBLE_INFERENCES_ARTIFACT_ROOT:-runtime-artifacts}"
if [ "$#" -ne 0 ]; then
  echo "usage: scripts/serve.sh" >&2
  exit 2
fi
exec cargo run --locked -p impossible-inferences-server -- serve --artifact-root "$artifact_root"
