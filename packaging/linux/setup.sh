#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "${1:-}" = "--offline" ]; then
  exec "$root/impossible-inferences" setup --artifact-root "$root/runtime-artifacts" --offline
fi
if [ "$#" -ne 0 ]; then
  echo "usage: ./setup.sh [--offline]" >&2
  exit 2
fi
exec "$root/impossible-inferences" setup --artifact-root "$root/runtime-artifacts"
