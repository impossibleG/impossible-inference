#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -ne 0 ]; then
  echo "usage: ./serve.sh" >&2
  exit 2
fi
exec "$root/impossible-inferences" serve --artifact-root "$root/runtime-artifacts"
