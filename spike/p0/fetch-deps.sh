#!/usr/bin/env bash
# runtimed cannot be consumed through the module proxy (its internal/execshim/nul.go
# makes the module zip invalid), so the spike builds against pinned local checkouts.
set -euo pipefail
cd "$(dirname "$0")"
pin() { # repo commit
  local dir=third_party/$1
  [ -d "$dir/.git" ] || git clone -q "https://github.com/k3sm-io/$1" "$dir"
  git -C "$dir" fetch -q origin "$2" && git -C "$dir" -c advice.detachedHead=false checkout -q "$2"
  echo "$1 @ $(git -C "$dir" rev-parse --short HEAD)"
}
pin apis f6ce8a4a981f6b37dd132c4b7a23ce3ccfe02853
pin runtimed 32fe2ec31ce9c0d524a4fe5034d3f6695595f2ed
