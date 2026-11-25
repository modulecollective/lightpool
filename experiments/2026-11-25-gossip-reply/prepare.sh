#!/usr/bin/env bash
# Build both producers/reducers from the same pinned tooling and frozen patch.
set -euo pipefail
[ "$#" -eq 1 ] || { echo 'usage: prepare.sh NEW_DIRECTORY' >&2; exit 1; }
record=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$record/../.." && pwd)
mkdir "$1"
out=$(cd "$1" && pwd)
base=$(cat "$record/tools-base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ]
git -C "$repo" merge-base --is-ancestor "$base" HEAD
(cd "$record" && sha256sum -c PATCHES.sha256)
mkdir "$out/baseline" "$out/candidate"
# Full source keeps all fixtures available for independent candidate checks.
git -C "$repo" archive "$base" | tar -x -C "$out/baseline"
cp -a "$out/baseline/." "$out/candidate/"
git -C "$out/candidate" apply "$record/reply.patch"
git -C "$out/candidate" apply "$record/ordinary-test.patch"
(cd "$out/baseline" && go build -buildvcs=false -o "$out/baseline-experiment" ./cmd/lightpool-experiment)
(cd "$out/candidate" && go build -buildvcs=false -o "$out/candidate-experiment" ./cmd/lightpool-experiment)
