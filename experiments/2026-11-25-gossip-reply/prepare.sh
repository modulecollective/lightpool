#!/usr/bin/env bash
# Build both producers/reducers from the same pinned tooling and frozen patch.
set -euo pipefail
[ "$#" -eq 1 ] || { echo 'usage: prepare.sh NEW_DIRECTORY' >&2; exit 1; }
record=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$record/../.." && pwd)
mkdir "$1"
out=$(cd "$1" && pwd)
base=$(cat "$record/tools-base")
commit=$(bash "$record/../revision.sh" "$base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(python3 --version)" = "Python 3.13.5" ]
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ]
git -C "$repo" merge-base --is-ancestor "$commit" HEAD
(cd "$record" && sha256sum -c PATCHES.sha256)
mkdir "$out/baseline" "$out/candidate"
# Build source and simulator fixtures; historical campaign payloads are not
# compiler inputs. The full-suite results are recorded against candidate-commit.
git -C "$repo" archive "$commit" -- '*.go' go.mod .golangci.yml examples sim/trace/testdata experiments/evidence.sh | tar -x -C "$out/baseline"
cp -a "$out/baseline/." "$out/candidate/"
git -C "$out/candidate" apply "$record/reply.patch"
git -C "$out/candidate" apply "$record/ordinary-test.patch"
(cd "$out/baseline" && go build -buildvcs=false -o "$out/baseline-experiment" ./cmd/lightpool-experiment)
(cd "$out/candidate" && go build -buildvcs=false -o "$out/candidate-experiment" ./cmd/lightpool-experiment)
