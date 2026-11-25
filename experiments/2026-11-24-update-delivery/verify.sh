#!/usr/bin/env bash
# Restore originals from Git, rather than proving only working-tree packaging.
set -euo pipefail
record=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$record/../.." && pwd)
relative=experiments/2026-11-24-update-delivery
[ "$#" -eq 1 ] || { echo 'usage: verify.sh NEW_DIRECTORY' >&2; exit 1; }
mkdir "$1"
out=$(cd "$1" && pwd)
base=$(cat "$record/base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ]
git -C "$repo" merge-base --is-ancestor "$base" HEAD
mkdir "$out/source" "$out/committed" "$out/restored"
git -C "$repo" archive "$base" | tar -x -C "$out/source"
git -C "$repo" archive HEAD "$relative/evidence" "$relative/cells.tsv" | tar -x -C "$out/committed"
(cd "$out/source" && go build -buildvcs=false -o "$out/lightpool-experiment" ./cmd/lightpool-experiment)
python3 "$record/rows.py" --header >"$out/cells.tsv"
for packed in "$out/committed/$relative/evidence/"*; do
 cell=${packed##*/}
 dir="$out/restored/$cell"
 bash "$repo/experiments/evidence.sh" unpack "$packed" "$dir"
 find "$dir" -maxdepth 1 -type f -printf '%f\n' | sort >"$out/$cell.before"
 code=0
 "$out/lightpool-experiment" -reduce "$dir" -out "$out/$cell.reduced.json" >"$out/$cell.reduce.stdout" 2>"$out/$cell.reduce.stderr" || code=$?
 [ "$code" -eq "$(cat "$dir/exit-code")" ]
 cmp "$dir/summary.json" "$out/$cell.reduced.json"
 (cd "$dir" && sha256sum -c "$packed/SHA256SUMS") >"$out/$cell.checksums"
 find "$dir" -maxdepth 1 -type f -printf '%f\n' | sort >"$out/$cell.after"
 cmp "$out/$cell.before" "$out/$cell.after"
 python3 "$record/rows.py" "$dir" >>"$out/cells.tsv"
 printf '%s: exact restoration, exit/status and offline summary match\n' "$cell"
done
# Packing order is lexicographic; the campaign's declaration order differs.
LC_ALL=C sort "$out/cells.tsv" >"$out/actual.sorted"
LC_ALL=C sort "$out/committed/$relative/cells.tsv" >"$out/expected.sorted"
cmp "$out/actual.sorted" "$out/expected.sorted"
echo 'all six committed originals verified'
