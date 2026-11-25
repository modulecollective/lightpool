#!/usr/bin/env bash
# Rebuild the pinned tooling and regenerate exactly the predeclared six rows.
set -euo pipefail
record=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$record/../.." && pwd)
[ "$#" -eq 1 ] || { echo 'usage: run.sh NEW_DIRECTORY' >&2; exit 1; }
mkdir "$1"
out=$(cd "$1" && pwd)
base=$(cat "$record/base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ]
git -C "$repo" merge-base --is-ancestor "$base" HEAD
mkdir "$out/src" "$out/cells"
git -C "$repo" archive "$base" | tar -x -C "$out/src"
(cd "$out/src" && go build -buildvcs=false -o "$out/lightpool-experiment" ./cmd/lightpool-experiment)
python3 "$record/rows.py" --header >"$out/cells.tsv"
failed=0
for loss in 0 10 30; do
 for mode in on off; do
  name="delivery-32-loss-$loss-exchange-$mode"
  cell="$name-r1"
  dir="$out/cells/$cell"
  code=0
  "$out/lightpool-experiment" -case "$name" -replicate 1 -revision "$base" -out "$dir" >"$out/$cell.stdout" 2>"$out/$cell.stderr" || code=$?
  # Keep diagnostics even when recording failed before directory creation.
  mkdir -p "$dir"
  mv "$out/$cell.stdout" "$dir/command.stdout"
  mv "$out/$cell.stderr" "$dir/command.stderr"
  printf '%s\n' "$code" >"$dir/exit-code"
  if [ -f "$dir/summary.json" ]; then
   reduce_code=0
   "$out/lightpool-experiment" -reduce "$dir" -out "$out/$cell.reduced.json" >"$out/$cell.reduce.stdout" 2>"$out/$cell.reduce.stderr" || reduce_code=$?
   if ! cmp -s "$dir/summary.json" "$out/$cell.reduced.json" || [ "$code" -ne "$reduce_code" ]; then
    echo "run.sh: $cell reduction mismatch" >&2; failed=1
   fi
  fi
  row=$(python3 "$record/rows.py" "$dir")
  printf '%s\n' "$row" >>"$out/cells.tsv"
  if [ -f "$record/cells.tsv" ]; then
   expected=$(awk -F '\t' -v cell="$cell" '$1 == cell' "$record/cells.tsv")
   if [ "$expected" != "$row" ]; then echo "run.sh: $cell row mismatch" >&2; failed=1; fi
  fi
  printf '%s exit=%s\n' "$cell" "$code" >&2
 done
done
exit "$failed"
