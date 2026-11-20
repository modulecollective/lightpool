#!/usr/bin/env bash
# Balanced mode order, one fresh process per attempt. Keep all attempts.
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: healthy.sh NEW_DIRECTORY' >&2; exit 2; }
mkdir "$1"
out=$(cd "$1" && pwd)
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
source=$(<"$record/healthy-base")
mkdir "$out/src" "$out/raw" "$out/evidence"
git -C "$repo" archive "$source" | tar -x -C "$out/src"
(cd "$out/src" && GOTOOLCHAIN=$(<"$record/toolchain") go test -c -trimpath -buildvcs=false -o "$out/driver.test" .)
digest=$(sha256sum "$out/driver.test" | cut -d' ' -f1)
uname -a > "$out/host.txt"
go version -m "$out/driver.test" >> "$out/host.txt"
printf 'attempt\tdirectory\tprovenance\texitCode\tstatus\trawBytes\tstoredBytes\n' > "$out/evidence.tsv"
size() { find "$1" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1}END{printf "%.0f",n}'; }
provenance=original
if [[ -e "$record/evidence.tsv" ]]; then provenance=regenerated; fi
for pair in 1 2 3 4; do
  modes=(off on)
  if (( pair % 2 == 0 )); then modes=(on off); fi
  for mode in "${modes[@]}"; do
    id="healthy-$pair-$mode"
    raw="$out/raw/$id"
    available=$(df -PB1 "$out" | awk 'END{print $4}')
    (( available >= 3 * 1024**3 )) || { echo 'insufficient space for next recording' >&2; exit 1; }
    code=0
    LIGHTPOOL_TIMING_SOURCE="$source" LIGHTPOOL_TIMING_BINARY="$digest" LIGHTPOOL_TIMING_MODE="$mode" LIGHTPOOL_TIMING_OUT="$raw" "$out/driver.test" -test.run '^TestHealthyTiming$' -test.timeout=90s -test.v > "$out/log.txt" 2>&1 || code=$?
    mkdir -p "$raw"
    mv "$out/log.txt" "$raw/"
    cp "$out/host.txt" "$raw/"
    printf '%s\n' "$digest" "$source" "$code" > "$raw/provenance.txt"
    "$repo/experiments/evidence.sh" pack "$raw" "$out/evidence/$id"
    status=complete
    if ((code!=0)); then status=incomplete; fi
    printf '%s\tevidence/%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$id" "$provenance" "$code" "$status" "$(size "$raw")" "$(size "$out/evidence/$id")" >> "$out/evidence.tsv"
    echo "retained $id ($status)"
  done
done
