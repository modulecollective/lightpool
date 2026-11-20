#!/usr/bin/env bash
# Recreate the fixed virtual companions in a new directory. Keep failed cells.
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: run.sh NEW_DIRECTORY' >&2; exit 2; }
mkdir -- "$1"
out=$(cd "$1" && pwd)
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
base=$(<"$record/base")
export GOTOOLCHAIN
GOTOOLCHAIN=$(<"$record/toolchain")
mkdir "$out/src" "$out/raw" "$out/evidence"
git -C "$repo" archive "$base" | tar -x -C "$out/src"
(cd "$out/src" && go build -trimpath -buildvcs=false -o "$out/producer" ./cmd/lightpool-experiment)
digest=$(sha256sum "$out/producer" | cut -d' ' -f1)
size() { find "$1" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1}END{printf "%.0f",n}'; }
provenance=original
if [[ -e "$record/evidence.tsv" ]]; then provenance=regenerated; fi
printf 'attempt\tdirectory\tprovenance\texitCode\tstatus\trawBytes\tstoredBytes\n' > "$out/evidence.tsv"
for fixture in correlated-dense{,-healthy,-latency-100us,-latency-10ms,-service-100us,-service-1ms}; do
  for i in 1 2 3; do
    available=$(df -PB1 "$out" | awk 'END{print $4}')
    (( available >= 3 * 1024**3 )) || { echo 'insufficient space for next trace and packaging' >&2; exit 1; }
    id="$fixture-r$i"
    raw="$out/raw/$id"
    code=0
    "$out/producer" -revision "$base" -case "$fixture" -replicate "$i" -out "$raw" >"$out/stdout.txt" 2>"$out/stderr.txt" || code=$?
    mkdir -p "$raw"
    mv "$out/stdout.txt" "$out/stderr.txt" "$raw/"
    printf '%s\n' "$digest" > "$raw/binary.sha256"
    printf '%s\n' "$code" > "$raw/exit.txt"
    status=unknown
    if [[ -f "$raw/status.json" ]]; then status=$(jq -r .status "$raw/status.json"); fi
    "$out/src/experiments/evidence.sh" pack "$raw" "$out/evidence/$id"
    printf '%s\tevidence/%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$id" "$provenance" "$code" "$status" "$(size "$raw")" "$(size "$out/evidence/$id")" >> "$out/evidence.tsv"
    if [[ $provenance == regenerated ]]; then
      for name in spec.json scenario.json status.json report.json trace.ndjson summary.json exit.txt; do
        awk -v name="$name" '$2==name{print; found=1} END{if (!found) exit 1}' "$record/evidence/$id/SHA256SUMS" > "$out/check.sha256"
        (cd "$raw" && sha256sum --status -c "$out/check.sha256") || { echo "$id differs in $name" >&2; exit 1; }
      done
    fi
    echo "retained $id ($status)"
  done
done
