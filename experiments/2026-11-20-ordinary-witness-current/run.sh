#!/usr/bin/env bash
# Run the frozen study in a new directory: socket controls, healthy timing,
# the 36 ordinary cells and the 18 current-baseline virtual cells, in that
# order. Every attempt is packed, restored and checked before its raw files
# are released; the packed total may not exceed 512 MiB. Keep every attempt.
# Ordinary attempts need unprivileged user namespaces (not an agent sandbox).
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: run.sh NEW_DIRECTORY' >&2; exit 2; }
mkdir -- "$1"
out=$(cd "$1" && pwd)
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
base=$(<"$record/base")
commit=$(bash "$record/../revision.sh" "$base")
export GOTOOLCHAIN
GOTOOLCHAIN=$(<"$record/toolchain")
cap=$((512 * 1024 * 1024))
mkdir "$out/src" "$out/raw" "$out/evidence" "$out/check"
git -C "$repo" archive "$commit" | tar -x -C "$out/src"
(cd "$out/src" && go test -c -trimpath -buildvcs=false -o "$out/driver.test" . &&
  go build -trimpath -buildvcs=false -o "$out/producer" ./cmd/lightpool-experiment)
driver=$(sha256sum "$out/driver.test" | cut -d' ' -f1)
producer=$(sha256sum "$out/producer" | cut -d' ' -f1)
{ uname -a; nproc; head -3 /proc/meminfo; go version -m "$out/driver.test" | head -2; } > "$out/host.txt"
size() { find "$1" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1}END{printf "%.0f",n}'; }
provenance=original
if [[ -e "$record/evidence.tsv" ]]; then provenance=regenerated; fi
printf 'attempt\tdirectory\tprovenance\texitCode\tstatus\trawBytes\tstoredBytes\n' > "$out/evidence.tsv"
preflight() {
  local available
  available=$(df -PB1 "$out" | awk 'END{print $4}')
  (( available >= 8 * 1024**3 )) || { echo 'insufficient space for the next attempt, packing and check' >&2; exit 1; }
}
# retain ID CODE STATUS [keep]: pack, restore, compare, account, then release.
retain() {
  local id=$1 raw="$out/raw/$1" used
  cp "$out/host.txt" "$raw/"
  printf '%s\n' "$2" > "$raw/exit.txt"
  "$out/src/experiments/evidence.sh" pack "$raw" "$out/evidence/$id"
  "$out/src/experiments/evidence.sh" unpack "$out/evidence/$id" "$out/check/$id"
  diff -r "$raw" "$out/check/$id"
  rm -rf "$out/check/$id"
  printf '%s\tevidence/%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$id" "$provenance" "$2" "$3" "$(size "$raw")" "$(size "$out/evidence/$id")" >> "$out/evidence.tsv"
  used=$(du -sb "$out/evidence" | cut -f1)
  echo "retained $id ($3), packed total $used bytes"
  (( used <= cap )) || { echo "packed evidence exceeds the 512 MiB cap after $id; stopping" >&2; exit 1; }
  [[ ${4:-} == keep ]] || rm -rf "$raw"
}
ordinary() {
  LIGHTPOOL_WITNESS_SOURCE="$base" LIGHTPOOL_WITNESS_BINARY="$driver" LIGHTPOOL_WITNESS_OUT="$out/raw" \
    LIGHTPOOL_WITNESS_PLAN="$1" LIGHTPOOL_WITNESS_CELLS="${2:-}" \
    "$out/driver.test" -test.run '^TestOrdinaryWitness$' -test.v -test.timeout=0
}
preflight
code=0
ordinary controls > "$out/controls.log" 2>&1 || code=$?
mv "$out/controls.log" "$out/raw/controls/log.txt"
status=$(jq -r 'if .pass then "pass" else "fail" end' "$out/raw/controls/controls.json")
retain controls "$code" "$status" keep
[[ $status == pass ]] || { echo 'socket controls failed; launch refused' >&2; exit 1; }
for pair in 1 2 3 4; do
  modes=(off on)
  if (( pair % 2 == 0 )); then modes=(on off); fi
  for mode in "${modes[@]}"; do
    id="healthy-$pair-$mode"
    preflight
    code=0
    LIGHTPOOL_TIMING_SOURCE="$base" LIGHTPOOL_TIMING_BINARY="$driver" LIGHTPOOL_TIMING_MODE="$mode" LIGHTPOOL_TIMING_OUT="$out/raw/$id" \
      "$out/driver.test" -test.run '^TestHealthyTiming$' -test.timeout=90s -test.v > "$out/log.txt" 2>&1 || code=$?
    mkdir -p "$out/raw/$id"
    mv "$out/log.txt" "$out/raw/$id/"
    status=complete
    if (( code != 0 )); then status=incomplete; fi
    retain "$id" "$code" "$status"
  done
done
for order in $(seq 1 36); do
  preflight
  code=0
  ordinary cells "$order" > "$out/log.txt" 2>&1 || code=$?
  id=$(cd "$out/raw" && ls -d "$(printf '%02d' "$order")"-* )
  mv "$out/log.txt" "$out/raw/$id/log.txt"
  retain "$id" "$code" "$(jq -r .status "$out/raw/$id/status.json")"
done
rm -rf "$out/raw/controls"
for fixture in correlated-dense{,-healthy,-latency-100us,-latency-10ms,-service-100us,-service-1ms}; do
  for i in 1 2 3; do
    preflight
    id="$fixture-r$i"
    code=0
    "$out/producer" -revision "$base" -case "$fixture" -replicate "$i" -out "$out/raw/$id" > "$out/stdout.txt" 2> "$out/stderr.txt" || code=$?
    mkdir -p "$out/raw/$id"
    mv "$out/stdout.txt" "$out/stderr.txt" "$out/raw/$id/"
    printf '%s\n' "$producer" > "$out/raw/$id/binary.sha256"
    status=unknown
    if [[ -f "$out/raw/$id/status.json" ]]; then status=$(jq -r .status "$out/raw/$id/status.json"); fi
    retain "$id" "$code" "$status"
  done
done
echo "complete: copy evidence/ and evidence.tsv into $record, then run verify.sh"
