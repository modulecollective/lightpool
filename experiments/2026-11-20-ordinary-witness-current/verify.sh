#!/usr/bin/env bash
# Restore every committed attempt, rebuild the pinned tools from base, reduce
# each saved attempt twice and compare the outputs, then rebuild the tables and
# compare them with the committed ones. No protocol process runs here.
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: verify.sh NEW_DIRECTORY' >&2; exit 2; }
mkdir -- "$1"
out=$(cd "$1" && pwd)
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
export GOTOOLCHAIN
GOTOOLCHAIN=$(<"$record/toolchain")
mkdir "$out/src" "$out/restored" "$out/reports"
git -C "$repo" archive "$(bash "$record/../revision.sh" "$(<"$record/base")")" | tar -x -C "$out/src"
(cd "$out/src" && go test -c -trimpath -buildvcs=false -o "$out/driver.test" . &&
  go build -trimpath -buildvcs=false -o "$out/producer" ./cmd/lightpool-experiment)
driver=$(sha256sum "$out/driver.test" | cut -d' ' -f1)
producer=$(sha256sum "$out/producer" | cut -d' ' -f1)
witness_header=$(grep -o '"attempt\\tmode[^"]*"' "$out/src/internal/experiment/ordinary_witness.go" | tr -d '"' | sed 's/\\t/\t/g')
printf '%s\n' "$witness_header" > "$out/ordinary-cells.tsv"
printf 'attempt\tmetric\tn\tmedian\tp95\tp99\tmax\n' > "$out/ordinary-timing.tsv"
printf 'case\tindex\tstatus\tfailedProbes\tlocalSuspicions\teligibleEpisodes\twitnesses\tharmful\thealthyDead\tpairExposureNS\tmaxBacklog\tlateTimers\tmaxTimerLatenessNS\tstop\n' > "$out/virtual-cells.tsv"
printf 'attempt\tmetric\tn\tmedian\tp95\tp99\tmax\n' > "$out/timing.tsv"
printf 'probe\texpect\tsent\tdelivered\tpass\tlowerP50NS\tupperMaxNS\n' > "$out/controls.tsv"
while IFS=$'\t' read -r id path provenance code status rawBytes storedBytes; do
  [[ $id != attempt ]] || continue
  dest="$out/restored/$id"
  "$repo/experiments/evidence.sh" unpack "$record/$path" "$dest"
  [[ $(find "$dest" -type f -printf '%s\n' | awk '{n+=$1} END{printf "%.0f", n}') == "$rawBytes" ]]
  [[ $(find "$record/$path" -type f -printf '%s\n' | awk '{n+=$1} END{printf "%.0f", n}') == "$storedBytes" ]]
  [[ $provenance == original && $(<"$dest/exit.txt") == "$code" ]]
  case $id in
  controls)
    [[ $(jq -r .binary "$dest/controls.json") == "$driver" && $status == pass ]]
    jq -r '.probes[] | [.name,.expect,.sent,.delivered,.pass,(.delayNS.lower.p50//"-"),(.delayNS.upper.max//"-")] | @tsv' "$dest/controls.json" >> "$out/controls.tsv"
    ;;
  healthy-*)
    if [[ $status != complete ]]; then
      printf '%s\tincomplete\t0\t-\t-\t-\t-\n' "$id" >> "$out/timing.tsv"
      continue
    fi
    if [[ -f "$dest/observer.ndjson" ]]; then
      LIGHTPOOL_REPLAY_HEALTHY_JOURNAL="$dest/observer.ndjson" "$out/driver.test" -test.run '^TestReplayHealthyJournal$' > /dev/null
    fi
    "$record/timing.sh" "$dest" "$id" >> "$out/timing.tsv"
    ;;
  [0-9][0-9]-*)
    [[ $(jq -r .binary "$dest/manifest.json") == "$driver" && $(jq -r .status "$dest/status.json") == "$status" ]]
    rows=()
    for pass in 1 2; do
      actual=0
      rows+=("$("$out/producer" -witness "$dest" -out "$out/reports/$id.$pass.json")") || actual=$?
      [[ ( $status == complete && $actual == 0 ) || ( $status != complete && $actual != 0 ) ]]
    done
    cmp "$out/reports/$id.1.json" "$out/reports/$id.2.json"
    [[ ${rows[0]} == "${rows[1]}" ]]
    printf '%s\n' "${rows[0]}" >> "$out/ordinary-cells.tsv"
    jq -r --arg id "$id" '.recordedTiming | to_entries[] | [$id, .key, .value.n, (.value.p50//"-"), (.value.p95//"-"), (.value.p99//"-"), (.value.max//"-")] | @tsv' "$out/reports/$id.1.json" >> "$out/ordinary-timing.tsv"
    ;;
  *)
    [[ $(<"$dest/binary.sha256") == "$producer" && $(jq -r .status "$dest/status.json") == "$status" ]]
    for pass in 1 2; do
      for verb in reduce admission; do
        actual=0
        "$out/producer" "-$verb" "$dest" -out "$out/reports/$id.$verb.$pass.json" || actual=$?
        [[ $actual == "$code" ]]
      done
      cmp "$dest/summary.json" "$out/reports/$id.reduce.$pass.json"
    done
    cmp "$out/reports/$id.admission.1.json" "$out/reports/$id.admission.2.json"
    jq -r '[.spec.case,.spec.replicate,.status,.aProbeFailures,.aLocalSuspicions,.eligibleEpisodes,.accelerationWitnesses,.harmfulEpisodes,(.populations[] | select(.label=="healthy-pair-exclusions")|.transitions),(.populations[] | select(.label=="healthy-pair-exclusions")|.pairNS),.maxBacklog,.lateTimers,.maxTimerLatenessNS,.chainStoppedAt]|@tsv' "$out/reports/$id.admission.1.json" >> "$out/virtual-cells.tsv"
    ;;
  esac
  echo "verified $id"
done < "$record/evidence.tsv"
[[ $(wc -l < "$out/ordinary-cells.tsv") == 37 && $(wc -l < "$out/virtual-cells.tsv") == 19 ]]
for table in ordinary-cells ordinary-timing virtual-cells timing controls; do
  if [[ -f "$record/$table.tsv" ]]; then cmp "$record/$table.tsv" "$out/$table.tsv"; fi
done
echo "verified all attempts and tables"
