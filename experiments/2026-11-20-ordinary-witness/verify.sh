#!/usr/bin/env bash
# Restore committed originals, remeasure locally and compare both derived tables.
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: verify.sh NEW_DIRECTORY' >&2; exit 2; }
mkdir "$1"
out=$(cd "$1" && pwd)
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
export GOTOOLCHAIN
GOTOOLCHAIN=$(<"$record/toolchain")
mkdir "$out/src" "$out/driver" "$out/restored"
git -C "$repo" archive "$(cat "$record/base")" | tar -x -C "$out/src"
(cd "$out/src" && go build -trimpath -buildvcs=false -o "$out/producer" ./cmd/lightpool-experiment)
git -C "$repo" archive "$(cat "$record/healthy-base")" | tar -x -C "$out/driver"
(cd "$out/driver" && go test -c -trimpath -buildvcs=false -o "$out/driver.test" .)
sha=$(sha256sum "$out/producer" | cut -d' ' -f1)
driver=$(sha256sum "$out/driver.test" | cut -d' ' -f1)
(cd "$repo" && go test -c -trimpath -buildvcs=false -o "$out/replay.test" .)
printf 'case\tindex\tstatus\tfailedProbes\tlocalSuspicions\teligibleEpisodes\twitnesses\tharmful\thealthyDead\tpairExposureNS\tmaxBacklog\tlateTimers\tmaxTimerLatenessNS\tstop\n' > "$out/cells.tsv"
printf 'attempt\tmetric\tn\tmedian\tp95\tp99\tmax\n' > "$out/timing.tsv"
while IFS=$'\t' read -r id path provenance code status rawBytes storedBytes; do
  [[ $id != attempt ]] || continue
  dest="$out/restored/$id"
  "$repo/experiments/evidence.sh" unpack "$record/$path" "$dest"
  raw=$(find "$dest" -type f -printf '%s\n' | awk '{n+=$1} END{printf "%.0f", n}')
  stored=$(find "$record/$path" -type f -printf '%s\n' | awk '{n+=$1} END{printf "%.0f", n}')
  [[ $raw == "$rawBytes" && $stored == "$storedBytes" && $provenance == original ]]
  if [[ $id == healthy-* ]]; then
    [[ $(head -1 "$dest/provenance.txt") == "$driver" && $(tail -1 "$dest/provenance.txt") == "$code" ]]
    if [[ $status != complete ]]; then
      printf '%s\tincomplete\t0\t-\t-\t-\t-\n' "$id" >> "$out/timing.tsv"
      continue
    fi
    [[ $code == 0 ]]
    if [[ -f "$dest/observer.ndjson" ]]; then
      LIGHTPOOL_REPLAY_HEALTHY_JOURNAL="$dest/observer.ndjson" "$out/replay.test" -test.run "^TestReplayHealthyJournal$"
    fi
    "$record/timing.sh" "$dest" "$id" >> "$out/timing.tsv"
    continue
  fi
  [[ $(<"$dest/binary.sha256") == "$sha" && $(<"$dest/exit.txt") == "$code" && $(jq -r .status "$dest/status.json") == "$status" ]]
  for pass in 1 2; do
    for verb in reduce admission; do
      actual=0
      "$out/producer" "-$verb" "$dest" -out "$out/$id.$verb.$pass.json" || actual=$?
      [[ $actual == "$code" ]]
    done
    cmp "$dest/summary.json" "$out/$id.reduce.$pass.json"
  done
  cmp "$out/$id.admission.1.json" "$out/$id.admission.2.json"
  jq -r '[.spec.case,.spec.replicate,.status,.aProbeFailures,.aLocalSuspicions,.eligibleEpisodes,.accelerationWitnesses,.harmfulEpisodes,(.populations[] | select(.label=="healthy-pair-exclusions")|.transitions),(.populations[] | select(.label=="healthy-pair-exclusions")|.pairNS),.maxBacklog,.lateTimers,.maxTimerLatenessNS,.chainStoppedAt]|@tsv' "$out/$id.admission.1.json" >> "$out/cells.tsv"
  echo "verified $id"
done < "$record/evidence.tsv"
[[ $(wc -l < "$out/cells.tsv") == 19 ]]
[[ $(cut -f1 "$record/evidence.tsv" | sort -u | wc -l) == 27 ]]
for table in cells timing; do
  if [[ -f "$record/$table.tsv" ]]; then cmp "$record/$table.tsv" "$out/$table.tsv"; fi
done
