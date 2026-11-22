#!/usr/bin/env bash
# Offline only. Restore the frozen originals; never run a protocol process.
set -euo pipefail
export LC_ALL=C
[[ $# == 1 ]] || { echo 'usage: run.sh NEW_DIRECTORY' >&2; exit 2; }
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
(cd "$record" && sha256sum -c inputs.sha256)
mkdir -- "$1"
out=$(cd "$1" && pwd)
reducer=$(<"$record/reducer-base")
evidence=$(<"$record/evidence-base")
export GOTOOLCHAIN
GOTOOLCHAIN=$(<"$record/toolchain")
mkdir "$out/src" "$out/source" "$out/summaries" "$out/logs"
# Only Go source is needed to build this standard-library-only reducer.
git -C "$repo" archive "$reducer" -- '*.go' go.mod | tar -x -C "$out/src"
(cd "$out/src" && go build -trimpath -buildvcs=false -o "$out/reducer" ./cmd/lightpool-experiment)
printf 'cell\tcase\treplicate\tproducer\treducer\tspecSHA256\tscenarioSHA256\ttraceSHA256\tsummarySHA256\toriginalStatus\toriginalExit\tstatus\texit\treason\tpopulation\tenrolledPairs\tnominalPairNS\tobservedPairNS\taliveNS\tsuspectNS\tdeadNS\tleftNS\tabsentNS\tnotAliveNS\tsuspectDeadNS\tnotAliveFraction\tstarts\tcarryIns\tcompleted\tcensored\tp50NS\tp95NS\tmaxNS\n' > "$out/cells.tsv"
failed=0
count=0
while IFS= read -r entry; do
  id=$(jq -r .id <<< "$entry")
  source_record=$(jq -r .record <<< "$entry")
  directory=$(jq -r .directory <<< "$entry")
  original_status=$(jq -r .originalStatus <<< "$entry")
  original_exit=$(jq -r .originalExit <<< "$entry")
  count=$((count+1))
  log="$out/logs/$id"
  restored="$out/restored"
  valid=1
  # Each attempt is restored separately; only this one raw trace is live.
  if ! git -C "$repo" archive "$evidence" -- "experiments/$source_record/$directory" experiments/evidence.sh | tar -x -C "$out/source"; then
    valid=0
  fi
  if [[ $valid == 1 ]]; then
    if ! bash "$out/source/experiments/evidence.sh" unpack "$out/source/experiments/$source_record/$directory" "$restored" > "$log.restore.stdout" 2> "$log.restore.stderr"; then valid=0; fi
  fi
  jq -r '.rawHashes | to_entries[] | "\(.value)  \(.key)"' <<< "$entry" > "$log.inputs.sha256"
  if [[ $valid == 1 ]]; then
    if ! (cd "$restored" && sha256sum -c "$log.inputs.sha256") > "$log.before" 2>&1; then valid=0; fi
    if ! jq -e --arg status "$original_status" '.status == $status' "$restored/status.json" > /dev/null; then valid=0; fi
  fi
  exit_code=1
  if [[ $valid == 1 ]]; then
    exit_code=0
    "$out/reducer" -reduce "$restored" -out "$out/$id.json" > "$log.stdout" 2> "$log.stderr" || exit_code=$?
  else
    echo 'original evidence restoration/validation failed' > "$log.stderr"
    : > "$log.stdout"
  fi
  printf '%s\n' "$exit_code" > "$log.exit"
  if [[ -f "$out/$id.json" ]]; then
    summary_hash=$(sha256sum "$out/$id.json" | cut -d' ' -f1)
    # Preserve every existing field; only the format and new family may differ.
    if ! cmp <(jq -S 'del(.nonAlive) | .format="lightpool-pilot/v1"' "$out/$id.json") <(jq -S . "$restored/summary.json") > "$log.compatibility" 2>&1; then failed=1; fi
    jq -r --arg reducer "$reducer" --arg id "$id" --arg hash "$summary_hash" --arg original_status "$original_status" --arg original_exit "$original_exit" --arg exit "$exit_code" '
      . as $s | .nonAlive.populations[] |
      [$id,$s.spec.case,$s.spec.replicate,$s.spec.code,$reducer,$s.specSHA256,$s.scenarioSHA256,$s.traceSHA256,$hash,$original_status,$original_exit,$s.status,$exit,$s.reason,.label,.enrolledPairs,.nominalPairNS,.observedPairNS,.aliveNS,.suspectNS,.deadNS,.leftNS,.absentNS,.notAliveNS,.suspectDeadNS,.notAliveFraction,.starts,.carryIns,.completed,(.censored|tojson),.durations.p50NS,.durations.p95NS,.durations.maxNS] | map(if . == null then "null" else . end) | @tsv
    ' "$out/$id.json" >> "$out/cells.tsv"
    gzip -n -6 < "$out/$id.json" > "$out/summaries/$id.json.gz"
    # The plain JSON is temporary; the retained output is lossless gzip.
    unlink "$out/$id.json"
  else
    jq -nr --argjson e "$entry" --arg reducer "$reducer" --arg exit "$exit_code" '[$e.id,$e.case,$e.replicate,$e.producer,$reducer,$e.rawHashes["spec.json"],$e.rawHashes["scenario.json"],$e.rawHashes["trace.ndjson"],"null",$e.originalStatus,$e.originalExit,"unmeasured",$exit,"restoration-or-reduction-failed","none"] | @tsv' >> "$out/cells.tsv"
    failed=1
  fi
  if [[ $exit_code != "$original_exit" ]]; then failed=1; fi
  if [[ -d $restored ]]; then
    if ! (cd "$restored" && sha256sum -c "$log.inputs.sha256") > "$log.after" 2>&1; then failed=1; fi
    # evidence.sh guarantees a flat regular payload set under our new directory.
    find "$restored" -type f -delete
    rmdir "$restored"
  fi
  echo "remeasured $id (exit $exit_code)"
done < <(jq -c '.attempts[]' "$record/inputs.json")
[[ $count == 24 ]] || failed=1
(cd "$out" && { printf '%s\n' cells.tsv; find summaries logs -type f | sort; } | xargs sha256sum > SHA256SUMS)
if [[ -f "$record/SHA256SUMS" ]]; then
  if ! cmp "$record/SHA256SUMS" "$out/SHA256SUMS"; then failed=1; fi
  if ! (cd "$out" && sha256sum -c "$record/SHA256SUMS") > "$out/reproduction.log"; then failed=1; fi
fi
if [[ $failed != 0 ]]; then echo 'fixed inventory finished with failures or reproduction differences' >&2; exit 1; fi
echo 'verified all 24 original attempts; offline summaries and old fields agree'
