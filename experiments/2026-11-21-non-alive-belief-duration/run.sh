#!/usr/bin/env bash
# Offline only. Restore the frozen originals; never run a protocol process.
set -euo pipefail
export LC_ALL=C
shopt -s nullglob dotglob
[[ $# == 1 ]] || { echo 'usage: run.sh NEW_DIRECTORY' >&2; exit 2; }
record=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$record" rev-parse --show-toplevel)
(cd "$record" && sha256sum -c inputs.sha256)
mkdir -- "$1"
out=$(cd "$1" && pwd)
work="$out/work"
# Diagnostics only: failures and interruptions must never trigger cleanup.
retained() {
  result=$?
  if [[ $result != 0 ]]; then
    echo "run.sh: failed; working files retained under $work; diagnostics under $out" >&2
  fi
}
trap retained EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
reducer=$(<"$record/reducer-base")
evidence=$(<"$record/evidence-base")
export GOTOOLCHAIN GOTMPDIR
GOTOOLCHAIN=$(<"$record/toolchain")
GOTMPDIR="$work/tmp"
mkdir "$work" "$work/src" "$work/source" "$GOTMPDIR" "$out/summaries" "$out/logs"
# Only Go source is needed to build this standard-library-only reducer.
git -C "$repo" archive "$reducer" -- '*.go' go.mod | tar -x -C "$work/src"
(cd "$work/src" && go build -trimpath -buildvcs=false -o "$work/reducer" ./cmd/lightpool-experiment)
printf 'cell\tcase\treplicate\tproducer\treducer\tspecSHA256\tscenarioSHA256\ttraceSHA256\tsummarySHA256\toriginalStatus\toriginalExit\tstatus\texit\treason\tpopulation\tenrolledPairs\tnominalPairNS\tobservedPairNS\taliveNS\tsuspectNS\tdeadNS\tleftNS\tabsentNS\tnotAliveNS\tsuspectDeadNS\tnotAliveFraction\tstarts\tcarryIns\tcompleted\tcensored\tp50NS\tp95NS\tmaxNS\n' > "$out/cells.tsv"

check_inputs() {
  local files=("$restored"/*) file
  [[ -d $restored && ! -L $restored && ${#files[@]} == "$payload_count" ]] || return 1
  for file in "${files[@]}"; do
    [[ -f $file && ! -L $file ]] || return 1
  done
  (cd "$restored" && sha256sum -c "$log.inputs.sha256")
}

unmeasured() {
  jq -nr --argjson e "$entry" --arg reducer "$reducer" --arg exit "$exit_code" --arg reason "$1" '[$e.id,$e.case,$e.replicate,$e.producer,$reducer,$e.rawHashes["spec.json"],$e.rawHashes["scenario.json"],$e.rawHashes["trace.ndjson"],"null",$e.originalStatus,$e.originalExit,"unmeasured",$exit,$reason,"none"] | @tsv' >> "$out/cells.tsv"
}

measure() {
  # Each attempt is restored separately, using this checkout's safety helper.
  git -C "$repo" archive "$evidence" -- "experiments/$source_record/$directory" | tar -x -C "$work/source" || return 1
  bash "$repo/experiments/evidence.sh" unpack "$work/source/experiments/$source_record/$directory" "$restored" > "$log.restore.stdout" 2> "$log.restore.stderr" || return 1
  check_inputs > "$log.before" 2>&1 || return 1
  jq -e --arg status "$original_status" '.status == $status' "$restored/status.json" > /dev/null || return 1
  exit_code=0
  "$work/reducer" -reduce "$restored" -out "$out/$id.json" > "$log.stdout" 2> "$log.stderr" || exit_code=$?
  printf '%s\n' "$exit_code" > "$log.exit" || return 1
  [[ $exit_code == "$original_exit" ]] || return 1
  summary_hash=$(sha256sum "$out/$id.json" | cut -d' ' -f1) || return 1
  # Preserve every existing field; only the format and new family may differ.
  jq -S 'del(.nonAlive) | .format="lightpool-pilot/v1"' "$out/$id.json" > "$work/compatible.json" || return 1
  jq -S . "$restored/summary.json" > "$work/original.json" || return 1
  cmp "$work/compatible.json" "$work/original.json" > "$log.compatibility" 2>&1 || return 1
  jq -r --arg reducer "$reducer" --arg id "$id" --arg hash "$summary_hash" --arg original_status "$original_status" --arg original_exit "$original_exit" --arg exit "$exit_code" '
    . as $s | .nonAlive.populations[] |
    [$id,$s.spec.case,$s.spec.replicate,$s.spec.code,$reducer,$s.specSHA256,$s.scenarioSHA256,$s.traceSHA256,$hash,$original_status,$original_exit,$s.status,$exit,$s.reason,.label,.enrolledPairs,.nominalPairNS,.observedPairNS,.aliveNS,.suspectNS,.deadNS,.leftNS,.absentNS,.notAliveNS,.suspectDeadNS,.notAliveFraction,.starts,.carryIns,.completed,(.censored|tojson),.durations.p50NS,.durations.p95NS,.durations.maxNS] | map(if . == null then "null" else . end) | @tsv
  ' "$out/$id.json" > "$work/row.tsv" || return 1
  gzip -n -6 < "$out/$id.json" > "$out/summaries/$id.json.gz" || return 1
  cmp "$record/summaries/$id.json.gz" "$out/summaries/$id.json.gz" >> "$out/work.log" 2>&1 || return 1
  check_inputs > "$log.after" 2>&1 || return 1
  cat "$work/row.tsv" >> "$out/cells.tsv" || return 1
}

failed=0
count=0
# Materialize the inventory so a parsing failure cannot look like an empty loop.
jq -c '.attempts[]' "$record/inputs.json" > "$work/entries.jsonl"
while IFS= read -r entry; do
  id=$(jq -r .id <<< "$entry")
  source_record=$(jq -r .record <<< "$entry")
  directory=$(jq -r .directory <<< "$entry")
  original_status=$(jq -r .originalStatus <<< "$entry")
  original_exit=$(jq -r .originalExit <<< "$entry")
  count=$((count+1))
  exit_code=1
  if [[ $failed != 0 ]]; then
    exit_code=null
    unmeasured "stopped-after-$failed_id"
    continue
  fi
  log="$out/logs/$id"
  restored="$work/restored"
  jq -r '.rawHashes | to_entries[] | "\(.value)  \(.key)"' <<< "$entry" > "$log.inputs.sha256"
  payload_count=$(jq '.rawHashes | length' <<< "$entry")
  success=0
  if measure; then success=1; fi
  if [[ $success == 1 ]]; then
    # Exact set/hash validation above authorizes deletion of this owned copy only.
    rm -r -- "$restored" "$work/source"
    rm -- "$out/$id.json" "$work/compatible.json" "$work/original.json" "$work/row.tsv"
    mkdir "$work/source"
    echo "remeasured $id (exit $exit_code)"
  else
    failed=1
    failed_id=$id
    unmeasured 'restoration-or-reduction-failed'
    echo "verification failed for $id; later attempts remain unmeasured" | tee -a "$out/work.log" >&2
  fi
done < "$work/entries.jsonl"
[[ $count == 24 ]] || failed=1
(cd "$out" && { printf '%s\n' cells.tsv; find summaries logs -type f | sort; } | xargs sha256sum > SHA256SUMS)
if [[ -f "$record/SHA256SUMS" && $failed == 0 ]]; then
  if ! cmp "$record/SHA256SUMS" "$out/SHA256SUMS"; then failed=1; fi
  if ! (cd "$out" && sha256sum -c "$record/SHA256SUMS") > "$out/reproduction.log"; then failed=1; fi
fi
if [[ $failed != 0 ]]; then echo 'fixed inventory finished with failures or reproduction differences' >&2; exit 1; fi
rm -r -- "$work"
echo 'verified all 24 original attempts; offline summaries and old fields agree'
