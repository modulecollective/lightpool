#!/usr/bin/env bash
# Six fixed baseline attempts, including controls and invalid outcomes.
# Usage: run.sh NEW_DIRECTORY (from any checkout containing this record).
# Host resource measurements and diagnostics are retained, never compared.
set -euo pipefail

record=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$record/../.." && pwd)
die() { echo "run.sh: $*" >&2; exit 1; }
[ "$#" -eq 1 ] || die "usage: run.sh NEW_DIRECTORY"
mkdir "$1" || die "output must be a new directory"
out=$(cd "$1" && pwd)
base=$(cat "$record/base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ] || die "replay is pinned to $platform"
git -C "$repo" merge-base --is-ancestor "$base" HEAD || die "base must be an ancestor of HEAD"
mkdir "$out/src" "$out/cells"
git -C "$repo" archive "$base" | tar -x -C "$out/src"
(cd "$out/src" && go build -buildvcs=false -o "$out/lightpool-experiment" ./cmd/lightpool-experiment)

printf 'cell\tcase\treplicate\tidentity\texitCode\tstatus\tspecSHA256\tscenarioSHA256\ttraceSHA256\tsummarySHA256\thealthyTransitions\thealthyPairs\thealthyPairNS\trunningTransitions\trunningPairNS\trecoveryCompleted\trecoveryEligible\n' >"$out/cells.tsv"
failed=0
for name in partial-partition partial-partition-healthy; do
	for replicate in 1 2 3; do
		cell="$name-r$replicate"
		dir="$out/cells/$cell"
		exit_code=0
		"$out/lightpool-experiment" -case "$name" -replicate "$replicate" -revision "$base" -out "$dir" >"$out/$cell.stdout" 2>"$out/$cell.stderr" || exit_code=$?
		# Keep diagnostics even if the runner fails before creating its directory.
		mkdir -p "$dir"
		mv "$out/$cell.stdout" "$dir/command.stdout"
		mv "$out/$cell.stderr" "$dir/command.stderr"
		status=recording-failure
		if [ -f "$dir/status.json" ]; then status=$(jq -r .status "$dir/status.json"); fi
		row=$(printf '%s\t%s\t%s\t%s\t%s\t%s' "$cell" "$name" "$replicate" "$base" "$exit_code" "$status")
		for file in spec.json scenario.json trace.ndjson summary.json; do
			digest=-
			if [ -f "$dir/$file" ]; then
				digest=$(sha256sum <"$dir/$file")
				digest=${digest%% *}
			fi
			row+=$'\t'"$digest"
		done
		if [ -f "$dir/summary.json" ]; then
			row+=$'\t'$(jq -r '[
				(.populations[] | select(.label == "healthy-pair-exclusions") | .transitions, .pairs, .pairNS),
				(.populations[] | select(.label == "running-target-exclusions") | .transitions, .pairNS),
				.recoveryDurations.completed, .recoveryDurations.eligible
			] | join("\t")' "$dir/summary.json")
			reduce_exit=0
			"$out/lightpool-experiment" -reduce "$dir" -out "$out/$cell.reduced.json" >"$out/$cell.reduce.stdout" 2>"$out/$cell.reduce.stderr" || reduce_exit=$?
			if ! cmp -s "$dir/summary.json" "$out/$cell.reduced.json" || [ "$reduce_exit" -ne "$exit_code" ]; then
				echo "run.sh: $cell offline replay differs" >&2
				failed=1
			fi
		else
			row+=$'\t-\t-\t-\t-\t-\t-\t-'
		fi
		printf '%s\n' "$row" >>"$out/cells.tsv"
		if [ -f "$record/cells.tsv" ]; then
			expected=$(awk -F '\t' -v c="$cell" '$1 == c' "$record/cells.tsv")
			if [ "$expected" != "$row" ]; then
				echo "run.sh: $cell does not reproduce its recorded row" >&2
				failed=1
			fi
		fi
		printf '%s\n' "$row" >&2
	done
done
exit "$failed"
