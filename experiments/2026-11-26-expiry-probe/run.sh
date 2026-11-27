#!/usr/bin/env bash
# Pre-expiry direct probe against baseline and three timeout retunes.
# The arms, cells, metrics and rejection rules are declared in README.md.
#
# Usage: run.sh [--retain] NEW_DIRECTORY [CELL ...]
# Run from a checkout containing this record. With no cells named, the whole
# grid runs in its declared order. A cell already listed in cells.tsv must
# reproduce its row exactly; rows produced are written to NEW_DIRECTORY/cells.tsv.
#
# --retain is the original campaign: each attempt is packed into evidence/,
# inventoried in evidence.tsv and its raw directory removed. Once evidence.tsv
# exists, --retain continues only with named cells never attempted before. It stops
# launching, and lists what it did not launch in unlaunched.tsv, once packed
# evidence passes 2 GiB or campaign time passes 6 hours. No retries.
set -euo pipefail

retain=0
if [ "${1-}" = --retain ]; then
	retain=1
	shift
fi
record=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$record/../.." && pwd)
die() { echo "run.sh: $*" >&2; exit 1; }
[ $# -ge 1 ] || die "usage: run.sh [--retain] NEW_DIRECTORY [CELL ...]"
mkdir "$1" || die "output must be a new directory"
out=$(cd "$1" && pwd)
shift
want=" $* "
base=$(cat "$record/base")
commit=$(bash "$record/../revision.sh" "$base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ] || die "replay is pinned to $platform"
git -C "$repo" merge-base --is-ancestor "$commit" HEAD || die "base $base is not an ancestor of HEAD"

# Candidates change the subject, never the instrument.
outside=$(git -C "$repo" apply --numstat "$record/probe.patch" | awk '$3 !~ /^(core|wire)\//')
[ -z "$outside" ] || die "probe.patch touches files outside core/ and wire/: $outside"
outside=$(awk '/^(rename|copy) from / { sub(/^(rename|copy) from /, ""); if ($0 !~ /^(core|wire)\//) print }' "$record/probe.patch")
[ -z "$outside" ] || die "probe.patch has sources outside core/ and wire/: $outside"

mkdir "$out/src" "$out/bin" "$out/cells"
for v in base probe; do
	mkdir "$out/src/$v"
	git -C "$repo" archive "$commit" | tar -x -C "$out/src/$v"
	if [ "$v" = probe ]; then
		# The ceiling stops git apply from finding an enclosing repository.
		GIT_CEILING_DIRECTORIES="$out/src" git -C "$out/src/$v" apply "$record/probe.patch"
	fi
	(cd "$out/src/$v" && go build -buildvcs=false -o "$out/bin/$v" ./cmd/lightpool-experiment)
done
probe_identity="$base+probe:$(sha256sum "$record/probe.patch" | cut -c1-12)"

# The declared grid, in launch order: regression and null controls, crash
# detection, the healthy aged control, then the aged replicates with every
# arm of one replicate launched together.
cells=()
for c in healthy loss correlated partial-partition interval threshold correlated-dense; do
	for r in 1 2 3; do cells+=("base $c $r" "probe $c $r"); done
done
for c in crash small-double-crash; do
	last=106
	[ "$c" = crash ] || last=110
	for r in $(seq 101 "$last"); do
		for a in base probe smax8 smax12 sm5; do cells+=("$a $c $r"); done
	done
done
for r in 101 102 103; do cells+=("base correlated-dense-64-aged-healthy $r" "probe correlated-dense-64-aged-healthy $r"); done
for r in $(seq 101 120); do
	cells+=("base correlated-dense-64-aged $r" "probe correlated-dense-64-aged $r" "smax12 correlated-dense-64-aged $r")
	[ "$r" -gt 110 ] || cells+=("smax8 correlated-dense-64-aged $r" "sm5 correlated-dense-64-aged $r")
done
# Byte-identical after the header when the candidate never arms a probe.
null_cases=" healthy loss correlated partial-partition "

header='cell\tarm\tcase\treplicate\tidentity\texitCode\tstatus\tspecSHA256\tscenarioSHA256\ttraceSHA256\tsummarySHA256\tnull'
header+='\tworkingPathExpiryDeaths\texpiryDeaths\trunningDeadDeclarations\trunningSuspectNS\trunningDeadNS'
header+='\tcrashSubjects\tcrashMaxNS\tcrashP50NS\tcrashCompleted\tcrashEligible\tcrashFullStatus'
header+='\tdatagramCount\tdatagramBytes\tstreamBytes\n'
printf "$header" >"$out/cells.tsv"
if ((retain)); then
	if [ -e "$record/evidence.tsv" ]; then
		# Continuing an interrupted campaign launches only named cells that
		# have never been attempted; an attempted cell is never retried.
		[ "$want" != "  " ] || die "evidence.tsv exists; name the never-attempted cells to continue"
		for cell in $want; do
			! awk -F '\t' -v c="$cell" '$1 == c { found = 1 } END { exit !found }' "$record/evidence.tsv" || die "$cell was already attempted"
		done
	else
		mkdir -p "$record/evidence"
		printf 'attempt\tdirectory\tprovenance\texitCode\tstatus\trawBytes\tstoredBytes\n' >"$record/evidence.tsv"
	fi
	printf 'cell\treason\n' >"$out/unlaunched.tsv"
fi
started=$(date +%s)
stored_total=0
if ((retain)); then
	stored_total=$(awk -F '\t' 'NR > 1 && $7 != "-" { n += $7 } END { printf "%.0f", n }' "$record/evidence.tsv")
fi
failed=0
for spec in "${cells[@]}"; do
	read -r arm name replicate <<<"$spec"
	cell="$arm-$name-r$replicate"
	[ "$want" = "  " ] || [[ $want == *" $cell "* ]] || continue
	if ((retain)); then
		reason=
		((stored_total < 2 * 1024 * 1024 * 1024)) || reason=evidence-cap
		(($(date +%s) - started < 6 * 3600)) || reason=time-cap
		if [ -n "$reason" ]; then
			printf '%s\t%s\n' "$cell" "$reason" >>"$out/unlaunched.tsv"
			continue
		fi
	fi
	dir="$out/cells/$cell"
	bin=base identity=$base flags=()
	case $arm in
	probe) bin=probe identity=$probe_identity ;;
	smax8 | smax12 | sm5) flags=(-protocol "$record/overlays/$arm.json") ;;
	esac
	exit_code=0
	"$out/bin/$bin" -case "$name" -replicate "$replicate" -revision "$identity" "${flags[@]}" -out "$dir" >"$out/$cell.stdout" 2>"$out/$cell.stderr" || exit_code=$?
	# Keep diagnostics even if the runner fails before creating its directory.
	mkdir -p "$dir"
	mv "$out/$cell.stdout" "$dir/command.stdout"
	mv "$out/$cell.stderr" "$dir/command.stderr"
	status=recording-failure
	[ ! -f "$dir/status.json" ] || status=$(jq -r .status "$dir/status.json")
	row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$cell" "$arm" "$name" "$replicate" "$identity" "$exit_code" "$status")
	for file in spec.json scenario.json trace.ndjson; do
		digest=-
		[ ! -f "$dir/$file" ] || digest=$(sha256sum <"$dir/$file" | cut -d' ' -f1)
		row+=$'\t'$digest
	done
	if [ -f "$dir/summary.json" ]; then
		# One reducer: every cell is measured by the baseline binary.
		reduce_exit=0
		"$out/bin/base" -reduce "$dir" -out "$out/$cell.summary.json" >/dev/null 2>"$out/$cell.reduce.stderr" || reduce_exit=$?
		if ! cmp -s "$dir/summary.json" "$out/$cell.summary.json" || [ "$reduce_exit" -ne "$exit_code" ]; then
			echo "run.sh: $cell: recorded and baseline-reduced summaries differ" >&2
			failed=1
		fi
		row+=$'\t'$(sha256sum <"$dir/summary.json" | cut -d' ' -f1)
	else
		row+=$'\t-'
	fi
	null=-
	twin="$out/cells/base-$name-r$replicate"
	if [ "$arm" = probe ] && [[ $null_cases == *" $name "* ]]; then
		# The header carries source identity, so whole files must differ.
		if [ -f "$twin/trace.ndjson" ] && cmp -s <(tail -n +2 "$twin/trace.ndjson") <(tail -n +2 "$dir/trace.ndjson") && ! cmp -s "$twin/trace.ndjson" "$dir/trace.ndjson"; then
			null=same
		else
			null=differs
			echo "run.sh: $cell: null control failed against base" >&2
			failed=1
		fi
	fi
	row+=$'\t'$null
	if [ -f "$dir/summary.json" ]; then
		# W excludes paused endpoints and any configured fault joining the pair
		# at declaration time, in either direction.
		row+=$'\t'$(jq -r '
			def running: .targetRunning and (.targetStopped | not) and (.targetLeft | not);
			def rte: .nonAlive.populations[] | select(.label == "running-target-exclusions");
			def crashes(f): if (.crashes | length) == 0 then "-" else [.crashes[] | f | tostring] | join(",") end;
			[
				([.episodes[] | select(.cause == "local-suspicion-expiry" and running and (.targetPaused | not) and (.observerPaused | not) and (.incidentConfiguredFaults | length) == 0)] | length),
				([.episodes[] | select(.cause == "local-suspicion-expiry")] | length),
				([.episodes[] | select(running and (.targetPaused | not))] | length),
				(rte | .suspectNS), (rte | .deadNS),
				crashes(.subject), crashes(.latencies.maxNS // "-"), crashes(.latencies.p50NS // "-"),
				crashes(.latencies.completed), crashes(.latencies.eligible), crashes(.fullStatus),
				.trafficCost.measured.attempted.datagramCount, .trafficCost.measured.attempted.datagramBytes, .trafficCost.measured.attempted.streamBytes
			] | map(tostring) | join("\t")' "$dir/summary.json")
	else
		row+=$'\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-'
	fi
	printf '%s\n' "$row" >>"$out/cells.tsv"
	if [ -f "$record/cells.tsv" ] && ! ((retain)); then
		expected=$(awk -F '\t' -v c="$cell" '$1 == c' "$record/cells.tsv")
		[ "$expected" = "$row" ] || { echo "run.sh: $cell does not reproduce its recorded row" >&2; failed=1; }
	fi
	printf '%s\n' "$row" >&2
	if ((retain)); then
		bash "$record/../evidence.sh" pack "$dir" "$record/evidence/$cell"
		raw=$(find "$dir" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1} END {printf "%.0f", n}')
		stored=$(find "$record/evidence/$cell" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1} END {printf "%.0f", n}')
		printf '%s\tevidence/%s\toriginal\t%s\t%s\t%s\t%s\n' "$cell" "$cell" "$exit_code" "$status" "$raw" "$stored" >>"$record/evidence.tsv"
		stored_total=$((stored_total + stored))
		# Raw copies are released once packed; a baseline null-control trace
		# waits for its probe twin.
		if [ "$arm" = probe ] && [[ $null_cases == *" $name "* ]]; then
			rm -r -- "$twin"
		fi
		if [ "$arm" != base ] || [[ $null_cases != *" $name "* ]]; then
			rm -r -- "$dir"
		fi
	fi
done
exit "$failed"
