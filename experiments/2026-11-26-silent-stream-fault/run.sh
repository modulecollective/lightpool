#!/usr/bin/env bash
# Refusing against silent A→T stream faults on the correlated-dense cases.
# The grid, cell order and decision rule are declared in README.md.
#
# Usage: run.sh [--retain] NEW_DIRECTORY [CELL ...]
# Run from a checkout containing this record. With no cells named, the whole
# grid runs in its declared order. Rows go to NEW_DIRECTORY/cells.tsv; a cell
# already listed in this record's cells.tsv must reproduce its row exactly.
#
# --retain is the original campaign: each attempt is packed into evidence/,
# inventoried in evidence.tsv and its raw directory removed. It stops
# launching once packed evidence passes 512 MiB, or after the fixed control
# block if any control row differs from the 2026-11-20 record, and lists
# what it did not launch in unlaunched.tsv. No retries.
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
reference="$record/../2026-11-20-ordinary-witness-current/virtual-cells.tsv"
[ -f "$reference" ] || die "missing control reference $reference"
if ((retain)) && [ -e "$record/evidence.tsv" ]; then
	die "evidence.tsv exists; the original campaign has run"
fi

mkdir "$out/src" "$out/cells"
git -C "$repo" archive "$commit" | tar -x -C "$out/src"
(cd "$out/src" && go build -trimpath -buildvcs=false -o "$out/producer" ./cmd/lightpool-experiment)
producer=$(sha256sum <"$out/producer" | cut -d' ' -f1)

# Launch order: fixed control, held-out control and silent interleaved by
# index, then fixed silent.
fixed=(correlated-dense correlated-dense-latency-100us correlated-dense-latency-10ms correlated-dense-service-100us correlated-dense-service-1ms)
cells=()
for c in "${fixed[@]:0:1}" correlated-dense-healthy "${fixed[@]:1}"; do
	for r in 1 2 3; do cells+=("control $c $r"); done
done
for r in $(seq 201 230); do cells+=("control correlated-dense $r" "silent correlated-dense-silent $r"); done
for c in "${fixed[@]}"; do
	for r in 1 2 3; do cells+=("silent $c-silent $r"); done
done
control_cells=18

header='cell\tarm\tcase\treplicate\texitCode\tstatus\tspecSHA256\tscenarioSHA256\ttraceSHA256\tsummarySHA256\tadmissionExit\tadmissionSHA256'
header+='\tadmissionStatus\tfailedProbes\tlocalSuspicions\teligibleEpisodes\twitnesses\tharmful\thealthyDead\tpairExposureNS\tmaxBacklog\tlateTimers\tmaxTimerLatenessNS\tstop\tcontrolGate\n'
printf "$header" >"$out/cells.tsv"
if ((retain)); then
	mkdir -p "$record/evidence"
	printf 'attempt\tdirectory\tprovenance\texitCode\tstatus\trawBytes\tstoredBytes\n' >"$record/evidence.tsv"
	printf 'cell\treason\n' >"$out/unlaunched.tsv"
fi
size() { find "$1" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1} END {printf "%.0f", n}'; }
stored_total=0 failed=0 gate_failed=0 launched=0
for spec in "${cells[@]}"; do
	read -r arm name replicate <<<"$spec"
	cell="$name-r$replicate"
	launched=$((launched + 1))
	[ "$want" = "  " ] || [[ $want == *" $cell "* ]] || continue
	if ((retain)); then
		reason=
		((stored_total <= 512 * 1024 * 1024)) || reason=evidence-cap
		((!gate_failed)) || reason=control-gate
		if [ -n "$reason" ]; then
			printf '%s\t%s\n' "$cell" "$reason" >>"$out/unlaunched.tsv"
			continue
		fi
	fi
	dir="$out/cells/$cell"
	exit_code=0
	"$out/producer" -case "$name" -replicate "$replicate" -revision "$base" -out "$dir" >"$out/$cell.stdout" 2>"$out/$cell.stderr" || exit_code=$?
	# Keep diagnostics even if the runner fails before creating its directory.
	mkdir -p "$dir"
	mv "$out/$cell.stdout" "$dir/command.stdout"
	mv "$out/$cell.stderr" "$dir/command.stderr"
	printf '%s\n' "$producer" >"$dir/binary.sha256"
	status=recording-failure
	[ ! -f "$dir/status.json" ] || status=$(jq -r .status "$dir/status.json")
	row=$(printf '%s\t%s\t%s\t%s\t%s\t%s' "$cell" "$arm" "$name" "$replicate" "$exit_code" "$status")
	for file in spec.json scenario.json trace.ndjson summary.json; do
		digest=-
		[ ! -f "$dir/$file" ] || digest=$(sha256sum <"$dir/$file" | cut -d' ' -f1)
		row+=$'\t'$digest
	done
	if [ -f "$dir/summary.json" ]; then
		reduce_exit=0
		"$out/producer" -reduce "$dir" -out "$out/$cell.summary.json" >/dev/null 2>"$out/$cell.reduce.stderr" || reduce_exit=$?
		if ! cmp -s "$dir/summary.json" "$out/$cell.summary.json" || [ "$reduce_exit" -ne "$exit_code" ]; then
			echo "run.sh: $cell: recorded and reduced summaries differ" >&2
			failed=1
		fi
		admission_exit=0
		"$out/producer" -admission "$dir" -out "$out/$cell.admission.json" >/dev/null 2>"$dir/admission.stderr" || admission_exit=$?
	else
		admission_exit=-
	fi
	if [ -f "$out/$cell.admission.json" ]; then
		mv "$out/$cell.admission.json" "$dir/admission.json"
		row+=$'\t'$admission_exit$'\t'$(sha256sum <"$dir/admission.json" | cut -d' ' -f1)
		# The 2026-11-20 virtual-cells.tsv columns, from its verify.sh.
		classified=$(jq -r '[.spec.case,.spec.replicate,.status,.aProbeFailures,.aLocalSuspicions,.eligibleEpisodes,.accelerationWitnesses,.harmfulEpisodes,(.populations[] | select(.label=="healthy-pair-exclusions")|.transitions),(.populations[] | select(.label=="healthy-pair-exclusions")|.pairNS),.maxBacklog,.lateTimers,.maxTimerLatenessNS,.chainStoppedAt]|@tsv' "$dir/admission.json")
		row+=$'\t'$(cut -f3- <<<"$classified")
	else
		classified=-
		row+=$'\t'$admission_exit$'\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-'
	fi
	gate=-
	if [ "$arm" = control ] && ((replicate <= 3)); then
		if [ "$classified" = "$(awk -F '\t' -v c="$name" -v r="$replicate" '$1 == c && $2 == r' "$reference")" ]; then
			gate=same
		else
			gate=differs
			echo "run.sh: $cell: control does not reproduce the 2026-11-20 record" >&2
			failed=1
			gate_failed=1
		fi
	fi
	row+=$'\t'$gate
	printf '%s\n' "$row" >>"$out/cells.tsv"
	if [ -f "$record/cells.tsv" ] && ! ((retain)); then
		expected=$(awk -F '\t' -v c="$cell" '$1 == c' "$record/cells.tsv")
		[ "$expected" = "$row" ] || { echo "run.sh: $cell does not reproduce its recorded row" >&2; failed=1; }
	fi
	printf '%s\n' "$row" >&2
	if ((retain)); then
		bash "$record/../evidence.sh" pack "$dir" "$record/evidence/$cell"
		raw=$(size "$dir")
		stored=$(size "$record/evidence/$cell")
		printf '%s\tevidence/%s\toriginal\t%s\t%s\t%s\t%s\n' "$cell" "$cell" "$exit_code" "$status" "$raw" "$stored" >>"$record/evidence.tsv"
		stored_total=$((stored_total + stored))
		rm -r -- "$dir"
	fi
	if ((launched == control_cells)) && ((gate_failed)); then
		echo "run.sh: control gate failed; the silent arm is not read" >&2
	fi
done
exit "$failed"
