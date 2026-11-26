#!/usr/bin/env bash
# Baseline SuspicionMult characterization on correlated-dense. The neutral
# variant renames a core helper; it exists to exercise patch identity, the
# path guard, the single reducer and the null control, not to test a candidate.
#
# Usage: run.sh <new-output-dir> [cell ...]
# Run from a checkout of this repository that contains this record. With no
# cells named, the whole grid runs. A cell already listed in cells.tsv must
# reproduce its row exactly; the rows produced are written to <out>/cells.tsv.
#
# Headline columns, named before recording: otDead and runningDead are new
# dead transitions in the healthy-pair and running-target populations;
# suspicions and suspicionsDead count suspicion episodes and those ending
# dead; sentBytesPerNodeSecond is total encoded traffic.
set -euo pipefail

cases=(correlated-dense)
mults=(1 2 4 8)
replicates=(1 2)

record=$(cd "$(dirname "$0")" && pwd)
die() { echo "run.sh: $*" >&2; exit 1; }
[ $# -ge 1 ] || die "usage: run.sh <new-output-dir> [cell ...]"
mkdir "$1" || die "output must be a new directory"
out=$(cd "$1" && pwd)
shift
want=" $* "
base=$(cat "$record/base")
# The pin stays the identity; Git gets what that commit is now.
commit=$(bash "$record/../revision.sh" "$base")
read -r toolchain platform <"$record/toolchain"
export GOTOOLCHAIN=$toolchain
[ "$(go env GOOS)/$(go env GOARCH)" = "$platform" ] || die "replay is pinned to $platform"
git merge-base --is-ancestor "$commit" HEAD || die "base $base is not an ancestor of HEAD"

# Candidates change the subject, never the instrument.
variants=()
for patch in "$record"/*.patch; do
	[ -e "$patch" ] || continue
	outside=$(git apply --numstat "$patch" | awk '$3 !~ /^(core|wire)\//')
	[ -z "$outside" ] || die "$(basename "$patch") touches files outside core/ and wire/: $outside"
	# Numstat reports only the destination of a rename or copy.
	outside=$(awk '/^(rename|copy) from / { sub(/^(rename|copy) from /, ""); if ($0 !~ /^(core|wire)\//) print }' "$patch")
	[ -z "$outside" ] || die "$(basename "$patch") has sources outside core/ and wire/: $outside"
	variants+=("$(basename "$patch" .patch)")
done

declare -A identity=([base]=$base)
mkdir "$out/src" "$out/bin" "$out/cells"
for v in base "${variants[@]}"; do
	mkdir "$out/src/$v"
	git archive "$commit" | tar -x -C "$out/src/$v"
	if [ "$v" != base ]; then
		patch="$record/$v.patch"
		# The ceiling stops git apply from finding an enclosing repository.
		GIT_CEILING_DIRECTORIES="$out/src" git -C "$out/src/$v" apply "$patch"
		identity[$v]="$base+$v:$(sha256sum "$patch" | cut -c1-12)"
	fi
	(cd "$out/src/$v" && go build -buildvcs=false -o "$out/bin/$v" ./cmd/lightpool-experiment)
done

printf 'cell\tvariant\tcase\tsuspicionMult\treplicate\tidentity\tstatus\ttraceSHA256\tsummarySHA256\tnull\totDead\trunningDead\tsuspicions\tsuspicionsDead\tsentBytesPerNodeSecond\n' >"$out/cells.tsv"
failed=0
for v in base "${variants[@]}"; do
	for c in "${cases[@]}"; do
		for m in "${mults[@]}"; do
			for r in "${replicates[@]}"; do
				cell="$v-$c-sm$m-r$r"
				[ "$want" = "  " ] || [[ $want == *" $cell "* ]] || continue
				dir="$out/cells/$cell"
				printf '{"SuspicionMult": %d}\n' "$m" >"$dir.protocol.json"
				"$out/bin/$v" -case "$c" -replicate "$r" -revision "${identity[$v]}" -protocol "$dir.protocol.json" -out "$dir" 2>/dev/null || true
				# One reducer: every cell is measured by the baseline binary.
				"$out/bin/base" -reduce "$dir" -out "$dir.summary.json" || true
				cmp -s "$dir/summary.json" "$dir.summary.json" || { echo "run.sh: $cell: recorded and baseline-reduced summaries differ" >&2; failed=1; }
				null=-
				twin="$out/cells/base-$c-sm$m-r$r"
				if [ "$v" != base ] && [ -e "$twin/trace.ndjson" ]; then
					# The header carries source identity, so whole files must differ.
					if cmp -s <(tail -n +2 "$twin/trace.ndjson") <(tail -n +2 "$dir/trace.ndjson") && ! cmp -s "$twin/trace.ndjson" "$dir/trace.ndjson"; then
						null=same
					else
						null=differs
						echo "run.sh: $cell: null control failed against $(basename "$twin")" >&2
						failed=1
					fi
				fi
				row=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t' "$cell" "$v" "$c" "$m" "$r" "${identity[$v]}" \
					"$(jq -r .status "$dir/status.json")" \
					"$(sha256sum <"$dir/trace.ndjson" | cut -d' ' -f1)" \
					"$(sha256sum <"$dir.summary.json" | cut -d' ' -f1)" "$null")
				row+=$(jq -r '[
					(.populations[] | select(.label == "healthy-pair-exclusions") | .transitions),
					(.populations[] | select(.label == "running-target-exclusions") | .transitions),
					(.mechanisms | length),
					([.mechanisms[] | select(.outcome == "dead")] | length),
					.traffic.sentBytesPerNodeSecond
				] | map(tostring) | join("\t")' "$dir.summary.json")
				echo "$row" >>"$out/cells.tsv"
				if [ -e "$record/cells.tsv" ] && recorded=$(awk -F"\t" -v c="$cell" '$1 == c' "$record/cells.tsv") && [ -n "$recorded" ]; then
					[ "$recorded" = "$row" ] || { echo "run.sh: $cell does not reproduce its recorded row" >&2; failed=1; }
				fi
				echo "$row" >&2
			done
		done
	done
done
exit "$failed"
