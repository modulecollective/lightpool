#!/usr/bin/env bash
# Keeps packed attempt directories in release assets instead of Git.
# Git holds each attempt's SHA256SUMS and the record's evidence-assets.tsv,
# which pins one archive per attempt by tag, size and SHA-256.
set -euo pipefail
export LC_ALL=C
shopt -s nullglob dotglob

repository=modulecollective/lightpool
# The last commit before any payload entered history. Every evidence tag
# points here: git clone fetches all tags, so a tag on a payload-carrying
# commit would pull that history into every clone.
target=1801ef926af5ee05d80617bb531f6dfcd24a71e1
# Test seam, not a setting: Go tests serve assets from a file:// directory.
base_url=${EVIDENCE_STORE_URL:-https://github.com/$repository/releases/download}
experiments=$(cd "$(dirname "$0")" && pwd -P)
header=$'attempt\ttag\tasset\tbytes\tsha256'

die() { echo "evidence-store.sh: $*" >&2; exit 1; }
usage() { die "usage: evidence-store.sh {publish RECORD BATCH | fetch RECORD [ATTEMPT...] | check}"; }

safe_name() { [[ $1 =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]]; }

record_dir() {
	safe_name "$1" && [[ -d $experiments/$1 ]] || die "no such record: $1"
	dir=$experiments/$1
}

has_payload() {
	local file
	for file in "$1"/*; do
		[[ ${file##*/} == SHA256SUMS ]] || return 0
	done
	return 1
}

# Reads a manifest into rows[attempt]="tag asset bytes sha256", refusing
# anything but the exact grammar publish writes.
declare -A rows=()
declare -a order=()
read_manifest() {
	rows=() order=()
	[[ -f $1 ]] || return 0
	local line first=1
	while IFS= read -r line || [[ -n $line ]]; do
		if ((first)); then
			[[ $line == "$header" ]] || die "$1: unexpected header"
			first=0
			continue
		fi
		[[ $line =~ ^([^$'\t']+)$'\t'(evidence-[^$'\t']+)$'\t'([^$'\t']+)$'\t'(0|[1-9][0-9]*)$'\t'([0-9a-f]{64})$ ]] || die "$1: invalid row: $line"
		local attempt=${BASH_REMATCH[1]} row="${BASH_REMATCH[2]} ${BASH_REMATCH[3]} ${BASH_REMATCH[4]} ${BASH_REMATCH[5]}" asset=${BASH_REMATCH[3]}
		safe_name "$attempt" && [[ $asset == "$attempt.tar.gz" ]] || die "$1: invalid attempt or asset: $line"
		[[ ! -v rows[$attempt] ]] || die "$1: duplicate attempt: $attempt"
		rows[$attempt]=$row
		order+=("$attempt")
	done <"$1"
	((!first)) || die "$1: empty manifest"
}

# Deterministic: the same attempt directory always yields the same bytes,
# so an interrupted publish can resume against an already-uploaded asset.
build_archive() {
	local names=() file
	for file in "$1"/*; do
		[[ -f $file && ! -L $file ]] || die "not a flat regular file: $file"
		names+=("${file##*/}")
	done
	tar --create --format=gnu --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
		--mode=a=r,u+w -C "$1" -- "${names[@]}" | gzip -n -6 >"$2"
}

# Prints "size sha256:<hex>" for one asset of a release, or nothing.
remote_asset() {
	gh api "repos/$repository/releases/$1/assets" --paginate \
		--jq ".[] | select(.name == \"$2\") | \"\(.size) \(.digest)\""
}

publish() {
	[[ $# -eq 2 && $2 =~ ^[1-9][0-9]*$ ]] || usage
	record_dir "$1"
	local record=$1 tag=evidence-$1-$2 manifest=$dir/evidence-assets.tsv
	read_manifest "$manifest"
	local pending=() attempt
	for attempt in "$dir"/evidence/*/; do
		attempt=${attempt%/}
		attempt=${attempt##*/}
		[[ -v rows[$attempt] ]] && continue
		has_payload "$dir/evidence/$attempt" && pending+=("$attempt")
	done
	[[ ${#pending[@]} -gt 0 ]] || die "$record: no unpublished attempt holds payloads"

	local id
	id=$(gh api "repos/$repository/releases" --paginate --jq ".[] | select(.tag_name == \"$tag\") | .id")
	if [[ -z $id ]]; then
		id=$(gh api "repos/$repository/releases" -X POST -f tag_name="$tag" -f target_commitish="$target" \
			-f name="Evidence: $record, batch $2" -F draft=true \
			-f body="Original attempt evidence for experiments/$record. Pinned by experiments/$record/evidence-assets.tsv; restore with experiments/evidence-store.sh fetch $record." \
			--jq .id)
	fi
	[[ $id =~ ^[0-9]+$ ]] || die "cannot resolve release $tag: $id"
	[[ -f $manifest ]] || printf '%s\n' "$header" >"$manifest"

	scratch=$(mktemp -d)
	trap 'rm -rf -- "$scratch"' EXIT
	for attempt in "${pending[@]}"; do
		local archive=$scratch/$attempt.tar.gz bytes sha remote
		build_archive "$dir/evidence/$attempt" "$archive"
		bytes=$(stat --format=%s -- "$archive")
		sha=$(sha256sum <"$archive")
		sha=${sha%% *}
		remote=$(remote_asset "$id" "$attempt.tar.gz")
		if [[ -z $remote ]]; then
			gh api "https://uploads.github.com/repos/$repository/releases/$id/assets?name=$attempt.tar.gz" \
				-X POST -H 'Content-Type: application/gzip' --input "$archive" >/dev/null
			remote=$(remote_asset "$id" "$attempt.tar.gz")
		fi
		# A row is written only for bytes the store has confirmed.
		[[ $remote == "$bytes sha256:$sha" ]] || die "$tag/$attempt.tar.gz: remote '$remote' differs from local '$bytes sha256:$sha'; nothing recorded"
		printf '%s\t%s\t%s\t%s\t%s\n' "$attempt" "$tag" "$attempt.tar.gz" "$bytes" "$sha" >>"$manifest"
		rm -- "$archive"
		echo "published $tag/$attempt.tar.gz ($bytes bytes)"
	done
	gh api "repos/$repository/releases/$id" -X PATCH -F draft=false >/dev/null
	local tagged
	tagged=$(gh api "repos/$repository/git/ref/tags/$tag" --jq .object.sha)
	[[ $tagged == "$target" ]] || die "$tag points at $tagged, not $target; move it before anything else is tagged"
}

fetch() {
	[[ $# -ge 1 ]] || usage
	record_dir "$1"
	local record=$1
	shift
	read_manifest "$dir/evidence-assets.tsv"
	[[ ${#order[@]} -gt 0 ]] || die "$record: no evidence-assets.tsv rows"
	local selected=("$@") attempt
	if [[ ${#selected[@]} -eq 0 ]]; then selected=("${order[@]}"); fi
	for attempt in "${selected[@]}"; do
		[[ -v rows[$attempt] ]] || die "$record: no manifest row for $attempt"
	done
	scratch=$(mktemp -d)
	trap 'rm -rf -- "$scratch"' EXIT
	for attempt in "${selected[@]}"; do
		local tag asset bytes sha destination=$dir/evidence/$attempt
		read -r tag asset bytes sha <<<"${rows[$attempt]}"
		[[ -f $destination/SHA256SUMS && ! -L $destination/SHA256SUMS ]] || die "$attempt: no committed SHA256SUMS"
		! has_payload "$destination" || die "$attempt: destination already holds payloads"
		local archive=$scratch/$asset extracted=$scratch/$attempt
		curl --fail --silent --show-error --location --proto '=https,file' -o "$archive" -- "$base_url/$tag/$asset" ||
			die "$attempt: download failed"
		[[ $(stat --format=%s -- "$archive") == "$bytes" ]] || die "$attempt: size differs from manifest"
		local got
		got=$(sha256sum <"$archive")
		[[ ${got%% *} == "$sha" ]] || die "$attempt: sha256 differs from manifest"
		local names=() name
		mapfile -t names < <(tar --list --gzip --file "$archive")
		for name in "${names[@]}"; do safe_name "$name" || die "$attempt: unsafe archive member: $name"; done
		mkdir -- "$extracted"
		tar --extract --gzip --no-same-owner --no-same-permissions --file "$archive" -C "$extracted"
		local files=("$extracted"/*)
		[[ ${#files[@]} -eq ${#names[@]} ]] || die "$attempt: archive member set is not flat and unique"
		for name in "${files[@]}"; do [[ -f $name && ! -L $name ]] || die "$attempt: not a regular file: ${name##*/}"; done
		cmp --silent -- "$extracted/SHA256SUMS" "$destination/SHA256SUMS" ||
			die "$attempt: archived SHA256SUMS differs from the committed one"
		rm -- "$extracted/SHA256SUMS" "$archive"
		mv -- "$extracted"/* "$destination"/
		rmdir -- "$extracted"
		echo "fetched $attempt"
	done
}

check() {
	[[ $# -eq 0 ]] || usage
	local tracked manifest record attempt path failed=0 checked=0
	# The index, not the working tree: refuse only attempts the push carries,
	# and never accept local payloads as a substitute for a published asset.
	# Captured rather than read through process substitution, which would
	# swallow the failure. :(glob) keeps * from crossing /.
	tracked=$(git -C "$experiments" ls-files -- ':(glob)*/evidence/*/SHA256SUMS') || die "check needs a Git checkout"
	local -a committed=() records=()
	[[ -z $tracked ]] || mapfile -t committed <<<"$tracked"
	declare -A assets=() visit=()
	for path in "${committed[@]}"; do visit[${path%%/*}]=1; done
	for manifest in "$experiments"/*/evidence-assets.tsv; do
		record=${manifest%/*}
		visit[${record##*/}]=1
	done
	[[ ${#visit[@]} -eq 0 ]] || mapfile -t records < <(printf '%s\n' "${!visit[@]}" | sort)
	for record in "${records[@]}"; do
		read_manifest "$experiments/$record/evidence-assets.tsv"
		for path in "${committed[@]}"; do
			[[ ${path%%/*} == "$record" ]] || continue
			attempt=${path#*/evidence/}
			attempt=${attempt%/SHA256SUMS}
			if [[ ! -v rows[$attempt] ]]; then
				echo "evidence-store.sh: $record/evidence/$attempt is committed without an asset row; publish it" >&2
				failed=1
			fi
		done
		for attempt in "${order[@]}"; do
			local tag asset bytes sha
			read -r tag asset bytes sha <<<"${rows[$attempt]}"
			if [[ ! -v assets[$tag/] ]]; then
				assets[$tag/]=1
				local line
				while IFS=$'\t' read -r line; do assets[$tag/${line%% *}]=${line#* }; done < <(
					gh api "repos/$repository/releases/tags/$tag" --jq '.assets[] | "\(.name) \(.size) \(.digest)"' || echo "-missing release $tag"
				)
			fi
			if [[ ${assets[$tag/$asset]-} != "$bytes sha256:$sha" ]]; then
				echo "evidence-store.sh: $tag/$asset: store has '${assets[$tag/$asset]-nothing}', manifest pins '$bytes sha256:$sha'" >&2
				failed=1
			fi
			checked=$((checked + 1))
		done
	done
	[[ $failed == 0 ]] || exit 1
	echo "evidence store: $checked assets match their manifests"
}

[[ $# -ge 1 ]] || usage
command=$1
shift
case $command in
publish | fetch | check) "$command" "$@" ;;
*) usage ;;
esac
