#!/usr/bin/env bash
# Lossless packaging only: callers own attempt identity and recording status.
set -euo pipefail
export LC_ALL=C
shopt -s nullglob dotglob

die() { echo "evidence.sh: $*" >&2; exit 1; }
[ "$#" -eq 3 ] || die "usage: evidence.sh {pack|unpack} SOURCE NEW_DEST"
mode=$1
[[ $mode == pack || $mode == unpack ]] || die "expected pack or unpack"
source_path=$2
while [[ $source_path != / && $source_path == */ ]]; do source_path=${source_path%/}; done
[[ -d $source_path && ! -L $source_path ]] || die "source must be a regular directory, not a symlink"
source_dir=$(cd -- "$source_path" && pwd -P)
[[ ! -e $3 && ! -L $3 ]] || die "destination already exists"
parent=$(cd -- "$(dirname -- "$3")" && pwd -P)
[[ $parent != "$source_dir" && $parent != "$source_dir/"* ]] || die "destination must be outside source"
destination=$parent/$(basename -- "$3")

safe_name() { [[ $1 =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ && $1 != SHA256SUMS ]]; }
stored_name() {
	stored=$1
	if [[ $stored == *.ndjson ]]; then stored+=.gz; fi
}

# Validate the entire file set and manifest before creating any output.
declare -A expected=() hashes=()
files=("$source_dir"/*)
for file in "${files[@]}"; do
	name=${file##*/}
	[[ -f $file && ! -L $file ]] || die "not a flat regular file: $name"
	if [[ $mode == unpack && $name == SHA256SUMS ]]; then continue; fi
	safe_name "$name" || die "unsafe or reserved basename: $name"
	if [[ $mode == pack ]]; then
		stored_name "$name"
		[[ ! -v expected[$stored] ]] || die "compressed-name collision: $stored"
		expected[$stored]=$name
	fi
done

if [[ $mode == unpack ]]; then
	[[ -f $source_dir/SHA256SUMS && ! -L $source_dir/SHA256SUMS ]] || die "missing checksum manifest"
	while IFS= read -r line || [[ -n $line ]]; do
		[[ $line =~ ^([0-9a-f]{64})\ \ ([a-zA-Z0-9][a-zA-Z0-9._-]*)$ ]] || die "invalid checksum manifest line"
		hash=${BASH_REMATCH[1]}
		name=${BASH_REMATCH[2]}
		safe_name "$name" || die "unsafe or reserved manifest basename: $name"
		[[ ! -v hashes[$name] ]] || die "duplicate manifest basename: $name"
		hashes[$name]=$hash
		stored_name "$name"
		[[ ! -v expected[$stored] ]] || die "compressed-name collision: $stored"
		expected[$stored]=$name
	done <"$source_dir/SHA256SUMS"
	for file in "${files[@]}"; do
		name=${file##*/}
		[[ $name == SHA256SUMS || -v expected[$name] ]] || die "unexpected payload: $name"
	done
	for stored in "${!expected[@]}"; do
		[[ -f $source_dir/$stored ]] || die "missing payload: $stored"
	done
fi

# Bash arithmetic is signed; reject invalid/overflowing tool output before using it.
max_integer=9223372036854775807
number() {
	[[ $1 =~ ^(0|[1-9][0-9]*)$ && ${#1} -le 19 ]] || die "invalid size/space result: $1"
	[[ ${#1} -lt 19 || $1 < $max_integer || $1 == "$max_integer" ]] || die "size/space overflow: $1"
}

if [[ $mode == unpack ]]; then
	declare -a sizes=()
	for stored in "${!expected[@]}"; do
		if [[ ${expected[$stored]} == *.ndjson ]]; then
			size=$(gzip -d -c -- "$source_dir/$stored" | wc -c) || die "cannot size payload: $stored"
			[[ $size =~ ^[[:space:]]*([0-9]+)[[:space:]]*$ ]] || die "invalid byte count: $stored"
			size=${BASH_REMATCH[1]}
		else
			size=$(stat --format='%s' -- "$source_dir/$stored") || die "cannot size payload: $stored"
		fi
		number "$size"
		sizes+=("$size")
	done
	space=$(stat -f --format='%a %S' -- "$parent") || die "cannot query destination space: $destination"
	[[ $space =~ ^([0-9]+)\ ([0-9]+)$ ]] || die "invalid destination space: $destination: $space"
	blocks=${BASH_REMATCH[1]}
	unit=${BASH_REMATCH[2]}
	number "$blocks"
	number "$unit"
	[[ $unit -gt 0 && $blocks -le $((max_integer / unit)) ]] || die "destination space overflow or zero allocation unit: $destination"
	available=$((blocks * unit))
	payload=0
	for size in "${sizes[@]}"; do
		allocated=$((size / unit + (size % unit != 0)))
		[[ $allocated -le $(((max_integer - payload) / unit)) ]] || die "payload allocation overflow: $destination"
		payload=$((payload + allocated * unit))
	done
	reserve=$((256 * 1024 * 1024))
	[[ $payload -le $((max_integer - reserve)) ]] || die "payload plus reserve overflow: $destination"
	[[ $available -ge $((payload + reserve)) ]] || die "insufficient space: destination=$destination available=$available payload=$payload reserve=$reserve bytes"
fi

# Leave a failed destination inspectable; never remove evidence on an error.
mkdir -- "$destination"
trap 'echo "evidence.sh: failed; partial destination retained: $destination" >&2' ERR
if [[ $mode == pack ]]; then
	: >"$destination/SHA256SUMS"
	for file in "${files[@]}"; do
		name=${file##*/}
		stored_name "$name"
		if [[ $name == *.ndjson ]]; then
			gzip -n -6 -c -- "$file" >"$destination/$stored"
		else
			cp -- "$file" "$destination/$stored"
		fi
		hash=$(sha256sum <"$file")
		printf '%s  %s\n' "${hash%% *}" "$name" >>"$destination/SHA256SUMS"
	done
else
	for stored in "${!expected[@]}"; do
		name=${expected[$stored]}
		if [[ $name == *.ndjson ]]; then
			gzip -d -c -- "$source_dir/$stored" >"$destination/$name"
		else
			cp -- "$source_dir/$stored" "$destination/$name"
		fi
		hash=$(sha256sum <"$destination/$name")
		[[ ${hash%% *} == "${hashes[$name]}" ]] || die "checksum mismatch: $name (partial destination retained)"
	done
fi
