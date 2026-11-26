#!/usr/bin/env bash
# A pin keeps the hash it was recorded with: that string is an identity in
# rows and digests. History from 8522aa2 was rewritten once to remove
# committed payloads, so a script hands a pin to Git through this map.
# Prints the rewritten hash, or the argument when it was never rewritten.
set -euo pipefail
[[ $# -eq 1 && $1 =~ ^[0-9a-f]{40}$ ]] || { echo 'usage: revision.sh FULL_COMMIT_HASH' >&2; exit 2; }
map=$(dirname "$0")/rewritten-commits.tsv
if [[ -f $map ]]; then
	awk -F '\t' -v old="$1" '$1 == old { print $2; found = 1; exit } END { if (!found) print old }' "$map"
else
	printf '%s\n' "$1"
fi
