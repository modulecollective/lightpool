#!/usr/bin/env bash
# Render locally with a pinned engine and fixed TeX bundle; no system install.
set -euo pipefail
[ "$#" -eq 1 ] || { echo 'usage: render.sh TOOL_CACHE_DIRECTORY' >&2; exit 1; }
record=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$1"
cache=$(cd "$1" && pwd)
archive="$cache/tectonic-0.15.0-x86_64-unknown-linux-musl.tar.gz"
sha=dfb82876f2986862996e564fa507a9e576e0c1e3bee63c2c1bd677c2543e6407
if [ ! -f "$archive" ]; then
 curl -fsSL -o "$archive" https://github.com/tectonic-typesetting/tectonic/releases/download/tectonic%400.15.0/tectonic-0.15.0-x86_64-unknown-linux-musl.tar.gz
fi
printf '%s  %s\n' "$sha" "$archive" | sha256sum -c -
tar -xzf "$archive" -C "$cache"
mkdir -p "$cache/tex-cache"
# Fixed bundle (not the moving default_bundle alias).
XDG_CACHE_HOME="$cache/tex-cache" SOURCE_DATE_EPOCH=1795478400 "$cache/tectonic" \
 --web-bundle https://data1b.fullyjustified.net/tlextras-2022.0r0.tar \
 --keep-logs --outdir "$record" "$record/paper.tex"
