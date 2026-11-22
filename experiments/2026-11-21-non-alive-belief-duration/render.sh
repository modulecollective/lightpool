#!/usr/bin/env bash
# Pinned Linux x86_64 renderer; output and downloads go to a new directory.
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: render.sh NEW_DIRECTORY' >&2; exit 2; }
record=$(cd "$(dirname "$0")" && pwd)
mkdir -- "$1"
out=$(cd "$1" && pwd)
url=https://github.com/tectonic-typesetting/tectonic/releases/download/tectonic%400.15.0/tectonic-0.15.0-x86_64-unknown-linux-musl.tar.gz
curl -sSfL "$url" -o "$out/tectonic.tar.gz"
echo 'dfb82876f2986862996e564fa507a9e576e0c1e3bee63c2c1bd677c2543e6407  tectonic.tar.gz' > "$out/renderer.sha256"
(cd "$out" && sha256sum -c renderer.sha256)
tar -xzf "$out/tectonic.tar.gz" -C "$out"
export XDG_CACHE_HOME="$out/cache"
bundle=https://data1b.fullyjustified.net/tlextras-2022.0r0.tar
"$out/tectonic" -X bundle -w "$bundle" cat SHA256SUM > "$out/bundle-id"
[[ $(<"$out/bundle-id") == 6ffe055852f8faf66c0acbe1a7fb27f87b869a90bad1204f3bf4d9683f597c7c ]]
# Fixed epoch excludes host wall-clock metadata from the derived PDF.
export SOURCE_DATE_EPOCH=1795305600
cp "$record/paper.tex" "$record/paper-table.tex" "$out/"
"$out/tectonic" -X compile -w "$bundle" --keep-logs --outdir "$out" "$out/paper.tex"
echo "rendered $out/paper.pdf"
