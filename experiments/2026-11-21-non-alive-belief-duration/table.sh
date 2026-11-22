#!/usr/bin/env bash
# Exact strings from the healthy population; no floating-point arithmetic.
set -euo pipefail
record=$(cd "$(dirname "$0")" && pwd)
awk -F '\t' '
BEGIN {
 print "\\begin{table}[ht]\\centering\\footnotesize"
 print "\\setlength{\\tabcolsep}{3pt}"
 print "\\begin{tabular}{llrrrrr}\\toprule"
 print "Case & Index & Pair-s & Without alive (ns) & Completed & p95 (ns) & Max (ns) \\\\ \\midrule"
}
NR>1 && $15=="healthy-pair-exclusions" {
 name=$2
 sub(/^partial-partition-healthy$/,"PP healthy (new)",name)
 sub(/^partial-partition$/,"PP (new)",name)
 sub(/^correlated-dense-healthy$/,"Dense healthy (old)",name)
 sub(/^correlated-dense-latency-100us$/,"Latency 100us (old)",name)
 sub(/^correlated-dense-latency-10ms$/,"Latency 10ms (old)",name)
 sub(/^correlated-dense-service-100us$/,"Service 100us (old)",name)
 sub(/^correlated-dense-service-1ms$/,"Service 1ms (old)",name)
 sub(/^correlated-dense$/,"Dense (old)",name)
 exposure=$18
 sub(/000000000$/,"",exposure)
 p95=$32; max=$33
 if(p95=="null")p95="--"
 if(max=="null")max="--"
 print name " & " $3 " & " exposure " & " $24 " & " $29 " & " p95 " & " max " \\\\"
 n++
}
END {
 print "\\bottomrule\\end{tabular}"
 print "\\caption{Fixed healthy-pair outcomes, kept separate by source baseline and case. All without-alive time is suspect time. All censor counts are zero; -- denotes an empty completion sample, not a zero quantile.}"
 print "\\end{table}"
 if(n!=24)exit 1
}' "$record/cells.tsv"
