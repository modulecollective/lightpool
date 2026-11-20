#!/usr/bin/env bash
# Local monotonic intervals in ns. Use nearest-rank quantiles.
set -euo pipefail
dir=$1
id=$2
first=$(jq '.Rounds[0].StartNS' "$dir/rounds.json")
last=$(jq '.Rounds[-1].EndNS' "$dir/rounds.json")
{
  jq -r '.Rounds[] | ["observerPingNS", .PingNS[0]]|@tsv' "$dir/rounds.json"
  if [[ -f "$dir/observer.ndjson" ]]; then
    jq -r --argjson start "$first" --argjson end "$last" '
      if .step and .step.StartNS >= $start then ["start",.step.sequence,.step.StartNS]
      elif .end and .end.endNS <= $end then ["end",.end.sequence,.end.endNS,.end.recorderNS]
      else empty end | @tsv' "$dir/observer.ndjson" | awk '
      $1=="start" {seq=$2; at=$3} $1=="end" && $2==seq {print "ownerStepNS\t" $3-at; print "recorderNS\t" $4}'
    jq -r --argjson start "$first" --argjson end "$last" '
      select(.step and .step.StartNS >= $start and .step.CoreEndNS <= $end) | .step |
      (["coreStepNS", .CoreEndNS-.CoreAtNS], ["queueDepth", .queueDepth],
       (select(.timerDueNS != null) | ["timerLatenessNS", .StartNS-.timerDueNS]),
       (select(.input.kind=="Datagram") | ["udpReadToHandleNS", .StartNS-.input.body.Received]),
       (select(.input.kind=="StreamMessage") | ["tcpFrameReadToHandleNS", .StartNS-.frameReadNS])) | @tsv' "$dir/observer.ndjson"
  fi
} | LC_ALL=C sort -k1,1 -k2,2n | awk -v id="$id" '
function emit(){if(n)printf "%s\t%s\t%d\t%d\t%d\t%d\t%d\n",id,k,n,a[int((n*50+99)/100)],a[int((n*95+99)/100)],a[int((n*99+99)/100)],a[n]}
$1!=k {emit(); k=$1; n=0} {a[++n]=$2} END{emit()}'
