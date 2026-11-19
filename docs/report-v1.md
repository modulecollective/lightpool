# Lightpool report v1

`lightpool-report -trace run.ndjson [-format text|json]` reads trace v1 and
reports single-run outcomes. Text is the default. JSON uses
`format: "lightpool-report"`, `version: 1`; counters, times, lifetimes and
seeds are decimal strings. Time fields ending in `NS` are nanoseconds.
JSON and text contain the same measurements. No simulation runs while
reporting, and no wall-clock generation timestamp enters the output.
The calculator is identified by its metric/schema version, separately from
the producer's full source/toolchain/scenario manifest and trace SHA256.

The command finishes parsing and aggregation before writing stdout. Malformed
complete lines fail with their line number and no report. A valid prefix,
including an unterminated trailing line, or a budget-limited recording emits
an INCOMPLETE report and exits nonzero. Only a complete end record permits a
completed-experiment report. Read, close and output errors propagate.

## Populations and episodes

Membership is observer-relative and resets on each observer lifetime.
Unknown (including removal), alive, suspect, dead and left remain distinct.
Only transitions into dead count as declarations; incarnation-only changes
and repeated dead upserts do not. Re-entry after removal can create another
episode. Target process truth, target/observer core stopped/left flags, pause
state and active configured fault windows incident to either endpoint
accompany every row.
These are context, not inferred causes. Running targets across partitions
are not automatically labeled false crash diagnoses.

Each explicit crash freezes a cohort of other running observers whose core
is neither stopped nor left and whose pre-crash belief is alive or suspect.
Already-dead, inactive and unknown/left exclusions are counted separately.
The next new dead transition completes each eligible observation. The first
such transition, per-observer latency and all unresolved/interrupted rows
are retained. An observer crash/restart/shutdown/leave or target restart
censors an open observation. A repeated crash of an already stopped target
has no new cohort and carries an ambiguity notice. A pause is not a crash.

Every running-target declaration opens a recovery episode until the same
observer next records alive. Removal does not recover it. Observer or target
termination/restart/leave interrupts it explicitly; stopped/left observers
cannot keep an active observation. A declaration about an already stopped/left
target core records a target_inactive recovery row, despite its process flag.
Explicit join actions involving the target during the episode are listed
without assigning causality. Older v1
traces lacking the optional action peer carry a coverage notice; no peer is
inferred from scenario timing.

Ordering is exact `(time, sequence)` with recorded change order inside an
event. Completed horizon-limited traces observe through the configured
horizon even without an event there. Other traces observe only through the
last valid event. Open rows' end time is that observation boundary and their
end sequence is the last evidence sequence, not an invented horizon event.
Null durations mean no observed completion, never zero-latency success.
Quantiles are nearest rank (`ceil(p * completed)`) over observed completions,
with completed and eligible counts together. Empty populations and
populations without completions carry null quantiles and a reason.
Distinct running-target pairs count observer, observer lifetime and subject;
target lifetime is recorded separately on each episode.

## Traffic and limits

Sum only traffic stages `sent` and `received`, separately for datagrams and
streams (including partial stream chunks). Attempts, arrivals and handler
events add no bytes. Recorded drop entries are grouped by transport and
reason; these are diagnostics, not a unique packet-loss population. End
sent/received totals are a cross-check. End `datagramDrops` counts directional
fault and receive-queue drops; it excludes some other diagnostic drops.
End `detections` counts raw dead-membership notifications regardless of
truth, and is not a detection-success metric.

Cluster byte rates divide total sent/received framed bytes by the observed
nanosecond duration from zero. Decimal bytes/s use six fractional places;
integer byte totals and the exact duration remain available. Zero duration
produces null rates and a reason. Bytes exclude IP/TCP overhead.

The report includes current model limitations: prescribed directional fault
windows with jitter sampled once per window, configured service time, no
shared bandwidth/path topology and message-level streams rather than TCP
packets. Accelerated example settings, including disabled TCP probes, remain
visible in the manifest; they are validation fixtures, not tuned research
baselines. Reporting does not calibrate the model or establish superiority.

Reduction is incremental. Retained membership and open cohorts can require
O(N²) state; output rows grow with outcome count. Text rendering buffers the
report, not trace history. JSON encodes the finished report to its writer.
