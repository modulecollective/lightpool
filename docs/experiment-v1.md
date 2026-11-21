# Fixed Lightpool baseline pilot v1

The baseline with the exchange-attribution fix treats suspect/dead exchange
entries as unattributed suspicion. Exchange alone cannot confirm an existing
timer or gossip a new accusation; the first known origin seeds attribution
without shortening. Finite expiry, direct buddy notification and ordinary
distinct-origin shortening remain. This deliberately changes the inherited
merge behavior. Historical records below retain their pinned protocol and
tools; their results are not measurements of this corrected baseline.

`lightpool-experiment` records one fixed case and paired replicate at a time.
These are workload and measurement analogues on current Lightpool, not
historical SWIM or Lifeguard implementations, benefit ratios or numerical
reproductions. The command performs no candidate search or statistical inference.

```sh
go build -o lightpool-experiment ./cmd/lightpool-experiment
./lightpool-experiment -case threshold -replicate 1 -out results-threshold-1
./lightpool-experiment -reduce results-threshold-1 -out replayed-summary.json
cmp results-threshold-1/summary.json replayed-summary.json
```

A clean VCS build supplies its revision automatically. Modified or unstamped
builds require `-revision <explicit-source-identity>`. Both recording and replay
refuse to overwrite output. Create the parent of the output directory first.
Reduction reads saved inputs; it does not execute protocol code or modify them.

Each run stores canonical `spec.json` and `scenario.json`, incremental
`trace.ndjson`, the existing offline `report.json`, and `summary.json` with
SHA256 digests of the specification, scenario and trace. The specification
records source, toolchain, platform, model and seed versions and fixed cohorts.
`resources.json` records elapsed wall time and process high-water resident
memory on Linux, separately from deterministic output. Host measurements are
never inputs to replay. `status.json` retains recording failures as well as
complete or invalid-warmup results. Record into a dedicated working
directory, then retain declared campaign evidence under its experiment record
using the packaging workflow below.

## Cases and boundaries

The six cases use 32 nodes, default `core.Config`, 1 ms one-way latency,
zero node service duration and 1 MiB datagram/stream capacities. TCP probing
and periodic push/pull remain enabled. Nodes join node 0 at 10 ms spacing;
identities are fixed and encryption is disabled.
There is no automatic rejoin. Warm-up ends at 15 seconds and the fixed horizon
is 120 seconds. Every observer must know every member as alive immediately
before the warm-up boundary. Failure is retained as `invalid-warmup`, never
repaired by changing the measurement window or selecting another replicate.

| Case | Intervention at 15 seconds |
| --- | --- |
| `healthy` | None. |
| `crash` | Crash one fault-seed-selected non-bootstrap node. |
| `threshold` | Pause four fixed nodes for 8.192 seconds. |
| `interval` | Pause the same four nodes for 2.048 seconds, run for 1.024 seconds, repeat through the horizon. |
| `loss` | 100,000 PPM independent datagram loss on all directed pairs through the horizon. Streams retain their message model. |
| `correlated` | For 16.384 seconds, drop datagrams and fail streams from four fixed impaired nodes A to a healthy target T. All other paths stay healthy. |

`threshold-128` changes size to 128 and anomaly count to 16. The companions
`ordinary-healthy`, `ordinary-pause`, and `ordinary-crash` use eight nodes and
one affected node. None uses forced probes. Targets and anomaly sets are
selected by sorting per-node SHA256 ranks keyed by the fault seed, separately
from protocol and schedule seeds.

Seed derivation `sha256-replicate-namespace/v1` hashes its UTF-8 version bytes,
LE64 replicate index, then UTF-8 namespace (`protocol`, `fault`, `schedule`),
and reads the first LE64 word. Replicate is the paired unit; it is not a
collection of independent observer-pair samples.

Each run has a 5,000,000-event budget and a 2 GiB trace-output cap. Either
limit, write failure or close failure exits nonzero and preserves failed
output. A partial final line is permitted by the existing reader. A failure
before a complete header is a recording failure with no reducible trace.
The runner never fabricates a successful end record. `-reduce` also exits
nonzero for incomplete or invalid evidence, retaining its new summary file.

## Protocol overlay

```sh
echo '{"SuspicionMult": 6}' > protocol.json
./lightpool-experiment -case correlated-dense -replicate 1 -protocol protocol.json -out results-sm6-1
```

`-protocol` decodes one JSON object onto the case's `core.Config` before
recording. Omitted fields keep the case's values; an unknown field, trailing
value or a configuration `core.Config.Validate` refuses is an error. Field
names are the Go names and durations are integer nanoseconds, as in
`scenario.json`. Cases, populations, seeds and the specification are
unchanged.

The overlay is only an input: it is saved in `scenario.json`, so `-reduce`
works unchanged. `-protocol` cannot be combined with `-reduce` or
`-admission`, and `-admission` refuses a retuned run because it is not the
fixed fixture.

## Measurement semantics

`summary.json` uses `lightpool-pilot/v1`; integer counters, times, lifetimes,
replicate indices and seeds use decimal strings. All `NS` fields are
nanoseconds. The measured interval is [15s,120s], with trace sequence and
change order resolving equal-time events. Incomplete prefixes end at their
last valid event and cannot claim a completed experiment.

All populations exclude self-pairs. Pause/no-fault cases label new dead
transitions about active running targets outside the fixed anomaly set as
`FP-analogue`. `FP-minus-analogue` additionally excludes anomalous observers.
Resuming does not change either population. Correlated cases predeclare O→T
pairs; neither endpoint is anomalous and both direct directions must remain
fault-free throughout measurement. A→T exclusions remain in the raw
running-target rows without becoming false-crash diagnoses. Loss cases also
retain running-target exclusions without an FP label.

Exposure counts active, running ordered-pair nanoseconds admitted at warm-up,
with lifetime identities retained. Pauses and dead beliefs never remove an
eligible pair from its own denominator. Termination/restart interrupts that
lifetime. Counts, affected targets, affected pairs, exposure, events per
million pair-seconds and fraction of eligible pairs ever excluded are separate
fields. Raw declarations retain process, pause, core-health and configured
fault context; a percentage of declarations is not an exposure rate.

True crashes reuse the existing report's frozen pre-crash alive/suspect
observer cohort and already-dead/inactive/unknown exclusions. First suspicion,
first dead, per-observer dead and full-dissemination time remain distinct.
A dead rumor can bypass suspicion. A preexisting suspicion retains its earlier
reference and a null post-crash latency. Full dissemination requires every
original observer; interruption before completion censors it, including an
early detector that terminates while others are still detecting.
Recovery uses the [existing lifetime rules](report-v1.md): removal is not
recovery, and interruption/horizon retains unresolved rows. Nearest-rank
p50/p95/max use observed completions with eligible and completed counts.
Null values are not replaced with zero or the horizon.

Traffic rates sum encoded sent datagrams and stream chunks in the window,
separating bytes by transport. Message counts count a datagram or stream
write once at its first sent bytes, regardless of contained semantic messages
or later chunks. Active node-nanoseconds include paused live nodes. Drop
records, reasons and dropped bytes are diagnostic and may repeat a stream
write or describe a partial chunk; they are not a unique lost-message count.
Bytes exclude IP/TCP overhead and retransmissions.

Mechanism rows preserve suspicion origin, distinct confirmation facts, timer
arms/cancellations/fires, target refutations, outcome and evidence references.
Only a live `timer/fire` for that suspicion subject preceding its dead change
in the same core step qualifies as `local-suspicion-expiry`. Other dead
transitions are `received-or-other-dead-transition`. Refutation references
within an episode and all target refutations are retained; temporal association
does not establish which packet carried a rumor or refutation.

## Independent datagram loss

`sim.Fault.LossPPM` is an integer in [0,1,000,000], default zero and omitted
from zero-loss JSON. Positive windows for the same directed link may not
overlap. Deterministic `Drop` takes precedence. Probability is sampled after
a successful encoded, in-budget send, before delivery; it never applies to
stream chunks. A duplicate copies the admitted datagram and consumes no draw.
Drop detail is `independent datagram loss`. The manifest records the draw
version only when a positive probability exists; zero-loss trace bytes are
preserved for otherwise identical inputs and source identity.

`sha256-link-ordinal-ppm/v1` hashes UTF-8 version bytes, LE64 fault seed,
LE64 byte length plus UTF-8 from-name, the same for to-name, LE64 one-based
send ordinal, then LE64 rejection counter starting at zero. The first LE64
hash word is rejected below 551,616; otherwise its remainder modulo 1,000,000
is compared with `LossPPM`. Every successful encoded, in-budget send to a
resolved link advances that link's ordinal, including outside loss windows
and during deterministic drops. Restarts do not reset it. Matching ordinals
share draws across variants; different traffic can realize different losses.

## Opt-in ordinary Linux probe

```sh
LIGHTPOOL_PILOT_OUT="$PWD/ordinary-results" \
LIGHTPOOL_PILOT_REVISION="<tested-source-identity>" \
  go test ./internal/experiment -run '^TestOrdinaryPilot$' -count=1 -v -timeout 22m
```

This runs healthy, 8.192-second SIGSTOP/SIGCONT pause, and SIGKILL crash cases
three times each, sequentially. One child process per node uses the public
node API and real loopback UDP/TCP, default detector values and no
encryption. Child startup is bounded at 30 seconds; all-ready establishes
time zero, followed by the same 15-second warm-up and 120-second horizon. A
warm-up failure is retained without a delayed fault or replacement run.
Normal unit CI skips the workload.

Children collect snapshots and traffic every nominal 100 ms independently;
a stopped child cannot block others. Raw collection start/end and parent
receipt bounds, actual join/signal times, confirmed process states, kernel,
host and toolchain, per-node sample counts/gaps, transition intervals and final
traffic/notification-drop counts are retained. Transition bounds span adjacent
actual samples. Short states and exact episode counts remain unobservable.
Cleanup resumes, kills and reaps every child on success and failure.
The ordinary seeds are ambient. Comparisons with virtual companions concern
cohort, recovery/non-completion and event ordering, not latency calibration or
directional-path fidelity.

## Fixed correlated mechanism screen

`correlated-dense` preserves the original replicate's target at fault-rank
index four. A contains the first 24 other ranked identities, including the
original four; the other seven identities are O. The fault window and
protocol defaults match `correlated`. `correlated-dense-healthy` uses the
same groups, seeds and seven O→T eligible pairs without any faults. Group A
is a comparison grouping in that control. The original case has 27 eligible
pairs. At full running exposure over 105 seconds these give 735 and 2,835
pair-seconds respectively; dead beliefs do not remove exposure.

Six additional fixed cases change one dimension from dense:

| Case suffix after `correlated-dense-` | Change |
| --- | --- |
| `latency-100us`, `latency-10ms` | One-way base latency 100 µs or 10 ms. |
| `service-100us`, `service-1ms` | Service duration on every node 100 µs or 1 ms. |
| `buffer-64k`, `buffer-16m` | Both transport capacities 64 KiB or 16 MiB. |

All retain the same bounds, populations, target and seed namespaces. These
are uncalibrated virtual workloads, not ordinary UDP/TCP reproductions.

```sh
./lightpool-experiment -admission results-correlated-dense-1 -out admission.json
```

`-admission` is mutually exclusive with recording flags and `-reduce`. It
validates the saved specification, scenario, manifest and report using the
existing reducer, requires an unchanged fixed correlated fixture, then makes
an additional streaming pass over the trace. It never executes protocol
code or overwrites evidence. `lightpool-correlated-admission/v1` output uses
decimal strings for counters/times and retains input/trace digests, status,
populations, traffic, backlog and timer-lateness diagnostics, target probes
and every target suspicion episode after warm-up. Evidence addresses use
(timeNS, seq, zero-based changeIndex), with lifetime and incarnation joins.
Backlog and timer diagnostics cover the full trace, including warm-up;
population exposure and traffic retain the measured interval above.
An invalid or incomplete input writes a diagnostic JSON result and exits
nonzero and cannot count eligible episodes or qualifying witnesses.
A complete null result succeeds. Unreadable evidence may only
supply a diagnostic reason; it cannot supply validated measurements.

An acceleration witness requires an eligible O→T episode beginning in the
fault window, an initiating A sender and a distinct accepted A confirming
sender. Each needs an earlier local failed natural probe and locally
emitted suspicion at the same target incarnation and lifetimes. An accepted
confirmation must be followed in the same step by a live rearm with a
strictly earlier effective deadline. Shortening must precede the first
matching higher-incarnation target refutation and its observed acceptance
at O. Missing generation or matching acceptance censors the chain. Sender
names in rumors alone supply no local-probe provenance. An earlier target
refutation with later acceptance remains an in-flight diagnostic, counted
per shortening, rather than a qualifying witness.

Harm is separate: a live shortened timer must fire before a dead change in
the same step, before acceptance, and before the initial unshortened
deadline. Canceled/stale fires and received dead rumors cannot establish
local expiry. Refutation lookup continues past dead outcomes. Timely
recovery can leave acceleration observed with no harm. These are observed
orderings; they supply no counterfactual candidate-benefit claim. Helper
identities and enclosing-step traffic are retained, but the route of a
successful ACK remains unknown.

## Experiment records

Protocol candidates are not configuration in `core`. A candidate is a patch
against a named baseline commit, and each experiment leaves one directory,
`experiments/<id>/`:

| File | Contents |
| --- | --- |
| `base` | Baseline commit, an ancestor of the commit carrying the record. |
| `toolchain` | Go version and platform; replay is only promised on both. |
| `<variant>.patch` | One per variant, from `git diff <base> -- core wire`. |
| `run.sh` | The grid as a recipe: cases, overlays, replicates, variants. |
| `cells.tsv` | One row per cell: identity, status, trace and summary SHA256, headline numbers named in advance. |
| `evidence/<attempt>/` | Original payloads, compressed NDJSON and raw-byte `SHA256SUMS`. |
| `evidence.tsv` | Attempt provenance, observed exit/status and raw/stored byte totals. |

Original campaign evidence is stored independently of regeneration. A virtual
cell is deterministic, so `run.sh <new-dir> [cell ...]` rebuilds the baseline and
each patched variant from `git archive <base>`, records the named cells (all
of them by default) and refuses any whose row differs from `cells.tsv`.
Patches never compile on `master`, and stop applying once `master` moves
away from their base; the record is a claim about that base.

The recipe keeps comparisons honest:

- A patch touching anything outside `core/` and `wire/` is refused. A
  candidate that needs a new trace fact lands that fact on `master` first,
  which makes a new baseline.
- A variant is recorded with `-revision <base>+<variant>:<first 12 hex of
  the patch SHA256>`, so every trace header identifies its source.
- The baseline binary reduces every cell, and its summary must equal the
  one written while recording.
- Null control: with inputs that should leave a variant inert, its trace
  must equal the baseline's after the header line. The whole files must
  differ, since the header carries the identity.

A winning candidate lands later as an ordinary unconditional change in
`core`, which becomes the next baseline.

## Retaining and restoring original evidence

Retain every attempt in a declared campaign under
`experiments/<id>/evidence/<attempt>/`, including controls, negative results,
invalid warm-ups, recording failures and retries. Each retry or fresh-process
repeat has a new attempt ID. Disposable development probes and compiled
binaries are excluded. Keep evidence indefinitely in ordinary Git history;
deleting it later does not reclaim that history. Measure campaign totals
before committing. A materially larger campaign needs a new storage decision,
not selective omission of failures. There is no automatic pruning or quota.

`experiments/evidence.sh pack SOURCE NEW_DEST` reads a finished, quiescent
attempt directory, compresses every `*.ndjson` as `*.ndjson.gz` with
`gzip -n -6`, copies other files byte-for-byte and writes `SHA256SUMS` over
original bytes and basenames. This covers both virtual `trace.ndjson` and
ordinary `samples.ndjson`. JSON stays readable. Keep available specs, inputs,
reports, summaries, admission results, status, resources and diagnostics.
Missing output stays missing; empty and partial traces stay unchanged.

`experiments/evidence.sh unpack SOURCE NEW_DEST` validates the manifest and
exact payload file set, restores original names and verifies every raw-byte
checksum. It runs no simulation or reduction. Both commands require Bash
(tested with 5.2), coreutils and gzip, an existing destination parent and a new
destination outside the source. Inputs must be flat regular files, with basenames starting
with an ASCII letter/digit followed by letters, digits, dots, underscores or
hyphens. Symlinks, nested directories, reserved `SHA256SUMS`, duplicate manifest
names and compressed-name collisions (such as `trace.ndjson` alongside
`trace.ndjson.gz`) are refused. Neither command changes its source or overwrites
an existing destination. On an I/O or checksum failure the command exits
nonzero; a partial destination remains inspectable. Retry into a new directory.

The campaign workflow owns `evidence.tsv`: packaging cannot infer a command's
exit code. Use these tab-separated columns:

```text
attempt	directory	provenance	exitCode	status	rawBytes	storedBytes
```

`directory` is relative to the experiment record. `provenance` is `original`
or `regenerated`; regenerating a missing historical result never recovers its
original host observations or failed output. Record unavailable historical
attempts with provenance/status `unavailable` and `-` for path and byte totals.
Use `unknown` for an exit code or status that was not observed. Preserve the
observed command exit code separately from the recording's status. Keep the
original source/toolchain, seeds and commands in the existing specs/recipe;
do not change `cells.tsv` metric columns. Historical records without retained
payloads remain regeneration recipes, not durable original evidence.

For example, from the repository root, with a built experiment binary and an
existing experiment record (choose a unique attempt and a new working path):

```sh
record=experiments/your-campaign-id
attempt=correlated-dense-r1-attempt1
work=/absolute/path/to/new-working-directory
mkdir "$work"
source_dir="$work/$attempt"
# Capture diagnostics even if recording fails before it creates source_dir.
if ./lightpool-experiment -case correlated-dense -replicate 1 \
    -out "$source_dir" >"$work/stdout.txt" 2>"$work/stderr.txt"; then
  exit_code=0
else
  exit_code=$?
fi
mkdir -p "$source_dir"
cp "$work/stdout.txt" "$work/stderr.txt" "$source_dir/"
status=unknown
if [ -f "$source_dir/status.json" ]; then
  status=$(jq -r .status "$source_dir/status.json")
fi
mkdir -p "$record/evidence"
experiments/evidence.sh pack "$source_dir" "$record/evidence/$attempt"
raw=$(find "$source_dir" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1} END {printf "%.0f", n}')
stored=$(find "$record/evidence/$attempt" -maxdepth 1 -type f -printf '%s\n' | awk '{n+=$1} END {printf "%.0f", n}')
# Initialize the header once when creating the campaign inventory.
if [ ! -e "$record/evidence.tsv" ]; then
  printf 'attempt\tdirectory\tprovenance\texitCode\tstatus\trawBytes\tstoredBytes\n' >"$record/evidence.tsv"
fi
printf '%s\tevidence/%s\toriginal\t%s\t%s\t%s\t%s\n' \
  "$attempt" "$attempt" "$exit_code" "$status" "$raw" "$stored" >>"$record/evidence.tsv"
awk -F '\t' 'NR>1 {raw+=$6; stored+=$7} END {printf "campaign raw=%.0f stored=%.0f bytes\n", raw, stored}' "$record/evidence.tsv"
```

If recording never created output, this retains only captured diagnostics and
an explicit observed failure in the inventory; it does not fabricate a trace
or status file. If the original attempt directory is lost and no diagnostics
survive, keep an unavailable inventory row with no invented evidence path.

Include the evidence and inventory in the source commit. Then verify from
that commit, using a fresh checkout or `git archive` and with the producing
directory unavailable. For example, after committing, from the repository root:

```sh
saved=/absolute/path/to/new-restoration-check
mkdir "$saved"
git archive HEAD | tar -x -C "$saved"
"$saved/experiments/evidence.sh" unpack \
  "$saved/$record/evidence/$attempt" "$saved/restored"
(cd "$saved/restored" && sha256sum -c "$saved/$record/evidence/$attempt/SHA256SUMS")
```

Only successful restoration from the committed record, followed by pushing
through the existing workflow, establishes durable completion. A local pack
alone does not. With the producing source/toolchain pinned, virtual recordings
can then be remeasured offline without simulation:

```sh
./lightpool-experiment -reduce "$saved/restored" -out "$saved/summary.json"
cmp "$saved/restored/summary.json" "$saved/summary.json"
# For fixed correlated evidence with a saved admission.json:
./lightpool-experiment -admission "$saved/restored" -out "$saved/admission.json"
cmp "$saved/restored/admission.json" "$saved/admission.json"
```

Ordinary samples have no virtual reducer. Incomplete virtual evidence must
still fail reduction; packaging success asserts byte preservation, not a
completed experiment. The existing `run.sh` remains the independent recipe
and refuses cells whose regenerated rows differ. It does not inventory or
package attempts automatically; archive its working attempt directories and
available external sidecars/diagnostics before discarding them.

### Ordinary recorder timing and virtual companions

`experiments/2026-11-20-ordinary-witness/verify.sh NEW_DIRECTORY` restores the
26 original attempts, verifies raw checksums and binary identities, replays
the recorded healthy nodes with the journal decoder pinned in `replay-base`,
and checks the derived virtual and timing tables.
The virtual table includes chain stopping points, exposure and validity.
Interpret counts only in rows with complete status.

From the same record, `run.sh NEW_DIRECTORY` regenerates the 18 virtual
fixtures and checks their deterministic outputs. `healthy.sh NEW_DIRECTORY`
runs four pairs of fresh healthy processes with the observer recorder off
and on, alternating mode order. Each process runs eight loopback nodes and
measures 200 rounds of eight concurrent pings. Each output directory must be
new. Keep the original evidence when comparing regenerated timing samples.

The timing table reports nearest-rank quantiles separately for each process.
Time columns use nanoseconds. Queue depth counts pending input, command and
deferred events after dequeue. UDP read-to-handle begins when the socket read
returns. A TCP frame sample, when present, begins at the final socket read.
Owner time includes effects through dispatch. Recorder time covers preparing
and submitting the input and transition data, through capture of the end
timestamp. Encoding and submitting the end record falls outside that sample.
Only enabled processes supply internal timing. Local replay reproduces the
core inputs, effects and inspection for one node in sequence.

### Ordinary witness on the current baseline

`experiments/2026-11-20-ordinary-witness-current` freezes this study before any
causal result is opened. `base` names the commit whose harness, reducer and
classifier produced and read every attempt. Its protocol code (`core`, `wire`,
`sim` and the ordinary driver) is unchanged from the inspected baseline
`4b45bcd720f0b79f42b4fefcdfbb380482bba0f1`; the commits between the two add
only the harness and offline tools. `run.sh NEW_DIRECTORY` runs, in order:

1. Socket controls. These send real UDP and TCP through the campaign's own
   router, veth, netem and nft code, on separate endpoints.
2. Four off/on pairs of the eight-node healthy timing workload, with one
   recorded observer.
3. The 36 ordinary cells of `WitnessCells`, each in a fresh namespace tree.
4. The 18 current-baseline virtual cells.

A failed control refuses launch. Every attempt is packed, restored and compared
before its raw files are released. The packed record may not exceed 512 MiB.
Ordinary attempts need unprivileged user namespaces.

Each ordinary attempt runs 32 nodes, with the cohorts of `correlated-dense`
at its index: 24 A, one T and seven O. Joins to `n000` are requested every
10 ms. The warm-up boundary is at 15 s. The A-to-T UDP/TCP drop covers
`[15 s, 31.384 s)`, and the horizon is 120 s. Both modes share 100 ms cached
membership samples and 1 Hz pipe clock exchanges. Each node's journal limits
are 1/32 of 5,000,000 records and 2 GiB, and every other file of one attempt
together is capped at 64 MiB. An impaired cell injects its fault only after a
full alive sample from every node at the boundary.

`lightpool-experiment -witness DIR -out FILE` reduces one attempt and prints
one table row. It refuses incomplete, unclean or unfrozen attempts, and those
must fail. For recorded attempts it also requires full alive membership from
every journal through its last step provably before the warm-up boundary.
The classifier orders cross-process facts only by disjoint whole-step
coordinator intervals. Overlap stays inconclusive and is never retuned. Off
attempts report sampled outcomes only.

Current ordinary classification accepts a sender's exact self-attributed
suspicion start or confirmation only after its own failed probe of T in the
same local step. A self-confirmation can establish provenance even when the
sender first suspected T from gossip or unattributed exchange; it uses the
latest recorded target membership, which must still be suspect at the matching
address and incarnation. Sender provenance alone does not establish
acceleration: O must still replace a live deadline with a strictly earlier one.
This classifier correction does not revise frozen records or their pinned
tools; future records must pin the corrected tool revision.

`verify.sh NEW_DIRECTORY` restores every attempt, reduces each ordinary and
virtual attempt twice, compares the outputs byte for byte, and checks the
committed `controls`, `timing`, `ordinary-cells`, `ordinary-timing` and
`virtual-cells` tables. A valid null ends this search. A tooling repair must
rerun the same inputs into a new directory, and both attempts are kept.
