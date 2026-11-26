# Fixed Lightpool baseline pilot v1

Lightpool treats suspect/dead exchange entries as unattributed suspicion.
Exchange alone cannot confirm an existing timer or gossip a new accusation;
the first known origin seeds attribution without shortening. Finite expiry,
direct buddy notification and ordinary distinct-origin shortening remain.
This deliberately changes the inherited merge behavior. Each experiment record
measures its pinned protocol and tools; results from different baselines are
not interchangeable.

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

The basic cases use 32 nodes, default `core.Config`, 1 ms one-way latency,
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
| `partial-partition` | The same four A and target T as `correlated`, with both A→T and T→A blocked for 16.384 seconds. All processes keep running and all other links stay healthy. |
| `partial-partition-healthy` | No faults; retains the paired A/T grouping and the same 27 O→T healthy pairs. |

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

The partial-partition cases select A at fault ranks 0–3 and T at rank 4;
the other 27 nodes are O. Eight directed faults, in sorted A order with
A→T then T→A, drop datagrams and fail streams over [15 s,31.384 s).
A↔O, O↔T, A↔A and O↔O remain healthy. Indirect probes through O remain
enabled. Datagram faults apply at send time, so traffic admitted before
the cut can arrive during it. Streams check faults at open, progress and
arrival. Healing permits new traffic but does not restore closed streams
or guarantee membership recovery.

Only the 27 predeclared O→T pairs enter `healthy-pair-exclusions` in these
two cases: 2,835 pair-seconds over [15 s,120 s] per complete run, regardless
of membership beliefs. `running-target-exclusions` retains all 992 ordered
pairs (104,160 pair-seconds), including partition exclusions on A↔T.
Neither case has pause `FP-analogue` labels. Zero exclusions are valid;
invalid warm-up and unresolved recovery remain explicit. These virtual
fixtures do not qualify for the fixed correlated `-admission` screen.

```sh
./lightpool-experiment -case partial-partition -replicate 1 -out partial-r1
./lightpool-experiment -reduce partial-r1 -out partial-r1-reduced.json
cmp partial-r1/summary.json partial-r1-reduced.json
./lightpool-experiment -case partial-partition-healthy -replicate 1 -out partial-healthy-r1
experiments/2026-11-21-partial-partition/run.sh new-partial-baseline
```

The baseline record contains both cases at paired indices 1, 2 and 3, with
original evidence and separate host resources. Its recipe rebuilds the
pinned source and checks deterministic rows; it makes no candidate or
ordinary-networking claim.

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

`summary.json` uses `lightpool-pilot-summary/v2`; integer counters, times, lifetimes,
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
| `evidence/<attempt>/` | Raw-byte `SHA256SUMS` in Git; payloads, with NDJSON compressed, restored by `evidence-store.sh fetch`. |
| `evidence.tsv` | Attempt provenance, observed exit/status and raw/stored byte totals. |
| `evidence-assets.tsv` | One release asset per attempt: tag, asset name, bytes and SHA-256. |

`experiments/` has its own `go.mod`, so the library's module zip excludes the
records and `go get` never downloads them. A record's Go tests import only the
standard library; one that wants library code is a new decision, not a
`replace` line added in passing.

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

A pin (`base` and every other `*base` file) keeps the hash it was recorded
with, because that string is an identity in traces, rows and digests.
Scripts hand a pin to Git through `experiments/revision.sh PIN`, which
prints the commit it now names: the mapped hash if
`experiments/rewritten-commits.tsv` lists it after a history rewrite, and
otherwise the pin itself.

## Retaining and restoring original evidence

Retain every attempt in a declared campaign under
`experiments/<id>/evidence/<attempt>/`, including controls, negative results,
invalid warm-ups, recording failures and retries. Each retry or fresh-process
repeat has a new attempt ID. Disposable development probes and compiled
binaries are excluded. Payloads never enter Git: each packed attempt is
kept as one release asset on the source repository, and Git keeps the
attempt's `SHA256SUMS` and the record's `evidence-assets.tsv`. An attempt
whose archive would near GitHub's 2 GiB asset limit needs a new storage
decision, not selective omission of failures. There is no automatic pruning.

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
(tested with 5.2), GNU coreutils and gzip, an existing destination parent and a new
destination outside the source. Inputs must be flat regular files, with basenames starting
with an ASCII letter/digit followed by letters, digits, dots, underscores or
hyphens. Symlinks, nested directories, reserved `SHA256SUMS`, duplicate manifest
names and compressed-name collisions (such as `trace.ndjson` alongside
`trace.ndjson.gz`) are refused. Neither command changes its source or overwrites
an existing destination. On an I/O or checksum failure the command exits
nonzero; a partial destination remains inspectable. Retry into a new directory.

Before creating an unpack destination, the helper counts every expanded byte
with a checked streaming decompression (including concatenated gzip members)
and measures sidecars. It checks free space on the physical destination parent
with `stat -f`, rounding each payload to the filesystem allocation unit and
adding a fixed 256 MiB reserve. Insufficient space, failed sizing or invalid
space results refuse before destination creation. The diagnostic names the
destination, available bytes, allocated payload requirement and reserve.
Success is silent. This is an early refusal, not a reservation: concurrent
writers, quotas, inode exhaustion and later I/O failures can still stop a
restore. Keep failed working copies for inspection; never scavenge unrelated
scratch or caches to make room.

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

`.gitignore` keeps everything under `experiments/*/evidence/*/` except
`SHA256SUMS` out of Git. `experiments/evidence-store.sh` moves the payloads:

- `publish RECORD BATCH` archives every attempt directory that holds payloads
  and has no `evidence-assets.tsv` row as a deterministic `<attempt>.tar.gz`
  (`tar --sort=name --mtime=@0`, `gzip -n -6`), uploads it to the draft
  release `evidence-<record>-<batch>`, and appends the row only when the
  store reports the same size and SHA-256. It then publishes the release.
  A rerun resumes. Every evidence tag points at
  `1801ef926af5ee05d80617bb531f6dfcd24a71e1`, the last commit before any
  payload entered history, because `git clone` fetches every tag. Needs an
  authenticated `gh` with write access.
- `fetch RECORD [ATTEMPT...]` downloads without credentials, checks size and
  SHA-256 against the manifest, refuses unsafe or nested archive members,
  requires the archived `SHA256SUMS` to equal the committed one and moves
  the payloads into `experiments/<record>/evidence/<attempt>/`. It refuses
  an attempt directory that already holds payloads.
- `check` confirms through the API that every manifest row names a published
  asset of the same size and digest, and that every attempt whose
  `SHA256SUMS` is committed has a row, payloads present or not. The pre-push
  hook runs it.

Two checksums guard two things: the manifest's `sha256` proves the archive
arrived intact, and the committed `SHA256SUMS` proves the restored files are
the original attempt. Recipes that restore originals take `SHA256SUMS` from
Git and the payloads from the working tree, and say to run `fetch` first
when the payloads are absent.

Publish, then commit the manifest, `SHA256SUMS` and inventory. Then verify
from a fresh clone of that commit, with the producing directory unavailable,
so the payloads can only come from the store. For example, from the
repository root:

```sh
# Choose an existing writable disk-backed parent; check this filesystem.
set -euo pipefail
scratch_parent=/absolute/path/on/writable-disk
df -h "$scratch_parent"
experiments/evidence-store.sh publish "${record#experiments/}" 1
git add "$record" && git commit -m "Retain $attempt"
saved=$(mktemp -d "$scratch_parent/evidence-check.XXXXXX")
git clone --quiet --no-local . "$saved/clone"
evidence_revision=$(git rev-parse HEAD)
reducer_revision=$evidence_revision # Use the actual pinned reducer revision.
(cd "$saved/clone" && experiments/evidence-store.sh fetch "${record#experiments/}" "$attempt")
printf 'evidence=%s\nreducer=%s\n' "$evidence_revision" "$reducer_revision" > "$saved/verification.txt"
cp "$saved/clone/$record/evidence/$attempt/SHA256SUMS" "$saved/input-hashes.sha256"
experiments/evidence.sh unpack \
  "$saved/clone/$record/evidence/$attempt" "$saved/restored"
(cd "$saved/restored" && sha256sum -c "$saved/input-hashes.sha256")
```

Only successful restoration through the store, followed by pushing through
the existing workflow, establishes durable completion. A local pack or an
upload alone does not. With the producing source/toolchain pinned, virtual recordings
can then be remeasured offline without simulation:

```sh
set -e
./lightpool-experiment -reduce "$saved/restored" -out "$saved/summary.json"
cmp "$saved/restored/summary.json" "$saved/summary.json"
# For fixed correlated evidence with a saved admission.json:
./lightpool-experiment -admission "$saved/restored" -out "$saved/admission.json"
cmp "$saved/restored/admission.json" "$saved/admission.json"
# After every required check succeeds, remove only this owned working copy.
# Retain verification.txt, input-hashes.sha256, outputs and diagnostic logs.
(cd "$saved/restored" && sha256sum -c "$saved/input-hashes.sha256")
rm -r -- "$saved/restored" "$saved/clone"
```

For optional checks, restore one attempt at a time with the checked-out helper,
retain the revision/input-hash manifest and capture command diagnostics. Build
the reducer from its recorded revision. A failed reduction or comparison keeps
the raw copy and partial outputs. Do not run cleanup after a failed check;
original committed evidence is never disposable. Older pinned recipes still
invoke their historical helper and do not acquire this guard retroactively.

The fixed non-alive remeasurement recipe uses the helper from its own checkout,
`evidence-base` only for the original attempts' committed `SHA256SUMS`
(payloads come from the tree after `evidence-store.sh fetch`), and `reducer-base` for the rebuilt
research tool. Its interface remains `run.sh NEW_DIRECTORY`. For example:

```sh
scratch_parent=/absolute/path/on/writable-disk
df -h "$scratch_parent"
verification=$(mktemp -d "$scratch_parent/non-alive-check.XXXXXX")
bash experiments/2026-11-21-non-alive-belief-duration/run.sh "$verification/result"
```

The recipe puts source, executable, Go build temporary files and the current
attempt under `result/work/`. It releases that attempt's archived source, raw
restoration and plain summary only after restoration, exact input-set/hash and
saved-status checks, the expected reducer exit (including recorded nonzero
exits), old-field compatibility, compressed-summary reproduction and unchanged
post-reduction inputs all pass. Summaries, inventory, input hashes and logs
remain. Build scratch is removed only after all 24 attempts and the final
committed manifest reproduce. The first verification failure retains its
scratch and partial summary, stops further archive/restore/reduction work and
marks later entries unmeasured. Build, I/O, cleanup and interruption failures
return nonzero with a retained-path diagnostic. Shared caches remain under
their existing owner. The explicit output filesystem stays in control; the
recipe does not fall back to another disk. Host-specific scratch diagnostics
live outside the deterministic `SHA256SUMS` contract.

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

## Time without an accepted alive state (summary v2)

New summaries use `lightpool-pilot-summary/v2`. Saved specifications still use
`lightpool-pilot/v1`, and trace v1 and `report.json` are unchanged. All previous
summary fields retain their definitions. An older summary without `nonAlive`
has no duration measurement; its absence never means zero. Strict readers
must accept the new output schema. Pinned historical tools keep their original
output; reduce saved evidence to a **new** file to obtain v2.

`nonAlive` contains `pairs`, `populations`, and `episodes`. Enrollment freezes
all non-self observer/target process lifetimes immediately before the first
event at or after W (`spec.warmupNS`). The all-members-alive warm-up check still
invalidates the entire attempt, without selecting successful pairs. The
intended pairs remain in output with zero exposure even if recording ends
before W. Such lifetime fields describe the last observed prefix, not a claim
that enrollment was reached. H is `spec.horizonNS`; elapsed time covers [W,H).
Events at W and H are processed in time, sequence, then change-index order.
An alive at H completes an episode; a departure at H is zero-time censored.
A quiet suffix in complete evidence contributes through H; an incomplete
prefix contributes through the earlier of its last valid event and H, with
incomplete status and `prefix` censoring even when it reaches H. Changes after
H cannot complete an episode or interrupt its measured exposure.

Each `pairs` row has the existing pair identity (`observer`,
`observerLifetime`, `subject`, `targetLifetime`), fixed `labels`,
`interruptions`, and the totals below. `populations` rows have `label` and
the same totals summed over those fixed pairs. Labels retain the existing
join keys: `running-target-exclusions`, the two pause-case `FP` analogues,
and exactly the predeclared `healthy-pair-exclusions`. Duration fields measure
more than dead exclusions. Healthy declarations must have neither a configured
path impairment in either direction nor an overlapping configured endpoint
pause. Empty healthy populations remain explicit. Populations overlap: never
sum them or treat pairs as independent statistical replicates. Raw running
populations under partition/loss are descriptive, not all false diagnoses.

| Totals field | Meaning |
| --- | --- |
| `enrolledPairs`, `nominalPairNS` | Intended count and count × (H−W), independent of beliefs and recording completion. |
| `observedPairNS`, `observedNS` | Equal: eligible time with both original endpoint lifetimes running, not stopped/left. Pauses retain time; lifecycle interruptions end it permanently. |
| `aliveNS`, `suspectNS`, `deadNS`, `leftNS`, `absentNS` | Five mutually exclusive state times summing to observed exposure. Absence is unknown membership, never imputed suspect/dead. Membership left is distinct from process leave. |
| `notAliveNS`, `suspectDeadNS` | Suspect + dead + left + absent; and suspect + dead, respectively. |
| `notAliveFraction`, `suspectDeadFraction` | Numerator / observed exposure, deterministic nine-decimal strings; null for zero exposure. |
| `starts`, `carryIns`, `completed` | In-window episode starts (including zero-time), pre-W open rows, and accepted-alive completions, respectively. |
| `censored`, `interrupted` | Sorted `{reason,count}` arrays: episode censoring and lifetime interruption counts by endpoint role/reason. An episode retains its first censoring cause; a pair can eventually record both endpoint interruptions. |
| `pairsEverNonAlive`, `pairsEverDead` | Pair counts, including zero-time transitions. |
| `durations` | Existing nearest-rank `p50NS`/`p95NS`/`maxNS`, `eligible` = starts, `completed` = completed non-carry-in sample size. Empty samples have null quantiles and an explicit reason. Censor counts and carry-ins remain adjacent. |

`interruptions` lists `{role,reason,at}`. Roles are `observer` and `target`;
reasons are `crash`, `restart`, `shutdown`, `leave`, or
`inactive-at-enrollment`. A later lifetime cannot resume exposure or complete
an old episode. No membership belief removes exposure. Counts are per frozen
pair lifetime and role, so later interruptions of the same role add no count.

Each episode has the pair identity and labels, `onset`, `cause`, `carryIn`,
`observationStart`, nullable `firstDead`, ordered `segments`, `end`, `status`,
`censorReason`, nullable `durationNS`, and the eight state/observed-time fields.
A departure from alive starts an episode at suspect, dead, left, or removal
(`cause: removed`). A missing warm-up membership has synthetic cause
`missing-warmup-membership`. Repeated state upserts, confirmations and higher
incarnations do not reset onset. Non-alive transitions split segments, and
removal preserves the episode as absent. Only accepted alive in the enrolled
lifetimes completes it. Carry-in rows retain pre-W onset/first-dead references
but start observed segments at W; they invalidate warm-up and never contribute
to duration quantiles. A missing-membership carry-in has a window onset when
no transition was recorded. An inactive pair at enrollment retains a zero-time diagnostic carry-in
when non-alive and records its interruption.

A segment is `{state,start,end,observedNS}`. Segment times sum to episode
`observedNS`, and episode totals sum to pair `notAliveNS`. `durationNS` is
accepted-alive minus onset only for completed, non-carry-in episodes; a genuine
zero duration is `"0"`. Censored or carry-in duration is null, never the horizon.
Censor reasons are `observer-`/`target-` plus lifecycle reason, `horizon`, or
`prefix`. A completed episode stays completed after a later interruption.
These are conditional completion summaries, not survival estimates.

Bounds are `{kind,timeNS,event,changeIndex}`. `kind: change` cites the existing
`event` reference (time and sequence) plus the zero-based decimal-string index
in that event's `changes`. `window`, `horizon` and `prefix` are synthetic and
have null `changeIndex`; their event is the preceding/last valid event (or
null when none exists). They do not fabricate membership changes or extend
existing report references with a new meaning. All integer times, counts and
indices are exact decimal strings; booleans remain JSON booleans.

The primary descriptive result is healthy-pair `notAliveNS / observedPairNS`,
with all four components and completion/censor counts. It is operational time
without an accepted alive state, not an exact detector-mistake duration. It
does not change dead recovery, crash detection, mechanism admission or protocol
behavior, and it does not by itself admit a candidate experiment.

## Fixed metadata-update delivery (summary v3)

Six additional fixed cases use the suffix `-latency-10ms` on the 32-node
names below. They change only uniform latency to 10 ms; service time stays
zero, seeds and workload remain identical. The 128-node names do not accept
this suffix.

`lightpool-experiment -case delivery-{32,128}-loss-{0,10,30}-exchange-{on,off}
-replicate INDEX -out NEW_DIRECTORY` selects one of twelve finite workloads.
Expand the braces to one case name. These cases start n000 onward with empty
metadata, join to n000 at 10 ms intervals, use 1 ms latency, zero service time,
1 MiB transport buffers and the existing event/trace caps. The protocol is
`core.DefaultConfig()`; `exchange-off` changes only `PushPullInterval=0`.
Join exchanges still run. `-protocol` overlays remain available for retuning.

The warm-up check requires full alive membership (self included) immediately
before the first event at or after 50 s. It freezes all lifetimes. Twenty
sources n000 through n019 update once each at 50,54,...,126 s, carrying the
unique eight-byte ASCII tokens `upd-0000` through `upd-0019`. Exact actions
are saved in `scenario.json`, not duplicated in the v1 spec. The horizon is
130 s. Every directed non-self datagram link has the named independent loss
from 0 through 130 s inclusive (fault end 130 s + 1 ns); streams have no
independent datagram loss. Protocol, fault and schedule seeds use the existing
separate derivation. Equal directed send ordinals pair draws, not necessarily
realized losses when traffic differs.

`updateDelivery` in `lightpool-pilot-summary/v3` freezes 20*(N-1) scheduled
source/receiver pairs: 620 or 2540. Suspect, dead, removed or left membership
beliefs never shrink that population. This bounded reducer requires unique
single-source eight-byte ASCII updates, no initial token collisions, zero
service time, pre-warm-up joins and no pauses, crashes, restarts or departures.
Unsupported delivery workloads fail explicitly; old cases retain their rules.

For each update, `action` cites its scheduled `action.update` event. Generation
is a matching authoritative self `member.upsert` in the subsequent source
`core.update` at the scheduled time and frozen lifetime. `generation` cites
event time, sequence and zero-based change index; `incarnation` is numeric.
Ambiguous origins fail reduction. An observed core update without its matching
self upsert fails generation. Missing execution is incomplete until its time
is closed by a later event or complete trace; complete missing generation is
failure. All scheduled updates stay in the denominator.

A pair completes on the first authoritative upsert at its frozen receiver
whose member name and metadata match the source/token and whose incarnation
is at least the generated incarnation. Gossip, probe piggybacking and stream
exchange all count; higher-incarnation refutations retaining the token count.
Alive state is not required. Receipt or queue completion is insufficient.
Acceptance must follow generation in sequence/change order and lie in the
closed [generation,generation+4 s] interval. Duplicates count once. Retained
references and integer-nanosecond latency identify the selected change.
Acceptance after four seconds cannot rewrite a miss.

A miss requires evidence covering the entire deadline: a complete horizon
covering it or a prefix reaching a strictly later event time. A prefix ending
exactly at a deadline leaves unresolved pairs incomplete because another
same-time event could still accept. Observed completions remain visible in
incomplete attempts. Generation failures have separate pair statuses and
never count as delivery misses. For valid warm-up, delivered, missed,
generation-failed and incomplete counts partition the fixed eligible pairs.
Invalid/unreached warm-up reports nominal counts with invalid/unavailable
status, zero eligible observations and null rates. Other cases explicitly say
`not-applicable`; absent fields in historical v1/v2 summaries are unavailable,
never measured zeros.

Per-update rows and receiver rows use stable schedule/name order and frozen
lifetime identities. Time to all receivers is the maximum pair latency only
when every eligible receiver completes; otherwise it is null with an explicit
non-completion status. Pair latency and all-receiver distributions use
nearest-rank p50/p95/max with eligible/completed counts, conditional on
completion within four seconds. Empty completion sets have null quantiles.
A four-second miss does not mean never delivered. Pairs in a replicate are
not independent statistical samples; six smoke attempts support no tail or
improvement inference.

`traffic` retains its existing measured-window sent-byte meanings and drop
diagnostics. The additional `trafficCost` envelope covers both the closed
[50 s,130 s] interval (`measured`) and [0,observed-through] (`wholeRun`). Each
interval labels its nominal bounds, observed-through time and coverage
(`complete`, `observed-prefix`, or `unreached`). Other cases use their own
warm-up/horizon for the measured interval. Incomplete costs describe only
observed records, never silently a full window.

Each interval separates `attempted` from `sent`: datagram counts and encoded
frame bytes, plus encoded stream-write bytes at `attempt`, versus actual
framed datagrams and stream chunks at `sent`. Zero-byte stream opens add no
bytes. Failed/partial writes may make offered and sent costs differ. Encoding
and budget rejections remain diagnostics, not claimed network bytes. Lost
sends still cost bytes; drop and duplicate-arrival records add no send cost.
`ipv4UDPBytes` is datagram frame bytes + 28*datagram count separately for
attempted and sent stages. Stream framed bytes remain alongside, never a TCP
wire-cost estimate. Whole-run sent bytes are checked against complete trace
end totals (`endTotalsMatch`); no end-total assertion is made for prefixes.
All traffic is included, not only packets carrying updates.

Saved specs and traces stay v1 with unchanged canonical serialization. Use
`lightpool-experiment -reduce SAVED_DIRECTORY -out NEW_SUMMARY.json` to write
v3 measurements without simulating or rewriting saved evidence, summaries or
digests. Existing exclusive output and incomplete/nonzero behavior apply.
The baseline smoke record is `experiments/2026-11-24-update-delivery/`; its
`run.sh NEW_DIRECTORY` rebuilds the pinned tooling, runs exactly the six
32-node replicate-1 cases, checks offline replay and refuses changed rows.
Its `verify.sh NEW_DIRECTORY` restores every original from committed evidence
and checks exact payload checksums, reductions, exit/status and table rows.
The record also includes an unreviewed draft report and pinned render command.
