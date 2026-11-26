# Pre-expiry direct probe

Declared before the first held-out cell was recorded. Nothing below may
change once `evidence.tsv` exists.

## Question

When a lone suspicion runs to its maximum, the observer declares the member
dead without consulting its own path to it. An observer that missed a
refutation holds a stale incarnation, and above 32 nodes the default
exchange interval (60 s) is longer than the lone maximum (43.3 s at 64
nodes), so a reachable member can be declared dead. Does one direct probe
sent one `ProbeTimeout` before the deadline remove those deaths, at no cost
in crash detection or traffic, and better than any single timeout retune?

## Arms

| Arm | Source | Protocol |
| --- | --- | --- |
| `base` | `base` | defaults |
| `probe` | `base` + `probe.patch` | defaults |
| `smax8` | `base` | `overlays/smax8.json`: `SuspicionMaxMult` 8 |
| `smax12` | `base` | `overlays/smax12.json`: `SuspicionMaxMult` 12 |
| `sm5` | `base` | `overlays/sm5.json`: `SuspicionMult` 5 |

`probe.patch` is `git diff <base> -- core wire` of the candidate commit,
which was dropped from the branch. It keeps the suspicion deadline and arms
a second timer, `expiry-probe:<name>`, `ProbeTimeout` before it. When that
fires and the member is still suspect, it sends one datagram, a ping and
the held accusation, to the member's address only. An ACK from that
address restarts the suspicion at the current confirmation level; silence
lets the unchanged deadline declare dead. No indirect leg, TCP fallback or
retry; health, probe statistics and results are untouched.

## Cells

`run.sh` holds the grid in launch order; 208 cells in total.

| Fixture | Replicates | Arms | Cells |
| --- | --- | --- | ---: |
| `correlated-dense-64-aged` | 101–120 | base, probe, smax12 | 60 |
| `correlated-dense-64-aged` | 101–110 | smax8, sm5 | 20 |
| `correlated-dense-64-aged-healthy` | 101–103 | base, probe | 6 |
| `crash` | 101–106 | all five | 30 |
| `small-double-crash` | 101–110 | all five | 50 |
| `healthy`, `loss`, `correlated`, `partial-partition`, `interval`, `threshold`, `correlated-dense` | 1–3 | base, probe | 42 |

Indices 1–23 were used in the design screen and are development; 101 and
up are held out. The last row is regression and null control: for
`healthy`, `loss`, `correlated` and `partial-partition` the probe trace
must equal baseline after the header line. `correlated-dense` index 2
should show its one death removed; that is a development check, not
evidence.

Stop launching once packed evidence passes 2 GiB or campaign time passes
6 hours, and list the unlaunched cells in `unlaunched.tsv`. No retries, no
added replicates, no outcome-driven extension. Invalid warm-ups and
failures are retained and make their comparison inconclusive for that
replicate.

## Metrics

All are read from the summary each cell's recording wrote, which the
baseline binary must reproduce offline. Columns are in `cells.tsv`.

- **W**, `workingPathExpiryDeaths`: episodes with `cause` =
  `local-suspicion-expiry`, a running target (`targetRunning`, not stopped,
  not left), neither endpoint paused, and no entry in
  `incidentConfiguredFaults`. `runningSuspectNS` and `runningDeadNS` are the
  suspect and dead pair-nanoseconds of the `running-target-exclusions`
  non-alive population, reported beside it.
- **L**, crash detection: for each crash, `latencies.maxNS`, the largest
  completed per-observer dead latency, with `p50NS` and completed and
  eligible counts. A cell's L is the mean of its crashes' `maxNS`; an arm's
  L is the mean of its cells' L over the compared replicates. Full
  dissemination (`fullStatus`) is reported, not compared.
- **C**, traffic: `trafficCost.measured.attempted` datagram count, datagram
  bytes and stream bytes.
- `runningDeadDeclarations` counts every dead declaration about a running,
  unpaused target, whatever its cause.

Compare replicate means over the same replicate indices. Pairs within a
run are dependent and are never pooled as samples.

## Rejection rules

Reject the candidate if any of these holds on held-out replicates:

1. *No benefit.* Baseline has at least 3 W deaths on the 20 aged
   replicates and the candidate has more than half as many. Fewer than 3
   baseline deaths is inconclusive, not a pass.
2. *A retune does as well.* Some single arm among `smax8`, `smax12` and
   `sm5` has W no greater than the candidate's on their common aged
   replicates, and mean L no more than 5% above the candidate's in both
   `crash` and `small-double-crash`.
3. *Harm.* Candidate mean L exceeds baseline's by more than 5% in `crash`
   or `small-double-crash`; or mean datagram count exceeds baseline's by
   more than 2% in any fixture; or the candidate has a dead declaration
   about a running, unpaused target in any cell where baseline has none;
   or a null control differs.

Surviving all three admits an adoption decision in a later run. It claims
nothing about ordinary networks, 128 nodes or steady-state rates. With
about six expected baseline deaths this is a screen: report the count of
replicates where one arm has a death and the other does not, and no
interval.

## Reproduce

```sh
experiments/2026-11-26-expiry-probe/run.sh NEW_DIR [CELL ...]
```

Rebuilds `base` and `base` + `probe.patch` from the pinned commit with the
pinned toolchain, records the named cells (all by default) and refuses any
whose row differs from `cells.tsv`. The original attempts are restored with
`experiments/evidence-store.sh fetch 2026-11-26-expiry-probe`.
