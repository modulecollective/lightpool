# Silent stream fault

Declared before the campaign. Nothing below may change once `evidence.tsv`
exists.

## Question

Correlated-suspicion acceleration witnesses appear in the virtual companions
of `2026-11-20-ordinary-witness-current` (five of fifteen impaired cells),
while nine ordinary cells with a real routed A-to-T drop recorded 187
eligible episodes and none. The simulator's directed fault refuses streams:
an open fails after one latency with `RemoteFailure` and an established
stream closes at once. An nftables drop is silent, so an open or a write
hangs until its deadline. Do the witnesses survive a silent stream fault?

## Arms

Both arms use the producer built from `base` with default protocol values.
No candidate patch and no overlay.

| Arm | Cases | A→T fault |
| --- | --- | --- |
| `control` | `correlated-dense` and its `-healthy`, `-latency-100us`, `-latency-10ms`, `-service-100us`, `-service-1ms` cases | `Drop` + `StreamFailure` (refusing) |
| `silent` | the five impaired cases with `-silent` | `Drop` + `StreamDrop` (silent) |

`StreamDrop` (`sim.Fault`, added in `base`) discards A→T stream traffic
without notice. An A→T open, or a T→A open whose handshake reply is lost,
times out at its deadline with no accept. An A→T chunk on an established
stream is dropped and the write runs to its deadline. Nothing is
retransmitted after the heal; real TCP retries a SYN at about 1, 3 and 7 s.
Recorded witnesses form 1.7 to 3.4 s into the 16.4 s fault window, far from
the heal. A silent cell shares its seeds, cohorts, window and protocol with
the control cell of the same index, but one differing event reshuffles the
rest of its schedule, so it is not the control minus one effect.

## Cells

`run.sh` holds the grid in launch order; 93 cells.

| Block | Cases | Indices | Cells |
| --- | --- | --- | ---: |
| Fixed control | the six recorded cases | 1–3 | 18 |
| Held-out control | `correlated-dense` | 201–230 | 30 |
| Held-out silent | `correlated-dense-silent` | 201–230 | 30 |
| Fixed silent | the five `-silent` cases | 1–3 | 15 |

Launch order: fixed control, then held-out control and silent interleaved
by index, then fixed silent. No record had used `correlated-dense` above
index 3. Stop launching once packed evidence passes 512 MiB, or after the
fixed control block if the control gate fails, and list the unlaunched
cells in `unlaunched.tsv`. No retries, no added replicates, no
outcome-driven extension.

Disclosure: while building `base`, one fixed silent cell
(`correlated-dense-silent` index 3) was run once as a disposable smoke test
of the producer and `-admission` path. It classified `complete` with 3
witnesses. The rule below was fixed in the run's design before that, and
the cell is recorded again here like every other.

## Metrics

Every cell is classified with the unchanged `-admission` pass, saved as
`admission.json` in its attempt. `cells.tsv` carries the
`2026-11-20-ordinary-witness-current/virtual-cells.tsv` columns
(`failedProbes` … `stop`) and `controlGate`. A **witness cell** is a cell
whose admission `status` is `complete` with `accelerationWitnesses > 0`.
Cells are the unit: episodes inside one cell share originating failures
and are not independent.

## Decision rule

Gate first: the 18 fixed control cells must reproduce `virtual-cells.tsv`
of the 2026-11-20 record in the classifier columns. If any differs, stop
and report that; the silent arm is not read.

Held-out pairs are matched by index. A pair with an invalid or incomplete
cell in either arm is dropped and listed. Let R and S be the witness-cell
counts of the control and silent arms over the valid pairs, b the pairs
where only the control has a witness and c the pairs where only the silent
arm has one.

- **Underpowered.** R < 5, or more than five pairs dropped. Report the
  table; no claim.
- **Eliminated.** R ≥ 5 and no silent cell, held-out or fixed, is a
  witness cell (0 of 45). The refused stream is the explanation and the
  virtual positives were an artifact of the fault model.
- **Reduced.** S > 0 and the one-sided exact sign test on the discordant
  pairs gives P(Bin(b + c, ½) ≥ b) ≤ 0.05. The refusal inflates the rate;
  witnesses still occur under a silent fault.
- **Not explained.** Anything else with R ≥ 5. Witnesses survive a silent
  fault at a rate this campaign cannot tell from the refusing one.

The fixed silent cells are reported cell by cell beside their controls and
count only toward the zero in "Eliminated". Failed probes, local
suspicions, eligible episodes and harmful episodes are tabulated for both
arms as description, not as a test.

## Reproduce

```sh
experiments/2026-11-26-silent-stream-fault/run.sh NEW_DIR [CELL ...]
```

Rebuilds the producer from the pinned commit with the pinned toolchain,
records the named cells (all by default) and refuses any whose row differs
from `cells.tsv`. The original attempts are restored with
`experiments/evidence-store.sh fetch 2026-11-26-silent-stream-fault`.

## Campaign log

Appended after launch; the sections above are as declared.

- Launch 1 (`run.sh --retain`, from `74650b8`) recorded the first 13 fixed
  control cells and stopped at `correlated-dense-service-100us-r1` with
  `control does not reproduce the 2026-11-20 record`. Its `lateTimers` was
  1296 against 1269 at `62c746a`. The seven gate columns (status, failed
  probes, local suspicions, eligible episodes, witnesses, harmful
  episodes, stopping point) all matched. `run.sh` had compared all 14
  columns, which is stricter than the rule above, and it stopped at that
  cell rather than at the end of the block. No silent cell had run.
- `960ae78` narrowed the comparison to the seven columns and let `--retain`
  continue with named never-attempted cells. That attempt's `controlGate`
  reads `same` in `cells.tsv`, as the corrected `run.sh` regenerates it.
- Launch 2 recorded the other 5 fixed control cells (all `same`), the 60
  held-out cells and `correlated-dense-silent-r1`. It then hit the 512 MiB
  cap at 540,401,824 packed bytes: cells packed to about 6.8 MB, not the
  4 MB estimated. The other 14 fixed silent cells are in
  `unlaunched.tsv`. They count only toward the zero in "Eliminated", which
  ten held-out silent witness cells already exclude, so the verdict does
  not depend on them.
- Index 229 is an invalid warm-up in both arms and is dropped.
- 79 attempts, all retained, none retried.
