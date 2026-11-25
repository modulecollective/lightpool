# Update-delivery measurement smoke

Six baseline-only 32-node attempts, declared in `DECLARATION` before recording.
All use replicate 1, no overlay, and the producer/reducer pinned in `base` with
the Go version/platform in `toolchain`. Periodic exchange-off preserves join
exchanges and probe TCP fallback. This is a measurement smoke study, not a
candidate evaluation or a retuned research baseline.

All six attempts completed with valid warm-ups and generated 20/20 updates.
Each has 620 fixed update/receiver pairs. Four-second misses in declaration
order are 1, 0, 0, 0, 0, 4. See `cells.tsv` for conditional quantiles,
generation/non-completion counts and attempted/sent traffic costs over both
intervals. Pairs within a cell are dependent; these six attempts support no
tail estimate or exchange-benefit claim. A miss does not mean never delivered.

`evidence/` retains every original input, trace, status, report, summary,
stdout/stderr, observed exit code and resource observation. NDJSON is compressed
losslessly by `../evidence.sh`; `SHA256SUMS` hashes the raw payloads. No original
was replaced by a rerun. `evidence.tsv` inventories raw/stored bytes, original
exit/status and host observations separately from deterministic metrics.
`campaign-costs.json` sums original CLI elapsed times and storage; it excludes
separate verification, packing and rendering time.

From the repository checkout:

```sh
experiments/2026-11-24-update-delivery/run.sh /tmp/new-regeneration
experiments/2026-11-24-update-delivery/verify.sh /tmp/new-restoration
```

`run.sh` rebuilds the pinned tool and refuses changed deterministic rows or
saved-input reduction differences. Host resources and progress diagnostics
may differ. `verify.sh` reads originals from committed Git history, validates
exact file sets and raw checksums, compares offline summary bytes and expected
exit/status, and checks that reduction did not mutate restored files. All
outputs must use new directories; failed working files remain inspectable.

`docs/experiment-v1.md` defines generation, ordered authoritative acceptance,
closed deadlines, fixed lifetimes, invalid/incomplete populations and traffic
costs. Spec/trace v1 bytes are unchanged; new summaries are v3. Earlier absent
metric fields are unavailable, never zeros. Historical evidence can be
reduced to a new file without rewriting originals.

The unreviewed agent-drafted `paper.tex` / `paper.pdf` includes methods,
related work, results, costs, limitations and reproduction. Numerical tables
come from `python3 tables.py`; `paper-costs.tex` comes from `evidence.tsv` and
`campaign-costs.json`. Check generated tables against `paper-tables.tex`.
Render with `render.sh TOOL_CACHE_DIRECTORY`; `document-toolchain` pins the
engine and TeX bundle. No outward release or submission is authorized.
