# Queued gossip reply experiment — incomplete build

The candidate and six timing fixtures are implemented and tested. The candidate
remains inert in `reply.patch`; the library protocol is unchanged. `base` pins
the protocol, `tools-base` the fixture tooling, and `PATCHES.sha256` the exact
candidate and ordinary-driver test. The two patches reconstruct `candidate-tree`.
The declaration fixed the campaign before its first original recording.

Build is blocked by repeated external SIGTERM termination of the execution
supervisor, including a one-attempt invocation. The source is not established.
There are 28 retained originals: 27 complete and one with producer finalization
unavailable. The interrupted original retains its trace/report/inputs and exact
absence of summary, producer status, resources and producer exit observation.
Nothing marks it complete. Original recordings were never retried.

The first two interruptions affected separate verification after successful
original recording. Their diagnostics and offline-only recovery identities
remain in `verification/`. The third interrupted producer finalization.
`progress.json` records the build block; it is not a scientific stopping-rule
verdict. No comparator set has been selected and no held-out attempt launched.
`unlaunched.json` retains the remaining 92 development and 234 possible held-out
identities. The paper is an unreviewed, incomplete build snapshot, not a
completed study or improvement claim.

Reproduction commands (run from this record):

- `bash prepare.sh NEW_DIRECTORY` rebuilds both exact producers/reducers.
- `python3 verify.py NEW_DIRECTORY [CELL]` restores committed originals,
  reproduces completed summaries, and checks explicit refusal when interrupted
  producer status is absent. The optional cell supports bounded inspection.
- `bash run.sh CELL NEW_DIRECTORY` independently regenerates a completed
  deterministic row; infrastructure interruptions are not protocol-replayable.
- `python3 results.py SAVED_RECORD NEW_DIRECTORY` derives report JSON/tables
  without simulation. `bash render.sh TOOL_CACHE_DIRECTORY` renders the PDF.

`campaign.py NEW_DIRECTORY` starts the frozen campaign. A reconciled live
campaign can use `--resume --max-attempts N` to launch only unlaunched cells.
Stopped campaigns and unreconciled raw work refuse continuation. Investigate the
external execution failures before reopening this campaign; preserve original
28 and any genuine infrastructure recovery under distinct identities.

All 28 originals passed online packing/restoration checks. Representative Git
restoration was exercised for a complete candidate and the interrupted original;
the remaining committed restorations and independent regeneration are still
verification gaps. No ordinary-network performance, mixed-version deployment,
held-out inference or library adoption is claimed.
