# Queued gossip reply experiment — build in progress

The candidate and six timing fixtures are implemented and tested. The candidate
remains inert in `reply.patch`; the library protocol is unchanged. `base` pins
the protocol, `tools-base` the fixture tooling, and `PATCHES.sha256` the exact
candidate and ordinary-driver test. The two patches reconstruct `candidate-tree`.
The declaration fixed the campaign before its first original recording.

The prior execution block is resolved: controlled idle commands reproduced a
Codex 0.172.0 Linux pipe-mode exit-143 failure; PTY execution avoids that process
lifetime path. Run long Codex jobs with **exec_command `tty: true`** and close
stdin (`</dev/null`). The sandbox and scientific inputs remain unchanged.
`execution/` retains source references, controls, cancellation observations,
full restoration of all 28 saved originals, and a byte-identical candidate
regeneration. The exact local spawning-thread retirement was not traced;
source/upstream reproduction supports that mechanism, while local tests verify
the mode-specific failure and working alternative.

`execution/recovery-pty-01.json` reopens only unlaunched declared cells after the
operator requested diagnosis/fix. Original 28 remains incomplete, with missing
producer finalization unavailable. It was never silently rerun or marked
complete. The earlier paper/results remain a labeled 28-original snapshot until
the campaign reaches its accepted stopping point and the report is regenerated.

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
Stopped campaigns and unreconciled raw work refuse automatic continuation.
No ordinary-network performance, mixed-version deployment, completed-study
benefit or library adoption is claimed by this build checkpoint.
