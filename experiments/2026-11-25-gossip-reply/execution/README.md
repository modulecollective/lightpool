# Execution failure diagnosis

Codex 0.172.0 on Linux killed a plain 45-second idle command in pipe mode
with exit 143. The identical PTY command completed, as did both ten-second
controls. This isolates the failure from Lightpool. Cgroup OOM/process-limit
counters were zero. Exact tool exits, completion markers and cancellation
observations are in `pipe-vs-pty.json`.

The version-pinned Codex source arms SIGTERM parent-death signaling in pipe
setup; PTY setup does not. The known creating-thread retirement failure is
reported with a trace and reproduction in
https://github.com/openai/codex/issues/46702. The exact local thread exit was
not traced; the local conclusion is a reproduced pipe-mode failure and a
verified alternative execution path, supported by that mechanism.

For long jobs through Codex, set `exec_command` **`tty: true`** and close stdin:
`python3 campaign.py OUTPUT --resume --max-attempts N </dev/null`.
Poll the returned session normally. The outer tool terminal changes; producer
stdout/stderr stay redirected to their original files. Keep the same sandbox,
protocol patch, inputs and seeds. Do not mask signals, detach jobs, fabricate
producer status or retry an original silently.

An owned PTY cancellation test exited on explicit Ctrl-C, and its descendant
heartbeat stopped. This checks termination, not graceful Python handler cleanup:
the sandbox can tear down its namespace before that handler writes a marker.
The first handler-marker probe did not produce its marker; the observable
cancellation check is the stopped descendant heartbeat.

These diagnostics are execution probes, not protocol campaign recordings.
Original 28 remains incomplete and must still invalidate its development
stratum. A runtime fix does not turn missing measurements into completed data.
