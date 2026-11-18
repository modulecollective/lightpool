# Lightpool trace v1

UTF-8 NDJSON, one atomic simulator event per newline. Records are ordered;
a newline commits a record. Maximum line size is 64 MiB. No payload bytes
are exported. All integer values (including the nested manifest) are decimal
strings; format `version` alone is the small JSON number `1`. Times are
nonnegative nanoseconds. Identifiers, incarnations, seeds and counters must
never be parsed as JavaScript Number. Display times may be converted after
subtracting an exact origin. Arrays preserve producer order.

* `header`: `format: "lightpool-trace"`, `version: 1`, `manifest` (normalized
  simulator manifest, including source Code, runtime, platform, full scenario,
  per-node seeds and prepared faults), `nodes` (name, address), `initial`
  (per-node process state; running false, lifetime "0", empty membership).
* `event`: `seq` (consecutive from "1"), `time`, `id` (scheduler event ID),
  `kind` (action.*, core.*, datagram, chunk, drain, connect and transport
  expiry kinds), `actor` (optional for transport-only events), `lifetime`,
  `changes`, `traffic`. All arrays are present, even when empty. All changes
  and immediate transport decisions in a record apply atomically.
* `end`: `seq` (last event sequence), `time` (last processed time), `reason`,
  `complete`, `nodes` (compact process/core/traffic measurements), `totals`.

Change `kind` is member.upsert, member.remove, process, summary, probe,
suspicion, refutation or timer. `node` is the observer/actor; `subject` is
optional. member.upsert carries `member` (name, address, incarnation, state,
optional base64 metadata and application); state is alive/suspect/dead/left.
member.remove carries subject. Absence means unknown. `process` carries
`process` (node, lifetime, running, pausedUntil); a process change phase is start, pause or crash; start/restart clears that
observer's members, probes and timers. `summary` carries health, backlog,
pendingStreams, stopped, left and decimal-string stats. `probe` carries id,
phase (direct/indirect/relay/finished), target, helpers and outcome.
`suspicion` and `refutation` carry subject and optional outcome.
`timer` carries id, generation, phase (arm/cancel/fire), deadline, purpose,
subject and operation. A stale core.Fired event is not a timer.fire change.

Traffic entries carry `id` (independent diagnostic counter), `transport`
(datagram/stream), `stage` (attempt/sent/arrival/received/handle/drop), `from`, `to`,
`bytes`, `messages` (stable wire variant names), optional `detail` (drop or
failure reason), `received` (network arrival time for handler events).
One datagram attempt may have multiple arrivals (duplication). Stream IDs
identify modeled message writes, not TCP packets; chunk arrivals preserve
actual byte accounting. Payload contents are omitted, not absent traffic.
Endpoint names must refer to header nodes (empty to means unresolved route).
Diagnostic IDs consume neither scheduler IDs nor protocol randomness.

A valid end with complete true alone permits a completed badge. A valid
prefix without end, or with complete false (event budget), is incomplete.
Ignore an unterminated trailing line; reject malformed complete lines,
invalid references, non-monotone time, sequence gaps, a second header/end,
records after end, and unknown format versions. Errors name the line.
No browser formula reconstructs protocol behavior: reduce recorded changes.
