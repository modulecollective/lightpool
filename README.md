<p align="center">
  <img src="docs/assets/stipple.png" alt="A black-and-white stippled illustration of a street lamp illuminating a building" width="100%">
</p>

# Lightpool

Lightpool is a Go 1.25 library for gossip membership and failure detection,
derived from HashiCorp memberlist (MPL-2.0). One serial protocol core runs
under an ordinary UDP/TCP driver or a deterministic message simulator.
Production code, simulation, reporting and tests use only the Go standard library.

Lightpool's API, framing costs and event ordering semantics differ from
memberlist. The comparison reference is fork commit
`bbb14559d09f347c2563d3a3885e05df8fd36fa4`.

## Ordinary execution

```go
cfg := lightpool.DefaultConfig("node-a")
cfg.Bind = netip.MustParseAddrPort("127.0.0.1:7946")
node, err := lightpool.New(cfg)
if err != nil {
    return err
}
defer node.Shutdown()

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_, err = node.Join(ctx, "node-b/127.0.0.1:7947")
```

The module root is package `lightpool`. `DefaultConfig` binds loopback;
wildcard binds require an explicit advertised IP. Join endpoints require
ports; DNS uses a context-bounded standard resolver. `Snapshot` returns
copies of membership, health, backlog and protocol counters. `Traffic`
reports actual encoded byte counts and rejected frames. CIDR admission
uses `Protocol.Allowed`; nil allows all, an empty slice denies all.

`Ping`, `Update` and `Leave` take contexts. `OperationError` preserves
typed local, remote, timeout and rejection failure classes. Canceling a
command stops its pending work; an applied membership update is not rolled
back. Update/Leave completion means the retransmission queue finished or
invalidated that broadcast. Join completes its own merge; the passive peer
responds before merging.

An optional `Notify` function receives membership notifications (join,
update, leave, suspect, refute, conflict) in order, outside the owner loop.
It can read snapshots and call the API reentrantly. Its queue is bounded;
drops appear in `Traffic`. Shutdown cancels its context without waiting for
it. Lightpool carries no application data: there are no user messages, ACK
payloads, push/pull state or approval callbacks.

## Core and simulation

`core.New(config, identity, seed)`, `Step(now, event)` and `Snapshot()` are
exported for external drivers. One owner calls Step at a time. Time is a
monotonic integer instant; receive timestamps are separate. Events and
effects are typed data with lifetime and generation tokens. The core owns
protocol state, deadlines, seeded PCG randomness and queue selection; it
performs no I/O, goroutine coordination or callbacks.

`sim.Run` accepts fixed IP identities, a horizon, an event budget, separate
protocol/fault/scheduling seeds, node service durations, timed actions and
directional fault windows. It models datagram delay, loss, duplication and
reordering; finite receive buffers; ordered streams with finite credits,
backpressure, close and deadlines; pauses, crashes and restarts. Complete
frames larger than transport capacity drain incrementally. Connect, read and
write deadlines advance during pauses; fully accepted frames can wait for
receiver processing. Encryption is rejected. DNS, clock skew and callbacks
have no simulator API.

Results carry a code/toolchain/seed manifest, canonical semantic traces and
membership/detection/byte measurements. Pin the actual source revision in
`Scenario.Code`. Budget exhaustion explicitly reports an incomplete run.
See `ExampleRun` (`go test ./sim -run ExampleRun -v`). This is a message model,
not a packet-level TCP stack or calibrated operating-system model.

## Record and report a simulation

```sh
go build -o lightpool-sim ./cmd/lightpool-sim
./lightpool-sim -scenario examples/sim/pause.json -out pause.ndjson
go run ./cmd/lightpool-report -trace pause.ndjson
go run ./cmd/lightpool-report -trace pause.ndjson -format json
```

The producer never overwrites an existing file. An unstamped or modified
build requires `-revision <explicit-source-identity>`; `go run` may need
that flag. Progress goes to stderr; trace output is NDJSON only. Scenario
files use the `sim.Scenario` field names and integer nanosecond durations;
the CLI bounds input at 16 MiB and rejects unknown fields. Event-budget
exhaustion writes an incomplete end and exits with an error. An I/O failure
leaves an inspectable prefix and stops execution.

`sim.Export(scenario, writer, options)` uses the same event loop as
`sim.Run`, without retaining the in-memory semantic trace or final membership
matrix. The caller owns and closes the writer; errors propagate. The opt-in
`(*core.Core).EnableInspection` journal records authoritative membership changes,
probe facts and timer lifecycle separately from protocol effects.
See the [trace format](docs/trace-v1.md) and [report definitions](docs/report-v1.md).

`cmd/lightpool-experiment` records fixed baseline pilot cases, windowed metrics
and replay inputs. For example, `lightpool-experiment -case threshold -replicate
1 -out results-threshold-1` retains the trace, scenario, report and summary in a
new directory. See the [pilot contract](docs/experiment-v1.md) for implemented
cases, exact populations, independent datagram loss and the opt-in Linux process
probe. Resource limits and invalid warm-up remain explicit failed results.

## Frame contract

One envelope carries one bounded binary message batch. UDP requires exactly
one envelope; stream readers consume a header followed by its declared body.
All fixed-width integers use network byte order.

| Offset | Size | Meaning |
| --- | --- | --- |
| 0 | 4 | ASCII `LPL!` |
| 4 | 1 | Version: must be `2` |
| 5 | 1 | Flags: bit 1 AES-GCM; other bits rejected |
| 6 | 2 | Label byte length, at most 64 |
| 8 | 4 | Protected body length, at most 8 MiB + 28 |
| 12 | variable | Label, then protected body |

The body is 1–256 messages concatenated, consuming the body exactly. Each
message is one tag byte followed by its fields in fixed order:

| Tag | Message | Fields |
| --- | --- | --- |
| 1 | ping | sequence, target text, source identity |
| 2 | indirect | sequence, target identity, source identity |
| 3 | ack | sequence |
| 4 | nack | sequence |
| 5 | alive | member |
| 6 | suspect | node text, incarnation, accuser text |
| 7 | dead | node text, incarnation, accuser text |
| 8 | exchange | member count, members |

A sequence is 8 bytes. Incarnations, lengths and counts are unsigned
varints (`encoding/binary` uvarint) in their shortest form. Text and bytes
are a length followed by the bytes; text is UTF-8. An identity is name text,
address and metadata bytes. An address is one length byte (4 or 16), the IP
and a 2-byte port. A member is an identity, incarnation and one state byte
(0 alive, 1 suspect, 2 dead, 3 left). Every value has exactly one encoding:
overlong varints, unknown tags, out-of-range states and address lengths,
trailing bytes and invalid identities are rejected, and an accepted body
re-encodes to the same bytes. Names and labels are limited to 64 bytes,
metadata to 512 bytes, exchanges to 16,384 members and bodies to 8 MiB.
Lengths and counts are checked against these limits and against the bytes
remaining before anything is allocated. These bounds are fork contracts.

Plaintext ends in a four-byte IEEE CRC32 over header, label and body.
Encrypted bodies use standard-library AES-GCM with a random 12-byte nonce
and 16-byte tag; the header and label are authenticated additional data.
A configured keyring requires encrypted input and output. Datagram budgeting
uses the exact frame size, computed without encoding, including label and
protection. The default budget remains 1,400 bytes. A suspect record costs
about 20 bytes and an alive record with 24 bytes of metadata about 44, so a
datagram carries roughly 69 or 31 of them.

The independent unencrypted ACK fixture (sequence 7, no label) is:

```text
4c504c21020000000000000d030000000000000007ce2ece0b
```

## Checks

```sh
go build ./...
go vet ./...
go mod tidy -diff
go test ./...
go test -race -count=1 ./...
go test ./wire -run '^$' -fuzz FuzzFrame -fuzztime 10s
go test ./wire -run '^$' -fuzz FuzzBody -fuzztime 10s
```

Inherited copyright notices and the MPL-2.0 license remain in the repository.
The banner and logo images are © James Phillips, licensed under CC BY 4.0.
