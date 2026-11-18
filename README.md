# Lightpool

Lightpool is a Go 1.25 library for gossip membership and failure detection,
derived from HashiCorp memberlist (MPL-2.0). One serial protocol core runs
under an ordinary UDP/TCP driver or a deterministic message simulator.
Production code and tests use only the Go standard library.

This is a new Lightpool baseline. Its API, framing costs and event ordering
semantics differ from memberlist. The comparison reference is fork commit
`bbb14559d09f347c2563d3a3885e05df8fd36fa4`; mixed-version interoperability
and the old `memberlist` package are removed.

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

`Ping`, `Send`, `Broadcast`, `Update` and `Leave` take contexts.
`OperationError` preserves typed local, remote, timeout and application
failure classes. Canceling a command stops its pending work; an applied
membership update is not rolled back. Update/Leave/Broadcast completion
means the retransmission queue finished or invalidated that broadcast.
UDP sends report local acceptance and reliable sends report completed local
writes. These are not receipts from the peer's application. Join completes
its own merge; the passive peer responds before merging.

An optional `Application` handles explicit data/approval requests outside
the owner loop. Requests can overlap; notifications are ordered. Only the
request's operation waits, and late approvals revalidate membership.
Snapshots can be read reentrantly. Callbacks must honor cancellation;
request contexts end when the response completes or the owning operation ends
(including cancellation and timeout). Canceled queued requests are skipped.
Shutdown cancels callbacks without waiting for user code. Request admission and
notification buffers are bounded; saturated requests are rejected and
notification drops appear in `Traffic`. `ApplicationVersion` is an exact
application-data compatibility identifier, separate from the wire version.

## Core and simulation

`core.New(config, identity, seed)`, `Step(now, event)` and `Snapshot()` are
exported for external drivers. One owner calls Step at a time. Time is a
monotonic integer instant; receive timestamps are separate. Events and
effects are typed data with lifetime and generation tokens. The core owns
protocol state, deadlines, seeded PCG randomness and queue selection; it
performs no I/O, goroutine coordination or application callbacks.

`sim.Run` accepts fixed IP identities, a horizon, an event budget, separate
protocol/fault/scheduling seeds, node service durations, timed actions and
directional fault windows. It models datagram delay, loss, duplication and
reordering; finite receive buffers; ordered streams with finite credits,
backpressure, close and deadlines; pauses, crashes and restarts. Complete
frames larger than transport capacity drain incrementally. Encryption is
rejected, and application behavior requires deterministic response fixtures.
DNS, clock skew and arbitrary application callbacks have no simulator API.

Results carry a code/toolchain/seed manifest, canonical semantic traces and
membership/detection/byte measurements. Pin the actual source revision in
`Scenario.Code`. Budget exhaustion explicitly reports an incomplete run.
See `ExampleRun` (`go test ./sim -run ExampleRun -v`). This is a message model,
not a packet-level TCP stack or calibrated operating-system model.

## Frame contract (wire version 1)

One envelope carries one bounded JSON message batch. UDP requires exactly
one envelope; stream readers consume a header followed by its declared body.
All multibyte integers use network byte order.

| Offset | Size | Meaning |
| --- | --- | --- |
| 0 | 4 | ASCII `LPL!` |
| 4 | 1 | Version `1` |
| 5 | 1 | Flags: bit 0 LZW, bit 1 AES-GCM; other bits rejected |
| 6 | 2 | Label byte length, at most 64 |
| 8 | 4 | Protected body length, at most 8 MiB + 28 |
| 12 | variable | Label, then protected body |

The uncompressed JSON body is an array of 1–256 `wire.Message` records,
encoded by `encoding/json.Marshal` with explicit field names. Exactly one
message variant is permitted per record. Unknown fields, invalid text,
trailing values, oversized arrays and invalid addresses are rejected.
Names/application identifiers and labels are limited to 64 UTF-8 bytes,
metadata/ACK data to 512 bytes, user/state data to 1 MiB, exchanges to
16,384 nodes, and uncompressed bodies to 8 MiB. The aggregate body limit
still applies when individual fields fit. These bounds are fork contracts.
Empty and nil user payloads both encode as `{"user":""}`; `user:null`
does not identify a message variant and is rejected.

Plaintext ends in a four-byte IEEE CRC32 over header, label and body.
Encrypted bodies use standard-library AES-GCM with a random 12-byte nonce
and 16-byte tag; the header and label are authenticated additional data.
Compression is LZW LSB/8, used only when smaller, before protection.
A configured keyring requires encrypted input and output. Datagram budgeting
uses the exact uncompressed upper bound, including base64 user bytes, batch
punctuation, label and protection. The default budget remains 1,400 bytes.

The independent unencrypted ACK fixture (sequence 7, no label) is:

```text
4c504c21010000000000001c5b7b2261636b223a7b2273657175656e6365223a377d7d5da1448745
```

## Checks

```sh
go build ./...
go vet ./...
go mod tidy -diff
go test ./...
go test -race -count=1 ./...
go test ./wire -run '^$' -fuzz FuzzFrame -fuzztime 10s
```

Inherited copyright notices and the MPL-2.0 license remain in the repository.
