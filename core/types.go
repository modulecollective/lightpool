// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

// Package core owns Lightpool protocol state. Step performs one indivisible
// transition. The caller supplies monotonic time and serializes all calls.
package core

import (
	"errors"
	"github.com/modulecollective/lightpool/wire"
	"net/netip"
	"time"
)

type Instant int64

func (t Instant) Add(d time.Duration) Instant { return t + Instant(d) }

type Seed struct{ State, Stream, Lifetime uint64 }

// Token identifies work in one node lifetime. Drivers must echo it unchanged.
type Token struct{ Lifetime, ID uint64 }
type Timer struct {
	Token
	Generation uint64
}
type Failure uint8

const (
	Success Failure = iota
	LocalFailure
	RemoteFailure
	Timeout
	Rejected
	Canceled
)

type Config struct {
	ProbeInterval, ProbeTimeout, PushPullInterval, GossipInterval                 time.Duration
	StreamTimeout, GossipToDead, ReclaimAfter                                     time.Duration
	IndirectChecks, RetransmitMult, SuspicionMult, SuspicionMaxMult, AwarenessMax int
	GossipNodes, QueueDepth, PacketBudget, MaxPushPull                            int
	DisableTCP                                                                    bool
	// Names with disabled TCP fallback, copied and sorted at construction.
	DisableTCPFor []string
	Label         string
	Protected     bool
	// Application enables explicit requests. With it disabled, data is empty
	// and alive/merge approvals succeed without calling application code.
	Application bool
	Allowed     []netip.Prefix // nil allows all; empty denies all.
}

func DefaultConfig() Config {
	return Config{
		ProbeInterval: time.Second, ProbeTimeout: 500 * time.Millisecond,
		PushPullInterval: 30 * time.Second, GossipInterval: 200 * time.Millisecond,
		StreamTimeout: 10 * time.Second, GossipToDead: 30 * time.Second,
		IndirectChecks: 3, RetransmitMult: 4, SuspicionMult: 4, SuspicionMaxMult: 6, AwarenessMax: 8,
		GossipNodes: 3, QueueDepth: 1024, PacketBudget: 1400, MaxPushPull: 128,
	}
}
func (c Config) Validate() error {
	if c.ProbeInterval < 0 || c.ProbeTimeout <= 0 || (c.ProbeInterval > 0 && c.ProbeTimeout >= c.ProbeInterval) || c.PushPullInterval < 0 || c.GossipInterval < 0 || c.StreamTimeout <= 0 || c.GossipToDead < 0 || c.ReclaimAfter < 0 || c.IndirectChecks < 0 || c.RetransmitMult < 1 || c.SuspicionMult < 1 || c.SuspicionMaxMult < 1 || c.AwarenessMax < 1 || c.GossipNodes < 0 || c.QueueDepth < 1 || c.PacketBudget < 256 || c.PacketBudget > 65507 || c.MaxPushPull < 1 || !wire.ValidText(c.Label, wire.MaxLabel) {
		return errors.New("invalid protocol config")
	}
	for _, p := range c.Allowed {
		if !p.IsValid() {
			return errors.New("invalid admission prefix")
		}
	}
	return nil
}

type Event interface{ event() }
type Start struct{}

func (Start) event() {}

type Shutdown struct{ Command uint64 }

func (Shutdown) event() {}

type Join struct {
	Command uint64
	Peer    wire.Identity
}

func (Join) event() {}

type Ping struct {
	Command uint64
	Peer    wire.Identity
}

func (Ping) event() {}

type Probe struct{ Peer string }

func (Probe) event() {}

type Update struct {
	Command uint64
	Meta    []byte
}

func (Update) event() {}

type Leave struct{ Command uint64 }

func (Leave) event() {}

type Send struct {
	Command             uint64
	Peer                wire.Identity
	Data                []byte
	Reliable, Broadcast bool
}

func (Send) event() {}

type Datagram struct {
	From     netip.AddrPort
	Received Instant
	Messages []wire.Message
}

func (Datagram) event() {}

type Sent struct {
	ID      Token
	Failure Failure
}

func (Sent) event() {}

type Fired struct{ Timer Timer }

func (Fired) event() {}

type Work struct{ ID Token }

func (Work) event() {}

type Accepted struct {
	ID   Token
	Peer netip.AddrPort
}

func (Accepted) event() {}

type Opened struct {
	ID      Token
	Failure Failure
}

func (Opened) event() {}

type Written struct {
	ID      Token
	Failure Failure
}

func (Written) event() {}

type StreamMessage struct {
	ID       Token
	Received Instant
	Messages []wire.Message
}

func (StreamMessage) event() {}

type Closed struct {
	ID      Token
	Failure Failure
}

func (Closed) event() {}

type Response struct {
	ID     Token
	Reject bool
	Data   []byte
}

func (Response) event() {}

type Effect interface{ effect() }
type SendDatagram struct {
	ID       Token
	To       netip.AddrPort
	Messages []wire.Message
}

func (SendDatagram) effect() {}

type OpenStream struct {
	ID       Token
	To       netip.AddrPort
	Deadline Instant
}

func (OpenStream) effect() {}

type WriteStream struct {
	ID       Token
	Messages []wire.Message
	Deadline Instant
}

func (WriteStream) effect() {}

type CloseStream struct{ ID Token }

func (CloseStream) effect() {}

type Arm struct {
	Timer Timer
	At    Instant
}

func (Arm) effect() {}

type Cancel struct{ Timer Timer }

func (Cancel) effect() {}

type Defer struct{ ID Token }

func (Defer) effect() {}

type Result struct {
	Command uint64
	Failure Failure
	RTT     time.Duration
}

func (Result) effect() {}

type RequestKind string

const (
	ApproveAlive RequestKind = "approve-alive"
	ApproveMerge RequestKind = "approve-merge"
	LocalState   RequestKind = "local-state"
	AckPayload   RequestKind = "ack-payload"
)

type Request struct {
	ID      Token
	Kind    RequestKind
	Member  wire.Member
	Members []wire.Member
	Join    bool
}

func (Request) effect() {}

type Notification struct {
	Kind          string
	Member, Other wire.Member
	Data          []byte
	Join          bool
	RTT           time.Duration
}

func (Notification) effect() {}

type Observation struct {
	Kind  string
	Node  string
	Value int64
}

func (Observation) effect() {}

type Stats struct{ Steps, Datagrams, Messages, QueueDrops, Probes, FailedProbes, TCPOnly uint64 }
type Snapshot struct {
	Now                             Instant
	Members                         []wire.Member
	Health, Backlog, PendingStreams int
	Stopped, Left                   bool
	Stats                           Stats
}
