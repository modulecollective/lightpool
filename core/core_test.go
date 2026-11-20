// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"bytes"
	"fmt"
	"github.com/modulecollective/lightpool/wire"
	"math/rand/v2"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"
)

func identity(name string, port uint16) wire.Identity {
	return wire.Identity{Name: name, Address: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)}
}
func testCore(t testing.TB) *Core {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ProbeInterval = time.Second
	cfg.ProbeTimeout = 100 * time.Millisecond
	cfg.PushPullInterval = 0
	cfg.GossipInterval = 0
	c, err := New(cfg, identity("a", 1), Seed{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	step(t, c, 0, Start{})
	add(t, c, "b", 2)
	return c
}
func step(t testing.TB, c *Core, now Instant, e Event) []Effect {
	t.Helper()
	out, err := c.Step(now, e)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func add(t testing.TB, c *Core, name string, port uint16) {
	t.Helper()
	m := wire.Member{Identity: identity(name, port), Incarnation: 1}
	effects := step(t, c, c.now, Datagram{From: m.Address, Received: c.now, Messages: []wire.Message{{Alive: &m}}})
	for _, e := range effects {
		if w, ok := e.(Defer); ok {
			step(t, c, c.now, Work(w))
		}
	}
}
func timer(t testing.TB, effects []Effect, prefix string) Arm {
	t.Helper()
	for _, e := range effects {
		if a, ok := e.(Arm); ok {
			if prefix == "" || a.At == 100*Instant(time.Millisecond) && prefix == "direct" || a.At == Instant(time.Second) && prefix == "overall" {
				return a
			}
		}
	}
	t.Fatalf("timer %s missing: %#v", prefix, effects)
	return Arm{}
}
func pingSend(t testing.TB, effects []Effect) SendDatagram {
	t.Helper()
	for _, e := range effects {
		if s, ok := e.(SendDatagram); ok && s.Messages[0].Ping != nil {
			return s
		}
	}
	t.Fatal("missing ping")
	return SendDatagram{}
}
func state(c *Core, name string) wire.State {
	for _, m := range c.Snapshot().Members {
		if m.Name == name {
			return m.State
		}
	}
	return wire.Left
}
func countResults(es []Effect, command uint64) int {
	n := 0
	for _, e := range es {
		if r, ok := e.(Result); ok && r.Command == command {
			n++
		}
	}
	return n
}
func TestDirectTimeoutACKMatrix(t *testing.T) {
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		for _, ackFirst := range []bool{true, false} {
			t.Run(fmt.Sprint(offset, "/", ackFirst), func(t *testing.T) {
				c := testCore(t)
				effects := step(t, c, 0, Ping{Command: 1, Peer: identity("b", 2)})
				send := pingSend(t, effects)
				a := timer(t, effects, "direct")
				now := a.At + Instant(offset)
				ack := Datagram{From: send.To, Received: now, Messages: []wire.Message{{Ack: &wire.Ack{Sequence: send.Messages[0].Ping.Sequence}}}}
				var out []Effect
				if now < a.At || ackFirst {
					out = append(out, step(t, c, now, ack)...)
					out = append(out, step(t, c, max(now, a.At), Fired{a.Timer})...)
				} else {
					out = append(out, step(t, c, now, Fired{a.Timer})...)
					out = append(out, step(t, c, now, ack)...)
				}
				if countResults(out, 1) != 1 {
					t.Fatalf("terminal results: %v", out)
				}
				want := Success
				if now >= a.At && !ackFirst {
					want = Timeout
				}
				for _, e := range out {
					if r, ok := e.(Result); ok && r.Command == 1 {
						if r.Failure != want || r.RTT < 0 {
							t.Fatal(r, want)
						}
					}
				}
			})
		}
	}
}
func TestProbeEscalationTCPAndAwareness(t *testing.T) {
	c := testCore(t)
	add(t, c, "c", 3)
	c.health = 2
	effects := step(t, c, 0, Probe{Peer: "b"})
	send := pingSend(t, effects)
	direct := timer(t, effects, "direct")
	var overall Arm
	for _, e := range effects {
		if a, ok := e.(Arm); ok && a.At == 3*Instant(time.Second) {
			overall = a
		}
	}
	if overall.Timer.ID == 0 {
		t.Fatal("overall window did not scale")
	}
	effects = step(t, c, direct.At, Fired{direct.Timer})
	var streamID Token
	indirect := false
	for _, e := range effects {
		switch v := e.(type) {
		case SendDatagram:
			indirect = indirect || v.Messages[0].Indirect != nil
		case OpenStream:
			streamID = v.ID
		}
	}
	if !indirect || streamID.ID == 0 {
		t.Fatal("missing indirect or concurrent fallback", effects)
	}
	step(t, c, direct.At, Opened{ID: streamID})
	step(t, c, direct.At, Written{ID: streamID})
	step(t, c, direct.At, StreamMessage{ID: streamID, Received: direct.At, Messages: []wire.Message{{Ack: &wire.Ack{Sequence: send.Messages[0].Ping.Sequence}}}})
	if len(c.probes) != 1 || c.health != 2 {
		t.Fatal("early TCP ended UDP window")
	}
	step(t, c, overall.At, Fired{overall.Timer})
	if state(c, "b") != wire.Alive || c.health != 1 || c.stats.TCPOnly != 1 {
		t.Fatal(c.Snapshot())
	}
}
func TestOverallNackMatrix(t *testing.T) {
	for _, first := range []bool{true, false} {
		c := testCore(t)
		add(t, c, "c", 3)
		effects := step(t, c, 0, Probe{Peer: "b"})
		send := pingSend(t, effects)
		direct := timer(t, effects, "direct")
		overall := timer(t, effects, "overall")
		step(t, c, direct.At, Fired{direct.Timer})
		nack := Datagram{From: identity("c", 3).Address, Received: overall.At, Messages: []wire.Message{{Nack: &wire.Ack{Sequence: send.Messages[0].Ping.Sequence}}}}
		if first {
			step(t, c, overall.At, nack)
		}
		step(t, c, overall.At, Fired{overall.Timer})
		if !first {
			step(t, c, overall.At, nack)
		}
		want := 1
		if first {
			want = 0
		}
		if c.health != want || state(c, "b") != wire.Suspect {
			t.Fatal(first, c.Snapshot())
		}
	}
}
func TestRTTReceiveBeforeSendCompletion(t *testing.T) {
	c := testCore(t)
	effects := step(t, c, 10, Ping{Command: 7, Peer: identity("b", 2)})
	s := pingSend(t, effects)
	out := step(t, c, 100, Datagram{From: s.To, Received: 20, Messages: []wire.Message{{Ack: &wire.Ack{Sequence: s.Messages[0].Ping.Sequence}}}})
	step(t, c, 101, Sent{ID: s.ID})
	for _, e := range out {
		if r, ok := e.(Result); ok && r.Command == 7 {
			if r.RTT != 10 {
				t.Fatal(r)
			}
		}
	}
	if countResults(out, 7) != 1 {
		t.Fatal(out)
	}
}
func suspicionEvent(c *Core, from string) {
	c.suspect(wire.Accusation{Node: "b", From: from, Incarnation: 1})
}
func TestSuspicionRefutationAndExpiryOrders(t *testing.T) {
	for _, first := range []bool{true, false} {
		c := testCore(t)
		suspicionEvent(c, "a")
		d := c.timers["suspect:b"]
		alive := wire.Member{Identity: identity("b", 2), Incarnation: 2}
		var out []Effect
		if first {
			c.now = d.at
			c.alive(alive)
		}
		out = step(t, c, d.at, Fired{d.timer})
		if !first {
			if state(c, "b") != wire.Dead {
				t.Fatal("expiry failed")
			}
			c.alive(alive)
		}
		if state(c, "b") != wire.Alive || c.nodes["b"].Incarnation != 2 {
			t.Fatal(first, c.Snapshot())
		}
		_ = out
	}
}
func TestSuspicionConfirmationsAndOverdue(t *testing.T) {
	c := testCore(t)
	for j := 3; j <= 5; j++ {
		add(t, c, fmt.Sprint(j), uint16(j))
	}
	suspicionEvent(c, "a")
	s := c.susp["b"]
	original := c.timers["suspect:b"]
	c.now = s.start.Add(s.min + time.Second)
	suspicionEvent(c, "c")
	short := c.timers["suspect:b"]
	suspicionEvent(c, "c")
	if c.timers["suspect:b"].timer != short.timer || s.n != 1 {
		t.Fatal("duplicate confirmation counted")
	}
	suspicionEvent(c, "d")
	due := c.timers["suspect:b"]
	if due.at != c.now || state(c, "b") != wire.Suspect {
		t.Fatal("overdue timer did not defer")
	}
	step(t, c, c.now, Fired{original.timer})
	if state(c, "b") != wire.Suspect {
		t.Fatal("stale expiry acted")
	}
	step(t, c, c.now, Fired{due.timer})
	if state(c, "b") != wire.Dead {
		t.Fatal("due expiry ignored")
	}
	small := testCore(t)
	suspicionEvent(small, "a")
	if small.susp["b"].k != 0 || small.timers["suspect:b"].at != Instant(4*time.Second) {
		t.Fatal("small-cluster timeout changed")
	}
}
func TestMembershipRules(t *testing.T) {
	c := testCore(t)
	n := c.nodes["b"].Clone()
	n.Meta = []byte("new")
	c.alive(n)
	if len(c.nodes["b"].Meta) != 0 {
		t.Fatal("equal incarnation replaced metadata")
	}
	n.Incarnation++
	c.alive(n)
	if !bytes.Equal(c.nodes["b"].Meta, n.Meta) {
		t.Fatal("metadata update ignored")
	}
	c.dead(wire.Accusation{Node: "b", From: "a", Incarnation: 1})
	if state(c, "b") != wire.Alive {
		t.Fatal("old death won")
	}
	c.dead(wire.Accusation{Node: "b", From: "b", Incarnation: 2})
	if state(c, "b") != wire.Left {
		t.Fatal("leave not distinguished")
	}
	c.alive(n)
	if state(c, "b") != wire.Left {
		t.Fatal("alive replay resurrected node")
	}
	n.Incarnation++
	c.alive(n)
	if state(c, "b") != wire.Alive {
		t.Fatal("higher incarnation did not resurrect")
	}
	before := c.nodes["a"].Incarnation
	c.suspect(wire.Accusation{Node: "a", From: "b", Incarnation: before})
	if c.nodes["a"].Incarnation != before+1 || state(c, "a") != wire.Alive || c.health != 1 {
		t.Fatal("self refutation failed")
	}
	n.Address = identity("b", 9).Address
	n.Incarnation++
	c.alive(n)
	if c.nodes["b"].Address == n.Address {
		t.Fatal("name conflict accepted")
	}
	snapshot := c.Snapshot()
	snapshot.Members[0].Meta = []byte("mutated")
	if reflect.DeepEqual(snapshot, c.Snapshot()) {
		t.Fatal("snapshot mutation invisible")
	}
}
func TestQueuePriorityOverflowAndLIFO(t *testing.T) {
	c := testCore(t)
	c.config.QueueDepth = 2
	c.high = nil
	c.low = nil
	c.work = Token{}
	first := wire.Accusation{Node: "b", Incarnation: 1, From: "x"}
	later := wire.Accusation{Node: "b", Incarnation: 1, From: "y"}
	n := wire.Member{Identity: identity("c", 3), Incarnation: 1}
	effects := step(t, c, 0, Datagram{From: n.Address, Messages: []wire.Message{{Suspect: &first}, {Dead: &later}, {Alive: &n}, {Suspect: &first}}})
	var work Token
	for _, e := range effects {
		if w, ok := e.(Defer); ok {
			work = w.ID
		}
	}
	if c.stats.QueueDrops != 1 {
		t.Fatal("overflow not counted")
	}
	step(t, c, 0, Work{ID: work})
	if c.nodes["c"] == nil {
		t.Fatal("alive did not win priority")
	}
	// LIFO: the later dead record runs before the earlier suspicion.
	step(t, c, 0, Work{ID: c.work})
	if state(c, "b") != wire.Dead {
		t.Fatal("not LIFO")
	}
}
func TestShutdownInvalidatesAllWorkAndLifetime(t *testing.T) {
	c := testCore(t)
	effects := step(t, c, 0, Probe{Peer: "b"})
	p := pingSend(t, effects)
	a := timer(t, effects, "direct")
	step(t, c, 0, Join{Command: 9, Peer: identity("b", 2)})
	out := step(t, c, 0, Shutdown{Command: 10})
	if countResults(out, 9) != 1 || countResults(out, 10) != 1 {
		t.Fatal(out)
	}
	for _, event := range []Event{Fired{a.Timer}, Sent{ID: p.ID, Failure: RemoteFailure}, Work{ID: p.ID}} {
		if es := step(t, c, 10, event); len(es) != 0 {
			t.Fatal("stale event acted", es)
		}
	}
	if len(c.timers) > 0 || len(c.streams) > 0 || len(c.probes) > 0 {
		t.Fatal("shutdown retained work")
	}
	cfg := DefaultConfig()
	cfg.ProbeInterval = 0
	cfg.GossipInterval = 0
	other, err := New(cfg, identity("a", 1), Seed{1, 2, 4})
	if err != nil {
		t.Fatal(err)
	}
	es := step(t, other, 0, Start{})
	if len(es) != 1 {
		t.Fatal("push-only schedule absent", es)
	}
	step(t, other, 0, Shutdown{})
	if len(other.timers) != 0 {
		t.Fatal("push-only shutdown failed")
	}
}
func TestPeriodicCoalescing(t *testing.T) {
	c := testCore(t)
	a := c.timers["periodic-probe"]
	step(t, c, Instant(10*time.Second), Fired{a.timer})
	id := c.activeProbe
	if id == 0 {
		t.Fatal("no probe")
	}
	next := c.timers["periodic-probe"]
	if next.at <= c.now || next.at > c.now.Add(time.Second) {
		t.Fatal(next)
	}
	step(t, c, next.at, Fired{next.timer})
	if c.activeProbe != id || !c.pendingProbe {
		t.Fatal("overlapping probe rounds")
	}
	p := c.probes[id]
	step(t, c, c.now, Datagram{From: p.peer.Address, Received: c.now, Messages: []wire.Message{{Ack: &wire.Ack{Sequence: id}}}})
	if c.activeProbe == 0 || c.activeProbe == id || c.pendingProbe {
		t.Fatal("pending tick not coalesced")
	}
}
func TestBroadcastReferenceAndBudget(t *testing.T) {
	c := testCore(t)
	c.broadcasts = nil
	c.broadcastKeys = make(map[string]*broadcast)
	r := rand.New(rand.NewPCG(11, 12))
	type item struct {
		name            string
		id              uint64
		size, transmits int
		message         wire.Message
	}
	var ref []item
	for j := range 1000 {
		if r.IntN(3) != 0 || len(ref) == 0 {
			name := fmt.Sprint(r.IntN(30))
			n := wire.Member{Identity: identity(name, uint16(10+j)), Incarnation: 1}
			n.Meta = bytes.Repeat([]byte{byte(j)}, 1+r.IntN(500))
			m := wire.Message{Alive: &n}
			ref = slices.DeleteFunc(ref, func(i item) bool { return i.name == name })
			c.queue(m, name, 0)
			b := c.broadcasts[len(c.broadcasts)-1]
			ref = append(ref, item{name, b.id, b.size, 0, m})
		} else {
			slices.SortFunc(ref, func(a, b item) int {
				if a.transmits != b.transmits {
					return a.transmits - b.transmits
				}
				if a.size != b.size {
					return b.size - a.size
				}
				if a.id > b.id {
					return -1
				}
				if a.id < b.id {
					return 1
				}
				return 0
			})
			used := frameOverhead(c.config)
			var want []wire.Message
			for idx := range ref {
				i := &ref[idx]
				cost := i.size
				if len(want) > 0 {
					cost++
				}
				if used+cost <= c.config.PacketBudget {
					used += cost
					want = append(want, i.message)
					i.transmits++
				}
			}
			got := c.pack(nil)
			if !reflect.DeepEqual(got, want) {
				t.Fatal("priority/budget mismatch", j)
			}
			ref = slices.DeleteFunc(ref, func(i item) bool { return i.transmits >= retransmitLimit(c.config.RetransmitMult, len(c.nodes)) })
			if len(got) > 0 {
				n, err := wire.Size(got, c.config.Label, c.config.Protected)
				if err != nil || n > 1400 {
					t.Fatal(n, err)
				}
			}
		}
	}
}
func TestBroadcastCompletionAndInvalidation(t *testing.T) {
	c := testCore(t)
	step(t, c, 0, Update{Command: 1, Meta: []byte("first")})
	es := step(t, c, 0, Update{Command: 2, Meta: []byte("second")})
	if countResults(es, 1) != 1 || countResults(es, 2) != 0 {
		t.Fatal(es)
	}
	c.effects = nil
	for range retransmitLimit(c.config.RetransmitMult, len(c.nodes)) {
		c.pack(nil)
	}
	if countResults(c.effects, 2) != 1 {
		t.Fatal(c.effects)
	}
}
func BenchmarkBacklog(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			c := testCore(b)
			c.broadcasts = nil
			for j := range count {
				n := wire.Member{Identity: identity(fmt.Sprint(j), uint16(10+j)), Incarnation: 1}
				c.queue(wire.Message{Alive: &n}, n.Name, 0)
			}
			records := slices.Clone(c.broadcasts)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c.broadcasts = slices.Clone(records)
				for _, r := range records {
					r.transmits = 0
				}
				c.pack(nil)
			}
		})
	}
}

func TestMonotonicTimeAndOwnership(t *testing.T) {
	c := testCore(t)
	step(t, c, 10, Update{Meta: []byte("retained")})
	if _, err := c.Step(9, Start{}); err == nil {
		t.Fatal("time moved backwards")
	}
	data := []byte("original")
	step(t, c, 10, Update{Command: 9, Meta: data})
	data[0] = 'X'
	if string(c.nodes["a"].Meta) != "original" || string(c.broadcastKeys["a"].message.Alive.Meta) != "original" {
		t.Fatal("state aliased command data")
	}
	before := c.nodes["a"].Meta
	s := c.Snapshot()
	for j := range s.Members {
		if s.Members[j].Name == "a" {
			s.Members[j].Meta[0] = 'X'
		}
	}
	if !bytes.Equal(c.nodes["a"].Meta, before) || c.nodes["a"].Meta[0] == 'X' {
		t.Fatal("snapshot aliases membership")
	}
}
func TestHelperSelectionAndReaping(t *testing.T) {
	c := testCore(t)
	for j := 3; j <= 20; j++ {
		add(t, c, fmt.Sprint(j), uint16(j))
	}
	chosen := c.randomNodes(3, func(n *member) bool { return n.Name == "a" || n.Name == "b" })
	if len(chosen) != 3 {
		t.Fatal("helper selection incomplete")
	}
	seen := make(map[string]bool)
	for _, n := range chosen {
		if seen[n.Name] || n.Name == "a" || n.Name == "b" {
			t.Fatal("duplicate/excluded helper")
		}
		seen[n.Name] = true
	}
	c.dead(wire.Accusation{Node: "b", From: "a", Incarnation: 1})
	c.now = Instant(c.config.GossipToDead)
	c.resetNodes()
	if c.nodes["b"] == nil {
		t.Fatal("dead node reaped before gossip window")
	}
	c.now++
	c.resetNodes()
	if c.nodes["b"] != nil {
		t.Fatal("expired dead node retained")
	}
}

func TestOverallACKAndNackOrdering(t *testing.T) {
	for _, kind := range []string{"ack", "nack"} {
		for _, offset := range []Instant{-1, 0, 1} {
			for _, first := range []bool{true, false} {
				c := testCore(t)
				add(t, c, "c", 3)
				es := step(t, c, 0, Probe{Peer: "b"})
				p := pingSend(t, es)
				direct := timer(t, es, "direct")
				overall := timer(t, es, "overall")
				step(t, c, direct.At, Fired{direct.Timer})
				now := overall.At + offset
				m := wire.Message{Ack: &wire.Ack{Sequence: p.Messages[0].Ping.Sequence}}
				from := p.To
				if kind == "nack" {
					m = wire.Message{Nack: &wire.Ack{Sequence: p.Messages[0].Ping.Sequence}}
					from = identity("c", 3).Address
				}
				packet := Datagram{From: from, Received: now, Messages: []wire.Message{m}}
				before := first || offset < 0
				if before {
					step(t, c, now, packet)
					step(t, c, max(now, overall.At), Fired{overall.Timer})
				} else {
					step(t, c, now, Fired{overall.Timer})
					step(t, c, now, packet)
				}
				if kind == "ack" {
					want := wire.Suspect
					if before {
						want = wire.Alive
					}
					if state(c, "b") != want {
						t.Fatal(kind, offset, first, c.Snapshot())
					}
				} else {
					want := 1
					if before {
						want = 0
					}
					if c.health != want || state(c, "b") != wire.Suspect {
						t.Fatal(kind, offset, first, c.Snapshot())
					}
				}
			}
		}
	}
}
func TestConfirmationExpiryOrder(t *testing.T) {
	for _, first := range []bool{true, false} {
		c := testCore(t)
		for j := 3; j <= 5; j++ {
			add(t, c, fmt.Sprint(j), uint16(j))
		}
		c.suspect(wire.Accusation{Node: "b", From: "a", Incarnation: 1})
		old := c.timers["suspect:b"]
		c.now = old.at
		if first {
			c.suspect(wire.Accusation{Node: "b", From: "c", Incarnation: 1})
		}
		step(t, c, old.at, Fired{old.timer})
		if first {
			if state(c, "b") != wire.Suspect {
				t.Fatal("confirmation failed to invalidate old expiry")
			}
			due := c.timers["suspect:b"]
			step(t, c, old.at, Fired{due.timer})
		} else {
			c.suspect(wire.Accusation{Node: "b", From: "c", Incarnation: 1})
		}
		if state(c, "b") != wire.Dead {
			t.Fatal("expiry never completed")
		}
	}
}
func TestCancelCommandClosesOnlyItsOperation(t *testing.T) {
	c := testCore(t)
	o := findOpen(t, step(t, c, 0, Join{Command: 1, Peer: identity("b", 2)}))
	step(t, c, 0, Ping{Command: 2, Peer: identity("b", 2)})
	es := step(t, c, 0, CancelCommand{Command: 1})
	if countResults(es, 1) != 1 || countResults(es, 2) != 0 || c.streams[o.ID.ID] != nil || len(c.probes) != 1 {
		t.Fatal(es)
	}
	if es = step(t, c, 0, Opened{ID: o.ID}); len(es) != 0 {
		t.Fatal("canceled open acted")
	}
}

func TestOversizeInputsFailExplicitly(t *testing.T) {
	c := testCore(t)
	es := step(t, c, 0, Update{Command: 1, Meta: make([]byte, wire.MaxMeta+1)})
	if len(es) != 1 {
		t.Fatal(es)
	}
	if r, ok := es[0].(Result); !ok || r.Failure != LocalFailure {
		t.Fatal(es)
	}
}
