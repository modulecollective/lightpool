package core

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/wire"
)

func exchangeCore(t *testing.T, self string) *Core {
	t.Helper()
	cfg := DefaultConfig()
	cfg.PushPullInterval, cfg.GossipInterval = 0, 0
	cfg.ProbeInterval = time.Second
	c, err := New(cfg, identity(self, uint16(self[0])), Seed{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	step(t, c, 0, Start{})
	for _, name := range []string{"a", "b", "c", "o", "t"} {
		if name != self {
			add(t, c, name, uint16(name[0]))
		}
	}
	c.EnableInspection()
	return c
}

// Drive both production merge entry paths, including the responder's write barrier.
func receiveExchange(t *testing.T, c *Core, peer wire.Identity, members []wire.Member, passive bool) []Effect {
	t.Helper()
	var id Token
	if passive {
		id = c.token()
		step(t, c, c.now, Accepted{ID: id, Peer: peer.Address})
	} else {
		id = findOpen(t, step(t, c, c.now, Join{Command: 88, Peer: peer})).ID
		step(t, c, c.now, Opened{ID: id})
		step(t, c, c.now, Written{ID: id})
	}
	es := step(t, c, c.now, StreamMessage{ID: id, Messages: []wire.Message{{Exchange: &wire.Exchange{Members: members}}}})
	if passive {
		es = step(t, c, c.now, Written{ID: id})
	}
	if c.streams[id.ID] != nil {
		t.Fatal("exchange not completed")
	}
	return es
}

func deliverAccusation(t *testing.T, c *Core, from string) {
	t.Helper()
	es := step(t, c, c.now, Datagram{From: identity(from, uint16(from[0])).Address, Messages: []wire.Message{{Suspect: &wire.Accusation{Node: "t", From: from, Incarnation: c.nodes["t"].Incarnation}}}})
	drainWork(t, c, es)
}

func drainWork(t *testing.T, c *Core, es []Effect) {
	t.Helper()
	for _, e := range es {
		if d, ok := e.(Defer); ok {
			drainWork(t, c, step(t, c, c.now, Work(d)))
		}
	}
}

func failedProbe(t *testing.T, c *Core) {
	t.Helper()
	es := step(t, c, c.now, Probe{Peer: "t"})
	var overall Arm
	for _, e := range es {
		if a, ok := e.(Arm); ok && a.At == c.now.Add(c.config.ProbeInterval*time.Duration(c.health+1)) {
			overall = a
		}
	}
	if overall.Timer.ID == 0 {
		t.Fatal("missing overall probe deadline")
	}
	step(t, c, overall.At, Fired{overall.Timer})
	if state(c, "t") != wire.Suspect {
		t.Fatal("failed probe did not suspect target")
	}
}

func assertNoSuspectGossip(t *testing.T, c *Core, target string) {
	t.Helper()
	// A real probe of an alive peer packs the broadcast queue into its datagram.
	es := step(t, c, c.now, Probe{Peer: "a"})
	found := false
	for _, e := range es {
		if s, ok := e.(SendDatagram); ok {
			found = true
			for _, m := range s.Messages {
				if err := m.Validate(); err != nil {
					t.Fatal(err)
				}
				if m.Suspect != nil && m.Suspect.Node == target {
					t.Fatal("exchange invented outgoing accusation", m.Suspect)
				}
				if m.Alive != nil && m.Alive.Name == target {
					t.Fatal("stale alive record escaped invalidation")
				}
			}
		}
	}
	if !found {
		t.Fatal("no outgoing datagram exercised")
	}
}

func TestExchangeExistingSuspicion(t *testing.T) {
	for _, passive := range []bool{false, true} {
		for _, remoteState := range []wire.State{wire.Suspect, wire.Dead} {
			t.Run(fmt.Sprint(passive, "/", remoteState), func(t *testing.T) {
				c := exchangeCore(t, "o")
				deliverAccusation(t, c, "a")
				original, s, queued := c.timers["suspect:t"], c.susp["t"], c.broadcastKeys["t"]
				start := s.start
				for _, peer := range []string{"b", "c", "b"} {
					m := c.nodes["t"].Clone()
					m.State = remoteState
					receiveExchange(t, c, c.nodes[peer].Identity, []wire.Member{m}, passive)
					if c.timers["suspect:t"] != original || s.start != start || s.n != 0 || len(s.confirmations) != 1 || !s.confirmations["a"] || c.broadcastKeys["t"] != queued {
						t.Fatal("exchange changed existing timer, reporters or queued origin")
					}
					for _, j := range c.Inspection() {
						if j.Kind == "suspicion" {
							t.Fatal("exchange recorded confirmation", j)
						}
					}
				}
			})
		}
	}
}

func TestExchangeFirstAttribution(t *testing.T) {
	for _, small := range []bool{false, true} {
		for _, localFirst := range []bool{false, true} {
			for _, remoteState := range []wire.State{wire.Suspect, wire.Dead} {
				t.Run(fmt.Sprint(small, "/", localFirst, "/", remoteState), func(t *testing.T) {
					c := exchangeCore(t, "o")
					if small {
						c.config.SuspicionMult = 2
					}
					unrelated := c.broadcastKeys["b"]
					// An invalidated queued command must still complete exactly once.
					c.broadcastKeys["t"].command = 77
					m := c.nodes["t"].Clone()
					m.State = remoteState
					es := receiveExchange(t, c, c.nodes["b"].Identity, []wire.Member{m}, true)
					s, original := c.susp["t"], c.timers["suspect:t"]
					if s == nil || len(s.confirmations) != 0 || s.n != 0 || original.at <= c.now || c.broadcastKeys["t"] != nil || c.broadcastKeys["b"] != unrelated || countResults(es, 77) != 1 {
						t.Fatal("invalid exchange-only suspicion or queue invalidation")
					}
					started := false
					for _, j := range c.Inspection() {
						started = started || j.Kind == "suspicion" && j.Outcome == "started:"
					}
					if !started {
						t.Fatal("missing unattributed start journal")
					}
					assertNoSuspectGossip(t, c, "t")
					first, second := "a", "o"
					if localFirst {
						first, second = "o", "a"
						failedProbe(t, c)
					} else {
						deliverAccusation(t, c, first)
					}
					seeded := c.timers["suspect:t"]
					if seeded.at != original.at || seeded.timer == original.timer || s.n != 0 || len(s.confirmations) != 1 || !s.confirmations[first] || s.start != 0 || c.broadcastKeys["t"].message.Suspect.From != first {
						t.Fatal("first named reporter shortened or reset exchange timer")
					}
					confirmed := false
					for _, j := range c.Inspection() {
						confirmed = confirmed || j.Kind == "suspicion" && j.Outcome == "confirmation:"+first
					}
					if !confirmed {
						t.Fatal("missing first attribution journal")
					}
					deliverAccusation(t, c, first)
					if c.timers["suspect:t"] != seeded {
						t.Fatal("replay rearmed timer")
					}
					if localFirst {
						deliverAccusation(t, c, second)
					} else {
						failedProbe(t, c)
					}
					if small {
						if s.k != 0 || s.n != 0 || c.timers["suspect:t"] != seeded {
							t.Fatal("small cluster confirmation shortened")
						}
					} else if s.n != 1 || !s.confirmations[second] || c.timers["suspect:t"].at >= seeded.at {
						t.Fatal("distinct genuine origin did not shorten")
					}
				})
			}
		}
	}
}

func TestExchangeMultiNodeCounterexample(t *testing.T) {
	a, b, c, o := exchangeCore(t, "a"), exchangeCore(t, "b"), exchangeCore(t, "c"), exchangeCore(t, "o")
	failedProbe(t, a)
	for i, pair := range [][2]*Core{{a, b}, {b, c}, {c, o}, {o, b}, {b, o}} {
		source, dest := pair[0], pair[1]
		dest.now = max(source.now, dest.now)
		receiveExchange(t, dest, source.nodes[source.self].Identity, source.Snapshot().Members, i%2 == 0)
		if dest.susp["t"] == nil || len(dest.susp["t"].confirmations) != 0 {
			t.Fatal("exchange manufactured a reporter", dest.self)
		}
	}
	original := o.timers["suspect:t"]
	for _, n := range []*Core{b, c, o} {
		assertNoSuspectGossip(t, n, "t")
	}
	// A's actual outgoing accusation remains attributed to A after relaying.
	es := step(t, a, a.now, Probe{Peer: "o"})
	var accusation *wire.Accusation
	for _, e := range es {
		if s, ok := e.(SendDatagram); ok {
			for _, m := range s.Messages {
				if m.Suspect != nil && m.Suspect.Node == "t" {
					accusation = m.Suspect
				}
			}
		}
	}
	if accusation == nil || accusation.From != "a" {
		t.Fatal("failed probe origin not disseminated")
	}
	for range 2 {
		drainWork(t, o, step(t, o, o.now, Datagram{From: a.nodes["a"].Address, Messages: []wire.Message{{Suspect: accusation}}}))
	}
	if o.susp["t"].n != 0 || len(o.susp["t"].confirmations) != 1 || o.timers["suspect:t"].at != original.at {
		t.Fatal("one origin became multiple reporters")
	}
	failedProbe(t, c)
	o.now = c.now
	deliverAccusation(t, o, "c")
	if o.susp["t"].n != 1 || o.timers["suspect:t"].at >= original.at {
		t.Fatal("positive distinct failed-probe control did not shorten")
	}
}

func TestExchangeRecoveryExpiryAndBuddy(t *testing.T) {
	for _, recover := range []bool{false, true} {
		c := exchangeCore(t, "o")
		m := c.nodes["t"].Clone()
		m.State = wire.Dead
		receiveExchange(t, c, c.nodes["b"].Identity, []wire.Member{m}, false)
		original := c.timers["suspect:t"]
		c.now = Instant(time.Second)
		receiveExchange(t, c, c.nodes["c"].Identity, []wire.Member{m}, true)
		if c.timers["suspect:t"] != original {
			t.Fatal("repeated exchange postponed expiry")
		}
		target := exchangeCore(t, "t")
		if recover {
			// Suspected-target probes retain the direct buddy accusation but do not queue it.
			es := step(t, c, c.now, Probe{Peer: "t"})
			buddy := pingSend(t, es)
			found := false
			for _, m := range buddy.Messages {
				found = found || m.Suspect != nil && m.Suspect.From == "o"
			}
			if !found || buddy.To != target.nodes["t"].Address || c.broadcastKeys["t"] != nil {
				t.Fatal("buddy notification boundary changed")
			}
			drainWork(t, target, step(t, target, c.now, Datagram{From: c.nodes["o"].Address, Messages: buddy.Messages}))
			if target.nodes["t"].Incarnation != 2 {
				t.Fatal("target did not refute buddy")
			}
			receiveExchange(t, c, target.nodes["t"].Identity, target.Snapshot().Members, false)
			if state(c, "t") != wire.Alive || c.susp["t"] != nil {
				t.Fatal("refutation did not clear suspicion")
			}
		}
		step(t, c, original.at, Fired{original.timer})
		if recover {
			if state(c, "t") != wire.Alive {
				t.Fatal("stale expiry defeated refutation")
			}
		} else if state(c, "t") != wire.Dead || c.broadcastKeys["t"].message.Dead.From != "o" {
			t.Fatal("exchange-only expiry failed to disseminate dead")
		} else {
			found := false
			for _, e := range step(t, c, c.now, Probe{Peer: "a"}) {
				if send, ok := e.(SendDatagram); ok {
					for _, m := range send.Messages {
						found = found || m.Dead != nil && m.Dead.Node == "t" && m.Dead.From == "o"
					}
				}
			}
			if !found {
				t.Fatal("expired exchange-only suspicion did not reach outgoing gossip")
			}
		}
	}
	// Self suspicion through exchange also refutes, with no accusation on the wire.
	c := exchangeCore(t, "t")
	m := c.nodes["t"].Clone()
	m.State = wire.Suspect
	receiveExchange(t, c, c.nodes["a"].Identity, []wire.Member{m}, true)
	if c.nodes["t"].Incarnation != 2 {
		t.Fatal("exchange self-refutation lost")
	}
}

func TestExchangeInspectionNeutral(t *testing.T) {
	a, b := exchangeCore(t, "o"), exchangeCore(t, "o")
	a.inspect = false
	events := []Event{Accepted{ID: Token{Lifetime: a.life, ID: 1}, Peer: a.nodes["b"].Address}, StreamMessage{ID: Token{Lifetime: a.life, ID: 1}, Messages: []wire.Message{{Exchange: &wire.Exchange{Members: []wire.Member{{Identity: a.nodes["t"].Identity, Incarnation: 1, State: wire.Suspect}}}}}}, Written{ID: Token{Lifetime: a.life, ID: 1}}}
	for _, e := range events {
		if !reflect.DeepEqual(step(t, a, 0, e), step(t, b, 0, e)) {
			t.Fatal("inspection changed exchange effects")
		}
	}
	for _, from := range []string{"a", "c"} {
		deliverAccusation(t, a, from)
		deliverAccusation(t, b, from)
		if !reflect.DeepEqual(a.effects, b.effects) || !reflect.DeepEqual(a.Snapshot(), b.Snapshot()) {
			t.Fatal("inspection changed attribution effects")
		}
	}
}

func TestExchangeMembershipBoundaries(t *testing.T) {
	c := exchangeCore(t, "o")
	m := c.nodes["t"].Clone()
	m.State = wire.Suspect
	m.Incarnation = 0
	receiveExchange(t, c, c.nodes["a"].Identity, []wire.Member{m}, false)
	if state(c, "t") != wire.Alive {
		t.Fatal("stale incarnation started suspicion")
	}
	m.Identity = identity("unknown", 999)
	m.Incarnation = 1
	receiveExchange(t, c, c.nodes["a"].Identity, []wire.Member{m}, true)
	if c.nodes["unknown"] != nil {
		t.Fatal("unknown suspect member admitted")
	}
	m = c.nodes["t"].Clone()
	m.State = wire.Suspect
	receiveExchange(t, c, c.nodes["a"].Identity, []wire.Member{m}, false)
	original := c.timers["suspect:t"]
	// First attribution after an overdue deadline still uses the original start.
	c.now = original.at + 1
	deliverAccusation(t, c, "a")
	if c.timers["suspect:t"].at != c.now || c.susp["t"].start != 0 || c.susp["t"].n != 0 {
		t.Fatal("overdue attribution reset start or failed to clamp")
	}
	step(t, c, c.now, Fired{original.timer})
	if state(c, "t") != wire.Suspect {
		t.Fatal("old timer generation fired")
	}
	m.State = wire.Alive
	m.Incarnation++
	receiveExchange(t, c, c.nodes["b"].Identity, []wire.Member{m}, true)
	if state(c, "t") != wire.Alive || c.susp["t"] != nil {
		t.Fatal("new alive failed to clear suspicion/reporters")
	}
	m.State = wire.Left
	receiveExchange(t, c, c.nodes["b"].Identity, []wire.Member{m}, false)
	if state(c, "t") != wire.Left || c.broadcastKeys["t"].message.Dead.From != "t" {
		t.Fatal("exchange leave lost self attribution")
	}
}
