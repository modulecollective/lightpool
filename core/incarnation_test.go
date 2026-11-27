// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"bytes"
	"math"
	"testing"

	"github.com/modulecollective/lightpool/wire"
)

// stepAll steps e and every deferred work item it schedules, returning all effects.
func stepAll(t *testing.T, c *Core, e Event) []Effect {
	t.Helper()
	es := step(t, c, c.now, e)
	all := append([]Effect(nil), es...)
	for _, f := range es {
		if d, ok := f.(Defer); ok {
			all = append(all, stepAll(t, c, Work(d))...)
		}
	}
	return all
}

func exhausted(es []Effect) bool {
	for _, e := range es {
		if o, ok := e.(Observation); ok && o.Kind == "incarnation-exhausted" {
			return true
		}
	}
	return false
}

func aboutSelf(c *Core, kind string, inc uint64) wire.Message {
	a := &wire.Accusation{Node: "a", From: "b", Incarnation: inc}
	switch kind {
	case "suspect":
		return wire.Message{Suspect: a}
	case "dead":
		return wire.Message{Dead: a}
	}
	m := c.nodes["a"].Clone()
	m.Incarnation = inc
	m.Meta = []byte("forged")
	return wire.Message{Alive: &m}
}

func TestRefutationSaturatesAtTheWireCap(t *testing.T) {
	for _, kind := range []string{"suspect", "dead", "alive"} {
		c := testCore(t)
		from := c.nodes["b"].Address
		for i := range 2 {
			es := stepAll(t, c, Datagram{From: from, Received: c.now, Messages: []wire.Message{aboutSelf(c, kind, wire.MaxIncarnation)}})
			if n := c.nodes["a"]; n.Incarnation != wire.MaxIncarnation || n.State != wire.Alive {
				t.Fatalf("%s %d: self at %d state %d", kind, i, n.Incarnation, n.State)
			}
			if !exhausted(es) {
				t.Fatalf("%s %d: no incarnation-exhausted observation: %#v", kind, i, es)
			}
		}
	}
}

func TestRefutationBelowTheCapReachesIt(t *testing.T) {
	for _, kind := range []string{"suspect", "dead", "alive"} {
		c := testCore(t)
		es := stepAll(t, c, Datagram{From: c.nodes["b"].Address, Received: c.now, Messages: []wire.Message{aboutSelf(c, kind, wire.MaxIncarnation-1)}})
		if n := c.nodes["a"]; n.Incarnation != wire.MaxIncarnation || n.State != wire.Alive {
			t.Fatalf("%s: self at %d state %d", kind, n.Incarnation, n.State)
		}
		if exhausted(es) {
			t.Fatalf("%s: exhausted below the cap", kind)
		}
	}
}

func TestExchangeRefutationSaturatesAtTheWireCap(t *testing.T) {
	for _, state := range []wire.State{wire.Suspect, wire.Dead} {
		c := exchangeCore(t, "t")
		m := c.nodes["t"].Clone()
		m.State, m.Incarnation = state, wire.MaxIncarnation
		es := receiveExchange(t, c, c.nodes["a"].Identity, []wire.Member{m}, false)
		if n := c.nodes["t"]; n.Incarnation != wire.MaxIncarnation || n.State != wire.Alive {
			t.Fatalf("state %d: self at %d state %d", state, n.Incarnation, n.State)
		}
		if !exhausted(es) {
			t.Fatalf("state %d: no incarnation-exhausted observation", state)
		}
		receiveExchange(t, c, c.nodes["a"].Identity, []wire.Member{m}, true)
		if n := c.nodes["t"]; n.Incarnation != wire.MaxIncarnation || n.State != wire.Alive {
			t.Fatalf("state %d passive: self at %d state %d", state, n.Incarnation, n.State)
		}
	}
}

func TestUpdateAtTheWireCapFails(t *testing.T) {
	c := testCore(t)
	stepAll(t, c, Datagram{From: c.nodes["b"].Address, Received: c.now, Messages: []wire.Message{aboutSelf(c, "suspect", wire.MaxIncarnation)}})
	before := c.nodes["a"].Clone()
	if before.Incarnation != wire.MaxIncarnation {
		t.Fatal("self not at the cap", before.Incarnation)
	}
	es := step(t, c, c.now, Update{Command: 1, Meta: []byte("next")})
	if len(es) != 1 {
		t.Fatal(es)
	}
	if r, ok := es[0].(Result); !ok || r.Failure != LocalFailure {
		t.Fatal(es)
	}
	if n := c.nodes["a"]; n.Incarnation != before.Incarnation || !bytes.Equal(n.Meta, before.Meta) {
		t.Fatal("failed update changed self", n.Member)
	}
}

func TestAccusationPastTheWireCapIsRejected(t *testing.T) {
	c := testCore(t)
	for _, inc := range []uint64{wire.MaxIncarnation + 1, math.MaxUint64} {
		m := aboutSelf(c, "suspect", inc)
		if _, err := c.Step(c.now, Datagram{From: c.nodes["b"].Address, Received: c.now, Messages: []wire.Message{m}}); err == nil {
			t.Fatal("accepted accusation at", inc)
		}
	}
	if c.nodes["a"].Incarnation != 1 {
		t.Fatal("rejected accusation moved self", c.nodes["a"].Incarnation)
	}
}
