package core

import (
	"reflect"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/wire"
)

func TestInspectionMembershipAndOwnership(t *testing.T) {
	c := testCore(t)
	c.EnableInspection()
	observed := map[string]wire.Member{}
	for _, m := range c.Snapshot().Members {
		observed[m.Name] = m
	}
	check := func() {
		t.Helper()
		for _, j := range c.Inspection() {
			if j.Kind == "member.upsert" {
				observed[j.Member.Name] = j.Member.Clone()
			}
			if j.Kind == "member.remove" {
				delete(observed, j.Subject)
			}
		}
		want := map[string]wire.Member{}
		for _, m := range c.Snapshot().Members {
			want[m.Name] = m
		}
		if !reflect.DeepEqual(observed, want) {
			t.Fatalf("journal != snapshot\ngot %#v\nwant %#v", observed, want)
		}
	}
	// Exercise direct mutations at event boundaries as well as Step. Each call
	// must publish silent updates that the application notification API omits.
	apply := func(f func()) { c.journal = nil; f(); check() }
	apply(func() { c.suspect(wire.Accusation{Node: "b", From: "a", Incarnation: 1}) })
	apply(func() { c.alive(wire.Member{Identity: identity("b", 2), Incarnation: 2}) })
	apply(func() { c.alive(wire.Member{Identity: identity("b", 2), Incarnation: 3}) })
	apply(func() { c.refute(10) })
	apply(func() { c.update(Update{Meta: []byte("owned")}) })
	j := c.Inspection()
	j[0].Member.Meta[0] = 'X'
	if string(c.nodes["a"].Meta) != "owned" || string(c.Inspection()[0].Member.Meta) != "owned" {
		t.Fatal("journal aliases core or another read")
	}
	apply(func() { c.dead(wire.Accusation{Node: "b", From: "a", Incarnation: 3}) })
	c.now = Instant(c.config.GossipToDead + time.Second)
	apply(c.resetNodes)
	if _, ok := observed["b"]; ok {
		t.Fatal("removal was not recorded")
	}
	step(t, c, c.now, Update{Meta: []byte("new")})
	check()
	step(t, c, c.now, CancelCommand{})
	check()
	if len(c.Inspection()) != 0 {
		t.Fatal("journal retained previous step")
	}
}
func TestInspectionDoesNotChangeEffects(t *testing.T) {
	a, b := testCore(t), testCore(t)
	b.EnableInspection()
	events := []Event{Probe{Peer: "b"}, Update{Meta: []byte("test")}, Leave{Command: 9}, Shutdown{Command: 10}}
	for i, e := range events {
		ea := step(t, a, Instant(i), e)
		eb := step(t, b, Instant(i), e)
		if !reflect.DeepEqual(ea, eb) || !reflect.DeepEqual(a.Snapshot(), b.Snapshot()) {
			t.Fatalf("inspection changed transition %d", i)
		}
	}
}
func TestInspectionTimerCancellationAndStaleFire(t *testing.T) {
	c := testCore(t)
	c.EnableInspection()
	es := step(t, c, 0, Probe{Peer: "b"})
	direct := timer(t, es, "direct")
	found := false
	for _, j := range c.Inspection() {
		found = found || j.Kind == "probe" && j.Phase == "direct"
	}
	if !found {
		t.Fatal("missing direct probe")
	}
	c.journal = nil
	for k, d := range c.timers {
		if d.timer == direct.Timer {
			c.cancel(k)
		}
	}
	if len(c.Inspection()) != 1 || c.Inspection()[0].Phase != "cancel" {
		t.Fatal(c.Inspection())
	}
	step(t, c, direct.At, Fired{Timer: direct.Timer})
	for _, j := range c.Inspection() {
		if j.Kind == "timer" && j.Phase == "fire" && j.ID == direct.Timer.ID {
			t.Fatal("stale timer reported as fired")
		}
	}
}

func TestInspectionSuspicionExpiryIsAuthoritativeAndNeutral(t *testing.T) {
	a, b := testCore(t), testCore(t)
	for _, c := range []*Core{a, b} {
		add(t, c, "c", 3)
		add(t, c, "d", 4)
	}
	b.EnableInspection()
	input := func(from string) Datagram {
		return Datagram{From: identity(from, 3).Address, Messages: []wire.Message{{Suspect: &wire.Accusation{Node: "b", From: from, Incarnation: 1}}}}
	}
	same := func(now Instant, e Event) []Effect {
		t.Helper()
		queue := []Event{e}
		var effects []Effect
		for len(queue) > 0 {
			e, queue = queue[0], queue[1:]
			ae, be := step(t, a, now, e), step(t, b, now, e)
			if !reflect.DeepEqual(ae, be) || !reflect.DeepEqual(a.Snapshot(), b.Snapshot()) {
				t.Fatal("inspection changed suspicion behavior")
			}
			effects = append(effects, be...)
			for _, effect := range be {
				if d, ok := effect.(Defer); ok {
					queue = append(queue, Work(d))
				}
			}
		}
		return effects
	}
	original := timer(t, same(0, input("c")), "")
	shortened := timer(t, same(Instant(time.Second), input("d")), "")
	if shortened.At >= original.At {
		t.Fatal("confirmation did not shorten timer")
	}
	same(shortened.At, Fired{Timer: shortened.Timer})
	journal := b.Inspection()
	fire, dead := -1, -1
	for i, j := range journal {
		if j.Kind == "timer" && j.Purpose == "suspicion" && j.Subject == "b" && j.Phase == "fire" && j.Generation == shortened.Timer.Generation {
			fire = i
		}
		if j.Kind == "member.upsert" && j.Member.Name == "b" && j.Member.State == wire.Dead {
			dead = i
		}
	}
	if fire < 0 || dead <= fire {
		t.Fatal("missing ordered expiry/dead witness", journal)
	}
	same(original.At, Fired{Timer: original.Timer})
	for _, j := range b.Inspection() {
		if j.Kind == "timer" && j.Purpose == "suspicion" && j.Phase == "fire" {
			t.Fatal("stale suspicion fire recorded", j)
		}
	}
}
