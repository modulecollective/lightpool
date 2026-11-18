// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"bytes"
	"github.com/modulecollective/lightpool/wire"
	"testing"
	"time"
)

func findOpen(t testing.TB, es []Effect) OpenStream {
	t.Helper()
	for _, e := range es {
		if o, ok := e.(OpenStream); ok {
			return o
		}
	}
	t.Fatal("missing open")
	return OpenStream{}
}

func TestOperationEndCancelsApplicationRequest(t *testing.T) {
	for _, reason := range []string{"cancel", "timeout", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			c := testCore(t)
			c.config.Application = true
			o := findOpen(t, step(t, c, 0, Join{Command: 7, Peer: identity("b", 2)}))
			req := findRequest(t, step(t, c, 0, Opened{ID: o.ID}), LocalState)
			var es []Effect
			switch reason {
			case "cancel":
				es = step(t, c, 1, CancelCommand{Command: 7})
			case "timeout":
				d := c.timers[key("stream:", o.ID.ID)]
				es = step(t, c, d.at, Fired{Timer: d.timer})
			case "shutdown":
				es = step(t, c, 1, Shutdown{})
			}
			cancels := 0
			for _, e := range es {
				if cancel, ok := e.(CancelRequest); ok {
					if cancel.ID != req.ID {
						t.Fatal("wrong request canceled", cancel)
					}
					cancels++
				}
			}
			if cancels != 1 || countResults(es, 7) != 1 {
				t.Fatal("missing unique cancellation/result", es)
			}
			if es = step(t, c, c.now.Add(time.Nanosecond), Response{ID: req.ID, Data: []byte("late")}); len(es) != 0 {
				t.Fatal("late response acted", es)
			}
		})
	}
}
func findRequest(t testing.TB, es []Effect, kind RequestKind) Request {
	t.Helper()
	for _, e := range es {
		if r, ok := e.(Request); ok && r.Kind == kind {
			return r
		}
	}
	t.Fatal("missing request", kind, es)
	return Request{}
}
func TestPassiveResponseBeforeMergeAndCap(t *testing.T) {
	c := testCore(t)
	c.config.Application = true
	c.config.MaxPushPull = 1
	id := Token{Lifetime: 3, ID: 1 << 63}
	step(t, c, 0, Accepted{ID: id, Peer: identity("c", 3).Address})
	remote := wire.Member{Identity: identity("c", 3), Incarnation: 1}
	es := step(t, c, 0, StreamMessage{ID: id, Messages: []wire.Message{{Exchange: &wire.Exchange{Join: true, Members: []wire.Member{remote}, Data: []byte("remote")}}}})
	local := findRequest(t, es, LocalState)
	es = step(t, c, 0, Response{ID: local.ID, Data: []byte("local")})
	for _, e := range es {
		if r, ok := e.(Request); ok && r.Kind == ApproveMerge {
			t.Fatal("merge approval preceded response write")
		}
	}
	if c.nodes["c"] != nil {
		t.Fatal("merged before response write")
	}
	second := Token{Lifetime: 3, ID: 1<<63 | 1}
	step(t, c, 0, Accepted{ID: second, Peer: identity("d", 4).Address})
	es = step(t, c, 0, StreamMessage{ID: second, Messages: []wire.Message{{Exchange: &wire.Exchange{}}}})
	if c.streams[second.ID] != nil || len(es) == 0 {
		t.Fatal("push/pull cap failed")
	}
	es = step(t, c, 0, Written{ID: id})
	merge := findRequest(t, es, ApproveMerge)
	if c.nodes["c"] != nil {
		t.Fatal("merged before approval")
	}
	es = step(t, c, 0, Response{ID: merge.ID})
	approval := findRequest(t, es, ApproveAlive)
	// A newer incarnation wins while the old asynchronous approval is pending.
	changed := remote.Clone()
	changed.Incarnation = 3
	changed.Meta = []byte("newer")
	c.applyAlive(changed)
	es = step(t, c, 0, Response{ID: approval.ID})
	if c.nodes["c"].Incarnation != 3 || !bytes.Equal(c.nodes["c"].Meta, []byte("newer")) {
		t.Fatal("late approval overwrote newer state")
	}
	found := false
	for _, e := range es {
		if n, ok := e.(Notification); ok && n.Kind == "merge-state" && bytes.Equal(n.Data, []byte("remote")) {
			found = true
		}
	}
	if !found {
		t.Fatal("user state absent")
	}
}
func TestStreamStagesAndDeadlineMatrix(t *testing.T) {
	for _, stage := range []string{"connect", "write", "receive", "approval"} {
		for _, before := range []bool{true, false} {
			t.Run(stage+"/"+map[bool]string{true: "before", false: "after"}[before], func(t *testing.T) {
				c := testCore(t)
				if stage == "approval" {
					c.config.Application = true
				}
				open := findOpen(t, step(t, c, 0, Join{Command: 10, Peer: identity("b", 2)}))
				id := open.ID
				switch stage {
				case "write", "receive":
					step(t, c, 0, Opened{ID: id})
					if stage == "receive" {
						step(t, c, 0, Written{ID: id})
					}
				case "approval":
					es := step(t, c, 0, Opened{ID: id})
					r := findRequest(t, es, LocalState)
					step(t, c, 0, Response{ID: r.ID})
					step(t, c, 0, Written{ID: id})
					step(t, c, 0, StreamMessage{ID: id, Messages: []wire.Message{{Exchange: &wire.Exchange{}}}})
				}
				d := c.timers[key("stream:", id.ID)]
				var event Event
				switch stage {
				case "connect":
					event = Opened{ID: id}
				case "write":
					event = Written{ID: id}
				case "receive":
					event = StreamMessage{ID: id, Messages: []wire.Message{{Exchange: &wire.Exchange{}}}}
				case "approval":
					for appID, p := range c.app {
						if p.kind == ApproveMerge {
							event = Response{ID: Token{3, appID}}
						}
					}
				}
				var effects []Effect
				if before {
					effects = append(effects, step(t, c, d.at, event)...)
					effects = append(effects, step(t, c, d.at, Fired{d.timer})...)
				} else {
					effects = append(effects, step(t, c, d.at, Fired{d.timer})...)
					effects = append(effects, step(t, c, d.at, event)...)
				}
				if before && (stage == "connect" || stage == "write") {
					if c.streams[id.ID] == nil {
						t.Fatal("stale stage timeout acted")
					}
					d = c.timers[key("stream:", id.ID)]
					effects = append(effects, step(t, c, d.at, Fired{d.timer})...)
				}
				if countResults(effects, 10) != 1 {
					t.Fatal("operation did not terminate exactly once", effects)
				}
			})
		}
	}
}
func TestReplyBeforeLocalWriteCompletion(t *testing.T) {
	c := testCore(t)
	o := findOpen(t, step(t, c, 0, Join{Command: 1, Peer: identity("b", 2)}))
	step(t, c, 0, Opened{ID: o.ID})
	remote := wire.Member{Identity: identity("c", 3), Incarnation: 1}
	step(t, c, 0, StreamMessage{ID: o.ID, Messages: []wire.Message{{Exchange: &wire.Exchange{Members: []wire.Member{remote}}}}})
	if c.nodes["c"] != nil {
		t.Fatal("early response merged")
	}
	es := step(t, c, 0, Written{ID: o.ID})
	if c.nodes["c"] == nil || countResults(es, 1) != 1 {
		t.Fatal(es)
	}
	if es = step(t, c, 0, Written{ID: o.ID}); len(es) != 0 {
		t.Fatal("duplicate completion acted")
	}
}
func TestApplicationRejectionTimeoutAndShutdown(t *testing.T) {
	c := testCore(t)
	c.config.Application = true
	o := findOpen(t, step(t, c, 0, Join{Command: 7, Peer: identity("b", 2)}))
	req := findRequest(t, step(t, c, 0, Opened{ID: o.ID}), LocalState)
	es := step(t, c, 0, Response{ID: req.ID, Reject: true})
	if countResults(es, 7) != 1 || len(c.streams) != 0 {
		t.Fatal(es)
	}
	o = findOpen(t, step(t, c, 0, Join{Command: 8, Peer: identity("b", 2)}))
	req = findRequest(t, step(t, c, 0, Opened{ID: o.ID}), LocalState)
	step(t, c, 0, Shutdown{})
	if es = step(t, c, 1, Response{ID: req.ID, Data: []byte("late")}); len(es) != 0 {
		t.Fatal("late application response acted")
	}
}
func TestTCPDeadlineEqualTime(t *testing.T) {
	for _, first := range []bool{true, false} {
		c := testCore(t)
		es := step(t, c, 0, Probe{Peer: "b"})
		ping := pingSend(t, es)
		a := timer(t, es, "direct")
		overall := timer(t, es, "overall")
		es = step(t, c, a.At, Fired{a.Timer})
		o := findOpen(t, es)
		step(t, c, a.At, Opened{ID: o.ID})
		step(t, c, a.At, Written{ID: o.ID})
		reply := StreamMessage{ID: o.ID, Received: overall.At, Messages: []wire.Message{{Ack: &wire.Ack{Sequence: ping.Messages[0].Ping.Sequence}}}}
		if first {
			step(t, c, overall.At, reply)
		}
		step(t, c, overall.At, Fired{overall.Timer})
		if !first {
			step(t, c, overall.At, reply)
		}
		expected := wire.Suspect
		if first {
			expected = wire.Alive
		}
		if state(c, "b") != expected {
			t.Fatal(first, c.Snapshot())
		}
	}
}
func TestLocalAndRemoteSendFailures(t *testing.T) {
	for _, failure := range []Failure{LocalFailure, RemoteFailure} {
		c := testCore(t)
		es := step(t, c, 0, Probe{Peer: "b"})
		s := pingSend(t, es)
		es = step(t, c, 0, Sent{ID: s.ID, Failure: failure})
		if failure == LocalFailure {
			if len(c.probes) != 0 || state(c, "b") != wire.Alive || c.health != 0 {
				t.Fatal("local facility failure suspected peer")
			}
		} else if len(es) == 0 || len(c.probes) != 1 {
			t.Fatal("remote failure did not escalate")
		}
	}
}
func TestLargestProbeControlMessagesFit(t *testing.T) {
	c := testCore(t)
	maxMeta := bytes.Repeat([]byte{1}, wire.MaxMeta)
	c.nodes["a"].Meta = maxMeta
	c.nodes["b"].Meta = maxMeta
	add(t, c, "c", 3)
	es := step(t, c, 0, Probe{Peer: "b"})
	a := timer(t, es, "direct")
	es = append(es, step(t, c, a.At, Fired{a.Timer})...)
	for _, e := range es {
		if s, ok := e.(SendDatagram); ok {
			n, err := wire.Size(s.Messages, c.config.Label, c.config.Protected)
			if err != nil || n > 1400 {
				t.Fatal(n, err)
			}
		}
	}
}

func TestReliableSendCloseCannotOvertakeWrite(t *testing.T) {
	for _, failure := range []Failure{Success, RemoteFailure} {
		c := testCore(t)
		es := step(t, c, 0, Send{Command: 1, Peer: identity("b", 2), Data: []byte("bytes"), Reliable: true})
		o := findOpen(t, es)
		step(t, c, 0, Opened{ID: o.ID})
		es = step(t, c, 0, Closed{ID: o.ID, Failure: RemoteFailure})
		if countResults(es, 1) != 0 {
			t.Fatal("read close ended pending write")
		}
		es = step(t, c, 0, Written{ID: o.ID, Failure: failure})
		if countResults(es, 1) != 1 {
			t.Fatal(es)
		}
		for _, e := range es {
			if r, ok := e.(Result); ok && r.Command == 1 && r.Failure != failure {
				t.Fatal("write completion lost", r)
			}
		}
	}
}

func TestExchangeRejectsUngossipableIdentity(t *testing.T) {
	c := testCore(t)
	application := string(bytes.Repeat([]byte{'<'}, wire.MaxName))
	c.nodes["a"].Application = application
	n := wire.Member{Identity: identity(string(bytes.Repeat([]byte{'<'}, wire.MaxName)), 3), Incarnation: 1}
	n.Application = application
	n.Meta = make([]byte, wire.MaxMeta)
	if c.fitsAlive(n) {
		t.Fatal("fixture fits")
	}
	o := findOpen(t, step(t, c, 0, Join{Command: 1, Peer: identity("b", 2)}))
	step(t, c, 0, Opened{ID: o.ID})
	step(t, c, 0, Written{ID: o.ID})
	es := step(t, c, 0, StreamMessage{ID: o.ID, Messages: []wire.Message{{Exchange: &wire.Exchange{Members: []wire.Member{n}}}}})
	if countResults(es, 1) != 1 || c.nodes[n.Name] != nil {
		t.Fatal("untransmittable identity merged", es)
	}
	for _, e := range es {
		if r, ok := e.(Result); ok && r.Command == 1 && r.Failure != Rejected {
			t.Fatal(r)
		}
	}
}

func TestMembershipSnapshotPrecedesApplicationState(t *testing.T) {
	c := testCore(t)
	c.config.Application = true
	o := findOpen(t, step(t, c, 0, Join{Command: 1, Peer: identity("b", 2)}))
	request := findRequest(t, step(t, c, 0, Opened{ID: o.ID}), LocalState)
	n := wire.Member{Identity: identity("c", 3), Incarnation: 1}
	c.applyAlive(n)
	es := step(t, c, 0, Response{ID: request.ID, Data: []byte("state")})
	found := false
	for _, e := range es {
		if write, ok := e.(WriteStream); ok {
			found = true
			for _, m := range write.Messages[0].Exchange.Members {
				if m.Name == "c" {
					t.Fatal("membership snapshot moved across application request")
				}
			}
		}
	}
	if !found {
		t.Fatal("no response write")
	}
}
