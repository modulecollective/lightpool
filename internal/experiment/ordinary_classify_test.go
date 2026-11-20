package experiment

import (
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/internal/ordinary"
	"github.com/modulecollective/lightpool/wire"
)

// Hand-calculated fixtures. Every child clock equals coordinator time with a
// 1 µs round trip, so offsets are [0, 1000] and a step [s,e] maps to
// [s, e+1000] in coordinator time.
const (
	sec   = int64(1_000_000_000)
	rtt   = int64(1000)
	width = int64(100)
)

func ordAddr(port uint16) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("10.0.0.1"), port)
}

func ordMember(port uint16, inc uint64, state wire.State) core.Inspection {
	return core.Inspection{Kind: "member.upsert", Member: &wire.Member{Identity: wire.Identity{Name: "t", Address: ordAddr(port)}, Incarnation: inc, State: state}}
}

func ordArm(id uint64, at int64) core.Inspection {
	return core.Inspection{Kind: "timer", Purpose: "suspicion", Subject: "t", Phase: "arm", ID: id, Generation: id, At: core.Instant(at)}
}

func ordTimer(phase string, id uint64, at int64) core.Inspection {
	i := ordArm(id, at)
	i.Phase = phase
	return i
}

func ordSuspicion(outcome string) core.Inspection {
	return core.Inspection{Kind: "suspicion", Subject: "t", Outcome: outcome}
}

type ordFixtureNode struct {
	role  string
	port  uint16
	steps [][]core.Inspection
	at    []int64
}

// origin is a natural local suspicion: the sender's own probe of T fails, then
// it suspects T at incarnation 1.
func ordOrigin(sender string) []core.Inspection {
	return []core.Inspection{
		{Kind: "probe", Subject: "t", Phase: "finished", Outcome: "timeout"},
		ordMember(24, 1, wire.Suspect),
		ordSuspicion("started:" + sender),
		ordArm(1, 99*sec),
	}
}

type ordFixture struct {
	nodes map[string]*ordFixtureNode
}

func (f *ordFixture) step(name string, at int64, journal ...core.Inspection) {
	n := f.nodes[name]
	n.steps = append(n.steps, journal)
	n.at = append(n.at, at)
}

func (f *ordFixture) input() OrdinaryInput {
	var names []string
	for name := range f.nodes {
		names = append(names, name)
	}
	slices.Sort(names)
	in := OrdinaryInput{Pairs: []Pair{{Observer: "o", Subject: "t"}}, Warmup: ordinary.Interval{LoNS: 15 * sec, HiNS: 15*sec + 1_000_000}, Horizon: ordinary.Interval{LoNS: 120 * sec, HiNS: 120*sec + 1_000_000}}
	for i, name := range names {
		n := f.nodes[name]
		node := OrdinaryNode{Name: name, Role: n.role, PID: 100 + i, Lifetime: uint64(1000 + i), Address: ordAddr(n.port), FileSHA256: "sha-" + name}
		steps := append([][]core.Inspection{nil}, n.steps...)
		at := append([]int64{0}, n.at...)
		for j := range steps {
			node.Steps = append(node.Steps, OrdinaryStep{Sequence: uint64(j + 1), StartNS: at[j], CoreAtNS: at[j], EndNS: at[j] + width, Journal: steps[j]})
		}
		node.Clock = []ordinary.Exchange{{PID: node.PID, SendNS: 0, ChildNS: 0, ReceiveNS: rtt}, {PID: node.PID, SendNS: 130 * sec, ChildNS: 130 * sec, ReceiveNS: 130*sec + rtt}}
		in.Nodes = append(in.Nodes, node)
	}
	return in
}

// positive builds the harmless acceleration chain:
// a1, a2 fail probes of t; o starts from a1 (deadline 30 s), a2 confirms
// (deadline 27 s); t refutes at 24 s; o accepts incarnation 2 at 25 s.
func ordPositive() *ordFixture {
	f := &ordFixture{nodes: map[string]*ordFixtureNode{"a1": {role: "A", port: 1}, "a2": {role: "A", port: 2}, "t": {role: "T", port: 24}, "o": {role: "O", port: 30}, "x": {role: "O", port: 31}}}
	f.step("a1", 20*sec, ordOrigin("a1")...)
	f.step("a2", 21*sec, ordOrigin("a2")...)
	f.step("o", 22*sec, ordMember(24, 1, wire.Suspect), ordSuspicion("started:a1"), ordArm(5, 30*sec))
	f.step("o", 23*sec, ordSuspicion("confirmation:a2"), ordTimer("cancel", 5, 30*sec), ordArm(6, 27*sec))
	f.step("t", 24*sec, ordMember(24, 2, wire.Alive), core.Inspection{Kind: "refutation", Subject: "t"})
	f.step("o", 25*sec, ordTimer("cancel", 6, 27*sec), ordMember(24, 2, wire.Alive))
	return f
}

func ordClassify(t *testing.T, f *ordFixture) *OrdinaryEpisode {
	t.Helper()
	out, err := ClassifyOrdinary(f.input())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Episodes) != 1 {
		t.Fatalf("episodes = %d", len(out.Episodes))
	}
	return out.Episodes[0]
}

func TestOrdinaryClassifierPositive(t *testing.T) {
	out, err := ClassifyOrdinary(ordPositive().input())
	if err != nil {
		t.Fatal(err)
	}
	if out.Accelerations != 1 || out.Harmful != 0 || out.Inconclusive != 0 || len(out.Episodes) != 1 {
		t.Fatalf("%+v", out)
	}
	ep := out.Episodes[0]
	s := ep.Shortenings[0]
	if ep.Status != "acceleration" || ep.EndCause != "accepted" || s.Status != "qualifying" || *s.PreviousDeadlineNS != 30*sec || *s.DeadlineNS != 27*sec {
		t.Fatalf("%+v %+v", ep, s)
	}
	if ep.Origin.Fact == nil || ep.Origin.Fact.Node != "a1" || ep.Origin.Fact.Step != 2 || ep.Origin.Fact.Inspection != 2 || ep.Origin.Fact.FileSHA256 != "sha-a1" || s.Origin.Fact.Node != "a2" {
		t.Fatalf("origin evidence %+v %+v", ep.Origin, s.Origin)
	}
	if ep.Refutation == nil || ep.RefutationIncarnation != 2 || ep.Acceptance == nil || ep.Acceptance.Step != 4 || ep.TargetLifetime == 0 {
		t.Fatalf("refutation evidence %+v", ep)
	}
	if want := []SharedOrigin{{Target: "t", Incarnation: 1, Senders: []string{"a1", "a2"}, Observers: []string{"o"}}}; !slicesEqualShared(out.SharedOrigins, want) {
		t.Fatalf("shared origins %+v", out.SharedOrigins)
	}
}

func slicesEqualShared(a, b []SharedOrigin) bool {
	return slices.EqualFunc(a, b, func(x, y SharedOrigin) bool {
		return x.Target == y.Target && x.Incarnation == y.Incarnation && slices.Equal(x.Senders, y.Senders) && slices.Equal(x.Observers, y.Observers)
	})
}

func TestOrdinaryClassifierHarmfulExpiry(t *testing.T) {
	f := ordPositive()
	o := f.nodes["o"]
	// The shortened 27 s timer fires before the original 30 s deadline; the
	// refutation generated at 24 s is accepted only afterwards at 28 s.
	o.steps[2] = []core.Inspection{ordTimer("fire", 6, 27*sec), ordMember(24, 1, wire.Dead)}
	o.at[2] = 27 * sec
	f.step("o", 28*sec, ordMember(24, 2, wire.Alive))
	ep := ordClassify(t, f)
	if ep.Status != "acceleration" || !ep.Harmful || ep.EndCause != "local-expiry" || ep.Expiry == nil || ep.Acceptance.Step != 5 {
		t.Fatalf("%+v", ep)
	}
	// Received dead gossip is a separate outcome, never harm.
	o.steps[2] = []core.Inspection{ordTimer("cancel", 6, 27*sec), ordMember(24, 1, wire.Dead)}
	ep = ordClassify(t, f)
	if ep.Status != "acceleration" || ep.Harmful || ep.EndCause != "received-dead" {
		t.Fatalf("%+v", ep)
	}
}

func TestOrdinaryClassifierValidNull(t *testing.T) {
	f := ordPositive()
	f.nodes["o"].steps = f.nodes["o"].steps[:1]
	f.nodes["o"].at = f.nodes["o"].at[:1]
	f.step("o", 25*sec, ordTimer("cancel", 5, 30*sec), ordMember(24, 2, wire.Alive))
	out, err := ClassifyOrdinary(f.input())
	if err != nil {
		t.Fatal(err)
	}
	if out.Accelerations != 0 || out.Inconclusive != 0 || len(out.Episodes) != 1 || out.Episodes[0].Status != "no-acceleration" || len(out.SharedOrigins) != 0 {
		t.Fatalf("%+v", out.Episodes[0])
	}
	f = &ordFixture{nodes: map[string]*ordFixtureNode{"t": {role: "T", port: 24}, "o": {role: "O", port: 30}}}
	if out, err = ClassifyOrdinary(f.input()); err != nil || len(out.Episodes) != 0 || out.Accelerations != 0 {
		t.Fatal(out, err)
	}
}

// Each negative fixture is one mutation of the positive chain.
func TestOrdinaryClassifierRejections(t *testing.T) {
	for name, c := range map[string]struct {
		mutate func(*ordFixture)
		status string
		reason string
	}{
		"equal deadline is not strict shortening": {func(f *ordFixture) {
			f.nodes["o"].steps[1][2] = ordArm(6, 30*sec)
		}, "no-acceleration", "deadline did not strictly shorten"},
		"refutation already in flight": {func(f *ordFixture) {
			f.nodes["t"].at[0] = 22*sec + 500_000_000
		}, "no-acceleration", "target refuted before shortening"},
		"duplicate sender": {func(f *ordFixture) {
			f.nodes["o"].steps[1][0] = ordSuspicion("confirmation:a1")
		}, "no-acceleration", "confirmation repeats the starting sender"},
		"wrong incarnation origin": {func(f *ordFixture) {
			f.nodes["a2"].steps[0][1] = ordMember(24, 0, wire.Suspect)
		}, "no-acceleration", "sender a2 lacks a same-incarnation failed-probe origin"},
		"origin without failed probe": {func(f *ordFixture) {
			f.nodes["a2"].steps[0][0].Outcome = "success"
		}, "no-acceleration", "sender a2 lacks a same-incarnation failed-probe origin"},
		"wrong target lifetime": {func(f *ordFixture) {
			f.nodes["o"].steps[0][0] = ordMember(99, 1, wire.Suspect)
		}, "no-acceleration", "suspected member is a different target lifetime"},
		"later matching suspicion is not provenance": {func(f *ordFixture) {
			f.nodes["a2"].at[0] = 23*sec + 500_000_000
		}, "no-acceleration", "sender a2 origin does not precede receipt"},
		"sender outside A": {func(f *ordFixture) {
			f.nodes["o"].steps[1][0] = ordSuspicion("confirmation:x")
		}, "no-acceleration", "sender x outside A"},
		"cancelled timer is not live": {func(f *ordFixture) {
			o := f.nodes["o"]
			o.steps = slices.Insert(o.steps, 1, []core.Inspection{ordTimer("cancel", 5, 30*sec)})
			o.at = slices.Insert(o.at, 1, 22*sec+500_000_000)
		}, "no-acceleration", "confirmation did not replace a live suspicion deadline"},
		"late refutation is censored": {func(f *ordFixture) {
			o := f.nodes["o"]
			o.steps, o.at = o.steps[:2], o.at[:2]
		}, "no-acceleration", "matching refutation acceptance censored"},
		"acceptance after horizon": {func(f *ordFixture) {
			f.nodes["o"].at[2] = 121 * sec
			f.nodes["o"].steps[2] = []core.Inspection{ordMember(24, 2, wire.Alive)}
		}, "no-acceleration", "refutation accepted after horizon"},
		"start before warm-up": {func(f *ordFixture) {
			f.nodes["a1"].at[0], f.nodes["a2"].at[0], f.nodes["o"].at[0] = 10*sec, 11*sec, 12*sec
		}, "ineligible", "suspicion started before warm-up"},
	} {
		t.Run(name, func(t *testing.T) {
			f := ordPositive()
			c.mutate(f)
			ep := ordClassify(t, f)
			all := append(append([]string{}, ep.Reasons...), ep.Shortenings[0].Reasons...)
			if ep.Status != c.status || ep.Harmful || !slices.Contains(all, c.reason) {
				t.Fatalf("status %s reasons %q", ep.Status, all)
			}
		})
	}
}

// Rearming replaces the live deadline: the shortening compares against the
// rearmed 31 s timer, not the cancelled 30 s one.
func TestOrdinaryClassifierRearmedTimer(t *testing.T) {
	f := ordPositive()
	o := f.nodes["o"]
	o.steps = slices.Insert(o.steps, 1, []core.Inspection{ordTimer("cancel", 5, 30*sec), ordArm(7, 31*sec)})
	o.at = slices.Insert(o.at, 1, 22*sec+500_000_000)
	ep := ordClassify(t, f)
	if ep.Status != "acceleration" || *ep.InitialDeadlineNS != 30*sec || *ep.Shortenings[0].PreviousDeadlineNS != 31*sec {
		t.Fatalf("%+v", ep)
	}
}

// Equal or overlapping coordinator bounds stay inconclusive; wall timestamps
// never break the tie.
func TestOrdinaryClassifierAmbiguousOrder(t *testing.T) {
	for name, c := range map[string]struct {
		mutate    func(*ordFixture)
		ambiguity string
	}{
		// Shortening step [23 s, 23 s+100] maps to [23 s, 23 s+1100]; a
		// refutation starting at exactly 23 s+1100 shares that endpoint.
		"equal endpoint":          {func(f *ordFixture) { f.nodes["t"].at[0] = 23*sec + width + rtt }, "shortening overlaps refutation generation"},
		"origin overlaps receipt": {func(f *ordFixture) { f.nodes["a2"].at[0] = 23*sec + 50 }, "sender a2 origin overlaps receipt"},
		"start overlaps warm-up": {func(f *ordFixture) {
			f.nodes["o"].at[0] = 15*sec + 500_000
			f.nodes["a1"].at[0], f.nodes["a2"].at[0] = 14*sec, 14*sec+1
		}, "suspicion start overlaps warm-up boundary"},
		"acceptance overlaps horizon": {func(f *ordFixture) {
			f.nodes["o"].at[2] = 120 * sec
		}, "acceptance overlaps horizon"},
	} {
		t.Run(name, func(t *testing.T) {
			f := ordPositive()
			c.mutate(f)
			out, err := ClassifyOrdinary(f.input())
			if err != nil {
				t.Fatal(err)
			}
			ep := out.Episodes[0]
			if ep.Status != "inconclusive" || out.Inconclusive != 1 || out.Accelerations != 0 || ep.Shortenings[0].Status != "inconclusive" || !slices.Contains(ep.Shortenings[0].Ambiguities, c.ambiguity) {
				t.Fatalf("%s %+v", ep.Status, ep.Shortenings[0])
			}
		})
	}
	// One nanosecond of separation is enough to prove order.
	f := ordPositive()
	f.nodes["t"].at[0] = 23*sec + width + rtt + 1
	if ep := ordClassify(t, f); ep.Status != "acceleration" {
		t.Fatalf("%+v", ep.Shortenings[0])
	}
}

func TestOrdinaryClassifierMalformedInput(t *testing.T) {
	for name, mutate := range map[string]func(*OrdinaryInput){
		"missing target": func(in *OrdinaryInput) {
			in.Nodes = slices.DeleteFunc(in.Nodes, func(n OrdinaryNode) bool { return n.Name == "t" })
		},
		"missing sender": func(in *OrdinaryInput) {
			in.Nodes = slices.DeleteFunc(in.Nodes, func(n OrdinaryNode) bool { return n.Name == "a2" })
		},
		"duplicate node":  func(in *OrdinaryInput) { in.Nodes = append(in.Nodes, in.Nodes[0]) },
		"reordered steps": func(in *OrdinaryInput) { s := in.Nodes[0].Steps; s[0], s[1] = s[1], s[0] },
		"unbounded step":  func(in *OrdinaryInput) { in.Nodes[0].Steps[1].EndNS = in.Nodes[0].Steps[1].StartNS - 1 },
		"no steps":        func(in *OrdinaryInput) { in.Nodes[0].Steps = nil },
		"changed process": func(in *OrdinaryInput) { in.Nodes[0].Clock[1].PID++ },
		"missing anchor":  func(in *OrdinaryInput) { in.Nodes[0].Clock = in.Nodes[0].Clock[:1] },
		"bad warm-up":     func(in *OrdinaryInput) { in.Warmup = in.Horizon },
		"no pairs":        func(in *OrdinaryInput) { in.Pairs = nil },
	} {
		t.Run(name, func(t *testing.T) {
			in := ordPositive().input()
			mutate(&in)
			if _, err := ClassifyOrdinary(in); err == nil {
				t.Fatal("malformed attempt classified")
			}
		})
	}
}

func TestOrdinaryClockBound(t *testing.T) {
	ex := []ordinary.Exchange{{PID: 7, SendNS: 100, ChildNS: 10, ReceiveNS: 130}, {PID: 7, SendNS: 200, ChildNS: 105, ReceiveNS: 205}}
	o, err := ordinary.Bound(7, ex, 10, 105)
	// [90,120] ∩ [95,100] = [95,100]
	if err != nil || o != (ordinary.Offset{LoNS: 95, HiNS: 100}) {
		t.Fatal(o, err)
	}
	if got := o.Map(20, 30); got != (ordinary.Interval{LoNS: 115, HiNS: 130}) {
		t.Fatal(got)
	}
	if a, b := (ordinary.Interval{LoNS: 0, HiNS: 5}), (ordinary.Interval{LoNS: 5, HiNS: 9}); a.Before(b) || b.Before(a) {
		t.Fatal("equal endpoints ordered")
	}
	for name, c := range map[string]struct {
		ex          []ordinary.Exchange
		first, last int64
		want        string
	}{
		"none":     {nil, 0, 0, "no clock exchanges"},
		"reversed": {[]ordinary.Exchange{{PID: 7, SendNS: 10, ChildNS: 0, ReceiveNS: 9}}, 0, 0, "reversed"},
		"empty":    {[]ordinary.Exchange{{PID: 7, SendNS: 0, ChildNS: 0, ReceiveNS: 10}, {PID: 7, SendNS: 100, ChildNS: 50, ReceiveNS: 110}}, 0, 50, "empty clock offset"},
		"foreign":  {[]ordinary.Exchange{{PID: 8, SendNS: 0, ChildNS: 0, ReceiveNS: 1}}, 0, 0, "from process 8"},
		"order":    {[]ordinary.Exchange{{PID: 7, SendNS: 0, ChildNS: 5, ReceiveNS: 10}, {PID: 7, SendNS: 20, ChildNS: 4, ReceiveNS: 30}}, 4, 5, "out of order"},
		"bracket":  {ex, 9, 105, "do not bracket"},
	} {
		if _, err := ordinary.Bound(7, c.ex, c.first, c.last); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
