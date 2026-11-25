package experiment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

func TestDeliveryFixtures(t *testing.T) {
	var seeds sim.Seeds
	for _, n := range []int{32, 128} {
		for _, loss := range []int{0, 10, 30} {
			for _, mode := range []string{"on", "off"} {
				name := fmt.Sprintf("delivery-%d-loss-%d-exchange-%s", n, loss, mode)
				spec, s, err := Fixture(name, 1, "source")
				if err != nil {
					t.Fatal(err)
				}
				if err = validateSpec(spec, s); err != nil {
					t.Fatal(err)
				}
				if seeds == (sim.Seeds{}) {
					seeds = s.Seeds
				}
				if s.Seeds != seeds || len(s.Nodes) != n || spec.WarmupNS != int64(50*time.Second) || s.Horizon != core.Instant(130*time.Second) || spec.HorizonNS != int64(s.Horizon) || spec.TraceCap != TraceCap || s.EventBudget != EventBudget || s.Latency != time.Millisecond || s.DatagramCapacity != 1<<20 || s.StreamCapacity != 1<<20 || len(spec.Anomaly) != 0 || len(spec.HealthyPairs) != 0 {
					t.Fatal("fixture dimensions", name)
				}
				cfg := core.DefaultConfig()
				if mode == "off" {
					cfg.PushPullInterval = 0
				}
				if HashJSON(cfg) != HashJSON(s.Protocol) {
					t.Fatal("extra protocol changes", name)
				}
				if len(s.Actions) != n-1+20 || len(s.Faults) != n*(n-1) {
					t.Fatal("workload size", name)
				}
				for i, node := range s.Nodes {
					if node.Identity.Name != fmt.Sprintf("n%03d", i) || node.Identity.Address.Addr().As4() != [4]byte{192, 0, 2, byte(i + 1)} || node.Identity.Address.Port() != 7946 || len(node.Identity.Meta) != 0 || node.Service != 0 {
						t.Fatal("node", i)
					}
				}
				for i := 0; i < n-1; i++ {
					a := s.Actions[i]
					if a.Kind != "join" || a.Node != s.Nodes[i+1].Identity.Name || a.Peer != "n000" || a.At != core.Instant(time.Duration(i+1)*10*time.Millisecond) {
						t.Fatal("join", i)
					}
				}
				for i, a := range s.Actions[n-1:] {
					if a.Kind != "update" || a.Node != fmt.Sprintf("n%03d", i) || a.At != core.Instant(time.Duration(50+4*i)*time.Second) || string(a.Data) != fmt.Sprintf("upd-%04d", i) || len(a.Data) != 8 {
						t.Fatal("update", a)
					}
				}
				seen := map[[2]string]bool{}
				for _, f := range s.Faults {
					k := [2]string{f.From, f.To}
					if seen[k] || f.From == f.To || f != (sim.Fault{From: f.From, To: f.To, End: s.Horizon + 1, LossPPM: loss * 10000}) {
						t.Fatal("fault", f)
					}
					seen[k] = true
				}
			}
		}
	}
	for _, name := range []string{"delivery-32-loss-1-exchange-on", "delivery-64-loss-0-exchange-on", "delivery-32-loss-0-exchange-maybe"} {
		if _, _, err := Fixture(name, 1, "source"); err == nil {
			t.Fatal("accepted", name)
		}
	}
}

func newDeliveryEvidence(t *testing.T) *evidenceBuilder {
	t.Helper()
	b := newEvidence(t)
	b.spec.Case = "delivery-32-loss-0-exchange-on"
	b.spec.Anomaly = nil
	b.spec.WarmupNS = int64(10 * time.Second)
	b.spec.HorizonNS = int64(20 * time.Second)
	b.scenario.Horizon = core.Instant(b.spec.HorizonNS)
	b.scenario.Actions = []sim.Action{{At: core.Instant(b.spec.WarmupNS), Kind: "update", Node: "n000", Data: []byte("upd-0000")}}
	return b
}

func updateMember(receiver, source, token, incarnation, state string) trace.Change {
	c := member(receiver, source, state)
	c.Member.Meta = []byte(token)
	c.Member.Incarnation = incarnation
	return c
}

func (b *evidenceBuilder) updateEvent(at int64, kind, actor string, changes ...trace.Change) Reference {
	ref := b.add(at, changes...)
	e := &b.events[len(b.events)-1]
	e.Kind, e.Actor = kind, actor
	return ref
}

func (b *evidenceBuilder) generate() Reference {
	b.updateEvent(b.spec.WarmupNS, "action.update", "n000")
	return b.updateEvent(b.spec.WarmupNS, "core.update", "n000", updateMember("n000", "n000", "upd-0000", "9", "alive"))
}

func TestDeliveryOrderedAcceptanceAndDeadline(t *testing.T) {
	b := newDeliveryEvidence(t)
	b.updateEvent(b.spec.WarmupNS, "core.datagram", "n001", updateMember("n001", "n000", "upd-0000", "10", "alive")) // before generation
	origin := b.generate()
	b.add(origin.TimeNS, updateMember("n001", "n000", "upd-0000", "8", "alive"), updateMember("n001", "n002", "upd-0000", "10", "alive"), updateMember("n001", "n000", "other", "10", "alive"))
	accepted := b.add(origin.TimeNS, updateMember("n001", "n000", "upd-0000", "10", "suspect"), updateMember("n001", "n000", "upd-0000", "11", "alive"))
	b.add(origin.TimeNS+deliveryWindowNS, updateMember("n002", "n000", "other", "10", "alive"))
	boundary := b.add(origin.TimeNS+deliveryWindowNS, updateMember("n002", "n000", "upd-0000", "9", "dead"))
	b.add(origin.TimeNS+deliveryWindowNS+1, updateMember("n003", "n000", "upd-0000", "10", "alive"))
	r := b.measure(t, true).UpdateDelivery
	u := r.Updates[0]
	if r.Status != "complete" || r.Scheduled != 1 || r.Generated != 1 || r.Delivered != 2 || r.Missed != 1 || r.EligiblePairs != 3 || r.NominalPairs != 3 || r.GenerationFailedPairs != 0 || r.IncompletePairs != 0 {
		t.Fatal(r)
	}
	if u.Action == nil || u.Generation == nil || *u.Generation.Event != origin || u.Generation.ChangeIndex == nil || *u.Generation.ChangeIndex != 0 || u.SourceLifetime != "1" || u.Status != "missed" || u.AllReceiversNS != nil {
		t.Fatal(u)
	}
	if p := u.Pairs[0]; p.Acceptance == nil || *p.Acceptance.Event != accepted || *p.Acceptance.ChangeIndex != 0 || *p.LatencyNS != 0 || p.Incarnation != "10" || p.ReceiverLifetime != "1" {
		t.Fatal("first valid acceptance", p)
	}
	if p := u.Pairs[1]; p.Acceptance == nil || *p.Acceptance.Event != boundary || *p.LatencyNS != deliveryWindowNS {
		t.Fatal("closed deadline", p)
	}
	if p := u.Pairs[2]; p.Status != "missed" || p.Acceptance != nil || p.LatencyNS != nil {
		t.Fatal("late acceptance", p)
	}
	if r.PairLatencies.Completed != 2 || *r.PairLatencies.P50NS != 0 || *r.PairLatencies.P95NS != deliveryWindowNS || r.AllReceiverTimes.Completed != 0 || r.AllReceiverTimes.P50NS != nil {
		t.Fatal("conditional quantiles", r)
	}
}

func TestDeliveryDeadlineClosureAndGeneration(t *testing.T) {
	for _, tc := range []struct {
		name               string
		advance            int64
		complete           bool
		missed, incomplete int
	}{
		{"before", deliveryWindowNS - 1, false, 0, 2}, {"equal-prefix", deliveryWindowNS, false, 0, 2}, {"after-prefix", deliveryWindowNS + 1, false, 2, 0}, {"complete", deliveryWindowNS, true, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newDeliveryEvidence(t)
			origin := b.generate()
			b.add(origin.TimeNS, updateMember("n001", "n000", "upd-0000", "9", "alive"))
			b.add(origin.TimeNS + tc.advance)
			if tc.complete {
				// A complete horizon closes this exact time; a prefix at the
				// same event time must leave later same-time changes possible.
				b.spec.HorizonNS = origin.TimeNS + deliveryWindowNS
				b.scenario.Horizon = core.Instant(b.spec.HorizonNS)
			}
			r := b.measure(t, tc.complete).UpdateDelivery
			if r.Delivered != 1 || r.Missed != tc.missed || r.IncompletePairs != tc.incomplete || r.EligiblePairs != r.Delivered+r.Missed+r.GenerationFailedPairs+r.IncompletePairs {
				t.Fatal(r)
			}
			if !tc.complete && r.Status != "incomplete" {
				t.Fatal("prefix became complete")
			}
		})
	}
	for _, tc := range []struct {
		name                                string
		action, execution, upsert, complete bool
		after                               bool
		want                                string
	}{
		{name: "missing-complete", complete: true, want: "failed"},
		{name: "unreached-action", want: "incomplete"},
		{name: "pending-core", action: true, want: "incomplete"},
		{name: "absent-core-closed", action: true, after: true, want: "failed"},
		{name: "executed-no-self", action: true, execution: true, want: "failed"},
		{name: "unrelated-self", upsert: true, want: "incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newDeliveryEvidence(t)
			b.add(b.spec.WarmupNS)
			if tc.action {
				b.updateEvent(b.spec.WarmupNS, "action.update", "n000")
			}
			if tc.execution {
				b.updateEvent(b.spec.WarmupNS, "core.update", "n000", member("n000", "n000", "alive"))
			}
			if tc.upsert {
				b.add(b.spec.WarmupNS, updateMember("n000", "n000", "upd-0000", "9", "alive"))
			}
			if tc.after {
				b.add(b.spec.WarmupNS + 1)
			}
			r := b.measure(t, tc.complete).UpdateDelivery
			if r.Updates[0].GenerationStatus != tc.want || r.Generated != 0 || r.Delivered != 0 || r.Missed != 0 {
				t.Fatal(r)
			}
			if tc.want == "failed" && (r.Failed != 1 || r.GenerationFailedPairs != 3) || tc.want == "incomplete" && (r.IncompleteUpdates != 1 || r.IncompletePairs != 3) {
				t.Fatal("partition", r)
			}
		})
	}
}

func TestDeliveryFrozenPopulationAndAllReceivers(t *testing.T) {
	b := newDeliveryEvidence(t)
	at := b.generate().TimeNS
	b.add(at, member("n001", "n000", "suspect"), member("n002", "n000", "dead"), trace.Change{Kind: "member.remove", Node: "n003", Subject: "n000"})
	for i := 1; i < 4; i++ {
		b.add(at+int64(i), updateMember(fmt.Sprintf("n%03d", i), "n000", "upd-0000", "10", "alive"))
	}
	r := b.measure(t, true).UpdateDelivery
	if r.EligiblePairs != 3 || r.Delivered != 3 || r.Missed != 0 || r.Updates[0].Status != "complete" || *r.Updates[0].AllReceiversNS != 3 || *r.AllReceiverTimes.MaxNS != 3 {
		t.Fatal(r)
	}
	for _, bad := range []string{"invalid", "unreached"} {
		t.Run(bad, func(t *testing.T) {
			b := newDeliveryEvidence(t)
			if bad == "invalid" {
				b.add(b.spec.WarmupNS-1, member("n001", "n000", "dead"))
				b.generate()
			}
			r := b.measure(t, bad == "invalid").UpdateDelivery
			if r.NominalPairs != 3 || r.EligiblePairs != 0 || r.DeliveredFraction != nil || r.Generated != 0 || r.Updates[0].Generation != nil || r.Updates[0].Pairs[0].Status != "unavailable" || (r.Status != "unavailable" && r.Status != "invalid-warmup") {
				t.Fatal(r)
			}
		})
	}
}

func TestDeliveryWorkloadValidation(t *testing.T) {
	for _, kind := range []string{"repeat-source", "reuse-token", "initial-token", "restart", "pause", "service", "late", "unordered"} {
		t.Run(kind, func(t *testing.T) {
			b := newDeliveryEvidence(t)
			a := b.scenario.Actions[0]
			switch kind {
			case "repeat-source":
				a.At++
				a.Data = []byte("upd-0001")
				b.scenario.Actions = append(b.scenario.Actions, a)
			case "reuse-token":
				a.At++
				a.Node = "n001"
				b.scenario.Actions = append(b.scenario.Actions, a)
			case "initial-token":
				b.scenario.Nodes[1].Identity.Meta = a.Data
			case "restart", "pause":
				b.scenario.Actions = append(b.scenario.Actions, sim.Action{At: a.At + 1, Node: a.Node, Kind: map[string]string{"restart": "start", "pause": "pause"}[kind]})
			case "service":
				b.scenario.Nodes[0].Service = 1
			case "late":
				b.scenario.Actions[0].At = b.scenario.Horizon
			case "unordered":
				a.At--
				a.Node = "n001"
				a.Data = []byte("upd-0001")
				b.scenario.Actions = append(b.scenario.Actions, a)
			}
			if err := validateSpec(b.spec, b.scenario); err == nil {
				t.Fatal("unsupported workload accepted")
			}
			b.spec.Case = "healthy" // Old cases must not inherit these restrictions.
			if err := validateSpec(b.spec, b.scenario); err != nil {
				t.Fatal("historical restrictions changed", err)
			}
		})
	}
	b := newDeliveryEvidence(t)
	b.generate()
	b.events[len(b.events)-1].Changes = append(b.events[len(b.events)-1].Changes, updateMember("n000", "n000", "upd-0000", "10", "alive"))
	raw := b.raw(t, true)
	base, err := report.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Measure(b.spec, b.scenario, base, bytes.NewReader(raw)); err == nil {
		t.Fatal("ambiguous origin accepted")
	}
}

func smallDeliveryFixture(t *testing.T) (Spec, sim.Scenario) {
	t.Helper()
	spec, s := smallFixture(t)
	spec.Case = "delivery-32-loss-0-exchange-on"
	s.Horizon = core.Instant(time.Duration(spec.WarmupNS) + 5*time.Second)
	spec.HorizonNS = int64(s.Horizon)
	s.Actions = append(s.Actions, sim.Action{At: core.Instant(spec.WarmupNS), Kind: "update", Node: "n000", Data: []byte("upd-0000")})
	s.Protocol.ProbeInterval = 0
	s.Protocol.PushPullInterval = 100 * time.Millisecond
	for _, from := range s.Nodes {
		for _, to := range s.Nodes {
			if from.Identity.Name != to.Identity.Name {
				s.Faults = append(s.Faults, sim.Fault{From: from.Identity.Name, To: to.Identity.Name, End: s.Horizon + 1, Drop: true})
			}
		}
	}
	return spec, s
}

func TestDeliveryRealStreamAcceptance(t *testing.T) {
	spec, s := smallDeliveryFixture(t)
	dir := filepath.Join(t.TempDir(), "attempt")
	if err := Run(spec, s, dir, nil); err != nil {
		t.Fatal(err)
	}
	r, err := Reduce(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.TrafficCost.WholeRun.Sent.DatagramCount == 0 || r.Traffic.Datagram.ReceivedBytes != 0 {
		t.Fatal("UDP suppression was not exercised", r.Traffic)
	}
	if r.UpdateDelivery.Delivered != 1 || r.UpdateDelivery.Generated != 1 || r.UpdateDelivery.Missed != 0 {
		t.Fatal(r.UpdateDelivery)
	}
	p := r.UpdateDelivery.Updates[0].Pairs[0]
	raw, err := os.ReadFile(filepath.Join(dir, "trace.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	_, err = trace.Read(bytes.NewReader(raw), func(raw json.RawMessage) error {
		var e trace.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.Type == "event" && eventRef(e) == *p.Acceptance.Event {
			c := e.Changes[*p.Acceptance.ChangeIndex]
			if e.Kind != "core.stream-message" || c.Kind != "member.upsert" || c.Node != p.Receiver || c.Member.Name != "n000" || string(c.Member.Meta) != "upd-0000" {
				t.Fatal("acceptance provenance", e)
			}
			found = true
		}
		return nil
	})
	if err != nil || !found || *p.LatencyNS < 0 || *p.LatencyNS > deliveryWindowNS {
		t.Fatal(found, p, err)
	}
}

func TestDeliveryFreshProcessReplay(t *testing.T) {
	if dir := os.Getenv("LIGHTPOOL_DELIVERY_TEST_CHILD"); dir != "" {
		spec, s := smallDeliveryFixture(t)
		if err := Run(spec, s, dir, nil); err != nil {
			t.Fatal(err)
		}
		return
	}
	var paths []string
	for _, procs := range []string{"1", "4"} {
		dir := filepath.Join(t.TempDir(), "attempt")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDeliveryFreshProcessReplay$")
		cmd.Env = append(slices.DeleteFunc(os.Environ(), func(s string) bool { return len(s) >= 11 && s[:11] == "GOMAXPROCS=" }), "GOMAXPROCS="+procs, "LIGHTPOOL_DELIVERY_TEST_CHILD="+dir)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatal(err, string(out))
		}
		paths = append(paths, dir)
	}
	for _, name := range []string{"spec.json", "scenario.json", "trace.ndjson", "report.json", "summary.json", "status.json"} {
		a, err := os.ReadFile(filepath.Join(paths[0], name))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(paths[1], name))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("fresh-process mismatch", name, err)
		}
	}
}

func TestDeliveryWithinEventChangeOrder(t *testing.T) {
	b := newDeliveryEvidence(t)
	b.updateEvent(b.spec.WarmupNS, "action.update", "n000")
	origin := b.updateEvent(b.spec.WarmupNS, "core.update", "n000",
		updateMember("n001", "n000", "upd-0000", "10", "alive"),
		updateMember("n000", "n000", "upd-0000", "9", "alive"),
		updateMember("n002", "n000", "upd-0000", "9", "alive"))
	r := b.measure(t, true).UpdateDelivery
	if r.Delivered != 1 || r.Missed != 2 || *r.Updates[0].Generation.ChangeIndex != 1 {
		t.Fatal(r)
	}
	p := r.Updates[0].Pairs[1]
	if *p.Acceptance.Event != origin || *p.Acceptance.ChangeIndex != 2 || *p.LatencyNS != 0 {
		t.Fatal(p)
	}
}

func TestDeliveryRunnerFailureAndRecovery(t *testing.T) {
	for _, kind := range []string{"event-budget", "invalid-warmup", "recording-close"} {
		t.Run(kind, func(t *testing.T) {
			spec, s := smallDeliveryFixture(t)
			switch kind {
			case "event-budget":
				s.EventBudget = 1
			case "invalid-warmup":
				s.Actions = s.Actions[1:]
			}
			dir := filepath.Join(t.TempDir(), "attempt")
			runErr := Run(spec, s, dir, nil)
			if (runErr == nil) != (kind == "recording-close") {
				t.Fatal("unexpected run status", runErr)
			}
			if kind == "recording-close" {
				if err := os.Remove(filepath.Join(dir, "status.json")); err != nil {
					t.Fatal(err)
				}
				if err := writeJSON(filepath.Join(dir, "status.json"), Status{Status: "incomplete", Reason: "recording close failed"}); err != nil {
					t.Fatal(err)
				}
			}
			out, err := Reduce(dir)
			if err != nil {
				t.Fatal(err)
			}
			if out.Status == "complete" || out.UpdateDelivery.Status == "complete" || out.UpdateDelivery.NominalPairs != 1 {
				t.Fatal("failed attempt became complete", out.UpdateDelivery)
			}
			path := filepath.Join(t.TempDir(), "remeasured.json")
			if err = WriteSummary(path, out); err != nil {
				t.Fatal(err)
			}
			if err = WriteSummary(path, out); !os.IsExist(err) {
				t.Fatal("overwrite not refused", err)
			}
			if kind == "recording-close" && (out.UpdateDelivery.Delivered != 1 || out.UpdateDelivery.Reason != "recording close failed") {
				t.Fatal("lost observed completion", out.UpdateDelivery)
			}
			if kind != "recording-close" && (out.UpdateDelivery.EligiblePairs != 0 || out.UpdateDelivery.DeliveredFraction != nil) {
				t.Fatal("invalid eligibility", out.UpdateDelivery)
			}
		})
	}
}
