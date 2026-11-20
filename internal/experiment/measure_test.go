package experiment

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

type evidenceBuilder struct {
	spec     Spec
	scenario sim.Scenario
	events   []trace.Event
}

func newEvidence(t *testing.T) *evidenceBuilder {
	t.Helper()
	spec, s, err := Fixture("threshold", 1, "metric-fixture")
	if err != nil {
		t.Fatal(err)
	}
	s.Nodes = s.Nodes[:4]
	s.Actions = nil
	s.Faults = nil
	s.Horizon = 100
	spec.WarmupNS = 10
	spec.HorizonNS = 100
	spec.Anomaly = []string{"n000"}
	b := &evidenceBuilder{spec: spec, scenario: s}
	for _, n := range s.Nodes {
		name := n.Identity.Name
		changes := []trace.Change{{Kind: "process", Node: name, Phase: "start", Process: &trace.Process{Node: name, Lifetime: "1", Running: true, PausedUntil: "0"}}}
		for _, subject := range s.Nodes {
			changes = append(changes, member(name, subject.Identity.Name, "alive"))
		}
		b.add(0, changes...)
	}
	return b
}

func member(o, s, state string) trace.Change {
	return trace.Change{Kind: "member.upsert", Node: o, Member: &trace.Member{Name: s, State: state, Incarnation: "1", Address: "192.0.2.1:7946"}}
}

func processChange(n, phase string, running bool, life string) trace.Change {
	return trace.Change{Kind: "process", Node: n, Phase: phase, Process: &trace.Process{Node: n, Lifetime: life, Running: running, PausedUntil: "0"}}
}

func timerChange(phase, generation, deadline string) trace.Change {
	return trace.Change{Kind: "timer", Node: "n001", Subject: "n003", ID: "1", Generation: generation, Operation: "0", Phase: phase, Purpose: "suspicion", Deadline: deadline}
}

func (b *evidenceBuilder) add(at int64, changes ...trace.Change) Reference {
	if changes == nil {
		changes = []trace.Change{}
	}
	seq := uint64(len(b.events) + 1)
	b.events = append(b.events, trace.Event{Type: "event", Seq: trace.Decimal(seq), ID: trace.Decimal(seq), Time: trace.Decimal(at), Lifetime: "1", Kind: "core.datagram", Changes: changes, Traffic: []trace.Traffic{}})
	return Reference{TimeNS: at, Seq: seq}
}

func (b *evidenceBuilder) traffic(at int64, transport, stage, id string, n int) {
	b.add(at)
	b.events[len(b.events)-1].Traffic = []trace.Traffic{{ID: id, Transport: transport, Stage: stage, From: "n000", To: "n001", Bytes: trace.Decimal(n), Messages: []string{"ping", "alive"}}}
}

func (b *evidenceBuilder) raw(t *testing.T, complete bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := trace.NewWriter(&buf)
	manifest, err := trace.Normalize(sim.Manifest{Code: b.scenario.Code, Toolchain: runtime.Version(), Platform: b.spec.Platform, Scenario: b.scenario})
	if err != nil {
		t.Fatal(err)
	}
	h := trace.Header{Type: "header", Format: "lightpool-trace", Version: 1, Manifest: manifest, Nodes: []trace.Node{}, Initial: []trace.Process{}}
	for _, n := range b.scenario.Nodes {
		h.Nodes = append(h.Nodes, trace.Node{Name: n.Identity.Name, Address: n.Identity.Address.String()})
		h.Initial = append(h.Initial, trace.Process{Node: n.Identity.Name, Lifetime: "0", PausedUntil: "0"})
	}
	if err = w.Write(h); err != nil {
		t.Fatal(err)
	}
	sent, received := 0, 0
	for _, e := range b.events {
		if err = w.Write(e); err != nil {
			t.Fatal(err)
		}
		for _, entry := range e.Traffic {
			var n int
			_, _ = fmt.Sscan(entry.Bytes, &n)
			if entry.Stage == "sent" {
				sent += n
			}
			if entry.Stage == "received" {
				received += n
			}
		}
	}
	if complete {
		end := trace.End{Type: "end", Seq: trace.Decimal(len(b.events)), Time: b.events[len(b.events)-1].Time, Complete: true, Reason: "horizon", Nodes: []trace.Measurement{}, Totals: trace.Totals{SentBytes: trace.Decimal(sent), ReceivedBytes: trace.Decimal(received), DatagramDrops: "0", Detections: "0"}}
		if err = w.Write(end); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func (b *evidenceBuilder) measure(t *testing.T, complete bool) *Summary {
	t.Helper()
	raw := b.raw(t, complete)
	base, err := report.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	out, err := Measure(b.spec, b.scenario, base, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func population(t *testing.T, s *Summary, label string) Population {
	t.Helper()
	for _, p := range s.Populations {
		if p.Label == label {
			return p
		}
	}
	t.Fatal("missing population", label)
	return Population{}
}

func TestFixedAnomalyEligibilityAndExposure(t *testing.T) {
	b := newEvidence(t)
	// n000 resumed before these declarations; it still belongs to the fixed set.
	b.add(10, trace.Change{Kind: "process", Node: "n000", Phase: "pause", Process: &trace.Process{Node: "n000", Lifetime: "1", Running: true, PausedUntil: "20"}})
	b.add(30, member("n001", "n000", "dead")) // outside FP numerator
	b.add(31, member("n000", "n001", "dead")) // FP, outside FP-minus
	b.add(32, member("n002", "n003", "dead")) // both
	b.add(33, member("n002", "n003", "dead")) // repeat is not an episode
	b.add(34, trace.Change{Kind: "member.remove", Node: "n002", Subject: "n003"})
	b.add(35, member("n002", "n003", "dead")) // new episode after removal
	b.add(40, member("n002", "n003", "alive"))
	s := b.measure(t, true)
	if s.Status != "complete" {
		t.Fatal(s.Status, s.WarmupFailures)
	}
	raw, fp, minus := population(t, s, "running-target-exclusions"), population(t, s, "FP-analogue"), population(t, s, "FP-minus-analogue")
	if raw.Transitions != 4 || fp.Transitions != 3 || minus.Transitions != 2 {
		t.Fatalf("fixed population filter: raw=%d FP=%d FP-minus=%d", raw.Transitions, fp.Transitions, minus.Transitions)
	}
	if fp.Targets != 2 || fp.Pairs != 2 || minus.Targets != 1 || minus.Pairs != 1 {
		t.Fatal("events versus distinct targets/pairs", fp, minus)
	}
	if raw.PairNS != 12*90 || fp.PairNS != 9*90 || minus.PairNS != 6*90 {
		t.Fatal("belief or pause reduced exposure", raw.PairNS, fp.PairNS, minus.PairNS)
	}
	if *fp.FractionEverExcluded != "0.222222222" {
		t.Fatal(*fp.FractionEverExcluded)
	}
	if s.Traffic.ActiveNodeNS != 4*90 {
		t.Fatal("paused process removed from active denominator")
	}
	if len(s.Recoveries) != 4 || s.RecoveryDurations.Completed != 2 {
		t.Fatal("removal/recovery episodes", s.Recoveries)
	}
}

func TestHealthyPairEligibilityIncludesOnlyPredeclaredOT(t *testing.T) {
	b := newEvidence(t)
	b.spec.Case = "correlated"
	b.spec.Target = "n003"
	b.spec.HealthyPairs = []EligiblePair{{"n001", "n003"}, {"n002", "n003"}}
	b.scenario.Faults = []sim.Fault{{From: "n000", To: "n003", Start: 10, End: 50, Drop: true, StreamFailure: true}}
	b.add(20, member("n000", "n003", "dead"))
	b.add(21, member("n001", "n003", "dead"))
	b.add(30, member("n001", "n003", "alive"))
	b.add(40, member("n001", "n003", "dead"))
	s := b.measure(t, true)
	p := population(t, s, "healthy-pair-exclusions")
	if p.Transitions != 2 || p.Pairs != 1 || p.Targets != 1 || p.PairNS != 180 || len(p.Exposure) != 2 {
		t.Fatalf("O→T eligibility filter: %+v", p)
	}
	if population(t, s, "running-target-exclusions").Transitions != 3 {
		t.Fatal("impaired A→T declaration disappeared")
	}
	// An outcome cannot justify calling the impaired direct pair healthy.
	b.spec.HealthyPairs = append(b.spec.HealthyPairs, EligiblePair{"n000", "n003"})
	raw := b.raw(t, true)
	base, err := report.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Measure(b.spec, b.scenario, base, bytes.NewReader(raw)); err == nil {
		t.Fatal("accepted impaired healthy cohort")
	}
}

func TestCrashSuspicionDeadAndFullCohortCensoring(t *testing.T) {
	b := newEvidence(t)
	b.spec.Case = "crash"
	// Membership must be complete before warmup; these exclusions occur afterward.
	b.add(11, member("n000", "n003", "dead"))
	b.add(12, processChange("n002", "crash", false, "1"))
	crash := b.add(20, processChange("n003", "crash", false, "1"))
	suspect := b.add(22, member("n001", "n003", "suspect"))
	dead := b.add(25, member("n001", "n003", "dead"))
	s := b.measure(t, true)
	c := s.Crashes[1]
	if c.Reference != crash || c.AlreadyDead != 1 || c.Inactive != 1 || len(c.Observations) != 1 || c.FullStatus != "complete" || *c.FullLatencyNS != 5 || *c.FirstSuspicion != suspect || c.First == nil || *c.First != dead {
		t.Fatalf("crash cohort: %+v", c)
	}
	if *c.Suspicions[0].LatencyNS != 2 || c.Latencies.Completed != 1 || c.SuspicionLatencies.Completed != 1 {
		t.Fatal(c)
	}
	// A rumor can bypass suspicion entirely; do not impute it from the dead event.
	b2 := newEvidence(t)
	b2.spec.Case = "crash"
	b2.add(20, processChange("n003", "crash", false, "1"))
	b2.add(25, member("n000", "n003", "dead"))
	b2.add(26, processChange("n000", "crash", false, "1")) // after its own detection, before full dissemination
	b2.add(27, member("n001", "n003", "dead"))
	b2.add(28, member("n002", "n003", "dead"))
	c = b2.measure(t, true).Crashes[0]
	if c.FirstSuspicion != nil || c.SuspicionLatencies.Completed != 0 || c.FullDissemination != nil || c.FullStatus != "observer-interrupted" || c.Latencies.Completed != 3 {
		t.Fatalf("shrunk cohort or imputed suspicion: %+v", c)
	}
}

func TestEqualTimeWarmupAndIncompletePrefix(t *testing.T) {
	b := newEvidence(t)
	b.add(10, processChange("n003", "crash", false, "1"))
	b.add(10, member("n001", "n003", "dead"))
	s := b.measure(t, true)
	if s.Status != "complete" || len(s.WarmupFailures) != 0 || s.Crashes[0].Latencies.Completed != 1 {
		t.Fatal("warm-up happened after equal-time intervention", s.Status)
	}
	// 12 pairs for 0 ns, then 6 pairs for 90 ns; dead beliefs never alter it.
	if population(t, s, "running-target-exclusions").PairNS != 6*90 {
		t.Fatal("equal-time exposure")
	}
	b2 := newEvidence(t)
	b2.add(9, member("n001", "n003", "dead"))
	b2.add(10)
	invalid := b2.measure(t, true)
	if invalid.Status != "invalid-warmup" || len(invalid.WarmupFailures) != 1 {
		t.Fatal("warmup was not authoritative", invalid.Status, invalid.WarmupFailures)
	}
	b3 := newEvidence(t)
	b3.add(20, member("n001", "n003", "dead"))
	prefix := b3.measure(t, false)
	if prefix.Status != "incomplete" || prefix.ObservedThroughNS != 20 || prefix.RecoveryDurations.Completed != 0 || prefix.Recoveries[0].End.TimeNS != 20 {
		t.Fatal("prefix claimed horizon completion", prefix)
	}
}

func TestWindowedFramedTrafficCountsMessagesOnce(t *testing.T) {
	b := newEvidence(t)
	b.traffic(9, "datagram", "sent", "1", 50)
	b.traffic(10, "datagram", "sent", "2", 100)
	b.traffic(11, "datagram", "received", "2", 100)
	b.traffic(12, "stream", "attempt", "3", 250)
	b.traffic(13, "stream", "sent", "3", 100)
	b.traffic(14, "stream", "sent", "3", 100)
	b.traffic(15, "stream", "sent", "3", 50)
	b.traffic(16, "stream", "received", "3", 250)
	b.traffic(17, "datagram", "drop", "2", 100)
	b.add(20, processChange("n003", "crash", false, "1"))
	s := b.measure(t, true)
	if s.Traffic.Datagram.Messages != 1 || s.Traffic.Stream.Messages != 1 || s.Traffic.Datagram.SentBytes != 100 || s.Traffic.Stream.SentBytes != 250 || s.Traffic.ActiveNodeNS != 4*10+3*80 {
		t.Fatalf("traffic %+v", s.Traffic)
	}
	if s.Traffic.Datagram.DropRecords != 1 || s.Traffic.Datagram.DroppedBytes != 100 {
		t.Fatal("dropped traffic lost")
	}
	if *s.Traffic.SentBytesPerNodeSecond != "1250000000.000000000" {
		t.Fatal(*s.Traffic.SentBytesPerNodeSecond)
	}
}

func TestMechanismDistinguishesTimerExpiryFromDeadRumor(t *testing.T) {
	b := newEvidence(t)
	start := b.add(20, member("n001", "n003", "suspect"), trace.Change{Kind: "suspicion", Node: "n001", Subject: "n003", Outcome: "started:n000"}, timerChange("arm", "1", "80"))
	b.add(25, trace.Change{Kind: "suspicion", Node: "n001", Subject: "n003", Outcome: "confirmation:n002"}, timerChange("cancel", "1", "80"), timerChange("arm", "2", "30"))
	b.add(27, member("n003", "n003", "alive"), trace.Change{Kind: "refutation", Node: "n003", Subject: "n003"})
	dead := b.add(30, timerChange("fire", "2", "30"), member("n001", "n003", "dead"))
	b.add(31, member("n002", "n003", "dead"))
	s := b.measure(t, true)
	if len(s.Mechanisms) != 1 {
		t.Fatal(s.Mechanisms)
	}
	m := s.Mechanisms[0]
	if m.Origin.Reference != start || m.End != dead || len(m.Confirmations) != 1 || len(m.Timers) != 4 || len(m.Refutations) != 1 || m.DeadCause != "local-suspicion-expiry" {
		t.Fatalf("witness %+v", m)
	}
	if s.Episodes[0].Cause != "local-suspicion-expiry" || s.Episodes[1].Cause != "received-or-other-dead-transition" {
		t.Fatal("rumor credited as timer expiry", s.Episodes)
	}
}

func TestRestartCannotCompleteAnEarlierLifetimeCohort(t *testing.T) {
	b := newEvidence(t)
	b.spec.Case = "crash"
	b.add(20, processChange("n003", "crash", false, "1"))
	b.add(25, processChange("n001", "crash", false, "1"))
	b.add(30, processChange("n001", "start", true, "2"))
	b.add(35, member("n001", "n003", "dead"))
	b.add(40, member("n000", "n003", "dead"))
	s := b.measure(t, true)
	c := s.Crashes[0]
	if c.FullDissemination != nil || c.FullStatus != "observer-interrupted" || c.Latencies.Completed != 1 || c.Latencies.Eligible != 3 {
		t.Fatalf("restart completed old cohort: %+v", c)
	}
	for _, o := range c.Observations {
		if o.Observer == "n001" && o.Status != "observer_crash" {
			t.Fatal("old lifetime re-entered", o)
		}
	}
}

func TestPreexistingSuspicionIsNotInventedAsCrashLatency(t *testing.T) {
	b := newEvidence(t)
	b.spec.Case = "crash"
	prior := b.add(15, member("n001", "n003", "suspect"))
	b.add(20, processChange("n003", "crash", false, "1"))
	b.add(25, member("n001", "n003", "dead"))
	c := b.measure(t, true).Crashes[0]
	for _, s := range c.Suspicions {
		if s.Observer == "n001" && (s.First == nil || *s.First != prior || s.LatencyNS != nil || s.Status != "preexisting-suspicion") {
			t.Fatal("preexisting suspicion relabeled", s)
		}
	}
	if c.FirstSuspicion != nil || c.SuspicionLatencies.Completed != 0 {
		t.Fatal("pre-crash suspicion became post-crash completion")
	}
}
