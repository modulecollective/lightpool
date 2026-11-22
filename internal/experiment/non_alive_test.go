package experiment

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

func beliefPair(t *testing.T, s *Summary) BeliefPair {
	t.Helper()
	for _, p := range s.NonAlive.Pairs {
		if p.Observer == "n001" && p.Subject == "n003" {
			return p
		}
	}
	t.Fatal("missing enrolled pair")
	return BeliefPair{}
}

func beliefPopulation(t *testing.T, s *Summary, label string) BeliefPopulation {
	t.Helper()
	for _, p := range s.NonAlive.Populations {
		if p.Label == label {
			return p
		}
	}
	t.Fatal("missing duration population", label)
	return BeliefPopulation{}
}

func assertBeliefConservation(t *testing.T, s *Summary) {
	t.Helper()
	check := func(b BeliefTime) {
		if b.ObservedNS != b.AliveNS+b.SuspectNS+b.DeadNS+b.LeftNS+b.AbsentNS || b.NotAliveNS != b.SuspectNS+b.DeadNS+b.LeftNS+b.AbsentNS || b.SuspectDeadNS != b.SuspectNS+b.DeadNS {
			t.Fatalf("state-time conservation: %+v", b)
		}
	}
	var previous string
	for _, p := range s.NonAlive.Pairs {
		key := p.Observer + "/" + p.Subject
		if key <= previous {
			t.Fatal("unstable pair order", previous, key)
		}
		previous = key
		check(p.BeliefTime)
		if p.ObservedPairNS != p.ObservedNS || p.ObservedPairNS > p.NominalPairNS {
			t.Fatal("exposure", p)
		}
		var ns int64
		for _, e := range s.NonAlive.Episodes {
			if e.Pair == p.Pair {
				ns += e.ObservedNS
			}
		}
		if ns != p.NotAliveNS {
			t.Fatal("episode/pair conservation", ns, p.NotAliveNS)
		}
	}
	for _, p := range s.NonAlive.Populations {
		check(p.BeliefTime)
	}
	for _, e := range s.NonAlive.Episodes {
		check(e.BeliefTime)
		var ns int64
		at := e.ObservationStart
		for _, segment := range e.Segments {
			if segment.Start != at || segment.End.TimeNS-segment.Start.TimeNS != segment.ObservedNS {
				t.Fatal("discontinuous segment", segment)
			}
			ns += segment.ObservedNS
			at = segment.End
		}
		if ns != e.ObservedNS || at != e.End || (e.Status == "censored" && e.DurationNS != nil) {
			t.Fatal("segment/episode conservation", e)
		}
	}
}

func TestNonAliveArithmeticAndReferences(t *testing.T) {
	b := newEvidence(t)
	start := b.add(20, member("n001", "n003", "suspect"))
	end := b.add(40, member("n001", "n003", "alive"))
	b.add(60, member("n001", "n003", "dead"))
	b.add(70, member("n001", "n003", "alive"))
	s := b.measure(t, true)
	p := beliefPair(t, s)
	if p.ObservedPairNS != 90 || p.AliveNS != 60 || p.SuspectNS != 20 || p.DeadNS != 10 || p.Starts != 2 || p.Completed != 2 || p.PairsEverDead != 1 || *p.Durations.P50NS != 10 || *p.Durations.P95NS != 20 || *p.NotAliveFraction != "0.333333333" {
		t.Fatalf("independent 90 ns budget / two episodes: %+v", p)
	}
	e := s.NonAlive.Episodes[0]
	if *e.Onset.Event != start || *e.End.Event != end || *e.Onset.ChangeIndex != 0 || *e.DurationNS != 20 || e.ObservationStart.Kind != "change" {
		t.Fatal("transition references", e)
	}
	if s.Format != SummaryVersion || s.Spec.Format != Version {
		t.Fatal("input/output version conflation")
	}
	raw, err := json.Marshal(s.NonAlive)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"durationNS":"20"`)) || !bytes.Contains(raw, []byte(`"firstDead":null`)) || !bytes.Contains(raw, []byte(`"changeIndex":"0"`)) {
		t.Fatal("integer/null JSON", string(raw))
	}
	assertBeliefConservation(t, s)
}

func TestNonAliveStateSegmentsDoNotResetOnConfirmationOrIncarnation(t *testing.T) {
	b := newEvidence(t)
	onset := b.add(20, member("n001", "n003", "suspect"))
	higher := member("n001", "n003", "suspect")
	higher.Member.Incarnation = "2"
	b.add(25, trace.Change{Kind: "suspicion", Node: "n001", Subject: "n003", Outcome: "confirmation:n002"}, higher)
	b.add(30, member("n001", "n003", "dead"))
	b.add(35, member("n001", "n003", "suspect"))
	b.add(40, member("n001", "n003", "dead"))
	b.add(50, member("n001", "n003", "alive"))
	s := b.measure(t, true)
	p, e := beliefPair(t, s), s.NonAlive.Episodes[0]
	if p.Starts != 1 || p.SuspectNS != 15 || p.DeadNS != 15 || *e.DurationNS != 30 || *e.Onset.Event != onset || len(e.Segments) != 4 || e.FirstDead.TimeNS != 30 {
		t.Fatal("episode reset on upsert/fact", p, e)
	}
	assertBeliefConservation(t, s)
}

func TestNonAliveRemovalLeftAndCensoring(t *testing.T) {
	for _, recover := range []bool{false, true} {
		b := newEvidence(t)
		b.add(20, member("n001", "n003", "suspect"))
		b.add(30, member("n001", "n003", "dead"))
		b.add(40, trace.Change{Kind: "member.remove", Node: "n001", Subject: "n003"})
		if recover {
			b.add(60, member("n001", "n003", "alive"))
		}
		s := b.measure(t, true)
		p, e := beliefPair(t, s), s.NonAlive.Episodes[0]
		if p.ObservedPairNS != 90 || p.SuspectNS != 10 || p.DeadNS != 10 || len(e.Segments) != 3 || e.FirstDead.TimeNS != 30 {
			t.Fatal(p, e)
		}
		if recover {
			if p.AbsentNS != 20 || *e.DurationNS != 40 || e.Status != "completed" {
				t.Fatal(e)
			}
		} else if p.AbsentNS != 60 || e.DurationNS != nil || e.CensorReason != "horizon" || e.ObservedNS != 80 || e.End.Kind != "horizon" {
			t.Fatal(e)
		}
		assertBeliefConservation(t, s)
	}
	b := newEvidence(t)
	b.add(20, trace.Change{Kind: "member.remove", Node: "n001", Subject: "n003"})
	b.add(30, member("n001", "n003", "left")) // belief, not process leave
	b.add(40, member("n001", "n003", "alive"))
	s := b.measure(t, true)
	p, e := beliefPair(t, s), s.NonAlive.Episodes[0]
	if p.AbsentNS != 10 || p.LeftNS != 10 || p.ObservedPairNS != 90 || e.Cause != "removed" || *e.DurationNS != 20 {
		t.Fatal(p, e)
	}
	assertBeliefConservation(t, s)
}

func TestNonAlivePrefixAndWindowBoundaries(t *testing.T) {
	b := newEvidence(t)
	b.add(20, member("n001", "n003", "suspect"))
	last := b.add(35)
	s := b.measure(t, false)
	p, e := beliefPair(t, s), s.NonAlive.Episodes[0]
	if s.Status != "incomplete" || p.ObservedPairNS != 25 || p.NominalPairNS != 90 || e.ObservedNS != 15 || e.CensorReason != "prefix" || *e.End.Event != last || e.DurationNS != nil {
		t.Fatal(p, e)
	}
	assertBeliefConservation(t, s)
	// A partial trailing alive record never completes the open episode.
	raw := append(b.raw(t, false), []byte(`{"type":"event","changes":[{"state":"alive"`)...)
	base, err := report.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	partial, err := Measure(b.spec, b.scenario, base, bytes.NewReader(raw))
	if err != nil || partial.NonAlive.Episodes[0].Status != "censored" {
		t.Fatal("partial alive became recovery", err)
	}
	if _, err = report.Read(bytes.NewReader(append(raw, '\n'))); err == nil {
		t.Fatal("accepted malformed committed line")
	}
	b = newEvidence(t)
	b.add(10, member("n001", "n003", "suspect"))
	b.add(100, member("n001", "n003", "alive"), member("n001", "n003", "suspect"))
	s = b.measure(t, true)
	if *s.NonAlive.Episodes[0].DurationNS != 90 || s.NonAlive.Episodes[1].ObservedNS != 0 || s.NonAlive.Episodes[1].CensorReason != "horizon" || *s.NonAlive.Episodes[1].Onset.ChangeIndex != 1 {
		t.Fatal("W/H transition ordering", s.NonAlive.Episodes)
	}
	assertBeliefConservation(t, s)
	b = newEvidence(t)
	b.add(20, member("n001", "n003", "suspect"), member("n001", "n003", "alive"))
	s = b.measure(t, true)
	e = s.NonAlive.Episodes[0]
	if *e.DurationNS != 0 || *e.Onset.ChangeIndex != 0 || *e.End.ChangeIndex != 1 || beliefPair(t, s).PairsEverNonAlive != 1 {
		t.Fatal("zero-time episode lost", e)
	}
	assertBeliefConservation(t, s)
}

func TestNonAliveIncompletePastHorizon(t *testing.T) {
	for _, tc := range []struct {
		name    string
		changes []trace.Change
	}{
		{name: "empty-event"},
		{name: "alive", changes: []trace.Change{member("n001", "n003", "alive")}},
		{name: "dead", changes: []trace.Change{member("n001", "n003", "dead")}},
		{name: "observer-crash", changes: []trace.Change{processChange("n001", "crash", false, "1")}},
		{name: "target-crash", changes: []trace.Change{processChange("n003", "crash", false, "1")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newEvidence(t)
			b.add(20, member("n001", "n003", "suspect"))
			last := b.add(110, tc.changes...)
			s := b.measure(t, false) // Decode and reduce the actual incomplete trace.
			p, e := beliefPair(t, s), s.NonAlive.Episodes[0]
			if s.Status != "incomplete" || s.LastEvent != last {
				t.Fatal("lost incomplete evidence status/reference", s.Status, s.LastEvent)
			}
			if p.ObservedPairNS != 90 || p.NominalPairNS != 90 || p.AliveNS != 10 || p.SuspectNS != 80 || p.DeadNS != 0 || len(p.Interruptions) != 0 {
				t.Fatal("prefix accounting exceeded [W,H)", p)
			}
			if e.Status != "censored" || e.CensorReason != "prefix" || e.DurationNS != nil || e.ObservedNS != 80 || e.End.Kind != "prefix" || e.End.TimeNS != 100 || *e.End.Event != last {
				t.Fatal("prefix episode exceeded H or used a post-H change", e)
			}
			for _, pop := range s.NonAlive.Populations {
				if pop.ObservedPairNS > pop.NominalPairNS {
					t.Fatal("population exceeded nominal exposure", pop)
				}
			}
			assertBeliefConservation(t, s)
		})
	}
}

func TestNonAliveWarmupDiagnosticsAndNulls(t *testing.T) {
	b := newEvidence(t)
	onset := b.add(5, member("n001", "n003", "suspect"))
	b.add(20, member("n001", "n003", "alive"))
	s := b.measure(t, true)
	e, p := s.NonAlive.Episodes[0], beliefPair(t, s)
	if s.Status != "invalid-warmup" || !e.CarryIn || *e.Onset.Event != onset || e.ObservationStart.Kind != "window" || e.ObservationStart.TimeNS != 10 || e.ObservedNS != 10 || e.DurationNS != nil || p.Starts != 0 || p.CarryIns != 1 || p.Durations.P50NS != nil {
		t.Fatal("carry-in clipping", s.Status, e, p)
	}
	assertBeliefConservation(t, s)
	b = newEvidence(t)
	b.add(5, member("n001", "n003", "suspect"))
	b.add(9, member("n001", "n003", "alive"))
	s = b.measure(t, true)
	if s.Status != "complete" || len(s.NonAlive.Episodes) != 0 || beliefPair(t, s).AliveNS != 90 {
		t.Fatal("pre-window recovery leaked")
	}
	empty := beliefPopulation(t, s, "healthy-pair-exclusions")
	if empty.EnrolledPairs != 0 || empty.NotAliveFraction != nil || empty.Durations.Reason != "empty eligible population" {
		t.Fatal("empty cohort nulls", empty)
	}
	rawPop := beliefPopulation(t, s, "running-target-exclusions")
	if *rawPop.NotAliveFraction != "0.000000000" || rawPop.ObservedPairNS != 1080 {
		t.Fatal("all-alive valid zero", rawPop)
	}
	b = newEvidence(t)
	b.add(5)
	s = b.measure(t, false)
	if s.Status != "incomplete" || len(s.NonAlive.Pairs) != 12 || beliefPair(t, s).ObservedPairNS != 0 || beliefPair(t, s).NotAliveFraction != nil || beliefPair(t, s).NominalPairNS != 90 {
		t.Fatal("incomplete before W", s.NonAlive)
	}
	b = newEvidence(t)
	b.add(9, trace.Change{Kind: "member.remove", Node: "n001", Subject: "n003"})
	b.add(10)
	s = b.measure(t, true)
	if s.Status != "invalid-warmup" || !s.NonAlive.Episodes[0].CarryIn || beliefPair(t, s).AbsentNS != 90 {
		t.Fatal("missing warmup membership", s.NonAlive)
	}
	assertBeliefConservation(t, s)
}

func TestNonAliveEndpointLifetimesAndRecordedOrder(t *testing.T) {
	for _, role := range []string{"observer", "target"} {
		for _, reason := range []string{"crash", "restart", "shutdown", "leave"} {
			for _, aliveFirst := range []bool{false, true} {
				t.Run(role+"/"+reason+"/"+map[bool]string{true: "alive-first", false: "lifecycle-first"}[aliveFirst], func(t *testing.T) {
					b := newEvidence(t)
					node := "n001"
					if role == "target" {
						node = "n003"
					}
					b.add(20, member("n001", "n003", "suspect"))
					change := processChange(node, "crash", false, "1")
					switch reason {
					case "restart":
						change = processChange(node, "start", true, "2")
					case "shutdown":
						change = trace.Change{Kind: "summary", Node: node, Summary: &trace.Summary{Stopped: true, Health: "0", Backlog: "0", PendingStreams: "0", Stats: map[string]string{}}}
					case "leave":
						change = trace.Change{Kind: "summary", Node: node, Summary: &trace.Summary{Left: true, Health: "0", Backlog: "0", PendingStreams: "0", Stats: map[string]string{}}}
					}
					alive := member("n001", "n003", "alive")
					if aliveFirst {
						b.add(30, alive, change)
					} else {
						b.add(30, change, alive)
					}
					b.add(40, processChange(node, "start", true, "3"))
					b.add(50, member("n001", "n003", "suspect"))
					b.add(60, alive)
					s := b.measure(t, true)
					p, e := beliefPair(t, s), s.NonAlive.Episodes[0]
					if p.ObservedPairNS != 20 || p.Starts != 1 || p.SuspectNS != 10 || len(p.Interruptions) != 1 || p.Interruptions[0].Reason != reason || p.ObserverLifetime != "1" || p.TargetLifetime != "1" {
						t.Fatal("replacement reentered", p)
					}
					if aliveFirst {
						if e.Status != "completed" || *e.DurationNS != 10 {
							t.Fatal("later interruption undid completion", e)
						}
					} else if e.Status != "censored" || e.CensorReason != role+"-"+reason || e.DurationNS != nil {
						t.Fatal("lifecycle censor/order", e)
					}
					assertBeliefConservation(t, s)
				})
			}
		}
	}
}

func TestNonAliveFixedHealthyAndAnomalyPopulations(t *testing.T) {
	b := newEvidence(t)
	b.spec.HealthyPairs = []EligiblePair{{"n001", "n003"}}
	b.add(10, trace.Change{Kind: "process", Node: "n000", Phase: "pause", Process: &trace.Process{Node: "n000", Lifetime: "1", Running: true, PausedUntil: "15"}})
	for _, o := range []string{"n001", "n000", "n003"} {
		b.add(20, member(o, "n003", "suspect"))
	}
	for _, o := range []string{"n001", "n000", "n003"} {
		b.add(30, member(o, "n003", "alive"))
	}
	// Also exercise the fixed anomalous target after it resumed.
	b.add(40, member("n002", "n000", "suspect"))
	b.add(50, member("n002", "n000", "alive"))
	s := b.measure(t, true)
	healthy := beliefPopulation(t, s, "healthy-pair-exclusions")
	raw := beliefPopulation(t, s, "running-target-exclusions")
	fp := beliefPopulation(t, s, "FP-analogue")
	minus := beliefPopulation(t, s, "FP-minus-analogue")
	if healthy.EnrolledPairs != 1 || healthy.ObservedPairNS != 90 || healthy.SuspectNS != 10 || healthy.Starts != 1 || raw.SuspectNS != 30 || raw.Starts != 3 || fp.SuspectNS != 20 || minus.SuspectNS != 10 || raw.ObservedPairNS != 1080 {
		t.Fatal("fixed healthy/anomaly/self filtering", healthy, raw, fp, minus)
	}
	assertBeliefConservation(t, s)
	for _, impairment := range []string{"path", "pause-observer", "pause-target"} {
		t.Run(impairment, func(t *testing.T) {
			bad := newEvidence(t)
			bad.spec.HealthyPairs = slices.Clone(b.spec.HealthyPairs)
			if impairment == "path" {
				bad.scenario.Faults = []sim.Fault{{From: "n003", To: "n001", Start: 10, End: 50, LossPPM: 1}}
			} else {
				node := "n001"
				if impairment == "pause-target" {
					node = "n003"
				}
				bad.scenario.Actions = []sim.Action{{At: 5, Kind: "pause", Node: node, Duration: 10}}
			}
			raw := bad.raw(t, true)
			base, err := report.Read(bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Measure(bad.spec, bad.scenario, base, bytes.NewReader(raw)); err == nil {
				t.Fatal("accepted inconsistent healthy declaration")
			}
		})
	}
}

func TestNonAliveInactiveWarmupAndUnstartedPair(t *testing.T) {
	b := newEvidence(t)
	b.add(5, member("n001", "n003", "suspect"))
	b.add(6, processChange("n001", "crash", false, "1"))
	b.add(10)
	s := b.measure(t, true)
	p := beliefPair(t, s)
	if s.Status != "invalid-warmup" || p.ObservedPairNS != 0 || p.CarryIns != 1 || len(p.Interruptions) != 1 || s.NonAlive.Episodes[0].ObservedNS != 0 || s.NonAlive.Episodes[0].CensorReason != "observer-inactive-at-enrollment" {
		t.Fatal("inactive diagnostic", p, s.NonAlive.Episodes)
	}
	assertBeliefConservation(t, s)
	b = newEvidence(t)
	b.events = b.events[:3] // target never starts or becomes known at other nodes
	for i := range b.events {
		b.events[i].Changes = slices.DeleteFunc(b.events[i].Changes, func(c trace.Change) bool { return c.Member != nil && c.Member.Name == "n003" })
	}
	b.add(10)
	s = b.measure(t, true)
	if s.Status != "invalid-warmup" || beliefPair(t, s).ObservedPairNS != 0 || beliefPair(t, s).EnrolledPairs != 1 {
		t.Fatal("missing endpoint changed enrollment", s.NonAlive)
	}
	assertBeliefConservation(t, s)
}
