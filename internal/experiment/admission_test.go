package experiment

import (
	"bytes"
	"cmp"
	"slices"
	"strconv"
	"testing"

	"github.com/modulecollective/lightpool/sim/trace"
)

func admissionTimer(phase, id, deadline string) trace.Change {
	return trace.Change{Kind: "timer", Node: "n002", Subject: "n003", ID: id, Generation: id, Operation: "0", Phase: phase, Purpose: "suspicion", Deadline: deadline}
}
func admissionSuspicion(node, outcome string) trace.Change {
	return trace.Change{Kind: "suspicion", Node: node, Subject: "n003", Outcome: outcome}
}
func admissionPositive(t *testing.T) *evidenceBuilder {
	t.Helper()
	b := newEvidence(t)
	b.spec.Case, b.spec.Target = "correlated-dense", "n003"
	b.spec.Anomaly = []string{"n000", "n001"}
	b.spec.HealthyPairs = []EligiblePair{{"n002", "n003"}}
	for _, node := range b.spec.Anomaly {
		b.add(11, trace.Change{Kind: "probe", Node: node, Subject: "n003", ID: "10", Phase: "direct"})
	}
	for _, node := range b.spec.Anomaly {
		b.add(12, trace.Change{Kind: "probe", Node: node, Subject: "n003", ID: "10", Phase: "indirect", Helpers: []string{"n002"}})
	}
	for _, node := range b.spec.Anomaly {
		b.add(13, trace.Change{Kind: "probe", Node: node, Subject: "n003", ID: "10", Phase: "finished", Outcome: "timeout"}, member(node, "n003", "suspect"), admissionSuspicion(node, "started:"+node))
	}
	b.add(20, member("n002", "n003", "suspect"), admissionSuspicion("n002", "started:n000"), admissionTimer("arm", "21", "90"))
	b.add(30, admissionSuspicion("n002", "confirmation:n001"), admissionTimer("cancel", "21", "90"), admissionTimer("arm", "22", "50"))
	b.add(50, admissionTimer("fire", "22", "50"), member("n002", "n003", "dead"))
	refute := member("n003", "n003", "alive")
	refute.Member.Incarnation = "2"
	b.add(60, refute, trace.Change{Kind: "refutation", Node: "n003", Subject: "n003"})
	accept := member("n002", "n003", "alive")
	accept.Member.Incarnation = "2"
	b.add(70, accept)
	return b
}
func classifyEvidence(t *testing.T, b *evidenceBuilder, complete bool) *Admission {
	t.Helper()
	summary := b.measure(t, complete)
	out, err := measureAdmission(summary, bytes.NewReader(b.raw(t, complete)))
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func observerEpisode(t *testing.T, out *Admission) *AdmissionEpisode {
	t.Helper()
	for _, ep := range out.Episodes {
		if ep.Observer == "n002" {
			return ep
		}
	}
	t.Fatal("missing O episode")
	return nil
}
func mutateChanges(b *evidenceBuilder, mutate func(*trace.Change)) {
	for i := range b.events {
		for j := range b.events[i].Changes {
			mutate(&b.events[i].Changes[j])
		}
	}
}
func TestAdmissionPositiveAccelerationAndPostDeathRefutation(t *testing.T) {
	out := classifyEvidence(t, admissionPositive(t), true)
	ep := observerEpisode(t, out)
	if out.Witnesses != 1 || out.Harmful != 1 || out.AFailures != 2 || out.AOrigins != 2 || out.EligibleEpisodes != 1 || !ep.Acceleration || !ep.Harmful {
		t.Fatalf("positive chain not admitted: %+v %+v", out, ep)
	}
	if ep.Refutation.TimeNS != 60 || ep.Acceptance.TimeNS != 70 || ep.End.TimeNS != 50 || ep.DeadCause != "local-suspicion-expiry" || ep.Refutation.ChangeIndex != 1 {
		t.Fatal("ordering/provenance", ep)
	}
	if ep.Origin.Probe.ID != "10" || ep.Origin.Probe.Incarnation != "1" || len(ep.Origin.Probe.Steps) != 3 || ep.Shortenings[0].Previous.Fact.Generation != "21" || ep.Shortenings[0].Arm.Fact.Generation != "22" {
		t.Fatal("lost evidence", ep)
	}
}
func TestAdmissionRejectsUnprovenAcceleration(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*evidenceBuilder)
	}{
		{"duplicate-sender", func(b *evidenceBuilder) {
			mutateChanges(b, func(c *trace.Change) {
				if c.Outcome == "confirmation:n001" {
					c.Outcome = "confirmation:n000"
				}
			})
		}},
		{"cross-group-confirmation", func(b *evidenceBuilder) { b.spec.Anomaly = []string{"n000"} }},
		{"cross-group-origin", func(b *evidenceBuilder) { b.spec.Anomaly = []string{"n001"} }},
		{"noneligible-observer", func(b *evidenceBuilder) { b.spec.HealthyPairs = nil }},
		{"missing-failed-probe", func(b *evidenceBuilder) {
			mutateChanges(b, func(c *trace.Change) {
				if c.Node == "n001" && c.Kind == "probe" && c.Phase == "finished" {
					c.Outcome = "success"
				}
			})
		}},
		{"missing-probe-start", func(b *evidenceBuilder) {
			mutateChanges(b, func(c *trace.Change) {
				if c.Node == "n001" && c.Kind == "probe" && c.Phase == "direct" {
					c.Phase = "relay"
				}
			})
		}},
		{"different-incarnation", func(b *evidenceBuilder) {
			mutateChanges(b, func(c *trace.Change) {
				if c.Node == "n001" && c.Member != nil && c.Member.State == "suspect" {
					c.Member.Incarnation = "2"
				}
			})
		}},
		{"different-sender-lifetime", func(b *evidenceBuilder) {
			for i := range b.events {
				if b.events[i].Time == "30" {
					b.events[i].Changes = append([]trace.Change{processChange("n001", "start", true, "2")}, b.events[i].Changes...)
				}
			}
		}},
		{"different-target-lifetime", func(b *evidenceBuilder) {
			for i := range b.events {
				if b.events[i].Time == "20" {
					b.events[i].Changes = append([]trace.Change{processChange("n003", "start", true, "2")}, b.events[i].Changes...)
				}
			}
		}},
		{"unchanged-deadline", func(b *evidenceBuilder) {
			mutateChanges(b, func(c *trace.Change) {
				if c.Kind == "timer" && c.ID == "22" {
					c.Deadline = "90"
				}
			})
		}},
		{"cancelled-before-rearm", func(b *evidenceBuilder) {
			for i := range b.events {
				if b.events[i].Time == "20" {
					b.events[i].Changes = append(b.events[i].Changes, admissionTimer("cancel", "21", "90"))
				}
			}
		}},
		{"refutation-first-in-flight", func(b *evidenceBuilder) { moveRefutation(b, "25") }},
		{"same-time-refutation-first", func(b *evidenceBuilder) { moveRefutation(b, "30") }},
		{"refutation-censored", func(b *evidenceBuilder) {
			b.events = slices.DeleteFunc(b.events, func(e trace.Event) bool { return e.Time == "60" || e.Time == "70" })
			resequence(b)
		}},
		{"acceptance-censored", func(b *evidenceBuilder) {
			b.events = slices.DeleteFunc(b.events, func(e trace.Event) bool { return e.Time == "70" })
			resequence(b)
		}},
		{"unmatched-refutation-incarnation", func(b *evidenceBuilder) {
			mutateChanges(b, func(c *trace.Change) {
				if c.Node == "n002" && c.Member != nil && c.Member.State == "alive" && c.Member.Incarnation == "2" {
					c.Member.Incarnation = "3"
				}
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := admissionPositive(t)
			tc.mutate(b)
			out := classifyEvidence(t, b, true)
			ep := observerEpisode(t, out)
			if out.Witnesses != 0 || out.Harmful != 0 || ep.Acceleration || ep.Harmful {
				t.Fatalf("unproven chain admitted: %+v", ep)
			}
			if tc.name == "refutation-first-in-flight" && out.RefutationsInFlight != 1 {
				t.Fatal("missing in-flight diagnostic", out.RefutationsInFlight)
			}
		})
	}
}
func resequence(b *evidenceBuilder) {
	for i := range b.events {
		b.events[i].Seq = trace.Decimal(i + 1)
		b.events[i].ID = trace.Decimal(i + 1)
	}
}
func moveRefutation(b *evidenceBuilder, at string) {
	index := slices.IndexFunc(b.events, func(e trace.Event) bool { return e.Time == "60" })
	event := b.events[index]
	event.Time = at
	b.events = slices.Delete(b.events, index, index+1)
	index = slices.IndexFunc(b.events, func(e trace.Event) bool { return e.Time == "30" })
	b.events = slices.Insert(b.events, index, event)
	resequence(b)
}
func TestAdmissionSeparatesHarmFromAcceleration(t *testing.T) {
	for _, kind := range []string{"received-rumor", "cancelled-fire", "stale-generation", "recovery", "same-time-refutation-after"} {
		t.Run(kind, func(t *testing.T) {
			b := admissionPositive(t)
			for i := range b.events {
				if b.events[i].Time == "50" {
					switch kind {
					case "received-rumor":
						b.events[i].Changes = b.events[i].Changes[1:]
					case "cancelled-fire":
						b.events[i].Changes = append([]trace.Change{admissionTimer("cancel", "22", "50")}, b.events[i].Changes...)
					case "stale-generation":
						b.events[i].Changes[0].Generation = "21"
					case "recovery":
						b.events[i].Changes = []trace.Change{}
					}
				}
				if kind == "same-time-refutation-after" && b.events[i].Time == "60" {
					b.events[i].Time = "30"
				}
			}
			if kind == "same-time-refutation-after" {
				slices.SortStableFunc(b.events, func(a, b trace.Event) int {
					av, _ := strconv.ParseInt(a.Time, 10, 64)
					bv, _ := strconv.ParseInt(b.Time, 10, 64)
					return cmp.Compare(av, bv)
				})
				resequence(b)
			}
			out := classifyEvidence(t, b, true)
			ep := observerEpisode(t, out)
			wantHarm := kind == "same-time-refutation-after"
			if out.Witnesses != 1 || ep.Harmful != wantHarm {
				t.Fatalf("harm conflated with acceleration: %+v", ep)
			}
		})
	}
}
func TestAdmissionNullAndIncomplete(t *testing.T) {
	b := admissionPositive(t)
	b.events = b.events[:4]
	out := classifyEvidence(t, b, true)
	if out.Witnesses != 0 || len(out.Episodes) != 0 || out.Stop != "no-A-to-T-probe-failure" {
		t.Fatal("null result", out)
	}
	b = admissionPositive(t)
	out = classifyEvidence(t, b, false)
	if out.Status != "incomplete" {
		t.Fatal("incomplete evidence became valid")
	}
}

func TestHealthyPairLabelsForNewEmptyOutcomeCases(t *testing.T) {
	for _, name := range []string{"correlated-dense", "correlated-dense-healthy", "correlated-dense-service-1ms"} {
		b := newEvidence(t)
		b.spec.Case = name
		b.spec.Target = "n003"
		b.spec.HealthyPairs = []EligiblePair{{"n002", "n003"}}
		b.add(100)
		summary := b.measure(t, true)
		p := population(t, summary, "healthy-pair-exclusions")
		if p.Transitions != 0 || p.PairNS != 90 || len(p.Exposure) != 1 {
			t.Fatal("empty healthy population disappeared", name, p)
		}
		for _, p := range summary.Populations {
			if p.Label == "FP-analogue" || p.Label == "FP-minus-analogue" {
				t.Fatal("correlated grouping became a pause-reference population")
			}
		}
	}
}
