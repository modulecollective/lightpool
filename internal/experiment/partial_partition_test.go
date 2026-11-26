package experiment

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

func TestPartialPartitionTopologyAndPairing(t *testing.T) {
	for replicate := uint64(1); replicate <= 3; replicate++ {
		original, directed, err := Fixture("correlated", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		cut, scenario, err := Fixture("partial-partition", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		if err = validateSpec(cut, scenario); err != nil {
			t.Fatal(err)
		}
		if len(cut.Anomaly) != 4 || !slices.IsSorted(cut.Anomaly) || len(cut.HealthyPairs) != 27 || len(scenario.Faults) != 8 {
			t.Fatal("wrong fixed populations or cut size", cut)
		}
		var expected []sim.Fault
		for _, a := range cut.Anomaly {
			for _, pair := range []EligiblePair{{a, cut.Target}, {cut.Target, a}} {
				expected = append(expected, sim.Fault{From: pair.Observer, To: pair.Subject, Start: core.Instant(15 * time.Second), End: core.Instant(31384 * time.Millisecond), Drop: true, StreamFailure: true})
			}
		}
		if !slices.Equal(scenario.Faults, expected) {
			t.Fatal("cut edges, order, transports or boundaries differ", scenario.Faults)
		}
		var pairs []EligiblePair
		for _, node := range scenario.Nodes {
			n := node.Identity.Name
			if n != cut.Target && !slices.Contains(cut.Anomaly, n) {
				pairs = append(pairs, EligiblePair{n, cut.Target})
			}
		}
		if !slices.Equal(cut.HealthyPairs, pairs) || len(pairs)*105 != 2835 {
			t.Fatal("healthy population includes a cut endpoint or misses O", cut.HealthyPairs)
		}
		// Equality of the complete scenario also protects every uncut link,
		// join, default, seed and transport dimension.
		withoutReverse := scenario
		withoutReverse.Faults = nil
		for i := 0; i < len(scenario.Faults); i += 2 {
			withoutReverse.Faults = append(withoutReverse.Faults, scenario.Faults[i])
		}
		cut.Case = original.Case
		if HashJSON(withoutReverse) != HashJSON(directed) || HashJSON(cut) != HashJSON(original) {
			t.Fatal("changed more than the reverse edges and case name")
		}
		healthy, control, err := Fixture("partial-partition-healthy", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		if err = validateSpec(healthy, control); err != nil {
			t.Fatal(err)
		}
		scenario.Faults = nil
		healthy.Case = original.Case
		if HashJSON(scenario) != HashJSON(control) || HashJSON(healthy) != HashJSON(original) {
			t.Fatal("control differs in more than faults and case name")
		}
	}
}

func TestPartialPartitionPopulationDiscrimination(t *testing.T) {
	b := newEvidence(t)
	b.spec.Case, b.spec.Target = "partial-partition", "n003"
	b.spec.HealthyPairs = []EligiblePair{{"n001", "n003"}, {"n002", "n003"}}
	b.scenario.Faults = []sim.Fault{
		{From: "n000", To: "n003", Start: 10, End: 50, Drop: true, StreamFailure: true},
		{From: "n003", To: "n000", Start: 10, End: 50, Drop: true, StreamFailure: true},
	}
	b.add(20, member("n000", "n003", "dead"))
	b.add(21, member("n003", "n000", "dead"))
	b.add(22, member("n001", "n003", "dead"))
	b.add(30, member("n001", "n003", "alive"))
	b.add(40, member("n001", "n003", "dead"))
	b.add(60, member("n001", "n003", "alive"))
	s := b.measure(t, true)
	p, raw := population(t, s, "healthy-pair-exclusions"), population(t, s, "running-target-exclusions")
	if p.Transitions != 2 || p.Pairs != 1 || p.Targets != 1 || p.PairNS != 180 || *p.FractionEverExcluded != "0.500000000" || raw.Transitions != 4 || raw.PairNS != 12*90 {
		t.Fatal("partition declarations contaminated healthy population or beliefs removed exposure", p, raw)
	}
	if len(s.Recoveries) != 4 || s.RecoveryDurations.Completed != 2 {
		t.Fatal("lost repeated exclusions or unresolved recovery", s.Recoveries)
	}
	for _, ep := range s.Episodes {
		if slices.Contains(ep.Labels, "healthy-pair-exclusions") != (ep.Observer == "n001" && ep.Subject == "n003") || slices.Contains(ep.Labels, "FP-analogue") || slices.Contains(ep.Labels, "FP-minus-analogue") {
			t.Fatal("wrong episode labels", ep)
		}
	}
	for _, pair := range []EligiblePair{{"n000", "n003"}, {"n003", "n000"}} {
		// Each direction must be rejected even if only the opposite fault
		// is configured: direct bidirectional health is the contract.
		for _, fault := range b.scenario.Faults {
			bad := *b
			bad.spec.HealthyPairs = append(slices.Clone(b.spec.HealthyPairs), pair)
			bad.scenario.Faults = []sim.Fault{fault}
			data := bad.raw(t, true)
			base, err := report.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Measure(bad.spec, bad.scenario, base, bytes.NewReader(data)); err == nil || !strings.Contains(err.Error(), "path impairment") {
				t.Fatal("accepted cut pair as healthy", pair, fault, err)
			}
		}
	}
}

func TestPartialPartitionAdmissionRefused(t *testing.T) {
	for _, name := range []string{"partial-partition", "partial-partition-healthy", "correlated-dense-64-aged", "correlated-dense-64-aged-healthy", "small-double-crash"} {
		// Exact fixed inputs isolate the case-name admission guard from the
		// unchanged-fixture check. Synthetic all-alive evidence is sufficient.
		spec, scenario, err := Fixture(name, 1, "source")
		if err != nil {
			t.Fatal(err)
		}
		b := &evidenceBuilder{spec: spec, scenario: scenario}
		for _, node := range scenario.Nodes {
			n := node.Identity.Name
			changes := []trace.Change{processChange(n, "start", true, "1")}
			for _, subject := range scenario.Nodes {
				changes = append(changes, member(n, subject.Identity.Name, "alive"))
			}
			b.add(0, changes...)
		}
		b.add(spec.HorizonNS)
		dir := t.TempDir()
		for _, file := range []struct {
			name string
			data any
		}{{"spec.json", spec}, {"scenario.json", scenario}, {"status.json", Status{Status: "complete"}}} {
			if err = writeJSON(filepath.Join(dir, file.name), file.data); err != nil {
				t.Fatal(err)
			}
		}
		if err = os.WriteFile(filepath.Join(dir, "trace.ndjson"), b.raw(t, true), 0600); err != nil {
			t.Fatal(err)
		}
		if summary, err := Reduce(dir); err != nil || summary.Status != "complete" {
			t.Fatal("new case did not reduce", name, err)
		}
		out, err := Admit(dir)
		if err == nil || out.Status != "invalid" || out.Reason != "admission requires an unchanged fixed correlated fixture" {
			t.Fatal("new case entered correlated admission", name, out, err)
		}
	}
}
