package experiment

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
)

func TestCorrelatedDense64AgedPopulationsAndWindow(t *testing.T) {
	for replicate := uint64(1); replicate <= 3; replicate++ {
		spec, scenario, err := Fixture("correlated-dense-64-aged", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		if err = validateSpec(spec, scenario); err != nil {
			t.Fatal(err)
		}
		if len(scenario.Nodes) != 64 || len(spec.Anomaly) != 48 || !slices.IsSorted(spec.Anomaly) || len(spec.HealthyPairs) != 15 || len(scenario.Faults) != 48 {
			t.Fatal("wrong populations", len(scenario.Nodes), len(spec.Anomaly), len(spec.HealthyPairs), len(scenario.Faults))
		}
		if spec.WarmupNS != int64(95*time.Second) || spec.HorizonNS != int64(200*time.Second) || scenario.Horizon != core.Instant(200*time.Second) {
			t.Fatal("wrong window", spec.WarmupNS, spec.HorizonNS)
		}
		// Groups follow fault rank: T at rank 4, A at ranks 0-3 and 5-48.
		ranks := rankedNames(scenario)
		if spec.Target != ranks[4] {
			t.Fatal("target is not fault rank 4", spec.Target, ranks[:5])
		}
		want := append(slices.Clone(ranks[:4]), ranks[5:49]...)
		slices.Sort(want)
		if !slices.Equal(spec.Anomaly, want) {
			t.Fatal("A is not ranks 0-3 and 5-48")
		}
		for i, a := range spec.Anomaly {
			f := scenario.Faults[i]
			if f != (sim.Fault{From: a, To: spec.Target, Start: core.Instant(95 * time.Second), End: core.Instant(111384 * time.Millisecond), Drop: true, StreamFailure: true}) {
				t.Fatal("wrong fault", f)
			}
		}
		for _, p := range spec.HealthyPairs {
			if p.Subject != spec.Target || p.Observer == spec.Target || slices.Contains(spec.Anomaly, p.Observer) {
				t.Fatal("healthy pair is not O to T", p)
			}
		}
		// Seeds pair with every other case at the same index.
		_, dense, _ := Fixture("correlated-dense", replicate, "source")
		if scenario.Seeds != dense.Seeds || HashJSON(scenario.Protocol) != HashJSON(dense.Protocol) || scenario.Latency != dense.Latency || scenario.EventBudget != dense.EventBudget {
			t.Fatal("aged case differs from correlated-dense outside size, groups and window")
		}

		healthy, control, err := Fixture("correlated-dense-64-aged-healthy", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		if err = validateSpec(healthy, control); err != nil {
			t.Fatal(err)
		}
		scenario.Faults = nil
		healthy.Case = spec.Case
		if HashJSON(scenario) != HashJSON(control) || HashJSON(healthy) != HashJSON(spec) {
			t.Fatal("healthy control differs in more than faults and case name")
		}
	}
}

func TestSmallDoubleCrash(t *testing.T) {
	for replicate := uint64(1); replicate <= 3; replicate++ {
		spec, scenario, err := Fixture("small-double-crash", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		if err = validateSpec(spec, scenario); err != nil {
			t.Fatal(err)
		}
		var crashed []string
		for _, a := range scenario.Actions {
			if a.Kind == "crash" {
				if a.At != core.Instant(15*time.Second) {
					t.Fatal("crash outside warm-up boundary", a)
				}
				crashed = append(crashed, a.Node)
			}
		}
		var want []string
		for _, n := range rankedNames(scenario) {
			if n != "n000" && len(want) < 2 {
				want = append(want, n)
			}
		}
		if len(scenario.Nodes) != 4 || !slices.Equal(crashed, want) || spec.Target != want[0] || len(spec.Anomaly) != 0 || len(spec.HealthyPairs) != 0 || len(scenario.Faults) != 0 {
			t.Fatal("wrong double crash", crashed, want, spec.Target)
		}
		if spec.WarmupNS != int64(15*time.Second) || spec.HorizonNS != int64(120*time.Second) {
			t.Fatal("wrong window")
		}
		// The single-crash case still crashes exactly one node.
		_, single, _ := Fixture("crash", replicate, "source")
		crashes := 0
		for _, a := range single.Actions {
			if a.Kind == "crash" {
				crashes++
			}
		}
		if crashes != 1 {
			t.Fatal("crash case changed", crashes)
		}
	}
}

// rankedNames repeats the fixture's fault-seed ranking independently.
func rankedNames(s sim.Scenario) []string {
	var names []string
	for _, n := range s.Nodes {
		names = append(names, n.Identity.Name)
	}
	slices.SortFunc(names, func(a, b string) int {
		ra, rb := faultRank(s.Seeds.Fault, a), faultRank(s.Seeds.Fault, b)
		return slices.Compare(ra[:], rb[:])
	})
	return names
}

func faultRank(seed uint64, name string) [32]byte {
	return sha256.Sum256(append(binary.LittleEndian.AppendUint64(nil, seed), name...))
}
