package experiment

import (
	"bytes"
	"strings"
	"testing"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
)

func TestSilentCasesDifferOnlyInFaultMode(t *testing.T) {
	for _, base := range silentCases {
		for replicate := uint64(1); replicate <= 3; replicate++ {
			spec, scenario, err := Fixture(base+"-silent", replicate, "source")
			if err != nil {
				t.Fatal(err)
			}
			if err = validateSpec(spec, scenario); err != nil {
				t.Fatal(base, err)
			}
			wantSpec, want, err := Fixture(base, replicate, "source")
			if err != nil {
				t.Fatal(err)
			}
			if spec.Case != base+"-silent" || len(scenario.Faults) != 24 || len(scenario.Faults) != len(want.Faults) {
				t.Fatal("silent case name or fault count", spec.Case, len(scenario.Faults))
			}
			for i, f := range scenario.Faults {
				if !f.Drop || !f.StreamDrop || f.StreamFailure {
					t.Fatal("silent fault flags", base, f)
				}
				f.StreamDrop, f.StreamFailure = false, true
				if f != want.Faults[i] {
					t.Fatal("silent fault differs outside its flags", base, f, want.Faults[i])
				}
			}
			scenario.Faults = want.Faults
			spec.Case = wantSpec.Case
			if HashJSON(spec) != HashJSON(wantSpec) || HashJSON(scenario) != HashJSON(want) {
				t.Fatal("silent case differs outside fault flags and case name", base)
			}
		}
	}
}

func TestSilentSuffixOnlyOnImpairedDenseCases(t *testing.T) {
	for _, name := range []string{"correlated-dense-healthy-silent", "correlated-silent", "partial-partition-silent", "correlated-dense-buffer-64k-silent", "correlated-dense-64-aged-silent", "correlated-dense-silent-silent", "-silent"} {
		if _, _, err := Fixture(name, 1, "source"); err == nil || !strings.Contains(err.Error(), "unknown fixed case") {
			t.Fatal("accepted silent twin of", name, err)
		}
	}
}

func TestStreamDropIsAHealthyPairImpairment(t *testing.T) {
	b := newEvidence(t)
	b.spec.Case = "correlated-dense-silent"
	b.spec.Target = "n003"
	b.spec.HealthyPairs = []EligiblePair{{"n002", "n003"}}
	b.add(100)
	b.measure(t, true)
	b.scenario.Faults = []sim.Fault{{From: "n002", To: "n003", Start: 10, End: 50, StreamDrop: true}}
	raw := b.raw(t, true)
	base, err := report.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Measure(b.spec, b.scenario, base, bytes.NewReader(raw)); err == nil || !strings.Contains(err.Error(), "healthy pair has a configured path impairment") {
		t.Fatal("StreamDrop-only fault left a healthy pair eligible", err)
	}
}
