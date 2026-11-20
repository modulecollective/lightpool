package experiment

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWitnessCellsFrozenOrder(t *testing.T) {
	cells := WitnessCells()
	if len(cells) != 36 {
		t.Fatal(len(cells))
	}
	var ids []string
	seen := map[string]bool{}
	for i, c := range cells {
		if c.Order != i+1 || seen[c.ID()] {
			t.Fatal("order or duplicate", c)
		}
		seen[c.ID()] = true
		ids = append(ids, c.ID())
	}
	// Groups alternate off->on, on->off; healthy precedes impaired in a group.
	want := []string{
		"01-100us-i1-healthy-off", "02-100us-i1-healthy-on", "03-100us-i1-impaired-off", "04-100us-i1-impaired-on",
		"05-100us-i2-healthy-on", "06-100us-i2-healthy-off", "07-100us-i2-impaired-on", "08-100us-i2-impaired-off",
		"09-100us-i3-healthy-off",
	}
	if strings.Join(ids[:9], " ") != strings.Join(want, " ") {
		t.Fatal(ids[:9])
	}
	if ids[12] != "13-1ms-i1-healthy-on" || ids[35] != "36-10ms-i3-impaired-on" {
		t.Fatal(ids[12], ids[35])
	}
	modes := map[string]int{}
	for _, c := range cells {
		modes[c.Mode]++
	}
	if modes["on"] != 18 || modes["off"] != 18 {
		t.Fatal(modes)
	}
}

// offAttempt writes a minimal complete off-mode attempt: every node samples
// full alive membership; O n0xx sees T suspect in one window sample.
func offAttempt(t *testing.T, mutate func(*WitnessManifest, *WitnessStatus, *[]WitnessSample)) string {
	t.Helper()
	cell := WitnessCells()[0]
	spec, _, err := Fixture("correlated-dense", cell.Index, "test")
	if err != nil {
		t.Fatal(err)
	}
	m := WitnessManifest{Format: WitnessVersion, Cell: cell, Spec: spec, Schedule: FrozenWitnessSchedule(), Limits: FrozenWitnessLimits(), SidecarBytes: WitnessSidecarBytes, Source: "test", Nodes: []WitnessNode{}}
	st := WitnessStatus{Format: WitnessVersion, Status: "complete", SampledWarmup: true, Joins: []WitnessJoin{}, Exits: []WitnessExit{}, Survivors: []int{}}
	st.Warmup.LoNS, st.Warmup.HiNS = 15*sec, 15*sec+1000
	st.Horizon.LoNS, st.Horizon.HiNS = 120*sec, 120*sec+1000
	var samples []WitnessSample
	for i := range WitnessNodes {
		name := fmt.Sprintf("n%03d", i)
		m.Nodes = append(m.Nodes, WitnessNode{Name: name, Role: WitnessRole(spec, name), Address: netip.MustParseAddrPort(fmt.Sprintf("10.77.%d.2:7946", i)), PID: 100 + i, Lifetime: 1, Seed: "unavailable"})
		st.Exits = append(st.Exits, WitnessExit{Node: name, PID: 100 + i})
		for at := int64(14 * sec); at <= 121*sec; at += int64(100 * time.Millisecond) {
			states := strings.Repeat("a", WitnessNodes)
			if WitnessRole(spec, name) == "O" && at == 20*sec {
				var target int
				_, _ = fmt.Sscanf(spec.Target, "n%03d", &target)
				states = states[:target] + "s" + states[target+1:]
			}
			samples = append(samples, WitnessSample{Node: name, ReceivedNS: at, States: states, SentBytes: uint64(at / 1000)})
		}
	}
	if mutate != nil {
		mutate(&m, &st, &samples)
	}
	dir := t.TempDir()
	write := func(name string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("manifest.json", m)
	write("status.json", st)
	f, err := os.Create(filepath.Join(dir, "samples.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	e := json.NewEncoder(f)
	for _, s := range samples {
		if err = e.Encode(s); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestReduceWitnessOffModeIsSampledOnly(t *testing.T) {
	r, err := ReduceWitness(offAttempt(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if r.Classification != nil || r.RecordedWarmup != nil || len(r.Timing) != 0 || len(r.Pairs) != 7 || r.PairExposureNS != 7*(105*sec-1000) {
		t.Fatalf("%+v", r)
	}
	for _, p := range r.Pairs {
		if p.Samples == 0 || p.NonAlive != 1 || p.Transitions != 1 || p.Dead != 0 || p.MissedPollPeriod != 0 {
			t.Fatalf("%+v", p)
		}
	}
	if !strings.HasPrefix(r.Row(), "01-100us-i1-healthy-off\toff\tcomplete\ttrue\t-\t-\t7\t0\t-") {
		t.Fatal(r.Row())
	}
}

func TestReduceWitnessRefusesMalformedAttempts(t *testing.T) {
	for name, c := range map[string]struct {
		mutate func(*WitnessManifest, *WitnessStatus, *[]WitnessSample)
		want   string
	}{
		"incomplete status": {func(_ *WitnessManifest, s *WitnessStatus, _ *[]WitnessSample) { s.Status, s.Reason = "incomplete", "x" }, "incomplete"},
		"changed schedule":  {func(m *WitnessManifest, _ *WitnessStatus, _ *[]WitnessSample) { m.Schedule.HorizonNS++ }, "frozen"},
		"changed limits":    {func(m *WitnessManifest, _ *WitnessStatus, _ *[]WitnessSample) { m.Limits.Records++ }, "frozen"},
		"wrong role":        {func(m *WitnessManifest, _ *WitnessStatus, _ *[]WitnessSample) { m.Nodes[0].Role = "T" }, "does not match"},
		"journal named":     {func(m *WitnessManifest, _ *WitnessStatus, _ *[]WitnessSample) { m.Nodes[0].Journal = "x" }, "does not match"},
		"survivor":          {func(_ *WitnessManifest, s *WitnessStatus, _ *[]WitnessSample) { s.Survivors = []int{9} }, "clean exits"},
		"unclean exit":      {func(_ *WitnessManifest, s *WitnessStatus, _ *[]WitnessSample) { s.Exits[3].Wait = "signal" }, "did not exit"},
		"fault on healthy":  {func(_ *WitnessManifest, s *WitnessStatus, _ *[]WitnessSample) { s.Fault = &WitnessFault{} }, "fault evidence"},
		"recording failed":  {func(_ *WitnessManifest, _ *WitnessStatus, s *[]WitnessSample) { (*s)[5].Recording = "cap" }, "failed sample"},
		"missing node": {func(_ *WitnessManifest, _ *WitnessStatus, s *[]WitnessSample) {
			kept := (*s)[:0]
			for _, x := range *s {
				if x.Node != "n031" {
					kept = append(kept, x)
				}
			}
			*s = kept
		}, "samples from 31 nodes"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ReduceWitness(offAttempt(t, c.mutate)); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatal(err)
			}
		})
	}
	dir := offAttempt(t, nil)
	if err := os.WriteFile(filepath.Join(dir, "node-n000.ndjson"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReduceWitness(dir); err == nil || !strings.Contains(err.Error(), "contains a journal") {
		t.Fatal(err)
	}
}
