package experiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/trace"
)

func smallFixture(t *testing.T) (Spec, sim.Scenario) {
	t.Helper()
	spec, s, err := Fixture("healthy", 1, "runner-fixture")
	if err != nil {
		t.Fatal(err)
	}
	s.Nodes = s.Nodes[:2]
	s.Actions = s.Actions[:1]
	s.Horizon = core.Instant(3 * time.Second)
	spec.WarmupNS = int64(50 * time.Millisecond)
	spec.HorizonNS = int64(s.Horizon)
	return spec, s
}

func TestRunnerSavedInputReplayAndOverwriteRefusal(t *testing.T) {
	spec, s := smallFixture(t)
	directory := filepath.Join(t.TempDir(), "run")
	if err := Run(spec, s, directory, nil); err != nil {
		t.Fatal(err)
	}
	var saved Summary
	if err := readJSON(filepath.Join(directory, "summary.json"), &saved); err != nil {
		t.Fatal(err)
	}
	again, err := Reduce(directory)
	if err != nil {
		t.Fatal(err)
	}
	if HashJSON(saved) != HashJSON(again) {
		t.Fatal("saved-input reducer replay differs")
	}
	for _, f := range []struct{ name, want string }{{"spec.json", saved.SpecSHA256}, {"scenario.json", saved.ScenarioSHA256}, {"trace.ndjson", saved.TraceSHA256}} {
		hash, err := hashFile(filepath.Join(directory, f.name))
		if err != nil || hash != f.want {
			t.Fatal("digest", f.name, hash, f.want, err)
		}
	}
	before, err := os.ReadFile(filepath.Join(directory, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = Run(spec, s, directory, nil); !errors.Is(err, os.ErrExist) {
		t.Fatal("overwrite not refused", err)
	}
	after, err := os.ReadFile(filepath.Join(directory, "summary.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("overwrite changed evidence", err)
	}
	// Host measurements are not a deterministic input.
	if err = os.WriteFile(filepath.Join(directory, "resources.json"), []byte("host data can differ"), 0600); err != nil {
		t.Fatal(err)
	}
	again, err = Reduce(directory)
	if err != nil || HashJSON(saved) != HashJSON(again) {
		t.Fatal("host observations contaminated summary", err)
	}
	if err = os.WriteFile(filepath.Join(directory, "report.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Reduce(directory); err == nil {
		t.Fatal("accepted inconsistent report")
	}
}

func TestRunnerRetainsIncompleteAndInvalidEvidence(t *testing.T) {
	for _, kind := range []string{"event-budget", "trace-cap", "header-cap", "invalid-warmup"} {
		t.Run(kind, func(t *testing.T) {
			spec, s := smallFixture(t)
			switch kind {
			case "event-budget":
				s.EventBudget = 1
			case "trace-cap":
				spec.TraceCap = 5000
			case "header-cap":
				spec.TraceCap = 1
			case "invalid-warmup":
				s.Actions = nil
			}
			directory := filepath.Join(t.TempDir(), "failed")
			if err := Run(spec, s, directory, nil); err == nil {
				t.Fatal("failed run became success")
			}
			var status Status
			if err := readJSON(filepath.Join(directory, "status.json"), &status); err != nil {
				t.Fatal(err)
			}
			want := "incomplete"
			if kind == "invalid-warmup" {
				want = "invalid-warmup"
			}
			if status.Status != want || status.Reason == "" {
				t.Fatal("failure not retained", status)
			}
			raw, err := os.ReadFile(filepath.Join(directory, "trace.ndjson"))
			if err != nil || len(raw) == 0 {
				t.Fatal("evidence removed", err)
			}
			if kind == "header-cap" {
				if _, err = Reduce(directory); err == nil {
					t.Fatal("missing header became reducible")
				}
				return
			}
			out, err := Reduce(directory)
			if err != nil {
				t.Fatal(err)
			}
			if out.Status == "complete" {
				t.Fatal("incomplete/invalid replay became complete")
			}
			var saved Summary
			if err = readJSON(filepath.Join(directory, "summary.json"), &saved); err != nil {
				t.Fatal(err)
			}
			if HashJSON(saved) != HashJSON(out) {
				t.Fatal("incomplete saved-input replay changed summary")
			}
			if kind == "trace-cap" {
				complete, err := trace.Read(bytes.NewReader(raw), nil)
				if err != nil || complete || int64(len(raw)) != spec.TraceCap {
					t.Fatal("cap did not retain valid prefix at exact byte bound", complete, len(raw), err)
				}
				if bytes.Contains(raw, []byte(`"type":"end"`)) {
					t.Fatal("cap synthesized end")
				}
			}
		})
	}
}

type brokenOutput struct {
	writeErr, closeErr error
	closed             bool
}

func (w *brokenOutput) Write([]byte) (int, error) { return 0, w.writeErr }
func (w *brokenOutput) Close() error              { w.closed = true; return w.closeErr }

func TestRecordingPropagatesWriteAndCloseFailures(t *testing.T) {
	_, s := smallFixture(t)
	closeErr := errors.New("close failed")
	out := &brokenOutput{writeErr: io.ErrClosedPipe, closeErr: closeErr}
	_, err := recordTrace(s, out, TraceCap, nil)
	if !errors.Is(err, io.ErrClosedPipe) || !errors.Is(err, closeErr) || !out.closed {
		t.Fatal("write/close failures lost", err, out.closed)
	}
}

func TestFixtureShapesAndSeedNamespaces(t *testing.T) {
	for _, name := range []string{"healthy", "crash", "threshold", "interval", "loss", "correlated", "partial-partition", "partial-partition-healthy", "threshold-128", "ordinary-healthy", "ordinary-pause", "ordinary-crash"} {
		spec, s, err := Fixture(name, 1, "source")
		if err != nil {
			t.Fatal(name, err)
		}
		expected := 32
		if name == "threshold-128" {
			expected = 128
		}
		if len(name) > 8 && name[:9] == "ordinary-" {
			expected = 8
		}
		if len(s.Nodes) != expected {
			t.Fatal(name, len(s.Nodes))
		}
		a, _ := json.Marshal(s.Protocol)
		b, _ := json.Marshal(core.DefaultConfig())
		if !bytes.Equal(a, b) {
			t.Fatal("detector defaults changed", name)
		}
		if s.EventBudget != EventBudget || spec.TraceCap != TraceCap || spec.WarmupNS != int64(15*time.Second) || spec.HorizonNS != int64(120*time.Second) || s.Latency != time.Millisecond || s.Nodes[0].Service != 0 || s.DatagramCapacity != 1<<20 || s.StreamCapacity != 1<<20 {
			t.Fatal("fixed bounds", name)
		}
		if s.Seeds.Protocol == s.Seeds.Fault || s.Seeds.Protocol == s.Seeds.Schedule || s.Seeds.Fault == s.Seeds.Schedule {
			t.Fatal("seed namespaces collapsed")
		}
		again, other, err := Fixture(name, 1, "source")
		if err != nil || HashJSON(spec) != HashJSON(again) || HashJSON(s) != HashJSON(other) {
			t.Fatal("unstable fixture", name, err)
		}
		if name == "loss" {
			if len(s.Faults) != 32*31 || s.Faults[0].LossPPM != 100000 || s.Faults[0].StreamFailure {
				t.Fatal("loss fixture")
			}
		}
		if name == "crash" && spec.Target == "n000" {
			t.Fatal("bootstrap crash target")
		}
		if name == "interval" && len(s.Actions) != 31+4*35 {
			t.Fatal("fixed pause cycles", len(s.Actions))
		}
	}
	if _, _, err := Fixture("unknown", 1, "source"); err == nil {
		t.Fatal("accepted unknown case")
	}
	if _, _, err := Fixture("healthy", 0, "source"); err == nil {
		t.Fatal("accepted replicate zero")
	}
	for namespace, want := range map[string]uint64{"protocol": 16508659275336165770, "fault": 11231724242123092822, "schedule": 255906654927968816} {
		if got := DeriveSeed(1, namespace); got != want {
			t.Fatalf("seed derivation vector %s: %d != %d", namespace, got, want)
		}
	}
}

func TestDenseFixturePairingAndOneFactorSensitivity(t *testing.T) {
	for replicate := uint64(1); replicate <= 3; replicate++ {
		original, originalScenario, err := Fixture("correlated", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		dense, nominal, err := Fixture("correlated-dense", replicate, "source")
		if err != nil {
			t.Fatal(err)
		}
		if len(dense.Anomaly) != 24 || len(dense.HealthyPairs) != 7 || dense.Target != original.Target || nominal.Seeds != originalScenario.Seeds {
			t.Fatal("dense pairing", dense)
		}
		if len(original.HealthyPairs)*105 != 2835 || len(dense.HealthyPairs)*105 != 735 {
			t.Fatal("exposure populations")
		}
		for _, n := range original.Anomaly {
			if !slices.Contains(dense.Anomaly, n) {
				t.Fatal("lost original A", n)
			}
		}
		for _, name := range []string{"correlated-dense-healthy", "correlated-dense-latency-100us", "correlated-dense-latency-10ms", "correlated-dense-service-100us", "correlated-dense-service-1ms", "correlated-dense-buffer-64k", "correlated-dense-buffer-16m"} {
			spec, scenario, err := Fixture(name, replicate, "source")
			if err != nil {
				t.Fatal(err)
			}
			expected := nominal
			// Nodes need a separate slice before changing service duration.
			expected.Nodes = slices.Clone(nominal.Nodes)
			switch name {
			case "correlated-dense-healthy":
				expected.Faults = nil
			case "correlated-dense-latency-100us":
				expected.Latency = 100 * time.Microsecond
			case "correlated-dense-latency-10ms":
				expected.Latency = 10 * time.Millisecond
			case "correlated-dense-service-100us", "correlated-dense-service-1ms":
				service := 100 * time.Microsecond
				if name == "correlated-dense-service-1ms" {
					service = time.Millisecond
				}
				for i := range expected.Nodes {
					expected.Nodes[i].Service = service
				}
			case "correlated-dense-buffer-64k":
				expected.DatagramCapacity, expected.StreamCapacity = 64<<10, 64<<10
			case "correlated-dense-buffer-16m":
				expected.DatagramCapacity, expected.StreamCapacity = 16<<20, 16<<20
			}
			if HashJSON(expected) != HashJSON(scenario) {
				t.Fatal("changed extra scenario dimension", name)
			}
			spec.Case = dense.Case
			if HashJSON(spec) != HashJSON(dense) {
				t.Fatal("changed seed/cohort specification", name)
			}
		}
	}
}

func TestProtocolOverlayIsStrictAndReplays(t *testing.T) {
	write := func(body string) string {
		path := filepath.Join(t.TempDir(), "protocol.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, body := range []string{`{"SuspicionMult": 7, "Candidate": true}`, `{"SuspicionMult": 0}`, `{"SuspicionMult": 7} {}`, `[]`} {
		_, s := smallFixture(t)
		if err := OverlayProtocol(&s, write(body)); err == nil {
			t.Fatal("accepted overlay", body)
		}
	}
	spec, s := smallFixture(t)
	if err := OverlayProtocol(&s, write(`{"SuspicionMult": 7}`)); err != nil {
		t.Fatal(err)
	}
	want := core.DefaultConfig()
	want.SuspicionMult = 7
	if HashJSON(s.Protocol) != HashJSON(want) {
		t.Fatal("overlay did not keep omitted fields", s.Protocol)
	}
	directory := filepath.Join(t.TempDir(), "run")
	if err := Run(spec, s, directory, nil); err != nil {
		t.Fatal(err)
	}
	var saved sim.Scenario
	if err := readJSON(filepath.Join(directory, "scenario.json"), &saved); err != nil || saved.Protocol.SuspicionMult != 7 {
		t.Fatal("saved scenario lost the overlay", saved.Protocol.SuspicionMult, err)
	}
	recorded, err := os.ReadFile(filepath.Join(directory, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	again, err := Reduce(directory)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "summary.json")
	if err = WriteSummary(out, again); err != nil {
		t.Fatal(err)
	}
	if replayed, err := os.ReadFile(out); err != nil || !bytes.Equal(recorded, replayed) {
		t.Fatal("overlay run did not reduce byte for byte", err)
	}
}

func TestReducerRetainsRecordingFailureWithCompleteTrace(t *testing.T) {
	spec, s := smallFixture(t)
	directory := filepath.Join(t.TempDir(), "run")
	if err := Run(spec, s, directory, nil); err != nil {
		t.Fatal(err)
	}
	status := filepath.Join(directory, "status.json")
	if err := os.Remove(status); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(status, Status{Status: "incomplete", Reason: "recording close failed"}); err != nil {
		t.Fatal(err)
	}
	out, err := Reduce(directory)
	if err != nil || out.Status != "incomplete" || out.Reason != "recording close failed" || out.Format != SummaryVersion {
		t.Fatal("recording failure became success", out, err)
	}
}
