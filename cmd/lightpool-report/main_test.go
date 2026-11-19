package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
)

func recording(t *testing.T, budget int) string {
	t.Helper()
	raw, err := os.ReadFile("../../examples/sim/crash.json")
	if err != nil {
		t.Fatal(err)
	}
	var s sim.Scenario
	if err = json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	s.Code = "cli-test-source"
	if budget > 0 {
		s.EventBudget = budget
	}
	path := filepath.Join(t.TempDir(), "recording.ndjson")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_, exportErr := sim.Export(s, f, sim.ExportOptions{})
	closeErr := f.Close()
	if exportErr != nil || closeErr != nil {
		t.Fatal(exportErr, closeErr)
	}
	return path
}
func TestCLIReportAndIncomplete(t *testing.T) {
	for _, budget := range []int{0, 10} {
		path := recording(t, budget)
		for _, format := range []string{"text", "json"} {
			var out, diagnostics bytes.Buffer
			err := run([]string{"-trace", path, "-format", format}, &out, &diagnostics)
			if budget == 0 && err != nil {
				t.Fatal(err)
			}
			if budget > 0 && (err == nil || !strings.Contains(err.Error(), "INCOMPLETE")) {
				t.Fatal(err)
			}
			if diagnostics.Len() != 0 {
				t.Fatal(diagnostics.String())
			}
			if format == "text" {
				if !strings.Contains(out.String(), "Crash detection") || (budget > 0 && !strings.HasPrefix(out.String(), "INCOMPLETE")) {
					t.Fatal(out.String())
				}
			} else {
				var r report.Report
				if err = json.Unmarshal(out.Bytes(), &r); err != nil {
					t.Fatal(err)
				}
				if r.Complete != (budget == 0) || r.NodeCount != 8 || (budget == 0 && r.Crashes[0].Latencies.Completed != 7) {
					t.Fatal(r)
				}
			}
		}
	}
}

type failedOutput struct{}

func (failedOutput) Write([]byte) (int, error) { return 0, errors.New("output failed") }
func TestCLIRejectsAndPropagates(t *testing.T) {
	path := recording(t, 10)
	for _, args := range [][]string{nil, {"-trace", path, "-format", "xml"}, {"-trace", path, "unexpected"}, {"-trace", path + "missing"}, {"-unknown"}} {
		var out, diag bytes.Buffer
		if err := run(args, &out, &diag); err == nil || out.Len() != 0 {
			t.Fatal(args, err, out.String())
		}
	}
	if err := run([]string{"-trace", path}, failedOutput{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "output failed") {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(t.TempDir(), "bad.ndjson")
	raw = append(raw, []byte("{}\n")...)
	if err = os.WriteFile(bad, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = run([]string{"-trace", bad}, &out, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "line ") || out.Len() != 0 {
		t.Fatal(err, out.String())
	}
}
