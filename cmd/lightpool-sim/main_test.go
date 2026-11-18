package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modulecollective/lightpool/sim/trace"
)

func TestCLIRecordingAndRefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pause.ndjson")
	args := []string{"-scenario", "../../examples/sim/pause.json", "-out", path, "-revision", "test-explicit"}
	var progress bytes.Buffer
	if err := run(args, &progress); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := trace.Read(f, nil)
	closeErr := f.Close()
	if err != nil || closeErr != nil || !complete {
		t.Fatal(complete, err, closeErr)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = run(args, &progress); err == nil {
		t.Fatal("overwrote recording")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed overwrite changed bytes")
	}
	if !strings.Contains(progress.String(), "events=") || !strings.Contains(progress.String(), "trace=") {
		t.Fatal("missing progress", progress.String())
	}
}
func TestInvalidScenarioDoesNotCreateOutput(t *testing.T) {
	dir := t.TempDir()
	scenario := filepath.Join(dir, "bad.json")
	out := filepath.Join(dir, "out.ndjson")
	if err := os.WriteFile(scenario, []byte(`{"Horizon":1,"Unknown":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-scenario", scenario, "-out", out, "-revision", "test"}, &bytes.Buffer{}); err == nil {
		t.Fatal("accepted unknown field")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("created output", err)
	}
}
