package main

import (
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/internal/experiment"
)

func TestCLIArgumentsAndReducer(t *testing.T) {
	for _, args := range [][]string{nil, {"-out", "unused", "-case", "unknown", "-replicate", "1", "-revision", "explicit"}, {"-out", "unused", "-case", "healthy", "-revision", "explicit"}, {"-out", "unused", "-reduce", "missing", "-case", "healthy"}, {"-out", "unused", "positional"}} {
		if err := run(args, io.Discard); err == nil {
			t.Fatal("accepted invalid arguments", args)
		}
	}
	if sourceIdentity("explicit") != "explicit" {
		t.Fatal("explicit source identity lost")
	}
	spec, s, err := experiment.Fixture("healthy", 1, "cli-reducer-fixture")
	if err != nil {
		t.Fatal(err)
	}
	s.Nodes = s.Nodes[:2]
	s.Actions = s.Actions[:1]
	s.Horizon = core.Instant(3 * time.Second)
	spec.HorizonNS = int64(s.Horizon)
	spec.WarmupNS = int64(50 * time.Millisecond)
	directory := filepath.Join(t.TempDir(), "run")
	if err = experiment.Run(spec, s, directory, nil); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "replayed.json")
	if err = run([]string{"-reduce", directory, "-out", out}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err = run([]string{"-reduce", directory, "-out", out}, io.Discard); err == nil {
		t.Fatal("summary overwrite accepted")
	}
}
