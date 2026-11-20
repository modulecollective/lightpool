package lightpool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/internal/experiment"
	"github.com/modulecollective/lightpool/internal/ordinary"
	"github.com/modulecollective/lightpool/wire"
)

// TestHealthyTiming is an opt-in load measurement. Run each mode in a fresh
// process with the recipe under experiments/2026-11-20-ordinary-witness.
func TestHealthyTiming(t *testing.T) {
	out := os.Getenv("LIGHTPOOL_TIMING_OUT")
	if out == "" {
		t.Skip("healthy timing experiment")
	}
	mode := os.Getenv("LIGHTPOOL_TIMING_MODE")
	if mode != "on" && mode != "off" {
		t.Fatal("invalid recording mode")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(out, "observer.ndjson")
	var recording *recordingConfig
	if mode == "on" {
		recording = &recordingConfig{Path: path, Source: os.Getenv("LIGHTPOOL_TIMING_SOURCE"), Binary: os.Getenv("LIGHTPOOL_TIMING_BINARY"), Limits: ordinary.Limits{QueueBytes: 8 << 20, QueueRecords: 4096, Records: 5_000_000, Bytes: 2 << 30}}
	}
	var nodes [8]*Node
	var targets [8]wire.Identity
	var seeds [8]uint64
	for i := range nodes {
		cfg := DefaultConfig(fmt.Sprintf("n%03d", i))
		cfg.Bind = netip.MustParseAddrPort("127.0.0.1:0")
		cfg.Compression = false
		var c *recordingConfig
		if i == 0 {
			c = recording
		}
		n, err := newOrdinaryNode(cfg, c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := n.Shutdown(); err != nil {
				t.Error(err)
			}
		})
		nodes[i], targets[i], seeds[i] = n, n.Identity(), n.life
		if i > 0 {
			join(t, n, nodes[0])
		}
	}
	wait(t, "healthy membership before sampling", func() bool {
		for _, n := range nodes {
			s := n.Snapshot()
			if len(s.Members) != len(nodes) {
				return false
			}
			for _, m := range s.Members {
				if m.State != wire.Alive {
					return false
				}
			}
		}
		return true
	})
	observer := nodes[0]
	// Each round starts seven inbound pings plus an outbound observer ping.
	// Measure API call duration, including owner scheduling in both directions.
	type round struct {
		StartNS, EndNS int64
		PingNS         [8]int64
	}
	samples := make([]round, 200)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	before := observer.Traffic()
	for i := range samples {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		sample := &samples[i]
		sample.StartNS = int64(observer.now())
		var wg sync.WaitGroup
		var failures [8]error
		for j, n := range nodes {
			target := targets[0]
			if j == 0 {
				target = targets[1+i%7]
			}
			wg.Go(func() {
				start := time.Now()
				_, failures[j] = n.Ping(ctx, target)
				sample.PingNS[j] = time.Since(start).Nanoseconds()
			})
		}
		wg.Wait()
		sample.EndNS = int64(observer.now())
		for j, err := range failures {
			if err != nil {
				t.Fatalf("round %d node %d: %v", i, j, err)
			}
		}
	}
	after := observer.Traffic()
	if err := observer.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if mode == "on" {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = experiment.ReplayOrdinary(f)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("replay %v, close %v", err, closeErr)
		}
	}
	result := struct {
		Mode          string
		Lifetimes     [8]uint64
		Before, After Traffic
		Rounds        []round
	}{mode, seeds, before, after, samples}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "rounds.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

// TestReplayHealthyJournal lets the evidence verifier replay a restored
// journal with the checked-in decoder, without starting a node.
func TestReplayHealthyJournal(t *testing.T) {
	path := os.Getenv("LIGHTPOOL_REPLAY_HEALTHY_JOURNAL")
	if path == "" {
		t.Skip("set journal path to verify retained healthy recording")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := experiment.ReplayOrdinary(file)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("replay %v, close %v", err, closeErr)
	}
	if counts["Ping"] < 200 || counts["Datagram"] < 200 {
		t.Fatalf("missing healthy workload: %v", counts)
	}
}
