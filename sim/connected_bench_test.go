package sim

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
)

// connectedScenario keeps real joins, inspection and encoding in the workload.
// The 512-node case is a fixed startup prefix, not a convergence benchmark.
func connectedScenario(nodes, budget int) Scenario {
	cfg := core.DefaultConfig()
	cfg.ProbeInterval = 250 * time.Millisecond
	cfg.ProbeTimeout = 50 * time.Millisecond
	cfg.GossipInterval = 50 * time.Millisecond
	cfg.PushPullInterval = 0
	cfg.MaxPushPull = wire.MaxNodes
	s := Scenario{Code: "connected-benchmark-v1", Protocol: cfg, Seeds: Seeds{Protocol: 1, Fault: 2, Schedule: 3}, Horizon: core.Instant(3 * time.Second), EventBudget: budget, DatagramCapacity: 1 << 20, StreamCapacity: 1 << 20, Latency: time.Millisecond}
	for i := range nodes {
		name := fmt.Sprintf("n%04d", i)
		s.Nodes = append(s.Nodes, Node{Identity: wire.Identity{Name: name, Address: netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, byte(i / 256), byte(i % 256)}), 7946)}})
		if i > 0 {
			s.Actions = append(s.Actions, Action{At: core.Instant(time.Duration(i) * time.Millisecond), Node: name, Kind: "join", Peer: "n0000"})
		}
	}
	return s
}

type countingDiscard struct{ bytes uint64 }

func (w *countingDiscard) Write(p []byte) (int, error) { w.bytes += uint64(len(p)); return len(p), nil }

// Opt in with: go test ./sim -run '^$' -bench BenchmarkConnectedExport -benchtime=1x -benchmem
func BenchmarkConnectedExport(b *testing.B) {
	for _, n := range []int{8, 32, 128, 512} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			budget := 1000000
			if n == 512 {
				budget = 100000
			}
			s := connectedScenario(n, budget)
			b.ReportAllocs()
			var result Result
			var w countingDiscard
			for b.Loop() {
				w.bytes = 0
				var err error
				result, err = Export(s, &w, ExportOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if n == 512 {
					if result.Complete || result.Events != budget || result.Reason != "event budget exhausted" {
						b.Fatal(result)
					}
				} else if !result.Complete {
					b.Fatal(result.Reason)
				}
			}
			b.ReportMetric(float64(result.Events), "events/op")
			b.ReportMetric(float64(w.bytes), "trace-B/op")
		})
	}
}
