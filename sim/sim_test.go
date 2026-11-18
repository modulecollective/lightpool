// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
	"net/netip"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fixture() Scenario {
	cfg := core.DefaultConfig()
	cfg.ProbeInterval = 100 * time.Millisecond
	cfg.ProbeTimeout = 20 * time.Millisecond
	cfg.GossipInterval = 10 * time.Millisecond
	cfg.PushPullInterval = 0
	cfg.StreamTimeout = 200 * time.Millisecond
	nodes := make([]Node, 3)
	for j, name := range []string{"a", "b", "c"} {
		nodes[j] = Node{Identity: wire.Identity{Name: name, Address: netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:7946", j+1))}, Service: 100 * time.Microsecond}
	}
	return Scenario{Code: "test-reference", Protocol: cfg, Nodes: nodes, Seeds: Seeds{1, 2, 3}, Horizon: core.Instant(time.Second), EventBudget: 100000, DatagramCapacity: 4096, StreamCapacity: 64, Latency: time.Millisecond, Actions: []Action{{At: core.Instant(10 * time.Millisecond), Kind: "join", Node: "b", Peer: "a"}, {At: core.Instant(100 * time.Millisecond), Kind: "join", Node: "c", Peer: "a"}}}
}
func run(t testing.TB, s Scenario) Result {
	t.Helper()
	r, err := Run(s)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Complete {
		t.Fatal(r.Reason, r.Events)
	}
	return r
}

type tagged struct {
	Kind   string
	Effect json.RawMessage
}

func effects(r Result, kind string) []json.RawMessage {
	var found []json.RawMessage
	for _, trace := range r.Trace {
		for _, raw := range trace.Effects {
			var e tagged
			_ = json.Unmarshal(raw, &e)
			if e.Kind == kind {
				found = append(found, e.Effect)
			}
		}
	}
	return found
}
func members(r Result, node string) map[string]wire.State {
	out := make(map[string]wire.State)
	for _, m := range r.Measurements {
		if m.Name == node {
			for _, n := range m.Snapshot.Members {
				out[n.Name] = n.State
			}
		}
	}
	return out
}
func TestHealthyJoinAndEncodedTraffic(t *testing.T) {
	r := run(t, fixture())
	for _, name := range []string{"a", "b", "c"} {
		if states := members(r, name); len(states) != 3 || states["a"] != wire.Alive || states["b"] != wire.Alive || states["c"] != wire.Alive {
			t.Fatal(name, states)
		}
	}
	if len(effects(r, "core.WriteStream")) < 4 || len(effects(r, "core.SendDatagram")) == 0 {
		t.Fatal("join and gossip paths absent")
	}
	for _, m := range r.Measurements {
		if m.SentBytes == 0 || m.ReceivedBytes == 0 {
			t.Fatal("wire bytes unmeasured", m)
		}
	}
}
func TestIndirectAndTCPOnly(t *testing.T) {
	for _, tcpOnly := range []bool{false, true} {
		s := fixture()
		s.Actions = append(s.Actions, Action{At: core.Instant(600 * time.Millisecond), Node: "a", Kind: "probe", Peer: "b"})
		s.Faults = []Fault{{From: "a", To: "b", Start: core.Instant(500 * time.Millisecond), End: s.Horizon, Drop: true}}
		if tcpOnly {
			for _, from := range []string{"a", "b", "c"} {
				for _, to := range []string{"a", "b", "c"} {
					if from != to {
						s.Faults = append(s.Faults, Fault{From: from, To: to, Start: core.Instant(500 * time.Millisecond), End: s.Horizon, Drop: true})
					}
				}
			}
		}
		r := run(t, s)
		if members(r, "a")["b"] != wire.Alive {
			t.Fatal(tcpOnly, members(r, "a"))
		}
		indirect := false
		for _, raw := range effects(r, "core.SendDatagram") {
			var e core.SendDatagram
			_ = json.Unmarshal(raw, &e)
			for _, m := range e.Messages {
				if m.Indirect != nil {
					indirect = true
				}
			}
		}
		if !indirect {
			t.Fatal("did not escalate through shared indirect handler")
		}
		if tcpOnly {
			found := false
			for _, m := range r.Measurements {
				if m.Name == "a" && m.Snapshot.Stats.TCPOnly > 0 && m.Snapshot.Stats.FailedProbes == 0 {
					found = true
				}
			}
			if !found {
				t.Fatal("TCP-only success was not observed", r.Measurements)
			}
		}
	}
}
func TestCrashPartitionHealingAndRejoin(t *testing.T) {
	crash := fixture()
	crash.Horizon = core.Instant(2 * time.Second)
	crash.Actions = append(crash.Actions, Action{At: core.Instant(500 * time.Millisecond), Node: "b", Kind: "crash"}, Action{At: core.Instant(600 * time.Millisecond), Node: "a", Kind: "probe", Peer: "b"})
	r := run(t, crash)
	if members(r, "a")["b"] != wire.Dead {
		t.Fatal("crash not detected", members(r, "a"))
	}
	partition := fixture()
	partition.Horizon = core.Instant(2 * time.Second)
	for _, from := range []string{"a", "b", "c"} {
		for _, to := range []string{"a", "b", "c"} {
			if from != to {
				partition.Faults = append(partition.Faults, Fault{From: from, To: to, Start: core.Instant(500 * time.Millisecond), End: core.Instant(time.Second), Drop: true, StreamFailure: true})
			}
		}
	}
	partition.Actions = append(partition.Actions, Action{At: core.Instant(600 * time.Millisecond), Node: "a", Kind: "probe", Peer: "b"}, Action{At: core.Instant(600 * time.Millisecond), Node: "b", Kind: "probe", Peer: "a"}, Action{At: core.Instant(1200 * time.Millisecond), Node: "b", Kind: "join", Peer: "a"})
	r = run(t, partition)
	if members(r, "a")["b"] != wire.Alive || members(r, "b")["a"] != wire.Alive {
		t.Fatal("explicit rejoin failed", r.Measurements)
	}
	suspects := 0
	for _, raw := range effects(r, "core.Notification") {
		var n core.Notification
		_ = json.Unmarshal(raw, &n)
		if n.Kind == "suspect" {
			suspects++
		}
	}
	if suspects == 0 {
		t.Fatal("partition never exercised suspicion")
	}
}
func TestPauseBuffersAndBackpressure(t *testing.T) {
	s := fixture()
	s.Horizon = core.Instant(2 * time.Second)
	s.Actions = append(s.Actions, Action{At: core.Instant(500 * time.Millisecond), Node: "b", Kind: "pause", Duration: 300 * time.Millisecond}, Action{At: core.Instant(510 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: bytes.Repeat([]byte{'x'}, 10000)}, Action{At: core.Instant(550 * time.Millisecond), Node: "a", Kind: "probe", Peer: "b"})
	r := run(t, s)
	user := false
	timeout := false
	for _, raw := range effects(r, "core.Notification") {
		var n core.Notification
		_ = json.Unmarshal(raw, &n)
		if n.Kind == "user" && len(n.Data) == 10000 {
			user = true
		}
	}
	for _, raw := range effects(r, "core.Result") {
		var n core.Result
		_ = json.Unmarshal(raw, &n)
		if n.Failure == core.Timeout {
			timeout = true
		}
	}
	if user || !timeout {
		t.Fatal("blocked large write did not time out", user, timeout)
	}
	for _, trace := range r.Trace {
		if trace.Node == "b" && strings.HasPrefix(trace.Kind, "core.") && trace.Time >= core.Instant(500*time.Millisecond) && trace.Time < core.Instant(800*time.Millisecond) {
			t.Fatal("paused core stepped", trace)
		}
	}
	// A frame larger than transport capacity progresses by draining credits.
	healthy := fixture()
	healthy.Actions = append(healthy.Actions, Action{At: core.Instant(500 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: bytes.Repeat([]byte{'x'}, 300)})
	r = run(t, healthy)
	user = false
	for _, raw := range effects(r, "core.Notification") {
		var n core.Notification
		_ = json.Unmarshal(raw, &n)
		if n.Kind == "user" && len(n.Data) == 300 {
			user = true
		}
	}
	if !user {
		t.Fatal("frame larger than capacity stalled")
	}
}
func TestFaultsDuplicateDelayAndIncomplete(t *testing.T) {
	s := fixture()
	s.Faults = []Fault{{From: "a", To: "b", Start: 0, End: s.Horizon, Delay: time.Millisecond, Jitter: time.Millisecond, Duplicate: true}}
	r := run(t, s)
	if len(r.Manifest.PreparedFaults) != 1 || r.Manifest.PreparedFaults[0].Jitter != 0 {
		t.Fatal("fault schedule not pinned")
	}
	s.EventBudget = 1
	limited, err := Run(s)
	if err != nil || limited.Complete || limited.Reason != "event budget exhausted" {
		t.Fatal(limited, err)
	}
	s = fixture()
	s.Protocol.Protected = true
	if _, err = Run(s); err == nil {
		t.Fatal("encryption silently ignored")
	}
	s = fixture()
	s.Protocol.Application = true
	if _, err = Run(s); err == nil {
		t.Fatal("arbitrary application scenario accepted")
	}
}
func raceScenario(seed uint64) Scenario {
	s := fixture()
	s.Nodes = s.Nodes[:2]
	for j := range s.Nodes {
		s.Nodes[j].Service = 0
	}
	s.Protocol.GossipInterval = 0
	s.Protocol.ProbeInterval = 0
	s.Protocol.ProbeTimeout = 100 * time.Millisecond
	s.Latency = 50 * time.Millisecond
	s.Actions = []Action{{At: core.Instant(time.Second), Kind: "ping", Node: "a", Peer: "b"}}
	s.Horizon = core.Instant(2 * time.Second)
	s.Seeds.Schedule = seed
	return s
}
func TestScheduleSeedExploresEqualTimeRace(t *testing.T) {
	seen := map[core.Failure]bool{}
	var reference Manifest
	for seed := uint64(1); seed <= 40; seed++ {
		s := raceScenario(seed)
		r := run(t, s)
		again := run(t, s)
		if !bytes.Equal(r.TraceBytes(), again.TraceBytes()) {
			t.Fatal("same seed changed trace")
		}
		for _, raw := range effects(r, "core.Result") {
			var result core.Result
			_ = json.Unmarshal(raw, &result)
			seen[result.Failure] = true
		}
		if seed == 1 {
			reference = r.Manifest
		} else if !reflect.DeepEqual(reference.NodeSeeds, r.Manifest.NodeSeeds) || !reflect.DeepEqual(reference.PreparedFaults, r.Manifest.PreparedFaults) {
			t.Fatal("schedule seed changed protocol seeds or faults")
		}
	}
	if !seen[core.Success] || !seen[core.Timeout] {
		t.Fatal("equal-time race did not explore both outcomes", seen)
	}
}
func replayScenario() Scenario {
	s := fixture()
	s.Faults = []Fault{{From: "a", To: "b", Start: core.Instant(500 * time.Millisecond), End: s.Horizon, Drop: true, Duplicate: true, Delay: time.Millisecond, Jitter: time.Millisecond}}
	s.Actions = append(s.Actions, Action{At: core.Instant(600 * time.Millisecond), Node: "a", Kind: "probe", Peer: "b"}, Action{At: core.Instant(700 * time.Millisecond), Node: "c", Kind: "pause", Duration: 50 * time.Millisecond})
	return s
}
func TestReplayProcess(t *testing.T) {
	if os.Getenv("LIGHTPOOL_REPLAY_CHILD") != "1" {
		return
	}
	r, err := Run(replayScenario())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, _ := json.Marshal(struct{ Trace, Measurements json.RawMessage }{r.TraceBytes(), r.MeasurementBytes()})
	_, _ = os.Stdout.Write(b)
	os.Exit(0)
}
func TestFreshProcessReplay(t *testing.T) {
	var reference []byte
	for j := 0; j < 10; j++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplayProcess$")
		cmd.Env = append(os.Environ(), "LIGHTPOOL_REPLAY_CHILD=1", "GOMAXPROCS="+strconv.Itoa(1+j%3))
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatal(err, string(out))
		}
		if j == 0 {
			reference = out
		} else if !bytes.Equal(reference, out) {
			t.Fatalf("fresh process %d differed", j)
		}
	}
	t.Logf("10 fresh processes, GOMAXPROCS=1/2/3: identical %d-byte traces and measurements", len(reference))
}

func TestPausedReceiveBufferOverflow(t *testing.T) {
	s := fixture()
	s.DatagramCapacity = 1000
	s.Actions = append(s.Actions, Action{At: core.Instant(500 * time.Millisecond), Node: "b", Kind: "pause", Duration: 200 * time.Millisecond}, Action{At: core.Instant(510 * time.Millisecond), Node: "a", Kind: "send", Peer: "b", Data: bytes.Repeat([]byte{'q'}, 700)}, Action{At: core.Instant(520 * time.Millisecond), Node: "a", Kind: "send", Peer: "b", Data: bytes.Repeat([]byte{'q'}, 700)})
	r := run(t, s)
	found := false
	for _, m := range r.Measurements {
		if m.Name == "b" && m.DatagramDrops > 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("finite receive buffer never overflowed")
	}
	users := 0
	for _, raw := range effects(r, "core.Notification") {
		var n core.Notification
		_ = json.Unmarshal(raw, &n)
		if n.Kind == "user" && len(n.Data) == 700 {
			users++
		}
	}
	if users != 1 {
		t.Fatal("expected exactly one buffered user message", users)
	}
}
func TestStreamClosesMidFrame(t *testing.T) {
	s := fixture()
	s.Actions = append(s.Actions, Action{At: core.Instant(500 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: bytes.Repeat([]byte{'z'}, 10000)}, Action{At: core.Instant(510 * time.Millisecond), Node: "b", Kind: "crash"})
	r := run(t, s)
	failed := false
	for _, raw := range effects(r, "core.Result") {
		var result core.Result
		_ = json.Unmarshal(raw, &result)
		if result.Failure == core.RemoteFailure {
			failed = true
		}
	}
	for _, raw := range effects(r, "core.Notification") {
		var n core.Notification
		_ = json.Unmarshal(raw, &n)
		if n.Kind == "user" && len(n.Data) == 10000 {
			t.Fatal("partial frame delivered")
		}
	}
	if !failed {
		t.Fatal("mid-frame close not surfaced")
	}
}
