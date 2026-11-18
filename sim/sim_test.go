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

func TestTransportDeadlineDuringBothEndpointPauses(t *testing.T) {
	for _, paused := range []bool{false, true} {
		t.Run(fmt.Sprint("paused=", paused), func(t *testing.T) {
			s := fixture()
			s.Nodes = s.Nodes[:2]
			for i := range s.Nodes {
				s.Nodes[i].Service = time.Millisecond
			}
			s.Protocol.ProbeInterval, s.Protocol.GossipInterval, s.Protocol.PushPullInterval = 0, 0, 0
			s.Protocol.StreamTimeout = 100 * time.Millisecond
			s.Seeds = Seeds{1, 2, 7}
			s.Latency = 0
			s.Horizon = core.Instant(1200 * time.Millisecond)
			s.Actions = []Action{{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: bytes.Repeat([]byte{'x'}, 200)}}
			if paused {
				for _, name := range []string{"a", "b"} {
					s.Actions = append(s.Actions, Action{At: core.Instant(13 * time.Millisecond), Node: name, Kind: "pause", Duration: time.Second})
				}
			}
			r := run(t, s)
			users := 0
			for _, raw := range effects(r, "core.Notification") {
				var n core.Notification
				_ = json.Unmarshal(raw, &n)
				if n.Kind == "user" {
					users++
				}
			}
			results := effects(r, "core.Result")
			if len(results) != 1 {
				t.Fatalf("terminal results: %s", results)
			}
			var result core.Result
			_ = json.Unmarshal(results[0], &result)
			if paused {
				if users != 0 || result.Failure != core.Timeout {
					t.Fatalf("expired transfer delivered: users=%d result=%+v", users, result)
				}
				for _, tr := range r.Trace {
					if tr.Kind == "chunk" && tr.Time > core.Instant(111*time.Millisecond) {
						t.Fatalf("transfer progressed after deadline: %+v", tr)
					}
				}
			} else if users != 1 || result.Failure != core.Success {
				t.Fatalf("healthy transfer: users=%d result=%+v", users, result)
			}
		})
	}
}

// A delayed application response starts the writer's deadline later than the
// reader's. Only independent read expiry can stop this partial frame on pause.
func TestReadDeadlineDuringReceiverPause(t *testing.T) {
	for _, tc := range []struct {
		name, reader string
		pauseAt      core.Instant
		readDeadline core.Instant
	}{
		{name: "unpaused"},
		{name: "accepted-reader", reader: "b", pauseAt: core.Instant(52 * time.Millisecond), readDeadline: core.Instant(110 * time.Millisecond)},
		{name: "dialed-reader", reader: "a", pauseAt: core.Instant(96 * time.Millisecond), readDeadline: core.Instant(151 * time.Millisecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture()
			s.Nodes = s.Nodes[:2]
			for i := range s.Nodes {
				s.Nodes[i].Service = time.Millisecond
			}
			s.Protocol.ProbeInterval, s.Protocol.GossipInterval, s.Protocol.PushPullInterval = 0, 0, 0
			s.Protocol.StreamTimeout = 100 * time.Millisecond
			s.Protocol.Application = true
			s.Fixtures = []Fixture{{Kind: core.LocalState, Delay: 40 * time.Millisecond, Data: bytes.Repeat([]byte{'x'}, 500)}}
			s.Seeds = Seeds{1, 2, 5}
			s.Latency = 0
			s.StreamCapacity = 256
			s.Horizon = core.Instant(400 * time.Millisecond)
			s.Actions = []Action{
				{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "join", Peer: "b"},
				{At: core.Instant(180 * time.Millisecond), Node: "a", Kind: "join", Peer: "b"},
			}
			if tc.reader != "" {
				s.Actions = append(s.Actions, Action{At: tc.pauseAt, Node: tc.reader, Kind: "pause", Duration: 73 * time.Millisecond})
			}
			r := run(t, s)
			chunks, receiverRequests := 0, 0
			for _, tr := range r.Trace {
				if tr.Time >= core.Instant(180*time.Millisecond) {
					continue
				}
				if tr.Kind == "chunk" {
					chunks++
					if tc.reader != "" && tr.Time > tc.readDeadline {
						t.Errorf("partial frame transferred after read deadline %v: %+v", time.Duration(tc.readDeadline), tr)
					}
				}
				for _, raw := range tr.Effects {
					var e tagged
					if err := json.Unmarshal(raw, &e); err != nil {
						t.Fatal(err)
					}
					if e.Kind == "core.Request" && tr.Node == "b" {
						var request core.Request
						if err := json.Unmarshal(e.Effect, &request); err != nil {
							t.Fatal(err)
						}
						if request.Kind == core.LocalState {
							receiverRequests++
						}
					}
				}
			}
			if chunks < 2 {
				t.Fatal("partial-frame transport path absent", chunks)
			}
			wantRequests := 1
			if tc.reader == "b" {
				wantRequests = 0
			}
			if receiverRequests != wantRequests {
				t.Errorf("receiver LocalState requests before retry: %d", receiverRequests)
			}
			results := effects(r, "core.Result")
			if len(results) != 2 {
				t.Fatalf("initial join and retry results: %s", results)
			}
			for i, raw := range results {
				var result core.Result
				if err := json.Unmarshal(raw, &result); err != nil {
					t.Fatal(err)
				}
				want := core.Success
				if tc.reader != "" && i == 0 {
					want = core.Timeout
				}
				if result.Failure != want {
					t.Errorf("join %d: failure=%v want=%v", i, result.Failure, want)
				}
			}
			for _, m := range r.Measurements {
				if len(m.Snapshot.Members) != 2 || m.Snapshot.PendingStreams != 0 {
					t.Fatal("retry did not recover", m)
				}
			}
			t.Logf("paused reader=%q: initial chunks=%d, receiver requests=%d; retry converged with zero pending streams", tc.reader, chunks, receiverRequests)
		})
	}
}

func TestConnectDeadlineDuringInitiatorPause(t *testing.T) {
	s := fixture()
	s.Nodes = s.Nodes[:2]
	s.Protocol.ProbeInterval, s.Protocol.GossipInterval, s.Protocol.PushPullInterval = 0, 0, 0
	s.Protocol.StreamTimeout = 100 * time.Millisecond
	s.Latency = 200 * time.Millisecond
	s.Horizon = core.Instant(1200 * time.Millisecond)
	s.Actions = []Action{
		{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: []byte("late")},
		{At: core.Instant(13 * time.Millisecond), Node: "a", Kind: "pause", Duration: time.Second},
	}
	r := run(t, s)
	if len(effects(r, "core.WriteStream")) != 0 || len(effects(r, "core.Notification")) != 0 {
		t.Fatal("expired connect established a stream")
	}
	results := effects(r, "core.Result")
	if len(results) != 1 {
		t.Fatalf("terminal results: %s", results)
	}
	var result core.Result
	_ = json.Unmarshal(results[0], &result)
	if result.Failure != core.Timeout {
		t.Fatal("expired connect did not report timeout", result)
	}
	for _, tr := range r.Trace {
		if tr.Kind == "core.Accepted" {
			t.Fatal("expired connect reached receiver", tr)
		}
	}
}

func TestStreamWriteRenewsReadDeadline(t *testing.T) {
	s := fixture()
	s.Nodes = s.Nodes[:2]
	for i := range s.Nodes {
		s.Nodes[i].Service = time.Millisecond
	}
	s.Protocol.ProbeInterval, s.Protocol.GossipInterval, s.Protocol.PushPullInterval = 0, 0, 0
	s.Protocol.StreamTimeout = 100 * time.Millisecond
	s.Protocol.Application = true
	s.Fixtures = []Fixture{{Kind: core.LocalState, Delay: 30 * time.Millisecond, Data: bytes.Repeat([]byte{'x'}, 100)}}
	s.Seeds = Seeds{1, 2, 5}
	s.Latency = 10 * time.Millisecond
	s.StreamCapacity = 256
	s.Horizon = core.Instant(300 * time.Millisecond)
	s.Actions = []Action{{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "join", Peer: "b"}}
	r := run(t, s)
	results := 0
	for _, tr := range r.Trace {
		for _, raw := range tr.Effects {
			var e tagged
			if err := json.Unmarshal(raw, &e); err != nil {
				t.Fatal(err)
			}
			if e.Kind == "core.Result" {
				var result core.Result
				if err := json.Unmarshal(e.Effect, &result); err != nil {
					t.Fatal(err)
				}
				if result.Failure != core.Success || tr.Time <= core.Instant(110*time.Millisecond) {
					t.Fatal("exchange must outlive the original read deadline", time.Duration(tr.Time), result)
				}
				t.Logf("join succeeded at %v after original 110ms read deadline", time.Duration(tr.Time))
				results++
			}
		}
	}
	if results != 1 || len(members(r, "a")) != 2 || len(members(r, "b")) != 2 {
		t.Fatal("renewed deadline did not permit exchange completion", results, r.Measurements)
	}
}

func TestShutdownInvalidatesPendingConnect(t *testing.T) {
	s := fixture()
	s.Nodes = s.Nodes[:2]
	s.Protocol.ProbeInterval, s.Protocol.GossipInterval, s.Protocol.PushPullInterval = 0, 0, 0
	s.Protocol.StreamTimeout = time.Second
	s.Latency = 200 * time.Millisecond
	s.Actions = []Action{
		{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: []byte("canceled")},
		{At: core.Instant(20 * time.Millisecond), Node: "a", Kind: "shutdown"},
	}
	r := run(t, s)
	for _, tr := range r.Trace {
		if tr.Kind == "core.Accepted" {
			t.Fatal("canceled connection reached receiver", tr)
		}
	}
}

func TestAcceptedFrameSurvivesTransportExpiry(t *testing.T) {
	s := fixture()
	s.Nodes = s.Nodes[:2]
	for i := range s.Nodes {
		s.Nodes[i].Service = time.Millisecond
	}
	s.Protocol.ProbeInterval, s.Protocol.GossipInterval, s.Protocol.PushPullInterval = 0, 0, 0
	s.Protocol.StreamTimeout = 100 * time.Millisecond
	s.Seeds = Seeds{1, 2, 7}
	s.Latency = 0
	s.Horizon = core.Instant(1200 * time.Millisecond)
	s.Actions = []Action{
		{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "reliable", Peer: "b", Data: []byte("x")},
		{At: core.Instant(9500 * time.Microsecond), Node: "b", Kind: "pause", Duration: time.Second},
		{At: core.Instant(11500 * time.Microsecond), Node: "a", Kind: "pause", Duration: time.Second},
	}
	r := run(t, s)
	users := 0
	for _, raw := range effects(r, "core.Notification") {
		var n core.Notification
		_ = json.Unmarshal(raw, &n)
		if n.Kind == "user" && bytes.Equal(n.Data, []byte("x")) {
			users++
		}
	}
	if users != 1 {
		t.Fatal("transport expiry discarded a frame already fully accepted", users)
	}
}

func TestUserPayloadBoundariesInSimulation(t *testing.T) {
	for _, mode := range []string{"send", "reliable", "broadcast"} {
		for _, data := range [][]byte{nil, {}, {1}} {
			t.Run(fmt.Sprintf("%s/%#v", mode, data), func(t *testing.T) {
				s := fixture()
				s.Nodes = s.Nodes[:2]
				s.Actions = []Action{
					{At: core.Instant(10 * time.Millisecond), Node: "a", Kind: "join", Peer: "b"},
					{At: core.Instant(100 * time.Millisecond), Node: "a", Kind: mode, Peer: "b", Data: data},
				}
				r := run(t, s)
				users := 0
				for _, raw := range effects(r, "core.Notification") {
					var n core.Notification
					_ = json.Unmarshal(raw, &n)
					if n.Kind == "user" {
						users++
						if !bytes.Equal(n.Data, data) {
							t.Fatalf("payload changed: %x", n.Data)
						}
					}
				}
				if users == 0 {
					t.Fatal("accepted user payload was not delivered")
				}
			})
		}
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
	for j := range 10 {
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

func TestRestartUsesANewLifetime(t *testing.T) {
	s := fixture()
	s.Horizon = core.Instant(2 * time.Second)
	s.Actions = append(s.Actions, Action{At: core.Instant(500 * time.Millisecond), Node: "b", Kind: "crash"}, Action{At: core.Instant(time.Second), Node: "b", Kind: "start"}, Action{At: core.Instant(1100 * time.Millisecond), Node: "b", Kind: "join", Peer: "a"})
	r := run(t, s)
	if members(r, "a")["b"] != wire.Alive {
		t.Fatal("restart did not rejoin")
	}
	found := false
	for _, trace := range r.Trace {
		if trace.Node != "b" || trace.Time < core.Instant(time.Second) {
			continue
		}
		for _, raw := range trace.Effects {
			var tag tagged
			_ = json.Unmarshal(raw, &tag)
			if tag.Kind == "core.Arm" {
				var arm core.Arm
				_ = json.Unmarshal(tag.Effect, &arm)
				if arm.Timer.Lifetime != 2 {
					t.Fatal("old lifetime reused")
				}
				found = true
			}
		}
	}
	if !found {
		t.Fatal("restart did not arm new work")
	}
}

func TestScheduleKeepsThePrescribedFaultedWorld(t *testing.T) {
	s := replayScenario()
	s.Seeds.Schedule = 1
	first := run(t, s)
	s.Seeds.Schedule = 2
	second := run(t, s)
	if len(first.Manifest.PreparedFaults) == 0 {
		t.Fatal("faulted fixture is empty")
	}
	if !reflect.DeepEqual(first.Manifest.PreparedFaults, second.Manifest.PreparedFaults) || !reflect.DeepEqual(first.Manifest.NodeSeeds, second.Manifest.NodeSeeds) {
		t.Fatal("schedule seed changed the faulted world or initial protocol seeds")
	}
}
