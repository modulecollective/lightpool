package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/modulecollective/lightpool/sim/trace"
)

type fixture struct {
	t              *testing.T
	b              bytes.Buffer
	seq            uint64
	at             int64
	sent, received uint64
}

func newFixture(t *testing.T, horizon string, faults []Fault) *fixture {
	t.Helper()
	f := &fixture{t: t}
	nodes := []trace.Node{}
	initial := []trace.Process{}
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		nodes = append(nodes, trace.Node{Name: n, Address: n + ":7946"})
		initial = append(initial, trace.Process{Node: n, Lifetime: "0", PausedUntil: "0"})
	}
	f.write(trace.Header{Type: "header", Format: "lightpool-trace", Version: 1, Nodes: nodes, Initial: initial, Manifest: map[string]any{"Code": "fixture-rev", "Toolchain": "fixture-go", "Scenario": map[string]any{"Horizon": horizon, "Protocol": map[string]any{"DisableTCP": true}, "Seeds": map[string]any{"Protocol": "9007199254740993"}, "Faults": faults}}})
	for _, n := range nodes {
		f.event(0, "action.start", n.Name, "", processChange(n.Name, "start", "1", true, "0"))
	}
	return f
}
func (f *fixture) write(v any) {
	f.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		f.t.Fatal(err)
	}
	f.b.Write(b)
	f.b.WriteByte('\n')
}
func (f *fixture) event(at int64, kind, actor, peer string, changes ...trace.Change) {
	f.t.Helper()
	f.seq++
	f.at = at
	if changes == nil {
		changes = []trace.Change{}
	}
	f.write(trace.Event{Type: "event", Seq: trace.Decimal(f.seq), Time: trace.Decimal(at), ID: trace.Decimal(f.seq), Kind: kind, Actor: actor, Peer: peer, Lifetime: "1", Changes: changes, Traffic: []trace.Traffic{}})
}
func (f *fixture) traffic(at int64, entries ...trace.Traffic) {
	f.seq++
	f.at = at
	for _, e := range entries {
		var n uint64
		_, err := fmt.Sscan(e.Bytes, &n)
		if err != nil {
			f.t.Fatal(err)
		}
		if e.Stage == "sent" {
			f.sent += n
		}
		if e.Stage == "received" {
			f.received += n
		}
	}
	f.write(trace.Event{Type: "event", Seq: trace.Decimal(f.seq), Time: trace.Decimal(at), ID: trace.Decimal(f.seq), Kind: "core.datagram", Lifetime: "1", Changes: []trace.Change{}, Traffic: entries})
}
func (f *fixture) end(complete bool) {
	f.write(trace.End{Type: "end", Seq: trace.Decimal(f.seq), Time: trace.Decimal(f.at), Reason: "horizon", Complete: complete, Nodes: []trace.Measurement{}, Totals: trace.Totals{SentBytes: trace.Decimal(f.sent), ReceivedBytes: trace.Decimal(f.received), DatagramDrops: "0", Detections: "0"}})
}
func (f *fixture) read() *Report {
	f.t.Helper()
	r, err := Read(bytes.NewReader(f.b.Bytes()))
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}
func processChange(node, phase, life string, running bool, paused string) trace.Change {
	return trace.Change{Kind: "process", Node: node, Phase: phase, Process: &trace.Process{Node: node, Lifetime: life, Running: running, PausedUntil: paused}}
}
func member(observer, state string) trace.Change {
	return trace.Change{Kind: "member.upsert", Node: observer, Member: &trace.Member{Name: "b", Address: "b:7946", Incarnation: "1", State: state}}
}
func summary(node string, stopped, left bool) trace.Change {
	return trace.Change{Kind: "summary", Node: node, Summary: &trace.Summary{Health: "0", Backlog: "0", PendingStreams: "0", Stopped: stopped, Left: left}}
}
func equalTime(t *testing.T, got *int64, want int64) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("time %v, want %d", got, want)
	}
}

func TestCrashCohortAndCensoring(t *testing.T) {
	f := newFixture(t, "100", nil)
	f.event(1, "core.datagram", "a", "", member("a", "alive"), member("c", "suspect"), member("d", "dead"))
	f.event(10, "action.crash", "b", "", processChange("b", "crash", "1", false, "0"))
	f.event(12, "core.datagram", "a", "", member("a", "dead"))
	f.event(13, "core.shutdown", "c", "", summary("c", true, false))
	again := member("a", "dead")
	again.Member.Incarnation = "2"
	f.event(14, "core.datagram", "a", "", again)
	f.end(true)
	r := f.read()
	c := r.Crashes[0]
	if c.Latencies.Eligible != 2 || c.Latencies.Completed != 1 || c.AlreadyDead != 1 || c.UnknownOrLeft != 1 || c.First == nil || c.First.TimeNS != 12 {
		t.Fatalf("cohort: %+v", c)
	}
	equalTime(t, c.Latencies.P50NS, 2)
	equalTime(t, c.Latencies.P95NS, 2)
	equalTime(t, c.Latencies.MaxNS, 2)
	if c.Observations[1].Status != "observer_shutdown" || c.Observations[1].End.TimeNS != 13 || c.Observations[1].LatencyNS != nil {
		t.Fatal(c.Observations)
	}
	if r.RunningDeclarations != 1 || r.StoppedDeclarations != 1 || len(r.Declarations) != 2 {
		t.Fatal(r.Declarations)
	}
	if r.ObservedThroughNS != 100 || r.LastEvent.TimeNS != 14 {
		t.Fatal("lost configured horizon")
	}
}
func TestRecoveryRemovalPauseFaultsAndJoin(t *testing.T) {
	fault := Fault{From: "c", To: "a", Start: "5", End: "20", Drop: true}
	f := newFixture(t, "40", []Fault{fault})
	f.event(5, "action.pause", "b", "", processChange("b", "pause", "1", true, "30"))
	f.event(10, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(11, "core.datagram", "a", "", member("a", "dead"))
	f.event(12, "core.timer-delivery", "a", "", trace.Change{Kind: "member.remove", Node: "a", Subject: "b"})
	f.event(13, "action.join", "b", "c")
	f.event(20, "core.datagram", "a", "", member("a", "alive"))
	f.event(21, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(22, "core.leave", "a", "", summary("a", false, true))
	f.end(true)
	r := f.read()
	if r.RunningDeclarations != 2 || r.AffectedRunningPairs != 1 || r.AffectedRunningTargets != 1 {
		t.Fatalf("counts %+v", r)
	}
	d := r.Declarations[0]
	if !d.TargetPaused || d.ObserverPaused || len(d.Faults) != 1 || len(r.Declarations[1].Faults) != 0 {
		t.Fatal(r.Declarations)
	}
	a, b := r.Recoveries[0], r.Recoveries[1]
	equalTime(t, a.DurationNS, 10)
	if a.Status != "recovered" || len(a.Joins) != 1 || a.Joins[0].Peer != "c" || b.Status != "observer_leave" || b.DurationNS != nil {
		t.Fatal(r.Recoveries)
	}
	if r.RecoveryDurations.Eligible != 2 || r.RecoveryDurations.Completed != 1 {
		t.Fatal(r.RecoveryDurations)
	}
}
func TestSameTimeOrderAndRestartLifetimes(t *testing.T) {
	for _, deadFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(deadFirst), func(t *testing.T) {
			f := newFixture(t, "50", nil)
			f.event(1, "core.datagram", "a", "", member("a", "alive"))
			if deadFirst {
				f.event(10, "core.timer-delivery", "a", "", member("a", "dead"))
			}
			f.event(10, "action.crash", "b", "", processChange("b", "crash", "1", false, "0"))
			if !deadFirst {
				f.event(10, "core.timer-delivery", "a", "", member("a", "dead"))
			}
			f.event(15, "action.start", "b", "", processChange("b", "start", "2", true, "0"))
			f.event(16, "core.datagram", "a", "", member("a", "alive"))
			f.event(17, "core.timer-delivery", "a", "", member("a", "dead"))
			f.event(18, "action.crash", "a", "", processChange("a", "crash", "1", false, "0"))
			f.event(19, "action.start", "a", "", processChange("a", "start", "2", true, "0"))
			f.event(20, "core.timer-delivery", "a", "", member("a", "dead"))
			f.end(true)
			r := f.read()
			c := r.Crashes[0]
			if deadFirst {
				if c.AlreadyDead != 1 || c.Latencies.Eligible != 0 || r.Recoveries[0].Status != "target_crash" {
					t.Fatal(c, r.Recoveries)
				}
			} else {
				if c.Latencies.Completed != 1 {
					t.Fatal(c)
				}
				equalTime(t, c.Latencies.P50NS, 0)
			}
			last := r.Declarations[len(r.Declarations)-1]
			if last.ObserverLifetime != "2" || last.TargetLifetime != "2" {
				t.Fatal(last)
			}
			if r.Recoveries[len(r.Recoveries)-1].Status != "unresolved" || r.Recoveries[len(r.Recoveries)-1].End.TimeNS != 50 {
				t.Fatal(r.Recoveries)
			}
		})
	}
}
func TestTargetRestartAndObserverRestartCensorCohorts(t *testing.T) {
	for _, node := range []string{"a", "b"} {
		t.Run(node, func(t *testing.T) {
			f := newFixture(t, "40", nil)
			f.event(1, "core.datagram", "a", "", member("a", "suspect"))
			f.event(10, "action.crash", "b", "", processChange("b", "crash", "1", false, "0"))
			if node == "a" {
				f.event(11, "action.crash", "a", "", processChange("a", "crash", "1", false, "0"))
			}
			f.event(12, "action.start", node, "", processChange(node, "start", "2", true, "0"))
			f.event(15, "core.datagram", "a", "", member("a", "dead"))
			f.end(true)
			c := f.read().Crashes[0]
			if c.Latencies.Completed != 0 || c.Observations[0].LatencyNS != nil || c.Observations[0].Status == "unresolved" {
				t.Fatal(c)
			}
		})
	}
}
func TestEmptyHealthyZeroDurationAndNearestRank(t *testing.T) {
	f := newFixture(t, "0", nil)
	f.end(true)
	r := f.read()
	if len(r.Declarations) != 0 || len(r.Crashes) != 0 || r.RecoveryDurations.P50NS != nil || r.RecoveryDurations.Reason != "empty eligible population" || r.Traffic.SentBytesPerSecond != nil || r.Traffic.RateReason == "" {
		t.Fatal(r)
	}
	d := distribution([]int64{10, 3, 8, 2, 1}, 9)
	equalTime(t, d.P50NS, 3)
	equalTime(t, d.P95NS, 10)
}
func TestExactTrafficStagesAndJSON(t *testing.T) {
	f := newFixture(t, "1000000000", nil)
	entry := trace.Traffic{ID: "9007199254740993", Transport: "datagram", From: "a", To: "b", Bytes: "9007199254740993", Messages: []string{"ping"}}
	for _, stage := range []string{"attempt", "sent", "arrival", "received", "handle", "drop"} {
		entry.Stage = stage
		entry.Detail = "directional fault"
		f.traffic(1, entry)
	}
	entry.Transport = "stream"
	entry.Bytes = "7"
	entry.Stage = "sent"
	f.traffic(2, entry)
	entry.Stage = "received"
	entry.Bytes = "3"
	f.traffic(3, entry)
	f.end(true)
	r := f.read()
	if r.Traffic.Datagram.Sent != 9007199254740993 || r.Traffic.Stream.Received != 3 || r.Traffic.EndBytesMatch == nil || !*r.Traffic.EndBytesMatch || len(r.Traffic.Drops) != 1 || r.Traffic.Drops[0].Count != 1 {
		t.Fatal(r.Traffic)
	}
	var out bytes.Buffer
	if err := Write(&out, r, "json"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"sent": "9007199254740993"`) || !strings.Contains(out.String(), `"Protocol": "9007199254740993"`) {
		t.Fatal(out.String())
	}
	if *r.Traffic.SentBytesPerSecond != "9007199254741000.000000" {
		t.Fatal(r.Traffic)
	}
}
func TestIncompleteMalformedAndReadFailures(t *testing.T) {
	f := newFixture(t, "100", nil)
	f.event(10, "core.datagram", "a", "", member("a", "dead"))
	prefix := append([]byte(nil), f.b.Bytes()...)
	for _, suffix := range []string{"", `{"type":"event"`} {
		r, err := Read(strings.NewReader(string(prefix) + suffix))
		if err != nil || r.Complete || r.ObservedThroughNS != 10 || r.Recoveries[0].End.TimeNS != 10 {
			t.Fatal(r, err)
		}
	}
	f.end(false)
	if r := f.read(); r.Complete || r.ObservedThroughNS != 10 {
		t.Fatal(r)
	}
	_, err := Read(strings.NewReader(string(prefix) + "{}\n"))
	if err == nil || !strings.Contains(err.Error(), "line 8:") {
		t.Fatal(err)
	}
	_, err = Read(io.MultiReader(bytes.NewReader(prefix), errorReader{}))
	if err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatal(err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func TestWriteFailuresAndOldJoinEvidence(t *testing.T) {
	f := newFixture(t, "20", nil)
	f.event(1, "core.datagram", "a", "", member("a", "dead"))
	f.event(2, "action.join", "b", "")
	f.end(true)
	r := f.read()
	if len(r.Unsupported) != 1 || len(r.Recoveries[0].Joins) != 1 {
		t.Fatal(r)
	}
	for _, format := range []string{"text", "json"} {
		if err := Write(errorWriter{}, r, format); err == nil || !strings.Contains(err.Error(), "write failed") {
			t.Fatal(err)
		}
	}
	if err := Write(io.Discard, r, "xml"); err == nil {
		t.Fatal("accepted format")
	}
}

func TestGracefulObserversUnknownAndNoCompletion(t *testing.T) {
	f := newFixture(t, "100", nil)
	f.event(1, "core.datagram", "a", "", member("a", "alive"), member("c", "alive"), member("d", "left"))
	f.event(2, "core.shutdown", "c", "", summary("c", true, false))
	f.event(10, "action.crash", "b", "", processChange("b", "crash", "1", false, "0"))
	f.end(true)
	r := f.read()
	c := r.Crashes[0]
	if len(r.Crashes) != 1 || c.Inactive != 1 || c.UnknownOrLeft != 2 || c.Latencies.Eligible != 1 || c.Latencies.Completed != 0 || c.Latencies.P50NS != nil || c.Latencies.Reason != "no observed completions" || c.Observations[0].Status != "unresolved" || c.Observations[0].End.TimeNS != 100 {
		t.Fatal(c)
	}
}
func TestRemovalReentryAndTargetRestart(t *testing.T) {
	f := newFixture(t, "50", nil)
	f.event(1, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(2, "core.timer-delivery", "a", "", trace.Change{Kind: "member.remove", Node: "a", Subject: "b"})
	f.event(3, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(4, "core.datagram", "a", "", member("a", "alive"))
	f.event(5, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(6, "action.crash", "b", "", processChange("b", "crash", "1", false, "0"))
	f.event(7, "action.start", "b", "", processChange("b", "start", "2", true, "0"))
	f.event(8, "core.datagram", "a", "", member("a", "alive"))
	f.event(9, "core.timer-delivery", "a", "", member("a", "dead"))
	f.end(true)
	r := f.read()
	if r.RunningDeclarations != 4 || r.AffectedRunningPairs != 1 || r.RecoveryDurations.Completed != 2 || r.Recoveries[2].Status != "target_crash" {
		t.Fatal(r)
	}
	equalTime(t, r.Recoveries[0].DurationNS, 3)
	equalTime(t, r.Recoveries[1].DurationNS, 1)
}
func TestLargeTimeAndCounterOverflow(t *testing.T) {
	const start = int64(9007199254740993)
	f := newFixture(t, "9007199254741093", nil)
	f.event(start, "action.pause", "a", "", processChange("a", "pause", "1", true, "9007199254741050"))
	f.event(start+1, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(start+8, "core.datagram", "a", "", member("a", "alive"))
	f.end(true)
	r := f.read()
	equalTime(t, r.Recoveries[0].DurationNS, 7)
	if !r.Declarations[0].ObserverPaused || r.Declarations[0].TimeNS != start+1 || r.ObservedThroughNS != start+100 {
		t.Fatal(r)
	}
	var b bytes.Buffer
	if err := Write(&b, r, "json"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `"timeNS": "9007199254740994"`) {
		t.Fatal(b.String())
	}
	overflow := newFixture(t, "2", nil)
	e := trace.Traffic{ID: "1", Transport: "stream", Stage: "sent", From: "a", To: "b", Bytes: "18446744073709551615", Messages: []string{}}
	overflow.traffic(1, e)
	e.Bytes = "1"
	overflow.traffic(2, e)
	if _, err := Read(bytes.NewReader(overflow.b.Bytes())); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatal(err)
	}
}
func TestHorizonAndManifestRefusal(t *testing.T) {
	f := newFixture(t, "1", nil)
	f.event(2, "core.datagram", "a", "")
	f.end(true)
	if _, err := Read(bytes.NewReader(f.b.Bytes())); err == nil || !strings.Contains(err.Error(), "horizon") {
		t.Fatal(err)
	}
	raw := strings.Replace(f.b.String(), `"Toolchain":"fixture-go"`, `"Toolchain":""`, 1)
	if _, err := Read(strings.NewReader(raw)); err == nil || !strings.Contains(err.Error(), "line 1:") {
		t.Fatal(err)
	}
}

func TestGracefulTargetIsNotActiveOrAnInjectedCrash(t *testing.T) {
	for _, left := range []bool{false, true} {
		f := newFixture(t, "100", nil)
		f.event(1, "core.shutdown", "b", "", summary("b", !left, left))
		f.event(2, "core.timer-delivery", "a", "", member("a", "dead"))
		f.end(true)
		r := f.read()
		d := r.Declarations[0]
		if len(r.Crashes) != 0 || !d.TargetRunning || d.TargetStopped == left || d.TargetLeft != left || r.Recoveries[0].Status != "target_inactive" || r.Recoveries[0].End.TimeNS != 2 {
			t.Fatal(r)
		}
	}
}

func TestTextKeepsFaultContextReadable(t *testing.T) {
	faults := []Fault{}
	for i := range 14 {
		faults = append(faults, Fault{From: "c", To: "a", Start: "1", End: "10", Delay: fmt.Sprint(i), Drop: true})
	}
	f := newFixture(t, "20", faults)
	f.event(2, "core.timer-delivery", "a", "", member("a", "dead"))
	f.event(3, "core.timer-delivery", "a", "", trace.Change{Kind: "member.remove", Node: "a", Subject: "b"})
	f.event(4, "core.timer-delivery", "a", "", member("a", "dead"))
	f.end(true)
	var out bytes.Buffer
	if err := Write(&out, f.read(), "text"); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), `"From":"c"`) != 14 || !strings.Contains(out.String(), "F14 ") {
		t.Fatal("fault legend missing or repeated")
	}
	for line := range strings.SplitSeq(out.String(), "\n") {
		if len(line) > 350 {
			t.Fatalf("unreadable report line (%d bytes): %.150s", len(line), line)
		}
	}
}

func TestIndependentLossFaultSurvivesOfflineReport(t *testing.T) {
	f := newFixture(t, "100", []Fault{{From: "a", To: "b", Start: "10", End: "90", LossPPM: "100000"}})
	f.event(1, "core.datagram", "a", "", member("a", "alive"))
	f.event(20, "core.datagram", "a", "", member("a", "dead"))
	f.end(true)
	r := f.read()
	if len(r.Declarations) != 1 || len(r.Declarations[0].Faults) != 1 || r.Declarations[0].Faults[0].LossPPM != "100000" {
		t.Fatal("loss field was discarded", r.Declarations)
	}
	var text bytes.Buffer
	if err := Write(&text, r, "text"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), `"LossPPM":"100000"`) || strings.Contains(text.String(), "No loss probabilities") {
		t.Fatal("loss context/model limit lost", text.String())
	}
}
