package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim/trace"
)

func TestExportMatchesRunAndReplaysState(t *testing.T) {
	scenarios := []Scenario{fixture(), fixture(), fixture()}
	scenarios[1].Actions = append(scenarios[1].Actions, Action{At: core.Instant(400 * time.Millisecond), Node: "b", Kind: "pause", Duration: 100 * time.Millisecond})
	scenarios[1].Faults = []Fault{{From: "a", To: "b", Start: 1, End: scenarios[1].Horizon, Duplicate: true}}
	scenarios[2].Actions = append(scenarios[2].Actions, Action{At: core.Instant(400 * time.Millisecond), Node: "b", Kind: "crash"}, Action{At: core.Instant(500 * time.Millisecond), Node: "b", Kind: "start"}, Action{At: core.Instant(550 * time.Millisecond), Node: "b", Kind: "join", Peer: "a"})
	var positions []Scenario
	for _, s := range scenarios {
		for _, h := range []time.Duration{250 * time.Millisecond, 450 * time.Millisecond, 500 * time.Millisecond, 650 * time.Millisecond, time.Second} {
			copy := s
			copy.Horizon = core.Instant(h)
			positions = append(positions, copy)
		}
	}
	for _, s := range positions {
		legacy := run(t, s)
		var output bytes.Buffer
		result, err := Export(s, &output, ExportOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if result.Trace != nil || result.Measurements != nil {
			t.Fatal("streaming retained history or full snapshots")
		}
		if result.Events != legacy.Events || !reflect.DeepEqual(result.Manifest, legacy.Manifest) {
			t.Fatal("export changed execution")
		}
		state := map[string]map[string]*trace.Member{}
		summaries := map[string]*trace.Summary{}
		var sent, received uint64
		index := 0
		var final trace.End
		complete, err := trace.Read(bytes.NewReader(output.Bytes()), func(b json.RawMessage) error {
			var tag struct{ Type string }
			if err := json.Unmarshal(b, &tag); err != nil {
				return err
			}
			if tag.Type == "event" {
				var e trace.Event
				if err := json.Unmarshal(b, &e); err != nil {
					return err
				}
				for _, traffic := range e.Traffic {
					var n uint64
					_, _ = fmt.Sscan(traffic.Bytes, &n)
					if traffic.Stage == "sent" {
						sent += n
					}
					if traffic.Stage == "received" {
						received += n
					}
				}
				oracle := legacy.Trace[index]
				index++
				if e.ID != trace.Decimal(oracle.ID) || e.Seq != trace.Decimal(oracle.Order) || e.Time != trace.Decimal(oracle.Time) {
					t.Fatalf("event schedule changed at %d", index)
				}
				for _, c := range e.Changes {
					switch c.Kind {
					case "process":
						if c.Phase == "start" {
							state[c.Node] = map[string]*trace.Member{}
						}
					case "member.upsert":
						state[c.Node][c.Member.Name] = c.Member
					case "member.remove":
						delete(state[c.Node], c.Subject)
					case "summary":
						summaries[c.Node] = c.Summary
					}
				}
			}
			if tag.Type == "end" {
				return json.Unmarshal(b, &final)
			}
			return nil
		})
		if err != nil || !complete {
			t.Fatal(complete, err)
		}
		for _, m := range legacy.Measurements {
			want := map[string]*trace.Member{}
			for _, n := range m.Snapshot.Members {
				want[n.Name] = trace.Membership(n)
			}
			if !reflect.DeepEqual(state[m.Name], want) {
				t.Fatalf("%s export state != snapshot", m.Name)
			}
			if !reflect.DeepEqual(summaries[m.Name], trace.Counters(m.Snapshot)) {
				t.Fatal("summary mismatch", m.Name)
			}
		}
		if final.Totals.SentBytes != trace.Decimal(sent) || final.Totals.ReceivedBytes != trace.Decimal(received) {
			t.Fatalf("traffic totals differ from measurements: sent %d/%s received %d/%s", sent, final.Totals.SentBytes, received, final.Totals.ReceivedBytes)
		}
		if len(final.Nodes) != len(legacy.Measurements) {
			t.Fatal("missing compact measurements")
		}
		var again bytes.Buffer
		if _, err = Export(s, &again, ExportOptions{Progress: func(Progress) {}}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(output.Bytes(), again.Bytes()) {
			t.Fatal("provenance-identical export differs")
		}
	}
}

func TestExportEndAfterUnrecordedEvents(t *testing.T) {
	for _, kind := range []string{"crash", "pause", "busy"} {
		t.Run(kind, func(t *testing.T) {
			raw, err := os.ReadFile("../examples/sim/healthy.json")
			if err != nil {
				t.Fatal(err)
			}
			var s Scenario
			if err := json.Unmarshal(raw, &s); err != nil {
				t.Fatal(err)
			}
			s.Nodes = s.Nodes[:1]
			s.Horizon = core.Instant(2 * time.Second)
			s.Actions = []Action{{At: core.Instant(100 * time.Millisecond), Kind: kind, Node: "a", Duration: 3 * time.Second}}
			wantTime := "100000000"
			if kind == "busy" {
				s.Actions = nil
				s.Nodes[0].Service = 3 * time.Second
				wantTime = "0"
			}
			legacy := run(t, s)
			var output bytes.Buffer
			result, err := Export(s, &output, ExportOptions{})
			if err != nil || !result.Complete || result.Events != legacy.Events || result.Reason != legacy.Reason {
				t.Fatalf("export changed execution: %+v, %v", result, err)
			}
			var events []trace.Event
			var end trace.End
			complete, err := trace.Read(bytes.NewReader(output.Bytes()), func(raw json.RawMessage) error {
				var tag struct{ Type string }
				if err := json.Unmarshal(raw, &tag); err != nil {
					return err
				}
				if tag.Type == "event" {
					var e trace.Event
					if err := json.Unmarshal(raw, &e); err != nil {
						return err
					}
					events = append(events, e)
				}
				if tag.Type == "end" {
					return json.Unmarshal(raw, &end)
				}
				return nil
			})
			if err != nil || !complete {
				t.Fatalf("successful export must be readable: complete=%v, %v", complete, err)
			}
			last := events[len(events)-1]
			if end.Time != wantTime || end.Time != last.Time || end.Seq != last.Seq {
				t.Fatalf("end does not match last event: %+v, %+v", end, last)
			}
			for i, e := range events {
				oracle := legacy.Trace[i]
				if e.ID != trace.Decimal(oracle.ID) || e.Seq != trace.Decimal(oracle.Order) || e.Time != trace.Decimal(oracle.Time) {
					t.Fatalf("event schedule changed at %d", i)
				}
			}
		})
	}
}

type failWriter struct{ remaining int }

func (w *failWriter) Write(b []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errors.New("disk full")
	}
	n := min(len(b), w.remaining)
	w.remaining -= n
	if n < len(b) {
		return n, errors.New("disk full")
	}
	return n, nil
}
func TestExportFailuresAndBudget(t *testing.T) {
	for _, limit := range []int{0, 10000, 70000} {
		_, err := Export(fixture(), &failWriter{limit}, ExportOptions{})
		if err == nil {
			t.Fatal("writer failure ignored", limit)
		}
	}
	var completeRecording bytes.Buffer
	if _, err := Export(fixture(), &completeRecording, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	// Refuse the last encoded byte, which is only written by the final flush.
	if _, err := Export(fixture(), &failWriter{completeRecording.Len() - 1}, ExportOptions{}); err == nil {
		t.Fatal("final flush failure ignored")
	}
	s := fixture()
	s.EventBudget = 3
	var b bytes.Buffer
	r, err := Export(s, &b, ExportOptions{})
	if err != nil || r.Complete {
		t.Fatal(r, err)
	}
	complete, err := trace.Read(&b, nil)
	if err != nil || complete {
		t.Fatal("budget marked complete", err)
	}
	if _, err = Export(fixture(), io.Discard, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
}

// The TypeScript reducer consumes this same fixture, so neither reader can
// pass merely by agreeing with another replay of itself.
func TestSharedSnapshotPositions(t *testing.T) {
	raw, err := os.ReadFile("trace/testdata/pause-states.json")
	if err != nil {
		t.Fatal(err)
	}
	var samples []struct {
		Horizon, Seq string
		Nodes        []struct {
			Node        string
			Running     bool
			PausedUntil string
			Members     []*trace.Member
			Summary     *trace.Summary
		}
	}
	if err = json.Unmarshal(raw, &samples); err != nil {
		t.Fatal(err)
	}
	scenario, err := os.ReadFile("../examples/sim/pause.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range samples {
		var s Scenario
		if err = json.Unmarshal(scenario, &s); err != nil {
			t.Fatal(err)
		}
		h, err := strconv.ParseInt(sample.Horizon, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		s.Horizon = core.Instant(h)
		r := run(t, s)
		if trace.Decimal(r.Events) != sample.Seq {
			t.Fatal("fixture event boundary changed")
		}
		for i, want := range sample.Nodes {
			got := r.Measurements[i]
			members := make([]*trace.Member, 0, len(got.Snapshot.Members))
			for _, m := range got.Snapshot.Members {
				members = append(members, trace.Membership(m))
			}
			if got.Name != want.Node || got.Running != want.Running || trace.Decimal(got.PausedUntil) != want.PausedUntil || !reflect.DeepEqual(members, want.Members) || !reflect.DeepEqual(trace.Counters(got.Snapshot), want.Summary) {
				t.Fatalf("stale snapshot fixture: %s at %s", want.Node, sample.Horizon)
			}
		}
	}
}

func TestExportPausedDeliveryAndTeachingFacts(t *testing.T) {
	s := fixture()
	s.Actions = append(s.Actions, Action{At: core.Instant(400 * time.Millisecond), Node: "b", Kind: "pause", Duration: 200 * time.Millisecond}, Action{At: core.Instant(450 * time.Millisecond), Node: "a", Peer: "b", Kind: "ping"})
	s.Faults = []Fault{{From: "a", To: "b", Start: 1, End: s.Horizon, Duplicate: true}}
	var output bytes.Buffer
	if _, err := Export(s, &output, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	arrivals := map[string]int{}
	delayed, cancelled := false, false
	_, err := trace.Read(&output, func(raw json.RawMessage) error {
		var e trace.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.Type != "event" {
			return nil
		}
		for _, traffic := range e.Traffic {
			if traffic.Stage == "arrival" && traffic.Transport == "datagram" {
				arrivals[traffic.ID]++
			}
			if traffic.Stage == "handle" && traffic.To == "b" && traffic.Received != "" {
				received, err := strconv.ParseInt(traffic.Received, 10, 64)
				if err != nil {
					return err
				}
				now, err := strconv.ParseInt(e.Time, 10, 64)
				if err != nil {
					return err
				}
				delayed = delayed || now-received > int64(100*time.Millisecond)
			}
		}
		for _, c := range e.Changes {
			cancelled = cancelled || c.Kind == "timer" && c.Phase == "cancel"
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate := false
	for _, count := range arrivals {
		duplicate = duplicate || count > 1
	}
	if !duplicate || !delayed || !cancelled {
		t.Fatal("missing duplicate/paused handler/cancellation evidence", duplicate, delayed, cancelled)
	}
	scenario, err := os.ReadFile("../examples/sim/pause.json")
	if err != nil {
		t.Fatal(err)
	}
	s = Scenario{}
	if err = json.Unmarshal(scenario, &s); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if _, err = Export(s, &output, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	refuted, indirect := false, false
	_, err = trace.Read(&output, func(raw json.RawMessage) error {
		var e trace.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		for _, c := range e.Changes {
			refuted = refuted || c.Kind == "refutation"
			indirect = indirect || c.Kind == "probe" && c.Phase == "indirect" && len(c.Helpers) > 0
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !refuted || !indirect {
		t.Fatal("teaching recording lacks refutation or helper evidence")
	}
}

func TestExportRecordsActionPeer(t *testing.T) {
	s := fixture()
	var out bytes.Buffer
	if _, err := Export(s, &out, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	joins := 0
	if _, err := trace.Read(bytes.NewReader(out.Bytes()), func(raw json.RawMessage) error {
		var e trace.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.Kind == "action.join" {
			joins++
			if (e.Actor != "b" && e.Actor != "c") || e.Peer != "a" {
				t.Fatalf("missing authoritative join endpoints: %+v", e)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if joins != 2 {
		t.Fatalf("got %d joins", joins)
	}
}
