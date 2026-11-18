package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
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
	for _, s := range scenarios {
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
