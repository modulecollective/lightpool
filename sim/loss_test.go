package sim

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim/trace"
	"github.com/modulecollective/lightpool/wire"
)

func TestLossDrawVectorsAndDistribution(t *testing.T) {
	for _, v := range []struct {
		seed     uint64
		from, to string
		ordinal  uint64
		want     int
	}{
		{0, "a", "b", 1, 171089}, {2, "a", "b", 1, 768228}, {2, "b", "a", 1, 545680}, {2, "a", "b", 2, 371726}, {^uint64(0), "α", "node", 10000, 901176},
	} {
		got := lossDraw(v.seed, v.from, v.to, v.ordinal)
		if got != v.want {
			t.Errorf("vector %v = %d", v, got)
		}
	}
	count := 0
	for n := uint64(1); n <= 100000; n++ {
		if lossDraw(42, "a", "b", n) < 100000 {
			count++
		}
	}
	if count < 9000 || count > 11000 {
		t.Fatalf("10%% sample admitted %d drops of 100000", count)
	}
}

func TestLossValidation(t *testing.T) {
	s := fixture()
	f := Fault{From: "a", To: "b", Start: 1, End: 10, LossPPM: 1}
	for _, ppm := range []int{-1, 1000001} {
		f.LossPPM = ppm
		s.Faults = []Fault{f}
		if Validate(s) == nil {
			t.Fatal("accepted probability", ppm)
		}
	}
	f.LossPPM = 1
	s.Faults = []Fault{f, f}
	if Validate(s) == nil {
		t.Fatal("accepted overlap")
	}
	s.Faults[1].Start = 10
	s.Faults[1].End = 20
	if err := Validate(s); err != nil {
		t.Fatal("adjacent windows", err)
	}
	s.Faults[1] = f
	s.Faults[1].LossPPM = 0
	s.Faults[1].Drop = true
	if err := Validate(s); err != nil {
		t.Fatal("deterministic overlap", err)
	}
	s.Faults[1] = f
	s.Faults[1].From = "b"
	s.Faults[1].To = "a"
	if err := Validate(s); err != nil {
		t.Fatal("reverse link", err)
	}
}

func TestLossSendBoundary(t *testing.T) {
	s := fixture()
	r := &runner{scenario: s, nodes: map[string]*machine{}, addresses: map[netipKey]string{}, ordinals: map[[2]string]uint64{}}
	for _, n := range s.Nodes {
		r.nodes[n.Identity.Name] = &machine{spec: n, alive: true, life: 1}
		r.addresses[netipKey(n.Identity.Address.String())] = n.Identity.Name
	}
	send := core.SendDatagram{To: s.Nodes[1].Identity.Address, Messages: []wire.Message{{Ack: &wire.Ack{Sequence: 1}}}}
	r.faults = []Fault{{From: "a", To: "b", Start: 10, End: 20, LossPPM: 1000000, Duplicate: true}, {From: "a", To: "b", Start: 12, End: 13, Drop: true}}
	for _, at := range []core.Instant{9, 10, 12, 19, 20} {
		r.now = at
		r.effect("a", send)
	}
	if r.ordinals[[2]string{"a", "b"}] != 5 || r.nodes["a"].drops != 3 {
		t.Fatal("window/count", r.ordinals, r.nodes["a"].drops)
	}
	r.now = 21
	r.faults = []Fault{{From: "a", To: "b", Start: 20, End: 30, Duplicate: true}}
	r.effect("a", send)
	arrivals := 0
	for _, e := range r.heap {
		if e.kind == "datagram" {
			arrivals++
		}
	}
	if arrivals != 4 || r.ordinals[[2]string{"a", "b"}] != 6 {
		t.Fatal("duplicates consumed trials", arrivals, r.ordinals)
	}
	// Unresolved or locally rejected sends do not consume a resolved link ordinal.
	send.To = s.Nodes[2].Identity.Address
	delete(r.addresses, netipKey(send.To.String()))
	r.effect("a", send)
	send.To = s.Nodes[1].Identity.Address
	data := make([]byte, 2000)
	send.Messages = []wire.Message{{User: &data}}
	r.effect("a", send)
	if r.ordinals[[2]string{"a", "b"}] != 6 {
		t.Fatal("invalid send counted")
	}
}

func TestLossReplayOrdinalContinuityAndSeedSeparation(t *testing.T) {
	s := fixture()
	s.Actions = append(s.Actions, Action{At: core.Instant(400 * time.Millisecond), Node: "a", Kind: "crash"}, Action{At: core.Instant(450 * time.Millisecond), Node: "a", Kind: "start"}, Action{At: core.Instant(460 * time.Millisecond), Node: "a", Kind: "join", Peer: "b"})
	s.Faults = []Fault{{From: "a", To: "b", Start: core.Instant(200 * time.Millisecond), End: core.Instant(300 * time.Millisecond), Drop: true}, {From: "a", To: "b", Start: core.Instant(300 * time.Millisecond), End: s.Horizon, LossPPM: 300000, Duplicate: true}}
	var first bytes.Buffer
	if _, err := Export(s, &first, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	// Independent oracle counts every sent datagram on the link, before classifying
	// the drop. It does not reset at process/start or count duplicate arrivals.
	ordinal := uint64(0)
	sends := map[string]bool{}
	expect := map[string]bool{}
	got := map[string]bool{}
	_, err := trace.Read(bytes.NewReader(first.Bytes()), func(raw json.RawMessage) error {
		var e trace.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		at, _ := strconv.ParseInt(e.Time, 10, 64)
		for _, entry := range e.Traffic {
			if entry.From != "a" || entry.To != "b" || entry.Transport != "datagram" {
				continue
			}
			if entry.Stage == "sent" {
				ordinal++
				sends[entry.ID] = true
				if at >= int64(300*time.Millisecond) && lossDraw(s.Seeds.Fault, "a", "b", ordinal) < 300000 {
					expect[entry.ID] = true
				}
			}
			if entry.Stage == "drop" && entry.Detail == "independent datagram loss" {
				got[entry.ID] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ordinal < 5 || len(expect) == 0 || !reflect.DeepEqual(got, expect) {
		t.Fatal("ordinal oracle", ordinal, got, expect, sends)
	}
	var again bytes.Buffer
	if _, err := Export(s, &again, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), again.Bytes()) {
		t.Fatal("replay differs")
	}
	// No positive-loss windows: changing only the fault seed cannot change protocol
	// or scheduling evidence when there is no jitter to draw.
	s.Faults = nil
	a := run(t, s)
	s.Seeds.Fault++
	b := run(t, s)
	if !bytes.Equal(a.TraceBytes(), b.TraceBytes()) {
		t.Fatal("fault randomness leaked")
	}
}

func TestIndependentLossNeverDropsStreamChunks(t *testing.T) {
	s := fixture()
	s.Protocol.ProbeInterval = 0
	s.Protocol.GossipInterval = 0
	s.Protocol.PushPullInterval = 0
	var baseline, loss bytes.Buffer
	if _, err := Export(s, &baseline, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, a := range s.Nodes {
		for _, b := range s.Nodes {
			if a.Identity.Name != b.Identity.Name {
				s.Faults = append(s.Faults, Fault{From: a.Identity.Name, To: b.Identity.Name, Start: 0, End: s.Horizon, LossPPM: 1000000})
			}
		}
	}
	if _, err := Export(s, &loss, ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	_, baselineEvents, ok := bytes.Cut(baseline.Bytes(), []byte("\n"))
	if !ok {
		t.Fatal("missing header")
	}
	_, lossEvents, ok := bytes.Cut(loss.Bytes(), []byte("\n"))
	if !ok {
		t.Fatal("missing header")
	}
	if !bytes.Equal(baselineEvents, lossEvents) {
		t.Fatal("loss probability changed stream-only execution")
	}
	if len(effects(run(t, s), "core.WriteStream")) < 4 {
		t.Fatal("join stream path did not execute")
	}
}
