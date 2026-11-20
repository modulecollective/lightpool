// Package report measures recorded outcomes without executing protocol code.
package report

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strconv"

	"github.com/modulecollective/lightpool/sim/trace"
)

// All measurements are decimal strings in JSON, including nanosecond times.
// Only the small schema version is a JSON number.
type Reference struct {
	TimeNS int64  `json:"timeNS,string"`
	Seq    uint64 `json:"seq,string"`
}

type Pair struct {
	Observer         string `json:"observer"`
	ObserverLifetime string `json:"observerLifetime"`
	Subject          string `json:"subject"`
	TargetLifetime   string `json:"targetLifetime"`
}

type Fault struct {
	From, To                 string
	Start, End               string
	Drop                     bool
	Delay, Jitter            string
	Duplicate, StreamFailure bool
	LossPPM                  string `json:",omitempty"`
}

type Join struct {
	Reference
	Node string `json:"node"`
	Peer string `json:"peer"`
}

type Declaration struct {
	Pair
	Reference
	TargetRunning   bool    `json:"targetRunning"`
	TargetStopped   bool    `json:"targetStopped"`
	TargetLeft      bool    `json:"targetLeft"`
	ObserverStopped bool    `json:"observerStopped"`
	ObserverLeft    bool    `json:"observerLeft"`
	TargetPaused    bool    `json:"targetPaused"`
	ObserverPaused  bool    `json:"observerPaused"`
	Faults          []Fault `json:"incidentConfiguredFaults"`
}

type Recovery struct {
	Pair
	Declaration Reference `json:"declaration"`
	End         Reference `json:"end"`
	Status      string    `json:"status"`
	DurationNS  *int64    `json:"durationNS,string"`
	Joins       []Join    `json:"joins"`
}

type Observation struct {
	Pair
	End       Reference `json:"end"`
	Status    string    `json:"status"`
	LatencyNS *int64    `json:"latencyNS,string"`
}

type Distribution struct {
	Completed int    `json:"completed,string"`
	Eligible  int    `json:"eligible,string"`
	P50NS     *int64 `json:"p50NS,string"`
	P95NS     *int64 `json:"p95NS,string"`
	MaxNS     *int64 `json:"maxNS,string"`
	Reason    string `json:"reason,omitempty"`
}

type Crash struct {
	Subject        string `json:"subject"`
	TargetLifetime string `json:"targetLifetime"`
	Reference
	AlreadyDead   int           `json:"alreadyDead,string"`
	Inactive      int           `json:"inactive,string"`
	UnknownOrLeft int           `json:"unknownOrLeft,string"`
	First         *Reference    `json:"first"`
	Observations  []Observation `json:"observations"`
	Latencies     Distribution  `json:"latencies"`
}

type Bytes struct {
	Sent     uint64 `json:"sent,string"`
	Received uint64 `json:"received,string"`
}

type Drop struct {
	Transport string `json:"transport"`
	Reason    string `json:"reason"`
	Count     uint64 `json:"count,string"`
}

type Traffic struct {
	Datagram               Bytes         `json:"datagram"`
	Stream                 Bytes         `json:"stream"`
	Drops                  []Drop        `json:"recordedDrops"`
	Total                  Bytes         `json:"total"`
	DurationNS             int64         `json:"durationNS,string"`
	SentBytesPerSecond     *string       `json:"sentBytesPerSecond"`
	ReceivedBytesPerSecond *string       `json:"receivedBytesPerSecond"`
	RateReason             string        `json:"rateReason,omitempty"`
	EndTotals              *trace.Totals `json:"endTotals"`
	EndBytesMatch          *bool         `json:"endBytesMatch"`
}

type Report struct {
	Format                 string          `json:"format"`
	Version                int             `json:"version"`
	Complete               bool            `json:"complete"`
	EndReason              string          `json:"endReason"`
	Calculator             string          `json:"calculator"`
	TraceSHA256            string          `json:"traceSHA256"`
	Producer               json.RawMessage `json:"producer"`
	NodeCount              int             `json:"nodeCount,string"`
	HorizonNS              int64           `json:"horizonNS,string"`
	LastEvent              Reference       `json:"lastEvent"`
	ObservedThroughNS      int64           `json:"observedThroughNS,string"`
	Declarations           []Declaration   `json:"declarations"`
	RunningDeclarations    int             `json:"runningDeclarations,string"`
	StoppedDeclarations    int             `json:"stoppedDeclarations,string"`
	AffectedRunningTargets int             `json:"affectedRunningTargets,string"`
	AffectedRunningPairs   int             `json:"affectedRunningPairs,string"`
	Crashes                []Crash         `json:"crashes"`
	Recoveries             []Recovery      `json:"recoveries"`
	RecoveryDurations      Distribution    `json:"recoveryDurations"`
	Traffic                Traffic         `json:"traffic"`
	Unsupported            []string        `json:"unsupported"`
	ModelLimits            []string        `json:"modelLimits"`
}

type process struct {
	life                   string
	running, stopped, left bool
	paused                 int64
	members                map[string]string
}

type key struct{ observer, subject string }
type pending struct{ crash, observation int }

type reducer struct {
	report    Report
	nodes     []trace.Node
	state     map[string]*process
	faults    []Fault
	recovery  map[key][]int
	detection map[key][]pending
	drops     map[[2]string]uint64
}

// Read validates and reduces one record at a time. It retains membership and
// open observations plus output rows, never the complete trace event history.
// No output should be written until Read succeeds, even for an incomplete trace.
func Read(input io.Reader) (*Report, error) {
	x := &reducer{state: map[string]*process{}, recovery: map[key][]int{}, detection: map[key][]pending{}, drops: map[[2]string]uint64{}}
	x.report = Report{Format: "lightpool-report", Version: 1, Calculator: "lightpool-report/v1", Declarations: []Declaration{}, Crashes: []Crash{}, Recoveries: []Recovery{}, Unsupported: []string{}, ModelLimits: []string{
		"Directional faults are prescribed send-time windows; jitter is sampled once per window. LossPPM independently admits each encoded datagram by directed-link send ordinal; duplicates copy the admitted datagram. Streams have no packet-loss probability.",
		"Node service durations are configured, not calibrated CPU or scheduler behavior. Shared bandwidth bottlenecks and path topology are not modeled.",
		"Streams model messages, ordering, credits and deadlines, not TCP packets. Bytes exclude IP/TCP overhead. This is a distinct Lightpool baseline.",
		"Running-target declarations carry truth and fault context, not inferred causes or a universal false-crash percentage. Example scenarios use accelerated settings, including disabled TCP probing.",
	}}
	digest := sha256.New()
	complete, err := trace.Read(io.TeeReader(input, digest), x.record)
	if err != nil {
		return nil, err
	}
	// Return report data without retaining the reducer's membership maps.
	r := x.report
	r.TraceSHA256 = hex.EncodeToString(digest.Sum(nil))
	r.Complete = complete
	if r.EndReason == "" {
		r.EndReason = "missing end (valid prefix)"
	}
	r.ObservedThroughNS = r.LastEvent.TimeNS
	if complete {
		if r.HorizonNS < r.LastEvent.TimeNS {
			return nil, fmt.Errorf("configured horizon precedes last event")
		}
		r.ObservedThroughNS = r.HorizonNS
	}
	end := Reference{TimeNS: r.ObservedThroughNS, Seq: r.LastEvent.Seq}
	for _, ids := range x.recovery {
		for _, id := range ids {
			r.Recoveries[id].End = end
		}
	}
	for _, ids := range x.detection {
		for _, id := range ids {
			r.Crashes[id.crash].Observations[id.observation].End = end
		}
	}
	targets := map[string]bool{}
	pairs := map[Pair]bool{}
	for _, d := range r.Declarations {
		if d.TargetRunning {
			r.RunningDeclarations++
			targets[d.Subject] = true
			pair := d.Pair
			pair.TargetLifetime = ""
			pairs[pair] = true
		} else {
			r.StoppedDeclarations++
		}
	}
	r.AffectedRunningTargets = len(targets)
	r.AffectedRunningPairs = len(pairs)
	for i := range r.Crashes {
		c := &r.Crashes[i]
		var values []int64
		for _, o := range c.Observations {
			if o.LatencyNS != nil {
				values = append(values, *o.LatencyNS)
			}
		}
		c.Latencies = distribution(values, len(c.Observations))
	}
	var values []int64
	for _, e := range r.Recoveries {
		if e.DurationNS != nil {
			values = append(values, *e.DurationNS)
		}
	}
	r.RecoveryDurations = distribution(values, len(r.Recoveries))
	for k, count := range x.drops {
		r.Traffic.Drops = append(r.Traffic.Drops, Drop{k[0], k[1], count})
	}
	if r.Traffic.Drops == nil {
		r.Traffic.Drops = []Drop{}
	}
	slices.SortFunc(r.Traffic.Drops, func(a, b Drop) int {
		if a.Transport != b.Transport {
			return cmp.Compare(a.Transport, b.Transport)
		}
		return cmp.Compare(a.Reason, b.Reason)
	})
	t := &r.Traffic
	t.Total = t.Datagram
	if err := add(&t.Total.Sent, t.Stream.Sent); err != nil {
		return nil, err
	}
	if err := add(&t.Total.Received, t.Stream.Received); err != nil {
		return nil, err
	}
	t.DurationNS = r.ObservedThroughNS
	if t.DurationNS == 0 {
		t.RateReason = "zero observed duration"
	} else {
		t.SentBytesPerSecond = rate(t.Total.Sent, t.DurationNS)
		t.ReceivedBytesPerSecond = rate(t.Total.Received, t.DurationNS)
	}
	if t.EndTotals != nil {
		match := trace.Decimal(t.Total.Sent) == t.EndTotals.SentBytes && trace.Decimal(t.Total.Received) == t.EndTotals.ReceivedBytes
		t.EndBytesMatch = &match
		if !match {
			r.Unsupported = append(r.Unsupported, "Recorded sent/received bytes disagree with end totals; traffic evidence is inconsistent.")
		}
	}
	return &r, nil
}

func add(total *uint64, n uint64) error {
	if ^uint64(0)-*total < n {
		return fmt.Errorf("traffic counter overflow")
	}
	*total += n
	return nil
}

func rate(bytes uint64, ns int64) *string {
	n := new(big.Int).SetUint64(bytes)
	n.Mul(n, big.NewInt(1e9))
	value := new(big.Rat).SetFrac(n, big.NewInt(ns)).FloatString(6)
	return &value
}

func distribution(values []int64, eligible int) Distribution {
	d := Distribution{Completed: len(values), Eligible: eligible}
	if len(values) == 0 {
		d.Reason = "no observed completions"
		if eligible == 0 {
			d.Reason = "empty eligible population"
		}
		return d
	}
	slices.Sort(values)
	p50, p95, maxValue := values[(len(values)*50+99)/100-1], values[(len(values)*95+99)/100-1], values[len(values)-1]
	d.P50NS, d.P95NS, d.MaxNS = &p50, &p95, &maxValue
	return d
}

func ref(e trace.Event) Reference {
	// trace.Read has already checked canonical decimal strings and ranges.
	t, _ := strconv.ParseInt(e.Time, 10, 64)
	seq, _ := strconv.ParseUint(e.Seq, 10, 64)
	return Reference{t, seq}
}

func (x *reducer) record(raw json.RawMessage) error {
	var tag struct{ Type string }
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	switch tag.Type {
	case "header":
		var h struct {
			Nodes    []trace.Node
			Initial  []trace.Process
			Manifest json.RawMessage
		}
		if err := json.Unmarshal(raw, &h); err != nil {
			return err
		}
		var m struct {
			Toolchain string
			Scenario  struct {
				Horizon         string
				Protocol, Seeds map[string]json.RawMessage
				Faults          []Fault
			}
		}
		if err := json.Unmarshal(h.Manifest, &m); err != nil {
			return err
		}
		horizon, err := strconv.ParseInt(m.Scenario.Horizon, 10, 64)
		if err != nil || horizon < 0 || m.Toolchain == "" || len(m.Scenario.Protocol) == 0 || len(m.Scenario.Seeds) == 0 {
			return fmt.Errorf("report requires producer toolchain, scenario horizon, protocol and seeds")
		}
		x.report.Producer = h.Manifest
		x.report.NodeCount = len(h.Nodes)
		x.report.HorizonNS = horizon
		x.nodes = h.Nodes
		slices.SortFunc(x.nodes, func(a, b trace.Node) int { return cmp.Compare(a.Name, b.Name) })
		x.faults = m.Scenario.Faults
		for _, f := range x.faults {
			start, e1 := strconv.ParseInt(f.Start, 10, 64)
			end, e2 := strconv.ParseInt(f.End, 10, 64)
			if e1 != nil || e2 != nil || start < 0 || end <= start {
				return fmt.Errorf("invalid configured fault interval")
			}
			if f.LossPPM != "" {
				n, err := strconv.Atoi(f.LossPPM)
				if err != nil || n < 0 || n > 1_000_000 {
					return fmt.Errorf("invalid configured loss probability")
				}
			}
		}
		for _, p := range h.Initial {
			x.state[p.Node] = &process{life: p.Lifetime, members: map[string]string{}}
		}
	case "event":
		var e trace.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		x.event(e)
		return x.traffic(e.Traffic)
	case "end":
		var e trace.End
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		x.report.EndReason = e.Reason
		x.report.Traffic.EndTotals = &e.Totals
	}
	return nil
}

// Counter arithmetic is checked before it can wrap.
func (x *reducer) traffic(entries []trace.Traffic) error {
	for _, t := range entries {
		n, _ := strconv.ParseUint(t.Bytes, 10, 64)
		total := &x.report.Traffic.Datagram
		if t.Transport == "stream" {
			total = &x.report.Traffic.Stream
		}
		switch t.Stage {
		case "sent":
			if err := add(&total.Sent, n); err != nil {
				return err
			}
		case "received":
			if err := add(&total.Received, n); err != nil {
				return err
			}
		case "drop":
			x.drops[[2]string{t.Transport, t.Detail}]++
		}
	}
	return nil
}
