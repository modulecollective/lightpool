package experiment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

type Evidence struct {
	Reference
	Fact trace.Change `json:"fact"`
}

type Mechanism struct {
	Pair
	Incarnation   string     `json:"incarnation"`
	Origin        Evidence   `json:"origin"`
	Confirmations []Evidence `json:"confirmations"`
	Timers        []Evidence `json:"timers"`
	End           Reference  `json:"end"`
	Outcome       string     `json:"outcome"`
	DeadCause     string     `json:"deadCause,omitempty"`
	Refutations   []Evidence `json:"targetRefutations"`
}

type Episode struct {
	report.Declaration
	Labels []string `json:"labels"`
	Cause  string   `json:"cause"`
}

type Exposure struct {
	Pair
	PairNS int64 `json:"pairNS,string"`
}

type Population struct {
	Label                       string     `json:"label"`
	Exposure                    []Exposure `json:"exposure"`
	PairNS                      int64      `json:"pairNS,string"`
	Transitions                 int        `json:"transitions,string"`
	Targets                     int        `json:"targets,string"`
	Pairs                       int        `json:"pairs,string"`
	EventsPerMillionPairSeconds *string    `json:"eventsPerMillionPairSeconds"`
	FractionEverExcluded        *string    `json:"fractionEverExcluded"`
}

type Suspicion struct {
	Pair
	First     *Reference `json:"first"`
	LatencyNS *int64     `json:"latencyNS,string"`
	Status    string     `json:"status"`
}

type Crash struct {
	report.Crash
	Suspicions         []Suspicion         `json:"suspicions"`
	FirstSuspicion     *Reference          `json:"firstSuspicion"`
	SuspicionLatencies report.Distribution `json:"suspicionLatencies"`
	FullDissemination  *Reference          `json:"fullDissemination"`
	FullLatencyNS      *int64              `json:"fullLatencyNS,string"`
	FullStatus         string              `json:"fullStatus"`
}

type Transport struct {
	Messages      uint64 `json:"messages,string"`
	SentBytes     uint64 `json:"sentBytes,string"`
	ReceivedBytes uint64 `json:"receivedBytes,string"`
	DropRecords   uint64 `json:"dropRecords,string"`
	DroppedBytes  uint64 `json:"droppedBytes,string"`
}

type Traffic struct {
	Datagram               Transport     `json:"datagram"`
	Stream                 Transport     `json:"stream"`
	ActiveNodeNS           int64         `json:"activeNodeNS,string"`
	SentBytesPerNodeSecond *string       `json:"sentBytesPerNodeSecond"`
	Drops                  []report.Drop `json:"dropReasons"`
}

type Summary struct {
	NonAlive          NonAlive            `json:"nonAlive"`
	Format            string              `json:"format"`
	Status            string              `json:"status"`
	Reason            string              `json:"reason"`
	SpecSHA256        string              `json:"specSHA256"`
	ScenarioSHA256    string              `json:"scenarioSHA256"`
	TraceSHA256       string              `json:"traceSHA256"`
	Spec              Spec                `json:"spec"`
	WarmupFailures    []EligiblePair      `json:"warmupFailures"`
	LastEvent         Reference           `json:"lastEvent"`
	ObservedThroughNS int64               `json:"observedThroughNS,string"`
	Events            uint64              `json:"events,string"`
	Populations       []Population        `json:"populations"`
	Episodes          []Episode           `json:"episodes"`
	Crashes           []Crash             `json:"crashes"`
	Recoveries        []report.Recovery   `json:"recoveries"`
	RecoveryDurations report.Distribution `json:"recoveryDurations"`
	Mechanisms        []Mechanism         `json:"mechanisms"`
	Refutations       []Evidence          `json:"refutations"`
	Traffic           Traffic             `json:"traffic"`
	Limits            []string            `json:"limits"`
}

type memberState struct {
	state, incarnation string
	suspect            *Reference
}

type nodeState struct {
	life                   string
	running, stopped, left bool
	members                map[string]memberState
}

type pairKey struct{ observer, subject string }

type interruption struct {
	node string
	at   Reference
}

type streamWrite struct {
	remaining uint64
	counted   bool
}

type measurements struct {
	spec          Spec
	nonAlive      *nonAliveMeasurements
	result        *Summary
	nodes         []string
	state         map[string]*nodeState
	warm          bool
	cohort        map[string]string
	exposures     map[Pair]int64
	now           int64
	suspect       map[Pair][]Reference
	preexisting   map[Reference]map[Pair]Reference
	mechanisms    map[pairKey]int
	refutations   map[string][]Evidence
	causes        map[Reference]map[pairKey]string
	interruptions []interruption
	streams       map[string]streamWrite
	drops         map[[2]string]uint64
}

// Measure makes a second streaming, offline pass. Existing report rows supply
// lifetime recovery and dead-detection rules; no protocol code executes here.
func Measure(spec Spec, scenario sim.Scenario, base *report.Report, input io.Reader) (*Summary, error) {
	if err := validateSpec(spec, scenario); err != nil {
		return nil, err
	}
	if len(base.Unsupported) > 0 {
		return nil, fmt.Errorf("unsupported report evidence: %s", strings.Join(base.Unsupported, "; "))
	}
	if base.HorizonNS != spec.HorizonNS {
		return nil, fmt.Errorf("invalid experiment specification")
	}
	normalized, err := trace.Normalize(scenario)
	if err != nil {
		return nil, err
	}
	expected, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	var producer struct {
		Scenario                  json.RawMessage
		Code, Toolchain, Platform string
	}
	if err = json.Unmarshal(base.Producer, &producer); err != nil {
		return nil, err
	}
	if !bytes.Equal(expected, producer.Scenario) || producer.Code != spec.Code || producer.Toolchain != spec.Toolchain || producer.Platform != spec.Platform {
		return nil, fmt.Errorf("saved scenario differs from trace manifest")
	}
	for _, p := range spec.HealthyPairs {
		if p.Observer == p.Subject {
			return nil, fmt.Errorf("self pair in healthy cohort")
		}
		for _, action := range scenario.Actions {
			if action.Kind == "pause" && (action.Node == p.Observer || action.Node == p.Subject) && int64(action.At) < spec.HorizonNS && int64(action.At)+int64(action.Duration) > spec.WarmupNS {
				return nil, fmt.Errorf("healthy pair has a configured processing pause")
			}
		}
		for _, f := range scenario.Faults {
			if f.Start < scenario.Horizon && int64(f.End) > spec.WarmupNS && ((f.From == p.Observer && f.To == p.Subject) || (f.From == p.Subject && f.To == p.Observer)) && (f.Drop || f.StreamFailure || f.Delay > 0 || f.Jitter > 0 || f.LossPPM > 0) {
				return nil, fmt.Errorf("healthy pair has a configured path impairment")
			}
		}
	}
	out := &Summary{Format: SummaryVersion, Status: "incomplete", Reason: base.EndReason, SpecSHA256: HashJSON(spec), ScenarioSHA256: HashJSON(scenario), TraceSHA256: base.TraceSHA256, Spec: spec, WarmupFailures: []EligiblePair{}, LastEvent: base.LastEvent, ObservedThroughNS: base.ObservedThroughNS, Events: base.LastEvent.Seq, Populations: []Population{}, Episodes: []Episode{}, Crashes: []Crash{}, Recoveries: []report.Recovery{}, Mechanisms: []Mechanism{}, Limits: []string{
		"Current Lightpool baseline; workload and metric analogues, not historical SWIM or Lifeguard reproduction.",
		"Warm-up is checked immediately before the first event at or after its boundary; equal-time outcomes follow trace sequence and change order.",
		"Paused live processes remain in exposure and active node-seconds. Excluded beliefs never reduce exposure. Lifetimes must enter through the warm-up check.",
		"Loss is sampled at send time by directed-link ordinal, not receiver-side packet loss. Different variant traffic need not realize the same losses. Streams are not TCP packets.",
		"Bytes are framed protocol bytes, excluding IP/TCP overhead and retransmissions. Drop records can describe partial stream chunks or repeated diagnostics, not unique lost messages.",
		"Quantiles describe observed completions with explicit eligible counts; three pilot replicates support no inferential or extreme-tail claim.",
		"Service time is uncalibrated; shared queues, CPU exhaustion, encryption, DNS and clock skew are outside this model.",
	}}
	x := &measurements{spec: spec, nonAlive: newNonAlive(), result: out, state: map[string]*nodeState{}, cohort: map[string]string{}, exposures: map[Pair]int64{}, suspect: map[Pair][]Reference{}, preexisting: map[Reference]map[Pair]Reference{}, mechanisms: map[pairKey]int{}, refutations: map[string][]Evidence{}, causes: map[Reference]map[pairKey]string{}, streams: map[string]streamWrite{}, drops: map[[2]string]uint64{}}
	for _, n := range scenario.Nodes {
		x.nodes = append(x.nodes, n.Identity.Name)
		x.state[n.Identity.Name] = &nodeState{members: map[string]memberState{}}
	}
	slices.Sort(x.nodes)
	digest := sha256.New()
	complete, err := trace.Read(io.TeeReader(input, digest), x.record)
	if err != nil {
		return nil, err
	}
	if complete != base.Complete || hex.EncodeToString(digest.Sum(nil)) != base.TraceSHA256 {
		return nil, fmt.Errorf("trace changed between reduction passes")
	}
	if base.ObservedThroughNS >= spec.WarmupNS && !x.warm {
		x.checkWarmup()
	}
	x.advance(base.ObservedThroughNS)
	if base.Complete {
		out.Status = "complete"
		if len(out.WarmupFailures) > 0 {
			out.Status, out.Reason = "invalid-warmup", "membership incomplete immediately before warm-up boundary"
		}
	}
	if !x.warm {
		out.Reason += "; warm-up boundary not reached"
	}
	for _, d := range base.Declarations {
		if d.TimeNS < spec.WarmupNS || d.TimeNS > base.ObservedThroughNS || d.Observer == d.Subject {
			continue
		}
		cause := "received-or-other-dead-transition"
		if x.causes[d.Reference][pairKey{d.Observer, d.Subject}] != "" {
			cause = x.causes[d.Reference][pairKey{d.Observer, d.Subject}]
		}
		out.Episodes = append(out.Episodes, Episode{d, x.labels(d.Pair, d.TargetRunning && !d.TargetStopped && !d.TargetLeft), cause})
	}
	labels := []string{"running-target-exclusions"}
	if x.pauseReference() {
		labels = append(labels, "FP-analogue", "FP-minus-analogue")
	}
	if len(spec.HealthyPairs) > 0 {
		labels = append(labels, "healthy-pair-exclusions")
	}
	pairs := make([]Pair, 0, len(x.exposures))
	for p := range x.exposures {
		pairs = append(pairs, p)
	}
	slices.SortFunc(pairs, func(a, b Pair) int { return strings.Compare(a.Observer+"\x00"+a.Subject, b.Observer+"\x00"+b.Subject) })
	for _, label := range labels {
		pop := Population{Label: label, Exposure: []Exposure{}}
		targets, pairsExcluded := map[string]bool{}, map[Pair]bool{}
		for _, p := range pairs {
			if slices.Contains(x.labels(p, true), label) {
				ns := x.exposures[p]
				pop.Exposure = append(pop.Exposure, Exposure{p, ns})
				pop.PairNS += ns
			}
		}
		for _, e := range out.Episodes {
			if slices.Contains(e.Labels, label) {
				pop.Transitions++
				targets[e.Subject] = true
				pairsExcluded[e.Pair] = true
			}
		}
		pop.Targets, pop.Pairs = len(targets), len(pairsExcluded)
		pop.EventsPerMillionPairSeconds = scaledRatio(int64(pop.Transitions), 1_000_000_000_000_000, pop.PairNS)
		pop.FractionEverExcluded = ratio(int64(pop.Pairs), int64(len(pop.Exposure)))
		out.Populations = append(out.Populations, pop)
	}
	for _, r := range base.Recoveries {
		if r.Declaration.TimeNS >= spec.WarmupNS && r.Declaration.TimeNS <= base.ObservedThroughNS && r.Observer != r.Subject {
			out.Recoveries = append(out.Recoveries, r)
		}
	}
	var durations []int64
	for _, r := range out.Recoveries {
		if r.DurationNS != nil {
			durations = append(durations, *r.DurationNS)
		}
	}
	out.RecoveryDurations = distribution(durations, len(out.Recoveries))
	for _, c := range base.Crashes {
		if c.TimeNS >= spec.WarmupNS {
			out.Crashes = append(out.Crashes, x.crash(c))
		}
	}
	for i := range out.Mechanisms {
		m := &out.Mechanisms[i]
		if m.Outcome == "unresolved" {
			m.End = Reference{TimeNS: base.ObservedThroughNS, Seq: base.LastEvent.Seq}
		}
		// These references are temporal evidence, not a claim that a specific rumor
		// carried the refutation. The trace reference locates its lifetime and
		// adjacent authoritative membership/incarnation changes.
		for _, e := range x.refutations[m.Subject] {
			if !before(e.Reference, m.Origin.Reference) && !before(m.End, e.Reference) {
				m.Refutations = append(m.Refutations, e)
			}
		}
	}
	for _, n := range x.nodes {
		out.Refutations = append(out.Refutations, x.refutations[n]...)
	}
	if out.Refutations == nil {
		out.Refutations = []Evidence{}
	}
	slices.SortFunc(out.Refutations, func(a, b Evidence) int {
		if before(a.Reference, b.Reference) {
			return -1
		}
		if before(b.Reference, a.Reference) {
			return 1
		}
		return strings.Compare(a.Fact.Node, b.Fact.Node)
	})
	for k, n := range x.drops {
		out.Traffic.Drops = append(out.Traffic.Drops, report.Drop{Transport: k[0], Reason: k[1], Count: n})
	}
	if out.Traffic.Drops == nil {
		out.Traffic.Drops = []report.Drop{}
	}
	slices.SortFunc(out.Traffic.Drops, func(a, b report.Drop) int {
		return strings.Compare(a.Transport+"\x00"+a.Reason, b.Transport+"\x00"+b.Reason)
	})
	out.NonAlive = x.nonAlive.finish(x, base.Complete, base.ObservedThroughNS)
	out.Traffic.SentBytesPerNodeSecond = scaledRatio(int64(out.Traffic.Datagram.SentBytes+out.Traffic.Stream.SentBytes), 1e9, out.Traffic.ActiveNodeNS)
	return out, nil
}

func HashJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	b = append(b, '\n')
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func ratio(n, d int64) *string {
	return scaledRatio(n, 1, d)
}

func scaledRatio(n, scale, d int64) *string {
	if d == 0 {
		return nil
	}
	numerator := new(big.Int).Mul(big.NewInt(n), big.NewInt(scale))
	s := new(big.Rat).SetFrac(numerator, big.NewInt(d)).FloatString(9)
	return &s
}

func distribution(values []int64, eligible int) report.Distribution {
	d := report.Distribution{Completed: len(values), Eligible: eligible}
	if len(values) == 0 {
		d.Reason = "no observed completions"
		if eligible == 0 {
			d.Reason = "empty eligible population"
		}
		return d
	}
	slices.Sort(values)
	a, b, c := values[(len(values)*50+99)/100-1], values[(len(values)*95+99)/100-1], values[len(values)-1]
	d.P50NS, d.P95NS, d.MaxNS = &a, &b, &c
	return d
}

func before(a, b Reference) bool { return a.TimeNS < b.TimeNS || a.TimeNS == b.TimeNS && a.Seq < b.Seq }

func eventRef(e trace.Event) Reference {
	t, _ := strconv.ParseInt(e.Time, 10, 64)
	s, _ := strconv.ParseUint(e.Seq, 10, 64)
	return Reference{TimeNS: t, Seq: s}
}

func (x *measurements) pair(o, t string) Pair {
	return Pair{Observer: o, ObserverLifetime: x.state[o].life, Subject: t, TargetLifetime: x.state[t].life}
}

func (x *measurements) active(n string) bool {
	p := x.state[n]
	return p.running && !p.stopped && !p.left
}

func (x *measurements) checkWarmup() {
	x.warm = true
	for _, o := range x.nodes {
		p := x.state[o]
		x.cohort[o] = p.life
		for _, t := range x.nodes {
			if !x.active(o) || !x.active(t) || p.members[t].state != "alive" {
				x.result.WarmupFailures = append(x.result.WarmupFailures, EligiblePair{o, t})
			}
		}
	}
	x.nonAlive.enroll(x, true)
}

func (x *measurements) advance(at int64) {
	start := max(x.now, x.spec.WarmupNS)
	end := min(at, x.spec.HorizonNS)
	if end > start && x.warm {
		for _, o := range x.nodes {
			if !x.active(o) {
				continue
			}
			x.result.Traffic.ActiveNodeNS += end - start
			if x.state[o].life != x.cohort[o] {
				continue
			}
			for _, t := range x.nodes {
				if o != t && x.active(t) && x.state[t].life == x.cohort[t] {
					x.exposures[x.pair(o, t)] += end - start
				}
			}
		}
	}
	x.now = at
}

func (x *measurements) pauseReference() bool {
	switch x.spec.Case {
	case "healthy", "threshold", "threshold-128", "interval", "ordinary-healthy", "ordinary-pause":
		return true
	}
	return false
}

func (x *measurements) labels(p Pair, running bool) []string {
	labels := []string{}
	if !running || p.Observer == p.Subject {
		return labels
	}
	labels = append(labels, "running-target-exclusions")
	if p.ObserverLifetime != x.cohort[p.Observer] || p.TargetLifetime != x.cohort[p.Subject] {
		return labels
	}
	if x.pauseReference() && !slices.Contains(x.spec.Anomaly, p.Subject) {
		labels = append(labels, "FP-analogue")
		if !slices.Contains(x.spec.Anomaly, p.Observer) {
			labels = append(labels, "FP-minus-analogue")
		}
	}
	if slices.Contains(x.spec.HealthyPairs, EligiblePair{p.Observer, p.Subject}) {
		labels = append(labels, "healthy-pair-exclusions")
	}
	return labels
}

func (x *measurements) interrupt(n string, at Reference, reason string) {
	x.interruptions = append(x.interruptions, interruption{n, at})
	for k, i := range x.mechanisms {
		if k.observer == n || k.subject == n {
			m := &x.result.Mechanisms[i]
			m.End, m.Outcome = at, reason
			delete(x.mechanisms, k)
		}
	}
}

func (x *measurements) record(raw json.RawMessage) error {
	var tag struct{ Type string }
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	if tag.Type != "event" {
		return nil
	}
	var e trace.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return err
	}
	at := eventRef(e)
	defer func() { x.nonAlive.lastEvent = &at }()
	if at.TimeNS >= x.spec.WarmupNS && !x.warm {
		x.checkWarmup()
	}
	// Exposure changes only at lifecycle boundaries. Pauses and beliefs do not
	// change it. Integrating N² pairs at every packet would dominate the pilot.
	for _, c := range e.Changes {
		p := x.state[c.Node]
		if c.Kind == "process" && (c.Phase == "start" || c.Phase == "crash") || c.Kind == "summary" && (p.stopped != c.Summary.Stopped || p.left != c.Summary.Left) {
			x.advance(at.TimeNS)
			break
		}
	}
	fired := map[pairKey]bool{}
	for index, c := range e.Changes {
		x.nonAlive.change(x, c, changeBound(at, index))
		p := x.state[c.Node]
		k := pairKey{c.Node, c.Subject}
		switch c.Kind {
		case "process":
			if c.Phase == "crash" && p.running {
				x.preexisting[at] = map[Pair]Reference{}
				for _, o := range x.nodes {
					if o != c.Node {
						if m := x.state[o].members[c.Node]; m.state == "suspect" && m.suspect != nil {
							x.preexisting[at][x.pair(o, c.Node)] = *m.suspect
						}
					}
				}
			}
			if c.Phase == "crash" || c.Phase == "start" {
				x.interrupt(c.Node, at, c.Phase)
			}
			if c.Phase == "start" {
				p.members = map[string]memberState{}
				p.stopped, p.left = false, false
			}
			p.running, p.life = c.Process.Running, c.Process.Lifetime
		case "summary":
			if !p.stopped && c.Summary.Stopped || !p.left && c.Summary.Left {
				x.interrupt(c.Node, at, "inactive")
			}
			p.stopped, p.left = c.Summary.Stopped, c.Summary.Left
		case "member.remove":
			delete(p.members, c.Subject)
		case "member.upsert":
			old := p.members[c.Member.Name]
			state := memberState{state: c.Member.State, incarnation: c.Member.Incarnation}
			mk := pairKey{c.Node, c.Member.Name}
			if state.state == "suspect" && old.state != "suspect" {
				ref := at
				state.suspect = &ref
				pair := x.pair(c.Node, c.Member.Name)
				x.suspect[pair] = append(x.suspect[pair], at)
			} else if state.state == "suspect" {
				state.suspect = old.suspect
			}
			p.members[c.Member.Name] = state
			if i, ok := x.mechanisms[mk]; ok && (state.state == "alive" || state.state == "dead" || state.state == "left") {
				m := &x.result.Mechanisms[i]
				m.End, m.Outcome = at, state.state
				if state.state == "dead" {
					m.DeadCause = "received-or-other-dead-transition"
					if fired[mk] {
						m.DeadCause = "local-suspicion-expiry"
					}
				}
				delete(x.mechanisms, mk)
			}
			if state.state == "dead" && old.state != "dead" && fired[mk] {
				if x.causes[at] == nil {
					x.causes[at] = map[pairKey]string{}
				}
				x.causes[at][mk] = "local-suspicion-expiry"
			}
		case "suspicion":
			if strings.HasPrefix(c.Outcome, "started:") {
				m := Mechanism{Pair: x.pair(c.Node, c.Subject), Incarnation: p.members[c.Subject].incarnation, Origin: Evidence{at, c}, Confirmations: []Evidence{}, Timers: []Evidence{}, End: at, Outcome: "unresolved", Refutations: []Evidence{}}
				x.result.Mechanisms = append(x.result.Mechanisms, m)
				x.mechanisms[k] = len(x.result.Mechanisms) - 1
			} else if i, ok := x.mechanisms[k]; ok {
				x.result.Mechanisms[i].Confirmations = append(x.result.Mechanisms[i].Confirmations, Evidence{at, c})
			}
		case "timer":
			if c.Purpose == "suspicion" {
				if i, ok := x.mechanisms[k]; ok {
					x.result.Mechanisms[i].Timers = append(x.result.Mechanisms[i].Timers, Evidence{at, c})
				}
				if c.Phase == "fire" {
					fired[k] = true
				}
			}
		case "refutation":
			x.refutations[c.Subject] = append(x.refutations[c.Subject], Evidence{at, c})
		}
	}
	for _, t := range e.Traffic {
		n, _ := strconv.ParseUint(t.Bytes, 10, 64)
		measured := at.TimeNS >= x.spec.WarmupNS && at.TimeNS <= x.spec.HorizonNS
		total := &x.result.Traffic.Datagram
		if t.Transport == "stream" {
			total = &x.result.Traffic.Stream
		}
		if t.Transport == "stream" && t.Stage == "attempt" && n > 0 {
			x.streams[t.ID] = streamWrite{remaining: n}
		}
		switch t.Stage {
		case "sent":
			first := true
			if t.Transport == "stream" {
				w := x.streams[t.ID]
				first = !w.counted
				w.counted = true
				w.remaining -= n
				if w.remaining == 0 {
					delete(x.streams, t.ID)
				} else {
					x.streams[t.ID] = w
				}
			}
			if measured {
				total.SentBytes += n
				if first {
					total.Messages++
				}
			}
		case "received":
			if measured {
				total.ReceivedBytes += n
			}
		case "drop":
			delete(x.streams, t.ID)
			if measured {
				total.DropRecords++
				total.DroppedBytes += n
				x.drops[[2]string{t.Transport, t.Detail}]++
			}
		}
	}
	return nil
}

func (x *measurements) crash(c report.Crash) Crash {
	out := Crash{Crash: c, Suspicions: []Suspicion{}, FullStatus: "unresolved"}
	var values []int64
	all := len(c.Observations) > 0
	last := c.Reference
	for _, o := range c.Observations {
		s := Suspicion{Pair: o.Pair, Status: "absent"}
		if prior, ok := x.preexisting[c.Reference][o.Pair]; ok {
			s.First = &prior
			s.Status = "preexisting-suspicion"
		}
		for _, at := range x.suspect[o.Pair] {
			if s.Status == "preexisting-suspicion" {
				break
			}
			if !before(at, c.Reference) && !before(o.End, at) {
				r := at
				s.First = &r
				v := at.TimeNS - c.TimeNS
				s.LatencyNS = &v
				s.Status = "observed"
				values = append(values, v)
				if out.FirstSuspicion == nil || before(at, *out.FirstSuspicion) {
					out.FirstSuspicion = &r
				}
				break
			}
		}
		out.Suspicions = append(out.Suspicions, s)
		if o.Status != "detected" {
			all = false
		}
		if before(last, o.End) {
			last = o.End
		}
	}
	out.SuspicionLatencies = distribution(values, len(c.Observations))
	// An observer that detected early but terminated before everyone else cannot
	// disappear from the full-dissemination cohort and manufacture completion.
	for _, i := range x.interruptions {
		if before(i.at, c.Reference) || before(last, i.at) {
			continue
		}
		for _, o := range c.Observations {
			if i.node == o.Observer {
				all = false
				out.FullStatus = "observer-interrupted"
			}
		}
		if i.node == c.Subject && i.at != c.Reference {
			all = false
			out.FullStatus = "target-interrupted"
		}
	}
	if all {
		r := last
		out.FullDissemination = &r
		v := last.TimeNS - c.TimeNS
		out.FullLatencyNS = &v
		out.FullStatus = "complete"
	}
	if len(c.Observations) == 0 {
		out.FullStatus = "empty-cohort"
	}
	return out
}
