package experiment

import (
	"slices"
	"strings"

	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

const SummaryVersion = "lightpool-pilot-summary/v2"

// BeliefBound locates a change without changing the meaning of report.Reference.
// Synthetic bounds cite the adjacent/last event, never an invented change.
type BeliefBound struct {
	Kind        string     `json:"kind"`
	TimeNS      int64      `json:"timeNS,string"`
	Event       *Reference `json:"event"`
	ChangeIndex *int       `json:"changeIndex,string"`
}

type BeliefTime struct {
	ObservedNS    int64 `json:"observedNS,string"`
	AliveNS       int64 `json:"aliveNS,string"`
	SuspectNS     int64 `json:"suspectNS,string"`
	DeadNS        int64 `json:"deadNS,string"`
	LeftNS        int64 `json:"leftNS,string"`
	AbsentNS      int64 `json:"absentNS,string"`
	NotAliveNS    int64 `json:"notAliveNS,string"`
	SuspectDeadNS int64 `json:"suspectDeadNS,string"`
}

type BeliefSegment struct {
	State      string      `json:"state"`
	Start      BeliefBound `json:"start"`
	End        BeliefBound `json:"end"`
	ObservedNS int64       `json:"observedNS,string"`
}

type BeliefEpisode struct {
	Pair
	Labels           []string        `json:"labels"`
	Onset            BeliefBound     `json:"onset"`
	Cause            string          `json:"cause"`
	CarryIn          bool            `json:"carryIn"`
	ObservationStart BeliefBound     `json:"observationStart"`
	FirstDead        *BeliefBound    `json:"firstDead"`
	Segments         []BeliefSegment `json:"segments"`
	End              BeliefBound     `json:"end"`
	Status           string          `json:"status"`
	CensorReason     string          `json:"censorReason"`
	DurationNS       *int64          `json:"durationNS,string"`
	BeliefTime
}

type ReasonCount struct {
	Reason string `json:"reason"`
	Count  int    `json:"count,string"`
}

type BeliefTotals struct {
	EnrolledPairs  int   `json:"enrolledPairs,string"`
	NominalPairNS  int64 `json:"nominalPairNS,string"`
	ObservedPairNS int64 `json:"observedPairNS,string"`
	BeliefTime
	Starts              int                 `json:"starts,string"`
	CarryIns            int                 `json:"carryIns,string"`
	Completed           int                 `json:"completed,string"`
	Censored            []ReasonCount       `json:"censored"`
	Interrupted         []ReasonCount       `json:"interrupted"`
	PairsEverNonAlive   int                 `json:"pairsEverNonAlive,string"`
	PairsEverDead       int                 `json:"pairsEverDead,string"`
	NotAliveFraction    *string             `json:"notAliveFraction"`
	SuspectDeadFraction *string             `json:"suspectDeadFraction"`
	Durations           report.Distribution `json:"durations"`
}

type BeliefInterruption struct {
	Role   string      `json:"role"`
	Reason string      `json:"reason"`
	At     BeliefBound `json:"at"`
}

type BeliefPair struct {
	Pair
	Labels        []string             `json:"labels"`
	Interruptions []BeliefInterruption `json:"interruptions"`
	BeliefTotals
}

type BeliefPopulation struct {
	Label string `json:"label"`
	BeliefTotals
}

type NonAlive struct {
	Pairs       []BeliefPair       `json:"pairs"`
	Populations []BeliefPopulation `json:"populations"`
	Episodes    []BeliefEpisode    `json:"episodes"`
}

type beliefOnset struct {
	at    BeliefBound
	cause string
	dead  *BeliefBound
}

type beliefPairState struct {
	last         int64
	state        string
	active       bool
	open         int
	segmentStart BeliefBound
	durations    []int64
}

type nonAliveMeasurements struct {
	result    NonAlive
	prefix    map[pairKey]beliefOnset
	pairs     []beliefPairState
	index     map[pairKey]int
	lastEvent *Reference
}

func newNonAlive() *nonAliveMeasurements {
	return &nonAliveMeasurements{result: NonAlive{Pairs: []BeliefPair{}, Populations: []BeliefPopulation{}, Episodes: []BeliefEpisode{}}, prefix: map[pairKey]beliefOnset{}, index: map[pairKey]int{}}
}

func changeBound(at Reference, index int) BeliefBound {
	return BeliefBound{Kind: "change", TimeNS: at.TimeNS, Event: &at, ChangeIndex: &index}
}

func (n *nonAliveMeasurements) boundary(kind string, at int64) BeliefBound {
	return BeliefBound{Kind: kind, TimeNS: at, Event: n.lastEvent}
}

func (n *nonAliveMeasurements) enroll(x *measurements, reached bool) {
	start := n.boundary("window", x.spec.WarmupNS)
	for _, o := range x.nodes {
		for _, t := range x.nodes {
			if o == t {
				continue
			}
			k := pairKey{o, t}
			p := BeliefPair{Pair: x.pair(o, t), Labels: x.labels(Pair{Observer: o, Subject: t, ObserverLifetime: x.cohort[o], TargetLifetime: x.cohort[t]}, true), Interruptions: []BeliefInterruption{}, BeliefTotals: BeliefTotals{EnrolledPairs: 1, NominalPairNS: x.spec.HorizonNS - x.spec.WarmupNS, Censored: []ReasonCount{}, Interrupted: []ReasonCount{}}}
			state := x.state[o].members[t].state
			if state == "" {
				state = "absent"
			}
			n.index[k] = len(n.pairs)
			n.result.Pairs = append(n.result.Pairs, p)
			n.pairs = append(n.pairs, beliefPairState{last: x.spec.WarmupNS, state: state, active: reached && x.active(o) && x.active(t), open: -1})
			i := len(n.pairs) - 1
			if !reached {
				continue
			}
			if state != "alive" {
				origin, ok := n.prefix[k]
				if !ok {
					origin = beliefOnset{at: start, cause: "missing-warmup-membership"}
				}
				n.start(i, origin, start, true)
			}
			for _, role := range []struct{ name, node string }{{"observer", o}, {"target", t}} {
				if !x.active(role.node) {
					n.interruptPair(i, role.name, "inactive-at-enrollment", start)
				}
			}
		}
	}
}

func (n *nonAliveMeasurements) start(i int, origin beliefOnset, at BeliefBound, carry bool) {
	p, s := &n.result.Pairs[i], &n.pairs[i]
	n.result.Episodes = append(n.result.Episodes, BeliefEpisode{Pair: p.Pair, Labels: p.Labels, Onset: origin.at, Cause: origin.cause, CarryIn: carry, ObservationStart: at, FirstDead: origin.dead, Segments: []BeliefSegment{}})
	s.open = len(n.result.Episodes) - 1
	s.segmentStart = at
	p.PairsEverNonAlive = 1
	if carry {
		p.CarryIns++
	} else {
		p.Starts++
	}
	if s.state == "dead" {
		p.PairsEverDead = 1
	}
}

func (b *BeliefTime) add(state string, ns int64) {
	b.ObservedNS += ns
	switch state {
	case "alive":
		b.AliveNS += ns
	case "suspect":
		b.SuspectNS += ns
	case "dead":
		b.DeadNS += ns
	case "left":
		b.LeftNS += ns
	case "absent":
		b.AbsentNS += ns
	default:
		panic("unsupported membership state: " + state)
	}
	b.NotAliveNS = b.SuspectNS + b.DeadNS + b.LeftNS + b.AbsentNS
	b.SuspectDeadNS = b.SuspectNS + b.DeadNS
}

func (n *nonAliveMeasurements) advance(i int, at int64) {
	s := &n.pairs[i]
	if s.active {
		n.result.Pairs[i].add(s.state, at-s.last)
	}
	s.last = at
}

func (n *nonAliveMeasurements) segment(i int, at BeliefBound) {
	s := &n.pairs[i]
	e := &n.result.Episodes[s.open]
	ns := at.TimeNS - s.segmentStart.TimeNS
	e.Segments = append(e.Segments, BeliefSegment{State: s.state, Start: s.segmentStart, End: at, ObservedNS: ns})
	e.add(s.state, ns)
	s.segmentStart = at
}

func bumpReason(counts *[]ReasonCount, reason string, count int) {
	for i := range *counts {
		if (*counts)[i].Reason == reason {
			(*counts)[i].Count += count
			return
		}
	}
	*counts = append(*counts, ReasonCount{Reason: reason, Count: count})
}

func (n *nonAliveMeasurements) end(i int, at BeliefBound, reason string) {
	s, p := &n.pairs[i], &n.result.Pairs[i]
	if s.open < 0 {
		return
	}
	n.segment(i, at)
	e := &n.result.Episodes[s.open]
	e.End, e.CensorReason = at, reason
	if reason == "" {
		e.Status = "completed"
		p.Completed++
		if !e.CarryIn {
			duration := at.TimeNS - e.Onset.TimeNS
			e.DurationNS = &duration
			s.durations = append(s.durations, duration)
		}
	} else {
		e.Status = "censored"
		bumpReason(&p.Censored, reason, 1)
	}
	s.open = -1
}

func (n *nonAliveMeasurements) interruptPair(i int, role, reason string, at BeliefBound) {
	p, s := &n.result.Pairs[i], &n.pairs[i]
	// A second endpoint can interrupt at the same instant, but exposure never
	// reopens, and an episode keeps the first recorded censoring cause.
	for _, old := range p.Interruptions {
		if old.Role == role {
			return
		}
	}
	p.Interruptions = append(p.Interruptions, BeliefInterruption{Role: role, Reason: reason, At: at})
	bumpReason(&p.Interrupted, role+"-"+reason, 1)
	n.advance(i, at.TimeNS)
	n.end(i, at, role+"-"+reason)
	s.active = false
}

// change runs before the existing reducer mutates lifecycle or membership.
func (n *nonAliveMeasurements) change(x *measurements, c trace.Change, at BeliefBound) {
	reason := ""
	p := x.state[c.Node]
	switch c.Kind {
	case "process":
		if c.Phase == "start" {
			reason = "restart"
		}
		if c.Phase == "crash" {
			reason = "crash"
		}
	case "summary":
		if !p.stopped && c.Summary.Stopped {
			reason = "shutdown"
		}
		if !p.left && c.Summary.Left {
			reason = "leave"
		}
	}
	if reason != "" {
		for k := range n.prefix {
			if k.observer == c.Node || k.subject == c.Node {
				delete(n.prefix, k)
			}
		}
		if x.warm && at.TimeNS <= x.spec.HorizonNS {
			for i := range n.result.Pairs {
				pair := n.result.Pairs[i].Pair
				if pair.Observer == c.Node {
					n.interruptPair(i, "observer", reason, at)
				}
				if pair.Subject == c.Node {
					n.interruptPair(i, "target", reason, at)
				}
			}
		}
	}
	state, subject, cause := "", c.Subject, "removed"
	switch c.Kind {
	case "member.remove":
		state = "absent"
	case "member.upsert":
		state, subject, cause = c.Member.State, c.Member.Name, c.Member.State
	default:
		return
	}
	if c.Node == subject {
		return
	}
	k := pairKey{c.Node, subject}
	old := p.members[subject].state
	if old == "" {
		old = "absent"
	}
	if old == state {
		return
	}
	origin, exists := n.prefix[k]
	if state == "alive" {
		delete(n.prefix, k)
	} else {
		if !exists {
			origin = beliefOnset{at: at, cause: cause}
		}
		if state == "dead" && origin.dead == nil {
			dead := at
			origin.dead = &dead
		}
		n.prefix[k] = origin
	}
	if !x.warm || at.TimeNS > x.spec.HorizonNS {
		return
	}
	i := n.index[k]
	s := &n.pairs[i]
	if !s.active {
		return
	}
	n.advance(i, at.TimeNS)
	if s.open >= 0 {
		if state == "alive" {
			n.end(i, at, "")
		} else {
			n.segment(i, at)
			e := &n.result.Episodes[s.open]
			if state == "dead" && e.FirstDead == nil {
				dead := at
				e.FirstDead = &dead
			}
		}
	}
	s.state = state
	if state == "dead" {
		n.result.Pairs[i].PairsEverDead = 1
	}
	if state != "alive" && s.open < 0 {
		n.start(i, origin, at, false)
	}
}

func finishBeliefTotals(b *BeliefTotals, durations []int64) {
	b.ObservedPairNS = b.ObservedNS
	b.NotAliveFraction = ratio(b.NotAliveNS, b.ObservedPairNS)
	b.SuspectDeadFraction = ratio(b.SuspectDeadNS, b.ObservedPairNS)
	b.Durations = distribution(durations, b.Starts)
	slices.SortFunc(b.Censored, func(a, b ReasonCount) int { return strings.Compare(a.Reason, b.Reason) })
	slices.SortFunc(b.Interrupted, func(a, b ReasonCount) int { return strings.Compare(a.Reason, b.Reason) })
}

func (n *nonAliveMeasurements) finish(x *measurements, complete bool, through int64) NonAlive {
	if !x.warm {
		n.enroll(x, false)
	}
	end := n.boundary("prefix", through)
	if complete {
		end = n.boundary("horizon", x.spec.HorizonNS)
	}
	for i := range n.pairs {
		if n.pairs[i].active {
			n.advance(i, end.TimeNS)
			n.end(i, end, end.Kind)
		}
		finishBeliefTotals(&n.result.Pairs[i].BeliefTotals, n.pairs[i].durations)
	}
	labels := []string{"running-target-exclusions"}
	if x.pauseReference() {
		labels = append(labels, "FP-analogue", "FP-minus-analogue")
	}
	// Keep the healthy population even when empty, with null fractions/quantiles.
	labels = append(labels, "healthy-pair-exclusions")
	for _, label := range labels {
		pop := BeliefPopulation{Label: label, BeliefTotals: BeliefTotals{Censored: []ReasonCount{}, Interrupted: []ReasonCount{}}}
		var durations []int64
		for i, p := range n.result.Pairs {
			if !slices.Contains(p.Labels, label) {
				continue
			}
			b := &pop.BeliefTotals
			b.EnrolledPairs += p.EnrolledPairs
			b.NominalPairNS += p.NominalPairNS
			b.AliveNS += p.AliveNS
			b.SuspectNS += p.SuspectNS
			b.DeadNS += p.DeadNS
			b.LeftNS += p.LeftNS
			b.AbsentNS += p.AbsentNS
			b.ObservedNS += p.ObservedNS
			b.NotAliveNS += p.NotAliveNS
			b.SuspectDeadNS += p.SuspectDeadNS
			b.Starts += p.Starts
			b.CarryIns += p.CarryIns
			b.Completed += p.Completed
			b.PairsEverNonAlive += p.PairsEverNonAlive
			b.PairsEverDead += p.PairsEverDead
			for _, count := range p.Censored {
				bumpReason(&b.Censored, count.Reason, count.Count)
			}
			for _, count := range p.Interrupted {
				bumpReason(&b.Interrupted, count.Reason, count.Count)
			}
			durations = append(durations, n.pairs[i].durations...)
		}
		finishBeliefTotals(&pop.BeliefTotals, durations)
		n.result.Populations = append(n.result.Populations, pop)
	}
	return n.result
}
