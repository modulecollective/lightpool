package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/trace"
)

const AdmissionVersion = "lightpool-correlated-admission/v1"

// OrderedEvidence addresses a journal fact within an indivisible core step.
// ChangeIndex is zero-based; traffic belongs to the enclosing step and does not
// identify which route delivered an ACK.
type OrderedEvidence struct {
	Reference
	ChangeIndex int          `json:"changeIndex,string"`
	Fact        trace.Change `json:"fact"`
}
type ProbeStep struct {
	OrderedEvidence
	Traffic []trace.Traffic `json:"stepTraffic"`
}
type AdmissionProbe struct {
	Pair
	ID             string           `json:"id"`
	Incarnation    string           `json:"incarnation"`
	Steps          []ProbeStep      `json:"steps"`
	Outcome        string           `json:"outcome"`
	LocalSuspicion *OrderedEvidence `json:"localSuspicion"`
}
type SenderEvidence struct {
	Sender   string          `json:"sender"`
	Lifetime string          `json:"lifetime"`
	Fact     OrderedEvidence `json:"fact"`
	Probe    *AdmissionProbe `json:"probe"`
}
type Shortening struct {
	Confirmation SenderEvidence  `json:"confirmation"`
	Previous     OrderedEvidence `json:"previousArm"`
	Arm          OrderedEvidence `json:"arm"`
	Qualifies    bool            `json:"qualifies"`
	Reasons      []string        `json:"missingEvidence"`
}
type AdmissionEpisode struct {
	Pair
	Incarnation      string            `json:"incarnation"`
	Origin           SenderEvidence    `json:"origin"`
	Eligible         bool              `json:"eligible"`
	InitialArm       *OrderedEvidence  `json:"initialArm"`
	Confirmations    []SenderEvidence  `json:"confirmations"`
	Shortenings      []Shortening      `json:"shortenings"`
	Timers           []OrderedEvidence `json:"timers"`
	End              *OrderedEvidence  `json:"end"`
	Outcome          string            `json:"outcome"`
	DeadCause        string            `json:"deadCause,omitempty"`
	Refutation       *OrderedEvidence  `json:"refutationGeneration"`
	Acceptance       *OrderedEvidence  `json:"refutationAcceptance"`
	RefutationStatus string            `json:"refutationStatus"`
	Acceleration     bool              `json:"acceleration"`
	Harmful          bool              `json:"harmful"`
	Reasons          []string          `json:"missingEvidence"`
	live             *OrderedEvidence
	fired            *OrderedEvidence
	seen             map[string]bool
	pending          *SenderEvidence
}
type Admission struct {
	Format              string              `json:"format"`
	Status              string              `json:"status"`
	Reason              string              `json:"reason"`
	SpecSHA256          string              `json:"specSHA256"`
	ScenarioSHA256      string              `json:"scenarioSHA256"`
	TraceSHA256         string              `json:"traceSHA256"`
	Spec                Spec                `json:"spec"`
	WarmupFailures      []EligiblePair      `json:"warmupFailures"`
	ObservedThroughNS   int64               `json:"observedThroughNS,string"`
	Populations         []Population        `json:"populations"`
	Traffic             Traffic             `json:"traffic"`
	Probes              []*AdmissionProbe   `json:"targetProbes"`
	Episodes            []*AdmissionEpisode `json:"episodes"`
	AFailures           int                 `json:"aProbeFailures,string"`
	AOrigins            int                 `json:"aLocalSuspicions,string"`
	EligibleEpisodes    int                 `json:"eligibleEpisodes,string"`
	Witnesses           int                 `json:"accelerationWitnesses,string"`
	Harmful             int                 `json:"harmfulEpisodes,string"`
	RefutationsInFlight int                 `json:"refutationsInFlight,string"`
	Stop                string              `json:"chainStoppedAt"`
	MaxBacklog          uint64              `json:"maxBacklog,string"`
	TimerFires          int                 `json:"timerFires,string"`
	LateTimers          int                 `json:"lateTimers,string"`
	MaxLatenessNS       int64               `json:"maxTimerLatenessNS,string"`
	Limits              []string            `json:"limits"`
}
type admissionRefutation struct {
	life, incarnation string
	evidence          OrderedEvidence
}
type admissionAcceptance struct {
	Pair
	incarnation string
	evidence    OrderedEvidence
}
type admissionNode struct {
	life              string
	running, inactive bool
	pausedUntil       int64
	member            trace.Member
}
type admissionScan struct {
	spec        Spec
	out         *Admission
	nodes       map[string]*admissionNode
	cohort      map[string]string
	warm        bool
	probes      map[string]*AdmissionProbe
	active      map[string]*AdmissionEpisode
	generations []admissionRefutation
	acceptances []admissionAcceptance
}

// Admit only remeasures validated saved evidence. A diagnostic result is returned
// alongside an error for invalid/incomplete input so the CLI can retain it.
func Admit(directory string) (*Admission, error) {
	out := &Admission{Format: AdmissionVersion, Status: "invalid", Reason: "evidence validation failed"}
	summary, err := Reduce(directory)
	if err != nil {
		out.Reason = err.Error()
		return out, err
	}
	out.Spec, out.SpecSHA256, out.ScenarioSHA256, out.TraceSHA256 = summary.Spec, summary.SpecSHA256, summary.ScenarioSHA256, summary.TraceSHA256
	var scenario sim.Scenario
	if err = readJSON(filepath.Join(directory, "scenario.json"), &scenario); err != nil {
		out.Reason = err.Error()
		return out, err
	}
	// Admission is deliberately limited to the predeclared fixtures. General
	// measurement retains its existing ability to reduce small custom fixtures.
	expectedSpec, expectedScenario, err := Fixture(summary.Spec.Case, summary.Spec.Replicate, summary.Spec.Code)
	if err == nil {
		expectedSpec.Toolchain, expectedSpec.Platform = summary.Spec.Toolchain, summary.Spec.Platform
		if !strings.HasPrefix(summary.Spec.Case, "correlated") || HashJSON(expectedSpec) != HashJSON(summary.Spec) || HashJSON(expectedScenario) != HashJSON(scenario) {
			err = fmt.Errorf("admission requires an unchanged fixed correlated fixture")
		}
	}
	if err != nil {
		out.Reason = err.Error()
		return out, err
	}
	f, err := os.Open(filepath.Join(directory, "trace.ndjson"))
	if err != nil {
		out.Reason = err.Error()
		return out, err
	}
	out, scanErr := measureAdmission(summary, f)
	err = errors.Join(scanErr, f.Close())
	if err != nil {
		out.Status, out.Reason = "invalid", err.Error()
		return out, err
	}
	if out.Status != "complete" {
		return out, fmt.Errorf("%s: %s", out.Status, out.Reason)
	}
	return out, nil
}
func WriteAdmission(path string, a *Admission) error { return writeJSON(path, a) }

func measureAdmission(summary *Summary, input io.Reader) (*Admission, error) {
	out := &Admission{Format: AdmissionVersion, Status: summary.Status, Reason: summary.Reason, SpecSHA256: summary.SpecSHA256, ScenarioSHA256: summary.ScenarioSHA256, TraceSHA256: summary.TraceSHA256, Spec: summary.Spec, WarmupFailures: summary.WarmupFailures, ObservedThroughNS: summary.ObservedThroughNS, Populations: summary.Populations, Traffic: summary.Traffic, Probes: []*AdmissionProbe{}, Episodes: []*AdmissionEpisode{}, Limits: []string{
		"Fixed virtual mechanism screen on current Lightpool; no candidate admission or ordinary transport reproduction.",
		"Step traffic and helper identities do not establish the route of a successful ACK.",
		"Refutation generation and authoritative higher-incarnation acceptance are temporal joins, not packet-route attribution.",
		"A null screen is bounded evidence, not a general falsification. Censored chains cannot qualify.",
	}}
	x := admissionScan{spec: summary.Spec, out: out, nodes: map[string]*admissionNode{}, cohort: map[string]string{}, probes: map[string]*AdmissionProbe{}, active: map[string]*AdmissionEpisode{}}
	h := sha256.New()
	complete, err := trace.Read(io.TeeReader(input, h), x.record)
	if err != nil {
		return out, err
	}
	if hex.EncodeToString(h.Sum(nil)) != summary.TraceSHA256 || summary.Status == "complete" && !complete {
		return out, fmt.Errorf("trace changed between admission passes")
	}
	for _, e := range out.Episodes {
		x.classify(e)
	}
	switch {
	case out.Witnesses > 0:
		out.Stop = "mechanism-observed"
	case out.AFailures == 0:
		out.Stop = "no-A-to-T-probe-failure"
	case out.AOrigins == 0:
		out.Stop = "no-A-local-suspicion"
	case out.EligibleEpisodes == 0:
		out.Stop = "no-eligible-O-suspicion"
	default:
		out.Stop = "no-qualifying-same-group-shortening-before-refutation"
	}
	return out, nil
}
func (x *admissionScan) node(name string) *admissionNode {
	if x.nodes[name] == nil {
		x.nodes[name] = &admissionNode{}
	}
	return x.nodes[name]
}
func (x *admissionScan) pair(observer string) Pair {
	return Pair{Observer: observer, ObserverLifetime: x.node(observer).life, Subject: x.spec.Target, TargetLifetime: x.node(x.spec.Target).life}
}
func orderedBefore(a, b OrderedEvidence) bool {
	return before(a.Reference, b.Reference) || a.Reference == b.Reference && a.ChangeIndex < b.ChangeIndex
}
func incarnationAfter(a, b string) bool {
	av, _ := strconv.ParseUint(a, 10, 64)
	bv, _ := strconv.ParseUint(b, 10, 64)
	return av > bv
}
func (x *admissionScan) activeAt(name string, at int64) bool {
	n := x.node(name)
	return n.running && !n.inactive && n.pausedUntil <= at
}
func (x *admissionScan) sender(c OrderedEvidence) SenderEvidence {
	_, sender, _ := strings.Cut(c.Fact.Outcome, ":")
	out := SenderEvidence{Sender: sender, Lifetime: x.node(sender).life, Fact: c}
	// Only a locally emitted suspicion in the same failed-probe step establishes
	// provenance. A sender name in a forwarded rumor establishes none.
	for _, p := range slices.Backward(x.out.Probes) {
		if p.Observer != sender || p.ObserverLifetime != out.Lifetime || p.TargetLifetime != x.node(x.spec.Target).life || p.Incarnation != x.node(c.Fact.Node).member.Incarnation || p.LocalSuspicion == nil {
			continue
		}
		if orderedBefore(*p.LocalSuspicion, c) || p.LocalSuspicion.Reference == c.Reference && p.LocalSuspicion.ChangeIndex == c.ChangeIndex {
			out.Probe = p
			break
		}
	}
	return out
}
func (x *admissionScan) record(raw json.RawMessage) error {
	var tag struct{ Type string }
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	if tag.Type != "event" {
		return nil
	}
	var event trace.Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	at := eventRef(event)
	if !x.warm && at.TimeNS >= x.spec.WarmupNS {
		x.warm = true
		for name, n := range x.nodes {
			x.cohort[name] = n.life
		}
	}
	for i, c := range event.Changes {
		ev := OrderedEvidence{at, i, c}
		n := x.node(c.Node)
		if c.Kind == "process" {
			if n.life != c.Process.Lifetime || !c.Process.Running {
				for observer, ep := range x.active {
					if observer == c.Node || c.Node == x.spec.Target {
						ep.Outcome, ep.End = "lifetime-interrupted", &ev
						delete(x.active, observer)
					}
				}
				n.member = trace.Member{}
			}
			n.life, n.running = c.Process.Lifetime, c.Process.Running
			n.pausedUntil, _ = strconv.ParseInt(c.Process.PausedUntil, 10, 64)
			n.inactive = false
		}
		if c.Kind == "summary" {
			n.inactive = c.Summary.Stopped || c.Summary.Left
			backlog, _ := strconv.ParseUint(c.Summary.Backlog, 10, 64)
			x.out.MaxBacklog = max(x.out.MaxBacklog, backlog)
		}
		if c.Kind == "timer" && c.Phase == "fire" {
			deadline, _ := strconv.ParseInt(c.Deadline, 10, 64)
			late := at.TimeNS - deadline
			x.out.TimerFires++
			if late > 0 {
				x.out.LateTimers++
				x.out.MaxLatenessNS = max(x.out.MaxLatenessNS, late)
			}
		}
		if c.Kind == "member.upsert" && c.Member.Name == x.spec.Target {
			n.member = *c.Member
			if c.Member.State == "alive" {
				x.acceptances = append(x.acceptances, admissionAcceptance{x.pair(c.Node), c.Member.Incarnation, ev})
			}
			if ep := x.active[c.Node]; ep != nil && (c.Member.State == "alive" || c.Member.State == "dead" || c.Member.State == "left") {
				ep.End, ep.Outcome = &ev, c.Member.State
				if c.Member.State == "dead" {
					ep.DeadCause = "received-or-other-dead-transition"
					if ep.fired != nil && ep.fired.Reference == at && orderedBefore(*ep.fired, ev) {
						ep.DeadCause = "local-suspicion-expiry"
					}
				}
				delete(x.active, c.Node)
			}
		}
		if c.Kind == "refutation" && c.Subject == x.spec.Target && c.Node == x.spec.Target {
			x.generations = append(x.generations, admissionRefutation{n.life, n.member.Incarnation, ev})
		}
		if c.Kind == "probe" && c.Subject == x.spec.Target {
			key := c.Node + "/" + n.life + "/" + c.ID
			if c.Phase == "direct" {
				p := &AdmissionProbe{Pair: x.pair(c.Node), ID: c.ID, Incarnation: n.member.Incarnation, Steps: []ProbeStep{}, Outcome: "unresolved"}
				x.probes[key] = p
				x.out.Probes = append(x.out.Probes, p)
			}
			if p := x.probes[key]; p != nil {
				p.Steps = append(p.Steps, ProbeStep{ev, event.Traffic})
				if c.Phase == "finished" {
					p.Outcome = c.Outcome
					if at.TimeNS >= x.spec.WarmupNS && at.TimeNS < x.spec.WarmupNS+16_384_000_000 && slices.Contains(x.spec.Anomaly, c.Node) && (c.Outcome == "timeout" || c.Outcome == "remote transport failure") {
						x.out.AFailures++
					}
				}
			}
		}
		if c.Kind == "suspicion" && c.Subject == x.spec.Target {
			_, sender, _ := strings.Cut(c.Outcome, ":")
			if sender == c.Node {
				for _, p := range slices.Backward(x.out.Probes) {
					last := p.Steps[len(p.Steps)-1]
					if p.Pair == x.pair(c.Node) && p.Incarnation == n.member.Incarnation && last.Reference == at && orderedBefore(last.OrderedEvidence, ev) && last.Fact.Phase == "finished" && (p.Outcome == "timeout" || p.Outcome == "remote transport failure") {
						p.LocalSuspicion = &ev
						if slices.Contains(x.spec.Anomaly, c.Node) && at.TimeNS >= x.spec.WarmupNS && at.TimeNS < x.spec.WarmupNS+16_384_000_000 {
							x.out.AOrigins++
						}
						break
					}
				}
			}
			if strings.HasPrefix(c.Outcome, "started:") {
				if at.TimeNS < x.spec.WarmupNS {
					continue
				}
				pair := x.pair(c.Node)
				eligible := x.out.Status == "complete" && at.TimeNS < x.spec.WarmupNS+16_384_000_000 && slices.Contains(x.spec.HealthyPairs, EligiblePair{c.Node, c.Subject}) && x.activeAt(c.Node, at.TimeNS) && x.activeAt(c.Subject, at.TimeNS) && pair.ObserverLifetime == x.cohort[c.Node] && pair.TargetLifetime == x.cohort[c.Subject]
				ep := &AdmissionEpisode{Pair: pair, Incarnation: n.member.Incarnation, Origin: x.sender(ev), Eligible: eligible, Outcome: "censored", Confirmations: []SenderEvidence{}, Shortenings: []Shortening{}, Timers: []OrderedEvidence{}, Reasons: []string{}, seen: map[string]bool{sender: true}}
				x.active[c.Node] = ep
				x.out.Episodes = append(x.out.Episodes, ep)
				if eligible {
					x.out.EligibleEpisodes++
				}
			} else if ep := x.active[c.Node]; ep != nil && strings.HasPrefix(c.Outcome, "confirmation:") {
				evidence := x.sender(ev)
				ep.Confirmations = append(ep.Confirmations, evidence)
				if !ep.seen[sender] {
					ep.pending = &evidence
				}
				ep.seen[sender] = true
			}
		}
		if c.Kind == "timer" && c.Subject == x.spec.Target && c.Purpose == "suspicion" {
			ep := x.active[c.Node]
			if ep == nil {
				continue
			}
			ep.Timers = append(ep.Timers, ev)
			switch c.Phase {
			case "arm":
				if ep.InitialArm == nil {
					ep.InitialArm = &ev
				}
				if ep.pending != nil && ep.pending.Fact.Reference == at && orderedBefore(ep.pending.Fact, ev) && ep.live != nil {
					// cancel precedes rearm in this same core step. Keep the previous arm
					// across that cancel so effective deadlines can be compared.
					ep.Shortenings = append(ep.Shortenings, Shortening{Confirmation: *ep.pending, Previous: *ep.live, Arm: ev, Reasons: []string{}})
				}
				ep.live, ep.pending, ep.fired = &ev, nil, nil
			case "cancel":
				// Only confirmation-driven replacement may retain the cancelled arm.
				if ep.pending == nil || ep.pending.Fact.Reference != at || !sameTimer(ep.live, ev) {
					ep.live = nil
				}
			case "fire":
				if sameTimer(ep.live, ev) {
					deadline, _ := strconv.ParseInt(ev.Fact.Deadline, 10, 64)
					if at.TimeNS >= deadline {
						ep.fired = &ev
					}
					ep.live = nil
				}
			}
		}
	}
	return nil
}
func sameTimer(arm *OrderedEvidence, e OrderedEvidence) bool {
	return arm != nil && arm.Fact.ID == e.Fact.ID && arm.Fact.Generation == e.Fact.Generation && arm.Fact.Deadline == e.Fact.Deadline
}
func (x *admissionScan) classify(ep *AdmissionEpisode) {
	if x.out.Status != "complete" {
		ep.Reasons = append(ep.Reasons, "recording or warm-up is not complete and valid")
	}
	for _, r := range x.generations {
		if r.life == ep.TargetLifetime && incarnationAfter(r.incarnation, ep.Incarnation) {
			ep.Refutation = &r.evidence
			for _, a := range x.acceptances {
				if a.Pair == ep.Pair && a.incarnation == r.incarnation && orderedBefore(r.evidence, a.evidence) && orderedBefore(ep.Origin.Fact, a.evidence) {
					ep.Acceptance = &a.evidence
					break
				}
			}
			break
		}
	}
	ep.RefutationStatus = "observed"
	if ep.Refutation == nil {
		ep.RefutationStatus = "generation-censored"
	} else if ep.Acceptance == nil {
		ep.RefutationStatus = "acceptance-censored"
	}
	if !ep.Eligible {
		ep.Reasons = append(ep.Reasons, "observer/time/lifetime/process outside eligible O-to-T cohort")
	}
	if !slices.Contains(x.spec.Anomaly, ep.Origin.Sender) {
		ep.Reasons = append(ep.Reasons, "initiating sender outside A")
	}
	if ep.Origin.Probe == nil {
		ep.Reasons = append(ep.Reasons, "initiating sender lacks earlier same-incarnation local failed-probe provenance")
	}
	if ep.InitialArm == nil {
		ep.Reasons = append(ep.Reasons, "missing initial suspicion arm")
	}
	if ep.Refutation == nil {
		ep.Reasons = append(ep.Reasons, "refutation generation censored")
	}
	if ep.Acceptance == nil {
		ep.Reasons = append(ep.Reasons, "matching refutation acceptance censored")
	}
	for i := range ep.Shortenings {
		s := &ep.Shortenings[i]
		s.Reasons = append(s.Reasons, ep.Reasons...)
		if !slices.Contains(x.spec.Anomaly, s.Confirmation.Sender) {
			s.Reasons = append(s.Reasons, "confirming sender outside A")
		}
		if s.Confirmation.Probe == nil {
			s.Reasons = append(s.Reasons, "confirming sender lacks earlier same-incarnation local failed-probe provenance")
		}
		old, _ := strconv.ParseInt(s.Previous.Fact.Deadline, 10, 64)
		next, _ := strconv.ParseInt(s.Arm.Fact.Deadline, 10, 64)
		if next >= old {
			s.Reasons = append(s.Reasons, "effective deadline did not strictly shorten")
		}
		if ep.Refutation != nil && !orderedBefore(s.Arm, *ep.Refutation) {
			s.Reasons = append(s.Reasons, "target refuted before shortening")
		}
		if ep.Acceptance != nil && !orderedBefore(s.Arm, *ep.Acceptance) {
			s.Reasons = append(s.Reasons, "observer accepted refutation before shortening")
		}
		s.Qualifies = len(s.Reasons) == 0
		if s.Qualifies {
			ep.Acceleration = true
		}
		if ep.Eligible && next < old && ep.Refutation != nil && ep.Acceptance != nil && orderedBefore(*ep.Refutation, s.Arm) && orderedBefore(s.Arm, *ep.Acceptance) {
			x.out.RefutationsInFlight++
		}
	}
	if !ep.Acceleration {
		ep.Reasons = append(ep.Reasons, "no qualifying distinct same-group confirmation with a live strictly earlier arm")
	}
	if ep.Acceleration {
		x.out.Witnesses++
		if ep.DeadCause == "local-suspicion-expiry" && ep.End != nil && ep.InitialArm != nil && ep.fired != nil && ep.Acceptance != nil {
			original, _ := strconv.ParseInt(ep.InitialArm.Fact.Deadline, 10, 64)
			fired, _ := strconv.ParseInt(ep.fired.Fact.Deadline, 10, 64)
			ep.Harmful = ep.End.TimeNS < original && fired < original && orderedBefore(*ep.End, *ep.Acceptance)
		}
	}
	if ep.Harmful {
		x.out.Harmful++
	}
}
