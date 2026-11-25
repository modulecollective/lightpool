package experiment

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

const deliveryWindowNS = int64(4 * time.Second)

type DeliveryPair struct {
	Receiver         string       `json:"receiver"`
	ReceiverLifetime string       `json:"receiverLifetime"`
	Status           string       `json:"status"`
	Acceptance       *BeliefBound `json:"acceptance"`
	Incarnation      string       `json:"incarnation,omitempty"`
	LatencyNS        *int64       `json:"latencyNS,string"`
}

type DeliveryUpdate struct {
	Source           string         `json:"source"`
	SourceLifetime   string         `json:"sourceLifetime"`
	Token            string         `json:"token"`
	ScheduledNS      int64          `json:"scheduledNS,string"`
	Action           *Reference     `json:"action"`
	Generation       *BeliefBound   `json:"generation"`
	Incarnation      string         `json:"incarnation,omitempty"`
	GenerationStatus string         `json:"generationStatus"`
	Status           string         `json:"status"`
	AllReceiversNS   *int64         `json:"allReceiversNS,string"`
	Pairs            []DeliveryPair `json:"pairs"`
}

type UpdateDelivery struct {
	Status                string              `json:"status"`
	Reason                string              `json:"reason"`
	DeadlineNS            int64               `json:"deadlineNS,string"`
	Scheduled             int                 `json:"scheduled,string"`
	Generated             int                 `json:"generated,string"`
	Failed                int                 `json:"failed,string"`
	IncompleteUpdates     int                 `json:"incompleteUpdates,string"`
	NominalPairs          int                 `json:"nominalPairs,string"`
	EligiblePairs         int                 `json:"eligiblePairs,string"`
	Delivered             int                 `json:"delivered,string"`
	Missed                int                 `json:"missed,string"`
	GenerationFailedPairs int                 `json:"generationFailedPairs,string"`
	IncompletePairs       int                 `json:"incompletePairs,string"`
	DeliveredFraction     *string             `json:"deliveredFraction"`
	ConditionalOn         string              `json:"conditionalOn"`
	PairLatencies         report.Distribution `json:"pairLatencies"`
	AllReceiverTimes      report.Distribution `json:"allReceiverTimes"`
	Updates               []DeliveryUpdate    `json:"updates"`
}

type deliveryMeasurements struct {
	result   UpdateDelivery
	source   map[string]int
	token    map[string]int
	executed []bool
}

// The reducer supports bounded single updates in fixed lifetimes. These checks
// deliberately do not depend on protocol settings (retuning uses an overlay).
func validateDelivery(spec Spec, s sim.Scenario) error {
	if len(s.Nodes) < 2 {
		return fmt.Errorf("delivery requires at least two nodes")
	}
	sources, tokens := map[string]bool{}, map[string]bool{}
	nodes := map[string]bool{}
	for _, n := range s.Nodes {
		nodes[n.Identity.Name] = true
		if n.Service != 0 {
			return fmt.Errorf("delivery requires zero node service time")
		}
	}
	last := int64(-1)
	for _, a := range s.Actions {
		if a.Kind == "join" && int64(a.At) < spec.WarmupNS {
			continue
		}
		if a.Kind != "update" || !nodes[a.Node] || sources[a.Node] || len(a.Data) != 8 || tokens[string(a.Data)] || int64(a.At) < spec.WarmupNS || int64(a.At) > spec.HorizonNS-deliveryWindowNS || int64(a.At) < last {
			return fmt.Errorf("unsupported delivery workload: require ordered unique updates, pre-warmup joins and fixed lifetimes")
		}
		for _, b := range a.Data {
			if b < 32 || b > 126 {
				return fmt.Errorf("delivery requires eight-byte ASCII tokens")
			}
		}
		sources[a.Node], tokens[string(a.Data)], last = true, true, int64(a.At)
	}
	if len(sources) == 0 || len(sources) > 20 {
		return fmt.Errorf("delivery requires 1 to 20 single-source updates")
	}
	for _, n := range s.Nodes {
		if tokens[string(n.Identity.Meta)] {
			return fmt.Errorf("delivery token collides with initial metadata")
		}
	}
	return nil
}

func newDelivery(spec Spec, s sim.Scenario) *deliveryMeasurements {
	d := &deliveryMeasurements{result: UpdateDelivery{Status: "not-applicable", Reason: "case has no delivery workload", Updates: []DeliveryUpdate{}}, source: map[string]int{}, token: map[string]int{}}
	if n, _, _ := deliveryCase(spec.Case); n == 0 {
		return d
	}
	d.result.Status, d.result.Reason = "incomplete", "warm-up boundary not reached"
	d.result.DeadlineNS = deliveryWindowNS
	d.result.ConditionalOn = "acceptance within four seconds of authoritative generation"
	var names []string
	for _, n := range s.Nodes {
		names = append(names, n.Identity.Name)
	}
	slices.Sort(names)
	for _, a := range s.Actions {
		if a.Kind != "update" {
			continue
		}
		u := DeliveryUpdate{Source: a.Node, Token: string(a.Data), ScheduledNS: int64(a.At), GenerationStatus: "incomplete", Status: "incomplete", Pairs: []DeliveryPair{}}
		for _, n := range names {
			if n != a.Node {
				u.Pairs = append(u.Pairs, DeliveryPair{Receiver: n, Status: "incomplete"})
			}
		}
		d.source[a.Node], d.token[string(a.Data)] = len(d.result.Updates), len(d.result.Updates)
		d.result.Updates = append(d.result.Updates, u)
		d.result.NominalPairs += len(u.Pairs)
		d.executed = append(d.executed, false)
	}
	d.result.Scheduled = len(d.result.Updates)
	return d
}

func (d *deliveryMeasurements) enroll(x *measurements) {
	for i := range d.result.Updates {
		u := &d.result.Updates[i]
		u.SourceLifetime = x.cohort[u.Source]
		for j := range u.Pairs {
			u.Pairs[j].ReceiverLifetime = x.cohort[u.Pairs[j].Receiver]
		}
	}
}

func (d *deliveryMeasurements) event(x *measurements, e trace.Event, at Reference) error {
	if len(d.result.Updates) == 0 || !x.warm || len(x.result.WarmupFailures) > 0 {
		return nil
	}
	if i, ok := d.source[e.Actor]; ok {
		u := &d.result.Updates[i]
		if at.TimeNS == u.ScheduledNS && e.Lifetime == u.SourceLifetime {
			switch e.Kind {
			case "action.update":
				if u.Action != nil {
					return fmt.Errorf("ambiguous delivery action for %s", u.Source)
				}
				u.Action = &at
			case "core.update":
				if u.Action != nil && before(*u.Action, at) {
					if d.executed[i] {
						return fmt.Errorf("ambiguous delivery generation for %s", u.Source)
					}
					d.executed[i] = true
					for index, c := range e.Changes {
						if c.Kind != "member.upsert" || c.Node != u.Source || c.Member.Name != u.Source || !bytes.Equal(c.Member.Meta, []byte(u.Token)) {
							continue
						}
						if u.Generation != nil {
							return fmt.Errorf("ambiguous delivery origin for %s", u.Source)
						}
						bound := changeBound(at, index)
						u.Generation, u.Incarnation, u.GenerationStatus = &bound, c.Member.Incarnation, "generated"
					}
					if u.Generation == nil {
						u.GenerationStatus = "failed"
					}
				}
			}
		}
	}
	// Changes retain their own order, including multiple changes in one event.
	for index, c := range e.Changes {
		if c.Kind != "member.upsert" {
			continue
		}
		i, ok := d.token[string(c.Member.Meta)]
		if !ok {
			continue
		}
		u := &d.result.Updates[i]
		if u.Generation == nil || c.Member.Name != u.Source || at.TimeNS > u.Generation.TimeNS+deliveryWindowNS {
			continue
		}
		if before(at, *u.Generation.Event) || at == *u.Generation.Event && index <= *u.Generation.ChangeIndex {
			continue
		}
		inc, _ := strconv.ParseUint(c.Member.Incarnation, 10, 64)
		origin, _ := strconv.ParseUint(u.Incarnation, 10, 64)
		if inc < origin {
			continue
		}
		for j := range u.Pairs {
			p := &u.Pairs[j]
			if p.Receiver != c.Node || p.Acceptance != nil || x.state[c.Node].life != p.ReceiverLifetime {
				continue
			}
			bound, latency := changeBound(at, index), at.TimeNS-u.Generation.TimeNS
			p.Acceptance, p.Incarnation, p.LatencyNS, p.Status = &bound, c.Member.Incarnation, &latency, "delivered"
		}
	}
	return nil
}

func (d *deliveryMeasurements) finish(x *measurements, complete bool, through int64) UpdateDelivery {
	r := &d.result
	if r.Status == "not-applicable" {
		return *r
	}
	if !x.warm || len(x.result.WarmupFailures) > 0 {
		r.Status = "unavailable"
		if x.warm {
			r.Status, r.Reason = "invalid-warmup", "membership incomplete immediately before warm-up boundary"
		}
		for i := range r.Updates {
			u := &r.Updates[i]
			u.Status, u.GenerationStatus = r.Status, "unavailable"
			for j := range u.Pairs {
				u.Pairs[j].Status = "unavailable"
			}
		}
		return *r
	}
	r.Status, r.Reason, r.EligiblePairs = "incomplete", "observed trace prefix", r.NominalPairs
	if complete {
		r.Status, r.Reason = "complete", "horizon"
	}
	var pairs, all []int64
	for i := range r.Updates {
		u := &r.Updates[i]
		if u.Generation == nil && (complete || through > u.ScheduledNS) {
			u.GenerationStatus = "failed"
		}
		switch u.GenerationStatus {
		case "generated":
			r.Generated++
		case "failed":
			r.Failed++
		default:
			r.IncompleteUpdates++
		}
		maxLatency, delivered := int64(0), 0
		u.Status = "complete"
		for j := range u.Pairs {
			p := &u.Pairs[j]
			switch {
			case u.GenerationStatus == "failed":
				p.Status = "generation-failed"
				r.GenerationFailedPairs++
			case u.Generation == nil:
				p.Status = "incomplete"
				r.IncompletePairs++
			case p.Acceptance != nil:
				r.Delivered++
				delivered++
				pairs = append(pairs, *p.LatencyNS)
				maxLatency = max(maxLatency, *p.LatencyNS)
			case complete && through >= u.Generation.TimeNS+deliveryWindowNS || through > u.Generation.TimeNS+deliveryWindowNS:
				p.Status = "missed"
				r.Missed++
			default:
				p.Status = "incomplete"
				r.IncompletePairs++
			}
			if p.Status != "delivered" {
				// Status priority is generation failure, then incomplete, then miss.
				if p.Status == "generation-failed" || u.Status == "complete" || p.Status == "incomplete" {
					u.Status = p.Status
				}
			}
		}
		if delivered == len(u.Pairs) {
			u.AllReceiversNS = &maxLatency
			all = append(all, maxLatency)
		}
	}
	r.DeliveredFraction = ratio(int64(r.Delivered), int64(r.EligiblePairs))
	r.PairLatencies = distribution(pairs, r.EligiblePairs)
	r.AllReceiverTimes = distribution(all, r.Scheduled)
	return *r
}
