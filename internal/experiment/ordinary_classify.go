package experiment

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/internal/ordinary"
	"github.com/modulecollective/lightpool/wire"
)

const OrdinaryClassificationVersion = "lightpool-ordinary-classification/v1"

// OrdinaryStep is one replay-verified core step with its full local interval:
// StartNS is the owner's step start and EndNS the matching End record.
type OrdinaryStep struct {
	Sequence uint64
	StartNS  int64
	CoreAtNS int64
	EndNS    int64
	Journal  []core.Inspection
}

// OrdinaryNode is one process's recorded lifetime and its raw clock exchanges.
// Role is "A", "T", "O" or "" for the remaining cluster.
type OrdinaryNode struct {
	Name       string
	Role       string
	PID        int
	Lifetime   uint64
	Address    netip.AddrPort
	FileSHA256 string
	Clock      []ordinary.Exchange
	Steps      []OrdinaryStep
}

// OrdinaryInput is a complete recorded attempt. Warm-up and horizon are the
// realized coordinator-time uncertainty of each boundary, not requested times.
type OrdinaryInput struct {
	Nodes   []OrdinaryNode
	Pairs   []Pair
	Warmup  ordinary.Interval
	Horizon ordinary.Interval
}

// OrdinaryRef names a journal fact by node, local step and inspection index,
// with the whole step mapped into coordinator time.
type OrdinaryRef struct {
	Node        string            `json:"node"`
	Step        uint64            `json:"step,string"`
	Inspection  int               `json:"inspection,string"`
	FileSHA256  string            `json:"fileSHA256"`
	Coordinator ordinary.Interval `json:"coordinator"`
}

type OrdinaryOrigin struct {
	Sender string       `json:"sender"`
	Fact   *OrdinaryRef `json:"fact"`
	Order  string       `json:"orderBeforeReceipt"`
}

type OrdinaryShortening struct {
	Confirmation       OrdinaryRef    `json:"confirmation"`
	Origin             OrdinaryOrigin `json:"origin"`
	PreviousDeadlineNS *int64         `json:"previousDeadlineNS,string"`
	Arm                *OrdinaryRef   `json:"arm"`
	DeadlineNS         *int64         `json:"deadlineNS,string"`
	Status             string         `json:"status"`
	Reasons            []string       `json:"rejections"`
	Ambiguities        []string       `json:"ambiguities"`
	generation         uint64
}

type OrdinaryEpisode struct {
	Pair
	TargetLifetime        uint64               `json:"targetLifetime,string"`
	Incarnation           uint64               `json:"incarnation,string"`
	Start                 OrdinaryRef          `json:"start"`
	Origin                OrdinaryOrigin       `json:"origin"`
	InitialDeadlineNS     *int64               `json:"initialDeadlineNS,string"`
	Shortenings           []OrdinaryShortening `json:"shortenings"`
	Refutation            *OrdinaryRef         `json:"refutationGeneration"`
	RefutationIncarnation uint64               `json:"refutationIncarnation,string"`
	Acceptance            *OrdinaryRef         `json:"refutationAcceptance"`
	Expiry                *OrdinaryRef         `json:"expiry"`
	End                   *OrdinaryRef         `json:"end"`
	EndCause              string               `json:"endCause"`
	Status                string               `json:"status"`
	Harmful               bool                 `json:"harmful"`
	Reasons               []string             `json:"rejections"`
	Ambiguities           []string             `json:"ambiguities"`
	expiryGeneration      uint64
	expiryCoreAt          int64
}

// SharedOrigin groups acceleration episodes caused by the same two A failures;
// they are one origin, not independent repetitions.
type SharedOrigin struct {
	Target      string   `json:"target"`
	Incarnation uint64   `json:"incarnation,string"`
	Senders     []string `json:"senders"`
	Observers   []string `json:"observers"`
}

type OrdinaryClassification struct {
	Format        string             `json:"format"`
	Episodes      []*OrdinaryEpisode `json:"episodes"`
	Accelerations int                `json:"accelerationEpisodes,string"`
	Harmful       int                `json:"harmfulEpisodes,string"`
	Inconclusive  int                `json:"inconclusiveEpisodes,string"`
	SharedOrigins []SharedOrigin     `json:"sharedOrigins"`
	Limits        []string           `json:"limits"`
}

type ordinaryFact struct {
	ref         OrdinaryRef
	incarnation uint64
}

type ordinaryNodeView struct {
	*OrdinaryNode
	offset ordinary.Offset
}

func (n *ordinaryNodeView) ref(s *OrdinaryStep, i int) OrdinaryRef {
	return OrdinaryRef{Node: n.Name, Step: s.Sequence, Inspection: i, FileSHA256: n.FileSHA256, Coordinator: n.offset.Map(s.StartNS, s.EndNS)}
}

func localBefore(a, b OrdinaryRef) bool {
	return a.Step < b.Step || a.Step == b.Step && a.Inspection < b.Inspection
}

// order reports a proven cross-process order, or ambiguity for any overlap.
func order(a, b OrdinaryRef) string {
	if a.Node == b.Node {
		if localBefore(a, b) {
			return "before"
		}
		return "after"
	}
	switch {
	case a.Coordinator.Before(b.Coordinator):
		return "before"
	case b.Coordinator.Before(a.Coordinator):
		return "after"
	}
	return "ambiguous"
}

// ClassifyOrdinary reduces a complete recorded attempt. Malformed input is an
// error; a valid attempt without acceleration is a successful null result.
func ClassifyOrdinary(in OrdinaryInput) (*OrdinaryClassification, error) {
	nodes := make(map[string]*ordinaryNodeView, len(in.Nodes))
	for i := range in.Nodes {
		n := &in.Nodes[i]
		if n.Name == "" || nodes[n.Name] != nil || n.FileSHA256 == "" || !slices.Contains([]string{"A", "T", "O", ""}, n.Role) {
			return nil, fmt.Errorf("invalid or duplicate ordinary node %q", n.Name)
		}
		if len(n.Steps) == 0 {
			return nil, fmt.Errorf("node %s has no recorded steps", n.Name)
		}
		for j, s := range n.Steps {
			if s.Sequence != uint64(j+1) || s.CoreAtNS < s.StartNS || s.EndNS < s.CoreAtNS || j > 0 && s.StartNS < n.Steps[j-1].EndNS {
				return nil, fmt.Errorf("node %s step %d is out of order or unbounded", n.Name, j+1)
			}
		}
		offset, err := ordinary.Bound(n.PID, n.Clock)
		if err != nil {
			return nil, fmt.Errorf("node %s clock: %w", n.Name, err)
		}
		nodes[n.Name] = &ordinaryNodeView{n, offset}
	}
	if in.Warmup.LoNS > in.Warmup.HiNS || in.Horizon.LoNS > in.Horizon.HiNS || !in.Warmup.Before(in.Horizon) {
		return nil, errors.New("invalid warm-up or horizon bounds")
	}
	if len(in.Pairs) == 0 {
		return nil, errors.New("no eligible pairs")
	}
	out := &OrdinaryClassification{Format: OrdinaryClassificationVersion, Episodes: []*OrdinaryEpisode{}, SharedOrigins: []SharedOrigin{}, Limits: []string{
		"Recorded ordinary execution only: every node journal was enabled, so this cannot establish unrecorded behavior.",
		"Cross-process order is proven only when whole-step coordinator intervals are disjoint; overlap, including equal endpoints, is ambiguous.",
		"Helper identities and step traffic do not reveal the packet route of an ACK or refutation.",
	}}
	for _, p := range in.Pairs {
		o, t := nodes[p.Observer], nodes[p.Subject]
		if o == nil || t == nil || o.Role != "O" || t.Role != "T" {
			return nil, fmt.Errorf("pair %s->%s lacks an O or T journal", p.Observer, p.Subject)
		}
		episodes, err := scanObserver(nodes, o, t, in)
		if err != nil {
			return nil, err
		}
		out.Episodes = append(out.Episodes, episodes...)
	}
	shared := map[string]*SharedOrigin{}
	var keys []string
	for _, ep := range out.Episodes {
		switch ep.Status {
		case "acceleration":
			out.Accelerations++
		case "inconclusive":
			out.Inconclusive++
		}
		if ep.Harmful {
			out.Harmful++
		}
		if ep.Status != "acceleration" {
			continue
		}
		for _, s := range ep.Shortenings {
			if s.Status != "qualifying" {
				continue
			}
			senders := []string{ep.Origin.Sender, s.Origin.Sender}
			slices.Sort(senders)
			key := fmt.Sprintf("%s/%d/%s", ep.Subject, ep.Incarnation, strings.Join(senders, "+"))
			g := shared[key]
			if g == nil {
				g = &SharedOrigin{Target: ep.Subject, Incarnation: ep.Incarnation, Senders: senders}
				shared[key] = g
				keys = append(keys, key)
			}
			if !slices.Contains(g.Observers, ep.Observer) {
				g.Observers = append(g.Observers, ep.Observer)
			}
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		out.SharedOrigins = append(out.SharedOrigins, *shared[k])
	}
	return out, nil
}

// scanObserver walks O's journal in local semantic order, tracking the live
// suspicion timer for T so every shortening compares against the deadline that
// was actually armed when the confirmation arrived.
func scanObserver(nodes map[string]*ordinaryNodeView, o, t *ordinaryNodeView, in OrdinaryInput) ([]*OrdinaryEpisode, error) {
	type live struct {
		id, generation uint64
		at             int64
	}
	var (
		episodes    []*OrdinaryEpisode
		open        *OrdinaryEpisode
		timer       *live
		pending     *OrdinaryShortening
		acceptances []ordinaryFact
	)
	for si := range o.Steps {
		s := &o.Steps[si]
		var upsert *wire.Member
		var fired *live
		var firedRef OrdinaryRef
		for i, j := range s.Journal {
			ref := o.ref(s, i)
			switch {
			case j.Kind == "member.upsert" && j.Member != nil && j.Member.Name == t.Name:
				m := *j.Member
				upsert = &m
				if m.State == wire.Alive {
					acceptances = append(acceptances, ordinaryFact{ref, m.Incarnation})
					if open != nil && m.Incarnation > open.Incarnation {
						open.End, open.EndCause, open = &ref, "accepted", nil
					}
				}
				if m.State == wire.Dead && open != nil {
					open.End, open.EndCause = &ref, "received-dead"
					if fired != nil {
						open.EndCause, open.Expiry = "local-expiry", &firedRef
						open.expiryGeneration, open.expiryCoreAt = fired.generation, s.CoreAtNS
					}
					open = nil
				}
			case j.Kind == "suspicion" && j.Subject == t.Name && strings.HasPrefix(j.Outcome, "started:"):
				if open != nil {
					open.EndCause = "superseded"
				}
				open = &OrdinaryEpisode{Pair: Pair{Observer: o.Name, Subject: t.Name}, TargetLifetime: t.Lifetime, Start: ref, Shortenings: []OrdinaryShortening{}, EndCause: "censored", Reasons: []string{}, Ambiguities: []string{}}
				open.Origin = OrdinaryOrigin{Sender: strings.TrimPrefix(j.Outcome, "started:")}
				if upsert == nil || upsert.State != wire.Suspect {
					open.Reasons = append(open.Reasons, "suspicion start lacks a same-step suspect membership")
				} else {
					open.Incarnation = upsert.Incarnation
					if upsert.Address != t.Address {
						open.Reasons = append(open.Reasons, "suspected member is a different target lifetime")
					}
				}
				episodes = append(episodes, open)
			case j.Kind == "suspicion" && j.Subject == t.Name && strings.HasPrefix(j.Outcome, "confirmation:"):
				if open == nil {
					continue
				}
				sh := OrdinaryShortening{Confirmation: ref, Origin: OrdinaryOrigin{Sender: strings.TrimPrefix(j.Outcome, "confirmation:")}, Reasons: []string{}, Ambiguities: []string{}}
				if timer != nil {
					at := timer.at
					sh.PreviousDeadlineNS = &at
				}
				open.Shortenings = append(open.Shortenings, sh)
				pending = &open.Shortenings[len(open.Shortenings)-1]
			case j.Kind == "timer" && j.Purpose == "suspicion" && j.Subject == t.Name:
				switch j.Phase {
				case "arm":
					timer = &live{j.ID, j.Generation, int64(j.At)}
					at := int64(j.At)
					if pending != nil {
						pending.Arm, pending.DeadlineNS, pending.generation = &ref, &at, j.Generation
						pending = nil
					} else if open != nil && open.InitialDeadlineNS == nil {
						open.InitialDeadlineNS = &at
					}
				case "cancel":
					timer = nil
				case "fire":
					if timer != nil && timer.id == j.ID && timer.generation == j.Generation {
						fired, firedRef = timer, ref
					}
					timer = nil
				}
			}
		}
		pending = nil
	}
	refutations := targetRefutations(t)
	for _, ep := range episodes {
		if err := classifyEpisode(nodes, t, ep, refutations, acceptances, in); err != nil {
			return nil, err
		}
	}
	return episodes, nil
}

func targetRefutations(t *ordinaryNodeView) []ordinaryFact {
	var out []ordinaryFact
	for si := range t.Steps {
		s := &t.Steps[si]
		var self *wire.Member
		for i, j := range s.Journal {
			if j.Kind == "member.upsert" && j.Member != nil && j.Member.Name == t.Name {
				m := *j.Member
				self = &m
			}
			if j.Kind == "refutation" && j.Subject == t.Name && self != nil && self.State == wire.Alive {
				out = append(out, ordinaryFact{t.ref(s, i), self.Incarnation})
			}
		}
	}
	return out
}

// origin finds the sender's first local suspicion of the same target lifetime
// and incarnation, preceded in its step by that sender's own failed probe.
// A later matching suspicion cannot provide provenance for an earlier receipt.
func origin(nodes map[string]*ordinaryNodeView, t *ordinaryNodeView, sender string, incarnation uint64, receipt OrdinaryRef) (OrdinaryOrigin, []string, []string, error) {
	o := OrdinaryOrigin{Sender: sender, Order: "missing"}
	a := nodes[sender]
	if a == nil {
		return o, nil, nil, fmt.Errorf("sender %s has no journal", sender)
	}
	if a.Role != "A" {
		return o, []string{"sender " + sender + " outside A"}, nil, nil
	}
	for si := range a.Steps {
		s := &a.Steps[si]
		var upsert *wire.Member
		failed := false
		for i, j := range s.Journal {
			switch {
			case j.Kind == "probe" && j.Subject == t.Name && j.Phase == "finished" && (j.Outcome == core.Timeout.String() || j.Outcome == core.RemoteFailure.String()):
				failed = true
			case j.Kind == "member.upsert" && j.Member != nil && j.Member.Name == t.Name:
				m := *j.Member
				upsert = &m
			case j.Kind == "suspicion" && j.Subject == t.Name && j.Outcome == "started:"+sender:
				if !failed || upsert == nil || upsert.State != wire.Suspect || upsert.Incarnation != incarnation || upsert.Address != t.Address {
					continue
				}
				ref := a.ref(s, i)
				o.Fact, o.Order = &ref, order(ref, receipt)
				switch o.Order {
				case "after":
					return o, []string{"sender " + sender + " origin does not precede receipt"}, nil, nil
				case "ambiguous":
					return o, nil, []string{"sender " + sender + " origin overlaps receipt"}, nil
				}
				return o, nil, nil, nil
			}
		}
	}
	return o, []string{"sender " + sender + " lacks a same-incarnation failed-probe origin"}, nil, nil
}

func classifyEpisode(nodes map[string]*ordinaryNodeView, t *ordinaryNodeView, ep *OrdinaryEpisode, refutations, acceptances []ordinaryFact, in OrdinaryInput) error {
	var err error
	var reasons, ambiguous []string
	ep.Origin, reasons, ambiguous, err = origin(nodes, t, ep.Origin.Sender, ep.Incarnation, ep.Start)
	if err != nil {
		return err
	}
	ep.Reasons = append(ep.Reasons, reasons...)
	ep.Ambiguities = append(ep.Ambiguities, ambiguous...)
	switch {
	case ep.Start.Coordinator.Before(in.Warmup):
		ep.Status = "ineligible"
		ep.Reasons = append(ep.Reasons, "suspicion started before warm-up")
	case !in.Warmup.Before(ep.Start.Coordinator):
		ep.Ambiguities = append(ep.Ambiguities, "suspicion start overlaps warm-up boundary")
	}
	if ep.InitialDeadlineNS == nil {
		ep.Reasons = append(ep.Reasons, "missing initial suspicion arm")
	}
	for _, r := range refutations {
		if r.incarnation > ep.Incarnation {
			ep.Refutation, ep.RefutationIncarnation = &r.ref, r.incarnation
			break
		}
	}
	if ep.Refutation == nil {
		ep.Reasons = append(ep.Reasons, "refutation generation censored")
	} else {
		for _, a := range acceptances {
			if a.incarnation == ep.RefutationIncarnation && localBefore(ep.Start, a.ref) {
				ep.Acceptance = &a.ref
				break
			}
		}
		switch {
		case ep.Acceptance == nil:
			ep.Reasons = append(ep.Reasons, "matching refutation acceptance censored")
		case in.Horizon.Before(ep.Acceptance.Coordinator):
			ep.Reasons = append(ep.Reasons, "refutation accepted after horizon")
		case !ep.Acceptance.Coordinator.Before(in.Horizon):
			ep.Ambiguities = append(ep.Ambiguities, "acceptance overlaps horizon")
		}
		if ep.Acceptance != nil {
			switch order(*ep.Refutation, *ep.Acceptance) {
			case "after":
				ep.Reasons = append(ep.Reasons, "acceptance precedes refutation generation")
			case "ambiguous":
				ep.Ambiguities = append(ep.Ambiguities, "refutation generation overlaps acceptance")
			}
		}
	}
	qualifying, inconclusive := false, false
	for i := range ep.Shortenings {
		s := &ep.Shortenings[i]
		s.Origin, reasons, ambiguous, err = origin(nodes, t, s.Origin.Sender, ep.Incarnation, s.Confirmation)
		if err != nil {
			return err
		}
		s.Reasons = append(append(s.Reasons, ep.Reasons...), reasons...)
		s.Ambiguities = append(append(s.Ambiguities, ep.Ambiguities...), ambiguous...)
		if s.Origin.Sender == ep.Origin.Sender {
			s.Reasons = append(s.Reasons, "confirmation repeats the starting sender")
		}
		switch {
		case s.Arm == nil || s.PreviousDeadlineNS == nil:
			s.Reasons = append(s.Reasons, "confirmation did not replace a live suspicion deadline")
		case *s.DeadlineNS >= *s.PreviousDeadlineNS:
			s.Reasons = append(s.Reasons, "deadline did not strictly shorten")
		}
		if s.Arm != nil && ep.Refutation != nil {
			switch order(*s.Arm, *ep.Refutation) {
			case "after":
				s.Reasons = append(s.Reasons, "target refuted before shortening")
			case "ambiguous":
				s.Ambiguities = append(s.Ambiguities, "shortening overlaps refutation generation")
			}
		}
		if s.Arm != nil && ep.Acceptance != nil && !localBefore(*s.Arm, *ep.Acceptance) {
			s.Reasons = append(s.Reasons, "observer accepted refutation before shortening")
		}
		switch {
		case len(s.Reasons) > 0:
			s.Status = "rejected"
		case len(s.Ambiguities) > 0:
			s.Status, inconclusive = "inconclusive", true
		default:
			s.Status, qualifying = "qualifying", true
		}
	}
	switch {
	case ep.Status == "ineligible":
	case qualifying:
		ep.Status = "acceleration"
	case inconclusive:
		ep.Status = "inconclusive"
	default:
		ep.Status = "no-acceleration"
		if len(ep.Shortenings) == 0 {
			ep.Reasons = append(ep.Reasons, "no confirmation reached the live suspicion")
		}
	}
	// Harm: the shortened timer itself fired and made T dead locally before the
	// original deadline and before O accepted the refutation. Received dead
	// gossip is a different outcome and never counts.
	if ep.Status == "acceleration" && ep.EndCause == "local-expiry" && ep.InitialDeadlineNS != nil && ep.expiryCoreAt < *ep.InitialDeadlineNS && ep.Acceptance != nil && localBefore(*ep.Expiry, *ep.Acceptance) && ep.Expiry.Coordinator.Before(in.Horizon) {
		for _, s := range ep.Shortenings {
			if s.Status == "qualifying" && s.generation == ep.expiryGeneration {
				ep.Harmful = true
			}
		}
	}
	return nil
}
