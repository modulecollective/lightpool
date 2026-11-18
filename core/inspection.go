package core

import "github.com/modulecollective/lightpool/wire"

// Inspection is an optional, owned per-step journal, independent of effects.
// Enabling it does not allocate tokens, use randomness, or change scheduling.
type Inspection struct {
	Kind, Subject, Phase, Purpose, Outcome string
	Member                                 *wire.Member
	ID, Generation, Operation              uint64
	At                                     Instant
	Helpers                                []string
}

func (c *Core) EnableInspection() { c.inspect = true }

// Inspection returns data owned by the caller. The next Step starts a new journal.
func (c *Core) Inspection() []Inspection {
	out := make([]Inspection, len(c.journal))
	for i, e := range c.journal {
		if e.Member != nil {
			e.Member = ptrMember(*e.Member)
		}
		e.Helpers = append([]string(nil), e.Helpers...)
		out[i] = e
	}
	return out
}
func (c *Core) record(e Inspection) {
	if c.inspect {
		c.journal = append(c.journal, e)
	}
}
func (c *Core) recordMember(n wire.Member) {
	if c.inspect {
		c.record(Inspection{Kind: "member.upsert", Member: ptrMember(n)})
	}
}

// Summary reads constant-size local counters without cloning membership.
func (c *Core) Summary() Snapshot {
	return Snapshot{Now: c.now, Health: c.health, Backlog: len(c.broadcasts), PendingStreams: len(c.streams), Stopped: c.stopped, Left: c.left, Stats: c.stats}
}
func (c *Core) recordProbe(p *probe, phase, outcome string) {
	if !c.inspect {
		return
	}
	helpers := make([]string, 0, len(p.helpers))
	for _, h := range p.helpers {
		helpers = append(helpers, h.Name)
	}
	c.record(Inspection{Kind: "probe", ID: p.id, Subject: p.peer.Name, Phase: phase, Outcome: outcome, Helpers: helpers})
}
