// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"bytes"
	"github.com/modulecollective/lightpool/wire"
	"math"
	"net/netip"
	"time"
)

type suspicion struct {
	start         Instant
	min, max      time.Duration
	k, n          int
	confirmations map[string]bool
}

// These formulas are retained from memberlist's util.go and suspicion.go.
func suspicionTimeout(mult, n int, interval time.Duration) time.Duration {
	scale := math.Max(1, math.Log10(math.Max(1, float64(n))))
	return time.Duration(mult) * time.Duration(scale*1000) * interval / 1000
}
func retransmitLimit(mult, n int) int { return mult * int(math.Ceil(math.Log10(float64(n+1)))) }
func pushPullScale(interval time.Duration, n int) time.Duration {
	if n <= 32 {
		return interval
	}
	return time.Duration(math.Ceil(math.Log2(float64(n))-5)+1) * interval
}
func (s *suspicion) duration() time.Duration {
	if s.k < 1 {
		return s.min
	}
	frac := math.Log(float64(s.n)+1) / math.Log(float64(s.k)+1)
	raw := s.max.Seconds() - frac*(s.max.Seconds()-s.min.Seconds())
	return max(s.min, time.Duration(math.Floor(1000*raw))*time.Millisecond)
}
func (c *Core) allowed(ip netip.Addr) bool {
	if c.config.Allowed == nil {
		return true
	}
	for _, p := range c.config.Allowed {
		if p.Contains(ip.Unmap()) {
			return true
		}
	}
	return false
}
func (c *Core) alive(n wire.Member, op uint64, approved bool) {
	if !c.fitsAlive(n) {
		c.emit(Observation{Kind: "oversize-alive", Node: n.Name, Value: 1})
		return
	}
	if !c.allowed(n.Address.Addr()) || n.Application != c.nodes[c.self].Application {
		if op != 0 {
			if s := c.streams[op]; s != nil {
				s.cursor++
				c.mergeNext(s)
			}
		}
		return
	}
	if c.config.Application && !approved {
		c.request(pending{kind: ApproveAlive, op: op, msg: wire.Message{Alive: ptrMember(n)}}, Request{Kind: ApproveAlive, Member: n})
		return
	}
	c.applyAlive(n)
}
func (c *Core) fitsAlive(n wire.Member) bool {
	size, err := wire.Size([]wire.Message{{Alive: &n}}, c.config.Label, c.config.Protected)
	return err == nil && size <= c.config.PacketBudget
}
func (c *Core) applyAlive(n wire.Member) {
	// Every approval re-enters here against current state, not a retained pointer.
	if n.Name == c.self && c.left {
		return
	}
	old := c.nodes[n.Name]
	reclaim := false
	if old != nil && old.Address != n.Address {
		reclaim = old.State == wire.Left || (old.State == wire.Dead && c.config.ReclaimAfter > 0 && c.now-old.changed > Instant(c.config.ReclaimAfter))
		if !reclaim {
			c.emit(Notification{Kind: "conflict", Member: old.Member.Clone(), Other: n.Clone()})
			return
		}
	}
	if old != nil {
		if n.Name == c.self {
			if n.Incarnation < old.Incarnation {
				return
			}
			if n.Incarnation == old.Incarnation && bytes.Equal(n.Meta, old.Meta) && n.Application == old.Application {
				return
			}
			c.refute(n.Incarnation)
			return
		}
		if n.Incarnation <= old.Incarnation && !reclaim {
			return
		}
	}
	c.cancel("suspect:" + n.Name)
	delete(c.susp, n.Name)
	kind := "update"
	if old == nil || old.State == wire.Dead || old.State == wire.Left {
		kind = "join"
	}
	changed := c.now
	if old != nil && old.State == wire.Alive {
		changed = old.changed
	}
	if old == nil {
		offset := 0
		if len(c.order) > 0 {
			offset = c.rng.IntN(len(c.order))
		}
		c.order = append(c.order, n.Name)
		last := len(c.order) - 1
		c.order[offset], c.order[last] = c.order[last], c.order[offset]
	}
	c.nodes[n.Name] = &member{n.Clone(), changed}
	c.queue(wire.Message{Alive: ptrMember(n)}, n.Name, 0)
	if kind == "join" || old == nil || !bytes.Equal(old.Meta, n.Meta) {
		c.notify(kind, n)
	}
}
func (c *Core) refute(inc uint64) {
	n := c.nodes[c.self]
	if c.left {
		return
	}
	n.Incarnation = max(n.Incarnation, inc) + 1
	n.State = wire.Alive
	c.adjustHealth(1)
	c.queue(wire.Message{Alive: ptrMember(n.Member)}, c.self, 0)
	c.notify("refute", n.Member)
}
func (c *Core) suspect(a wire.Accusation) {
	n := c.nodes[a.Node]
	if n == nil || a.Incarnation < n.Incarnation {
		return
	}
	if s := c.susp[a.Node]; s != nil {
		if s.n >= s.k || s.confirmations[a.From] {
			return
		}
		s.confirmations[a.From] = true
		s.n++
		c.queue(wire.Message{Suspect: &a}, a.Node, 0)
		// arm clamps an already shortened deadline to now; no recursive transition.
		c.arm("suspect:"+a.Node, "suspicion", s.start.Add(s.duration()), 0, a.Node)
		return
	}
	if n.State != wire.Alive {
		return
	}
	if a.Node == c.self {
		c.refute(a.Incarnation)
		return
	}
	n.State = wire.Suspect
	n.Incarnation = a.Incarnation
	n.changed = c.now
	c.queue(wire.Message{Suspect: &a}, a.Node, 0)
	c.notify("suspect", n.Member)
	k := c.config.SuspicionMult - 2
	if len(c.nodes)-2 < k {
		k = 0
	}
	interval := c.config.ProbeInterval
	min := suspicionTimeout(c.config.SuspicionMult, len(c.nodes), interval)
	s := &suspicion{start: c.now, min: min, max: time.Duration(c.config.SuspicionMaxMult) * min, k: k, confirmations: map[string]bool{a.From: true}}
	c.susp[a.Node] = s
	c.arm("suspect:"+a.Node, "suspicion", c.now.Add(s.duration()), 0, a.Node)
}
func (c *Core) dead(a wire.Accusation) {
	n := c.nodes[a.Node]
	if n == nil || a.Incarnation < n.Incarnation {
		return
	}
	if a.Node == c.self && !c.left {
		c.refute(a.Incarnation)
		return
	}
	c.cancel("suspect:" + a.Node)
	delete(c.susp, a.Node)
	if n.State == wire.Dead || n.State == wire.Left {
		return
	}
	n.Incarnation = a.Incarnation
	n.State = wire.Dead
	if a.Node == a.From {
		n.State = wire.Left
	}
	n.changed = c.now
	c.queue(wire.Message{Dead: &a}, a.Node, 0)
	c.notify("leave", n.Member)
}
func (c *Core) update(e Update) {
	if c.left || len(e.Meta) > wire.MaxMeta {
		c.emit(Result{Command: e.Command, Failure: LocalFailure})
		return
	}
	n := c.nodes[c.self]
	copyNode := n.Member.Clone()
	copyNode.Meta = bytes.Clone(e.Meta)
	copyNode.Incarnation++
	size, err := wire.Size([]wire.Message{{Alive: &copyNode}}, c.config.Label, c.config.Protected)
	if err != nil || size > c.config.PacketBudget {
		c.emit(Result{Command: e.Command, Failure: LocalFailure})
		return
	}
	n.Member = copyNode
	c.queue(wire.Message{Alive: ptrMember(n.Member)}, c.self, e.Command)
	c.notify("update", n.Member)
}
func (c *Core) leave(command uint64) {
	if c.left {
		c.emit(Result{Command: command})
		return
	}
	c.left = true
	n := c.nodes[c.self]
	a := wire.Accusation{Node: c.self, From: c.self, Incarnation: n.Incarnation}
	c.dead(a)
	// Replace the just-enqueued record with the same terminal notification rule
	// as Update: queue completion, including invalidation, never remote receipt.
	c.queue(wire.Message{Dead: &a}, c.self, command)
}
func (c *Core) hasPeers() bool {
	for _, name := range c.order {
		if name != c.self {
			s := c.nodes[name].State
			if s == wire.Alive || s == wire.Suspect {
				return true
			}
		}
	}
	return false
}
func (c *Core) randomNodes(k int, exclude func(*member) bool) []*member {
	n := len(c.order)
	out := make([]*member, 0, k)
	if n < k*3 {
		names := append([]string(nil), c.order...)
		c.rng.Shuffle(n, func(i, j int) { names[i], names[j] = names[j], names[i] })
		for _, name := range names {
			m := c.nodes[name]
			if !exclude(m) {
				out = append(out, m)
				if len(out) == k {
					break
				}
			}
		}
		return out
	}
	for j := 0; j < 3*n && len(out) < k; j++ {
		m := c.nodes[c.order[c.rng.IntN(n)]]
		if exclude(m) {
			continue
		}
		found := false
		for _, old := range out {
			if old.Name == m.Name {
				found = true
				break
			}
		}
		if !found {
			out = append(out, m)
		}
	}
	return out
}
func (c *Core) resetNodes() {
	kept := c.order[:0]
	for _, name := range c.order {
		n := c.nodes[name]
		if name != c.self && (n.State == wire.Dead || n.State == wire.Left) && c.now-n.changed > Instant(c.config.GossipToDead) {
			delete(c.nodes, name)
		} else {
			kept = append(kept, name)
		}
	}
	c.order = kept
	c.rng.Shuffle(len(c.order), func(i, j int) { c.order[i], c.order[j] = c.order[j], c.order[i] })
	c.probeIndex = 0
}
