// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"github.com/modulecollective/lightpool/wire"
	"net/netip"
	"slices"
	"time"
)

type probe struct {
	id                                uint64
	peer                              wire.Identity
	incarnation                       uint64
	command                           uint64
	start, deadline                   Instant
	explicit, periodic, indirect, tcp bool
	helpers                           []wire.Identity
	nacks                             map[netip.AddrPort]bool
	relay                             *wire.Identity
	relaySequence                     uint64
}

func (c *Core) admit(m wire.Message, from netip.AddrPort, received Instant) {
	switch {
	case m.Ping != nil:
		p := m.Ping
		if p.Target != c.self {
			return
		}
		if c.config.Application {
			c.request(pending{kind: AckPayload, peer: p.Source.Clone(), sequence: p.Sequence}, Request{Kind: AckPayload})
		} else {
			c.datagram(p.Source.Address, []wire.Message{{Ack: &wire.Ack{Sequence: p.Sequence}}}, 0, 0, true)
		}
	case m.Indirect != nil:
		p := m.Indirect
		id := c.token().ID
		c.probes[id] = &probe{id: id, peer: p.Target.Clone(), start: c.now, relay: &p.Source, relaySequence: p.Sequence}
		c.arm(key("relay:", id), "relay", c.now.Add(c.config.ProbeTimeout), id, "")
		c.datagram(p.Target.Address, []wire.Message{{Ping: &wire.Ping{Sequence: id, Target: p.Target.Name, Source: routing(c.nodes[c.self].Identity)}}}, 0, id, true)
	case m.Ack != nil:
		c.ack(m.Ack, from, received)
	case m.Nack != nil:
		if p := c.probes[m.Nack.Sequence]; p != nil {
			for _, helper := range p.helpers {
				if helper.Address == from {
					p.nacks[from] = true
					break
				}
			}
		}
	case m.Alive != nil || m.Suspect != nil || m.Dead != nil || m.User != nil:
		q := &c.low
		if m.Alive != nil {
			q = &c.high
		}
		if len(*q) >= c.config.QueueDepth {
			c.stats.QueueDrops++
			c.emit(Observation{Kind: "queue-drop", Value: 1})
			return
		}
		*q = append(*q, handoff{m})
		c.nextDeferred()
	}
}
func (c *Core) startProbe(peer wire.Identity, command uint64, explicit, periodic bool) {
	id := c.token().ID
	window := c.config.ProbeInterval * time.Duration(c.health+1)
	p := &probe{id: id, peer: peer.Clone(), command: command, start: c.now, deadline: c.now.Add(window), explicit: explicit, periodic: periodic, nacks: make(map[netip.AddrPort]bool)}
	if n := c.nodes[peer.Name]; n != nil {
		p.incarnation = n.Incarnation
	}
	c.probes[id] = p
	c.stats.Probes++
	if periodic {
		c.activeProbe = id
	}
	c.arm(key("direct:", id), "direct", c.now.Add(c.config.ProbeTimeout), id, "")
	if !explicit {
		c.arm(key("overall:", id), "overall", p.deadline, id, "")
	}
	messages := []wire.Message{{Ping: &wire.Ping{Sequence: id, Target: peer.Name, Source: routing(c.nodes[c.self].Identity)}}}
	if n := c.nodes[peer.Name]; !explicit && n != nil && n.State != wire.Alive {
		messages = append(messages, wire.Message{Suspect: &wire.Accusation{Node: n.Name, Incarnation: n.Incarnation, From: c.self}})
	}
	c.datagram(peer.Address, messages, 0, id, true)
}
func (c *Core) ack(a *wire.Ack, from netip.AddrPort, received Instant) {
	p := c.probes[a.Sequence]
	if p == nil {
		return
	}
	valid := from == p.peer.Address
	for _, helper := range p.helpers {
		valid = valid || from == helper.Address
	}
	if !valid {
		return
	}
	if p.relay != nil {
		target := p.relay.Address
		seq := p.relaySequence
		c.finishProbe(p.id, Success, 0, nil)
		c.datagram(target, []wire.Message{{Ack: &wire.Ack{Sequence: seq}}}, 0, 0, true)
		return
	}
	rtt := time.Duration(max(0, received-p.start))
	// Direct RTT includes send-side work but cannot become negative if the
	// send completion arrives after the network receive timestamp.
	c.finishProbe(p.id, Success, rtt, a.Payload)
}
func (c *Core) escalate(id uint64) {
	p := c.probes[id]
	if p == nil || p.indirect {
		return
	}
	if p.explicit {
		c.finishProbe(id, Timeout, 0, nil)
		return
	}
	p.indirect = true
	c.cancel(key("direct:", id))
	peers := c.randomNodes(c.config.IndirectChecks, func(n *member) bool { return n.Name == c.self || n.Name == p.peer.Name || n.State != wire.Alive })
	for _, n := range peers {
		p.helpers = append(p.helpers, n.Identity.Clone())
		c.datagram(n.Address, []wire.Message{{Indirect: &wire.Indirect{Sequence: id, Target: routing(p.peer), Source: routing(c.nodes[c.self].Identity)}}}, 0, 0, true)
	}
	if !c.tcpDisabled(p.peer.Name) {
		c.openPing(p)
	}
}
func (c *Core) sendFailed(id uint64, f Failure) {
	p := c.probes[id]
	if p == nil {
		return
	}
	if p.relay != nil {
		if f == LocalFailure {
			c.finishProbe(id, f, 0, nil)
		}
		return
	}
	if f == LocalFailure || p.explicit {
		c.finishProbe(id, f, 0, nil)
	} else {
		c.escalate(id)
	}
}
func (c *Core) finishProbe(id uint64, f Failure, rtt time.Duration, payload []byte) {
	p := c.probes[id]
	if p == nil {
		return
	}
	delete(c.probes, id)
	c.cancel(key("direct:", id))
	c.cancel(key("overall:", id))
	c.cancel(key("relay:", id))
	for _, sid := range sortedIDs(c.streams) {
		if c.streams[sid].probe == id {
			c.endStream(sid, Canceled)
		}
	}
	for _, sid := range sortedIDs(c.sends) {
		if c.sends[sid].probe == id {
			delete(c.sends, sid)
		}
	}
	if p.relay == nil && !p.explicit {
		switch f {
		case Success:
			c.adjustHealth(-1)
		case Timeout, RemoteFailure:
			c.stats.FailedProbes++
			delta := 1
			if len(p.helpers) > 0 {
				delta = len(p.helpers) - len(p.nacks)
			}
			c.adjustHealth(delta)
			// Failure is evidence about the membership we probed, even if its
			// name has since been refuted or reclaimed at another address.
			if n := c.nodes[p.peer.Name]; n != nil && n.Address == p.peer.Address && n.Incarnation == p.incarnation {
				c.suspect(wire.Accusation{Node: p.peer.Name, Incarnation: p.incarnation, From: c.self})
			}
		}
	}
	if f == Success && !p.indirect && p.relay == nil {
		c.emit(Notification{Kind: "ping", Member: wire.Member{Identity: p.peer.Clone()}, Data: slices.Clone(payload), RTT: rtt})
	}
	if p.command != 0 {
		c.emit(Result{Command: p.command, Failure: f, RTT: rtt})
	}
	if p.periodic {
		c.activeProbe = 0
		if c.pendingProbe && !c.stopped {
			c.pendingProbe = false
			c.probeRound()
		}
	}
}
func (c *Core) probeRound() {
	if c.left {
		return
	}
	if c.activeProbe != 0 {
		c.pendingProbe = true
		return
	}
	for checked := 0; checked <= len(c.order); checked++ {
		if c.probeIndex >= len(c.order) {
			c.resetNodes()
		}
		if len(c.order) == 0 {
			return
		}
		name := c.order[c.probeIndex]
		c.probeIndex++
		n := c.nodes[name]
		if name == c.self || n.State == wire.Dead || n.State == wire.Left {
			continue
		}
		c.startProbe(n.Identity, 0, false, true)
		return
	}
}
func (c *Core) startSchedules() {
	if d := c.config.ProbeInterval; d > 0 {
		c.arm("periodic-probe", "periodic-probe", c.now.Add(d+time.Duration(c.rng.Int64N(int64(d)))), 0, "")
	}
	if d := c.config.GossipInterval; d > 0 && c.config.GossipNodes > 0 {
		c.arm("periodic-gossip", "periodic-gossip", c.now.Add(d+time.Duration(c.rng.Int64N(int64(d)))), 0, "")
	}
	if d := c.config.PushPullInterval; d > 0 {
		c.arm("periodic-push", "periodic-push", c.now.Add(pushPullScale(d, len(c.nodes))+time.Duration(c.rng.Int64N(int64(d)))), 0, "")
	}
}
func (c *Core) periodicAgain(key string, d deadline, interval time.Duration) {
	next := d.at.Add(interval)
	if next <= c.now {
		next += Instant((time.Duration(c.now-next)/interval + 1) * interval)
	}
	c.arm(key, d.kind, next, 0, "")
}
func (c *Core) schedulePush() {
	if !c.stopped && c.config.PushPullInterval > 0 {
		c.arm("periodic-push", "periodic-push", c.now.Add(pushPullScale(c.config.PushPullInterval, len(c.nodes))), 0, "")
	}
}
func (c *Core) gossip() {
	peers := c.randomNodes(c.config.GossipNodes, func(n *member) bool {
		return n.Name == c.self || n.State == wire.Left || (n.State == wire.Dead && c.now-n.changed > Instant(c.config.GossipToDead))
	})
	for _, n := range peers {
		c.datagram(n.Address, nil, 0, 0, true)
	}
}
func (c *Core) fire(t Timer) {
	if !c.valid(t.Token) {
		return
	}
	k, ok := c.timerKeys[t.ID]
	if !ok {
		return
	}
	d := c.timers[k]
	if d.timer != t || d.at > c.now {
		return
	}
	delete(c.timers, k)
	delete(c.timerKeys, t.ID)
	switch d.kind {
	case "direct":
		c.escalate(d.op)
	case "overall":
		if p := c.probes[d.op]; p != nil {
			f := Timeout
			if p.tcp {
				f = Success
				c.stats.TCPOnly++
				c.emit(Observation{Kind: "tcp-only", Node: p.peer.Name, Value: 1})
			}
			c.finishProbe(d.op, f, 0, nil)
		}
	case "relay":
		if p := c.probes[d.op]; p != nil {
			to := p.relay.Address
			seq := p.relaySequence
			c.finishProbe(d.op, Timeout, 0, nil)
			c.datagram(to, []wire.Message{{Nack: &wire.Ack{Sequence: seq}}}, 0, 0, true)
		}
	case "suspicion":
		if n := c.nodes[d.node]; n != nil && n.State == wire.Suspect {
			c.dead(wire.Accusation{Node: n.Name, Incarnation: n.Incarnation, From: c.self})
		}
	case "stream":
		c.endStream(d.op, Timeout)
	case "periodic-probe":
		c.periodicAgain(k, d, c.config.ProbeInterval)
		c.probeRound()
	case "periodic-gossip":
		c.periodicAgain(k, d, c.config.GossipInterval)
		c.gossip()
	case "periodic-push":
		peers := c.randomNodes(1, func(n *member) bool { return n.Name == c.self || n.State != wire.Alive })
		if len(peers) == 0 {
			c.schedulePush()
		} else {
			c.openExchange(peers[0].Identity, 0, false, true)
		}
	}
}

func routing(i wire.Identity) wire.Identity { i.Meta = nil; i.Application = ""; return i }
