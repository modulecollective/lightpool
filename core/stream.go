// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"github.com/modulecollective/lightpool/wire"
	"net/netip"
	"slices"
)

type stream struct {
	id                      Token
	peer                    netip.AddrPort
	command, probe          uint64
	purpose, phase          string
	join, periodic, passive bool
	outgoing                wire.Message
	remote                  *wire.Exchange
	early                   *StreamMessage
	cursor                  int
	deadline                Instant
	closedFailure           Failure
	closedPending           bool
}

func (c *Core) beginStream(peer wire.Identity, command uint64, purpose string) *stream {
	s := &stream{id: c.token(), peer: peer.Address, command: command, purpose: purpose, phase: "connect", deadline: c.now.Add(c.config.StreamTimeout)}
	c.streams[s.id.ID] = s
	c.arm(key("stream:", s.id.ID), "stream", s.deadline, s.id.ID, "")
	return s
}
func (c *Core) openExchange(peer wire.Identity, command uint64, join, periodic bool) {
	if c.exchanges() >= c.config.MaxPushPull {
		if command != 0 {
			c.emit(Result{Command: command, Failure: LocalFailure})
		}
		if periodic {
			c.schedulePush()
		}
		return
	}
	s := c.beginStream(peer, command, "exchange")
	s.join = join
	s.periodic = periodic
	c.emit(OpenStream{ID: s.id, To: s.peer, Deadline: s.deadline})
}
func (c *Core) openPing(p *probe) {
	s := c.beginStream(p.peer, 0, "ping")
	s.probe = p.id
	s.deadline = p.deadline
	c.arm(key("stream:", s.id.ID), "stream", s.deadline, s.id.ID, "")
	s.outgoing = wire.Message{Ping: &wire.Ping{Sequence: p.id, Target: p.peer.Name, Source: routing(c.nodes[c.self].Identity)}}
	c.emit(OpenStream{ID: s.id, To: s.peer, Deadline: s.deadline})
}
func (c *Core) openUser(peer wire.Identity, m wire.Message, command uint64) {
	s := c.beginStream(peer, command, "user")
	s.outgoing = m
	c.emit(OpenStream{ID: s.id, To: s.peer, Deadline: s.deadline})
}
func (c *Core) exchanges() int {
	n := 0
	for _, s := range c.streams {
		if s.purpose == "exchange" {
			n++
		}
	}
	return n
}
func (c *Core) accept(e Accepted) {
	if _, ok := c.streams[e.ID.ID]; ok {
		return
	}
	if len(c.streams) >= c.config.MaxPushPull*2 {
		c.emit(CloseStream{ID: e.ID})
		return
	}
	s := &stream{id: e.ID, peer: e.Peer, passive: true, phase: "receive", deadline: c.now.Add(c.config.StreamTimeout)}
	c.streams[e.ID.ID] = s
	c.arm(key("stream:", s.id.ID), "stream", s.deadline, s.id.ID, "")
}
func (c *Core) opened(e Opened) {
	s := c.streams[e.ID.ID]
	if s == nil || s.id != e.ID || s.phase != "connect" {
		return
	}
	if e.Failure != Success {
		c.endStream(s.id.ID, e.Failure)
		return
	}
	if s.purpose == "exchange" {
		c.prepareExchange(s)
	} else {
		c.write(s, s.outgoing, "request")
	}
}
func (c *Core) prepareExchange(s *stream) {
	s.phase = "local-state"
	if c.config.Application {
		c.request(pending{kind: LocalState, op: s.id.ID}, Request{Kind: LocalState, Join: s.join})
	} else {
		c.writeExchange(s, nil)
	}
}
func (c *Core) writeExchange(s *stream, data []byte) {
	members := c.Snapshot().Members
	m := wire.Message{Exchange: &wire.Exchange{Join: s.join, Members: members, Data: slices.Clone(data)}}
	stage := "request"
	if s.passive {
		stage = "response"
	}
	c.write(s, m, stage)
}
func (c *Core) write(s *stream, m wire.Message, stage string) {
	if m.Validate() != nil {
		c.endStream(s.id.ID, LocalFailure)
		return
	}
	s.phase = "write-" + stage
	s.deadline = c.now.Add(c.config.StreamTimeout)
	if p := c.probes[s.probe]; p != nil {
		s.deadline = min(s.deadline, p.deadline)
	}
	c.arm(key("stream:", s.id.ID), "stream", s.deadline, s.id.ID, "")
	c.emit(WriteStream{ID: s.id, Messages: []wire.Message{m.Clone()}, Deadline: s.deadline})
}
func (c *Core) written(e Written) {
	s := c.streams[e.ID.ID]
	if s == nil || s.id != e.ID {
		return
	}
	if s.phase != "write-request" && s.phase != "write-response" && s.phase != "write-reply" {
		return
	}
	if e.Failure != Success {
		c.endStream(s.id.ID, e.Failure)
		return
	}
	if s.phase == "write-reply" || (s.purpose == "user" && !s.passive) {
		c.endStream(s.id.ID, Success)
		return
	}
	if s.passive {
		c.beginMerge(s)
		return
	}
	s.phase = "receive"
	s.deadline = c.now.Add(c.config.StreamTimeout)
	if p := c.probes[s.probe]; p != nil {
		s.deadline = min(s.deadline, p.deadline)
	}
	c.arm(key("stream:", s.id.ID), "stream", s.deadline, s.id.ID, "")
	if s.early != nil {
		event := *s.early
		s.early = nil
		c.streamMessage(event)
	} else if s.closedPending {
		c.endStream(s.id.ID, s.closedFailure)
	}
}

// A read-side close cannot overtake the local write completion. Reliable user
// sends report local write acceptance, including when the peer closes first.
func (c *Core) closed(e Closed) {
	s := c.streams[e.ID.ID]
	if s == nil || s.id != e.ID {
		return
	}
	if s.phase == "write-request" || s.phase == "write-response" || s.phase == "write-reply" {
		s.closedPending = true
		s.closedFailure = e.Failure
		return
	}
	c.endStream(e.ID.ID, e.Failure)
}
func (c *Core) streamMessage(e StreamMessage) {
	s := c.streams[e.ID.ID]
	if s == nil || s.id != e.ID {
		return
	}
	if len(e.Messages) != 1 {
		c.endStream(s.id.ID, RemoteFailure)
		return
	}
	if s.phase == "write-request" || s.phase == "local-state" && !s.passive {
		if s.early != nil {
			c.endStream(s.id.ID, RemoteFailure)
			return
		}
		copied := e
		copied.Messages = []wire.Message{e.Messages[0].Clone()}
		s.early = &copied
		return
	}
	if s.phase != "receive" {
		c.endStream(s.id.ID, RemoteFailure)
		return
	}
	m := e.Messages[0].Clone()
	if !s.passive {
		if s.purpose == "ping" && m.Ack != nil && m.Ack.Sequence == s.probe {
			if p := c.probes[s.probe]; p != nil {
				p.tcp = true
			}
			c.endStream(s.id.ID, Success)
			return
		}
		if s.purpose == "exchange" && m.Exchange != nil {
			s.remote = m.Exchange
			c.beginMerge(s)
			return
		}
		c.endStream(s.id.ID, RemoteFailure)
		return
	}
	switch {
	case m.Ping != nil:
		if m.Ping.Target != c.self {
			c.endStream(s.id.ID, Rejected)
			return
		}
		s.purpose = "ping"
		s.phase = "ack-payload"
		if c.config.Application {
			c.request(pending{kind: AckPayload, op: s.id.ID, sequence: m.Ping.Sequence}, Request{Kind: AckPayload})
		} else {
			c.write(s, wire.Message{Ack: &wire.Ack{Sequence: m.Ping.Sequence}}, "reply")
		}
	case m.User != nil:
		c.emit(Notification{Kind: "user", Data: slices.Clone(*m.User)})
		c.endStream(s.id.ID, Success)
	case m.Exchange != nil:
		if c.exchanges() >= c.config.MaxPushPull {
			c.endStream(s.id.ID, Rejected)
			return
		}
		s.purpose = "exchange"
		s.remote = m.Exchange
		s.join = m.Exchange.Join
		c.prepareExchange(s)
	default:
		c.endStream(s.id.ID, RemoteFailure)
	}
}
func (c *Core) beginMerge(s *stream) {
	if s.remote == nil {
		c.endStream(s.id.ID, RemoteFailure)
		return
	}
	for _, n := range s.remote.Members {
		if n.State == wire.Alive && (n.Application != c.nodes[c.self].Application || !c.fitsAlive(n)) {
			c.endStream(s.id.ID, Rejected)
			return
		}
	}
	if s.join && c.config.Application {
		s.phase = "approve-merge"
		members := make([]wire.Member, len(s.remote.Members))
		copy(members, s.remote.Members)
		c.request(pending{kind: ApproveMerge, op: s.id.ID}, Request{Kind: ApproveMerge, Members: members, Join: true})
		return
	}
	s.phase = "merge"
	c.mergeNext(s)
}
func (c *Core) mergeNext(s *stream) {
	for s.cursor < len(s.remote.Members) {
		n := s.remote.Members[s.cursor]
		switch n.State {
		case wire.Alive:
			if c.config.Application {
				s.phase = "approve-alive"
				c.alive(n, s.id.ID, false)
				return
			}
			c.alive(n, 0, true)
		case wire.Left:
			c.dead(wire.Accusation{Node: n.Name, From: n.Name, Incarnation: n.Incarnation})
		case wire.Dead, wire.Suspect:
			c.suspect(wire.Accusation{Node: n.Name, From: c.self, Incarnation: n.Incarnation})
		}
		s.cursor++
	}
	if s.remote.Data != nil {
		c.emit(Notification{Kind: "merge-state", Data: slices.Clone(s.remote.Data), Join: s.join})
	}
	c.endStream(s.id.ID, Success)
}
func (c *Core) endStream(id uint64, f Failure) {
	s := c.streams[id]
	if s == nil {
		return
	}
	delete(c.streams, id)
	c.cancel(key("stream:", id))
	c.cancelOperationApps(id)
	c.emit(CloseStream{ID: s.id})
	if s.command != 0 {
		c.emit(Result{Command: s.command, Failure: f})
	}
	if s.periodic {
		c.schedulePush()
	}
}
