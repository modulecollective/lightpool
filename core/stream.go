// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"net/netip"

	"github.com/modulecollective/lightpool/wire"
)

type stream struct {
	id                Token
	peer              netip.AddrPort
	command, probe    uint64
	purpose, phase    string
	periodic, passive bool
	outgoing          wire.Message
	remote            *wire.Exchange
	early             *StreamMessage
	deadline          Instant
	closedFailure     Failure
	closedPending     bool
}

func (c *Core) beginStream(peer wire.Identity, command uint64, purpose string) *stream {
	s := &stream{id: c.token(), peer: peer.Address, command: command, purpose: purpose, phase: "connect", deadline: c.now.Add(c.config.StreamTimeout)}
	c.streams[s.id.ID] = s
	c.arm(key("stream:", s.id.ID), "stream", s.deadline, s.id.ID, "")
	return s
}

func (c *Core) openExchange(peer wire.Identity, command uint64, periodic bool) {
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
		c.writeExchange(s)
	} else {
		c.write(s, s.outgoing, "request")
	}
}

func (c *Core) writeExchange(s *stream) {
	// Each side sends its membership before merging the peer's, as memberlist does.
	m := wire.Message{Exchange: &wire.Exchange{Members: c.Snapshot().Members}}
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
	if s.phase == "write-reply" {
		c.endStream(s.id.ID, Success)
		return
	}
	if s.passive {
		c.merge(s)
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

// A read-side close cannot overtake the local write completion.
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
	if s.phase == "write-request" {
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
			c.merge(s)
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
		c.write(s, wire.Message{Ack: &wire.Ack{Sequence: m.Ping.Sequence}}, "reply")
	case m.Exchange != nil:
		if c.exchanges() >= c.config.MaxPushPull {
			c.endStream(s.id.ID, Rejected)
			return
		}
		s.purpose = "exchange"
		s.remote = m.Exchange
		c.writeExchange(s)
	default:
		c.endStream(s.id.ID, RemoteFailure)
	}
}

func (c *Core) merge(s *stream) {
	if s.remote == nil {
		c.endStream(s.id.ID, RemoteFailure)
		return
	}
	for _, n := range s.remote.Members {
		if n.State == wire.Alive && !c.fitsAlive(n) {
			c.endStream(s.id.ID, Rejected)
			return
		}
	}
	for _, n := range s.remote.Members {
		switch n.State {
		case wire.Alive:
			c.alive(n)
		case wire.Left:
			c.dead(wire.Accusation{Node: n.Name, From: n.Name, Incarnation: n.Incarnation})
		case wire.Dead, wire.Suspect:
			// Exchange carries state, not the origin of an accusation.
			c.suspect(wire.Accusation{Node: n.Name, Incarnation: n.Incarnation})
		}
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
	c.emit(CloseStream{ID: s.id})
	if s.command != 0 {
		c.emit(Result{Command: s.command, Failure: f})
	}
	if s.periodic {
		c.schedulePush()
	}
}
