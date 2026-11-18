// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"github.com/modulecollective/lightpool/wire"
	"math/rand/v2"
	"slices"
	"strconv"
)

type member struct {
	wire.Member
	changed Instant
}
type deadline struct {
	timer Timer
	at    Instant
	kind  string
	op    uint64
	node  string
}
type handoff struct{ msg wire.Message }
type pending struct {
	kind     RequestKind
	op       uint64
	msg      wire.Message
	peer     wire.Identity
	sequence uint64
}
type Core struct {
	config                 Config
	self                   string
	now                    Instant
	started, stopped, left bool
	rng                    *rand.Rand
	life, next             uint64
	nodes                  map[string]*member
	susp                   map[string]*suspicion
	order                  []string
	probeIndex, health     int
	timers                 map[string]deadline
	timerKeys              map[uint64]string
	probes                 map[uint64]*probe
	activeProbe            uint64
	pendingProbe           bool
	streams                map[uint64]*stream
	app                    map[uint64]pending
	sends                  map[uint64]sendPending
	broadcasts             []*broadcast
	broadcastKeys          map[string]*broadcast
	high, low              []handoff
	work                   Token
	effects                []Effect
	stats                  Stats
}
type sendPending struct{ command, probe uint64 }

func New(config Config, identity wire.Identity, seed Seed) (*Core, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	config.DisableTCPFor = slices.Clone(config.DisableTCPFor)
	slices.Sort(config.DisableTCPFor)
	config.Allowed = slices.Clone(config.Allowed)
	c := &Core{
		config: config, self: identity.Name, life: seed.Lifetime,
		rng:    rand.New(rand.NewPCG(seed.State, seed.Stream)),
		nodes:  make(map[string]*member),
		timers: make(map[string]deadline), timerKeys: make(map[uint64]string),
		probes: make(map[uint64]*probe), streams: make(map[uint64]*stream),
		app: make(map[uint64]pending), sends: make(map[uint64]sendPending),
		broadcastKeys: make(map[string]*broadcast),
	}
	c.susp = make(map[string]*suspicion)
	c.next = c.rng.Uint64() >> 1
	c.nodes[c.self] = &member{Member: wire.Member{Identity: identity.Clone(), Incarnation: 1, State: wire.Alive}}
	c.order = []string{c.self}
	// Refuse configurations whose own largest routine control record cannot fit.
	n := c.nodes[c.self].Member
	size, err := wire.Size([]wire.Message{{Alive: &n}}, config.Label, config.Protected)
	if err != nil || size > config.PacketBudget {
		return nil, errors.New("local identity exceeds datagram budget")
	}
	return c, nil
}
func (c *Core) token() Token       { c.next++; return Token{c.life, c.next} }
func (c *Core) valid(t Token) bool { return t.Lifetime == c.life }
func (c *Core) emit(e Effect)      { c.effects = append(c.effects, e) }
func (c *Core) Step(now Instant, input Event) ([]Effect, error) {
	if now < c.now {
		return nil, errors.New("protocol time moved backwards")
	}
	c.now = now
	c.effects = nil
	c.stats.Steps++
	if c.stopped {
		if s, ok := input.(Shutdown); ok {
			c.emit(Result{Command: s.Command})
		}
		return c.effects, nil
	}
	switch e := input.(type) {
	case CancelCommand:
		c.cancelCommand(e.Command)
	case Start:
		if !c.started {
			c.started = true
			c.queue(wire.Message{Alive: ptrMember(c.nodes[c.self].Member)}, c.self, 0)
			c.startSchedules()
		}
	case Shutdown:
		c.shutdown(e.Command)
	case Join:
		if err := e.Peer.Validate(); err != nil {
			c.emit(Result{Command: e.Command, Failure: LocalFailure})
		} else {
			c.openExchange(e.Peer, e.Command, true, false)
		}
	case Ping:
		if e.Peer.Validate() != nil {
			c.emit(Result{Command: e.Command, Failure: LocalFailure})
		} else {
			c.startProbe(e.Peer, e.Command, true, false)
		}
	case Probe:
		if n := c.nodes[e.Peer]; n != nil && !c.left {
			c.startProbe(n.Identity, 0, false, false)
		}
	case Update:
		c.update(e)
	case Leave:
		c.leave(e.Command)
	case Send:
		c.send(e)
	case Datagram:
		if e.Received > now {
			return nil, errors.New("receive time exceeds processing time")
		}
		if !c.allowed(e.From.Addr()) {
			break
		}
		for _, m := range e.Messages {
			if err := m.Validate(); err != nil {
				return nil, err
			}
		}
		c.stats.Datagrams++
		c.stats.Messages += uint64(len(e.Messages))
		for _, m := range e.Messages {
			c.admit(m.Clone(), e.From, e.Received)
		}
	case Work:
		if e.ID == c.work && c.valid(e.ID) {
			c.work = Token{}
			c.workOne()
		}
	case Fired:
		c.fire(e.Timer)
	case Sent:
		if c.valid(e.ID) {
			p, ok := c.sends[e.ID.ID]
			if ok {
				delete(c.sends, e.ID.ID)
				if p.command != 0 {
					c.emit(Result{Command: p.command, Failure: e.Failure})
				}
				if p.probe != 0 && e.Failure != Success {
					c.sendFailed(p.probe, e.Failure)
				}
			}
		}
	case Accepted:
		if c.valid(e.ID) && c.allowed(e.Peer.Addr()) {
			c.accept(e)
		} else {
			c.emit(CloseStream{ID: e.ID})
		}
	case Opened:
		if c.valid(e.ID) {
			c.opened(e)
		}
	case Written:
		if c.valid(e.ID) {
			c.written(e)
		}
	case StreamMessage:
		if e.Received > now {
			return nil, errors.New("receive time exceeds processing time")
		}
		for _, m := range e.Messages {
			if err := m.Validate(); err != nil {
				return nil, err
			}
		}
		if c.valid(e.ID) {
			c.streamMessage(e)
		}
	case Closed:
		if c.valid(e.ID) {
			c.closed(e)
		}
	case Response:
		if c.valid(e.ID) {
			c.response(e)
		}
	default:
		return nil, errors.New("unknown event")
	}
	return c.effects, nil
}
func ptrMember(n wire.Member) *wire.Member { n = n.Clone(); return &n }
func (c *Core) Snapshot() Snapshot {
	s := Snapshot{Now: c.now, Health: c.health, Backlog: len(c.broadcasts), PendingStreams: len(c.streams), Stopped: c.stopped, Left: c.left, Stats: c.stats}
	for _, name := range c.names() {
		s.Members = append(s.Members, c.nodes[name].Member.Clone())
	}
	return s
}
func (c *Core) names() []string {
	names := make([]string, 0, len(c.nodes))
	for name := range c.nodes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
func (c *Core) arm(key, kind string, at Instant, op uint64, node string) {
	c.cancel(key)
	id := c.token()
	t := Timer{Token: id, Generation: id.ID}
	d := deadline{t, max(c.now, at), kind, op, node}
	c.timers[key] = d
	c.timerKeys[t.ID] = key
	c.emit(Arm{t, d.at})
}
func (c *Core) cancel(key string) {
	if d, ok := c.timers[key]; ok {
		delete(c.timers, key)
		delete(c.timerKeys, d.timer.ID)
		c.emit(Cancel{d.timer})
	}
}
func (c *Core) adjustHealth(delta int) {
	c.health = min(c.config.AwarenessMax-1, max(0, c.health+delta))
	c.emit(Observation{Kind: "health", Value: int64(c.health)})
}
func (c *Core) notify(kind string, n wire.Member) {
	c.emit(Notification{Kind: kind, Member: n.Clone()})
}
func (c *Core) request(p pending, r Request) {
	id := c.token()
	r.ID = id
	c.app[id.ID] = p
	r.Member = r.Member.Clone()
	for j := range r.Members {
		r.Members[j] = r.Members[j].Clone()
	}
	c.emit(r)
}
func (c *Core) shutdown(command uint64) {
	c.stopped = true
	c.pendingProbe = false
	keys := make([]string, 0, len(c.timers))
	for key := range c.timers {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		c.cancel(key)
	}
	for _, id := range sortedIDs(c.streams) {
		c.endStream(id, Canceled)
	}
	for _, id := range sortedIDs(c.probes) {
		c.finishProbe(id, Canceled, 0, nil)
	}
	for _, b := range c.broadcasts {
		c.finishBroadcast(b, Canceled)
	}
	for _, id := range sortedIDs(c.sends) {
		if p := c.sends[id]; p.command != 0 {
			c.emit(Result{Command: p.command, Failure: Canceled})
		}
	}
	c.stopped = true
	for _, id := range sortedIDs(c.app) {
		c.emit(CancelRequest{ID: Token{Lifetime: c.life, ID: id}})
		delete(c.app, id)
	}
	c.sends = make(map[uint64]sendPending)
	c.high = nil
	c.low = nil
	c.broadcasts = nil
	c.broadcastKeys = make(map[string]*broadcast)
	c.work = Token{}
	c.emit(Result{Command: command})
}
func sortedIDs[V any](m map[uint64]V) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
func key(prefix string, id uint64) string { return prefix + strconv.FormatUint(id, 10) }
func (c *Core) cancelOperationApps(op uint64) {
	for _, id := range sortedIDs(c.app) {
		if c.app[id].op == op {
			c.emit(CancelRequest{ID: Token{Lifetime: c.life, ID: id}})
			delete(c.app, id)
		}
	}
}
func (c *Core) tcpDisabled(name string) bool {
	_, found := slices.BinarySearch(c.config.DisableTCPFor, name)
	return c.config.DisableTCP || found
}
func (c *Core) nextDeferred() {
	if c.work == (Token{}) && len(c.high)+len(c.low) > 0 {
		c.work = c.token()
		c.emit(Defer{c.work})
	}
}
func (c *Core) workOne() {
	q := &c.high
	if len(*q) == 0 {
		q = &c.low
	}
	if len(*q) == 0 {
		return
	}
	m := (*q)[len(*q)-1].msg
	*q = (*q)[:len(*q)-1]
	switch {
	case m.Alive != nil:
		c.alive(*m.Alive, 0, false)
	case m.Suspect != nil:
		c.suspect(*m.Suspect)
	case m.Dead != nil:
		c.dead(*m.Dead)
	case m.User != nil:
		c.emit(Notification{Kind: "user", Data: append([]byte(nil), (*m.User)...)})
	}
	c.nextDeferred()
}
func (c *Core) response(e Response) {
	p, ok := c.app[e.ID.ID]
	if !ok {
		return
	}
	delete(c.app, e.ID.ID)
	switch p.kind {
	case ApproveAlive:
		if !e.Reject {
			c.applyAlive(*p.msg.Alive)
		}
		if p.op != 0 {
			if s := c.streams[p.op]; s != nil {
				s.cursor++
				c.mergeNext(s)
			}
		}
	case ApproveMerge:
		if e.Reject {
			c.endStream(p.op, Rejected)
		} else if s := c.streams[p.op]; s != nil {
			s.phase = "merge"
			c.mergeNext(s)
		}
	case LocalState:
		if s := c.streams[p.op]; s != nil {
			if e.Reject || len(e.Data) > wire.MaxData {
				c.endStream(p.op, LocalFailure)
			} else {
				c.writeExchange(s, e.Data)
			}
		}
	case AckPayload:
		if e.Reject || len(e.Data) > wire.MaxMeta {
			return
		}
		m := wire.Message{Ack: &wire.Ack{Sequence: p.sequence, Payload: append([]byte(nil), e.Data...)}}
		if p.op != 0 {
			if s := c.streams[p.op]; s != nil {
				c.write(s, m, "reply")
			}
		} else {
			c.datagram(p.peer.Address, []wire.Message{m}, 0, 0, true)
		}
	}
}

func (c *Core) cancelCommand(command uint64) {
	if command == 0 {
		return
	}
	for _, id := range sortedIDs(c.probes) {
		if c.probes[id].command == command {
			c.finishProbe(id, Canceled, 0, nil)
		}
	}
	for _, id := range sortedIDs(c.streams) {
		if c.streams[id].command == command {
			c.endStream(id, Canceled)
		}
	}
	for _, b := range c.broadcasts {
		if b.command == command {
			c.finishBroadcast(b, Canceled)
		}
	}
	for _, id := range sortedIDs(c.sends) {
		if c.sends[id].command == command {
			delete(c.sends, id)
			c.emit(Result{Command: command, Failure: Canceled})
		}
	}
}
