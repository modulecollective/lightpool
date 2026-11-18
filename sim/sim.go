// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

// Package sim drives the same core and codecs without sockets, goroutines,
// channels, sleeps or ambient time. It models messages, not a TCP packet stack.
package sim

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
	"math/rand/v2"
	"runtime"
	"slices"
	"time"
)

type Seeds struct{ Protocol, Fault, Schedule uint64 }
type Node struct {
	Identity wire.Identity
	Service  time.Duration
}
type Action struct {
	At               core.Instant
	Node, Kind, Peer string
	Data             []byte
	Duration         time.Duration
}
type Fault struct {
	From, To      string
	Start, End    core.Instant
	Drop          bool
	Delay, Jitter time.Duration
	Duplicate     bool
	StreamFailure bool
}
type Fixture struct {
	Kind   core.RequestKind
	Delay  time.Duration
	Reject bool
	Data   []byte
}
type Scenario struct {
	Code                             string
	Protocol                         core.Config
	Nodes                            []Node
	Actions                          []Action
	Faults                           []Fault
	Fixtures                         []Fixture
	Seeds                            Seeds
	Horizon                          core.Instant
	EventBudget                      int
	DatagramCapacity, StreamCapacity int
	Latency                          time.Duration
	Compression                      bool
}
type NodeSeed struct {
	Name string
	Seed core.Seed
}
type Manifest struct {
	Code, Toolchain, Generator string
	Scenario                   Scenario
	NodeSeeds                  []NodeSeed
	PreparedFaults             []Fault
}
type Trace struct {
	Order, ID        uint64
	Time             core.Instant
	Node, Kind, Hash string
	Effects          []json.RawMessage
}
type NodeMeasurement struct {
	Name                                                string
	Snapshot                                            core.Snapshot
	SentBytes, ReceivedBytes, DatagramDrops, Detections uint64
}
type Result struct {
	Manifest     Manifest
	Trace        []Trace
	Measurements []NodeMeasurement
	Complete     bool
	Reason       string
	Events       int
}

func (r Result) TraceBytes() []byte       { b, _ := json.Marshal(r.Trace); return b }
func (r Result) MeasurementBytes() []byte { b, _ := json.Marshal(r.Measurements); return b }

type machine struct {
	spec                              Node
	engine                            *core.Core
	life                              uint64
	alive                             bool
	paused, busy                      core.Instant
	buffered                          int
	sent, received, drops, detections uint64
}
type event struct {
	at         core.Instant
	id         uint64
	node, kind string
	life       uint64
	input      core.Event
	action     Action
	frame      []byte
	from       string
	stream     uint64
	direction  int
}
type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].id < h[j].id
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(v any)   { *h = append(*h, v.(*event)) }
func (h *eventHeap) Pop() any     { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }

type direction struct {
	frame      []byte
	offset     int
	assembled  []byte
	received   core.Instant
	writing    bool
	generation uint64
	delivered  bool
}
type connection struct {
	id           uint64
	names        [2]string
	tokens       [2]core.Token
	directions   [2]direction
	closed       bool
	closedSide   [2]bool
	accepted     bool
	waitingDrain *event
}
type runner struct {
	scenario       Scenario
	manifest       Manifest
	now            core.Instant
	next, streamID uint64
	nodes          map[string]*machine
	addresses      map[netipKey]string
	order          []string
	heap           eventHeap
	schedule       *rand.Rand
	faults         []Fault
	streams        map[uint64]*connection
	tokens         map[string]map[core.Token]uint64
	trace          []Trace
}

// netipKey stores the canonical endpoint text; no DNS exists in this model.
type netipKey string

func Run(s Scenario) (Result, error) {
	if err := validate(s); err != nil {
		return Result{}, err
	}
	// Freeze caller-owned scenario buffers, including nested protocol settings.
	b, err := json.Marshal(s)
	if err != nil {
		return Result{}, err
	}
	if err = json.Unmarshal(b, &s); err != nil {
		return Result{}, err
	}
	r := &runner{scenario: s, nodes: make(map[string]*machine), addresses: make(map[netipKey]string), schedule: rand.New(rand.NewPCG(s.Seeds.Schedule, 0x7363686564756c65)), streams: make(map[uint64]*connection), tokens: make(map[string]map[core.Token]uint64)}
	r.manifest = Manifest{Code: s.Code, Toolchain: runtime.Version(), Generator: "math/rand/v2.PCG", Scenario: s}
	faultRNG := rand.New(rand.NewPCG(s.Seeds.Fault, 0x6661756c74))
	for _, f := range s.Faults {
		if f.Jitter > 0 {
			f.Delay += time.Duration(faultRNG.Int64N(int64(f.Jitter) + 1))
		}
		f.Jitter = 0
		r.faults = append(r.faults, f)
	}
	r.manifest.PreparedFaults = slices.Clone(r.faults)
	nodes := slices.Clone(s.Nodes)
	slices.SortFunc(nodes, func(a, b Node) int { return stringsCompare(a.Identity.Name, b.Identity.Name) })
	for _, n := range nodes {
		name := n.Identity.Name
		r.order = append(r.order, name)
		r.nodes[name] = &machine{spec: n}
		r.addresses[netipKey(n.Identity.Address.String())] = name
		r.tokens[name] = make(map[core.Token]uint64)
		seed := nodeSeed(s.Seeds.Protocol, name, 1)
		r.manifest.NodeSeeds = append(r.manifest.NodeSeeds, NodeSeed{name, seed})
		r.enqueue(&event{at: 0, kind: "action", action: Action{Kind: "start", Node: name}})
	}
	for _, a := range s.Actions {
		r.enqueue(&event{at: a.At, kind: "action", action: a})
	}
	result := Result{Manifest: r.manifest, Complete: true, Reason: "horizon"}
	for len(r.heap) > 0 {
		if result.Events >= s.EventBudget {
			result.Complete = false
			result.Reason = "event budget exhausted"
			break
		}
		e := r.pop()
		if e.at > s.Horizon {
			break
		}
		r.now = e.at
		if e.node != "" {
			m := r.nodes[e.node]
			if !m.alive || e.life != m.life {
				continue
			}
			eligible := max(m.paused, m.busy)
			if eligible > r.now {
				e.at = eligible
				heap.Push(&r.heap, e)
				continue
			}
			m.busy = r.now.Add(m.spec.Service)
		}
		result.Events++
		trace := Trace{Order: uint64(result.Events), ID: e.id, Time: r.now, Node: e.node, Kind: e.kind}
		var effects []core.Effect
		switch e.kind {
		case "action":
			trace.Node = e.action.Node
			trace.Kind = "action:" + e.action.Kind
			effects, err = r.action(e.action)
		case "core":
			m := r.nodes[e.node]
			trace.Kind = fmt.Sprintf("%T", e.input)
			if d, ok := e.input.(core.Datagram); ok {
				m.buffered -= len(e.frame)
				trace.Hash = hash(e.frame)
				e.input = d
			}
			effects, err = m.engine.Step(r.now, e.input)
			if a, ok := e.input.(core.Accepted); ok {
				if conn := r.streams[r.tokens[e.node][a.ID]]; conn != nil {
					conn.accepted = true
					if pending := conn.waitingDrain; pending != nil {
						pending.at = r.now
						r.enqueue(pending)
						conn.waitingDrain = nil
					}
				}
			}
		case "datagram":
			r.datagramArrival(e)
			trace.Hash = hash(e.frame)
		case "chunk":
			r.chunkArrival(e)
			trace.Hash = hash(e.frame)
		case "drain":
			r.drain(e)
		default:
			err = errors.New("unknown simulation event")
		}
		if err != nil {
			return Result{}, fmt.Errorf("%s at %d: %w", trace.Kind, r.now, err)
		}
		if e.kind == "core" {
			data, _ := json.Marshal(e.input)
			trace.Hash = hash(data)
		}
		for _, effect := range effects {
			raw, _ := json.Marshal(effect)
			tag, _ := json.Marshal(struct {
				Kind   string
				Effect json.RawMessage
			}{fmt.Sprintf("%T", effect), raw})
			trace.Effects = append(trace.Effects, tag)
			r.effect(trace.Node, effect)
		}
		r.trace = append(r.trace, trace)
	}
	result.Trace = r.trace
	for _, name := range r.order {
		m := r.nodes[name]
		snapshot := core.Snapshot{}
		if m.engine != nil {
			snapshot = m.engine.Snapshot()
		}
		result.Measurements = append(result.Measurements, NodeMeasurement{name, snapshot, m.sent, m.received, m.drops, m.detections})
	}
	return result, nil
}
func stringsCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func validate(s Scenario) error {
	if s.Code == "" || s.Horizon < 0 || s.EventBudget <= 0 || s.DatagramCapacity <= 0 || s.StreamCapacity <= 0 || s.Latency < 0 || len(s.Nodes) == 0 {
		return errors.New("invalid scenario bounds or code revision")
	}
	if err := s.Protocol.Validate(); err != nil {
		return err
	}
	if s.Protocol.Protected {
		return errors.New("encryption is unsupported in simulation")
	}
	if s.Protocol.Application && len(s.Fixtures) == 0 {
		return errors.New("application scenarios require deterministic fixtures")
	}
	names := make(map[string]bool)
	addresses := make(map[netipKey]bool)
	for _, n := range s.Nodes {
		if n.Identity.Validate() != nil || n.Service < 0 || names[n.Identity.Name] || addresses[netipKey(n.Identity.Address.String())] {
			return errors.New("invalid or duplicate node")
		}
		names[n.Identity.Name] = true
		addresses[netipKey(n.Identity.Address.String())] = true
	}
	for _, a := range s.Actions {
		if !names[a.Node] || a.At < 0 || a.Duration < 0 || len(a.Data) > wire.MaxData {
			return errors.New("invalid scenario action")
		}
		switch a.Kind {
		case "join", "ping", "probe", "send", "reliable":
			if !names[a.Peer] {
				return errors.New("unknown action peer")
			}
		case "pause", "crash", "start", "leave", "update", "shutdown", "broadcast":
		default:
			return errors.New("unsupported scenario action")
		}
	}
	for _, f := range s.Faults {
		if !names[f.From] || !names[f.To] || f.Start < 0 || f.End <= f.Start || f.Delay < 0 || f.Jitter < 0 {
			return errors.New("invalid directional fault")
		}
	}
	for _, f := range s.Fixtures {
		if f.Delay < 0 || len(f.Data) > wire.MaxData {
			return errors.New("invalid application fixture")
		}
	}
	return nil
}
func nodeSeed(seed uint64, name string, life uint64) core.Seed {
	b := binary.LittleEndian.AppendUint64(nil, seed)
	b = append(b, []byte(name)...)
	if life > 1 {
		b = binary.LittleEndian.AppendUint64(b, life)
	}
	sum := sha256.Sum256(b)
	return core.Seed{State: binary.LittleEndian.Uint64(sum[:8]), Stream: binary.LittleEndian.Uint64(sum[8:16]), Lifetime: life}
}
func hash(b []byte) string         { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func (r *runner) enqueue(e *event) { r.next++; e.id = r.next; heap.Push(&r.heap, e) }
func (r *runner) pop() *event {
	first := heap.Pop(&r.heap).(*event)
	same := []*event{first}
	for len(r.heap) > 0 && r.heap[0].at == first.at {
		same = append(same, heap.Pop(&r.heap).(*event))
	}
	// Heap order already makes the eligible set stable by creation ID.
	chosen := r.schedule.IntN(len(same))
	out := same[chosen]
	for j, e := range same {
		if j != chosen {
			heap.Push(&r.heap, e)
		}
	}
	return out
}
func (r *runner) coreEvent(name string, at core.Instant, input core.Event) {
	m := r.nodes[name]
	if m != nil && m.alive {
		r.enqueue(&event{at: at, node: name, life: m.life, kind: "core", input: input})
	}
}
func (r *runner) action(a Action) ([]core.Effect, error) {
	m := r.nodes[a.Node]
	switch a.Kind {
	case "start":
		if m.alive {
			return nil, errors.New("start of running node")
		}
		m.life++
		m.alive = true
		m.paused = 0
		m.busy = 0
		m.buffered = 0
		engine, err := core.New(r.scenario.Protocol, m.spec.Identity, nodeSeed(r.scenario.Seeds.Protocol, a.Node, m.life))
		if err != nil {
			return nil, err
		}
		m.engine = engine
		r.coreEvent(a.Node, r.now, core.Start{})
		return nil, nil
	case "crash":
		m.alive = false
		m.buffered = 0
		ids := make([]uint64, 0, len(r.streams))
		for id := range r.streams {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			conn := r.streams[id]
			if conn.names[0] == a.Node || conn.names[1] == a.Node {
				r.close(conn, core.RemoteFailure)
			}
		}
		return nil, nil
	case "pause":
		m.paused = max(m.paused, r.now.Add(a.Duration))
		return nil, nil
	}
	if !m.alive {
		return nil, nil
	}
	command := r.next + 1
	var input core.Event
	peer := r.nodes[a.Peer]
	var identity wire.Identity
	if peer != nil {
		identity = peer.spec.Identity.Clone()
	}
	switch a.Kind {
	case "join":
		input = core.Join{Command: command, Peer: identity}
	case "ping":
		input = core.Ping{Command: command, Peer: identity}
	case "probe":
		input = core.Probe{Peer: a.Peer}
	case "leave":
		input = core.Leave{Command: command}
	case "update":
		input = core.Update{Command: command, Meta: a.Data}
	case "send", "reliable", "broadcast":
		input = core.Send{Command: command, Peer: identity, Data: a.Data, Reliable: a.Kind == "reliable", Broadcast: a.Kind == "broadcast"}
	case "shutdown":
		input = core.Shutdown{Command: command}
	}
	r.coreEvent(a.Node, r.now, input)
	return nil, nil
}
func (r *runner) fault(from, to string, at core.Instant) Fault {
	result := Fault{}
	for _, f := range r.faults {
		if f.From == from && f.To == to && at >= f.Start && at < f.End {
			result.Drop = result.Drop || f.Drop
			result.StreamFailure = result.StreamFailure || f.StreamFailure
			result.Duplicate = result.Duplicate || f.Duplicate
			result.Delay += f.Delay
		}
	}
	return result
}
func (r *runner) effect(name string, e core.Effect) {
	m := r.nodes[name]
	switch e := e.(type) {
	case core.SendDatagram:
		b, err := wire.Encode(e.Messages, wire.Options{Label: r.scenario.Protocol.Label, Compress: r.scenario.Compression})
		if err != nil || len(b) > r.scenario.Protocol.PacketBudget {
			r.coreEvent(name, r.now, core.Sent{ID: e.ID, Failure: core.LocalFailure})
			return
		}
		m.sent += uint64(len(b))
		r.coreEvent(name, r.now, core.Sent{ID: e.ID})
		to := r.addresses[netipKey(e.To.String())]
		if to == "" {
			return
		}
		f := r.fault(name, to, r.now)
		if f.Drop {
			m.drops++
			return
		}
		copies := 1
		if f.Duplicate {
			copies++
		}
		for j := 0; j < copies; j++ {
			r.enqueue(&event{at: r.now.Add(r.scenario.Latency + f.Delay + time.Duration(j)), kind: "datagram", from: name, frame: b, action: Action{Node: to}})
		}
	case core.Arm:
		r.coreEvent(name, e.At, core.Fired{Timer: e.Timer})
	case core.Cancel: // Queued generations deliberately remain to test cancellation.
	case core.Defer:
		r.coreEvent(name, r.now, core.Work{ID: e.ID})
	case core.OpenStream:
		r.open(name, e)
	case core.WriteStream:
		r.write(name, e)
	case core.CloseStream:
		if id := r.tokens[name][e.ID]; id != 0 {
			r.closeSide(r.streams[id], name)
		}
	case core.Request:
		fixture := Fixture{Kind: e.Kind}
		for _, f := range r.scenario.Fixtures {
			if f.Kind == e.Kind {
				fixture = f
				break
			}
		}
		r.coreEvent(name, r.now.Add(fixture.Delay), core.Response{ID: e.ID, Reject: fixture.Reject, Data: slices.Clone(fixture.Data)})
	case core.Notification:
		if e.Kind == "leave" && e.Member.State == wire.Dead {
			m.detections++
		}
	case core.Observation, core.Result:
	}
}
func (r *runner) datagramArrival(e *event) {
	to := e.action.Node
	m := r.nodes[to]
	if !m.alive {
		return
	}
	m.received += uint64(len(e.frame))
	if m.buffered+len(e.frame) > r.scenario.DatagramCapacity {
		m.drops++
		return
	}
	messages, err := wire.Decode(e.frame, wire.Options{Label: r.scenario.Protocol.Label})
	if err != nil {
		panic(err)
	}
	m.buffered += len(e.frame)
	r.enqueue(&event{at: r.now, node: to, life: m.life, kind: "core", input: core.Datagram{From: r.nodes[e.from].spec.Identity.Address, Received: r.now, Messages: messages}, frame: e.frame})
}
func (r *runner) open(name string, e core.OpenStream) {
	to := r.addresses[netipKey(e.To.String())]
	if to == "" || !r.nodes[to].alive || r.fault(name, to, r.now).StreamFailure {
		r.coreEvent(name, r.now.Add(r.scenario.Latency), core.Opened{ID: e.ID, Failure: core.RemoteFailure})
		return
	}
	r.streamID++
	id := r.streamID
	token := core.Token{Lifetime: r.nodes[to].life, ID: 1<<63 | id}
	conn := &connection{id: id, names: [2]string{name, to}, tokens: [2]core.Token{e.ID, token}}
	r.streams[id] = conn
	r.tokens[name][e.ID] = id
	r.tokens[to][token] = id
	at := r.now.Add(r.scenario.Latency + r.fault(name, to, r.now).Delay)
	// Open and accept are distinct events; delivery cannot precede accept.
	r.coreEvent(to, at, core.Accepted{ID: token, Peer: r.nodes[name].spec.Identity.Address})
	r.coreEvent(name, at, core.Opened{ID: e.ID})
}
func (r *runner) write(name string, e core.WriteStream) {
	id := r.tokens[name][e.ID]
	conn := r.streams[id]
	if conn == nil || conn.closed {
		r.coreEvent(name, r.now, core.Written{ID: e.ID, Failure: core.RemoteFailure})
		return
	}
	side := 0
	if conn.names[1] == name {
		side = 1
	}
	if conn.closedSide[1-side] {
		r.coreEvent(name, r.now, core.Written{ID: e.ID, Failure: core.RemoteFailure})
		return
	}
	d := &conn.directions[side]
	if d.writing {
		r.coreEvent(name, r.now, core.Written{ID: e.ID, Failure: core.LocalFailure})
		return
	}
	b, err := wire.Encode(e.Messages, wire.Options{Label: r.scenario.Protocol.Label, Compress: r.scenario.Compression})
	if err != nil {
		r.coreEvent(name, r.now, core.Written{ID: e.ID, Failure: core.LocalFailure})
		return
	}
	d.frame = b
	d.offset = 0
	d.assembled = nil
	d.writing = true
	d.generation++
	r.pump(conn, side)
}
func (r *runner) pump(conn *connection, side int) {
	if conn.closed {
		return
	}
	d := &conn.directions[side]
	from, to := conn.names[side], conn.names[1-side]
	if r.fault(from, to, r.now).StreamFailure {
		r.close(conn, core.RemoteFailure)
		return
	}
	count := min(r.scenario.StreamCapacity, len(d.frame)-d.offset)
	chunk := slices.Clone(d.frame[d.offset : d.offset+count])
	d.offset += count
	r.nodes[from].sent += uint64(count)
	r.enqueue(&event{at: r.now.Add(r.scenario.Latency + r.fault(from, to, r.now).Delay), kind: "chunk", stream: conn.id, direction: side, life: d.generation, frame: chunk})
}
func (r *runner) chunkArrival(e *event) {
	conn := r.streams[e.stream]
	if conn == nil || conn.closed {
		return
	}
	side := e.direction
	d := &conn.directions[side]
	if d.generation != e.life {
		return
	}
	to := conn.names[1-side]
	m := r.nodes[to]
	if !m.alive || r.fault(conn.names[side], to, r.now).StreamFailure {
		r.close(conn, core.RemoteFailure)
		return
	}
	m.received += uint64(len(e.frame))
	d.received = r.now
	if d.offset == len(d.frame) {
		r.coreEvent(conn.names[side], r.now, core.Written{ID: conn.tokens[side]})
	}
	pending := &event{at: r.now, node: to, life: m.life, kind: "drain", stream: conn.id, direction: side, frame: e.frame}
	if side == 0 && !conn.accepted {
		conn.waitingDrain = pending
	} else {
		r.enqueue(pending)
	}
}
func (r *runner) drain(e *event) {
	conn := r.streams[e.stream]
	if conn == nil || conn.closed {
		return
	}
	d := &conn.directions[e.direction]
	d.assembled = append(d.assembled, e.frame...)
	if len(d.assembled) > wire.MaxFrame {
		panic("unbounded stream reassembly")
	}
	if len(d.assembled) < len(d.frame) {
		r.pump(conn, e.direction)
		return
	}
	d.writing = false
	d.delivered = true
	messages, err := wire.Decode(d.assembled, wire.Options{Label: r.scenario.Protocol.Label})
	if err != nil {
		panic(err)
	}
	to := conn.names[1-e.direction]
	r.coreEvent(to, r.now, core.StreamMessage{ID: conn.tokens[1-e.direction], Received: d.received, Messages: messages})
	d.assembled = nil
	d.frame = nil
}
func (r *runner) close(conn *connection, f core.Failure) {
	if conn == nil || conn.closed {
		return
	}
	conn.closed = true
	for side, name := range conn.names {
		if conn.directions[side].writing {
			r.coreEvent(name, r.now, core.Written{ID: conn.tokens[side], Failure: f})
		}
		delete(r.tokens[name], conn.tokens[side])
		r.coreEvent(name, r.now, core.Closed{ID: conn.tokens[side], Failure: f})
	}
	delete(r.streams, conn.id)
}

// A successful local write can be closed while bytes still await the receiver.
// Keep those bytes and buffer credits alive until the receiver drains them.
func (r *runner) closeSide(conn *connection, name string) {
	if conn == nil || conn.closed {
		return
	}
	side := 0
	if conn.names[1] == name {
		side = 1
	}
	conn.closedSide[side] = true
	d := &conn.directions[side]
	if d.writing && d.offset < len(d.frame) {
		r.close(conn, core.RemoteFailure)
		return
	}
	if !d.writing && !d.delivered {
		r.coreEvent(conn.names[1-side], r.now, core.Closed{ID: conn.tokens[1-side], Failure: core.RemoteFailure})
	}
	if conn.closedSide[0] && conn.closedSide[1] && !conn.directions[0].writing && !conn.directions[1].writing {
		for j, node := range conn.names {
			delete(r.tokens[node], conn.tokens[j])
		}
		delete(r.streams, conn.id)
	}
}
