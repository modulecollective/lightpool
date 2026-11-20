package sim

import (
	"io"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim/trace"
	"github.com/modulecollective/lightpool/wire"
)

type Progress struct {
	Time   core.Instant
	Events int
	Bytes  uint64
}
type ExportOptions struct {
	// Progress is called after flushing, at most once per second of wall time.
	Progress func(Progress)
}
type exporter struct {
	writer  *trace.Writer
	options ExportOptions
	last    time.Time
	event   trace.Event
	time    core.Instant
	next    uint64
}

// Export runs the same event loop as Run, writing incrementally without retaining
// the legacy semantic trace or final membership snapshots. The caller owns w.
func Export(s Scenario, w io.Writer, options ExportOptions) (Result, error) {
	x := &exporter{writer: trace.NewWriter(w), options: options, last: time.Now()}
	result, err := execute(s, x)
	if err != nil {
		return result, err
	}
	if err = x.writer.Flush(); err != nil {
		return result, err
	}
	if options.Progress != nil {
		options.Progress(Progress{Events: result.Events, Time: core.Instant(x.time), Bytes: x.writer.Bytes})
	}
	return result, nil
}
func (r *runner) exportHeader() error {
	if r.export == nil {
		return nil
	}
	manifest, err := trace.Normalize(r.manifest)
	if err != nil {
		return err
	}
	h := trace.Header{Type: "header", Format: "lightpool-trace", Version: 1, Manifest: manifest, Nodes: []trace.Node{}, Initial: []trace.Process{}}
	for _, n := range r.order {
		h.Nodes = append(h.Nodes, trace.Node{Name: n, Address: r.nodes[n].spec.Identity.Address.String()})
		h.Initial = append(h.Initial, r.process(n))
	}
	if err = r.export.writer.Write(h); err != nil {
		return err
	}
	return r.export.writer.Flush()
}
func (r *runner) process(name string) trace.Process {
	m := r.nodes[name]
	return trace.Process{Node: name, Lifetime: trace.Decimal(m.life), Running: m.alive, PausedUntil: trace.Decimal(m.paused)}
}
func (r *runner) beginExport(e *event, seq int) {
	if r.export == nil {
		return
	}
	actor := e.node
	kind := e.kind
	if e.kind == "action" {
		actor = e.action.Node
		kind = "action." + e.action.Kind
	}
	if e.kind == "core" {
		kind = coreKind(e.input)
	}
	life := uint64(0)
	if actor != "" {
		life = r.nodes[actor].life
	}
	r.export.time = r.now
	r.export.event = trace.Event{Type: "event", Seq: trace.Decimal(seq), Time: trace.Decimal(r.now), ID: trace.Decimal(e.id), Kind: kind, Actor: actor, Lifetime: trace.Decimal(life), Changes: []trace.Change{}, Traffic: []trace.Traffic{}}
	if e.kind == "action" {
		r.export.event.Peer = e.action.Peer
	}
	if e.kind == "core" && e.traffic != nil {
		t := *e.traffic
		t.Stage = "handle"
		r.traffic(t)
	}
}
func (r *runner) finishExport(e *event, events int) error {
	if r.export == nil {
		return nil
	}
	x := r.export
	if e.kind == "action" && (e.action.Kind == "start" || e.action.Kind == "crash" || e.action.Kind == "pause") {
		p := r.process(e.action.Node)
		x.event.Lifetime = p.Lifetime
		x.event.Changes = append(x.event.Changes, trace.Change{Kind: "process", Node: e.action.Node, Phase: e.action.Kind, Process: &p})
	}
	if e.kind == "core" || e.kind == "action" && e.action.Kind == "start" {
		name := e.node
		if name == "" {
			name = e.action.Node
		}
		m := r.nodes[name]
		for _, j := range m.engine.Inspection() {
			c := trace.Change{Kind: j.Kind, Node: name, Subject: j.Subject, Phase: j.Phase, Purpose: j.Purpose, Outcome: j.Outcome, Helpers: j.Helpers}
			if j.Member != nil {
				c.Member = trace.Membership(*j.Member)
			}
			if j.Kind == "probe" || j.Kind == "timer" {
				c.ID = trace.Decimal(j.ID)
			}
			if j.Kind == "timer" {
				c.Deadline = trace.Decimal(j.At)
				c.Generation = trace.Decimal(j.Generation)
				c.Operation = trace.Decimal(j.Operation)
			}
			x.event.Changes = append(x.event.Changes, c)
		}
		x.event.Changes = append(x.event.Changes, trace.Change{Kind: "summary", Node: name, Summary: trace.Counters(m.engine.Summary())})
	}
	if err := x.writer.Write(x.event); err != nil {
		return err
	}
	if time.Since(x.last) >= time.Second {
		if err := x.writer.Flush(); err != nil {
			return err
		}
		x.last = time.Now()
		if x.options.Progress != nil {
			x.options.Progress(Progress{r.now, events, x.writer.Bytes})
		}
	}
	return nil
}
func (r *runner) exportEnd(result Result) error {
	if r.export == nil {
		return nil
	}
	// Discarded or deferred events can advance the scheduler past the last record.
	end := trace.End{Type: "end", Seq: trace.Decimal(result.Events), Time: trace.Decimal(r.export.time), Reason: result.Reason, Complete: result.Complete, Nodes: []trace.Measurement{}}
	var sent, received, drops, detections uint64
	for _, name := range r.order {
		m := r.nodes[name]
		v := trace.Measurement{Process: r.process(name), Totals: trace.Totals{SentBytes: trace.Decimal(m.sent), ReceivedBytes: trace.Decimal(m.received), DatagramDrops: trace.Decimal(m.drops), Detections: trace.Decimal(m.detections)}}
		if m.engine != nil {
			v.Summary = trace.Counters(m.engine.Summary())
		}
		end.Nodes = append(end.Nodes, v)
		sent += m.sent
		received += m.received
		drops += m.drops
		detections += m.detections
	}
	end.Totals = trace.Totals{SentBytes: trace.Decimal(sent), ReceivedBytes: trace.Decimal(received), DatagramDrops: trace.Decimal(drops), Detections: trace.Decimal(detections)}
	return r.export.writer.Write(end)
}
func (r *runner) traffic(t trace.Traffic) {
	if r.export != nil {
		r.export.event.Traffic = append(r.export.event.Traffic, t)
	}
}
func (r *runner) attempt(transport, from, to string, messages []wire.Message, bytes int) trace.Traffic {
	if r.export == nil {
		return trace.Traffic{}
	}
	r.export.next++
	t := trace.Traffic{ID: trace.Decimal(r.export.next), Transport: transport, Stage: "attempt", From: from, To: to, Bytes: trace.Decimal(bytes), Messages: messageKinds(messages)}
	r.traffic(t)
	return t
}
func messageKinds(ms []wire.Message) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		switch {
		case m.Ping != nil:
			out = append(out, "ping")
		case m.Indirect != nil:
			out = append(out, "indirect")
		case m.Ack != nil:
			out = append(out, "ack")
		case m.Nack != nil:
			out = append(out, "nack")
		case m.Alive != nil:
			out = append(out, "alive")
		case m.Suspect != nil:
			out = append(out, "suspect")
		case m.Dead != nil:
			out = append(out, "dead")
		case m.Exchange != nil:
			out = append(out, "exchange")
		}
	}
	return out
}
func (r *runner) trafficCore(name string, input core.Event, t trace.Traffic) {
	e := r.coreEvent(name, r.now, input)
	if r.export != nil && e != nil {
		e.traffic = &t
	}
}

func coreKind(input core.Event) string {
	switch input.(type) {
	case core.Start:
		return "core.start"
	case core.Shutdown:
		return "core.shutdown"
	case core.Join:
		return "core.join"
	case core.Ping:
		return "core.ping"
	case core.Probe:
		return "core.probe"
	case core.Update:
		return "core.update"
	case core.Leave:
		return "core.leave"
	case core.Datagram:
		return "core.datagram"
	case core.Sent:
		return "core.sent"
	case core.Fired:
		return "core.timer-delivery"
	case core.Work:
		return "core.work"
	case core.Accepted:
		return "core.stream-accepted"
	case core.Opened:
		return "core.stream-opened"
	case core.Written:
		return "core.stream-written"
	case core.StreamMessage:
		return "core.stream-message"
	case core.Closed:
		return "core.stream-closed"
	case core.CancelCommand:
		return "core.cancel-command"
	default:
		panic("unrecognized core event")
	}
}
