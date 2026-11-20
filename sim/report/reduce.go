package report

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/modulecollective/lightpool/sim/trace"
)

func (x *reducer) event(e trace.Event) {
	at := ref(e)
	x.report.LastEvent = at
	if e.Kind == "action.join" {
		if e.Peer == "" {
			x.unsupported("Join peer absent in an older v1 trace; recovery rows can identify only joins initiated by the target.")
		}
		join := Join{at, e.Actor, e.Peer}
		for k, ids := range x.recovery {
			if k.subject == e.Actor || k.subject == e.Peer {
				for _, id := range ids {
					x.report.Recoveries[id].Joins = append(x.report.Recoveries[id].Joins, join)
				}
			}
		}
	}
	for _, c := range e.Changes {
		p := x.state[c.Node]
		switch c.Kind {
		case "process":
			if c.Phase == "crash" {
				x.crash(c.Node, at)
				x.interrupt(c.Node, at, "crash")
			}
			if c.Phase == "start" {
				x.interrupt(c.Node, at, "restart")
				p.members = map[string]string{}
				p.stopped, p.left = false, false
			}
			p.life, p.running = c.Process.Lifetime, c.Process.Running
			p.paused, _ = strconv.ParseInt(c.Process.PausedUntil, 10, 64)
		case "summary":
			if !p.stopped && c.Summary.Stopped {
				x.interrupt(c.Node, at, "shutdown")
			}
			if !p.left && c.Summary.Left {
				x.interrupt(c.Node, at, "leave")
			}
			p.stopped, p.left = c.Summary.Stopped, c.Summary.Left
		case "member.remove":
			delete(p.members, c.Subject)
		case "member.upsert":
			subject := c.Member.Name
			before := p.members[subject]
			p.members[subject] = c.Member.State
			if before != "dead" && c.Member.State == "dead" {
				x.declare(c.Node, subject, at)
			}
			if c.Member.State == "alive" {
				k := key{c.Node, subject}
				for _, id := range x.recovery[k] {
					r := &x.report.Recoveries[id]
					r.End, r.Status = at, "recovered"
					duration := at.TimeNS - r.Declaration.TimeNS
					r.DurationNS = &duration
				}
				delete(x.recovery, k)
			}
		}
	}
}

func (x *reducer) unsupported(message string) {
	if slices.Contains(x.report.Unsupported, message) {
		return
	}
	x.report.Unsupported = append(x.report.Unsupported, message)
}

func (x *reducer) pair(observer, subject string) Pair {
	return Pair{observer, x.state[observer].life, subject, x.state[subject].life}
}

func (x *reducer) declare(observer, subject string, at Reference) {
	p, target := x.state[observer], x.state[subject]
	d := Declaration{Pair: x.pair(observer, subject), Reference: at, TargetRunning: target.running, TargetStopped: target.stopped, TargetLeft: target.left, ObserverStopped: p.stopped, ObserverLeft: p.left, TargetPaused: target.running && target.paused > at.TimeNS, ObserverPaused: p.running && p.paused > at.TimeNS, Faults: []Fault{}}
	for _, f := range x.faults {
		start, _ := strconv.ParseInt(f.Start, 10, 64)
		end, _ := strconv.ParseInt(f.End, 10, 64)
		if at.TimeNS >= start && at.TimeNS < end && (f.From == observer || f.To == observer || f.From == subject || f.To == subject) {
			d.Faults = append(d.Faults, f)
		}
	}
	x.report.Declarations = append(x.report.Declarations, d)
	k := key{observer, subject}
	for _, id := range x.detection[k] {
		c := &x.report.Crashes[id.crash]
		o := &c.Observations[id.observation]
		o.End, o.Status = at, "detected"
		latency := at.TimeNS - c.TimeNS
		o.LatencyNS = &latency
		if c.First == nil {
			first := at
			c.First = &first
		}
	}
	delete(x.detection, k)
	if target.running {
		r := Recovery{Pair: d.Pair, Declaration: at, End: at, Status: "unresolved", Joins: []Join{}}
		if target.stopped || target.left {
			r.Status = "target_inactive"
		}
		if !p.running || p.stopped || p.left {
			r.Status = "observer_inactive"
		}
		x.report.Recoveries = append(x.report.Recoveries, r)
		if r.Status == "unresolved" {
			x.recovery[k] = append(x.recovery[k], len(x.report.Recoveries)-1)
		}
	}
}

func (x *reducer) crash(subject string, at Reference) {
	c := Crash{Subject: subject, TargetLifetime: x.state[subject].life, Reference: at, Observations: []Observation{}}
	// A repeated crash of an already stopped process is still visible, but cannot
	// create a fresh detection cohort for an earlier failure.
	if !x.state[subject].running {
		x.unsupported(fmt.Sprintf("Crash at seq %d targets already stopped process %s; no new cohort.", at.Seq, subject))
		x.report.Crashes = append(x.report.Crashes, c)
		return
	}
	for _, n := range x.nodes {
		if n.Name == subject {
			continue
		}
		p := x.state[n.Name]
		switch {
		case !p.running || p.stopped || p.left:
			c.Inactive++
		case p.members[subject] == "dead":
			c.AlreadyDead++
		case p.members[subject] != "alive" && p.members[subject] != "suspect":
			c.UnknownOrLeft++
		default:
			k := key{n.Name, subject}
			// Any previous cohort was interrupted when its target restarted.
			x.detection[k] = append(x.detection[k], pending{len(x.report.Crashes), len(c.Observations)})
			c.Observations = append(c.Observations, Observation{Pair: x.pair(n.Name, subject), End: at, Status: "unresolved"})
		}
	}
	x.report.Crashes = append(x.report.Crashes, c)
}

func (x *reducer) interrupt(node string, at Reference, reason string) {
	for k, ids := range x.recovery {
		if k.observer != node && k.subject != node {
			continue
		}
		role := "target_"
		if k.observer == node {
			role = "observer_"
		}
		for _, id := range ids {
			r := &x.report.Recoveries[id]
			r.End, r.Status = at, role+reason
		}
		delete(x.recovery, k)
	}
	for k, ids := range x.detection {
		// The crash itself opens its cohort. Only the target's restart censors it;
		// observer shutdown/leave/crash/restart always censors its own observation.
		if k.observer != node && (k.subject != node || reason != "restart") {
			continue
		}
		role := "target_"
		if k.observer == node {
			role = "observer_"
		}
		for _, id := range ids {
			o := &x.report.Crashes[id.crash].Observations[id.observation]
			o.End, o.Status = at, role+reason
		}
		delete(x.detection, k)
	}
}
