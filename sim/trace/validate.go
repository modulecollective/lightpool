package trace

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type validator struct{ names map[string]bool }

func number(v string) error {
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != v {
		return fmt.Errorf("invalid decimal string %q", v)
	}
	return nil
}
func instant(v string) error {
	if err := number(v); err != nil {
		return err
	}
	_, err := strconv.ParseInt(v, 10, 64)
	return err
}
func (v *validator) node(n string) error {
	if !v.names[n] {
		return fmt.Errorf("invalid node reference %q", n)
	}
	return nil
}
func (v *validator) process(p Process) error {
	if err := v.node(p.Node); err != nil {
		return err
	}
	if err := number(p.Lifetime); err != nil {
		return err
	}
	return instant(p.PausedUntil)
}
func (v *validator) summary(s *Summary) error {
	if s == nil {
		return fmt.Errorf("missing summary")
	}
	for _, n := range []string{s.Health, s.Backlog, s.PendingStreams} {
		if err := number(n); err != nil {
			return err
		}
	}
	for _, n := range s.Stats {
		if err := number(n); err != nil {
			return err
		}
	}
	return nil
}
func (v *validator) totals(t Totals) error {
	for _, n := range []string{t.SentBytes, t.ReceivedBytes, t.DatagramDrops, t.Detections} {
		if err := number(n); err != nil {
			return err
		}
	}
	return nil
}
func (v *validator) payload(b []byte, kind string) error {
	switch kind {
	case "header":
		var h Header
		if err := json.Unmarshal(b, &h); err != nil {
			return err
		}
		if len(h.Nodes) == 0 || len(h.Nodes) > 16384 || len(h.Initial) != len(h.Nodes) {
			return fmt.Errorf("invalid node list")
		}
		v.names = make(map[string]bool, len(h.Nodes))
		for _, n := range h.Nodes {
			if n.Name == "" || n.Address == "" || v.names[n.Name] {
				return fmt.Errorf("invalid/duplicate node")
			}
			v.names[n.Name] = true
		}
		seen := make(map[string]bool)
		for _, p := range h.Initial {
			if err := v.process(p); err != nil {
				return err
			}
			if p.Running || p.Lifetime != "0" || p.PausedUntil != "0" || seen[p.Node] {
				return fmt.Errorf("invalid initial state")
			}
			seen[p.Node] = true
		}
		var manifest map[string]any
		if raw, err := json.Marshal(h.Manifest); err != nil {
			return err
		} else if err = json.Unmarshal(raw, &manifest); err != nil {
			return err
		}
		code, ok := manifest["Code"].(string)
		if !ok || code == "" {
			return fmt.Errorf("missing source identity")
		}
	case "event":
		var e Event
		if err := json.Unmarshal(b, &e); err != nil {
			return err
		}
		if e.Kind == "" || e.Changes == nil || e.Traffic == nil {
			return fmt.Errorf("missing event fields")
		}
		if err := number(e.ID); err != nil {
			return err
		}
		if err := number(e.Lifetime); err != nil {
			return err
		}
		if e.Actor != "" {
			if err := v.node(e.Actor); err != nil {
				return err
			}
		}
		if e.Peer != "" {
			if err := v.node(e.Peer); err != nil {
				return err
			}
		}
		for _, c := range e.Changes {
			if err := v.node(c.Node); err != nil {
				return err
			}
			if c.Subject != "" {
				if err := v.node(c.Subject); err != nil {
					return err
				}
			}
			for _, n := range c.Helpers {
				if err := v.node(n); err != nil {
					return err
				}
			}
			switch c.Kind {
			case "process":
				if c.Process == nil || c.Process.Node != c.Node || (c.Phase != "start" && c.Phase != "pause" && c.Phase != "crash") {
					return fmt.Errorf("invalid process change")
				}
				if err := v.process(*c.Process); err != nil {
					return err
				}
			case "summary":
				if err := v.summary(c.Summary); err != nil {
					return err
				}
			case "member.upsert":
				if c.Member == nil {
					return fmt.Errorf("missing member")
				}
				if err := v.node(c.Member.Name); err != nil {
					return err
				}
				if err := number(c.Member.Incarnation); err != nil {
					return err
				}
				switch c.Member.State {
				case "alive", "suspect", "dead", "left":
				default:
					return fmt.Errorf("invalid member state")
				}
			case "member.remove":
				if c.Subject == "" {
					return fmt.Errorf("missing removal subject")
				}
			case "probe":
				if err := number(c.ID); err != nil {
					return err
				}
				switch c.Phase {
				case "direct", "indirect", "relay", "finished":
				default:
					return fmt.Errorf("invalid probe phase")
				}
			case "timer":
				for _, n := range []string{c.ID, c.Generation, c.Operation} {
					if err := number(n); err != nil {
						return err
					}
				}
				if err := instant(c.Deadline); err != nil {
					return err
				}
				switch c.Phase {
				case "arm", "cancel", "fire":
				default:
					return fmt.Errorf("invalid timer phase")
				}
			case "suspicion", "refutation":
			default:
				return fmt.Errorf("unknown change %q", c.Kind)
			}
		}
		for _, t := range e.Traffic {
			if err := number(t.ID); err != nil {
				return err
			}
			if err := number(t.Bytes); err != nil {
				return err
			}
			if err := v.node(t.From); err != nil {
				return err
			}
			if t.To != "" {
				if err := v.node(t.To); err != nil {
					return err
				}
			}
			if t.Received != "" {
				if err := instant(t.Received); err != nil {
					return err
				}
			}
			if t.Transport != "datagram" && t.Transport != "stream" {
				return fmt.Errorf("invalid transport")
			}
			switch t.Stage {
			case "attempt", "sent", "arrival", "received", "handle", "drop":
			default:
				return fmt.Errorf("invalid traffic stage")
			}
			if t.Messages == nil {
				return fmt.Errorf("missing message kinds")
			}
			for _, m := range t.Messages {
				switch m {
				case "ping", "indirect", "ack", "nack", "alive", "suspect", "dead", "exchange", "user":
				default:
					return fmt.Errorf("invalid message kind")
				}
			}
		}
	case "end":
		var e End
		if err := json.Unmarshal(b, &e); err != nil {
			return err
		}
		if e.Reason == "" || e.Nodes == nil {
			return fmt.Errorf("invalid end")
		}
		for _, p := range e.Nodes {
			if err := v.process(p.Process); err != nil {
				return err
			}
			if err := v.totals(p.Totals); err != nil {
				return err
			}
			if p.Summary != nil {
				if err := v.summary(p.Summary); err != nil {
					return err
				}
			}
		}
		return v.totals(e.Totals)
	}
	return nil
}

// Inspect raw JSON numbers recursively: even the nested manifest is exact.
func exactJSON(b []byte, header bool) error {
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var walk func(any, bool) error
	walk = func(value any, root bool) error {
		switch x := value.(type) {
		case float64:
			return fmt.Errorf("unsafe JSON number")
		case []any:
			for _, v := range x {
				if err := walk(v, false); err != nil {
					return err
				}
			}
		case map[string]any:
			for k, v := range x {
				if root && k == "version" {
					continue
				}
				if err := walk(v, false); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(raw, header)
}
