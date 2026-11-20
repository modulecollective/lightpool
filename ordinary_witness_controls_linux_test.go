//go:build linux

package lightpool

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/modulecollective/lightpool/internal/experiment"
)

// Socket controls drive real UDP and TCP through the same router, veth, netem
// and nft code as the campaign, on separate endpoints and ports, before any
// protocol attempt. All times come from the coordinator's monotonic clock, so
// a delivered datagram's application-to-application delay lies in
// [read return - send end, read return - send start]. That includes
// scheduling and kernel delay, not pure link transit.

type controlProbe struct {
	Name      string                                    `json:"name"`
	Phase     string                                    `json:"phase"`
	Proto     string                                    `json:"proto"`
	From      string                                    `json:"from"`
	To        string                                    `json:"to"`
	Expect    string                                    `json:"expect"`
	Sent      int                                       `json:"sent"`
	Delivered int                                       `json:"delivered"`
	DelayNS   map[string]experiment.WitnessDistribution `json:"delayNS,omitempty"`
	StartNS   int64                                     `json:"startNS,string"`
	EndNS     int64                                     `json:"endNS,string"`
	Error     string                                    `json:"error,omitempty"`
	Pass      bool                                      `json:"pass"`
}

type controlEndpoint struct {
	name string
	ns   *os.File
	addr netip.Addr
	udp  *net.UDPConn
	tcp  *net.TCPListener
	mu   sync.Mutex
	got  map[string]int64 // payload -> read return
}

func (e *controlEndpoint) received(payload string) (int64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	at, ok := e.got[payload]
	return at, ok
}

func (h *witnessHost) controls(dir string) (err error) {
	if err = os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	start := time.Now()
	since := func() int64 { return time.Since(start).Nanoseconds() }
	result := struct {
		Format  string                   `json:"format"`
		Source  string                   `json:"source"`
		Binary  string                   `json:"binary"`
		Kernel  string                   `json:"kernel"`
		Tools   map[string]string        `json:"tools"`
		Latency string                   `json:"egressDelay"`
		Probes  []*controlProbe          `json:"probes"`
		Fault   *experiment.WitnessFault `json:"fault"`
		Pass    bool                     `json:"pass"`
		Error   string                   `json:"error,omitempty"`
		Limits  []string                 `json:"limits"`
	}{Format: "lightpool-ordinary-witness-controls/v1", Source: h.source, Binary: h.binary, Kernel: h.kernel, Tools: h.tools, Latency: "1ms", Probes: []*controlProbe{}, Limits: []string{
		"One 1 ms egress delay per endpoint; 20 datagrams per UDP probe, 5 ms apart, counted delivered within 1 s of the last send.",
		"TCP connect and exchange use a 2 s bound; a timeout is the expected blocked outcome because nothing sends RST or ICMP.",
		"Packet identity is the application payload; kernel arrival and in-kernel drops other than the rule counter stay unknown.",
	}}
	endpoints := map[string]*controlEndpoint{}
	defer func() {
		for _, e := range endpoints {
			if e.udp != nil {
				_ = e.udp.Close()
			}
			if e.tcp != nil {
				_ = e.tcp.Close()
			}
			_ = e.ns.Close()
		}
		_, _ = h.run(nil, "", "nft", "delete", "table", "inet", "lightpool")
		if survivors := witnessSurvivors(); len(survivors) > 0 {
			err = errors.Join(err, fmt.Errorf("surviving processes %v", survivors))
		}
		if err != nil {
			result.Pass, result.Error = false, err.Error()
		}
		b, e := json.MarshalIndent(result, "", "  ")
		if e == nil {
			e = os.WriteFile(filepath.Join(dir, "controls.json"), append(b, '\n'), 0o600)
		}
		err = errors.Join(err, e)
	}()
	if _, err = h.run(nil, witnessTable, "nft", "-f", "-"); err != nil {
		return err
	}
	for i, name := range []string{"A", "T", "O"} {
		ns, err := newNetns()
		if err != nil {
			return err
		}
		e := &controlEndpoint{name: name, ns: ns, addr: witnessAddress(i), got: map[string]int64{}}
		endpoints[name] = e
		if err = h.link(i, ns, time.Millisecond); err != nil {
			return err
		}
		err = inNetns(ns, func() (err error) {
			if e.udp, err = net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(e.addr, 9000))); err != nil {
				return err
			}
			e.tcp, err = net.ListenTCP("tcp", net.TCPAddrFromAddrPort(netip.AddrPortFrom(e.addr, 9001)))
			return err
		})
		if err != nil {
			return err
		}
		go func() {
			buf := make([]byte, 1500)
			for {
				n, _, err := e.udp.ReadFromUDPAddrPort(buf)
				at := since()
				if err != nil {
					return
				}
				e.mu.Lock()
				e.got[string(buf[:n])] = at
				e.mu.Unlock()
			}
		}()
		go func() {
			for {
				c, err := e.tcp.Accept()
				if err != nil {
					return
				}
				go func() {
					defer func() { _ = c.Close() }()
					buf := make([]byte, 64)
					for {
						n, err := c.Read(buf)
						if err != nil {
							return
						}
						at := since()
						e.mu.Lock()
						e.got[string(buf[:n])] = at
						e.mu.Unlock()
						if _, err = c.Write(buf[:n]); err != nil {
							return
						}
					}
				}()
			}
		}()
	}
	sequence := 0
	udp := func(phase, from, to, expect string) error {
		a, b := endpoints[from], endpoints[to]
		p := &controlProbe{Name: fmt.Sprintf("%s-udp-%s-%s", phase, from, to), Phase: phase, Proto: "udp", From: from, To: to, Expect: expect, StartNS: since()}
		var conn *net.UDPConn
		if err := inNetns(a.ns, func() (err error) {
			conn, err = net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(netip.AddrPortFrom(b.addr, 9000)))
			return err
		}); err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		type sent struct {
			payload    string
			start, end int64
		}
		var all []sent
		for range 20 {
			sequence++
			payload := "probe-" + strconv.Itoa(sequence)
			s := sent{payload: payload, start: since()}
			if _, err := conn.Write([]byte(payload)); err != nil {
				p.Error = err.Error()
			}
			s.end = since()
			all = append(all, s)
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(time.Second)
		var lower, upper []int64
		for _, s := range all {
			if at, ok := b.received(s.payload); ok {
				p.Delivered++
				lower, upper = append(lower, at-s.end), append(upper, at-s.start)
			}
		}
		p.Sent, p.EndNS = len(all), since()
		p.DelayNS = map[string]experiment.WitnessDistribution{"lower": witnessDist(lower), "upper": witnessDist(upper)}
		p.Pass = p.Error == "" && (expect == "delivered" && p.Delivered == p.Sent || expect == "dropped" && p.Delivered == 0)
		result.Probes = append(result.Probes, p)
		return nil
	}
	// connect dials and exchanges one message; a blocked path must time out.
	connect := func(phase, from, to, expect string) net.Conn {
		a, b := endpoints[from], endpoints[to]
		p := &controlProbe{Name: fmt.Sprintf("%s-tcp-%s-%s", phase, from, to), Phase: phase, Proto: "tcp", From: from, To: to, Expect: expect, StartNS: since(), Sent: 1}
		result.Probes = append(result.Probes, p)
		var conn net.Conn
		err := inNetns(a.ns, func() (err error) {
			conn, err = net.DialTimeout("tcp", netip.AddrPortFrom(b.addr, 9001).String(), 2*time.Second)
			return err
		})
		if err == nil {
			sequence++
			payload := "stream-" + strconv.Itoa(sequence)
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			buf := make([]byte, 64)
			if _, err = conn.Write([]byte(payload)); err == nil {
				_, err = conn.Read(buf)
			}
			_ = conn.SetDeadline(time.Time{})
		}
		p.EndNS = since()
		var timeout net.Error
		if err != nil {
			p.Error = err.Error()
			p.Pass = expect == "blocked" && errors.As(err, &timeout) && timeout.Timeout()
			return nil
		}
		p.Delivered = 1
		p.Pass = expect == "delivered"
		return conn
	}
	healthy := [][3]string{{"A", "T", "delivered"}, {"T", "A", "delivered"}, {"O", "T", "delivered"}, {"T", "O", "delivered"}}
	for _, c := range healthy {
		if err = udp("healthy", c[0], c[1], c[2]); err != nil {
			return err
		}
	}
	for _, c := range [][2]string{{"A", "T"}, {"T", "A"}, {"O", "T"}} {
		if conn := connect("healthy", c[0], c[1], "delivered"); conn != nil {
			_ = conn.Close()
		}
	}
	// An A-to-T stream established while healthy carries data across the fault.
	established := connect("established", "A", "T", "delivered")
	if established == nil {
		return errors.New("could not establish the A-to-T stream")
	}
	defer func() { _ = established.Close() }()
	if result.Fault, err = h.installFault([]netip.Addr{endpoints["A"].addr}, endpoints["T"].addr, since); err != nil {
		return err
	}
	for _, c := range [][3]string{{"A", "T", "dropped"}, {"T", "A", "delivered"}, {"O", "T", "delivered"}, {"T", "O", "delivered"}} {
		if err = udp("fault", c[0], c[1], c[2]); err != nil {
			return err
		}
	}
	for _, c := range [][3]string{{"A", "T", "blocked"}, {"T", "A", "blocked"}, {"O", "T", "delivered"}} {
		if conn := connect("fault", c[0], c[1], c[2]); conn != nil {
			_ = conn.Close()
		}
	}
	sequence++
	pending := "held-" + strconv.Itoa(sequence)
	held := &controlProbe{Name: "fault-established-A-T", Phase: "fault", Proto: "tcp", From: "A", To: "T", Expect: "held then recovered", Sent: 1, StartNS: since()}
	result.Probes = append(result.Probes, held)
	if _, err = established.Write([]byte(pending)); err != nil {
		held.Error = err.Error()
	}
	time.Sleep(2 * time.Second)
	_, heldDuringFault := endpoints["T"].received(pending)
	if err = h.removeFault(result.Fault, since); err != nil {
		return err
	}
	for _, c := range [][3]string{{"A", "T", "delivered"}, {"T", "A", "delivered"}} {
		if err = udp("recovery", c[0], c[1], c[2]); err != nil {
			return err
		}
	}
	if conn := connect("recovery", "A", "T", "delivered"); conn != nil {
		_ = conn.Close()
	}
	deadline := time.Now().Add(15 * time.Second)
	at, recovered := endpoints["T"].received(pending)
	for !recovered && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		at, recovered = endpoints["T"].received(pending)
	}
	held.EndNS = since()
	if recovered {
		held.Delivered = 1
		held.DelayNS = map[string]experiment.WitnessDistribution{"afterRemoval": witnessDist([]int64{at - result.Fault.RemovedNS})}
	}
	held.Pass = held.Error == "" && !heldDuringFault && recovered
	result.Pass = result.Fault.Packets > 0
	var failed []string
	for _, p := range result.Probes {
		if !p.Pass {
			failed = append(failed, p.Name)
		}
	}
	if len(failed) > 0 || !result.Pass {
		return fmt.Errorf("socket controls failed: %s (fault packets %d)", strings.Join(failed, ", "), result.Fault.Packets)
	}
	return nil
}

func witnessDist(values []int64) experiment.WitnessDistribution {
	return experiment.WitnessDistributionOf(values, "coordinator monotonic send-call and read-return bounds")
}
