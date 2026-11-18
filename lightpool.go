// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

// Package lightpool drives the shared protocol core over real UDP and TCP.
package lightpool

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
	"log"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Application runs outside the owner loop. Request calls can overlap and must
// honor cancellation. Notify calls are ordered. Both may read Snapshot or call
// APIs reentrantly. Shutdown cancels callbacks; it never waits for user code.
type Application interface {
	Request(context.Context, core.Request) (core.Response, error)
	Notify(context.Context, core.Notification)
}
type Config struct {
	Protocol           core.Config
	Name               string
	Bind, Advertise    netip.AddrPort
	Meta               []byte
	ApplicationVersion string
	Application        Application
	Keyring            *Keyring
	Compression        bool
	Resolver           *net.Resolver
	Logger             *log.Logger
}

func DefaultConfig(name string) Config {
	return Config{Protocol: core.DefaultConfig(), Name: name, Bind: netip.MustParseAddrPort("127.0.0.1:7946"), Compression: true}
}

type command struct {
	event   core.Event
	id      uint64
	reply   chan core.Result
	abandon uint64
}
type ioStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   net.Conn
}
type Traffic struct{ SentBytes, ReceivedBytes, RejectedFrames, NotificationDrops uint64 }
type counters struct{ sent, received, rejected, drops atomic.Uint64 }
type Node struct {
	cfg                Config
	engine             *core.Core
	epoch              time.Time
	life               uint64
	ctx                context.Context
	cancel             context.CancelFunc
	udp                *net.UDPConn
	tcp                *net.TCPListener
	inbox              chan core.Event
	commands           chan command
	deferred           chan core.Event
	writes             chan core.SendDatagram
	requests           chan core.Request
	notices            chan core.Notification
	done               chan struct{}
	workers            sync.WaitGroup
	streamMu           sync.Mutex
	streams            map[core.Token]*ioStream
	timers             map[core.Timer]*time.Timer  // owner only
	waiters            map[uint64]chan core.Result // owner only
	sequence, accepted atomic.Uint64
	snapshot           atomic.Pointer[core.Snapshot]
	counts             counters
	shutdownOnce       sync.Once
}

func New(cfg Config) (*Node, error) {
	if !cfg.Bind.IsValid() {
		return nil, errors.New("explicit bind address required")
	}
	if cfg.Bind.Addr().IsUnspecified() && (!cfg.Advertise.IsValid() || cfg.Advertise.Addr().IsUnspecified()) {
		return nil, errors.New("wildcard bind requires explicit advertise address")
	}
	if cfg.Resolver == nil {
		cfg.Resolver = net.DefaultResolver
	}
	cfg.Protocol.Application = cfg.Application != nil
	cfg.Protocol.Protected = cfg.Keyring != nil
	opts := wire.Options{Label: cfg.Protocol.Label}
	if cfg.Keyring != nil {
		opts.Keys = cfg.Keyring.Keys()
		if len(opts.Keys) == 0 {
			return nil, errors.New("empty keyring")
		}
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.Protocol.Validate(); err != nil {
		return nil, err
	}
	// Binding the same ephemeral port for both protocols can race; retry only
	// EADDRINUSE on the second bind, closing the first before every retry.
	var tcp *net.TCPListener
	var udp *net.UDPConn
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		tcp, err = net.ListenTCP("tcp", net.TCPAddrFromAddrPort(cfg.Bind))
		if err != nil {
			return nil, err
		}
		addr := tcp.Addr().(*net.TCPAddr).AddrPort()
		udp, err = net.ListenUDP("udp", net.UDPAddrFromAddrPort(addr))
		if err == nil {
			break
		}
		_ = tcp.Close()
		if cfg.Bind.Port() != 0 {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	bound := tcp.Addr().(*net.TCPAddr).AddrPort()
	advertised := cfg.Advertise
	if !advertised.IsValid() {
		advertised = bound
	}
	if advertised.Port() == 0 {
		advertised = netip.AddrPortFrom(advertised.Addr(), bound.Port())
	}
	var entropy [24]byte
	if _, err = rand.Read(entropy[:]); err != nil {
		_ = tcp.Close()
		_ = udp.Close()
		return nil, err
	}
	seed := core.Seed{State: binary.LittleEndian.Uint64(entropy[:8]), Stream: binary.LittleEndian.Uint64(entropy[8:16]), Lifetime: binary.LittleEndian.Uint64(entropy[16:])}
	engine, err := core.New(cfg.Protocol, wire.Identity{Name: cfg.Name, Address: advertised, Meta: cfg.Meta, Application: cfg.ApplicationVersion}, seed)
	if err != nil {
		_ = tcp.Close()
		_ = udp.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, engine: engine, epoch: time.Now(), life: seed.Lifetime, ctx: ctx, cancel: cancel, udp: udp, tcp: tcp, inbox: make(chan core.Event, 256), commands: make(chan command, 256), deferred: make(chan core.Event, 1), writes: make(chan core.SendDatagram, 256), requests: make(chan core.Request, 256), notices: make(chan core.Notification, 1024), done: make(chan struct{}), streams: make(map[core.Token]*ioStream), timers: make(map[core.Timer]*time.Timer), waiters: make(map[uint64]chan core.Result)}
	n.saveSnapshot()
	n.workers.Add(3)
	go n.readUDP()
	go n.acceptTCP()
	go n.writeUDP()
	if cfg.Application != nil {
		for range 8 {
			go n.applicationWorker()
		}
		go n.notificationWorker()
	}
	go n.run()
	return n, nil
}
func (n *Node) now() core.Instant { return core.Instant(time.Since(n.epoch)) }
func (n *Node) options() wire.Options {
	o := wire.Options{Label: n.cfg.Protocol.Label, Compress: n.cfg.Compression}
	if n.cfg.Keyring != nil {
		o.Keys = n.cfg.Keyring.Keys()
	}
	return o
}
func (n *Node) post(e core.Event) {
	select {
	case n.inbox <- e:
	case <-n.ctx.Done():
	}
}
func (n *Node) saveSnapshot() { s := n.engine.Snapshot(); n.snapshot.Store(&s) }
func (n *Node) Snapshot() core.Snapshot {
	s := *n.snapshot.Load()
	s.Members = slices.Clone(s.Members)
	for j := range s.Members {
		s.Members[j] = s.Members[j].Clone()
	}
	return s
}
func (n *Node) Identity() wire.Identity {
	for _, m := range n.Snapshot().Members {
		if m.Name == n.cfg.Name {
			return m.Identity
		}
	}
	panic("local identity missing")
}
func (n *Node) Traffic() Traffic {
	return Traffic{n.counts.sent.Load(), n.counts.received.Load(), n.counts.rejected.Load(), n.counts.drops.Load()}
}
func (n *Node) run() {
	defer close(n.done)
	n.step(core.Start{})
	for {
		select {
		case e := <-n.inbox:
			n.step(e)
		case e := <-n.deferred:
			n.step(e)
		case cmd := <-n.commands:
			if cmd.abandon != 0 {
				delete(n.waiters, cmd.abandon)
				continue
			}
			if cmd.reply != nil {
				n.waiters[cmd.id] = cmd.reply
			}
			n.step(cmd.event)
		case <-n.ctx.Done():
			n.step(core.Shutdown{})
			for _, timer := range n.timers {
				timer.Stop()
			}
			n.timers = nil
			for id, ch := range n.waiters {
				ch <- core.Result{Command: id, Failure: core.Canceled}
				delete(n.waiters, id)
			}
			return
		}
	}
}
func (n *Node) step(e core.Event) {
	effects, err := n.engine.Step(n.now(), e)
	if err != nil {
		if n.cfg.Logger != nil {
			n.cfg.Logger.Printf("protocol input rejected: %v", err)
		}
		return
	}
	n.saveSnapshot()
	var immediate []core.Event
	for _, effect := range effects {
		switch e := effect.(type) {
		case core.SendDatagram:
			select {
			case n.writes <- e:
			default:
				immediate = append(immediate, core.Sent{ID: e.ID, Failure: core.LocalFailure})
			}
		case core.OpenStream:
			if !n.dial(e) {
				immediate = append(immediate, core.Opened{ID: e.ID, Failure: core.LocalFailure})
			}
		case core.WriteStream:
			n.writeStream(e)
		case core.CloseStream:
			n.closeStream(e.ID)
		case core.Arm:
			event := core.Fired{Timer: e.Timer}
			n.timers[e.Timer] = time.AfterFunc(time.Duration(max(0, e.At-n.now())), func() { n.post(event) })
		case core.Cancel:
			if t := n.timers[e.Timer]; t != nil {
				t.Stop()
				delete(n.timers, e.Timer)
			}
		case core.Defer:
			select {
			case n.deferred <- core.Work{ID: e.ID}:
			default:
				panic("duplicate deferred turn")
			}
		case core.Request:
			select {
			case n.requests <- e:
			default:
				immediate = append(immediate, core.Response{ID: e.ID, Reject: true})
			}
		case core.Notification:
			if n.cfg.Application != nil {
				select {
				case n.notices <- e:
				default:
					n.counts.drops.Add(1)
				}
			}
		case core.Result:
			if ch := n.waiters[e.Command]; ch != nil {
				ch <- e
				delete(n.waiters, e.Command)
			}
		case core.Observation: // Numeric core observations are retained in snapshots.
		}
	}
	if fired, ok := e.(core.Fired); ok {
		delete(n.timers, fired.Timer)
	}
	for _, next := range immediate {
		n.step(next)
	}
}
func (n *Node) call(ctx context.Context, makeEvent func(uint64) core.Event) (core.Result, error) {
	id := n.sequence.Add(1)
	reply := make(chan core.Result, 1)
	select {
	case n.commands <- command{event: makeEvent(id), id: id, reply: reply}:
	case <-ctx.Done():
		return core.Result{}, ctx.Err()
	case <-n.done:
		return core.Result{}, errors.New("node is shut down")
	}
	select {
	case r := <-reply:
		if r.Failure != core.Success {
			return r, fmt.Errorf("protocol operation failed: %d", r.Failure)
		}
		return r, nil
	case <-ctx.Done():
		select {
		case n.commands <- command{abandon: id}:
		case <-n.done:
		}
		return core.Result{}, ctx.Err()
	case <-n.done:
		return core.Result{}, errors.New("node is shut down")
	}
}
func (n *Node) Ping(ctx context.Context, peer wire.Identity) (time.Duration, error) {
	r, err := n.call(ctx, func(id uint64) core.Event { return core.Ping{Command: id, Peer: peer.Clone()} })
	return r.RTT, err
}
func (n *Node) Update(ctx context.Context, meta []byte) error {
	data := slices.Clone(meta)
	_, err := n.call(ctx, func(id uint64) core.Event { return core.Update{Command: id, Meta: data} })
	return err
}
func (n *Node) Leave(ctx context.Context) error {
	_, err := n.call(ctx, func(id uint64) core.Event { return core.Leave{Command: id} })
	return err
}
func (n *Node) Send(ctx context.Context, peer wire.Identity, data []byte, reliable bool) error {
	msg := slices.Clone(data)
	_, err := n.call(ctx, func(id uint64) core.Event {
		return core.Send{Command: id, Peer: peer.Clone(), Data: msg, Reliable: reliable}
	})
	return err
}
func (n *Node) Broadcast(ctx context.Context, data []byte) error {
	msg := slices.Clone(data)
	_, err := n.call(ctx, func(id uint64) core.Event { return core.Send{Command: id, Data: msg, Broadcast: true} })
	return err
}

// Join requires explicit ports. Optional "name/host:port" syntax keeps DNS in
// the adapter; custom discovery can simply pass resolved endpoints.
func (n *Node) Join(ctx context.Context, endpoints ...string) (int, error) {
	count := 0
	var errs []error
	for _, endpoint := range endpoints {
		peers, err := n.resolve(ctx, endpoint)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, peer := range peers {
			_, err = n.call(ctx, func(id uint64) core.Event { return core.Join{Command: id, Peer: peer} })
			if err != nil {
				errs = append(errs, err)
			} else {
				count++
			}
		}
	}
	if count > 0 {
		return count, nil
	}
	return 0, errors.Join(errs...)
}
func (n *Node) resolve(ctx context.Context, endpoint string) ([]wire.Identity, error) {
	name, hostport, hasName := strings.Cut(endpoint, "/")
	if !hasName {
		hostport = endpoint
		name = endpoint
	}
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, err
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 {
		return nil, errors.New("explicit valid port required")
	}
	deadline, cancel := context.WithTimeout(ctx, n.cfg.Protocol.StreamTimeout)
	defer cancel()
	ips, err := n.cfg.Resolver.LookupNetIP(deadline, "ip", host)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(ips, func(a, b netip.Addr) int { return a.Compare(b) })
	peers := make([]wire.Identity, 0, len(ips))
	for _, ip := range ips {
		peer := wire.Identity{Name: name, Address: netip.AddrPortFrom(ip.Unmap(), uint16(p))}
		if err = peer.Validate(); err != nil {
			return nil, err
		}
		peers = append(peers, peer)
	}
	return peers, nil
}
func (n *Node) Shutdown() error {
	n.shutdownOnce.Do(func() {
		n.cancel()
		_ = n.udp.Close()
		_ = n.tcp.Close()
		n.streamMu.Lock()
		for _, s := range n.streams {
			s.cancel()
			if s.conn != nil {
				_ = s.conn.Close()
			}
		}
		n.streamMu.Unlock()
	})
	<-n.done
	n.workers.Wait()
	return nil
}
func (n *Node) applicationWorker() {
	for {
		select {
		case r := <-n.requests:
			response, err := n.cfg.Application.Request(n.ctx, r)
			response.ID = r.ID
			response.Reject = response.Reject || err != nil
			n.post(response)
		case <-n.ctx.Done():
			return
		}
	}
}
func (n *Node) notificationWorker() {
	for {
		select {
		case e := <-n.notices:
			n.cfg.Application.Notify(n.ctx, e)
		case <-n.ctx.Done():
			return
		}
	}
}
