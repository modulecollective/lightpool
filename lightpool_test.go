// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package lightpool

import (
	"bytes"
	"context"
	"fmt"
	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(name string) Config {
	c := DefaultConfig(name)
	c.Bind = netip.MustParseAddrPort("127.0.0.1:0")
	c.Protocol.ProbeInterval = 100 * time.Millisecond
	c.Protocol.ProbeTimeout = 50 * time.Millisecond
	c.Protocol.GossipInterval = 10 * time.Millisecond
	c.Protocol.StreamTimeout = 500 * time.Millisecond
	c.Protocol.PushPullInterval = 200 * time.Millisecond
	return c
}
func newNode(t testing.TB, c Config) *Node {
	t.Helper()
	n, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	return n
}
func wait(t testing.TB, description string, predicate func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("timed out: " + description)
		}
	}
}
func join(t testing.TB, n, peer *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	count, err := n.Join(ctx, peer.Identity().Address.String())
	if err != nil || count != 1 {
		t.Fatal(count, err)
	}
}
func memberState(n *Node, name string) (wire.State, bool) {
	for _, m := range n.Snapshot().Members {
		if m.Name == name {
			return m.State, true
		}
	}
	return 0, false
}
func TestOrdinaryClusterConcurrentOperations(t *testing.T) {
	nodes := make([]*Node, 8)
	for j := range nodes {
		nodes[j] = newNode(t, testConfig(fmt.Sprint(j)))
	}
	for j := 1; j < len(nodes); j++ {
		join(t, nodes[j], nodes[0])
	}
	wait(t, "healthy convergence", func() bool {
		for _, n := range nodes {
			s := n.Snapshot()
			if len(s.Members) != len(nodes) {
				return false
			}
			for _, m := range s.Members {
				if m.State != wire.Alive {
					return false
				}
			}
		}
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	errs := make(chan error, len(nodes)*2)
	for j, n := range nodes {
		workers.Add(1)
		go func() {
			defer workers.Done()
			rtt, err := n.Ping(ctx, nodes[(j+1)%len(nodes)].Identity())
			if err != nil {
				errs <- err
			}
			if rtt < 0 {
				errs <- fmt.Errorf("negative RTT: %s", rtt)
			}
			if err = n.Update(ctx, []byte(fmt.Sprint(j))); err != nil {
				errs <- err
			}
			s := n.Snapshot()
			s.Members[0].Meta = []byte("caller")
		}()
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	wait(t, "several ordinary probe rounds", func() bool {
		for _, n := range nodes {
			if n.Snapshot().Stats.Probes < 5 {
				return false
			}
		}
		return true
	})
	for _, n := range nodes {
		if n.Snapshot().Stats.FailedProbes != 0 {
			t.Fatal("routine probe deadlines missed", n.Snapshot())
		}
		traffic := n.Traffic()
		if traffic.SentBytes == 0 || traffic.ReceivedBytes == 0 {
			t.Fatal("missing traffic counts")
		}
	}
	if err := nodes[7].Leave(ctx); err != nil {
		t.Fatal(err)
	}
	wait(t, "leave disseminates", func() bool { s, ok := memberState(nodes[0], "7"); return ok && s == wire.Left })
	var shutdowns sync.WaitGroup
	for range 8 {
		shutdowns.Add(1)
		go func() {
			defer shutdowns.Done()
			if err := nodes[0].Shutdown(); err != nil {
				t.Error(err)
			}
		}()
	}
	shutdowns.Wait()
	if _, err := nodes[0].Ping(ctx, nodes[1].Identity()); err == nil {
		t.Fatal("command accepted after shutdown")
	}
}

type app struct {
	request func(context.Context, core.Request) (core.Response, error)
	notify  func(context.Context, core.Notification)
}

func (a app) Request(ctx context.Context, r core.Request) (core.Response, error) {
	if a.request != nil {
		return a.request(ctx, r)
	}
	return core.Response{}, nil
}
func (a app) Notify(ctx context.Context, n core.Notification) {
	if a.notify != nil {
		a.notify(ctx, n)
	}
}
func TestApplicationReentrancyDataAndDelayedState(t *testing.T) {
	var reference atomic.Pointer[Node]
	var merge atomic.Bool
	var users atomic.Int64
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	var delayed atomic.Bool
	c := testConfig("a")
	c.Application = app{request: func(ctx context.Context, r core.Request) (core.Response, error) {
		if n := reference.Load(); n != nil {
			_ = n.Snapshot()
		}
		switch r.Kind {
		case core.LocalState:
			if delayed.Load() {
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-gate:
				case <-ctx.Done():
					return core.Response{}, ctx.Err()
				}
			}
			return core.Response{Data: []byte("state")}, nil
		case core.AckPayload:
			return core.Response{Data: []byte("ack")}, nil
		}
		return core.Response{}, nil
	}, notify: func(_ context.Context, n core.Notification) {
		_ = reference.Load().Snapshot()
		if n.Kind == "merge-state" && bytes.Equal(n.Data, []byte("state")) {
			merge.Store(true)
		}
		if n.Kind == "user" {
			users.Add(1)
		}
	}}
	a := newNode(t, c)
	reference.Store(a)
	b := newNode(t, testConfig("b"))
	join(t, b, a)
	join(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Send(ctx, a.Identity(), []byte("datagram"), false); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(ctx, a.Identity(), []byte("stream"), true); err != nil {
		t.Fatal(err)
	}
	wait(t, "application user bytes", func() bool { return users.Load() == 2 })
	// Block one push/pull's application data; an unrelated explicit ping still completes.
	delayed.Store(true)
	joined := make(chan error, 1)
	go func() { _, err := a.Join(ctx, b.Identity().Address.String()); joined <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := b.Ping(ctx, a.Identity()); err != nil {
		t.Fatal("slow local-state blocked probe", err)
	}
	close(gate)
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	// b has no LocalState application; a's local state still reaches an enabled peer.
	d := testConfig("c")
	d.Application = app{notify: func(_ context.Context, n core.Notification) {
		if n.Kind == "merge-state" && bytes.Equal(n.Data, []byte("state")) {
			merge.Store(true)
		}
	}}
	third := newNode(t, d)
	join(t, third, a)
	wait(t, "state bytes reach application", merge.Load)
}
func TestApplicationVeto(t *testing.T) {
	for _, kind := range []core.RequestKind{core.ApproveMerge, core.ApproveAlive} {
		t.Run(string(kind), func(t *testing.T) {
			c := testConfig("a")
			c.Application = app{request: func(_ context.Context, r core.Request) (core.Response, error) {
				return core.Response{Reject: r.Kind == kind}, nil
			}}
			a := newNode(t, c)
			b := newNode(t, testConfig("b"))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = b.Join(ctx, a.Identity().Address.String())
			wait(t, "veto processed", func() bool { return a.Snapshot().PendingStreams == 0 })
			if _, ok := memberState(a, "b"); ok {
				t.Fatal("rejected peer merged")
			}
		})
	}
}
func TestEncryptedLabeledLoopbackAndRotation(t *testing.T) {
	old, newKey := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 32)
	ka, err := NewKeyring(old)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := NewKeyring(old)
	if err != nil {
		t.Fatal(err)
	}
	ca, cb := testConfig("a"), testConfig("b")
	ca.Protocol.Label = "secret-cluster"
	cb.Protocol.Label = ca.Protocol.Label
	ca.Keyring = ka
	cb.Keyring = kb
	a, b := newNode(t, ca), newNode(t, cb)
	join(t, b, a)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = a.Ping(ctx, b.Identity()); err != nil {
		t.Fatal(err)
	}
	for _, k := range []*Keyring{ka, kb} {
		if err = k.Add(newKey); err != nil {
			t.Fatal(err)
		}
	}
	if err = ka.Use(newKey); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Ping(ctx, b.Identity()); err != nil {
		t.Fatal(err)
	}
	if err = kb.Use(newKey); err != nil {
		t.Fatal(err)
	}
	for _, k := range []*Keyring{ka, kb} {
		if err = k.Remove(old); err != nil {
			t.Fatal(err)
		}
	}
	join(t, a, b)
	udp, err := net.Dial("udp", a.Identity().Address.String())
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	plain, err := wire.Encode([]wire.Message{{Ack: &wire.Ack{Sequence: 7}}}, wire.Options{Label: ca.Protocol.Label})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = udp.Write(plain); err != nil {
		t.Fatal(err)
	}
	wait(t, "encrypted runtime rejects plaintext", func() bool { return a.Traffic().RejectedFrames > 0 })
	// Drive the real stream reader with byte-at-a-time encrypted input.
	conn, err := net.Dial("tcp", a.Identity().Address.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	frame, err := wire.Encode([]wire.Message{{Ping: &wire.Ping{Sequence: 123, Target: "a", Source: b.Identity()}}}, wire.Options{Label: ca.Protocol.Label, Keys: ka.Keys()})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range frame {
		if _, err = conn.Write([]byte{value}); err != nil {
			t.Fatal(err)
		}
	}
	reply, err := wire.Read(conn, wire.Options{Label: ca.Protocol.Label, Keys: ka.Keys()})
	if err != nil || len(reply) != 1 || reply[0].Ack == nil || reply[0].Ack.Sequence != 123 {
		t.Fatal(reply, err)
	}
	wrong := testConfig("wrong")
	wrong.Protocol.Label = "wrong"
	wrong.Keyring = ka
	w := newNode(t, wrong)
	if count, err := w.Join(ctx, a.Identity().Address.String()); err == nil || count != 0 {
		t.Fatal("wrong label joined", count, err)
	}
}
func TestBindRulesAndKeyOwnership(t *testing.T) {
	c := testConfig("a")
	c.Bind = netip.MustParseAddrPort("0.0.0.0:0")
	if _, err := New(c); err == nil {
		t.Fatal("wildcard advertised implicitly")
	}
	key := bytes.Repeat([]byte{1}, 16)
	k, err := NewKeyring(key)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 9
	keys := k.Keys()
	keys[0][0] = 8
	if k.Keys()[0][0] != 1 {
		t.Fatal("key aliases caller")
	}
	if err = k.Remove(k.Keys()[0]); err == nil {
		t.Fatal("removed primary key")
	}
}
