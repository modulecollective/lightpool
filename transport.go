// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package lightpool

import (
	"context"
	"errors"
	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
	"io"
	"net"
	"net/netip"
	"time"
)

func normalized(a netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}
func (n *Node) readUDP() {
	defer n.workers.Done()
	buf := make([]byte, 65535)
	for {
		count, from, err := n.udp.ReadFromUDPAddrPort(buf)
		received := n.now()
		if err != nil {
			return
		}
		n.counts.received.Add(uint64(count))
		if count > n.cfg.Protocol.PacketBudget {
			n.counts.rejected.Add(1)
			continue
		}
		messages, err := wire.Decode(buf[:count], n.options())
		if err != nil {
			n.counts.rejected.Add(1)
			continue
		}
		n.post(core.Datagram{From: normalized(from), Received: received, Messages: messages})
	}
}
func (n *Node) writeUDP() {
	defer n.workers.Done()
	for {
		select {
		case e := <-n.writes:
			b, err := wire.Encode(e.Messages, n.options())
			f := core.Success
			if err != nil || len(b) > n.cfg.Protocol.PacketBudget {
				f = core.LocalFailure
			} else {
				err = n.udp.SetWriteDeadline(time.Now().Add(n.cfg.Protocol.StreamTimeout))
				if err == nil {
					var count int
					count, err = n.udp.WriteToUDPAddrPort(b, e.To)
					n.counts.sent.Add(uint64(count))
					if err == nil && count != len(b) {
						err = io.ErrShortWrite
					}
				}
				if err != nil {
					f = networkFailure(err)
				}
			}
			n.post(core.Sent{ID: e.ID, Failure: f})
		case <-n.ctx.Done():
			return
		}
	}
}
func networkFailure(err error) core.Failure {
	var e net.Error
	if errors.As(err, &e) && e.Timeout() {
		return core.Timeout
	}
	return core.RemoteFailure
}
func (n *Node) register(id core.Token, conn net.Conn) *ioStream {
	n.streamMu.Lock()
	defer n.streamMu.Unlock()
	if n.ctx.Err() != nil || len(n.streams) >= n.cfg.Protocol.MaxPushPull*2 {
		if conn != nil {
			_ = conn.Close()
		}
		return nil
	}
	ctx, cancel := context.WithCancel(n.ctx)
	s := &ioStream{ctx: ctx, cancel: cancel, conn: conn}
	n.streams[id] = s
	return s
}
func (n *Node) acceptTCP() {
	defer n.workers.Done()
	for {
		conn, err := n.tcp.AcceptTCP()
		if err != nil {
			return
		}
		id := core.Token{Lifetime: n.life, ID: 1<<63 | n.accepted.Add(1)}
		s := n.register(id, conn)
		if s == nil {
			if n.ctx.Err() != nil {
				return
			}
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(n.cfg.Protocol.StreamTimeout))
		n.workers.Add(1)
		go func() {
			defer n.workers.Done()
			n.post(core.Accepted{ID: id, Peer: normalized(conn.RemoteAddr().(*net.TCPAddr).AddrPort())})
			n.readStream(id, s, conn)
		}()
	}
}
func (n *Node) dial(e core.OpenStream) bool {
	s := n.register(e.ID, nil)
	if s == nil {
		return false
	}
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		ctx, cancel := context.WithDeadline(s.ctx, n.epoch.Add(time.Duration(e.Deadline)))
		defer cancel()
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", e.To.String())
		if err != nil {
			n.post(core.Opened{ID: e.ID, Failure: networkFailure(err)})
			return
		}
		n.streamMu.Lock()
		if n.streams[e.ID] != s || s.ctx.Err() != nil {
			n.streamMu.Unlock()
			_ = conn.Close()
			return
		}
		s.conn = conn
		n.streamMu.Unlock()
		_ = conn.SetDeadline(n.epoch.Add(time.Duration(e.Deadline)))
		n.post(core.Opened{ID: e.ID})
		n.readStream(e.ID, s, conn)
	}()
	return true
}
func (n *Node) readStream(id core.Token, s *ioStream, conn net.Conn) {
	// Each exchange carries one request and one response. After a complete
	// frame, the core owns closure; EOF cannot overtake a pending write result.
	messages, err := wire.Read(countingReader{conn, &n.counts}, n.options())
	received := n.now()
	if err != nil {
		if s.ctx.Err() == nil {
			if !errors.Is(err, io.EOF) {
				n.counts.rejected.Add(1)
			}
			n.post(core.Closed{ID: id, Failure: networkFailure(err)})
		}
		return
	}
	n.post(core.StreamMessage{ID: id, Received: received, Messages: messages})
}
func (n *Node) writeStream(e core.WriteStream) {
	n.streamMu.Lock()
	s := n.streams[e.ID]
	var conn net.Conn
	if s != nil {
		conn = s.conn
	}
	n.streamMu.Unlock()
	if conn == nil {
		n.post(core.Written{ID: e.ID, Failure: core.LocalFailure})
		return
	}
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		b, err := wire.Encode(e.Messages, n.options())
		f := core.Success
		if err != nil {
			f = core.LocalFailure
		} else {
			err = conn.SetDeadline(n.epoch.Add(time.Duration(e.Deadline)))
			for err == nil && len(b) > 0 {
				var count int
				count, err = conn.Write(b)
				n.counts.sent.Add(uint64(count))
				b = b[count:]
				if count == 0 && err == nil {
					err = io.ErrShortWrite
				}
			}
			if err != nil {
				f = networkFailure(err)
			}
		}
		n.post(core.Written{ID: e.ID, Failure: f})
	}()
}
func (n *Node) closeStream(id core.Token) {
	n.streamMu.Lock()
	s := n.streams[id]
	delete(n.streams, id)
	n.streamMu.Unlock()
	if s != nil {
		s.cancel()
		if s.conn != nil {
			_ = s.conn.Close()
		}
	}
}

type countingReader struct {
	r      io.Reader
	counts *counters
}

func (c countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.counts.received.Add(uint64(n))
	return n, err
}
