// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package wire

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/netip"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// The ACK fixture (sequence 7, no label) was assembled by hand from the format
// description; its CRC32 came from gzip's trailer and `cksum -a crc32b`.
var ackFixture = "4c504c21020000000000000d030000000000000007ce2ece0b"

func TestIndependentFixture(t *testing.T) {
	frame, err := hex.DecodeString(ackFixture)
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := Decode(frame, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Ack == nil || msgs[0].Ack.Sequence != 7 {
		t.Fatal(msgs)
	}
	got, err := Encode([]Message{{Ack: &Ack{Sequence: 7}}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, frame) {
		t.Fatalf("fixture mismatch %x", got)
	}
	for j := range frame {
		if _, err = Decode(frame[:j], Options{}); err == nil {
			t.Fatalf("accepted prefix %d", j)
		}
	}
	for _, j := range []int{0, 4, 5, 6, 8, 12, len(frame) - 1} {
		b := bytes.Clone(frame)
		b[j] ^= 128
		if _, err = Decode(b, Options{}); err == nil {
			t.Fatalf("accepted mutation at %d", j)
		}
	}
	if _, err = Decode(append(frame, 0), Options{}); err == nil {
		t.Fatal("accepted trailing bytes")
	}
}

type fragments struct{ r io.Reader }

func (f fragments) Read(b []byte) (int, error) { return f.r.Read(b[:min(len(b), 1)]) }
func TestFragmentedStream(t *testing.T) {
	b, _ := hex.DecodeString(ackFixture)
	r := bytes.NewReader(append(bytes.Clone(b), b...))
	for range 2 {
		if _, err := Read(fragments{r}, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if r.Len() != 0 {
		t.Fatal("unread frame bytes")
	}
	if _, err := Read(r, Options{}); err != io.EOF {
		t.Fatal(err)
	}
}

func identity(name string, addr string, meta int) Identity {
	i := Identity{Name: name, Address: netip.MustParseAddrPort(addr)}
	if meta > 0 {
		i.Meta = bytes.Repeat([]byte{0xa5}, meta)
	}
	return i
}

// variants returns every message kind, with fields at their smallest and
// largest, IPv4, IPv6 and IPv4-mapped addresses, and incarnations of several
// uvarint widths.
func variants() [][]Message {
	small := identity("n", "192.0.2.1:1", 0)
	large := identity(strings.Repeat("é", MaxName/2), "[2001:db8::ffff]:65535", MaxMeta)
	mapped := identity("m", "[::ffff:192.0.2.1]:7946", 1)
	var out [][]Message
	for _, i := range []Identity{small, large, mapped} {
		for _, inc := range []uint64{0, 127, 128, 1 << 35, ^uint64(0)} {
			member := Member{Identity: i, Incarnation: inc}
			left := member
			left.State = Left
			accusation := Accusation{Node: i.Name, Incarnation: inc, From: strings.Repeat("f", MaxName)}
			out = append(out,
				[]Message{{Ping: &Ping{Sequence: inc, Target: i.Name, Source: i}}},
				[]Message{{Indirect: &Indirect{Sequence: inc, Target: i, Source: small}}},
				[]Message{{Ack: &Ack{Sequence: inc}}},
				[]Message{{Nack: &Ack{Sequence: inc}}},
				[]Message{{Alive: &member}},
				[]Message{{Suspect: &accusation}},
				[]Message{{Dead: &accusation}},
				[]Message{{Exchange: &Exchange{}}},
				[]Message{{Exchange: &Exchange{Members: []Member{member, left}}}},
				[]Message{{Ack: &Ack{Sequence: inc}}, {Alive: &member}, {Suspect: &accusation}},
			)
		}
	}
	return out
}

func TestSizeMatchesEncodingAndRoundTrips(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	for _, messages := range variants() {
		for _, o := range []Options{{}, {Label: strings.Repeat("l", MaxLabel)}, {Keys: [][]byte{key}}} {
			size, err := Size(messages, o.Label, len(o.Keys) > 0)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := Encode(messages, o)
			if err != nil {
				t.Fatal(err)
			}
			if size != len(frame) {
				t.Fatalf("Size=%d, encoded %d: %+v", size, len(frame), messages)
			}
			decoded, err := Decode(frame, o)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, messages) {
				t.Fatalf("round trip changed\n got %+v\nwant %+v", decoded, messages)
			}
		}
		encoded, err := body(messages)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, m := range messages {
			n += MessageSize(m)
		}
		if n != len(encoded) {
			t.Fatalf("MessageSize sum %d, body %d", n, len(encoded))
		}
	}
}

// TestRecordSizes pins the per-record cost the queue and datagram budget see.
func TestRecordSizes(t *testing.T) {
	self := identity("node-012", "10.0.0.12:7946", 0)
	peer := identity("node-345", "10.0.3.45:7946", 0)
	meta := identity("node-012", "10.0.0.12:7946", 24)
	alive := Member{Identity: meta, Incarnation: 3}
	accusation := Accusation{Node: "node-345", Incarnation: 3, From: "node-012"}
	for _, tc := range []struct {
		name  string
		m     Message
		frame int
	}{
		{"ack", Message{Ack: &Ack{Sequence: 1 << 62}}, 25},
		{"suspect", Message{Suspect: &accusation}, 36},
		{"ping", Message{Ping: &Ping{Sequence: 1 << 62, Target: "node-345", Source: self}}, 51},
		{"indirect", Message{Indirect: &Indirect{Sequence: 1 << 62, Target: peer, Source: self}}, 59},
		{"alive with 24 B meta", Message{Alive: &alive}, 60},
	} {
		size, err := Size([]Message{tc.m}, "", false)
		if err != nil {
			t.Fatal(err)
		}
		if size != tc.frame {
			t.Errorf("%s: frame %d bytes, want %d", tc.name, size, tc.frame)
		}
		t.Logf("%s: %d-byte frame, %d-byte record; %d per 1,400-byte datagram", tc.name, size, MessageSize(tc.m), (1400-HeaderSize-4)/MessageSize(tc.m))
	}
}

func TestRejectedBodies(t *testing.T) {
	ack := []byte{tagAck, 0, 0, 0, 0, 0, 0, 0, 7}
	alive := func(state byte) []byte {
		return []byte{tagAlive, 1, 'a', 4, 192, 0, 2, 1, 0, 1, 0, 1, state}
	}
	if _, err := DecodeBody(alive(0)); err != nil {
		t.Fatal("reference alive rejected", err)
	}
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"tag 0", []byte{0}},
		{"tag 9", []byte{9, 0, 0, 0, 0, 0, 0, 0, 7}},
		{"short sequence", ack[:8]},
		{"trailing byte", append(bytes.Clone(ack), 0)},
		{"trailing partial message", append(bytes.Clone(ack), ack[:5]...)},
		{"alive state 1", alive(1)},
		{"state 4", []byte{tagExchange, 1, 1, 'a', 4, 192, 0, 2, 1, 0, 1, 0, 1, 4}},
		{"address length 5", []byte{tagAlive, 1, 'a', 5, 192, 0, 2, 1, 0, 0, 1, 0, 1, 0}},
		{"zero port", []byte{tagAlive, 1, 'a', 4, 192, 0, 2, 1, 0, 0, 0, 1, 0}},
		{"overlong incarnation", []byte{tagAlive, 1, 'a', 4, 192, 0, 2, 1, 0, 1, 0, 0x81, 0x00, 0}},
		{"overlong name length", []byte{tagAlive, 0x81, 0x00, 'a', 4, 192, 0, 2, 1, 0, 1, 0, 1, 0}},
		{"uvarint overflow", append([]byte{tagSuspect, 1, 'a'}, bytes.Repeat([]byte{0xff}, 11)...)},
		{"name over limit", append(append([]byte{tagSuspect, MaxName + 1}, bytes.Repeat([]byte{'a'}, MaxName+1)...), 1, 1, 'b')},
		{"name past end", []byte{tagSuspect, 10, 'a'}},
		{"invalid UTF-8 name", []byte{tagSuspect, 1, 0xff, 1, 1, 'b'}},
		{"empty accuser", []byte{tagSuspect, 1, 'a', 1, 0}},
		{"meta over limit", append(append([]byte{tagAlive, 1, 'a', 4, 192, 0, 2, 1, 0, 1, 0x81, 0x04}, make([]byte, MaxMeta+1)...), 1, 0)},
		{"count past remaining bytes", []byte{tagExchange, 2, 1, 'a', 4, 192, 0, 2, 1, 0, 1, 0, 1, 0}},
		{"count over limit", append([]byte{tagExchange, 0x81, 0x80, 0x01}, make([]byte, minMember*(MaxNodes+1))...)},
		{"batch over limit", bytes.Repeat(ack, MaxBatch+1)},
	} {
		if _, err := DecodeBody(tc.body); err == nil {
			t.Errorf("%s: accepted %x", tc.name, tc.body)
		}
	}
	if _, err := DecodeBody(bytes.Repeat(ack, MaxBatch)); err != nil {
		t.Fatal("batch at limit rejected", err)
	}
}

func TestRejectedHeaders(t *testing.T) {
	frame, _ := hex.DecodeString(ackFixture)
	for _, tc := range []struct {
		name   string
		offset int
		value  byte
	}{
		{"version 1", 4, 1},
		{"version 3", 4, 3},
		{"flag bit 0", 5, 1},
		{"flag bit 2", 5, 4},
	} {
		b := bytes.Clone(frame)
		b[tc.offset] = tc.value
		if _, err := Decode(b, Options{}); err == nil {
			t.Errorf("%s accepted", tc.name)
		}
	}
}

// A small frame cannot make the decoder allocate for a large declared count.
func TestDeclaredCountsAllocateNothingFirst(t *testing.T) {
	for _, body := range [][]byte{
		{tagExchange, 0x80, 0x80, 0x01, 1, 'a', 4, 192, 0, 2, 1, 0, 1, 0, 1, 0},
		append([]byte{tagExchange, 0xff, 0xff, 0x03}, make([]byte, 64)...),
	} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range 100 {
			if _, err := DecodeBody(body); err == nil {
				t.Fatal("accepted")
			}
		}
		runtime.ReadMemStats(&after)
		if perRun := (after.TotalAlloc - before.TotalAlloc) / 100; perRun > 1024 {
			t.Fatalf("%d bytes allocated per refused count", perRun)
		}
	}
}

func TestProtectionAndKeys(t *testing.T) {
	messages := []Message{{Exchange: &Exchange{Members: bulkMembers(400)}}}
	a, b := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 32)
	for _, o := range []Options{{Label: "cluster"}, {Label: "cluster", Keys: [][]byte{a}}} {
		frame, err := Encode(messages, o)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(frame, o)
		if err != nil || len(decoded[0].Exchange.Members) != 400 {
			t.Fatal(err)
		}
		if _, err = Decode(frame, Options{Label: "wrong", Keys: o.Keys}); err == nil {
			t.Fatal("wrong label")
		}
		changed := bytes.Clone(frame)
		changed[len(changed)-1] ^= 1
		if _, err = Decode(changed, o); err == nil {
			t.Fatal("tampering accepted")
		}
	}
	plaintext, _ := Encode(messages, Options{Label: "cluster"})
	if _, err := Decode(plaintext, Options{Label: "cluster", Keys: [][]byte{a}}); err == nil {
		t.Fatal("plaintext accepted")
	}
	protected, _ := Encode(messages, Options{Keys: [][]byte{b}})
	if _, err := Decode(protected, Options{Keys: [][]byte{a, b}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(protected, Options{Keys: [][]byte{a}}); err == nil {
		t.Fatal("wrong key accepted")
	}
	second, _ := Encode(messages, Options{Keys: [][]byte{b}})
	if bytes.Equal(protected, second) {
		t.Fatal("nonce reused")
	}
}
func TestDefaultPacketCosts(t *testing.T) {
	for _, addr := range []string{"192.0.2.255:65535", "[2001:db8:ffff:ffff:ffff:ffff:ffff:ffff]:65535"} {
		n := Member{Identity: Identity{Name: strings.Repeat("n", MaxName), Address: netip.MustParseAddrPort(addr), Meta: bytes.Repeat([]byte{255}, MaxMeta)}, Incarnation: ^uint64(0), State: Alive}
		for _, label := range []string{"", strings.Repeat("l", MaxLabel)} {
			for _, protected := range []bool{false, true} {
				messages := []Message{{Alive: &n}}
				size, err := Size(messages, label, protected)
				if err != nil {
					t.Fatal(err)
				}
				if size > 1400 {
					t.Fatalf("control record exceeds unchanged budget: %d", size)
				}
				o := Options{Label: label}
				if protected {
					o.Keys = [][]byte{bytes.Repeat([]byte{3}, 32)}
				}
				frame, err := Encode(messages, o)
				if err != nil || len(frame) != size {
					t.Fatal(size, len(frame), err)
				}
				t.Logf("alive %s label=%d protected=%v: %d bytes", addr, len(label), protected, size)
			}
		}
	}
}

// Every frame the decoder accepts re-encodes to the identical bytes.
func FuzzFrame(f *testing.F) {
	b, _ := hex.DecodeString(ackFixture)
	f.Add(b)
	f.Add([]byte("old memberlist"))
	for _, messages := range variants()[:20] {
		frame, _ := Encode(messages, Options{})
		f.Add(frame)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		messages, err := Decode(b, Options{})
		if err != nil {
			return
		}
		encoded, err := Encode(messages, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded, b) {
			t.Fatalf("accepted frame is not canonical\n got %x\nwant %x", encoded, b)
		}
	})
}

// Every body the decoder accepts re-encodes to the identical bytes, and the
// arithmetic sizes agree with it.
func FuzzBody(f *testing.F) {
	for _, messages := range variants() {
		encoded, _ := body(messages)
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		messages, err := DecodeBody(b)
		if err != nil {
			return
		}
		encoded, err := body(messages)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded, b) {
			t.Fatalf("accepted body is not canonical\n got %x\nwant %x", encoded, b)
		}
		if n, err := bodySize(messages); err != nil || n != len(b) {
			t.Fatal("size disagrees", n, len(b), err)
		}
	})
}

// Every valid message decodes to an equal message.
func FuzzMessageRoundTrip(f *testing.F) {
	f.Add(uint8(0), uint64(7), "a", "b", []byte("meta"), []byte{192, 0, 2, 1}, uint16(7946), uint8(0))
	f.Add(uint8(7), uint64(1<<40), "é", "", []byte(nil), bytes.Repeat([]byte{1}, 16), uint16(1), uint8(3))
	f.Fuzz(func(t *testing.T, kind uint8, n uint64, name, other string, meta, ip []byte, port uint16, state uint8) {
		addr, ok := netip.AddrFromSlice(ip)
		if !ok {
			return
		}
		i := Identity{Name: name, Address: netip.AddrPortFrom(addr, port), Meta: meta}
		if len(i.Meta) == 0 {
			i.Meta = nil // the decoder yields nil for empty bytes
		}
		member := Member{Identity: i, Incarnation: n, State: State(state)}
		accusation := Accusation{Node: name, Incarnation: n, From: other}
		var m Message
		switch kind % 8 {
		case 0:
			m.Ping = &Ping{Sequence: n, Target: other, Source: i}
		case 1:
			m.Indirect = &Indirect{Sequence: n, Target: i, Source: i}
		case 2:
			m.Ack = &Ack{Sequence: n}
		case 3:
			m.Nack = &Ack{Sequence: n}
		case 4:
			m.Alive = &member
		case 5:
			m.Suspect = &accusation
		case 6:
			m.Dead = &accusation
		case 7:
			m.Exchange = &Exchange{Members: []Member{member, member}}
		}
		if m.Validate() != nil {
			return
		}
		frame, err := Encode([]Message{m}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(frame, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, []Message{m}) {
			t.Fatalf("round trip changed\n got %+v\nwant %+v", decoded[0], m)
		}
	})
}

func BenchmarkCodec(b *testing.B) {
	for _, name := range []string{"probe", "gossip", "push-pull"} {
		b.Run(name, func(b *testing.B) {
			n := Member{Identity: Identity{Name: "node", Address: netip.MustParseAddrPort("127.0.0.1:7946")}, Incarnation: 1}
			var m Message
			switch name {
			case "probe":
				m.Ping = &Ping{Sequence: 1, Target: "target", Source: n.Identity}
			case "gossip":
				m.Alive = &n
			case "push-pull":
				m.Exchange = &Exchange{Members: []Member{n, n, n, n, n, n, n, n, n, n}}
			}
			frame, _ := Encode([]Message{m}, Options{})
			b.ReportAllocs()
			for b.Loop() {
				encoded, err := Encode([]Message{m}, Options{})
				if err != nil {
					b.Fatal(err)
				}
				if _, err = Decode(encoded, Options{}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(frame)), "bytes/frame")
		})
	}
}
func bulkMembers(count int) []Member {
	n := Member{Identity: Identity{Name: "node", Address: netip.MustParseAddrPort("127.0.0.1:7946")}, Incarnation: 1}
	members := make([]Member, count)
	for j := range members {
		members[j] = n
	}
	return members
}
