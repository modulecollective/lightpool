// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package wire

import (
	"bytes"
	"compress/lzw"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/netip"
	"strings"
	"testing"
)

var ackFixture = "4c504c21010000000000001c5b7b2261636b223a7b2273657175656e6365223a377d7d5da1448745"

func TestEmptyUserRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {1}} {
		for _, clone := range []bool{false, true} {
			m := Message{User: &payload}
			if clone {
				m = m.Clone()
			}
			frame, err := Encode([]Message{m}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			size, err := Size([]Message{m}, "", false)
			if err != nil || size != len(frame) {
				t.Fatalf("empty-user frame budget: size=%d encoded=%d err=%v", size, len(frame), err)
			}
			if !clone && payload == nil && *m.User != nil {
				t.Fatal("codec mutated caller's nil payload")
			}
			decoded, err := Decode(frame, Options{})
			if err != nil {
				t.Errorf("payload=%v clone=%v: %v", payload, clone, err)
				continue
			}
			if len(decoded) != 1 || decoded[0].User == nil || !bytes.Equal(*decoded[0].User, payload) {
				t.Fatalf("payload changed: %+v", decoded)
			}
			if len(payload) == 0 && !bytes.Contains(frame, []byte(`{"user":""}`)) {
				t.Fatalf("empty user is not canonical: %s", frame)
			}
		}
	}
}

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
func TestStrictBodies(t *testing.T) {
	cases := []string{`null`, `[]`, `[{}]`, `[{"ack":{"sequence":7,"extra":1}}]`, `[{"ack":{"sequence":7},"nack":{"sequence":7}}]`, `[{"ack":{"sequence":7}}] {}`, `[{"alive":{"name":"\ud800","address":"127.0.0.1:1","incarnation":1,"state":0}}]`, `[{"alive":{"name":"a","address":"127.0.0.1:0","incarnation":1,"state":0}}]`, `[{"alive":{"name":"a","address":"127.0.0.1:1","incarnation":1,"state":0,"unknown":1}}]`, "[" + strings.Repeat(`{"ack":{"sequence":7}},`, MaxBatch) + `{"ack":{"sequence":7}}]`}
	for _, b := range cases {
		if _, err := DecodeBody([]byte(b)); err == nil {
			t.Fatalf("accepted invalid body %.120s", b)
		}
	}
	invalid := []byte(`[{"alive":{"name":"x","address":"127.0.0.1:1","incarnation":1,"state":0}}]`)
	invalid[19] = 255
	if _, err := DecodeBody(invalid); err == nil {
		t.Fatal("invalid UTF-8")
	}
}
func TestProtectionCompressionAndKeys(t *testing.T) {
	data := bytes.Repeat([]byte("payload"), 10000)
	messages := []Message{{User: &data}}
	a, b := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 32)
	for _, o := range []Options{{Label: "cluster"}, {Label: "cluster", Compress: true}, {Label: "cluster", Compress: true, Keys: [][]byte{a}}} {
		frame, err := Encode(messages, o)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(frame, o)
		if err != nil || !bytes.Equal(*decoded[0].User, data) {
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
		if o.Compress && len(frame) > len(data)/10 {
			t.Fatalf("not compressed: %d", len(frame))
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
func TestDecompressionLimit(t *testing.T) {
	var compressed bytes.Buffer
	w := lzw.NewWriter(&compressed, lzw.LSB, 8)
	chunk := bytes.Repeat([]byte{' '}, 4096)
	for j := 0; j <= MaxBody/len(chunk); j++ {
		if _, err := w.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	h := []byte{'L', 'P', 'L', '!', 1, 1, 0, 0}
	h = binary.BigEndian.AppendUint32(h, uint32(compressed.Len()+4))
	h = append(h, compressed.Bytes()...)
	h = binary.BigEndian.AppendUint32(h, crc32.ChecksumIEEE(h))
	if _, err := Decode(h, Options{}); err == nil {
		t.Fatal("decompression exceeded limit")
	}
}
func TestDefaultPacketCosts(t *testing.T) {
	for _, addr := range []string{"192.0.2.255:65535", "[2001:db8:ffff:ffff:ffff:ffff:ffff:ffff]:65535"} {
		n := Member{Identity: Identity{Name: strings.Repeat("n", MaxName), Address: netip.MustParseAddrPort(addr), Meta: bytes.Repeat([]byte{255}, MaxMeta), Application: strings.Repeat("a", MaxName)}, Incarnation: ^uint64(0), State: Alive}
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
func FuzzFrame(f *testing.F) {
	b, _ := hex.DecodeString(ackFixture)
	f.Add(b)
	f.Add([]byte("old memberlist"))
	f.Fuzz(func(t *testing.T, b []byte) {
		messages, err := Decode(b, Options{})
		if err != nil {
			return
		}
		encoded, err := Encode(messages, Options{})
		if err != nil {
			t.Fatal(err)
		}
		round, err := Decode(encoded, Options{})
		if err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(messages)
		z, _ := json.Marshal(round)
		if !bytes.Equal(a, z) {
			t.Fatal("logical message changed")
		}
	})
}
func FuzzBody(f *testing.F) {
	f.Add([]byte(`[{"ack":{"sequence":7}}]`))
	f.Fuzz(func(t *testing.T, b []byte) {
		messages, err := DecodeBody(b)
		if err != nil {
			return
		}
		encoded, err := body(messages)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodeBody(encoded); err != nil {
			t.Fatal(err)
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

func FuzzCompressedBody(f *testing.F) {
	var z bytes.Buffer
	w := lzw.NewWriter(&z, lzw.LSB, 8)
	_, _ = w.Write([]byte(`[{"ack":{"sequence":7}}]`))
	_ = w.Close()
	f.Add(z.Bytes())
	f.Add([]byte{0, 1, 255})
	f.Fuzz(func(t *testing.T, compressed []byte) {
		if len(compressed) > MaxBody {
			return
		}
		h := []byte{'L', 'P', 'L', '!', 1, 1, 0, 0}
		h = binary.BigEndian.AppendUint32(h, uint32(len(compressed)+4))
		h = append(h, compressed...)
		h = binary.BigEndian.AppendUint32(h, crc32.ChecksumIEEE(h))
		messages, err := Decode(h, Options{})
		if err == nil {
			b, err := Encode(messages, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Decode(b, Options{}); err != nil {
				t.Fatal(err)
			}
		}
	})
}
func TestCompressedTrailingBytesRejected(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, 500)
	frame, err := Encode([]Message{{User: &data}}, Options{Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	payload := append(bytes.Clone(frame[:len(frame)-4]), 0)
	binary.BigEndian.PutUint32(payload[8:12], binary.BigEndian.Uint32(payload[8:12])+1)
	payload = binary.BigEndian.AppendUint32(payload, crc32.ChecksumIEEE(payload))
	if _, err = Decode(payload, Options{}); err == nil {
		t.Fatal("trailing compressed data accepted")
	}
}
