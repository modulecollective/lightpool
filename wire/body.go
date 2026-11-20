// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package wire

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"net/netip"
)

// A body is 1 to MaxBatch messages, concatenated, consuming the body exactly.
// Each message is a tag byte followed by its fields in fixed order. Sequences
// are 8 bytes big-endian; incarnations, lengths and counts are minimal
// uvarints; text and bytes are a length followed by the bytes; an address is
// a length byte (4 or 16), the IP and a 2-byte big-endian port. Every value
// has exactly one encoding, so an accepted body re-encodes to the same bytes.
const (
	tagPing byte = 1 + iota
	tagIndirect
	tagAck
	tagNack
	tagAlive
	tagSuspect
	tagDead
	tagExchange
)

// minMember is the smallest encoded member: empty name and meta, IPv4 address,
// one-byte incarnation and the state byte. Counts are checked against it.
const minMember = 1 + 7 + 1 + 1 + 1

func uvarintSize(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }
func textSize(n int) int       { return uvarintSize(uint64(n)) + n }

func addressSize(a netip.AddrPort) int {
	if a.Addr().Is4() {
		return 1 + 4 + 2
	}
	return 1 + 16 + 2
}

func identitySize(i Identity) int {
	return textSize(len(i.Name)) + addressSize(i.Address) + textSize(len(i.Meta))
}

func memberSize(m Member) int { return identitySize(m.Identity) + uvarintSize(m.Incarnation) + 1 }

func accusationSize(a Accusation) int {
	return textSize(len(a.Node)) + uvarintSize(a.Incarnation) + textSize(len(a.From))
}

// MessageSize is the encoded body size of one valid message.
func MessageSize(m Message) int {
	n := 1
	switch {
	case m.Ping != nil:
		n += 8 + textSize(len(m.Ping.Target)) + identitySize(m.Ping.Source)
	case m.Indirect != nil:
		n += 8 + identitySize(m.Indirect.Target) + identitySize(m.Indirect.Source)
	case m.Ack != nil, m.Nack != nil:
		n += 8
	case m.Alive != nil:
		n += memberSize(*m.Alive)
	case m.Suspect != nil:
		n += accusationSize(*m.Suspect)
	case m.Dead != nil:
		n += accusationSize(*m.Dead)
	case m.Exchange != nil:
		n += uvarintSize(uint64(len(m.Exchange.Members)))
		for _, member := range m.Exchange.Members {
			n += memberSize(member)
		}
	}
	return n
}

func bodySize(messages []Message) (int, error) {
	if len(messages) == 0 || len(messages) > MaxBatch {
		return 0, errors.New("invalid batch count")
	}
	n := 0
	for _, m := range messages {
		if err := m.Validate(); err != nil {
			return 0, err
		}
		n += MessageSize(m)
	}
	if n > MaxBody {
		return 0, errors.New("body exceeds limit")
	}
	return n, nil
}

func appendText(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

func appendBytes(b, v []byte) []byte {
	return append(binary.AppendUvarint(b, uint64(len(v))), v...)
}

func appendAddress(b []byte, a netip.AddrPort) []byte {
	ip := a.Addr()
	if ip.Is4() {
		v := ip.As4()
		b = append(append(b, 4), v[:]...)
	} else {
		v := ip.As16()
		b = append(append(b, 16), v[:]...)
	}
	return binary.BigEndian.AppendUint16(b, a.Port())
}

func appendIdentity(b []byte, i Identity) []byte {
	return appendBytes(appendAddress(appendText(b, i.Name), i.Address), i.Meta)
}

func appendMember(b []byte, m Member) []byte {
	return append(binary.AppendUvarint(appendIdentity(b, m.Identity), m.Incarnation), byte(m.State))
}

func appendAccusation(b []byte, a Accusation) []byte {
	return appendText(binary.AppendUvarint(appendText(b, a.Node), a.Incarnation), a.From)
}

func appendMessage(b []byte, m Message) []byte {
	switch {
	case m.Ping != nil:
		b = binary.BigEndian.AppendUint64(append(b, tagPing), m.Ping.Sequence)
		b = appendIdentity(appendText(b, m.Ping.Target), m.Ping.Source)
	case m.Indirect != nil:
		b = binary.BigEndian.AppendUint64(append(b, tagIndirect), m.Indirect.Sequence)
		b = appendIdentity(appendIdentity(b, m.Indirect.Target), m.Indirect.Source)
	case m.Ack != nil:
		b = binary.BigEndian.AppendUint64(append(b, tagAck), m.Ack.Sequence)
	case m.Nack != nil:
		b = binary.BigEndian.AppendUint64(append(b, tagNack), m.Nack.Sequence)
	case m.Alive != nil:
		b = appendMember(append(b, tagAlive), *m.Alive)
	case m.Suspect != nil:
		b = appendAccusation(append(b, tagSuspect), *m.Suspect)
	case m.Dead != nil:
		b = appendAccusation(append(b, tagDead), *m.Dead)
	case m.Exchange != nil:
		b = binary.AppendUvarint(append(b, tagExchange), uint64(len(m.Exchange.Members)))
		for _, member := range m.Exchange.Members {
			b = appendMember(b, member)
		}
	}
	return b
}

// body validates the batch and encodes it.
func body(messages []Message) ([]byte, error) {
	n, err := bodySize(messages)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, n)
	for _, m := range messages {
		b = appendMessage(b, m)
	}
	return b, nil
}

var errBody = errors.New("malformed body")

// reader consumes a body. Every length and count is checked against its limit
// and against the bytes remaining before anything is allocated for it.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail() { r.err = errBody; r.b = nil }

func (r *reader) byte() byte {
	if r.err != nil || len(r.b) < 1 {
		r.fail()
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) fixed(n int) []byte {
	if r.err != nil || len(r.b) < n {
		r.fail()
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	// binary.Uvarint accepts overlong forms; only the minimal one is canonical.
	if n <= 0 || n != uvarintSize(v) {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) length(max int) int {
	n := r.uvarint()
	if r.err == nil && (n > uint64(max) || n > uint64(len(r.b))) {
		r.fail()
		return 0
	}
	return int(n)
}

func (r *reader) text() string { return string(r.fixed(r.length(MaxName))) }

func (r *reader) bytes(max int) []byte {
	n := r.length(max)
	if n == 0 {
		return nil
	}
	return append([]byte(nil), r.fixed(n)...)
}

func (r *reader) address() netip.AddrPort {
	n := int(r.byte())
	if n != 4 && n != 16 {
		r.fail()
	}
	v := r.fixed(n + 2)
	if r.err != nil {
		return netip.AddrPort{}
	}
	ip, _ := netip.AddrFromSlice(v[:n])
	return netip.AddrPortFrom(ip, binary.BigEndian.Uint16(v[n:]))
}

func (r *reader) sequence() uint64 {
	v := r.fixed(8)
	if r.err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func (r *reader) identity() Identity {
	return Identity{Name: r.text(), Address: r.address(), Meta: r.bytes(MaxMeta)}
}

func (r *reader) member() Member {
	m := Member{Identity: r.identity(), Incarnation: r.uvarint()}
	if s := r.byte(); s <= byte(Left) {
		m.State = State(s)
	} else {
		r.fail()
	}
	return m
}

func (r *reader) accusation() *Accusation {
	return &Accusation{Node: r.text(), Incarnation: r.uvarint(), From: r.text()}
}

func (r *reader) message() Message {
	var m Message
	switch r.byte() {
	case tagPing:
		m.Ping = &Ping{Sequence: r.sequence(), Target: r.text(), Source: r.identity()}
	case tagIndirect:
		m.Indirect = &Indirect{Sequence: r.sequence(), Target: r.identity(), Source: r.identity()}
	case tagAck:
		m.Ack = &Ack{Sequence: r.sequence()}
	case tagNack:
		m.Nack = &Ack{Sequence: r.sequence()}
	case tagAlive:
		n := r.member()
		m.Alive = &n
	case tagSuspect:
		m.Suspect = r.accusation()
	case tagDead:
		m.Dead = r.accusation()
	case tagExchange:
		count := r.uvarint()
		if r.err != nil || count > MaxNodes || count > uint64(len(r.b)/minMember) {
			r.fail()
			return m
		}
		var members []Member
		if count > 0 {
			members = make([]Member, count)
		}
		for j := range members {
			members[j] = r.member()
		}
		m.Exchange = &Exchange{Members: members}
	default:
		r.fail()
	}
	return m
}

// DecodeBody decodes and validates one body. It also serves external drivers
// and native fuzzing.
func DecodeBody(b []byte) ([]Message, error) {
	if len(b) == 0 || len(b) > MaxBody {
		return nil, errBody
	}
	r := reader{b: b}
	var messages []Message
	for len(r.b) > 0 && r.err == nil {
		if len(messages) == MaxBatch {
			return nil, errors.New("invalid batch count")
		}
		messages = append(messages, r.message())
	}
	if r.err != nil {
		return nil, r.err
	}
	for _, m := range messages {
		if err := m.Validate(); err != nil {
			return nil, err
		}
	}
	return messages, nil
}
