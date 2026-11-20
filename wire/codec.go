// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package wire

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const HeaderSize = 12
const Version = 2
const encrypted = 2
const MaxFrame = HeaderSize + MaxLabel + MaxBody + 28

// Options is an immutable codec configuration. Keys[0] encrypts; every key may
// decrypt. A nonempty key list requires encryption on all incoming frames.
type Options struct {
	Label string
	Keys  [][]byte
}

func (o Options) Validate() error {
	if !ValidText(o.Label, MaxLabel) {
		return errors.New("invalid label")
	}
	for _, k := range o.Keys {
		if _, err := aes.NewCipher(k); err != nil {
			return err
		}
	}
	return nil
}
func (o Options) Clone() Options {
	keys := o.Keys
	o.Keys = make([][]byte, len(keys))
	for j, k := range keys {
		o.Keys[j] = bytes.Clone(k)
	}
	return o
}

// Size is the exact frame size, including all protection overhead, computed
// without encoding.
func Size(messages []Message, label string, protect bool) (int, error) {
	n, err := bodySize(messages)
	if err != nil {
		return 0, err
	}
	n += HeaderSize + len(label) + 4
	if protect {
		n += 24
	}
	return n, nil
}
func Encode(messages []Message, o Options) ([]byte, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	b, err := body(messages)
	if err != nil {
		return nil, err
	}
	flags := byte(0)
	extra := 4
	if len(o.Keys) > 0 {
		flags |= encrypted
		extra = 28
	}
	h := make([]byte, HeaderSize+len(o.Label))
	copy(h, "LPL!")
	h[4] = Version
	h[5] = flags
	binary.BigEndian.PutUint16(h[6:8], uint16(len(o.Label)))
	binary.BigEndian.PutUint32(h[8:12], uint32(len(b)+extra))
	copy(h[HeaderSize:], o.Label)
	if flags&encrypted != 0 {
		block, _ := aes.NewCipher(o.Keys[0])
		g, err := cipher.NewGCMWithRandomNonce(block)
		if err != nil {
			return nil, err
		}
		return g.Seal(h, nil, b, h), nil
	}
	out := append(h, b...)
	sum := crc32.ChecksumIEEE(out)
	out = binary.BigEndian.AppendUint32(out, sum)
	return out, nil
}
func header(h []byte) (int, int, error) {
	if len(h) < HeaderSize || string(h[:4]) != "LPL!" || h[4] != Version || h[5]&^encrypted != 0 {
		return 0, 0, errors.New("unsupported frame header")
	}
	label := int(binary.BigEndian.Uint16(h[6:8]))
	n := int(binary.BigEndian.Uint32(h[8:12]))
	if label > MaxLabel || n > MaxBody+28 || n < 4 {
		return 0, 0, errors.New("invalid frame length")
	}
	return label, n, nil
}

// Read consumes exactly one frame, including fragmented reads. The header is
// checked before allocating the declared body. Further frames remain unread.
func Read(r io.Reader, o Options) ([]Message, error) {
	h := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, h); err != nil {
		return nil, err
	}
	label, n, err := header(h)
	if err != nil {
		return nil, err
	}
	b := make([]byte, HeaderSize+label+n)
	copy(b, h)
	if _, err = io.ReadFull(r, b[HeaderSize:]); err != nil {
		return nil, err
	}
	return Decode(b, o)
}
func Decode(frame []byte, o Options) ([]Message, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	label, n, err := header(frame)
	if err != nil {
		return nil, err
	}
	if len(frame) != HeaderSize+label+n {
		return nil, errors.New("frame length mismatch")
	}
	h := frame[:HeaderSize+label]
	if string(h[HeaderSize:]) != o.Label {
		return nil, errors.New("label mismatch")
	}
	b := frame[len(h):]
	if frame[5]&encrypted != 0 {
		if len(o.Keys) == 0 || len(b) < 28 {
			return nil, errors.New("encrypted frame without key or nonce")
		}
		var plain []byte
		for _, k := range o.Keys {
			block, _ := aes.NewCipher(k)
			g, e := cipher.NewGCMWithRandomNonce(block)
			if e != nil {
				return nil, e
			}
			plain, err = g.Open(nil, nil, b, h)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, errors.New("authentication failed")
		}
		b = plain
	} else {
		if len(o.Keys) > 0 {
			return nil, errors.New("plaintext rejected")
		}
		if binary.BigEndian.Uint32(b[len(b)-4:]) != crc32.ChecksumIEEE(frame[:len(frame)-4]) {
			return nil, errors.New("checksum mismatch")
		}
		b = b[:len(b)-4]
	}
	return DecodeBody(b)
}
