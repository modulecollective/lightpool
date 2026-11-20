// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package wire

import (
	"bytes"
	"compress/lzw"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"strconv"
	"unicode/utf8"
)

const HeaderSize = 12
const Version = 1
const compressed = 1
const encrypted = 2
const MaxFrame = HeaderSize + MaxLabel + MaxBody + 28

// Options is an immutable codec configuration. Keys[0] encrypts; every key may
// decrypt. A nonempty key list requires encryption on all incoming frames.
type Options struct {
	Label    string
	Compress bool
	Keys     [][]byte
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
func body(messages []Message) ([]byte, error) {
	if len(messages) == 0 || len(messages) > MaxBatch {
		return nil, errors.New("invalid batch count")
	}
	for _, m := range messages {
		if err := m.Validate(); err != nil {
			return nil, err
		}
	}
	b, err := json.Marshal(messages)
	if len(b) > MaxBody {
		return nil, errors.New("body exceeds limit")
	}
	return b, err
}

// Size is the exact uncompressed frame size, including all protection overhead.
// Compression can only reduce this upper bound. It never needs entropy.
func Size(messages []Message, label string, protect bool) (int, error) {
	b, err := body(messages)
	if err != nil {
		return 0, err
	}
	n := HeaderSize + len(label) + len(b) + 4
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
	if o.Compress {
		var z bytes.Buffer
		w := lzw.NewWriter(&z, lzw.LSB, 8)
		if _, err = w.Write(b); err != nil {
			return nil, err
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		if z.Len() < len(b) {
			b = z.Bytes()
			flags |= compressed
		}
	}
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
	if len(h) < HeaderSize || string(h[:4]) != "LPL!" || h[4] != Version || h[5]&^(byte(compressed|encrypted)) != 0 {
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
	if frame[5]&compressed != 0 {
		input := bytes.NewReader(b)
		r := lzw.NewReader(input, lzw.LSB, 8)
		plain, e := io.ReadAll(io.LimitReader(r, MaxBody+1))
		closeErr := r.Close()
		if e != nil {
			return nil, e
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if input.Len() != 0 {
			return nil, errors.New("trailing compressed content")
		}
		if len(plain) > MaxBody {
			return nil, errors.New("decompression limit")
		}
		b = plain
	}
	return DecodeBody(b)
}

// DecodeBody also serves external drivers and native fuzzing. JSON's normal
// replacement of invalid UTF-8 or unpaired surrogates is refused before decode.
func DecodeBody(b []byte) ([]Message, error) {
	if len(b) > MaxBody || !utf8.Valid(b) || !validStrings(b) || !boundedJSON(b) {
		return nil, errors.New("invalid body text")
	}
	var messages []Message
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&messages); err != nil {
		return nil, err
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return nil, errors.New("trailing body content")
	}
	if len(messages) == 0 || len(messages) > MaxBatch {
		return nil, errors.New("invalid batch count")
	}
	for _, m := range messages {
		if err := m.Validate(); err != nil {
			return nil, err
		}
	}
	return messages, nil
}
func validStrings(b []byte) bool {
	for j := 0; j < len(b); j++ {
		if b[j] != '"' {
			continue
		}
		j++
		for ; j < len(b) && b[j] != '"'; j++ {
			if b[j] != '\\' {
				continue
			}
			j++
			if j >= len(b) {
				return false
			}
			if b[j] != 'u' {
				continue
			}
			if j+4 >= len(b) {
				return false
			}
			v, err := strconv.ParseUint(string(b[j+1:j+5]), 16, 16)
			if err != nil {
				return false
			}
			j += 4
			if v >= 0xdc00 && v <= 0xdfff {
				return false
			}
			if v >= 0xd800 && v <= 0xdbff {
				if j+6 >= len(b) || b[j+1] != '\\' || b[j+2] != 'u' {
					return false
				}
				w, e := strconv.ParseUint(string(b[j+3:j+7]), 16, 16)
				if e != nil || w < 0xdc00 || w > 0xdfff {
					return false
				}
				j += 6
			}
		}
	}
	return true
}

// Check array counts before encoding/json allocates the typed slices. A tiny
// frame containing millions of empty objects must not expand into a huge batch.
func boundedJSON(b []byte) bool {
	type level struct {
		array bool
		count int
	}
	var stack [16]level
	depth := 0
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch c {
		case ' ', '\t', '\n', '\r', ',', ':':
			continue
		case ']', '}':
			if depth == 0 {
				return false
			}
			depth--
			continue
		}
		if depth > 0 && stack[depth-1].array {
			l := &stack[depth-1]
			l.count++
			limit := MaxNodes
			if depth == 1 {
				limit = MaxBatch
			}
			if l.count > limit {
				return false
			}
		}
		switch c {
		case '[', '{':
			if depth == len(stack) {
				return false
			}
			stack[depth] = level{array: c == '['}
			depth++
		case '"':
			// Skip the escaped byte too: even/odd backslash runs then naturally
			// distinguish an escaped quote from the string's closing quote.
			i++
			for ; i < len(b) && b[i] != '"'; i++ {
				if b[i] == '\\' {
					i++
				}
			}
			if i >= len(b) {
				return false
			}
		default:
			// Scalar tokens count once, regardless of their spelling or length.
			// json.Valid below owns syntax, delimiters and token validation.
			for i+1 < len(b) {
				next := b[i+1]
				if next == ',' || next == ']' || next == '}' || next == ' ' || next == '\t' || next == '\n' || next == '\r' {
					break
				}
				i++
			}
		}
	}
	// Bound depth/counts before even the standard library's syntax scan can
	// grow its nesting stack; typed member/message expansion happens later.
	return depth == 0 && json.Valid(b)
}
