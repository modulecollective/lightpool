// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

// Package wire defines Lightpool's shared, versioned datagram and stream format.
package wire

import (
	"errors"
	"net/netip"
	"unicode/utf8"
)

const (
	MaxName  = 64
	MaxLabel = 64
	MaxMeta  = 512
	MaxData  = 1 << 20
	MaxNodes = 16384
	MaxBatch = 256
	MaxBody  = 8 << 20
)

type State uint8

const (
	Alive State = iota
	Suspect
	Dead
	Left
)

type Identity struct {
	Name    string         `json:"name"`
	Address netip.AddrPort `json:"address"`
	Meta    []byte         `json:"meta,omitempty"`
	// Application is a distinct application-data compatibility identifier.
	Application string `json:"application,omitempty"`
}

func ValidText(s string, max int) bool { return len(s) <= max && utf8.ValidString(s) }
func (i Identity) Validate() error {
	if i.Name == "" || !ValidText(i.Name, MaxName) || !ValidText(i.Application, MaxName) || !i.Address.IsValid() || i.Address.Port() == 0 || i.Address.Addr().IsUnspecified() || i.Address.Addr().Zone() != "" || len(i.Meta) > MaxMeta {
		return errors.New("invalid identity")
	}
	return nil
}

type Member struct {
	Identity
	Incarnation uint64 `json:"incarnation"`
	State       State  `json:"state"`
}
type Ping struct {
	Sequence uint64   `json:"sequence"`
	Target   string   `json:"target"`
	Source   Identity `json:"source"`
}
type Indirect struct {
	Sequence uint64   `json:"sequence"`
	Target   Identity `json:"target"`
	Source   Identity `json:"source"`
}
type Ack struct {
	Sequence uint64 `json:"sequence"`
	Payload  []byte `json:"payload,omitempty"`
}
type Accusation struct {
	Node        string `json:"node"`
	Incarnation uint64 `json:"incarnation"`
	From        string `json:"from"`
}
type Exchange struct {
	Join    bool     `json:"join"`
	Members []Member `json:"members"`
	Data    []byte   `json:"data,omitempty"`
}

// Message is a closed tagged union: exactly one field must be set. No maps or
// custom JSON decoding hooks occur anywhere in the body grammar.
type Message struct {
	Ping     *Ping       `json:"ping,omitempty"`
	Indirect *Indirect   `json:"indirect,omitempty"`
	Ack      *Ack        `json:"ack,omitempty"`
	Nack     *Ack        `json:"nack,omitempty"`
	Alive    *Member     `json:"alive,omitempty"`
	Suspect  *Accusation `json:"suspect,omitempty"`
	Dead     *Accusation `json:"dead,omitempty"`
	Exchange *Exchange   `json:"exchange,omitempty"`
	User     *[]byte     `json:"user,omitempty"`
}

func (m Message) Validate() error {
	count := 0
	if m.Ping != nil {
		count++
		if m.Ping.Source.Validate() != nil || m.Ping.Target == "" || !ValidText(m.Ping.Target, MaxName) {
			return errors.New("invalid ping")
		}
	}
	if m.Indirect != nil {
		count++
		if m.Indirect.Source.Validate() != nil || m.Indirect.Target.Validate() != nil {
			return errors.New("invalid indirect ping")
		}
	}
	for _, a := range []*Ack{m.Ack, m.Nack} {
		if a != nil {
			count++
			if len(a.Payload) > MaxMeta {
				return errors.New("oversized ack")
			}
		}
	}
	if m.Alive != nil {
		count++
		if m.Alive.Identity.Validate() != nil || m.Alive.State != Alive {
			return errors.New("invalid alive")
		}
	}
	for _, a := range []*Accusation{m.Suspect, m.Dead} {
		if a != nil {
			count++
			if a.Node == "" || a.From == "" || !ValidText(a.Node, MaxName) || !ValidText(a.From, MaxName) {
				return errors.New("invalid accusation")
			}
		}
	}
	if m.Exchange != nil {
		count++
		if len(m.Exchange.Members) > MaxNodes || len(m.Exchange.Data) > MaxData {
			return errors.New("oversized exchange")
		}
		for _, n := range m.Exchange.Members {
			if n.Identity.Validate() != nil || n.State > Left {
				return errors.New("invalid member")
			}
		}
	}
	if m.User != nil {
		count++
		if len(*m.User) > MaxData {
			return errors.New("oversized user data")
		}
	}
	if count != 1 {
		return errors.New("message requires exactly one variant")
	}
	return nil
}

// Clone makes ownership explicit at event/effect and snapshot boundaries.
func (i Identity) Clone() Identity { i.Meta = append([]byte(nil), i.Meta...); return i }
func (m Member) Clone() Member     { m.Identity = m.Identity.Clone(); return m }
func (m Message) Clone() Message {
	if m.Ping != nil {
		p := *m.Ping
		p.Source = p.Source.Clone()
		m.Ping = &p
	}
	if m.Indirect != nil {
		p := *m.Indirect
		p.Source = p.Source.Clone()
		p.Target = p.Target.Clone()
		m.Indirect = &p
	}
	if m.Ack != nil {
		p := *m.Ack
		p.Payload = append([]byte(nil), p.Payload...)
		m.Ack = &p
	}
	if m.Nack != nil {
		p := *m.Nack
		p.Payload = append([]byte(nil), p.Payload...)
		m.Nack = &p
	}
	if m.Alive != nil {
		p := m.Alive.Clone()
		m.Alive = &p
	}
	if m.Suspect != nil {
		p := *m.Suspect
		m.Suspect = &p
	}
	if m.Dead != nil {
		p := *m.Dead
		m.Dead = &p
	}
	if m.Exchange != nil {
		p := *m.Exchange
		p.Data = append([]byte(nil), p.Data...)
		p.Members = make([]Member, len(m.Exchange.Members))
		for j, n := range m.Exchange.Members {
			p.Members[j] = n.Clone()
		}
		m.Exchange = &p
	}
	if m.User != nil {
		p := append([]byte(nil), (*m.User)...)
		m.User = &p
	}
	return m
}
