// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"net/netip"
	"slices"

	"github.com/modulecollective/lightpool/wire"
)

type broadcast struct {
	message         wire.Message
	key             string
	id              uint64
	size, transmits int
	command         uint64
}

func (c *Core) finishBroadcast(b *broadcast, f Failure) {
	if b.command != 0 {
		c.emit(Result{Command: b.command, Failure: f})
		b.command = 0
	}
}

func (c *Core) queue(m wire.Message, name string, command uint64) {
	if old := c.broadcastKeys[name]; old != nil {
		c.finishBroadcast(old, Success)
		c.broadcasts = slices.DeleteFunc(c.broadcasts, func(b *broadcast) bool { return b == old })
	}
	if command != 0 && !c.hasPeers() {
		c.emit(Result{Command: command})
		command = 0
	}
	record := &broadcast{message: m.Clone(), key: name, id: c.token().ID, size: wire.MessageSize(m), command: command}
	c.broadcasts = append(c.broadcasts, record)
	c.broadcastKeys[name] = record
}

func broadcastCompare(a, b *broadcast) int {
	if v := cmp.Compare(a.transmits, b.transmits); v != 0 {
		return v
	}
	if v := cmp.Compare(b.size, a.size); v != 0 {
		return v
	}
	return cmp.Compare(b.id, a.id)
}

func (c *Core) pack(base []wire.Message) []wire.Message {
	// Sort a selection snapshot before updating any transmit counts, ensuring
	// each record appears at most once in a packet (memberlist queue semantics).
	records := slices.Clone(c.broadcasts)
	slices.SortFunc(records, broadcastCompare)
	used := frameOverhead(c.config)
	if len(base) > 0 {
		n, err := wire.Size(base, c.config.Label, c.config.Protected)
		if err != nil {
			panic(err)
		}
		used = n
	}
	messages := slices.Clone(base)
	sent := make(map[*broadcast]bool)
	limit := retransmitLimit(c.config.RetransmitMult, len(c.nodes))
	for _, b := range records {
		if used+b.size > c.config.PacketBudget || len(messages) >= wire.MaxBatch {
			continue
		}
		used += b.size
		messages = append(messages, b.message.Clone())
		b.transmits++
		if b.transmits >= limit {
			sent[b] = true
			c.finishBroadcast(b, Success)
			delete(c.broadcastKeys, b.key)
		}
	}
	if len(sent) > 0 {
		c.broadcasts = slices.DeleteFunc(c.broadcasts, func(b *broadcast) bool { return sent[b] })
	}
	return messages
}

// frameOverhead is the header, label, checksum and AEAD overhead of a frame.
func frameOverhead(config Config) int {
	n := wire.HeaderSize + len(config.Label) + 4
	if config.Protected {
		n += 24
	}
	return n
}

func (c *Core) datagram(to netip.AddrPort, messages []wire.Message, probe uint64) {
	n, err := wire.Size(messages, c.config.Label, c.config.Protected)
	if len(messages) > 0 && (err != nil || n > c.config.PacketBudget) {
		if probe != 0 {
			c.finishProbe(probe, LocalFailure, 0)
		}
		return
	}
	messages = c.pack(messages)
	if len(messages) == 0 {
		return
	}
	id := c.token()
	if probe != 0 {
		c.sends[id.ID] = probe
	}
	c.emit(SendDatagram{ID: id, To: to, Messages: messages})
}
