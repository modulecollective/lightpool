// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"encoding/json"
	"github.com/modulecollective/lightpool/wire"
	"net/netip"
	"slices"
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
	if old := c.broadcastKeys[name]; name != "" && old != nil {
		c.finishBroadcast(old, Success)
		c.broadcasts = slices.DeleteFunc(c.broadcasts, func(b *broadcast) bool { return b == old })
	}
	if command != 0 && !c.hasPeers() {
		c.emit(Result{Command: command})
		command = 0
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	record := &broadcast{message: m.Clone(), key: name, id: c.token().ID, size: len(b), command: command}
	c.broadcasts = append(c.broadcasts, record)
	if name != "" {
		c.broadcastKeys[name] = record
	}
}
func broadcastCompare(a, b *broadcast) int {
	if (a.message.User == nil) != (b.message.User == nil) {
		if a.message.User == nil {
			return -1
		}
		return 1
	}
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
		cost := b.size
		if len(messages) > 0 {
			cost++
		}
		if used+cost > c.config.PacketBudget || len(messages) >= wire.MaxBatch {
			continue
		}
		used += cost
		messages = append(messages, b.message.Clone())
		b.transmits++
		if b.transmits >= limit {
			sent[b] = true
			c.finishBroadcast(b, Success)
			if b.key != "" {
				delete(c.broadcastKeys, b.key)
			}
		}
	}
	if len(sent) > 0 {
		c.broadcasts = slices.DeleteFunc(c.broadcasts, func(b *broadcast) bool { return sent[b] })
	}
	return messages
}

// frameOverhead includes the JSON batch brackets, checksum, label and AEAD overhead.
func frameOverhead(config Config) int {
	n := wire.HeaderSize + len(config.Label) + 4 + 2
	if config.Protected {
		n += 24
	}
	return n
}
func (c *Core) datagram(to netip.AddrPort, messages []wire.Message, command, probe uint64, piggyback bool) {
	n, err := wire.Size(messages, c.config.Label, c.config.Protected)
	if len(messages) > 0 && (err != nil || n > c.config.PacketBudget) {
		if command != 0 {
			c.emit(Result{Command: command, Failure: LocalFailure})
		}
		if probe != 0 {
			c.finishProbe(probe, LocalFailure, 0, nil)
		}
		return
	}
	if piggyback {
		messages = c.pack(messages)
	}
	if len(messages) == 0 {
		return
	}
	id := c.token()
	if command != 0 || probe != 0 {
		c.sends[id.ID] = sendPending{command, probe}
	}
	c.emit(SendDatagram{ID: id, To: to, Messages: messages})
}
func (c *Core) send(e Send) {
	if len(e.Data) > wire.MaxData || (!e.Broadcast && e.Peer.Validate() != nil) {
		c.emit(Result{Command: e.Command, Failure: LocalFailure})
		return
	}
	data := slices.Clone(e.Data)
	m := wire.Message{User: &data}
	if e.Broadcast {
		n, err := wire.Size([]wire.Message{m}, c.config.Label, c.config.Protected)
		if err != nil || n > c.config.PacketBudget {
			c.emit(Result{Command: e.Command, Failure: LocalFailure})
			return
		}
		c.queue(m, "", e.Command)
	} else if e.Reliable {
		c.openUser(e.Peer, m, e.Command)
	} else {
		c.datagram(e.Peer.Address, []wire.Message{m}, e.Command, 0, false)
	}
}
