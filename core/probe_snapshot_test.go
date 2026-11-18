// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"testing"
	"time"

	"github.com/modulecollective/lightpool/wire"
)

func TestProbeFailureUsesProbedMembership(t *testing.T) {
	for _, change := range []string{"unchanged", "new-incarnation", "reclaimed-address"} {
		t.Run(change, func(t *testing.T) {
			c := testCore(t)
			es := step(t, c, 0, Probe{Peer: "b"})
			expiry := timer(t, es, "overall")
			n := c.nodes["b"].Clone()
			switch change {
			case "new-incarnation":
				n.Incarnation++
			case "reclaimed-address":
				step(t, c, 1, Datagram{From: n.Address, Received: 1, Messages: []wire.Message{{Dead: &wire.Accusation{Node: "b", From: "b", Incarnation: 1}}}})
				step(t, c, 1, Work{ID: c.work})
				n.Address = identity("b", 9).Address
			}
			if change != "unchanged" {
				step(t, c, 2, Datagram{From: n.Address, Received: 2, Messages: []wire.Message{{Alive: &n}}})
				step(t, c, 2, Work{ID: c.work})
			}
			step(t, c, Instant(time.Second), Fired{Timer: expiry.Timer})
			want := wire.Alive
			if change == "unchanged" {
				want = wire.Suspect
			}
			got := c.nodes["b"].Member
			if got.State != want || got.Incarnation != n.Incarnation || got.Address != n.Address {
				t.Fatalf("probe of incarnation 1 at old address changed membership: %+v", got)
			}
		})
	}
}
