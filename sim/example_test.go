// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"fmt"
	"net/netip"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/wire"
)

func ExampleRun() {
	result, err := sim.Run(sim.Scenario{
		Code: "example-source-revision", Protocol: core.DefaultConfig(), Seeds: sim.Seeds{Protocol: 1, Fault: 2, Schedule: 3},
		Nodes:   []sim.Node{{Identity: wire.Identity{Name: "a", Address: netip.MustParseAddrPort("192.0.2.1:7946")}}, {Identity: wire.Identity{Name: "b", Address: netip.MustParseAddrPort("192.0.2.2:7946")}}},
		Actions: []sim.Action{{At: core.Instant(time.Millisecond), Node: "b", Kind: "join", Peer: "a"}},
		Horizon: core.Instant(time.Second), EventBudget: 10000, DatagramCapacity: 4096, StreamCapacity: 128, Latency: time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Complete, len(result.Measurements[0].Snapshot.Members))
	// Output: true 2
}
