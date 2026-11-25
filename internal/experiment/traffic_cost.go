package experiment

import (
	"fmt"
	"strconv"

	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/sim/trace"
)

// Datagram counts and bytes are per stage; stream bytes are per chunk at sent
// and per encoded write at attempt. Neither is a TCP wire-cost estimate.
type SendCost struct {
	DatagramCount uint64 `json:"datagramCount,string"`
	DatagramBytes uint64 `json:"datagramBytes,string"`
	StreamBytes   uint64 `json:"streamBytes,string"`
	IPv4UDPBytes  uint64 `json:"ipv4UDPBytes,string"`
}

type CostInterval struct {
	Status            string   `json:"status"`
	FromNS            int64    `json:"fromNS,string"`
	ToNS              int64    `json:"toNS,string"`
	ObservedThroughNS int64    `json:"observedThroughNS,string"`
	Interval          string   `json:"interval"`
	Attempted         SendCost `json:"attempted"`
	Sent              SendCost `json:"sent"`
}

type TrafficCost struct {
	Status         string       `json:"status"`
	Measured       CostInterval `json:"measured"`
	WholeRun       CostInterval `json:"wholeRun"`
	EndTotalsMatch *bool        `json:"endTotalsMatch"`
}

func (c *SendCost) add(t trace.Traffic, n uint64) {
	switch t.Transport {
	case "datagram":
		c.DatagramCount++
		c.DatagramBytes += n
		c.IPv4UDPBytes += n + 28
	case "stream":
		c.StreamBytes += n
	}
}

func (c *TrafficCost) record(at int64, spec Spec, t trace.Traffic) {
	n, _ := strconv.ParseUint(t.Bytes, 10, 64)
	for _, window := range []struct {
		dst     *CostInterval
		include bool
	}{
		{&c.WholeRun, true}, {&c.Measured, at >= spec.WarmupNS && at <= spec.HorizonNS},
	} {
		if !window.include {
			continue
		}
		switch t.Stage {
		case "attempt":
			window.dst.Attempted.add(t, n)
		case "sent":
			window.dst.Sent.add(t, n)
		}
	}
}

func (c *TrafficCost) finish(spec Spec, base *report.Report) error {
	c.Status = "observed-prefix"
	if base.Complete {
		c.Status = "complete"
	}
	c.Measured.Status, c.WholeRun.Status = c.Status, c.Status
	if base.ObservedThroughNS < spec.WarmupNS {
		c.Measured.Status = "unreached"
	}
	c.Measured.FromNS, c.Measured.ToNS = spec.WarmupNS, spec.HorizonNS
	c.WholeRun.ToNS = base.ObservedThroughNS
	c.Measured.ObservedThroughNS = min(base.ObservedThroughNS, spec.HorizonNS)
	c.WholeRun.ObservedThroughNS = base.ObservedThroughNS
	c.Measured.Interval, c.WholeRun.Interval = "closed", "closed"
	if base.Complete {
		if base.Traffic.EndTotals == nil {
			return fmt.Errorf("complete trace lacks traffic end totals")
		}
		match := trace.Decimal(c.WholeRun.Sent.DatagramBytes+c.WholeRun.Sent.StreamBytes) == base.Traffic.EndTotals.SentBytes
		c.EndTotalsMatch = &match
		if !match {
			return fmt.Errorf("whole-run sent costs disagree with trace totals")
		}
	}
	return nil
}
