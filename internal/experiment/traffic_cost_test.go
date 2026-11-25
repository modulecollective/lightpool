package experiment

import (
	"bytes"
	"testing"

	"github.com/modulecollective/lightpool/sim/report"
)

func TestTrafficCostStagesWindowsAndPrefixes(t *testing.T) {
	b := newEvidence(t)
	b.traffic(9, "datagram", "attempt", "1", 50)
	b.traffic(9, "datagram", "sent", "1", 50)
	b.traffic(10, "datagram", "attempt", "2", 100)
	b.traffic(10, "datagram", "sent", "2", 100)
	b.traffic(11, "datagram", "drop", "2", 100)   // sent and then lost still costs bytes
	b.traffic(12, "datagram", "attempt", "3", 80) // failed send: offered only
	b.traffic(12, "datagram", "drop", "3", 80)
	b.traffic(13, "stream", "attempt", "4", 0) // open, not a write
	b.traffic(14, "stream", "attempt", "5", 250)
	b.traffic(15, "stream", "sent", "5", 70)
	b.traffic(16, "stream", "sent", "5", 50) // partial write
	b.traffic(17, "stream", "drop", "5", 130)
	b.traffic(18, "datagram", "received", "2", 100)
	b.traffic(18, "datagram", "received", "2", 100) // duplication costs no second send
	b.traffic(100, "datagram", "attempt", "6", 60)
	b.traffic(100, "datagram", "sent", "6", 60)
	b.traffic(100, "stream", "attempt", "7", 40) // failed stream write
	s := b.measure(t, true)
	c := s.TrafficCost
	if c.Status != "complete" || c.EndTotalsMatch == nil || !*c.EndTotalsMatch || c.Measured.FromNS != 10 || c.Measured.ToNS != 100 || c.Measured.ObservedThroughNS != 100 || c.Measured.Interval != "closed" {
		t.Fatal(c)
	}
	if c.Measured.Attempted != (SendCost{DatagramCount: 3, DatagramBytes: 240, StreamBytes: 290, IPv4UDPBytes: 324}) || c.Measured.Sent != (SendCost{DatagramCount: 2, DatagramBytes: 160, StreamBytes: 120, IPv4UDPBytes: 216}) {
		t.Fatal("attempt/sent arithmetic", c.Measured)
	}
	if c.WholeRun.Attempted.DatagramBytes != 290 || c.WholeRun.Sent.DatagramBytes != 210 || c.WholeRun.Sent.IPv4UDPBytes != 294 || c.WholeRun.ToNS != 100 {
		t.Fatal("whole-run", c.WholeRun)
	}
	if s.Traffic.Datagram.SentBytes != 160 || s.Traffic.Stream.SentBytes != 120 || s.Traffic.Datagram.Messages != 2 || s.Traffic.Stream.Messages != 1 {
		t.Fatal("legacy traffic changed", s.Traffic)
	}
	prefix := b.measure(t, false).TrafficCost
	if prefix.Status != "observed-prefix" || prefix.Measured.Status != "observed-prefix" || prefix.EndTotalsMatch != nil || prefix.Measured.Sent != c.Measured.Sent {
		t.Fatal("prefix claimed full coverage", prefix)
	}
	b.events = b.events[:10]
	prefix = b.measure(t, false).TrafficCost
	if prefix.Measured.ObservedThroughNS != 12 || prefix.Measured.ToNS != 100 || prefix.WholeRun.ToNS != 12 {
		t.Fatal("prefix bounds", prefix)
	}
	// Outside the closed measured interval remains in whole-run cost only.
	b.traffic(101, "datagram", "sent", "8", 20)
	s = b.measure(t, false)
	if s.TrafficCost.Measured.Sent.DatagramBytes != 100 || s.TrafficCost.WholeRun.Sent.DatagramBytes != 170 {
		t.Fatal("upper bound", s.TrafficCost)
	}
}

func TestWholeRunCostChecksCompleteTraceTotals(t *testing.T) {
	b := newEvidence(t)
	b.traffic(10, "datagram", "sent", "1", 100)
	raw := b.raw(t, true)
	base, err := report.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	base.Traffic.EndTotals.SentBytes = "101"
	if _, err = Measure(b.spec, b.scenario, base, bytes.NewReader(raw)); err == nil {
		t.Fatal("accepted inconsistent whole-run totals")
	}
}

func TestHistoricalMetricAvailabilityAndSpecEncoding(t *testing.T) {
	b := newEvidence(t)
	oldSpec := HashJSON(b.spec)
	b.add(20, member("n001", "n003", "dead"))
	b.add(30, member("n001", "n003", "alive"))
	s := b.measure(t, true)
	if s.SpecSHA256 != oldSpec || s.Format != "lightpool-pilot-summary/v3" || s.Spec.Format != "lightpool-pilot/v1" || s.UpdateDelivery.Status != "not-applicable" || s.UpdateDelivery.DeliveredFraction != nil || s.UpdateDelivery.PairLatencies.P50NS != nil {
		t.Fatal("encoding/availability", s.UpdateDelivery)
	}
	if len(s.Episodes) != 1 || s.RecoveryDurations.Completed != 1 || *s.RecoveryDurations.P50NS != 10 || population(t, s, "FP-minus-analogue").Transitions != 1 {
		t.Fatal("prior metrics changed")
	}
}
