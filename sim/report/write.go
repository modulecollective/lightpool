package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// Write renders already validated measurements. Presentation never determines
// eligibility or reclassifies an incomplete recording as a completed experiment.
func Write(w io.Writer, r *Report, format string) error {
	switch format {
	case "json":
		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		return e.Encode(r)
	case "text":
		return writeText(w, r)
	default:
		return fmt.Errorf("unsupported format %q (use text or json)", format)
	}
}
func duration(n *int64) string {
	if n == nil {
		return "n/a"
	}
	return time.Duration(*n).String()
}
func value(s *string) string {
	if s == nil {
		return "n/a"
	}
	return *s
}
func position(r Reference) string { return fmt.Sprintf("%s/#%d", time.Duration(r.TimeNS), r.Seq) }
func population(d Distribution) string {
	return fmt.Sprintf("%d/%d completed; p50=%s p95=%s max=%s %s", d.Completed, d.Eligible, duration(d.P50NS), duration(d.P95NS), duration(d.MaxNS), d.Reason)
}
func writeText(w io.Writer, r *Report) error {
	// Assemble only the report, after aggregation; never buffer the trace itself.
	var b strings.Builder
	status := "COMPLETE"
	if !r.Complete {
		status = "INCOMPLETE (valid observed prefix only)"
	}
	fmt.Fprintf(&b, "%s — %s\nTrace SHA256: %s\nCalculator: %s\nNodes: %d; horizon: %s; observed through: %s; last event: %s\n\n", status, r.EndReason, r.TraceSHA256, r.Calculator, r.NodeCount, time.Duration(r.HorizonNS), time.Duration(r.ObservedThroughNS), position(r.LastEvent))
	fmt.Fprintf(&b, "Dead declarations: %d running-target, %d stopped-target; %d distinct running targets, %d observer-lifetime/target pairs\n", r.RunningDeclarations, r.StoppedDeclarations, r.AffectedRunningTargets, r.AffectedRunningPairs)
	t := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(t, "observer/lifetime\tsubject/lifetime\tat/seq\ttarget running\ttarget core stopped/left\tobserver core stopped/left\ttarget paused\tobserver paused\tincident configured faults"); err != nil {
		return err
	}
	for _, d := range r.Declarations {
		faults, _ := json.Marshal(d.Faults)
		if _, err := fmt.Fprintf(t, "%s/%s\t%s/%s\t%s\t%t\t%t/%t\t%t/%t\t%t\t%t\t%s\n", d.Observer, d.ObserverLifetime, d.Subject, d.TargetLifetime, position(d.Reference), d.TargetRunning, d.TargetStopped, d.TargetLeft, d.ObserverStopped, d.ObserverLeft, d.TargetPaused, d.ObserverPaused, faults); err != nil {
			return err
		}
	}
	if err := t.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(&b, "\nCrash detection (new dead transitions only; pre-crash alive/suspect cohort):")
	for _, c := range r.Crashes {
		first := "n/a"
		if c.First != nil {
			first = position(*c.First)
		}
		fmt.Fprintf(&b, "%s/%s crashed at %s: %s; first=%s; excluded already-dead=%d inactive=%d unknown/left=%d\n", c.Subject, c.TargetLifetime, position(c.Reference), population(c.Latencies), first, c.AlreadyDead, c.Inactive, c.UnknownOrLeft)
		for _, o := range c.Observations {
			fmt.Fprintf(&b, "  %s/%s -> %s/%s: %s at %s; latency=%s\n", o.Observer, o.ObserverLifetime, o.Subject, o.TargetLifetime, o.Status, position(o.End), duration(o.LatencyNS))
		}
	}
	if len(r.Crashes) == 0 {
		fmt.Fprintln(&b, "n/a: no explicit crash actions")
	}
	fmt.Fprintf(&b, "\nRunning-target exclusion until next observed alive: %s\n", population(r.RecoveryDurations))
	for _, e := range r.Recoveries {
		fmt.Fprintf(&b, "%s/%s -> %s/%s: %s -> %s; %s; duration=%s\n", e.Observer, e.ObserverLifetime, e.Subject, e.TargetLifetime, position(e.Declaration), position(e.End), e.Status, duration(e.DurationNS))
		for _, j := range e.Joins {
			fmt.Fprintf(&b, "  explicit join %s -> %s at %s (causality not inferred)\n", j.Node, j.Peer, position(j.Reference))
		}
	}
	fmt.Fprintf(&b, "\nEncoded framed traffic (bytes; no IP/TCP overhead):\n  datagram sent=%d received=%d\n  stream sent=%d received=%d\n  cluster sent=%d received=%d; denominator=%s\n  bytes/s sent=%s received=%s %s\n", r.Traffic.Datagram.Sent, r.Traffic.Datagram.Received, r.Traffic.Stream.Sent, r.Traffic.Stream.Received, r.Traffic.Total.Sent, r.Traffic.Total.Received, time.Duration(r.Traffic.DurationNS), value(r.Traffic.SentBytesPerSecond), value(r.Traffic.ReceivedBytesPerSecond), r.Traffic.RateReason)
	fmt.Fprintln(&b, "Recorded drop entries (diagnostics, not unique packets):")
	for _, d := range r.Traffic.Drops {
		fmt.Fprintf(&b, "  %s: %s = %d\n", d.Transport, d.Reason, d.Count)
	}
	if r.Traffic.EndTotals != nil {
		totals, _ := json.Marshal(r.Traffic.EndTotals)
		fmt.Fprintf(&b, "End totals cross-check: %s; byte totals match=%t\n", totals, *r.Traffic.EndBytesMatch)
	}
	for _, s := range r.Unsupported {
		fmt.Fprintf(&b, "UNSUPPORTED/AMBIGUOUS: %s\n", s)
	}
	fmt.Fprintln(&b, "\nModel limits:")
	for _, s := range r.ModelLimits {
		fmt.Fprintf(&b, "- %s\n", s)
	}
	fmt.Fprintf(&b, "\nProducer manifest (source, toolchain, seeds, protocol and scenario):\n%s\n", r.Producer)
	_, err := io.WriteString(w, b.String())
	return err
}
