// lightpool-experiment records fixed baseline fixtures or remeasures saved evidence.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"

	"github.com/modulecollective/lightpool/internal/experiment"
	"github.com/modulecollective/lightpool/sim"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "lightpool-experiment:", err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("lightpool-experiment", flag.ContinueOnError)
	flags.SetOutput(stderr)
	name := flags.String("case", "", "fixed case: healthy, crash, threshold, interval, loss, correlated, partial-partition, partial-partition-healthy, correlated-dense, correlated-dense-healthy, correlated-dense-{latency-100us,latency-10ms,service-100us,service-1ms,buffer-64k,buffer-16m}, threshold-128, ordinary-{healthy,pause,crash}, delivery-{32,128}-loss-{0,10,30}-exchange-{on,off}, delivery-32-loss-{0,10,30}-exchange-{on,off}-latency-10ms")
	replicate := flags.Uint64("replicate", 0, "positive paired replicate index")
	out := flags.String("out", "", "new run directory, or new JSON file with -reduce/-admission")
	revision := flags.String("revision", "", "explicit source identity for unstamped/modified builds")
	reduce := flags.String("reduce", "", "saved run directory to remeasure offline")
	admission := flags.String("admission", "", "saved fixed correlated run to classify offline")
	protocol := flags.String("protocol", "", "JSON object overlaid onto the case's core.Config when recording")
	witness := flags.String("witness", "", "saved ordinary witness attempt to reduce offline; prints one table row")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *out == "" || flags.NArg() != 0 {
		return errors.New("-out is required; no positional arguments")
	}
	if *witness != "" {
		if *admission != "" || *reduce != "" || *name != "" || *replicate != 0 || *revision != "" || *protocol != "" {
			return errors.New("-witness cannot be combined with other modes")
		}
		report, err := experiment.ReduceWitness(*witness)
		if report == nil {
			return err
		}
		file, createErr := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil {
			return errors.Join(err, createErr)
		}
		if writeErr := errors.Join(experiment.WriteWitnessReport(file, report), file.Close()); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		fmt.Println(report.Row())
		return err
	}
	if *admission != "" {
		if *reduce != "" || *name != "" || *replicate != 0 || *revision != "" || *protocol != "" {
			return errors.New("-admission cannot be combined with recording flags or -reduce")
		}
		result, err := experiment.Admit(*admission)
		return errors.Join(err, experiment.WriteAdmission(*out, result))
	}
	if *reduce != "" {
		if *name != "" || *replicate != 0 || *revision != "" || *protocol != "" {
			return errors.New("-reduce cannot be combined with recording flags")
		}
		summary, err := experiment.Reduce(*reduce)
		if err != nil {
			return err
		}
		if err = experiment.WriteSummary(*out, summary); err != nil {
			return err
		}
		if summary.Status != "complete" {
			return fmt.Errorf("%s: %s", summary.Status, summary.Reason)
		}
		return nil
	}
	code := sourceIdentity(*revision)
	if code == "" {
		return errors.New("source revision unavailable or modified; supply -revision explicitly")
	}
	spec, scenario, err := experiment.Fixture(*name, *replicate, code)
	if err != nil {
		return err
	}
	if *protocol != "" {
		if err = experiment.OverlayProtocol(&scenario, *protocol); err != nil {
			return err
		}
	}
	return experiment.Run(spec, scenario, *out, func(p sim.Progress) {
		_, _ = fmt.Fprintf(stderr, "sim=%s events=%d trace=%d bytes\n", time.Duration(p.Time), p.Events, p.Bytes)
	})
}

func sourceIdentity(explicit string) string {
	if explicit != "" {
		return explicit
	}
	code := ""
	clean := false
	if b, ok := debug.ReadBuildInfo(); ok {
		for _, s := range b.Settings {
			if s.Key == "vcs.revision" {
				code = s.Value
			}
			if s.Key == "vcs.modified" {
				clean = s.Value == "false"
			}
		}
	}
	if !clean {
		return ""
	}
	return code
}
