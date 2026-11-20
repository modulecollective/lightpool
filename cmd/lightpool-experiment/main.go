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
	name := flags.String("case", "", "fixed pilot case")
	replicate := flags.Uint64("replicate", 0, "positive paired replicate index")
	out := flags.String("out", "", "new run directory, or new summary file with -reduce")
	revision := flags.String("revision", "", "explicit source identity for unstamped/modified builds")
	reduce := flags.String("reduce", "", "saved run directory to remeasure offline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *out == "" || flags.NArg() != 0 {
		return errors.New("-out is required; no positional arguments")
	}
	if *reduce != "" {
		if *name != "" || *replicate != 0 || *revision != "" {
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
