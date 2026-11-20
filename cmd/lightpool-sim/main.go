// lightpool-sim records deterministic simulations for offline outcome reports.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "lightpool-sim:", err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("lightpool-sim", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("scenario", "", "scenario JSON (nanosecond durations)")
	out := flags.String("out", "", "new trace file")
	revision := flags.String("revision", "", "explicit source identity for unstamped/modified builds")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || *out == "" || flags.NArg() != 0 {
		return errors.New("-scenario and -out are required")
	}
	code := *revision
	if code == "" {
		if b, ok := debug.ReadBuildInfo(); ok {
			clean := false
			for _, s := range b.Settings {
				if s.Key == "vcs.revision" {
					code = s.Value
				}
				if s.Key == "vcs.modified" {
					clean = s.Value == "false"
				}
			}
			if !clean {
				code = ""
			}
		}
	}
	if code == "" {
		return errors.New("source revision unavailable or modified; supply -revision explicitly")
	}
	s, err := readScenario(*path)
	if err != nil {
		return err
	}
	s.Code = code
	// Validate before creating the output. Core identity/config checks run again
	// at construction so both entry points enforce the same scenario contract.
	if err = sim.Validate(s); err != nil {
		return err
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	result, runErr := sim.Export(s, file, sim.ExportOptions{Progress: func(p sim.Progress) {
		_, _ = fmt.Fprintf(stderr, "sim=%s events=%d trace=%d bytes\n", time.Duration(p.Time), p.Events, p.Bytes)
	}})
	closeErr := file.Close()
	if runErr != nil {
		return fmt.Errorf("recording incomplete at %s: %w", *out, runErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close trace: %w", closeErr)
	}
	if !result.Complete {
		return fmt.Errorf("recording incomplete: %s", result.Reason)
	}
	return nil
}

func readScenario(path string) (sim.Scenario, error) {
	var s = sim.Scenario{Protocol: core.DefaultConfig()}
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer func() { _ = f.Close() }()
	const max = 16 << 20
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return s, err
	}
	if len(b) > max {
		return s, errors.New("scenario exceeds 16 MiB")
	}
	// Decode once and require EOF; unknown fields must not silently alter a run.
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&s); err != nil {
		return s, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return s, errors.New("scenario must contain one JSON object")
	}
	return s, nil
}
