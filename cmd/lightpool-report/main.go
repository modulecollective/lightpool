// lightpool-report measures outcomes in portable simulation recordings.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/modulecollective/lightpool/sim/report"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "lightpool-report:", err)
		os.Exit(1)
	}
}
func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("lightpool-report", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("trace", "", "trace NDJSON file")
	format := flags.String("format", "text", "text or json")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return errors.New("-trace is required")
	}
	if *format != "text" && *format != "json" {
		return errors.New("-format must be text or json")
	}
	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	r, readErr := report.Read(f)
	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err = report.Write(stdout, r, *format); err != nil {
		return err
	}
	if !r.Complete {
		return fmt.Errorf("INCOMPLETE: %s", r.EndReason)
	}
	return nil
}
