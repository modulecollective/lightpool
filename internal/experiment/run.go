package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
)

var errTraceCap = errors.New("2 GiB trace-output cap reached")

type cappedWriter struct {
	w    io.Writer
	left int64
}

func (w *cappedWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.left {
		n, err := w.w.Write(b[:w.left])
		w.left -= int64(n)
		if err != nil {
			return n, err
		}
		return n, errTraceCap
	}
	n, err := w.w.Write(b)
	w.left -= int64(n)
	return n, err
}

type Status struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type Resources struct {
	ElapsedNS    int64  `json:"elapsedNS,string"`
	PeakRSSBytes *int64 `json:"peakRSSBytes,string"`
	MemorySource string `json:"memorySource"`
}

// Run creates a directory exclusively. Failed evidence is retained in place;
// retrying requires a different directory, never deletion of the failed run.
func Run(spec Spec, scenario sim.Scenario, directory string, progress func(sim.Progress)) (err error) {
	if err = validateSpec(spec, scenario); err != nil {
		return err
	}
	if err = sim.Validate(scenario); err != nil {
		return err
	}
	if err = os.Mkdir(directory, 0700); err != nil {
		return err
	}
	started := time.Now()
	status := Status{Status: "incomplete", Reason: "recording did not finish"}
	defer func() {
		rss, source := peakRSS()
		resourceErr := writeJSON(filepath.Join(directory, "resources.json"), Resources{time.Since(started).Nanoseconds(), rss, source})
		err = errors.Join(err, resourceErr)
		if err != nil && (resourceErr != nil || status.Status == "complete" || status.Reason == "recording did not finish") {
			status = Status{Status: "incomplete", Reason: err.Error()}
		}
		err = errors.Join(err, writeJSON(filepath.Join(directory, "status.json"), status))
	}()
	if err = writeJSON(filepath.Join(directory, "spec.json"), spec); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(directory, "scenario.json"), scenario); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(directory, "trace.ndjson"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	result, recordErr := recordTrace(scenario, file, spec.TraceCap, progress)
	// Reduction of a valid prefix is useful even when export or close failed. No
	// end record is appended by this layer, and failed I/O can never mean success.
	summary, reduceErr := reduce(directory, true)
	if reduceErr == nil {
		if recordErr != nil {
			summary.Status = "incomplete"
			summary.Reason = recordErr.Error()
		}
		status = Status{Status: summary.Status, Reason: summary.Reason}
		reduceErr = writeJSON(filepath.Join(directory, "summary.json"), summary)
	}
	if err = errors.Join(recordErr, reduceErr); err != nil {
		return err
	}
	status = Status{Status: summary.Status, Reason: summary.Reason}
	if !result.Complete {
		return fmt.Errorf("incomplete recording: %s", result.Reason)
	}
	if summary.Status != "complete" {
		return fmt.Errorf("%s: %s", summary.Status, summary.Reason)
	}
	return nil
}

func recordTrace(scenario sim.Scenario, file io.WriteCloser, cap int64, progress func(sim.Progress)) (sim.Result, error) {
	result, err := sim.Export(scenario, &cappedWriter{w: file, left: cap}, sim.ExportOptions{Progress: progress})
	return result, errors.Join(err, file.Close())
}

// Reduce regenerates measurements only from saved inputs, without simulating.
// If report.json exists it must match; this path never writes saved evidence.
func Reduce(directory string) (*Summary, error) { return reduce(directory, false) }

func reduce(directory string, recording bool) (*Summary, error) {
	var spec Spec
	var scenario sim.Scenario
	if err := readJSON(filepath.Join(directory, "spec.json"), &spec); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(directory, "scenario.json"), &scenario); err != nil {
		return nil, err
	}
	for _, input := range []struct {
		name  string
		value any
	}{{"spec.json", spec}, {"scenario.json", scenario}} {
		digest, err := hashFile(filepath.Join(directory, input.name))
		if err != nil {
			return nil, err
		}
		if digest != HashJSON(input.value) {
			return nil, fmt.Errorf("%s differs from canonical saved input encoding", input.name)
		}
	}
	path := filepath.Join(directory, "trace.ndjson")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	base, readErr := report.Read(f)
	err = errors.Join(readErr, f.Close())
	if err != nil {
		return nil, err
	}
	reportPath := filepath.Join(directory, "report.json")
	if _, err = os.Stat(reportPath); errors.Is(err, os.ErrNotExist) {
		if recording {
			if err = writeJSON(reportPath, base); err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	} else {
		sum, err := hashFile(reportPath)
		if err != nil {
			return nil, err
		}
		if sum != HashJSON(base) {
			return nil, fmt.Errorf("saved report disagrees with recorded trace")
		}
	}
	f, err = os.Open(path)
	if err != nil {
		return nil, err
	}
	out, readErr := Measure(spec, scenario, base, f)
	err = errors.Join(readErr, f.Close())
	if err != nil {
		return nil, err
	}
	if !recording {
		var status Status
		if err = readJSON(filepath.Join(directory, "status.json"), &status); err != nil {
			return nil, err
		}
		if status.Status == "incomplete" {
			out.Status, out.Reason = status.Status, status.Reason
		}
	}
	return out, nil
}

func validateSpec(spec Spec, scenario sim.Scenario) error {
	if spec.Format != Version || spec.Code == "" || spec.Replicate == 0 || spec.SeedDerivation != SeedVersion || spec.LossDraw != sim.LossDrawVersion || spec.WarmupNS < 0 || spec.HorizonNS <= spec.WarmupNS || int64(scenario.Horizon) != spec.HorizonNS || spec.Code != scenario.Code || spec.TraceCap <= 0 || spec.TraceCap > TraceCap {
		return fmt.Errorf("invalid experiment specification")
	}
	switch spec.Case {
	case "partial-partition", "partial-partition-healthy":
	case "healthy", "crash", "threshold", "interval", "loss", "correlated", "threshold-128", "ordinary-healthy", "ordinary-pause", "ordinary-crash", "correlated-dense", "correlated-dense-healthy", "correlated-dense-latency-100us", "correlated-dense-latency-10ms", "correlated-dense-service-100us", "correlated-dense-service-1ms", "correlated-dense-buffer-64k", "correlated-dense-buffer-16m":
	default:
		return fmt.Errorf("unknown experiment case %q", spec.Case)
	}
	names := map[string]bool{}
	for _, n := range scenario.Nodes {
		names[n.Identity.Name] = true
	}
	seen := map[string]bool{}
	for _, n := range spec.Anomaly {
		if !names[n] || seen[n] {
			return fmt.Errorf("invalid anomaly population")
		}
		seen[n] = true
	}
	if spec.Target != "" && !names[spec.Target] {
		return fmt.Errorf("unknown experiment target")
	}
	pairs := map[EligiblePair]bool{}
	for _, p := range spec.HealthyPairs {
		if !names[p.Observer] || !names[p.Subject] || p.Observer == p.Subject || pairs[p] {
			return fmt.Errorf("invalid healthy-pair population")
		}
		pairs[p] = true
	}
	return nil
}

func writeJSON(path string, v any) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return errors.Join(encoder.Encode(v), file.Close())
}

// WriteSummary emits a new summary file for reducer-only CLI replay.
func WriteSummary(path string, s *Summary) error { return writeJSON(path, s) }

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	d := json.NewDecoder(io.LimitReader(f, 16<<20))
	d.DisallowUnknownFields()
	err = d.Decode(v)
	if err == nil {
		var extra any
		if d.Decode(&extra) != io.EOF {
			err = fmt.Errorf("expected one JSON value")
		}
	}
	return errors.Join(err, f.Close())
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	err = errors.Join(copyErr, f.Close())
	return hex.EncodeToString(h.Sum(nil)), err
}
