package experiment

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/internal/ordinary"
)

// ReplayOrdinary verifies a single process against its seeded core. The
// resulting input counts describe only a complete, orderly local recording.
func ReplayOrdinary(input io.Reader) (map[string]int, error) {
	scan := bufio.NewScanner(input)
	// The recorder queue also bounds each serialized record. Refuse unexpectedly
	// large lines before allocating unbounded memory on malformed offline input.
	scan.Buffer(make([]byte, 64<<10), ordinary.MaxLineBytes)
	scan.Split(func(data []byte, eof bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			return i + 1, data[:i+1], nil
		}
		if eof && len(data) > 0 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, nil
	})
	counts := make(map[string]int)
	deadlines := make(map[core.Timer]int64)
	var engine *core.Core
	var last *ordinary.Step
	var limits ordinary.Limits
	var index, sequence uint64
	var size, previousEnd int64
	var complete, stopped bool
	for scan.Scan() {
		var r ordinary.Record
		if err := ordinary.Decode(scan.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("record %d: %w", index+1, err)
		}
		index++
		variants := 0
		for _, v := range []bool{r.Header != nil, r.Step != nil, r.End != nil, r.Diagnostic != nil, r.Footer != nil} {
			if v {
				variants++
			}
		}
		if variants != 1 || r.Index != index || complete || (index == 1) != (r.Header != nil) {
			return nil, fmt.Errorf("invalid ordinary record at index %d", index)
		}
		switch {
		case r.Header != nil:
			h := r.Header
			if !h.Limits.Valid() || h.Format != ordinary.Version || h.Source == "" || h.Toolchain == "" || h.Binary == "" {
				return nil, errors.New("invalid ordinary header")
			}
			limits = h.Limits
			var err error
			engine, err = core.New(h.Protocol, h.Identity, h.Seed)
			if err != nil {
				return nil, err
			}
			engine.EnableInspection()
			if !reflect.DeepEqual(h.Baseline, engine.Inspection()) {
				return nil, errors.New("initial inspection mismatch")
			}
		case r.Step != nil:
			s := r.Step
			sequence++
			if stopped || last != nil || s.Sequence != sequence || s.Parent >= sequence || s.StartNS < previousEnd || s.CoreAtNS < s.StartNS || s.CoreEndNS < s.CoreAtNS || s.QueueDepth < 0 || (s.EnqueueNS != nil && (*s.EnqueueNS < 0 || *s.EnqueueNS > s.StartNS)) || (sequence == 1) != (s.Input.Kind == "Start") {
				return nil, fmt.Errorf("invalid ordinary step %d", sequence)
			}
			event, err := s.Input.Event()
			if err != nil {
				return nil, err
			}
			var due *int64
			if fired, ok := event.(core.Fired); ok {
				if at, live := deadlines[fired.Timer]; live {
					due = &at
				}
				delete(deadlines, fired.Timer)
			}
			if !reflect.DeepEqual(s.TimerDueNS, due) {
				return nil, fmt.Errorf("timer deadline mismatch at step %d", sequence)
			}
			if s.Error == "" {
				switch v := event.(type) {
				case core.StreamMessage:
					if s.FrameReadNS == nil || *s.FrameReadNS < 0 || *s.FrameReadNS > int64(v.Received) || s.EnqueueNS == nil || int64(v.Received) > *s.EnqueueNS {
						return nil, fmt.Errorf("invalid TCP read bounds at step %d", sequence)
					}
				case core.Datagram:
					if v.Received < 0 || s.EnqueueNS == nil || int64(v.Received) > *s.EnqueueNS {
						return nil, fmt.Errorf("invalid UDP read bounds at step %d", sequence)
					}
				}
			}
			effects, err := engine.Step(core.Instant(s.CoreAtNS), event)
			reason := ""
			if err != nil {
				reason = err.Error()
			}
			if reason != s.Error || !reflect.DeepEqual(s.Journal, engine.Inspection()) || len(effects) != len(s.Effects) {
				return nil, fmt.Errorf("step %d replay mismatch", sequence)
			}
			for i, e := range effects {
				switch t := e.(type) {
				case core.Arm:
					deadlines[t.Timer] = int64(t.At)
				case core.Cancel:
					delete(deadlines, t.Timer)
				}
				value, err := ordinary.Encode(e)
				if err != nil || value.Kind != s.Effects[i].Kind || !bytes.Equal(value.Body, s.Effects[i].Body) {
					return nil, fmt.Errorf("step %d effect %d mismatch", sequence, i)
				}
			}
			counts[s.Input.Kind]++
			stopped = s.Input.Kind == "Shutdown"
			last = s
		case r.End != nil:
			e := r.End
			if last == nil || e.Sequence != sequence || e.EndNS < last.CoreEndNS || e.RecorderNS < 0 || e.RecorderNS > e.EndNS-last.StartNS {
				return nil, fmt.Errorf("invalid step end %d", sequence)
			}
			previousEnd = e.EndNS
			last = nil
		case r.Diagnostic != nil:
			if r.Diagnostic.AtNS < 0 || r.Diagnostic.Error == "" || r.Diagnostic.Boundary == "" {
				return nil, errors.New("invalid driver diagnostic")
			}
		case r.Footer != nil:
			f := r.Footer
			if !stopped || last != nil || f.Status != "complete" || f.Error != "" || f.Sequence != sequence || f.Records != index-1 || f.Bytes != size {
				return nil, fmt.Errorf("invalid ordinary footer: %s %s", f.Status, f.Error)
			}
			complete = true
		}
		size += int64(len(scan.Bytes()))
		if size > limits.Bytes || index > limits.Records {
			return nil, errors.New("journal exceeds declared limit")
		}
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if !complete {
		return nil, errors.New("missing ordinary footer")
	}
	return counts, nil
}

// LoadOrdinaryJournal replays one complete journal against its seeded core and
// returns the node's recorded lifetime for classification. Role, process and
// clock exchanges come from the harness manifest, not the journal.
func LoadOrdinaryJournal(data []byte) (*OrdinaryNode, error) {
	if _, err := ReplayOrdinary(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	n := &OrdinaryNode{FileSHA256: hex.EncodeToString(sum[:])}
	for _, line := range bytes.SplitAfter(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var r ordinary.Record
		if err := ordinary.Decode(line, &r); err != nil {
			return nil, err
		}
		switch {
		case r.Header != nil:
			n.Name, n.Address, n.Lifetime = r.Header.Identity.Name, r.Header.Identity.Address, r.Header.Seed.Lifetime
		case r.Step != nil:
			n.Steps = append(n.Steps, OrdinaryStep{Sequence: r.Step.Sequence, StartNS: r.Step.StartNS, CoreAtNS: r.Step.CoreAtNS, Journal: r.Step.Journal})
		case r.End != nil:
			// Replay has proven every step is closed by its own End record.
			n.Steps[len(n.Steps)-1].EndNS = r.End.EndNS
		}
	}
	return n, nil
}
