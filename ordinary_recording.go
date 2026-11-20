package lightpool

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/internal/ordinary"
	"github.com/modulecollective/lightpool/wire"
)

// recordingConfig is deliberately private: ordinary experiments use a root
// package test binary, without adding a public instrumentation or seed API.
type recordingConfig struct {
	Path, Source, Binary string
	Limits               ordinary.Limits
}

type recordingFailure struct{ err error }

type ordinaryRecorder struct {
	cfg         recordingConfig
	output      io.WriteCloser
	queue       chan []byte
	done        chan struct{}
	failure     atomic.Pointer[recordingFailure]
	queuedBytes atomic.Int64
	mu          sync.Mutex // serializes producers, never shared with disk I/O
	index       uint64
	closeOnce   sync.Once
	sequence    uint64               // owner only
	due         map[core.Timer]int64 // owner only
}

func newOrdinaryRecorder(cfg recordingConfig, identity wire.Identity, seed core.Seed, protocol core.Config, epoch time.Time, baseline []core.Inspection) (*ordinaryRecorder, error) {
	if !cfg.Limits.Valid() || cfg.Source == "" || cfg.Binary == "" {
		return nil, errors.New("invalid ordinary recording limits or provenance")
	}
	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	r := startOrdinaryWriter(cfg, f)
	r.submit(ordinary.Record{Header: &ordinary.Header{Format: ordinary.Version, Identity: identity, Seed: seed, Protocol: protocol, Source: cfg.Source, Binary: cfg.Binary, Toolchain: runtime.Version(), EpochUnixNS: epoch.UnixNano(), Baseline: baseline, Limits: cfg.Limits}})
	return r, nil
}

func startOrdinaryWriter(cfg recordingConfig, output io.WriteCloser) *ordinaryRecorder {
	r := &ordinaryRecorder{cfg: cfg, output: output, queue: make(chan []byte, cfg.Limits.QueueRecords), done: make(chan struct{}), due: make(map[core.Timer]int64)}
	go r.write()
	return r
}

func (r *ordinaryRecorder) fail(err error) { r.failure.CompareAndSwap(nil, &recordingFailure{err}) }

func (r *ordinaryRecorder) err() error {
	if f := r.failure.Load(); f != nil {
		return f.err
	}
	return nil
}

func (r *ordinaryRecorder) submit(record ordinary.Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err() != nil {
		return
	}
	r.index++
	record.Index = r.index
	b, err := json.Marshal(record)
	if err != nil {
		r.fail(err)
		return
	}
	b = append(b, '\n')
	if r.queuedBytes.Add(int64(len(b))) > r.cfg.Limits.QueueBytes {
		r.queuedBytes.Add(-int64(len(b)))
		r.fail(errors.New("ordinary recording byte queue overflow"))
		return
	}
	select {
	case r.queue <- b:
	default:
		r.queuedBytes.Add(-int64(len(b)))
		r.fail(errors.New("ordinary recording record queue overflow"))
	}
}

func (r *ordinaryRecorder) write() {
	defer close(r.done)
	var records uint64
	var bytes int64
	diskFailed := false
	capReached := false
	for b := range r.queue {
		// Keep the reservation until Write returns: a stalled disk cannot grow memory.
		if !diskFailed && !capReached {
			if records+2 > r.cfg.Limits.Records || bytes+int64(len(b))+1024 > r.cfg.Limits.Bytes {
				capReached = true
				r.fail(errors.New("ordinary recording file cap exhausted"))
			} else {
				n, err := r.output.Write(b)
				bytes += int64(n)
				if err == nil && n != len(b) {
					err = io.ErrShortWrite
				}
				if err != nil {
					r.fail(fmt.Errorf("ordinary journal write: %w", err))
					diskFailed = true
				} else {
					records++
				}
			}
		}
		r.queuedBytes.Add(-int64(len(b)))
	}
	if !diskFailed {
		footer := ordinary.Footer{Sequence: r.sequence, Records: records, Bytes: bytes, Status: "complete"}
		if err := r.err(); err != nil {
			footer.Status, footer.Error = "incomplete", err.Error()
		}
		b, err := json.Marshal(ordinary.Record{Index: records + 1, Footer: &footer})
		if err == nil {
			b = append(b, '\n')
			var n int
			n, err = r.output.Write(b)
			if err == nil && n != len(b) {
				err = io.ErrShortWrite
			}
		}
		if err != nil {
			r.fail(fmt.Errorf("ordinary journal footer: %w", err))
		}
	}
	if err := r.output.Close(); err != nil {
		r.fail(fmt.Errorf("ordinary journal close: %w", err))
	}
}

// All producers have stopped before finish: Shutdown joins the owner and workers.
func (r *ordinaryRecorder) finish() error {
	r.closeOnce.Do(func() { close(r.queue) })
	<-r.done
	return r.err()
}

func (n *Node) diagnostic(boundary string, err error) {
	if n.recorder != nil {
		n.recorder.submit(ordinary.Record{Diagnostic: &ordinary.Diagnostic{Boundary: boundary, AtNS: int64(n.now()), Error: err.Error()}})
	}
}

func (n *Node) recordStepEnd(sequence uint64, recorderNS int64) {
	n.recorder.submit(ordinary.Record{End: &ordinary.End{Sequence: sequence, EndNS: int64(n.now()), RecorderNS: recorderNS}})
}

func (n *Node) recordInput(input driverInput, parent uint64) (*ordinary.Step, int64) {
	start := int64(n.now())
	r := n.recorder
	r.sequence++
	value, err := ordinary.Encode(input.event)
	if err != nil {
		r.fail(err)
	}
	s := &ordinary.Step{Sequence: r.sequence, Parent: parent, Input: value, StartNS: start, EnqueueNS: input.enqueued, QueueDepth: len(n.inbox) + len(n.commands) + len(n.deferred), FrameReadNS: input.frameRead}
	if fired, ok := input.event.(core.Fired); ok {
		if due, exists := r.due[fired.Timer]; exists {
			s.TimerDueNS = &due
		}
	}
	return s, int64(n.now()) - start
}

func (n *Node) recordTransition(s *ordinary.Step, coreAt, coreEnd core.Instant, effects []core.Effect, stepErr error) int64 {
	s.CoreAtNS, s.CoreEndNS = int64(coreAt), int64(coreEnd)
	s.Journal = n.engine.Inspection()
	for _, effect := range effects {
		value, err := ordinary.Encode(effect)
		if err != nil {
			n.recorder.fail(err)
		}
		s.Effects = append(s.Effects, value)
	}
	if stepErr != nil {
		s.Error = stepErr.Error()
	}
	n.recorder.submit(ordinary.Record{Step: s})
	return int64(n.now() - coreEnd)
}
