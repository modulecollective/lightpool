// Package ordinary defines the private ordinary-driver journal grammar. It is
// separate from simulator traces: local sequences do not order other processes.
package ordinary

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
)

const Version = "lightpool-ordinary-journal/v2"

// Value preserves the concrete type of an input or effect for exact local replay.
type Value struct {
	Kind string          `json:"kind"`
	Body json.RawMessage `json:"body"`
}

func Encode(v any) (Value, error) {
	b, err := json.Marshal(v)
	return Value{Kind: reflect.TypeOf(v).Name(), Body: b}, err
}

func (v Value) Event() (core.Event, error) {
	var event core.Event
	switch v.Kind {
	case "Start":
		event = &core.Start{}
	case "Shutdown":
		event = &core.Shutdown{}
	case "Join":
		event = &core.Join{}
	case "Ping":
		event = &core.Ping{}
	case "Probe":
		event = &core.Probe{}
	case "Update":
		event = &core.Update{}
	case "Leave":
		event = &core.Leave{}
	case "Datagram":
		event = &core.Datagram{}
	case "Sent":
		event = &core.Sent{}
	case "Fired":
		event = &core.Fired{}
	case "Work":
		event = &core.Work{}
	case "Accepted":
		event = &core.Accepted{}
	case "Opened":
		event = &core.Opened{}
	case "Written":
		event = &core.Written{}
	case "StreamMessage":
		event = &core.StreamMessage{}
	case "Closed":
		event = &core.Closed{}
	case "CancelCommand":
		event = &core.CancelCommand{}
	default:
		return nil, fmt.Errorf("unknown ordinary input %q", v.Kind)
	}
	if err := Decode(v.Body, event); err != nil {
		return nil, err
	}
	return reflect.ValueOf(event).Elem().Interface().(core.Event), nil
}

func Decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("ordinary JSON must contain one value: %v", err)
	}
	return nil
}

type Limits struct {
	QueueRecords int    `json:"queueRecords"`
	QueueBytes   int64  `json:"queueBytes"`
	Records      uint64 `json:"records"`
	Bytes        int64  `json:"bytes"`
}

func (l Limits) Valid() bool {
	return l.QueueRecords >= 1 && l.QueueRecords <= 5_000_000 && l.QueueBytes >= 1024 && l.QueueBytes <= MaxLineBytes && l.Records >= 4 && l.Records <= 5_000_000 && l.Bytes >= 4096 && l.Bytes <= 2<<30
}

const MaxLineBytes = 64 << 20

type Header struct {
	Format                    string        `json:"format"`
	Identity                  wire.Identity `json:"identity"`
	Seed                      core.Seed     `json:"seed"`
	Protocol                  core.Config   `json:"protocol"`
	Source, Binary, Toolchain string
	EpochUnixNS               int64             `json:"epochUnixNS"`
	Baseline                  []core.Inspection `json:"baseline"`
	Limits                    Limits            `json:"limits"`
}

// Step is written before effects are executed, including immediate completions.
// Received timestamps remain inside the input and mean application read return.
type Step struct {
	Sequence                     uint64 `json:"sequence"`
	Parent                       uint64 `json:"parent"`
	Input                        Value  `json:"input"`
	StartNS, CoreAtNS, CoreEndNS int64
	EnqueueNS                    *int64            `json:"enqueueNS,omitempty"`
	FrameReadNS                  *int64            `json:"frameReadNS,omitempty"`
	QueueDepth                   int               `json:"queueDepth"`
	TimerDueNS                   *int64            `json:"timerDueNS,omitempty"`
	Effects                      []Value           `json:"effects"`
	Journal                      []core.Inspection `json:"journal"`
	Error                        string            `json:"error,omitempty"`
}

// End measures the owner's work before recursive immediate completions. Recorder
// capture and queue submission are measured separately from core execution.
type End struct {
	Sequence   uint64 `json:"sequence"`
	EndNS      int64  `json:"endNS"`
	RecorderNS int64  `json:"recorderNS"`
}

type Diagnostic struct {
	Boundary string `json:"boundary"`
	AtNS     int64  `json:"atNS"`
	Error    string `json:"error"`
}

type Footer struct {
	Sequence uint64 `json:"sequence"`
	Records  uint64 `json:"records"`
	Bytes    int64  `json:"bytes"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

// Record has exactly one variant. Index is contiguous across all producers.
type Record struct {
	Index      uint64      `json:"index"`
	Header     *Header     `json:"header,omitempty"`
	Step       *Step       `json:"step,omitempty"`
	End        *End        `json:"end,omitempty"`
	Diagnostic *Diagnostic `json:"diagnostic,omitempty"`
	Footer     *Footer     `json:"footer,omitempty"`
}
