// Package trace defines the portable Lightpool trace v1 disk contract.
package trace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
)

func Decimal[T ~int | ~int64 | ~uint64](v T) string { return fmt.Sprint(v) }

type Node struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

type Process struct {
	Node        string `json:"node"`
	Lifetime    string `json:"lifetime"`
	Running     bool   `json:"running"`
	PausedUntil string `json:"pausedUntil"`
}

type Member struct {
	Name        string `json:"name"`
	Address     string `json:"address"`
	Incarnation string `json:"incarnation"`
	State       string `json:"state"`
	Meta        []byte `json:"meta,omitempty"`
	// Application is no longer produced; v1 traces that carry it still read.
	Application string `json:"application,omitempty"`
}

func Membership(n wire.Member) *Member {
	return &Member{n.Name, n.Address.String(), Decimal(n.Incarnation), []string{"alive", "suspect", "dead", "left"}[n.State], n.Meta, ""}
}

type Summary struct {
	Health         string            `json:"health"`
	Backlog        string            `json:"backlog"`
	PendingStreams string            `json:"pendingStreams"`
	Stopped        bool              `json:"stopped"`
	Left           bool              `json:"left"`
	Stats          map[string]string `json:"stats"`
}

func Counters(s core.Snapshot) *Summary {
	return &Summary{Decimal(s.Health), Decimal(s.Backlog), Decimal(s.PendingStreams), s.Stopped, s.Left, map[string]string{
		"steps": Decimal(s.Stats.Steps), "datagrams": Decimal(s.Stats.Datagrams), "messages": Decimal(s.Stats.Messages), "queueDrops": Decimal(s.Stats.QueueDrops), "probes": Decimal(s.Stats.Probes), "failedProbes": Decimal(s.Stats.FailedProbes), "tcpOnly": Decimal(s.Stats.TCPOnly)}}
}

type Change struct {
	Kind       string   `json:"kind"`
	Node       string   `json:"node"`
	Subject    string   `json:"subject,omitempty"`
	Member     *Member  `json:"member,omitempty"`
	Process    *Process `json:"process,omitempty"`
	Summary    *Summary `json:"summary,omitempty"`
	ID         string   `json:"id,omitempty"`
	Generation string   `json:"generation,omitempty"`
	Operation  string   `json:"operation,omitempty"`
	Phase      string   `json:"phase,omitempty"`
	Purpose    string   `json:"purpose,omitempty"`
	Outcome    string   `json:"outcome,omitempty"`
	Deadline   string   `json:"deadline,omitempty"`
	Helpers    []string `json:"helpers,omitempty"`
}

type Traffic struct {
	ID        string   `json:"id"`
	Transport string   `json:"transport"`
	Stage     string   `json:"stage"`
	From      string   `json:"from"`
	To        string   `json:"to"`
	Bytes     string   `json:"bytes"`
	Messages  []string `json:"messages"`
	Detail    string   `json:"detail,omitempty"`
	Received  string   `json:"received,omitempty"`
}

type Header struct {
	Type     string    `json:"type"`
	Format   string    `json:"format"`
	Version  int       `json:"version"`
	Manifest any       `json:"manifest"`
	Nodes    []Node    `json:"nodes"`
	Initial  []Process `json:"initial"`
}

type Event struct {
	Type     string    `json:"type"`
	Seq      string    `json:"seq"`
	Time     string    `json:"time"`
	ID       string    `json:"id"`
	Kind     string    `json:"kind"`
	Actor    string    `json:"actor,omitempty"`
	Peer     string    `json:"peer,omitempty"`
	Lifetime string    `json:"lifetime"`
	Changes  []Change  `json:"changes"`
	Traffic  []Traffic `json:"traffic"`
}

type Totals struct {
	SentBytes     string `json:"sentBytes"`
	ReceivedBytes string `json:"receivedBytes"`
	DatagramDrops string `json:"datagramDrops"`
	Detections    string `json:"detections"`
}

type Measurement struct {
	Process
	Summary *Summary `json:"summary,omitempty"`
	Totals
}

type End struct {
	Type     string        `json:"type"`
	Seq      string        `json:"seq"`
	Time     string        `json:"time"`
	Reason   string        `json:"reason"`
	Complete bool          `json:"complete"`
	Nodes    []Measurement `json:"nodes"`
	Totals   Totals        `json:"totals"`
}

// Normalize also protects the nested scenario/configuration from JSON rounding.
func Normalize(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var out any
	if err = d.Decode(&out); err != nil {
		return nil, err
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case json.Number:
			return x.String()
		case []any:
			for i := range x {
				x[i] = walk(x[i])
			}
		case map[string]any:
			for k := range x {
				x[k] = walk(x[k])
			}
		}
		return v
	}
	return walk(out), nil
}

// Writer owns a bounded buffer, not the underlying file. The caller must close it.
type Writer struct {
	buffer *bufio.Writer
	Bytes  uint64
}

func NewWriter(w io.Writer) *Writer { b := bufio.NewWriterSize(w, 64<<10); return &Writer{buffer: b} }

func (w *Writer) Write(v any) error {
	// Marshal one record so Bytes describes committed encoding, including newline.
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b)+1 > 64<<20 {
		return fmt.Errorf("trace record exceeds 64 MiB")
	}
	n, err := w.buffer.Write(append(b, '\n'))
	w.Bytes += uint64(n)
	return err
}

func (w *Writer) Flush() error { return w.buffer.Flush() }

// Read validates envelope order and complete lines. Consumers may validate the
// typed payload further. An unterminated trailing record is deliberately ignored.
func Read(r io.Reader, visit func(json.RawMessage) error) (bool, error) {
	scanner := bufio.NewReaderSize(r, 64<<10)
	line := 0
	seq := uint64(0)
	at := int64(0)
	ended := false
	complete := false
	validation := validator{}
	for {
		b, err := readLine(scanner)
		if err == io.EOF {
			if line == 0 {
				return false, fmt.Errorf("line 1: missing complete header")
			}
			return complete, nil
		}
		if err != nil {
			return false, fmt.Errorf("line %d: %w", line+1, err)
		}
		line++
		if len(b) > 64<<20 {
			return false, fmt.Errorf("line %d: record exceeds limit", line)
		}
		var e struct {
			Type, Format, Seq, Time string
			Version                 int
			Complete                bool
		}
		fail := func(err error) (bool, error) { return false, fmt.Errorf("line %d: %w", line, err) }
		if !utf8.Valid(b) {
			return fail(fmt.Errorf("invalid UTF-8"))
		}
		if err = json.Unmarshal(b, &e); err != nil {
			return fail(err)
		}
		if ended {
			return fail(fmt.Errorf("record after end"))
		}
		if line == 1 {
			if e.Type != "header" || e.Format != "lightpool-trace" || e.Version != 1 {
				return fail(fmt.Errorf("unsupported header"))
			}
		} else {
			if err := number(e.Seq); err != nil {
				return fail(err)
			}
			n, err := strconv.ParseUint(e.Seq, 10, 64)
			if err != nil {
				return fail(err)
			}
			if err := instant(e.Time); err != nil {
				return fail(err)
			}
			t, err := strconv.ParseInt(e.Time, 10, 64)
			if err != nil || t < at || t < 0 {
				return fail(fmt.Errorf("invalid time"))
			}
			switch e.Type {
			case "event":
				if n != seq+1 {
					return fail(fmt.Errorf("sequence gap"))
				}
				seq = n
				at = t
			case "end":
				if n != seq || t != at {
					return fail(fmt.Errorf("end sequence mismatch"))
				}
				ended = true
				complete = e.Complete
			default:
				return fail(fmt.Errorf("unexpected record type"))
			}
		}
		if err = exactJSON(b, line == 1); err != nil {
			return fail(err)
		}
		if err = validation.payload(b, e.Type); err != nil {
			return fail(err)
		}
		if visit != nil {
			if err = visit(json.RawMessage(b)); err != nil {
				return fail(err)
			}
		}
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(line)+len(part) > 64<<20 {
			return nil, fmt.Errorf("record exceeds 64 MiB")
		}
		line = append(line, part...)
		if err != bufio.ErrBufferFull {
			return line, err
		}
	}
}
