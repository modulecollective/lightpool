package lightpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/modulecollective/lightpool/core"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/internal/experiment"
	"github.com/modulecollective/lightpool/internal/ordinary"
)

func smokeRecording(t testing.TB) recordingConfig {
	return recordingConfig{Path: filepath.Join(t.TempDir(), "node.ndjson"), Source: "smoke", Binary: "test", Limits: ordinary.Limits{QueueRecords: 512, QueueBytes: 1 << 20, Records: 10000, Bytes: 16 << 20}}
}

func TestOrdinaryRecordingReplay(t *testing.T) {
	config := smokeRecording(t)
	peer := newNode(t, testConfig("peer"))
	if peer.recorder != nil {
		t.Fatal("public constructor enabled recording")
	}
	node, err := newOrdinaryNode(testConfig("observer"), &config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	join(t, node, peer)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := node.Ping(ctx, peer.Identity()); err != nil {
		t.Fatal(err)
	}
	meta := []byte("original")
	if err := node.Update(ctx, meta); err != nil {
		t.Fatal(err)
	}
	meta[0] = 'X'
	if err := node.Shutdown(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(config.Path)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := experiment.ReplayOrdinary(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"Start", "Join", "Opened", "Written", "StreamMessage", "Ping", "Datagram", "Update", "Shutdown"} {
		if counts[kind] == 0 {
			t.Fatalf("missing real input %s: %v", kind, counts)
		}
	}
	t.Logf("replayed healthy TCP join, UDP ping, update and shutdown: %v", counts)
	lines := bytes.SplitAfter(data, []byte{'\n'})
	corrupt := bytes.Replace(data, []byte(`"Kind":"member.upsert"`), []byte(`"Kind":"member.remove"`), 1)
	if bytes.Equal(corrupt, data) {
		t.Fatal("missing baseline mutation")
	}
	for _, bad := range [][]byte{bytes.Join(lines[:len(lines)-2], nil), append(bytes.Clone(lines[0]), data...), data[:len(data)-1], corrupt} {
		if _, err := experiment.ReplayOrdinary(bytes.NewReader(bad)); err == nil {
			t.Fatal("invalid journal replayed")
		}
	}
}

type stalledWriter struct {
	entered, release chan struct{}
	bytes.Buffer
}

func (w *stalledWriter) Write(b []byte) (int, error) {
	select {
	case <-w.entered:
	default:
		close(w.entered)
	}
	<-w.release
	return w.Buffer.Write(b)
}
func (*stalledWriter) Close() error { return nil }

type brokenJournal struct{}

func (brokenJournal) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (brokenJournal) Close() error              { return nil }

func TestOrdinaryWriterFailure(t *testing.T) {
	cfg := smokeRecording(t)
	record := ordinary.Record{Diagnostic: &ordinary.Diagnostic{Boundary: "read", Error: "failure"}}
	w := startOrdinaryWriter(cfg, brokenJournal{})
	w.submit(record)
	if err := w.finish(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	cfg.Limits.QueueRecords = 1
	stalled := &stalledWriter{entered: make(chan struct{}), release: make(chan struct{})}
	w = startOrdinaryWriter(cfg, stalled)
	w.submit(record)
	<-stalled.entered
	w.submit(record)
	w.submit(record)
	close(stalled.release)
	if err := w.finish(); err == nil || !strings.Contains(err.Error(), "overflow") || !strings.Contains(stalled.String(), `"status":"incomplete"`) {
		t.Fatalf("overflow: %v %s", err, stalled.String())
	}
}

func TestOrdinaryRecorderFileCap(t *testing.T) {
	cfg := smokeRecording(t)
	cfg.Limits.Records = 4
	node, err := newOrdinaryNode(testConfig("limited"), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Shutdown(); err == nil || !strings.Contains(err.Error(), "cap exhausted") {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"status":"incomplete"`)) {
		t.Fatal("missing incomplete status")
	}
	if _, err := experiment.ReplayOrdinary(bytes.NewReader(data)); err == nil {
		t.Fatal("replayed capped journal")
	}
}

// Exhaust the driver's stream registry and check the exact parent of the
// synchronous Opened failure. A socket peer then holds the next join open
// until its caller cancels, so cancellation has a real outstanding operation.
func TestOrdinaryRecordingImmediateAndCancel(t *testing.T) {
	cfg := testConfig("observer")
	cfg.Protocol.MaxPushPull = 1
	cfg.Protocol.StreamTimeout = 10 * time.Second
	cfg.Protocol.ProbeInterval = 0
	cfg.Protocol.PushPullInterval = 0
	cfg.Protocol.GossipInterval = 0
	recording := smokeRecording(t)
	node, err := newOrdinaryNode(cfg, &recording)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	for id := uint64(1); id <= 2; id++ {
		if node.register(core.Token{ID: id}, nil) == nil {
			t.Fatal("stream slot unavailable")
		}
	}
	socket, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = socket.Close() }()
	endpoint := "peer/" + socket.Addr().String()
	if _, err := node.Join(t.Context(), endpoint); !errors.Is(err, OperationError{core.LocalFailure}) {
		t.Fatalf("expected immediate stream failure, got %v", err)
	}
	node.closeStream(core.Token{ID: 1})
	node.closeStream(core.Token{ID: 2})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	joined := make(chan error, 1)
	go func() { _, err := node.Join(ctx, endpoint); joined <- err }()
	if err := socket.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := socket.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	cancel()
	if err := <-joined; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Commands on the same FIFO follow the CancelCommand from Join.
	if err := node.Update(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if err := node.Shutdown(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(recording.Path)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := experiment.ReplayOrdinary(bytes.NewReader(data))
	if err != nil || counts["CancelCommand"] != 1 {
		t.Fatalf("replay %v: %v", counts, err)
	}
	var joinSeq uint64
	for line := range bytes.Lines(data) {
		var r ordinary.Record
		if err := ordinary.Decode(line, &r); err != nil {
			t.Fatal(err)
		}
		if s := r.Step; s != nil {
			if s.Input.Kind == "Join" {
				joinSeq = s.Sequence
			}
			if s.Input.Kind == "Opened" && joinSeq != 0 && s.Parent == joinSeq {
				return
			}
		}
	}
	t.Fatal("missing recursive Opened input for Join")
}

func BenchmarkHealthyRecordedPing(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("recording-%t", enabled), func(b *testing.B) {
			cfg := testConfig("observer")
			cfg.Protocol.ProbeInterval = 0
			cfg.Protocol.PushPullInterval = 0
			cfg.Protocol.GossipInterval = 0
			peerCfg := cfg
			peerCfg.Name = "peer"
			peer := newNode(b, peerCfg)
			var recording *recordingConfig
			if enabled {
				c := smokeRecording(b)
				c.Limits.Records = 5_000_000
				c.Limits.Bytes = 2 << 30
				recording = &c
			}
			node, err := newOrdinaryNode(cfg, recording)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := node.Shutdown(); err != nil {
					b.Error(err)
				}
			})
			join(b, node, peer)
			target := peer.Identity()
			for b.Loop() {
				if _, err := node.Ping(b.Context(), target); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// An altered timing annotation must fail even when the input, effects,
// inspection and byte count still match the original journal.
func TestReplayChecksTimerDeadline(t *testing.T) {
	cfg := testConfig("observer")
	cfg.Protocol.GossipInterval = time.Millisecond
	c := smokeRecording(t)
	node, err := newOrdinaryNode(cfg, &c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	wait(t, "timer dispatch", func() bool { return node.Snapshot().Stats.Steps >= 3 })
	if err := node.Shutdown(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(c.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := experiment.ReplayOrdinary(bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	marker := []byte(`"timerDueNS":`)
	i := bytes.Index(raw, marker)
	if i < 0 {
		t.Fatal("missing due time")
	}
	pos := i + len(marker)
	if raw[pos] == '1' {
		raw[pos] = '2'
	} else {
		raw[pos] = '1'
	}
	if _, err := experiment.ReplayOrdinary(bytes.NewReader(raw)); err == nil || !strings.Contains(err.Error(), "timer deadline mismatch") {
		t.Fatalf("altered timer due: %v", err)
	}
}
