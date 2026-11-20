package experiment

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	lightpool "github.com/modulecollective/lightpool"
	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/wire"
)

const ordinaryPoll = 100 * time.Millisecond

type pilotCommand struct{ Kind, Peer string }

type pilotMessage struct {
	Kind     string             `json:"kind"`
	Node     string             `json:"node"`
	Identity *wire.Identity     `json:"identity,omitempty"`
	StartNS  int64              `json:"collectionStartUnixNS,string"`
	EndNS    int64              `json:"collectionEndUnixNS,string"`
	Snapshot *core.Snapshot     `json:"snapshot,omitempty"`
	Traffic  *lightpool.Traffic `json:"traffic,omitempty"`
	Error    string             `json:"error,omitempty"`
}

// TestOrdinaryPilotNode is invoked only as an owned child by the opt-in probe.
// No production pause API or application callbacks are involved.
func TestOrdinaryPilotNode(t *testing.T) {
	name := os.Getenv("LIGHTPOOL_PILOT_CHILD")
	if name == "" {
		t.Skip("owned subprocess helper")
	}
	if err := pilotNode(name); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func pilotNode(name string) error {
	cfg := lightpool.DefaultConfig(name)
	cfg.Bind = netip.MustParseAddrPort("127.0.0.1:0")
	node, err := lightpool.New(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = node.Shutdown() }()
	encoder := json.NewEncoder(os.Stdout)
	var output sync.Mutex
	emit := func(m pilotMessage) error { output.Lock(); defer output.Unlock(); return encoder.Encode(m) }
	identity := node.Identity()
	if err = emit(pilotMessage{Kind: "ready", Node: name, Identity: &identity}); err != nil {
		return err
	}
	commands := make(chan pilotCommand)
	inputErrors := make(chan error, 1)
	go func() {
		d := json.NewDecoder(os.Stdin)
		for {
			var c pilotCommand
			if err := d.Decode(&c); err != nil {
				inputErrors <- err
				return
			}
			commands <- c
		}
	}()
	var tick <-chan time.Time
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		select {
		case err := <-inputErrors:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case c := <-commands:
			switch c.Kind {
			case "begin":
				ticker = time.NewTicker(ordinaryPoll)
				tick = ticker.C
			case "join":
				go func() {
					start := time.Now().UnixNano()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_, err := node.Join(ctx, c.Peer)
					m := pilotMessage{Kind: "joined", Node: name, StartNS: start, EndNS: time.Now().UnixNano()}
					if err != nil {
						m.Error = err.Error()
					}
					if err = emit(m); err != nil {
						fmt.Fprintln(os.Stderr, err)
						os.Exit(1)
					}
				}()
			default:
				return fmt.Errorf("unknown child command %q", c.Kind)
			}
		case <-tick:
			start := time.Now().UnixNano()
			snapshot := node.Snapshot()
			traffic := node.Traffic()
			end := time.Now().UnixNano()
			if err = emit(pilotMessage{Kind: "sample", Node: name, StartNS: start, EndNS: end, Snapshot: &snapshot, Traffic: &traffic}); err != nil {
				return err
			}
		}
	}
}

type child struct {
	cmd      *exec.Cmd
	input    io.WriteCloser
	done     chan error
	identity wire.Identity
	log      *os.File
}

type received struct {
	message pilotMessage
	at      time.Time
	err     error
}

type sampleState struct {
	StartNS    int64        `json:"startNS,string"`
	EndNS      int64        `json:"endNS,string"`
	ReceivedNS int64        `json:"receivedNS,string"`
	Message    pilotMessage `json:"message"`
}

type observedTransition struct {
	Observer, Subject, Before, After string
	LowerNS                          int64 `json:"lowerNS,string"`
	UpperNS                          int64 `json:"upperNS,string"`
}

type ordinaryEvidence struct {
	Status                                string      `json:"status"`
	Reason                                string      `json:"reason"`
	Case                                  string      `json:"case"`
	Replicate                             uint64      `json:"replicate,string"`
	CompanionSpec                         Spec        `json:"companionSpec"`
	Protocol                              core.Config `json:"protocol"`
	Toolchain, Platform, Hostname, Kernel string
	EpochUnixNS                           int64                `json:"epochUnixNS,string"`
	PollNS                                int64                `json:"pollNS,string"`
	FaultTarget                           string               `json:"faultTarget"`
	Faults                                []signalEvidence     `json:"faults"`
	Warmup                                []sampleState        `json:"warmup"`
	Final                                 []sampleState        `json:"final"`
	Transitions                           []observedTransition `json:"transitions"`
	MaxSampleGapNS                        map[string]int64     `json:"maxSampleGapNS"`
	Samples                               map[string]int       `json:"samples"`
	Joins                                 []pilotMessage       `json:"joins"`
	Limits                                []string             `json:"limits"`
}

type signalEvidence struct {
	Signal      string
	RequestedNS int64 `json:"requestedNS,string"`
	ConfirmedNS int64 `json:"confirmedNS,string"`
	State       string
}

// TestOrdinaryPilot is opt-in and takes about eighteen minutes for nine runs.
// Each child has one real UDP/TCP node. Always resume, kill and reap on return.
func TestOrdinaryPilot(t *testing.T) {
	root := os.Getenv("LIGHTPOOL_PILOT_OUT")
	if root == "" {
		t.Skip("set LIGHTPOOL_PILOT_OUT to a new output directory for the 9-run Linux probe")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ordinary-healthy", "ordinary-pause", "ordinary-crash"} {
		for replicate := uint64(1); replicate <= 3; replicate++ {
			t.Run(fmt.Sprintf("%s-%d", name, replicate), func(t *testing.T) { runOrdinary(t, root, name, replicate) })
		}
	}
}

func runOrdinary(t *testing.T, root, name string, replicate uint64) {
	directory := filepath.Join(root, fmt.Sprintf("%s-%d", name, replicate))
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	spec, _, err := Fixture(name, replicate, os.Getenv("LIGHTPOOL_PILOT_REVISION"))
	if err != nil {
		t.Fatal("set LIGHTPOOL_PILOT_REVISION to the tested source identity:", err)
	}
	hostname, _ := os.Hostname()
	kernel, _ := os.ReadFile("/proc/version")
	evidence := ordinaryEvidence{Status: "incomplete", Case: name, Replicate: replicate, CompanionSpec: spec, Protocol: core.DefaultConfig(), Toolchain: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Hostname: hostname, Kernel: strings.TrimSpace(string(kernel)), PollNS: int64(ordinaryPoll), Faults: []signalEvidence{}, Warmup: []sampleState{}, Final: []sampleState{}, Transitions: []observedTransition{}, MaxSampleGapNS: map[string]int64{}, Samples: map[string]int{}, Joins: []pilotMessage{}, Limits: []string{
		"Separate processes use public node API, real UDP/TCP, unchanged detector defaults, no encryption. Notification drops are sampled; notifications are disabled without callbacks.",
		"Ordinary protocol seeds are ambient and unpaired. Compare cohorts, observed recovery/non-completion and event ordering with simulated companions, not calibrated latency distributions.",
		"Transition intervals span adjacent actual snapshot collection bounds, including gaps. Short transient states and exact episode counts are unobservable. Missing stopped-child samples are never fresh observations.",
		"Linux loopback and process SIGSTOP differ from directional path faults, CPU stress and Lifeguard I/O blocking. No directional-path fidelity claim is admitted.",
	}}
	defer func() {
		if evidence.Status == "incomplete" && evidence.Reason == "" {
			evidence.Reason = "probe aborted before the fixed horizon; see test output and child stderr"
		}
		if err := writeJSON(filepath.Join(directory, "summary.json"), evidence); err != nil {
			t.Error(err)
		}
	}()
	records, err := os.OpenFile(filepath.Join(directory, "samples.ndjson"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := records.Close(); err != nil {
			t.Error(err)
		}
	}()
	encoder := json.NewEncoder(records)
	messages := make(chan received, 1024)
	children := make([]*child, 0, 8)
	// The defers run on failed startup, bad warm-up, a signal failure and horizon.
	defer func() {
		for _, c := range children {
			_ = c.cmd.Process.Signal(syscall.SIGCONT)
			_ = c.cmd.Process.Kill()
			_ = c.input.Close()
			select {
			case exitErr := <-c.done:
				var killed *exec.ExitError
				if exitErr != nil && !errors.As(exitErr, &killed) {
					t.Error("wait child:", exitErr)
				}
			case <-time.After(5 * time.Second):
				t.Error("child did not reap", c.cmd.Process.Pid)
			}
			_ = c.log.Close()
		}
	}()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	startup := time.NewTimer(30 * time.Second)
	defer startup.Stop()
	for i := range 8 {
		node := fmt.Sprintf("n%03d", i)
		cmd := exec.Command(executable, "-test.run=^TestOrdinaryPilotNode$")
		cmd.Env = append(os.Environ(), "LIGHTPOOL_PILOT_CHILD="+node)
		input, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		output, outputWriter, err := childOutput(cmd)
		if err != nil {
			_ = input.Close()
			t.Fatal(err)
		}
		logfile, err := os.OpenFile(filepath.Join(directory, node+".stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = input.Close()
			_ = output.Close()
			_ = outputWriter.Close()
			t.Fatal(err)
		}
		cmd.Stderr = logfile
		if err = cmd.Start(); err != nil {
			_ = logfile.Close()
			_ = output.Close()
			_ = outputWriter.Close()
			_ = input.Close()
			t.Fatal(err)
		}
		c := &child{cmd: cmd, input: input, done: make(chan error, 1), log: logfile}
		children = append(children, c)
		go func() {
			defer func() { _ = output.Close() }()
			d := json.NewDecoder(bufio.NewReader(output))
			for {
				var m pilotMessage
				if err := d.Decode(&m); err != nil {
					if !errors.Is(err, io.EOF) {
						messages <- received{err: err}
					}
					return
				}
				messages <- received{message: m, at: time.Now()}
			}
		}()
		go func() { c.done <- cmd.Wait() }()
		if err = outputWriter.Close(); err != nil {
			t.Fatal(err)
		}
	}
	ready := 0
	for ready < 8 {
		select {
		case r := <-messages:
			if r.err != nil {
				evidence.Reason = r.err.Error()
				t.Fatal(r.err)
			}
			if r.message.Kind != "ready" {
				t.Fatal("expected ready", r.message)
			}
			i := 0
			if _, err = fmt.Sscanf(r.message.Node, "n%d", &i); err != nil || i < 0 || i >= 8 {
				t.Fatal("invalid child identity")
			}
			children[i].identity = *r.message.Identity
			ready++
		case <-startup.C:
			evidence.Reason = "30-second child startup timeout"
			t.Fatal(evidence.Reason)
		}
	}
	epoch := time.Now()
	evidence.EpochUnixNS = epoch.UnixNano()
	for _, c := range children {
		if err = json.NewEncoder(c.input).Encode(pilotCommand{Kind: "begin"}); err != nil {
			t.Fatal(err)
		}
	}
	joins := time.NewTicker(10 * time.Millisecond)
	defer joins.Stop()
	warmup := time.NewTimer(15 * time.Second)
	defer warmup.Stop()
	horizon := time.NewTimer(120 * time.Second)
	defer horizon.Stop()
	var resume <-chan time.Time
	var resumeTimer *time.Timer
	defer func() {
		if resumeTimer != nil {
			resumeTimer.Stop()
		}
	}()
	latest := map[string]sampleState{}
	warmupLatest := map[string]sampleState{}
	nextJoin := 1
	faulted := false
	var target *child
	evidence.FaultTarget = spec.Target
	if len(spec.Anomaly) > 0 {
		evidence.FaultTarget = spec.Anomaly[0]
	}
	for _, c := range children {
		if c.identity.Name == evidence.FaultTarget {
			target = c
		}
	}
	signal := func(sig syscall.Signal, label string) {
		requested := time.Since(epoch).Nanoseconds()
		if err := target.cmd.Process.Signal(sig); err != nil {
			t.Fatal(err)
		}
		state, err := awaitProcessState(target.cmd.Process.Pid, sig == syscall.SIGSTOP)
		if err != nil {
			t.Fatal(err)
		}
		evidence.Faults = append(evidence.Faults, signalEvidence{label, requested, time.Since(epoch).Nanoseconds(), state})
	}
	for {
		select {
		case <-joins.C:
			if nextJoin < 8 {
				peer := children[0].identity.Name + "/" + children[0].identity.Address.String()
				if err = json.NewEncoder(children[nextJoin].input).Encode(pilotCommand{Kind: "join", Peer: peer}); err != nil {
					t.Fatal(err)
				}
				nextJoin++
			} else {
				joins.Stop()
			}
		case r := <-messages:
			if r.err != nil {
				evidence.Reason = r.err.Error()
				t.Fatal(r.err)
			}
			m := r.message
			if m.Kind == "joined" {
				evidence.Joins = append(evidence.Joins, m)
				if m.Error != "" {
					evidence.Status, evidence.Reason = "invalid-join", m.Error
				}
				continue
			}
			if m.Kind != "sample" {
				t.Fatal("unexpected child output", m.Kind)
			}
			sample := sampleState{StartNS: m.StartNS - epoch.UnixNano(), EndNS: m.EndNS - epoch.UnixNano(), ReceivedNS: r.at.Sub(epoch).Nanoseconds(), Message: m}
			if err = encoder.Encode(sample); err != nil {
				t.Fatal(err)
			}
			if previous, ok := latest[m.Node]; ok {
				gap := sample.StartNS - previous.EndNS
				evidence.MaxSampleGapNS[m.Node] = max(gap, evidence.MaxSampleGapNS[m.Node])
				old, current := snapshotStates(previous.Message.Snapshot), snapshotStates(m.Snapshot)
				for _, n := range children {
					before, after := old[n.identity.Name], current[n.identity.Name]
					if before != after {
						evidence.Transitions = append(evidence.Transitions, observedTransition{m.Node, n.identity.Name, before, after, previous.StartNS, sample.EndNS})
					}
				}
			}
			latest[m.Node] = sample
			if sample.EndNS <= int64(15*time.Second) {
				warmupLatest[m.Node] = sample
			}
			evidence.Samples[m.Node]++
		case <-warmup.C:
			valid := evidence.Reason == ""
			for _, c := range children {
				s, ok := warmupLatest[c.identity.Name]
				evidence.Warmup = append(evidence.Warmup, s)
				if !ok || s.EndNS > int64(15*time.Second) || s.EndNS < int64(15*time.Second-2*ordinaryPoll) || !fullMembership(s.Message.Snapshot, children) {
					valid = false
				}
			}
			if !valid {
				evidence.Status, evidence.Reason = "invalid-warmup", "full membership not observed at the fixed warm-up boundary; fault not injected"
				t.Error(evidence.Reason)
				continue
			}
			faulted = true
			switch name {
			case "ordinary-pause":
				signal(syscall.SIGSTOP, "SIGSTOP")
				resumeTimer = time.NewTimer(8192 * time.Millisecond)
				resume = resumeTimer.C
			case "ordinary-crash":
				requested := time.Since(epoch).Nanoseconds()
				if err = target.cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				// Wait is already running in the child's owner goroutine. Keep its result
				// available for the common cleanup path after verifying abrupt termination.
				select {
				case exitErr := <-target.done:
					target.done <- exitErr
					evidence.Faults = append(evidence.Faults, signalEvidence{"SIGKILL", requested, time.Since(epoch).Nanoseconds(), fmt.Sprint(exitErr)})
				case <-time.After(time.Second):
					t.Fatal("killed child did not exit")
				}
			}
		case <-resume:
			signal(syscall.SIGCONT, "SIGCONT")
			resume = nil
		case <-horizon.C:
			for _, c := range children {
				if s, ok := latest[c.identity.Name]; ok {
					evidence.Final = append(evidence.Final, s)
					if (name != "ordinary-crash" || !faulted || c != target) && s.EndNS < int64(120*time.Second-2*ordinaryPoll) {
						evidence.Status, evidence.Reason = "incomplete", "missing fresh final sample for "+c.identity.Name
					}
				}
			}
			if evidence.Reason == "" && faulted {
				evidence.Status, evidence.Reason = "complete", "fixed 120-second horizon"
			}
			if evidence.Status != "complete" {
				t.Error(evidence.Status, evidence.Reason)
			}
			t.Logf("%s: %s; samples=%v transitions=%d", directory, evidence.Status, evidence.Samples, len(evidence.Transitions))
			return
		}
	}
}

func snapshotStates(s *core.Snapshot) map[string]string {
	out := map[string]string{}
	if s != nil {
		for _, m := range s.Members {
			out[m.Name] = []string{"alive", "suspect", "dead", "left"}[m.State]
		}
	}
	return out
}

func fullMembership(s *core.Snapshot, children []*child) bool {
	if s == nil || len(s.Members) != 8 {
		return false
	}
	states := snapshotStates(s)
	for _, c := range children {
		if states[c.identity.Name] != "alive" {
			return false
		}
	}
	return true
}

func awaitProcessState(pid int, stopped bool) (string, error) {
	deadline := time.Now().Add(time.Second)
	for {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			return "", err
		}
		state := ""
		for line := range strings.SplitSeq(string(data), "\n") {
			if strings.HasPrefix(line, "State:") {
				state = line
				break
			}
		}
		isStopped := strings.Contains(state, "T (stopped)")
		if isStopped == stopped {
			return state, nil
		}
		if time.Now().After(deadline) {
			return state, fmt.Errorf("process state did not become stopped=%t: %s", stopped, state)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Cmd.Wait closes StdoutPipe readers. A crash can race that close with Decode;
// own the reader instead, and close the parent's writer after Start so child
// exit yields EOF only after every buffered sample has been drained.
func childOutput(cmd *exec.Cmd) (*os.File, *os.File, error) {
	reader, writer, err := os.Pipe()
	if err == nil {
		cmd.Stdout = writer
	}
	return reader, writer, err
}

func TestPilotOutputChild(t *testing.T) {
	if os.Getenv("LIGHTPOOL_PILOT_OUTPUT_CHILD") == "" {
		t.Skip("owned output helper")
	}
	if err := json.NewEncoder(os.Stdout).Encode(pilotMessage{Kind: "sample", Node: "drained-after-wait"}); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestChildExitDoesNotCloseUndrainedSamples(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPilotOutputChild$")
	cmd.Env = append(os.Environ(), "LIGHTPOOL_PILOT_OUTPUT_CHILD=1")
	output, writer, err := childOutput(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close(); _ = writer.Close() }()
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	// Force the problematic ordering deterministically, after the child wrote
	// its small sample but before the parent has read a single byte.
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	var sample pilotMessage
	decoder := json.NewDecoder(output)
	if err = decoder.Decode(&sample); err != nil {
		t.Fatal("Wait closed pending evidence:", err)
	}
	if sample.Node != "drained-after-wait" {
		t.Fatal(sample)
	}
	if err = decoder.Decode(&sample); !errors.Is(err, io.EOF) {
		t.Fatal("expected clean EOF after final sample", err)
	}
}
