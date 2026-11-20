//go:build linux

package lightpool

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/modulecollective/lightpool/internal/experiment"
	"github.com/modulecollective/lightpool/internal/ordinary"
	"github.com/modulecollective/lightpool/wire"
)

// The ordinary witness harness. TestOrdinaryWitness re-executes itself as pid 1
// of a private user, network, pid and mount namespace tree; that process is the
// router. Each node runs in its own network namespace behind a veth, so its TCP
// source address is its own. When pid 1 exits the kernel kills every process
// left in the tree; the outer test then checks no marked process survived.

const (
	witnessChildEnv = "LIGHTPOOL_WITNESS_CHILD"
	witnessTokenEnv = "LIGHTPOOL_WITNESS_TOKEN"
	sysSetns        = 308 // amd64; the harness refuses other architectures
)

type witnessCommand struct {
	Kind    string           `json:"kind"`
	ID      uint64           `json:"id,omitempty"`
	Name    string           `json:"name,omitempty"`
	Bind    string           `json:"bind,omitempty"`
	Journal string           `json:"journal,omitempty"`
	Limits  *ordinary.Limits `json:"limits,omitempty"`
	Source  string           `json:"source,omitempty"`
	Binary  string           `json:"binary,omitempty"`
	Peer    string           `json:"peer,omitempty"`
}

type witnessReply struct {
	Kind      string                    `json:"kind"`
	Node      string                    `json:"node"`
	ID        uint64                    `json:"id,omitempty"`
	PID       int                       `json:"pid,omitempty"`
	Identity  *wire.Identity            `json:"identity,omitempty"`
	Lifetime  uint64                    `json:"lifetime,omitempty"`
	ChildNS   int64                     `json:"childNS,omitempty"`
	Join      *experiment.WitnessJoin   `json:"join,omitempty"`
	Sample    *experiment.WitnessSample `json:"sample,omitempty"`
	Shutdown  string                    `json:"shutdown,omitempty"`
	Recording string                    `json:"recording,omitempty"`
}

// TestOrdinaryWitnessChild is only run as an owned subprocess.
func TestOrdinaryWitnessChild(t *testing.T) {
	if os.Getenv(witnessChildEnv) == "" {
		t.Skip("owned witness subprocess")
	}
	if err := witnessChild(); err != nil {
		fmt.Fprintln(os.Stderr, "witness child:", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func witnessChild() error {
	encoder := json.NewEncoder(os.Stdout)
	var output sync.Mutex
	emit := func(r witnessReply) {
		output.Lock()
		defer output.Unlock()
		if err := encoder.Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, "witness child output:", err)
			os.Exit(1)
		}
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	var node *Node
	var name string
	stop := make(chan struct{})
	var sampling sync.WaitGroup
	defer func() {
		if node != nil {
			_ = node.Shutdown()
		}
	}()
	for {
		var c witnessCommand
		if err := decoder.Decode(&c); err != nil {
			return fmt.Errorf("coordinator input: %w", err)
		}
		if node == nil && c.Kind != "start" {
			return fmt.Errorf("%s before start", c.Kind)
		}
		switch c.Kind {
		case "start":
			bind, err := netip.ParseAddrPort(c.Bind)
			if err != nil {
				return err
			}
			cfg := DefaultConfig(c.Name)
			cfg.Bind = bind
			var recording *recordingConfig
			if c.Journal != "" {
				recording = &recordingConfig{Path: c.Journal, Source: c.Source, Binary: c.Binary, Limits: *c.Limits}
			}
			if node, err = newOrdinaryNode(cfg, recording); err != nil {
				return err
			}
			name = c.Name
			identity := node.Identity()
			emit(witnessReply{Kind: "ready", Node: name, PID: os.Getpid(), Identity: &identity, Lifetime: node.life})
		case "clock":
			emit(witnessReply{Kind: "clock", Node: name, ID: c.ID, ChildNS: int64(node.now())})
		case "join":
			go func() {
				j := experiment.WitnessJoin{Node: name, StartNS: int64(node.now())}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := node.Join(ctx, c.Peer); err != nil {
					j.Error = err.Error()
				}
				j.EndNS = int64(node.now())
				emit(witnessReply{Kind: "joined", Node: name, Join: &j})
			}()
		case "begin":
			sampling.Go(func() {
				ticker := time.NewTicker(time.Duration(experiment.FrozenWitnessSchedule().PollNS))
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
					}
					s := experiment.WitnessSample{Node: name, StartNS: int64(node.now())}
					snapshot, traffic := node.Snapshot(), node.Traffic()
					s.EndNS = int64(node.now())
					s.States, s.Health, s.Backlog, s.SentBytes, s.ReceivedBytes = experiment.WitnessStates(snapshot), snapshot.Health, snapshot.Backlog, traffic.SentBytes, traffic.ReceivedBytes
					if node.recorder != nil {
						if err := node.recorder.err(); err != nil {
							s.Recording = err.Error()
						}
					}
					emit(witnessReply{Kind: "sample", Node: name, Sample: &s})
				}
			})
		case "stop":
			close(stop)
			sampling.Wait()
			r := witnessReply{Kind: "exit", Node: name}
			if err := node.Shutdown(); err != nil {
				r.Recording = err.Error()
			}
			node = nil
			emit(r)
			return nil
		default:
			return fmt.Errorf("unknown command %q", c.Kind)
		}
	}
}

// TestOrdinaryWitness is opt-in. LIGHTPOOL_WITNESS_PLAN selects controls,
// smoke, selftest or cells; results land in LIGHTPOOL_WITNESS_OUT.
func TestOrdinaryWitness(t *testing.T) {
	out, plan := os.Getenv("LIGHTPOOL_WITNESS_OUT"), os.Getenv("LIGHTPOOL_WITNESS_PLAN")
	if out == "" {
		t.Skip("set LIGHTPOOL_WITNESS_OUT and LIGHTPOOL_WITNESS_PLAN for the Linux witness study")
	}
	if runtime.GOARCH != "amd64" {
		t.Skip("setns syscall number is pinned for amd64")
	}
	if os.Getenv("LIGHTPOOL_WITNESS_INNER") == "" {
		witnessOuter(t)
		return
	}
	if os.Getpid() != 1 || os.Getuid() != 0 {
		t.Fatal("inner coordinator must be pid 1 and root of its own namespaces")
	}
	h, err := newWitnessHost()
	if err != nil {
		t.Fatal(err)
	}
	switch plan {
	case "controls":
		if err := h.controls(filepath.Join(out, "controls")); err != nil {
			t.Fatal(err)
		}
	case "smoke":
		cell := experiment.WitnessCell{Order: 0, LatencyNS: int64(time.Millisecond), Index: 1, Mode: "on"}
		if err := h.attempt(filepath.Join(out, "smoke"), cell, ""); err != nil {
			t.Fatal(err)
		}
	case "selftest":
		h.selftest(t, out)
	case "cells":
		h.cells(t, out)
	default:
		t.Fatalf("unknown plan %q", plan)
	}
}

func witnessOuter(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	token := strconv.FormatInt(time.Now().UnixNano(), 36)
	cmd := exec.Command("unshare", "--user", "--map-root-user", "--net", "--pid", "--fork", "--mount-proc", "--kill-child",
		executable, "-test.run=^TestOrdinaryWitness$", "-test.v", "-test.timeout=0")
	cmd.Env = append(os.Environ(), "LIGHTPOOL_WITNESS_INNER=1", witnessTokenEnv+"="+token)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	runErr := cmd.Run()
	// Every process in the tree carries the token; none may outlive pid 1.
	if survivors := witnessMarked(token); len(survivors) > 0 {
		for _, pid := range survivors {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		t.Errorf("processes survived the namespace tree: %v", survivors)
	}
	if runErr != nil {
		t.Fatal("inner coordinator:", runErr)
	}
}

func witnessMarked(token string) []int {
	var out []int
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		env, err := os.ReadFile(filepath.Join("/proc", e.Name(), "environ"))
		if err == nil && slices.Contains(strings.Split(string(env), "\x00"), witnessTokenEnv+"="+token) {
			out = append(out, pid)
		}
	}
	return out
}

type witnessHost struct {
	tools      map[string]string
	source     string
	binary     string
	executable string
	kernel     string
}

func newWitnessHost() (*witnessHost, error) {
	h := &witnessHost{tools: map[string]string{}, source: os.Getenv("LIGHTPOOL_WITNESS_SOURCE"), binary: os.Getenv("LIGHTPOOL_WITNESS_BINARY")}
	if h.source == "" || h.binary == "" {
		return nil, errors.New("LIGHTPOOL_WITNESS_SOURCE and LIGHTPOOL_WITNESS_BINARY are required")
	}
	var err error
	if h.executable, err = os.Executable(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(h.executable)
	if err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != h.binary {
		return nil, errors.New("running binary differs from LIGHTPOOL_WITNESS_BINARY")
	}
	kernel, _ := os.ReadFile("/proc/version")
	h.kernel = strings.TrimSpace(string(kernel))
	path := os.Getenv("PATH") + ":/usr/sbin:/sbin"
	for _, tool := range []string{"ip", "tc", "nft", "nsenter"} {
		for dir := range strings.SplitSeq(path, ":") {
			if p := filepath.Join(dir, tool); dir != "" && h.tools[tool] == "" {
				if info, err := os.Stat(p); err == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 {
					h.tools[tool] = p
				}
			}
		}
		if h.tools[tool] == "" {
			return nil, fmt.Errorf("%s not found", tool)
		}
	}
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0); err != nil {
		return nil, err
	}
	_, err = h.run(nil, "", "ip", "link", "set", "lo", "up")
	return h, err
}

// run executes a tool, optionally inside ns (passed as fd 3 to nsenter).
func (h *witnessHost) run(ns *os.File, stdin string, tool string, args ...string) (string, error) {
	cmd := exec.Command(h.tools[tool], args...)
	if ns != nil {
		cmd = exec.Command(h.tools["nsenter"], append([]string{"--net=/proc/self/fd/3", h.tools[tool]}, args...)...)
		cmd.ExtraFiles = []*os.File{ns}
	}
	cmd.Stdin = strings.NewReader(stdin)
	// Every tool call is bounded; a wedged netlink call fails the attempt.
	timer := time.AfterFunc(30*time.Second, func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	defer timer.Stop()
	b, err := cmd.CombinedOutput()
	if err != nil {
		return string(b), fmt.Errorf("%s %s: %w: %s", tool, strings.Join(args, " "), err, b)
	}
	return string(b), nil
}

func setns(f *os.File) error {
	if _, _, errno := syscall.RawSyscall(sysSetns, f.Fd(), syscall.CLONE_NEWNET, 0); errno != 0 {
		return errno
	}
	return nil
}

// inNetns runs fn on a locked thread switched into ns. Sockets keep the
// namespace they were created in. If switching back fails the thread stays
// locked and is discarded with its goroutine.
func inNetns(ns *os.File, fn func() error) error {
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		original, err := os.Open("/proc/thread-self/ns/net")
		if err != nil {
			runtime.UnlockOSThread()
			done <- err
			return
		}
		defer func() { _ = original.Close() }()
		if ns == nil {
			err = syscall.Unshare(syscall.CLONE_NEWNET)
		} else {
			err = setns(ns)
		}
		if err == nil {
			err = fn()
		}
		if back := setns(original); back != nil {
			done <- errors.Join(err, back)
			return
		}
		runtime.UnlockOSThread()
		done <- err
	}()
	return <-done
}

func newNetns() (*os.File, error) {
	var ns *os.File
	err := inNetns(nil, func() (err error) {
		ns, err = os.Open("/proc/thread-self/ns/net")
		return err
	})
	return ns, err
}

func witnessAddress(i int) netip.Addr { return netip.AddrFrom4([4]byte{10, 77, byte(i), 2}) }

// The fault is one atomic forward rule with a named counter, so the final
// packet count survives the rule's removal.
const witnessTable = "table inet lightpool {\n\tcounter fault {}\n\tchain forward {\n\t\ttype filter hook forward priority 0; policy accept;\n\t}\n}\n"

func (h *witnessHost) installFault(anomaly []netip.Addr, target netip.Addr, since func() int64) (*experiment.WitnessFault, error) {
	sources := make([]string, len(anomaly))
	for i, a := range anomaly {
		sources[i] = a.String()
	}
	rule := fmt.Sprintf("ip saddr { %s } ip daddr %s meta l4proto { tcp, udp } counter name \"fault\" drop", strings.Join(sources, ", "), target)
	f := &experiment.WitnessFault{Rule: rule, RequestedNS: since()}
	out, err := h.run(nil, "", "nft", "-e", "-a", "add", "rule", "inet", "lightpool", "forward", rule)
	f.AppliedNS = since()
	if err != nil {
		return nil, err
	}
	m := regexp.MustCompile(`# handle (\d+)`).FindStringSubmatch(out)
	if m == nil {
		return nil, fmt.Errorf("nft did not echo a rule handle: %s", out)
	}
	f.Handle, _ = strconv.Atoi(m[1])
	return f, nil
}

func (h *witnessHost) removeFault(f *experiment.WitnessFault, since func() int64) error {
	f.RemoveRequestedNS = since()
	_, err := h.run(nil, "", "nft", "delete", "rule", "inet", "lightpool", "forward", "handle", strconv.Itoa(f.Handle))
	f.RemovedNS = since()
	if err != nil {
		return err
	}
	out, err := h.run(nil, "", "nft", "-j", "list", "counter", "inet", "lightpool", "fault")
	if err != nil {
		return err
	}
	var counter struct {
		Nftables []struct {
			Counter *struct{ Packets, Bytes uint64 } `json:"counter"`
		} `json:"nftables"`
	}
	if err := json.Unmarshal([]byte(out), &counter); err != nil {
		return err
	}
	for _, c := range counter.Nftables {
		if c.Counter != nil {
			f.Packets, f.Bytes = c.Counter.Packets, c.Counter.Bytes
			return nil
		}
	}
	return errors.New("fault counter missing")
}

// survivors lists every process in this pid namespace except pid 1.
func witnessSurvivors() []int {
	out := []int{}
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		if pid, err := strconv.Atoi(e.Name()); err == nil && pid != 1 {
			out = append(out, pid)
		}
	}
	return out
}

func freeBytes(path string) (uint64, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return 0, err
	}
	return s.Bavail * uint64(s.Bsize), nil
}

type witnessProcess struct {
	name   string
	cmd    *exec.Cmd
	input  io.WriteCloser
	done   chan error
	waited bool
	exit   *experiment.WitnessExit
	log    *os.File
}

type witnessMessage struct {
	reply witnessReply
	at    int64
	err   error
}

// sidecar bounds every coordinator-written stream of one attempt together.
type sidecar struct {
	file  *os.File
	total *int64
}

func (s sidecar) Write(b []byte) (int, error) {
	if *s.total += int64(len(b)); *s.total > experiment.WitnessSidecarBytes {
		return 0, errors.New("attempt sidecar budget exhausted")
	}
	return s.file.Write(b)
}

// attempt runs one cell in dir. inject names a deliberate failure for the
// selftest; campaign cells always pass "". The returned error is also the
// status reason; every path tears down the namespaces and child processes.
func (h *witnessHost) attempt(dir string, cell experiment.WitnessCell, inject string) (err error) {
	start := time.Now()
	since := func() int64 { return time.Since(start).Nanoseconds() }
	schedule := experiment.FrozenWitnessSchedule()
	spec, _, err := experiment.Fixture("correlated-dense", cell.Index, h.source)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	need := uint64(3 * experiment.WitnessSidecarBytes)
	if cell.Mode == "on" {
		need += 3 * uint64(experiment.FrozenWitnessLimits().Bytes) * experiment.WitnessNodes
	}
	free, err := freeBytes(dir)
	if err != nil || free < need {
		return fmt.Errorf("disk preflight: %d bytes free, %d required (%v)", free, need, err)
	}
	st := experiment.WitnessStatus{Format: experiment.WitnessVersion, Status: "incomplete", Joins: []experiment.WitnessJoin{}, Exits: []experiment.WitnessExit{}, Survivors: []int{}}
	m := experiment.WitnessManifest{Format: experiment.WitnessVersion, Cell: cell, Spec: spec, Schedule: schedule, Limits: experiment.FrozenWitnessLimits(), SidecarBytes: experiment.WitnessSidecarBytes, Source: h.source, Binary: h.binary, Toolchain: runtime.Version(), Kernel: h.kernel, Tools: h.tools}
	var total int64
	open := func(name string) (sidecar, error) {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		return sidecar{f, &total}, err
	}
	samples, err := open("samples.ndjson")
	if err != nil {
		return err
	}
	clocks, err := open("clock.ndjson")
	if err != nil {
		return err
	}
	resources := map[string]any{"cpus": runtime.NumCPU(), "freeBytesBefore": free}
	readProc := func(key string) {
		for _, f := range []string{"loadavg", "meminfo"} {
			b, _ := os.ReadFile("/proc/" + f)
			if f == "meminfo" {
				b = []byte(strings.SplitN(string(b), "\n", 4)[2])
			}
			resources[f+key] = strings.TrimSpace(string(b))
		}
	}
	readProc("Before")
	var before syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_CHILDREN, &before)
	var namespaces []*os.File
	var processes []*witnessProcess
	var stopAll func()
	qdisc := map[string]json.RawMessage{}
	defer func() {
		if stopAll != nil {
			stopAll()
		}
		// Orderly first: stop asks nodes to finish their journals. Then kill and
		// reap whatever remains, release namespaces and check for survivors.
		for _, p := range processes {
			if !p.waited {
				_ = p.cmd.Process.Signal(syscall.SIGCONT)
				_ = p.cmd.Process.Kill()
				select {
				case e := <-p.done:
					p.waited = true
					if p.exit == nil {
						p.exit = &experiment.WitnessExit{Node: p.name, PID: p.cmd.Process.Pid, Shutdown: "killed during teardown"}
					}
					p.exit.Wait = fmt.Sprint(e)
				case <-time.After(5 * time.Second):
					err = errors.Join(err, fmt.Errorf("%s did not reap", p.name))
				}
			}
			_ = p.input.Close()
			_ = p.log.Close()
		}
		for i, ns := range namespaces {
			if out, e := h.run(ns, "", "tc", "-s", "-j", "qdisc", "show", "dev", fmt.Sprintf("c%d", i)); e == nil {
				qdisc[fmt.Sprintf("n%03d", i)] = json.RawMessage(out)
			}
			_ = ns.Close()
		}
		_, _ = h.run(nil, "", "nft", "delete", "table", "inet", "lightpool")
		if links, e := h.run(nil, "", "ip", "-br", "link"); e != nil || strings.Contains(links, "r0@") {
			err = errors.Join(err, fmt.Errorf("router links survived teardown: %v %s", e, links))
		}
		st.Survivors = witnessSurvivors()
		for _, pid := range st.Survivors {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		if len(st.Survivors) > 0 {
			err = errors.Join(err, fmt.Errorf("surviving processes %v", st.Survivors))
		}
		for _, p := range processes {
			if p.exit != nil {
				st.Exits = append(st.Exits, *p.exit)
			}
		}
		var after syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_CHILDREN, &after)
		resources["childUserNS"] = (after.Utime.Nano() - before.Utime.Nano())
		resources["childSystemNS"] = (after.Stime.Nano() - before.Stime.Nano())
		resources["maxChildRSSKiB"] = after.Maxrss
		readProc("After")
		resources["freeBytesAfter"], _ = freeBytes(dir)
		if err != nil {
			st.Status, st.Reason = "incomplete", err.Error()
		} else if !st.SampledWarmup {
			st.Status, st.Reason = "invalid-warmup", "sampled membership not fully alive at the warm-up boundary; fault not injected"
		} else {
			st.Status, st.Reason = "complete", "fixed 120-second horizon"
		}
		for name, v := range map[string]any{"status.json": st, "qdisc.json": qdisc, "resources.json": resources} {
			b, e := json.MarshalIndent(v, "", "  ")
			if e == nil {
				e = os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o600)
			}
			err = errors.Join(err, e)
		}
		err = errors.Join(err, samples.file.Close(), clocks.file.Close())
		if err == nil && st.Status != "complete" {
			err = errors.New(st.Reason)
		}
	}()
	if _, err = h.run(nil, witnessTable, "nft", "-f", "-"); err != nil {
		return err
	}
	messages := make(chan witnessMessage, 4096)
	for i := range experiment.WitnessNodes {
		name := fmt.Sprintf("n%03d", i)
		ns, err := newNetns()
		if err != nil {
			return err
		}
		namespaces = append(namespaces, ns)
		if err = h.link(i, ns, time.Duration(cell.LatencyNS)); err != nil {
			return err
		}
		cmd := exec.Command(h.tools["nsenter"], "--net=/proc/self/fd/3", h.executable, "-test.run=^TestOrdinaryWitnessChild$", "-test.timeout=0")
		cmd.ExtraFiles = []*os.File{ns}
		cmd.Env = append(os.Environ(), witnessChildEnv+"=1")
		input, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		reader, writer, err := os.Pipe()
		if err != nil {
			return err
		}
		cmd.Stdout = writer
		log, err := os.OpenFile(filepath.Join(dir, "node-"+name+".stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		cmd.Stderr = log
		if err = cmd.Start(); err != nil {
			return err
		}
		_ = writer.Close()
		p := &witnessProcess{name: name, cmd: cmd, input: input, done: make(chan error, 1), log: log}
		processes = append(processes, p)
		go func() { p.done <- cmd.Wait() }()
		go func() {
			defer func() { _ = reader.Close() }()
			d := json.NewDecoder(bufio.NewReader(reader))
			for {
				var r witnessReply
				if err := d.Decode(&r); err != nil {
					if !errors.Is(err, io.EOF) {
						messages <- witnessMessage{err: fmt.Errorf("%s output: %w", name, err)}
					}
					return
				}
				messages <- witnessMessage{reply: r, at: since()}
			}
		}()
	}
	send := func(p *witnessProcess, c witnessCommand) error {
		b, err := json.Marshal(c)
		if err == nil {
			_, err = p.input.Write(append(b, '\n'))
		}
		if err != nil {
			return fmt.Errorf("%s input: %w", p.name, err)
		}
		return nil
	}
	limits := experiment.FrozenWitnessLimits()
	for i, p := range processes {
		c := witnessCommand{Kind: "start", Name: p.name, Bind: netip.AddrPortFrom(witnessAddress(i), 7946).String()}
		if cell.Mode == "on" {
			l := limits
			if inject == "record-cap" && i == 3 {
				l.Records = 64
			}
			c.Journal, c.Limits, c.Source, c.Binary = filepath.Join(dir, "node-"+p.name+".ndjson"), &l, h.source, h.binary
		}
		if inject == "startup" && i == 5 {
			c.Bind = "192.0.2.1:7946"
		}
		if err = send(p, c); err != nil {
			return err
		}
	}
	byName := map[string]*witnessProcess{}
	for _, p := range processes {
		byName[p.name] = p
	}
	// One loop owns all coordinator state. await consumes messages until done.
	var latest = map[string]experiment.WitnessSample{}
	type request struct {
		id       uint64
		exchange ordinary.Exchange
	}
	pending := map[string]request{}
	var clockID uint64
	phase := "pre-join"
	var failure error
	handle := func(msg witnessMessage) {
		if msg.err != nil {
			failure = errors.Join(failure, msg.err)
			return
		}
		r := msg.reply
		p := byName[r.Node]
		if p == nil {
			failure = errors.Join(failure, fmt.Errorf("reply from unknown node %q", r.Node))
			return
		}
		switch r.Kind {
		case "ready":
			if r.PID != p.cmd.Process.Pid || r.Identity == nil || r.Identity.Name != p.name {
				failure = errors.Join(failure, fmt.Errorf("%s ready from a different process or identity", p.name))
				return
			}
			seed := "full seed in the journal header"
			journal := "node-" + p.name + ".ndjson"
			if cell.Mode == "off" {
				seed, journal = "unavailable: journal disabled", ""
			}
			m.Nodes = append(m.Nodes, experiment.WitnessNode{Name: p.name, Role: experiment.WitnessRole(spec, p.name), Address: r.Identity.Address, PID: r.PID, Lifetime: r.Lifetime, Journal: journal, Seed: seed})
		case "clock":
			q, ok := pending[r.Node]
			if !ok || q.id != r.ID {
				failure = errors.Join(failure, fmt.Errorf("%s answered an unknown clock request", r.Node))
				return
			}
			delete(pending, r.Node)
			e := q.exchange
			e.ChildNS, e.ReceiveNS = r.ChildNS, msg.at
			b, _ := json.Marshal(experiment.WitnessClock{Node: r.Node, Exchange: e})
			if _, err := clocks.Write(append(b, '\n')); err != nil {
				failure = errors.Join(failure, err)
			}
		case "joined":
			for i := range st.Joins {
				if st.Joins[i].Node == r.Node {
					st.Joins[i].StartNS, st.Joins[i].EndNS, st.Joins[i].Error = r.Join.StartNS, r.Join.EndNS, r.Join.Error
				}
			}
		case "sample":
			s := *r.Sample
			s.ReceivedNS = msg.at
			latest[r.Node] = s
			b, _ := json.Marshal(s)
			if _, err := samples.Write(append(b, '\n')); err != nil {
				failure = errors.Join(failure, err)
			}
			if s.Recording != "" {
				failure = errors.Join(failure, fmt.Errorf("%s recording failed: %s", r.Node, s.Recording))
			}
		case "exit":
			p.exit = &experiment.WitnessExit{Node: p.name, PID: p.cmd.Process.Pid, Shutdown: r.Shutdown, Recording: r.Recording}
		}
	}
	exited := func() error {
		for _, p := range processes {
			select {
			case e := <-p.done:
				p.waited = true
				p.done <- e
				return fmt.Errorf("%s exited early: %v", p.name, e)
			default:
			}
		}
		return nil
	}
	await := func(what string, limit time.Duration, ready func() bool) error {
		deadline := time.NewTimer(limit)
		defer deadline.Stop()
		for !ready() {
			select {
			case msg := <-messages:
				handle(msg)
				if failure != nil {
					return failure
				}
			case <-deadline.C:
				return fmt.Errorf("%s: timed out after %s", what, limit)
			case <-time.After(100 * time.Millisecond):
				if e := exited(); e != nil {
					return e
				}
			}
		}
		return nil
	}
	sample := func() error {
		for _, p := range processes {
			if _, busy := pending[p.name]; busy {
				continue // an unanswered request is never overlapped
			}
			clockID++
			pending[p.name] = request{clockID, ordinary.Exchange{PID: p.cmd.Process.Pid, Phase: phase, SendNS: since()}}
			if err := send(p, witnessCommand{Kind: "clock", ID: clockID}); err != nil {
				return err
			}
		}
		return nil
	}
	// On any failure, ask live nodes to stop so recorders still write footers;
	// teardown kills whatever does not answer within the bound.
	stopAll = func() {
		var live []*witnessProcess
		for _, p := range processes {
			if p.exit == nil && !p.waited && send(p, witnessCommand{Kind: "stop"}) == nil {
				live = append(live, p)
			}
		}
		deadline := time.After(30 * time.Second)
		for slices.ContainsFunc(live, func(p *witnessProcess) bool { return p.exit == nil }) {
			select {
			case msg := <-messages:
				handle(msg)
			case <-deadline:
				return
			}
		}
	}
	if err = await("startup", 30*time.Second, func() bool { return len(m.Nodes) == experiment.WitnessNodes }); err != nil {
		return err
	}
	slices.SortFunc(m.Nodes, func(a, b experiment.WitnessNode) int { return strings.Compare(a.Name, b.Name) })
	b, _ := json.MarshalIndent(m, "", "  ")
	if err = os.WriteFile(filepath.Join(dir, "manifest.json"), append(b, '\n'), 0o600); err != nil {
		return err
	}
	if err = sample(); err != nil {
		return err
	}
	if err = await("pre-join clock", 5*time.Second, func() bool { return len(pending) == 0 }); err != nil {
		return err
	}
	phase = "measure"
	epoch := since()
	st.EpochNS = epoch
	at := func(ns int64) <-chan time.Time { return time.After(time.Duration(epoch + ns - since())) }
	for _, p := range processes {
		if err = send(p, witnessCommand{Kind: "begin"}); err != nil {
			return err
		}
	}
	anomaly := make([]netip.Addr, 0, len(spec.Anomaly))
	for _, a := range spec.Anomaly {
		var i int
		_, _ = fmt.Sscanf(a, "n%03d", &i)
		anomaly = append(anomaly, witnessAddress(i))
	}
	var target int
	_, _ = fmt.Sscanf(spec.Target, "n%03d", &target)
	nextJoin := 1
	join := at(schedule.JoinSpacingNS)
	clock := time.NewTicker(time.Duration(schedule.ClockNS))
	defer clock.Stop()
	warmup, faultEnd, horizon := at(schedule.WarmupNS), at(schedule.FaultEndNS), at(schedule.HorizonNS)
	var cancel <-chan time.Time
	if inject == "cancel" {
		cancel = at(5 * int64(time.Second))
	}
	var fault *experiment.WitnessFault
	for horizon != nil {
		select {
		case msg := <-messages:
			handle(msg)
		case <-join:
			p := processes[nextJoin]
			peer := m.Nodes[0]
			st.Joins = append(st.Joins, experiment.WitnessJoin{Node: p.name, RequestedNS: epoch + int64(nextJoin)*schedule.JoinSpacingNS, SentNS: since()})
			failure = errors.Join(failure, send(p, witnessCommand{Kind: "join", Peer: peer.Name + "/" + peer.Address.String()}))
			if nextJoin++; nextJoin < experiment.WitnessNodes {
				join = at(int64(nextJoin) * schedule.JoinSpacingNS)
			} else {
				join = nil
			}
		case <-clock.C:
			failure = errors.Join(failure, sample())
		case <-warmup:
			st.Warmup.LoNS = since()
			st.SampledWarmup = true
			for _, p := range processes {
				s, ok := latest[p.name]
				if !ok || s.ReceivedNS < st.Warmup.LoNS-2*schedule.PollNS || strings.Trim(s.States, "a") != "" {
					st.SampledWarmup = false
				}
			}
			if cell.Impaired && st.SampledWarmup {
				fault, err = h.installFault(anomaly, witnessAddress(target), since)
				failure = errors.Join(failure, err)
				st.Fault = fault
			}
			st.Warmup.HiNS = since()
		case <-faultEnd:
			if fault != nil {
				failure = errors.Join(failure, h.removeFault(fault, since))
			}
		case <-horizon:
			horizon = nil
		case <-cancel:
			failure = errors.Join(failure, errors.New("attempt canceled by coordinator"))
		case <-time.After(200 * time.Millisecond):
			failure = errors.Join(failure, exited())
		}
		if failure != nil {
			if fault != nil && fault.RemovedNS == 0 {
				_ = h.removeFault(fault, since)
			}
			return failure
		}
	}
	st.Horizon.LoNS = since()
	phase = "post-horizon"
	for len(pending) > 0 {
		if err = await("final clock", 5*time.Second, func() bool { return len(pending) == 0 }); err != nil {
			return err
		}
	}
	if err = sample(); err != nil {
		return err
	}
	if err = await("post-horizon clock", 5*time.Second, func() bool { return len(pending) == 0 }); err != nil {
		return err
	}
	st.Horizon.HiNS = since()
	stopAll = nil
	for _, p := range processes {
		if err = send(p, witnessCommand{Kind: "stop"}); err != nil {
			return err
		}
	}
	if err = await("orderly stop", 60*time.Second, func() bool {
		return !slices.ContainsFunc(processes, func(p *witnessProcess) bool { return p.exit == nil })
	}); err != nil {
		return err
	}
	for _, p := range processes {
		select {
		case e := <-p.done:
			p.waited = true
			if e != nil {
				p.exit.Wait = e.Error()
				failure = errors.Join(failure, fmt.Errorf("%s: %w", p.name, e))
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("%s did not exit after stop", p.name)
		}
	}
	for _, p := range processes {
		if p.exit.Recording != "" || p.exit.Shutdown != "" {
			failure = errors.Join(failure, fmt.Errorf("%s shutdown %q recording %q", p.name, p.exit.Shutdown, p.exit.Recording))
		}
	}
	return failure
}

// link connects ns as host i behind router interface r<i>. ip receives ns as
// fd 3 for the veth peer; the host side gets one egress netem delay.
func (h *witnessHost) link(i int, ns *os.File, latency time.Duration) error {
	router, peer := fmt.Sprintf("r%d", i), fmt.Sprintf("c%d", i)
	cmd := exec.Command(h.tools["ip"], "link", "add", router, "type", "veth", "peer", "name", peer, "netns", "/proc/self/fd/3")
	cmd.ExtraFiles = []*os.File{ns}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("veth %d: %w: %s", i, err, out)
	}
	if _, err := h.run(nil, fmt.Sprintf("addr add 10.77.%d.1/24 dev %s\nlink set %s up\n", i, router, router), "ip", "-batch", "-"); err != nil {
		return err
	}
	if _, err := h.run(ns, fmt.Sprintf("link set lo up\naddr add %s/24 dev %s\nlink set %s up\nroute add default via 10.77.%d.1\n", witnessAddress(i), peer, peer, i), "ip", "-batch", "-"); err != nil {
		return err
	}
	_, err := h.run(ns, "", "tc", "qdisc", "add", "dev", peer, "root", "netem", "delay", fmt.Sprintf("%dus", latency.Microseconds()))
	return err
}

func (h *witnessHost) selftest(t *testing.T, out string) {
	cell := experiment.WitnessCell{LatencyNS: int64(time.Millisecond), Index: 1, Impaired: true, Mode: "on"}
	for _, c := range []struct{ inject, want string }{
		{"startup", "n005 exited early"},
		{"record-cap", "recording failed"},
		{"cancel", "canceled"},
	} {
		dir := filepath.Join(out, "selftest-"+c.inject)
		err := h.attempt(dir, cell, c.inject)
		var st experiment.WitnessStatus
		b, readErr := os.ReadFile(filepath.Join(dir, "status.json"))
		if readErr == nil {
			readErr = json.Unmarshal(b, &st)
		}
		if err == nil || !strings.Contains(err.Error(), c.want) || readErr != nil || st.Status != "incomplete" || len(st.Survivors) != 0 || len(witnessSurvivors()) != 0 {
			t.Errorf("%s: err=%v status=%+v read=%v", c.inject, err, st.Status, readErr)
			continue
		}
		if _, err := experiment.ReduceWitness(dir); err == nil {
			t.Errorf("%s: incomplete attempt reduced", c.inject)
		}
		t.Logf("%s: torn down cleanly: %s", c.inject, st.Reason)
	}
}

// cells runs the frozen ordinary matrix, or the orders listed in
// LIGHTPOOL_WITNESS_CELLS, after a passing socket control in the same output.
func (h *witnessHost) cells(t *testing.T, out string) {
	var controls struct{ Pass bool }
	b, err := os.ReadFile(filepath.Join(out, "controls", "controls.json"))
	if err == nil {
		err = json.Unmarshal(b, &controls)
	}
	if err != nil || !controls.Pass {
		t.Fatal("socket controls missing or failed; launch refused:", err)
	}
	want := os.Getenv("LIGHTPOOL_WITNESS_CELLS")
	for _, cell := range experiment.WitnessCells() {
		if want != "all" && !slices.Contains(strings.Split(want, ","), strconv.Itoa(cell.Order)) {
			continue
		}
		started := time.Now()
		err := h.attempt(filepath.Join(out, cell.ID()), cell, "")
		t.Logf("%s finished in %s: %v", cell.ID(), time.Since(started).Round(time.Second), err)
		if survivors := witnessSurvivors(); len(survivors) > 0 {
			t.Fatal("unsafe to continue: surviving processes", survivors)
		}
		if err != nil {
			t.Error(cell.ID(), err)
		}
	}
}
