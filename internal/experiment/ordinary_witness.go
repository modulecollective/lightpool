package experiment

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/internal/ordinary"
	"github.com/modulecollective/lightpool/wire"
)

// The ordinary witness study: 32 real processes in a private namespace tree,
// run with all node journals on or all off. These types are the evidence the
// Linux harness writes and the offline reducer reads.
const (
	WitnessVersion       = "lightpool-ordinary-witness/v1"
	WitnessReportVersion = "lightpool-ordinary-witness-report/v1"
	WitnessNodes         = 32
)

type WitnessCell struct {
	Order     int    `json:"order"`
	LatencyNS int64  `json:"latencyNS,string"`
	Index     uint64 `json:"index,string"`
	Impaired  bool   `json:"impaired"`
	Mode      string `json:"mode"`
}

func (c WitnessCell) ID() string {
	latency := fmt.Sprintf("%dus", c.LatencyNS/1000)
	if c.LatencyNS%int64(time.Millisecond) == 0 {
		latency = fmt.Sprintf("%dms", c.LatencyNS/int64(time.Millisecond))
	}
	shape := "healthy"
	if c.Impaired {
		shape = "impaired"
	}
	return fmt.Sprintf("%02d-%s-i%d-%s-%s", c.Order, latency, c.Index, shape, c.Mode)
}

// WitnessCells is the frozen ordinary matrix: increasing latency, then index,
// healthy before impaired. Each latency/index group alternates its mode order,
// beginning off then on, and uses the same order for both of its shapes.
func WitnessCells() []WitnessCell {
	var cells []WitnessCell
	for l, latency := range []time.Duration{100 * time.Microsecond, time.Millisecond, 10 * time.Millisecond} {
		for index := uint64(1); index <= 3; index++ {
			modes := []string{"off", "on"}
			if (l*3+int(index)-1)%2 == 1 {
				modes = []string{"on", "off"}
			}
			for _, impaired := range []bool{false, true} {
				for _, mode := range modes {
					cells = append(cells, WitnessCell{Order: len(cells) + 1, LatencyNS: int64(latency), Index: index, Impaired: impaired, Mode: mode})
				}
			}
		}
	}
	return cells
}

type WitnessSchedule struct {
	JoinSpacingNS int64 `json:"joinSpacingNS,string"`
	PollNS        int64 `json:"pollNS,string"`
	ClockNS       int64 `json:"clockNS,string"`
	WarmupNS      int64 `json:"warmupNS,string"`
	FaultEndNS    int64 `json:"faultEndNS,string"`
	HorizonNS     int64 `json:"horizonNS,string"`
}

// FrozenWitnessSchedule is relative to the common launch epoch. The fault
// interval is [warm-up, 31.384 s), matching the virtual dense fixture.
func FrozenWitnessSchedule() WitnessSchedule {
	return WitnessSchedule{JoinSpacingNS: int64(10 * time.Millisecond), PollNS: int64(100 * time.Millisecond), ClockNS: int64(time.Second), WarmupNS: int64(15 * time.Second), FaultEndNS: int64(15*time.Second + 16384*time.Millisecond), HorizonNS: int64(120 * time.Second)}
}

// FrozenWitnessLimits are fixed per-node shares of the 5,000,000-record and
// 2 GiB attempt budgets; each file keeps the recorder's own footer reserve.
func FrozenWitnessLimits() ordinary.Limits {
	return ordinary.Limits{QueueRecords: 4096, QueueBytes: 8 << 20, Records: 5_000_000 / WitnessNodes, Bytes: (2 << 30) / WitnessNodes}
}

// WitnessSidecarBytes bounds every non-journal file of one attempt together.
const WitnessSidecarBytes = 64 << 20

type WitnessNode struct {
	Name     string         `json:"name"`
	Role     string         `json:"role"`
	Address  netip.AddrPort `json:"address"`
	PID      int            `json:"pid"`
	Lifetime uint64         `json:"lifetime,string"`
	Journal  string         `json:"journal"`
	Seed     string         `json:"seed"`
}

type WitnessManifest struct {
	Format       string            `json:"format"`
	Cell         WitnessCell       `json:"cell"`
	Spec         Spec              `json:"spec"`
	Schedule     WitnessSchedule   `json:"schedule"`
	Limits       ordinary.Limits   `json:"limits"`
	SidecarBytes int64             `json:"sidecarBytes,string"`
	Source       string            `json:"source"`
	Binary       string            `json:"binary"`
	Toolchain    string            `json:"toolchain"`
	Kernel       string            `json:"kernel"`
	Tools        map[string]string `json:"tools"`
	Nodes        []WitnessNode     `json:"nodes"`
}

type WitnessFault struct {
	Rule              string `json:"rule"`
	Handle            int    `json:"handle"`
	RequestedNS       int64  `json:"requestedNS,string"`
	AppliedNS         int64  `json:"appliedNS,string"`
	RemoveRequestedNS int64  `json:"removeRequestedNS,string"`
	RemovedNS         int64  `json:"removedNS,string"`
	Packets           uint64 `json:"packets,string"`
	Bytes             uint64 `json:"bytes,string"`
}

type WitnessJoin struct {
	Node        string `json:"node"`
	RequestedNS int64  `json:"requestedNS,string"`
	SentNS      int64  `json:"sentNS,string"`
	StartNS     int64  `json:"childStartNS,string"`
	EndNS       int64  `json:"childEndNS,string"`
	Error       string `json:"error"`
}

type WitnessExit struct {
	Node      string `json:"node"`
	PID       int    `json:"pid"`
	Shutdown  string `json:"shutdown"`
	Recording string `json:"recording"`
	Wait      string `json:"wait"`
}

// WitnessStatus is written last. All times are coordinator nanoseconds since
// the attempt began; Warmup and Horizon are the realized boundary intervals.
type WitnessStatus struct {
	Format        string            `json:"format"`
	Status        string            `json:"status"`
	Reason        string            `json:"reason"`
	EpochNS       int64             `json:"epochNS,string"`
	SampledWarmup bool              `json:"sampledWarmup"`
	Warmup        ordinary.Interval `json:"warmup"`
	Horizon       ordinary.Interval `json:"horizon"`
	Fault         *WitnessFault     `json:"fault"`
	Joins         []WitnessJoin     `json:"joins"`
	Exits         []WitnessExit     `json:"exits"`
	Survivors     []int             `json:"survivors"`
}

type WitnessClock struct {
	Node     string            `json:"node"`
	Exchange ordinary.Exchange `json:"exchange"`
}

// WitnessSample is one 100 ms cached-membership read. States has one byte per
// node n000..n031: a(live), s(uspect), d(ead), l(eft) or - (absent).
type WitnessSample struct {
	Node          string `json:"node"`
	StartNS       int64  `json:"childStartNS,string"`
	EndNS         int64  `json:"childEndNS,string"`
	ReceivedNS    int64  `json:"receivedNS,string"`
	States        string `json:"states"`
	Health        int    `json:"health"`
	Backlog       int    `json:"backlog"`
	SentBytes     uint64 `json:"sentBytes,string"`
	ReceivedBytes uint64 `json:"receivedBytes,string"`
	Recording     string `json:"recording,omitempty"`
}

func WitnessStates(s core.Snapshot) string {
	b := bytes.Repeat([]byte{'-'}, WitnessNodes)
	for _, m := range s.Members {
		var i int
		if _, err := fmt.Sscanf(m.Name, "n%03d", &i); err == nil && i >= 0 && i < WitnessNodes {
			b[i] = "asdl"[m.State]
		}
	}
	return string(b)
}

type WitnessDistribution struct {
	N      int    `json:"n"`
	P50    *int64 `json:"p50,string"`
	P95    *int64 `json:"p95,string"`
	P99    *int64 `json:"p99,string"`
	Max    *int64 `json:"max,string"`
	Note   string `json:"note,omitempty"`
	Bounds string `json:"bounds"`
}

func WitnessDistributionOf(values []int64, bounds string) WitnessDistribution {
	d := WitnessDistribution{N: len(values), Bounds: bounds}
	if len(values) == 0 {
		return d
	}
	slices.Sort(values)
	rank := func(p int) *int64 { v := values[(len(values)*p+99)/100-1]; return &v }
	d.P50, d.P95, d.P99, d.Max = rank(50), rank(95), rank(99), rank(100)
	if *d.P99 == *d.Max {
		d.Note = "nearest-rank p99 equals the maximum"
	}
	return d
}

type WitnessPair struct {
	Pair
	Samples          int    `json:"samples"`
	NonAlive         int    `json:"nonAliveSamples"`
	Dead             int    `json:"deadSamples"`
	Transitions      int    `json:"aliveToNonAlive"`
	FirstNonAliveNS  *int64 `json:"firstNonAliveReceivedNS,string"`
	MaxSampleGapNS   int64  `json:"maxSampleGapNS,string"`
	MissedPollPeriod int    `json:"missedPollPeriods"`
}

type WitnessReport struct {
	Format         string                         `json:"format"`
	Attempt        string                         `json:"attempt"`
	Status         string                         `json:"status"`
	Reason         string                         `json:"reason"`
	Cell           WitnessCell                    `json:"cell"`
	Files          map[string]string              `json:"fileSHA256"`
	SampledWarmup  bool                           `json:"sampledWarmup"`
	RecordedWarmup *bool                          `json:"recordedWarmup"`
	WarmupFailures []string                       `json:"recordedWarmupFailures"`
	Fault          *WitnessFault                  `json:"fault"`
	Window         ordinary.Interval              `json:"measurementWindow"`
	PairExposureNS int64                          `json:"pairExposureNS,string"`
	Pairs          []WitnessPair                  `json:"pairs"`
	SentBytes      uint64                         `json:"windowSentBytes,string"`
	Timing         map[string]WitnessDistribution `json:"recordedTiming"`
	Classification *OrdinaryClassification        `json:"classification"`
	Limits         []string                       `json:"limits"`
}

// Row is the compact table line for one attempt.
func (r *WitnessReport) Row() string {
	recorded, episodes, acc, harm, inconclusive := "-", "-", "-", "-", "-"
	if r.RecordedWarmup != nil {
		recorded = fmt.Sprint(*r.RecordedWarmup)
	}
	if c := r.Classification; c != nil {
		episodes, acc, harm, inconclusive = fmt.Sprint(len(c.Episodes)), fmt.Sprint(c.Accelerations), fmt.Sprint(c.Harmful), fmt.Sprint(c.Inconclusive)
	}
	packets := "-"
	if r.Fault != nil {
		packets = fmt.Sprint(r.Fault.Packets)
	}
	nonAlive, dead := 0, 0
	for _, p := range r.Pairs {
		nonAlive += p.NonAlive
		dead += p.Dead
	}
	return strings.Join([]string{r.Attempt, r.Cell.Mode, r.Status, fmt.Sprint(r.SampledWarmup), recorded, packets, fmt.Sprint(nonAlive), fmt.Sprint(dead), episodes, acc, harm, inconclusive, fmt.Sprint(r.SentBytes)}, "\t")
}

const WitnessHeader = "attempt\tmode\tstatus\tsampledWarmup\trecordedWarmup\tfaultPackets\tpairNonAliveSamples\tpairDeadSamples\tepisodes\taccelerations\tharmful\tinconclusive\twindowSentBytes"

func decodeFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return ordinary.Decode(b, v)
}

func decodeLines[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []T
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), 1<<20)
	for scan.Scan() {
		var v T
		if err := ordinary.Decode(scan.Bytes(), &v); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", filepath.Base(path), len(out)+1, err)
		}
		out = append(out, v)
	}
	return out, scan.Err()
}

func WitnessRole(spec Spec, name string) string {
	switch {
	case slices.Contains(spec.Anomaly, name):
		return "A"
	case name == spec.Target:
		return "T"
	case slices.ContainsFunc(spec.HealthyPairs, func(p EligiblePair) bool { return p.Observer == name }):
		return "O"
	}
	return ""
}

// ReduceWitness re-derives one attempt's report from its saved files without
// running a node. Malformed or incomplete evidence is an error; a complete
// attempt with no acceleration is a successful null.
func ReduceWitness(directory string) (*WitnessReport, error) {
	var m WitnessManifest
	var st WitnessStatus
	if err := decodeFile(filepath.Join(directory, "manifest.json"), &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if err := decodeFile(filepath.Join(directory, "status.json"), &st); err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	r := &WitnessReport{Format: WitnessReportVersion, Attempt: m.Cell.ID(), Status: st.Status, Reason: st.Reason, Cell: m.Cell, Files: map[string]string{}, SampledWarmup: st.SampledWarmup, WarmupFailures: []string{}, Fault: st.Fault, Pairs: []WitnessPair{}, Timing: map[string]WitnessDistribution{}, Limits: []string{
		"Both modes share the 100 ms cached-membership sampling and 1 Hz clock pipe workload; it is not an invisible observer.",
		"Sampled states cannot resolve confirmations or brief transitions; off-mode outcomes are sampled-warmup controls, never causal evidence.",
		"Recorded timings are local monotonic durations inside each node; kernel arrival, queue loss and unmatched packet identities stay unknown.",
		"The impaired path drops A-to-T UDP and TCP, including ACKs of reverse-initiated streams; simulated StreamFailure is immediate instead.",
	}}
	if m.Format != WitnessVersion || st.Format != WitnessVersion || !slices.Contains(WitnessCells(), m.Cell) {
		return r, errors.New("unknown witness format or cell")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return r, err
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(directory, e.Name()))
		if err != nil {
			return r, err
		}
		sum := sha256.Sum256(b)
		r.Files[e.Name()] = hex.EncodeToString(sum[:])
	}
	spec, _, err := Fixture("correlated-dense", m.Cell.Index, m.Source)
	if err != nil || HashJSON(spec) != HashJSON(m.Spec) || m.Schedule != FrozenWitnessSchedule() || m.Limits != FrozenWitnessLimits() || m.SidecarBytes != WitnessSidecarBytes {
		return r, errors.New("manifest differs from the frozen cohorts, schedule or limits")
	}
	if len(m.Nodes) != WitnessNodes {
		return r, fmt.Errorf("manifest has %d nodes", len(m.Nodes))
	}
	for i, n := range m.Nodes {
		journal := ""
		if m.Cell.Mode == "on" {
			journal = "node-" + n.Name + ".ndjson"
		}
		if n.Name != fmt.Sprintf("n%03d", i) || n.Role != WitnessRole(spec, n.Name) || n.PID <= 0 || n.Journal != journal {
			return r, fmt.Errorf("manifest node %d does not match the fixture", i)
		}
		if m.Cell.Mode == "off" && r.Files["node-"+n.Name+".ndjson"] != "" {
			return r, errors.New("off-mode attempt contains a journal")
		}
	}
	if st.Status != "complete" {
		return r, fmt.Errorf("%s: %s", st.Status, st.Reason)
	}
	if len(st.Survivors) != 0 || len(st.Exits) != WitnessNodes || st.Warmup.LoNS > st.Warmup.HiNS || !st.Warmup.Before(st.Horizon) || st.Horizon.LoNS > st.Horizon.HiNS {
		return r, errors.New("status lacks clean exits or valid boundaries")
	}
	if m.Cell.Impaired != (st.Fault != nil) || st.Fault != nil && (st.Fault.RequestedNS > st.Fault.AppliedNS || st.Fault.AppliedNS > st.Fault.RemoveRequestedNS || st.Fault.RemoveRequestedNS > st.Fault.RemovedNS) {
		return r, errors.New("fault evidence does not match the cell")
	}
	for _, e := range st.Exits {
		if e.Shutdown != "" || e.Recording != "" || e.Wait != "" {
			return r, fmt.Errorf("node %s did not exit cleanly", e.Node)
		}
	}
	r.Window = ordinary.Interval{LoNS: st.Warmup.HiNS, HiNS: st.Horizon.LoNS}
	if err := r.sampled(directory, spec); err != nil {
		return r, err
	}
	if m.Cell.Mode == "off" {
		r.Limits = append(r.Limits, "Journals off: no causal classification, recorded warm-up or internal timing exists for this attempt.")
		return r, nil
	}
	clocks, err := decodeLines[WitnessClock](filepath.Join(directory, "clock.ndjson"))
	if err != nil {
		return r, err
	}
	in := OrdinaryInput{Warmup: st.Warmup, Horizon: st.Horizon}
	for _, p := range spec.HealthyPairs {
		in.Pairs = append(in.Pairs, Pair{Observer: p.Observer, Subject: p.Subject})
	}
	for _, mn := range m.Nodes {
		data, err := os.ReadFile(filepath.Join(directory, mn.Journal))
		if err != nil {
			return r, err
		}
		n, err := LoadOrdinaryJournal(data)
		if err != nil {
			return r, fmt.Errorf("%s: %w", mn.Journal, err)
		}
		if n.Name != mn.Name || n.Address != mn.Address || n.Lifetime != mn.Lifetime {
			return r, fmt.Errorf("%s identity differs from manifest", mn.Journal)
		}
		n.Role, n.PID = mn.Role, mn.PID
		for _, c := range clocks {
			if c.Node == mn.Name {
				n.Clock = append(n.Clock, c.Exchange)
			}
		}
		if len(n.Clock) == 0 || n.Clock[0].Phase != "pre-join" || n.Clock[len(n.Clock)-1].Phase != "post-horizon" {
			return r, fmt.Errorf("%s lacks pre-join and post-horizon clock anchors", mn.Name)
		}
		in.Nodes = append(in.Nodes, *n)
	}
	if r.Classification, err = ClassifyOrdinary(in); err != nil {
		return r, err
	}
	ok := true
	timing := map[string][]int64{}
	for i := range in.Nodes {
		n := &in.Nodes[i]
		offset, _ := ordinary.Bound(n.PID, n.Clock)
		if !recordedWarm(n, offset, st.Warmup) {
			ok = false
			r.WarmupFailures = append(r.WarmupFailures, n.Name)
		}
		for _, s := range n.Steps {
			at := offset.Map(s.StartNS, s.EndNS)
			if r.Window.LoNS < at.LoNS && at.HiNS < r.Window.HiNS {
				if s.ReadNS != nil {
					timing["readToOwnerNS"] = append(timing["readToOwnerNS"], s.StartNS-*s.ReadNS)
				}
				if s.TimerDueNS != nil {
					timing["timerLatenessNS"] = append(timing["timerLatenessNS"], s.StartNS-*s.TimerDueNS)
				}
				timing["queueDepth"] = append(timing["queueDepth"], int64(s.QueueDepth))
				timing["coreNS"] = append(timing["coreNS"], s.CoreEndNS-s.CoreAtNS)
				timing["ownerNS"] = append(timing["ownerNS"], s.EndNS-s.StartNS)
				timing["recorderNS"] = append(timing["recorderNS"], s.RecorderNS)
			}
		}
	}
	r.RecordedWarmup = &ok
	for k, v := range timing {
		r.Timing[k] = WitnessDistributionOf(v, "all nodes' steps whose mapped interval lies inside the measurement window")
	}
	if !ok {
		r.Status, r.Reason = "invalid-warmup", "recorded membership not fully alive just before the warm-up boundary"
		return r, errors.New(r.Reason)
	}
	return r, nil
}

// recordedWarm replays membership through the last step provably before the
// warm-up boundary and requires every node, including self, to be alive.
func recordedWarm(n *OrdinaryNode, offset ordinary.Offset, warmup ordinary.Interval) bool {
	last := -1
	for i, s := range n.Steps {
		if offset.Map(s.StartNS, s.EndNS).Before(warmup) {
			last = i
		}
	}
	states := map[string]wire.State{}
	for _, j := range n.Baseline {
		if j.Member != nil {
			states[j.Member.Name] = j.Member.State
		}
	}
	for _, s := range n.Steps[:last+1] {
		for _, j := range s.Journal {
			switch {
			case j.Kind == "member.upsert" && j.Member != nil:
				states[j.Member.Name] = j.Member.State
			case j.Kind == "member.remove":
				delete(states, j.Subject)
			}
		}
	}
	if last < 0 || len(states) != WitnessNodes {
		return false
	}
	for _, s := range states {
		if s != wire.Alive {
			return false
		}
	}
	return true
}

func (r *WitnessReport) sampled(directory string, spec Spec) error {
	samples, err := decodeLines[WitnessSample](filepath.Join(directory, "samples.ndjson"))
	if err != nil {
		return err
	}
	byNode := map[string][]WitnessSample{}
	for _, s := range samples {
		if len(s.States) != WitnessNodes || s.Recording != "" {
			return fmt.Errorf("malformed or failed sample from %s", s.Node)
		}
		byNode[s.Node] = append(byNode[s.Node], s)
	}
	poll := FrozenWitnessSchedule().PollNS
	var start, end uint64
	for name, list := range byNode {
		var before, through *WitnessSample
		for i := range list {
			if list[i].ReceivedNS <= r.Window.LoNS {
				before = &list[i]
			}
			if list[i].ReceivedNS <= r.Window.HiNS {
				through = &list[i]
			}
		}
		if before == nil || through == nil {
			return fmt.Errorf("node %s lacks samples bracketing the window", name)
		}
		start += before.SentBytes
		end += through.SentBytes
	}
	if len(byNode) != WitnessNodes {
		return fmt.Errorf("samples from %d nodes", len(byNode))
	}
	r.SentBytes = end - start
	for _, p := range spec.HealthyPairs {
		var target int
		if _, err := fmt.Sscanf(p.Subject, "n%03d", &target); err != nil {
			return err
		}
		out := WitnessPair{Pair: Pair{Observer: p.Observer, Subject: p.Subject}}
		previous, alive := r.Window.LoNS, true
		for _, s := range byNode[p.Observer] {
			if s.ReceivedNS <= r.Window.LoNS || s.ReceivedNS > r.Window.HiNS {
				continue
			}
			out.Samples++
			gap := s.ReceivedNS - previous
			out.MaxSampleGapNS = max(out.MaxSampleGapNS, gap)
			out.MissedPollPeriod += int(max(0, gap/poll-1))
			previous = s.ReceivedNS
			state := s.States[target]
			if state != 'a' {
				out.NonAlive++
				if out.FirstNonAliveNS == nil {
					at := s.ReceivedNS
					out.FirstNonAliveNS = &at
				}
				if alive {
					out.Transitions++
				}
			}
			if state == 'd' {
				out.Dead++
			}
			alive = state == 'a'
		}
		r.Pairs = append(r.Pairs, out)
		r.PairExposureNS += r.Window.HiNS - r.Window.LoNS
	}
	return nil
}

// WriteWitnessReport is deterministic: map keys are sorted by encoding/json.
func WriteWitnessReport(w io.Writer, r *WitnessReport) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}
