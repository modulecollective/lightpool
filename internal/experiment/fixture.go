// Package experiment records a fixed baseline pilot and reduces saved evidence.
// It is deliberately not a general experiment or candidate-search framework.
package experiment

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/netip"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/modulecollective/lightpool/core"
	"github.com/modulecollective/lightpool/sim"
	"github.com/modulecollective/lightpool/sim/report"
	"github.com/modulecollective/lightpool/wire"
)

const Version = "lightpool-pilot/v1"
const SeedVersion = "sha256-replicate-namespace/v1"
const TraceCap int64 = 2 << 30
const EventBudget = 5_000_000

type EligiblePair struct {
	Observer string `json:"observer"`
	Subject  string `json:"subject"`
}

type Spec struct {
	Format         string         `json:"format"`
	Case           string         `json:"case"`
	Replicate      uint64         `json:"replicate,string"`
	Code           string         `json:"code"`
	Toolchain      string         `json:"toolchain"`
	Platform       string         `json:"platform"`
	SeedDerivation string         `json:"seedDerivation"`
	LossDraw       string         `json:"lossDraw"`
	LossBoundary   string         `json:"lossBoundary"`
	Pairing        string         `json:"pairing"`
	WarmupNS       int64          `json:"warmupNS,string"`
	HorizonNS      int64          `json:"horizonNS,string"`
	TraceCap       int64          `json:"traceCap,string"`
	Anomaly        []string       `json:"anomaly"`
	Target         string         `json:"target,omitempty"`
	HealthyPairs   []EligiblePair `json:"healthyPairs"`
}

// DeriveSeed hashes version bytes, LE64 replicate, then UTF-8 namespace.
func DeriveSeed(replicate uint64, namespace string) uint64 {
	b := binary.LittleEndian.AppendUint64([]byte(SeedVersion), replicate)
	sum := sha256.Sum256(append(b, namespace...))
	return binary.LittleEndian.Uint64(sum[:8])
}

func Fixture(name string, replicate uint64, code string) (Spec, sim.Scenario, error) {
	n, count, shape := 32, 4, name
	deliveryN, loss, exchange := deliveryCase(name)
	switch name {
	case "healthy", "crash", "threshold", "interval", "loss", "correlated":
	case "partial-partition", "partial-partition-healthy":
		shape = "correlated"
	case "correlated-dense", "correlated-dense-healthy", "correlated-dense-latency-100us", "correlated-dense-latency-10ms", "correlated-dense-service-100us", "correlated-dense-service-1ms", "correlated-dense-buffer-64k", "correlated-dense-buffer-16m":
		count, shape = 24, "correlated"
	case "correlated-dense-64-aged", "correlated-dense-64-aged-healthy":
		n, count, shape = 64, 48, "correlated"
	case "small-double-crash":
		n, count, shape = 4, 2, "crash"
	case "threshold-128":
		n, count, shape = 128, 16, "threshold"
	case "ordinary-healthy":
		n, count, shape = 8, 1, "healthy"
	case "ordinary-pause":
		n, count, shape = 8, 1, "threshold"
	case "ordinary-crash":
		n, count, shape = 8, 1, "crash"
	default:
		if deliveryN == 0 {
			return Spec{}, sim.Scenario{}, fmt.Errorf("unknown fixed case %q", name)
		}
		n, shape = deliveryN, "delivery"
	}
	if replicate == 0 || code == "" {
		return Spec{}, sim.Scenario{}, fmt.Errorf("positive replicate and source identity required")
	}
	spec := Spec{Format: Version, Case: name, Replicate: replicate, Code: code, Toolchain: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, SeedDerivation: SeedVersion, LossDraw: sim.LossDrawVersion, LossBoundary: "after successful encoded in-budget datagram send, before delivery; not receiver loss", Pairing: "same fault tape at matching directed-link send ordinals; different traffic can realize different losses", WarmupNS: int64(15 * time.Second), HorizonNS: int64(120 * time.Second), TraceCap: TraceCap, Anomaly: []string{}, HealthyPairs: []EligiblePair{}}
	if shape == "delivery" {
		spec.WarmupNS, spec.HorizonNS = int64(50*time.Second), int64(130*time.Second)
	}
	if n == 64 {
		// Old enough that every node's first periodic exchange precedes the fault.
		spec.WarmupNS, spec.HorizonNS = int64(95*time.Second), int64(200*time.Second)
	}
	s := sim.Scenario{Code: code, Protocol: core.DefaultConfig(), Seeds: sim.Seeds{Protocol: DeriveSeed(replicate, "protocol"), Fault: DeriveSeed(replicate, "fault"), Schedule: DeriveSeed(replicate, "schedule")}, Horizon: core.Instant(spec.HorizonNS), EventBudget: EventBudget, DatagramCapacity: 1 << 20, StreamCapacity: 1 << 20, Latency: time.Millisecond}
	for i := 0; i < n; i++ {
		node := sim.Node{Identity: wire.Identity{Name: fmt.Sprintf("n%03d", i), Address: netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(i + 1)}), 7946)}}
		s.Nodes = append(s.Nodes, node)
		if i > 0 {
			s.Actions = append(s.Actions, sim.Action{At: core.Instant(time.Duration(i) * 10 * time.Millisecond), Kind: "join", Node: node.Identity.Name, Peer: "n000"})
		}
	}
	if shape == "delivery" {
		if strings.HasSuffix(name, "-latency-10ms") {
			s.Latency = 10 * time.Millisecond
		}
		if !exchange {
			s.Protocol.PushPullInterval = 0
		}
		for i := range 20 {
			s.Actions = append(s.Actions, sim.Action{At: core.Instant(time.Duration(50+4*i) * time.Second), Kind: "update", Node: s.Nodes[i].Identity.Name, Data: []byte(fmt.Sprintf("upd-%04d", i))})
		}
		for _, a := range s.Nodes {
			for _, b := range s.Nodes {
				if a.Identity.Name != b.Identity.Name {
					s.Faults = append(s.Faults, sim.Fault{From: a.Identity.Name, To: b.Identity.Name, End: s.Horizon + 1, LossPPM: loss})
				}
			}
		}
		return spec, s, sim.Validate(s)
	}
	// Rank by a per-identity hash of the fault seed, avoiding shared RNG state and
	// architecture-dependent shuffles. Only crash selection excludes bootstrap.
	ranked := slices.Clone(s.Nodes)
	slices.SortFunc(ranked, func(a, b sim.Node) int {
		rank := func(n sim.Node) [32]byte {
			b := binary.LittleEndian.AppendUint64(nil, s.Seeds.Fault)
			return sha256.Sum256(append(b, n.Identity.Name...))
		}
		ra, rb := rank(a), rank(b)
		for i := range ra {
			if ra[i] < rb[i] {
				return -1
			}
			if ra[i] > rb[i] {
				return 1
			}
		}
		return 0
	})
	switch name {
	case "correlated-dense-latency-100us":
		s.Latency = 100 * time.Microsecond
	case "correlated-dense-latency-10ms":
		s.Latency = 10 * time.Millisecond
	case "correlated-dense-service-100us", "correlated-dense-service-1ms":
		service := 100 * time.Microsecond
		if name == "correlated-dense-service-1ms" {
			service = time.Millisecond
		}
		for i := range s.Nodes {
			s.Nodes[i].Service = service
		}
	case "correlated-dense-buffer-64k":
		s.DatagramCapacity, s.StreamCapacity = 64<<10, 64<<10
	case "correlated-dense-buffer-16m":
		s.DatagramCapacity, s.StreamCapacity = 16<<20, 16<<20
	}
	start, end := core.Instant(spec.WarmupNS), s.Horizon
	switch shape {
	case "crash":
		crashed, want := 0, 1
		if name == "small-double-crash" {
			want = count
		}
		for _, node := range ranked {
			if node.Identity.Name != "n000" && crashed < want {
				if crashed == 0 {
					spec.Target = node.Identity.Name
				}
				s.Actions = append(s.Actions, sim.Action{At: start, Kind: "crash", Node: node.Identity.Name})
				crashed++
			}
		}
	case "threshold", "interval", "correlated":
		selected := ranked[:count]
		if shape == "correlated" && count > 4 {
			// Keep T at its original fault rank; grow A around it.
			selected = append(slices.Clone(ranked[:4]), ranked[5:count+1]...)
		}
		for _, node := range selected {
			spec.Anomaly = append(spec.Anomaly, node.Identity.Name)
		}
		slices.Sort(spec.Anomaly)
		if shape == "correlated" {
			spec.Target = ranked[4].Identity.Name
			for _, a := range spec.Anomaly {
				if name == "correlated-dense-healthy" || name == "partial-partition-healthy" || name == "correlated-dense-64-aged-healthy" {
					continue
				}
				s.Faults = append(s.Faults, sim.Fault{From: a, To: spec.Target, Start: start, End: start.Add(16384 * time.Millisecond), Drop: true, StreamFailure: true})
				if name == "partial-partition" {
					s.Faults = append(s.Faults, sim.Fault{From: spec.Target, To: a, Start: start, End: start.Add(16384 * time.Millisecond), Drop: true, StreamFailure: true})
				}
			}
			for _, o := range s.Nodes {
				if o.Identity.Name != spec.Target && !slices.Contains(spec.Anomaly, o.Identity.Name) {
					spec.HealthyPairs = append(spec.HealthyPairs, EligiblePair{o.Identity.Name, spec.Target})
				}
			}
		} else {
			duration, step := 8192*time.Millisecond, time.Duration(spec.HorizonNS)
			if shape == "interval" {
				duration, step = 2048*time.Millisecond, 3072*time.Millisecond
			}
			for at := start; at < end; at = at.Add(step) {
				for _, node := range spec.Anomaly {
					s.Actions = append(s.Actions, sim.Action{At: at, Kind: "pause", Node: node, Duration: duration})
				}
			}
		}
	case "loss":
		for _, a := range s.Nodes {
			for _, b := range s.Nodes {
				if a.Identity.Name != b.Identity.Name {
					s.Faults = append(s.Faults, sim.Fault{From: a.Identity.Name, To: b.Identity.Name, Start: start, End: end, LossPPM: 100000})
				}
			}
		}
	}
	return spec, s, sim.Validate(s)
}

// OverlayProtocol decodes the JSON object at path onto scenario.Protocol, so
// fields it omits keep their fixed-case values. Unknown fields are refused. The
// overlay is harness input: it lands in the saved scenario.json, which -reduce
// reads unchanged and -admission refuses as a differing fixture.
func OverlayProtocol(scenario *sim.Scenario, path string) error {
	if err := readJSON(path, &scenario.Protocol); err != nil {
		return fmt.Errorf("protocol overlay: %w", err)
	}
	return sim.Validate(*scenario)
}

// Use the report's lifetime, recovery and crash-cohort rules unchanged.
type Reference = report.Reference
type Pair = report.Pair

// Exact finite names; malformed near-matches never select a delivery workload.
func deliveryCase(name string) (nodes, loss int, exchange bool) {
	for _, n := range []int{32, 128} {
		for _, percent := range []int{0, 10, 30} {
			for _, mode := range []string{"on", "off"} {
				base := fmt.Sprintf("delivery-%d-loss-%d-exchange-%s", n, percent, mode)
				if name == base || (n == 32 && name == base+"-latency-10ms") {
					return n, percent * 10000, mode == "on"
				}
			}
		}
	}
	return 0, 0, false
}
