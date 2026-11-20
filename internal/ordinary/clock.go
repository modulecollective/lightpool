package ordinary

import (
	"errors"
	"fmt"
	"math"
)

// Exchange is one coordinator pipe round trip bracketing a child's n.now()
// sample. SendNS and ReceiveNS are coordinator time; ChildNS is the child's
// journal epoch. PID guards against a replaced process answering the pipe.
type Exchange struct {
	PID       int    `json:"pid"`
	Phase     string `json:"phase"`
	SendNS    int64  `json:"sendNS,string"`
	ChildNS   int64  `json:"childNS,string"`
	ReceiveNS int64  `json:"receiveNS,string"`
}

// Offset bounds coordinator = child + offset. It is the intersection of every
// exchange's [send-child, receive-child]; no exchange is preferred or fitted.
// FromNS..ThroughNS is the child-time span the exchanges actually sampled.
type Offset struct {
	LoNS      int64 `json:"loNS,string"`
	HiNS      int64 `json:"hiNS,string"`
	FromNS    int64 `json:"fromNS,string"`
	ThroughNS int64 `json:"throughNS,string"`
}

// Interval is a closed coordinator-time interval.
type Interval struct {
	LoNS int64 `json:"loNS,string"`
	HiNS int64 `json:"hiNS,string"`
}

// Unanchored is the interval of a step outside the sampled span: it is never
// ordered against anything, so no order is extrapolated past the samples.
var Unanchored = Interval{math.MinInt64, math.MaxInt64}

// Map widens a closed child-time interval by the offset uncertainty.
func (o Offset) Map(startNS, endNS int64) Interval {
	if startNS < o.FromNS || endNS > o.ThroughNS {
		return Unanchored
	}
	return Interval{startNS + o.LoNS, endNS + o.HiNS}
}

// Before is proven order only: equal endpoints and any overlap are ambiguous.
func (a Interval) Before(b Interval) bool { return a.HiNS < b.LoNS }

// Bound intersects a process's exchanges. At least two are required so the
// span has a start and end anchor. Any reversed, out-of-order, foreign or
// empty bound fails.
func Bound(pid int, exchanges []Exchange) (Offset, error) {
	if len(exchanges) < 2 {
		return Offset{}, errors.New("fewer than two clock exchanges")
	}
	o := Offset{LoNS: math.MinInt64, HiNS: math.MaxInt64, FromNS: exchanges[0].ChildNS, ThroughNS: exchanges[len(exchanges)-1].ChildNS}
	for i, e := range exchanges {
		if e.PID != pid {
			return Offset{}, fmt.Errorf("clock exchange %d from process %d, want %d", i, e.PID, pid)
		}
		if e.ReceiveNS < e.SendNS {
			return Offset{}, fmt.Errorf("clock exchange %d reversed", i)
		}
		if i > 0 && (e.ChildNS < exchanges[i-1].ChildNS || e.SendNS < exchanges[i-1].ReceiveNS) {
			return Offset{}, fmt.Errorf("clock exchange %d out of order", i)
		}
		o.LoNS = max(o.LoNS, e.SendNS-e.ChildNS)
		o.HiNS = min(o.HiNS, e.ReceiveNS-e.ChildNS)
	}
	if o.LoNS > o.HiNS {
		return Offset{}, errors.New("empty clock offset intersection")
	}
	return o, nil
}
