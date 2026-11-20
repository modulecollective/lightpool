package ordinary

import (
	"errors"
	"fmt"
)

// Exchange is one coordinator pipe round trip bracketing a child's n.now()
// sample. SendNS and ReceiveNS are coordinator time; ChildNS is the child's
// journal epoch. PID guards against a replaced process answering the pipe.
type Exchange struct {
	PID       int   `json:"pid"`
	SendNS    int64 `json:"sendNS,string"`
	ChildNS   int64 `json:"childNS,string"`
	ReceiveNS int64 `json:"receiveNS,string"`
}

// Offset bounds coordinator = child + offset. It is the intersection of every
// exchange's [send-child, receive-child]; no exchange is preferred or fitted.
type Offset struct {
	LoNS int64 `json:"loNS,string"`
	HiNS int64 `json:"hiNS,string"`
}

// Interval is a closed coordinator-time interval.
type Interval struct {
	LoNS int64 `json:"loNS,string"`
	HiNS int64 `json:"hiNS,string"`
}

// Map widens a closed child-time interval by the offset uncertainty.
func (o Offset) Map(startNS, endNS int64) Interval {
	return Interval{startNS + o.LoNS, endNS + o.HiNS}
}

// Before is proven order only: equal endpoints and any overlap are ambiguous.
func (a Interval) Before(b Interval) bool { return a.HiNS < b.LoNS }

// Bound intersects a process's exchanges. Exchanges must bracket the whole
// journal (firstNS..lastNS) so no recorded step is extrapolated outside a
// sampled anchor. Any reversed, out-of-order, foreign or empty bound fails.
func Bound(pid int, exchanges []Exchange, firstNS, lastNS int64) (Offset, error) {
	if len(exchanges) == 0 {
		return Offset{}, errors.New("no clock exchanges")
	}
	if firstNS > lastNS {
		return Offset{}, errors.New("reversed journal span")
	}
	o := Offset{LoNS: -1 << 63, HiNS: 1<<63 - 1}
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
	if exchanges[0].ChildNS > firstNS || exchanges[len(exchanges)-1].ChildNS < lastNS {
		return Offset{}, errors.New("clock exchanges do not bracket the journal")
	}
	return o, nil
}
