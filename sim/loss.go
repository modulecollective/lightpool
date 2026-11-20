package sim

import (
	"crypto/sha256"
	"encoding/binary"
)

// LossDrawVersion identifies a send-time exogenous tape, separate from protocol,
// schedule and window-jitter randomness. Matching directed-link send ordinals
// share draws across variants; different traffic need not share realized losses.
const LossDrawVersion = "sha256-link-ordinal-ppm/v1"

// Hash UTF-8 version bytes, LE64 seed, LE64 byte lengths + endpoint bytes,
// LE64 ordinal (one-based), then LE64 rejection counter (starting at zero).
// Rejection sampling avoids modulo bias. Restarts never reset link ordinals.
func lossDraw(seed uint64, from, to string, ordinal uint64) int {
	b := append([]byte(LossDrawVersion), binary.LittleEndian.AppendUint64(nil, seed)...)
	for _, name := range []string{from, to} {
		b = binary.LittleEndian.AppendUint64(b, uint64(len(name)))
		b = append(b, name...)
	}
	b = binary.LittleEndian.AppendUint64(b, ordinal)
	for retry := uint64(0); ; retry++ {
		sum := sha256.Sum256(binary.LittleEndian.AppendUint64(b, retry))
		n := binary.LittleEndian.Uint64(sum[:8])
		// 2^64 mod 1,000,000 = 551,616.
		if n >= 551_616 {
			return int(n % 1_000_000)
		}
	}
}
