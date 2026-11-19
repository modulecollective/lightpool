package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

func arrayBody(value string, n int) string { return "[" + strings.Repeat(value+",", n-1) + value + "]" }

func TestJSONResourceBounds(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"batch at limit", arrayBody(`{"ack":{"sequence":7}}`, MaxBatch), true},
		{"batch over limit", arrayBody(`{"ack":{"sequence":7}}`, MaxBatch+1), false},
		{"members at limit", `{"members":` + arrayBody(`{"x":[{},[],1,"[,}]"]}`, MaxNodes) + `}`, true},
		{"members over limit", `{"members":` + arrayBody(`{}`, MaxNodes+1) + `}`, false},
		{"scalar members at limit", `{"members":` + arrayBody(`null`, MaxNodes) + `}`, true},
		{"scalar members over limit", `{"members":` + arrayBody(`null`, MaxNodes+1) + `}`, false},
		{"depth at limit", strings.Repeat(`[`, 16) + strings.Repeat(`]`, 16), true},
		{"depth over limit", strings.Repeat(`[`, 17) + strings.Repeat(`]`, 17), false},
		{"object depth at limit", strings.Repeat(`{"x":`, 16) + `0` + strings.Repeat(`}`, 16), true},
		{"object depth over limit", strings.Repeat(`{"x":`, 17) + `0` + strings.Repeat(`}`, 17), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := boundedJSON([]byte(tc.body)); got != tc.want {
				t.Fatalf("boundedJSON=%v want %v", got, tc.want)
			}
		})
	}
	// These are valid typed messages: later validation cannot hide a broken guard.
	member := `{"name":"n","address":"127.0.0.1:7946","incarnation":1,"state":0}`
	for _, n := range []int{MaxNodes, MaxNodes + 1} {
		b := []byte(`[{"exchange":{"members":` + arrayBody(member, n) + `}}]`)
		if got := boundedJSON(b); got != (n == MaxNodes) {
			t.Fatalf("exchange count %d preflight=%v", n, got)
		}
		if _, err := DecodeBody(b); (err == nil) != (n == MaxNodes) {
			t.Fatalf("exchange count %d: %v", n, err)
		}
	}
	for _, n := range []int{1, MaxBatch, MaxNodes} {
		b := []byte(`{"members":` + arrayBody(`{"name":"string","value":[1,true,null]}`, n) + `}`)
		if !boundedJSON(b) {
			t.Fatal("unexpected rejection")
		}
		if a := testing.AllocsPerRun(10, func() {
			if !boundedJSON(b) {
				panic("unexpected rejection")
			}
		}); a != 0 {
			t.Fatalf("n=%d: preflight allocated %g times", n, a)
		}
	}
}

func compareDecode(t *testing.T, b []byte) {
	t.Helper()
	got, err := DecodeBody(b)
	want, refErr := referenceDecodeBody(b)
	if (err == nil) != (refErr == nil) || (err == nil && !reflect.DeepEqual(got, want)) {
		t.Fatalf("decode differs for %.200q: new=%+v/%v reference=%+v/%v", b, got, err, want, refErr)
	}
}

func differentialSeeds() [][]byte {
	seeds := [][]byte{}
	for _, s := range []string{
		`[{"ack":{"sequence":7}}]`, `[{"ack":{"sequence":1,"sequence":2}}]`,
		`[{"ack":{"sequence":1},"ack":{"sequence":2}}]`,
		`[{"ack":{"sequence":18446744073709551615}}]`, `[{"ack":{"sequence":18446744073709551616}}]`,
		`[{"ack":{"sequence":-1}}]`, `[{"ack":{"sequence":1.0}}]`, `[{"ack":{"sequence":1e0}}]`,
		`[{"ack":{"sequence":null}}]`, `[{"ack":{"unknown":0}}]`, `[{"ack":{},"extra":1}]`,
		`[{"alive":{"name":"a,[]{}\\\"","address":"127.0.0.1:1","state":0}}]`,
		`[{"alive":{"name":"\u0061\ud83d\ude00","address":"127.0.0.1:1"}}]`,
		`[{"alive":{"name":"\ud800","address":"127.0.0.1:1"}}]`, `"\udfff"`,
		`[]`, `{}`, `null`, `[[],{},true,false,null,123,"[,}]"]`,
		`[{"ack":{}}] {}`, `[`, `"abc\`, `"abc`, `[1,]`, `[}`, `{]`, `[[[`, `1 2`, ``,
		arrayBody(`{"ack":{"sequence":7}}`, MaxBatch), arrayBody(`{"ack":{"sequence":7}}`, MaxBatch+1),
		strings.Repeat(`[`, 16) + strings.Repeat(`]`, 16), strings.Repeat(`[`, 17) + strings.Repeat(`]`, 17),
	} {
		seeds = append(seeds, []byte(s))
	}
	seeds = append(seeds, []byte{0xff, '[', ']'})
	for n := range 8 {
		seeds = append(seeds, []byte(`[{"alive":{"name":"x`+strings.Repeat(`\`, n)+`","address":"127.0.0.1:1"}}]`))
	}
	member := `{"name":"n","address":"127.0.0.1:7946","incarnation":1,"state":0}`
	for _, n := range []int{MaxNodes, MaxNodes + 1} {
		seeds = append(seeds, []byte(`[{"exchange":{"members":`+arrayBody(member, n)+`}}]`))
	}
	return seeds
}

func TestDecodeDifferential(t *testing.T) {
	for _, b := range differentialSeeds() {
		compareDecode(t, b)
	}
	// Pin accepted duplicate-key merging/last-value behavior explicitly.
	for _, s := range []string{`[{"ack":{"sequence":1,"sequence":2}}]`, `[{"ack":{"sequence":1},"ack":{"sequence":2}}]`} {
		m, err := DecodeBody([]byte(s))
		if err != nil || len(m) != 1 || m[0].Ack.Sequence != 2 {
			t.Fatalf("duplicate behavior changed: %+v %v", m, err)
		}
	}
	rng := rand.New(rand.NewPCG(11, 17))
	for range 1000 {
		name := fmt.Sprintf("n%d [,] \\ \" 😀", rng.Uint64())
		b, err := json.Marshal([]any{map[string]any{"alive": map[string]any{"name": name, "address": "127.0.0.1:7946", "incarnation": rng.Uint64(), "state": 0}}})
		if err != nil {
			t.Fatal(err)
		}
		compareDecode(t, b)
		// Deterministic truncation and byte mutation mix syntax and typed failures.
		compareDecode(t, b[:rng.IntN(len(b))])
		mutated := bytes.Clone(b)
		mutated[rng.IntN(len(b))] = byte(rng.UintN(256))
		compareDecode(t, mutated)
	}
}

func FuzzDecodeDifferential(f *testing.F) {
	for _, b := range differentialSeeds() {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { compareDecode(t, b) })
}
