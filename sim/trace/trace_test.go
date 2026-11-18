package trace

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestSharedFixtureAndRecovery(t *testing.T) {
	b, err := os.ReadFile("testdata/exact.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(b), "\n")
	cases := []struct {
		name, input    string
		complete, fail bool
	}{
		{"complete", string(b), true, false},
		{"prefix", lines[0] + lines[1], false, false},
		{"partial", lines[0] + lines[1] + `{"type":"end"`, false, false},
		{"malformed", lines[0] + "{broken}\n", false, true},
		{"version", strings.Replace(string(b), `"version":1`, `"version":2`, 1), false, true},
		{"gap", strings.Replace(string(b), `"seq":"1"`, `"seq":"2"`, 1), false, true},
		{"after end", string(b) + lines[1], false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			complete, err := Read(strings.NewReader(c.input), nil)
			if (err != nil) != c.fail || complete != c.complete {
				t.Fatal(complete, err)
			}
			if err != nil && !strings.Contains(err.Error(), "line ") {
				t.Fatal("missing line", err)
			}
		})
	}
	var h Header
	if err = json.Unmarshal([]byte(lines[0]), &h); err != nil {
		t.Fatal(err)
	}
	normalized, err := Normalize(struct{ Value uint64 }{18446744073709551615})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(normalized)
	if !bytes.Contains(raw, []byte(`"18446744073709551615"`)) {
		t.Fatal(string(raw))
	}
}
