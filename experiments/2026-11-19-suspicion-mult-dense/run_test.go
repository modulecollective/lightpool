package experiment_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPatchPathGuard(t *testing.T) {
	recipe, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	const contents = "first\nrename from sim/not-a-header.go\ncopy from sim/not-a-header.go\nlast\n"
	operation := func(kind, source, destination string) string {
		return fmt.Sprintf("diff --git a/%s b/%s\nsimilarity index 100%%\n%s from %s\n%s to %s\n",
			source, destination, kind, source, kind, destination)
	}
	modify := func(path string) string {
		return fmt.Sprintf("diff --git a/%s b/%s\n--- a/%s\n+++ b/%s\n@@ -1,4 +1,4 @@\n first\n rename from sim/not-a-header.go\n copy from sim/not-a-header.go\n-last\n+changed\n",
			path, path, path, path)
	}
	tests := []struct {
		name       string
		patch      string
		diagnostic string
	}{
		{"rename_in", operation("rename", "sim/x.go", "core/moved.go"), "has sources outside core/ and wire/: sim/x.go"},
		{"copy_in", operation("copy", "sim/x.go", "wire/copied.go"), "has sources outside core/ and wire/: sim/x.go"},
		{"modify_outside", modify("sim/x.go"), "touches files outside core/ and wire/: 1\t1\tsim/x.go"},
		{"rename_out", operation("rename", "core/x.go", "sim/moved.go"), "touches files outside core/ and wire/: 0\t0\tsim/moved.go"},
		{"modify_core_with_header_context", modify("core/x.go"), ""},
		{"rename_core_to_wire", operation("rename", "core/x.go", "wire/moved.go"), ""},
		{"copy_wire_to_core", operation("copy", "wire/x.go", "core/copied.go"), ""},
		{"rename_spaced_source", operation("rename", "core/with space.go", "wire/moved.go"), ""},
		{"copy_spaced_source", operation("copy", "core/with space.go", "wire/copied.go"), ""},
		{"quoted_outside_source", "diff --git \"a/sim/tab\\tname.go\" b/core/moved.go\nsimilarity index 100%\nrename from \"sim/tab\\tname.go\"\nrename to core/moved.go\n", "has sources outside core/ and wire/: \"sim/tab\\tname.go\""},
		{"quoted_inside_source", "diff --git \"a/core/tab\\tname.go\" b/wire/moved.go\nsimilarity index 100%\nrename from \"core/tab\\tname.go\"\nrename to wire/moved.go\n", "has sources outside core/ and wire/: \"core/tab\\tname.go\""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := t.TempDir()
			write := func(path, data string, mode os.FileMode) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(data), mode); err != nil {
					t.Fatal(err)
				}
			}
			git := func(args ...string) string {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = repo
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v\n%s", args, err, output)
				}
				return strings.TrimSpace(string(output))
			}
			git("init", "--quiet")
			for _, path := range []string{"sim/x.go", "core/x.go", "wire/x.go", "core/with space.go", "sim/tab\tname.go", "core/tab\tname.go"} {
				write(filepath.Join(repo, path), contents, 0o644)
			}
			git("add", ".")
			git("-c", "user.name=Guard Test", "-c", "user.email=guard@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "-m", "fixture")
			record := filepath.Join(repo, "experiments", "fixture")
			write(filepath.Join(record, "base"), git("rev-parse", "HEAD")+"\n", 0o644)
			write(filepath.Join(record, "toolchain"), "go1.26.3 linux/amd64\n", 0o644)
			write(filepath.Join(record, "run.sh"), string(recipe), 0o755)
			patch := filepath.Join(record, "candidate.patch")
			write(patch, tt.patch, 0o644)
			// Syntax failures must not masquerade as path-guard refusals.
			git("apply", "--check", patch)
			stub := filepath.Join(repo, "stub")
			write(filepath.Join(stub, "go"), `#!/usr/bin/env bash
case "$*" in
  'env GOOS') echo linux ;;
  'env GOARCH') echo amd64 ;;
  'build '*) touch "$GUARD_BUILD_MARKER"; exit 73 ;;
  *) echo "unexpected go invocation: $*" >&2; exit 74 ;;
esac
`, 0o755)
			marker := filepath.Join(repo, "build-reached")
			out := filepath.Join(repo, "output")
			cmd := exec.Command("bash", filepath.Join(record, "run.sh"), out)
			cmd.Dir = repo
			cmd.Env = append(os.Environ(), "PATH="+stub+string(os.PathListSeparator)+os.Getenv("PATH"), "GUARD_BUILD_MARKER="+marker)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("expected recipe exit status, got %v\n%s", err, output)
			}
			_, markerErr := os.Stat(marker)
			if tt.diagnostic == "" {
				if exitErr.ExitCode() != 73 || markerErr != nil || strings.Contains(string(output), "run.sh:") {
					t.Fatalf("allowed patch did not reach build sentinel: exit=%d marker=%v\n%s", exitErr.ExitCode(), markerErr, output)
				}
				return
			}
			if markerErr == nil {
				t.Fatalf("outside patch reached build marker instead of source/destination refusal: exit=%d\n%s", exitErr.ExitCode(), output)
			}
			if !os.IsNotExist(markerErr) {
				t.Fatal(markerErr)
			}
			if exitErr.ExitCode() != 1 || !strings.Contains(string(output), "run.sh: candidate.patch "+tt.diagnostic) {
				t.Fatalf("missing path refusal: exit=%d\n%s", exitErr.ExitCode(), output)
			}
			if _, err := os.Stat(filepath.Join(out, "src")); !os.IsNotExist(err) {
				t.Fatalf("refused patch created source tree: %v", err)
			}
		})
	}
}
