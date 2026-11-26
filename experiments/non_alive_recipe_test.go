package experiments_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the checked-in recipe with real Git archives and packaging, replacing
// only the expensive Go build/reducer and the particular failing I/O boundary.
func TestNonAliveRecipe(t *testing.T) {
	for _, fault := range []string{"success", "low", "partial", "reducer", "bytes", "set", "symlink", "status", "compatibility", "summary", "compression", "final", "interrupt", "build", "archive", "cleanup", "absent"} {
		t.Run(fault, func(t *testing.T) {
			repo, bin := t.TempDir(), t.TempDir()
			write := func(path, data string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				writeFile(t, path, []byte(data))
			}
			run := func(dir, name string, args ...string) []byte {
				t.Helper()
				cmd := exec.Command(name, args...)
				cmd.Dir = dir
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%s %v: %v\n%s", name, args, err, out)
				}
				return out
			}
			record := filepath.Join(repo, "experiments", "fixture")
			recipe, err := os.ReadFile("2026-11-21-non-alive-belief-duration/run.sh")
			if err != nil {
				t.Fatal(err)
			}
			helper, err := os.ReadFile("evidence.sh")
			if err != nil {
				t.Fatal(err)
			}
			resolver, err := os.ReadFile("revision.sh")
			if err != nil {
				t.Fatal(err)
			}
			run(repo, "git", "init", "--quiet")
			write(filepath.Join(repo, "go.mod"), "module fixture\ngo 1.25.0\n")
			write(filepath.Join(repo, "main.go"), "package main\nfunc main() {}\n")
			// The archived historical helper is deliberately unusable. Production
			// must invoke the current checkout's helper instead of this blob.
			write(filepath.Join(repo, "experiments", "evidence.sh"), "exit 98\n")
			var entries []map[string]any
			for i := 1; i <= 24; i++ {
				id := fmt.Sprintf("a%02d", i)
				status, exit := "complete", "0"
				if i == 1 { // A recorded failure can still reproduce successfully.
					status, exit = "recording-failed", "1"
				}
				old := fmt.Sprintf(`{"format":"lightpool-pilot/v1","spec":{"case":"fixture","replicate":%d,"code":"producer"},"status":%q}`, i, status)
				files := map[string]string{
					"summary.json": old, "status.json": fmt.Sprintf(`{"status":%q,"exit":%s}`, status, exit),
					"trace.ndjson": "{}\n", "scenario.json": "{}\n", "spec.json": fmt.Sprintf(`{"id":%q}`, id),
				}
				raw := filepath.Join(repo, "raw")
				if err := os.Mkdir(raw, 0o700); err != nil {
					t.Fatal(err)
				}
				hashes := map[string]string{}
				for name, data := range files {
					write(filepath.Join(raw, name), data)
					hashes[name] = fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
				}
				directory := filepath.Join(repo, "experiments", "original", "evidence", id)
				if err := os.MkdirAll(filepath.Dir(directory), 0o700); err != nil {
					t.Fatal(err)
				}
				evidence(t, "pack", raw, directory, false)
				derived := run(repo, "jq", `.format="lightpool-pilot-summary/v2" | .nonAlive={populations:[{label:"healthy"}]}`, filepath.Join(raw, "summary.json"))
				cmd := exec.Command("gzip", "-n", "-6")
				cmd.Stdin = bytes.NewReader(derived)
				compressed, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				write(filepath.Join(record, "summaries", id+".json.gz"), string(compressed))
				if fault == "status" && i == 2 {
					status = "expected-other-status"
				}
				entries = append(entries, map[string]any{
					"id": id, "record": "original", "directory": "evidence/" + id, "case": "fixture", "replicate": fmt.Sprint(i),
					"producer": "producer", "originalStatus": status, "originalExit": exit, "rawHashes": hashes,
				})
				if err := os.RemoveAll(raw); err != nil {
					t.Fatal(err)
				}
			}
			// As in production, Git keeps each attempt's SHA256SUMS and the
			// payloads are fetched into the working tree.
			write(filepath.Join(repo, ".gitignore"), "experiments/*/evidence/*/*\n!experiments/*/evidence/*/SHA256SUMS\n")
			if fault == "absent" {
				for _, name := range []string{"spec.json", "status.json", "summary.json", "scenario.json", "trace.ndjson.gz"} {
					if err := os.Remove(filepath.Join(repo, "experiments", "original", "evidence", "a02", name)); err != nil {
						t.Fatal(err)
					}
				}
			}
			run(repo, "git", "add", ".")
			run(repo, "git", "-c", "user.name=Recipe Test", "-c", "user.email=recipe@example.invalid", "commit", "--quiet", "-m", "pinned inputs")
			base := strings.TrimSpace(string(run(repo, "git", "rev-parse", "HEAD")))
			write(filepath.Join(repo, "experiments", "evidence.sh"), string(helper))
			write(filepath.Join(record, "run.sh"), string(recipe))
			write(filepath.Join(repo, "experiments", "revision.sh"), string(resolver))
			write(filepath.Join(record, "reducer-base"), base+"\n")
			write(filepath.Join(record, "evidence-base"), base+"\n")
			write(filepath.Join(record, "toolchain"), "local\n")
			inputs, err := json.Marshal(map[string]any{"attempts": entries})
			if err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(record, "inputs.json"), string(inputs))
			write(filepath.Join(record, "inputs.sha256"), fmt.Sprintf("%x  inputs.json\n", sha256.Sum256(inputs)))
			if fault == "final" {
				write(filepath.Join(record, "SHA256SUMS"), "different final manifest\n")
			}
			out := filepath.Join(repo, "output")
			t.Setenv("FAULT", fault)
			t.Setenv("RECIPE_OUT", out)
			t.Setenv("RECIPE_CALLS", filepath.Join(repo, "calls"))
			toolShim(t, bin, "go", `[[ $GOTMPDIR == "$RECIPE_OUT/work/tmp" ]] || exit 80
[[ $FAULT != build ]] || exit 81
[[ $1 == build && $4 == -o ]] || exit 82
cp "$STUB_REDUCER" "$5"
chmod +x "$5"`)
			reducer := filepath.Join(bin, "reducer")
			toolShim(t, bin, "reducer", `[[ $1 == -reduce && $3 == -out ]] || exit 83
id=$(jq -r .id "$2/spec.json")
echo "reduce $id" >> "$RECIPE_CALLS"
if [[ $id == a02 ]]; then
  case $FAULT in
    reducer) echo 'unexpected reducer failure' >&2; echo partial > "$4"; exit 7 ;;
    bytes) echo changed >> "$2/trace.ndjson" ;;
    set) touch "$2/extra" ;;
    symlink) rm "$2/trace.ndjson"; ln -s status.json "$2/trace.ndjson" ;;
  esac
fi
jq '.format="lightpool-pilot-summary/v2" | .nonAlive={populations:[{label:"healthy"}]}' "$2/summary.json" > "$4"
if [[ $id == a02 && $FAULT == compatibility ]]; then
  jq '.status="changed"' "$4" > "$4.tmp"; mv "$4.tmp" "$4"
fi
if [[ $id == a02 && $FAULT == summary ]]; then echo ' ' >> "$4"; fi
exit "$(jq -r .exit "$2/status.json")"`)
			t.Setenv("STUB_REDUCER", reducer)
			for _, name := range []string{"stat", "gzip", "git", "rm"} {
				real, err := exec.LookPath(name)
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("REAL_"+strings.ToUpper(name), real)
			}
			toolShim(t, bin, "stat", `if [[ $1 == -f ]]; then
  if [[ -e "$RECIPE_OUT/logs/a02.inputs.sha256" && $FAULT == low ]]; then echo '0 4096'; exit; fi
  echo '1000000 4096'; exit
fi
exec "$REAL_STAT" "$@"`)
			toolShim(t, bin, "gzip", `if [[ -e "$RECIPE_OUT/logs/a02.inputs.sha256" ]]; then
  if [[ $1 == -d && -d "$RECIPE_OUT/work/restored" && $FAULT == partial ]]; then echo partial; exit 42; fi
  if [[ $1 == -d && -d "$RECIPE_OUT/work/restored" && $FAULT == interrupt ]]; then kill -TERM "$(ps -o ppid= -p "$PPID" | tr -d ' ')"; exit 42; fi
  if [[ $1 == -n && $FAULT == compression ]]; then echo partial; exit 42; fi
fi
exec "$REAL_GZIP" "$@"`)
			toolShim(t, bin, "git", `if [[ $3 == archive ]]; then
  echo "archive ${@: -1}" >> "$RECIPE_CALLS"
  if [[ ${@: -1} == experiments/original/evidence/a02 ]]; then
    # At the next archive boundary no preceding raw or compressed input remains.
    [[ ! -e "$RECIPE_OUT/work/restored" && ! -e "$RECIPE_OUT/work/source/experiments/original/evidence/a01" && ! -e "$RECIPE_OUT/a01.json" ]] || exit 84
    [[ $FAULT != archive ]] || exit 85
  fi
fi
exec "$REAL_GIT" "$@"`)
			toolShim(t, bin, "rm", `[[ $FAULT != cleanup || $* != *a01.json* ]] || exit 86
exec "$REAL_RM" "$@"`)
			t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
			cmd := exec.Command("bash", filepath.Join(record, "run.sh"), out)
			output, runErr := cmd.CombinedOutput()
			if (runErr == nil) != (fault == "success") {
				t.Fatalf("fault=%s: %v\n%s", fault, runErr, output)
			}
			work := filepath.Join(out, "work")
			if fault == "success" {
				if _, err := os.Stat(work); !os.IsNotExist(err) {
					t.Fatalf("successful work tree remains: %v", err)
				}
				for _, path := range []string{"SHA256SUMS", "summaries/a24.json.gz", "logs/a01.after", "logs/a01.inputs.sha256", "logs/a01.exit"} {
					if _, err := os.Stat(filepath.Join(out, path)); err != nil {
						t.Fatal(err)
					}
				}
				data, err := os.ReadFile(filepath.Join(out, "logs/a01.exit"))
				if err != nil || string(data) != "1\n" {
					t.Fatalf("original failed attempt did not reproduce: %s %v", data, err)
				}
				return
			}
			if !strings.Contains(string(output), "working files retained under "+work) {
				t.Fatalf("missing retained-path diagnostic: %s", output)
			}
			if _, err := os.Stat(work); err != nil {
				t.Fatalf("failure lost working files: %v", err)
			}
			if fault == "build" || fault == "interrupt" || fault == "cleanup" {
				if fault == "interrupt" {
					if exit, ok := runErr.(*exec.ExitError); !ok || exit.ExitCode() != 143 {
						t.Fatalf("recipe did not handle SIGTERM: %v\n%s", runErr, output)
					}
					if _, err := os.Stat(filepath.Join(work, "restored")); err != nil {
						t.Fatal(err)
					}
				}
				return
			}
			data, err := os.ReadFile(filepath.Join(out, "cells.tsv"))
			if err != nil {
				t.Fatal(err)
			}
			if len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 25 {
				t.Fatalf("inventory incomplete: %s", data)
			}
			if fault == "final" {
				if _, err := os.Stat(filepath.Join(work, "src")); err != nil {
					t.Fatal(err)
				}
				return
			}
			if strings.Count(string(data), "stopped-after-a02") != 22 || !strings.Contains(string(data), "a02\tfixture") {
				t.Fatalf("later attempts not explicitly unmeasured: %s", data)
			}
			calls, err := os.ReadFile(filepath.Join(repo, "calls"))
			if err != nil || strings.Contains(string(calls), "a03") {
				t.Fatalf("work continued after failure: %s %v", calls, err)
			}
			if (fault == "low" || fault == "partial" || fault == "archive" || fault == "status" || fault == "absent") && strings.Contains(string(calls), "reduce a02") {
				t.Fatalf("reducer reached invalid restoration: %s", calls)
			}
			if fault == "absent" && !strings.Contains(string(output), "evidence-store.sh fetch original first") {
				t.Fatalf("absent payloads not diagnosed: %s", output)
			}
			if fault != "low" && fault != "archive" && fault != "absent" {
				if _, err := os.Stat(filepath.Join(work, "restored")); err != nil {
					t.Fatalf("failed restoration lost: %v", err)
				}
			}
			if fault == "reducer" {
				data, err := os.ReadFile(filepath.Join(out, "a02.json"))
				if err != nil || string(data) != "partial\n" {
					t.Fatalf("partial summary lost: %s %v", data, err)
				}
			}
			if fault == "compatibility" {
				data, err := os.ReadFile(filepath.Join(out, "logs/a02.compatibility"))
				if err != nil || len(data) == 0 {
					t.Fatalf("old-field mismatch not diagnosed: %s %v", data, err)
				}
			}
		})
	}
}
