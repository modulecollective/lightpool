package experiments_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func evidence(t *testing.T, mode, source, destination string, wantError bool) string {
	t.Helper()
	cmd := exec.Command("bash", "evidence.sh", mode, source, destination)
	output, err := cmd.CombinedOutput()
	if (err != nil) != wantError {
		t.Fatalf("%s %s %s: %v\n%s", mode, source, destination, err, output)
	}
	return string(output)
}

func assertFiles(t *testing.T, directory string, want map[string][]byte) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(want) {
		t.Fatalf("%s: got %d files, want %d", directory, len(entries), len(want))
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		original, ok := want[entry.Name()]
		if err != nil || !ok || !bytes.Equal(data, original) {
			t.Fatalf("%s: file differs: %v", entry.Name(), err)
		}
	}
}

func TestEvidenceRoundTrip(t *testing.T) {
	for _, name := range []string{"trace.ndjson", "samples.ndjson"} {
		t.Run(name, func(t *testing.T) {
			source := t.TempDir()
			want := map[string][]byte{
				name:           []byte("{\"event\":1}\n{\"partial\":"),
				"empty.ndjson": {},
				"status.json":  []byte("{\"status\":\"recording-failed\"}\n"),
				"stderr.txt":   {0, 0xff, '\n'},
				"report.gz":    []byte("an original sidecar with a gz extension"),
			}
			for name, data := range want {
				writeFile(t, filepath.Join(source, name), data)
			}
			parent := t.TempDir()
			archive := filepath.Join(parent, "archive")
			evidence(t, "pack", source, archive, false)
			assertFiles(t, source, want)
			// Identical inputs yield identical gzip headers and checksum ordering.
			second := filepath.Join(parent, "second")
			evidence(t, "pack", source, second, false)
			entries, err := os.ReadDir(archive)
			if err != nil {
				t.Fatal(err)
			}
			packed := make(map[string][]byte)
			for _, entry := range entries {
				data, err := os.ReadFile(filepath.Join(archive, entry.Name()))
				if err != nil {
					t.Fatal(err)
				}
				packed[entry.Name()] = data
			}
			assertFiles(t, second, packed)
			if err := os.RemoveAll(source); err != nil {
				t.Fatal(err)
			}
			restored := filepath.Join(parent, "restored")
			evidence(t, "unpack", archive, restored, false)
			assertFiles(t, restored, want)
			assertFiles(t, archive, packed)
		})
	}
	for _, files := range []map[string][]byte{
		{}, {"stderr.txt": []byte("failed before output directory creation\n")},
	} {
		source := t.TempDir()
		for name, data := range files {
			writeFile(t, filepath.Join(source, name), data)
		}
		parent := t.TempDir()
		evidence(t, "pack", source, filepath.Join(parent, "packed"), false)
		evidence(t, "unpack", filepath.Join(parent, "packed"), filepath.Join(parent, "restored"), false)
		assertFiles(t, filepath.Join(parent, "restored"), files)
	}
}

func TestEvidenceRefusesInvalidSources(t *testing.T) {
	for _, mode := range []string{"pack", "unpack"} {
		for _, kind := range []string{"symlink", "nested", "unsafe", "reserved", "collision", "source-symlink", "source-symlink-slash"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				source := t.TempDir()
				writeFile(t, filepath.Join(source, "status.json"), []byte("{}\n"))
				switch kind {
				case "symlink":
					if err := os.Symlink("status.json", filepath.Join(source, "link")); err != nil {
						t.Fatal(err)
					}
				case "nested":
					if err := os.Mkdir(filepath.Join(source, "nested"), 0o700); err != nil {
						t.Fatal(err)
					}
				case "unsafe":
					writeFile(t, filepath.Join(source, "bad\nname"), nil)
				case "reserved":
					writeFile(t, filepath.Join(source, "SHA256SUMS"), []byte("not a manifest\n"))
				case "collision":
					writeFile(t, filepath.Join(source, "trace.ndjson"), nil)
					writeFile(t, filepath.Join(source, "trace.ndjson.gz"), nil)
				case "source-symlink", "source-symlink-slash":
					link := filepath.Join(t.TempDir(), "link")
					if err := os.Symlink(source, link); err != nil {
						t.Fatal(err)
					}
					source = link
					if kind == "source-symlink-slash" {
						source += "///"
					}
				}
				destination := filepath.Join(t.TempDir(), "output")
				evidence(t, mode, source, destination, true)
				if _, err := os.Lstat(destination); !os.IsNotExist(err) {
					t.Fatalf("invalid source created destination: %v", err)
				}
				data, err := os.ReadFile(filepath.Join(source, "status.json"))
				if err != nil || string(data) != "{}\n" {
					t.Fatalf("source modified: %v", err)
				}
			})
		}
	}
}

func TestEvidenceCorruption(t *testing.T) {
	for _, kind := range []string{"gzip", "sidecar", "missing", "extra", "traversal", "absolute", "shell", "duplicate", "collision", "reserved", "no-manifest", "bad-hash"} {
		t.Run(kind, func(t *testing.T) {
			source := t.TempDir()
			writeFile(t, filepath.Join(source, "trace.ndjson"), []byte("{}\n"))
			writeFile(t, filepath.Join(source, "status.json"), []byte("{}\n"))
			parent := t.TempDir()
			archive := filepath.Join(parent, "archive")
			evidence(t, "pack", source, archive, false)
			manifest := filepath.Join(archive, "SHA256SUMS")
			switch kind {
			case "gzip":
				writeFile(t, filepath.Join(archive, "trace.ndjson.gz"), []byte("broken"))
			case "sidecar":
				writeFile(t, filepath.Join(archive, "status.json"), []byte("changed"))
			case "missing", "no-manifest":
				path := filepath.Join(archive, "status.json")
				if kind == "no-manifest" {
					path = manifest
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "extra":
				writeFile(t, filepath.Join(archive, "extra.json"), nil)
			default:
				names := map[string]string{
					"traversal": "../escaped", "absolute": filepath.Join(parent, "escaped"),
					"shell": "$(touch escaped)", "reserved": "SHA256SUMS", "bad-hash": "status.json",
					"duplicate": "status.json", "collision": "trace.ndjson.gz",
				}
				data, err := os.ReadFile(manifest)
				if err != nil {
					t.Fatal(err)
				}
				hash := fmt.Sprintf("%x", sha256.Sum256(nil))
				if kind == "bad-hash" {
					hash = "invalid"
				}
				writeFile(t, manifest, fmt.Appendf(data, "%s  %s\n", hash, names[kind]))
			}
			output := evidence(t, "unpack", archive, filepath.Join(parent, "restored"), true)
			if kind == "sidecar" && !strings.Contains(output, "checksum mismatch") {
				t.Fatalf("expected checksum failure: %s", output)
			}
			if kind != "gzip" && kind != "sidecar" {
				if _, err := os.Lstat(filepath.Join(parent, "restored")); !os.IsNotExist(err) {
					t.Fatalf("invalid archive created destination: %v", err)
				}
			}
			if _, err := os.Stat(filepath.Join(parent, "escaped")); !os.IsNotExist(err) {
				t.Fatalf("manifest escaped destination: %v", err)
			}
		})
	}
}

func TestEvidenceRetryAfterChecksumFailure(t *testing.T) {
	source := t.TempDir()
	want := map[string][]byte{"status.json": []byte("{\"status\":\"incomplete\"}\n")}
	writeFile(t, filepath.Join(source, "status.json"), want["status.json"])
	parent := t.TempDir()
	archive := filepath.Join(parent, "archive")
	evidence(t, "pack", source, archive, false)
	writeFile(t, filepath.Join(archive, "status.json"), []byte("corrupted"))
	failed := filepath.Join(parent, "failed")
	evidence(t, "unpack", archive, failed, true)
	assertFiles(t, failed, map[string][]byte{"status.json": []byte("corrupted")})
	writeFile(t, filepath.Join(archive, "status.json"), want["status.json"])
	evidence(t, "unpack", archive, failed, true)
	retry := filepath.Join(parent, "retry")
	evidence(t, "unpack", archive, retry, false)
	assertFiles(t, retry, want)
	assertFiles(t, source, want)
}

func TestEvidenceDestinationGuards(t *testing.T) {
	source := t.TempDir()
	want := map[string][]byte{"stderr.txt": []byte("early failure")}
	writeFile(t, filepath.Join(source, "stderr.txt"), want["stderr.txt"])
	parent := t.TempDir()
	archive := filepath.Join(parent, "archive")
	evidence(t, "pack", source, archive, false)
	for _, mode := range []string{"pack", "unpack"} {
		input := source
		if mode == "unpack" {
			input = archive
		}
		evidence(t, mode, input, source, true)
		assertFiles(t, source, want)
		evidence(t, mode, input, filepath.Join(input, "child"), true)
		link := filepath.Join(t.TempDir(), "dangling")
		if err := os.Symlink("missing", link); err != nil {
			t.Fatal(err)
		}
		evidence(t, mode, input, link, true)
	}
}

func TestEvidenceToolFailures(t *testing.T) {
	for _, tool := range []string{"gzip", "cp", "sha256sum"} {
		t.Run(tool, func(t *testing.T) {
			source := t.TempDir()
			writeFile(t, filepath.Join(source, "trace.ndjson"), []byte("{}\n"))
			writeFile(t, filepath.Join(source, "status.json"), []byte("{}\n"))
			parent := t.TempDir()
			archive := filepath.Join(parent, "archive")
			evidence(t, "pack", source, archive, false)
			bin := t.TempDir()
			shim := filepath.Join(bin, tool)
			writeFile(t, shim, []byte("#!/bin/sh\nexit 42\n"))
			if err := os.Chmod(shim, 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			evidence(t, "pack", source, filepath.Join(parent, "failed-pack"), true)
			evidence(t, "unpack", archive, filepath.Join(parent, "failed-unpack"), true)
		})
	}
}

func toolShim(t *testing.T, bin, name, body string) {
	t.Helper()
	path := filepath.Join(bin, name)
	writeFile(t, path, []byte("#!/usr/bin/env bash\nset -eu\n"+body+"\n"))
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name, space string
		fail        bool
	}{
		{"low", "65538 4096", true}, // Both gzip members together need two blocks, plus the sidecar.
		{"equal", "65539 4096", false},
		{"ample", "99999 4096", false},
		{"malformed", "no space", true},
		{"negative", "-1 4096", true},
		{"zero-unit", "10 0", true},
		{"overflow", "9223372036854775808 4096", true},
		{"product-overflow", "9223372036854775807 4096", true},
		{"query-failure", "fail", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, parent, bin := t.TempDir(), t.TempDir(), t.TempDir()
			want := map[string][]byte{"trace.ndjson": []byte(strings.Repeat("a", 4096) + "b"), "empty.ndjson": {}, "status.json": []byte("x")}
			for name, data := range want {
				writeFile(t, filepath.Join(source, name), data)
			}
			archive := filepath.Join(parent, "archive")
			evidence(t, "pack", source, archive, false)
			// A concatenated gzip stream must be counted in full, including both members.
			var members []byte
			for _, data := range []string{strings.Repeat("a", 4096), "b"} {
				cmd := exec.Command("gzip", "-n", "-c")
				cmd.Stdin = strings.NewReader(data)
				out, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				members = append(members, out...)
			}
			writeFile(t, filepath.Join(archive, "trace.ndjson.gz"), members)
			stat, err := exec.LookPath("stat")
			if err != nil {
				t.Fatal(err)
			}
			gzip, err := exec.LookPath("gzip")
			if err != nil {
				t.Fatal(err)
			}
			toolShim(t, bin, "stat", `if [[ $1 == -f ]]; then
  [[ ${@: -1} == "$SPACE_PARENT" ]] || exit 71
  [[ $SPACE != fail ]] || exit 72
  echo "$SPACE"
else
  exec "$REAL_STAT" "$@"
fi`)
			toolShim(t, bin, "gzip", `if [[ -d $RESTORED ]]; then echo write; else echo size; fi >> "$GZIP_CALLS"
exec "$REAL_GZIP" "$@"`)
			restored := filepath.Join(parent, "restored")
			t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
			t.Setenv("SPACE_PARENT", parent)
			t.Setenv("SPACE", tc.space)
			t.Setenv("REAL_STAT", stat)
			t.Setenv("REAL_GZIP", gzip)
			t.Setenv("RESTORED", restored)
			calls := filepath.Join(parent, "calls")
			t.Setenv("GZIP_CALLS", calls)
			alias := filepath.Join(t.TempDir(), "parent-link")
			if err := os.Symlink(parent, alias); err != nil {
				t.Fatal(err)
			}
			output := evidence(t, "unpack", archive, filepath.Join(alias, "restored"), tc.fail)
			assertFiles(t, source, want)
			data, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			if tc.fail {
				if _, err := os.Lstat(restored); !os.IsNotExist(err) || strings.Contains(string(data), "write") {
					t.Fatalf("preflight wrote payloads: %v, calls=%s", err, data)
				}
				if tc.name == "low" && !strings.Contains(output, "available=268443648 payload=12288 reserve=268435456") {
					t.Fatalf("missing sizing diagnostic: %s", output)
				}
			} else {
				assertFiles(t, restored, want)
				if output != "" || strings.Count(string(data), "size") != 2 || strings.Count(string(data), "write") != 2 {
					t.Fatalf("unexpected successful restore: %q, calls=%s", output, data)
				}
			}
			// Source compressed bytes also remain untouched.
			got, err := os.ReadFile(filepath.Join(archive, "trace.ndjson.gz"))
			if err != nil || !bytes.Equal(got, members) {
				t.Fatalf("archive changed: %v", err)
			}
		})
	}
}

func TestEvidenceSizingAndWriteFailures(t *testing.T) {
	for _, kind := range []string{"sizing-gzip", "sizing-count", "count-malformed", "size-overflow", "allocation-overflow", "reserve-overflow", "writing-gzip", "writing-copy"} {
		t.Run(kind, func(t *testing.T) {
			source, parent, bin := t.TempDir(), t.TempDir(), t.TempDir()
			want := map[string][]byte{"trace.ndjson": []byte("{}\n"), "status.json": []byte("{}\n")}
			for name, data := range want {
				writeFile(t, filepath.Join(source, name), data)
			}
			archive := filepath.Join(parent, "archive")
			evidence(t, "pack", source, archive, false)
			restored := filepath.Join(parent, "restored")
			t.Setenv("RESTORED", restored)
			name, body := "gzip", "exit 42"
			switch kind {
			case "sizing-count":
				name = "wc"
			case "count-malformed":
				name, body = "wc", "cat >/dev/null; echo invalid"
			case "size-overflow":
				name, body = "wc", "cat >/dev/null; echo 9223372036854775808"
			case "allocation-overflow":
				name, body = "wc", "cat >/dev/null; echo 9223372036854775807"
			case "reserve-overflow":
				name, body = "wc", "cat >/dev/null; echo 9223372036854771712"
			case "writing-gzip", "writing-copy":
				if kind == "writing-copy" {
					name = "cp"
				}
				real, err := exec.LookPath(name)
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("REAL_TOOL", real)
				body = `if [[ -d $RESTORED ]]; then echo partial; exit 42; fi
exec "$REAL_TOOL" "$@"`
			}
			toolShim(t, bin, name, body)
			t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
			evidence(t, "unpack", archive, restored, true)
			_, err := os.Stat(restored)
			if strings.HasPrefix(kind, "writing-") {
				if err != nil {
					t.Fatalf("lost partial destination: %v", err)
				}
				evidence(t, "unpack", archive, restored, true)
			} else if !os.IsNotExist(err) {
				t.Fatalf("sizing failure created destination: %v", err)
			}
			assertFiles(t, source, want)
		})
	}
}
