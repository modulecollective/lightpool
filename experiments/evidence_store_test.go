package experiments_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// fakeGitHub answers the few release API calls evidence-store.sh makes, from
// one release kept under $STORE. Published assets are copied to
// $STORE/download/<tag>/, which fetch reads through a file:// base URL.
const fakeGitHub = `[[ $1 == api ]] || exit 90
path=$2; shift 2
method=GET jq= input= tag=
while (($#)); do
  case $1 in
    -X) method=$2; shift 2 ;;
    --jq) jq=$2; shift 2 ;;
    --input) input=$2; shift 2 ;;
    -f|-F) [[ $2 != tag_name=* ]] || tag=${2#tag_name=}; shift 2 ;;
    -H) shift 2 ;;
    --paginate) shift ;;
    *) exit 91 ;;
  esac
done
digest() { echo "$(stat -c %s "$1") sha256:$(sha256sum <"$1" | cut -d' ' -f1)"; }
repo=modulecollective/lightpool
case "$method $path" in
  "GET repos/$repo/releases") [[ ! -f $STORE/id ]] || cat "$STORE/id" ;;
  "POST repos/$repo/releases") echo 7 >"$STORE/id"; echo "$tag" >"$STORE/tag"; mkdir -p "$STORE/assets"; echo 7 ;;
  "GET repos/$repo/releases/7/assets")
    name=$(sed -E 's/.*\.name == "([^"]+)".*/\1/' <<<"$jq")
    [[ ! -f $STORE/assets/$name ]] || digest "$STORE/assets/$name" ;;
  "POST https://uploads.github.com/repos/$repo/releases/7/assets?name="*)
    name=${path##*name=}
    echo "$name" >>"$STORE/uploads"
    cp "$input" "$STORE/assets/$name"
    [[ ${FAULT-} != corrupt ]] || echo x >>"$STORE/assets/$name" ;;
  "PATCH repos/$repo/releases/7")
    mkdir -p "$STORE/download/$(<"$STORE/tag")"
    cp "$STORE"/assets/* "$STORE/download/$(<"$STORE/tag")/" ;;
  "GET repos/$repo/git/ref/tags/"*) echo "${TAGGED:-1801ef926af5ee05d80617bb531f6dfcd24a71e1}" ;;
  "GET repos/$repo/releases/tags/"*)
    dir=$STORE/download/${path##*/}
    [[ -d $dir ]] || { echo 'HTTP 404: Not Found' >&2; exit 1; }
    for f in "$dir"/*; do echo "${f##*/} $(digest "$f")"; done ;;
  *) echo "unexpected: $method $path" >&2; exit 92 ;;
esac`

type storeFixture struct {
	t                   *testing.T
	experiments, record string
	packed              map[string]map[string][]byte // attempt -> packed files
}

// newStoreFixture lays out experiments/rec with two packed attempts beside a
// copy of the real tools, and puts the fake gh first on PATH.
func newStoreFixture(t *testing.T) *storeFixture {
	t.Helper()
	root, bin, store := t.TempDir(), t.TempDir(), t.TempDir()
	f := &storeFixture{t: t, experiments: filepath.Join(root, "experiments"), packed: map[string]map[string][]byte{}}
	f.record = filepath.Join(f.experiments, "rec")
	if err := os.MkdirAll(filepath.Join(f.record, "evidence"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"evidence.sh", "evidence-store.sh"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(f.experiments, name), data)
	}
	for attempt, trace := range map[string]string{"a-r1": "{\"event\":1}\n", "a-r2": "{\"event\":2}\n{\"partial\":"} {
		raw := filepath.Join(root, "raw-"+attempt)
		if err := os.Mkdir(raw, 0o700); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(raw, "trace.ndjson"), []byte(trace))
		writeFile(t, filepath.Join(raw, "status.json"), []byte("{\"status\":\""+attempt+"\"}\n"))
		directory := filepath.Join(f.record, "evidence", attempt)
		evidence(t, "pack", raw, directory, false)
		f.packed[attempt] = readDir(t, directory)
	}
	toolShim(t, bin, "gh", fakeGitHub)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	t.Setenv("STORE", store)
	t.Setenv("EVIDENCE_STORE_URL", "file://"+filepath.Join(store, "download"))
	t.Setenv("FAULT", "")
	return f
}

func readDir(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = data
	}
	return files
}

func (f *storeFixture) run(wantError bool, args ...string) string {
	f.t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(f.experiments, "evidence-store.sh")}, args...)...)
	output, err := cmd.CombinedOutput()
	if (err != nil) != wantError {
		f.t.Fatalf("evidence-store.sh %v: %v\n%s", args, err, output)
	}
	return string(output)
}

// dropPayloads leaves each attempt as a fresh clone has it: SHA256SUMS only.
func (f *storeFixture) dropPayloads() {
	f.t.Helper()
	for attempt, files := range f.packed {
		for name := range files {
			if name != "SHA256SUMS" {
				if err := os.Remove(filepath.Join(f.record, "evidence", attempt, name)); err != nil {
					f.t.Fatal(err)
				}
			}
		}
	}
}

func (f *storeFixture) manifest() string {
	f.t.Helper()
	data, err := os.ReadFile(filepath.Join(f.record, "evidence-assets.tsv"))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(data)
}

func (f *storeFixture) editManifest(old, replacement string) {
	f.t.Helper()
	data := f.manifest()
	if !strings.Contains(data, old) {
		f.t.Fatalf("manifest lacks %q:\n%s", old, data)
	}
	writeFile(f.t, filepath.Join(f.record, "evidence-assets.tsv"), []byte(strings.Replace(data, old, replacement, 1)))
}

var manifestRow = regexp.MustCompile(`(?m)^(a-r[12])\tevidence-rec-1\t(a-r[12])\.tar\.gz\t([0-9]+)\t([0-9a-f]{64})$`)

func TestEvidenceStoreRoundTrip(t *testing.T) {
	f := newStoreFixture(t)
	f.run(false, "publish", "rec", "1")
	rows := manifestRow.FindAllStringSubmatch(f.manifest(), -1)
	if !strings.HasPrefix(f.manifest(), "attempt\ttag\tasset\tbytes\tsha256\n") || len(rows) != 2 {
		t.Fatalf("manifest:\n%s", f.manifest())
	}
	// Publishing leaves the committed attempt untouched.
	for attempt, files := range f.packed {
		assertFiles(t, filepath.Join(f.record, "evidence", attempt), files)
	}
	if out := f.run(false, "check"); !strings.Contains(out, "2 assets match") {
		t.Fatalf("check: %s", out)
	}
	if out := f.run(true, "publish", "rec", "1"); !strings.Contains(out, "no unpublished attempt") {
		t.Fatalf("republish: %s", out)
	}
	f.dropPayloads()
	f.run(false, "check")
	f.run(false, "fetch", "rec", "a-r2")
	assertFiles(t, filepath.Join(f.record, "evidence", "a-r2"), f.packed["a-r2"])
	f.run(false, "fetch", "rec", "a-r1")
	for attempt, files := range f.packed {
		assertFiles(t, filepath.Join(f.record, "evidence", attempt), files)
		// The restored directory is still a valid packed attempt.
		evidence(t, "unpack", filepath.Join(f.record, "evidence", attempt), filepath.Join(t.TempDir(), "raw"), false)
	}
}

func TestEvidenceStorePublishResumesAndRefusesMismatch(t *testing.T) {
	t.Run("resume", func(t *testing.T) {
		f := newStoreFixture(t)
		f.run(false, "publish", "rec", "1")
		first := readDir(t, filepath.Join(os.Getenv("STORE"), "assets"))
		// An interrupted run uploaded both assets but recorded nothing.
		writeFile(t, filepath.Join(f.record, "evidence-assets.tsv"), []byte("attempt\ttag\tasset\tbytes\tsha256\n"))
		if err := os.Remove(filepath.Join(os.Getenv("STORE"), "uploads")); err != nil {
			t.Fatal(err)
		}
		f.run(false, "publish", "rec", "1")
		if _, err := os.Stat(filepath.Join(os.Getenv("STORE"), "uploads")); !os.IsNotExist(err) {
			t.Fatalf("resume uploaded again: %v", err)
		}
		if len(manifestRow.FindAllString(f.manifest(), -1)) != 2 {
			t.Fatalf("manifest:\n%s", f.manifest())
		}
		assertFiles(t, filepath.Join(os.Getenv("STORE"), "assets"), first)
	})
	t.Run("corrupt-upload", func(t *testing.T) {
		f := newStoreFixture(t)
		t.Setenv("FAULT", "corrupt")
		if out := f.run(true, "publish", "rec", "1"); !strings.Contains(out, "nothing recorded") {
			t.Fatalf("publish: %s", out)
		}
		if f.manifest() != "attempt\ttag\tasset\tbytes\tsha256\n" {
			t.Fatalf("row recorded for unconfirmed bytes:\n%s", f.manifest())
		}
	})
	t.Run("tag-moved", func(t *testing.T) {
		f := newStoreFixture(t)
		t.Setenv("TAGGED", strings.Repeat("0", 40))
		if out := f.run(true, "publish", "rec", "1"); !strings.Contains(out, "points at") {
			t.Fatalf("publish: %s", out)
		}
	})
}

func TestEvidenceStoreFetchRefusals(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"size", "size differs"},
		{"digest", "sha256 differs"},
		{"sums", "differs from the committed one"},
		{"occupied", "already holds payloads"},
		{"missing", "download failed"},
		{"unlisted", "no manifest row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStoreFixture(t)
			f.run(false, "publish", "rec", "1")
			f.dropPayloads()
			row := manifestRow.FindStringSubmatch(f.manifest())
			attempt := row[1]
			attemptDir := filepath.Join(f.record, "evidence", attempt)
			args := []string{"fetch", "rec", attempt}
			switch tc.name {
			case "size":
				f.editManifest("\t"+row[3]+"\t", "\t1"+row[3]+"\t")
			case "digest":
				f.editManifest(row[4], strings.Repeat("0", 64))
			case "sums":
				writeFile(t, filepath.Join(attemptDir, "SHA256SUMS"), []byte(strings.Repeat("0", 64)+"  status.json\n"))
			case "occupied":
				writeFile(t, filepath.Join(attemptDir, "status.json"), []byte("local\n"))
			case "missing":
				if err := os.RemoveAll(filepath.Join(os.Getenv("STORE"), "download")); err != nil {
					t.Fatal(err)
				}
			case "unlisted":
				args = []string{"fetch", "rec", "a-r9"}
			}
			before := readDir(t, attemptDir)
			if out := f.run(true, args...); !strings.Contains(out, tc.want) {
				t.Fatalf("fetch: %s", out)
			}
			assertFiles(t, attemptDir, before)
		})
	}
}

func TestEvidenceStoreCheckDiscriminates(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"digest", "manifest pins"},
		{"release", "nothing"},
		{"orphan", "neither payloads nor an asset row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStoreFixture(t)
			f.run(false, "publish", "rec", "1")
			row := manifestRow.FindStringSubmatch(f.manifest())
			switch tc.name {
			case "digest": // One character is enough.
				f.editManifest(row[4], row[4][:63]+string("10"[row[4][63]&1]))
			case "release":
				if err := os.RemoveAll(filepath.Join(os.Getenv("STORE"), "download")); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				f.dropPayloads()
				f.editManifest(row[0]+"\n", "")
			}
			if out := f.run(true, "check"); !strings.Contains(out, tc.want) {
				t.Fatalf("check: %s", out)
			}
		})
	}
}

func TestRevision(t *testing.T) {
	dir := t.TempDir()
	data, err := os.ReadFile("revision.sh")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "revision.sh"), data)
	old, rewritten, other := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	resolve := func(arg string, wantError bool) string {
		t.Helper()
		out, err := exec.Command("bash", filepath.Join(dir, "revision.sh"), arg).Output()
		if (err != nil) != wantError {
			t.Fatalf("revision.sh %s: %v", arg, err)
		}
		return string(out)
	}
	if got := resolve(old, false); got != old+"\n" {
		t.Fatalf("no map: %q", got)
	}
	writeFile(t, filepath.Join(dir, "rewritten-commits.tsv"), []byte(other+"\t"+other+"\n"+old+"\t"+rewritten+"\n"))
	if got := resolve(old, false); got != rewritten+"\n" {
		t.Fatalf("mapped: %q", got)
	}
	if got := resolve(strings.Repeat("d", 40), false); got != strings.Repeat("d", 40)+"\n" {
		t.Fatalf("unmapped: %q", got)
	}
	for _, bad := range []string{"aaaa", "HEAD", old[:39] + "g"} {
		resolve(bad, true)
	}
}
