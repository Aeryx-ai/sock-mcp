package replies

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

func TestRecordThenSince(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "replies")
	if ok, err := Since(dir, "c1", t0); err != nil || ok {
		t.Fatalf("nothing recorded yet: ok=%v err=%v", ok, err)
	}
	if err := Record(dir, "c1", t0); err != nil {
		t.Fatal(err)
	}
	for since, want := range map[time.Time]bool{
		t0.Add(-time.Second): true,
		t0:                   true,
		t0.Add(time.Second):  false,
	} {
		if ok, err := Since(dir, "c1", since); err != nil || ok != want {
			t.Fatalf("Since(%v) = %v, %v; want %v", since, ok, err, want)
		}
	}
	if ok, _ := Since(dir, "c2", t0.Add(-time.Hour)); ok {
		t.Fatal("another conversation's reply counted")
	}
}

func TestRecordKeepsTheNewestAndIsPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "replies")
	if err := Record(dir, "c1", t0); err != nil {
		t.Fatal(err)
	}
	if err := Record(dir, "c1", t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if ok, _ := Since(dir, "c1", t0.Add(30*time.Second)); !ok {
		t.Fatal("the newer reply was lost")
	}
	di, err := os.Stat(dir)
	if err != nil || di.Mode().Perm() != 0o700 {
		t.Fatalf("dir %v %v", di, err)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Fatalf("%d entries", len(ents))
	}
	fi, _ := ents[0].Info()
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("file mode %v", fi.Mode())
	}
}

// A conversation id is a hub value, but it must never choose a path.
func TestConversationIDCannotChooseAPath(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "replies")
	if err := Record(dir, "../../escape", t0); err != nil {
		t.Fatal(err)
	}
	var seen []string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			seen = append(seen, p)
		}
		return nil
	})
	if len(seen) != 1 || filepath.Dir(seen[0]) != dir {
		t.Fatalf("files %v", seen)
	}
}

func TestRecordPrunesWhatTheHubHasLongForgotten(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "replies")
	if err := Record(dir, "old", t0.Add(-40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := Record(dir, "new", t0); err != nil {
		t.Fatal(err)
	}
	if ok, _ := Since(dir, "old", t0.Add(-100*24*time.Hour)); ok {
		t.Fatal("a 40 day old record survived")
	}
}

func TestDir(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if d, err := Dir(get(map[string]string{"XDG_STATE_HOME": "/x/state"})); err != nil || d != "/x/state/sock-mcp/replies" {
		t.Fatalf("%q %v", d, err)
	}
	if d, err := Dir(get(map[string]string{"XDG_STATE_HOME": "relative", "HOME": "/h"})); err != nil || d != "/h/.local/state/sock-mcp/replies" {
		t.Fatalf("a relative XDG_STATE_HOME must be ignored: %q %v", d, err)
	}
	if _, err := Dir(get(nil)); err == nil {
		t.Fatal("no home: want an error, not a path under the working directory")
	}
}
