package asks

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

const conv = "0d6f9c7e-1111-4222-8333-444455556666"

func read(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, conv))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRecordKeepsTheNewest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "codex-asks")
	if err := Record(dir, conv, "s2", t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := Record(dir, conv, "s1", t0); err != nil {
		t.Fatal(err)
	}
	if got := read(t, dir); got != "s2 1792065660\n" {
		t.Fatalf("got %q", got)
	}
	if err := Record(dir, conv, "s3", t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := read(t, dir); got != "s3 1792065720\n" {
		t.Fatalf("got %q", got)
	}
	di, _ := os.Stat(dir)
	fi, _ := os.Stat(filepath.Join(dir, conv))
	if di.Mode().Perm() != 0o700 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("modes %v %v", di.Mode(), fi.Mode())
	}
}

func TestRecordRefusesBadValues(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "codex-asks")
	for name, c := range map[string][2]string{
		"conversation path": {"../x", "s1"},
		"conversation case": {"0D6F9C7E-1111-4222-8333-444455556666", "s1"},
		"session space":     {conv, "s 1"},
		"session newline":   {conv, "s1\nx"},
		"session empty":     {conv, ""},
		"session long":      {conv, string(make([]byte, 65))},
	} {
		if err := Record(dir, c[0], c[1], t0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
