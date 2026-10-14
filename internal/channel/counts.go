package channel

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MaxPushes is how many times one delivery is pushed without the model acting
// on it. A host that drops channel notifications never acts, so this bounds
// what a restart loop of such sessions can push; past it the delivery stays
// pending for sock_fetch.
const MaxPushes = 5

// countTTL is past the hub's message retention (30 days): a count older than
// it is of a message the hub has swept.
const countTTL = 31 * 24 * time.Hour

// Counts persists each delivery's push count across restarts: one 0600 file
// per message id, named by the id's SHA-256, in a 0700 directory per puppet.
// Several sock-mcp processes of one puppet share it; a lost increment between
// two of them only loosens the bound, never removes it.
type Counts struct {
	dir string
}

// CountsDir is the push counts of one puppet:
// ${XDG_STATE_HOME:-~/.local/state}/sock-mcp/pushes/<account id's hash>.
func CountsDir(getenv func(string) string, accountID string) (string, error) {
	state := getenv("XDG_STATE_HOME")
	if !filepath.IsAbs(state) { // XDG: a relative path is ignored
		home := getenv("HOME")
		if home == "" {
			home = getenv("USERPROFILE")
		}
		state = filepath.Join(home, ".local", "state")
	}
	if !filepath.IsAbs(state) {
		return "", errors.New("channel: no home directory to keep push counts in (set HOME or XDG_STATE_HOME)")
	}
	return filepath.Join(state, "sock-mcp", "pushes", hashName(accountID)), nil
}

// OpenCounts makes dir private and prunes counts last touched before
// now - countTTL.
func OpenCounts(dir string, now time.Time) (*Counts, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("channel: push counts: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("channel: push counts: %w", err)
	}
	c := &Counts{dir: dir}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("channel: push counts: %w", err)
	}
	for _, e := range ents {
		if info, err := e.Info(); err == nil && info.ModTime().Before(now.Add(-countTTL)) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return c, nil
}

func hashName(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (c *Counts) path(id string) string { return filepath.Join(c.dir, hashName(id)) }

// Take reports whether id may be pushed once more, and records the push
// before it happens. It fails safe: a count it cannot read or write is
// treated as spent (false, with the reason), so nothing is pushed that a
// restart could repeat.
func (c *Counts) Take(id string) (bool, error) {
	if c == nil {
		return false, errors.New("no push counts")
	}
	p := c.path(id)
	n := 0
	switch b, err := os.ReadFile(p); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return false, err
	default:
		n, err = strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || n < 0 {
			return false, fmt.Errorf("unreadable push count %q", b)
		}
	}
	if n >= MaxPushes {
		return false, nil
	}
	tmp, err := os.CreateTemp(c.dir, ".tmp-*")
	if err != nil {
		return false, err
	}
	_, werr := tmp.WriteString(strconv.Itoa(n + 1))
	cerr := tmp.Close()
	if err := errors.Join(werr, cerr); err != nil {
		_ = os.Remove(tmp.Name())
		return false, err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		_ = os.Remove(tmp.Name())
		return false, err
	}
	return true, nil
}

// Forget drops the counts of acked messages.
func (c *Counts) Forget(ids ...string) {
	if c == nil {
		return
	}
	for _, id := range ids {
		_ = os.Remove(c.path(id))
	}
}
