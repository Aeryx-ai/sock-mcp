// Package replies records that a reply was posted, so a headless runner can
// tell an agent that answered from one that exited 0 without sending
// anything. The hub has no call that reads a conversation back, so the record
// is made where the post happens: sock-mcp's sock_send.
package replies

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// keep is how long a record lives: the hub keeps a message 30 days.
const keep = 31 * 24 * time.Hour

// Dir is ${XDG_STATE_HOME:-~/.local/state}/sock-mcp/replies. A relative
// XDG_STATE_HOME is ignored (XDG), and with no home there is no directory:
// never one under the working directory.
func Dir(getenv func(string) string) (string, error) {
	state := getenv("XDG_STATE_HOME")
	if !filepath.IsAbs(state) {
		home := getenv("HOME")
		if home == "" {
			home = getenv("USERPROFILE")
		}
		state = filepath.Join(home, ".local", "state")
	}
	if !filepath.IsAbs(state) {
		return "", errors.New("replies: no home directory to keep reply records in (set HOME or XDG_STATE_HOME)")
	}
	return filepath.Join(state, "sock-mcp", "replies"), nil
}

// name is a conversation's file: a hash, so an id never chooses a path.
func name(conversationID string) string {
	sum := sha256.Sum256([]byte(conversationID))
	return hex.EncodeToString(sum[:])
}

// Record notes that a reply was posted in conversationID at at. It makes dir
// private and prunes records older than keep.
func Record(dir, conversationID string, at time.Time) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("replies: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("replies: %w", err)
	}
	if ents, err := os.ReadDir(dir); err == nil {
		for _, e := range ents {
			if info, err := e.Info(); err == nil && info.ModTime().Before(at.Add(-keep)) {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
	p := filepath.Join(dir, name(conversationID))
	if info, err := os.Stat(p); err == nil && info.ModTime().After(at) {
		return nil // a newer reply is already recorded
	}
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		return fmt.Errorf("replies: %w", err)
	}
	if err := os.Chmod(p, 0o600); err != nil {
		return fmt.Errorf("replies: %w", err)
	}
	if err := os.Chtimes(p, at, at); err != nil {
		return fmt.Errorf("replies: %w", err)
	}
	return nil
}

// Since reports whether a reply was recorded in conversationID at or after
// since.
func Since(dir, conversationID string, since time.Time) (bool, error) {
	info, err := os.Stat(filepath.Join(dir, name(conversationID)))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("replies: %w", err)
	}
	return !info.ModTime().Before(since), nil
}
