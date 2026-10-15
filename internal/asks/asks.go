// Package asks records which Codex session last posted in a conversation, so
// the owner's reply can be put back into that session (integrations
// docs/specs/codex-inbound.md, "Session ask"). sock-mcp and the plugin's
// notify.sh write it; sock-codex-inject reads it.
package asks

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	conversationRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	sessionRE      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// Dir is ${XDG_STATE_HOME:-~/.local/state}/sock-integrations/codex-asks. A
// relative XDG_STATE_HOME is ignored (XDG); with no home there is none.
func Dir(getenv func(string) string) (string, error) {
	state := getenv("XDG_STATE_HOME")
	if !filepath.IsAbs(state) {
		state = filepath.Join(getenv("HOME"), ".local", "state")
	}
	if !filepath.IsAbs(state) {
		return "", errors.New("asks: no home directory (set HOME or XDG_STATE_HOME)")
	}
	return filepath.Join(state, "sock-integrations", "codex-asks"), nil
}

// Record notes that session posted in conversation at at: one line
// "<session> <unix seconds>" in the file named by the conversation id. An ask
// already recorded later is kept.
func Record(dir, conversation, session string, at time.Time) error {
	if !conversationRE.MatchString(conversation) || !sessionRE.MatchString(session) {
		return errors.New("asks: conversation must be a lowercase UUID and session 1 to 64 of A-Z a-z 0-9 _ -")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	p := filepath.Join(dir, conversation)
	if b, err := os.ReadFile(p); err == nil {
		if f := strings.Fields(string(b)); len(f) == 2 {
			if prev, err := strconv.ParseInt(f[1], 10, 64); err == nil && prev > at.Unix() {
				return nil
			}
		}
	}
	tmp, err := os.CreateTemp(dir, ".ask-*")
	if err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := fmt.Fprintf(tmp, "%s %d\n", session, at.Unix()); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("asks: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	return nil
}
