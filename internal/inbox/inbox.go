// Package inbox writes a user message into a running Claude Code session
// through the session's inbox: the Unix socket Claude Code hands its MCP
// servers as CLAUDE_CODE_MESSAGING_SOCKET, with CLAUDE_CODE_MESSAGING_TOKEN
// as the auth line (sock-integrations ADR 4).
package inbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"
)

// Inbox is one Claude Code session's inbox.
type Inbox struct {
	Socket string
	Token  string
}

// FromEnv is the inbox Claude Code gave this process, or false outside a
// Claude Code session.
func FromEnv(getenv func(string) string) (Inbox, bool) {
	b := Inbox{Socket: getenv("CLAUDE_CODE_MESSAGING_SOCKET"), Token: getenv("CLAUDE_CODE_MESSAGING_TOKEN")}
	return b, b.Socket != ""
}

// timeout bounds one Send: Claude Code closes a connection that sends no
// complete line soon after connecting, so both lines go in one write.
const timeout = 5 * time.Second

// Send queues text as a user message in the session. A nil error means the
// inbox took both lines. reply is whatever the session wrote back, up to 4
// KiB: what Claude Code answers is not documented, so the caller logs it.
func (b Inbox) Send(ctx context.Context, text string) (reply string, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", b.Socket)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	var frame []byte
	if b.Token != "" {
		auth, _ := json.Marshal(map[string]string{"type": "auth", "token": b.Token})
		frame = append(append(frame, auth...), '\n')
	}
	user, err := json.Marshal(map[string]any{"type": "user", "message": map[string]string{"role": "user", "content": text}})
	if err != nil {
		return "", err
	}
	frame = append(append(frame, user...), '\n')
	if _, err := conn.Write(frame); err != nil {
		return "", err
	}
	if uc, ok := conn.(*net.UnixConn); ok {
		if err := uc.CloseWrite(); err != nil {
			return "", err
		}
	}
	// Wait for the session to close its end, so the lines were read before
	// this returns. A session that keeps it open is fine once the deadline
	// passes: the write was taken.
	var got bytes.Buffer
	_, err = io.Copy(&limited{&got, 4096}, conn)
	var ne net.Error
	if err != nil && (!errors.As(err, &ne) || !ne.Timeout()) {
		return got.String(), err
	}
	return got.String(), nil
}

// limited keeps the first n bytes written and discards the rest.
type limited struct {
	b *bytes.Buffer
	n int
}

func (l *limited) Write(p []byte) (int, error) {
	if room := l.n - l.b.Len(); room > 0 {
		l.b.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}
