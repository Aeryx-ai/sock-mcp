package inbox

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// listen serves a fake inbox and returns its path and the lines it read.
func listen(t *testing.T) (string, <-chan []string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ib") // a Unix socket path is at most 104 bytes on macOS
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	got := make(chan []string, 4)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			var lines []string
			sc := bufio.NewScanner(c)
			for sc.Scan() {
				lines = append(lines, sc.Text())
			}
			_ = c.Close()
			got <- lines
		}
	}()
	return path, got
}

func TestSendWritesTheAuthThenTheUserLine(t *testing.T) {
	path, got := listen(t)
	if err := (Inbox{Socket: path, Token: "tok"}).Send(context.Background(), "hello \"there\"\nline two"); err != nil {
		t.Fatal(err)
	}
	lines := <-got
	if len(lines) != 2 || lines[0] != `{"token":"tok","type":"auth"}` {
		t.Fatalf("lines %q", lines)
	}
	var u struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &u); err != nil {
		t.Fatal(err)
	}
	if u.Type != "user" || u.Message.Role != "user" || u.Message.Content != "hello \"there\"\nline two" {
		t.Fatalf("user line %q", lines[1])
	}
}

func TestSendWithoutATokenSendsNoAuthLine(t *testing.T) {
	path, got := listen(t)
	if err := (Inbox{Socket: path}).Send(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if lines := <-got; len(lines) != 1 {
		t.Fatalf("lines %q", lines)
	}
}

func TestSendToAGoneSessionFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := (Inbox{Socket: filepath.Join(t.TempDir(), "gone.sock")}).Send(ctx, "x"); err == nil {
		t.Fatal("sent to a socket nobody listens on")
	}
}

func TestFromEnv(t *testing.T) {
	env := map[string]string{"CLAUDE_CODE_MESSAGING_SOCKET": "/tmp/s", "CLAUDE_CODE_MESSAGING_TOKEN": "t"}
	if b, ok := FromEnv(func(k string) string { return env[k] }); !ok || b != (Inbox{Socket: "/tmp/s", Token: "t"}) {
		t.Fatalf("%+v %v", b, ok)
	}
	if _, ok := FromEnv(func(string) string { return "" }); ok {
		t.Fatal("an inbox outside Claude Code")
	}
}
