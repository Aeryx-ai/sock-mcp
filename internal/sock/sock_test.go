package sock

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	client "github.com/guygrigsby/sock-client"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfig(t *testing.T) {
	c, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc\n"}), nil)
	if err != nil || c.Token != "sock_pt_abc" || c.URL != "https://api.sock.aeryx.ai" {
		t.Fatalf("got %+v, %v", c, err)
	}
	c, err = LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc", "SOCK_URL": "http://127.0.0.1:8080"}), nil)
	if err != nil || c.URL != "http://127.0.0.1:8080" {
		t.Fatalf("got %+v, %v", c, err)
	}
	for name, e := range map[string]map[string]string{
		"no token":    {},
		"remote http": {"SOCK_TOKEN": "sock_pt_abc", "SOCK_URL": "http://example.com"},
	} {
		if _, err := LoadConfig(env(e), nil); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}
}

func TestCallClassifies(t *testing.T) {
	c := New(client.Config{URL: "https://x.example", Token: "sock_pt_t"}, "test")
	c.Retrier.Sleep = func(context.Context, time.Duration) error { return nil }
	_, err := Call(t.Context(), c, func(context.Context) (int, error) { return 0, connect.NewError(connect.CodeUnavailable, nil) })
	if err == nil || !strings.HasPrefix(err.Error(), "transient: ") {
		t.Fatalf("err = %v", err)
	}
	_, err = Call(t.Context(), c, func(context.Context) (int, error) { return 0, connect.NewError(connect.CodeNotFound, nil) })
	if err == nil || !strings.HasPrefix(err.Error(), "permanent: ") {
		t.Fatalf("err = %v", err)
	}
}

// the staging access gate is read like the token and goes to the configured
// host only (sock-client's transport)
func TestLoadConfigReadsTheAccessGate(t *testing.T) {
	c, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc", "SOCK_ACCESS_GATE": " gate-1\n"}), nil)
	if err != nil || c.AccessGate != "gate-1" {
		t.Fatalf("got %+v, %v", c, err)
	}
	if _, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc", "SOCK_ACCESS_GATE": "a b"}), nil); err == nil {
		t.Fatal("a gate with a space was accepted")
	}
	if c, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc"}), nil); err != nil || c.AccessGate != "" {
		t.Fatalf("no gate set: %+v, %v", c, err)
	}
}

// A host that starts the server without the environment (a Codex plugin) gets
// its configuration from the CLI's file.
func writeCLIConfig(t *testing.T, mode os.FileMode, body string) func(string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "sock", "config.toml")
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return func(k string) string { return map[string]string{"XDG_CONFIG_HOME": dir}[k] }
}

func withEnv(base func(string) string, extra map[string]string) func(string) string {
	return func(k string) string {
		if v, ok := extra[k]; ok {
			return v
		}
		return base(k)
	}
}

func TestLoadConfigFallsBackToTheCLIFile(t *testing.T) {
	g := writeCLIConfig(t, 0o600, "url = \"https://file.example\"\ntoken = \"sock_pt_file\"\naccess_gate = \"gate-f\"\n")
	c, err := LoadConfig(g, nil)
	if err != nil || c.Token != "sock_pt_file" || c.URL != "https://file.example" || c.AccessGate != "gate-f" {
		t.Fatalf("got %+v, %v", c, err)
	}
}

func TestLoadConfigFileWithoutURLUsesTheDefault(t *testing.T) {
	g := writeCLIConfig(t, 0o600, "token = \"sock_pt_file\"\n")
	if c, err := LoadConfig(g, nil); err != nil || c.URL != client.DefaultURL {
		t.Fatalf("got %+v, %v", c, err)
	}
}

// the token and the URL it is sent to come from one source
func TestLoadConfigEnvTokenIgnoresTheFile(t *testing.T) {
	g := writeCLIConfig(t, 0o600, "url = \"https://file.example\"\ntoken = \"sock_pt_file\"\naccess_gate = \"gate-f\"\n")
	c, err := LoadConfig(withEnv(g, map[string]string{"SOCK_TOKEN": "sock_pt_env"}), nil)
	if err != nil || c.Token != "sock_pt_env" || c.URL != client.DefaultURL || c.AccessGate != "" {
		t.Fatalf("got %+v, %v: the file's url or gate leaked into an env token", c, err)
	}
}

func TestLoadConfigFileTokenNeverGoesToAnAmbientURL(t *testing.T) {
	g := writeCLIConfig(t, 0o600, "url = \"https://file.example\"\ntoken = \"sock_pt_file\"\naccess_gate = \"gate-f\"\n")
	var logged []string
	logf := func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) }
	c, err := LoadConfig(withEnv(g, map[string]string{"SOCK_URL": "https://evil.example", "SOCK_ACCESS_GATE": "gate-env"}), logf)
	if err != nil || c.URL != "https://file.example" || c.AccessGate != "gate-f" {
		t.Fatalf("got %+v, %v", c, err)
	}
	if len(logged) != 1 || !strings.Contains(logged[0], "SOCK_URL") || strings.Contains(logged[0], "sock_pt_file") || strings.Contains(logged[0], "evil.example") {
		t.Fatalf("log %q", logged)
	}
}

func TestLoadConfigRefusesAReadableFileWithAToken(t *testing.T) {
	g := writeCLIConfig(t, 0o644, "token = \"sock_pt_file\"\n")
	if _, err := LoadConfig(g, nil); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadConfigNoTokenAnywhereKeepsTheOldError(t *testing.T) {
	g := writeCLIConfig(t, 0o600, "url = \"https://file.example\"\n")
	_, err := LoadConfig(g, nil)
	if err == nil || !strings.Contains(err.Error(), "no token: set SOCK_TOKEN") {
		t.Fatalf("err = %v", err)
	}
}
