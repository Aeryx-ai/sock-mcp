package sock

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	client "github.com/guygrigsby/sock-client"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfig(t *testing.T) {
	c, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc\n"}))
	if err != nil || c.Token != "sock_pt_abc" || c.URL != "https://api.sock.aeryx.ai" {
		t.Fatalf("got %+v, %v", c, err)
	}
	c, err = LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc", "SOCK_URL": "http://127.0.0.1:8080"}))
	if err != nil || c.URL != "http://127.0.0.1:8080" {
		t.Fatalf("got %+v, %v", c, err)
	}
	for name, e := range map[string]map[string]string{
		"no token":    {},
		"remote http": {"SOCK_TOKEN": "sock_pt_abc", "SOCK_URL": "http://example.com"},
	} {
		if _, err := LoadConfig(env(e)); err == nil {
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
	c, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc", "SOCK_ACCESS_GATE": " gate-1\n"}))
	if err != nil || c.AccessGate != "gate-1" {
		t.Fatalf("got %+v, %v", c, err)
	}
	if _, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc", "SOCK_ACCESS_GATE": "a b"})); err == nil {
		t.Fatal("a gate with a space was accepted")
	}
	if c, err := LoadConfig(env(map[string]string{"SOCK_TOKEN": "sock_pt_abc"})); err != nil || c.AccessGate != "" {
		t.Fatalf("no gate set: %+v, %v", c, err)
	}
}
