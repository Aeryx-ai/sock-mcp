package sock

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfig(t *testing.T) {
	good := "sock_pt_abc"
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
		wantURL string
	}{
		{"default url", map[string]string{"SOCK_TOKEN": good}, "", DefaultURL},
		{"trims newline", map[string]string{"SOCK_TOKEN": good + "\n"}, "", DefaultURL},
		{"loopback http", map[string]string{"SOCK_TOKEN": good, "SOCK_URL": "http://127.0.0.1:8080"}, "", "http://127.0.0.1:8080"},
		{"no token", map[string]string{}, "no token", ""},
		{"bad prefix", map[string]string{"SOCK_TOKEN": "nope"}, "must start with", ""},
		{"inner space", map[string]string{"SOCK_TOKEN": "sock_pt_a b"}, "whitespace", ""},
		{"operator token", map[string]string{"SOCK_TOKEN": "sock_ot_1_2"}, "must start with", ""},
		{"plain http remote", map[string]string{"SOCK_TOKEN": good, "SOCK_URL": "http://example.com"}, "must use https", ""},
		{"url credentials", map[string]string{"SOCK_TOKEN": good, "SOCK_URL": "https://u:hunter2@example.com"}, "user or password", ""},
		{"scheme-less credentials", map[string]string{"SOCK_TOKEN": good, "SOCK_URL": "user:hunter2@example.com"}, "not a valid absolute", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := LoadConfig(env(tc.env))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), "hunter2") {
					t.Fatalf("error leaks credentials: %v", err)
				}
				return
			}
			if err != nil || c.URL != tc.wantURL || c.Token != good {
				t.Fatalf("got %+v, %v", c, err)
			}
		})
	}
}

func unavailable(retryAfter string) error {
	e := connect.NewError(connect.CodeUnavailable, nil)
	if retryAfter != "" {
		e.Meta().Set("Retry-After", retryAfter)
	}
	return e
}

func TestRetrierHonorsRetryAfterCappedAtMax(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	var waits []time.Duration
	r := Retrier{Attempts: 4, Base: time.Second, Max: 10 * time.Second, Now: func() time.Time { return now },
		Sleep: func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }}
	errs := []error{unavailable("3"), unavailable(now.Add(2 * time.Second).UTC().Format(http.TimeFormat)), unavailable("86400")}
	i := 0
	err := r.Do(t.Context(), func(context.Context) error {
		if i < len(errs) {
			i++
			return errs[i-1]
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{3 * time.Second, 2 * time.Second, 10 * time.Second}
	if len(waits) != 3 || waits[0] != want[0] || waits[1] != want[1] || waits[2] != want[2] {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
}

func TestRetrierBackoffAndStops(t *testing.T) {
	var waits []time.Duration
	r := Retrier{Attempts: 4, Base: time.Second, Max: 3 * time.Second,
		Sleep: func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }}
	calls := 0
	err := r.Do(t.Context(), func(context.Context) error { calls++; return unavailable("") })
	if err == nil || calls != 4 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if len(waits) != 3 || waits[0] != time.Second || waits[1] != 2*time.Second || waits[2] != 3*time.Second {
		t.Fatalf("waits = %v", waits)
	}
	calls = 0
	perm := connect.NewError(connect.CodeInvalidArgument, nil)
	if err := r.Do(t.Context(), func(context.Context) error { calls++; return perm }); !errors.Is(err, perm) || calls != 1 {
		t.Fatalf("permanent error retried: err=%v calls=%d", err, calls)
	}
}

func TestRetrierZeroValueIsSafe(t *testing.T) {
	calls := 0
	if err := (Retrier{}).Do(t.Context(), func(context.Context) error { calls++; return unavailable("") }); err == nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestTokenOnlyGoesToConfiguredHost(t *testing.T) {
	var other string
	elsewhere := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { other = r.Header.Get("Authorization") }))
	defer elsewhere.Close()
	home := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL, http.StatusTemporaryRedirect)
	}))
	defer home.Close()
	c := New(Config{URL: home.URL, Token: "sock_pt_x"}, "test")
	c.Retrier = Retrier{Attempts: 1}
	_, err := Call(t.Context(), c, func(ctx context.Context) (struct{}, error) {
		_, err := c.Messaging.ListConversations(ctx, connect.NewRequest(&sockv1.ListConversationsRequest{}))
		return struct{}{}, err
	})
	if err == nil {
		t.Fatal("redirect should surface as an error, not be followed")
	}
	if other != "" {
		t.Fatalf("token reached another host: %q", other)
	}
}
