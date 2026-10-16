package codex

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/guygrigsby/sock-mcp/internal/codex/codextest"
)

const msgID = "1c7e4d3b-2a5f-4d9b-8e3c-6f7a8b9c0d1e"

const prompt = "This message is your owner's reply.\n<sock-message from=\"owner\" conversation=\"x\" id=\"y\">\nsecret-text ✓ \"quoted\"\n</sock-message>\n"

func daemon(t *testing.T, f *codextest.Server) Daemon {
	t.Helper()
	return Daemon{Socket: codextest.Serve(t, f), Version: "test"}
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestThreadsListsLoadedInteractiveThreads(t *testing.T) {
	execSrc := "exec"
	f := &codextest.Server{
		Threads: []codextest.Thread{
			{ID: "tui", Name: "Fix the parser", Source: "vscode", Cwd: "/p"},
			{ID: "fresh", Cwd: "/q"}, // no turn yet: no name, not in thread/list
			{ID: "title", Ephemeral: true, Cwd: "/p"},
			{ID: "exec", ThreadSource: &execSrc, Cwd: "/p"},
			{ID: "sub", Source: "subAgent", Cwd: "/p"},
			{ID: "cold", Cwd: "/p"},
		},
		Loaded: []string{"tui", "fresh", "title", "exec", "sub"},
	}
	got, err := daemon(t, f).Threads(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(got, func(a, b Thread) int { return strings.Compare(a.ID, b.ID) })
	want := []Thread{{ID: "fresh", Cwd: "/q"}, {ID: "tui", Name: "Fix the parser", Cwd: "/p"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %+v", got)
	}
}

func TestInjectStartsATurnInThatThread(t *testing.T) {
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "s1"}}, Loaded: []string{"s1"}}
	r := daemon(t, f).Inject(ctx(t), "s1", msgID, prompt)
	if r.Outcome != Accepted || r.Turn != "turn-1" {
		t.Fatalf("got %+v", r)
	}
	var p struct {
		ThreadID string `json:"threadId"`
		Input    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"input"`
		ClientUserMessageID string `json:"clientUserMessageId"`
	}
	if err := json.Unmarshal(f.Params("turn/start"), &p); err != nil {
		t.Fatal(err)
	}
	if p.ThreadID != "s1" || len(p.Input) != 1 || p.Input[0].Type != "text" || p.Input[0].Text != prompt || p.ClientUserMessageID != msgID {
		t.Fatalf("turn/start %s", f.Params("turn/start"))
	}
	if strings.Contains(r.Reason, "secret-text") {
		t.Fatal("prompt leaked into the reason")
	}
}

// A thread that is not loaded is never resumed: resuming would not keep the
// settings it was started with.
func TestInjectLeavesAThreadThatIsNotLoaded(t *testing.T) {
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "cold"}}}
	if r := daemon(t, f).Inject(ctx(t), "cold", msgID, prompt); r.Outcome != NotLoaded {
		t.Fatalf("got %+v", r)
	}
	if f.Called("thread/resume") || f.Called("turn/start") {
		t.Fatalf("calls %v", f.Order())
	}
}

func TestInjectFindsAnEarlierDelivery(t *testing.T) {
	f := &codextest.Server{
		Threads:   []codextest.Thread{{ID: "s1"}},
		Loaded:    []string{"s1"},
		ClientIDs: map[string][]string{"s1": {"other", msgID}},
	}
	if r := daemon(t, f).Inject(ctx(t), "s1", msgID, prompt); r.Outcome != Duplicate {
		t.Fatalf("got %+v", r)
	}
	if f.Called("turn/start") {
		t.Fatal("injected a duplicate")
	}
}

func TestInjectRefusedAndUnavailable(t *testing.T) {
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "s1"}}, Loaded: []string{"s1"}, TurnErr: true}
	if r := daemon(t, f).Inject(ctx(t), "s1", msgID, prompt); r.Outcome != Refused {
		t.Fatalf("turn/start error: %+v", r)
	}
	if r := (Daemon{Socket: "/nonexistent/s.sock"}).Inject(ctx(t), "s1", msgID, prompt); r.Outcome != Unavailable {
		t.Fatalf("no socket: %+v", r)
	}
	if r := (Daemon{Socket: "/nonexistent/s.sock"}).Inject(ctx(t), "../x", msgID, prompt); r.Outcome != Refused {
		t.Fatalf("bad thread id: %+v", r)
	}
}

// Watch tells the listener to look again at start, when a thread starts, goes
// idle or unloaded, is renamed or closes, and not otherwise.
func TestWatchFiresOnThreadChanges(t *testing.T) {
	f := &codextest.Server{Notify: []string{
		`{"method":"thread/started","params":{"thread":{"id":"n"}}}`,
		`{"method":"thread/status/changed","params":{"threadId":"n","status":{"type":"active"}}}`,
		`{"method":"thread/status/changed","params":{"threadId":"n","status":{"type":"idle"}}}`,
		`{"method":"thread/name/updated","params":{}}`,
		`{"method":"thread/status/changed","params":{"threadId":"n","status":{"type":"notLoaded"}}}`,
		`{"method":"thread/closed","params":{}}`,
		`{"method":"thread/tokenUsage/updated","params":{}}`,
	}}
	sock := codextest.Serve(t, f)
	c := ctx(t)
	fired := make(chan struct{}, 16)
	go func() { _ = Daemon{Socket: sock}.Watch(c, func() { fired <- struct{}{} }) }()
	for range 6 {
		select {
		case <-fired:
		case <-c.Done():
			t.Fatal("Watch missed a thread change")
		}
	}
	select {
	case <-fired:
		t.Fatal("fired on a notification that changes no thread's session")
	case <-time.After(200 * time.Millisecond):
	}
}
