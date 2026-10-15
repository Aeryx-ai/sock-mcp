package codex

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/guygrigsby/sock-mcp/internal/asks"
	"github.com/guygrigsby/sock-mcp/internal/codex/codextest"
)

const (
	conv  = "0b6d3c2a-1f4e-4c8a-9d2b-7e5f6a1b2c3d"
	msgID = "1c7e4d3b-2a5f-4d9b-8e3c-6f7a8b9c0d1e"
)

type result struct {
	code           int
	stdout, stderr string
}

// inject runs one injection the way sock-mcp's listener does and renders the
// result as a line and an exit-like code, so cases read as before.
func inject(t *testing.T, sock, project, asksDir string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	r := Daemon{Socket: sock, Version: "test"}.Inject(ctx, Reply{Project: project, Asked: asks.Read(asksDir, conv), MessageID: msgID, Prompt: prompt})
	line := r.Outcome.String()
	if r.Session != "" {
		line += " session=" + r.Session
	}
	if r.Turn != "" {
		line += " turn=" + r.Turn
	}
	return result{code: []int{0, 3, 4, 5, 6}[r.Outcome], stdout: line + "\n", stderr: r.Reason}
}

func writeAsk(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if content != "" {
		if err := os.WriteFile(filepath.Join(dir, conv), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func projectDir(t *testing.T) string {
	t.Helper()
	p, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

const prompt = "This message is your owner's reply.\n<sock-message from=\"owner\" conversation=\"x\" id=\"y\">\nsecret-text ✓ \"quoted\"\n</sock-message>\n"

func turnStart(t *testing.T, f *codextest.Server) (threadID, text, clientID string) {
	t.Helper()
	var p struct {
		ThreadID string `json:"threadId"`
		Input    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"input"`
		ClientUserMessageID string `json:"clientUserMessageId"`
	}
	raw := f.Params("turn/start")
	if raw == nil {
		t.Fatal("no turn/start")
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Input) != 1 || p.Input[0].Type != "text" {
		t.Fatalf("input %s", raw)
	}
	return p.ThreadID, p.Input[0].Text, p.ClientUserMessageID
}

func noPromptLeak(t *testing.T, r result) {
	t.Helper()
	if strings.Contains(r.stdout+r.stderr, "secret-text") {
		t.Fatalf("prompt leaked: stdout %q stderr %q", r.stdout, r.stderr)
	}
}

func TestAcceptedPicksTheAsksSession(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{
		Threads: []codextest.Thread{
			{ID: "newest", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 300},
			{ID: "asked", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 100},
		},
		Loaded: []string{"newest", "asked"},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, "asked 1792100000\n"))
	if r.code != 0 || r.stdout != "accepted session=asked turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
	id, text, cid := turnStart(t, f)
	if id != "asked" || text != prompt || cid != msgID {
		t.Fatalf("turn/start %q %q %q", id, text, cid)
	}
	if f.Called("thread/resume") {
		t.Fatal("resumed a loaded thread")
	}
	noPromptLeak(t, r)
}

func TestNewestWhenTheAskedSessionIsGone(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{
		Threads: []codextest.Thread{
			{ID: "old", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 100},
			{ID: "new", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 200},
		},
		Loaded: []string{"old", "new"},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, "vanished 1792100000\n"))
	if r.code != 0 || r.stdout != "accepted session=new turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
}

func TestOnlyInteractiveSessionsInTheProject(t *testing.T) {
	p := gitInit(t, projectDir(t))
	sub := filepath.Join(p, "pkg", "x")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	other := projectDir(t)
	execSrc := "exec"
	f := &codextest.Server{
		Threads: []codextest.Thread{
			{ID: "ephemeral", Cwd: p, Ephemeral: true, ThreadSource: codextest.User(), UpdatedAt: 900},
			{ID: "title", Cwd: p, ThreadSource: &execSrc, UpdatedAt: 800},
			{ID: "elsewhere", Cwd: other, ThreadSource: codextest.User(), UpdatedAt: 700},
			{ID: "sibling", Cwd: p + "-other", ThreadSource: codextest.User(), UpdatedAt: 650},
			{ID: "subdir", Cwd: sub, ThreadSource: codextest.User(), UpdatedAt: 10},
		},
		Loaded: []string{"ephemeral", "title", "elsewhere", "subdir"},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, "elsewhere 1792100000\n"))
	if r.code != 0 || r.stdout != "accepted session=subdir turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
}

// Live Codex 0.163 thread/list leaves threadSource null on TUI threads (only
// thread/read fills it); the list is already limited to interactive sources.
func TestListedSessionWithoutThreadSourceCounts(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "tui", Cwd: p, UpdatedAt: 5}, {ID: "x", Cwd: "/nowhere"}}, Loaded: []string{"tui"}}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, ""))
	if r.code != 0 || r.stdout != "accepted session=tui turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
}

// A session the daemon does not have loaded is not injected into: resuming it
// would run under whatever the daemon derives at resume, not the settings the
// person started it with. The reply waits for a session to be open.
func TestNotLoadedIsNotACandidate(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "cold", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 1}, {ID: "x", Cwd: "/nowhere", ThreadSource: codextest.User()}}}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, "cold 1792100000\n"))
	if r.code != 4 || r.stdout != "no-session\n" {
		t.Fatalf("got %+v", r)
	}
	if f.Called("thread/resume") || f.Called("turn/start") {
		t.Fatalf("order %v", f.Order())
	}
}

// The project is the puppet's: a nested repository inside the project is
// another project, whatever its path says.
func TestNestedRepositoryIsAnotherProject(t *testing.T) {
	p := gitInit(t, projectDir(t))
	nested := gitInit(t, filepath.Join(p, "vendor", "n"))
	f := &codextest.Server{
		Threads: []codextest.Thread{{ID: "own", Cwd: p, UpdatedAt: 1}, {ID: "nested", Cwd: nested, UpdatedAt: 9}},
		Loaded:  []string{"own", "nested"},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, "nested 1792100000\n"))
	if r.code != 0 || r.stdout != "accepted session=own turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
}

// A linked worktree outside the repository speaks as the repository's
// puppet, so its session is the project's.
func TestWorktreeOutsideIsTheSameProject(t *testing.T) {
	p := gitInit(t, projectDir(t))
	git(t, p, "commit", "-q", "--allow-empty", "-m", "x")
	wt := filepath.Join(projectDir(t), "wt")
	git(t, p, "worktree", "add", "-q", wt)
	f := &codextest.Server{
		Threads: []codextest.Thread{{ID: "main", Cwd: p, UpdatedAt: 1}, {ID: "tree", Cwd: wt, UpdatedAt: 2}},
		Loaded:  []string{"main", "tree"},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, ""))
	if r.code != 0 || r.stdout != "accepted session=tree turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
}

// A retry that would choose another session still finds the earlier delivery.
func TestDuplicateInAnotherSessionOfTheProject(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{
		Threads:   []codextest.Thread{{ID: "first", Cwd: p, UpdatedAt: 1}, {ID: "second", Cwd: p, UpdatedAt: 2}},
		Loaded:    []string{"first", "second"},
		ClientIDs: map[string][]string{"first": {msgID}},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, ""))
	if r.code != 3 || r.stdout != "duplicate session=first\n" {
		t.Fatalf("got %+v", r)
	}
	if f.Called("turn/start") {
		t.Fatal("injected a duplicate")
	}
}

// thread/list leaves out a loaded thread with no turn yet (a TUI just
// opened); thread/read of each loaded thread does not.
func TestLoadedThreadWithoutATurnCounts(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "fresh", Cwd: p, UpdatedAt: 1}}, Loaded: []string{"fresh"}}
	if r := inject(t, codextest.Serve(t, f), p, writeAsk(t, "")); r.code != 0 || r.stdout != "accepted session=fresh turn=turn-1\n" {
		t.Fatalf("got %+v", r)
	}
	if f.Called("thread/list") {
		t.Fatal("searched thread/list, which misses threads with no turn")
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.email=t@t", "-c", "user.name=t"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
}

func gitInit(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "init", "-q")
	return dir
}

func TestDuplicateClientIDIsNotInjectedAgain(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{
		Threads:   []codextest.Thread{{ID: "s", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 1}, {ID: "x", Cwd: "/nowhere"}},
		Loaded:    []string{"s"},
		ClientIDs: map[string][]string{"s": {"other", msgID}},
	}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, ""))
	if r.code != 3 || r.stdout != "duplicate session=s\n" {
		t.Fatalf("got %+v", r)
	}
	if f.Called("turn/start") {
		t.Fatal("injected a duplicate")
	}
}

func TestTurnStartErrorIsRefused(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "s", Cwd: p, ThreadSource: codextest.User(), UpdatedAt: 1}, {ID: "x", Cwd: "/nowhere"}}, Loaded: []string{"s"}, TurnErr: true}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, ""))
	if r.code != 5 || r.stdout != "refused session=s\n" {
		t.Fatalf("got %+v", r)
	}
	noPromptLeak(t, r)
}

func TestMissingSocketIsDaemonUnavailable(t *testing.T) {
	r := inject(t, filepath.Join(t.TempDir(), "absent.sock"), projectDir(t), writeAsk(t, ""))
	if r.code != 6 || r.stdout != "daemon-unavailable\n" {
		t.Fatalf("got %+v", r)
	}
}

func TestNoCandidatesIsNoSession(t *testing.T) {
	p := projectDir(t)
	f := &codextest.Server{Threads: []codextest.Thread{{ID: "a", Cwd: "/elsewhere", ThreadSource: codextest.User()}, {ID: "b", Cwd: "/else", ThreadSource: codextest.User()}}}
	r := inject(t, codextest.Serve(t, f), p, writeAsk(t, ""))
	if r.code != 4 || r.stdout != "no-session\n" {
		t.Fatalf("got %+v", r)
	}
	if f.Called("turn/start") {
		t.Fatal("injected with no session")
	}
}

// Watch tells the listener to look again at start, on every new thread and
// when a thread goes idle (a turn that may have made the project's puppet).
func TestWatchFiresAtStartAndOnThreadStarted(t *testing.T) {
	f := &codextest.Server{Notify: []string{`{"method":"thread/started","params":{"thread":{"id":"n"}}}`, `{"method":"thread/status/changed","params":{"threadId":"n","status":{"type":"active"}}}`, `{"method":"thread/status/changed","params":{"threadId":"n","status":{"type":"idle"}}}`, `{"method":"thread/name/updated","params":{}}`}}
	sock := codextest.Serve(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fired := make(chan struct{}, 4)
	go func() { _ = Daemon{Socket: sock}.Watch(ctx, func() { fired <- struct{}{} }) }()
	for range 3 {
		select {
		case <-fired:
		case <-ctx.Done():
			t.Fatal("Watch did not fire at start, on thread/started and on idle")
		}
	}
	select {
	case <-fired:
		t.Fatal("fired on a notification that is neither a new thread nor an idle one")
	case <-time.After(200 * time.Millisecond):
	}
}
