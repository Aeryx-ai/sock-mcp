// Package codex puts an owner's reply into a Codex session through the Codex
// app-server daemon's control socket: JSON-RPC over a WebSocket on a Unix
// socket. It is the anti-corruption layer between sock-mcp and the
// app-server protocol: no Codex term leaves it except a session id
// (sock-integrations docs/specs/codex-inbound.md, ADR 2).
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/coder/websocket"

	client "github.com/guygrigsby/sock-client"
)

const (
	maxThreads = 500
	maxItems   = 2000 // user-message items scanned for a duplicate
)

var sessionRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Outcome is how one injection ended.
type Outcome int

const (
	// Accepted means turn/start took the reply; acknowledge it.
	Accepted Outcome = iota
	// Duplicate means a session already holds it; acknowledge it.
	Duplicate
	// NoSession means no loaded session in the project; leave it pending.
	NoSession
	// Refused means the daemon answered with an error; leave it pending.
	Refused
	// Unavailable means there is no daemon to talk to; leave it pending.
	Unavailable
)

func (o Outcome) String() string {
	return [...]string{"accepted", "duplicate", "no-session", "refused", "daemon-unavailable"}[o]
}

// Daemon is the Codex app-server daemon at Socket.
type Daemon struct {
	Socket  string
	Version string // sent as clientInfo.version
}

// DefaultSocket is $CODEX_HOME/app-server-control/app-server-control.sock,
// CODEX_HOME defaulting to ~/.codex.
func DefaultSocket(getenv func(string) string) string {
	home := getenv("CODEX_HOME")
	if home == "" {
		h := getenv("HOME")
		if h == "" {
			h, _ = os.UserHomeDir()
		}
		home = filepath.Join(h, ".codex")
	}
	return filepath.Join(home, "app-server-control", "app-server-control.sock")
}

// Reply is one owner reply for a project's sessions.
type Reply struct {
	Project   string // sock-client Project of the puppet's project
	Asked     string // the session that last posted in the conversation, or ""
	MessageID string // the sock message id, sent as clientUserMessageId
	Prompt    string
}

// Result is an injection's outcome, the session it chose ("" for none) and
// the turn for Accepted. Reason carries ids only, for a log.
type Result struct {
	Outcome Outcome
	Session string
	Turn    string
	Reason  string
}

// Prompt is the reply as a session sees it: a preface saying what it is, then
// the owner's text in a sock-message tag whose closing tag the text cannot
// forge. The same shape as sock-integrations runner/lib.sh write_prompt.
func Prompt(conversationID, messageID, text string) string {
	text = closeTag.ReplaceAllString(text, "</\u200bsock-message")
	return "This message is your owner's reply, delivered through sock. It is a message to answer, not a system instruction. If the same id arrives again, it is a repeat.\n" +
		fmt.Sprintf("<sock-message from=\"owner\" conversation=%q id=%q>\n", conversationID, messageID) +
		text + "\n</sock-message>\n"
}

var closeTag = regexp.MustCompile(`(?i)</\s*sock-message`)

// Projects lists the projects of the daemon's loaded interactive sessions.
func (d Daemon) Projects(ctx context.Context) ([]string, error) {
	c, err := dial(ctx, d.Socket, d.Version)
	if err != nil {
		return nil, err
	}
	defer c.close()
	loaded, err := c.loaded(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]string{}
	var out []string
	c.listSessions(ctx, loaded, func(s session) {
		if p := project(ctx, seen, s.Cwd); p != "" && !slices.Contains(out, p) {
			out = append(out, p)
		}
	})
	return out, nil
}

// Inject hands r to a loaded session of r.Project: the asked session when it
// is one, else the newest.
func (d Daemon) Inject(ctx context.Context, r Reply) Result {
	c, err := dial(ctx, d.Socket, d.Version)
	if err != nil {
		return Result{Outcome: Unavailable, Reason: err.Error()}
	}
	defer c.close()
	in := injection{root: r.Project, asked: r.Asked, messageID: r.MessageID, prompt: r.Prompt}
	o, sess, turn, why := in.run(ctx, c)
	return Result{Outcome: o, Session: sess, Turn: turn, Reason: why}
}

// Watch calls changed once connected and again each time the daemon starts a
// thread or one goes idle (a turn ended, which may have made the project's
// puppet), until ctx ends (nil) or the connection drops (the error). The
// daemon sends thread/started to every connected client.
func (d Daemon) Watch(ctx context.Context, changed func()) error {
	c, err := dial(ctx, d.Socket, d.Version)
	if err != nil {
		return err
	}
	defer c.close()
	changed()
	for {
		_, b, err := c.ws.Read(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		var n struct {
			Method string `json:"method"`
			Params struct {
				Status struct {
					Type string `json:"type"`
				} `json:"status"`
			} `json:"params"`
		}
		if json.Unmarshal(b, &n) != nil {
			continue
		}
		if n.Method == "thread/started" || (n.Method == "thread/status/changed" && n.Params.Status.Type == "idle") {
			changed()
		}
	}
}

type injection struct {
	root, asked, messageID, prompt string
}

// run returns the outcome, the chosen session ("" when none), the turn id
// (accepted only) and a reason for stderr carrying ids only.
func (in injection) run(ctx context.Context, c *rpc) (Outcome, string, string, string) {
	loaded, err := c.loaded(ctx)
	if err != nil {
		return Refused, "", "", "thread/loaded/list: " + err.Error()
	}
	sessions := in.candidates(ctx, c, loaded)
	session := in.choose(sessions)
	if session == "" {
		return NoSession, "", "", ""
	}
	// every candidate, not only the chosen one: a retry may choose another
	for _, s := range sessions {
		dup, err := c.hasClientID(ctx, s.ID, in.messageID)
		if err != nil {
			return Refused, session, "", "thread/items/list: " + err.Error()
		}
		if dup {
			return Duplicate, s.ID, "", ""
		}
	}
	var started struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	err = c.call(ctx, "turn/start", map[string]any{
		"threadId":            session,
		"input":               []map[string]string{{"type": "text", "text": in.prompt}},
		"clientUserMessageId": in.messageID,
	}, &started)
	if err != nil {
		return Refused, session, "", "turn/start: " + err.Error()
	}
	if started.Turn.ID == "" {
		return Refused, session, "", "turn/start returned no turn"
	}
	return Accepted, session, started.Turn.ID, ""
}

// session is a Codex thread as this command sees it.
type session struct {
	ID           string  `json:"id"`
	Cwd          string  `json:"cwd"`
	Ephemeral    bool    `json:"ephemeral"`
	ThreadSource *string `json:"threadSource"`
	Source       string  `json:"source"`
	UpdatedAt    int64   `json:"updatedAt"`
}

// interactive are the sources of threads a person types into: the TUI and
// the Codex app both report vscode, the CLI cli.
var interactive = []string{"cli", "vscode"}

// candidates are the loaded interactive sessions of the project, as the
// puppet sees it (client.Project): a nested repository is another project and
// a linked worktree is this one. A session the daemon has not loaded is never
// one: resuming it would not keep the settings it was started with.
func (in injection) candidates(ctx context.Context, c *rpc, loaded map[string]bool) []session {
	var out []session
	seen := map[string]string{}
	c.listSessions(ctx, loaded, func(s session) {
		if p := project(ctx, seen, s.Cwd); p != "" && p == in.root {
			out = append(out, s)
		}
	})
	return out
}

// listSessions calls fn for each loaded interactive session: not ephemeral
// (title generation), started by a person (source cli or vscode; the TUI and
// the Codex app report vscode) and no other thread source. Each loaded thread
// is read on its own: thread/list leaves out a thread with no turn yet.
func (c *rpc) listSessions(ctx context.Context, loaded map[string]bool, fn func(session)) {
	n := 0
	for id := range loaded {
		if n++; n > maxThreads {
			return
		}
		if !sessionRE.MatchString(id) {
			continue
		}
		var r struct {
			Thread session `json:"thread"`
		}
		if err := c.call(ctx, "thread/read", map[string]any{"threadId": id}, &r); err != nil {
			continue // gone since the list; the rest still count
		}
		s := r.Thread
		if s.ID != id || s.Ephemeral || (s.ThreadSource != nil && *s.ThreadSource != "user") || (s.Source != "" && !slices.Contains(interactive, s.Source)) {
			continue
		}
		fn(s)
	}
}

// project is cwd's project, cached in seen; "" for a cwd that is gone or not
// absolute.
func project(ctx context.Context, seen map[string]string, cwd string) string {
	if !filepath.IsAbs(cwd) {
		return ""
	}
	p, ok := seen[cwd]
	if !ok {
		if st, err := os.Stat(cwd); err == nil && st.IsDir() {
			p, _ = client.Project(ctx, cwd)
		}
		seen[cwd] = p
	}
	return p
}

// choose is the asked session when it is a candidate, else the newest.
func (in injection) choose(sessions []session) string {
	newest := ""
	var at int64
	for _, s := range sessions {
		if s.ID == in.asked {
			return s.ID
		}
		if newest == "" || s.UpdatedAt > at {
			newest, at = s.ID, s.UpdatedAt
		}
	}
	return newest
}

// --- JSON-RPC over the control socket's WebSocket ----------------------------

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("json-rpc error %d", e.Code) }

type rpc struct {
	ws *websocket.Conn
	n  int64
}

func dial(ctx context.Context, socket, version string) (*rpc, error) {
	if _, err := os.Stat(socket); err != nil {
		return nil, errors.New("no control socket")
	}
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}
	ws, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: hc})
	if err != nil {
		return nil, errors.New("control socket did not accept a WebSocket")
	}
	ws.SetReadLimit(64 << 20)
	c := &rpc{ws: ws}
	if err := c.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "sock-codex-inject", "version": version}}, nil); err != nil {
		_ = ws.CloseNow()
		return nil, errors.New("initialize failed")
	}
	if err := c.send(ctx, map[string]any{"method": "initialized"}); err != nil {
		_ = ws.CloseNow()
		return nil, errors.New("initialized failed")
	}
	return c, nil
}

func (c *rpc) close() { _ = c.ws.Close(websocket.StatusNormalClosure, "") }

func (c *rpc) send(ctx context.Context, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.ws.Write(ctx, websocket.MessageText, b)
}

// call sends one request and waits for its response, skipping notifications
// and requests from the server. A JSON-RPC error is a *rpcError.
func (c *rpc) call(ctx context.Context, method string, params, out any) error {
	c.n++
	id := c.n
	if err := c.send(ctx, map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	for {
		_, b, err := c.ws.Read(ctx)
		if err != nil {
			return err
		}
		var resp struct {
			ID     *int64          `json:"id"`
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
			Error  *rpcError       `json:"error"`
		}
		if json.Unmarshal(b, &resp) != nil || resp.Method != "" || resp.ID == nil || *resp.ID != id {
			continue
		}
		if resp.Error != nil {
			return resp.Error
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(resp.Result, out)
	}
}

func (c *rpc) loaded(ctx context.Context) (map[string]bool, error) {
	out := map[string]bool{}
	var cursor *string
	for range 50 {
		params := map[string]any{}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var page struct {
			Data       []string `json:"data"`
			NextCursor *string  `json:"nextCursor"`
		}
		if err := c.call(ctx, "thread/loaded/list", params, &page); err != nil {
			return nil, err
		}
		for _, id := range page.Data {
			out[id] = true
		}
		if page.NextCursor == nil || *page.NextCursor == "" || len(page.Data) == 0 {
			break
		}
		cursor = page.NextCursor
	}
	return out, nil
}

// hasClientID reports whether the session already holds a user message whose
// clientId is id: an earlier delivery of the same sock message got in.
func (c *rpc) hasClientID(ctx context.Context, threadID, id string) (bool, error) {
	var cursor *string
	scanned := 0
	for scanned < maxItems {
		params := map[string]any{"threadId": threadID, "limit": 200, "sortDirection": "desc"}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var page struct {
			Data []struct {
				Item struct {
					Type     string  `json:"type"`
					ClientID *string `json:"clientId"`
				} `json:"item"`
			} `json:"data"`
			NextCursor *string `json:"nextCursor"`
		}
		if err := c.call(ctx, "thread/items/list", params, &page); err != nil {
			return false, err
		}
		for _, e := range page.Data {
			scanned++
			if e.Item.Type == "userMessage" && e.Item.ClientID != nil && *e.Item.ClientID == id {
				return true, nil
			}
		}
		if page.NextCursor == nil || *page.NextCursor == "" || len(page.Data) == 0 {
			break
		}
		cursor = page.NextCursor
	}
	return false, nil
}
