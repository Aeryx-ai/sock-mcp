// Package codex puts an owner's reply into a Codex thread through the Codex
// app-server daemon's control socket: JSON-RPC over a WebSocket on a Unix
// socket. It is the anti-corruption layer between sock-mcp and the
// app-server protocol: no Codex term leaves it except a thread's id, name and
// directory.
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
)

const (
	maxThreads = 500
	maxItems   = 2000 // user-message items scanned for a duplicate
)

var threadRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Outcome is how one injection ended.
type Outcome int

const (
	// Accepted means turn/start took the reply; acknowledge it.
	Accepted Outcome = iota
	// Duplicate means the thread already holds it; acknowledge it.
	Duplicate
	// NotLoaded means the daemon does not have the thread loaded; leave it
	// pending until the thread runs again.
	NotLoaded
	// Refused means the daemon answered with an error; leave it pending.
	Refused
	// Unavailable means there is no daemon to talk to; leave it pending.
	Unavailable
)

func (o Outcome) String() string {
	return [...]string{"accepted", "duplicate", "not-loaded", "refused", "daemon-unavailable"}[o]
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

// Thread is a loaded interactive Codex thread: one agent session.
type Thread struct {
	ID   string
	Name string // "" until Codex titles it after the first turn
	Cwd  string
}

// Result is an injection's outcome and, for Accepted, the turn. Reason carries
// ids only, for a log.
type Result struct {
	Outcome Outcome
	Turn    string
	Reason  string
}

// Threads lists the daemon's loaded interactive threads.
func (d Daemon) Threads(ctx context.Context) ([]Thread, error) {
	c, err := dial(ctx, d.Socket, d.Version)
	if err != nil {
		return nil, err
	}
	defer c.close()
	loaded, err := c.loaded(ctx)
	if err != nil {
		return nil, err
	}
	var out []Thread
	c.interactive(ctx, loaded, func(t thread) {
		name := ""
		if t.Name != nil {
			name = *t.Name
		}
		out = append(out, Thread{ID: t.ID, Name: name, Cwd: t.Cwd})
	})
	return out, nil
}

// Inject hands prompt to threadID as a user turn. A thread that already holds
// messageID (an earlier delivery got in) is Duplicate; one the daemon does not
// have loaded is NotLoaded and is not resumed, since resuming would not keep
// the settings it was started with.
func (d Daemon) Inject(ctx context.Context, threadID, messageID, prompt string) Result {
	if !threadRE.MatchString(threadID) {
		return Result{Outcome: Refused, Reason: "not a thread id"}
	}
	c, err := dial(ctx, d.Socket, d.Version)
	if err != nil {
		return Result{Outcome: Unavailable, Reason: err.Error()}
	}
	defer c.close()
	loaded, err := c.loaded(ctx)
	if err != nil {
		return Result{Outcome: Refused, Reason: "thread/loaded/list: " + err.Error()}
	}
	if !loaded[threadID] {
		return Result{Outcome: NotLoaded}
	}
	dup, err := c.hasClientID(ctx, threadID, messageID)
	if err != nil {
		return Result{Outcome: Refused, Reason: "thread/items/list: " + err.Error()}
	}
	if dup {
		return Result{Outcome: Duplicate}
	}
	var started struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	err = c.call(ctx, "turn/start", map[string]any{
		"threadId":            threadID,
		"input":               []map[string]string{{"type": "text", "text": prompt}},
		"clientUserMessageId": messageID,
	}, &started)
	if err != nil {
		return Result{Outcome: Refused, Reason: "turn/start: " + err.Error()}
	}
	if started.Turn.ID == "" {
		return Result{Outcome: Refused, Reason: "turn/start returned no turn"}
	}
	return Result{Outcome: Accepted, Turn: started.Turn.ID}
}

// Watch calls changed once connected and again each time a thread starts, goes
// idle (a turn ended, which may have made its puppet), is renamed or closes,
// until ctx ends (nil) or the connection drops (the error). The daemon sends
// these to every connected client.
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
		switch n.Method {
		case "thread/started", "thread/name/updated", "thread/closed", "thread/archived":
			changed()
		case "thread/status/changed":
			if n.Params.Status.Type == "idle" || n.Params.Status.Type == "notLoaded" {
				changed()
			}
		}
	}
}

// thread is a Codex thread as thread/read returns it.
type thread struct {
	ID           string  `json:"id"`
	Name         *string `json:"name"`
	Cwd          string  `json:"cwd"`
	Ephemeral    bool    `json:"ephemeral"`
	ThreadSource *string `json:"threadSource"`
	Source       string  `json:"source"`
}

// interactiveSources are the sources of threads a person types into: the TUI
// and the Codex app both report vscode, the CLI cli.
var interactiveSources = []string{"cli", "vscode"}

// interactive calls fn for each loaded interactive thread: not ephemeral
// (title generation), started by a person and no other thread source. Each
// loaded thread is read on its own: thread/list leaves out a thread with no
// turn yet.
func (c *rpc) interactive(ctx context.Context, loaded map[string]bool, fn func(thread)) {
	n := 0
	for id := range loaded {
		if n++; n > maxThreads {
			return
		}
		if !threadRE.MatchString(id) {
			continue
		}
		var r struct {
			Thread thread `json:"thread"`
		}
		if err := c.call(ctx, "thread/read", map[string]any{"threadId": id}, &r); err != nil {
			continue // gone since the list; the rest still count
		}
		t := r.Thread
		if t.ID != id || t.Ephemeral || (t.ThreadSource != nil && *t.ThreadSource != "user") || (t.Source != "" && !slices.Contains(interactiveSources, t.Source)) {
			continue
		}
		fn(t)
	}
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
	if err := c.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "sock-mcp", "version": version}}, nil); err != nil {
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
		if page.NextCursor == nil || *page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return out, nil
}

// hasClientID reports whether the thread already holds a user message whose
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
		if page.NextCursor == nil || *page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return false, nil
}
