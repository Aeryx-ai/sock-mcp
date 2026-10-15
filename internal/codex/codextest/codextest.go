// Package codextest is a fake Codex app-server control socket for tests.
package codextest

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/coder/websocket"
)

// Thread is a thread as thread/list returns it.
type Thread struct {
	ID           string  `json:"id"`
	Cwd          string  `json:"cwd"`
	Ephemeral    bool    `json:"ephemeral"`
	ThreadSource *string `json:"threadSource"`
	UpdatedAt    int64   `json:"updatedAt"`
}

// User is the threadSource of a person's thread.
func User() *string { s := "user"; return &s }

// Server is the Codex app-server's control socket: JSON-RPC over a
// WebSocket upgrade on a Unix socket.
type Server struct {
	Threads   []Thread
	Loaded    []string
	ClientIDs map[string][]string // thread id -> user message clientIds
	TurnErr   bool
	// Notify are raw notifications sent to a client once it is initialized.
	Notify []string

	mu      sync.Mutex
	methods []string
	params  map[string]json.RawMessage // last params per method
	starts  []json.RawMessage          // every turn/start's params
}

func (f *Server) handle(method string, params json.RawMessage) (any, *RPCError) {
	f.mu.Lock()
	f.methods = append(f.methods, method)
	f.params[method] = params
	f.mu.Unlock()
	switch method {
	case "initialize":
		return map[string]any{"userAgent": "fake"}, nil
	case "thread/list":
		var p struct {
			Cursor *string `json:"cursor"`
		}
		_ = json.Unmarshal(params, &p)
		// two pages, to prove pagination
		half := len(f.Threads) / 2
		if p.Cursor == nil {
			return map[string]any{"data": f.Threads[:half], "nextCursor": "page2"}, nil
		}
		return map[string]any{"data": f.Threads[half:], "nextCursor": nil}, nil
	case "thread/loaded/list":
		return map[string]any{"data": f.Loaded, "nextCursor": nil}, nil
	case "thread/resume":
		return map[string]any{"thread": map[string]any{}}, nil
	case "thread/items/list":
		var p struct {
			ThreadID string `json:"threadId"`
		}
		_ = json.Unmarshal(params, &p)
		var data []any
		data = append(data, map[string]any{"turnId": "t0", "item": map[string]any{"type": "agentMessage", "id": "a1", "text": "hi"}})
		for _, id := range f.ClientIDs[p.ThreadID] {
			data = append(data, map[string]any{"turnId": "t0", "item": map[string]any{"type": "userMessage", "id": "u-" + id, "clientId": id, "content": []any{}}})
		}
		return map[string]any{"data": data, "nextCursor": nil}, nil
	case "turn/start":
		f.mu.Lock()
		f.starts = append(f.starts, params)
		f.mu.Unlock()
		if f.TurnErr {
			return nil, &RPCError{Code: -32600, Message: "no"}
		}
		return map[string]any{"turn": map[string]any{"id": "turn-1", "status": "inProgress", "items": []any{}}}, nil
	}
	return nil, &RPCError{Code: -32601, Message: "method not found"}
}

// Called reports whether method was called.
func (f *Server) Called(method string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.methods {
		if m == method {
			return true
		}
	}
	return false
}

// Order is every method called, in order.
func (f *Server) Order() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.methods...)
}

// Serve starts f on a short Unix socket path and returns it.
func Serve(t *testing.T, f *Server) string {
	t.Helper()
	f.params = map[string]json.RawMessage{}
	dir, err := os.MkdirTemp("/tmp", "ci")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		c.SetReadLimit(4 << 20)
		ctx := r.Context()
		// a notification first, which the client must skip
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"method":"thread/status/changed","params":{}}`))
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				return
			}
			var req struct {
				ID     *int64          `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if json.Unmarshal(b, &req) != nil {
				continue
			}
			if req.ID == nil {
				if req.Method == "initialized" {
					for _, n := range f.Notify {
						_ = c.Write(ctx, websocket.MessageText, []byte(n))
					}
				}
				continue
			}
			res, rerr := f.handle(req.Method, req.Params)
			out := map[string]any{"id": *req.ID}
			if rerr != nil {
				out["error"] = rerr
			} else {
				out["result"] = res
			}
			b, _ = json.Marshal(out)
			if c.Write(ctx, websocket.MessageText, b) != nil {
				return
			}
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

// Params is the last params of method.
func (f *Server) Params(method string) json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.params[method]
}

// TurnStarts is every turn/start's params, in order.
func (f *Server) TurnStarts() []json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]json.RawMessage(nil), f.starts...)
}

// RPCError is a JSON-RPC error object.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
