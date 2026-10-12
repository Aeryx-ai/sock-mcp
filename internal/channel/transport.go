// Package channel pushes inbound sock messages into a Claude Code session as
// notifications/claude/channel events.
package channel

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Method is the Claude Code channel notification.
const Method = "notifications/claude/channel"

// Event is the channel notification payload. Meta keys must be letters,
// digits and underscores or Claude Code silently drops them.
type Event struct {
	Content string            `json:"content"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// Transport wraps an MCP transport so the server can also send the custom
// channel notification, which the SDK has no public API for.
type Transport struct {
	Inner mcp.Transport

	mu   sync.Mutex
	conn mcp.Connection
}

// Connect connects the inner transport and remembers the connection.
func (t *Transport) Connect(ctx context.Context) (mcp.Connection, error) {
	c, err := t.Inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conn = c
	t.mu.Unlock()
	return c, nil
}

// Push writes one channel event. Claude Code never acknowledges it: success
// means the message was written to the transport.
func (t *Transport) Push(ctx context.Context, ev Event) error {
	t.mu.Lock()
	c := t.conn
	t.mu.Unlock()
	if c == nil {
		return errors.New("channel: transport not connected")
	}
	params, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	// A request with no ID is a JSON-RPC notification.
	return c.Write(ctx, &jsonrpc.Request{Method: Method, Params: params})
}
