package tools_test

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

var toolNames = []string{"sock_whoami", "sock_list_conversations", "sock_send", "sock_fetch", "sock_ack"}

// FuzzToolArgs calls each tool with arbitrary JSON arguments. A call either
// fails or returns a result; nothing panics; sock_send never reaches the
// owner unprompted under the default policy; sock_fetch asks for 1 to 200;
// sock_ack reports what it submitted.
func FuzzToolArgs(f *testing.F) {
	for i, args := range []string{
		`{}`, `{}`,
		`{"text":"hi","conversation_id":"c1"}`,
		`{"limit":1000}`,
		`{"message_ids":["m1","m2"]}`,
	} {
		f.Add(uint8(i), args)
	}
	f.Add(uint8(2), `{"text":"to the owner"}`)
	f.Add(uint8(2), `{"text":"","conversation_id":""}`)
	f.Add(uint8(3), `{"limit":-5}`)
	f.Add(uint8(3), `{"limit":1e300}`)
	f.Add(uint8(3), `{"limit":"7"}`)
	f.Add(uint8(4), `{"message_ids":null}`)
	f.Add(uint8(4), `[1,2]`)
	f.Add(uint8(0), `not json`)
	f.Fuzz(func(t *testing.T, which uint8, args string) {
		if !json.Valid([]byte(args)) {
			return // the client refuses to send it
		}
		name := toolNames[int(which)%len(toolNames)]
		fk := &fake{pending: []*sockv1.Message{msg("pending")}}
		cs := start(t, fk)
		res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: json.RawMessage(args)})
		if err == nil && res == nil {
			t.Fatal("no result and no error")
		}
		ok := err == nil && !res.IsError
		fk.mu.Lock()
		defer fk.mu.Unlock()
		if len(fk.notifies) > 0 {
			t.Fatalf("%s %s reached NotifyOwner under the default policy", name, args)
		}
		switch name {
		case "sock_fetch":
			if ok && (fk.fetchLimit < 1 || fk.fetchLimit > 200) {
				t.Fatalf("%s %s asked for %d", name, args, fk.fetchLimit)
			}
		case "sock_ack":
			if ok {
				n, _ := structuredOf(res)["acked"].(float64)
				if len(fk.acked) != 1 || int(n) != len(fk.acked[0]) {
					t.Fatalf("%s %s: acked %v, reported %v", name, args, fk.acked, n)
				}
			}
		case "sock_send":
			var in struct {
				ConversationID string `json:"conversation_id"`
			}
			_ = json.Unmarshal([]byte(args), &in)
			if ok && in.ConversationID == "" {
				t.Fatalf("%s %s succeeded without a conversation", name, args)
			}
		}
	})
}

func structuredOf(res *mcp.CallToolResult) map[string]any {
	m, _ := res.StructuredContent.(map[string]any)
	return m
}
