package tools_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	client "github.com/guygrigsby/sock-client"

	"github.com/guygrigsby/sock-mcp/sockclient"
	"github.com/guygrigsby/sock-mcp/tools"
)

var update = flag.Bool("update", false, "rewrite testdata goldens")

// The outbound surface is what sock-mcpd serves and pins, and its sessions
// never own deliveries, so it must not change with the consuming tools:
// testdata/outbound_tools.golden.json is its tools/list as of sock-mcp
// 285c1dc, byte for byte.
func TestOutboundToolsListIsPinned(t *testing.T) {
	s := mcp.NewServer(&mcp.Implementation{Name: "sock", Version: "test"}, nil)
	tools.RegisterOutbound(s, sockclient.New(client.Config{URL: "http://127.0.0.1:1", Token: token}, "test"), tools.Policy{})
	ct, st := mcp.NewInMemoryTransports()
	ss, err := s.Connect(t.Context(), st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(t.Context(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	res, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(res.Tools, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	const path = "testdata/outbound_tools.golden.json"
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("outbound tools/list changed:\n got %s\nwant %s", got, want)
	}
}

// An outbound session owns no deliveries: whatever it is sent, it acks none.
func TestOutboundSendAcksNothing(t *testing.T) {
	f := &fake{}
	cs := startSurface(t, f, tools.RegisterOutbound)
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "sock_send", Arguments: map[string]any{"text": "x", "conversation_id": "c1", "message_ids": []string{"m1"}}})
	if err == nil && !res.IsError && len(f.sends) != 1 {
		t.Fatalf("sends %v", f.sends)
	}
	if got := ackedIDs(f); len(got) != 0 {
		t.Fatalf("outbound sock_send acked %v", got)
	}
}

func TestConsumingSurfaceTakesMessageIDs(t *testing.T) {
	res, err := start(t, &fake{}).ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range res.Tools {
		b, _ := json.Marshal(tl)
		if tl.Name == "sock_send" && !bytes.Contains(b, []byte(`"message_ids"`)) {
			t.Fatalf("sock_send schema %s", b)
		}
		if tl.Name == "sock_whoami" && !bytes.Contains(b, []byte(`"pending_unacked"`)) {
			t.Fatalf("sock_whoami schema %s", b)
		}
	}
}
