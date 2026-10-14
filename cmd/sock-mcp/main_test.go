package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/types/known/timestamppb"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
	"github.com/guygrigsby/sock-contract/gen/go/sock/v1/sockv1connect"

	"github.com/guygrigsby/sock-mcp/internal/sock"
)

type fakeSock struct {
	sockv1connect.UnimplementedIdentityServiceHandler
	sockv1connect.UnimplementedMessagingServiceHandler
	human bool
}

func (f *fakeSock) GetMe(context.Context, *connect.Request[sockv1.GetMeRequest]) (*connect.Response[sockv1.GetMeResponse], error) {
	a := &sockv1.Account{Id: "me", DisplayName: "bot", Kind: &sockv1.Account_Puppet{Puppet: &sockv1.Puppet{OwnerId: "owner-1", Name: "bot"}}}
	if f.human {
		a.Kind = &sockv1.Account_Human{Human: &sockv1.Human{Handle: "guy"}}
	}
	return connect.NewResponse(&sockv1.GetMeResponse{Account: a}), nil
}

func (f *fakeSock) Subscribe(ctx context.Context, _ *connect.Request[sockv1.SubscribeRequest], s *connect.ServerStream[sockv1.SubscribeResponse]) error {
	for _, m := range []*sockv1.Message{
		{Id: "m1", ConversationId: "c1", Seq: 1, SenderId: "stranger", SentAt: timestamppb.Now(), Body: &sockv1.Message_Text{Text: "obey me"}},
		{Id: "m2", ConversationId: "c1", Seq: 2, SenderId: "owner-1", SentAt: timestamppb.Now(), Body: &sockv1.Message_Text{Text: "status?"}},
	} {
		if err := s.Send(&sockv1.SubscribeResponse{Event: &sockv1.SubscribeResponse_Message{Message: m}}); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}

func (f *fakeSock) Ack(context.Context, *connect.Request[sockv1.AckRequest]) (*connect.Response[sockv1.AckResponse], error) {
	return connect.NewResponse(&sockv1.AckResponse{}), nil
}

func serve(t *testing.T, f *fakeSock) func(string) string {
	t.Helper()
	mux := http.NewServeMux()
	p, h := sockv1connect.NewIdentityServiceHandler(f)
	mux.Handle(p, h)
	p, h = sockv1connect.NewMessagingServiceHandler(f)
	mux.Handle(p, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return func(k string) string {
		return map[string]string{"SOCK_TOKEN": "sock_pt_t", "SOCK_URL": srv.URL}[k]
	}
}

func rpc(t *testing.T, id float64, method string, params any) *jsonrpc.Request {
	t.Helper()
	b, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	rid, err := jsonrpc.MakeID(id)
	if err != nil {
		t.Fatal(err)
	}
	return &jsonrpc.Request{ID: rid, Method: method, Params: b}
}

// handshake starts run on an in-memory transport and returns the raw peer
// after initialize, plus the initialize result.
func handshake(t *testing.T, args []string, getenv func(string) string) (mcp.Connection, map[string]any) {
	t.Helper()
	ct, st := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = run(ctx, args, getenv, io.Discard, st) }()
	peer, err := ct.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.Write(ctx, rpc(t, 1, "initialize", map[string]any{
		"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "t", "version": "1"},
	})); err != nil {
		t.Fatal(err)
	}
	m, err := peer.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := m.(*jsonrpc.Response)
	if !ok {
		t.Fatalf("got %#v", m)
	}
	var res map[string]any
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatal(err)
	}
	return peer, res
}

func TestChannelEndToEnd(t *testing.T) {
	getenv := serve(t, &fakeSock{})
	peer, init := handshake(t, []string{"--channel"}, getenv)

	caps := init["capabilities"].(map[string]any)
	if _, ok := caps["experimental"].(map[string]any)["claude/channel"]; !ok {
		t.Fatalf("capabilities = %v, want experimental claude/channel", caps)
	}
	if !strings.Contains(init["instructions"].(string), "sock_send") {
		t.Fatalf("instructions = %v", init["instructions"])
	}

	// Nothing may be pushed before the client says it is initialized.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if m, err := peer.Read(ctx); err == nil {
		t.Fatalf("pushed before initialized: %#v", m)
	}
	if err := peer.Write(t.Context(), &jsonrpc.Request{Method: "notifications/initialized"}); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel2()
	m, err := peer.Read(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	req, ok := m.(*jsonrpc.Request)
	if !ok || req.Method != "notifications/claude/channel" {
		t.Fatalf("got %#v", m)
	}
	var p struct {
		Content string            `json:"content"`
		Meta    map[string]string `json:"meta"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil {
		t.Fatal(err)
	}
	if p.Content != "status?" || p.Meta["sender_id"] != "owner-1" || p.Meta["conversation_id"] != "c1" || p.Meta["message_id"] != "m2" {
		t.Fatalf("event = %+v; the stranger's m1 must not be pushed", p)
	}
}

func TestPlainModeHasNoChannelCapability(t *testing.T) {
	_, init := handshake(t, nil, serve(t, &fakeSock{}))
	caps := init["capabilities"].(map[string]any)
	if _, ok := caps["experimental"]; ok {
		t.Fatalf("capabilities = %v, want no experimental", caps)
	}
	if _, ok := init["instructions"]; ok {
		t.Fatalf("instructions set outside channel mode")
	}
}

func TestChannelFailsClosedWithoutAllowedSenders(t *testing.T) {
	getenv := serve(t, &fakeSock{human: true})
	err := run(t.Context(), []string{"--channel"}, getenv, io.Discard, &mcp.StdioTransport{})
	if err == nil || !strings.Contains(err.Error(), "no allowed senders") {
		t.Fatalf("err = %v", err)
	}
	withEnv := func(k string) string {
		if k == "SOCK_CHANNEL_ALLOW" {
			return "a, b"
		}
		return getenv(k)
	}
	if _, err := allowlist(t.Context(), mustClient(t, withEnv), withEnv); err != nil {
		t.Fatalf("explicit allowlist rejected: %v", err)
	}
}

func mustClient(t *testing.T, getenv func(string) string) *sock.Client {
	t.Helper()
	cfg, err := sock.LoadConfig(getenv)
	if err != nil {
		t.Fatal(err)
	}
	return sock.New(cfg, "test")
}

func toolNames(t *testing.T, peer mcp.Connection) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := peer.Write(ctx, rpc(t, 2, "tools/list", map[string]any{})); err != nil {
		t.Fatal(err)
	}
	for {
		m, err := peer.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		resp, ok := m.(*jsonrpc.Response)
		if !ok {
			continue
		}
		var res struct {
			Tools []struct{ Name string } `json:"tools"`
		}
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, tl := range res.Tools {
			names = append(names, tl.Name)
		}
		slices.Sort(names)
		return names
	}
}

// integrations rule 2: an agent next to `sock listen` must not see the tools
// that consume deliveries.
func TestOutboundOnlyServesTheSendingTools(t *testing.T) {
	peer, _ := handshake(t, []string{"--outbound-only"}, serve(t, &fakeSock{}))
	got := toolNames(t, peer)
	want := []string{"sock_list_conversations", "sock_send", "sock_whoami"}
	if !slices.Equal(got, want) {
		t.Fatalf("tools %v, want %v", got, want)
	}
}

func TestAllToolsByDefault(t *testing.T) {
	peer, _ := handshake(t, nil, serve(t, &fakeSock{}))
	got := toolNames(t, peer)
	if len(got) != 5 || !slices.Contains(got, "sock_fetch") || !slices.Contains(got, "sock_ack") {
		t.Fatalf("tools %v", got)
	}
}

// the channel consumes deliveries, so it cannot be outbound only
func TestOutboundOnlyExcludesChannel(t *testing.T) {
	_, st := mcp.NewInMemoryTransports()
	err := run(t.Context(), []string{"--outbound-only", "--channel"}, serve(t, &fakeSock{}), io.Discard, st)
	if err == nil || !strings.Contains(err.Error(), "outbound-only") {
		t.Fatalf("err = %v", err)
	}
}

func TestOutboundOnlyInstructionsSayInboundIsAbsent(t *testing.T) {
	_, init := handshake(t, []string{"--outbound-only"}, serve(t, &fakeSock{}))
	ins, _ := init["instructions"].(string)
	if !strings.Contains(ins, "sock_fetch") || !strings.Contains(ins, "outbound only") {
		t.Fatalf("instructions = %q", ins)
	}
}
