package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/types/known/timestamppb"

	client "github.com/aeryx-ai/sock-client"
	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
	"github.com/aeryx-ai/sock-contract/gen/go/sock/v1/sockv1connect"

	"github.com/aeryx-ai/sock-mcp/internal/channel"
	"github.com/aeryx-ai/sock-mcp/internal/codex/codextest"
	"github.com/aeryx-ai/sock-mcp/sockclient"
)

type fakeSock struct {
	sockv1connect.UnimplementedIdentityServiceHandler
	sockv1connect.UnimplementedMessagingServiceHandler
	human bool

	mu                   sync.Mutex
	notifies             []string // NotifyOwner texts
	posts                []string // Send conversation ids
	auths                []string // Authorization of each GetMe
	creates              []string // puppet names asked for
	createAuth           string
	createErr            error
	createAuths          []string // Authorization of each CreatePuppet
	deadKey              string   // a machine key CreatePuppet rejects as TOKEN_INVALID
	acks                 []string // acked message ids
	subscribes           int
	archives             []string // "archive <id>" or "unarchive <id>"
	renames              []string // "<authorization> <name>"
	archiveCalls         int
	archiveUnimplemented bool
}

func (f *fakeSock) CreatePuppet(_ context.Context, r *connect.Request[sockv1.CreatePuppetRequest]) (*connect.Response[sockv1.CreatePuppetResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, r.Msg.GetName())
	f.createAuth = r.Header().Get("Authorization")
	f.createAuths = append(f.createAuths, f.createAuth)
	if f.deadKey != "" && f.createAuth == "Bearer "+f.deadKey {
		e := connect.NewError(connect.CodeUnauthenticated, errors.New("token invalid"))
		if d, err := connect.NewErrorDetail(&sockv1.ErrorInfo{Reason: sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID}); err == nil {
			e.AddDetail(d)
		}
		return nil, e
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
	return connect.NewResponse(&sockv1.CreatePuppetResponse{Puppet: &sockv1.Account{Id: "proj-puppet"}, Token: "sock_pt_projdev_secret"}), nil
}

func (f *fakeSock) GetMe(_ context.Context, r *connect.Request[sockv1.GetMeRequest]) (*connect.Response[sockv1.GetMeResponse], error) {
	f.mu.Lock()
	f.auths = append(f.auths, r.Header().Get("Authorization"))
	f.mu.Unlock()
	a := &sockv1.Account{Id: "me", DisplayName: "bot", Kind: &sockv1.Account_Puppet{Puppet: &sockv1.Puppet{OwnerId: "owner-1", Name: "bot"}}}
	if f.human {
		a.Kind = &sockv1.Account_Human{Human: &sockv1.Human{Handle: "guy"}}
	}
	return connect.NewResponse(&sockv1.GetMeResponse{Account: a}), nil
}

func (f *fakeSock) Subscribe(ctx context.Context, _ *connect.Request[sockv1.SubscribeRequest], s *connect.ServerStream[sockv1.SubscribeResponse]) error {
	f.mu.Lock()
	f.subscribes++
	f.mu.Unlock()
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

func (f *fakeSock) NotifyOwner(_ context.Context, r *connect.Request[sockv1.NotifyOwnerRequest]) (*connect.Response[sockv1.NotifyOwnerResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notifies = append(f.notifies, r.Msg.GetText())
	return connect.NewResponse(&sockv1.NotifyOwnerResponse{Message: &sockv1.Message{Id: "n1", SentAt: timestamppb.Now()}}), nil
}

func (f *fakeSock) Send(_ context.Context, r *connect.Request[sockv1.SendRequest]) (*connect.Response[sockv1.SendResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.posts = append(f.posts, r.Msg.GetConversationId())
	return connect.NewResponse(&sockv1.SendResponse{Message: &sockv1.Message{Id: "s1", ConversationId: r.Msg.GetConversationId(), SentAt: timestamppb.Now()}}), nil
}

func (f *fakeSock) Ack(_ context.Context, r *connect.Request[sockv1.AckRequest]) (*connect.Response[sockv1.AckResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, r.Msg.GetMessageIds()...)
	return connect.NewResponse(&sockv1.AckResponse{}), nil
}

func (f *fakeSock) acked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.acks...)
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
	state, config := t.TempDir(), t.TempDir()
	// The config dir is always the test's own: the developer's real config
	// file (a machine key beats SOCK_TOKEN, sock-client v0.1.7) must never
	// reach a test, which would then talk to the real hub.
	return func(k string) string {
		return map[string]string{"SOCK_TOKEN": "sock_pt_t", "SOCK_URL": srv.URL, "XDG_STATE_HOME": state, "XDG_CONFIG_HOME": config}[k]
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
	return handshakeWith(t, args, getenv, "")
}

// handshakeWith is handshake with owner as the SOCK_MCP_MESSAGE_OWNER main
// took from the process environment.
func handshakeWith(t *testing.T, args []string, getenv func(string) string, owner string) (mcp.Connection, map[string]any) {
	t.Helper()
	ct, st := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = run(ctx, args, getenv, owner, io.Discard, st) }()
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
	f := &fakeSock{}
	getenv := serve(t, f)
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

	// The write reached the host, which proves nothing about display (a host
	// without channels drops it), so nothing is acked until the model acts.
	time.Sleep(200 * time.Millisecond)
	strangerOnly(t, f.acked(), "on a push alone")
	// A post into the conversation that names no message is no evidence the
	// model saw the push: a session without channels can still find the
	// owner's conversation and write there.
	if !sendTool(t, peer, map[string]any{"text": "build done", "conversation_id": "c1"}) {
		t.Fatal("send failed")
	}
	strangerOnly(t, f.acked(), "on a post that answers no message")
	// Naming the message from its channel tag is: the model had to see it.
	if !sendTool(t, peer, map[string]any{"text": "all green", "conversation_id": "c1", "message_ids": []string{"m2"}}) {
		t.Fatal("reply failed")
	}
	if got := f.acked(); !slices.Contains(got, "m2") {
		t.Fatalf("acked %v after the reply, want m2 among them", got)
	}
}

// strangerOnly is every ack a push alone may cause: the stranger's m1, which
// the model never sees and which would otherwise sit in the hub's page of the
// 500 oldest pending messages and starve the owner. The owner's m2 is never
// acked by a push.
func strangerOnly(t *testing.T, got []string, why string) {
	t.Helper()
	for _, id := range got {
		if id != "m1" {
			t.Fatalf("acked %v %s: only the stranger's m1 may be", got, why)
		}
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
	err := run(t.Context(), []string{"--channel"}, getenv, "", io.Discard, &mcp.StdioTransport{})
	if err == nil || !strings.Contains(err.Error(), "no allowed senders") {
		t.Fatalf("err = %v", err)
	}
	withEnv := func(k string) string {
		if k == "SOCK_CHANNEL_ALLOW" {
			return "a, b"
		}
		return getenv(k)
	}
	if _, _, err := allowlist(t.Context(), mustClient(t, withEnv), withEnv); err != nil {
		t.Fatalf("explicit allowlist rejected: %v", err)
	}
}

func mustClient(t *testing.T, getenv func(string) string) *sockclient.Client {
	t.Helper()
	cfg, err := sockclient.LoadConfig(getenv, nil)
	if err != nil {
		t.Fatal(err)
	}
	return sockclient.New(cfg, "test")
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
	err := run(t.Context(), []string{"--outbound-only", "--channel"}, serve(t, &fakeSock{}), "", io.Discard, st)
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

// discoverVersions asks a freshly started server, before any initialize as
// Claude Code does, what protocol versions it offers (server/discover, the
// 2026-07-28 revision's handshake).
func discoverVersions(t *testing.T, args []string) []string {
	t.Helper()
	ct, st := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	getenv := serve(t, &fakeSock{})
	go func() { _ = run(ctx, args, getenv, "", io.Discard, st) }()
	peer, err := ct.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.Write(ctx, rpc(t, 3, "server/discover", map[string]any{"_meta": map[string]any{
		"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "t", "version": "1"},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
	}})); err != nil {
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
		if resp.Error != nil {
			// A server that does not offer the requested revision answers with
			// the versions it does offer, which is how a client falls back.
			var we *jsonrpc.Error
			if errors.As(resp.Error, &we) && we.Code == -32022 {
				var data struct {
					Supported []string `json:"supported"`
				}
				if err := json.Unmarshal(we.Data, &data); err != nil {
					t.Fatal(err)
				}
				return data.Supported
			}
			t.Fatalf("server/discover: %v", resp.Error)
		}
		var res struct {
			SupportedVersions []string `json:"supportedVersions"`
		}
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			t.Fatal(err)
		}
		return res.SupportedVersions
	}
}

// integrations-pvq: Claude Code registers a channel only on the legacy
// handshake. A server that offers revision 2026-07-28 in server/discover gets
// the new handshake and loses channel delivery, so --channel caps the offer.
func TestChannelOffersNoRevisionFromTheNewHandshake(t *testing.T) {
	got := discoverVersions(t, []string{"--channel"})
	if len(got) == 0 {
		t.Fatal("no versions offered")
	}
	for _, v := range got {
		if v >= "2026-07-28" {
			t.Fatalf("--channel offers %s: %v", v, got)
		}
	}
	if !slices.Contains(got, "2025-11-25") {
		t.Fatalf("--channel dropped the legacy revision: %v", got)
	}
}

func TestWithoutChannelTheNewRevisionIsStillOffered(t *testing.T) {
	got := discoverVersions(t, nil)
	if !slices.ContainsFunc(got, func(v string) bool { return v >= "2026-07-28" }) {
		t.Fatalf("offered %v", got)
	}
}

func TestChannelInitializeStillNegotiatesTheLegacyRevision(t *testing.T) {
	_, init := handshake(t, []string{"--channel"}, serve(t, &fakeSock{}))
	if v, _ := init["protocolVersion"].(string); v == "" || v >= "2026-07-28" {
		t.Fatalf("protocolVersion = %q", v)
	}
}

// With a machine key as the credential, sock-mcp
// runs as the project's own puppet.
func machineKeyEnv(t *testing.T, base func(string) string) (func(string) string, string) {
	t.Helper()
	proj := filepath.Join(t.TempDir(), "myproj")
	if err := os.Mkdir(proj, 0o755); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(t.TempDir(), "state")
	env := map[string]string{
		"SOCK_TOKEN": "sock_mk_dev_machinesecret", "SOCK_URL": base("SOCK_URL"),
		"CLAUDE_PROJECT_DIR": proj, "XDG_STATE_HOME": state, "HOME": t.TempDir(),
	}
	return func(k string) string { return env[k] }, state
}

func TestMachineKeyRunsAsTheProjectsPuppet(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	getenv, state := machineKeyEnv(t, base)
	handshake(t, []string{"--channel"}, getenv)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 1 || !strings.HasPrefix(f.creates[0], "myproj on ") || f.createAuth != "Bearer sock_mk_dev_machinesecret" {
		t.Fatalf("creates %v as %q", f.creates, f.createAuth)
	}
	if len(f.auths) == 0 {
		t.Fatal("no call as the puppet")
	}
	for _, a := range f.auths {
		if a != "Bearer sock_pt_projdev_secret" {
			t.Fatalf("a call went out as %q: the machine key must only mint", a)
		}
	}
	if _, err := os.Stat(filepath.Join(state, "sock-integrations", "puppets")); err != nil {
		t.Fatalf("no cache: %v", err)
	}
}

func TestMachineKeyReusesTheCachedPuppet(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	getenv, _ := machineKeyEnv(t, base)
	handshake(t, []string{"--outbound-only"}, getenv)
	handshake(t, []string{"--outbound-only"}, getenv)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 1 {
		t.Fatalf("creates %v", f.creates)
	}
}

func TestMachineKeyFailureNamesNoSecret(t *testing.T) {
	f := &fakeSock{createErr: connect.NewError(connect.CodePermissionDenied, errors.New("nope sock_mk_dev_machinesecret"))}
	base := serve(t, f)
	getenv, _ := machineKeyEnv(t, base)
	_, st := mcp.NewInMemoryTransports()
	err := run(t.Context(), []string{"--outbound-only"}, getenv, "", io.Discard, st)
	if err == nil || !strings.Contains(err.Error(), "puppet") || strings.Contains(err.Error(), "machinesecret") {
		t.Fatalf("err = %v", err)
	}
}

// one project, two hubs, one state dir: each hub gets its own puppet, and a
// token cached for one is never presented to the other
func TestMachineKeyKeepsEachHubsPuppetApart(t *testing.T) {
	a, b := &fakeSock{}, &fakeSock{}
	getenvA, _ := machineKeyEnv(t, serve(t, a))
	baseB := serve(t, b)
	getenvB := func(k string) string {
		if k == "SOCK_URL" {
			return baseB("SOCK_URL")
		}
		return getenvA(k)
	}
	handshake(t, []string{"--outbound-only"}, getenvA)
	handshake(t, []string{"--outbound-only"}, getenvB)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.creates) != 1 {
		t.Fatalf("hub B created %v: it reused hub A's cached puppet", b.creates)
	}
}

// codexEnv is a Codex start: no SOCK_TOKEN, no CLAUDE_PROJECT_DIR, the
// config file pointing at the fake with a machine key plus extra lines, and
// the working directory cwd. pluginRoot, when set, is PLUGIN_ROOT.
func codexEnv(t *testing.T, base func(string) string, extra, cwd, pluginRoot string) func(string) string {
	t.Helper()
	cfgHome := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cfgHome, "sock"), 0o700); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf("url = %q\nmachine_key = \"sock_mk_dev_machinesecret\"\n%s", base("SOCK_URL"), extra)
	if err := os.WriteFile(filepath.Join(cfgHome, "sock", "config.toml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(cwd)
	env := map[string]string{"XDG_CONFIG_HOME": cfgHome, "XDG_STATE_HOME": filepath.Join(t.TempDir(), "state"), "HOME": t.TempDir()}
	if pluginRoot != "" {
		env["PLUGIN_ROOT"] = pluginRoot
	}
	return func(k string) string { return env[k] }
}

func mkdir(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func gitRepo(t *testing.T, dir string) string {
	t.Helper()
	mkdir(t, dir)
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, out)
	}
	return dir
}

// sock-cli-qb6: Codex starts a config.toml server in the project with neither
// SOCK_TOKEN nor CLAUDE_PROJECT_DIR; the file's machine_key and the working
// directory's repository give it the project's own puppet.
func TestFileMachineKeyRunsAsTheWorkingDirectorysPuppet(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	proj := gitRepo(t, filepath.Join(t.TempDir(), "codexproj"))
	getenv := codexEnv(t, base, "puppet_token = \"sock_pt_shared\"\n", mkdir(t, filepath.Join(proj, "sub")), "")
	handshake(t, []string{"--outbound-only"}, getenv)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 1 || !strings.HasPrefix(f.creates[0], "codexproj on ") || f.createAuth != "Bearer sock_mk_dev_machinesecret" {
		t.Fatalf("creates %v as %q", f.creates, f.createAuth)
	}
	for _, a := range f.auths {
		if a != "Bearer sock_pt_projdev_secret" {
			t.Fatalf("a call went out as %q", a)
		}
	}
}

// A Codex plugin server starts in the plugin cache, inside PLUGIN_ROOT: no
// project there, so the shared puppet_token, said once on stderr.
func TestPluginRootFallsBackToThePuppetToken(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
	getenv := codexEnv(t, base, "puppet_token = \"sock_pt_shared\"\n", mkdir(t, filepath.Join(root, "bin")), root)
	cfg, err := sockclient.LoadConfig(getenv, nil)
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	got, err := projectPuppet(t.Context(), cfg, getenv, &stderr)
	if err != nil || got.Token != "sock_pt_shared" || got.URL != base("SOCK_URL") {
		t.Fatalf("got %+v, %v", got, err)
	}
	if len(f.creates) != 0 {
		t.Fatalf("minted %v in the plugin root", f.creates)
	}
	if n := strings.Count(stderr.String(), "\n"); n != 1 || !strings.Contains(stderr.String(), "puppet_token") || strings.Contains(stderr.String(), "secret") || strings.Contains(stderr.String(), "sock_") {
		t.Fatalf("stderr %q", stderr.String())
	}
}

func TestNoProjectWithoutAPuppetTokenFails(t *testing.T) {
	for name, pluginRoot := range map[string]bool{"plugin root": true, "not a repository": false} {
		t.Run(name, func(t *testing.T) {
			f := &fakeSock{}
			base := serve(t, f)
			dir := mkdir(t, filepath.Join(t.TempDir(), "cache"))
			root := ""
			if pluginRoot {
				gitRepo(t, dir)
				root = dir
			}
			getenv := codexEnv(t, base, "", dir, root)
			cfg, err := sockclient.LoadConfig(getenv, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := projectPuppet(t.Context(), cfg, getenv, io.Discard); err == nil || err.Error() != "no project directory: Codex plugin servers start in the plugin root; use the config.toml route for per-project puppets" {
				t.Fatalf("err = %v", err)
			}
			if len(f.creates) != 0 {
				t.Fatalf("minted %v", f.creates)
			}
		})
	}
}

// one source, never a mix: a machine key from SOCK_TOKEN never falls back to
// the file's puppet_token (and its url), it fails
func TestEnvMachineKeyWithNoProjectDoesNotReadTheFile(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	fileEnv := codexEnv(t, base, "puppet_token = \"sock_pt_shared\"\n", mkdir(t, filepath.Join(t.TempDir(), "bare")), "")
	getenv := func(k string) string {
		switch k {
		case "SOCK_TOKEN":
			return "sock_mk_dev_machinesecret"
		case "SOCK_URL":
			return base("SOCK_URL")
		}
		return fileEnv(k)
	}
	cfg, err := sockclient.LoadConfig(getenv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := projectPuppet(t.Context(), cfg, getenv, io.Discard); err == nil || !strings.HasPrefix(err.Error(), "no project directory") {
		t.Fatalf("got %+v, %v", got, err)
	}
	if len(f.creates) != 0 {
		t.Fatalf("minted %v", f.creates)
	}
}

// sendTool calls sock_send on peer and reports whether it succeeded.
func sendTool(t *testing.T, peer mcp.Connection, args map[string]any) bool {
	t.Helper()
	ok, _ := sendResult(t, peer, args)
	return ok
}

// sendResult is sendTool with the result's text.
func sendResult(t *testing.T, peer mcp.Connection, args map[string]any) (bool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := peer.Write(ctx, rpc(t, 3, "tools/call", map[string]any{"name": "sock_send", "arguments": args})); err != nil {
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
		if resp.Error != nil {
			return false, resp.Error.Error()
		}
		var res struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			t.Fatal(err)
		}
		var text string
		for _, c := range res.Content {
			text += c.Text
		}
		return !res.IsError, text
	}
}

// ownerSession starts sock-mcp the way main does, with env set in the
// process environment first and the config file holding file (none when ""),
// and returns the peer, the fake hub and the config file's path.
func ownerSession(t *testing.T, env map[string]string, file string) (mcp.Connection, *fakeSock, string) {
	t.Helper()
	t.Setenv(ownerEnv, "") // restored after the test
	_ = os.Unsetenv(ownerEnv)
	for k, v := range env {
		t.Setenv(k, v)
	}
	f := &fakeSock{}
	base := serve(t, f)
	path := filepath.Join(base("XDG_CONFIG_HOME"), "sock", "config.toml")
	if file != "" {
		writeConfig(t, path, file)
	}
	getenv := func(k string) string {
		if v := base(k); v != "" {
			return v
		}
		return os.Getenv(k)
	}
	peer, _ := handshakeWith(t, []string{"--outbound-only"}, getenv, takeMessageOwner())
	return peer, f, path
}

// writeConfig writes body to the sock config file at path, mode 600.
func writeConfig(t *testing.T, path, body string) {
	t.Helper()
	mkdir(t, filepath.Dir(path))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// ownerSent sends one owner message and one reply on peer and checks what
// the hub saw since the last check: the reply always, the owner message only
// when want.
func ownerSent(t *testing.T, peer mcp.Connection, f *fakeSock, what string, want bool) {
	t.Helper()
	if got := sendTool(t, peer, map[string]any{"text": "owner"}); got != want {
		t.Errorf("%s: owner message sent = %v, want %v", what, got, want)
	}
	if !sendTool(t, peer, map[string]any{"text": "re", "conversation_id": "c1"}) {
		t.Errorf("%s: reply refused", what)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	wantNotifies := []string(nil)
	if want {
		wantNotifies = []string{"owner"}
	}
	if !slices.Equal(f.notifies, wantNotifies) || !slices.Equal(f.posts, []string{"c1"}) {
		t.Errorf("%s: hub saw notifies=%v posts=%v", what, f.notifies, f.posts)
	}
	f.notifies, f.posts = nil, nil
}

// Owner messaging is decided at each send: SOCK_MCP_MESSAGE_OWNER when set
// (1, true or on is on, any other value off), else the config file's
// message_owner, else on.
// The file is read again for every send, so `sock settings --message-owner`
// applies without a restart. No session signal stands in for either.
func TestOwnerMessagingIsDecidedAtEachSend(t *testing.T) {
	peer, f, path := ownerSession(t, map[string]string{"CLAUDE_CODE_ENTRYPOINT": "cli", "SOCK_MCP_ATTENDED": "1"}, "")
	ownerSent(t, peer, f, "no file", true)
	writeConfig(t, path, "message_owner = false\n")
	ownerSent(t, peer, f, "file off", false)
	writeConfig(t, path, "message_owner = true\n")
	ownerSent(t, peer, f, "file on", true)
	writeConfig(t, path, "url = \"https://example.test\"\n")
	ownerSent(t, peer, f, "file without the key", true)

	for _, tc := range []struct {
		env  map[string]string
		file string
		want bool
	}{
		{map[string]string{ownerEnv: "0"}, "message_owner = true\n", false},
		{map[string]string{ownerEnv: "false"}, "", false},
		{map[string]string{ownerEnv: "1"}, "message_owner = false\n", true},
		{map[string]string{ownerEnv: "true"}, "message_owner = false\n", true},
		{map[string]string{ownerEnv: "on"}, "message_owner = false\n", true},
		// set but not on: off, whatever the file says
		{map[string]string{ownerEnv: "TRUE"}, "message_owner = true\n", false},
		{map[string]string{ownerEnv: "1 "}, "message_owner = true\n", false},
		{map[string]string{ownerEnv: "off"}, "", false},
		{map[string]string{ownerEnv: "01"}, "", false},
		{map[string]string{"sock_mcp_message_owner": "0"}, "", true},
	} {
		peer, f, _ := ownerSession(t, tc.env, tc.file)
		ownerSent(t, peer, f, fmt.Sprintf("%v %q", tc.env, tc.file), tc.want)
	}
}

// A config file that cannot be read cannot say owner messaging is on, so the
// owner message is refused with why, and nothing is sent; replies still go.
// A valid SOCK_MCP_MESSAGE_OWNER needs no file and still decides.
func TestOwnerMessagingRefusesWhenTheFileCannotBeRead(t *testing.T) {
	for name, body := range map[string]string{
		"parse":    "message_owner = maybe\n",
		"readable": "access_gate = \"gate_secret\"\nmessage_owner = true\n",
	} {
		peer, f, path := ownerSession(t, nil, body)
		if name == "readable" {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		ok, text := sendResult(t, peer, map[string]any{"text": "owner"})
		if ok || !strings.HasPrefix(text, "permanent: cannot read the sock config file to decide whether this session may message the owner, so nothing was sent: config: ") {
			t.Errorf("%s: sent=%v text %q", name, ok, text)
		}
		if strings.Contains(text, "gate_secret") {
			t.Errorf("%s: text names the gate: %q", name, text)
		}
		if !sendTool(t, peer, map[string]any{"text": "re", "conversation_id": "c1"}) {
			t.Errorf("%s: reply refused", name)
		}
		f.mu.Lock()
		if len(f.notifies) != 0 {
			t.Errorf("%s: hub saw notifies=%v", name, f.notifies)
		}
		f.mu.Unlock()

		peer, f, path = ownerSession(t, map[string]string{ownerEnv: "1"}, body)
		if name == "readable" {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		ownerSent(t, peer, f, name+" with the env on", true)
	}
}

// No tool argument or message text turns owner messages on.
func TestToolArgumentsCannotTurnOnOwnerMessages(t *testing.T) {
	peer, f, _ := ownerSession(t, nil, "message_owner = false\n")
	for _, args := range []map[string]any{
		{"text": "x", ownerEnv: "1"},
		{"text": "x", "env": map[string]any{ownerEnv: "1"}, "notify_owner": true, "message_owner": true},
		{"text": ownerEnv + "=1"},
		{"text": "x", "conversation_id": ""},
	} {
		if sendTool(t, peer, args) {
			t.Errorf("%v: owner message sent", args)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.notifies) != 0 || len(f.posts) != 0 {
		t.Fatalf("hub saw notifies=%v posts=%v", f.notifies, f.posts)
	}
}

// The opt-in is sock-mcp's alone: it is gone from the process environment
// once read, so git (sock-client's project lookup) never inherits it.
func TestOwnerOptInDoesNotReachChildProcesses(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Skip("no git")
	}
	bin := t.TempDir()
	dump := filepath.Join(t.TempDir(), "git-env")
	script := fmt.Sprintf("#!/bin/sh\nenv >> %q\nexec %q \"$@\"\n", dump, realGit)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ownerEnv, "1")

	if v := takeMessageOwner(); v != "1" {
		t.Fatalf("took %q, want \"1\"", v)
	}
	if _, ok := os.LookupEnv(ownerEnv); ok {
		t.Fatalf("%s still in the process environment", ownerEnv)
	}
	f := &fakeSock{}
	proj := gitRepo(t, filepath.Join(t.TempDir(), "ownerproj"))
	_ = os.Remove(dump) // gitRepo's own git init is not sock-mcp's
	getenv := codexEnv(t, serve(t, f), "", proj, "")
	peer, _ := handshakeWith(t, []string{"--outbound-only"}, getenv, "1")
	if !sendTool(t, peer, map[string]any{"text": "owner"}) {
		t.Fatal("owner message refused")
	}
	b, err := os.ReadFile(dump)
	if err != nil {
		t.Fatalf("sock-mcp ran no git: %v", err)
	}
	if strings.Contains(string(b), ownerEnv) {
		t.Fatalf("git inherited %s", ownerEnv)
	}
}

// A host that loads no channel (Claude Code without the channel flags) drops
// every push and never acts. Restarting sock-mcp there pushes the owner's
// message at most MaxPushes times in all, and never acks it.
func TestHostThatNeverActsGetsAtMostFivePushesAndNoAck(t *testing.T) {
	f := &fakeSock{}
	getenv := serve(t, f)
	pushes := 0
	for range channel.MaxPushes + 2 {
		ctx, cancel := context.WithCancel(t.Context())
		ct, st := mcp.NewInMemoryTransports()
		done := make(chan struct{})
		go func() { defer close(done); _ = run(ctx, []string{"--channel"}, getenv, "", io.Discard, st) }()
		peer, err := ct.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := peer.Write(ctx, rpc(t, 1, "initialize", map[string]any{
			"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "t", "version": "1"},
		})); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Read(ctx); err != nil {
			t.Fatal(err)
		}
		if err := peer.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized"}); err != nil {
			t.Fatal(err)
		}
		rctx, rcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if m, err := peer.Read(rctx); err == nil {
			if req, ok := m.(*jsonrpc.Request); ok && req.Method == channel.Method {
				pushes++
			}
		}
		rcancel()
		_ = peer.Close()
		cancel()
		<-done
	}
	if pushes != channel.MaxPushes {
		t.Fatalf("pushed %d times over %d restarts, want %d", pushes, channel.MaxPushes+2, channel.MaxPushes)
	}
	strangerOnly(t, f.acked(), "though the host never acted")
}

// `sock-mcp replied` is how a headless runner learns that the agent it woke
// actually answered: a send that posted is recorded, and the check reads it.
func TestRepliedReportsAPostedSend(t *testing.T) {
	f := &fakeSock{}
	getenv := serve(t, f)
	peer, _ := handshake(t, []string{"--outbound-only"}, getenv)
	replied := func(conv string, since time.Time) error {
		return run(t.Context(), []string{"replied", "--conversation", conv, "--since", strconv.FormatFloat(float64(since.UnixNano())/1e9, 'f', 3, 64)}, getenv, "", io.Discard, &mcp.StdioTransport{})
	}
	start := time.Now().Add(-time.Second)
	if err := replied("c1", start); err == nil {
		t.Fatal("nothing was sent yet: replied must fail")
	}
	if !sendTool(t, peer, map[string]any{"text": "answer", "conversation_id": "c1"}) {
		t.Fatal("send failed")
	}
	if err := replied("c1", start); err != nil {
		t.Fatalf("a reply was posted: %v", err)
	}
	if err := replied("c2", start); err == nil {
		t.Fatal("another conversation's reply counted")
	}
	if err := replied("c1", time.Now().Add(time.Hour)); err == nil {
		t.Fatal("a reply before --since counted")
	}
}

func TestRepliedRejectsBadArguments(t *testing.T) {
	getenv := serve(t, &fakeSock{})
	for name, args := range map[string][]string{
		"no conversation": {"replied", "--since", "0"},
		"no since":        {"replied", "--conversation", "c1"},
		"bad since":       {"replied", "--conversation", "c1", "--since", "yesterday"},
	} {
		if err := run(t.Context(), args, getenv, "", io.Discard, &mcp.StdioTransport{}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

// A plugin starts sock-mcp before anyone has signed in. Failing at startup
// shows nothing in a tool result, so with no credential it starts anyway,
// lists its tools, and answers every call with what to do.
func TestNoCredentialStartsAndEveryToolSaysToSignIn(t *testing.T) {
	home := t.TempDir()
	getenv := func(k string) string {
		return map[string]string{"HOME": home, "XDG_STATE_HOME": filepath.Join(home, "state"), "XDG_CONFIG_HOME": filepath.Join(home, "config")}[k]
	}
	for _, args := range [][]string{nil, {"--outbound-only"}, {"--channel"}} {
		peer, _ := handshake(t, args, getenv)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		for _, tool := range []string{"sock_whoami", "sock_send"} {
			if err := peer.Write(ctx, rpc(t, 7, "tools/call", map[string]any{"name": tool, "arguments": map[string]any{"text": "hi", "conversation_id": "c1"}})); err != nil {
				t.Fatal(err)
			}
			var text string
			for {
				m, err := peer.Read(ctx)
				if err != nil {
					t.Fatalf("%v %s: %v", args, tool, err)
				}
				resp, ok := m.(*jsonrpc.Response)
				if !ok {
					continue
				}
				var res struct {
					IsError bool `json:"isError"`
					Content []struct {
						Text string `json:"text"`
					} `json:"content"`
				}
				if err := json.Unmarshal(resp.Result, &res); err != nil || !res.IsError || len(res.Content) != 1 {
					t.Fatalf("%v %s: result %s (err %v)", args, tool, resp.Result, err)
				}
				text = res.Content[0].Text
				break
			}
			if text != "permanent: not signed in: run sock login (in Claude Code, /sock:login), then reload the plugin" {
				t.Fatalf("%v %s: text %q", args, tool, text)
			}
		}
		cancel()
	}
}

// callTool calls sock_whoami on peer with _meta and returns the raw result, or
// fails on a JSON-RPC error.
func callTool(t *testing.T, peer mcp.Connection, id float64, meta map[string]any) map[string]any {
	const name = "sock_whoami"
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	params := map[string]any{"name": name, "arguments": map[string]any{}}
	if meta != nil {
		params["_meta"] = meta
	}
	if err := peer.Write(ctx, rpc(t, id, "tools/call", params)); err != nil {
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
		if resp.Error != nil {
			t.Fatalf("tools/call %s: %v", name, resp.Error)
		}
		var res map[string]any
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			t.Fatal(err)
		}
		return res
	}
}

// codexMeta is the _meta Codex puts on a tools/call from thread in dir.
func codexMeta(dir, thread string) map[string]any {
	return map[string]any{
		"codex/sandbox-state-meta": map[string]any{"sandboxCwd": "file://" + dir},
		"x-codex-turn-metadata":    map[string]any{"thread_id": thread},
	}
}

func (f *fakeSock) ArchivePuppet(_ context.Context, r *connect.Request[sockv1.ArchivePuppetRequest]) (*connect.Response[sockv1.ArchivePuppetResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.archiveCalls++
	if f.archiveUnimplemented {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("not yet"))
	}
	f.archives = append(f.archives, "archive "+r.Msg.GetPuppetId())
	return connect.NewResponse(&sockv1.ArchivePuppetResponse{}), nil
}

func (f *fakeSock) UnarchivePuppet(_ context.Context, r *connect.Request[sockv1.UnarchivePuppetRequest]) (*connect.Response[sockv1.UnarchivePuppetResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.archives = append(f.archives, "unarchive "+r.Msg.GetPuppetId())
	return connect.NewResponse(&sockv1.UnarchivePuppetResponse{}), nil
}

func (f *fakeSock) UpdateDisplayName(_ context.Context, r *connect.Request[sockv1.UpdateDisplayNameRequest]) (*connect.Response[sockv1.UpdateDisplayNameResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renames = append(f.renames, r.Header().Get("Authorization")+" "+r.Msg.GetDisplayName())
	return connect.NewResponse(&sockv1.UpdateDisplayNameResponse{}), nil
}

// Each Codex thread is its own session puppet, made
// with the machine key on the thread's first call and named for its
// directory until Codex titles it.
func TestCodexCallsRunAsTheThreadsPuppet(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
	proj := gitRepo(t, filepath.Join(t.TempDir(), "codexproj"))
	peer, init := handshake(t, []string{"--outbound-only"}, codexEnv(t, base, "", root, root))
	caps, _ := init["capabilities"].(map[string]any)
	if exp, _ := caps["experimental"].(map[string]any); exp["codex/sandbox-state-meta"] == nil {
		t.Fatalf("capabilities %v do not ask Codex for the sandbox cwd", caps)
	}
	for i, thread := range []string{"aaaaaaaa-1111", "aaaaaaaa-1111", "bbbbbbbb-2222"} {
		if res := callTool(t, peer, float64(2+i), codexMeta(proj, thread)); res["isError"] == true {
			t.Fatalf("whoami: %v", res)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 2 || !strings.HasPrefix(f.creates[0], "codexproj aaaaaaaa on ") || !strings.HasPrefix(f.creates[1], "codexproj bbbbbbbb on ") || f.createAuth != "Bearer sock_mk_dev_machinesecret" {
		t.Fatalf("creates %v as %q", f.creates, f.createAuth)
	}
	for _, a := range f.auths {
		if a != "Bearer sock_pt_projdev_secret" {
			t.Fatalf("a call went out as %q", a)
		}
	}
}

// A call that names no thread has no session: it fails alone.
func TestCodexCallWithoutAThreadFailsAlone(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
	peer, _ := handshake(t, []string{"--outbound-only"}, codexEnv(t, base, "", root, root))
	res := callTool(t, peer, 2, nil)
	if res["isError"] != true || !strings.Contains(fmt.Sprint(res["content"]), "names no Codex thread") {
		t.Fatalf("got %v", res)
	}
	if res := callTool(t, peer, 3, codexMeta(root, "cccccccc-3333")); res["isError"] == true {
		t.Fatalf("server did not recover: %v", res)
	}
}

// codexDaemon serves d as the Codex daemon under a short CODEX_HOME and
// returns env with CODEX_HOME set.
func codexDaemon(t *testing.T, d *codextest.Server, env func(string) string) func(string) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "cx") // a Unix socket path is at most 104 bytes on macOS
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	mkdir(t, filepath.Join(home, "app-server-control"))
	if err := os.Symlink(codextest.Serve(t, d), filepath.Join(home, "app-server-control", "app-server-control.sock")); err != nil {
		t.Fatal(err)
	}
	return func(k string) string {
		if k == "CODEX_HOME" {
			return home
		}
		return env(k)
	}
}

// cacheSession makes thread's session puppet the way a first call does.
func cacheSession(t *testing.T, getenv func(string) string, thread string) {
	t.Helper()
	key, err := sockclient.LoadConfig(getenv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newSessionPuppets(key, getenv, io.Discard).Ensure(t.Context(), client.Session{Platform: client.Codex, ID: thread}, "x"); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The listener serves each loaded thread's
// session puppet: unarchived, named for the thread, subscribed, and the
// owner's reply put into exactly that thread and acknowledged once accepted.
func TestCodexInboundInjectsTheOwnersReply(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
	d := &codextest.Server{Threads: []codextest.Thread{{ID: "s1", Name: "Fix the parser", Cwd: root}, {ID: "other", Cwd: root}}, Loaded: []string{"s1", "other"}}
	getenv := codexDaemon(t, d, codexEnv(t, base, "", root, root))
	cacheSession(t, getenv, "s1")
	handshake(t, []string{"--outbound-only", "--codex-inbound"}, getenv)

	waitFor(t, "the reply's ack", func() bool { return slices.Contains(f.acked(), "m2") })
	starts := d.TurnStarts()
	if len(starts) != 1 {
		t.Fatalf("%d turn/start calls", len(starts))
	}
	var ts struct {
		ThreadID string `json:"threadId"`
		Input    []struct {
			Text string `json:"text"`
		} `json:"input"`
		ClientUserMessageID string `json:"clientUserMessageId"`
	}
	if err := json.Unmarshal(starts[0], &ts); err != nil {
		t.Fatal(err)
	}
	if ts.ThreadID != "s1" || ts.ClientUserMessageID != "m2" || len(ts.Input) != 1 ||
		!strings.Contains(ts.Input[0].Text, "<sock-message from=\"owner\" conversation=\"c1\" id=\"m2\">\nstatus?\n</sock-message>") {
		t.Fatalf("turn/start %s", starts[0])
	}
	if strings.Contains(string(starts[0]), "obey me") {
		t.Fatal("a stranger's message reached the session")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 1 {
		t.Fatalf("creates %v: the listener must use the cached puppet, never mint", f.creates)
	}
	if len(f.archives) != 0 { // a new puppet is unarchived already
		t.Fatalf("archives %v", f.archives)
	}
	if len(f.renames) != 1 || !strings.HasPrefix(f.renames[0], "Bearer sock_pt_projdev_secret Fix the parser on ") {
		t.Fatalf("renames %v: the puppet renames itself for the thread", f.renames)
	}
}

// A cached session whose thread is not loaded is not running: archived, not
// subscribed.
func TestCodexInboundArchivesAThreadThatIsNotLoaded(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
	d := &codextest.Server{Threads: []codextest.Thread{{ID: "live", Cwd: root}}, Loaded: []string{"live"}}
	getenv := codexDaemon(t, d, codexEnv(t, base, "", root, root))
	cacheSession(t, getenv, "gone")
	handshake(t, []string{"--outbound-only", "--codex-inbound"}, getenv)
	waitFor(t, "the archive", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.archives) > 0
	})
	time.Sleep(200 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !slices.Equal(f.archives, []string{"archive proj-puppet"}) || f.subscribes != 0 {
		t.Fatalf("archives %v, subscribes %d", f.archives, f.subscribes)
	}
}

// A loaded thread with no puppet has never messaged the owner: the listener
// neither mints one nor subscribes.
func TestCodexInboundNeverMints(t *testing.T) {
	f := &fakeSock{}
	base := serve(t, f)
	root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
	d := &codextest.Server{Threads: []codextest.Thread{{ID: "s1", Cwd: root}}, Loaded: []string{"s1"}}
	handshake(t, []string{"--outbound-only", "--codex-inbound"}, codexDaemon(t, d, codexEnv(t, base, "", root, root)))
	waitFor(t, "the listener to read the daemon", func() bool { return d.Called("thread/read") })
	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 0 || f.subscribes != 0 {
		t.Fatalf("creates %v, subscribes %d", f.creates, f.subscribes)
	}
}

// Security review 2026-10-15: rescans fire on every thread event. A puppet
// already archived is not archived again, and a hub without the call (not yet
// deployed) is asked once, not on every event.
func TestCodexInboundArchivesOnceAndStopsOnUnimplemented(t *testing.T) {
	for name, unimpl := range map[string]bool{"deployed": false, "not deployed": true} {
		t.Run(name, func(t *testing.T) {
			f := &fakeSock{archiveUnimplemented: unimpl}
			base := serve(t, f)
			root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
			n := `{"method":"thread/status/changed","params":{"threadId":"live","status":{"type":"idle"}}}`
			d := &codextest.Server{Threads: []codextest.Thread{{ID: "live", Cwd: root}}, Loaded: []string{"live"}, Notify: []string{n, n, n, n}}
			getenv := codexDaemon(t, d, codexEnv(t, base, "", root, root))
			cacheSession(t, getenv, "gone")
			handshake(t, []string{"--outbound-only", "--codex-inbound"}, getenv)
			waitFor(t, "an archive call", func() bool {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.archiveCalls > 0
			})
			time.Sleep(500 * time.Millisecond)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.archiveCalls != 1 {
				t.Fatalf("%d archive calls over 5 rescans", f.archiveCalls)
			}
		})
	}
}

// fakeInbox serves a Claude Code session inbox and collects each user line's
// content.
type fakeInbox struct {
	path string
	mu   sync.Mutex
	got  []string
	auth []string
}

func newFakeInbox(t *testing.T) *fakeInbox {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ib") // a Unix socket path is at most 104 bytes on macOS
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	b := &fakeInbox{path: filepath.Join(dir, "s.sock")}
	l, err := net.Listen("unix", b.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			sc := bufio.NewScanner(c)
			for sc.Scan() {
				var line struct {
					Type    string `json:"type"`
					Token   string `json:"token"`
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				}
				_ = json.Unmarshal(sc.Bytes(), &line)
				b.mu.Lock()
				if line.Type == "auth" {
					b.auth = append(b.auth, line.Token)
				} else {
					b.got = append(b.got, line.Message.Content)
				}
				b.mu.Unlock()
			}
			_ = c.Close()
		}
	}()
	return b
}

func (b *fakeInbox) messages() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string{}, b.got...)
}

const claudeSID = "0123abcd-4444-5555-6666-777788889999"

// claudeEnv is a Claude Code plugin server's environment: a machine key in
// the config file, the session's id and inbox, and a transcript home.
func claudeEnv(t *testing.T, base func(string) string, b *fakeInbox) func(string) string {
	t.Helper()
	proj := gitRepo(t, filepath.Join(t.TempDir(), "claudeproj"))
	env := codexEnv(t, base, "", proj, "")
	extra := map[string]string{
		"CLAUDE_CODE_SESSION_ID":       claudeSID,
		"CLAUDE_PROJECT_DIR":           proj,
		"CLAUDE_CODE_MESSAGING_SOCKET": b.path,
		"CLAUDE_CODE_MESSAGING_TOKEN":  "inbox-secret",
		"CLAUDE_CONFIG_DIR":            t.TempDir(),
	}
	return func(k string) string {
		if v, ok := extra[k]; ok {
			return v
		}
		return env(k)
	}
}

// writeTitle appends a /rename entry to the session's transcript.
func writeTitle(t *testing.T, getenv func(string) string, title string) {
	t.Helper()
	dir := mkdir(t, filepath.Join(getenv("CLAUDE_CONFIG_DIR"), "projects", "-claudeproj"))
	f, err := os.OpenFile(filepath.Join(dir, claudeSID+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	_, _ = fmt.Fprintf(f, "{\"type\":\"user\",\"message\":{\"content\":\"hi\"}}\n{\"type\":\"custom-title\",\"customTitle\":%q,\"sessionId\":%q}\n", title, claudeSID)
}

func cacheClaudeSession(t *testing.T, getenv func(string) string, id string) {
	t.Helper()
	key, err := sockclient.LoadConfig(getenv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newSessionPuppets(key, getenv, io.Discard).Ensure(t.Context(), client.Session{Platform: client.ClaudeCode, ID: id}, "x"); err != nil {
		t.Fatal(err)
	}
}

// claudeServer runs a --claude-inbound server until stop, which waits for it.
func claudeServer(t *testing.T, getenv func(string) string) (mcp.Connection, func()) {
	t.Helper()
	ct, st := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = run(ctx, []string{"--outbound-only", "--claude-inbound"}, getenv, "", io.Discard, st)
	}()
	peer, err := ct.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(ctx, rpc(t, 1, "initialize", map[string]any{
		"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "t", "version": "1"},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Read(ctx); err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized", Params: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	stop := func() {
		cancel()
		_ = peer.Close()
		<-done
	}
	t.Cleanup(stop)
	return peer, stop
}

// A Claude Code session is its own puppet, made with
// the machine key on its first call and named for its project until the
// owner names the session.
func TestClaudeCallsRunAsTheSessionsPuppet(t *testing.T) {
	f := &fakeSock{}
	getenv := claudeEnv(t, serve(t, f), newFakeInbox(t))
	peer, _ := claudeServer(t, getenv)
	for i := range 2 {
		if res := callTool(t, peer, float64(2+i), nil); res["isError"] == true {
			t.Fatalf("whoami: %v", res)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) != 1 || !strings.HasPrefix(f.creates[0], "claudeproj 0123abcd on ") || f.createAuth != "Bearer sock_mk_dev_machinesecret" {
		t.Fatalf("creates %v as %q", f.creates, f.createAuth)
	}
	for _, a := range f.auths {
		if a != "Bearer sock_pt_projdev_secret" {
			t.Fatalf("a call went out as %q", a)
		}
	}
}

// The owner's reply to the session's puppet is
// written into the session's inbox, with its auth line, and acknowledged; a
// stranger's never reaches the session.
func TestClaudeInboundPutsTheOwnersReplyInTheInbox(t *testing.T) {
	f := &fakeSock{}
	b := newFakeInbox(t)
	getenv := claudeEnv(t, serve(t, f), b)
	cacheClaudeSession(t, getenv, claudeSID)
	claudeServer(t, getenv)
	waitFor(t, "the reply's ack", func() bool { return slices.Contains(f.acked(), "m2") })
	got := b.messages()
	if len(got) != 1 || !strings.Contains(got[0], "<sock-message from=\"owner\" conversation=\"c1\" id=\"m2\">\nstatus?\n</sock-message>") {
		t.Fatalf("inbox got %q", got)
	}
	b.mu.Lock()
	auth := b.auth
	b.mu.Unlock()
	if !slices.Equal(auth, []string{"inbox-secret"}) {
		t.Fatalf("auth lines %q", auth)
	}
}

// A session that never called a tool has no puppet: nothing listens until
// its first call makes one, and then it does.
func TestClaudeInboundListensOnceTheFirstCallMakesThePuppet(t *testing.T) {
	f := &fakeSock{}
	b := newFakeInbox(t)
	getenv := claudeEnv(t, serve(t, f), b)
	peer, _ := claudeServer(t, getenv)
	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	subs := f.subscribes
	f.mu.Unlock()
	if subs != 0 {
		t.Fatalf("%d subscribes before the session had a puppet", subs)
	}
	callTool(t, peer, 2, nil)
	waitFor(t, "the reply in the inbox", func() bool { return len(b.messages()) == 1 })
}

// The puppet takes the session's /rename title, and is archived when the
// session's server ends.
func TestClaudeInboundRenamesForTheTitleAndArchivesAtExit(t *testing.T) {
	f := &fakeSock{}
	getenv := claudeEnv(t, serve(t, f), newFakeInbox(t))
	cacheClaudeSession(t, getenv, claudeSID)
	writeTitle(t, getenv, "Fix the parser")
	_, stop := claudeServer(t, getenv)
	waitFor(t, "the rename", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.renames) == 1
	})
	stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasPrefix(f.renames[0], "Bearer sock_pt_projdev_secret Fix the parser on ") {
		t.Fatalf("renames %v", f.renames)
	}
	if !slices.Equal(f.archives, []string{"archive proj-puppet"}) {
		t.Fatalf("archives %v", f.archives)
	}
}

// A Claude Code session puppet whose server is gone without archiving it is
// archived by the next server to start; a running one is not.
func TestClaudeInboundArchivesSessionsWhoseServerDied(t *testing.T) {
	f := &fakeSock{}
	getenv := claudeEnv(t, serve(t, f), newFakeInbox(t))
	cacheClaudeSession(t, getenv, "dead-session")
	claudeServer(t, getenv)
	waitFor(t, "the archive", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.archives) == 1
	})
}

// Without a machine key every session of the project is the same puppet, and
// the hub hands each subscriber every delivery: one reply would be acted on
// in all of them (security review 2026-10-16). Such a server serves the
// tools and never listens.
func TestClaudeInboundWithAPuppetTokenDoesNotListen(t *testing.T) {
	f := &fakeSock{}
	b := newFakeInbox(t)
	base := serve(t, f)
	proj := gitRepo(t, filepath.Join(t.TempDir(), "tokproj"))
	t.Chdir(proj)
	getenv := func(k string) string {
		switch k {
		case "CLAUDE_CODE_SESSION_ID":
			return claudeSID
		case "CLAUDE_PROJECT_DIR":
			return proj
		case "CLAUDE_CODE_MESSAGING_SOCKET":
			return b.path
		}
		return base(k)
	}
	peer, _ := claudeServer(t, getenv)
	if res := callTool(t, peer, 2, nil); res["isError"] == true {
		t.Fatalf("whoami: %v", res)
	}
	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribes != 0 || len(f.creates) != 0 {
		t.Fatalf("subscribes %d, creates %v", f.subscribes, f.creates)
	}
}

// The prompt says the owner sent it, so only the owner reaches the session:
// SOCK_CHANNEL_ALLOW, meant for --channel, adds no one here (security review
// 2026-10-16).
func TestClaudeInboundIgnoresTheChannelAllowlist(t *testing.T) {
	f := &fakeSock{}
	b := newFakeInbox(t)
	env := claudeEnv(t, serve(t, f), b)
	getenv := func(k string) string {
		if k == "SOCK_CHANNEL_ALLOW" {
			return "stranger"
		}
		return env(k)
	}
	cacheClaudeSession(t, getenv, claudeSID)
	claudeServer(t, getenv)
	waitFor(t, "the reply's ack", func() bool { return slices.Contains(f.acked(), "m2") })
	time.Sleep(200 * time.Millisecond)
	for _, m := range b.messages() {
		if strings.Contains(m, "obey me") {
			t.Fatal("a SOCK_CHANNEL_ALLOW sender reached the session as the owner")
		}
	}
}

// A session without an inbox still holds its run lock and archives at exit,
// so another server's reconcile never archives it while it runs.
func TestClaudeSessionWithoutAnInboxStillHoldsItsRunLock(t *testing.T) {
	f := &fakeSock{}
	env := claudeEnv(t, serve(t, f), newFakeInbox(t))
	getenv := func(k string) string {
		if k == "CLAUDE_CODE_MESSAGING_SOCKET" {
			return ""
		}
		return env(k)
	}
	cacheClaudeSession(t, getenv, claudeSID)
	_, stop := claudeServer(t, getenv)
	key, err := sockclient.LoadConfig(getenv, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := client.ProjectsDir(getenv, key)
	waitFor(t, "the run lock", func() bool {
		unlock, held := tryRunLock(dir, client.Session{Platform: client.ClaudeCode, ID: claudeSID})
		if !held {
			unlock()
		}
		return held
	})
	stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribes != 0 || !slices.Equal(f.archives, []string{"archive proj-puppet"}) {
		t.Fatalf("subscribes %d, archives %v", f.subscribes, f.archives)
	}
}

func TestClaudeInboundExcludesTheOtherConsumers(t *testing.T) {
	for _, extra := range []string{"--channel", "--codex-inbound"} {
		err := run(t.Context(), []string{"--claude-inbound", extra}, func(string) string { return "" }, "", io.Discard, nil)
		if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Fatalf("%s: %v", extra, err)
		}
	}
}

// The session's first send may be a hook's ask (sock send --as-puppet
// --session), which makes the puppet outside this server: the listener
// finds it in the shared cache and listens.
func TestClaudeInboundListensForAPuppetAHookMade(t *testing.T) {
	f := &fakeSock{}
	b := newFakeInbox(t)
	getenv := claudeEnv(t, serve(t, f), b)
	claudeServer(t, getenv)
	time.Sleep(200 * time.Millisecond)
	cacheClaudeSession(t, getenv, claudeSID)
	waitFor(t, "the reply in the inbox", func() bool { return len(b.messages()) == 1 })
}

// configPath is the sock config file getenv's server reads.
func configPath(getenv func(string) string) string {
	return filepath.Join(getenv("XDG_CONFIG_HOME"), "sock", "config.toml")
}

// integrations-vzq: `sock machine-key use --force` replaces the key a running
// server started with. The hub rejects the old one; the server reads the file
// again and makes the session's puppet with the new key, without a reload.
// A file that still holds the rejected key gets the error, after one try.
func TestSessionPuppetsFollowAReplacedMachineKey(t *testing.T) {
	const old, fresh = "sock_mk_dev_machinesecret", "sock_mk_dev_newsecret"
	for name, start := range map[string]func(*testing.T, *fakeSock) (mcp.Connection, func(string) string, map[string]any){
		"claude": func(t *testing.T, f *fakeSock) (mcp.Connection, func(string) string, map[string]any) {
			getenv := claudeEnv(t, serve(t, f), newFakeInbox(t))
			peer, _ := claudeServer(t, getenv)
			return peer, getenv, nil
		},
		"codex": func(t *testing.T, f *fakeSock) (mcp.Connection, func(string) string, map[string]any) {
			root := gitRepo(t, filepath.Join(t.TempDir(), "plugins", "sock", "1.0.0"))
			proj := gitRepo(t, filepath.Join(t.TempDir(), "codexproj"))
			getenv := codexEnv(t, serve(t, f), "", root, root)
			peer, _ := handshake(t, []string{"--outbound-only"}, getenv)
			return peer, getenv, codexMeta(proj, "dddddddd-4444")
		},
	} {
		t.Run(name+"/replaced", func(t *testing.T) {
			f := &fakeSock{deadKey: old}
			peer, getenv, meta := start(t, f)
			body, err := os.ReadFile(configPath(getenv))
			if err != nil {
				t.Fatal(err)
			}
			writeConfig(t, configPath(getenv), strings.Replace(string(body), old, fresh, 1))
			for i := range 2 {
				if res := callTool(t, peer, float64(2+i), meta); res["isError"] == true {
					t.Fatalf("whoami %d: %v", i, res)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if !slices.Equal(f.createAuths, []string{"Bearer " + old, "Bearer " + fresh}) {
				t.Fatalf("creates as %v", f.createAuths)
			}
			for _, a := range f.auths {
				if a != "Bearer sock_pt_projdev_secret" {
					t.Fatalf("a call went out as %q", a)
				}
			}
		})
		t.Run(name+"/unchanged", func(t *testing.T) {
			f := &fakeSock{deadKey: old}
			peer, _, meta := start(t, f)
			for i := range 2 {
				res := callTool(t, peer, float64(2+i), meta)
				if res["isError"] != true || strings.Contains(fmt.Sprint(res["content"]), "machinesecret") {
					t.Fatalf("whoami %d: %v", i, res)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if !slices.Equal(f.createAuths, []string{"Bearer " + old, "Bearer " + old}) {
				t.Fatalf("creates as %v: want one try per call", f.createAuths)
			}
		})
	}
}

// fakePuppetAPI answers every call with err, or with its key when err is nil.
type fakePuppetAPI struct {
	key   string
	err   error
	calls *[]string
}

func (a fakePuppetAPI) answer(call string) error {
	*a.calls = append(*a.calls, call+" "+a.key)
	return a.err
}

func (a fakePuppetAPI) CreatePuppet(context.Context, string, string) (string, string, error) {
	return "id", "sock_pt_" + a.key, a.answer("create")
}

func (a fakePuppetAPI) ListPuppets(context.Context) ([]client.PuppetInfo, error) {
	return []client.PuppetInfo{{ID: a.key}}, a.answer("list")
}

func (a fakePuppetAPI) MintPuppetToken(context.Context, string, string) (string, error) {
	return "sock_pt_" + a.key, a.answer("mint")
}

func (a fakePuppetAPI) ErasePuppet(context.Context, string) error     { return a.answer("erase") }
func (a fakePuppetAPI) ArchivePuppet(context.Context, string) error   { return a.answer("archive") }
func (a fakePuppetAPI) UnarchivePuppet(context.Context, string) error { return a.answer("unarchive") }

// Only a rejected key, replaced in the file by another machine key for the
// same server, is swapped, and the call is tried once more with it. Any other
// failure, or any other file, returns the first answer.
func TestRekeyedPuppetsSwapOnlyAReplacedKeyForTheSameServer(t *testing.T) {
	unauthenticated := connect.NewError(connect.CodeUnauthenticated, errors.New("no"))
	key := client.Config{URL: "https://hub.test", Token: "sock_mk_old"}
	for name, tc := range map[string]struct {
		first  error
		file   client.Config
		read   error
		want   []string
		wantOK bool
	}{
		"replaced":             {unauthenticated, client.Config{URL: key.URL, Token: "sock_mk_new"}, nil, []string{"mint sock_mk_old", "mint sock_mk_new"}, true},
		"same key":             {unauthenticated, key, nil, []string{"mint sock_mk_old"}, false},
		"other server":         {unauthenticated, client.Config{URL: "https://other.test", Token: "sock_mk_new"}, nil, []string{"mint sock_mk_old"}, false},
		"not a key":            {unauthenticated, client.Config{URL: key.URL, Token: "sock_pt_new"}, nil, []string{"mint sock_mk_old"}, false},
		"unreadable":           {unauthenticated, client.Config{}, errors.New("config: parse"), []string{"mint sock_mk_old"}, false},
		"not rejected":         {connect.NewError(connect.CodeUnavailable, errors.New("down")), client.Config{URL: key.URL, Token: "sock_mk_new"}, nil, []string{"mint sock_mk_old"}, false},
		"denied, not rejected": {connect.NewError(connect.CodePermissionDenied, errors.New("no")), client.Config{URL: key.URL, Token: "sock_mk_new"}, nil, []string{"mint sock_mk_old"}, false},
	} {
		var calls []string
		reads := 0
		r := newRekeyedPuppets(key, func() (client.Config, error) { reads++; return tc.file, tc.read }, func(c client.Config) client.PuppetAPI {
			a := fakePuppetAPI{key: c.Token, calls: &calls}
			if c.Token == key.Token {
				a.err = tc.first
			}
			return a
		}, nil)
		tok, err := r.MintPuppetToken(t.Context(), "id", "l")
		if !slices.Equal(calls, tc.want) || (err == nil) != tc.wantOK {
			t.Errorf("%s: calls %v, token %q, err %v", name, calls, tok, err)
		}
		if tc.wantOK && tok != "sock_pt_sock_mk_new" {
			t.Errorf("%s: token %q", name, tok)
		}
		if wantReads := map[bool]int{true: 1, false: 0}[errors.Is(tc.first, unauthenticated)]; reads != wantReads {
			t.Errorf("%s: read the file %d times, want %d", name, reads, wantReads)
		}
		// once swapped, the next call goes straight to the new key
		if tc.wantOK {
			calls = nil
			if _, err := r.ListPuppets(t.Context()); err != nil || !slices.Equal(calls, []string{"list sock_mk_new"}) || reads != 1 {
				t.Errorf("%s: after the swap: calls %v, reads %d, err %v", name, calls, reads, err)
			}
		}
	}
}
