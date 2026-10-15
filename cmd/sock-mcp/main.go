// Command sock-mcp serves sock messaging to an agent over MCP on stdio. With
// --channel it also pushes inbound messages into a Claude Code session.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	client "github.com/guygrigsby/sock-client"
	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"

	"github.com/guygrigsby/sock-mcp/internal/asks"
	"github.com/guygrigsby/sock-mcp/internal/channel"
	"github.com/guygrigsby/sock-mcp/internal/replies"
	"github.com/guygrigsby/sock-mcp/sockclient"
	"github.com/guygrigsby/sock-mcp/tools"
)

var version = "dev"

// newHandshakeRevision is the first MCP revision whose handshake (server/discover)
// Claude Code does not deliver channel messages on.
const newHandshakeRevision = "2026-07-28"

// legacyHandshakeVersions is every protocol version the SDK supports before
// newHandshakeRevision. Claude Code 2.1.292 and later negotiates the newest
// revision a stdio server offers, and a server on 2026-07-28 is not registered
// as a channel; offering only the earlier ones makes it use the handshake that
// has a channel delivery path. Lift this when Claude Code's changelog adds
// channel delivery on 2026-07-28 (integrations-pvq).
func legacyHandshakeVersions() []string {
	var out []string
	for _, v := range mcp.SupportedProtocolVersions() {
		if v < newHandshakeRevision {
			out = append(out, v)
		}
	}
	return out
}

// projectPuppet is sock-client's ProjectPuppet with sock-mcp's options: a
// machine key becomes the token of this project's puppet (ADR 10).
func projectPuppet(ctx context.Context, cfg client.Config, getenv func(string) string, stderr io.Writer) (client.Config, error) {
	return client.ProjectPuppet(ctx, cfg, puppetOptions(getenv, stderr))
}

// sandboxStateMeta is the experimental capability that makes Codex put the
// session's sandbox state, sandboxCwd among it, in every tools/call _meta.
const sandboxStateMeta = "codex/sandbox-state-meta"

// perCallProject reports whether this is a Codex plugin server holding a
// machine key: it starts in the plugin root, so the project is known only
// from each call's sandboxCwd.
func perCallProject(cfg client.Config, getenv func(string) string) bool {
	return client.TokenKind(cfg.Token) == client.KindMachineKey && getenv("PLUGIN_ROOT") != "" && getenv("CLAUDE_PROJECT_DIR") == ""
}

// projectClients runs each call as the puppet of the call's sandboxCwd,
// minting it with the machine key the first time and reusing it after. A call
// without one gets ProjectPuppet's answer for the plugin root.
type projectClients struct {
	key  client.Config
	opts client.PuppetOptions

	mu    sync.Mutex
	byDir map[string]*sockclient.Client
}

func (p *projectClients) For(ctx context.Context, req *mcp.CallToolRequest) (*sockclient.Client, error) {
	dir := sandboxCwd(req)
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.byDir[dir]; ok {
		return c, nil
	}
	o := p.opts
	o.WorkDir = dir
	cfg, err := client.ProjectPuppet(ctx, p.key, o)
	if err != nil {
		return nil, err
	}
	c := sockclient.New(cfg, version)
	p.byDir[dir] = c
	return c, nil
}

// codexThread is the Codex thread that made the call, from the turn metadata
// Codex puts in every tools/call _meta, or "" from any other client.
func codexThread(req *mcp.CallToolRequest) string {
	turn, _ := req.Params.GetMeta()["x-codex-turn-metadata"].(map[string]any)
	id, _ := turn["thread_id"].(string)
	return id
}

// sandboxCwd is the local path of the call's codex/sandbox-state-meta
// sandboxCwd, or "" when Codex sent none. Codex sets _meta, not the model.
func sandboxCwd(req *mcp.CallToolRequest) string {
	state, _ := req.Params.GetMeta()[sandboxStateMeta].(map[string]any)
	raw, _ := state["sandboxCwd"].(string)
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "file" || !filepath.IsAbs(u.Path) {
		return ""
	}
	return filepath.Clean(u.Path)
}

// puppetOptions are sock-mcp's inputs to sock-client's puppet resolution: the
// project is CLAUDE_PROJECT_DIR, else the working directory.
func puppetOptions(getenv func(string) string, stderr io.Writer) client.PuppetOptions {
	return client.PuppetOptions{
		Getenv:        getenv,
		NoProjectHint: "set CLAUDE_PROJECT_DIR or start sock-mcp in the project's repository",
		UserAgent:     "sock-mcp/" + version,
		Log:           sockclient.Prefixed(func(f string, a ...any) { _, _ = fmt.Fprintf(stderr, f+"\n", a...) }),
	}
}

// outboundInstructions tells the model the inbound tools are absent on purpose,
// so it does not go looking for them.
const outboundInstructions = "This server is outbound only: it has sock_send, sock_whoami and sock_list_conversations. " +
	"There is no sock_fetch or sock_ack here; incoming messages are handled elsewhere, so do not look for them."

// ownerEnv lets this session message the owner without a conversation. It is
// set in the server's config (the Claude Code plugin's message_owner option);
// nothing a model controls can set it. The hub's owner-direct rate limit
// (ADR 20) bounds what an opted-in session sends.
const ownerEnv = "SOCK_MCP_MESSAGE_OWNER"

// takeMessageOwner reports whether ownerEnv is exactly "1" or "true" (what
// Claude Code substitutes for a boolean plugin option) and removes it
// from the process environment, so no child process (git, for one) inherits
// it. main calls it once, before anything else runs.
func takeMessageOwner() bool {
	v := os.Getenv(ownerEnv)
	on := v == "1" || v == "true"
	_ = os.Unsetenv(ownerEnv)
	return on
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, takeMessageOwner(), os.Stderr, &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, "sock-mcp:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, messageOwner bool, stderr io.Writer, transport mcp.Transport) error {
	if len(args) > 0 && args[0] == "replied" {
		return replied(args[1:], getenv, stderr)
	}
	fs := flag.NewFlagSet("sock-mcp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	channelMode := fs.Bool("channel", false, "also push inbound messages to a Claude Code session (claude/channel)")
	outbound := fs.Bool("outbound-only", false, "serve only sock_whoami, sock_list_conversations and sock_send (no sock_fetch or sock_ack)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outbound && *channelMode {
		return errors.New("--outbound-only and --channel cannot be combined: the channel consumes deliveries")
	}
	po := puppetOptions(getenv, stderr)
	cfg, err := client.LoadPuppetConfig(po)
	if errors.Is(err, client.ErrNoPuppetCredential) {
		// Nobody has signed in yet. Failing here shows nothing in a tool
		// result, so serve the tools and answer each call with what to do.
		return serveWithoutCredential(ctx, *outbound, stderr, transport)
	}
	if err != nil {
		return err
	}
	var perCall *projectClients
	if *outbound && perCallProject(cfg, getenv) {
		perCall = &projectClients{key: cfg, opts: po, byDir: map[string]*sockclient.Client{}}
	} else if cfg, err = client.ProjectPuppet(ctx, cfg, po); err != nil {
		return err
	}
	client := sockclient.New(cfg, version)
	opts := &mcp.ServerOptions{}
	if *outbound {
		opts.Instructions = outboundInstructions
	}
	if perCall != nil {
		opts.Capabilities = &mcp.ServerCapabilities{Experimental: map[string]any{sandboxStateMeta: map[string]any{}}}
	}
	if *channelMode {
		opts.SupportedProtocolVersions = legacyHandshakeVersions()
	}
	var ch *channel.Channel
	tr := &channel.Transport{Inner: transport}
	policy := tools.Policy{NotifyOwner: messageOwner}
	// Best effort: the send already posted, so a failed record is only logged.
	repliesDir, repliesErr := replies.Dir(getenv)
	asksDir, asksErr := asks.Dir(getenv)
	policy.Replied = func(req *mcp.CallToolRequest, conversationID string) {
		now := time.Now()
		if repliesErr == nil {
			if err := replies.Record(repliesDir, conversationID, now); err != nil {
				_, _ = fmt.Fprintln(stderr, "sock-mcp:", err)
			}
		}
		if session := codexThread(req); session != "" && asksErr == nil {
			if err := asks.Record(asksDir, conversationID, session, now); err != nil {
				_, _ = fmt.Fprintln(stderr, "sock-mcp:", err)
			}
		}
	}
	if *channelMode {
		me, allow, err := allowlist(ctx, client, getenv)
		if err != nil {
			return err
		}
		dir, err := channel.CountsDir(getenv, me)
		if err != nil {
			return err
		}
		counts, err := channel.OpenCounts(dir, time.Now())
		if err != nil {
			return err
		}
		policy.Acked = counts.Forget
		opts.Capabilities = &mcp.ServerCapabilities{
			Experimental: map[string]any{"claude/channel": map[string]any{}},
		}
		opts.Instructions = channel.Instructions
		ch = &channel.Channel{Client: client, Allow: allow, Push: tr.Push, Counts: counts, Log: slog.New(slog.NewTextHandler(stderr, nil))}
	}
	// Events pushed before the client finishes initializing can be dropped,
	// so the subscription starts at notifications/initialized.
	chErr := make(chan error, 1)
	if ch != nil {
		opts.InitializedHandler = func(context.Context, *mcp.InitializedRequest) {
			go func() { chErr <- ch.Run(ctx) }()
		}
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "sock", Version: version}, opts)
	if perCall != nil {
		tools.RegisterOutboundFor(s, perCall.For, policy)
	} else if *outbound {
		tools.RegisterOutbound(s, client, policy)
	} else {
		tools.Register(s, client, policy)
	}
	ss, err := s.Connect(ctx, tr, nil)
	if err != nil {
		return err
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- ss.Wait() }()
	select {
	case err := <-waitErr:
		return err
	case err := <-chErr:
		_ = ss.Close()
		return err
	}
}

// allowlist is this account's id and the sender ids the channel may push: the
// puppet's owner plus SOCK_CHANNEL_ALLOW (comma separated account ids). Empty
// fails closed.
func allowlist(ctx context.Context, c *sockclient.Client, getenv func(string) string) (string, map[string]bool, error) {
	allow := map[string]bool{}
	me, err := sockclient.Call(ctx, c, func(ctx context.Context) (*sockv1.Account, error) {
		res, err := c.Identity.GetMe(ctx, connect.NewRequest(&sockv1.GetMeRequest{}))
		if err != nil {
			return nil, err
		}
		return res.Msg.GetAccount(), nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("channel: look up own account: %w", err)
	}
	if p := me.GetPuppet(); p != nil {
		allow[p.GetOwnerId()] = true
	}
	for id := range strings.SplitSeq(getenv("SOCK_CHANNEL_ALLOW"), ",") {
		if id = strings.TrimSpace(id); id != "" {
			allow[id] = true
		}
	}
	if len(allow) == 0 {
		return "", nil, errors.New("channel: no allowed senders: use a puppet token, or set SOCK_CHANNEL_ALLOW to account ids")
	}
	return me.GetId(), allow, nil
}

// replied is `sock-mcp replied --conversation <id> --since <unix seconds>`:
// exit 0 when a send posted in that conversation at or after --since, an
// error otherwise. A headless runner calls it after the agent exits, so a
// message is acknowledged only once its reply went out.
func replied(args []string, getenv func(string) string, stderr io.Writer) error {
	fs := flag.NewFlagSet("sock-mcp replied", flag.ContinueOnError)
	fs.SetOutput(stderr)
	conv := fs.String("conversation", "", "conversation id the reply belongs in")
	since := fs.String("since", "", "unix time in seconds (fractions allowed): a reply at or after it counts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *conv == "" || *since == "" {
		return errors.New("replied: --conversation and --since are required")
	}
	secs, err := strconv.ParseFloat(*since, 64)
	if err != nil || secs < 0 {
		return fmt.Errorf("replied: --since must be unix seconds, got %q", *since)
	}
	dir, err := replies.Dir(getenv)
	if err != nil {
		return err
	}
	ok, err := replies.Since(dir, *conv, time.Unix(0, int64(secs*1e9)))
	if err != nil {
		return err
	}
	if !ok {
		return errNoReply
	}
	return nil
}

// errNoReply is replied's answer when nothing was posted.
var errNoReply = errors.New("no reply was posted in that conversation since then")

// serveWithoutCredential serves the same tools as a signed-in sock-mcp, and
// every call answers "permanent: <ErrNoPuppetCredential>". No stream is
// opened: there is nobody to listen as.
func serveWithoutCredential(ctx context.Context, outbound bool, stderr io.Writer, transport mcp.Transport) error {
	_, _ = fmt.Fprintln(stderr, "sock-mcp:", client.ErrNoPuppetCredential)
	s := mcp.NewServer(&mcp.Implementation{Name: "sock", Version: version}, &mcp.ServerOptions{})
	// handlers never run: the middleware below answers first
	idle := sockclient.New(client.Config{URL: client.DefaultURL}, version)
	if outbound {
		tools.RegisterOutbound(s, idle, tools.Policy{})
	} else {
		tools.Register(s, idle, tools.Policy{})
	}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "tools/call" {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{&mcp.TextContent{Text: "permanent: " + client.ErrNoPuppetCredential.Error()}},
				}, nil
			}
			return next(ctx, method, req)
		}
	})
	ss, err := s.Connect(ctx, transport, nil)
	if err != nil {
		return err
	}
	return ss.Wait()
}
