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
	"os"
	"os/signal"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	client "github.com/guygrigsby/sock-client"
	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"

	"github.com/guygrigsby/sock-mcp/internal/channel"
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

// projectPuppet turns a machine key into the token of this project's puppet
// (ADR 10). Any other credential is returned as it is. The machine key reaches
// the hub only to make, list and erase puppets; every tool call goes out as the
// puppet. A failure never carries the key.
func projectPuppet(ctx context.Context, cfg client.Config, getenv func(string) string, stderr io.Writer) (client.Config, error) {
	if client.TokenKind(cfg.Token) != client.KindMachineKey {
		return cfg, nil
	}
	dir := getenv("CLAUDE_PROJECT_DIR")
	if dir == "" {
		var err error
		if dir, err = os.Getwd(); err != nil {
			return cfg, fmt.Errorf("project puppet: %w", err)
		}
	}
	projects := &client.Projects{
		API:  client.NewPuppetAPI(client.New(cfg, "sock-mcp/"+version), client.DefaultRetrier()),
		Dir:  client.ProjectsDir(getenv, cfg),
		Host: client.ShortHost(),
		Log:  func(f string, a ...any) { _, _ = fmt.Fprintf(stderr, "sock-mcp: "+f+"\n", a...) },
	}
	// a cached token the hub rejects (the puppet was erased) is replaced
	projects.Check = client.NewTokenCheck(cfg, "sock-mcp/"+version)
	// separate budgets: a slow sweep must not leave Ensure without time
	sweepCtx, cancelSweep := context.WithTimeout(ctx, 20*time.Second)
	projects.Sweep(sweepCtx)
	cancelSweep()
	ensureCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	tok, err := projects.Ensure(ensureCtx, dir)
	if err != nil {
		return cfg, errors.New(strings.ReplaceAll(err.Error(), cfg.Token, "[redacted]"))
	}
	cfg.Token = tok
	return cfg, nil
}

// outboundInstructions tells the model the inbound tools are absent on purpose,
// so it does not go looking for them.
const outboundInstructions = "This server is outbound only: it has sock_send, sock_whoami and sock_list_conversations. " +
	"There is no sock_fetch or sock_ack here; incoming messages are handled elsewhere, so do not look for them."

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr, &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, "sock-mcp:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer, transport mcp.Transport) error {
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
	cfg, err := sockclient.LoadConfig(getenv, func(f string, a ...any) { _, _ = fmt.Fprintf(stderr, f+"\n", a...) })
	if err != nil {
		return err
	}
	if cfg, err = projectPuppet(ctx, cfg, getenv, stderr); err != nil {
		return err
	}
	client := sockclient.New(cfg, version)
	opts := &mcp.ServerOptions{}
	if *outbound {
		opts.Instructions = outboundInstructions
	}
	if *channelMode {
		opts.SupportedProtocolVersions = legacyHandshakeVersions()
	}
	var ch *channel.Channel
	tr := &channel.Transport{Inner: transport}
	if *channelMode {
		allow, err := allowlist(ctx, client, getenv)
		if err != nil {
			return err
		}
		opts.Capabilities = &mcp.ServerCapabilities{
			Experimental: map[string]any{"claude/channel": map[string]any{}},
		}
		opts.Instructions = channel.Instructions
		ch = &channel.Channel{Client: client, Allow: allow, Push: tr.Push, Log: slog.New(slog.NewTextHandler(stderr, nil))}
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
	if *outbound {
		tools.RegisterOutbound(s, client)
	} else {
		tools.Register(s, client)
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

// allowlist is the sender ids the channel may push: the puppet's owner plus
// SOCK_CHANNEL_ALLOW (comma separated account ids). Empty fails closed.
func allowlist(ctx context.Context, c *sockclient.Client, getenv func(string) string) (map[string]bool, error) {
	allow := map[string]bool{}
	me, err := sockclient.Call(ctx, c, func(ctx context.Context) (*sockv1.Account, error) {
		res, err := c.Identity.GetMe(ctx, connect.NewRequest(&sockv1.GetMeRequest{}))
		if err != nil {
			return nil, err
		}
		return res.Msg.GetAccount(), nil
	})
	if err != nil {
		return nil, fmt.Errorf("channel: look up own account: %w", err)
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
		return nil, errors.New("channel: no allowed senders: use a puppet token, or set SOCK_CHANNEL_ALLOW to account ids")
	}
	return allow, nil
}
