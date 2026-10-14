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

// projectPuppet is sock-client's ProjectPuppet with sock-mcp's options: a
// machine key becomes the token of this project's puppet (ADR 10).
func projectPuppet(ctx context.Context, cfg client.Config, getenv func(string) string, stderr io.Writer) (client.Config, error) {
	return client.ProjectPuppet(ctx, cfg, puppetOptions(getenv, stderr))
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

// digestEnv marks the owner's digest channel session, the one session that
// may message the owner without a conversation. An operator sets it in that
// session's server config; nothing a model controls can.
const digestEnv = "SOCK_MCP_DIGEST_CHANNEL"

// takeDigestChannel reports whether digestEnv is exactly "1" and removes it
// from the process environment, so no child process (git, for one) inherits
// it. main calls it once, before anything else runs.
func takeDigestChannel() bool {
	on := os.Getenv(digestEnv) == "1"
	_ = os.Unsetenv(digestEnv)
	return on
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, takeDigestChannel(), os.Stderr, &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, "sock-mcp:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, digest bool, stderr io.Writer, transport mcp.Transport) error {
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
	cfg, err := client.ResolvePuppet(ctx, puppetOptions(getenv, stderr))
	if err != nil {
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
	policy := tools.Policy{NotifyOwner: digest}
	if *outbound {
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
