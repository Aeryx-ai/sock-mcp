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

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"

	"github.com/guygrigsby/sock-mcp/internal/channel"
	"github.com/guygrigsby/sock-mcp/internal/sock"
	"github.com/guygrigsby/sock-mcp/internal/tools"
)

var version = "dev"

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
	cfg, err := sock.LoadConfig(getenv)
	if err != nil {
		return err
	}
	client := sock.New(cfg, version)
	opts := &mcp.ServerOptions{}
	if *outbound {
		opts.Instructions = outboundInstructions
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
func allowlist(ctx context.Context, c *sock.Client, getenv func(string) string) (map[string]bool, error) {
	allow := map[string]bool{}
	me, err := sock.Call(ctx, c, func(ctx context.Context) (*sockv1.Account, error) {
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
