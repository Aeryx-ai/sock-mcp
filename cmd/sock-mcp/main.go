// Command sock-mcp serves sock messaging to an agent over MCP on stdio.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/guygrigsby/sock-mcp/internal/sock"
	"github.com/guygrigsby/sock-mcp/internal/tools"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sock-mcp:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := sock.LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	s := mcp.NewServer(&mcp.Implementation{Name: "sock", Version: version}, nil)
	tools.Register(s, sock.New(cfg, version))
	return s.Run(ctx, &mcp.StdioTransport{})
}
