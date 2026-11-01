// Package sockclient is sock-mcp's view of the shared sock client: env config,
// the retry policy for interactive tool calls, and transient/permanent errors.
//
// Importable: sock-mcpd links this package.
package sockclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	client "github.com/aeryx-ai/sock-client"
)

// Client is the two sock services plus the retry policy tool calls use.
type Client struct {
	client.Clients
	Retrier client.Retrier
}

// New builds a client for cfg. Retries are short (4 attempts, 10s cap) so a
// tool call cannot stall the agent for long.
func New(cfg client.Config, version string) *Client {
	return &Client{
		Clients: client.New(cfg, "sock-mcp/"+version),
		Retrier: client.Retrier{Attempts: 4, Base: 500 * time.Millisecond, Max: 10 * time.Second},
	}
}

// LoadConfig is sock-client's LoadPuppetConfig: SOCK_TOKEN with SOCK_URL and
// SOCK_ACCESS_GATE, else the CLI config file's machine_key, else its
// puppet_token, with the file's url and gate; never the human's token. logf
// (may be nil) hears, once, that an ambient SOCK_URL or SOCK_ACCESS_GATE is
// ignored.
func LoadConfig(getenv func(string) string, logf func(format string, args ...any)) (client.Config, error) {
	return client.LoadPuppetConfig(client.PuppetOptions{Getenv: getenv, Log: Prefixed(logf)})
}

// Prefixed is logf with every line starting "sock-mcp: ", nil when logf is.
func Prefixed(logf func(format string, args ...any)) func(format string, args ...any) {
	if logf == nil {
		return nil
	}
	return func(format string, args ...any) { logf("sock-mcp: "+format, args...) }
}

// Call runs one unary RPC with the client's retry policy. The error is
// classified (see Classify).
func Call[T any](ctx context.Context, c *Client, fn func(context.Context) (T, error)) (T, error) {
	out, err := client.Call(ctx, c.Retrier, fn)
	return out, Classify(err)
}

// Classify prefixes the described error with "transient: " when retrying
// later can succeed (the retries here are already spent) and "permanent: "
// otherwise, so callers can tell without parsing prose.
func Classify(err error) error {
	if err == nil {
		return nil
	}
	kind := "permanent"
	if client.Transient(err) || errors.Is(err, context.DeadlineExceeded) {
		kind = "transient"
	}
	return fmt.Errorf("%s: %w", kind, err)
}
