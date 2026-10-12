// Package sock is sock-mcp's view of the shared sock client: env config, the
// retry policy for interactive tool calls, and transient/permanent errors.
package sock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	client "github.com/guygrigsby/sock-client"
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

// LoadConfig reads SOCK_URL and SOCK_TOKEN through getenv and validates them.
func LoadConfig(getenv func(string) string) (client.Config, error) {
	c := client.Config{URL: client.DefaultURL, Token: strings.TrimSpace(getenv("SOCK_TOKEN"))}
	if v := getenv("SOCK_URL"); v != "" {
		c.URL = v
	}
	if err := client.Validate(c); err != nil {
		return client.Config{}, err
	}
	return c, nil
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
