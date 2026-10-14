// Package sockclient is sock-mcp's view of the shared sock client: env config,
// the retry policy for interactive tool calls, and transient/permanent errors.
//
// Importable: sock-mcpd links this package.
package sockclient

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

// LoadConfig resolves the configuration from one source, never a mix, so a
// token is only ever sent to the URL that came with it. With SOCK_TOKEN set it
// is SOCK_URL, SOCK_TOKEN and SOCK_ACCESS_GATE from getenv and the CLI's config
// file is not read. With SOCK_TOKEN unset (a host such as a Codex plugin starts
// the server without the environment) it is the url and access_gate of the
// CLI's config file (mode 0600 when it holds a secret) with its machine_key
// (`sock machine-key use` writes it), else its puppet_token (`sock puppets use`
// writes it); the human's token is never used. An ambient SOCK_URL or
// SOCK_ACCESS_GATE is then ignored, and logf (may be nil) says so once. The
// gate goes to the configured host only, as sock-client's transport sends it.
func LoadConfig(getenv func(string) string, logf func(format string, args ...any)) (client.Config, error) {
	c := client.Config{URL: client.DefaultURL, Token: strings.TrimSpace(getenv("SOCK_TOKEN")), AccessGate: strings.TrimSpace(getenv("SOCK_ACCESS_GATE"))}
	if v := getenv("SOCK_URL"); v != "" {
		c.URL = v
	}
	if c.Token == "" {
		if fc, path, err := fileConfig(getenv); err != nil {
			return client.Config{}, err
		} else if tok, err := fileToken(fc, path); err != nil {
			return client.Config{}, err
		} else if tok != "" {
			if logf != nil && (getenv("SOCK_URL") != "" || getenv("SOCK_ACCESS_GATE") != "") {
				logf("sock-mcp: ignoring SOCK_URL and SOCK_ACCESS_GATE: the token comes from %s, so the URL and gate do too", path)
			}
			c = client.Config{URL: client.DefaultURL, Token: tok, AccessGate: strings.TrimSpace(fc.AccessGate)}
			if fc.URL != "" {
				c.URL = fc.URL
			}
		}
	}
	if err := client.Validate(c); err != nil {
		return client.Config{}, err
	}
	return c, nil
}

// fileToken is the credential the config file gives sock-mcp: machine_key
// first, so each project gets its own puppet, then puppet_token. "" means
// neither is set. An error names the key, never its value.
func fileToken(fc client.FileConfig, path string) (string, error) {
	if mk := strings.TrimSpace(fc.MachineKey); mk != "" {
		if client.TokenKind(mk) != client.KindMachineKey {
			return "", fmt.Errorf("machine_key in %s is not a machine key (sock_mk_...); run sock machine-key use", path)
		}
		return mk, nil
	}
	pt := strings.TrimSpace(fc.PuppetToken)
	if pt != "" && client.TokenKind(pt) != client.KindPuppet {
		return "", fmt.Errorf("puppet_token in %s is not a puppet token (sock_pt_...); run sock puppets use <puppet id>", path)
	}
	return pt, nil
}

// fileConfig reads the CLI's config file. No home directory, or no file, is an
// empty config, not an error: the caller reports the missing token.
func fileConfig(getenv func(string) string) (client.FileConfig, string, error) {
	path, err := client.DefaultConfigPath(getenv)
	if err != nil {
		return client.FileConfig{}, "", nil
	}
	fc, err := client.LoadFileConfig(path)
	return fc, path, err
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
