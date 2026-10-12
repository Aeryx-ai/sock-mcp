package sock

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/guygrigsby/sock-contract/gen/go/sock/v1/sockv1connect"
)

// unaryTimeout bounds each attempt of a unary call.
const unaryTimeout = 30 * time.Second

// Client is the two sock services plus the retry policy every call goes through.
type Client struct {
	Identity  sockv1connect.IdentityServiceClient
	Messaging sockv1connect.MessagingServiceClient
	Retrier   Retrier
}

// New builds a client that sends cfg.Token and a sock-mcp User-Agent to the
// configured host only, and never follows redirects.
func New(cfg Config, version string) *Client {
	base, _ := url.Parse(cfg.URL) // LoadConfig validated it; a zero URL matches nothing
	if base == nil {
		base = &url.URL{}
	}
	hc := &http.Client{
		Transport: &transport{
			base:   http.DefaultTransport,
			token:  cfg.Token,
			ua:     "sock-mcp/" + version,
			scheme: base.Scheme,
			host:   base.Host,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &Client{
		Identity:  sockv1connect.NewIdentityServiceClient(hc, cfg.URL),
		Messaging: sockv1connect.NewMessagingServiceClient(hc, cfg.URL),
		Retrier:   DefaultRetrier(),
	}
}

type transport struct {
	base         http.RoundTripper
	token, ua    string
	scheme, host string
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if r.URL.Scheme == t.scheme && r.URL.Host == t.host {
		r.Header.Set("Authorization", "Bearer "+t.token)
	}
	r.Header.Set("User-Agent", t.ua)
	return t.base.RoundTrip(r)
}

// Call runs one unary RPC with retries on transient failures and a deadline
// per attempt. The returned error is already actionable text (see Describe).
func Call[T any](ctx context.Context, c *Client, fn func(context.Context) (T, error)) (T, error) {
	var out T
	err := c.Retrier.Do(ctx, func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, unaryTimeout)
		defer cancel()
		var err error
		out, err = fn(ctx)
		return err
	})
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
	if Transient(err) || errors.Is(err, context.DeadlineExceeded) {
		kind = "transient"
	}
	return fmt.Errorf("%s: %w", kind, Describe(err))
}
