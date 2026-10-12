// Package sock is the sock API client: config, an authenticated Connect
// transport, retries and error text.
package sock

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"unicode"
)

// DefaultURL is the public sock hub.
const DefaultURL = "https://api.sock.aeryx.ai"

// Config is what the server needs to reach sock.
type Config struct {
	URL   string
	Token string
}

// LoadConfig reads SOCK_URL and SOCK_TOKEN through getenv and validates them.
// Error text never echoes the URL or the token.
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{URL: DefaultURL, Token: strings.TrimSpace(getenv("SOCK_TOKEN"))}
	if v := getenv("SOCK_URL"); v != "" {
		c.URL = v
	}
	if c.Token == "" {
		return Config{}, errors.New("no token: set SOCK_TOKEN to a puppet token (sock_pt_...)")
	}
	if strings.ContainsFunc(c.Token, unicode.IsSpace) {
		return Config{}, errors.New("token contains whitespace; check for a stray line break or a second value")
	}
	if !strings.HasPrefix(c.Token, "sock_pt_") && !strings.HasPrefix(c.Token, "sock_st_") {
		return Config{}, errors.New("token must start with sock_pt_ or sock_st_")
	}
	if err := validateURL(c.URL); err != nil {
		return Config{}, err
	}
	return c, nil
}

func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err // url.Error repeats the input; keep only the reason
		}
		return fmt.Errorf("SOCK_URL is not a valid URL: %w", err)
	}
	if u.User != nil {
		return errors.New("SOCK_URL must not contain a user or password; put the token in SOCK_TOKEN")
	}
	// A scheme-less "user:pass@host" parses as scheme "user" with the rest in Opaque.
	if u.Host == "" || u.Opaque != "" {
		return errors.New("SOCK_URL is not a valid absolute https URL")
	}
	if u.Scheme == "https" || (u.Scheme == "http" && isLoopback(u.Hostname())) {
		return nil
	}
	return fmt.Errorf("SOCK_URL %s://%s must use https (http only for loopback)", u.Scheme, u.Host)
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
