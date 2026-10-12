package sock

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

// maxRetryAfterSecs bounds parsed Retry-After seconds so Duration cannot overflow.
const maxRetryAfterSecs = 1_000_000_000

// Retrier retries transient failures with exponential backoff. Retry-After
// from the server overrides the computed delay, capped at Max so a tool call
// cannot stall the agent.
type Retrier struct {
	Attempts int
	Base     time.Duration
	Max      time.Duration
	Sleep    func(context.Context, time.Duration) error
	Now      func() time.Time
}

// DefaultRetrier is 4 attempts, 500ms doubling, capped at 10s.
func DefaultRetrier() Retrier {
	return Retrier{Attempts: 4, Base: 500 * time.Millisecond, Max: 10 * time.Second}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Do calls fn until it succeeds, fails permanently or attempts run out. The
// zero value is safe: Attempts below 1 count as 1, nil Sleep and Now default,
// a zero Max is 10s.
func (r Retrier) Do(ctx context.Context, fn func(context.Context) error) error {
	attempts := max(r.Attempts, 1)
	sleep, now, maxWait := r.Sleep, r.Now, r.Max
	if sleep == nil {
		sleep = sleepCtx
	}
	if now == nil {
		now = time.Now
	}
	if maxWait <= 0 {
		maxWait = 10 * time.Second
	}
	delay := min(r.Base, maxWait)
	var err error
	for attempt := range attempts {
		err = fn(ctx)
		if err == nil || !Transient(err) || attempt == attempts-1 {
			return err
		}
		wait := delay
		if ra, ok := RetryAfter(err, now()); ok {
			wait = min(ra, maxWait)
		}
		if serr := sleep(ctx, wait); serr != nil {
			return serr
		}
		delay = min(delay*2, maxWait)
	}
	return err
}

// Transient reports whether retrying the same request can succeed.
func Transient(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		return true
	case connect.CodeResourceExhausted:
		return Reason(err) == sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED
	}
	return false
}

// RetryAfter reads Retry-After from a Connect error, in delta-seconds or
// HTTP-date form. A date in the past means retry now.
func RetryAfter(err error, now time.Time) (time.Duration, bool) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return 0, false
	}
	v := strings.TrimSpace(ce.Meta().Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if secs, perr := strconv.ParseInt(v, 10, 64); perr == nil && secs >= 0 {
		return time.Duration(min(secs, maxRetryAfterSecs)) * time.Second, true
	}
	if at, perr := time.Parse(http.TimeFormat, v); perr == nil {
		return max(at.Sub(now), 0), true
	}
	return 0, false
}
