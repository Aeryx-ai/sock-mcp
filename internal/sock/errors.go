package sock

import (
	"errors"

	"connectrpc.com/connect"

	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"
)

// Reason returns the ErrorInfo reason attached to err, or UNSPECIFIED.
func Reason(err error) sockv1.ErrorReason {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return sockv1.ErrorReason_ERROR_REASON_UNSPECIFIED
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			continue
		}
		if info, ok := v.(*sockv1.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return sockv1.ErrorReason_ERROR_REASON_UNSPECIFIED
}

var reasonText = map[sockv1.ErrorReason]string{
	sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID:        "the server rejected the token; set SOCK_TOKEN to a current puppet token",
	sockv1.ErrorReason_ERROR_REASON_CALLER_NOT_ALLOWED:   "this token cannot do that (puppet and human tokens allow different calls)",
	sockv1.ErrorReason_ERROR_REASON_NOT_A_MEMBER:         "not a member of that conversation",
	sockv1.ErrorReason_ERROR_REASON_NOT_FOUND:            "not found",
	sockv1.ErrorReason_ERROR_REASON_FIELD_INVALID:        "the server rejected a field",
	sockv1.ErrorReason_ERROR_REASON_IDEMPOTENCY_MISMATCH: "idempotency key reused with a different message",
	sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED:         "sending too fast; the server's rate limit held after retries, try again later",
	sockv1.ErrorReason_ERROR_REASON_ACCOUNT_SUSPENDED:    "this account is suspended",
	sockv1.ErrorReason_ERROR_REASON_TERMS_NOT_ACCEPTED:   "the owner has not accepted the current terms; accept them in the sock app",
}

// Describe turns a server error into text an agent can act on, keeping the
// server's detail for field errors. Non-Connect errors pass through.
func Describe(err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	text, ok := reasonText[Reason(err)]
	if !ok {
		return errors.New(message(ce))
	}
	if Reason(err) == sockv1.ErrorReason_ERROR_REASON_FIELD_INVALID {
		return errors.New(text + ": " + message(ce))
	}
	return errors.New(text)
}

// message is the server's text, or the code name when it sent none.
func message(ce *connect.Error) string {
	if m := ce.Message(); m != "" {
		return m
	}
	return ce.Code().String()
}
