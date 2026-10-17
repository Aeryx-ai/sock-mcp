package sockclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"

	client "github.com/guygrigsby/sock-client"
)

// FuzzClassify checks every error gets exactly one of the two prefixes, the
// one Transient (or a deadline) picks, and still unwraps to the original.
func FuzzClassify(f *testing.F) {
	f.Add(uint32(connect.CodeUnavailable), int32(0), "down", false)
	f.Add(uint32(connect.CodeResourceExhausted), int32(sockv1.ErrorReason_ERROR_REASON_RATE_LIMITED), "", false)
	f.Add(uint32(connect.CodePermissionDenied), int32(sockv1.ErrorReason_ERROR_REASON_NOT_A_MEMBER), "transient: fake", false)
	f.Add(uint32(0), int32(0), "", true)
	f.Fuzz(func(t *testing.T, code uint32, reason int32, msg string, deadline bool) {
		ce := connect.NewError(connect.Code(code), errors.New(msg))
		if reason != 0 {
			d, derr := connect.NewErrorDetail(&sockv1.ErrorInfo{Reason: sockv1.ErrorReason(reason)})
			if derr != nil {
				t.Fatal(derr)
			}
			ce.AddDetail(d)
		}
		var err error = ce
		if deadline {
			err = fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
		}
		got := Classify(err)
		want := "permanent: "
		if client.Transient(err) || errors.Is(err, context.DeadlineExceeded) {
			want = "transient: "
		}
		if !strings.HasPrefix(got.Error(), want) {
			t.Fatalf("Classify = %q, want prefix %q", got, want)
		}
		if !errors.Is(got, err) || client.Reason(got) != client.Reason(err) {
			t.Fatal("classified error lost the original")
		}
		if Classify(nil) != nil {
			t.Fatal("Classify(nil) != nil")
		}
	})
}
