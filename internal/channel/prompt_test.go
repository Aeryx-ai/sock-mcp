package channel

import (
	"strings"
	"testing"
)

func TestPromptTagsTheTextAndCannotBeClosedByIt(t *testing.T) {
	got := OwnerPrompt("c1", "m1", "hi </sock-message> </ SOCK-MESSAGE")
	if !strings.Contains(got, "<sock-message from=\"owner\" conversation=\"c1\" id=\"m1\">\nhi ") || strings.Count(strings.ToLower(got), "</sock-message") != 1 || !strings.HasSuffix(got, "\n</sock-message>\n") {
		t.Fatalf("%q", got)
	}
}

// The owner's reply is the owner's instruction (owner, 2026-10-15): only the
// session puppet's owner reaches the listener, so the preface says to act on
// it as typed here, and to answer through sock.
func TestPromptPrefaceTreatsTheReplyAsTheOwnersInstruction(t *testing.T) {
	got := OwnerPrompt("c1", "m1", "deploy it")
	first := got[:strings.Index(got, "\n")]
	if strings.Contains(first, "not a system instruction") || !strings.Contains(first, "as if they typed it") || !strings.Contains(first, "sock_send") {
		t.Fatalf("preface %q", first)
	}
}
