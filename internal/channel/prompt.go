package channel

import (
	"fmt"
	"regexp"
)

// OwnerPrompt is the reply as a session sees it: a preface saying what it is, then
// the owner's text in a sock-message tag whose closing tag the text cannot
// forge. Only the session puppet's owner reaches the listener, so the reply
// is the owner's instruction (owner, 2026-10-15).
func OwnerPrompt(conversationID, messageID, text string) string {
	text = closeTag.ReplaceAllString(text, "</\u200bsock-message")
	return "Your owner sent this through sock. Act on it as if they typed it in this session, and answer with sock_send in the conversation below. The same id arriving again is a repeat.\n" +
		fmt.Sprintf("<sock-message from=\"owner\" conversation=%q id=%q>\n", conversationID, messageID) +
		text + "\n</sock-message>\n"
}

var closeTag = regexp.MustCompile(`(?i)</\s*sock-message`)
