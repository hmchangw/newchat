package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// messageLink builds the canonical deep link to a message from trusted inputs.
// Single source of truth for the link format, shared by the authoritative
// history fetch (fetcher_history.go) and the degraded-mode placeholder snapshot
// (handler.go) so the two paths can't drift. baseURL is operator-supplied
// (CHAT_BASE_URL); its trailing slash is trimmed so the link never doubles up.
func messageLink(baseURL, roomID, messageID string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(baseURL, "/"), roomID, messageID)
}

// validateContentSize caps a client-supplied body at maxContentBytes, naming the
// offending field in the error. Shared by every body the client can send (the
// message content and the forwarded-snapshot override) so the cap, the message
// and the metadata keys can't drift apart between them.
func validateContentSize(field, value string) error {
	if len(value) <= maxContentBytes {
		return nil
	}
	return errcode.BadRequest(
		fmt.Sprintf("%s exceeds maximum size of %d bytes", field, maxContentBytes),
		errcode.WithMetadata("maxContentBytes", strconv.Itoa(maxContentBytes), "attempted", strconv.Itoa(len(value))),
	)
}

// isBot reports whether account is bot-like — a real ".bot" bot or the
// "p_admin" platform-admin pseudo-account — via the model taxonomy. Plain
// "p_" QA test accounts are ordinary users and return false.
func isBot(account string) bool {
	return model.IsBot(account) || model.IsPlatformAdminAccount(account)
}
