package main

import (
	"fmt"
	"strings"

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

// isBot reports whether account is bot-like — a real ".bot" bot or the
// "p_tchatadmin_" platform-admin pseudo-account — via the model taxonomy. Plain
// "p_" QA test accounts are ordinary users and return false.
func isBot(account string) bool {
	return model.IsBot(account) || model.IsPlatformAdminAccount(account)
}
